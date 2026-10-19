#include "shared_cache.h"
#include "shared_cache_events.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <thread>

namespace NKikimr::NSharedCache {

template <class TTraits>
TSharedCacheCollectionRefImpl<TTraits> TSharedCacheImpl<TTraits>::AdmitCollection(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> collection)
    requires std::is_same_v<TTraits, TProdTraits>
{
    auto binding = BindCurrentThreadHazard();
    const TCollectionLocation location{ .Id = collection->Label(), .BackingSize = collection->BackingSize() };
    TCollectionCacheItem inserted;
    TSharedCacheCollectionRefImpl<TTraits> hit;
    auto status = FindOrInsert(location, inserted, hit);
    while (status == ESharedCacheResultStatus::Pending) {
        // A concurrent metadata publisher completes synchronously; share its ready collection.
        std::this_thread::yield();
        status = FindOrInsert(location, inserted, hit);
    }
    if (status == ESharedCacheResultStatus::Inserted) {
        Y_ENSURE(MakeReady(inserted, MakeHolder<TCollection>(std::move(collection), inserted)));
        Y_ENSURE(SetCollectionPagesCacheMode(inserted, ECacheMode::Regular));
        Y_ENSURE(Find(location.Id, hit) == ESharedCacheResultStatus::Hit);
    } else {
        Y_ENSURE(status == ESharedCacheResultStatus::Hit);
        hit->SetSkipBTreeIndexV1Shadow(collection->SkipBTreeIndexV1Shadow());
    }
    return hit;
}

template <class TTraits>
TSharedPageRefImpl<TTraits> TSharedCacheImpl<TTraits>::AdmitPage(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, NTable::NPage::TPageLocation location,
    NActors::TSharedData&& data, bool sticky)
    requires std::is_same_v<TTraits, TProdTraits>
{
    TEvSaveCompactedPages admission(std::move(collection));
    return admission.AddPage(*this, NPageCollection::TPageData(location, std::move(data)), sticky);
}

class TCompactedPageWaiter final : public TPageFetchWaiter {
public:
    void Complete(TPageCacheItem item, EPageFetchCompletion completion) noexcept override {
        Y_ABORT_UNLESS(completion == EPageFetchCompletion::Ready);
        // Acquire before the fetch releases its owner, so admission cannot race eviction.
        Y_ABORT_UNLESS(TSharedCache::Get().AcquirePage(item, Page));
    }

    TSharedPageRef Page;
};

TSharedPageRef TEvSaveCompactedPages::AddPage(
    TSharedCache& cachePages, NPageCollection::TPageData&& page, bool sticky) {
    return AddPage(cachePages, std::move(page), sticky, TSharedCache::EPageAdmission::Bounded);
}

TSharedPageRef TEvSaveCompactedPages::AddWrittenPage(
    TSharedCache& cachePages, NPageCollection::TPageData&& page, bool sticky) {
    return AddPage(cachePages, std::move(page), sticky, TSharedCache::EPageAdmission::Written);
}

TSharedPageRef TEvSaveCompactedPages::AddPage(
    TSharedCache& cachePages, NPageCollection::TPageData&& page, bool sticky, TSharedCache::EPageAdmission admission) {
    auto binding = cachePages.BindCurrentThreadHazard();
    if (!CacheItem) {
        auto collection = cachePages.AdmitCollection(PageCollection);
        CacheItem = collection.CacheItem();
    }
    Pages.push_back(page.Location);
    auto waiter = MakeIntrusive<TCompactedPageWaiter>();
    TSharedCachePageRequest request(page.Location, waiter, sticky);
    const bool admitted = admission == TSharedCache::EPageAdmission::Written
                              ? cachePages.FindOrInsertWrittenBatch(CacheItem, { &request, 1 })
                              : cachePages.FindOrInsertBatch(CacheItem, { &request, 1 }, false);
    if (!admitted) {
        Y_ENSURE(!sticky, "Cannot admit Sticky compacted page " << page.Location.Offset);
        return {};
    }
    if (request.Status() == ESharedCacheResultStatus::Hit) {
        return std::move(request.Page());
    }
    Y_ENSURE(request.Status() == ESharedCacheResultStatus::Inserted);
    Y_ENSURE(request.Fetch().MakeReady(std::move(page.Data)));
    return std::move(waiter->Page);
}

TRequestCompletion::TRequestCompletion(TRequestCompletionParams&& params) noexcept
    : ActorSystem_(params.ActorSystem)
    , ReplyTo_(params.ReplyTo)
    , EventCookie_(params.EventCookie)
    , ExecutorGeneration_(params.ExecutorGeneration)
    , RequestId_(params.RequestId)
    , PageCollection_(std::move(params.PageCollection))
    , Locations_(std::move(params.Pages))
    , ReadyPages_(Locations_.size())
    , WaitPad_(std::move(params.WaitPad))
    , Cookie_(params.Cookie)
    , Notify_(params.Notify)
    , CoreRoute_(params.CoreRoute)
    , CompletionId_(params.CompletionId)
    , RemainingPages_(static_cast<ui32>(Locations_.size()))
{
    Y_DEBUG_ABORT_UNLESS(Locations_.size() <= Max<ui32>());
    Y_DEBUG_ABORT_UNLESS(ActorSystem_ && ReplyTo_ && PageCollection_);
    if (RemainingPages_.load(std::memory_order_relaxed) == 0) {
        SendResult();
    }
}

void TRequestCompletion::Complete(ui32 index, TSharedPageRef page, EPageFetchCompletion completion) noexcept {
    Y_DEBUG_ABORT_UNLESS(index < ReadyPages_.size());
    Y_DEBUG_ABORT_UNLESS(completion != EPageFetchCompletion::Pending);
    TGuard<TMutex> guard(Mutex_);
    if (completion == EPageFetchCompletion::Ready && Status_.load(std::memory_order_relaxed) == NKikimrProto::OK) {
        Y_DEBUG_ABORT_UNLESS(page);
        ReadyPages_[index] = std::move(page);
        if (WaitPad_) {
            WaitPad_->HasPinnedPages.store(true, std::memory_order_release);
        }
    } else {
        TEvResult::EStatus expected = NKikimrProto::OK;
        if (Status_.compare_exchange_strong(expected, NKikimrProto::ERROR, std::memory_order_relaxed)) {
            for (auto& readyPage : ReadyPages_) {
                readyPage.Drop();
            }
        }
    }

    const ui32 remaining = RemainingPages_.fetch_sub(1, std::memory_order_acq_rel);
    Y_DEBUG_ABORT_UNLESS(remaining != 0);
    if (remaining == 1 || HasFailed()) {
        SendResult();
    }
}

void TRequestCompletion::Cancel(bool replyImmediately) noexcept {
    TGuard<TMutex> guard(Mutex_);
    Status_.store(NKikimrProto::RACE, std::memory_order_relaxed);
    for (auto& page : ReadyPages_) {
        page.Drop();
    }
    if (replyImmediately) {
        SendResult();
    }
}

void TRequestCompletion::PostponeForResources() noexcept {
    TGuard<TMutex> guard(Mutex_);
    ResourcePressure_ = true;
    Status_.store(NKikimrProto::RACE, std::memory_order_relaxed);
    for (auto& page : ReadyPages_) {
        page.Drop();
    }
    SendResult();
}

void TRequestCompletion::NotifyResourcesReady() noexcept {
    TGuard<TMutex> guard(Mutex_);
    auto* result =
        new TEvResult(PageCollection_, NKikimrProto::OK, Cookie_, ExecutorGeneration_, RequestId_, CoreRoute_);
    result->WaitPad = WaitPad_;
    result->ResourcesReady = true;
    ActorSystem_->Send(ReplyTo_, result, 0, EventCookie_);
}

void TRequestCompletion::SendResult() noexcept {
    if (ResultSent_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    const TEvResult::EStatus status = Status_.load(std::memory_order_relaxed);
    auto result = MakeHolder<TEvResult>(PageCollection_, status, Cookie_, ExecutorGeneration_, RequestId_, CoreRoute_);
    result->WaitPad = WaitPad_;
    result->ResourcePressure = ResourcePressure_;
    if (status == NKikimrProto::OK) {
        result->Pages.reserve(ReadyPages_.size());
        for (ui32 index = 0; index < ReadyPages_.size(); ++index) {
            const NTable::NPage::TPageLocation& location = Locations_[index];
            Y_DEBUG_ABORT_UNLESS(ReadyPages_[index]);
            result->Pages.emplace_back(location.Offset, location.Size, std::move(ReadyPages_[index]));
        }
    }
    if (Notify_) {
        ActorSystem_->Send(Notify_, new TEvRequestAnswered(status, CompletionId_), 0, 0);
    }
    ActorSystem_->Send(ReplyTo_, result.Release(), 0, EventCookie_);
}

TRequestPageWaiter::TRequestPageWaiter(TIntrusivePtr<TRequestCompletion> completion, ui32 index) noexcept
    : Completion_(std::move(completion))
    , Index_(index)
{
    Y_DEBUG_ABORT_UNLESS(Completion_);
}

bool TRequestPageWaiter::IsActive() const noexcept {
    return Completion_->IsActive();
}

void TRequestPageWaiter::Complete(TPageCacheItem page, EPageFetchCompletion completion) noexcept {
    TSharedPageRef result;
    if (completion == EPageFetchCompletion::Ready) {
        TSharedPageRef cachePage;
        if (TSharedCache* cache = TSharedCache::TryGet()) {
            auto binding = cache->BindCurrentThreadHazard();
            if (cache->AcquirePage(page, cachePage)) {
                result = std::move(cachePage);
            } else {
                completion = EPageFetchCompletion::Failed;
            }
        } else {
            completion = EPageFetchCompletion::Failed;
        }
    }
    Completion_->Complete(Index_, std::move(result), completion);
    Completion_.Drop();
}

template TSharedCacheCollectionRef TSharedCacheImpl<TProdTraits>::AdmitCollection(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> collection);
template TSharedPageRef TSharedCacheImpl<TProdTraits>::AdmitPage(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, NTable::NPage::TPageLocation location,
    NActors::TSharedData&& data, bool sticky);

} // namespace NKikimr::NSharedCache
