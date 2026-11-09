#include "cache.h"
#include "shared_cache_events.h"

#include <ydb/library/actors/core/actorsystem.h>

#include <thread>

namespace NKikimr::NSharedCache {

template <class TTraits>
TCacheCollectionRefImpl<TTraits> TCacheImpl<TTraits>::AdmitCollection(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> collection)
    requires std::is_same_v<TTraits, TProdTraits>
{
    THolder<TCollection> value;
    TCollectionCacheItem inserted;
    TCacheCollectionRefImpl<TTraits> hit;
    auto status = FindOrInsert(collection, value, inserted, hit);
    while (status == ECacheResultStatus::Pending) {
        // A concurrent metadata publisher completes synchronously; share its ready collection.
        std::this_thread::yield();
        status = FindOrInsert(collection, value, inserted, hit);
    }
    if (status == ECacheResultStatus::Inserted) {
        Y_ENSURE(MakeReady(inserted, std::move(value), hit));
    } else {
        Y_ENSURE(status == ECacheResultStatus::Hit);
        Y_ENSURE(hit->PageCollection()->IsCompatible(*collection), "Conflicting collection metadata descriptor");
        hit->SetSkipBTreeIndexV1Shadow(collection->SkipBTreeIndexV1Shadow());
    }
    return hit;
}

template <class TTraits>
TCachePageRefImpl<TTraits> TCacheImpl<TTraits>::AdmitPage(
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
        Y_ABORT_UNLESS(TCache::Get().AcquirePage(item, Page));
    }

    TCachePageRef Page;
};

TCachePageRef TEvSaveCompactedPages::AddPage(
    TCache& cachePages, NPageCollection::TPageData&& page, bool sticky, TCache::EPageAdmission admission) {
    if (!Collection_) {
        Collection_ = cachePages.AdmitCollection(PageCollection);
        CacheItem = Collection_.CacheItem();
    }
    Pages.push_back(page.Location);
    auto waiter = MakeIntrusive<TCompactedPageWaiter>();
    TCachePageRequest request(page.Location, waiter, sticky);
    const bool admitted = admission == TCache::EPageAdmission::Written
                              ? cachePages.FindOrInsertWrittenBatch(CacheItem, { &request, 1 })
                              : cachePages.FindOrInsertBatch(CacheItem, { &request, 1 }, false);
    if (!admitted) {
        Y_ENSURE(!sticky, "Cannot admit Sticky compacted page " << page.Location.Offset);
        return {};
    }
    if (request.Status() == ECacheResultStatus::Hit) {
        return std::move(request.Page());
    }
    Y_ENSURE(request.Status() == ECacheResultStatus::Inserted);
    Y_ENSURE(request.Fetch().MakeReady(std::move(page.Data)));
    return std::move(waiter->Page);
}

TRequestCompletion::TRequestCompletion(TRequestCompletionParams&& params) noexcept
    : ActorSystem_(params.ActorSystem)
    , ReplyTo_(params.ReplyTo)
    , EventCookie_(params.EventCookie)
    , ExecutorGeneration_(params.ExecutorGeneration)
    , RequestId_(params.RequestId)
    , PageCollection_(params.Collection ? params.Collection->PageCollection() : std::move(params.PageCollection))
    , Collection_(std::move(params.Collection))
    , Locations_(std::move(params.Pages))
    , ReadyPages_(Locations_.empty() ? nullptr : new std::atomic<ui64>[Locations_.size()])
    , WaitPad_(std::move(params.WaitPad))
    , Cookie_(params.Cookie)
    , Notify_(params.Notify)
    , CoreRoute_(params.CoreRoute)
    , CompletionId_(params.CompletionId)
    , Control_(params.Status)
    , RemainingPages_(static_cast<ui32>(Locations_.size()))
{
    Y_DEBUG_ABORT_UNLESS(Locations_.size() <= Max<ui32>());
    Y_DEBUG_ABORT_UNLESS(ActorSystem_ && ReplyTo_ && PageCollection_);
    for (ui32 index = 0; index < Locations_.size(); ++index) {
        ReadyPages_[index].store(params.Status == NKikimrProto::OK ? 0 : ClosedPageSlot, std::memory_order_relaxed);
    }
    if (RemainingPages_.load(std::memory_order_relaxed) == 0 || params.Status != NKikimrProto::OK) {
        Apply(Update(EAction::Finish, EReplyMode::Now));
    }
}

TRequestCompletion::~TRequestCompletion() {
    ReleasePages();
}

bool TRequestCompletion::BindCollection(TCacheCollectionRef collection) noexcept {
    Y_DEBUG_ABORT_UNLESS(collection);
    if (Collection_) {
        return Collection_.CacheItem() == collection.CacheItem();
    }
    if (!collection->PageCollection()->IsCompatible(*PageCollection_)) {
        return false;
    }
    auto descriptor = collection->PageCollection();
    Collection_ = std::move(collection);
    // The old input is released after both canonical fields have been initialized
    PageCollection_.Swap(descriptor);
    return true;
}

void TRequestCompletion::Fail() noexcept {
    Apply(Update(EAction::Fail, EReplyMode::Now));
}

void TRequestCompletion::Complete(ui32 index, TCachePageRef page, EPageFetchCompletion completion) noexcept {
    Y_DEBUG_ABORT_UNLESS(index < Locations_.size());
    Y_DEBUG_ABORT_UNLESS(completion != EPageFetchCompletion::Pending);
    TAction action;
    if (completion != EPageFetchCompletion::Ready || !IsActive() || !PublishPage(index, page)) {
        action = Update(EAction::Fail, EReplyMode::Now);
    }
    // A rejected local owner survives countdown and sending, including destructor reentry
    Apply(FinishPage(action));
}

void TRequestCompletion::Cancel(EReplyMode replyMode) noexcept {
    Apply(Update(EAction::Cancel, replyMode));
}

void TRequestCompletion::PostponeForResources() noexcept {
    Apply(Update(EAction::Postpone, EReplyMode::Now));
}

bool TRequestCompletion::PublishPage(ui32 index, TCachePageRef& page) noexcept {
    Y_DEBUG_ABORT_UNLESS(page);
    // Disarm before publication: a closer may take and destroy this owner as soon as CAS succeeds
    const TCacheItem item = page.TakeNativeRef();
    ui64 expected = 0;
    if (!ReadyPages_[index].compare_exchange_strong(
            expected, item.Raw(), std::memory_order_release, std::memory_order_relaxed))
    {
        page = TCachePageRef::AdoptNativeRef(item);
        return false;
    }
    if (WaitPad_) {
        WaitPad_->HasPinnedPages.store(true, std::memory_order_release);
    }
    return true;
}

TRequestCompletion::TAction TRequestCompletion::Update(EAction action, EReplyMode replyMode) noexcept {
    ui64 expected = Control_.load(std::memory_order_acquire);
    for (;;) {
        if (expected & ReplyClaimedMask) {
            return {};
        }
        ui64 desired = expected;
        const bool active = Status(expected) == NKikimrProto::OK;
        // A late page failure and an explicit failure both preserve the first stop reason
        if (action == EAction::Fail && !active) {
            return {};
        }
        if (active) {
            switch (action) {
                case EAction::Fail:
                    desired = (expected & ~StatusMask) | NKikimrProto::ERROR;
                    break;
                case EAction::Cancel:
                    desired = (expected & ~StatusMask) | NKikimrProto::RACE;
                    break;
                case EAction::Postpone:
                    desired = (expected & ~StatusMask) | NKikimrProto::RACE | ResourcePressureMask;
                    break;
                case EAction::Finish:
                    break;
            }
        }

        TAction result;
        // Only the first stop closes slots; a claimed reply cannot change its outcome
        result.ReleasePages = active && Status(desired) != NKikimrProto::OK;
        if (replyMode == EReplyMode::Now) {
            desired |= ReplyClaimedMask;
            result.Reply = desired;
        }
        if (desired == expected ||
            Control_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            return result;
        }
    }
}

TRequestCompletion::TAction TRequestCompletion::FinishPage(TAction action) noexcept {
    const ui32 remaining = RemainingPages_.fetch_sub(1, std::memory_order_acq_rel);
    Y_DEBUG_ABORT_UNLESS(remaining != 0);
    if (remaining == 1) {
        const TAction last = Update(EAction::Finish, EReplyMode::Now);
        if (last.Reply) {
            Y_DEBUG_ABORT_UNLESS(!action.Reply);
            action.Reply = last.Reply;
        }
        action.ReleasePages |= last.ReleasePages;
    }
    return action;
}

void TRequestCompletion::Apply(TAction action) noexcept {
    if (action.Reply) {
        SendResult(TakeResult(action.Reply));
    }
    if (action.ReleasePages) {
        ReleasePages();
    }
}

void TRequestCompletion::ReleasePages() noexcept {
    for (ui32 index = 0; index < Locations_.size(); ++index) {
        const TCacheItem item =
            TCacheItem::FromRaw(ReadyPages_[index].exchange(ClosedPageSlot, std::memory_order_acq_rel));
        if (!item.IsNull()) {
            // No other callback helps this closer, even if native destruction reenters Complete
            auto page = TCachePageRef::AdoptNativeRef(item);
        }
    }
}

void TRequestCompletion::NotifyResourcesReady() noexcept {
    auto* result =
        new TEvResult(PageCollection(), NKikimrProto::OK, Cookie_, ExecutorGeneration_, RequestId_, CoreRoute_);
    result->WaitPad = WaitPad_;
    result->ResourcesReady = true;
    ActorSystem_->Send(ReplyTo_, result, 0, EventCookie_);
}

void TRequestCompletion::NotifyWorkingSetTooLarge() noexcept {
    auto* result =
        new TEvResult(PageCollection(), NKikimrProto::ERROR, Cookie_, ExecutorGeneration_, RequestId_, CoreRoute_);
    result->WaitPad = WaitPad_;
    result->WorkingSetTooLarge = true;
    ActorSystem_->Send(ReplyTo_, result, 0, EventCookie_);
}

THolder<TEvResult> TRequestCompletion::TakeResult(ui64 reply) noexcept {
    const TEvResult::EStatus status = Status(reply);
    auto result = MakeHolder<TEvResult>(PageCollection(), status, Cookie_, ExecutorGeneration_, RequestId_, CoreRoute_);
    result->WaitPad = WaitPad_;
    result->ResourcePressure = reply & ResourcePressureMask;
    if (status == NKikimrProto::OK) {
        result->Pages.reserve(Locations_.size());
        for (ui32 index = 0; index < Locations_.size(); ++index) {
            const NTable::NPage::TPageLocation& location = Locations_[index];
            const TCacheItem item =
                TCacheItem::FromRaw(ReadyPages_[index].exchange(ClosedPageSlot, std::memory_order_acq_rel));
            Y_DEBUG_ABORT_UNLESS(!item.IsNull());
            result->Pages.emplace_back(location.Offset, location.Size, TCachePageRef::AdoptNativeRef(item));
        }
    }
    return result;
}

void TRequestCompletion::SendResult(THolder<TEvResult> result) noexcept {
    if (!result) {
        return;
    }
    if (Notify_) {
        ActorSystem_->Send(Notify_, new TEvRequestAnswered(result->Status, CompletionId_), 0, 0);
    }
    ActorSystem_->Send(ReplyTo_, result.Release(), 0, EventCookie_);
}

void TRequestPageWaiter::Complete(TPageCacheItem page, EPageFetchCompletion completion) noexcept {
    TCachePageRef result;
    if (completion == EPageFetchCompletion::Ready) {
        TCachePageRef cachePage;
        if (TCache* cache = TCache::TryGet()) {
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

template TCacheCollectionRef TCacheImpl<TProdTraits>::AdmitCollection(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> collection);
template TCachePageRef TCacheImpl<TProdTraits>::AdmitPage(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, NTable::NPage::TPageLocation location,
    NActors::TSharedData&& data, bool sticky);

} // namespace NKikimr::NSharedCache
