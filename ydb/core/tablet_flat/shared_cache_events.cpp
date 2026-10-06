#include "shared_cache.h"
#include "shared_cache_events.h"
#include "shared_handle.h"

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NSharedCache {

TRequestCompletion::TRequestCompletion(TRequestCompletionParams&& params) noexcept
    : ActorSystem_(params.ActorSystem)
    , ReplyTo_(params.ReplyTo)
    , EventCookie_(params.EventCookie)
    , ExecutorGeneration_(params.ExecutorGeneration)
    , RequestId_(params.RequestId)
    , PageCollection_(std::move(params.PageCollection))
    , Locations_(std::move(params.Pages))
    , Pages_(Locations_.size())
    , WaitPad_(std::move(params.WaitPad))
    , Cookie_(params.Cookie)
    , Notify_(params.Notify)
    , CoreRoute_(params.CoreRoute)
    , CompletionId_(params.CompletionId)
    , Remaining_(static_cast<ui32>(Locations_.size()))
{
    Y_DEBUG_ABORT_UNLESS(Locations_.size() <= Max<ui32>());
    Y_DEBUG_ABORT_UNLESS(ActorSystem_ && ReplyTo_ && PageCollection_);
    if (Remaining_.load(std::memory_order_relaxed) == 0) {
        SendResult();
    }
}

void TRequestCompletion::Complete(ui32 index, TSharedPageRef page, EPageFetchCompletion completion) noexcept {
    Y_DEBUG_ABORT_UNLESS(index < Pages_.size());
    Y_DEBUG_ABORT_UNLESS(completion != EPageFetchCompletion::Pending);
    TGuard<TMutex> guard(Mutex_);
    if (completion == EPageFetchCompletion::Ready && Status_.load(std::memory_order_relaxed) == NKikimrProto::OK) {
        Y_DEBUG_ABORT_UNLESS(page);
        Pages_[index] = std::move(page);
    } else {
        TEvResult::EStatus expected = NKikimrProto::OK;
        Status_.compare_exchange_strong(expected, NKikimrProto::ERROR, std::memory_order_relaxed);
    }

    const ui32 remaining = Remaining_.fetch_sub(1, std::memory_order_acq_rel);
    Y_DEBUG_ABORT_UNLESS(remaining != 0);
    if (remaining == 1) {
        SendResult();
    }
}

void TRequestCompletion::Cancel(bool replyImmediately) noexcept {
    TGuard<TMutex> guard(Mutex_);
    Status_.store(NKikimrProto::RACE, std::memory_order_relaxed);
    for (auto& page : Pages_) {
        page.Drop();
    }
    if (replyImmediately) {
        SendResult();
    }
}

bool TRequestCompletion::HasReadyPages() const noexcept {
    TGuard<TMutex> guard(Mutex_);
    for (const auto& page : Pages_) {
        if (page && !page.IsSticky()) {
            return true;
        }
    }
    return false;
}

void TRequestCompletion::PostponeForResources() noexcept {
    TGuard<TMutex> guard(Mutex_);
    ResourcePressure_ = true;
    Status_.store(NKikimrProto::RACE, std::memory_order_relaxed);
    for (auto& page : Pages_) {
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
        result->Pages.reserve(Pages_.size());
        for (ui32 index = 0; index < Pages_.size(); ++index) {
            const NTable::NPage::TPageLocation& location = Locations_[index];
            Y_DEBUG_ABORT_UNLESS(Pages_[index]);
            result->Pages.emplace_back(location.Offset, location.Size, std::move(Pages_[index]));
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

void TRequestPageWaiter::Complete(TPageCacheItem page, EPageFetchCompletion completion) noexcept {
    TSharedPageRef result;
    if (completion == EPageFetchCompletion::Ready) {
        TSharedCachePageRef cachePage;
        if (TSharedCache* cache = TSharedCache::TrySharedCachePages()) {
            auto binding = cache->BindCurrentThreadHazard();
            if (cache->AcquirePage(page, cachePage)) {
                result = MakeSharedPageRef(std::move(cachePage));
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

} // namespace NKikimr::NSharedCache
