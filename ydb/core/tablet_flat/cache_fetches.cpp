#include "cache_fetches.h"
#include "cache_items.h"
#include "cache.h"
#include "cache_collections.h"

#include <util/generic/vector.h>

#include <new>

namespace NKikimr::NSharedCache {

template <class TTraits>
TIntrusivePtr<TPageFetchState> TCacheFetches<TTraits>::TryAcquire(
    TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef<TTraits>& owner) noexcept {
    if (Cache_.Items_.BindOperation(spaceOp).TryAcquire(page.CacheItem(), true, owner.Get()) !=
        TSharedCacheImpl<TTraits>::EAcquireStatus::Pending) {
        return {};
    }
    TIntrusivePtr<TPageFetchState> fetch = spaceOp.Handles()[page.Index()].Body.Fetch;
    Y_DEBUG_ABORT_UNLESS(fetch);
    return fetch;
}

template <class TTraits>
THandle* TCacheFetches<TTraits>::TryGetPendingHandle(
    TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(owner);
    TSpaceOperation& spaceOp = owner.SpaceOperation();
    const TCacheItem cacheItem = owner.CacheItem();
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AccessibleHandleCount());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!MatchesPendingFetch(handle, state, cacheItem, fetch) || fetch.Completion() != EPageFetchCompletion::Pending)
    {
        return nullptr;
    }
    return &handle;
}

template <class TTraits>
bool TCacheFetches<TTraits>::Complete(
    TOperationItemRef<TTraits>& owner, TPageFetchState& fetch, NActors::TSharedData&& data) noexcept {
    Y_DEBUG_ABORT_UNLESS(data);
    THandle* handle = TryGetPendingHandle(owner, fetch);
    if (!handle || THandleState::FromRaw(handle->State.load(std::memory_order_acquire)).IsWaitingForMemory()) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(handle->Key2OrSize.load(std::memory_order_relaxed) == data.size());
    fetch.SetReady(std::move(data));
    // Drop the I/O-owner ref; the last pending releaser performs MakeReadyState.
    Release(owner, fetch);
    return true;
}

template <class TTraits>
bool TCacheFetches<TTraits>::Fail(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept {
    if (!TryGetPendingHandle(owner, fetch)) {
        return false;
    }
    fetch.SetFailed();
    Release(owner, fetch);
    return true;
}

template <class TTraits>
void TCacheFetches<TTraits>::Release(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(owner);
    TSpaceOperation& spaceOp = owner.SpaceOperation();
    const TCacheItem cacheItem = owner.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(MatchesPendingFetch(handle, expected, cacheItem, fetch) && expected.Refs() > 0);
        const bool finish = expected.Refs() == 1 && fetch.Completion() != EPageFetchCompletion::Pending;
        if (finish && expected.IsStickyField() && fetch.Completion() == EPageFetchCompletion::Failed) {
            // List owners may need this pending predecessor while withdrawing Sticky siblings
            Cache_.Collections_.BindOperation(spaceOp).UnlinkPendingStickyPage(cacheItem);
            expectedRaw = handle.State.load(std::memory_order_relaxed);
            continue;
        }
        const THandleState desired =
            finish ? expected.WithState(EHandleState::Completing).WithFrequency(0) : expected.DecrementRefs();
        const std::memory_order successOrder = finish ? std::memory_order_acq_rel : std::memory_order_release;
        if (!handle.State.compare_exchange_weak(expectedRaw, desired.Raw(), successOrder, std::memory_order_relaxed)) {
            continue;
        }
        if (finish) {
            Cache_.InvokeHook(ECacheHookPoint::AfterFetchCompletingClaim, cacheItem);
            Finish(owner, fetch, !expected.IsWaitingForMemory());
        } else {
            // The CAS above already decremented this ref.
            owner.Disarm();
        }
        return;
    }
}

template <class TTraits>
void TCacheFetches<TTraits>::FinishFailed(TOperationItemRef<TTraits>& finalizerRef, TPageFetchState& fetch,
    TPageFetchWaiter* waiters, bool reserved) noexcept {
    TSpaceOperation& spaceOp = finalizerRef.SpaceOperation();
    const TCacheItem cacheItem = finalizerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    THandleState completing = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(completing) && completing.IsCompleting() && completing.Refs() == 1 &&
                         handle.Body.Fetch == &fetch);
    Y_DEBUG_ABORT_UNLESS(completing.IsStickyNoneField());
    DeletePendingFetch(handle);
    new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer();
    if (reserved) {
        Cache_.ReleaseReservation(TCacheItems<TTraits>::PageBytes(handle), 1);
    }
    const bool begun = Cache_.Table_.TryBeginErase(spaceOp, finalizerRef.Get(), completing);
    Y_DEBUG_ABORT_UNLESS(begun);
    Cache_.InvokeHook(ECacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
    const TCacheKey key = LoadSharedCacheKey(handle, EItemKind::Page);
    Cache_.Table_.FinishErase(spaceOp, cacheItem, key);
    TPageFetchState::DrainWaiters(waiters, [cacheItem](TIntrusivePtr<TPageFetchWaiter> waiter) {
        waiter->Complete(TPageCacheItem::FromValidated(cacheItem), EPageFetchCompletion::Failed);
    });
}

template <class TTraits>
void TCacheFetches<TTraits>::Finish(
    TOperationItemRef<TTraits>& finalizerRef, TPageFetchState& fetch, bool reserved) noexcept {
    TSpaceOperation& spaceOp = finalizerRef.SpaceOperation();
    const TCacheItem page = finalizerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[page.Index()];
    const THandleState completing = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(
        page.Matches(completing) && completing.IsCompleting() && completing.Refs() == 1 && handle.Body.Fetch == &fetch);
    TPageFetchWaiter* waiters = fetch.DetachWaiters();
    Y_DEBUG_ABORT_UNLESS(waiters != TPageFetchState::Sealed);
    if (fetch.Completion() == EPageFetchCompletion::Failed) {
        FinishFailed(finalizerRef, fetch, waiters, reserved);
        return;
    }

    Y_DEBUG_ABORT_UNLESS(reserved && fetch.Completion() == EPageFetchCompletion::Ready);
    {
        Cache_.InvokeHook(ECacheHookPoint::BeforePageReadyState, page);
        NActors::TSharedData data = fetch.TakeData();
        TKeepReservation keep = std::move(fetch.KeepReservation_);
        DeletePendingFetch(handle);
        new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer(std::move(data).Extract());
        TCollection* owner = Cache_.Collections_.BindOperation(spaceOp).PageCollection(handle);
        Cache_.Memory_.CompleteReservedPage();
        Cache_.MakeReadyState(spaceOp, page, EHandleState::Hot, owner, &keep);
    }
    Cache_.InvokeHook(ECacheHookPoint::BeforeReadyWaiters, page);
    TPageFetchState::DrainWaiters(waiters, [page](TIntrusivePtr<TPageFetchWaiter> waiter) {
        waiter->Complete(TPageCacheItem::FromValidated(page), EPageFetchCompletion::Ready);
    });
    // Callbacks own their immutable results now; follow any relocation before enforcing placement
    TOperationItemRef<TTraits> current(spaceOp, Cache_.AcquirePageItem(spaceOp, TPageCacheItem::FromValidated(page)));
    Cache_.InvokeHook(ECacheHookPoint::BeforeReadyReconcile, current.CacheItem());
    if (current && Cache_.Eviction_.BindOperation(spaceOp).TouchPage(current.Get())) {
        const auto state =
            THandleState::FromRaw(spaceOp.Handles()[current.CacheItem().Index()].State.load(std::memory_order_acquire));
        if (state.IsHot()) {
            Cache_.Eviction_.BindOperation(spaceOp).RouteOwnedHot(current.Get(), EHotLevel::L1);
        }
    }
    Cache_.NotifyResourcesAvailable();
}

template <class TTraits>
bool TCacheFetches<TTraits>::ReserveMemory(TSpaceOperation& spaceOp, TCacheItem item, TPageFetchState& fetch) noexcept {
    THandle& handle = spaceOp.Handles()[item.Index()];
    ui64 raw = handle.State.load(std::memory_order_acquire);
    THandleState state = THandleState::FromRaw(raw);
    if (!MatchesPendingFetch(handle, state, item, fetch)) {
        return false;
    }
    if (!state.IsWaitingForMemory()) {
        return true;
    }
    const ui64 bytes = TCacheItems<TTraits>::PageBytes(handle);
    if (!Cache_.TryReserve(spaceOp, bytes, 1)) {
        return false;
    }
    // Only the move-only fetch owner reserves memory. Subscribers may change refs/Sticky concurrently.
    for (;;) {
        state = THandleState::FromRaw(raw);
        if (!MatchesPendingFetch(handle, state, item, fetch) || !state.IsWaitingForMemory()) {
            Cache_.ReleaseReservation(bytes, 1);
            return false;
        }
        if (handle.State.compare_exchange_weak(raw, state.WithState(EHandleState::Begin).Raw(),
                std::memory_order_release, std::memory_order_relaxed)) {
            fetch.KeepReservation_.MarkPending();
            return true;
        }
    }
}

template <class TTraits>
bool TCacheFetches<TTraits>::TryReserveBatch(
    TSpaceOperation& spaceOp, TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept {
    TVector<TPageFetchState*> waiting;
    ui64 bytes = 0;
    for (auto& page : pages) {
        TPageFetchState& fetch = *page.FetchState_;
        const TCacheItem item = fetch.Page().CacheItem();
        THandle& handle = spaceOp.Handles()[item.Index()];
        const auto state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_ABORT_UNLESS(MatchesPendingFetch(handle, state, item, fetch));
        if (state.IsWaitingForMemory()) {
            bytes += TCacheItems<TTraits>::PageBytes(handle);
            waiting.push_back(&fetch);
        }
    }
    // Reserve the missing batch as a whole; two non-transactional requests must not each
    // hold a partial batch while waiting for the other one's bytes.
    if (!Cache_.TryReserve(spaceOp, bytes, waiting.size())) {
        return false;
    }
    for (auto* fetch : waiting) {
        const TCacheItem item = fetch->Page().CacheItem();
        THandle& handle = spaceOp.Handles()[item.Index()];
        ui64 raw = handle.State.load(std::memory_order_relaxed);
        for (;;) {
            const auto state = THandleState::FromRaw(raw);
            Y_ABORT_UNLESS(MatchesPendingFetch(handle, state, item, *fetch) && state.IsWaitingForMemory());
            if (handle.State.compare_exchange_weak(raw, state.WithState(EHandleState::Begin).Raw(),
                    std::memory_order_release, std::memory_order_relaxed)) {
                fetch->KeepReservation_.MarkPending();
                break;
            }
        }
    }
    return true;
}

template <class TTraits>
bool TCacheFetches<TTraits>::Dispatch(TSpaceOperation& spaceOp, TCacheItem cacheItem, TPageFetchState& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!MatchesPendingFetch(handle, expected, cacheItem, fetch)) {
            return false;
        }
        if (expected.IsRequested() || expected.IsQueuedRequested()) {
            return true;
        }
        if (!expected.IsBegin() && !expected.IsQueued()) {
            return false;
        }
        const EHandleState requested = expected.IsQueued() ? EHandleState::QueuedRequested : EHandleState::Requested;
        const THandleState desired = expected.WithState(requested);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

template class TCacheFetches<TProdTraits>;
template class TCacheFetches<TTestTraits>;

} // namespace NKikimr::NSharedCache
