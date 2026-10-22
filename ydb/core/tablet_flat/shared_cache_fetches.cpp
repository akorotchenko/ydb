#include "shared_cache_fetches.h"
#include "shared_cache.h"
#include "shared_cache_collections.h"

#include <util/generic/vector.h>

#include <new>

namespace NKikimr::NSharedCache {

template <class TTraits>
bool TCacheFetchServices<TTraits>::TryReserve(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages) noexcept {
    return Cache_.TryReserve(spaceOp, bytes, pages);
}

template <class TTraits>
void TCacheFetchServices<TTraits>::ReleaseReservation(ui64 bytes, ui64 pages) noexcept {
    Cache_.ReleaseReservation(bytes, pages);
}

template <class TTraits>
ui64 TCacheFetchServices<TTraits>::PageBytes(const THandle& handle) noexcept {
    return TSharedCacheImpl<TTraits>::PageBytes(handle);
}

template <class TTraits>
bool TCacheFetchServices<TTraits>::TryAcquirePending(
    TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef<TTraits>& owner) noexcept {
    return Cache_.TryAcquire(spaceOp, page.CacheItem(), true, owner.Get()) ==
           TSharedCacheImpl<TTraits>::EAcquireStatus::Pending;
}

template <class TTraits>
void TCacheFetchServices<TTraits>::MakeReadyState(
    TSpaceOperation& spaceOp, TCacheItem item, EHandleState state, TCollection* owner) noexcept {
    Cache_.MakeReadyState(spaceOp, item, state, owner);
}

template <class TTraits>
bool TCacheFetches<TTraits>::MatchesPendingFetch(
    const THandle& handle, THandleState state, TCacheItem cacheItem, const TPageFetchState& fetch) noexcept {
    return cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() && handle.Body.Fetch == &fetch &&
           fetch.Page().CacheItem() == cacheItem;
}

template <class TTraits>
TIntrusivePtr<TPageFetchState> TCacheFetches<TTraits>::TryAcquire(
    TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef<TTraits>& owner) noexcept {
    if (!Services_.TryAcquirePending(spaceOp, page, owner)) {
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
        const THandleState desired =
            finish ? expected.WithState(EHandleState::Completing).WithFrequency(0) : expected.DecrementRefs();
        const std::memory_order successOrder = finish ? std::memory_order_acq_rel : std::memory_order_release;
        if (!handle.State.compare_exchange_weak(expectedRaw, desired.Raw(), successOrder, std::memory_order_relaxed)) {
            continue;
        }
        if (finish) {
            Finish(owner, fetch, !expected.IsWaitingForMemory());
        } else {
            // The CAS above already decremented this ref.
            owner.Disarm();
        }
        return;
    }
}

template <class TTraits>
void TCacheFetches<TTraits>::DeletePendingFetch(THandle& handle) noexcept {
    if (TPageFetchState* fetch = std::exchange(handle.Body.Fetch, nullptr)) {
        fetch->UnRef();
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
    if (completing.IsStickyField()) {
        Collections_.BindOperation(spaceOp).UnlinkPendingStickyPage(cacheItem);
        completing = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    }
    DeletePendingFetch(handle);
    new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer();
    if (reserved) {
        Services_.ReleaseReservation(Services_.PageBytes(handle), 1);
    }
    const bool begun = Table_.TryBeginErase(spaceOp, finalizerRef.Get(), completing);
    Y_DEBUG_ABORT_UNLESS(begun);
    Traits_.Invoke(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
    const TSharedCacheKey key = LoadSharedCacheKey(handle, EItemKind::Page);
    Table_.FinishErase(spaceOp, cacheItem, key);
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
        Traits_.Invoke(ESharedCacheHookPoint::BeforePageReadyState, page);
        NActors::TSharedData data = fetch.TakeData();
        DeletePendingFetch(handle);
        new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer(std::move(data).Extract());
        const ui64 bytes = Services_.PageBytes(handle);
        EHandleState readyState = EHandleState::Hot;
        TCollection* owner = Collections_.BindOperation(spaceOp).PageCollection(handle);
        const bool regular = owner && owner->GetCacheMode() == ECacheMode::Regular &&
                             !THandleState::FromRaw(handle.State.load(std::memory_order_acquire)).IsStickyField();
        if (regular) {
            if (!Memory_.TryAddRetainedBytes(bytes)) {
                readyState = EHandleState::Cold;
            }
        } else {
            Memory_.AddRetainedBytes(bytes);
        }
        if (owner) {
            Collections_.AddResidentPageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
        }
        Memory_.CompleteReservedPage();
        Services_.MakeReadyState(spaceOp, page, readyState, owner);
    }
    TPageFetchState::DrainWaiters(waiters, [page](TIntrusivePtr<TPageFetchWaiter> waiter) {
        waiter->Complete(TPageCacheItem::FromValidated(page), EPageFetchCompletion::Ready);
    });
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
    const ui64 bytes = Services_.PageBytes(handle);
    if (!Services_.TryReserve(spaceOp, bytes, 1)) {
        return false;
    }
    // Only the move-only fetch owner reserves memory. Subscribers may change refs/Sticky concurrently.
    for (;;) {
        state = THandleState::FromRaw(raw);
        if (!MatchesPendingFetch(handle, state, item, fetch) || !state.IsWaitingForMemory()) {
            Services_.ReleaseReservation(bytes, 1);
            return false;
        }
        if (handle.State.compare_exchange_weak(raw, state.WithState(EHandleState::Begin).Raw(),
                std::memory_order_release, std::memory_order_relaxed)) {
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
            bytes += Services_.PageBytes(handle);
            waiting.push_back(&fetch);
        }
    }
    // Reserve the missing batch as a whole; two non-transactional requests must not each
    // hold a partial batch while waiting for the other one's bytes.
    if (!Services_.TryReserve(spaceOp, bytes, waiting.size())) {
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

template <class TTraits>
TPageFetchImpl<TTraits>::TPageFetchImpl(TPageFetchState* fetch) noexcept
    : FetchState_(fetch)
{
    Y_DEBUG_ABORT_UNLESS(FetchState_ && FetchState_->Page());
}

template <class TTraits>
TPageFetchImpl<TTraits>::TPageFetchImpl(TPageFetchImpl<TTraits>&& other) noexcept
    : FetchState_(std::move(other.FetchState_))
{}

template <class TTraits>
TPageFetchImpl<TTraits>& TPageFetchImpl<TTraits>::operator=(TPageFetchImpl<TTraits>&& other) noexcept {
    if (this != &other) {
        FailReady();
        FetchState_ = std::move(other.FetchState_);
    }
    return *this;
}

template <class TTraits>
TPageFetchImpl<TTraits>::~TPageFetchImpl() {
    FailReady();
}

template <class TTraits>
TPageCacheItem TPageFetchImpl<TTraits>::CacheItem() const noexcept {
    Y_DEBUG_ABORT_UNLESS(FetchState_);
    return FetchState_->Page();
}

template <class TTraits>
NTable::NPage::TPageLocation TPageFetchImpl<TTraits>::Location() const noexcept {
    Y_DEBUG_ABORT_UNLESS(FetchState_);
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    auto spaceOp = cache.BeginOperation();
    const TCacheItem cacheItem = FetchState_->Page().CacheItem();
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(
        cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() && handle.Body.Fetch == FetchState_.Get());
    return {
        NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
        handle.Key2OrSize.load(std::memory_order_relaxed),
        handle.Metadata.Page.Type,
        handle.Metadata.Page.Crc32,
    };
}

template <class TTraits>
ui64 TPageFetchImpl<TTraits>::Size() const noexcept {
    return Location().Size;
}

template <class TTraits>
bool TPageFetchImpl<TTraits>::HasActiveWaiters() const noexcept {
    return FetchState_ && FetchState_->HasActiveWaiters();
}

template <class TTraits>
void TPageFetchImpl<TTraits>::ForEachActiveWaiter(TFunctionRef<void(const TPageFetchWaiter&)> visitor) const noexcept {
    if (FetchState_) {
        FetchState_->ForEachActiveWaiter(visitor);
    }
}

template <class TTraits>
bool TPageFetchImpl<TTraits>::TryReserveMemory() noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    auto cacheOp = cache.BeginOperation();
    return cacheOp.Fetches().ReserveMemory(FetchState_->Page().CacheItem(), *FetchState_);
}

template <class TTraits>
bool TPageFetchImpl<TTraits>::Dispatch() noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    auto cacheOp = cache.BeginOperation();
    return cacheOp.Fetches().Dispatch(FetchState_->Page().CacheItem(), *FetchState_);
}

template <class TTraits>
bool TPageFetchImpl<TTraits>::MakeReady(NActors::TSharedData&& data) noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    bool ready;
    {
        auto cacheOp = cache.BeginOperation();
        // The fetch owns the insertion ref represented by FetchState_, without storing a second item identity.
        TOperationItemRef<TTraits> owner(cacheOp, FetchState_->Page().CacheItem());
        ready = cacheOp.Fetches().Complete(owner, *FetchState_, std::move(data));
    }
    FetchState_.Drop();
    return ready;
}

template <class TTraits>
bool TPageFetchImpl<TTraits>::FailReady() noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCacheImpl<TTraits>* cache = TSharedCacheImpl<TTraits>::TryGet();
    if (!cache) {
        // Missing actor context is terminal shutdown, as for native item references.
        FetchState_->AbandonWaiters();
        FetchState_.Drop();
        return false;
    }
    bool failed;
    {
        auto cacheOp = cache->BeginOperation();
        TOperationItemRef<TTraits> owner(cacheOp, FetchState_->Page().CacheItem());
        failed = cacheOp.Fetches().Fail(owner, *FetchState_);
    }
    FetchState_.Drop();
    return failed;
}

template class TCacheFetchServices<TProdTraits>;
template class TCacheFetchServices<TTestTraits>;
template class TCacheFetches<TProdTraits>;
template class TCacheFetches<TTestTraits>;

template class TPageFetchImpl<TProdTraits>;
template class TPageFetchImpl<TTestTraits>;

} // namespace NKikimr::NSharedCache
