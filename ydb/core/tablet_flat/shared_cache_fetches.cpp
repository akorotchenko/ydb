#include "shared_cache_fetches.h"
#include "shared_cache.h"

#include <util/generic/vector.h>

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
bool TCacheFetches<TTraits>::MatchesPendingFetch(
    const THandle& handle, THandleState state, TCacheItem cacheItem, const TPageFetchState& fetch) noexcept {
    return cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() && handle.Body.Fetch == &fetch &&
           fetch.Page().CacheItem() == cacheItem;
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

template class TCacheFetchServices<TProdTraits>;
template class TCacheFetchServices<TTestTraits>;
template class TCacheFetches<TProdTraits>;
template class TCacheFetches<TTestTraits>;

} // namespace NKikimr::NSharedCache
