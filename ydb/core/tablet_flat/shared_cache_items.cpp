#include "shared_cache_items.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::TryAcquireHotEvictionPin(
    TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen()) {
        return {};
    }
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsHot() || !expected.IsStickyNoneField()) {
            return {};
        }
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            return TSharedCacheItemRefImpl<TTraits>(cacheItem);
        }
    }
}

// Native Hot pin acquisition is instantiated here for both trait variants
template class TCacheItems<TProdTraits>;
template class TCacheItems<TTestTraits>;

} // namespace NKikimr::NSharedCache
