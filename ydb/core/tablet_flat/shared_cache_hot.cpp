#include "shared_cache.h"

namespace NKikimr::NSharedCache {

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TSharedCacheItemRef TSharedCacheItemRefImpl<TTraits>

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef TSharedCache::TryAcquireHotEvictionPin(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
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
            return TSharedCacheItemRef(cacheItem);
        }
    }
}

#undef TSharedCacheItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

// Native Hot eviction pin acquisition is instantiated here for both trait variants
template TSharedCacheItemRefImpl<TProdTraits> TSharedCacheImpl<TProdTraits>::TryAcquireHotEvictionPin(
    TSpaceOperation&, TCacheItem) noexcept;
template TSharedCacheItemRefImpl<TTestTraits> TSharedCacheImpl<TTestTraits>::TryAcquireHotEvictionPin(
    TSpaceOperation&, TCacheItem) noexcept;

} // namespace NKikimr::NSharedCache
