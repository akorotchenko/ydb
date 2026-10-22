#pragma once

#include "shared_cache_items.h"
#include "shared_cache_memory.h"
#include "shared_cache_space.h"

namespace NKikimr::NSharedCache {

// Structural reference acquisition
template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::TryAcquireItemRef(
    TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen())
    {
        return {};
    }

    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || expected.IsFree() || expected.IsReplacing() || expected.IsReplaced() ||
            expected.IsTombstone())
        {
            return {};
        }

        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
        {
            if (Memory_.UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired) &&
                Memory_.NotifyResourcesAvailable()) {
                Traits_.NotifyResourcesAvailable();
            }
            return TSharedCacheItemRefImpl<TTraits>(cacheItem);
        }
    }
}

} // namespace NKikimr::NSharedCache
