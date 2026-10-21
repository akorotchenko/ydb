#include "shared_cache.h"

namespace NKikimr::NSharedCache {

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
void TSharedCache::RouteCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return;
    }

    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!state.IsCold() || !state.IsStickyNoneField() || state.Refs() != 0) {
        return;
    }

    Memory_.AddColdRingEntry();
    InvokeHook(ESharedCacheHookPoint::BeforeColdExchange, coldItem);
    const TColdCacheItem displaced = spaceOp.PutCold(coldItem);
    if (!displaced.IsNull()) {
        Memory_.RemoveColdRingEntry();
        cacheOp.EraseCold(displaced);
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimCold() noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.ReclaimCold();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimCold(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TRingView cold = spaceOp.Cold();
    for (ui64 attempt = 0; attempt < cold.Capacity; ++attempt) {
        if (ColdRingEntries() == 0) {
            return false;
        }
        InvokeHook(ESharedCacheHookPoint::BeforeColdRingPop, TCacheItem{});
        const ui64 rawItem = cold.Pop();
        if (rawItem != 0) {
            Memory_.RemoveColdRingEntry();
            if (cacheOp.EraseCold(TColdCacheItem::FromRaw(rawItem))) {
                return true;
            }
        }
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DemoteKeepColdForSoftLimit(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TRingView keep = spaceOp.KeepCold();
    for (ui64 attempt = 0; attempt < keep.Capacity && KeepColdOwnedBytes() != 0; ++attempt) {
        const ui64 raw = keep.Pop();
        if (raw == 0) {
            continue;
        }
        const TColdCacheItem item = TColdCacheItem::FromRaw(raw);
        if (cacheOp.Eviction().DemoteKeepCold(item)) {
            return true;
        }
        // The FIFO entry is gone even when a structural ref delays demotion.
        cacheOp.Eviction().WithdrawKeepColdEntry(item);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimKeepCold(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TRingView keep = spaceOp.KeepCold();
    for (ui64 attempt = 0; attempt < keep.Capacity && KeepColdOwnedBytes() != 0; ++attempt) {
        const ui64 raw = keep.Pop();
        if (raw == 0) {
            continue;
        }
        const TColdCacheItem item = TColdCacheItem::FromRaw(raw);
        if (!cacheOp.Eviction().MatchesKeepColdEntry(item)) {
            continue;
        }
        InvokeHook(ESharedCacheHookPoint::AfterKeepColdEntryMatched, item);
        if (cacheOp.EraseCold(item)) {
            return true;
        }
        cacheOp.Eviction().WithdrawKeepColdEntry(item);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::Reclaim(TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (cacheOp.ReclaimCold() || cacheOp.ReclaimKeepCold()) {
        return true;
    }
    // Cold is empty, so only moving the hot/cold boundary can make room, and that is EnforceCurrentLimit's job.
    return cacheOp.EnforceCurrentLimit(bytes);
}

#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

// Explicit instantiations of this file's Cold/KeepCold definitions for both trait variants.
template void TSharedCacheImpl<TProdTraits>::RouteCold(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::ReclaimCold() noexcept;
template bool TSharedCacheImpl<TProdTraits>::ReclaimCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::DemoteKeepColdForSoftLimit(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::ReclaimKeepCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::Reclaim(TSpaceOperation&, ui64) noexcept;

template void TSharedCacheImpl<TTestTraits>::RouteCold(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::ReclaimCold() noexcept;
template bool TSharedCacheImpl<TTestTraits>::ReclaimCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::DemoteKeepColdForSoftLimit(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::ReclaimKeepCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::Reclaim(TSpaceOperation&, ui64) noexcept;

} // namespace NKikimr::NSharedCache
