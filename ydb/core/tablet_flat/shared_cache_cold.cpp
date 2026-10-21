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
bool TSharedCache::WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (TCollection* collection = cacheOp.Collections().PageCollection(page)) {
        return collection->CacheMode_.load(std::memory_order_acquire) == ECacheMode::TryKeepInMemory &&
               collection->IsKeepAllowedPage(page.Metadata.Page.Type);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DemoteKeepCold(
    TSpaceOperation& spaceOp, TColdCacheItem coldItem, TPageCacheItem expectedPage) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return false;
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!expected.IsKeepCold() || expected.Refs() != 0 || (expectedPage && !expectedPage.Matches(expected)) ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion())
        {
            return false;
        }
        // Claim the demotion and a native ref together: an already-matched consumer may still tombstone it.
        const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Cold);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            TOperationItemRef pin(spaceOp, TCacheItem::Make(expected.Version(), coldItem.Index()));
            InvokeHook(ESharedCacheHookPoint::AfterKeepColdDemoted, coldItem);
            const ui64 bytes = PageBytes(handle);
            AdvanceColdMembership(handle);
            Memory_.ReleaseRetainedBytes(bytes);
            Memory_.KeepCold().TransferBytesTo(Memory_.Cold(), bytes);
            Memory_.KeepCold().TransferPageTo(Memory_.Cold(), EItemKind::Page);
            Memory_.AddColdItem();
            Memory_.ReleaseKeepColdOwnedBytes(bytes);
            if (TCollection* owner = cacheOp.Collections().PageCollection(handle)) {
                Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
            cacheOp.ReleaseColdDemotionPin(pin);
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseColdDemotionPin(TSpaceOperation& spaceOp, TOperationItemRef& pin) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem cacheItem = pin.CacheItem();
    pin.Disarm();
    // A policy demotion must not readmit the page to KeepCold when releasing its temporary ref.
    if (TCollection* owner = cacheOp.ReleaseItem(cacheItem, false)) {
        cacheOp.Collections().DropOwnerReference(*owner);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (cacheOp.DemoteKeepCold(coldItem)) {
        return;
    }
    if (!cacheOp.MatchesKeepColdEntry(coldItem)) {
        return;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    ui32 coldVersion;
    if (!AdvanceColdMembership(handle, coldItem.ColdVersion(), false, coldVersion)) {
        return;
    }
    cacheOp.DemoteKeepCold(TColdCacheItem::Make(coldVersion, coldItem.Index()));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MatchesKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return false;
    }
    const THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return false;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    return state.IsKeepCold();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::IsKeepColdQueued(const THandle& handle) noexcept {
    return (handle.ColdVersion.load(std::memory_order_acquire) & KeepColdQueuedMask) != 0;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RouteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!expected.IsKeepCold() ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
            return;
        }
        if (expected.Refs() != 0) {
            // The caller took this page's ring entry; its last reference will process the withdrawal.
            cacheOp.WithdrawKeepColdEntry(coldItem);
            return;
        }
        if (handle.State.compare_exchange_weak(
                expectedRaw, expected.IncrementRefs().Raw(), std::memory_order_acquire, std::memory_order_relaxed)) {
            break;
        }
    }
    TOperationItemRef pin(spaceOp, TCacheItem::Make(THandleState::FromRaw(expectedRaw).Version(), coldItem.Index()));
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    if (!cacheOp.WantsKeepCold(handle)) {
        cacheOp.WithdrawKeepColdEntry(coldItem);
        return;
    }
    ui32 queuedVersion;
    if (!AdvanceColdMembership(handle, coldItem.ColdVersion(), true, queuedVersion)) {
        return;
    }
    const TColdCacheItem queuedItem = TColdCacheItem::Make(queuedVersion, coldItem.Index());
    const ui64 displaced = spaceOp.KeepCold().PutAndPop(queuedItem.Raw());
    InvokeHook(ESharedCacheHookPoint::AfterKeepColdQueued, queuedItem);
    if (displaced != 0) {
        const TColdCacheItem displacedItem = TColdCacheItem::FromRaw(displaced);
        if (!cacheOp.DemoteKeepCold(displacedItem)) {
            // The displaced page stays KeepCold without a ring entry: demote it on its last release.
            cacheOp.WithdrawKeepColdEntry(displacedItem);
        }
    }
    if (!cacheOp.WantsKeepCold(handle)) {
        cacheOp.WithdrawKeepColdEntry(queuedItem);
    }
    pin.Get().Drop(spaceOp);
    cacheOp.Eviction().TrimKeepCold();
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
        if (cacheOp.DemoteKeepCold(item)) {
            return true;
        }
        // The FIFO entry is gone even when a structural ref delays demotion.
        cacheOp.WithdrawKeepColdEntry(item);
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
        if (!cacheOp.MatchesKeepColdEntry(item)) {
            continue;
        }
        InvokeHook(ESharedCacheHookPoint::AfterKeepColdEntryMatched, item);
        if (cacheOp.EraseCold(item)) {
            return true;
        }
        cacheOp.WithdrawKeepColdEntry(item);
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
template bool TSharedCacheImpl<TProdTraits>::WantsKeepCold(TSpaceOperation&, const THandle&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::DemoteKeepCold(TSpaceOperation&, TColdCacheItem, TPageCacheItem) noexcept;
template void TSharedCacheImpl<TProdTraits>::ReleaseColdDemotionPin(
    TSpaceOperation&, TOperationItemRef<TProdTraits>&) noexcept;
template void TSharedCacheImpl<TProdTraits>::WithdrawKeepColdEntry(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::MatchesKeepColdEntry(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::IsKeepColdQueued(const THandle&) noexcept;
template void TSharedCacheImpl<TProdTraits>::RouteKeepCold(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::ReclaimCold() noexcept;
template bool TSharedCacheImpl<TProdTraits>::ReclaimCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::DemoteKeepColdForSoftLimit(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::ReclaimKeepCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::Reclaim(TSpaceOperation&, ui64) noexcept;

template void TSharedCacheImpl<TTestTraits>::RouteCold(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::WantsKeepCold(TSpaceOperation&, const THandle&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::DemoteKeepCold(TSpaceOperation&, TColdCacheItem, TPageCacheItem) noexcept;
template void TSharedCacheImpl<TTestTraits>::ReleaseColdDemotionPin(
    TSpaceOperation&, TOperationItemRef<TTestTraits>&) noexcept;
template void TSharedCacheImpl<TTestTraits>::WithdrawKeepColdEntry(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::MatchesKeepColdEntry(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::IsKeepColdQueued(const THandle&) noexcept;
template void TSharedCacheImpl<TTestTraits>::RouteKeepCold(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::ReclaimCold() noexcept;
template bool TSharedCacheImpl<TTestTraits>::ReclaimCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::DemoteKeepColdForSoftLimit(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::ReclaimKeepCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::Reclaim(TSpaceOperation&, ui64) noexcept;

} // namespace NKikimr::NSharedCache
