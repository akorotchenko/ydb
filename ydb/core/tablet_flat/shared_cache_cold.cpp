#include "shared_cache.h"

namespace NKikimr::NSharedCache {

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
void TSharedCache::RouteCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
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

    ColdRingEntries_.fetch_add(1, std::memory_order_relaxed);
    InvokeHook(ESharedCacheHookPoint::BeforeColdExchange, coldItem);
    const TColdCacheItem displaced = spaceOp.PutCold(coldItem);
    if (!displaced.IsNull()) {
        const ui64 previous = ColdRingEntries_.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
        EraseCold(spaceOp, displaced);
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    if (TCollection* collection = PageCollection(spaceOp, page)) {
        return collection->CacheMode_.load(std::memory_order_acquire) == ECacheMode::TryKeepInMemory &&
               collection->IsKeepAllowedPage(page.Metadata.Page.Type);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DemoteKeepCold(
    TSpaceOperation& spaceOp, TColdCacheItem coldItem, TPageCacheItem expectedPage) noexcept {
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
            SubtractExactBytes(RetainedBytes_, bytes);
            TransferEstimatedBytes(KeepColdBytes_, ColdBytes_, bytes);
            TransferEstimatedPages(KeepColdPages_, ColdPages_, EItemKind::Page);
            ColdItems_.fetch_add(1, std::memory_order_relaxed);
            SubtractExactBytes(KeepColdOwnedBytes_, bytes);
            if (TCollection* owner = PageCollection(spaceOp, handle)) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
            ReleaseColdDemotionPin(spaceOp, pin);
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseColdDemotionPin(TSpaceOperation& spaceOp, TOperationItemRef& pin) noexcept {
    const TCacheItem cacheItem = pin.CacheItem();
    pin.Disarm();
    // A policy demotion must not readmit the page to KeepCold when releasing its temporary ref.
    if (TCollection* owner = ReleaseItem(spaceOp, cacheItem, false)) {
        DropOwnerReference(spaceOp, *owner);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    if (DemoteKeepCold(spaceOp, coldItem)) {
        return;
    }
    if (!MatchesKeepColdEntry(spaceOp, coldItem)) {
        return;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    ui32 coldVersion;
    if (!AdvanceColdMembership(handle, coldItem.ColdVersion(), false, coldVersion)) {
        return;
    }
    DemoteKeepCold(spaceOp, TColdCacheItem::Make(coldVersion, coldItem.Index()));
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
            WithdrawKeepColdEntry(spaceOp, coldItem);
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
    if (!WantsKeepCold(spaceOp, handle)) {
        WithdrawKeepColdEntry(spaceOp, coldItem);
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
        if (!DemoteKeepCold(spaceOp, displacedItem)) {
            // The displaced page stays KeepCold without a ring entry: demote it on its last release.
            WithdrawKeepColdEntry(spaceOp, displacedItem);
        }
    }
    if (!WantsKeepCold(spaceOp, handle)) {
        WithdrawKeepColdEntry(spaceOp, queuedItem);
    }
    pin.Get().Drop(spaceOp);
    TrimKeepCold(spaceOp);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AddKeepColdOwnedBytes(ui64 bytes) noexcept {
    KeepColdOwnedBytes_.fetch_add(bytes, std::memory_order_relaxed);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TrimKeepCold(TSpaceOperation& spaceOp) noexcept {
    const ui64 limit = KeepColdLimit();
    if (KeepColdOwnedBytes_.load(std::memory_order_relaxed) <= limit &&
        !KeepColdTrimActive_.load(std::memory_order_relaxed))
    {
        return false;
    }
    if (!KeepColdTrimActive_.exchange(true, std::memory_order_acq_rel)) {
        KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
    }
    const ui64 lowWatermark = limit - (limit / 20 + (limit % 20 != 0));
    ui64 remaining = KeepColdTrimScanRemaining_.load(std::memory_order_acquire);
    ui64 attempts;
    for (;;) {
        if (remaining == 0) {
            KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
            remaining = spaceOp.KeepCold().Capacity;
        }
        attempts = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
        if (KeepColdTrimScanRemaining_.compare_exchange_weak(
                remaining, remaining - attempts, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            break;
        }
    }
    bool progress = false;
    for (ui64 index = 0; index < attempts && KeepColdOwnedBytes_.load(std::memory_order_relaxed) > lowWatermark;
         ++index)
    {
        const ui64 raw = spaceOp.KeepCold().Pop();
        if (raw == 0) {
            continue;
        }
        const TColdCacheItem coldItem = TColdCacheItem::FromRaw(raw);
        if (DemoteKeepCold(spaceOp, coldItem)) {
            progress = true;
        } else {
            const ui64 keepColdBytes = KeepColdOwnedBytes_.load(std::memory_order_relaxed);
            WithdrawKeepColdEntry(spaceOp, coldItem);
            // A referenced page cannot be demoted yet, but its entry is gone now: advance the membership so
            // that the page leaves KeepCold on its last release. The withdrawal helper also demotes it if the
            // last release raced with the membership advance.
            progress = progress || KeepColdOwnedBytes_.load(std::memory_order_relaxed) < keepColdBytes;
        }
    }
    if (KeepColdOwnedBytes_.load(std::memory_order_relaxed) <= lowWatermark) {
        KeepColdTrimActive_.store(false, std::memory_order_relaxed);
        KeepColdTrimScanRemaining_.store(0, std::memory_order_relaxed);
    } else if (progress) {
        KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_relaxed);
    }
    return progress || remaining > attempts;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept {
    // Finish the previous FIFO trim before reducing the target again: sparse slots or held pages may delay it.
    if (KeepColdTrimActive_.load(std::memory_order_relaxed)) {
        return TrimKeepCold(spaceOp);
    }
    ui64 limit = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 occupied = Min(KeepColdLimit(), KeepColdOwnedBytes());
        const ui64 step = Max<ui64>(1, FractionCeil(occupied, Policy_.ResizeStep));
        const ui64 target = occupied - Min(occupied, step);
        if (limit == target) {
            return TrimKeepCold(spaceOp);
        }
        if (KeepColdLimit_.compare_exchange_weak(limit, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            TrimKeepCold(spaceOp);
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::GrowKeepCold() noexcept {
    ui64 limit = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 budget = EvictableByteBudget();
        const ui64 maximum =
            Min(KeepColdMaxBytes_.load(std::memory_order_relaxed), budget - FractionCeil(budget, Policy_.HotMin));
        if (limit >= maximum) {
            return false;
        }
        const ui64 step = Max<ui64>(1, FractionCeil(maximum, Policy_.ResizeStep));
        const ui64 target = limit + Min(maximum - limit, step);
        if (KeepColdLimit_.compare_exchange_weak(limit, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

// Cold and KeepCold members are instantiated here; other cache members are instantiated in shared_cache.cpp.
template void TSharedCacheImpl<TProdTraits>::RouteCold(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::WantsKeepCold(TSpaceOperation&, const THandle&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::DemoteKeepCold(TSpaceOperation&, TColdCacheItem, TPageCacheItem) noexcept;
template void TSharedCacheImpl<TProdTraits>::ReleaseColdDemotionPin(
    TSpaceOperation&, TOperationItemRef<TProdTraits>&) noexcept;
template void TSharedCacheImpl<TProdTraits>::WithdrawKeepColdEntry(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::MatchesKeepColdEntry(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::IsKeepColdQueued(const THandle&) noexcept;
template void TSharedCacheImpl<TProdTraits>::RouteKeepCold(TSpaceOperation&, TColdCacheItem) noexcept;
template void TSharedCacheImpl<TProdTraits>::AddKeepColdOwnedBytes(ui64) noexcept;
template bool TSharedCacheImpl<TProdTraits>::TrimKeepCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::ShrinkKeepCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::GrowKeepCold() noexcept;

template void TSharedCacheImpl<TTestTraits>::RouteCold(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::WantsKeepCold(TSpaceOperation&, const THandle&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::DemoteKeepCold(TSpaceOperation&, TColdCacheItem, TPageCacheItem) noexcept;
template void TSharedCacheImpl<TTestTraits>::ReleaseColdDemotionPin(
    TSpaceOperation&, TOperationItemRef<TTestTraits>&) noexcept;
template void TSharedCacheImpl<TTestTraits>::WithdrawKeepColdEntry(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::MatchesKeepColdEntry(TSpaceOperation&, TColdCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::IsKeepColdQueued(const THandle&) noexcept;
template void TSharedCacheImpl<TTestTraits>::RouteKeepCold(TSpaceOperation&, TColdCacheItem) noexcept;
template void TSharedCacheImpl<TTestTraits>::AddKeepColdOwnedBytes(ui64) noexcept;
template bool TSharedCacheImpl<TTestTraits>::TrimKeepCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::ShrinkKeepCold(TSpaceOperation&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::GrowKeepCold() noexcept;

} // namespace NKikimr::NSharedCache
