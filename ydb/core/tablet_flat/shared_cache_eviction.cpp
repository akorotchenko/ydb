#include "shared_cache_eviction.h"
#include "shared_cache.h"

#include <cmath>

namespace NKikimr::NSharedCache {

template <class TTraits>
ui64 TCacheEvictionServices<TTraits>::PageBytes(const THandle& handle) noexcept {
    return TSharedCacheImpl<TTraits>::PageBytes(handle);
}

template <class TTraits>
ui32 TCacheEvictionServices<TTraits>::AdvanceColdMembership(THandle& handle) noexcept {
    return TSharedCacheImpl<TTraits>::AdvanceColdMembership(handle);
}

template <class TTraits>
bool TCacheEvictionServices<TTraits>::AdvanceColdMembership(
    THandle& handle, ui32 expectedVersion, bool keepColdQueued, ui32& newVersion) noexcept {
    return TSharedCacheImpl<TTraits>::AdvanceColdMembership(handle, expectedVersion, keepColdQueued, newVersion);
}

template <class TTraits>
TCollection* TCacheEvictionServices<TTraits>::ReleaseForColdDemotion(
    TSpaceOperation& spaceOp, TCacheItem item) noexcept {
    return Cache_.ReleaseItem(spaceOp, item, false);
}

template <class TTraits>
void TCacheEvictionServices<TTraits>::NotifyResourcesAvailable() noexcept {
    Cache_.NotifyResourcesAvailable();
}

template <class TTraits>
ui64 TCacheEviction<TTraits>::FractionCeil(ui64 value, double fraction) noexcept {
    Y_DEBUG_ABORT_UNLESS(fraction >= 0.0 && fraction <= 1.0);
    return fraction == 1.0 ? value : static_cast<ui64>(std::ceil(static_cast<double>(value) * fraction));
}

template <class TTraits>
bool TCacheEviction<TTraits>::UpdateKeepColdLimit(ui64 limit) noexcept {
    if (limit > Memory_.HardLimit()) {
        return false;
    }
    const ui64 previousMaximum = KeepColdMaxBytes_.exchange(limit, std::memory_order_relaxed);
    if (previousMaximum == limit) {
        return true;
    }
    ui64 current = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 growth = limit > previousMaximum ? limit - previousMaximum : 0;
        const ui64 target = Min(current, limit) + Min(growth, limit - Min(current, limit));
        if (KeepColdLimit_.compare_exchange_weak(current, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::ScheduleKeepColdSweep(ui64 slots) noexcept {
    KeepColdSweepRemaining_.store(slots, std::memory_order_release);
}

template <class TTraits>
bool TCacheEviction<TTraits>::SweepKeepCold(TSharedCacheSpace& space) noexcept {
    ui64 remaining = KeepColdSweepRemaining_.load(std::memory_order_acquire);
    while (remaining != 0) {
        const ui64 batch = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
        if (KeepColdSweepRemaining_.compare_exchange_weak(
                remaining, remaining - batch, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            auto spaceOp = space.BeginOperation();
            for (ui64 index = 0; index < batch; ++index) {
                const ui64 raw = spaceOp.KeepCold().Pop();
                if (raw != 0) {
                    RouteKeepCold(spaceOp, TColdCacheItem::FromRaw(raw));
                }
            }
            return true;
        }
    }
    return false;
}

template <class TTraits>
bool TCacheEviction<TTraits>::TrimKeepCold(TSpaceOperation& spaceOp) noexcept {
    const ui64 limit = KeepColdLimit();
    if (Memory_.KeepColdOwnedBytes() <= limit && !KeepColdTrimActive_.load(std::memory_order_relaxed))
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
    for (ui64 index = 0; index < attempts && Memory_.KeepColdOwnedBytes() > lowWatermark; ++index)
    {
        const ui64 raw = spaceOp.KeepCold().Pop();
        if (raw == 0) {
            continue;
        }
        const TColdCacheItem coldItem = TColdCacheItem::FromRaw(raw);
        if (DemoteKeepCold(spaceOp, coldItem)) {
            progress = true;
        } else {
            const ui64 keepColdBytes = Memory_.KeepColdOwnedBytes();
            WithdrawKeepColdEntry(spaceOp, coldItem);
            // A referenced page cannot be demoted yet, but its entry is gone now: advance the membership so
            // that the page leaves KeepCold on its last release. The withdrawal helper also demotes it if the
            // last release raced with the membership advance.
            progress = progress || Memory_.KeepColdOwnedBytes() < keepColdBytes;
        }
    }
    if (Memory_.KeepColdOwnedBytes() <= lowWatermark) {
        KeepColdTrimActive_.store(false, std::memory_order_relaxed);
        KeepColdTrimScanRemaining_.store(0, std::memory_order_relaxed);
    } else if (progress) {
        KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_relaxed);
    }
    return progress || remaining > attempts;
}

template <class TTraits>
bool TCacheEviction<TTraits>::ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept {
    // Finish the previous FIFO trim before reducing the target again: sparse slots or held pages may delay it.
    if (KeepColdTrimActive_.load(std::memory_order_relaxed)) {
        return TrimKeepCold(spaceOp);
    }
    ui64 limit = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 occupied = Min(KeepColdLimit(), Memory_.KeepColdOwnedBytes());
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

template <class TTraits>
bool TCacheEviction<TTraits>::GrowKeepCold() noexcept {
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

template <class TTraits>
bool TCacheEviction<TTraits>::WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    if (TCollection* collection = Collections_.BindOperation(spaceOp).PageCollection(page)) {
        return collection->GetCacheMode() == ECacheMode::TryKeepInMemory &&
               collection->IsKeepAllowedPage(page.Metadata.Page.Type);
    }
    return false;
}

template <class TTraits>
bool TCacheEviction<TTraits>::DemoteKeepCold(
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
            TOperationItemRef<TTraits> pin(spaceOp, TCacheItem::Make(expected.Version(), coldItem.Index()));
            Traits_.Invoke(ESharedCacheHookPoint::AfterKeepColdDemoted, TCacheItem::FromRaw(coldItem.Raw()));
            const ui64 bytes = Services_.PageBytes(handle);
            Services_.AdvanceColdMembership(handle);
            Memory_.ReleaseRetainedBytes(bytes);
            Memory_.KeepCold().TransferBytesTo(Memory_.Cold(), bytes);
            Memory_.KeepCold().TransferPageTo(Memory_.Cold(), EItemKind::Page);
            Memory_.AddColdItem();
            Memory_.ReleaseKeepColdOwnedBytes(bytes);
            if (TCollection* owner = Collections_.BindOperation(spaceOp).PageCollection(handle)) {
                Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
            ReleaseColdDemotionPin(spaceOp, pin);
            return true;
        }
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::ReleaseColdDemotionPin(
    TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& pin) noexcept {
    const TCacheItem cacheItem = pin.CacheItem();
    pin.Disarm();
    // A policy demotion must not readmit the page to KeepCold when releasing its temporary ref.
    if (TCollection* owner = Services_.ReleaseForColdDemotion(spaceOp, cacheItem)) {
        Collections_.BindOperation(spaceOp).DropOwnerReference(*owner);
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    if (DemoteKeepCold(spaceOp, coldItem)) {
        return;
    }
    if (!MatchesKeepColdEntry(spaceOp, coldItem)) {
        return;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    ui32 coldVersion;
    if (!Services_.AdvanceColdMembership(handle, coldItem.ColdVersion(), false, coldVersion)) {
        return;
    }
    DemoteKeepCold(spaceOp, TColdCacheItem::Make(coldVersion, coldItem.Index()));
}

template <class TTraits>
bool TCacheEviction<TTraits>::MatchesKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
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

template <class TTraits>
bool TCacheEviction<TTraits>::IsKeepColdQueued(const THandle& handle) noexcept {
    return (handle.ColdVersion.load(std::memory_order_acquire) & KeepColdQueuedMask) != 0;
}

template <class TTraits>
void TCacheEviction<TTraits>::RouteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
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
    TOperationItemRef<TTraits> pin(
        spaceOp, TCacheItem::Make(THandleState::FromRaw(expectedRaw).Version(), coldItem.Index()));
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    if (!WantsKeepCold(spaceOp, handle)) {
        WithdrawKeepColdEntry(spaceOp, coldItem);
        return;
    }
    ui32 queuedVersion;
    if (!Services_.AdvanceColdMembership(handle, coldItem.ColdVersion(), true, queuedVersion)) {
        return;
    }
    const TColdCacheItem queuedItem = TColdCacheItem::Make(queuedVersion, coldItem.Index());
    const ui64 displaced = spaceOp.KeepCold().PutAndPop(queuedItem.Raw());
    Traits_.Invoke(ESharedCacheHookPoint::AfterKeepColdQueued, TCacheItem::FromRaw(queuedItem.Raw()));
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
    pin.Drop();
    TrimKeepCold(spaceOp);
}

template <class TTraits>
void TCacheEviction<TTraits>::ReleaseKeepColdPin(
    TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& pin, ui32 initialColdVersion) noexcept {
    const TCacheItem cacheItem = pin.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsKeepCold() || expected.Refs() != 1 ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != initialColdVersion ||
            IsKeepColdQueued(handle) || !WantsKeepCold(spaceOp, handle))
        {
            pin.Drop();
            return;
        }
        const THandleState desired = expected.DecrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            pin.Disarm();
            RouteKeepCold(spaceOp, TColdCacheItem::Make(initialColdVersion, cacheItem.Index()));
            Services_.NotifyResourcesAvailable();
            return;
        }
    }
}

template class TCacheEvictionServices<TProdTraits>;
template class TCacheEvictionServices<TTestTraits>;
template class TCacheEviction<TProdTraits>;
template class TCacheEviction<TTestTraits>;

} // namespace NKikimr::NSharedCache
