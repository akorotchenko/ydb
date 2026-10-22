#include "shared_cache_eviction.h"

#include <cmath>

namespace NKikimr::NSharedCache {

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
bool TCacheEviction<TTraits>::ExceedsLimit(ui64 usage, ui64 limit, ui64 bytes) noexcept {
    return usage + bytes > limit;
}

template <class TTraits>
bool TCacheEviction<TTraits>::Reclaim(
    TSpaceOperation& spaceOp, TSharedCacheSpace& space, THotResize& hotResize, ui64 bytes) noexcept {
    if (ReclaimCold(spaceOp) || ReclaimKeepCold(spaceOp)) {
        return true;
    }
    // Cold is empty, so only moving the hot/cold boundary can make room, and that is EnforceCurrentLimit's job.
    return EnforceCurrentLimit(spaceOp, space, hotResize, bytes);
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReclaimToLimits(
    TSpaceOperation& spaceOp, ui64 softLimit, ui32 step, ui64 bytes) noexcept {
    bool progress = false;
    for (ui32 attempt = 0; attempt < step; ++attempt) {
        if (ExceedsLimit(Memory_.PayloadUsage(), Memory_.CurrentLimit(), bytes)) {
            if (!ReclaimCold(spaceOp) && !ReclaimKeepCold(spaceOp)) {
                break;
            }
        } else if (softLimit == 0 && Memory_.ColdReclaimableItems() != 0) {
            if (!ReclaimCold(spaceOp)) {
                break;
            }
        } else if (Memory_.RetainedBytes() > softLimit) {
            if (!DemoteKeepColdForSoftLimit(spaceOp)) {
                break;
            }
        } else {
            break;
        }
        progress = true;
    }
    return progress;
}

template <class TTraits>
void TCacheEviction<TTraits>::RefreshEvictableByteBudget() noexcept {
    const ui64 budget = EvictableByteBudget();
    if (ColdByteBudget_ == budget) {
        return;
    }
    ColdByteBudget_ = budget;
    ColdMinBytes_.store(FractionCeil(budget, Policy_.ColdMin), std::memory_order_relaxed);
    ColdTargetBytes_.store(FractionCeil(budget, (Policy_.ColdMin + Policy_.Grow) / 2), std::memory_order_relaxed);
    ColdGrowBytes_.store(FractionCeil(budget, Policy_.Grow), std::memory_order_relaxed);
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::MinimumHotSlots(
    const TSharedCacheSpace& space, const TSpaceOperation& spaceOp, bool underPressure) const noexcept {
    const ui64 hotPages = Memory_.Hot().Pages();
    const ui64 hotBytes = Memory_.Hot().Bytes();
    const ui64 meanPageBytes = hotPages ? Max<ui64>(1, hotBytes / hotPages)
                                        : Services_.AccountedPageBytes(space.ReservedConfiguration().ExpectedPageSize);
    const ui64 hotMinBytes =
        FractionCeil(EvictableByteBudget(), underPressure ? Policy_.HotMinUnderPressure : Policy_.HotMin);
    const ui64 byteMinimumSlots = hotMinBytes / meanPageBytes + (hotMinBytes % meanPageBytes != 0);
    return static_cast<ui32>(
        Min<ui64>(spaceOp.View().HotSlotCount, Max<ui64>(Policy_.MinHotSlotsUnderPressure, byteMinimumSlots)));
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::ShrinkHotTarget(const TSharedCacheSpace& space, const TSpaceOperation& spaceOp,
    ui32 effectiveHotSlots, ui32 step, bool underPressure) const noexcept {
    const ui32 minimum = MinimumHotSlots(space, spaceOp, underPressure);
    if (effectiveHotSlots <= minimum) {
        return 0;
    }
    return effectiveHotSlots - minimum > step ? effectiveHotSlots - step : minimum;
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::GrowHotTarget(
    const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step) const noexcept {
    const ui32 maximum = static_cast<ui32>(spaceOp.View().HotSlotCount);
    const ui32 growthStep = Max<ui32>(1, step / 2);
    const ui64 grownHotSlots = ui64(effectiveHotSlots) + growthStep;
    if (grownHotSlots < maximum) {
        return static_cast<ui32>(grownHotSlots);
    }
    return effectiveHotSlots < maximum ? maximum : 0;
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::HotResizeStep(ui32 effectiveHotSlots) const noexcept {
    return Max(Policy_.MinResizeStep, static_cast<ui32>(std::ceil(effectiveHotSlots * Policy_.ResizeStep)));
}

template <class TTraits>
bool TCacheEviction<TTraits>::DrainHotResize(
    TSpaceOperation& spaceOp, TSharedCacheSpace& space, THotResize& hotResize) noexcept {
    bool progress = false;
    while (hotResize.Phase() == EHotResizePhase::Cut) {
        ui32 level;
        ui64 raw;
        if (!space.DrainHotResize(hotResize, level, raw)) {
            break;
        }
        progress = true;
        if (raw != 0) {
            ProcessHot(spaceOp, static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
        }
    }
    return progress;
}

template <class TTraits>
bool TCacheEviction<TTraits>::EnforceCurrentLimit(TSpaceOperation& spaceOp, TSharedCacheSpace& space,
    THotResize& hotResize, ui64 bytes, bool forceShrinkHot) noexcept {
    const ui64 softLimit = Memory_.SoftLimit();
    const bool hadCurrentPressure = ExceedsLimit(Memory_.PayloadUsage(), Memory_.CurrentLimit(), bytes);
    const ui32 resizeStep = HotResizeStep(static_cast<ui32>(spaceOp.EffectiveHotSlots()));
    // Retained cache bytes drive soft pressure; total page commitment independently drives current pressure.
    const bool enforceSoftLimit =
        SoftLimitPressure_.exchange(false, std::memory_order_relaxed) || Memory_.RetainedBytes() > softLimit;

    bool progress = false;
    if (hadCurrentPressure) {
        for (ui32 attempt = 0;
             attempt < resizeStep && ExceedsLimit(Memory_.PayloadUsage(), Memory_.CurrentLimit(), bytes); ++attempt) {
            if (!ReclaimCold(spaceOp)) {
                break;
            }
            progress = true;
        }
    }
    const bool enforceCurrentLimit = ExceedsLimit(Memory_.PayloadUsage(), Memory_.CurrentLimit(), bytes);
    if (auto hotResizeGuard = hotResize.TryLock()) {
        // Publication uses the same lock, so Hot maintenance can route newly added handles safely.
        auto hotSpaceOp = space.BeginOperation();
        const ui32 effectiveHotSlots = static_cast<ui32>(hotSpaceOp.EffectiveHotSlots());
        const ui32 hotResizeStep = HotResizeStep(effectiveHotSlots);
        RefreshEvictableByteBudget();
        if (hotResize.Phase() == EHotResizePhase::Cut) {
            progress = DrainHotResize(hotSpaceOp, space, hotResize) || progress;
        } else {
            const ui64 pageUsage = Memory_.RetainedBytes();
            const ui64 physicalUsage = Memory_.PayloadUsage();
            const ui64 currentLimit = Memory_.CurrentLimit();
            const ui64 freeBytes = currentLimit - Min(currentLimit, physicalUsage);
            const ui64 coldBytes = Memory_.ColdReclaimableBytes();
            // Forecast the room remaining after admitting the caller's payload.
            const ui64 freeAfterAdmission = freeBytes - Min(freeBytes, bytes);
            const ui64 coldFreeBytes = coldBytes + freeAfterAdmission;
            const ui64 coldFreeHandles = Memory_.ColdReclaimableItems() + hotSpaceOp.View().Free().Count();
            const ui64 payloadBudget = EvictableByteBudget();

            const ui64 coldTargetBytes = ColdTargetBytes_.load(std::memory_order_relaxed);
            const ui64 evictableHandles =
                hotSpaceOp.AllocationLimit() - 2 - Min(hotSpaceOp.AllocationLimit() - 2, Memory_.Sticky().Pages());
            const ui64 coldMinHandles = (evictableHandles + 4) / 5;
            const ui64 coldGrowHandles = (evictableHandles * 3 + 9) / 10;
            const ui64 coldTargetHandles = (coldMinHandles + coldGrowHandles) / 2;
            const bool belowColdMinimum =
                coldFreeBytes < ColdMinBytes_.load(std::memory_order_relaxed) || coldFreeHandles < coldMinHandles;
            if (coldFreeBytes >= coldTargetBytes && coldFreeHandles >= coldTargetHandles) {
                KeepColdPressure_.store(false, std::memory_order_relaxed);
            }
            const bool refillKeepCold =
                belowColdMinimum || (KeepColdPressure_.load(std::memory_order_relaxed) &&
                                        (coldFreeBytes < coldTargetBytes || coldFreeHandles < coldTargetHandles));
            ui32 targetHotSlots = 0;
            // The caller's own requirement counts as already resident, so it lowers the cold plus free room.
            // Referenced Cold and in-flight loads cannot be reclaimed yet. Under current-limit
            // pressure, let Hot shrink further to compensate for their physical commitment.
            const bool hotUnderPressure =
                enforceCurrentLimit && (Memory_.ReservedBytes() != 0 || Memory_.Cold().Bytes() > coldBytes);
            const ui32 minimumHotSlots = MinimumHotSlots(space, hotSpaceOp, hotUnderPressure);
            if (effectiveHotSlots < minimumHotSlots) {
                targetHotSlots = minimumHotSlots;
            } else if (enforceCurrentLimit || forceShrinkHot || refillKeepCold ||
                       (pageUsage > softLimit && Memory_.Hot().Bytes() > payloadBudget))
            {
                targetHotSlots = ShrinkHotTarget(space, hotSpaceOp, effectiveHotSlots, hotResizeStep, hotUnderPressure);
                if (targetHotSlots == 0) {
                    const bool needColdHandles =
                        coldFreeHandles < coldMinHandles ||
                        (KeepColdPressure_.load(std::memory_order_relaxed) && coldFreeHandles < coldTargetHandles);
                    if (needColdHandles && Memory_.KeepColdOwnedBytes() != 0) {
                        KeepColdPressure_.store(true, std::memory_order_relaxed);
                        progress = ShrinkKeepCold(hotSpaceOp) || progress;
                    }
                    if (enforceCurrentLimit || forceShrinkHot || pageUsage > softLimit) {
                        progress = ProcessHotAtMinimum(hotSpaceOp) || progress;
                    }
                }
            } else if (!hadCurrentPressure && pageUsage <= softLimit &&
                       coldFreeBytes > ColdGrowBytes_.load(std::memory_order_relaxed) &&
                       coldFreeHandles > coldGrowHandles)
            {
                targetHotSlots = GrowHotTarget(hotSpaceOp, effectiveHotSlots, hotResizeStep);
                progress = GrowKeepCold() || progress;
            }

            if (targetHotSlots != 0 && space.BeginHotResize(targetHotSlots, hotResize)) {
                if (hotResize.Phase() == EHotResizePhase::Cut) {
                    progress = DrainHotResize(hotSpaceOp, space, hotResize) || progress;
                }
            }
        }
    }

    if (enforceCurrentLimit || enforceSoftLimit || (softLimit == 0 && Memory_.ColdReclaimableItems() != 0)) {
        progress = ReclaimToLimits(spaceOp, softLimit, resizeStep, bytes) || progress;
        if (Memory_.RetainedBytes() > softLimit) {
            SoftLimitPressure_.store(true, std::memory_order_relaxed);
        }
    }
    return progress;
}

// Moved methods are instantiated here for both trait variants
template ui64 TCacheEviction<TProdTraits>::FractionCeil(ui64, double) noexcept;
template bool TCacheEviction<TProdTraits>::UpdateKeepColdLimit(ui64) noexcept;
template void TCacheEviction<TProdTraits>::ScheduleKeepColdSweep(ui64) noexcept;
template bool TCacheEviction<TProdTraits>::SweepKeepCold(TSharedCacheSpace&) noexcept;
template bool TCacheEviction<TProdTraits>::TrimKeepCold(TSpaceOperation&) noexcept;
template bool TCacheEviction<TProdTraits>::ShrinkKeepCold(TSpaceOperation&) noexcept;
template bool TCacheEviction<TProdTraits>::GrowKeepCold() noexcept;
template bool TCacheEviction<TProdTraits>::ExceedsLimit(ui64, ui64, ui64) noexcept;
template bool TCacheEviction<TProdTraits>::Reclaim(TSpaceOperation&, TSharedCacheSpace&, THotResize&, ui64) noexcept;
template bool TCacheEviction<TProdTraits>::ReclaimToLimits(TSpaceOperation&, ui64, ui32, ui64) noexcept;
template void TCacheEviction<TProdTraits>::RefreshEvictableByteBudget() noexcept;
template ui32 TCacheEviction<TProdTraits>::MinimumHotSlots(
    const TSharedCacheSpace&, const TSpaceOperation&, bool) const noexcept;
template ui32 TCacheEviction<TProdTraits>::ShrinkHotTarget(
    const TSharedCacheSpace&, const TSpaceOperation&, ui32, ui32, bool) const noexcept;
template ui32 TCacheEviction<TProdTraits>::GrowHotTarget(const TSpaceOperation&, ui32, ui32) const noexcept;
template ui32 TCacheEviction<TProdTraits>::HotResizeStep(ui32) const noexcept;
template bool TCacheEviction<TProdTraits>::DrainHotResize(TSpaceOperation&, TSharedCacheSpace&, THotResize&) noexcept;
template bool TCacheEviction<TProdTraits>::EnforceCurrentLimit(
    TSpaceOperation&, TSharedCacheSpace&, THotResize&, ui64, bool) noexcept;

template ui64 TCacheEviction<TTestTraits>::FractionCeil(ui64, double) noexcept;
template bool TCacheEviction<TTestTraits>::UpdateKeepColdLimit(ui64) noexcept;
template void TCacheEviction<TTestTraits>::ScheduleKeepColdSweep(ui64) noexcept;
template bool TCacheEviction<TTestTraits>::SweepKeepCold(TSharedCacheSpace&) noexcept;
template bool TCacheEviction<TTestTraits>::TrimKeepCold(TSpaceOperation&) noexcept;
template bool TCacheEviction<TTestTraits>::ShrinkKeepCold(TSpaceOperation&) noexcept;
template bool TCacheEviction<TTestTraits>::GrowKeepCold() noexcept;
template bool TCacheEviction<TTestTraits>::ExceedsLimit(ui64, ui64, ui64) noexcept;
template bool TCacheEviction<TTestTraits>::Reclaim(TSpaceOperation&, TSharedCacheSpace&, THotResize&, ui64) noexcept;
template bool TCacheEviction<TTestTraits>::ReclaimToLimits(TSpaceOperation&, ui64, ui32, ui64) noexcept;
template void TCacheEviction<TTestTraits>::RefreshEvictableByteBudget() noexcept;
template ui32 TCacheEviction<TTestTraits>::MinimumHotSlots(
    const TSharedCacheSpace&, const TSpaceOperation&, bool) const noexcept;
template ui32 TCacheEviction<TTestTraits>::ShrinkHotTarget(
    const TSharedCacheSpace&, const TSpaceOperation&, ui32, ui32, bool) const noexcept;
template ui32 TCacheEviction<TTestTraits>::GrowHotTarget(const TSpaceOperation&, ui32, ui32) const noexcept;
template ui32 TCacheEviction<TTestTraits>::HotResizeStep(ui32) const noexcept;
template bool TCacheEviction<TTestTraits>::DrainHotResize(TSpaceOperation&, TSharedCacheSpace&, THotResize&) noexcept;
template bool TCacheEviction<TTestTraits>::EnforceCurrentLimit(
    TSpaceOperation&, TSharedCacheSpace&, THotResize&, ui64, bool) noexcept;

} // namespace NKikimr::NSharedCache
