#include "shared_cache_eviction.h"
#include "shared_cache.h"

#include <cmath>

namespace NKikimr::NSharedCache {

template <class TTraits>
ui64 TCacheEviction<TTraits>::FractionCeil(ui64 value, double fraction) noexcept {
    Y_DEBUG_ABORT_UNLESS(fraction >= 0.0 && fraction <= 1.0);
    return fraction == 1.0 ? value : static_cast<ui64>(std::ceil(static_cast<double>(value) * fraction));
}

template <class TTraits>
bool TCacheEviction<TTraits>::UpdateKeepColdLimit(ui64 limit) noexcept {
    if (limit > Cache_.Memory_.HardLimit()) {
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
bool TCacheEviction<TTraits>::SweepKeepCold() noexcept {
    ui64 remaining = KeepColdSweepRemaining_.load(std::memory_order_acquire);
    while (remaining != 0) {
        const ui64 batch = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
        if (KeepColdSweepRemaining_.compare_exchange_weak(
                remaining, remaining - batch, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            auto spaceOp = Cache_.Space_->BeginOperation();
            for (ui64 index = 0; index < batch; ++index) {
                const ui64 raw = spaceOp.KeepCold().Pop();
                if (raw != 0) {
                    spaceOp.View().Tiers[ECacheTier::Keep].Rings->RemoveColdEntry();
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
    if (Cache_.Memory_.KeepColdOwnedBytes() <= limit && !KeepColdTrimActive_.load(std::memory_order_relaxed))
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
    for (ui64 index = 0; index < attempts && Cache_.Memory_.KeepColdOwnedBytes() > lowWatermark; ++index)
    {
        const ui64 raw = spaceOp.KeepCold().Pop();
        if (raw == 0) {
            continue;
        }
        spaceOp.View().Tiers[ECacheTier::Keep].Rings->RemoveColdEntry();
        const TColdCacheItem coldItem = TColdCacheItem::FromRaw(raw);
        if (DemoteKeepCold(spaceOp, coldItem)) {
            progress = true;
        } else {
            const ui64 keepColdBytes = Cache_.Memory_.KeepColdOwnedBytes();
            WithdrawKeepColdEntry(spaceOp, coldItem);
            // A referenced page cannot be demoted yet, but its entry is gone now: advance the membership so
            // that the page leaves KeepCold on its last release. The withdrawal helper also demotes it if the
            // last release raced with the membership advance.
            progress = progress || Cache_.Memory_.KeepColdOwnedBytes() < keepColdBytes;
        }
    }
    if (Cache_.Memory_.KeepColdOwnedBytes() <= lowWatermark) {
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
        const ui64 occupied = Min(KeepColdLimit(), Cache_.Memory_.KeepColdOwnedBytes());
        const ui64 step = Max<ui64>(1, FractionCeil(occupied, Cache_.Policy_.ResizeStep));
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
        const ui64 maximum = Min(KeepColdMaxBytes_.load(std::memory_order_relaxed),
            budget - FractionCeil(budget, 1.0 - Cache_.Policy_.KeepColdMaxShare));
        if (limit >= maximum) {
            return false;
        }
        const ui64 step = Max<ui64>(1, FractionCeil(maximum, Cache_.Policy_.ResizeStep));
        const ui64 target = limit + Min(maximum - limit, step);
        if (KeepColdLimit_.compare_exchange_weak(limit, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::Reclaim(TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    if (ReclaimCold(spaceOp) || ReclaimKeepCold(spaceOp)) {
        return true;
    }
    // Cold is empty, so only moving the hot/cold boundary can make room, and that is EnforceCurrentLimit's job.
    return EnforceCurrentLimit(spaceOp, bytes);
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReclaimToLimits(
    TSpaceOperation& spaceOp, ui64 softLimit, ui32 step, ui64 bytes) noexcept {
    bool progress = false;
    for (ui32 attempt = 0; attempt < step; ++attempt) {
        if (ExceedsLimit(Cache_.Memory_.PayloadUsage(), Cache_.Memory_.CurrentLimit(), bytes)) {
            if (!ReclaimCold(spaceOp) && !ReclaimKeepCold(spaceOp)) {
                break;
            }
        } else if (softLimit == 0 && Cache_.Memory_.ColdReclaimableItems() != 0) {
            if (!ReclaimCold(spaceOp)) {
                break;
            }
        } else if (Cache_.Memory_.RetainedBytes() > softLimit) {
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
    ColdMinBytes_.store(FractionCeil(budget, Cache_.Policy_.ColdMin), std::memory_order_relaxed);
    ColdTargetBytes_.store(
        FractionCeil(budget, (Cache_.Policy_.ColdMin + Cache_.Policy_.Grow) / 2), std::memory_order_relaxed);
    ColdGrowBytes_.store(FractionCeil(budget, Cache_.Policy_.Grow), std::memory_order_relaxed);
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::ShrinkHotTarget(ui32 effectiveHotSlots, ui32 step) const noexcept {
    const ui32 minimum = MinimumHotSlots();
    if (effectiveHotSlots <= minimum) {
        return 0;
    }
    return effectiveHotSlots - minimum > step ? effectiveHotSlots - step : minimum;
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::GrowHotTarget(
    const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step) const noexcept {
    const ui32 maximum = static_cast<ui32>(spaceOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount);
    const ui32 growthStep = Max<ui32>(1, step / 2);
    const ui64 grownHotSlots = ui64(effectiveHotSlots) + growthStep;
    if (grownHotSlots < maximum) {
        return static_cast<ui32>(grownHotSlots);
    }
    return effectiveHotSlots < maximum ? maximum : 0;
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::HotResizeStep(ui32 effectiveHotSlots) const noexcept {
    return Max(
        Cache_.Policy_.MinResizeStep, static_cast<ui32>(std::ceil(effectiveHotSlots * Cache_.Policy_.ResizeStep)));
}

template <class TTraits>
bool TCacheEviction<TTraits>::DrainHotResize(TSpaceOperation& spaceOp) noexcept {
    bool progress = false;
    while (Cache_.HotResize_.Phase() == EHotResizePhase::Cut) {
        ui32 level;
        ui64 raw;
        if (!Cache_.Space_->DrainHotResize(Cache_.HotResize_, level, raw)) {
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
bool TCacheEviction<TTraits>::EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes, bool forceShrinkHot) noexcept {
    const ui64 softLimit = Cache_.Memory_.SoftLimit();
    const bool hadCurrentPressure = ExceedsLimit(Cache_.Memory_.PayloadUsage(), Cache_.Memory_.CurrentLimit(), bytes);
    const ui32 resizeStep = HotResizeStep(static_cast<ui32>(spaceOp.EffectiveHotSlots()));
    // Retained cache bytes drive soft pressure; total page commitment independently drives current pressure.
    const bool enforceSoftLimit =
        SoftLimitPressure_.exchange(false, std::memory_order_relaxed) || Cache_.Memory_.RetainedBytes() > softLimit;

    bool progress = false;
    if (hadCurrentPressure) {
        for (ui32 attempt = 0;
             attempt < resizeStep && ExceedsLimit(Cache_.Memory_.PayloadUsage(), Cache_.Memory_.CurrentLimit(), bytes);
             ++attempt) {
            if (!ReclaimCold(spaceOp)) {
                break;
            }
            progress = true;
        }
    }
    const bool enforceCurrentLimit = ExceedsLimit(Cache_.Memory_.PayloadUsage(), Cache_.Memory_.CurrentLimit(), bytes);
    if (auto hotResizeGuard = Cache_.HotResize_.TryLock()) {
        // Publication uses the same lock, so Hot maintenance can route newly added handles safely.
        auto hotSpaceOp = Cache_.Space_->BeginOperation();
        const ui32 effectiveHotSlots = static_cast<ui32>(hotSpaceOp.EffectiveHotSlots());
        const ui32 hotResizeStep = HotResizeStep(effectiveHotSlots);
        RefreshEvictableByteBudget();
        if (Cache_.HotResize_.Phase() == EHotResizePhase::Cut) {
            progress = DrainHotResize(hotSpaceOp) || progress;
        } else {
            const ui64 pageUsage = Cache_.Memory_.RetainedBytes();
            const ui64 physicalUsage = Cache_.Memory_.PayloadUsage();
            const ui64 currentLimit = Cache_.Memory_.CurrentLimit();
            const ui64 freeBytes = currentLimit - Min(currentLimit, physicalUsage);
            const ui64 coldBytes = Cache_.Memory_.ColdReclaimableBytes();
            // Forecast the room remaining after admitting the caller's payload.
            const ui64 freeAfterAdmission = freeBytes - Min(freeBytes, bytes);
            const ui64 coldFreeBytes = coldBytes + freeAfterAdmission;
            const ui64 coldFreeHandles = Cache_.Memory_.ColdReclaimableItems() + hotSpaceOp.View().Free().Count();
            const ui64 payloadBudget = EvictableByteBudget();

            const ui64 coldTargetBytes = ColdTargetBytes_.load(std::memory_order_relaxed);
            const ui64 evictableHandles = hotSpaceOp.AllocationLimit() - 2 -
                                          Min(hotSpaceOp.AllocationLimit() - 2, Cache_.Memory_.Sticky().Pages());
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
            const ui32 minimumHotSlots = MinimumHotSlots();
            if (effectiveHotSlots < minimumHotSlots) {
                targetHotSlots = minimumHotSlots;
            } else if (enforceCurrentLimit || forceShrinkHot || refillKeepCold ||
                       (pageUsage > softLimit && Cache_.Memory_.Hot().Bytes() > payloadBudget))
            {
                targetHotSlots = ShrinkHotTarget(effectiveHotSlots, hotResizeStep);
                if (targetHotSlots == 0) {
                    const bool needColdHandles =
                        coldFreeHandles < coldMinHandles ||
                        (KeepColdPressure_.load(std::memory_order_relaxed) && coldFreeHandles < coldTargetHandles);
                    if (needColdHandles && Cache_.Memory_.KeepColdOwnedBytes() != 0) {
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

            if (targetHotSlots != 0 && Cache_.Space_->BeginHotResize(targetHotSlots, Cache_.HotResize_)) {
                progress = true;
                if (Cache_.HotResize_.Phase() == EHotResizePhase::Cut) {
                    progress = DrainHotResize(hotSpaceOp) || progress;
                }
            }
        }
    }

    if (enforceCurrentLimit || enforceSoftLimit || (softLimit == 0 && Cache_.Memory_.ColdReclaimableItems() != 0)) {
        progress = ReclaimToLimits(spaceOp, softLimit, resizeStep, bytes) || progress;
        if (Cache_.Memory_.RetainedBytes() > softLimit) {
            SoftLimitPressure_.store(true, std::memory_order_relaxed);
        }
    }
    return progress;
}

// Moved methods are instantiated here for both trait variants
template ui64 TCacheEviction<TProdTraits>::FractionCeil(ui64, double) noexcept;
template bool TCacheEviction<TProdTraits>::UpdateKeepColdLimit(ui64) noexcept;
template bool TCacheEviction<TProdTraits>::SweepKeepCold() noexcept;
template bool TCacheEviction<TProdTraits>::TrimKeepCold(TSpaceOperation&) noexcept;
template bool TCacheEviction<TProdTraits>::ShrinkKeepCold(TSpaceOperation&) noexcept;
template bool TCacheEviction<TProdTraits>::GrowKeepCold() noexcept;
template bool TCacheEviction<TProdTraits>::Reclaim(TSpaceOperation&, ui64) noexcept;
template bool TCacheEviction<TProdTraits>::ReclaimToLimits(TSpaceOperation&, ui64, ui32, ui64) noexcept;
template void TCacheEviction<TProdTraits>::RefreshEvictableByteBudget() noexcept;
template ui32 TCacheEviction<TProdTraits>::ShrinkHotTarget(ui32, ui32) const noexcept;
template ui32 TCacheEviction<TProdTraits>::GrowHotTarget(const TSpaceOperation&, ui32, ui32) const noexcept;
template ui32 TCacheEviction<TProdTraits>::HotResizeStep(ui32) const noexcept;
template bool TCacheEviction<TProdTraits>::DrainHotResize(TSpaceOperation&) noexcept;
template bool TCacheEviction<TProdTraits>::EnforceCurrentLimit(TSpaceOperation&, ui64, bool) noexcept;

template ui64 TCacheEviction<TTestTraits>::FractionCeil(ui64, double) noexcept;
template bool TCacheEviction<TTestTraits>::UpdateKeepColdLimit(ui64) noexcept;
template bool TCacheEviction<TTestTraits>::SweepKeepCold() noexcept;
template bool TCacheEviction<TTestTraits>::TrimKeepCold(TSpaceOperation&) noexcept;
template bool TCacheEviction<TTestTraits>::ShrinkKeepCold(TSpaceOperation&) noexcept;
template bool TCacheEviction<TTestTraits>::GrowKeepCold() noexcept;
template bool TCacheEviction<TTestTraits>::Reclaim(TSpaceOperation&, ui64) noexcept;
template bool TCacheEviction<TTestTraits>::ReclaimToLimits(TSpaceOperation&, ui64, ui32, ui64) noexcept;
template void TCacheEviction<TTestTraits>::RefreshEvictableByteBudget() noexcept;
template ui32 TCacheEviction<TTestTraits>::ShrinkHotTarget(ui32, ui32) const noexcept;
template ui32 TCacheEviction<TTestTraits>::GrowHotTarget(const TSpaceOperation&, ui32, ui32) const noexcept;
template ui32 TCacheEviction<TTestTraits>::HotResizeStep(ui32) const noexcept;
template bool TCacheEviction<TTestTraits>::DrainHotResize(TSpaceOperation&) noexcept;
template bool TCacheEviction<TTestTraits>::EnforceCurrentLimit(TSpaceOperation&, ui64, bool) noexcept;

} // namespace NKikimr::NSharedCache
