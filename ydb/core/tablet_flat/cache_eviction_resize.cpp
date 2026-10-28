#include "cache_eviction.h"
#include "cache.h"

#include <cmath>

namespace NKikimr::NSharedCache {

template <class TTraits>
ui64 TCacheEviction<TTraits>::FractionCeil(ui64 value, double fraction) noexcept {
    return fraction == 1.0 ? value : static_cast<ui64>(std::ceil(static_cast<double>(value) * fraction));
}

template <class TTraits>
ui64 TCacheEviction<TTraits>::KeepLimit() const noexcept {
    const ui64 budget = EvictableByteBudget();
    const ui64 maximum = budget - FractionCeil(budget, 1.0 - Cache_.Policy_.KeepMaxShare);
    return Min(KeepRequestedBytes_.load(std::memory_order_relaxed), maximum);
}

template <class TTraits>
bool TCacheEviction<TTraits>::UpdateKeepLimit(ui64 limit) noexcept {
    if (limit > Cache_.Memory_.HardLimit()) {
        return false;
    }
    if (KeepRequestedBytes_.exchange(limit, std::memory_order_relaxed) != limit) {
        RefreshKeepLimits();
        Cache_.Capacity_.RequestTierRebalance();
    }
    return true;
}

template <class TTraits>
void TCacheEviction<TTraits>::RefreshKeepLimits() noexcept {
    const auto& keep = Cache_.Space_->CurrentConfiguration().Tiers[ECacheTier::Keep];
    const ui64 bytes = KeepLimit();
    const ui64 pages = keep.HotSlots + keep.ColdSlots;
    const ui64 observed = ObservedKeepLimit_.exchange(bytes, std::memory_order_acq_rel);
    const ui64 oldPages = Cache_.Memory_.KeepLimitPages();
    const bool changed = observed != bytes || oldPages != pages;
    Cache_.Memory_.SetKeepPageLimit(pages);
    if (!changed || (oldPages == 0 && observed == bytes)) {
        return;
    }
    Cache_.Capacity_.RequestTierRebalance();
}

template <class TTraits>
void TCacheEviction<TTraits>::ScheduleModeSweep(ui64 slots) noexcept {
    // Collection mode, Keep detachment, or V1 index shadow eligibility changed
    ModeGeneration_.fetch_add(1, std::memory_order_release);
    ModeSweepRestart_.exchange(true, std::memory_order_acq_rel);
    ui64 remaining = ModeSweepRemaining_.load(std::memory_order_acquire);
    while (!ModeSweepRemaining_.compare_exchange_weak(
        remaining, Max(remaining, slots), std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::RestartModeSweep(TSpaceOperation& spaceOp) noexcept {
    ui64 slots = 0;
    for (const auto& tier : spaceOp.View().Tiers) {
        slots += tier.HotStorage.SlotCount + tier.ColdStorage.SlotCount;
    }
    // An old-view publisher may request less than a complete current-view walk
    // Chain coalesced publishers' writes through the restart flag
    ModeSweepRestart_.exchange(true, std::memory_order_acq_rel);
    // Publish the restart even when pending work already covers this span
    ui64 remaining = ModeSweepRemaining_.load(std::memory_order_acquire);
    while (!ModeSweepRemaining_.compare_exchange_weak(
        remaining, Max(remaining, slots), std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::CheckModeGeneration(TSpaceOperation& spaceOp, ui64 generation) noexcept {
    if (ModeGeneration_.load(std::memory_order_acquire) != generation) {
        RestartModeSweep(spaceOp);
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::SweepModes() noexcept {
    ui64 remaining = ModeSweepRemaining_.load(std::memory_order_acquire);
    ui64 work;
    do {
        if (remaining == 0) {
            return false;
        }
        work = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
    } while (!ModeSweepRemaining_.compare_exchange_weak(
        remaining, remaining - work, std::memory_order_acq_rel, std::memory_order_acquire));
    const ui64 generation = ModeGeneration_.load(std::memory_order_acquire);
    auto spaceOp = Cache_.Space_->BeginOperation();
    const auto loadHotSlots = [&] {
        std::array<ui64, CacheTierCount> slots{};
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            slots[tier] = spaceOp.View().Tiers[tier].EffectiveHotSlotCount();
        }
        return slots;
    };
    const auto hotSlots = loadHotSlots();
    const bool restart = ModeSweepRestart_.exchange(false, std::memory_order_acq_rel);
    if (restart || ModeSweepGeneration_ != generation || ModeSweepViewGeneration_ != spaceOp.View().Generation ||
        ModeSweepHotSlots_ != hotSlots) {
        // A claimed final batch is still a pending walk when its view changes
        ui64 span = 0;
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            span += hotSlots[tier] + spaceOp.View().Tiers[tier].ColdStorage.SlotCount;
        }
        work = Min<ui64>(span, SharedCacheTransitionWorkBatch);
        const ui64 tail = span - work;
        ui64 pending = ModeSweepRemaining_.load(std::memory_order_acquire);
        while (!ModeSweepRemaining_.compare_exchange_weak(
            pending, Max(pending, tail), std::memory_order_acq_rel, std::memory_order_acquire)) {
        }
        ModeSweepGeneration_ = generation;
        ModeSweepViewGeneration_ = spaceOp.View().Generation;
        ModeSweepHotSlots_ = hotSlots;
        ModeSweepQueue_ = 0;
        ModeSweepQueueRemaining_ = 0;
    }
    constexpr ui32 queuesPerTier = HotLevelCount + 1;
    constexpr std::array<ECacheTier, CacheTierCount> tiers{ ECacheTier::Regular, ECacheTier::Keep };
    constexpr std::array<EHotLevel, HotLevelCount> levels{ EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 };
    while (work-- != 0) {
        const ECacheTier id = tiers[ModeSweepQueue_ / queuesPerTier];
        const ui32 queue = ModeSweepQueue_ % queuesPerTier;
        const auto& tier = spaceOp.View().Tiers[id];
        const bool cold = queue == HotLevelCount;
        const TRingView ring = cold ? tier.Cold() : tier.Hot(levels[queue]);
        if (ModeSweepQueueRemaining_ == 0) {
            ModeSweepQueueRemaining_ = ring.Capacity;
        }
        // Inspect Hot slots without moving unchanged words through the aging policy
        auto* hotSlot = cold ? nullptr : ring.Slots + (ModeSweepQueueRemaining_ - 1) % ring.Capacity;
        const ui64 raw = cold ? ring.Pop() : hotSlot->load(std::memory_order_acquire);
        if (--ModeSweepQueueRemaining_ == 0) {
            ModeSweepQueue_ = (ModeSweepQueue_ + 1) % (queuesPerTier * CacheTierCount);
        }
        if (raw == 0) {
            continue;
        }
        if (!cold) {
            TOperationItemRef<TTraits> ref(
                spaceOp, Cache_.Items_.BindOperation(spaceOp).TryAcquireHotItemRef(TCacheItem::FromRaw(raw), id));
            if (ref && ReconcilePage(spaceOp, ref.Get())) {
                ui64 expected = raw;
                hotSlot->compare_exchange_strong(expected, 0, std::memory_order_acq_rel, std::memory_order_relaxed);
                RouteOwnedHot(spaceOp, ref.Get(), levels[queue]);
            }
            continue;
        }
        tier.Rings->RemoveColdEntry();
        const TColdCacheItem hint = TColdCacheItem::FromRaw(raw);
        if (!spaceOp.Contains(hint) || hint.IsFrozen()) {
            continue;
        }
        THandle& handle = spaceOp.Handles()[hint.Index()];
        const auto before = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (!before.IsColdInTier(id) ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != hint.ColdVersion()) {
            continue;
        }
        TOperationItemRef<TTraits> ref(spaceOp, Cache_.Items_.BindOperation(spaceOp).TryAcquireStructural(
                                                    TCacheItem::Make(before.Version(), hint.Index())));
        if (ref) {
            Cache_.AdvanceColdMembership(handle);
            if (ReconcilePage(spaceOp, ref.Get())) {
                const auto after = THandleState::FromRaw(
                    spaceOp.Handles()[ref.CacheItem().Index()].State.load(std::memory_order_acquire));
                if (after.IsHot()) {
                    RouteOwnedHot(spaceOp, ref.Get(), EHotLevel::L1);
                }
            }
        }
    }
    CheckModeGeneration(spaceOp, generation);
    if (hotSlots != loadHotSlots()) {
        RestartModeSweep(spaceOp);
    }
    return true;
}

template <class TTraits>
bool TCacheEviction<TTraits>::TrimKeep(TSpaceOperation& spaceOp) noexcept {
    if (!NeedsKeepTrim()) {
        return false;
    }
    if (ReclaimCold(spaceOp, ECacheTier::Keep)) {
        return true;
    }
    return HotPolicy_.ProcessBatch(spaceOp, spaceOp.View().Tiers[ECacheTier::Keep]);
}

template <class TTraits>
bool TCacheEviction<TTraits>::Reclaim(TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    return ReclaimCold(spaceOp, ECacheTier::Regular) || ReclaimCold(spaceOp, ECacheTier::Keep) ||
           EnforceCurrentLimit(spaceOp, bytes);
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReclaimToLimits(
    TSpaceOperation& spaceOp, ui64 softLimit, ui32 step, ui64 bytes) noexcept {
    bool progress = false;
    for (ui32 attempt = 0; attempt < step; ++attempt) {
        if (ExceedsLimit(Cache_.Memory_.PayloadUsage(), Cache_.Memory_.CurrentLimit(), bytes) ||
            (softLimit == 0 && Cache_.Memory_.ColdReclaimableItems() != 0)) {
            if (!ReclaimCold(spaceOp, ECacheTier::Regular) && !ReclaimCold(spaceOp, ECacheTier::Keep)) {
                break;
            }
        } else if (Cache_.Memory_.RetainedBytes() > softLimit) {
            if (!ProcessHotAtMinimum(spaceOp, ECacheTier::Regular) && !ProcessHotAtMinimum(spaceOp, ECacheTier::Keep)) {
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
ui32 TCacheEviction<TTraits>::ShrinkHotTarget(ui32 slots, ui32 step, ui32 minimum) const noexcept {
    return slots <= minimum ? 0 : slots - Min(slots - minimum, step);
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::GrowHotTarget(
    const TSpaceOperation& spaceOp, ECacheTier tier, ui32 slots, ui32 step) const noexcept {
    const ui32 maximum = static_cast<ui32>(spaceOp.View().Tiers[tier].HotStorage.SlotCount);
    return slots < maximum ? slots + Min(maximum - slots, Max<ui32>(1, step / 2)) : 0;
}

template <class TTraits>
ui32 TCacheEviction<TTraits>::HotResizeStep(ui32 slots) const noexcept {
    return Max(Cache_.Policy_.MinResizeStep, static_cast<ui32>(std::ceil(slots * Cache_.Policy_.ResizeStep)));
}

template <class TTraits>
bool TCacheEviction<TTraits>::DrainHotResize(TSpaceOperation& spaceOp) noexcept {
    bool progress = false;
    while (Cache_.HotResize_.Phase() == EHotResizePhase::Cut) {
        const ECacheTier tier = Cache_.HotResize_.Tier();
        ui32 level;
        ui64 raw;
        if (!Cache_.Space_->DrainHotResize(Cache_.HotResize_, level, raw)) {
            break;
        }
        progress = true;
        if (raw != 0) {
            ProcessHot(spaceOp, tier, static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
        }
    }
    return progress;
}

template <class TTraits>
bool TCacheEviction<TTraits>::EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes, bool forceShrinkHot) noexcept {
    const ui64 liveKeep = KeepLimit();
    if (ObservedKeepLimit_.exchange(liveKeep, std::memory_order_acq_rel) != liveKeep) {
        Cache_.Capacity_.RequestTierRebalance();
    }
    const ui64 soft = Cache_.Memory_.SoftLimit();
    const bool currentPressure = ExceedsLimit(Cache_.Memory_.PayloadUsage(), Cache_.Memory_.CurrentLimit(), bytes);
    const bool softPressure =
        SoftLimitPressure_.exchange(false, std::memory_order_relaxed) || Cache_.Memory_.RetainedBytes() > soft;
    bool progress = false;
    if (currentPressure) {
        progress = ReclaimCold(spaceOp, ECacheTier::Regular) || ReclaimCold(spaceOp, ECacheTier::Keep);
    }
    if (auto guard = Cache_.HotResize_.TryLock()) {
        auto current = Cache_.Space_->BeginOperation();
        if (Cache_.HotResize_.Phase() == EHotResizePhase::Cut) {
            progress = DrainHotResize(current) || progress;
        } else {
            const ui64 physical = Cache_.Memory_.PayloadUsage();
            const ui64 limit = Cache_.Memory_.CurrentLimit();
            const ui64 free = limit - Min(limit, physical);
            const ui64 headroom = Cache_.Memory_.ColdReclaimableBytes() + free - Min(free, bytes);
            const ui64 freeHandles = Cache_.Memory_.ColdReclaimableItems() + current.View().Free().Count();
            const ui64 usable =
                current.AllocationLimit() - 2 - Min(current.AllocationLimit() - 2, Cache_.Memory_.Sticky().Pages());
            const bool low = headroom < FractionCeil(EvictableByteBudget(), Cache_.Policy_.ColdMin) ||
                             freeHandles < (usable + 4) / 5;
            const bool high = headroom > FractionCeil(EvictableByteBudget(), Cache_.Policy_.Grow) &&
                              freeHandles > (usable * 3 + 9) / 10;
            const bool pressure = ExceedsLimit(physical, limit, bytes) || forceShrinkHot || softPressure;
            const auto minimums = Cache_.Policy_.HotMinimums(HasKeepWork());
            for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                const ui32 slots = static_cast<ui32>(current.View().Tiers[tier].EffectiveHotSlotCount());
                const ui32 minimum =
                    static_cast<ui32>(Min<ui64>(minimums[tier], current.View().Tiers[tier].HotStorage.SlotCount));
                const ui32 step = HotResizeStep(slots);
                ui32 target = 0;
                if (slots < minimum) {
                    target = minimum;
                } else if (pressure || low || (tier == ECacheTier::Keep && NeedsKeepTrim())) {
                    target = ShrinkHotTarget(slots, step, minimum);
                    if (!target) {
                        progress = ProcessHotAtMinimum(current, tier) || progress;
                    }
                } else if (high && !currentPressure) {
                    target = GrowHotTarget(current, tier, slots, step);
                }
                if (target && Cache_.Space_->BeginHotResize(tier, target, Cache_.HotResize_)) {
                    progress = true;
                    progress = DrainHotResize(current) || progress;
                    break;
                }
                if (progress && tier == ECacheTier::Regular && !pressure) {
                    break;
                }
            }
        }
    }
    if (currentPressure || softPressure || soft == 0) {
        progress = ReclaimToLimits(spaceOp, soft, SharedCacheTransitionWorkBatch, bytes) || progress;
        if (Cache_.Memory_.RetainedBytes() > soft) {
            SoftLimitPressure_.store(true, std::memory_order_relaxed);
        }
    }
    return progress;
}

template ui64 TCacheEviction<TProdTraits>::FractionCeil(ui64, double) noexcept;
template ui64 TCacheEviction<TProdTraits>::KeepLimit() const noexcept;
template bool TCacheEviction<TProdTraits>::UpdateKeepLimit(ui64) noexcept;
template void TCacheEviction<TProdTraits>::RefreshKeepLimits() noexcept;
template void TCacheEviction<TProdTraits>::ScheduleModeSweep(ui64) noexcept;
template void TCacheEviction<TProdTraits>::RestartModeSweep(TSpaceOperation&) noexcept;
template void TCacheEviction<TProdTraits>::CheckModeGeneration(TSpaceOperation&, ui64) noexcept;
template bool TCacheEviction<TProdTraits>::SweepModes() noexcept;
template bool TCacheEviction<TProdTraits>::TrimKeep(TSpaceOperation&) noexcept;
template bool TCacheEviction<TProdTraits>::Reclaim(TSpaceOperation&, ui64) noexcept;
template bool TCacheEviction<TProdTraits>::ReclaimToLimits(TSpaceOperation&, ui64, ui32, ui64) noexcept;
template ui32 TCacheEviction<TProdTraits>::ShrinkHotTarget(ui32, ui32, ui32) const noexcept;
template ui32 TCacheEviction<TProdTraits>::GrowHotTarget(const TSpaceOperation&, ECacheTier, ui32, ui32) const noexcept;
template ui32 TCacheEviction<TProdTraits>::HotResizeStep(ui32) const noexcept;
template bool TCacheEviction<TProdTraits>::DrainHotResize(TSpaceOperation&) noexcept;
template bool TCacheEviction<TProdTraits>::EnforceCurrentLimit(TSpaceOperation&, ui64, bool) noexcept;
template ui64 TCacheEviction<TTestTraits>::FractionCeil(ui64, double) noexcept;
template ui64 TCacheEviction<TTestTraits>::KeepLimit() const noexcept;
template bool TCacheEviction<TTestTraits>::UpdateKeepLimit(ui64) noexcept;
template void TCacheEviction<TTestTraits>::RefreshKeepLimits() noexcept;
template void TCacheEviction<TTestTraits>::ScheduleModeSweep(ui64) noexcept;
template void TCacheEviction<TTestTraits>::RestartModeSweep(TSpaceOperation&) noexcept;
template void TCacheEviction<TTestTraits>::CheckModeGeneration(TSpaceOperation&, ui64) noexcept;
template bool TCacheEviction<TTestTraits>::SweepModes() noexcept;
template bool TCacheEviction<TTestTraits>::TrimKeep(TSpaceOperation&) noexcept;
template bool TCacheEviction<TTestTraits>::Reclaim(TSpaceOperation&, ui64) noexcept;
template bool TCacheEviction<TTestTraits>::ReclaimToLimits(TSpaceOperation&, ui64, ui32, ui64) noexcept;
template ui32 TCacheEviction<TTestTraits>::ShrinkHotTarget(ui32, ui32, ui32) const noexcept;
template ui32 TCacheEviction<TTestTraits>::GrowHotTarget(const TSpaceOperation&, ECacheTier, ui32, ui32) const noexcept;
template ui32 TCacheEviction<TTestTraits>::HotResizeStep(ui32) const noexcept;
template bool TCacheEviction<TTestTraits>::DrainHotResize(TSpaceOperation&) noexcept;
template bool TCacheEviction<TTestTraits>::EnforceCurrentLimit(TSpaceOperation&, ui64, bool) noexcept;

} // namespace NKikimr::NSharedCache
