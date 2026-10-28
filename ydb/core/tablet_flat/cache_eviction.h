#pragma once

#include "cache_hot_policy.h"
#include "cache_cold_policy.h"
#include "cache_memory.h"
#include "cache_space.h"
#include "cache_traits.h"

#include <array>
#include <atomic>

namespace NKikimr::NSharedCache {

template <class TTraits>
class TCacheItemRefImpl;
template <class TTraits>
class TOperationItemRef;

template <class TTraits>
class TCacheEviction {
public:
    using TOperation = TCacheEvictionOperation<TTraits>;

    explicit TCacheEviction(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
        , HotPolicy_(cache)
        , ColdPolicy_(cache)
    {}

    Y_FORCE_INLINE TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { Cache_, spaceOp };
    }

    // Policy resizing and maintenance
    bool UpdateKeepLimit(ui64 limit) noexcept;
    ui64 KeepLimit() const noexcept;
    void RefreshKeepLimits() noexcept;

    void MarkSoftLimitPressure() noexcept {
        SoftLimitPressure_.store(true, std::memory_order_relaxed);
    }

    void ScheduleModeSweep(ui64 slots) noexcept;
    bool SweepModes() noexcept;

    ui64 ModeGeneration() const noexcept {
        return ModeGeneration_.load(std::memory_order_acquire);
    }

    bool NeedsKeepTrim() const noexcept {
        return Cache_.Memory_.KeepOverLimit(KeepLimit());
    }

    bool HasKeepWork() const noexcept {
        return KeepRequestedBytes_.load(std::memory_order_relaxed) != 0 || Cache_.Memory_.KeepOwnedPages() != 0;
    }

    ui64 EvictableByteBudget() const noexcept {
        const ui64 softLimit = Cache_.Memory_.SoftLimit();
        return softLimit - Min(softLimit, Cache_.Memory_.Sticky().Bytes());
    }

private:
    friend class TCacheEvictionOperation<TTraits>;
    friend class TCacheHotPolicy<TTraits>;
    friend class TCacheColdPolicy<TTraits>;
    friend class TCacheTestAccess;

    // Placement reconciliation
    bool WantsKeepPage(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    bool ReconcilePage(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner) noexcept;
    bool TouchPage(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner) noexcept;
    TKeepReservation ReserveKeepForDemand(TSpaceOperation& spaceOp, ui64 bytes) noexcept;

    template <bool Demand>
    bool ReconcilePageImpl(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner) noexcept;
    void RestartModeSweep(TSpaceOperation& spaceOp) noexcept;
    void CheckModeGeneration(TSpaceOperation& spaceOp, ui64 generation) noexcept;

    // Hot routing and eviction
    void RouteHot(TSpaceOperation& spaceOp, ECacheTier tier, EHotLevel level, TCacheItem item) noexcept;
    void RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item) noexcept;
    void RouteOwnedHot(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner, EHotLevel level) noexcept;
    bool TryEvictHot(THandle& handle, THandleState state, TOperationItemRef<TTraits>& evictionRef) noexcept;

    Y_FORCE_INLINE void ProcessHot(
        TSpaceOperation& spaceOp, ECacheTier tier, EHotLevel level, TCacheItem item) noexcept {
        HotPolicy_.ProcessHot(spaceOp, spaceOp.View().Tiers[tier], level, item);
    }
    bool EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept;
    bool EvictFromHot(TSpaceOperation& spaceOp, TCacheItem item, ECacheTier expectedTier) noexcept;
    bool PromoteCold(TSpaceOperation& spaceOp, TCacheItem item, bool force = false) noexcept;

    Y_FORCE_INLINE bool ProcessHotAtMinimum(TSpaceOperation& spaceOp, ECacheTier tier = ECacheTier::Regular) noexcept {
        return HotPolicy_.ProcessBatch(spaceOp, spaceOp.View().Tiers[tier]);
    }

    // Cold routing and reclamation
    void FinishPageErase(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    bool EraseCold(TSpaceOperation& spaceOp, TColdCacheItem item, ECacheTier expectedTier) noexcept;

    Y_FORCE_INLINE void PublishCold(TSpaceOperation& spaceOp, TCacheItem item, ECacheTier tier) noexcept {
        ColdPolicy_.PublishCold(spaceOp, spaceOp.View().Tiers[tier], item);
    }

    Y_FORCE_INLINE void RouteCold(TSpaceOperation& spaceOp, ECacheTier tier, TColdCacheItem item) noexcept {
        ColdPolicy_.RouteCold(spaceOp, spaceOp.View().Tiers[tier], item);
    }

    Y_FORCE_INLINE bool ReclaimCold(TSpaceOperation& spaceOp, ECacheTier tier = ECacheTier::Regular) noexcept {
        return ColdPolicy_.Reclaim(spaceOp, spaceOp.View().Tiers[tier]);
    }

    bool TryEnqueueColdForPreload(TSpaceOperation& spaceOp, const TCacheTierView& tier, TColdCacheItem item,
        THandle& handle, THandleState state) noexcept;

    // Preload handle reclamation
    TCacheItemRefImpl<TTraits> AcquireRegularCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    bool IsPreloadProtected(
        TSpaceOperation& spaceOp, ui64 raw, bool cold, TOperationItemRef<TTraits>& collectionRef) noexcept;
    TRingExchangeResult PopForPreload(TSpaceOperation& spaceOp, TRingView ring, bool cold) noexcept;
    TRingExchangeResult ExchangeForPreload(
        TSpaceOperation& spaceOp, std::atomic<ui64>* slot, ui64 raw, bool cold) noexcept;
    bool TryErasePreloadPage(TSpaceOperation& spaceOp, TColdCacheItem item, ECacheTier expectedTier) noexcept;
    bool FinishPreloadCold(TSpaceOperation& spaceOp, const TCacheTierView& tier, TColdCacheItem item,
        std::atomic<ui64>* vacatedSlot = nullptr) noexcept;
    bool ReclaimRegularPage(TSpaceOperation& spaceOp) noexcept;

    // Policy resizing and maintenance
    bool TrimKeep(TSpaceOperation& spaceOp) noexcept;
    static ui64 FractionCeil(ui64 value, double fraction) noexcept;

    static Y_FORCE_INLINE bool ExceedsLimit(ui64 usage, ui64 limit, ui64 bytes) noexcept {
        return usage + bytes > limit;
    }

    // Cooperative reclamation: takes a cold entry and, when cold is empty, lets EnforceCurrentLimit move the
    // hot/cold boundary so that the coldest hot entries become reclaimable.
    bool Reclaim(TSpaceOperation& spaceOp, ui64 bytes = 0) noexcept;
    bool ReclaimToLimits(TSpaceOperation& spaceOp, ui64 softLimit, ui32 step, ui64 bytes = 0) noexcept;

    Y_FORCE_INLINE ui32 MinimumHotSlots(ECacheTier tier) const noexcept {
        return Cache_.Policy_.HotMinimums(HasKeepWork())[tier];
    }

    ui32 ShrinkHotTarget(ui32 effectiveHotSlots, ui32 step, ui32 minimum) const noexcept;
    ui32 GrowHotTarget(
        const TSpaceOperation& spaceOp, ECacheTier tier, ui32 effectiveHotSlots, ui32 step) const noexcept;
    ui32 HotResizeStep(ui32 effectiveHotSlots) const noexcept;
    bool DrainHotResize(TSpaceOperation& spaceOp) noexcept;
    bool EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes = 0, bool forceShrinkHot = false) noexcept;

private:
    // Dependencies
    TSharedCacheImpl<TTraits>& Cache_;
    TCacheHotPolicy<TTraits> HotPolicy_;
    TCacheColdPolicy<TTraits> ColdPolicy_;

    // Controller state
    std::atomic<ui64> KeepRequestedBytes_{ 0 };
    std::atomic<bool> SoftLimitPressure_{ false };
    std::atomic<ui64> ObservedKeepLimit_{ 0 };
    std::atomic<ui64> ModeGeneration_{ 0 };
    std::atomic<ui64> ModeSweepRemaining_{ 0 };
    std::atomic<bool> ModeSweepRestart_{ false };
    // Sweep cursors are used only by the serialized maintenance caller
    ui32 ModeSweepQueue_ = 0;
    ui64 ModeSweepQueueRemaining_ = 0;
    ui64 ModeSweepGeneration_ = Max<ui64>();
    ui32 ModeSweepViewGeneration_ = Max<ui32>();
    std::array<ui64, CacheTierCount> ModeSweepHotSlots_{};
};

// Leave operation constructors implicitly instantiated
template <class TTraits>
class TCacheEvictionOperation {
public:
    // Placement reconciliation
    Y_FORCE_INLINE bool WantsKeepPage(const THandle& page) noexcept {
        return Cache_.Eviction_.WantsKeepPage(SpaceOp_, page);
    }

    Y_FORCE_INLINE bool ReconcilePage(TCacheItemRefImpl<TTraits>& owner) noexcept {
        return Cache_.Eviction_.ReconcilePage(SpaceOp_, owner);
    }

    Y_FORCE_INLINE bool TouchPage(TCacheItemRefImpl<TTraits>& owner) noexcept {
        return Cache_.Eviction_.TouchPage(SpaceOp_, owner);
    }

    Y_FORCE_INLINE TKeepReservation ReserveKeepForDemand(ui64 bytes) noexcept {
        return Cache_.Eviction_.ReserveKeepForDemand(SpaceOp_, bytes);
    }

    ui64 ModeGeneration() const noexcept {
        return Cache_.Eviction_.ModeGeneration();
    }

    void CheckModeGeneration(ui64 generation) noexcept {
        Cache_.Eviction_.CheckModeGeneration(SpaceOp_, generation);
    }

    // Hot routing and eviction
    Y_FORCE_INLINE void RouteHot(EHotLevel level, TCacheItem item) noexcept {
        Cache_.Eviction_.RouteHot(SpaceOp_, level, item);
    }

    Y_FORCE_INLINE void RouteHot(ECacheTier tier, EHotLevel level, TCacheItem item) noexcept {
        Cache_.Eviction_.RouteHot(SpaceOp_, tier, level, item);
    }

    Y_FORCE_INLINE void RouteOwnedHot(TCacheItemRefImpl<TTraits>& owner, EHotLevel level) noexcept {
        Cache_.Eviction_.RouteOwnedHot(SpaceOp_, owner, level);
    }

    Y_FORCE_INLINE void ProcessHot(EHotLevel level, TCacheItem item) noexcept {
        Cache_.Eviction_.ProcessHot(SpaceOp_, ECacheTier::Regular, level, item);
    }

    Y_FORCE_INLINE bool EvictFromHot(ui32 index) noexcept {
        return Cache_.Eviction_.EvictFromHot(SpaceOp_, index);
    }

    Y_FORCE_INLINE bool EvictFromHot(TCacheItem item) noexcept {
        return Cache_.Eviction_.EvictFromHot(SpaceOp_, item, ECacheTier::Regular);
    }

    Y_FORCE_INLINE bool PromoteCold(TCacheItem item, bool force = false) noexcept {
        return Cache_.Eviction_.PromoteCold(SpaceOp_, item, force);
    }

    Y_FORCE_INLINE bool ProcessHotAtMinimum() noexcept {
        return Cache_.Eviction_.ProcessHotAtMinimum(SpaceOp_);
    }

    // Cold routing and reclamation
    Y_FORCE_INLINE void FinishPageErase(TCacheItem item) noexcept {
        Cache_.Eviction_.FinishPageErase(SpaceOp_, item);
    }

    Y_FORCE_INLINE bool EraseCold(TColdCacheItem item, ECacheTier expectedTier) noexcept {
        return Cache_.Eviction_.EraseCold(SpaceOp_, item, expectedTier);
    }

    Y_FORCE_INLINE void RouteCold(ECacheTier tier, TColdCacheItem item) noexcept {
        Cache_.Eviction_.RouteCold(SpaceOp_, tier, item);
    }

    Y_FORCE_INLINE void PublishCold(TCacheItem nativeItem, ECacheTier tier) noexcept {
        Cache_.Eviction_.PublishCold(SpaceOp_, nativeItem, tier);
    }

    Y_FORCE_INLINE bool ReclaimCold(ECacheTier tier = ECacheTier::Regular) noexcept {
        return Cache_.Eviction_.ReclaimCold(SpaceOp_, tier);
    }

    // Preload handle reclamation
    Y_FORCE_INLINE bool ReclaimRegularPage() noexcept {
        return Cache_.Eviction_.ReclaimRegularPage(SpaceOp_);
    }

    // Policy resizing and maintenance
    Y_FORCE_INLINE bool TrimKeep() noexcept {
        return Cache_.Eviction_.TrimKeep(SpaceOp_);
    }

    Y_FORCE_INLINE bool Reclaim(ui64 bytes = 0) noexcept {
        return Cache_.Eviction_.Reclaim(SpaceOp_, bytes);
    }

    Y_FORCE_INLINE bool ReclaimToLimits(ui64 softLimit, ui32 step, ui64 bytes = 0) noexcept {
        return Cache_.Eviction_.ReclaimToLimits(SpaceOp_, softLimit, step, bytes);
    }

    Y_FORCE_INLINE bool EnforceCurrentLimit(ui64 bytes = 0, bool forceShrinkHot = false) noexcept {
        return Cache_.Eviction_.EnforceCurrentLimit(SpaceOp_, bytes, forceShrinkHot);
    }

    Y_FORCE_INLINE bool DrainHotResize() noexcept {
        return Cache_.Eviction_.DrainHotResize(SpaceOp_);
    }

private:
    friend class TCacheEviction<TTraits>;
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheOperationView<TTraits>;

    Y_FORCE_INLINE TCacheEvictionOperation(TSharedCacheImpl<TTraits>& cache, TSpaceOperation& spaceOp) noexcept
        : Cache_(cache)
        , SpaceOp_(spaceOp)
    {}

private:
    TSharedCacheImpl<TTraits>& Cache_;
    TSpaceOperation& SpaceOp_;
};

} // namespace NKikimr::NSharedCache
