#pragma once

#include "shared_cache_memory.h"
#include "shared_cache_space.h"
#include "shared_cache_traits.h"

#include <atomic>

namespace NKikimr::NSharedCache {

template <class TTraits>
class TSharedCacheItemRefImpl;
template <class TTraits>
class TOperationItemRef;

enum class EHotLevel : ui32 {
    L0,
    L1,
    L2,
    L3,
};

template <class TTraits>
class TCacheEviction {
public:
    class TOperation {
    public:
        // KeepCold routing
        bool WantsKeepCold(const THandle& page) noexcept {
            return Eviction_.WantsKeepCold(SpaceOp_, page);
        }

        bool TryReleaseColdPageToKeepCold(TCacheItem item, ui64& expectedRaw) noexcept {
            return Eviction_.TryReleaseColdPageToKeepCold(SpaceOp_, item, expectedRaw);
        }

        bool DemoteKeepCold(TColdCacheItem item, TPageCacheItem expectedPage = {}) noexcept {
            return Eviction_.DemoteKeepCold(SpaceOp_, item, expectedPage);
        }

        void WithdrawKeepColdEntry(TColdCacheItem item) noexcept {
            Eviction_.WithdrawKeepColdEntry(SpaceOp_, item);
        }

        bool MatchesKeepColdEntry(TColdCacheItem item) noexcept {
            return Eviction_.MatchesKeepColdEntry(SpaceOp_, item);
        }

        void RouteKeepCold(TColdCacheItem item) noexcept {
            Eviction_.RouteKeepCold(SpaceOp_, item);
        }

        void ReleaseKeepColdRef(TOperationItemRef<TTraits>& itemRef, ui32 initialColdVersion) noexcept {
            Eviction_.ReleaseKeepColdRef(SpaceOp_, itemRef, initialColdVersion);
        }

        // Hot routing and eviction
        void RouteHot(EHotLevel level, TCacheItem item) noexcept {
            Eviction_.RouteHot(SpaceOp_, level, item);
        }

        void ProcessHot(EHotLevel level, TCacheItem item) noexcept {
            Eviction_.ProcessHot(SpaceOp_, level, item);
        }

        bool EvictFromHot(ui32 index) noexcept {
            return Eviction_.EvictFromHot(SpaceOp_, index);
        }

        bool EvictFromHot(TCacheItem item) noexcept {
            return Eviction_.EvictFromHot(SpaceOp_, item);
        }

        bool PromoteCold(TCacheItem item, bool force = false) noexcept {
            return Eviction_.PromoteCold(SpaceOp_, item, force);
        }

        bool ProcessHotAtMinimum() noexcept {
            return Eviction_.ProcessHotAtMinimum(SpaceOp_);
        }

        // Cold routing and reclamation
        void CompleteColdPageRelease(TOperationItemRef<TTraits>& ownerRef) noexcept {
            Eviction_.CompleteColdPageRelease(ownerRef);
        }

        bool EraseCold(TColdCacheItem item) noexcept {
            return Eviction_.EraseCold(SpaceOp_, item);
        }

        void RouteCold(TColdCacheItem item) noexcept {
            Eviction_.RouteCold(SpaceOp_, item);
        }

        bool ReclaimCold() noexcept {
            return Eviction_.ReclaimCold(SpaceOp_);
        }

        bool ReclaimKeepCold() noexcept {
            return Eviction_.ReclaimKeepCold(SpaceOp_);
        }

        // Policy resizing and maintenance
        bool TrimKeepCold() noexcept {
            return Eviction_.TrimKeepCold(SpaceOp_);
        }

        bool ShrinkKeepCold() noexcept {
            return Eviction_.ShrinkKeepCold(SpaceOp_);
        }

        bool Reclaim(ui64 bytes = 0) noexcept {
            return Eviction_.Reclaim(SpaceOp_, bytes);
        }

        bool ReclaimToLimits(ui64 softLimit, ui32 step, ui64 bytes = 0) noexcept {
            return Eviction_.ReclaimToLimits(SpaceOp_, softLimit, step, bytes);
        }

        bool EnforceCurrentLimit(ui64 bytes = 0, bool forceShrinkHot = false) noexcept {
            return Eviction_.EnforceCurrentLimit(SpaceOp_, bytes, forceShrinkHot);
        }

        bool DrainHotResize() noexcept {
            return Eviction_.DrainHotResize(SpaceOp_);
        }

    private:
        friend class TCacheEviction;

        TOperation(TCacheEviction& eviction, TSpaceOperation& spaceOp) noexcept
            : Eviction_(eviction)
            , SpaceOp_(spaceOp)
        {}

    private:
        TCacheEviction& Eviction_;
        TSpaceOperation& SpaceOp_;
    };

    explicit TCacheEviction(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

    // Policy resizing and maintenance
    bool UpdateKeepColdLimit(ui64 limit) noexcept;

    ui64 EvictableByteBudget() const noexcept {
        const ui64 softLimit = Cache_.Memory_.SoftLimit();
        return softLimit - Min(softLimit, Cache_.Memory_.Sticky().Bytes());
    }

    ui64 KeepColdLimit() const noexcept {
        const ui64 budget = EvictableByteBudget();
        // KeepCold uses the remainder after the normal Hot minimum (40% with HotMin = 60%).
        const ui64 maximum = budget - FractionCeil(budget, Cache_.Policy_.HotMin);
        return Min(Min(KeepColdLimit_.load(std::memory_order_relaxed),
                       KeepColdMaxBytes_.load(std::memory_order_relaxed)), maximum);
    }

    bool NeedsKeepColdTrim() const noexcept {
        return Cache_.Memory_.KeepColdOwnedBytes() > KeepColdLimit() ||
               KeepColdTrimActive_.load(std::memory_order_relaxed);
    }

    void MarkSoftLimitPressure() noexcept {
        SoftLimitPressure_.store(true, std::memory_order_relaxed);
    }

    // Requires the shared HotResize lock
    void RefreshEvictableByteBudget() noexcept;

    bool GrowKeepCold() noexcept;
    void ScheduleKeepColdSweep(ui64 slots) noexcept;
    bool SweepKeepCold() noexcept;

    // Routing state
    static bool IsKeepColdQueued(const THandle& handle) noexcept;

private:
    friend class TSharedCacheTestAccess;

    // KeepCold routing
    bool WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    bool TryReleaseColdPageToKeepCold(TSpaceOperation& spaceOp, TCacheItem item, ui64& expectedRaw) noexcept;
    bool DemoteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem item, TPageCacheItem expectedPage = {}) noexcept;
    void ReleaseColdDemotionRef(TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& itemRef) noexcept;
    void WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    bool MatchesKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    void RouteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    void ReleaseKeepColdRef(
        TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& itemRef, ui32 initialColdVersion) noexcept;

    // Hot routing and eviction
    TCacheItem PutHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item) noexcept;
    void RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item) noexcept;
    static Y_FORCE_INLINE bool TakeHotFrequency(
        THandle& handle, TCacheItem item, THandleState& state, ui8& frequency) noexcept;
    Y_FORCE_INLINE void CompleteHotEviction(TSpaceOperation& spaceOp, THandle& handle, THandleState coldState,
        ui64 bytes, TOperationItemRef<TTraits>& itemRef, ui32 initialColdVersion) noexcept;
    void ProcessHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item) noexcept;
    bool EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept;
    bool EvictFromHot(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    bool PromoteCold(TSpaceOperation& spaceOp, TCacheItem item, bool force = false) noexcept;
    bool ProcessHotAtMinimum(TSpaceOperation& spaceOp) noexcept;

    // Cold routing and reclamation
    void CompleteColdPageRelease(TOperationItemRef<TTraits>& ownerRef) noexcept;
    bool EraseCold(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    void RouteCold(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    bool ReclaimCold(TSpaceOperation& spaceOp) noexcept;
    bool DemoteKeepColdForSoftLimit(TSpaceOperation& spaceOp) noexcept;
    bool ReclaimKeepCold(TSpaceOperation& spaceOp) noexcept;

    // Policy resizing and maintenance
    bool TrimKeepCold(TSpaceOperation& spaceOp) noexcept;
    bool ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept;
    static ui64 FractionCeil(ui64 value, double fraction) noexcept;
    static bool ExceedsLimit(ui64 usage, ui64 limit, ui64 bytes) noexcept;

    // Cooperative reclamation: takes a cold entry and, when cold is empty, lets EnforceCurrentLimit move the
    // hot/cold boundary so that the coldest hot entries become reclaimable.
    bool Reclaim(TSpaceOperation& spaceOp, ui64 bytes = 0) noexcept;
    bool ReclaimToLimits(TSpaceOperation& spaceOp, ui64 softLimit, ui32 step, ui64 bytes = 0) noexcept;

    ui32 MinimumHotSlots(const TSpaceOperation& spaceOp, bool underPressure = false) const noexcept;
    ui32 ShrinkHotTarget(
        const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step, bool underPressure) const noexcept;
    ui32 GrowHotTarget(const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step) const noexcept;
    ui32 HotResizeStep(ui32 effectiveHotSlots) const noexcept;
    bool DrainHotResize(TSpaceOperation& spaceOp) noexcept;
    bool EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes = 0, bool forceShrinkHot = false) noexcept;

private:
    // Dependencies
    TSharedCacheImpl<TTraits>& Cache_;

    // Policy resizing state
    ui64 ColdByteBudget_ = Max<ui64>(); // Protected by the shared HotResize lock; watermark calculation input
    std::atomic<ui64> ColdMinBytes_{ 0 };
    std::atomic<ui64> ColdTargetBytes_{ 0 };
    std::atomic<ui64> ColdGrowBytes_{ 0 };
    std::atomic<bool> KeepColdPressure_{ false };
    std::atomic<bool> SoftLimitPressure_{ false };
    std::atomic<ui64> KeepColdMaxBytes_{ 0 }; // Configured ceiling; repeated updates preserve pressure shrinkage.
    std::atomic<ui64> KeepColdLimit_{ 0 }; // Effective budget, resized according to Cold+Free headroom.
    std::atomic<ui64> KeepColdTrimScanRemaining_{ 0 };
    std::atomic<bool> KeepColdTrimActive_{ false };
    std::atomic<ui64> KeepColdSweepRemaining_{ 0 };
};

} // namespace NKikimr::NSharedCache
