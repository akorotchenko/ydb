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
    using TOperation = TCacheEvictionOperation<TTraits>;

    explicit TCacheEviction(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    Y_FORCE_INLINE TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { Cache_, spaceOp };
    }

    // Policy resizing and maintenance
    bool UpdateKeepColdLimit(ui64 limit) noexcept;

    ui64 EvictableByteBudget() const noexcept {
        const ui64 softLimit = Cache_.Memory_.SoftLimit();
        return softLimit - Min(softLimit, Cache_.Memory_.Sticky().Bytes());
    }

    ui64 KeepColdLimit() const noexcept {
        const ui64 budget = EvictableByteBudget();
        // KeepCold has its own share of the evictable byte budget
        const ui64 maximum = budget - FractionCeil(budget, 1.0 - Cache_.Policy_.KeepColdMaxShare);
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

    Y_FORCE_INLINE void ScheduleKeepColdSweep(ui64 slots) noexcept {
        KeepColdSweepRemaining_.store(slots, std::memory_order_release);
    }

    bool SweepKeepCold() noexcept;

    // Routing state
    static Y_FORCE_INLINE bool IsKeepColdQueued(const THandle& handle) noexcept {
        return (handle.ColdVersion.load(std::memory_order_acquire) & KeepColdQueuedMask) != 0;
    }

private:
    friend class TCacheEvictionOperation<TTraits>;
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
    template <bool Preload, bool Requeue = false>
    TCacheItem PutHotImpl(
        TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item, TRingExchangeResult* placement = nullptr) noexcept;

    Y_FORCE_INLINE void RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item) noexcept {
        const TCacheItem displaced = PutHot(spaceOp, level, item);
        if (!displaced.IsNull()) {
            ProcessHot(spaceOp, level, displaced);
        }
    }

    static Y_FORCE_INLINE bool TakeHotFrequency(
        THandle& handle, TCacheItem item, THandleState& state, ui8& frequency) noexcept;
    Y_FORCE_INLINE void CompleteHotEviction(TSpaceOperation& spaceOp, THandle& handle, THandleState coldState,
        ui64 bytes, TOperationItemRef<TTraits>& itemRef, ui32 initialColdVersion) noexcept;
    void ProcessHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item) noexcept;
    template <bool Preload>
    void ProcessHotImpl(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item) noexcept;
    bool EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept;
    bool EvictFromHot(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    bool PromoteCold(TSpaceOperation& spaceOp, TCacheItem item, bool force = false) noexcept;
    bool ProcessHotAtMinimum(TSpaceOperation& spaceOp) noexcept;
    template <bool Preload>
    bool ProcessHotBatch(TSpaceOperation& spaceOp) noexcept;

    // Cold routing and reclamation
    void CompleteColdPageRelease(TOperationItemRef<TTraits>& ownerRef) noexcept;
    bool EraseCold(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    void RouteCold(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    bool ReclaimCold(TSpaceOperation& spaceOp) noexcept;
    template <bool Preload>
    bool ReclaimColdImpl(TSpaceOperation& spaceOp) noexcept;
    bool DemoteKeepColdForSoftLimit(TSpaceOperation& spaceOp) noexcept;
    bool ReclaimKeepCold(TSpaceOperation& spaceOp) noexcept;

    // Preload handle reclamation
    TSharedCacheItemRefImpl<TTraits> AcquireRegularCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    bool IsPreloadProtected(
        TSpaceOperation& spaceOp, ui64 raw, bool cold, TOperationItemRef<TTraits>& collectionRef) noexcept;
    TRingExchangeResult PopForPreload(TSpaceOperation& spaceOp, TRingView ring, bool cold) noexcept;
    TRingExchangeResult ExchangeForPreload(
        TSpaceOperation& spaceOp, std::atomic<ui64>* slot, ui64 raw, bool cold) noexcept;
    bool TryErasePreloadPage(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    bool ReclaimRegularPage(TSpaceOperation& spaceOp) noexcept;

    // Policy resizing and maintenance
    bool TrimKeepCold(TSpaceOperation& spaceOp) noexcept;
    bool ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept;
    static ui64 FractionCeil(ui64 value, double fraction) noexcept;

    static Y_FORCE_INLINE bool ExceedsLimit(ui64 usage, ui64 limit, ui64 bytes) noexcept {
        return usage + bytes > limit;
    }

    // Cooperative reclamation: takes a cold entry and, when cold is empty, lets EnforceCurrentLimit move the
    // hot/cold boundary so that the coldest hot entries become reclaimable.
    bool Reclaim(TSpaceOperation& spaceOp, ui64 bytes = 0) noexcept;
    bool ReclaimToLimits(TSpaceOperation& spaceOp, ui64 softLimit, ui32 step, ui64 bytes = 0) noexcept;

    Y_FORCE_INLINE ui32 MinimumHotSlots() const noexcept {
        return Cache_.Policy_.MinHotSlots;
    }

    ui32 ShrinkHotTarget(ui32 effectiveHotSlots, ui32 step) const noexcept;
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

// Leave operation constructors implicitly instantiated
template <class TTraits>
class TCacheEvictionOperation {
public:
    // KeepCold routing
    Y_FORCE_INLINE bool WantsKeepCold(const THandle& page) noexcept {
        return Cache_.Eviction_.WantsKeepCold(SpaceOp_, page);
    }

    Y_FORCE_INLINE bool TryReleaseColdPageToKeepCold(TCacheItem item, ui64& expectedRaw) noexcept {
        return Cache_.Eviction_.TryReleaseColdPageToKeepCold(SpaceOp_, item, expectedRaw);
    }

    Y_FORCE_INLINE bool DemoteKeepCold(TColdCacheItem item, TPageCacheItem expectedPage = {}) noexcept {
        return Cache_.Eviction_.DemoteKeepCold(SpaceOp_, item, expectedPage);
    }

    Y_FORCE_INLINE void WithdrawKeepColdEntry(TColdCacheItem item) noexcept {
        Cache_.Eviction_.WithdrawKeepColdEntry(SpaceOp_, item);
    }

    Y_FORCE_INLINE bool MatchesKeepColdEntry(TColdCacheItem item) noexcept {
        return Cache_.Eviction_.MatchesKeepColdEntry(SpaceOp_, item);
    }

    Y_FORCE_INLINE void RouteKeepCold(TColdCacheItem item) noexcept {
        Cache_.Eviction_.RouteKeepCold(SpaceOp_, item);
    }

    Y_FORCE_INLINE void ReleaseKeepColdRef(TOperationItemRef<TTraits>& itemRef, ui32 initialColdVersion) noexcept {
        Cache_.Eviction_.ReleaseKeepColdRef(SpaceOp_, itemRef, initialColdVersion);
    }

    // Hot routing and eviction
    Y_FORCE_INLINE void RouteHot(EHotLevel level, TCacheItem item) noexcept {
        Cache_.Eviction_.RouteHot(SpaceOp_, level, item);
    }

    Y_FORCE_INLINE void ProcessHot(EHotLevel level, TCacheItem item) noexcept {
        Cache_.Eviction_.ProcessHot(SpaceOp_, level, item);
    }

    Y_FORCE_INLINE bool EvictFromHot(ui32 index) noexcept {
        return Cache_.Eviction_.EvictFromHot(SpaceOp_, index);
    }

    Y_FORCE_INLINE bool EvictFromHot(TCacheItem item) noexcept {
        return Cache_.Eviction_.EvictFromHot(SpaceOp_, item);
    }

    Y_FORCE_INLINE bool PromoteCold(TCacheItem item, bool force = false) noexcept {
        return Cache_.Eviction_.PromoteCold(SpaceOp_, item, force);
    }

    Y_FORCE_INLINE bool ProcessHotAtMinimum() noexcept {
        return Cache_.Eviction_.ProcessHotAtMinimum(SpaceOp_);
    }

    // Cold routing and reclamation
    Y_FORCE_INLINE void CompleteColdPageRelease(TOperationItemRef<TTraits>& ownerRef) noexcept {
        Cache_.Eviction_.CompleteColdPageRelease(ownerRef);
    }

    Y_FORCE_INLINE bool EraseCold(TColdCacheItem item) noexcept {
        return Cache_.Eviction_.EraseCold(SpaceOp_, item);
    }

    Y_FORCE_INLINE void RouteCold(TColdCacheItem item) noexcept {
        Cache_.Eviction_.RouteCold(SpaceOp_, item);
    }

    Y_FORCE_INLINE bool ReclaimCold() noexcept {
        return Cache_.Eviction_.ReclaimCold(SpaceOp_);
    }

    Y_FORCE_INLINE bool ReclaimKeepCold() noexcept {
        return Cache_.Eviction_.ReclaimKeepCold(SpaceOp_);
    }

    // Preload handle reclamation
    Y_FORCE_INLINE bool ReclaimRegularPage() noexcept {
        return Cache_.Eviction_.ReclaimRegularPage(SpaceOp_);
    }

    // Policy resizing and maintenance
    Y_FORCE_INLINE bool TrimKeepCold() noexcept {
        return Cache_.Eviction_.TrimKeepCold(SpaceOp_);
    }

    Y_FORCE_INLINE bool ShrinkKeepCold() noexcept {
        return Cache_.Eviction_.ShrinkKeepCold(SpaceOp_);
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
