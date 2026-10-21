#pragma once

#include "shared_cache_memory.h"
#include "shared_cache_space.h"
#include "shared_cache_traits.h"

#include <atomic>

namespace NKikimr::NSharedCache {

template <class TTraits>
class TOperationItemRef;
template <class TTraits>
class TCacheCollections;
class TCollection;

template <class TTraits>
class TCacheEvictionServices {
public:
    explicit TCacheEvictionServices(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    static ui64 PageBytes(const THandle& handle) noexcept;
    static ui32 AdvanceColdMembership(THandle& handle) noexcept;
    static bool AdvanceColdMembership(
        THandle& handle, ui32 expectedVersion, bool keepColdQueued, ui32& newVersion) noexcept;
    TCollection* ReleaseForColdDemotion(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    void NotifyResourcesAvailable() noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
};

template <class TTraits>
class TCacheEviction {
public:
    class TOperation {
    public:
        bool TrimKeepCold() noexcept {
            return Eviction_.TrimKeepCold(SpaceOp_);
        }

        bool ShrinkKeepCold() noexcept {
            return Eviction_.ShrinkKeepCold(SpaceOp_);
        }

        bool WantsKeepCold(const THandle& page) noexcept {
            return Eviction_.WantsKeepCold(SpaceOp_, page);
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

        void ReleaseKeepColdPin(TOperationItemRef<TTraits>& pin, ui32 initialColdVersion) noexcept {
            Eviction_.ReleaseKeepColdPin(SpaceOp_, pin, initialColdVersion);
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

    TCacheEviction(TCacheEvictionServices<TTraits> services, TCacheMemory& memory,
        TCacheCollections<TTraits>& collections, const TSharedCachePolicy& policy, const TTraits& traits) noexcept
        : Services_(services)
        , Memory_(memory)
        , Collections_(collections)
        , Policy_(policy)
        , Traits_(traits)
    {}

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

    bool UpdateKeepColdLimit(ui64 limit) noexcept;

    ui64 EvictableByteBudget() const noexcept {
        const ui64 softLimit = Memory_.SoftLimit();
        return softLimit - Min(softLimit, Memory_.Sticky().Bytes());
    }

    ui64 KeepColdLimit() const noexcept {
        const ui64 budget = EvictableByteBudget();
        // KeepCold uses the remainder after the normal Hot minimum (40% with HotMin = 60%).
        const ui64 maximum = budget - FractionCeil(budget, Policy_.HotMin);
        return Min(Min(KeepColdLimit_.load(std::memory_order_relaxed),
                       KeepColdMaxBytes_.load(std::memory_order_relaxed)), maximum);
    }

    bool NeedsKeepColdTrim() const noexcept {
        return Memory_.KeepColdOwnedBytes() > KeepColdLimit() || KeepColdTrimActive_.load(std::memory_order_relaxed);
    }

    bool GrowKeepCold() noexcept;
    static bool IsKeepColdQueued(const THandle& handle) noexcept;
    void ScheduleKeepColdSweep(ui64 slots) noexcept;
    bool SweepKeepCold(TSharedCacheSpace& space) noexcept;

private:
    bool TrimKeepCold(TSpaceOperation& spaceOp) noexcept;
    bool ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept;
    static ui64 FractionCeil(ui64 value, double fraction) noexcept;

    bool WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    bool DemoteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem item, TPageCacheItem expectedPage = {}) noexcept;
    void ReleaseColdDemotionPin(TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& pin) noexcept;
    void WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    bool MatchesKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    void RouteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    void ReleaseKeepColdPin(
        TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& pin, ui32 initialColdVersion) noexcept;

private:
    TCacheEvictionServices<TTraits> Services_;
    TCacheMemory& Memory_;
    TCacheCollections<TTraits>& Collections_;
    const TSharedCachePolicy& Policy_;
    const TTraits& Traits_;
    std::atomic<ui64> KeepColdMaxBytes_{ 0 }; // Configured ceiling; repeated updates preserve pressure shrinkage.
    std::atomic<ui64> KeepColdLimit_{ 0 }; // Effective budget, resized according to Cold+Free headroom.
    std::atomic<ui64> KeepColdTrimScanRemaining_{ 0 };
    std::atomic<bool> KeepColdTrimActive_{ false };
    std::atomic<ui64> KeepColdSweepRemaining_{ 0 };
};

} // namespace NKikimr::NSharedCache
