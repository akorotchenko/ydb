#pragma once

#include "shared_cache_memory.h"
#include "shared_cache_space.h"
#include "shared_cache_traits.h"

#include <atomic>

namespace NKikimr::NSharedCache {

template <class TTraits>
class TCacheEvictionServices {
public:
    explicit TCacheEvictionServices(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    bool DemoteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;
    void WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept;

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

    TCacheEviction(
        TCacheEvictionServices<TTraits> services, TCacheMemory& memory, const TSharedCachePolicy& policy) noexcept
        : Services_(services)
        , Memory_(memory)
        , Policy_(policy)
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

private:
    bool TrimKeepCold(TSpaceOperation& spaceOp) noexcept;
    bool ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept;
    static ui64 FractionCeil(ui64 value, double fraction) noexcept;

private:
    TCacheEvictionServices<TTraits> Services_;
    TCacheMemory& Memory_;
    const TSharedCachePolicy& Policy_;
    std::atomic<ui64> KeepColdMaxBytes_{ 0 }; // Configured ceiling; repeated updates preserve pressure shrinkage.
    std::atomic<ui64> KeepColdLimit_{ 0 }; // Effective budget, resized according to Cold+Free headroom.
    std::atomic<ui64> KeepColdTrimScanRemaining_{ 0 };
    std::atomic<bool> KeepColdTrimActive_{ false };
};

} // namespace NKikimr::NSharedCache
