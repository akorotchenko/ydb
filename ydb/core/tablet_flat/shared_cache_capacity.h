#pragma once

#include "shared_cache_space.h"

#include <atomic>

namespace NKikimr::NSharedCache {

class TCacheMemory;
template <class TTraits>
class TSharedCacheTableImpl;
template <class TTraits>
class TCacheEviction;

enum class EShrinkCutStatus {
    Blocked,
    Progress,
    Complete,
};

template <class TTraits>
class TCacheCapacityServices {
public:
    explicit TCacheCapacityServices(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    EShrinkCutStatus ReclaimShrinkHandle(TSpaceOperation& spaceOp, ui32 index) noexcept;
    void RefreshSoftLimit() noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
};

template <class TTraits>
class TCacheCapacity {
public:
    TCacheCapacity(TCacheCapacityServices<TTraits> services, TSharedCacheSpace& space,
        TSharedCacheTableImpl<TTraits>& table, TCacheMemory& memory, TCacheEviction<TTraits>& eviction) noexcept
        : Services_(services)
        , Space_(space)
        , Table_(table)
        , Memory_(memory)
        , Eviction_(eviction)
        , TargetHandleCount_(space.CurrentConfiguration().HandleCount())
        , Target_(space.CurrentConfiguration())
    {}

    void SetTarget(const TSharedCacheCapacity& target) noexcept;
    bool StartTransition() noexcept;
    bool RunMaintenance(THotResize& hotResize) noexcept;

    bool NeedsMaintenance() const noexcept {
        return Transition_.Phase() != ETransitionPhase::Idle ||
               Space_.CurrentConfiguration().HandleCount() != TargetHandleCount();
    }

    ui64 TargetHandleCount() const noexcept {
        return TargetHandleCount_.load(std::memory_order_relaxed);
    }

private:
    friend class TSharedCacheTestAccess;

    // Capacity transitions
    bool PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept;
    bool PublishMigrationView(TTransition& transition, THotResize& hotResize) noexcept;
    bool TryDrainTransition(TTransition& transition) noexcept;
    bool PublishFinalView(TTransition& transition, THotResize& hotResize) noexcept;
    bool AppendFreeHandles(TTransition& transition) noexcept;
    bool FinalDrain(TTransition& transition) noexcept;
    bool TryReleaseTransition(TTransition& transition) noexcept;
    bool CommitTransition(TTransition& transition) noexcept;
    bool AbandonTransition(TTransition& transition) noexcept;

    // Bucket transitions
    bool AdvanceBucketResize() noexcept;

    // Shrink work
    EShrinkCutStatus ReclaimShrinkCut() noexcept;

private:
    // Dependencies
    TCacheCapacityServices<TTraits> Services_;
    TSharedCacheSpace& Space_;
    TSharedCacheTableImpl<TTraits>& Table_;
    TCacheMemory& Memory_;
    TCacheEviction<TTraits>& Eviction_;

    // Capacity state
    std::atomic<ui64> TargetHandleCount_;
    TSharedCacheCapacity Target_;
    TTransition Transition_;
};

} // namespace NKikimr::NSharedCache
