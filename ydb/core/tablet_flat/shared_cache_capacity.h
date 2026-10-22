#pragma once

#include "shared_cache_space.h"

#include <atomic>

namespace NKikimr::NSharedCache {

class TCacheMemory;
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

    bool AdvanceBucketResize() noexcept;
    EShrinkCutStatus ReclaimShrinkHandle(TSpaceOperation& spaceOp, ui32 index) noexcept;
    void RefreshSoftLimit() noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
};

template <class TTraits>
class TCacheCapacity {
public:
    TCacheCapacity(TCacheCapacityServices<TTraits> services, TSharedCacheSpace& space, TCacheMemory& memory,
        TCacheEviction<TTraits>& eviction) noexcept
        : Services_(services)
        , Space_(space)
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

    bool PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept;
    bool PublishMigrationView(TTransition& transition, THotResize& hotResize) noexcept;
    bool TryDrainTransition(TTransition& transition) noexcept;
    bool PublishFinalView(TTransition& transition, THotResize& hotResize) noexcept;
    bool AppendFreeHandles(TTransition& transition) noexcept;
    bool FinalDrain(TTransition& transition) noexcept;
    bool TryReleaseTransition(TTransition& transition) noexcept;
    bool CommitTransition(TTransition& transition) noexcept;
    bool AbandonTransition(TTransition& transition) noexcept;
    EShrinkCutStatus ReclaimShrinkCut() noexcept;

private:
    TCacheCapacityServices<TTraits> Services_;
    TSharedCacheSpace& Space_;
    TCacheMemory& Memory_;
    TCacheEviction<TTraits>& Eviction_;
    std::atomic<ui64> TargetHandleCount_;
    TSharedCacheCapacity Target_;
    TTransition Transition_;
};

} // namespace NKikimr::NSharedCache
