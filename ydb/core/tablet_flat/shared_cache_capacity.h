#pragma once

#include "shared_cache_space.h"

#include <atomic>

namespace NKikimr::NSharedCache {

enum class EShrinkCutStatus {
    Blocked,
    Progress,
    Complete,
};

template <class TTraits>
class TCacheCapacity {
public:
    explicit TCacheCapacity(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
        , TargetHandleCount_(cache.Space_->CurrentConfiguration().HandleCount())
        , Target_(cache.Space_->CurrentConfiguration())
    {}

    void SetTarget(const TSharedCacheCapacity& target) noexcept;
    bool StartTransition() noexcept;
    bool RunMaintenance() noexcept;

    bool NeedsMaintenance() const noexcept {
        return Transition_.Phase() != ETransitionPhase::Idle ||
               Cache_.Space_->CurrentConfiguration().HandleCount() != TargetHandleCount();
    }

    ui64 TargetHandleCount() const noexcept {
        return TargetHandleCount_.load(std::memory_order_relaxed);
    }

private:
    friend class TSharedCacheTestAccess;

    // Capacity transitions
    bool PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept;
    bool PublishMigrationView(TTransition& transition) noexcept;
    bool TryDrainTransition(TTransition& transition) noexcept;
    bool PublishFinalView(TTransition& transition) noexcept;
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
    TSharedCacheImpl<TTraits>& Cache_;

    // Capacity state
    std::atomic<ui64> TargetHandleCount_;
    TSharedCacheCapacity Target_;
    TTransition Transition_;
};

} // namespace NKikimr::NSharedCache
