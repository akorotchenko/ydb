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

    Y_FORCE_INLINE void SetTarget(const TSharedCacheCapacity& target) noexcept {
        Target_ = target;
        TargetHandleCount_.store(target.HandleCount(), std::memory_order_relaxed);
    }

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
    EPrepareResult PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept;

    bool PublishMigrationView(TTransition& transition) noexcept;

    Y_FORCE_INLINE bool TryDrainTransition(TTransition& transition) noexcept {
        return Cache_.Space_->TryDrainTransition(transition);
    }

    bool PublishFinalView(TTransition& transition) noexcept;
    bool AppendFreeHandles(TTransition& transition) noexcept;
    bool FinalDrain(TTransition& transition) noexcept;

    Y_FORCE_INLINE bool TryReleaseTransition(TTransition& transition) noexcept {
        return Cache_.Space_->TryReleaseTransition(transition);
    }

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
