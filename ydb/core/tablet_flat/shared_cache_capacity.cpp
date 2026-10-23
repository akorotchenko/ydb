#include "shared_cache_capacity.h"
#include "shared_cache.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
void TCacheCapacity<TTraits>::SetTarget(const TSharedCacheCapacity& target) noexcept {
    Target_ = target;
    TargetHandleCount_.store(target.HandleCount(), std::memory_order_relaxed);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept {
    const ui64 currentStaticBytes = Cache_.Space_->CurrentConfiguration().StaticBytes;
    const ui64 staticDeltaBytes = target.StaticBytes > currentStaticBytes ? target.StaticBytes - currentStaticBytes : 0;
    if (staticDeltaBytes != 0) {
        Cache_.Memory_.PrepareStaticResize(staticDeltaBytes);
    }
    if (Cache_.Space_->PrepareTransition(target, transition)) {
        return true;
    }
    if (staticDeltaBytes != 0) {
        Cache_.Memory_.CancelStaticResize(staticDeltaBytes);
    }
    return false;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PublishMigrationView(TTransition& transition) noexcept {
    if (auto hotResizeGuard = Cache_.HotResize_.TryLock()) {
        return Cache_.Space_->PublishMigrationView(transition);
    }
    return false;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::TryDrainTransition(TTransition& transition) noexcept {
    return Cache_.Space_->TryDrainTransition(transition);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PublishFinalView(TTransition& transition) noexcept {
    bool published = false;
    if (auto hotResizeGuard = Cache_.HotResize_.TryLock()) {
        if (Cache_.HotResize_.Phase() == EHotResizePhase::Cut) {
            auto spaceOp = Cache_.Space_->BeginOperation();
            Cache_.Eviction_.BindOperation(spaceOp).DrainHotResize();
        }
        if (Cache_.HotResize_.Phase() != EHotResizePhase::Idle) {
            return false;
        }
        published = Cache_.Space_->PublishFinalView(transition);
    }
    if (published && transition.TargetConfiguration().HandleCount() > transition.OldConfiguration().HandleCount()) {
        Cache_.NotifyResourcesAvailable();
    }
    return published;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::AppendFreeHandles(TTransition& transition) noexcept {
    if (!Cache_.Space_->AppendFreeHandles(transition)) {
        return false;
    }
    Cache_.NotifyResourcesAvailable();
    return true;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::StartTransition() noexcept {
    if (Transition_.Phase() != ETransitionPhase::Idle) {
        return false;
    }

    const TSharedCacheCapacity& current = Cache_.Space_->CurrentConfiguration();
    if (current.AddressBits == Target_.AddressBits) {
        return false;
    }

    const ui8 nextAddressBits =
        current.AddressBits < Target_.AddressBits ? current.AddressBits + 1 : current.AddressBits - 1;
    TSharedCacheCapacity next;
    if (!TryCalculateSharedCacheFootprint(
            nextAddressBits, current.ExpectedPageSize, current.FixedBytes, Cache_.Space_->HazardCount(), next))
    {
        return false;
    }
    next.Limit = Cache_.Memory_.HardLimit();
    return PrepareTransition(next, Transition_);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::FinalDrain(TTransition& transition) noexcept {
    auto spaceOp = Cache_.Space_->BeginOperation();
    auto evictionOp = Cache_.Eviction_.BindOperation(spaceOp);
    return Cache_.Space_->FinalDrain(
        transition,
        [&](ui32 level, ui64 raw) noexcept {
            evictionOp.RouteHot(static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
        },
        [&](ui64 raw) noexcept {
            Cache_.Memory_.RemoveColdRingEntry();
            evictionOp.RouteCold(TColdCacheItem::FromRaw(raw));
        },
        [&](ui64 raw) noexcept {
            evictionOp.RouteKeepCold(TColdCacheItem::FromRaw(raw));
        });
}

template <class TTraits>
bool TCacheCapacity<TTraits>::TryReleaseTransition(TTransition& transition) noexcept {
    return Cache_.Space_->TryReleaseTransition(transition);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::CommitTransition(TTransition& transition) noexcept {
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    const ui64 targetLimit = transition.TargetConfiguration().Limit;
    const ui64 targetHandleCount = transition.TargetConfiguration().HandleCount();
    const bool commitTargetLimit = &transition != &Transition_;
    if (!Cache_.Space_->CommitTransition(transition)) {
        return false;
    }

    Cache_.Memory_.CommitStaticResize(oldStaticBytes, targetStaticBytes);
    if (commitTargetLimit) {
        TargetHandleCount_.store(targetHandleCount, std::memory_order_relaxed);
        Cache_.Memory_.SetHardLimit(targetLimit);
    }
    Cache_.RefreshSoftLimit();
    return true;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::AbandonTransition(TTransition& transition) noexcept {
    const ETransitionPhase phase = transition.Phase();
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    if (!Cache_.Space_->AbandonTransition(transition)) {
        return false;
    }
    if ((phase == ETransitionPhase::Prepare || phase == ETransitionPhase::InitializeGrowth) &&
        targetStaticBytes > oldStaticBytes)
    {
        const ui64 delta = targetStaticBytes - oldStaticBytes;
        Cache_.Memory_.CancelStaticResize(delta);
    }
    return true;
}

template <class TTraits>
EShrinkCutStatus TCacheCapacity<TTraits>::ReclaimShrinkCut() noexcept {
    TTransition& transition = Transition_;
    const ui64 allocationLimit = transition.TargetConfiguration().HandleCount();
    const TCacheItem spare = Cache_.Space_->TakeSpareForShrink(allocationLimit);
    if (!spare.IsNull()) {
        auto spaceOp = Cache_.Space_->BeginOperation();
        Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(spare) && spare.Index() >= allocationLimit);
        THandle& handle = spaceOp.Handles()[spare.Index()];
        const THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(spare.Matches(expected) && expected.IsBegin() && expected.Refs() == 0);
        handle.State.store(expected.WithState(EHandleState::Free).Raw(), std::memory_order_release);
        return EShrinkCutStatus::Progress;
    }

    auto spaceOp = Cache_.Space_->BeginOperation();
    const ui64 handleCount = spaceOp.HandleCount();
    ui64& index = transition.NextWorkIndex_;
    if (index < allocationLimit || index > handleCount) {
        index = allocationLimit;
        transition.CutBlocked_ = false;
    }
    const ui64 end = Min(handleCount, index + SharedCacheTransitionWorkBatch);
    while (index < end) {
        switch (Cache_.ReclaimShrinkHandle(spaceOp, static_cast<ui32>(index))) {
            case EShrinkCutStatus::Progress:
                return EShrinkCutStatus::Progress;
            case EShrinkCutStatus::Blocked:
                transition.CutBlocked_ = true;
                break;
            case EShrinkCutStatus::Complete:
                break;
        }
        ++index;
    }
    if (index < handleCount) {
        return EShrinkCutStatus::Progress;
    }
    const bool blocked = transition.CutBlocked_;
    index = 0;
    transition.CutBlocked_ = false;
    return blocked ? EShrinkCutStatus::Blocked : EShrinkCutStatus::Complete;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::AdvanceBucketResize() noexcept {
    const TSpaceState spaceState = Cache_.Space_->CurrentSpaceState();
    if (!spaceState.Resizing()) {
        return false;
    }

    const TBucketResizeState resize = Cache_.Space_->BucketResizeState();
    auto spaceOp = Cache_.Space_->BeginOperation();
    switch (resize.Phase()) {
        case EBucketResizePhase::Prepared:
            return Cache_.Table_.PrepareBucketSplit(spaceOp) && Cache_.Table_.ActivateBucketSplitOrMerge(spaceOp);

        case EBucketResizePhase::Activated:
            return Cache_.Table_.PublishBucketSplit(spaceOp);

        case EBucketResizePhase::CursorPublished:
            if (resize.Direction() == EBucketResize::Shrinking) {
                return Cache_.Table_.CloseBucketSplit(spaceOp);
            }
            break;

        case EBucketResizePhase::Closed:
            break;

        case EBucketResizePhase::Stable:
            return false;
    }

    if (!Cache_.Table_.RemoveBucketSplit(spaceOp)) {
        return false;
    }
    return resize.Cursor() != resize.PairCount() || Cache_.Space_->FinishBucketResize();
}

template <class TTraits>
bool TCacheCapacity<TTraits>::RunMaintenance() noexcept {
    switch (Transition_.Phase()) {
        case ETransitionPhase::Idle:
            return StartTransition();

        case ETransitionPhase::Prepare:
        case ETransitionPhase::InitializeGrowth:
            return PublishMigrationView(Transition_);

        case ETransitionPhase::MigrationDrain:
            return TryDrainTransition(Transition_);

        case ETransitionPhase::Migrate: {
            const TSpaceState state = Cache_.Space_->CurrentSpaceState();
            if (state.Resizing()) {
                return AdvanceBucketResize();
            }
            const ui8 targetAddressBits = Transition_.TargetConfiguration().AddressBits;
            if (state.AddressBits() != targetAddressBits) {
                return Cache_.Space_->BeginBucketResize(targetAddressBits);
            }
            if (Transition_.OldConfiguration().AddressBits > targetAddressBits) {
                switch (ReclaimShrinkCut()) {
                    case EShrinkCutStatus::Blocked:
                        return false;
                    case EShrinkCutStatus::Progress:
                        return true;
                    case EShrinkCutStatus::Complete:
                        break;
                }
            }
            return PublishFinalView(Transition_);
        }

        case ETransitionPhase::AppendGrowth:
            return AppendFreeHandles(Transition_);

        case ETransitionPhase::FinalDrain:
            return TryDrainTransition(Transition_) && FinalDrain(Transition_);

        case ETransitionPhase::Release:
        case ETransitionPhase::ReleaseRetry:
            return TryReleaseTransition(Transition_);

        case ETransitionPhase::Commit:
            return CommitTransition(Transition_);

        case ETransitionPhase::PublishMigration:
        case ETransitionPhase::AbandonedForShutdown:
            return false;
    }
    Y_UNREACHABLE();
}

// Capacity component is instantiated here for both trait variants
template class TCacheCapacity<TProdTraits>;
template class TCacheCapacity<TTestTraits>;

} // namespace NKikimr::NSharedCache
