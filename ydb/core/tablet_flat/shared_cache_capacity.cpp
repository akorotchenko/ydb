#include "shared_cache_capacity.h"
#include "shared_cache.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
bool TCacheCapacityServices<TTraits>::AdvanceBucketResize() noexcept {
    return Cache_.AdvanceBucketResize();
}

template <class TTraits>
EShrinkCutStatus TCacheCapacityServices<TTraits>::ReclaimShrinkHandle(TSpaceOperation& spaceOp, ui32 index) noexcept {
    return Cache_.ReclaimShrinkHandle(spaceOp, index);
}

template <class TTraits>
void TCacheCapacityServices<TTraits>::RefreshSoftLimit() noexcept {
    Cache_.RefreshSoftLimit();
}

template <class TTraits>
void TCacheCapacity<TTraits>::SetTarget(const TSharedCacheCapacity& target) noexcept {
    Target_ = target;
    TargetHandleCount_.store(target.HandleCount(), std::memory_order_relaxed);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept {
    const ui64 currentStaticBytes = Space_.CurrentConfiguration().StaticBytes;
    const ui64 staticDeltaBytes = target.StaticBytes > currentStaticBytes ? target.StaticBytes - currentStaticBytes : 0;
    if (staticDeltaBytes != 0) {
        Memory_.PrepareStaticResize(staticDeltaBytes);
    }
    if (Space_.PrepareTransition(target, transition)) {
        return true;
    }
    if (staticDeltaBytes != 0) {
        Memory_.CancelStaticResize(staticDeltaBytes);
    }
    return false;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PublishMigrationView(TTransition& transition, THotResize& hotResize) noexcept {
    if (auto hotResizeGuard = hotResize.TryLock()) {
        return Space_.PublishMigrationView(transition);
    }
    return false;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::TryDrainTransition(TTransition& transition) noexcept {
    return Space_.TryDrainTransition(transition);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PublishFinalView(TTransition& transition, THotResize& hotResize) noexcept {
    if (auto hotResizeGuard = hotResize.TryLock()) {
        if (hotResize.Phase() == EHotResizePhase::Cut) {
            auto spaceOp = Space_.BeginOperation();
            Eviction_.BindOperation(spaceOp).DrainHotResize(Space_, hotResize);
        }
        if (hotResize.Phase() != EHotResizePhase::Idle) {
            return false;
        }
        return Space_.PublishFinalView(transition);
    }
    return false;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::AppendFreeHandles(TTransition& transition) noexcept {
    return Space_.AppendFreeHandles(transition);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::StartTransition() noexcept {
    if (Transition_.Phase() != ETransitionPhase::Idle) {
        return false;
    }

    const TSharedCacheCapacity& current = Space_.CurrentConfiguration();
    if (current.AddressBits == Target_.AddressBits) {
        return false;
    }

    const ui8 nextAddressBits =
        current.AddressBits < Target_.AddressBits ? current.AddressBits + 1 : current.AddressBits - 1;
    TSharedCacheCapacity next;
    if (!TryCalculateSharedCacheFootprint(
            nextAddressBits, current.ExpectedPageSize, current.FixedBytes, Space_.HazardCount(), next))
    {
        return false;
    }
    next.Limit = Memory_.HardLimit();
    return PrepareTransition(next, Transition_);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::FinalDrain(TTransition& transition) noexcept {
    auto spaceOp = Space_.BeginOperation();
    auto evictionOp = Eviction_.BindOperation(spaceOp);
    return Space_.FinalDrain(
        transition,
        [&](ui32 level, ui64 raw) noexcept {
            evictionOp.RouteHot(static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
        },
        [&](ui64 raw) noexcept {
            Memory_.RemoveColdRingEntry();
            evictionOp.RouteCold(TColdCacheItem::FromRaw(raw));
        },
        [&](ui64 raw) noexcept {
            evictionOp.RouteKeepCold(TColdCacheItem::FromRaw(raw));
        });
}

template <class TTraits>
bool TCacheCapacity<TTraits>::TryReleaseTransition(TTransition& transition) noexcept {
    return Space_.TryReleaseTransition(transition);
}

template <class TTraits>
bool TCacheCapacity<TTraits>::CommitTransition(TTransition& transition) noexcept {
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    const ui64 targetLimit = transition.TargetConfiguration().Limit;
    const ui64 targetHandleCount = transition.TargetConfiguration().HandleCount();
    const bool commitTargetLimit = &transition != &Transition_;
    if (!Space_.CommitTransition(transition)) {
        return false;
    }

    Memory_.CommitStaticResize(oldStaticBytes, targetStaticBytes);
    if (commitTargetLimit) {
        TargetHandleCount_.store(targetHandleCount, std::memory_order_relaxed);
        Memory_.SetHardLimit(targetLimit);
    }
    Services_.RefreshSoftLimit();
    return true;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::AbandonTransition(TTransition& transition) noexcept {
    const ETransitionPhase phase = transition.Phase();
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    if (!Space_.AbandonTransition(transition)) {
        return false;
    }
    if ((phase == ETransitionPhase::Prepare || phase == ETransitionPhase::InitializeGrowth) &&
        targetStaticBytes > oldStaticBytes)
    {
        const ui64 delta = targetStaticBytes - oldStaticBytes;
        Memory_.CancelStaticResize(delta);
    }
    return true;
}

template <class TTraits>
EShrinkCutStatus TCacheCapacity<TTraits>::ReclaimShrinkCut() noexcept {
    TTransition& transition = Transition_;
    const ui64 allocationLimit = transition.TargetConfiguration().HandleCount();
    const TCacheItem spare = Space_.TakeSpareForShrink(allocationLimit);
    if (!spare.IsNull()) {
        auto spaceOp = Space_.BeginOperation();
        Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(spare) && spare.Index() >= allocationLimit);
        THandle& handle = spaceOp.Handles()[spare.Index()];
        const THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(spare.Matches(expected) && expected.IsBegin() && expected.Refs() == 0);
        handle.State.store(expected.WithState(EHandleState::Free).Raw(), std::memory_order_release);
        return EShrinkCutStatus::Progress;
    }

    auto spaceOp = Space_.BeginOperation();
    const ui64 handleCount = spaceOp.HandleCount();
    ui64& index = transition.NextWorkIndex_;
    if (index < allocationLimit || index > handleCount) {
        index = allocationLimit;
        transition.CutBlocked_ = false;
    }
    const ui64 end = Min(handleCount, index + SharedCacheTransitionWorkBatch);
    while (index < end) {
        switch (Services_.ReclaimShrinkHandle(spaceOp, static_cast<ui32>(index))) {
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
bool TCacheCapacity<TTraits>::RunMaintenance(THotResize& hotResize) noexcept {
    switch (Transition_.Phase()) {
        case ETransitionPhase::Idle:
            return StartTransition();

        case ETransitionPhase::Prepare:
        case ETransitionPhase::InitializeGrowth:
            return PublishMigrationView(Transition_, hotResize);

        case ETransitionPhase::MigrationDrain:
            return TryDrainTransition(Transition_);

        case ETransitionPhase::Migrate: {
            const TSpaceState state = Space_.CurrentSpaceState();
            if (state.Resizing()) {
                return Services_.AdvanceBucketResize();
            }
            const ui8 targetAddressBits = Transition_.TargetConfiguration().AddressBits;
            if (state.AddressBits() != targetAddressBits) {
                return Space_.BeginBucketResize(targetAddressBits);
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
            return PublishFinalView(Transition_, hotResize);
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
template class TCacheCapacityServices<TProdTraits>;
template class TCacheCapacityServices<TTestTraits>;
template class TCacheCapacity<TProdTraits>;
template class TCacheCapacity<TTestTraits>;

} // namespace NKikimr::NSharedCache
