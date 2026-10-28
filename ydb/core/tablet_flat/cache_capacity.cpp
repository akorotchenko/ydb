#include "cache_capacity.h"
#include "cache.h"

#include <algorithm>

namespace NKikimr::NSharedCache {

template <class TTraits>
EPrepareResult TCacheCapacity<TTraits>::PrepareTransition(
    const TCacheCapacityConfig& target, TTransition& transition) noexcept {
    Y_ABORT_UNLESS(transition.Phase() == ETransitionPhase::Idle);
    auto hotResizeGuard = Cache_.HotResize_.TryLock();
    if (!hotResizeGuard || !Cache_.HotResize_.ClaimCapacityTransition(transition)) {
        return EPrepareResult::Rejected;
    }
    const ui64 currentStaticBytes = Cache_.Space_->CurrentConfiguration().StaticBytes;
    const ui64 staticDeltaBytes = target.StaticBytes > currentStaticBytes ? target.StaticBytes - currentStaticBytes : 0;
    if (staticDeltaBytes != 0) {
        Cache_.Memory_.PrepareStaticResize(staticDeltaBytes);
    }
    const EPrepareResult result = Cache_.Space_->PrepareTransition(target, transition);
    if (result != EPrepareResult::Rejected) {
        return result;
    }
    if (staticDeltaBytes != 0) {
        Cache_.Memory_.CancelStaticResize(staticDeltaBytes);
    }
    Cache_.HotResize_.ReleaseCapacityTransition(transition);
    return EPrepareResult::Rejected;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PrepareReceiver(TTransition& transition) noexcept {
    if (auto guard = Cache_.HotResize_.TryLock()) {
        if (Cache_.HotResize_.OwnsCapacityTransition(transition)) {
            return Cache_.Space_->PrepareReceiver(transition);
        }
    }
    return false;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PublishMigrationView(TTransition& transition) noexcept {
    if (auto hotResizeGuard = Cache_.HotResize_.TryLock()) {
        if (Cache_.HotResize_.OwnsCapacityTransition(transition)) {
            return Cache_.Space_->PublishMigrationView(transition);
        }
    }
    return false;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::PublishFinalView(TTransition& transition) noexcept {
    bool published = false;
    if (auto hotResizeGuard = Cache_.HotResize_.TryLock()) {
        if (!Cache_.HotResize_.OwnsCapacityTransition(transition)) {
            return false;
        }
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
    TierRebalanceRequested_.exchange(false, std::memory_order_acq_rel);
    const bool keepActive = Cache_.Eviction_.HasKeepWork();
    TargetKeepActive_.store(keepActive, std::memory_order_relaxed);
    const auto minimums = Cache_.Policy_.HotMinimums(keepActive);
    const auto& current = Cache_.Space_->CurrentConfiguration();
    const ui64 budget = Cache_.Eviction_.EvictableByteBudget();
    const double share =
        budget ? static_cast<double>(Min(budget, Cache_.Eviction_.KeepLimit())) / static_cast<double>(budget) : 0.0;
    const ui64 hot = Target_.HotSlotCount();
    const ui64 cold = Target_.HandleCount();
    const ui64 keepHot = std::clamp<ui64>(
        static_cast<ui64>(hot * share), minimums[ECacheTier::Keep], hot - minimums[ECacheTier::Regular]);
    const ui64 keepCold = std::clamp<ui64>(static_cast<ui64>(cold * share), 1, cold - 1);
    Target_.Tiers = { TCacheTierCapacity{ hot - keepHot, cold - keepCold }, TCacheTierCapacity{ keepHot, keepCold } };
    if (current.AddressBits == Target_.AddressBits) {
        return current.Tiers != Target_.Tiers && PrepareTransition(Target_, Transition_) == EPrepareResult::Prepared;
    }
    const bool growing = current.AddressBits < Target_.AddressBits;
    const ui8 nextBits = growing ? current.AddressBits + 1 : current.AddressBits - 1;
    TCacheCapacityConfig next;
    if (!TryCalculateSharedCacheFootprint(
            nextBits, current.ExpectedPageSize, current.FixedBytes, Cache_.Space_->HazardCount(), next)) {
        return false;
    }
    // Capacity changes stay monotone; the following same-address transfer adjusts the share
    const ui64 scaledHot =
        growing ? current.Tiers[ECacheTier::Keep].HotSlots * 2 : current.Tiers[ECacheTier::Keep].HotSlots / 2;
    const ui64 scaledCold =
        growing ? current.Tiers[ECacheTier::Keep].ColdSlots * 2 : current.Tiers[ECacheTier::Keep].ColdSlots / 2;
    const ui64 nextKeepHot = std::clamp<ui64>(scaledHot, HotLevelCount, next.HotSlotCount() - HotLevelCount);
    const ui64 nextKeepCold = std::clamp<ui64>(scaledCold, 1, next.HandleCount() - 1);
    next.Tiers = { TCacheTierCapacity{ next.HotSlotCount() - nextKeepHot, next.HandleCount() - nextKeepCold },
        TCacheTierCapacity{ nextKeepHot, nextKeepCold } };
    next.Limit = Cache_.Memory_.HardLimit();
    return PrepareTransition(next, Transition_) == EPrepareResult::Prepared;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::FinalDrain(TTransition& transition) noexcept {
    auto spaceOp = Cache_.Space_->BeginOperation();
    auto evictionOp = Cache_.Eviction_.BindOperation(spaceOp);
    return Cache_.Space_->FinalDrain(
        transition,
        [&](ECacheTier tier, EHotLevel level, ui64 raw) noexcept {
            evictionOp.RouteHot(tier, level, TCacheItem::FromRaw(raw));
        },
        [&](ECacheTier tier, ui64 raw) noexcept {
            spaceOp.View().Tiers[tier].Rings->RemoveColdEntry();
            evictionOp.RouteCold(tier, TColdCacheItem::FromRaw(raw));
        });
}

template <class TTraits>
bool TCacheCapacity<TTraits>::CommitTransition(TTransition& transition) noexcept {
    auto hotResizeGuard = Cache_.HotResize_.TryLock();
    if (!hotResizeGuard || !Cache_.HotResize_.OwnsCapacityTransition(transition)) {
        return false;
    }
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
    Cache_.HotResize_.ReleaseCapacityTransition(transition);
    hotResizeGuard = {};
    Cache_.RefreshSoftLimit();
    Cache_.NotifyResourcesAvailable();
    return true;
}

template <class TTraits>
bool TCacheCapacity<TTraits>::AbandonTransition(TTransition& transition) noexcept {
    auto hotResizeGuard = Cache_.HotResize_.TryLock();
    if (!hotResizeGuard || !Cache_.HotResize_.OwnsCapacityTransition(transition)) {
        return false;
    }
    const bool canCancel = transition.CanCancel();
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    if (!Cache_.Space_->AbandonTransition(transition)) {
        return false;
    }
    if (canCancel) {
        if (targetStaticBytes > oldStaticBytes) {
            const ui64 delta = targetStaticBytes - oldStaticBytes;
            Cache_.Memory_.CancelStaticResize(delta);
        }
        Cache_.HotResize_.ReleaseCapacityTransition(transition);
        hotResizeGuard = {};
        Cache_.NotifyResourcesAvailable();
    } else {
        Cache_.HotResize_.CloseCapacityTransition(transition);
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
    using EResizeStep = typename TCacheTableImpl<TTraits>::EResizeStep;
    const TSpaceState spaceState = Cache_.Space_->CurrentSpaceState();
    if (!spaceState.Resizing()) {
        return false;
    }

    const TBucketResizeState resize = Cache_.Space_->BucketResizeState();
    auto spaceOp = Cache_.Space_->BeginOperation();
    switch (resize.Phase()) {
        case EBucketResizePhase::Prepared: {
            const auto step = Cache_.Table_.PrepareBucketSplit(spaceOp);
            return step == EResizeStep::Complete
                       ? Cache_.Table_.ActivateBucketSplitOrMerge(spaceOp) != EResizeStep::Blocked
                       : step == EResizeStep::Retry;
        }

        case EBucketResizePhase::Activated:
            return Cache_.Table_.PublishBucketSplit(spaceOp) != EResizeStep::Blocked;

        case EBucketResizePhase::CursorPublished:
            if (resize.Direction() == EBucketResize::Shrinking) {
                return Cache_.Table_.CloseBucketSplit(spaceOp) != EResizeStep::Blocked;
            }
            break;

        case EBucketResizePhase::Closed:
            break;

        case EBucketResizePhase::Stable:
            return false;
    }

    const auto step = Cache_.Table_.RemoveBucketSplit(spaceOp);
    if (step != EResizeStep::Complete) {
        return step == EResizeStep::Retry;
    }
    return resize.Cursor() != resize.PairCount() || Cache_.Space_->FinishBucketResize();
}

template <class TTraits>
bool TCacheCapacity<TTraits>::RunMaintenance() noexcept {
    switch (Transition_.Phase()) {
        case ETransitionPhase::Idle:
            return StartTransition();

        case ETransitionPhase::CancelRetry:
            // OS cleanup retries yield to periodic GC instead of scheduling themselves
            AbandonTransition(Transition_);
            return false;

        case ETransitionPhase::PrepareReceiver:
        case ETransitionPhase::CancelReceiver:
            return PrepareReceiver(Transition_);

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
