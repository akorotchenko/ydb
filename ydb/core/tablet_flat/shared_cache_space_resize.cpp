#include "shared_cache_space.h"

#include <new>

namespace NKikimr::NSharedCache {

bool TTransition::CanCancel() const noexcept {
    return Step_ != EStep::Receiver &&
           (Phase_ == ETransitionPhase::Prepare || Phase_ == ETransitionPhase::InitializeGrowth ||
               Phase_ == ETransitionPhase::CancelRetry);
}

bool TTransition::IsGrowingStep() const noexcept {
    return Step_ == EStep::Receiver || TargetConfiguration_.HandleCount() > OldConfiguration_.HandleCount();
}

bool THotResize::ClaimCapacityTransition(const TTransition& transition) noexcept {
    Y_DEBUG_ABORT_UNLESS(State_.load(std::memory_order_relaxed) & Locked);
    if (CapacityAbandoned_ || CapacityTransition_ ||
        (Phase_ != EHotResizePhase::Idle && Phase_ != EHotResizePhase::Cut)) {
        return false;
    }
    CapacityTransition_ = &transition;
    return true;
}

bool THotResize::OwnsCapacityTransition(const TTransition& transition) const noexcept {
    Y_DEBUG_ABORT_UNLESS(State_.load(std::memory_order_relaxed) & Locked);
    return !CapacityAbandoned_ && CapacityTransition_ == &transition;
}

void THotResize::ReleaseCapacityTransition(const TTransition& transition) noexcept {
    Y_DEBUG_ABORT_UNLESS(State_.load(std::memory_order_relaxed) & Locked);
    Y_DEBUG_ABORT_UNLESS(OwnsCapacityTransition(transition));
    CapacityTransition_ = nullptr;
}

void THotResize::CloseCapacityTransition(const TTransition& transition) noexcept {
    Y_DEBUG_ABORT_UNLESS(State_.load(std::memory_order_relaxed) & Locked);
    Y_DEBUG_ABORT_UNLESS(OwnsCapacityTransition(transition));
    CapacityAbandoned_ = true;
}

bool THotResize::CanChangeTiers() const noexcept {
    Y_DEBUG_ABORT_UNLESS(State_.load(std::memory_order_relaxed) & Locked);
    return !CapacityAbandoned_ && !CapacityTransition_ && Phase_ == EHotResizePhase::Idle;
}

bool TSharedCacheSpace::BeginBucketResize(ui8 newAddressBits) noexcept {
    const TSpaceState spaceState = CurrentSpaceState();
    if (spaceState.Resizing()) {
        return false;
    }

    const ui8 oldAddressBits = spaceState.AddressBits();
    EBucketResize direction;
    if (newAddressBits == oldAddressBits + 1 && newAddressBits <= ReservedCapacity_.AddressBits &&
        SharedCacheBucketCount(newAddressBits) <= NewestView().BucketsBytes / sizeof(std::atomic<ui64>))
    {
        direction = EBucketResize::Growing;
    } else if (newAddressBits + 1 == oldAddressBits) {
        direction = EBucketResize::Shrinking;
    } else {
        return false;
    }

    if (direction == EBucketResize::Growing) {
        std::atomic<ui64>* buckets = NewestView().Buckets;
        const ui64 oldBucketCount = SharedCacheBucketCount(oldAddressBits);
        const ui64 newBucketCount = SharedCacheBucketCount(newAddressBits);
        for (ui64 index = oldBucketCount; index < newBucketCount; ++index) {
            new (&buckets[index]) std::atomic<ui64>(0);
        }
    }

    BucketResizeState_.store(TBucketResizeState::Make(direction, EBucketResizePhase::Prepared, oldAddressBits).Raw(),
        std::memory_order_relaxed);
    SpaceState_.store(spaceState.BeginResize().Raw(), std::memory_order_release);
    return true;
}

bool TSharedCacheSpace::AdvanceBucketResizePhase(EBucketResizePhase phase) noexcept {
    Y_ABORT_UNLESS(phase != EBucketResizePhase::Stable);
    if (!CurrentSpaceState().Resizing()) {
        return false;
    }
    const TBucketResizeState state = TBucketResizeState::FromRaw(BucketResizeState_.load(std::memory_order_acquire));
    if (!state || static_cast<ui8>(phase) < static_cast<ui8>(state.Phase())) {
        return false;
    }
    BucketResizeState_.store(state.WithPhase(phase).Raw(), std::memory_order_release);
    return true;
}

bool TSharedCacheSpace::PublishBucketResizeCursor(ui32 cursor) noexcept {
    if (!CurrentSpaceState().Resizing()) {
        return false;
    }
    const TBucketResizeState state = TBucketResizeState::FromRaw(BucketResizeState_.load(std::memory_order_acquire));
    if (!state || state.Phase() != EBucketResizePhase::Activated || cursor > state.PairCount() ||
        cursor < state.Cursor())
    {
        return false;
    }
    BucketResizeState_.store(
        state.WithCursorAndPhase(cursor, EBucketResizePhase::CursorPublished).Raw(), std::memory_order_release);
    return true;
}

bool TSharedCacheSpace::PrepareNextBucketResizePair() noexcept {
    if (!CurrentSpaceState().Resizing()) {
        return false;
    }
    const TBucketResizeState state = TBucketResizeState::FromRaw(BucketResizeState_.load(std::memory_order_acquire));
    if (!state || state.Cursor() >= state.PairCount() ||
        (state.Phase() != EBucketResizePhase::CursorPublished && state.Phase() != EBucketResizePhase::Closed))
    {
        return false;
    }
    BucketResizeState_.store(state.WithPhase(EBucketResizePhase::Prepared).Raw(), std::memory_order_release);
    return true;
}

bool TSharedCacheSpace::FinishBucketResize() noexcept {
    const TSpaceState spaceState = CurrentSpaceState();
    if (!spaceState.Resizing()) {
        return false;
    }
    const TBucketResizeState state = TBucketResizeState::FromRaw(BucketResizeState_.load(std::memory_order_acquire));
    const ui32 pairCount = state.PairCount();
    if (!state || state.Cursor() != pairCount ||
        (state.Direction() == EBucketResize::Shrinking && state.Phase() != EBucketResizePhase::Closed) ||
        (state.Direction() == EBucketResize::Growing && state.Phase() != EBucketResizePhase::CursorPublished))
    {
        return false;
    }

    SpaceState_.store(spaceState.WithAddressBits(state.NewAddressBits()).Raw(), std::memory_order_release);
    return true;
}

bool TSharedCacheSpace::AdvanceSpaceGeneration() noexcept {
    const TSpaceState current = CurrentSpaceState();
    Y_ABORT_UNLESS(current.Generation() < MaxSpaceGeneration);
    const TSpaceState next = current.WithGeneration(current.Generation() + 1);
    PrepareView(next.Generation(), SpaceView(current));
    SpaceState_.store(next.Raw(), std::memory_order_release);
    return true;
}

bool TSharedCacheSpace::BeginHotResize(ui32 targetEffectiveHotSlots, THotResize& resize) noexcept {
    const ui64 currentEffectiveHotSlots = EffectiveHotSlots();
    const ui64 maximumHotSlots = SpaceView(CurrentSpaceState()).Tiers[ECacheTier::Regular].HotStorage.SlotCount;
    if (resize.Phase_ != EHotResizePhase::Idle || !SharedCacheHotLayoutIsValid(targetEffectiveHotSlots) ||
        targetEffectiveHotSlots > maximumHotSlots || targetEffectiveHotSlots == currentEffectiveHotSlots)
    {
        return false;
    }

    resize.OldEffectiveHotSlots_ = currentEffectiveHotSlots;
    resize.NextCutSlot_ = targetEffectiveHotSlots;
    EffectiveHotSlots_[ECacheTier::Regular].store(targetEffectiveHotSlots, std::memory_order_release);
    if (targetEffectiveHotSlots < currentEffectiveHotSlots) {
        resize.Phase_ = EHotResizePhase::Cut;
    } else {
        resize.Reset();
    }
    return true;
}

bool TSharedCacheSpace::DrainHotResize(THotResize& resize, ui32& level, ui64& raw) noexcept {
    if (resize.Phase_ != EHotResizePhase::Cut || resize.NextCutSlot_ >= resize.OldEffectiveHotSlots_)
    {
        return false;
    }

    const ui64 slot = resize.NextCutSlot_++;
    level = HotLevelForSlot(slot, resize.OldEffectiveHotSlots_);
    raw = NewestView().Tiers[ECacheTier::Regular].HotStorage.Slots[slot].exchange(0, std::memory_order_acq_rel);
    if (resize.NextCutSlot_ == resize.OldEffectiveHotSlots_) {
        resize.Reset();
    }
    return true;
}

EPrepareResult TSharedCacheSpace::PrepareTransition(
    const TSharedCacheCapacity& target, TTransition& transition) noexcept {
    return PrepareTransitionImpl<false>(target, transition, {});
}

EPrepareResult TSharedCacheSpace::PrepareTransitionWithHook(
    const TSharedCacheCapacity& target, TTransition& transition, TPrepareHook hook) noexcept {
    return PrepareTransitionImpl<true>(target, transition, hook);
}

template <bool WithHook>
EPrepareResult TSharedCacheSpace::PrepareTransitionImpl(
    const TSharedCacheCapacity& target, TTransition& transition, TPrepareHook hook) noexcept {
    Y_ABORT_UNLESS(transition.Phase_ == ETransitionPhase::Idle);
    const TSpaceState spaceState = CurrentSpaceState();
    const TSpaceView& current = SpaceView(spaceState);
    if (target.AddressBits < MinSharedCacheAddressBits || target.AddressBits > MaxSharedCacheAddressBits ||
        target.AddressBits > ReservedCapacity_.AddressBits ||
        target.ExpectedPageSize != ReservedCapacity_.ExpectedPageSize ||
        target.StaticBytes > ReservedCapacity_.StaticBytes || current.HandleCount != Capacity_.HandleCount() ||
        current.AllocationLimit != Capacity_.HandleCount() || spaceState.Resizing() ||
        spaceState.AddressBits() != Capacity_.AddressBits || !target.HasValidTierCapacity())
    {
        return EPrepareResult::Rejected;
    }

    const bool transfer = target.AddressBits == Capacity_.AddressBits;
    const ui32 generations = transfer ? 4 : 2;
    if ((transfer && (target.Tiers == Capacity_.Tiers || target.StaticBytes != Capacity_.StaticBytes)) ||
        spaceState.Generation() > MaxSpaceGeneration - generations) {
        return EPrepareResult::Rejected;
    }

    const bool growing = target.HandleCount() > Capacity_.HandleCount();
    for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
        const auto& old = Capacity_.Tiers[tier];
        const auto& next = target.Tiers[tier];
        // Share transfers cut donors first; ordinary capacity changes remain monotone
        if ((old.HotSlots == 0) != (next.HotSlots == 0) ||
            (!transfer && (growing ? next.HotSlots < old.HotSlots || next.ColdSlots < old.ColdSlots
                                   : next.HotSlots > old.HotSlots || next.ColdSlots > old.ColdSlots))) {
            return EPrepareResult::Rejected;
        }
    }

    // Save ownership and intended extents before any mapping can leave partial work
    transition.PreparedView_ = MakeHolder<TSpaceView>();
    auto& view = *transition.PreparedView_;
    SetTierSlotCounts(view, target);
    transition.Step_ = transfer ? TTransition::EStep::Donor : TTransition::EStep::Capacity;
    if (transfer) {
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            view.Tiers[tier].HotStorage.SlotCount = Min(Capacity_.Tiers[tier].HotSlots, target.Tiers[tier].HotSlots);
            view.Tiers[tier].ColdStorage.SlotCount = Min(Capacity_.Tiers[tier].ColdSlots, target.Tiers[tier].ColdSlots);
        }
    }
    transition.Phase_ = ETransitionPhase::Prepare;
    transition.Generation_ = spaceState.Generation();
    transition.OldConfiguration_ = Capacity_;
    transition.TargetConfiguration_ = target;
    transition.ResizeDeltaBits_ = Capacity_.AddressBits < target.AddressBits
                                      ? target.AddressBits - Capacity_.AddressBits
                                      : Capacity_.AddressBits - target.AddressBits;
    transition.ReleaseStage_ = ESpaceMap::Handles;
    if (!PrepareMappings<WithHook>(transition, hook)) {
        transition.Phase_ = ETransitionPhase::CancelRetry;
        if (CancelPreparedView(transition)) {
            transition.Reset();
            return EPrepareResult::Rejected;
        }
        return EPrepareResult::CleanupPending;
    }
    transition.ReleaseStage_ = ESpaceMap::Done;
    return EPrepareResult::Prepared;
}

template <bool WithHook>
bool TSharedCacheSpace::PrepareMappings(TTransition& transition, TPrepareHook hook) noexcept {
    const auto& target = transition.TargetConfiguration_;
    auto& view = *transition.PreparedView_;
    std::array<size_t, static_cast<size_t>(ESpaceMap::Done)> sizes;
    if (!TryMappingSize(target.HandleCount(), sizeof(THandle), sizes[static_cast<size_t>(ESpaceMap::Handles)]) ||
        !TryMappingSize(
            target.BucketCount(), sizeof(std::atomic<ui64>), sizes[static_cast<size_t>(ESpaceMap::Buckets)]) ||
        !TryMappingSize(target.HandleCount(), sizeof(std::atomic<ui64>), sizes[static_cast<size_t>(ESpaceMap::Free)]))
    {
        return false;
    }

    for (auto mapping : { ESpaceMap::RegularHot, ESpaceMap::RegularCold, ESpaceMap::KeepHot, ESpaceMap::KeepCold }) {
        const auto& ring = RingStorage(view, mapping);
        if (!TryMappingSize(ring.SlotCount, sizeof(std::atomic<ui64>), sizes[static_cast<size_t>(mapping)])) {
            return false;
        }
    }

    const auto prepare = [&](ESpaceMap mapping, TSharedCacheMapping& owner, size_t& bytes, auto*& slots) noexcept {
        if constexpr (WithHook) {
            if (!hook.Run(mapping)) {
                return false;
            }
        }
        bytes = sizes[static_cast<size_t>(mapping)];
        return owner.Prepare(bytes, slots);
    };
    bool prepared = prepare(ESpaceMap::Handles, HandlesMapping_, view.HandlesBytes, view.Handles) &&
                    prepare(ESpaceMap::Buckets, BucketsMapping_, view.BucketsBytes, view.Buckets);
    for (auto mapping : { ESpaceMap::RegularHot, ESpaceMap::RegularCold, ESpaceMap::KeepHot, ESpaceMap::KeepCold }) {
        auto& ring = RingStorage(view, mapping);
        if (!prepared) {
            break;
        }
        if (ring.SlotCount != 0) {
            prepared = prepare(mapping, RingMapping(mapping), ring.Bytes, ring.Slots);
        }
    }
    if (!prepared || !prepare(ESpaceMap::Free, FreeMapping_, view.FreeSlotBytes, view.FreeSlots)) {
        return false;
    }
    return true;
}

bool TSharedCacheSpace::PrepareReceiver(TTransition& transition) noexcept {
    return PrepareReceiverImpl<false>(transition, {});
}

bool TSharedCacheSpace::PrepareReceiverWithHook(TTransition& transition, TPrepareHook hook) noexcept {
    return PrepareReceiverImpl<true>(transition, hook);
}

template <bool WithHook>
bool TSharedCacheSpace::PrepareReceiverImpl(TTransition& transition, TPrepareHook hook) noexcept {
    if (transition.Step_ != TTransition::EStep::Receiver) {
        return false;
    }
    if (transition.Phase_ == ETransitionPhase::CancelReceiver) {
        if (CancelPreparedView(transition)) {
            transition.Phase_ = ETransitionPhase::PrepareReceiver;
        }
        // Completed cleanup still yields before another private attempt
        return false;
    }
    if (transition.Phase_ != ETransitionPhase::PrepareReceiver) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(!transition.OldView_ && !transition.PreparedView_);
    transition.PreparedView_ = MakeHolder<TSpaceView>();
    SetTierSlotCounts(*transition.PreparedView_, transition.TargetConfiguration_);
    transition.ReleaseStage_ = ESpaceMap::Handles;
    if (!PrepareMappings<WithHook>(transition, hook)) {
        transition.Phase_ = ETransitionPhase::CancelReceiver;
        if (CancelPreparedView(transition)) {
            transition.Phase_ = ETransitionPhase::PrepareReceiver;
        }
        return false;
    }
    transition.ReleaseStage_ = ESpaceMap::Done;
    transition.NextWorkIndex_ = 0;
    transition.Phase_ = ETransitionPhase::Prepare;
    return true;
}

bool TSharedCacheSpace::CancelPreparedView(TTransition& transition) noexcept {
    auto& view = *transition.PreparedView_;
    const auto cancel = [](TSharedCacheMapping& owner, auto*& pointer, size_t bytes) noexcept {
        return bytes == 0 || owner.Cancel(pointer, bytes);
    };
    while (transition.ReleaseStage_ != ESpaceMap::Done) {
        bool complete = false;
        switch (transition.ReleaseStage_) {
            case ESpaceMap::Handles:
                complete = cancel(HandlesMapping_, view.Handles, view.HandlesBytes);
                break;
            case ESpaceMap::Buckets:
                complete = cancel(BucketsMapping_, view.Buckets, view.BucketsBytes);
                break;
            case ESpaceMap::RegularHot:
            case ESpaceMap::RegularCold:
            case ESpaceMap::KeepHot:
            case ESpaceMap::KeepCold: {
                auto& ring = RingStorage(view, transition.ReleaseStage_);
                complete = cancel(RingMapping(transition.ReleaseStage_), ring.Slots, ring.Bytes);
                break;
            }
            case ESpaceMap::Free:
                complete = cancel(FreeMapping_, view.FreeSlots, view.FreeSlotBytes);
                break;
            case ESpaceMap::Done:
                Y_UNREACHABLE();
        }
        if (!complete) {
            return false;
        }
        transition.ReleaseStage_ = static_cast<ESpaceMap>(static_cast<ui8>(transition.ReleaseStage_) + 1);
    }
    transition.PreparedView_.Reset();
    return true;
}

void TSharedCacheSpace::PublishPreparedView(THolder<TSpaceView> view, THolder<TSpaceView>& oldView) noexcept {
    Y_DEBUG_ABORT_UNLESS(view);
    Y_DEBUG_ABORT_UNLESS(view->Handles == Handles_);
    oldView = std::move(NewestViewOwner_);
    HandlesMapping_.Publish(view->Handles, view->HandlesBytes);
    BucketsMapping_.Publish(view->Buckets, view->BucketsBytes);
    for (ui32 index = 0; index < CacheTierCount; ++index) {
        const auto& tier = view->Tiers[index];
        if (tier.HotStorage.Slots) {
            TierStorage_[index].Hot.Publish(tier.HotStorage.Slots, tier.HotStorage.Bytes);
        }
        if (tier.ColdStorage.Slots) {
            TierStorage_[index].Cold.Publish(tier.ColdStorage.Slots, tier.ColdStorage.Bytes);
        }
    }
    FreeMapping_.Publish(view->FreeSlots, view->FreeSlotBytes);
    NewestViewOwner_ = std::move(view);
    NewestView_.store(NewestViewOwner_.Get(), std::memory_order_release);
}

bool TSharedCacheSpace::InitializeGrowth(TTransition& transition) noexcept {
    Y_DEBUG_ABORT_UNLESS(transition.PreparedView_);
    TSpaceView& view = *transition.PreparedView_;
    const TSharedCacheCapacity& old = transition.OldConfiguration_;
    const TSharedCacheCapacity& target = transition.TargetConfiguration_;
    const TSpaceView& source = NewestView();
    const ui64 handleDelta = target.HandleCount() - old.HandleCount();
    ui64 work = handleDelta;
    for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
        work = Max(work, view.Tiers[tier].HotStorage.SlotCount - source.Tiers[tier].HotStorage.SlotCount);
        work = Max(work, view.Tiers[tier].ColdStorage.SlotCount - source.Tiers[tier].ColdStorage.SlotCount);
    }
    const ui64 end = Min(work, transition.NextWorkIndex_ + SharedCacheTransitionWorkBatch);
    while (transition.NextWorkIndex_ < end) {
        const ui64 offset = transition.NextWorkIndex_++;
        if (offset < handleDelta) {
            const ui64 handleIndex = old.HandleCount() + offset;
            new (&view.Handles[handleIndex]) THandle();
            new (&view.Buckets[handleIndex]) std::atomic<ui64>(0);
            new (&view.FreeSlots[handleIndex]) std::atomic<ui64>(0);
        }
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            const auto& before = source.Tiers[tier];
            const auto& after = view.Tiers[tier];
            if (offset < after.HotStorage.SlotCount - before.HotStorage.SlotCount) {
                new (&view.Tiers[tier].HotStorage.Slots[before.HotStorage.SlotCount + offset]) std::atomic<ui64>(0);
            }
            if (offset < after.ColdStorage.SlotCount - before.ColdStorage.SlotCount) {
                new (&view.Tiers[tier].ColdStorage.Slots[before.ColdStorage.SlotCount + offset]) std::atomic<ui64>(0);
            }
        }
    }
    const bool complete = transition.NextWorkIndex_ == work;
    if (complete) {
        transition.NextWorkIndex_ = 0;
    }
    return complete;
}

bool TSharedCacheSpace::AppendFreeHandles(TTransition& transition) noexcept {
    if (transition.Phase_ != ETransitionPhase::AppendGrowth ||
        transition.Generation_ != CurrentSpaceState().Generation())
    {
        return false;
    }

    const TSpaceView& view = SpaceView(CurrentSpaceState());
    const ui64 oldCount = transition.OldConfiguration_.HandleCount();
    const ui64 targetCount = transition.TargetConfiguration_.HandleCount();
    Y_DEBUG_ABORT_UNLESS(oldCount < targetCount && view.HandleCount == targetCount);
    const ui64 end = Min(targetCount, transition.NextWorkIndex_ + SharedCacheTransitionWorkBatch);
    while (transition.NextWorkIndex_ < end) {
        const ui64 index = transition.NextWorkIndex_++;
        RouteFreeWord(view, TCacheItem::Make(0, static_cast<ui32>(index)).Raw());
    }
    if (transition.NextWorkIndex_ == targetCount) {
        transition.NextWorkIndex_ = 0;
        transition.FinalDrainStage_ = ESpaceMap::Done;
        transition.Phase_ = ETransitionPhase::FinalDrain;
    }
    return true;
}

void TSharedCacheSpace::ResetHazardScan(TTransition& transition) noexcept {
    transition.NextHazardIndex_ = 0;
}

bool TSharedCacheSpace::PublishMigrationView(TTransition& transition) noexcept {
    return PublishMigrationViewImpl<false>(transition, {});
}

template <bool WithHook>
bool TSharedCacheSpace::PublishMigrationViewImpl(TTransition& transition, TPublishMigrationViewHook hook) noexcept {
    const TSpaceState expectedState = CurrentSpaceState();
    if ((transition.Phase_ != ETransitionPhase::Prepare && transition.Phase_ != ETransitionPhase::InitializeGrowth) ||
        expectedState.Resizing() || expectedState.AddressBits() != transition.OldConfiguration_.AddressBits ||
        expectedState.Generation() == MaxSpaceGeneration)
    {
        return false;
    }

    const bool growing = transition.IsGrowingStep();
    if (transition.Phase_ == ETransitionPhase::Prepare) {
        transition.Phase_ = growing ? ETransitionPhase::InitializeGrowth : ETransitionPhase::PublishMigration;
    }
    if (transition.Phase_ == ETransitionPhase::InitializeGrowth) {
        if (!InitializeGrowth(transition)) {
            return true;
        }
        transition.Phase_ = ETransitionPhase::PublishMigration;
    }
    if (growing) {
        PublishPreparedView(std::move(transition.PreparedView_), transition.OldView_);
    }

    const TSpaceState nextState = expectedState.WithGeneration(expectedState.Generation() + 1);
    const TSpaceView& current = SpaceView(expectedState);
    TSpaceView nextView = current;
    nextView.CopyMappings(NewestView());
    nextView.AllocationLimit = growing ? current.AllocationLimit : transition.TargetConfiguration_.HandleCount();
    PrepareView(nextState.Generation(), nextView);
    if constexpr (WithHook) {
        hook.Run();
    }

    SpaceState_.store(nextState.Raw(), std::memory_order_release);
    transition.Generation_ = nextState.Generation();
    ResetHazardScan(transition);
    transition.Phase_ = ETransitionPhase::MigrationDrain;
    return true;
}

bool TSharedCacheSpace::PublishMigrationViewWithHook(TTransition& transition, TPublishMigrationViewHook hook) noexcept {
    return PublishMigrationViewImpl<true>(transition, hook);
}

bool TSharedCacheSpace::TryDrainTransition(TTransition& transition) noexcept {
    if ((transition.Phase_ != ETransitionPhase::MigrationDrain && transition.Phase_ != ETransitionPhase::FinalDrain) ||
        transition.Generation_ != CurrentSpaceState().Generation())
    {
        return false;
    }

    if (!TryDrainBefore(transition.Generation_, transition.NextHazardIndex_)) {
        return false;
    }
    if (transition.Phase_ == ETransitionPhase::MigrationDrain) {
        transition.Phase_ = ETransitionPhase::Migrate;
    }
    return true;
}

bool TSharedCacheSpace::TryDrainBefore(ui32 publishedGeneration, ui32& nextHazardIndex) noexcept {
    while (nextHazardIndex < HazardSlotCount_) {
        // Pair with registration CAS so late readers observe the published generation.
        auto& state = Hazards_[nextHazardIndex]->State;
        ui64 hazardState = state.fetch_add(0, std::memory_order_acq_rel);
        if (!SpaceHazardIsDrained(hazardState, publishedGeneration)) {
            if (!state.compare_exchange_strong(hazardState, hazardState | SpaceHazardDrainWaiter,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                continue;
            }
            return false;
        }
        ++nextHazardIndex;
    }
    return true;
}

bool TSharedCacheSpace::PublishFinalView(TTransition& transition) noexcept {
    const TSpaceState expectedState = CurrentSpaceState();
    if (transition.Phase_ != ETransitionPhase::Migrate || transition.NextHazardIndex_ != HazardSlotCount_ ||
        expectedState.Resizing() || expectedState.AddressBits() != transition.TargetConfiguration_.AddressBits ||
        expectedState.Generation() == MaxSpaceGeneration)
    {
        return false;
    }

    const bool growing = transition.IsGrowingStep();
    if (!growing) {
        PublishPreparedView(std::move(transition.PreparedView_), transition.OldView_);
    }

    const TSpaceState retired = expectedState.WithGeneration(expectedState.Generation() + 1);
    TSpaceView retiredView = SpaceView(expectedState);
    retiredView.CopyMappings(NewestView());
    retiredView.HandleCount = transition.TargetConfiguration_.HandleCount();
    retiredView.AllocationLimit = transition.TargetConfiguration_.HandleCount();
    for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
        retiredView.Tiers[tier].HotStorage.SlotCount = NewestView().Tiers[tier].HotStorage.SlotCount;
        retiredView.Tiers[tier].ColdStorage.SlotCount = NewestView().Tiers[tier].ColdStorage.SlotCount;
    }
    PrepareView(retired.Generation(), retiredView);

    for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
        const ui64 current = EffectiveHotSlots_[tier].load(std::memory_order_acquire);
        const ui64 effective = Min(current, retiredView.Tiers[tier].HotStorage.SlotCount);
        if (effective != current) {
            EffectiveHotSlots_[tier].store(effective, std::memory_order_release);
        }
    }
    SpaceState_.store(retired.Raw(), std::memory_order_release);
    transition.Generation_ = retired.Generation();
    transition.ReleaseStage_ = ESpaceMap::Handles;
    ResetHazardScan(transition);
    if (growing && transition.Step_ == TTransition::EStep::Capacity) {
        transition.NextWorkIndex_ = transition.OldConfiguration_.HandleCount();
        transition.FinalDrainStage_ = ESpaceMap::Done;
        transition.Phase_ = ETransitionPhase::AppendGrowth;
        return AppendFreeHandles(transition);
    }

    transition.NextWorkIndex_ = retiredView.Tiers[ECacheTier::Regular].HotStorage.SlotCount;
    transition.FinalDrainStage_ = growing ? ESpaceMap::Done : ESpaceMap::RegularHot;
    transition.Phase_ = ETransitionPhase::FinalDrain;
    return true;
}

template <bool WithHook>
bool TSharedCacheSpace::TryReleaseOldMapping(ESpaceMap mapping, TTransition& transition, TReleaseHook hook) noexcept {
    Y_DEBUG_ABORT_UNLESS(transition.OldView_);
    TSpaceView& old = *transition.OldView_;
    const TSpaceView& newest = *NewestViewOwner_;
    TSharedCacheMapping* owner;
    void* oldPointer;
    size_t oldSize;
    void* currentPointer;
    size_t currentSize;
    switch (mapping) {
        case ESpaceMap::Handles:
            owner = &HandlesMapping_;
            oldPointer = old.Handles;
            oldSize = old.HandlesBytes;
            currentPointer = newest.Handles;
            currentSize = newest.HandlesBytes;
            break;
        case ESpaceMap::Buckets:
            owner = &BucketsMapping_;
            oldPointer = old.Buckets;
            oldSize = old.BucketsBytes;
            currentPointer = newest.Buckets;
            currentSize = newest.BucketsBytes;
            break;
        case ESpaceMap::RegularHot:
        case ESpaceMap::RegularCold:
        case ESpaceMap::KeepHot:
        case ESpaceMap::KeepCold: {
            auto& oldRing = RingStorage(old, mapping);
            const auto& currentRing = RingStorage(*NewestViewOwner_, mapping);
            if (oldRing.Bytes == 0 && currentRing.Bytes == 0) {
                return true;
            }
            owner = &RingMapping(mapping);
            oldPointer = oldRing.Slots;
            oldSize = oldRing.Bytes;
            currentPointer = currentRing.Slots;
            currentSize = currentRing.Bytes;
            break;
        }
        case ESpaceMap::Free:
            owner = &FreeMapping_;
            oldPointer = old.FreeSlots;
            oldSize = old.FreeSlotBytes;
            currentPointer = newest.FreeSlots;
            currentSize = newest.FreeSlotBytes;
            break;
        case ESpaceMap::Done:
            Y_ABORT("Invalid shared-cache release stage");
    }

    if (!owner->NeedsRelease(oldPointer, oldSize, currentPointer, currentSize)) {
        return true;
    }
    if constexpr (WithHook) {
        const void* begin;
        size_t size;
        owner->ReleaseRange(oldPointer, oldSize, currentPointer, currentSize, begin, size);
        if (!hook.Run(mapping, begin, size)) {
            return false;
        }
    }
    const bool released = owner->Release(oldPointer, oldSize, currentPointer, currentSize);

    // Release may unmap the old view before failing to shrink its backing; preserve that progress for retry.
    switch (mapping) {
        case ESpaceMap::Handles:
            old.Handles = static_cast<THandle*>(oldPointer);
            break;
        case ESpaceMap::Buckets:
            old.Buckets = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::RegularHot:
        case ESpaceMap::RegularCold:
        case ESpaceMap::KeepHot:
        case ESpaceMap::KeepCold:
            RingStorage(old, mapping).Slots = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::Free:
            old.FreeSlots = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::Done:
            Y_ABORT("Invalid shared-cache release stage");
    }
    return released;
}

bool TSharedCacheSpace::TryReleaseTransition(TTransition& transition) noexcept {
    return TryReleaseTransitionImpl<false>(transition, {});
}

template <bool WithHook>
bool TSharedCacheSpace::TryReleaseTransitionImpl(TTransition& transition, TReleaseHook hook) noexcept {
    if ((transition.Phase_ != ETransitionPhase::Release && transition.Phase_ != ETransitionPhase::ReleaseRetry) ||
        transition.NextHazardIndex_ != HazardSlotCount_)
    {
        return false;
    }

    while (transition.ReleaseStage_ != ESpaceMap::Done) {
        if (!TryReleaseOldMapping<WithHook>(transition.ReleaseStage_, transition, hook)) {
            transition.Phase_ = ETransitionPhase::ReleaseRetry;
            return false;
        }
        transition.ReleaseStage_ = static_cast<ESpaceMap>(static_cast<ui8>(transition.ReleaseStage_) + 1);
    }

    transition.OldView_.Reset();
    if (transition.Step_ == TTransition::EStep::Donor) {
        transition.Step_ = TTransition::EStep::Receiver;
        transition.Phase_ = ETransitionPhase::PrepareReceiver;
    } else {
        transition.Phase_ = ETransitionPhase::Commit;
    }
    return true;
}

bool TSharedCacheSpace::TryReleaseTransitionWithHook(TTransition& transition, TReleaseHook hook) noexcept {
    return TryReleaseTransitionImpl<true>(transition, hook);
}

bool TSharedCacheSpace::CommitTransition(TTransition& transition) noexcept {
    if (transition.Phase_ != ETransitionPhase::Commit || transition.ReleaseStage_ != ESpaceMap::Done)
    {
        return false;
    }
    Capacity_ = transition.TargetConfiguration_;
    transition.Reset();
    return true;
}

bool TSharedCacheSpace::AbandonTransition(TTransition& transition) noexcept {
    if (transition.Phase_ == ETransitionPhase::Idle || transition.Phase_ == ETransitionPhase::AbandonedForShutdown)
    {
        return false;
    }
    if (transition.CanCancel()) {
        if (transition.Phase_ != ETransitionPhase::CancelRetry) {
            transition.ReleaseStage_ = ESpaceMap::Handles;
            transition.Phase_ = ETransitionPhase::CancelRetry;
        }
        if (!CancelPreparedView(transition)) {
            return false;
        }
        transition.Reset();
        return true;
    }
    transition.Phase_ = ETransitionPhase::AbandonedForShutdown;
    return true;
}

} // namespace NKikimr::NSharedCache
