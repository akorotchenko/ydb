#include "shared_cache_space.h"

#include <new>

namespace NKikimr::NSharedCache {

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
    EffectiveHotSlots_.store(targetEffectiveHotSlots, std::memory_order_release);
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
        target.AddressBits > ReservedCapacity_.AddressBits || target.AddressBits == Capacity_.AddressBits ||
        target.ExpectedPageSize != ReservedCapacity_.ExpectedPageSize ||
        target.StaticBytes > ReservedCapacity_.StaticBytes || current.HandleCount != Capacity_.HandleCount() ||
        current.AllocationLimit != Capacity_.HandleCount() || spaceState.Resizing() ||
        spaceState.AddressBits() != Capacity_.AddressBits)
    {
        return EPrepareResult::Rejected;
    }

    std::array<size_t, static_cast<size_t>(ESpaceMap::Done)> sizes;
    if (!TryMappingSize(target.HandleCount(), sizeof(THandle), sizes[static_cast<size_t>(ESpaceMap::Handles)]) ||
        !TryMappingSize(
            target.BucketCount(), sizeof(std::atomic<ui64>), sizes[static_cast<size_t>(ESpaceMap::Buckets)]) ||
        !TryMappingSize(target.HotSlotCount(), sizeof(std::atomic<ui64>), sizes[static_cast<size_t>(ESpaceMap::Hot)]) ||
        !TryMappingSize(
            target.ColdSlotCount(), sizeof(std::atomic<ui64>), sizes[static_cast<size_t>(ESpaceMap::Cold)]) ||
        !TryMappingSize(
            target.KeepColdSlotCount(), sizeof(std::atomic<ui64>), sizes[static_cast<size_t>(ESpaceMap::KeepCold)]) ||
        !TryMappingSize(target.HandleCount(), sizeof(std::atomic<ui64>), sizes[static_cast<size_t>(ESpaceMap::Free)]))
    {
        return EPrepareResult::Rejected;
    }

    // Save ownership and intended extents before any mapping can leave partial work
    transition.PreparedView_ = MakeHolder<TSpaceView>();
    auto& view = *transition.PreparedView_;
    SetLegacyTierSlotCounts(view, target);
    transition.Phase_ = ETransitionPhase::Prepare;
    transition.Generation_ = spaceState.Generation();
    transition.OldConfiguration_ = Capacity_;
    transition.TargetConfiguration_ = target;
    transition.ResizeDeltaBits_ = Capacity_.AddressBits < target.AddressBits
                                      ? target.AddressBits - Capacity_.AddressBits
                                      : Capacity_.AddressBits - target.AddressBits;
    transition.ReleaseStage_ = ESpaceMap::Handles;
    const auto prepare = [&](ESpaceMap mapping, TSharedCacheMapping& owner, size_t& bytes, auto*& slots) noexcept {
        if constexpr (WithHook) {
            if (!hook.Run(mapping)) {
                return false;
            }
        }
        bytes = sizes[static_cast<size_t>(mapping)];
        return owner.Prepare(bytes, slots);
    };
    if (!prepare(ESpaceMap::Handles, HandlesMapping_, view.HandlesBytes, view.Handles) ||
        !prepare(ESpaceMap::Buckets, BucketsMapping_, view.BucketsBytes, view.Buckets) ||
        !prepare(ESpaceMap::Hot, TierStorage_[ECacheTier::Regular].Hot,
            view.Tiers[ECacheTier::Regular].HotStorage.Bytes, view.Tiers[ECacheTier::Regular].HotStorage.Slots) ||
        !prepare(ESpaceMap::Cold, TierStorage_[ECacheTier::Regular].Cold,
            view.Tiers[ECacheTier::Regular].ColdStorage.Bytes, view.Tiers[ECacheTier::Regular].ColdStorage.Slots) ||
        !prepare(ESpaceMap::KeepCold, TierStorage_[ECacheTier::Keep].Cold,
            view.Tiers[ECacheTier::Keep].ColdStorage.Bytes, view.Tiers[ECacheTier::Keep].ColdStorage.Slots) ||
        !prepare(ESpaceMap::Free, FreeMapping_, view.FreeSlotBytes, view.FreeSlots))
    {
        return CancelPreparedView(transition) ? EPrepareResult::Rejected : EPrepareResult::CleanupPending;
    }
    transition.ReleaseStage_ = ESpaceMap::Done;
    return EPrepareResult::Prepared;
}

bool TSharedCacheSpace::CancelPreparedView(TTransition& transition) noexcept {
    if (transition.Phase_ != ETransitionPhase::CancelRetry) {
        transition.Phase_ = ETransitionPhase::CancelRetry;
        transition.ReleaseStage_ = ESpaceMap::Handles;
    }
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
            case ESpaceMap::Hot:
                complete = cancel(TierStorage_[ECacheTier::Regular].Hot,
                    view.Tiers[ECacheTier::Regular].HotStorage.Slots, view.Tiers[ECacheTier::Regular].HotStorage.Bytes);
                break;
            case ESpaceMap::Cold:
                complete =
                    cancel(TierStorage_[ECacheTier::Regular].Cold, view.Tiers[ECacheTier::Regular].ColdStorage.Slots,
                        view.Tiers[ECacheTier::Regular].ColdStorage.Bytes);
                break;
            case ESpaceMap::KeepCold:
                complete = cancel(TierStorage_[ECacheTier::Keep].Cold, view.Tiers[ECacheTier::Keep].ColdStorage.Slots,
                    view.Tiers[ECacheTier::Keep].ColdStorage.Bytes);
                break;
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
    transition.Reset();
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
    const ui64 handleDelta = target.HandleCount() - old.HandleCount();
    const ui64 hotDelta = target.HotSlotCount() - old.HotSlotCount();
    const ui64 coldDelta = target.ColdSlotCount() - old.ColdSlotCount();
    const ui64 keepColdDelta = target.KeepColdSlotCount() - old.KeepColdSlotCount();
    const ui64 end = Min(handleDelta, transition.NextWorkIndex_ + SharedCacheTransitionWorkBatch);
    while (transition.NextWorkIndex_ < end) {
        const ui64 offset = transition.NextWorkIndex_++;
        const ui64 handleIndex = old.HandleCount() + offset;
        new (&view.Handles[handleIndex]) THandle();
        new (&view.Buckets[handleIndex]) std::atomic<ui64>(0);
        new (&view.FreeSlots[handleIndex]) std::atomic<ui64>(0);
        if (offset < coldDelta) {
            new (&view.Tiers[ECacheTier::Regular].ColdStorage.Slots[old.ColdSlotCount() + offset]) std::atomic<ui64>(0);
        }
        if (offset < keepColdDelta) {
            new (&view.Tiers[ECacheTier::Keep].ColdStorage.Slots[old.KeepColdSlotCount() + offset])
                std::atomic<ui64>(0);
        }
        if (offset < hotDelta) {
            new (&view.Tiers[ECacheTier::Regular].HotStorage.Slots[old.HotSlotCount() + offset]) std::atomic<ui64>(0);
        }
    }
    const bool complete = transition.NextWorkIndex_ == handleDelta;
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

    const bool growing = transition.TargetConfiguration_.HandleCount() > transition.OldConfiguration_.HandleCount();
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

    const bool growing = transition.TargetConfiguration_.HandleCount() > transition.OldConfiguration_.HandleCount();
    if (!growing) {
        PublishPreparedView(std::move(transition.PreparedView_), transition.OldView_);
    }

    const TSpaceState retired = expectedState.WithGeneration(expectedState.Generation() + 1);
    TSpaceView retiredView = SpaceView(expectedState);
    retiredView.CopyMappings(NewestView());
    retiredView.HandleCount = transition.TargetConfiguration_.HandleCount();
    retiredView.AllocationLimit = transition.TargetConfiguration_.HandleCount();
    SetLegacyTierSlotCounts(retiredView, transition.TargetConfiguration_);
    PrepareView(retired.Generation(), retiredView);

    const ui64 currentEffectiveHotSlots = EffectiveHotSlots();
    const ui64 effectiveHotSlots = Min(currentEffectiveHotSlots, transition.TargetConfiguration_.HotSlotCount());
    if (effectiveHotSlots != currentEffectiveHotSlots) {
        EffectiveHotSlots_.store(effectiveHotSlots, std::memory_order_release);
    }
    SpaceState_.store(retired.Raw(), std::memory_order_release);
    transition.Generation_ = retired.Generation();
    transition.ReleaseStage_ = ESpaceMap::Handles;
    ResetHazardScan(transition);
    if (growing) {
        transition.NextWorkIndex_ = transition.OldConfiguration_.HandleCount();
        transition.FinalDrainStage_ = ESpaceMap::Done;
        transition.Phase_ = ETransitionPhase::AppendGrowth;
        return AppendFreeHandles(transition);
    }

    transition.NextWorkIndex_ = transition.TargetConfiguration_.HotSlotCount();
    transition.FinalDrainStage_ = ESpaceMap::Hot;
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
        case ESpaceMap::Hot:
            owner = &TierStorage_[ECacheTier::Regular].Hot;
            oldPointer = old.Tiers[ECacheTier::Regular].HotStorage.Slots;
            oldSize = old.Tiers[ECacheTier::Regular].HotStorage.Bytes;
            currentPointer = newest.Tiers[ECacheTier::Regular].HotStorage.Slots;
            currentSize = newest.Tiers[ECacheTier::Regular].HotStorage.Bytes;
            break;
        case ESpaceMap::Cold:
            owner = &TierStorage_[ECacheTier::Regular].Cold;
            oldPointer = old.Tiers[ECacheTier::Regular].ColdStorage.Slots;
            oldSize = old.Tiers[ECacheTier::Regular].ColdStorage.Bytes;
            currentPointer = newest.Tiers[ECacheTier::Regular].ColdStorage.Slots;
            currentSize = newest.Tiers[ECacheTier::Regular].ColdStorage.Bytes;
            break;
        case ESpaceMap::KeepCold:
            owner = &TierStorage_[ECacheTier::Keep].Cold;
            oldPointer = old.Tiers[ECacheTier::Keep].ColdStorage.Slots;
            oldSize = old.Tiers[ECacheTier::Keep].ColdStorage.Bytes;
            currentPointer = newest.Tiers[ECacheTier::Keep].ColdStorage.Slots;
            currentSize = newest.Tiers[ECacheTier::Keep].ColdStorage.Bytes;
            break;
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
        case ESpaceMap::Hot:
            old.Tiers[ECacheTier::Regular].HotStorage.Slots = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::Cold:
            old.Tiers[ECacheTier::Regular].ColdStorage.Slots = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::KeepCold:
            old.Tiers[ECacheTier::Keep].ColdStorage.Slots = static_cast<std::atomic<ui64>*>(oldPointer);
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
    transition.Phase_ = ETransitionPhase::Commit;
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
    if (transition.Phase_ == ETransitionPhase::Prepare || transition.Phase_ == ETransitionPhase::InitializeGrowth ||
        transition.Phase_ == ETransitionPhase::CancelRetry)
    {
        return CancelPreparedView(transition);
    }
    transition.Phase_ = ETransitionPhase::AbandonedForShutdown;
    return true;
}

} // namespace NKikimr::NSharedCache
