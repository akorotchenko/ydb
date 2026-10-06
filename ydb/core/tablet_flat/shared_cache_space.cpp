#include "shared_cache_space.h"

#include <array>
#include <new>

namespace NKikimr::NSharedCache {
namespace {

    bool TryAdd(ui64 left, ui64 right, ui64& result) noexcept {
        if (left > Max<ui64>() - right) {
            return false;
        }
        result = left + right;
        return true;
    }

    bool TryMultiply(ui64 left, ui64 right, ui64& result) noexcept {
        if (left != 0 && right > Max<ui64>() / left) {
            return false;
        }
        result = left * right;
        return true;
    }

    bool TryAccumulateProduct(ui64 count, ui64 size, ui64& total) noexcept {
        ui64 product;
        return TryMultiply(count, size, product) && TryAdd(total, product, total);
    }

    bool TryMappingSize(ui64 count, ui64 itemSize, size_t& result) noexcept {
        ui64 bytes;
        if (!TryMultiply(count, itemSize, bytes) || bytes > Max<size_t>()) {
            return false;
        }
        result = static_cast<size_t>(bytes);
        return true;
    }

} // anonymous namespace

TSharedCacheSpace::~TSharedCacheSpace() = default;

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

bool TSharedCacheSpace::PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept {
    const TSpaceState spaceState = CurrentSpaceState();
    const TSpaceView& current = SpaceView(spaceState);
    if (transition.Phase_ != ETransitionPhase::Idle || target.AddressBits < MinSharedCacheAddressBits ||
        target.AddressBits > MaxSharedCacheAddressBits || target.AddressBits > ReservedCapacity_.AddressBits ||
        target.AddressBits == Capacity_.AddressBits || target.StaticBytes > ReservedCapacity_.StaticBytes ||
        current.HandleCount != Capacity_.HandleCount() || current.AllocationLimit != Capacity_.HandleCount())
    {
        return false;
    }

    if (spaceState.Resizing() || spaceState.AddressBits() != Capacity_.AddressBits)
    {
        return false;
    }

    THolder<TSpaceView> view = MakeHolder<TSpaceView>();
    if (!TryMappingSize(target.HandleCount(), sizeof(THandle), view->HandlesBytes) ||
        !TryMappingSize(target.BucketCount(), sizeof(std::atomic<ui64>), view->BucketsBytes) ||
        !TryMappingSize(target.HotSlotCount(), sizeof(std::atomic<ui64>), view->HotSlotBytes) ||
        !TryMappingSize(target.ColdSlotCount(), sizeof(std::atomic<ui64>), view->ColdSlotBytes) ||
        !TryMappingSize(target.KeepColdSlotCount(), sizeof(std::atomic<ui64>), view->KeepColdSlotBytes) ||
        !TryMappingSize(target.HandleCount(), sizeof(std::atomic<ui64>), view->FreeSlotBytes) ||
        !HandlesMapping_.Prepare(view->HandlesBytes, view->Handles) ||
        !BucketsMapping_.Prepare(view->BucketsBytes, view->Buckets) ||
        !HotMapping_.Prepare(view->HotSlotBytes, view->HotSlots) ||
        !ColdMapping_.Prepare(view->ColdSlotBytes, view->ColdSlots) ||
        !KeepColdMapping_.Prepare(view->KeepColdSlotBytes, view->KeepColdSlots) ||
        !FreeMapping_.Prepare(view->FreeSlotBytes, view->FreeSlots))
    {
        CancelPreparedView(*view);
        return false;
    }

    transition.Phase_ = ETransitionPhase::Prepare;
    transition.Generation_ = spaceState.Generation();
    transition.OldConfiguration_ = Capacity_;
    transition.TargetConfiguration_ = target;
    transition.ResizeDeltaBits_ = Capacity_.AddressBits < target.AddressBits
                                      ? target.AddressBits - Capacity_.AddressBits
                                      : Capacity_.AddressBits - target.AddressBits;
    transition.ReleaseStage_ = ESpaceMap::Done;
    transition.NextWorkIndex_ = 0;
    transition.FinalDrainStage_ = ESpaceMap::Done;
    transition.CutBlocked_ = false;
    transition.PreparedView_ = std::move(view);
    transition.OldView_.Reset();
    return true;
}

void TSharedCacheSpace::CancelPreparedView(TSpaceView& view) noexcept {
    HandlesMapping_.Cancel(view.Handles, view.HandlesBytes);
    BucketsMapping_.Cancel(view.Buckets, view.BucketsBytes);
    HotMapping_.Cancel(view.HotSlots, view.HotSlotBytes);
    ColdMapping_.Cancel(view.ColdSlots, view.ColdSlotBytes);
    KeepColdMapping_.Cancel(view.KeepColdSlots, view.KeepColdSlotBytes);
    FreeMapping_.Cancel(view.FreeSlots, view.FreeSlotBytes);
}

void TSharedCacheSpace::PublishPreparedView(THolder<TSpaceView> view, THolder<TSpaceView>& oldView) noexcept {
    Y_DEBUG_ABORT_UNLESS(view);
    oldView = std::move(NewestViewOwner_);
    HandlesMapping_.Publish(view->Handles, view->HandlesBytes);
    BucketsMapping_.Publish(view->Buckets, view->BucketsBytes);
    HotMapping_.Publish(view->HotSlots, view->HotSlotBytes);
    ColdMapping_.Publish(view->ColdSlots, view->ColdSlotBytes);
    KeepColdMapping_.Publish(view->KeepColdSlots, view->KeepColdSlotBytes);
    FreeMapping_.Publish(view->FreeSlots, view->FreeSlotBytes);
    NewestViewOwner_ = std::move(view);
    NewestView_.store(NewestViewOwner_.Get(), std::memory_order_release);
}

void TSharedCacheSpace::PrepareView(ui32 generation, TSpaceView view) noexcept {
    Y_DEBUG_ABORT_UNLESS(S5FifoRings_ && FreeRingCursor_);
    view.Space = this;
    view.Generation = generation;
    const ui64 usableHandles = view.AllocationLimit - 2;
    view.ColdMinHandles = (usableHandles + 4) / 5;
    view.ColdGrowHandles = (usableHandles * 3 + 9) / 10;
    view.Rings = S5FifoRings_.Get();
    view.FreeCursor = FreeRingCursor_.Get();
    view.EffectiveHotSlots = &EffectiveHotSlots_;
    SpaceViews_[generation & 1] = view;
}

ui32 TSharedCacheSpace::HotLevelForSlot(ui64 slot, ui64 hotSlotCount) noexcept {
    const ui64 l3End = hotSlotCount / 15;
    const ui64 l2End = l3End + (hotSlotCount * 2) / 15;
    const ui64 l1End = l2End + (hotSlotCount * 5) / 15;
    if (slot < l3End) {
        return 3;
    }
    if (slot < l2End) {
        return 2;
    }
    if (slot < l1End) {
        return 1;
    }
    return 0;
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
            new (&view.ColdSlots[old.ColdSlotCount() + offset]) std::atomic<ui64>(0);
        }
        if (offset < keepColdDelta) {
            new (&view.KeepColdSlots[old.KeepColdSlotCount() + offset]) std::atomic<ui64>(0);
        }
        if (offset < hotDelta) {
            new (&view.HotSlots[old.HotSlotCount() + offset]) std::atomic<ui64>(0);
        }
    }
    const bool complete = transition.NextWorkIndex_ == handleDelta;
    if (complete) {
        transition.NextWorkIndex_ = 0;
    }
    return complete;
}

bool TSharedCacheSpace::IsCurrentFreeWord(const TSpaceView& view, ui64 raw) const noexcept {
    const TCacheItem cacheItem = TCacheItem::FromRaw(raw);
    if (cacheItem.IsNull() || cacheItem.IsFrozen() || cacheItem.Index() < 2 ||
        cacheItem.Index() >= view.AllocationLimit || cacheItem.Index() >= view.HandleCount)
    {
        return false;
    }
    const THandleState state =
        THandleState::FromRaw(view.Handles[cacheItem.Index()].State.load(std::memory_order_acquire));
    return cacheItem.Matches(state) && state.IsFree() && state.Refs() == 0;
}

void TSharedCacheSpace::RouteFreeWord(const TSpaceView& view, ui64 raw) noexcept {
    const TFreeRingView free = view.Free();
    while (raw != 0 && IsCurrentFreeWord(view, raw)) {
        raw = free.Push(raw).Previous;
    }
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

    while (transition.NextHazardIndex_ < HazardSlotCount_) {
        const ui64 hazardState = Hazards_[transition.NextHazardIndex_].State.load(std::memory_order_acquire);
        if (!SpaceHazardIsDrained(hazardState, transition.Generation_)) {
            return false;
        }
        ++transition.NextHazardIndex_;
    }
    if (transition.Phase_ == ETransitionPhase::MigrationDrain) {
        transition.Phase_ = ETransitionPhase::Migrate;
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
    retiredView.HotSlotCount = transition.TargetConfiguration_.HotSlotCount();
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
            owner = &HotMapping_;
            oldPointer = old.HotSlots;
            oldSize = old.HotSlotBytes;
            currentPointer = newest.HotSlots;
            currentSize = newest.HotSlotBytes;
            break;
        case ESpaceMap::Cold:
            owner = &ColdMapping_;
            oldPointer = old.ColdSlots;
            oldSize = old.ColdSlotBytes;
            currentPointer = newest.ColdSlots;
            currentSize = newest.ColdSlotBytes;
            break;
        case ESpaceMap::KeepCold:
            owner = &KeepColdMapping_;
            oldPointer = old.KeepColdSlots;
            oldSize = old.KeepColdSlotBytes;
            currentPointer = newest.KeepColdSlots;
            currentSize = newest.KeepColdSlotBytes;
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
    if (!owner->Release(oldPointer, oldSize, currentPointer, currentSize)) {
        return false;
    }

    switch (mapping) {
        case ESpaceMap::Handles:
            old.Handles = static_cast<THandle*>(oldPointer);
            break;
        case ESpaceMap::Buckets:
            old.Buckets = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::Hot:
            old.HotSlots = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::Cold:
            old.ColdSlots = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::KeepCold:
            old.KeepColdSlots = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::Free:
            old.FreeSlots = static_cast<std::atomic<ui64>*>(oldPointer);
            break;
        case ESpaceMap::Done:
            Y_ABORT("Invalid shared-cache release stage");
    }
    return true;
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
    if (transition.Phase_ == ETransitionPhase::Prepare || transition.Phase_ == ETransitionPhase::InitializeGrowth)
    {
        CancelPreparedView(*transition.PreparedView_);
        transition.Reset();
        return true;
    }
    transition.Phase_ = ETransitionPhase::AbandonedForShutdown;
    return true;
}

TSpaceHazardBinding::TSpaceHazardBinding(const TSharedCacheSpace* space, TSpaceHazard* hazard) noexcept
    : Space_(space)
    , Hazard_(hazard)
{
    Y_DEBUG_ABORT_UNLESS(!TSharedCacheSpace::TlsSpace_);
    Y_ABORT_UNLESS(!TSharedCacheSpace::TlsHazard_);
    Y_IF_DEBUG(TSharedCacheSpace::TlsSpace_ = space;)
    TSharedCacheSpace::TlsHazard_ = hazard;
}

TSpaceHazardBinding::TSpaceHazardBinding(TSpaceHazardBinding&& other) noexcept
    : Space_(std::exchange(other.Space_, nullptr))
    , Hazard_(std::exchange(other.Hazard_, nullptr))
{
}

TSpaceHazardBinding& TSpaceHazardBinding::operator=(TSpaceHazardBinding&& other) noexcept {
    if (this != &other) {
        Reset();
        Space_ = std::exchange(other.Space_, nullptr);
        Hazard_ = std::exchange(other.Hazard_, nullptr);
    }
    return *this;
}

TSpaceHazardBinding::~TSpaceHazardBinding() {
    Reset();
}

void TSpaceHazardBinding::Reset() noexcept {
    if (Space_) {
        Y_DEBUG_ABORT_UNLESS(TSharedCacheSpace::TlsSpace_ == Space_);
        Y_ABORT_UNLESS(TSharedCacheSpace::TlsHazard_ == Hazard_);
        Y_IF_DEBUG(TSharedCacheSpace::TlsSpace_ = nullptr;)
        TSharedCacheSpace::TlsHazard_ = nullptr;
        Space_ = nullptr;
        Hazard_ = nullptr;
    }
}

TSpaceHazardBinding TSharedCacheSpace::BindThreadHazard(ui32 index) const noexcept {
    Y_ABORT_UNLESS(index < HazardSlotCount_);
    return TSpaceHazardBinding(this, &Hazards_[index]);
}

TCacheItem TSharedCacheSpace::TakeCutSpare(ui64 allocationLimit) noexcept {
    const auto take = [allocationLimit](TSpaceHazard& hazard) noexcept {
        ui64 raw = hazard.SpareItem.load(std::memory_order_acquire);
        while (raw != 0) {
            const TCacheItem cacheItem = TCacheItem::FromRaw(raw);
            if (cacheItem.Index() < allocationLimit) {
                return TCacheItem{};
            }
            if (hazard.SpareItem.compare_exchange_weak(raw, 0, std::memory_order_acquire, std::memory_order_relaxed))
            {
                return cacheItem;
            }
        }
        return TCacheItem{};
    };

    for (ui32 index = 0; index < HazardSlotCount_; ++index) {
        TCacheItem cacheItem = take(Hazards_[index]);
        if (!cacheItem.IsNull()) {
            return cacheItem;
        }
    }
    return {};
}

bool TSharedCacheSpace::BeginHotResize(ui32 targetEffectiveHotSlots, THotResize& resize) noexcept {
    const ui64 currentEffectiveHotSlots = EffectiveHotSlots();
    const ui64 maximumHotSlots = SpaceView(CurrentSpaceState()).HotSlotCount;
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
    raw = NewestView().HotSlots[slot].exchange(0, std::memory_order_release);
    if (resize.NextCutSlot_ == resize.OldEffectiveHotSlots_) {
        resize.Reset();
    }
    return true;
}

bool TryCalculateSharedCacheFootprint(
    ui8 addressBits, ui64 expectedPageSize, ui64 fixedBytes, ui32 hazardCount, TSharedCacheCapacity& result) noexcept {
    if (addressBits < MinSharedCacheAddressBits || addressBits > MaxSharedCacheAddressBits || expectedPageSize == 0) {
        return false;
    }

    const ui64 handleCount = ui64{ 1 } << addressBits;
    const ui64 bucketCount = handleCount;
    const ui64 hotSlotCount = (handleCount * 3) / 4;
    ui64 accountedPageBytes;
    if (!TryAdd(expectedPageSize, NActors::TSharedData::OverheadSize, accountedPageBytes)) {
        return false;
    }

    ui64 staticBytes = fixedBytes;
    if (!TryAccumulateProduct(handleCount, sizeof(THandle), staticBytes) ||
        !TryAccumulateProduct(bucketCount, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(hotSlotCount, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(handleCount / 2, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(handleCount / 2, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(handleCount, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(Max<ui64>(hazardCount, 1), sizeof(TSpaceHazard), staticBytes))
    {
        return false;
    }

    ui64 minimumPayloadBytes;
    if (!TryMultiply(handleCount - 2, accountedPageBytes, minimumPayloadBytes)) {
        return false;
    }

    ui64 totalBytes;
    if (!TryAdd(staticBytes, minimumPayloadBytes, totalBytes)) {
        return false;
    }

    result = {
        .AddressBits = addressBits,
        .Limit = totalBytes,
        .ExpectedPageSize = expectedPageSize,
        .FixedBytes = fixedBytes,
        .StaticBytes = staticBytes,
        .MinimumPayloadBytes = minimumPayloadBytes,
        .TotalBytes = totalBytes,
    };
    return true;
}

bool TryCalculateSharedCacheCapacity(
    ui64 limit, ui64 expectedPageSize, ui64 fixedBytes, ui32 hazardCount, TSharedCacheCapacity& result) noexcept {
    bool found = false;
    TSharedCacheCapacity best;
    for (ui32 addressBits = MinSharedCacheAddressBits; addressBits <= MaxSharedCacheAddressBits; ++addressBits) {
        TSharedCacheCapacity footprint;
        if (!TryCalculateSharedCacheFootprint(
                static_cast<ui8>(addressBits), expectedPageSize, fixedBytes, hazardCount, footprint) ||
            footprint.TotalBytes > limit)
        {
            break;
        }
        best = footprint;
        found = true;
    }
    if (!found) {
        return false;
    }
    best.Limit = limit;
    result = best;
    return true;
}

THolder<TSharedCacheSpace> TSharedCacheSpace::Create(
    const TSharedCacheCapacity& current, const TSharedCacheCapacity& reserved, ui32 hazardCount)
{
    THolder<TSharedCacheSpace> result(new TSharedCacheSpace());
    if (!result->Initialize(current, reserved, hazardCount)) {
        return nullptr;
    }
    return result;
}

bool TSharedCacheSpace::Initialize(
    const TSharedCacheCapacity& current, const TSharedCacheCapacity& reserved, ui32 hazardCount)
{
    if (current.AddressBits < MinSharedCacheAddressBits || current.AddressBits > MaxSharedCacheAddressBits ||
        reserved.AddressBits < current.AddressBits || reserved.AddressBits > MaxSharedCacheAddressBits ||
        current.ExpectedPageSize == 0 || current.ExpectedPageSize != reserved.ExpectedPageSize ||
        current.FixedBytes != reserved.FixedBytes || current.Limit < current.TotalBytes ||
        reserved.Limit < reserved.TotalBytes || reserved.Limit < current.Limit)
    {
        return false;
    }

    THolder<TSpaceView> view = MakeHolder<TSpaceView>();
    size_t reservedHandlesBytes;
    size_t reservedBucketsBytes;
    size_t reservedHotBytes;
    size_t reservedSlotsBytes;
    if (!TryMappingSize(current.HandleCount(), sizeof(THandle), view->HandlesBytes) ||
        !TryMappingSize(current.BucketCount(), sizeof(std::atomic<ui64>), view->BucketsBytes) ||
        !TryMappingSize(current.HotSlotCount(), sizeof(std::atomic<ui64>), view->HotSlotBytes) ||
        !TryMappingSize(current.ColdSlotCount(), sizeof(std::atomic<ui64>), view->ColdSlotBytes) ||
        !TryMappingSize(current.KeepColdSlotCount(), sizeof(std::atomic<ui64>), view->KeepColdSlotBytes) ||
        !TryMappingSize(current.HandleCount(), sizeof(std::atomic<ui64>), view->FreeSlotBytes) ||
        !TryMappingSize(reserved.HandleCount(), sizeof(THandle), reservedHandlesBytes) ||
        !TryMappingSize(reserved.BucketCount(), sizeof(std::atomic<ui64>), reservedBucketsBytes) ||
        !TryMappingSize(reserved.HotSlotCount(), sizeof(std::atomic<ui64>), reservedHotBytes) ||
        !TryMappingSize(reserved.HandleCount(), sizeof(std::atomic<ui64>), reservedSlotsBytes) ||
        !HandlesMapping_.Allocate(view->HandlesBytes, reservedHandlesBytes, view->Handles) ||
        !BucketsMapping_.Allocate(view->BucketsBytes, reservedBucketsBytes, view->Buckets) ||
        !HotMapping_.Allocate(view->HotSlotBytes, reservedHotBytes, view->HotSlots) ||
        !ColdMapping_.Allocate(view->ColdSlotBytes, reservedSlotsBytes / 2, view->ColdSlots) ||
        !KeepColdMapping_.Allocate(view->KeepColdSlotBytes, reservedSlotsBytes / 2, view->KeepColdSlots) ||
        !FreeMapping_.Allocate(view->FreeSlotBytes, reservedSlotsBytes, view->FreeSlots))
    {
        return false;
    }

    for (ui64 index = 0; index < current.HandleCount(); ++index) {
        new (&view->Handles[index]) THandle();
        new (&view->FreeSlots[index]) std::atomic<ui64>(0);
    }
    for (ui64 index = 0; index < current.ColdSlotCount(); ++index) {
        new (&view->ColdSlots[index]) std::atomic<ui64>(0);
    }
    for (ui64 index = 0; index < current.BucketCount(); ++index) {
        new (&view->Buckets[index]) std::atomic<ui64>(0);
    }
    for (ui64 index = 0; index < current.HotSlotCount(); ++index) {
        new (&view->HotSlots[index]) std::atomic<ui64>(0);
    }
    for (ui64 index = 0; index < current.KeepColdSlotCount(); ++index) {
        new (&view->KeepColdSlots[index]) std::atomic<ui64>(0);
    }

    for (ui64 index = 2; index < current.HandleCount(); ++index) {
        view->FreeSlots[index - 2].store(
            TCacheItem::Make(0, static_cast<ui32>(index)).Raw(), std::memory_order_relaxed);
    }

    HazardSlotCount_ = Max<ui32>(hazardCount, 1);
    Hazards_ = TArrayHolder<TSpaceHazard>(new TSpaceHazard[HazardSlotCount_]);
    HazardCount_ = hazardCount;
    Capacity_ = current;
    ReservedCapacity_ = reserved;
    NewestViewOwner_ = std::move(view);
    S5FifoRings_ = MakeHolder<TS5FifoRings>();
    FreeRingCursor_ = MakeHolder<TFreeRingCursor>(0, current.HandleCount() - 2, current.HandleCount() - 2);
    NewestView_.store(NewestViewOwner_.Get(), std::memory_order_release);
    TSpaceView initialView;
    initialView.CopyMappings(*NewestViewOwner_);
    initialView.HandleCount = current.HandleCount();
    initialView.AllocationLimit = current.HandleCount();
    initialView.HotSlotCount = current.HotSlotCount();
    PrepareView(0, initialView);
    SpaceState_.store(TSpaceState::Make(0, current.AddressBits).Raw(), std::memory_order_release);
    EffectiveHotSlots_.store(current.HotSlotCount(), std::memory_order_release);
    return true;
}

ui32 TSpaceOperation::TryAllocateHandle() noexcept {
    return View().Space->TryAllocateHandleImpl<false>(*this, {});
}

template <bool WithHook>
Y_FORCE_INLINE ui32 TSharedCacheSpace::TryAllocateHandleImpl(
    TSpaceOperation& spaceOp, TFreeExchangeHook hook) noexcept {
    for (;;) {
        const TSpaceState attemptState = CurrentSpaceState();
        const TSpaceView& attemptView = SpaceView(attemptState);
        const TFreeRingView free = attemptView.Free();
        const TRingExchangeResult exchange = free.Pop([&] {
            if constexpr (WithHook) {
                hook.Run();
            }
        });
        if (!exchange.Slot) {
            return 0;
        }
        const ui64 rawItem = exchange.Previous;
        if (rawItem == 0) {
            continue;
        }

        const TCacheItem cacheItem = TCacheItem::FromRaw(rawItem);
        if (CurrentSpaceState().Generation() != attemptState.Generation())
        {
            if (!cacheItem.IsNull() && !cacheItem.IsFrozen()) {
                spaceOp.ReturnFreeHandle(cacheItem.Index(), cacheItem.Version());
            }
            continue;
        }
        if (cacheItem.IsNull() || cacheItem.IsFrozen() || cacheItem.Index() >= attemptView.AllocationLimit) {
            continue;
        }

        THandle& handle = attemptView.Handles[cacheItem.Index()];
        ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsFree() || expected.Refs() != 0)
        {
            continue;
        }

        const THandleState desired =
            expected.WithState(EHandleState::Begin).WithFrequency(0).WithSticky(EStickyState::None);
        if (handle.State.compare_exchange_strong(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            return cacheItem.Index();
        }
    }
}

ui32 TSharedCacheSpace::TryAllocateHandleWithHook(TSpaceOperation& spaceOp, TFreeExchangeHook hook) noexcept {
    return TryAllocateHandleImpl<true>(spaceOp, hook);
}

bool TSpaceOperation::ReturnFreeHandle(ui32 index, ui32 version) noexcept {
    return View().Space->ReturnFreeHandleImpl<false>(*this, index, version, {});
}

template <bool WithHook>
Y_FORCE_INLINE bool TSharedCacheSpace::ReturnFreeHandleImpl(
    TSpaceOperation& spaceOp, ui32 index, ui32 version, TFreeExchangeHook hook) noexcept {
    TSpaceState currentState = CurrentSpaceState();
    const TSpaceView* currentView = &SpaceView(currentState);
    const TSpaceView& operationView = spaceOp.View();
    if (index < 2 || index >= operationView.HandleCount) {
        return false;
    }

    const THandleState state =
        THandleState::FromRaw(operationView.Handles[index].State.load(std::memory_order_acquire));
    if (state.Version() != version || !state.IsFree() || state.Refs() != 0) {
        return false;
    }
    if (index >= currentView->AllocationLimit) {
        return true;
    }

    std::array<ui64, 2> pending = {
        TCacheItem::Make(version, index).Raw(),
        0,
    };
    ui32 pendingCount = 1;
    while (pendingCount) {
        ui64 carried = pending[--pendingCount];
        for (;;) {
            const TCacheItem cacheItem = TCacheItem::FromRaw(carried);
            currentState = CurrentSpaceState();
            currentView = &SpaceView(currentState);
            if (cacheItem.IsNull() || cacheItem.IsFrozen() || cacheItem.Index() >= currentView->AllocationLimit)
            {
                break;
            }
            const THandleState handleState =
                THandleState::FromRaw(currentView->Handles[cacheItem.Index()].State.load(std::memory_order_acquire));
            if (!cacheItem.Matches(handleState) || !handleState.IsFree() || handleState.Refs() != 0)
            {
                break;
            }

            const TSpaceState attemptState = currentState;
            const TSpaceView* attemptView = currentView;
            const TRingExchangeResult exchange = attemptView->Free().Push(carried, [&] {
                if constexpr (WithHook) {
                    hook.Run();
                }
            });
            const ui64 displaced = exchange.Previous;

            if (CurrentSpaceState().Generation() == attemptState.Generation())
            {
                if (displaced == 0) {
                    break;
                }
                carried = displaced;
                continue;
            }

            const ui64 late = FreeRingCursor_->Clear(exchange.Slot);
            if (late != 0) {
                Y_ABORT_UNLESS(pendingCount < pending.size());
                pending[pendingCount++] = late;
            }
            if (displaced == 0) {
                break;
            }
            carried = displaced;
        }
    }
    return true;
}

bool TSharedCacheSpace::ReturnFreeHandleWithHook(
    TSpaceOperation& spaceOp, ui32 index, ui32 version, TFreeExchangeHook hook) noexcept {
    return ReturnFreeHandleImpl<true>(spaceOp, index, version, hook);
}

} // namespace NKikimr::NSharedCache
