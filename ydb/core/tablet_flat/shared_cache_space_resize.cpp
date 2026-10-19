#include "shared_cache_space.h"

#include <new>

namespace NKikimr::NSharedCache {

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
    raw = NewestView().HotSlots[slot].exchange(0, std::memory_order_acq_rel);
    if (resize.NextCutSlot_ == resize.OldEffectiveHotSlots_) {
        resize.Reset();
    }
    return true;
}

} // namespace NKikimr::NSharedCache
