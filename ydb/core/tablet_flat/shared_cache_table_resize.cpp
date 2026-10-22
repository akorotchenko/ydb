#include "shared_cache.h"

namespace NKikimr::NSharedCache {

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCacheTable TSharedCacheTableImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::FindSplitBoundary(
    const TSpaceOperation& spaceOp, ui32 bucket, ui32 splitBit, bool suffix, TOrderedLink& result) noexcept {
restart:
    result.Link = &spaceOp.Buckets()[bucket];
    result.Expected = TCacheItem::FromRaw(result.Link->load(std::memory_order_acquire));
    result.Owner = {};
    result.Equal = false;
    result.SawSplit = false;
    bool atBucketHead = true;
    for (;;) {
        if (atBucketHead && result.Expected.IsClosedBucketHead()) {
            return false;
        }
        if (result.Expected.IsNull()) {
            if (!ValidateTerminalOwner(spaceOp, result)) {
                goto restart;
            }
            return true;
        }
        if (!spaceOp.Contains(result.Expected)) {
            goto restart;
        }

        THandle& handle = spaceOp.Handles()[result.Expected.Index()];
        const TCacheItem next = TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire));
        const ui64 key0 = handle.Key0.load(std::memory_order_relaxed);
        const ui64 key1 = handle.Key1.load(std::memory_order_relaxed);
        const ui64 key2 = handle.Key2OrSize.load(std::memory_order_relaxed);
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (!result.Expected.Matches(state) || state.IsFree()) {
            goto restart;
        }
        if (state.IsReplacing() || state.IsReplaced()) {
            if (!next.IsFrozen() || next.WithoutFrozen().IsNull() || next.WithoutFrozen() == result.Expected) {
                goto restart;
            }
            result.Expected = next.WithoutFrozen();
            continue;
        }
        if (state.IsBucketSplit() || (state.IsTombstone() && result.Expected.IsResizeMarker()))
        {
            if (result.Expected.IsResizeMarker()) {
                return true;
            }
            result.Link = &handle.Next;
            result.Owner = result.Expected;
            result.Expected = next.WithoutFrozen();
            atBucketHead = false;
            continue;
        }

        const TSharedCacheKey key = TSharedCacheKey::FromWords(state.Kind(), key0, key1, key2);
        const bool isSuffix = splitBit != 0 && (key.Hash() & splitBit) != 0;
        if (splitBit == 0 || suffix != isSuffix) {
            result.Link = &handle.Next;
            result.Owner = result.Expected;
            result.Expected = next.WithoutFrozen();
            atBucketHead = false;
            continue;
        }
        return true;
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::PrepareSplitMarker(const TSpaceOperation& spaceOp, TCacheItem suffix) noexcept {
    THandle& marker = spaceOp.Handles()[ResizeMarkerIndex];
    ui64 expectedRaw = marker.State.load(std::memory_order_acquire);
    const THandleState expected = THandleState::FromRaw(expectedRaw);
    // Helpers may still hold the retired marker from the previous pair.
    if ((expected.IsBucketSplit() || expected.IsTombstone()) && expected.Refs() > 0) {
        return false;
    }
    Y_ABORT_UNLESS(expected.IsFree() && expected.Refs() == 0);
    const TCacheItem next = suffix.IsNull() ? TCacheItem::Make(expected.Version(), 0) : suffix;
    marker.Next.store(next.Raw(), std::memory_order_relaxed);
    const THandleState desired =
        expected.WithState(EHandleState::BucketSplit).WithFrequency(0).WithSticky(EStickyState::None).WithRefs(1);
    if (!marker.State.compare_exchange_strong(
            expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
    {
        return false;
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::TransitionSplitMarkerToTombstone(const TSpaceOperation& spaceOp, TCacheItem marker) noexcept {
    THandle& handle = spaceOp.Handles()[ResizeMarkerIndex];
    ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!marker.Matches(expected)) {
            return false;
        }
        if (expected.IsTombstone()) {
            return true;
        }
        if (!expected.IsBucketSplit()) {
            return false;
        }
        const THandleState desired = expected.WithState(EHandleState::Tombstone).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_acquire))
        {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::SplitMarkerIsSoleOwner(const TSpaceOperation& spaceOp, TCacheItem marker) const noexcept {
    const THandleState state =
        THandleState::FromRaw(spaceOp.Handles()[ResizeMarkerIndex].State.load(std::memory_order_acquire));
    return marker.Matches(state) && (state.IsBucketSplit() || state.IsTombstone()) && state.Refs() == 1;
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::PrepareBucketSplit(TSpaceOperation& spaceOp) noexcept {
    const TBucketResizeState resize = Cache_.Space_->BucketResizeState();
    const ui32 pairCount = resize.PairCount();
    if (!resize || resize.Phase() != EBucketResizePhase::Prepared || resize.Cursor() >= pairCount)
    {
        return false;
    }

    const THandleState markerState =
        THandleState::FromRaw(spaceOp.Handles()[ResizeMarkerIndex].State.load(std::memory_order_acquire));
    if (markerState.IsBucketSplit()) {
        return true;
    }
    const ui32 bucket = resize.Cursor();
    TOrderedLink boundary;
    TCacheItem suffix;
    if (resize.Direction() == EBucketResize::Growing) {
        if (!FindSplitBoundary(spaceOp, bucket, resize.SplitBit(), true, boundary)) {
            return false;
        }
        suffix = boundary.Expected;
    } else {
        if (!FindSplitBoundary(spaceOp, bucket, 0, false, boundary)) {
            return false;
        }
        suffix = TCacheItem::FromRaw(spaceOp.Buckets()[bucket | resize.SplitBit()].load(std::memory_order_acquire));
    }

    return PrepareSplitMarker(spaceOp, suffix);
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::ActivateBucketSplitOrMerge(TSpaceOperation& spaceOp) noexcept {
    const TBucketResizeState resize = Cache_.Space_->BucketResizeState();
    const ui32 pairCount = resize.PairCount();
    if (!resize || resize.Phase() != EBucketResizePhase::Prepared || resize.Cursor() >= pairCount)
    {
        return false;
    }

    const THandleState markerState =
        THandleState::FromRaw(spaceOp.Handles()[ResizeMarkerIndex].State.load(std::memory_order_acquire));
    if (!markerState.IsBucketSplit()) {
        return false;
    }
    const TCacheItem marker = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);
    return resize.Direction() == EBucketResize::Growing ? ActivateBucketSplit(spaceOp, resize, marker)
                                                        : ActivateBucketMerge(spaceOp, resize, marker);
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::ActivateBucketSplit(
    TSpaceOperation& spaceOp, const TBucketResizeState& resize, TCacheItem marker) noexcept {
    TOrderedLink boundary;
    const ui32 bucket = resize.Cursor();
    if (!FindSplitBoundary(spaceOp, bucket, resize.SplitBit(), true, boundary)) {
        return false;
    }
    if (boundary.Expected == marker) {
        return Cache_.Space_->AdvanceBucketResizePhase(EBucketResizePhase::Activated);
    }
    TOperationItemRef ownerRef(spaceOp);
    if (!boundary.Owner.IsNull()) {
        ownerRef.Get() = TryAcquire(spaceOp, boundary.Owner);
        if (!ownerRef) {
            return false;
        }
        const THandleState ownerState =
            THandleState::FromRaw(spaceOp.Handles()[boundary.Owner.Index()].State.load(std::memory_order_relaxed));
        if (!boundary.Owner.Matches(ownerState) || ownerState.IsTombstone() || ownerState.IsFree() ||
            boundary.Link->load(std::memory_order_acquire) != boundary.Expected.Raw())
        {
            return false;
        }
    }
    const TCacheItem next = boundary.Expected.IsNull() ? TCacheItem::Make(marker.Version(), 0) : boundary.Expected;
    spaceOp.Handles()[ResizeMarkerIndex].Next.store(next.Raw(), std::memory_order_relaxed);
    ui64 expectedPeer = 0;
    if (!spaceOp.Buckets()[bucket | resize.SplitBit()].compare_exchange_strong(
            expectedPeer, marker.Raw(), std::memory_order_release, std::memory_order_acquire) &&
        expectedPeer != marker.Raw())
    {
        return false;
    }
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterGrowthBucketPublished, marker);
    ui64 expected = boundary.Expected.Raw();
    if (!boundary.Link->compare_exchange_strong(
            expected, marker.Raw(), std::memory_order_release, std::memory_order_acquire))
    {
        return false;
    }
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterBucketSplitActivated, marker);
    return Cache_.Space_->AdvanceBucketResizePhase(EBucketResizePhase::Activated);
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::ActivateBucketMerge(
    TSpaceOperation& spaceOp, const TBucketResizeState& resize, TCacheItem marker) noexcept {
    const ui32 bucket = resize.Cursor();
    const TCacheItem highHead =
        TCacheItem::FromRaw(spaceOp.Buckets()[bucket | resize.SplitBit()].load(std::memory_order_acquire));
    const TCacheItem next = highHead.IsNull() ? TCacheItem::Make(marker.Version(), 0) : highHead;
    spaceOp.Handles()[ResizeMarkerIndex].Next.store(next.Raw(), std::memory_order_relaxed);
    TOrderedLink boundary;
    if (!FindSplitBoundary(spaceOp, bucket, 0, false, boundary)) {
        return false;
    }
    if (boundary.Expected != marker) {
        ui64 expected = boundary.Expected.Raw();
        if (!boundary.Link->compare_exchange_strong(
                expected, marker.Raw(), std::memory_order_release, std::memory_order_acquire))
        {
            return false;
        }
    }
    ui64 expectedHigh = highHead.Raw();
    if (!spaceOp.Buckets()[bucket | resize.SplitBit()].compare_exchange_strong(
            expectedHigh, marker.Raw(), std::memory_order_release, std::memory_order_acquire) &&
        expectedHigh != marker.Raw())
    {
        return false;
    }
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterBucketSplitActivated, marker);
    return Cache_.Space_->AdvanceBucketResizePhase(EBucketResizePhase::Activated);
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::PublishBucketSplit(TSpaceOperation& spaceOp) noexcept {
    const TBucketResizeState resize = Cache_.Space_->BucketResizeState();
    if (!resize || resize.Phase() != EBucketResizePhase::Activated || resize.Cursor() >= resize.PairCount())
    {
        return false;
    }
    if (!Cache_.Space_->PublishBucketResizeCursor(resize.Cursor() + 1)) {
        return false;
    }
    const THandleState markerState =
        THandleState::FromRaw(spaceOp.Handles()[ResizeMarkerIndex].State.load(std::memory_order_relaxed));
    const TCacheItem marker = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterBucketResizeCursorPublished, marker);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::CloseBucketSplit(TSpaceOperation& spaceOp) noexcept {
    const TBucketResizeState resize = Cache_.Space_->BucketResizeState();
    if (!resize || resize.Direction() != EBucketResize::Shrinking ||
        resize.Phase() != EBucketResizePhase::CursorPublished || resize.Cursor() == 0)
    {
        return false;
    }
    const ui32 bucket = resize.Cursor() - 1;
    const TCacheItem marker =
        TCacheItem::FromRaw(spaceOp.Buckets()[bucket | resize.SplitBit()].load(std::memory_order_acquire));
    if (!marker.IsResizeMarker()) {
        return false;
    }
    ui64 expected = marker.Raw();
    if (!spaceOp.Buckets()[bucket | resize.SplitBit()].compare_exchange_strong(
            expected, TCacheItem::ClosedBucketHead().Raw(), std::memory_order_release, std::memory_order_acquire) &&
        expected != TCacheItem::ClosedBucketHead().Raw())
    {
        return false;
    }
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterShrinkBucketClosed, marker);
    return Cache_.Space_->AdvanceBucketResizePhase(EBucketResizePhase::Closed);
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::RemoveBucketSplit(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    const TBucketResizeState resize = Cache_.Space_->BucketResizeState();
    if (!resize || (resize.Direction() == EBucketResize::Shrinking && resize.Phase() != EBucketResizePhase::Closed) ||
        (resize.Direction() == EBucketResize::Growing && resize.Phase() != EBucketResizePhase::CursorPublished) ||
        resize.Cursor() == 0)
    {
        return false;
    }

    const THandleState markerState =
        THandleState::FromRaw(spaceOp.Handles()[ResizeMarkerIndex].State.load(std::memory_order_acquire));
    if (!markerState.IsBucketSplit() && !markerState.IsTombstone()) {
        return false;
    }
    const TCacheItem markerIdentity = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);

    const auto unlinkMarker = [&](const TOrderedLink& markerLink, TCacheItem successor,
                                  TOperationItemRef& ownerRef) noexcept {
        Cache_.InvokeHook(ESharedCacheHookPoint::BeforeBucketSplitLinkClaim, markerLink.Owner);
        if (!markerLink.Owner.IsNull()) {
            ownerRef.Get() = TryAcquire(spaceOp, markerLink.Owner);
            if (!ownerRef) {
                return false;
            }
        }
        const TCacheItem replacement = successor.IsNull() ? TCacheItem::Make(markerLink.Owner.Version(), 0) : successor;
        ui64 expected = markerIdentity.Raw();
        Cache_.InvokeHook(ESharedCacheHookPoint::BeforeTableLinkCas, markerLink.Owner);
        const bool removed = markerLink.Link->compare_exchange_strong(
            expected, replacement.Raw(), std::memory_order_release, std::memory_order_acquire);
        if (removed) {
            Cache_.InvokeHook(ESharedCacheHookPoint::AfterTableLinkCas, markerLink.Owner);
        }
        return removed;
    };

    const ui32 bucket = resize.Cursor() - 1;
    // Releasing a Cold predecessor may reclaim another page; keep both refs until the marker is retired.
    TOperationItemRef oldOwnerRef(spaceOp);
    TOperationItemRef markerOwnerRef(spaceOp);
    if (resize.Direction() == EBucketResize::Growing && markerState.IsBucketSplit()) {
        TOrderedLink oldLink;
        if (!FindLinkOnBucket(spaceOp, markerIdentity, bucket, oldLink)) {
            return false;
        }
        if (!unlinkMarker(oldLink, {}, oldOwnerRef)) {
            return false;
        }
    }
    // A Tombstone marker must have only one incoming link, so ordinary helpers can retire it safely.
    if (!TransitionSplitMarkerToTombstone(spaceOp, markerIdentity) ||
        !SplitMarkerIsSoleOwner(spaceOp, markerIdentity)) {
        return false;
    }
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterBucketSplitOwnerCheck, markerIdentity);
    const ui32 markerBucket = resize.Direction() == EBucketResize::Growing ? bucket | resize.SplitBit() : bucket;
    TOrderedLink markerLink;
    if (FindLinkOnBucket(spaceOp, markerIdentity, markerBucket, markerLink)) {
        const TCacheItem successor =
            TCacheItem::FromRaw(spaceOp.Handles()[ResizeMarkerIndex].Next.load(std::memory_order_acquire))
                .WithoutFrozen();
        if (!unlinkMarker(markerLink, successor, markerOwnerRef)) {
            return false;
        }
    }
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterBucketSplitRemoved, markerIdentity);
    cacheOp.Release(markerIdentity);
    const ui32 pairCount = resize.PairCount();
    if (resize.Cursor() < pairCount) {
        return Cache_.Space_->PrepareNextBucketResizePair();
    }
    return true;
}

#undef TOperationItemRef
#undef TSharedCacheTable
#undef SHARED_CACHE_TEMPLATE

// Resize members are instantiated here; other table members are instantiated in shared_cache_table.cpp.
template bool TSharedCacheTableImpl<TProdTraits>::FindSplitBoundary(
    const TSpaceOperation&, ui32, ui32, bool, TSharedCacheTableImpl<TProdTraits>::TOrderedLink&) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::PrepareSplitMarker(const TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::TransitionSplitMarkerToTombstone(
    const TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::SplitMarkerIsSoleOwner(
    const TSpaceOperation&, TCacheItem) const noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::PrepareBucketSplit(TSpaceOperation&) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::ActivateBucketSplitOrMerge(TSpaceOperation&) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::ActivateBucketSplit(
    TSpaceOperation&, const TBucketResizeState&, TCacheItem) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::ActivateBucketMerge(
    TSpaceOperation&, const TBucketResizeState&, TCacheItem) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::PublishBucketSplit(TSpaceOperation&) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::CloseBucketSplit(TSpaceOperation&) noexcept;
template bool TSharedCacheTableImpl<TProdTraits>::RemoveBucketSplit(TSpaceOperation&) noexcept;

template bool TSharedCacheTableImpl<TTestTraits>::FindSplitBoundary(
    const TSpaceOperation&, ui32, ui32, bool, TSharedCacheTableImpl<TTestTraits>::TOrderedLink&) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::PrepareSplitMarker(const TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::TransitionSplitMarkerToTombstone(
    const TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::SplitMarkerIsSoleOwner(
    const TSpaceOperation&, TCacheItem) const noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::PrepareBucketSplit(TSpaceOperation&) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::ActivateBucketSplitOrMerge(TSpaceOperation&) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::ActivateBucketSplit(
    TSpaceOperation&, const TBucketResizeState&, TCacheItem) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::ActivateBucketMerge(
    TSpaceOperation&, const TBucketResizeState&, TCacheItem) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::PublishBucketSplit(TSpaceOperation&) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::CloseBucketSplit(TSpaceOperation&) noexcept;
template bool TSharedCacheTableImpl<TTestTraits>::RemoveBucketSplit(TSpaceOperation&) noexcept;

} // namespace NKikimr::NSharedCache
