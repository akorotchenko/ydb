#include "cache.h"
#include "cache_traits.h"

#include <util/generic/bitops.h>

#include <new>

namespace NKikimr::NSharedCache {

#define CACHE_TEMPLATE template <class TTraits>
#define TCache TCacheImpl<TTraits>
#define TCacheItemRef TCacheItemRefImpl<TTraits>
#define TCacheTable TCacheTableImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

int CompareCacheKeys(const TCacheKey& left, ui32 leftHash, const TCacheKey& right, ui32 rightHash) noexcept {
    if (right.IsPageKind()) {
        return NDetail::CompareKnownCacheKeys<true>(left, leftHash, right, rightHash);
    }
    return NDetail::CompareKnownCacheKeys<false>(left, leftHash, right, rightHash);
}

void StoreCacheKey(THandle& handle, const TCacheKey& key) noexcept {
    handle.Key0.store(key.Word(0), std::memory_order_release);
    handle.Key1.store(key.Word(1), std::memory_order_release);
    handle.Key2OrSize.store(key.Word(2), std::memory_order_release);
}

TCacheKey LoadCacheKey(const THandle& handle, EItemKind kind) noexcept {
    const ui64 word0 = handle.Key0.load(std::memory_order_relaxed);
    const ui64 word1 = handle.Key1.load(std::memory_order_relaxed);
    const ui64 word2 = handle.Key2OrSize.load(std::memory_order_relaxed);
    return TCacheKey::FromWords(kind, word0, word1, word2);
}

CACHE_TEMPLATE
TCacheItemRef TCacheTable::TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    return cacheOp.TryAcquireStructural(cacheItem);
}

CACHE_TEMPLATE
ECacheResultStatus TCacheTable::FindPageForInsert(
    TSpaceOperation& spaceOp, const TCacheKey& key, TCacheItem& cacheItem, TInsertPosition& insertPosition) noexcept {
    const auto collection = TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(key.Word(0)));
    return FindOrInsert<true, true>(
        spaceOp, key, HashPageKey(collection, key.Word(1)), spaceOp.EntrySpaceState(), cacheItem, &insertPosition);
}

CACHE_TEMPLATE
ECacheResultStatus TCacheTable::FindCollectionForInsert(
    TSpaceOperation& spaceOp, const TCacheKey& key, TCacheItem& cacheItem, TInsertPosition& insertPosition) noexcept {
    const TLogoBlobID id(key.Word(0), key.Word(1), key.Word(2));
    return FindOrInsert<true, false>(
        spaceOp, key, HashCollectionKey(id), spaceOp.EntrySpaceState(), cacheItem, &insertPosition);
}

CACHE_TEMPLATE
Y_FORCE_INLINE typename TCacheTable::TBucketRoute TCacheTable::SelectBucket(
    ui32 hash, TSpaceState spaceState) const noexcept {
    if (Y_UNLIKELY(spaceState.Resizing())) {
        return SelectResizeBucket(hash);
    }
    const ui32 bucket = hash & spaceState.BucketMask();
    return { .Bucket = bucket, .BaseBucket = bucket };
}

CACHE_TEMPLATE
Y_FORCE_INLINE typename TCacheTable::TBucketRoute TCacheTable::SelectResizeBucket(ui32 hash) const noexcept {
    const TBucketResizeState resize = Cache_.Space_->BucketResizeState();
    const ui32 splitBit = resize.SplitBit();
    const ui32 base = hash & (splitBit - 1);
    const bool suffix = (hash & splitBit) != 0;
    if (resize.Direction() == EBucketResize::Growing) {
        if (suffix && base < resize.Cursor()) {
            return { .Resize = resize, .Bucket = base | splitBit, .BaseBucket = base, .FollowsSplitSuffix = true };
        }
        return { .Resize = resize, .Bucket = base, .BaseBucket = base, .FollowsSplitSuffix = suffix };
    }

    if (suffix && base >= resize.Cursor()) {
        return { .Resize = resize, .Bucket = base | splitBit, .BaseBucket = base, .FollowsSplitSuffix = true };
    }
    return { .Resize = resize, .Bucket = base, .BaseBucket = base, .FollowsSplitSuffix = suffix };
}

CACHE_TEMPLATE
template <typename TCacheTable::ETombstoneMode Mode>
Y_FORCE_INLINE bool TCacheTable::SelectGrowthBucket(TBucketRoute& route, const TOrderedLink& link) noexcept {
    const TBucketResizeState& resize = route.Resize;
    if (!resize || resize.Direction() != EBucketResize::Growing) {
        return false;
    }
    if constexpr (Mode == ETombstoneMode::Help) {
        if (resize.Phase() != EBucketResizePhase::Activated) {
            return false;
        }
    } else if (resize.Phase() != EBucketResizePhase::Prepared && resize.Phase() != EBucketResizePhase::Activated)
    {
        return false;
    }
    if (route.FollowsSplitSuffix && route.Bucket == route.BaseBucket && !link.SawSplit &&
        route.BaseBucket == resize.Cursor())
    {
        route.Bucket = route.BaseBucket | resize.SplitBit();
        return true;
    }
    return false;
}

CACHE_TEMPLATE
template <typename TCacheTable::ETombstoneMode Mode, bool IsPage>
typename TCacheTable::EBucketRouteResult TCacheTable::FindWithResize(TSpaceOperation& spaceOp, const TCacheKey& key,
    ui32 hash, TSpaceState& spaceState, TBucketRoute& route, TOrderedLink& result) noexcept {
    route = SelectResizeBucket(hash);
    for (;;) {
        const EBucketRouteResult status = FindRoute<Mode, IsPage, true>(spaceOp, key, hash, spaceState, route, result);
        if (status != EBucketRouteResult::Next) {
            return status;
        }
    }
}

CACHE_TEMPLATE
bool TCacheTable::InsertAt(TSpaceOperation& spaceOp, TInsertPosition& insertPosition, TCacheItem candidate) noexcept {
    TSpaceState spaceState = insertPosition.SpaceState;
    TBucketResizeState resizeState = insertPosition.ResizeState;
    if (!Cache_.Space_->RefreshBucketRoute(spaceState, resizeState)) {
        insertPosition.OwnerRef.Drop(spaceOp);
        return false;
    }

    const ui32 candidateIndex = candidate.Index();
    Y_DEBUG_ABORT_UNLESS(candidateIndex >= 2 && candidateIndex < spaceOp.AccessibleHandleCount());
    THandle& handle = spaceOp.Handles()[candidateIndex];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(candidate.Matches(state) && !state.IsFree() && !state.IsTombstone() && state.Refs() == 0);

    const TCacheItem successor =
        insertPosition.Expected.IsNull() ? TCacheItem::Make(state.Version(), 0) : insertPosition.Expected;
    handle.Next.store(successor.Raw(), std::memory_order_release);
    ui64 expectedRaw = insertPosition.Expected.Raw();
    Cache_.InvokeHook(ECacheHookPoint::BeforeTableLinkCas, insertPosition.Owner);
    const bool inserted = insertPosition.Link->compare_exchange_strong(
        expectedRaw, candidate.Raw(), std::memory_order_release, std::memory_order_relaxed);
    if (inserted) {
        Cache_.InvokeHook(ECacheHookPoint::AfterTableLinkCas, insertPosition.Owner);
    }
    insertPosition.OwnerRef.Drop(spaceOp);
    return inserted;
}

CACHE_TEMPLATE
bool TCacheTable::FindLinkOnBucket(
    const TSpaceOperation& spaceOp, TCacheItem target, ui32 bucket, TOrderedLink& result) noexcept {
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
        const TCacheItem current = result.Expected.WithoutFrozen();
        if (current.IsNull()) {
            return false;
        }
        if (current == target) {
            result.Equal = true;
            result.Expected = current;
            return true;
        }
        if (!spaceOp.Contains(current)) {
            goto restart;
        }

        THandle& handle = spaceOp.Handles()[current.Index()];
        const TCacheItem next = TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire));
        Cache_.InvokeHook(ECacheHookPoint::BeforeTableItemValidation, current);
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        if (!current.Matches(state) || state.IsFree()) {
            goto restart;
        }
        result.Link = &handle.Next;
        result.Owner = current;
        result.Expected = next;
        atBucketHead = false;
    }
}

CACHE_TEMPLATE
TCacheItemRef TCacheTable::TryAcquireTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto ref = Cache_.Items_.BindOperation(spaceOp).TryAcquireRetiredItemRef(cacheItem);
    if (ref) {
        Cache_.InvokeHook(ECacheHookPoint::AfterTombstoneHelperClaim, cacheItem);
    }
    return ref;
}

CACHE_TEMPLATE
TCacheItem TCacheTable::FreezeNext(const TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 nextRaw = handle.Next.load(std::memory_order_acquire);
    for (;;) {
        const TCacheItem next = TCacheItem::FromRaw(nextRaw);
        if (next.IsFrozen()) {
            return next;
        }
        const TCacheItem desired = next.WithFrozen();
        if (handle.Next.compare_exchange_weak(
                nextRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return desired;
        }
        nextRaw = handle.Next.load(std::memory_order_acquire);
    }
}

CACHE_TEMPLATE
bool TCacheTable::FindLink(
    TSpaceOperation& spaceOp, const TCacheKey& key, TCacheItem target, TOrderedLink& result) noexcept {
    const ui32 hash = key.Hash();
    TSpaceState spaceState = spaceOp.EntrySpaceState();
    for (;;) {
        TBucketRoute route = SelectBucket(hash, spaceState);
        if (FindLinkOnBucket(spaceOp, target, route.Bucket, result)) {
            return true;
        }
        if (route.Resize) {
            const ui32 peer = route.BaseBucket | route.Resize.SplitBit();
            if (peer != route.Bucket && FindLinkOnBucket(spaceOp, target, peer, result)) {
                return true;
            }
        }
        if (Cache_.Space_->RefreshBucketRoute(spaceState, route.Resize)) {
            return false;
        }
    }
}

CACHE_TEMPLATE
bool TCacheTable::UnlinkTombstone(TCacheItem cacheItem, const TCacheKey& key, TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    const TCacheItem successor = FreezeNext(spaceOp, cacheItem).WithoutFrozen();
    TCacheItem leftmostTombstone;
    TCacheItem rightmostTombstone;
    TCacheItem current = cacheItem;

    for (;;) {
        TCacheKey currentKey = key;
        if (current != cacheItem && !current.IsResizeMarker()) {
            THandle& tombstone = spaceOp.Handles()[current.Index()];
            const THandleState state = THandleState::FromRaw(tombstone.State.load(std::memory_order_relaxed));
            Y_DEBUG_ABORT_UNLESS(current.Matches(state) && state.IsTombstone() && state.Refs() > 0);
            currentKey = LoadCacheKey(tombstone, state.Kind());
        }
        TOrderedLink orderedLink;
        if (!FindLink(spaceOp, currentKey, current, orderedLink)) {
            if (current == cacheItem) {
                Y_DEBUG_ABORT_UNLESS(leftmostTombstone.IsNull());
                return true;
            }

            THandle& tombstone = spaceOp.Handles()[current.Index()];
            const TCacheItem next = TCacheItem::FromRaw(tombstone.Next.load(std::memory_order_acquire)).WithoutFrozen();
            const bool wasRightmost = current == rightmostTombstone;
            cacheOp.ReleaseCleanup(current);
            if (wasRightmost) {
                leftmostTombstone = {};
                rightmostTombstone = {};
                current = cacheItem;
            } else {
                leftmostTombstone = next;
                current = next;
            }
            continue;
        }

        bool removed;
        {
            TOperationItemRef ownerRef(spaceOp);
            if (!orderedLink.Owner.IsNull()) {
                const THandleState ownerState = THandleState::FromRaw(
                    spaceOp.Handles()[orderedLink.Owner.Index()].State.load(std::memory_order_relaxed));
                if (!orderedLink.Owner.Matches(ownerState)) {
                    continue;
                }
                if (ownerState.IsTombstone()) {
                    auto tombstoneRef = TryAcquireTombstone(spaceOp, orderedLink.Owner);
                    if (!tombstoneRef) {
                        continue;
                    }
                    const TCacheItem frozenNext = FreezeNext(spaceOp, orderedLink.Owner).WithoutFrozen();
                    if (frozenNext != current) {
                        tombstoneRef.Drop(spaceOp);
                        continue;
                    }
                    tombstoneRef.Disarm();
                    if (leftmostTombstone.IsNull()) {
                        rightmostTombstone = orderedLink.Owner;
                    }
                    leftmostTombstone = orderedLink.Owner;
                    current = orderedLink.Owner;
                    continue;
                }
                ownerRef.Get() = TryAcquire(spaceOp, orderedLink.Owner);
                if (!ownerRef) {
                    continue;
                }
                if (orderedLink.Link->load(std::memory_order_acquire) != orderedLink.Expected.Raw()) {
                    continue;
                }
            }

            TCacheItem replacement = successor;
            if (successor.IsNull()) {
                replacement =
                    orderedLink.Owner.IsNull() ? TCacheItem{} : TCacheItem::Make(orderedLink.Owner.Version(), 0);
            }
            ui64 expectedRaw = orderedLink.Expected.Raw();
            removed = orderedLink.Link->compare_exchange_strong(
                expectedRaw, replacement.Raw(), std::memory_order_release, std::memory_order_relaxed);
        }
        if (removed) {
            current = leftmostTombstone;
            while (!current.IsNull()) {
                THandle& tombstone = spaceOp.Handles()[current.Index()];
                const TCacheItem next =
                    TCacheItem::FromRaw(tombstone.Next.load(std::memory_order_acquire)).WithoutFrozen();
                const bool wasRightmost = current == rightmostTombstone;
                cacheOp.ReleaseCleanup(current);
                if (wasRightmost) {
                    break;
                }
                current = next;
            }
            Cache_.NotifyResourcesAvailable();
            return true;
        }
    }
}

CACHE_TEMPLATE
void TCacheTable::PublishPageReplacement(
    TCacheItem source, TCacheItem replacement, const TCacheKey& key, TSpaceOperation& spaceOp) noexcept {
    for (;;) {
        TOrderedLink orderedLink;
        const bool found = FindLink(spaceOp, key, source, orderedLink);
        Y_DEBUG_ABORT_UNLESS(found);
        if (!found) {
            return;
        }

        bool replaced;
        {
            TOperationItemRef ownerRef(spaceOp);
            if (!orderedLink.Owner.IsNull()) {
                const THandleState ownerState = THandleState::FromRaw(
                    spaceOp.Handles()[orderedLink.Owner.Index()].State.load(std::memory_order_relaxed));
                if (!orderedLink.Owner.Matches(ownerState)) {
                    continue;
                }
                if (ownerState.IsTombstone()) {
                    TryFinishErase(orderedLink.Owner, key, spaceOp);
                    continue;
                }
                ownerRef.Get() = TryAcquire(spaceOp, orderedLink.Owner);
                if (!ownerRef) {
                    continue;
                }
                if (orderedLink.Link->load(std::memory_order_acquire) != orderedLink.Expected.Raw()) {
                    continue;
                }
            }

            ui64 expectedRaw = source.Raw();
            Cache_.InvokeHook(ECacheHookPoint::BeforeTableLinkCas, orderedLink.Owner);
            replaced = orderedLink.Link->compare_exchange_strong(
                expectedRaw, replacement.Raw(), std::memory_order_release, std::memory_order_relaxed);
            if (replaced) {
                Cache_.InvokeHook(ECacheHookPoint::AfterTableLinkCas, orderedLink.Owner);
            }
        }
        if (replaced) {
            return;
        }
    }
}

CACHE_TEMPLATE
void TCacheTable::TryFinishErase(TCacheItem cacheItem, const TCacheKey& routeKey, TSpaceOperation& spaceOp) noexcept {
    TOperationItemRef hold(spaceOp, TryAcquireTombstone(spaceOp, cacheItem));
    if (!hold) {
        return;
    }
    const TCacheItem tombstone = hold.CacheItem();
    THandle& handle = spaceOp.Handles()[tombstone.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    if (tombstone.Matches(state) && state.IsTombstone()) {
        const TCacheKey key = tombstone.IsResizeMarker() ? routeKey : LoadCacheKey(handle, state.Kind());
        FreezeNext(spaceOp, tombstone);
        UnlinkTombstone(tombstone, key, spaceOp);
    }
}

CACHE_TEMPLATE
bool TCacheTable::TryBeginErase(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, THandleState& expected, TCacheItemRef& owner) noexcept {
    Y_DEBUG_ABORT_UNLESS(!owner && !expected.IsCompleting());
    if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen() || !cacheItem.Matches(expected)) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
    const THandleState desired = expected.IncrementRefs()
                                     .WithState(EHandleState::Tombstone)
                                     .WithFrequency(0)
                                     .WithPageAttachment(expected.HasPageAttachment())
                                     .WithShrinkWaiter(expected.HasShrinkWaiter());
    ui64 expectedRaw = expected.Raw();
    if (!spaceOp.Handles()[cacheItem.Index()].State.compare_exchange_strong(
            expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed)) {
        expected = THandleState::FromRaw(expectedRaw);
        return false;
    }
    owner = TCacheItemRef(cacheItem);
    Cache_.Items_.RetirePayload(spaceOp, spaceOp.Handles()[cacheItem.Index()], expected);
    return true;
}

CACHE_TEMPLATE
bool TCacheTable::TryBeginErase(TSpaceOperation& spaceOp, TCacheItemRef& owner, THandleState& expected) noexcept {
    Y_DEBUG_ABORT_UNLESS(owner && expected.Refs() > 0);
    const TCacheItem cacheItem = owner.CacheItem();
    if (!cacheItem.Matches(expected)) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    if (expected.IsCompleting()) {
        Y_DEBUG_ABORT_UNLESS(expected.IsPageKind() && expected.Refs() == 1);
        handle.State.store(expected.WithState(EHandleState::Tombstone).WithSticky(EStickyState::None).Raw(),
            std::memory_order_release);
        return true;
    }
    const THandleState desired = expected.WithState(EHandleState::Tombstone)
                                     .WithFrequency(0)
                                     .WithPageAttachment(expected.HasPageAttachment())
                                     .WithShrinkWaiter(expected.HasShrinkWaiter());
    ui64 expectedRaw = expected.Raw();
    if (!handle.State.compare_exchange_strong(
            expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
        expected = THandleState::FromRaw(expectedRaw);
        return false;
    }
    Cache_.Items_.RetirePayload(spaceOp, handle, expected);
    return true;
}

CACHE_TEMPLATE
void TCacheTable::FinishErase(TSpaceOperation& spaceOp, TCacheItem cacheItem, const TCacheKey& key) noexcept {
    const THandleState state =
        THandleState::FromRaw(spaceOp.Handles()[cacheItem.Index()].State.load(std::memory_order_acquire));
    Y_ABORT_UNLESS(cacheItem.Matches(state) && state.IsTombstone() && state.Refs() > 0);
    FreezeNext(spaceOp, cacheItem);
    UnlinkTombstone(cacheItem, key, spaceOp);
}

#undef TCacheTable
#undef TCacheItemRef
#undef TOperationItemRef
#undef TCache
#undef CACHE_TEMPLATE

template class TCacheTableImpl<TProdTraits>;
template class TCacheTableImpl<TTestTraits>;

} // namespace NKikimr::NSharedCache
