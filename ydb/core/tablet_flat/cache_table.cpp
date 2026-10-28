#include "cache.h"
#include "cache_traits.h"

#include <util/generic/bitops.h>

#include <new>

namespace NKikimr::NSharedCache {
namespace {

    Y_FORCE_INLINE ui64 HashCombine(ui64 hash, ui64 value) noexcept {
        hash ^= 0x9E3779B97F4A7C15ULL + value;
        hash ^= hash >> 30;
        hash *= 0xBF58476D1CE4E5B9ULL;
        hash ^= hash >> 27;
        hash *= 0x94D049BB133111EBULL;
        hash ^= hash >> 31;
        return hash;
    }

    Y_FORCE_INLINE ui32 FinishHash(ui64 hash) noexcept {
        return static_cast<ui32>(hash ^ (hash >> 32));
    }

    int CompareWords(const TCacheKey& left, const TCacheKey& right) noexcept {
        const ui32 wordCount = left.IsPageKind() ? 2 : 3;
        for (ui32 index = 0; index < wordCount; ++index) {
            if (left.Word(index) < right.Word(index)) {
                return -1;
            }
            if (left.Word(index) > right.Word(index)) {
                return 1;
            }
        }
        return 0;
    }

} // anonymous namespace

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TCacheItemRef TCacheItemRefImpl<TTraits>
#define TCacheTable TCacheTableImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

ui32 HashPageKey(TCollectionCacheItem collection, ui64 offset) noexcept {
    ui64 hash = HashCombine(offset, collection.Raw());
    return FinishHash(hash);
}

ui32 HashCollectionKey(const TLogoBlobID& id) noexcept {
    const ui64* words = id.GetRaw();
    ui64 hash = HashCombine(words[0], words[1]);
    return FinishHash(HashCombine(hash, words[2]));
}

int CompareSharedCacheKeys(const TCacheKey& left, ui32 leftHash, const TCacheKey& right, ui32 rightHash) noexcept {
    const ui32 leftOrder = ReverseBits(leftHash);
    const ui32 rightOrder = ReverseBits(rightHash);
    if (leftOrder < rightOrder) {
        return -1;
    }
    if (leftOrder > rightOrder) {
        return 1;
    }
    if (left.Kind() < right.Kind()) {
        return -1;
    }
    if (left.Kind() > right.Kind()) {
        return 1;
    }
    return CompareWords(left, right);
}

void StoreSharedCacheKey(THandle& handle, const TCacheKey& key) noexcept {
    handle.Key0.store(key.Word(0), std::memory_order_relaxed);
    handle.Key1.store(key.Word(1), std::memory_order_relaxed);
    handle.Key2OrSize.store(key.Word(2), std::memory_order_relaxed);
}

TCacheKey LoadSharedCacheKey(const THandle& handle, EItemKind kind) noexcept {
    const ui64 word0 = handle.Key0.load(std::memory_order_relaxed);
    const ui64 word1 = handle.Key1.load(std::memory_order_relaxed);
    const ui64 word2 = handle.Key2OrSize.load(std::memory_order_relaxed);
    return TCacheKey::FromWords(kind, word0, word1, word2);
}

SHARED_CACHE_TEMPLATE
template <typename TCacheTable::ETombstoneMode Mode>
bool TCacheTable::FindLinkOnBucket(TSpaceOperation& spaceOp, const TCacheKey& key, ui32 hash, ui32 bucket,
    bool followsSplitSuffix, TOrderedLink& result) noexcept {
    if constexpr (Mode == ETombstoneMode::Traverse) {
        Y_UNUSED(spaceOp);
    }

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
        if (result.Expected.IsFrozen() || !spaceOp.Contains(result.Expected)) {
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
            result.SawSplit = true;
            if (!followsSplitSuffix) {
                return true;
            }
            result.Link = &handle.Next;
            result.Owner = TCacheItem::Make(state.Version(), result.Expected.Index());
            result.Expected = next.WithoutFrozen();
            atBucketHead = false;
            continue;
        }
        const TCacheKey itemKey = TCacheKey::FromWords(state.Kind(), key0, key1, key2);
        const int comparison = CompareSharedCacheKeys(itemKey, itemKey.Hash(), key, hash);
        if (state.IsTombstone()) {
            if constexpr (Mode == ETombstoneMode::Help) {
                TryFinishErase(result.Expected, key, spaceOp);
                goto restart;
            }
            if (comparison >= 0) {
                result.Equal = false;
                return true;
            }
            result.Link = &handle.Next;
            result.Owner = TCacheItem::Make(state.Version(), result.Expected.Index());
            result.Expected = next.WithoutFrozen();
            atBucketHead = false;
            continue;
        }
        if (next.IsFrozen()) {
            goto restart;
        }
        if (comparison >= 0) {
            result.Equal = comparison == 0;
            return true;
        }

        result.Link = &handle.Next;
        result.Owner = TCacheItem::Make(state.Version(), result.Expected.Index());
        result.Expected = next;
        atBucketHead = false;
    }
}

SHARED_CACHE_TEMPLATE
bool TCacheTable::ValidateTerminalOwner(
    const TSpaceOperation& spaceOp, const TOrderedLink& orderedLink) const noexcept {
    if (orderedLink.Owner.IsNull()) {
        return true;
    }
    if (orderedLink.Expected.Version() != orderedLink.Owner.Version() || !spaceOp.Contains(orderedLink.Owner))
    {
        return false;
    }

    const THandleState ownerState =
        THandleState::FromRaw(spaceOp.Handles()[orderedLink.Owner.Index()].State.load(std::memory_order_acquire));
    return orderedLink.Owner.Matches(ownerState) && !ownerState.IsFree();
}

SHARED_CACHE_TEMPLATE
TCacheItemRef TCacheTable::TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    return cacheOp.TryAcquireStructural(cacheItem);
}

SHARED_CACHE_TEMPLATE
ECacheResultStatus TCacheTable::Find(TSpaceOperation& spaceOp, const TCacheKey& key, TCacheItem& cacheItem) noexcept {
    return FindOrInsert<false>(spaceOp, key, cacheItem);
}

SHARED_CACHE_TEMPLATE
ECacheResultStatus TCacheTable::FindForInsert(
    TSpaceOperation& spaceOp, const TCacheKey& key, TCacheItem& cacheItem, TInsertPosition& insertPosition) noexcept {
    return FindOrInsert<true>(spaceOp, key, cacheItem, &insertPosition);
}

SHARED_CACHE_TEMPLATE
Y_FORCE_INLINE typename TCacheTable::TBucketRoute TCacheTable::SelectBucket(
    ui32 hash, TSpaceState spaceState) const noexcept {
    if (!spaceState.Resizing()) {
        const ui32 bucket = hash & spaceState.BucketMask();
        return { .Bucket = bucket, .BaseBucket = bucket };
    }

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

SHARED_CACHE_TEMPLATE
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

SHARED_CACHE_TEMPLATE
template <typename TCacheTable::ETombstoneMode Mode>
Y_FORCE_INLINE typename TCacheTable::EBucketRouteResult TCacheTable::ResolveBucketMiss(
    TSpaceState& spaceState, TBucketRoute& route, const TOrderedLink& link) const noexcept {
    if (!Cache_.Space_->RefreshBucketRoute(spaceState, route.Resize)) {
        return EBucketRouteResult::Retry;
    }
    if (SelectGrowthBucket<Mode>(route, link)) {
        return EBucketRouteResult::Next;
    }
    return EBucketRouteResult::Done;
}

SHARED_CACHE_TEMPLATE
template <bool Insert>
Y_FORCE_INLINE ECacheResultStatus TCacheTable::FindOrInsert(
    TSpaceOperation& spaceOp, const TCacheKey& key, TCacheItem& cacheItem, TInsertPosition* insertPosition) noexcept {
    static constexpr ETombstoneMode Mode = Insert ? ETombstoneMode::Help : ETombstoneMode::Traverse;
    const ui32 hash = key.Hash();
    TSpaceState spaceState = spaceOp.EntrySpaceState();
    if constexpr (!Insert) {
        // Exact page-head hits do not need the ordered-key comparison
        if (key.IsPageKind() && !spaceState.Resizing()) {
            const ui32 bucket = hash & spaceState.BucketMask();
            const TCacheItem head = TCacheItem::FromRaw(spaceOp.Buckets()[bucket].load(std::memory_order_acquire));
            if (!head.IsFrozen() && spaceOp.Contains(head)) {
                const THandle& handle = spaceOp.Handles()[head.Index()];
                const TCacheItem next = TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire));
                const ui64 collection = handle.Key0.load(std::memory_order_relaxed);
                const ui64 offset = handle.Key1.load(std::memory_order_relaxed);
                const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
                if (head.Matches(state) && state.IsPageKind() && state.IsReady() && !next.IsFrozen() &&
                    collection == key.Word(0) && offset == key.Word(1)) {
                    cacheItem = head;
                    return ECacheResultStatus::Hit;
                }
            }
        }
    }
    for (;;) {
        TBucketRoute route = SelectBucket(hash, spaceState);
        TOrderedLink orderedLink;
        bool retry = false;
        for (;;) {
            if (!FindLinkOnBucket<Mode>(spaceOp, key, hash, route.Bucket, route.FollowsSplitSuffix, orderedLink))
            {
                Cache_.Space_->RefreshBucketRoute(spaceState, route.Resize);
                retry = true;
                break;
            }
            if (orderedLink.Equal) {
                break;
            }
            const EBucketRouteResult routeResult = ResolveBucketMiss<Mode>(spaceState, route, orderedLink);
            retry = routeResult == EBucketRouteResult::Retry;
            if (routeResult != EBucketRouteResult::Next) {
                break;
            }
        }
        if (retry) {
            continue;
        }
        if (orderedLink.Equal) {
            cacheItem = orderedLink.Expected;
            return ECacheResultStatus::Hit;
        }
        if constexpr (!Insert) {
            cacheItem = {};
            return ECacheResultStatus::Miss;
        }
        Y_DEBUG_ABORT_UNLESS(insertPosition);

        TOperationItemRef ownerRef(spaceOp);
        if (!orderedLink.Owner.IsNull()) {
            Cache_.InvokeHook(ECacheHookPoint::BeforeInsertOwnerAcquire, orderedLink.Owner);
            ownerRef.Get() = TryAcquire(spaceOp, orderedLink.Owner);
            if (!ownerRef) {
                continue;
            }
            const THandleState ownerState = THandleState::FromRaw(
                spaceOp.Handles()[orderedLink.Owner.Index()].State.load(std::memory_order_relaxed));
            if (!orderedLink.Owner.Matches(ownerState) || ownerState.IsTombstone() || ownerState.IsFree() ||
                orderedLink.Link->load(std::memory_order_acquire) != orderedLink.Expected.Raw())
            {
                continue;
            }
        }
        insertPosition->Link = orderedLink.Link;
        insertPosition->Expected = orderedLink.Expected;
        insertPosition->Owner = orderedLink.Owner;
        insertPosition->OwnerRef = ownerRef.Take();
        insertPosition->SpaceState = spaceState;
        insertPosition->ResizeState = route.Resize;
        Cache_.InvokeHook(ECacheHookPoint::AfterInsertPositionFound, insertPosition->Owner);
        cacheItem = {};
        return ECacheResultStatus::Miss;
    }
}

SHARED_CACHE_TEMPLATE
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
    handle.Next.store(successor.Raw(), std::memory_order_relaxed);
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

SHARED_CACHE_TEMPLATE
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

SHARED_CACHE_TEMPLATE
TCacheItemRef TCacheTable::TryAcquireTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto ref = Cache_.Items_.BindOperation(spaceOp).TryAcquireRetiredItemRef(cacheItem);
    if (ref) {
        Cache_.InvokeHook(ECacheHookPoint::AfterTombstoneHelperClaim, cacheItem);
    }
    return ref;
}

SHARED_CACHE_TEMPLATE
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

SHARED_CACHE_TEMPLATE
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

SHARED_CACHE_TEMPLATE
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
            currentKey = LoadSharedCacheKey(tombstone, state.Kind());
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
            cacheOp.Release(current);
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
                cacheOp.Release(current);
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

SHARED_CACHE_TEMPLATE
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

SHARED_CACHE_TEMPLATE
void TCacheTable::TryFinishErase(TCacheItem cacheItem, const TCacheKey& routeKey, TSpaceOperation& spaceOp) noexcept {
    TOperationItemRef hold(spaceOp, TryAcquireTombstone(spaceOp, cacheItem));
    if (!hold) {
        return;
    }
    const TCacheItem tombstone = hold.CacheItem();
    THandle& handle = spaceOp.Handles()[tombstone.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    if (tombstone.Matches(state) && state.IsTombstone()) {
        const TCacheKey key = tombstone.IsResizeMarker() ? routeKey : LoadSharedCacheKey(handle, state.Kind());
        FreezeNext(spaceOp, tombstone);
        UnlinkTombstone(tombstone, key, spaceOp);
    }
}

SHARED_CACHE_TEMPLATE
bool TCacheTable::TryBeginErase(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, THandleState& expected, TCacheItemRef& owner) noexcept {
    Y_DEBUG_ABORT_UNLESS(!owner && !expected.IsCompleting());
    if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen() || !cacheItem.Matches(expected)) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
    const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
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

SHARED_CACHE_TEMPLATE
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
    const THandleState desired = expected.WithState(EHandleState::Tombstone).WithFrequency(0);
    ui64 expectedRaw = expected.Raw();
    if (!handle.State.compare_exchange_strong(
            expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
        expected = THandleState::FromRaw(expectedRaw);
        return false;
    }
    Cache_.Items_.RetirePayload(spaceOp, handle, expected);
    return true;
}

SHARED_CACHE_TEMPLATE
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
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

template class TCacheTableImpl<TProdTraits>;
template class TCacheTableImpl<TTestTraits>;

} // namespace NKikimr::NSharedCache
