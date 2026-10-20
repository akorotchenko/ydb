#include "shared_cache.h"
#include "shared_cache_traits.h"

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

    int CompareWords(const TSharedCacheKey& left, const TSharedCacheKey& right) noexcept {
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
#define TSharedCacheItemRef TSharedCacheItemRefImpl<TTraits>
#define TSharedCacheTable TSharedCacheTableImpl<TTraits>
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

ui32 TSharedCacheKey::Hash() const noexcept {
    return Kind_ == EItemKind::Page
               ? HashPageKey(TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(Words_[0])), Words_[1])
               : HashCollectionKey(TLogoBlobID(Words_.data()));
}

int CompareSharedCacheKeys(
    const TSharedCacheKey& left, ui32 leftHash, const TSharedCacheKey& right, ui32 rightHash) noexcept {
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

void StoreSharedCacheKey(THandle& handle, const TSharedCacheKey& key) noexcept {
    handle.Key0.store(key.Word(0), std::memory_order_relaxed);
    handle.Key1.store(key.Word(1), std::memory_order_relaxed);
    handle.Key2OrSize.store(key.Word(2), std::memory_order_relaxed);
}

TSharedCacheKey LoadSharedCacheKey(const THandle& handle, EItemKind kind) noexcept {
    const ui64 word0 = handle.Key0.load(std::memory_order_relaxed);
    const ui64 word1 = handle.Key1.load(std::memory_order_relaxed);
    const ui64 word2 = handle.Key2OrSize.load(std::memory_order_relaxed);
    return TSharedCacheKey::FromWords(kind, word0, word1, word2);
}

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef::TSharedCacheItemRefImpl(TCacheItem cacheItem) noexcept
    : CacheItem_(cacheItem)
{}

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef::TSharedCacheItemRefImpl(TSharedCacheItemRef&& other) noexcept
    : CacheItem_(std::exchange(other.CacheItem_, {}))
{}

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef& TSharedCacheItemRef::operator=(TSharedCacheItemRef&& other) noexcept {
    if (this != &other) {
        Drop();
        CacheItem_ = std::exchange(other.CacheItem_, {});
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef::~TSharedCacheItemRefImpl() {
    Drop();
}

SHARED_CACHE_TEMPLATE
void TSharedCacheItemRef::Drop() noexcept {
    const TCacheItem cacheItem = Take();
    if (!cacheItem.IsNull()) {
        // A missing resolver is the explicit terminal-shutdown abandonment path.
        if (TSharedCache* cache = TSharedCache::TryGet()) {
            auto binding = cache->BindCurrentThreadHazard();
            auto cacheOp = cache->BeginOperation();
            cacheOp.Release(cacheItem);
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCacheItemRef::Drop(TSpaceOperation& spaceOp) noexcept {
    const TCacheItem cacheItem = Take();
    if (!cacheItem.IsNull()) {
        TSharedCache::Get().Release(spaceOp, cacheItem);
    }
}

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCacheItemRef::Take() noexcept {
    return std::exchange(CacheItem_, {});
}

SHARED_CACHE_TEMPLATE
void TSharedCacheItemRef::Disarm() noexcept {
    Y_DEBUG_ABORT_UNLESS(!CacheItem_.IsNull());
    CacheItem_ = {};
}

SHARED_CACHE_TEMPLATE
template <typename TSharedCacheTable::ETombstoneMode Mode>
bool TSharedCacheTable::FindLinkOnBucket(TSpaceOperation& spaceOp, const TSharedCacheKey& key, ui32 hash, ui32 bucket,
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
        const TSharedCacheKey itemKey = TSharedCacheKey::FromWords(state.Kind(), key0, key1, key2);
        const int comparison = CompareSharedCacheKeys(itemKey, itemKey.Hash(), key, hash);
        if (state.IsTombstone()) {
            if constexpr (Mode == ETombstoneMode::Help) {
                HelpTombstone(result.Expected, key, spaceOp);
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
bool TSharedCacheTable::ValidateTerminalOwner(
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
TSharedCacheItemRef TSharedCacheTable::TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    return Cache_.TryAcquireStructural(spaceOp, cacheItem);
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCacheTable::Find(
    TSpaceOperation& spaceOp, const TSharedCacheKey& key, TCacheItem& cacheItem) noexcept {
    return FindOrInsert<false>(spaceOp, key, cacheItem);
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCacheTable::FindForInsert(TSpaceOperation& spaceOp, const TSharedCacheKey& key,
    TCacheItem& cacheItem, TInsertPosition& insertPosition) noexcept {
    return FindOrInsert<true>(spaceOp, key, cacheItem, &insertPosition);
}

SHARED_CACHE_TEMPLATE
Y_FORCE_INLINE typename TSharedCacheTable::TBucketRoute TSharedCacheTable::SelectBucket(
    ui32 hash, TSpaceState spaceState) const noexcept {
    if (!spaceState.Resizing()) {
        const ui32 bucket = hash & spaceState.BucketMask();
        return { .Bucket = bucket, .BaseBucket = bucket };
    }

    const TBucketResizeState resize = Space_.BucketResizeState();
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
template <typename TSharedCacheTable::ETombstoneMode Mode>
Y_FORCE_INLINE bool TSharedCacheTable::SelectGrowthBucket(TBucketRoute& route, const TOrderedLink& link) noexcept {
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
template <typename TSharedCacheTable::ETombstoneMode Mode>
Y_FORCE_INLINE typename TSharedCacheTable::EBucketRouteResult TSharedCacheTable::ResolveBucketMiss(
    TSpaceState& spaceState, TBucketRoute& route, const TOrderedLink& link) const noexcept {
    if (!Space_.RefreshBucketRoute(spaceState, route.Resize)) {
        return EBucketRouteResult::Retry;
    }
    if (SelectGrowthBucket<Mode>(route, link)) {
        return EBucketRouteResult::Next;
    }
    return EBucketRouteResult::Done;
}

SHARED_CACHE_TEMPLATE
template <bool Insert>
Y_FORCE_INLINE ESharedCacheResultStatus TSharedCacheTable::FindOrInsert(TSpaceOperation& spaceOp,
    const TSharedCacheKey& key, TCacheItem& cacheItem, TInsertPosition* insertPosition) noexcept {
    static constexpr ETombstoneMode Mode = Insert ? ETombstoneMode::Help : ETombstoneMode::Traverse;
    const ui32 hash = key.Hash();
    TSpaceState spaceState = spaceOp.EntrySpaceState();
    for (;;) {
        TBucketRoute route = SelectBucket(hash, spaceState);
        TOrderedLink orderedLink;
        bool retry = false;
        for (;;) {
            if (!FindLinkOnBucket<Mode>(spaceOp, key, hash, route.Bucket, route.FollowsSplitSuffix, orderedLink))
            {
                Space_.RefreshBucketRoute(spaceState, route.Resize);
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
            return ESharedCacheResultStatus::Hit;
        }
        if constexpr (!Insert) {
            cacheItem = {};
            return ESharedCacheResultStatus::Miss;
        }
        Y_DEBUG_ABORT_UNLESS(insertPosition);

        TOperationItemRef ownerRef(spaceOp);
        if (!orderedLink.Owner.IsNull()) {
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
        Cache_.InvokeHook(ESharedCacheHookPoint::AfterInsertPositionFound, insertPosition->Owner);
        cacheItem = {};
        return ESharedCacheResultStatus::Miss;
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::InsertAt(
    TSpaceOperation& spaceOp, TInsertPosition& insertPosition, TCacheItem candidate) noexcept {
    TSpaceState spaceState = insertPosition.SpaceState;
    TBucketResizeState resizeState = insertPosition.ResizeState;
    if (!Space_.RefreshBucketRoute(spaceState, resizeState)) {
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
    Cache_.InvokeHook(ESharedCacheHookPoint::BeforeTableLinkCas, insertPosition.Owner);
    const bool inserted = insertPosition.Link->compare_exchange_strong(
        expectedRaw, candidate.Raw(), std::memory_order_release, std::memory_order_relaxed);
    if (inserted) {
        Cache_.InvokeHook(ESharedCacheHookPoint::AfterTableLinkCas, insertPosition.Owner);
    }
    insertPosition.OwnerRef.Drop(spaceOp);
    return inserted;
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::FindLinkOnBucket(
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
TSharedCacheItemRef TSharedCacheTable::TryAcquireTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    if (!spaceOp.Contains(cacheItem)) {
        return {};
    }

    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsTombstone())
        {
            return {};
        }
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            Cache_.InvokeHook(ESharedCacheHookPoint::AfterTombstoneHelperClaim, cacheItem);
            return TSharedCacheItemRef(cacheItem);
        }
    }
}

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCacheTable::FreezeNext(const TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
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
bool TSharedCacheTable::FindLink(
    TSpaceOperation& spaceOp, const TSharedCacheKey& key, TCacheItem target, TOrderedLink& result) noexcept {
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
        if (Space_.RefreshBucketRoute(spaceState, route.Resize)) {
            return false;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::BridgeTombstone(
    TCacheItem cacheItem, const TSharedCacheKey& key, TSpaceOperation& spaceOp) noexcept {
    const TCacheItem successor = FreezeNext(spaceOp, cacheItem).WithoutFrozen();
    TCacheItem leftmostTombstone;
    TCacheItem rightmostTombstone;
    TCacheItem current = cacheItem;

    for (;;) {
        TSharedCacheKey currentKey = key;
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
            Cache_.Release(spaceOp, current);
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
                Cache_.Release(spaceOp, current);
                if (wasRightmost) {
                    break;
                }
                current = next;
            }
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCacheTable::PublishPageReplacement(
    TCacheItem source, TCacheItem replacement, const TSharedCacheKey& key, TSpaceOperation& spaceOp) noexcept {
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
                    HelpTombstone(orderedLink.Owner, key, spaceOp);
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
            Cache_.InvokeHook(ESharedCacheHookPoint::BeforeTableLinkCas, orderedLink.Owner);
            replaced = orderedLink.Link->compare_exchange_strong(
                expectedRaw, replacement.Raw(), std::memory_order_release, std::memory_order_relaxed);
            if (replaced) {
                Cache_.InvokeHook(ESharedCacheHookPoint::AfterTableLinkCas, orderedLink.Owner);
            }
        }
        if (replaced) {
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCacheTable::HelpTombstone(
    TCacheItem cacheItem, const TSharedCacheKey& routeKey, TSpaceOperation& spaceOp) noexcept {
    TOperationItemRef hold(spaceOp, TryAcquireTombstone(spaceOp, cacheItem));
    if (!hold) {
        return;
    }
    const TCacheItem tombstone = hold.CacheItem();
    THandle& handle = spaceOp.Handles()[tombstone.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    if (tombstone.Matches(state) && state.IsTombstone()) {
        const TSharedCacheKey key = tombstone.IsResizeMarker() ? routeKey : LoadSharedCacheKey(handle, state.Kind());
        FreezeNext(spaceOp, tombstone);
        BridgeTombstone(tombstone, key, spaceOp);
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::TryBeginErase(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, THandleState& expected, TSharedCacheItemRef& owner) noexcept {
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
    owner = TSharedCacheItemRef(cacheItem);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheTable::TryBeginErase(
    TSpaceOperation& spaceOp, TSharedCacheItemRef& owner, THandleState& expected) noexcept {
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
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCacheTable::FinishErase(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, const TSharedCacheKey& key) noexcept {
    const THandleState state =
        THandleState::FromRaw(spaceOp.Handles()[cacheItem.Index()].State.load(std::memory_order_acquire));
    Y_ABORT_UNLESS(cacheItem.Matches(state) && state.IsTombstone() && state.Refs() > 0);
    FreezeNext(spaceOp, cacheItem);
    BridgeTombstone(cacheItem, key, spaceOp);
}

#undef TSharedCacheTable
#undef TSharedCacheItemRef
#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

template class TSharedCacheItemRefImpl<TProdTraits>;
template class TSharedCacheTableImpl<TProdTraits>;
template class TSharedCacheItemRefImpl<TTestTraits>;
template class TSharedCacheTableImpl<TTestTraits>;

} // namespace NKikimr::NSharedCache
