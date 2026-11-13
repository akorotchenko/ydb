#pragma once

#include "cache_fwd.h"
#include "cache_ref.h"
#include "cache_space.h"
#include "cache_traits.h"

#include <util/generic/ptr.h>
#include <util/generic/bitops.h>

#include <array>
#include <utility>

namespace NKikimr::NSharedCache {

Y_FORCE_INLINE ui32 HashPageKey(TCollectionCacheItem collection, ui64 offset) noexcept;
Y_FORCE_INLINE ui32 HashCollectionKey(const TLogoBlobID& id) noexcept;

class TCacheKey {
public:
    static constexpr TCacheKey Page(TCollectionCacheItem collection, ui64 offset) noexcept {
        return FromWords(EItemKind::Page, collection.Raw(), offset, 0);
    }

    static TCacheKey Collection(const TLogoBlobID& id) noexcept {
        const ui64* words = id.GetRaw();
        return FromWords(EItemKind::Collection, words[0], words[1], words[2]);
    }

    static constexpr TCacheKey FromWords(EItemKind kind, ui64 word0, ui64 word1, ui64 word2) noexcept {
        return TCacheKey(kind, { word0, word1, kind == EItemKind::Page ? 0 : word2 });
    }

    constexpr EItemKind Kind() const noexcept {
        return Kind_;
    }

    constexpr bool IsPageKind() const noexcept {
        return Kind() == EItemKind::Page;
    }

    constexpr bool IsCollectionKind() const noexcept {
        return Kind() == EItemKind::Collection;
    }

    constexpr ui64 Word(ui32 index) const noexcept {
        return Words_[index];
    }

    Y_FORCE_INLINE ui32 Hash() const noexcept {
        return Kind_ == EItemKind::Page
                   ? HashPageKey(TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(Words_[0])), Words_[1])
                   : HashCollectionKey(TLogoBlobID(Words_.data()));
    }

    friend constexpr bool operator==(const TCacheKey&, const TCacheKey&) noexcept = default;

private:
    constexpr TCacheKey(EItemKind kind, const std::array<ui64, 3>& words) noexcept
        : Kind_(kind)
        , Words_(words)
    {}

private:
    EItemKind Kind_;
    std::array<ui64, 3> Words_;
};

namespace NDetail {

    template <bool IsPage>
    Y_FORCE_INLINE int CompareCacheKeyWords(const TCacheKey& left, const TCacheKey& right) noexcept {
        constexpr ui32 wordCount = IsPage ? 2 : 3;
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

    template <bool IsPage>
    Y_FORCE_INLINE int CompareKnownCacheKeys(
        const TCacheKey& left, ui32 leftHash, const TCacheKey& right, ui32 rightHash) noexcept {
        const ui32 leftOrder = ReverseBits(leftHash);
        const ui32 rightOrder = ReverseBits(rightHash);
        if (leftOrder < rightOrder) {
            return -1;
        }
        if (leftOrder > rightOrder) {
            return 1;
        }
        constexpr EItemKind rightKind = IsPage ? EItemKind::Page : EItemKind::Collection;
        if (left.Kind() < rightKind) {
            return -1;
        }
        if (left.Kind() > rightKind) {
            return 1;
        }
        return CompareCacheKeyWords<IsPage>(left, right);
    }

    template <bool IsPage>
    Y_FORCE_INLINE bool CacheKeyWordsEqual(const TCacheKey& left, const TCacheKey& right) noexcept {
        return left.Word(0) == right.Word(0) && left.Word(1) == right.Word(1) &&
               (IsPage || left.Word(2) == right.Word(2));
    }

} // namespace NDetail

int CompareCacheKeys(const TCacheKey& left, ui32 leftHash, const TCacheKey& right, ui32 rightHash) noexcept;

inline int CompareCacheKeys(const TCacheKey& left, const TCacheKey& right) noexcept {
    return CompareCacheKeys(left, left.Hash(), right, right.Hash());
}

// The caller owns a fresh handle or has observed its current generation before rewriting its key
void StoreCacheKey(THandle& handle, const TCacheKey& key) noexcept;
TCacheKey LoadCacheKey(const THandle& handle, EItemKind kind) noexcept;

template <class TTraits = TProdTraits>
class TCacheTableImpl {
public: // Cache table operations
    struct TInsertPosition : NDetail::TFoundItem {
        void DropOwner(TSpaceOperation& spaceOp) noexcept {
            OwnerRef.Drop(spaceOp);
        }

    private:
        friend class TCacheTableImpl<TTraits>;

        TCacheItemRefImpl<TTraits> OwnerRef;
        TSpaceState SpaceState;
        TBucketResizeState ResizeState;
    };

    explicit TCacheTableImpl(TCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    Y_FORCE_INLINE ECacheResultStatus FindPage(
        TSpaceOperation& spaceOp, TCollectionCacheItem collection, ui64 offset, NDetail::TFoundItem& found) noexcept;

    Y_FORCE_INLINE ECacheResultStatus FindCollection(
        TSpaceOperation& spaceOp, const TLogoBlobID& id, NDetail::TFoundItem& found) noexcept;

    ECacheResultStatus FindPageForInsert(
        TSpaceOperation& spaceOp, const TCacheKey& key, TInsertPosition& found) noexcept;

    ECacheResultStatus FindCollectionForInsert(
        TSpaceOperation& spaceOp, const TCacheKey& key, TInsertPosition& found) noexcept;

    bool InsertAt(TSpaceOperation& spaceOp, TInsertPosition& insertPosition, TCacheItem candidate) noexcept;

    bool TryBeginErase(TSpaceOperation& spaceOp, TCacheItem cacheItem, THandleState& expected,
        TCacheItemRefImpl<TTraits>& owner) noexcept;

    bool TryBeginErase(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner, THandleState& expected) noexcept;

    void FinishErase(TSpaceOperation& spaceOp, TCacheItem cacheItem, const TCacheKey& key) noexcept;

public: // Resizing
    // Retry keeps bounded maintenance running; Blocked waits for a producer
    enum class EResizeStep : ui8 {
        Blocked,
        Retry,
        Complete,
    };

    EResizeStep PrepareBucketSplit(TSpaceOperation& spaceOp) noexcept;
    EResizeStep ActivateBucketSplitOrMerge(TSpaceOperation& spaceOp) noexcept;
    EResizeStep PublishBucketSplit(TSpaceOperation& spaceOp) noexcept;
    EResizeStep CloseBucketSplit(TSpaceOperation& spaceOp) noexcept;
    EResizeStep RemoveBucketSplit(TSpaceOperation& spaceOp) noexcept;

    void PublishPageReplacement(
        TCacheItem source, TCacheItem replacement, const TCacheKey& key, TSpaceOperation& spaceOp) noexcept;

private: // Table helpers
    friend class TCacheTestAccess;

    using TFoundItem = NDetail::TFoundItem;

    struct TBucketRoute {
        TBucketResizeState Resize;
        ui32 Bucket = 0;
        ui32 BaseBucket = 0;
        bool FollowsSplitSuffix = false;
    };

    enum class ETombstoneMode {
        Traverse,
        Help,
    };

    enum class EBucketRouteResult {
        Retry,
        Next,
        Done,
    };

    template <bool Insert, bool IsPage>
    Y_FORCE_INLINE ECacheResultStatus FindOrInsert(TSpaceOperation& spaceOp, const TCacheKey& key, ui32 hash,
        TSpaceState spaceState, std::conditional_t<Insert, TInsertPosition, TFoundItem>& found) noexcept;

    Y_FORCE_INLINE bool ValidateTerminalOwner(
        const TSpaceOperation& spaceOp, const TFoundItem& orderedLink) const noexcept;

    bool FindLink(TSpaceOperation& spaceOp, const TCacheKey& key, TCacheItem target, TFoundItem& result) noexcept;

    TCacheItemRefImpl<TTraits> TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    TCacheItemRefImpl<TTraits> TryAcquireTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void TryFinishErase(TCacheItem cacheItem, const TCacheKey& routeKey, TSpaceOperation& spaceOp) noexcept;

    TCacheItem FreezeNext(const TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    bool UnlinkTombstone(TCacheItem cacheItem, const TCacheKey& key, TSpaceOperation& spaceOp) noexcept;

    template <ETombstoneMode Mode, bool IsPage>
    Y_FORCE_INLINE bool FindInBucket(TSpaceOperation& spaceOp, const TCacheKey& key, ui32 hash, ui32 bucket,
        bool followsSplitSuffix, TFoundItem& result) noexcept;

    template <ETombstoneMode Mode, bool IsPage, bool ResizeActive>
    Y_FORCE_INLINE EBucketRouteResult FindRoute(TSpaceOperation& spaceOp, const TCacheKey& key, ui32 hash,
        TSpaceState& spaceState, TBucketRoute& route, TFoundItem& result) noexcept;

    template <ETombstoneMode Mode, bool IsPage>
    EBucketRouteResult FindWithResize(TSpaceOperation& spaceOp, const TCacheKey& key, ui32 hash,
        TSpaceState& spaceState, TBucketRoute& route, TFoundItem& result) noexcept;

    bool FindLinkOnBucket(const TSpaceOperation& spaceOp, TCacheItem target, ui32 bucket, TFoundItem& result) noexcept;

    Y_FORCE_INLINE TBucketRoute SelectBucket(ui32 hash, TSpaceState spaceState) const noexcept;

    Y_FORCE_INLINE TBucketRoute SelectResizeBucket(ui32 hash) const noexcept;

    template <ETombstoneMode Mode>
    static Y_FORCE_INLINE bool SelectGrowthBucket(TBucketRoute& route, const TFoundItem& link) noexcept;

    template <ETombstoneMode Mode, bool ResizeActive>
    Y_FORCE_INLINE EBucketRouteResult ResolveBucketMiss(
        TSpaceState& spaceState, TBucketRoute& route, const TFoundItem& link) const noexcept;

private: // Resize helpers
    EResizeStep FindSplitBoundary(
        const TSpaceOperation& spaceOp, ui32 bucket, ui32 splitBit, bool suffix, TFoundItem& result) noexcept;

    EResizeStep ResizeOwnerResult(const TSpaceOperation& spaceOp, TCacheItem owner) const noexcept;
    EResizeStep ActivateBucketSplit(
        TSpaceOperation& spaceOp, const TBucketResizeState& resize, TCacheItem marker) noexcept;
    EResizeStep ActivateBucketMerge(
        TSpaceOperation& spaceOp, const TBucketResizeState& resize, TCacheItem marker) noexcept;

    bool PrepareSplitMarker(const TSpaceOperation& spaceOp, TCacheItem suffix) noexcept;
    bool TransitionSplitMarkerToTombstone(const TSpaceOperation& spaceOp, TCacheItem marker) noexcept;
    bool SplitMarkerIsSoleOwner(const TSpaceOperation& spaceOp, TCacheItem marker) const noexcept;

private: // Data members
    TCacheImpl<TTraits>& Cache_;
};

using TCacheItemRef = TCacheItemRefImpl<>;
using TCacheTable = TCacheTableImpl<>;

// Inline implementations

namespace NDetail {

    Y_FORCE_INLINE ui64 HashCacheKeyWord(ui64 hash, ui64 value) noexcept {
        hash ^= 0x9E3779B97F4A7C15ULL + value;
        hash ^= hash >> 30;
        hash *= 0xBF58476D1CE4E5B9ULL;
        hash ^= hash >> 27;
        hash *= 0x94D049BB133111EBULL;
        hash ^= hash >> 31;
        return hash;
    }

    Y_FORCE_INLINE ui32 FinishCacheKeyHash(ui64 hash) noexcept {
        return static_cast<ui32>(hash ^ (hash >> 32));
    }

} // namespace NDetail

Y_FORCE_INLINE ui32 HashPageKey(TCollectionCacheItem collection, ui64 offset) noexcept {
    const ui64 hash = NDetail::HashCacheKeyWord(offset, collection.Raw());
    return NDetail::FinishCacheKeyHash(hash);
}

Y_FORCE_INLINE ui32 HashCollectionKey(const TLogoBlobID& id) noexcept {
    const ui64* words = id.GetRaw();
    const ui64 hash = NDetail::HashCacheKeyWord(words[0], words[1]);
    return NDetail::FinishCacheKeyHash(NDetail::HashCacheKeyWord(hash, words[2]));
}

template <class TTraits>
template <typename TCacheTableImpl<TTraits>::ETombstoneMode Mode, bool IsPage>
Y_FORCE_INLINE bool TCacheTableImpl<TTraits>::FindInBucket(TSpaceOperation& spaceOp, const TCacheKey& key, ui32 hash,
    ui32 bucket, bool followsSplitSuffix, TFoundItem& result) noexcept {
    Y_DEBUG_ABORT_UNLESS(key.IsPageKind() == IsPage);

restart:
    result.Link = &spaceOp.Buckets()[bucket];
    result.Item = TCacheItem::FromRaw(result.Link->load(std::memory_order_acquire));
    result.Owner = {};
    result.Equal = false;
    result.SawSplit = false;
    result.State = {};
    if (Y_UNLIKELY(result.Item.IsClosedBucketHead())) {
        return false;
    }
    for (;;) {
        if (result.Item.IsNull()) {
            if (!ValidateTerminalOwner(spaceOp, result)) {
                goto restart;
            }
            return true;
        }
        if (Y_UNLIKELY(result.Item.IsFrozen() || !spaceOp.Contains(result.Item))) {
            goto restart;
        }

        THandle& handle = spaceOp.Handles()[result.Item.Index()];
        const TCacheItem next = TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire));
        // Acquire rewritten words before validating their handle generation
        const ui64 key0 = handle.Key0.load(std::memory_order_acquire);
        const ui64 key1 = handle.Key1.load(std::memory_order_acquire);
        const ui64 key2 = handle.Key2OrSize.load(std::memory_order_acquire);
        Cache_.InvokeHook(ECacheHookPoint::BeforeTableItemValidation, result.Item);
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (Y_UNLIKELY(!result.Item.Matches(state) || state.IsFree())) {
            goto restart;
        }
        if (Y_UNLIKELY(state.IsReplacing() || state.IsReplaced())) {
            if (!next.IsFrozen() || next.WithoutFrozen().IsNull() || next.WithoutFrozen() == result.Item) {
                goto restart;
            }
            result.Item = next.WithoutFrozen();
            continue;
        }
        if (Y_UNLIKELY(state.IsBucketSplit() || (state.IsTombstone() && result.Item.IsResizeMarker())))
        {
            result.SawSplit = true;
            if (!followsSplitSuffix) {
                return true;
            }
            result.Link = &handle.Next;
            result.Owner = TCacheItem::Make(state.Version(), result.Item.Index());
            result.Item = next.WithoutFrozen();
            continue;
        }
        const bool tombstone = state.IsTombstone();
        if (Y_UNLIKELY(tombstone)) {
            if constexpr (Mode == ETombstoneMode::Help) {
                TryFinishErase(result.Item, key, spaceOp);
                goto restart;
            }
        } else if (Y_UNLIKELY(next.IsFrozen())) {
            goto restart;
        }
        const TCacheKey itemKey = TCacheKey::FromWords(state.Kind(), key0, key1, key2);
        if (Y_LIKELY(IsPage == state.IsPageKind() && NDetail::CacheKeyWordsEqual<IsPage>(itemKey, key))) {
            result.Equal = !tombstone;
            result.State = state;
            return true;
        }
        if (NDetail::CompareKnownCacheKeys<IsPage>(itemKey, itemKey.Hash(), key, hash) > 0) {
            return true;
        }
        result.Link = &handle.Next;
        result.Owner = TCacheItem::Make(state.Version(), result.Item.Index());
        result.Item = next.WithoutFrozen();
    }
}

template <class TTraits>
Y_FORCE_INLINE bool TCacheTableImpl<TTraits>::ValidateTerminalOwner(
    const TSpaceOperation& spaceOp, const TFoundItem& orderedLink) const noexcept {
    if (orderedLink.Owner.IsNull()) {
        return true;
    }
    if (orderedLink.Item.Version() != orderedLink.Owner.Version() || !spaceOp.Contains(orderedLink.Owner))
    {
        return false;
    }

    const THandleState ownerState =
        THandleState::FromRaw(spaceOp.Handles()[orderedLink.Owner.Index()].State.load(std::memory_order_acquire));
    return orderedLink.Owner.Matches(ownerState) && !ownerState.IsFree();
}

template <class TTraits>
template <typename TCacheTableImpl<TTraits>::ETombstoneMode Mode, bool ResizeActive>
Y_FORCE_INLINE typename TCacheTableImpl<TTraits>::EBucketRouteResult TCacheTableImpl<TTraits>::ResolveBucketMiss(
    TSpaceState& spaceState, TBucketRoute& route, const TFoundItem& link) const noexcept {
    if (!Cache_.Space_->RefreshBucketRoute(spaceState, route.Resize)) {
        return EBucketRouteResult::Retry;
    }
    if constexpr (ResizeActive) {
        if (SelectGrowthBucket<Mode>(route, link)) {
            return EBucketRouteResult::Next;
        }
    }
    return EBucketRouteResult::Done;
}

template <class TTraits>
template <typename TCacheTableImpl<TTraits>::ETombstoneMode Mode, bool IsPage, bool ResizeActive>
Y_FORCE_INLINE typename TCacheTableImpl<TTraits>::EBucketRouteResult TCacheTableImpl<TTraits>::FindRoute(
    TSpaceOperation& spaceOp, const TCacheKey& key, ui32 hash, TSpaceState& spaceState, TBucketRoute& route,
    TFoundItem& result) noexcept {
    if (!FindInBucket<Mode, IsPage>(spaceOp, key, hash, route.Bucket, route.FollowsSplitSuffix, result)) {
        Cache_.Space_->RefreshBucketRoute(spaceState, route.Resize);
        return EBucketRouteResult::Retry;
    }
    if (Y_LIKELY(result.Equal)) {
        return EBucketRouteResult::Done;
    }
    return ResolveBucketMiss<Mode, ResizeActive>(spaceState, route, result);
}

template <class TTraits>
template <bool Insert, bool IsPage>
Y_FORCE_INLINE ECacheResultStatus TCacheTableImpl<TTraits>::FindOrInsert(TSpaceOperation& spaceOp, const TCacheKey& key,
    ui32 hash, TSpaceState spaceState, std::conditional_t<Insert, TInsertPosition, TFoundItem>& found) noexcept {
    static constexpr ETombstoneMode Mode = Insert ? ETombstoneMode::Help : ETombstoneMode::Traverse;
    for (;;) {
        TBucketRoute route;
        found = {};
        EBucketRouteResult status;
        if (Y_UNLIKELY(spaceState.Resizing())) {
            status = FindWithResize<Mode, IsPage>(spaceOp, key, hash, spaceState, route, found);
        } else {
            const ui32 bucket = hash & spaceState.BucketMask();
            route = { .Bucket = bucket, .BaseBucket = bucket };
            status = FindRoute<Mode, IsPage, false>(spaceOp, key, hash, spaceState, route, found);
        }
        if (Y_LIKELY(found.Equal)) {
            return ECacheResultStatus::Hit;
        }
        if (Y_UNLIKELY(status == EBucketRouteResult::Retry)) {
            continue;
        }
        if constexpr (!Insert) {
            found = {};
            return ECacheResultStatus::Miss;
        } else {
            TOperationItemRef<TTraits> ownerRef(spaceOp);
            if (!found.Owner.IsNull()) {
                Cache_.InvokeHook(ECacheHookPoint::BeforeInsertOwnerAcquire, found.Owner);
                ownerRef.Get() = TryAcquire(spaceOp, found.Owner);
                if (!ownerRef) {
                    continue;
                }
                const THandleState ownerState =
                    THandleState::FromRaw(spaceOp.Handles()[found.Owner.Index()].State.load(std::memory_order_relaxed));
                if (!found.Owner.Matches(ownerState) || ownerState.IsTombstone() || ownerState.IsFree() ||
                    found.Link->load(std::memory_order_acquire) != found.Item.Raw())
                {
                    continue;
                }
            }
            found.OwnerRef = ownerRef.Take();
            found.SpaceState = spaceState;
            found.ResizeState = route.Resize;
            Cache_.InvokeHook(ECacheHookPoint::AfterInsertPositionFound, found.Owner);
            return ECacheResultStatus::Miss;
        }
    }
}

template <class TTraits>
Y_FORCE_INLINE ECacheResultStatus TCacheTableImpl<TTraits>::FindPage(
    TSpaceOperation& spaceOp, TCollectionCacheItem collection, ui64 offset, NDetail::TFoundItem& found) noexcept {
    return FindOrInsert<false, true>(spaceOp, TCacheKey::Page(collection, offset), HashPageKey(collection, offset),
        spaceOp.EntrySpaceState(), found);
}

template <class TTraits>
Y_FORCE_INLINE ECacheResultStatus TCacheTableImpl<TTraits>::FindCollection(
    TSpaceOperation& spaceOp, const TLogoBlobID& id, NDetail::TFoundItem& found) noexcept {
    return FindOrInsert<false, false>(
        spaceOp, TCacheKey::Collection(id), HashCollectionKey(id), spaceOp.EntrySpaceState(), found);
}

} // namespace NKikimr::NSharedCache
