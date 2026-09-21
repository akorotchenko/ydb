#pragma once

#include "shared_cache_fwd.h"
#include "shared_cache_space.h"
#include "shared_cache_traits.h"

#include <util/generic/ptr.h>

#include <array>
#include <utility>

namespace NKikimr::NSharedCache {

class TSharedCacheKey {
public:
    static constexpr TSharedCacheKey Page(TCollectionCacheItem collection, ui64 offset) noexcept {
        return FromWords(EItemKind::Page, collection.Raw(), offset, 0);
    }

    static TSharedCacheKey Collection(const TLogoBlobID& id) noexcept {
        const ui64* words = id.GetRaw();
        return FromWords(EItemKind::Collection, words[0], words[1], words[2]);
    }

    static constexpr TSharedCacheKey FromWords(EItemKind kind, ui64 word0, ui64 word1, ui64 word2) noexcept {
        return TSharedCacheKey(kind, { word0, word1, kind == EItemKind::Page ? 0 : word2 });
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

    ui32 Hash() const noexcept;

    friend constexpr bool operator==(const TSharedCacheKey&, const TSharedCacheKey&) noexcept = default;

private:
    constexpr TSharedCacheKey(EItemKind kind, const std::array<ui64, 3>& words) noexcept
        : Kind_(kind)
        , Words_(words)
    {
    }

private:
    EItemKind Kind_;
    std::array<ui64, 3> Words_;
};

ui32 HashPageKey(TCollectionCacheItem collection, ui64 offset) noexcept;
ui32 HashCollectionKey(const TLogoBlobID& id) noexcept;

int CompareSharedCacheKeys(
    const TSharedCacheKey& left, ui32 leftHash, const TSharedCacheKey& right, ui32 rightHash) noexcept;

inline int CompareSharedCacheKeys(const TSharedCacheKey& left, const TSharedCacheKey& right) noexcept {
    return CompareSharedCacheKeys(left, left.Hash(), right, right.Hash());
}

void StoreSharedCacheKey(THandle& handle, const TSharedCacheKey& key) noexcept;
TSharedCacheKey LoadSharedCacheKey(const THandle& handle, EItemKind kind) noexcept;

enum class ESharedCacheResultStatus {
    Miss,
    Inserted,
    Pending,
    Hit,
};

template <class TTraits>
class TSharedCacheTableImpl;
template <class TTraits>
class TSharedCacheImpl;
template <class TTraits>
class TSharedCacheCollectionRefImpl;
template <class TTraits>
class TOperationItemRef;

template <class TTraits = TProdTraits>
class TSharedCacheItemRefImpl {
public:
    TSharedCacheItemRefImpl() noexcept = default;
    TSharedCacheItemRefImpl(const TSharedCacheItemRefImpl&) = delete;
    TSharedCacheItemRefImpl& operator=(const TSharedCacheItemRefImpl&) = delete;
    TSharedCacheItemRefImpl(TSharedCacheItemRefImpl&& other) noexcept;
    TSharedCacheItemRefImpl& operator=(TSharedCacheItemRefImpl&& other) noexcept;
    ~TSharedCacheItemRefImpl();

    explicit operator bool() const noexcept {
        return !CacheItem_.IsNull();
    }

    ui32 Index() const noexcept {
        return CacheItem_.Index();
    }

    ui32 Version() const noexcept {
        return CacheItem_.Version();
    }

    TCacheItem CacheItem() const noexcept {
        return CacheItem_;
    }

    void Drop() noexcept;

private:
    friend class TSharedCacheTableImpl<TTraits>;
    friend class TSharedCacheImpl<TTraits>;
    friend class TSharedCachePageRefImpl<TTraits>;
    friend class TSharedCacheCollectionRefImpl<TTraits>;
    friend class TOperationItemRef<TTraits>;

    explicit TSharedCacheItemRefImpl(TCacheItem cacheItem) noexcept;

    TCacheItem Take() noexcept;
    void Disarm() noexcept;
    void Drop(TSpaceOperation& spaceOp) noexcept;

private:
    TCacheItem CacheItem_;
};

template <class TTraits>
class TOperationItemRef {
public:
    explicit TOperationItemRef(TSpaceOperation& spaceOp) noexcept
        : SpaceOp_(spaceOp)
    {
    }

    TOperationItemRef(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept
        : SpaceOp_(spaceOp)
        , Ref_(cacheItem)
    {
    }

    TOperationItemRef(TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) noexcept
        : SpaceOp_(spaceOp)
        , Ref_(std::move(ref))
    {
    }

    TOperationItemRef(const TOperationItemRef&) = delete;
    TOperationItemRef& operator=(const TOperationItemRef&) = delete;
    TOperationItemRef(TOperationItemRef&&) = delete;
    TOperationItemRef& operator=(TOperationItemRef&&) = delete;

    ~TOperationItemRef() {
        Ref_.Drop(SpaceOp_);
    }

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    TCacheItem CacheItem() const noexcept {
        return Ref_.CacheItem();
    }

    TSharedCacheItemRefImpl<TTraits>& Get() noexcept {
        return Ref_;
    }

    TSharedCacheItemRefImpl<TTraits> Take() noexcept {
        return std::move(Ref_);
    }

    TSpaceOperation& SpaceOperation() const noexcept {
        return SpaceOp_;
    }

    void Disarm() noexcept {
        Ref_.Disarm();
    }

private:
    TSpaceOperation& SpaceOp_;
    TSharedCacheItemRefImpl<TTraits> Ref_;
};

template <class TTraits = TProdTraits>
class TSharedCacheTableImpl {
public:
    TSharedCacheTableImpl(TSharedCacheSpace& space, TSharedCacheImpl<TTraits>& cache) noexcept
        : Space_(space)
        , Cache_(cache)
    {
    }

    ESharedCacheResultStatus Find(TSpaceOperation& spaceOp, const TSharedCacheKey& key, TCacheItem& cacheItem) noexcept;

    void CompleteTombstone(TCacheItem cacheItem, const TSharedCacheKey& key, TSpaceOperation& spaceOp) noexcept;

    bool PrepareBucketSplit(TSpaceOperation& spaceOp) noexcept;
    bool ActivateBucketSplit(TSpaceOperation& spaceOp) noexcept;
    bool PublishBucketSplit(TSpaceOperation& spaceOp) noexcept;
    bool CloseBucketSplit(TSpaceOperation& spaceOp) noexcept;
    bool RemoveBucketSplit(TSpaceOperation& spaceOp) noexcept;

    TCacheItem FreezeNext(const TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    bool BridgeTombstone(TCacheItem cacheItem, const TSharedCacheKey& key, TSpaceOperation& spaceOp) noexcept;

    void PublishPageReplacement(
        TCacheItem source, TCacheItem replacement, const TSharedCacheKey& key, TSpaceOperation& spaceOp) noexcept;

private:
    friend class TSharedCacheTestAccess;
    friend class TSharedCacheImpl<TTraits>;

    struct TInsertPosition {
        std::atomic<ui64>* Link = nullptr;
        TCacheItem Expected;
        TCacheItem Owner;
        TSharedCacheItemRefImpl<TTraits> OwnerRef;
        TSpaceState SpaceState;
        TBucketResizeState ResizeState;
    };

    struct TOrderedLink {
        std::atomic<ui64>* Link = nullptr;
        TCacheItem Expected;
        TCacheItem Owner;
        bool Equal = false;
        bool SawSplit = false;
    };

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

    template <bool Insert>
    Y_FORCE_INLINE ESharedCacheResultStatus FindOrInsert(TSpaceOperation& spaceOp, const TSharedCacheKey& key,
        TCacheItem& cacheItem, TInsertPosition* insertPosition = nullptr) noexcept;

    ESharedCacheResultStatus FindForInsert(TSpaceOperation& spaceOp, const TSharedCacheKey& key, TCacheItem& cacheItem,
        TInsertPosition& insertPosition) noexcept;

    bool InsertAt(TSpaceOperation& spaceOp, TInsertPosition& insertPosition, TCacheItem candidate) noexcept;

    bool ValidateTerminalOwner(const TSpaceOperation& spaceOp, const TOrderedLink& orderedLink) const noexcept;

    bool FindLink(
        TSpaceOperation& spaceOp, const TSharedCacheKey& key, TCacheItem target, TOrderedLink& result) noexcept;

    TSharedCacheItemRefImpl<TTraits> TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    TSharedCacheItemRefImpl<TTraits> TryAcquireTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void HelpTombstone(TCacheItem cacheItem, TSpaceOperation& spaceOp) noexcept;

    Y_FORCE_INLINE TBucketRoute SelectBucket(ui32 hash, TSpaceState spaceState) const noexcept {
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

    template <ETombstoneMode Mode>
    static Y_FORCE_INLINE bool SelectGrowthBucket(TBucketRoute& route, const TOrderedLink& link) noexcept {
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

    template <ETombstoneMode Mode>
    Y_FORCE_INLINE EBucketRouteResult ResolveBucketMiss(
        TSpaceState& spaceState, TBucketRoute& route, const TOrderedLink& link) const noexcept {
        if (!Space_.RefreshBucketRoute(spaceState, route.Resize)) {
            return EBucketRouteResult::Retry;
        }
        if (SelectGrowthBucket<Mode>(route, link)) {
            return EBucketRouteResult::Next;
        }
        return EBucketRouteResult::Done;
    }

    template <ETombstoneMode Mode>
    bool FindLinkOnBucket(TSpaceOperation& spaceOp, const TSharedCacheKey& key, ui32 hash, ui32 bucket,
        bool followsSplitSuffix, TOrderedLink& result) noexcept;

    bool FindLinkOnBucket(
        const TSpaceOperation& spaceOp, TCacheItem target, ui32 bucket, TOrderedLink& result) noexcept;

    bool FindSplitBoundary(
        const TSpaceOperation& spaceOp, ui32 bucket, ui32 splitBit, bool suffix, TOrderedLink& result) noexcept;

    bool PrepareSplitMarker(const TSpaceOperation& spaceOp, TCacheItem suffix) noexcept;
    bool TransitionSplitMarkerToTombstone(const TSpaceOperation& spaceOp, TCacheItem marker) noexcept;
    bool SplitMarkerIsSoleOwner(const TSpaceOperation& spaceOp, TCacheItem marker) const noexcept;

private:
    TSharedCacheSpace& Space_;
    TSharedCacheImpl<TTraits>& Cache_;
};

using TSharedCacheItemRef = TSharedCacheItemRefImpl<>;
using TSharedCacheTable = TSharedCacheTableImpl<>;

} // namespace NKikimr::NSharedCache
