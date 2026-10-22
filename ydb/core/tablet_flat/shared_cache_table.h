#pragma once

#include "shared_cache_fwd.h"
#include "shared_cache_ref.h"
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
    {}

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

template <class TTraits = TProdTraits>
class TSharedCacheTableImpl {
public: // Cache table operations
    struct TInsertPosition {
        void DropOwner(TSpaceOperation& spaceOp) noexcept {
            OwnerRef.Drop(spaceOp);
        }

    private:
        friend class TSharedCacheTableImpl<TTraits>;

        std::atomic<ui64>* Link = nullptr;
        TCacheItem Expected;
        TCacheItem Owner;
        TSharedCacheItemRefImpl<TTraits> OwnerRef;
        TSpaceState SpaceState;
        TBucketResizeState ResizeState;
    };

    TSharedCacheTableImpl(TSharedCacheSpace& space, TSharedCacheImpl<TTraits>& cache) noexcept
        : Space_(space)
        , Cache_(cache)
    {}

    ESharedCacheResultStatus Find(TSpaceOperation& spaceOp, const TSharedCacheKey& key, TCacheItem& cacheItem) noexcept;

    ESharedCacheResultStatus FindForInsert(TSpaceOperation& spaceOp, const TSharedCacheKey& key, TCacheItem& cacheItem,
        TInsertPosition& insertPosition) noexcept;

    bool InsertAt(TSpaceOperation& spaceOp, TInsertPosition& insertPosition, TCacheItem candidate) noexcept;

    bool TryBeginErase(TSpaceOperation& spaceOp, TCacheItem cacheItem, THandleState& expected,
        TSharedCacheItemRefImpl<TTraits>& owner) noexcept;

    bool TryBeginErase(
        TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>& owner, THandleState& expected) noexcept;

    void FinishErase(TSpaceOperation& spaceOp, TCacheItem cacheItem, const TSharedCacheKey& key) noexcept;

public: // Resizing
    // Resize steps may return false after partial progress; recheck the phase and cursor before retrying.
    bool PrepareBucketSplit(TSpaceOperation& spaceOp) noexcept;
    bool ActivateBucketSplitOrMerge(TSpaceOperation& spaceOp) noexcept;
    bool PublishBucketSplit(TSpaceOperation& spaceOp) noexcept;
    bool CloseBucketSplit(TSpaceOperation& spaceOp) noexcept;
    bool RemoveBucketSplit(TSpaceOperation& spaceOp) noexcept;

    void PublishPageReplacement(
        TCacheItem source, TCacheItem replacement, const TSharedCacheKey& key, TSpaceOperation& spaceOp) noexcept;

private: // Table helpers
    friend class TSharedCacheTestAccess;

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

    bool ValidateTerminalOwner(const TSpaceOperation& spaceOp, const TOrderedLink& orderedLink) const noexcept;

    bool FindLink(
        TSpaceOperation& spaceOp, const TSharedCacheKey& key, TCacheItem target, TOrderedLink& result) noexcept;

    TSharedCacheItemRefImpl<TTraits> TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    TSharedCacheItemRefImpl<TTraits> TryAcquireTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void TryFinishErase(TCacheItem cacheItem, const TSharedCacheKey& routeKey, TSpaceOperation& spaceOp) noexcept;

    TCacheItem FreezeNext(const TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    bool UnlinkTombstone(TCacheItem cacheItem, const TSharedCacheKey& key, TSpaceOperation& spaceOp) noexcept;

    template <ETombstoneMode Mode>
    bool FindLinkOnBucket(TSpaceOperation& spaceOp, const TSharedCacheKey& key, ui32 hash, ui32 bucket,
        bool followsSplitSuffix, TOrderedLink& result) noexcept;

    bool FindLinkOnBucket(
        const TSpaceOperation& spaceOp, TCacheItem target, ui32 bucket, TOrderedLink& result) noexcept;

    Y_FORCE_INLINE TBucketRoute SelectBucket(ui32 hash, TSpaceState spaceState) const noexcept;

    template <ETombstoneMode Mode>
    static Y_FORCE_INLINE bool SelectGrowthBucket(TBucketRoute& route, const TOrderedLink& link) noexcept;

    template <ETombstoneMode Mode>
    Y_FORCE_INLINE EBucketRouteResult ResolveBucketMiss(
        TSpaceState& spaceState, TBucketRoute& route, const TOrderedLink& link) const noexcept;

private: // Resize helpers
    bool FindSplitBoundary(
        const TSpaceOperation& spaceOp, ui32 bucket, ui32 splitBit, bool suffix, TOrderedLink& result) noexcept;

    bool ActivateBucketSplit(TSpaceOperation& spaceOp, const TBucketResizeState& resize, TCacheItem marker) noexcept;
    bool ActivateBucketMerge(TSpaceOperation& spaceOp, const TBucketResizeState& resize, TCacheItem marker) noexcept;

    bool PrepareSplitMarker(const TSpaceOperation& spaceOp, TCacheItem suffix) noexcept;
    bool TransitionSplitMarkerToTombstone(const TSpaceOperation& spaceOp, TCacheItem marker) noexcept;
    bool SplitMarkerIsSoleOwner(const TSpaceOperation& spaceOp, TCacheItem marker) const noexcept;

private: // Data members
    TSharedCacheSpace& Space_;
    TSharedCacheImpl<TTraits>& Cache_;
};

using TSharedCacheItemRef = TSharedCacheItemRefImpl<>;
using TSharedCacheTable = TSharedCacheTableImpl<>;

} // namespace NKikimr::NSharedCache
