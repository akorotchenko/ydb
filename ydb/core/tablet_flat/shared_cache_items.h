#pragma once

#include "shared_cache_ref.h"
#include "shared_cache_space.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
class TCacheItems {
public:
    class TOperation {
    public:
        // Structural protection
        TSharedCacheItemRefImpl<TTraits> TryAcquireStructural(TCacheItem item) noexcept {
            return Items_.TryAcquireItemRef(SpaceOp_, item);
        }

        // Native reference acquisition
        TSharedCacheItemRefImpl<TTraits> TryAcquireHotItemRef(TCacheItem item) noexcept {
            return Items_.TryAcquireHotItemRef(SpaceOp_, item);
        }

        TSharedCacheItemRefImpl<TTraits> AcquireMorePageRef(TCacheItem item) noexcept {
            return Items_.AcquireMorePageRef(SpaceOp_, item);
        }

        TSharedCacheItemRefImpl<TTraits> AcquireMoreCollectionRef(TCacheItem item) noexcept {
            return Items_.AcquireMoreCollectionRef(SpaceOp_, item);
        }

        // Reference construction
        TSharedPageRefImpl<TTraits> BuildPageRef(TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
            return Items_.BuildPageRef(SpaceOp_, std::move(ref));
        }

        TSharedCacheCollectionRefImpl<TTraits> BuildCollectionRef(
            TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
            return Items_.BuildCollectionRef(SpaceOp_, std::move(ref));
        }

        // Candidate cleanup
        void DiscardCandidate(TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes) noexcept {
            Items_.DiscardCandidate(SpaceOp_, cacheItem, kind, reservedBytes);
        }

        // Handle initialization
        void InitializePage(TCacheItem cacheItem, TCollectionCacheItem collection, ui64 offset, ui64 size,
            NTable::NPage::EPage type, ui32 crc32, EStickyState sticky, bool reserved,
            bool collectionOwnerHeld) noexcept {
            Items_.InitializePage(
                SpaceOp_, cacheItem, collection, offset, size, type, crc32, sticky, reserved, collectionOwnerHeld);
        }

        void InitializeCollection(TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept {
            Items_.InitializeCollection(SpaceOp_, cacheItem, id, bytes);
        }

    private:
        friend class TCacheItems;

        TOperation(TCacheItems& items, TSpaceOperation& spaceOp) noexcept
            : Items_(items)
            , SpaceOp_(spaceOp)
        {}

    private:
        TCacheItems& Items_;
        TSpaceOperation& SpaceOp_;
    };

    // Payload sizes
    static ui64 PayloadBytes(const THandle& handle, EItemKind kind) noexcept {
        return kind == EItemKind::Page ? PageBytes(handle) : handle.Metadata.PayloadBytes;
    }

    static ui64 PageBytes(const THandle& handle) noexcept {
        return AccountedPageBytes(handle.Key2OrSize.load(std::memory_order_relaxed));
    }

    static ui64 AccountedPageBytes(ui64 size) noexcept {
        Y_ABORT_UNLESS(size <= Max<ui64>() - NActors::TSharedData::OverheadSize);
        return size + NActors::TSharedData::OverheadSize;
    }

    explicit TCacheItems(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

    // Payload destruction
    void DeletePayload(THandle& handle, EItemKind kind, EHandleState state, TCollection* owner = nullptr) noexcept;

private:
    // Structural reference acquisition
    TSharedCacheItemRefImpl<TTraits> TryAcquireItemRef(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
        if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen())
        {
            return {};
        }

        THandle& handle = spaceOp.Handles()[cacheItem.Index()];
        ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState expected = THandleState::FromRaw(expectedRaw);
            if (!cacheItem.Matches(expected) || expected.IsFree() || expected.IsReplacing() || expected.IsReplaced() ||
                expected.IsTombstone())
            {
                return {};
            }

            Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
            const THandleState desired = expected.IncrementRefs();
            if (handle.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
            {
                Cache_.UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired);
                return TSharedCacheItemRefImpl<TTraits>(cacheItem);
            }
        }
    }

    // Native reference acquisition
    TSharedCacheItemRefImpl<TTraits> TryAcquireHotItemRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireMorePageRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireMoreCollectionRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;

    // Reference construction
    TSharedPageRefImpl<TTraits> BuildPageRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;
    TSharedCacheCollectionRefImpl<TTraits> BuildCollectionRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;

    // Candidate cleanup
    void DiscardCandidate(TSpaceOperation& spaceOp, TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes) noexcept;

    // Handle initialization
    void InitializePage(TSpaceOperation& spaceOp, TCacheItem cacheItem, TCollectionCacheItem collection, ui64 offset,
        ui64 size, NTable::NPage::EPage type, ui32 crc32, EStickyState sticky, bool reserved,
        bool collectionOwnerHeld) noexcept;
    void InitializeCollection(
        TSpaceOperation& spaceOp, TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept;

private:
    // Dependencies
    TSharedCacheImpl<TTraits>& Cache_;
};

} // namespace NKikimr::NSharedCache
