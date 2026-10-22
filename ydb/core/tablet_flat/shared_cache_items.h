#pragma once

#include "shared_cache_ref.h"

namespace NKikimr::NSharedCache {

class TCacheMemory;

template <class TTraits>
class TCacheCollections;

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

        // Handle initialization
        void InitializePage(TCacheCollections<TTraits>& collections, TCacheItem cacheItem,
            TCollectionCacheItem collection, ui64 offset, ui64 size, NTable::NPage::EPage type, ui32 crc32,
            EStickyState sticky, bool reserved, bool collectionOwnerHeld) noexcept {
            Items_.InitializePage(SpaceOp_, collections, cacheItem, collection, offset, size, type, crc32, sticky,
                reserved, collectionOwnerHeld);
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

    TCacheItems(TCacheMemory& memory, const TTraits& traits) noexcept
        : Memory_(memory)
        , Traits_(traits)
    {}

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

    // Payload destruction
    void DeletePayload(TCacheCollections<TTraits>& collections, THandle& handle, EItemKind kind, EHandleState state,
        TCollection* owner = nullptr) noexcept;

private:
    // Structural reference acquisition
    TSharedCacheItemRefImpl<TTraits> TryAcquireItemRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;

    // Native reference acquisition
    TSharedCacheItemRefImpl<TTraits> TryAcquireHotItemRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireMorePageRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireMoreCollectionRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;

    // Reference construction
    TSharedPageRefImpl<TTraits> BuildPageRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;
    TSharedCacheCollectionRefImpl<TTraits> BuildCollectionRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;

    // Handle initialization
    void InitializePage(TSpaceOperation& spaceOp, TCacheCollections<TTraits>& collections, TCacheItem cacheItem,
        TCollectionCacheItem collection, ui64 offset, ui64 size, NTable::NPage::EPage type, ui32 crc32,
        EStickyState sticky, bool reserved, bool collectionOwnerHeld) noexcept;
    void InitializeCollection(
        TSpaceOperation& spaceOp, TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept;

private:
    // Dependencies
    TCacheMemory& Memory_;
    const TTraits& Traits_;
};

} // namespace NKikimr::NSharedCache
