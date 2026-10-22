#pragma once

#include "shared_cache_table.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
class TCacheItems {
public:
    class TOperation {
    public:
        // Native pin acquisition
        TSharedCacheItemRefImpl<TTraits> TryAcquireHotEvictionPin(TCacheItem item) noexcept {
            return Items_.TryAcquireHotEvictionPin(SpaceOp_, item);
        }

        TSharedCacheItemRefImpl<TTraits> AcquireMorePageRef(TCacheItem item) noexcept {
            return Items_.AcquireMorePageRef(SpaceOp_, item);
        }

        TSharedCacheItemRefImpl<TTraits> AcquireMoreCollectionRef(TCacheItem item) noexcept {
            return Items_.AcquireMoreCollectionRef(SpaceOp_, item);
        }

        // Handle initialization
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

    TCacheItems() noexcept = default;

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

private:
    // Native pin acquisition
    TSharedCacheItemRefImpl<TTraits> TryAcquireHotEvictionPin(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireMorePageRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireMoreCollectionRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;

    // Handle initialization
    void InitializeCollection(
        TSpaceOperation& spaceOp, TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept;
};

} // namespace NKikimr::NSharedCache
