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

        TSharedCacheItemRefImpl<TTraits> AcquireHeldPage(TCacheItem item) noexcept {
            return Items_.AcquireHeldPage(SpaceOp_, item);
        }

        TSharedCacheItemRefImpl<TTraits> AcquireHeldCollection(TCacheItem item) noexcept {
            return Items_.AcquireHeldCollection(SpaceOp_, item);
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
    TSharedCacheItemRefImpl<TTraits> AcquireHeldPage(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireHeldCollection(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
};

} // namespace NKikimr::NSharedCache
