#pragma once

#include "shared_cache_collection.h"
#include "shared_cache_space.h"

#include <atomic>

namespace NKikimr::NSharedCache {

class TCacheMemory;

template <class TTraits>
class TCacheCollections {
public:
    class TOperation {
    public:
        TCollection* PageCollection(const THandle& page) noexcept {
            return Collections_.PageCollection(SpaceOp_, page);
        }

        TCollection* PageCollection(TCollectionCacheItem item) noexcept {
            return Collections_.PageCollection(SpaceOp_, item);
        }

    private:
        friend class TCacheCollections;

        TOperation(TCacheCollections& collections, TSpaceOperation& spaceOp) noexcept
            : Collections_(collections)
            , SpaceOp_(spaceOp)
        {}

    private:
        TCacheCollections& Collections_;
        TSpaceOperation& SpaceOp_;
    };

    TCacheCollections(TCacheMemory& memory, const TTraits& traits) noexcept
        : Memory_(memory)
        , Traits_(traits)
    {}

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

    void AddResidentPageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept;
    void AddActivePageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept;
    void PublishKeepPageBytes(TCollection& owner) noexcept;
    void SetSkipBTreeIndexV1Shadow(TCollection& owner, bool skip) noexcept;
    void AcknowledgeBytes(TCollection& owner) noexcept;

private:
    static TCollection* PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    static TCollection* PageCollection(TSpaceOperation& spaceOp, TCollectionCacheItem item) noexcept;
    static void SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept;

private:
    TCacheMemory& Memory_;
    const TTraits& Traits_;
};

} // namespace NKikimr::NSharedCache
