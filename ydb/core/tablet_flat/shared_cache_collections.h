#pragma once

#include "shared_cache_collection.h"
#include "shared_cache_space.h"

#include <util/generic/function_ref.h>

#include <atomic>

namespace NKikimr::NSharedCache {

class TCacheMemory;

template <class TTraits>
class TSharedCacheItemRefImpl;
template <class TTraits>
class TOperationItemRef;

template <class TTraits>
class TCacheCollectionServices {
public:
    explicit TCacheCollectionServices(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    TSharedCacheItemRefImpl<TTraits> TryAcquireStructural(TSpaceOperation& spaceOp, TCacheItem item) noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
};

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

        bool Link(TCollectionRegistry& registry, TCacheItem item) noexcept {
            return Collections_.Link(SpaceOp_, registry, item);
        }

        bool Unlink(TCollectionRegistry& registry, TCacheItem item, EHandleState expectedState) noexcept {
            return Collections_.Unlink(SpaceOp_, registry, item, expectedState);
        }

        bool Unlink(TCollectionRegistry& registry, TCacheItem item) noexcept {
            return Collections_.Unlink(SpaceOp_, registry, item);
        }

        void Unlink(TCollectionRegistry& registry, TOperationItemRef<TTraits>& owner) noexcept {
            Collections_.Unlink(registry, owner);
        }

        void ForEach(TFunctionRef<void(TCollection&)> callback) noexcept {
            Collections_.ForEach(SpaceOp_, *Collections_.CollectionRegistry_, callback);
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

    TCacheCollections(TCacheCollectionServices<TTraits> services, TCacheMemory& memory, const TTraits& traits) noexcept
        : Services_(services)
        , Memory_(memory)
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
    friend class TSharedCacheImpl<TTraits>;

    TCollectionRegistry& Registry() noexcept {
        return *CollectionRegistry_;
    }

    static TCollection* PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    static TCollection* PageCollection(TSpaceOperation& spaceOp, TCollectionCacheItem item) noexcept;
    static void SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept;

    bool Link(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item) noexcept;
    bool Unlink(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item, EHandleState expectedState) noexcept;
    bool Unlink(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item) noexcept;
    void Unlink(TCollectionRegistry& registry, TOperationItemRef<TTraits>& owner) noexcept;
    void ForEach(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TFunctionRef<void(TCollection&)> callback) noexcept;

private:
    TCacheCollectionServices<TTraits> Services_;
    TIntrusivePtr<TCollectionRegistry> CollectionRegistry_ = new TCollectionRegistry;
    TCacheMemory& Memory_;
    const TTraits& Traits_;
};

} // namespace NKikimr::NSharedCache
