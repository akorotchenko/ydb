#pragma once

#include "shared_cache_collection.h"
#include "shared_cache_items.h"
#include "shared_cache_space.h"

#include <util/generic/function_ref.h>

#include <atomic>

namespace NKikimr::NSharedCache {

template <class TTraits>
class TSharedCacheItemRefImpl;
template <class TTraits>
class TOperationItemRef;

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

        void DropOwnerReference(TCollection& owner) noexcept {
            Collections_.DropOwnerReference(SpaceOp_, owner);
        }

        void DropPageItemRef(THandle& page) noexcept {
            Collections_.DropPageItemRef(SpaceOp_, page);
        }

        void PublishCollectionForReclaim(TCacheItem item) noexcept {
            Collections_.PublishCollectionForReclaim(SpaceOp_, item);
        }

        TSharedCacheItemRefImpl<TTraits> AcquireSticky(TCollectionCacheItem item, TCollection*& value) noexcept {
            return Collections_.AcquireSticky(SpaceOp_, item, value);
        }

        void DrainStickyPages(TCollection& collection) noexcept {
            Collections_.DrainStickyPages(SpaceOp_, collection);
        }

        void MergeRetainedStickyPages(TCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept {
            Collections_.MergeRetainedStickyPages(SpaceOp_, collection, retainedHead, retainedTail);
        }

        bool LinkStickyPage(TCacheItem item, TSharedCacheItemRefImpl<TTraits>& collectionRef) noexcept {
            return Collections_.LinkStickyPage(SpaceOp_, item, collectionRef);
        }

        void UnlinkPendingStickyPage(TCacheItem item) noexcept {
            Collections_.UnlinkPendingStickyPage(SpaceOp_, item);
        }

        bool MakePageSticky(TCacheItem item) noexcept {
            return Collections_.MakePageSticky(SpaceOp_, item);
        }

        bool UnstickyCutPages(TCacheItem item, ui64 allocationLimit) noexcept {
            return Collections_.UnstickyCutPages(SpaceOp_, item, allocationLimit);
        }

        bool SetPagesCacheMode(TCollection& collection, ECacheMode mode) noexcept {
            return Collections_.SetPagesCacheMode(SpaceOp_, collection, mode);
        }

        bool Detach(TCollectionRegistry& registry, TCacheItem item) noexcept {
            return Collections_.Detach(SpaceOp_, registry, item);
        }

        bool Erase(TCollectionRegistry* expectedRegistry, TCacheItem item) noexcept {
            return Collections_.Erase(SpaceOp_, expectedRegistry, item);
        }

        bool MoveRegistry(TCollectionRegistry& source, TCollectionRegistry& destination, TCacheItem item) noexcept {
            return Collections_.MoveRegistry(SpaceOp_, source, destination, item);
        }

        bool Attach(TCollectionRegistry& registry, TCacheItem item) noexcept {
            return Collections_.Attach(SpaceOp_, registry, item);
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

    explicit TCacheCollections(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

    void AddResidentPageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept;
    void AddActivePageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept;
    void PublishKeepPageBytes(TCollection& owner) noexcept;
    void SetSkipBTreeIndexV1Shadow(TCollection& owner, bool skip) noexcept;
    void AcknowledgeBytes(TCollection& owner) noexcept;
    static void AddReference(TCollection& owner) noexcept;
    static bool DropReference(TCollection& owner) noexcept;
    static bool WantsStickyPage(const TCollection& collection) noexcept;
    static bool IsUnregisteredAndUnreferenced(const TCollection& collection) noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;

    typename TCacheItems<TTraits>::TOperation Items(TSpaceOperation& spaceOp) const noexcept {
        return Cache_.Items_.BindOperation(spaceOp);
    }

    TCollectionRegistry& Registry() noexcept {
        return *CollectionRegistry_;
    }

    static TCollection* PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    static TCollection* PageCollection(TSpaceOperation& spaceOp, TCollectionCacheItem item) noexcept;
    static void SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept;

    void DropOwnerReference(TSpaceOperation& spaceOp, TCollection& owner) noexcept;
    void DropPageItemRef(TSpaceOperation& spaceOp, THandle& page) noexcept;
    void PublishCollectionForReclaim(TSpaceOperation& spaceOp, TCacheItem collection) noexcept;
    void FinishCollectionRetain(TSpaceOperation& spaceOp, TCacheItem collection) noexcept;

    TSharedCacheItemRefImpl<TTraits> AcquireSticky(
        TSpaceOperation& spaceOp, TCollectionCacheItem item, TCollection*& value) noexcept;
    static void RestoreStickyPageList(TSpaceOperation& spaceOp, TCollection& collection, ui32 detachedHead) noexcept;
    void MergeRetainedStickyPages(
        TSpaceOperation& spaceOp, TCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept;
    void DrainStickyPages(TSpaceOperation& spaceOp, TCollection& collection) noexcept;

    bool LinkStickyPage(
        TSpaceOperation& spaceOp, TCacheItem item, TSharedCacheItemRefImpl<TTraits>& collectionRef) noexcept;
    void CancelStickyLink(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    void UnlinkPendingStickyPage(TSpaceOperation& spaceOp, TCacheItem item) noexcept;

    bool MakePageSticky(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    bool FinishStickyPromotion(TSpaceOperation& spaceOp, TCacheItem item, ui64 bytes) noexcept;
    static void PublishPageUnsticky(THandle& page, THandleState state) noexcept;
    bool UnstickyCutPages(TSpaceOperation& spaceOp, TCacheItem item, ui64 allocationLimit) noexcept;

    bool SetPagesCacheMode(TSpaceOperation& spaceOp, TCollection& collection, ECacheMode mode) noexcept;
    bool TryBeginDetach(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item) noexcept;
    bool Detach(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item) noexcept;

    bool TryClaimDeletion(TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry, TCacheItem item,
        THandleState& sourceState, TSharedCacheItemRefImpl<TTraits>& owner) noexcept;
    bool Erase(TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry, TCacheItem item) noexcept;
    bool MoveRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& source, TCollectionRegistry& destination,
        TCacheItem item) noexcept;

    bool TryBeginAttach(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item, THandleState& attachState) noexcept;
    void FinishAttach(TSpaceOperation& spaceOp, TCacheItem item, EHandleState detachedState) noexcept;
    bool Attach(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item) noexcept;

    bool Link(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item) noexcept;
    bool Unlink(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item, EHandleState expectedState) noexcept;
    bool Unlink(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem item) noexcept;
    void Unlink(TCollectionRegistry& registry, TOperationItemRef<TTraits>& owner) noexcept;
    void ForEach(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TFunctionRef<void(TCollection&)> callback) noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
    TIntrusivePtr<TCollectionRegistry> CollectionRegistry_ = new TCollectionRegistry;
};

} // namespace NKikimr::NSharedCache
