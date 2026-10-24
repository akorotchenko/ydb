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
    using TOperation = TCacheCollectionsOperation<TTraits>;

    explicit TCacheCollections(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    Y_FORCE_INLINE TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { Cache_, spaceOp };
    }

    void AddResidentPageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept;
    void AddActivePageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept;
    void PublishKeepPageBytes(TCollection& owner) noexcept;

    Y_FORCE_INLINE void SetSkipBTreeIndexV1Shadow(TCollection& owner, bool skip) noexcept {
        owner.PageCollection()->SetSkipBTreeIndexV1Shadow(skip);
        PublishKeepPageBytes(owner);
    }

    Y_FORCE_INLINE void AcknowledgeBytes(TCollection& owner) noexcept {
        // Clear before the actor samples bytes, so a racing update can schedule the next notification.
        owner.BytesNotificationPending_.store(false, std::memory_order_release);
    }

    static Y_FORCE_INLINE void AddReference(TCollection& owner) noexcept {
        owner.References.fetch_add(1, std::memory_order_relaxed);
    }

    static Y_FORCE_INLINE bool DropReference(TCollection& owner) noexcept {
        const ui64 previous = owner.References.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
        return previous == 1;
    }

    static Y_FORCE_INLINE bool WantsStickyPage(const TCollection& collection) noexcept {
        return collection.CacheMode_.load(std::memory_order_acquire) == ECacheMode::Sticky;
    }

    static Y_FORCE_INLINE bool IsUnregisteredAndUnreferenced(const TCollection& collection) noexcept {
        return !collection.Registry_.load(std::memory_order_acquire) &&
               collection.References.load(std::memory_order_acquire) == 0;
    }

private:
    friend class TCacheCollectionsOperation<TTraits>;
    friend class TSharedCacheImpl<TTraits>;

    Y_FORCE_INLINE typename TCacheItems<TTraits>::TOperation Items(TSpaceOperation& spaceOp) const noexcept {
        return Cache_.Items_.BindOperation(spaceOp);
    }

    TCollectionRegistry& Registry() noexcept {
        return *CollectionRegistry_;
    }

    static Y_FORCE_INLINE TCollection* PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept {
        return PageCollection(spaceOp,
            TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed))));
    }

    static Y_FORCE_INLINE TCollection* PageCollection(TSpaceOperation& spaceOp, TCollectionCacheItem item) noexcept {
        if (item.Index() >= spaceOp.AccessibleHandleCount()) {
            return nullptr;
        }
        THandle& collection = spaceOp.Handles()[item.Index()];
        const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
        if (!item.Matches(state) || !collection.Body.Collection) {
            return nullptr;
        }
        return collection.Body.Collection;
    }

    static Y_FORCE_INLINE void SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept {
        const ui64 previous = counter.fetch_sub(bytes, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous >= bytes);
    }

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

    Y_FORCE_INLINE void Unlink(TCollectionRegistry& registry, TOperationItemRef<TTraits>& owner) noexcept {
        const bool unlinked = Unlink(owner.SpaceOperation(), registry, owner.CacheItem(), EHandleState::Tombstone);
        Y_DEBUG_ABORT_UNLESS(unlinked);
    }

    void ForEach(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TFunctionRef<void(TCollection&)> callback) noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
    TIntrusivePtr<TCollectionRegistry> CollectionRegistry_ = new TCollectionRegistry;
};

// Leave operation constructors implicitly instantiated
template <class TTraits>
class TCacheCollectionsOperation {
public:
    Y_FORCE_INLINE TCollection* PageCollection(const THandle& page) noexcept {
        return Cache_.Collections_.PageCollection(SpaceOp_, page);
    }

    Y_FORCE_INLINE TCollection* PageCollection(TCollectionCacheItem item) noexcept {
        return Cache_.Collections_.PageCollection(SpaceOp_, item);
    }

    Y_FORCE_INLINE void DropOwnerReference(TCollection& owner) noexcept {
        Cache_.Collections_.DropOwnerReference(SpaceOp_, owner);
    }

    Y_FORCE_INLINE void DropPageItemRef(THandle& page) noexcept {
        Cache_.Collections_.DropPageItemRef(SpaceOp_, page);
    }

    Y_FORCE_INLINE void PublishCollectionForReclaim(TCacheItem item) noexcept {
        Cache_.Collections_.PublishCollectionForReclaim(SpaceOp_, item);
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> AcquireSticky(
        TCollectionCacheItem item, TCollection*& value) noexcept {
        return Cache_.Collections_.AcquireSticky(SpaceOp_, item, value);
    }

    Y_FORCE_INLINE void DrainStickyPages(TCollection& collection) noexcept {
        Cache_.Collections_.DrainStickyPages(SpaceOp_, collection);
    }

    Y_FORCE_INLINE void MergeRetainedStickyPages(
        TCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept {
        Cache_.Collections_.MergeRetainedStickyPages(SpaceOp_, collection, retainedHead, retainedTail);
    }

    Y_FORCE_INLINE bool LinkStickyPage(TCacheItem item, TSharedCacheItemRefImpl<TTraits>& collectionRef) noexcept {
        return Cache_.Collections_.LinkStickyPage(SpaceOp_, item, collectionRef);
    }

    Y_FORCE_INLINE void UnlinkPendingStickyPage(TCacheItem item) noexcept {
        Cache_.Collections_.UnlinkPendingStickyPage(SpaceOp_, item);
    }

    Y_FORCE_INLINE bool MakePageSticky(TCacheItem item) noexcept {
        return Cache_.Collections_.MakePageSticky(SpaceOp_, item);
    }

    Y_FORCE_INLINE bool UnstickyCutPages(TCacheItem item, ui64 allocationLimit) noexcept {
        return Cache_.Collections_.UnstickyCutPages(SpaceOp_, item, allocationLimit);
    }

    Y_FORCE_INLINE bool SetPagesCacheMode(TCollection& collection, ECacheMode mode) noexcept {
        return Cache_.Collections_.SetPagesCacheMode(SpaceOp_, collection, mode);
    }

    Y_FORCE_INLINE bool Detach(TCollectionRegistry& registry, TCacheItem item) noexcept {
        return Cache_.Collections_.Detach(SpaceOp_, registry, item);
    }

    Y_FORCE_INLINE bool Erase(TCollectionRegistry* expectedRegistry, TCacheItem item) noexcept {
        return Cache_.Collections_.Erase(SpaceOp_, expectedRegistry, item);
    }

    Y_FORCE_INLINE bool MoveRegistry(
        TCollectionRegistry& source, TCollectionRegistry& destination, TCacheItem item) noexcept {
        return Cache_.Collections_.MoveRegistry(SpaceOp_, source, destination, item);
    }

    Y_FORCE_INLINE bool Attach(TCollectionRegistry& registry, TCacheItem item) noexcept {
        return Cache_.Collections_.Attach(SpaceOp_, registry, item);
    }

    Y_FORCE_INLINE bool Link(TCollectionRegistry& registry, TCacheItem item) noexcept {
        return Cache_.Collections_.Link(SpaceOp_, registry, item);
    }

    Y_FORCE_INLINE bool Unlink(TCollectionRegistry& registry, TCacheItem item, EHandleState expectedState) noexcept {
        return Cache_.Collections_.Unlink(SpaceOp_, registry, item, expectedState);
    }

    Y_FORCE_INLINE bool Unlink(TCollectionRegistry& registry, TCacheItem item) noexcept {
        return Cache_.Collections_.Unlink(SpaceOp_, registry, item);
    }

    Y_FORCE_INLINE void Unlink(TCollectionRegistry& registry, TOperationItemRef<TTraits>& owner) noexcept {
        Cache_.Collections_.Unlink(registry, owner);
    }

    Y_FORCE_INLINE void ForEach(TFunctionRef<void(TCollection&)> callback) noexcept {
        Cache_.Collections_.ForEach(SpaceOp_, *Cache_.Collections_.CollectionRegistry_, callback);
    }

private:
    friend class TCacheCollections<TTraits>;
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheOperationView<TTraits>;

    Y_FORCE_INLINE TCacheCollectionsOperation(TSharedCacheImpl<TTraits>& cache, TSpaceOperation& spaceOp) noexcept
        : Cache_(cache)
        , SpaceOp_(spaceOp)
    {}

private:
    TSharedCacheImpl<TTraits>& Cache_;
    TSpaceOperation& SpaceOp_;
};

} // namespace NKikimr::NSharedCache
