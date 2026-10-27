#pragma once

#include "shared_cache_ref.h"
#include "shared_cache_collection.h"
#include "shared_cache_eviction.h"
#include "shared_cache_traits.h"
#include "shared_cache_space.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
class TCacheItems {
public:
    enum class EAcquireStatus {
        Stale,
        Pending,
        Ready,
    };

    using TOperation = TCacheItemsOperation<TTraits>;

    // Payload sizes
    static ui64 PayloadBytes(const THandle& handle, EItemKind kind) noexcept {
        return kind == EItemKind::Page ? PageBytes(handle) : handle.Metadata.PayloadBytes;
    }

    static ui64 PageBytes(const THandle& handle) noexcept {
        return AccountedPageBytes(handle.Key2OrSize.load(std::memory_order_relaxed));
    }

    static ui64 AccountedPageBytes(ui64 size) noexcept {
        Y_DEBUG_ABORT_UNLESS(size <= Max<ui64>() - NActors::TSharedData::OverheadSize);
        return size + NActors::TSharedData::OverheadSize;
    }

    explicit TCacheItems(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    Y_FORCE_INLINE TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { Cache_, spaceOp };
    }

    // Payload destruction
    void DeletePayload(THandle& handle, EItemKind kind, EHandleState state, TCollection* owner = nullptr) noexcept;

private:
    friend class TCacheItemsOperation<TTraits>;
    // Structural reference acquisition
    TSharedCacheItemRefImpl<TTraits> TryAcquireItemRef(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    // Reference acquisition
    Y_FORCE_INLINE EAcquireStatus TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowPending,
        TSharedCacheItemRefImpl<TTraits>& ref) noexcept;
    TSharedCacheItemRefImpl<TTraits> TryAcquireHotItemRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireMorePageRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;
    TSharedCacheItemRefImpl<TTraits> AcquireMoreCollectionRef(TSpaceOperation& spaceOp, TCacheItem item) noexcept;

    // Reference construction
    TSharedPageRefImpl<TTraits> BuildPageRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;
    TSharedCacheCollectionRefImpl<TTraits> BuildCollectionRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;

    // Reference release
    Y_FORCE_INLINE TCollection* ReleaseItem(
        TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowKeepCold) noexcept;

    Y_FORCE_INLINE bool TryReleaseItemRef(TSpaceOperation& spaceOp, TCacheItem cacheItem, ui64& expectedRaw) noexcept;

    TCollection* ReleaseTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    // Allocation
    ui32 TryAllocateAvailableHandle(TSpaceOperation& spaceOp) noexcept;
    ui32 TryAllocateHandle(TSpaceOperation& spaceOp) noexcept;
    TCacheItem Allocate(TSpaceOperation& spaceOp, ui32 index) noexcept;
    TCacheItem Allocate(TSpaceOperation& spaceOp) noexcept;
    TCacheItem AllocateForPreload(TSpaceOperation& spaceOp) noexcept;
    TCacheItem TryAllocateReservedItem(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages) noexcept;

    // Spare handles
    void RefillSpareItem(TSpaceOperation& spaceOp) noexcept;

    // Candidate cleanup
    void RecycleCandidate(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;
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

// Leave operation constructors implicitly instantiated
template <class TTraits>
class TCacheItemsOperation {
public:
    using EAcquireStatus = typename TCacheItems<TTraits>::EAcquireStatus;

    // Structural protection
    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> TryAcquireStructural(TCacheItem item) noexcept {
        return Cache_.Items_.TryAcquireItemRef(SpaceOp_, item);
    }

    // Reference acquisition
    Y_FORCE_INLINE EAcquireStatus TryAcquire(
        TCacheItem cacheItem, bool allowPending, TSharedCacheItemRefImpl<TTraits>& ref) noexcept {
        return Cache_.Items_.TryAcquire(SpaceOp_, cacheItem, allowPending, ref);
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> TryAcquireHotItemRef(TCacheItem item) noexcept {
        return Cache_.Items_.TryAcquireHotItemRef(SpaceOp_, item);
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> AcquireMorePageRef(TCacheItem item) noexcept {
        return Cache_.Items_.AcquireMorePageRef(SpaceOp_, item);
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> AcquireMoreCollectionRef(TCacheItem item) noexcept {
        return Cache_.Items_.AcquireMoreCollectionRef(SpaceOp_, item);
    }

    // Reference construction
    Y_FORCE_INLINE TSharedPageRefImpl<TTraits> BuildPageRef(TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
        return Cache_.Items_.BuildPageRef(SpaceOp_, std::move(ref));
    }

    Y_FORCE_INLINE TSharedCacheCollectionRefImpl<TTraits> BuildCollectionRef(
        TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
        return Cache_.Items_.BuildCollectionRef(SpaceOp_, std::move(ref));
    }

    // Reference release
    Y_FORCE_INLINE TCollection* ReleaseItem(TCacheItem cacheItem, bool allowKeepCold) noexcept {
        return Cache_.Items_.ReleaseItem(SpaceOp_, cacheItem, allowKeepCold);
    }

    // Allocation
    Y_FORCE_INLINE ui32 TryAllocateHandle() noexcept {
        return Cache_.Items_.TryAllocateHandle(SpaceOp_);
    }

    Y_FORCE_INLINE TCacheItem Allocate() noexcept {
        return Cache_.Items_.Allocate(SpaceOp_);
    }

    Y_FORCE_INLINE TCacheItem AllocateForPreload() noexcept {
        return Cache_.Items_.AllocateForPreload(SpaceOp_);
    }

    Y_FORCE_INLINE TCacheItem TryAllocateReservedItem(ui64 bytes, ui64 pages) noexcept {
        return Cache_.Items_.TryAllocateReservedItem(SpaceOp_, bytes, pages);
    }

    // Spare handles
    Y_FORCE_INLINE void RefillSpareItem() noexcept {
        Cache_.Items_.RefillSpareItem(SpaceOp_);
    }

    // Candidate cleanup
    Y_FORCE_INLINE void RecycleCandidate(TCacheItem cacheItem) noexcept {
        Cache_.Items_.RecycleCandidate(SpaceOp_, cacheItem);
    }

    Y_FORCE_INLINE void DiscardCandidate(TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes) noexcept {
        Cache_.Items_.DiscardCandidate(SpaceOp_, cacheItem, kind, reservedBytes);
    }

    // Handle initialization
    Y_FORCE_INLINE void InitializePage(TCacheItem cacheItem, TCollectionCacheItem collection, ui64 offset, ui64 size,
        NTable::NPage::EPage type, ui32 crc32, EStickyState sticky, bool reserved, bool collectionOwnerHeld) noexcept {
        Cache_.Items_.InitializePage(
            SpaceOp_, cacheItem, collection, offset, size, type, crc32, sticky, reserved, collectionOwnerHeld);
    }

    Y_FORCE_INLINE void InitializeCollection(TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept {
        Cache_.Items_.InitializeCollection(SpaceOp_, cacheItem, id, bytes);
    }

private:
    friend class TCacheItems<TTraits>;
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheOperationView<TTraits>;

    Y_FORCE_INLINE TCacheItemsOperation(TSharedCacheImpl<TTraits>& cache, TSpaceOperation& spaceOp) noexcept
        : Cache_(cache)
        , SpaceOp_(spaceOp)
    {}

private:
    TSharedCacheImpl<TTraits>& Cache_;
    TSpaceOperation& SpaceOp_;
};

// Reference construction
template <class TTraits>
TSharedPageRefImpl<TTraits> TCacheItems<TTraits>::BuildPageRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
    Y_DEBUG_ABORT_UNLESS(ref);
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    Y_DEBUG_ABORT_UNLESS([&] {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        return cacheItem.Matches(state) && state.IsPageKind() &&
               (state.IsReady() || state.IsReplacing() || state.IsReplaced() || state.IsTombstone()) &&
               state.Refs() > 0 && handle.Body.PageBuffer.data();
    }());
    return TSharedPageRefImpl<TTraits>(std::move(ref));
}

template <class TTraits>
TSharedCacheCollectionRefImpl<TTraits> TCacheItems<TTraits>::BuildCollectionRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    Y_DEBUG_ABORT_UNLESS([&] {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        return cacheItem.Matches(state) && state.IsCollectionKind() &&
               (state.IsSticky() || state.IsHot() || state.IsCold() || state.IsTombstone()) && state.Refs() > 0 &&
               handle.Body.Collection;
    }());
    return TSharedCacheCollectionRefImpl<TTraits>(std::move(ref));
}

// Reference acquisition
template <class TTraits>
Y_FORCE_INLINE typename TCacheItems<TTraits>::EAcquireStatus TCacheItems<TTraits>::TryAcquire(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowPending, TSharedCacheItemRefImpl<TTraits>& ref) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    if (!spaceOp.Contains(cacheItem)) {
        return EAcquireStatus::Stale;
    }

    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || expected.IsFree() || expected.IsReplacing() || expected.IsReplaced() ||
            expected.IsTombstone())
        {
            return EAcquireStatus::Stale;
        }

        if (expected.IsPending() && !allowPending) {
            return EAcquireStatus::Pending;
        }
        if (!expected.IsPending() && !expected.IsReady()) {
            return EAcquireStatus::Stale;
        }

        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        THandleState desired = expected.IncrementRefs();
        const bool pending = expected.IsPending();
        if (expected.IsHot()) {
            desired = desired.WithFrequency(Min<ui8>(expected.Frequency() + 1, 2));
        }

        if (expected.IsKeepCold()) {
            desired = desired.WithState(EHandleState::Hot).WithFrequency(0);
        }
        if (!handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            continue;
        }
        if (expected.IsCold()) {
            Cache_.UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired);
        }
        if (expected.IsKeepCold()) {
            Cache_.AdvanceColdMembership(handle);
            const ui64 bytes = PageBytes(handle);
            Cache_.Memory_.KeepCold().TransferBytesTo(Cache_.Memory_.Hot(), bytes);
            Cache_.Memory_.KeepCold().TransferPageTo(Cache_.Memory_.Hot(), EItemKind::Page);
            Cache_.Memory_.ReleaseKeepColdOwnedBytes(bytes);
            cacheOp.Eviction().RouteHot(EHotLevel::L1, cacheItem);
        } else if (expected.IsCold() && expected.IsPageKind()) {
            if (cacheOp.Eviction().PromoteCold(cacheItem)) {
                cacheOp.Eviction().RouteHot(EHotLevel::L1, cacheItem);
            } else {
                Cache_.AdvanceColdMembership(handle);
            }
        }
        ref = TSharedCacheItemRefImpl<TTraits>(cacheItem);
        return pending ? EAcquireStatus::Pending : EAcquireStatus::Ready;
    }
}

// Reference release
template <class TTraits>
Y_FORCE_INLINE TCollection* TCacheItems<TTraits>::ReleaseItem(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowKeepCold) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (cacheItem.Matches(expected) && expected.IsTombstone()) {
            return ReleaseTombstone(spaceOp, cacheItem);
        }
        if (cacheItem.Matches(expected) && expected.IsReplacing()) {
            cacheOp.ReleaseReplacing(cacheItem);
            return nullptr;
        }
        if (cacheItem.Matches(expected) && expected.IsReplaced()) {
            cacheOp.ReleaseReplaced(cacheItem);
            return nullptr;
        }
        if (cacheItem.Matches(expected) && expected.IsPageKind() && expected.IsPending())
        {
            TPageFetchState* fetch = handle.Body.Fetch;
            Y_DEBUG_ABORT_UNLESS(fetch && fetch->Page().CacheItem() == cacheItem);
            TOperationItemRef<TTraits> ownerRef(spaceOp, cacheItem);
            cacheOp.Fetches().Release(ownerRef, *fetch);
            return nullptr;
        }
        if (cacheItem.Matches(expected) && expected.IsCold() && expected.IsPageKind() && expected.Refs() == 1 &&
            Cache_.SoftLimit() == 0) {
            TOperationItemRef<TTraits> ownerRef(spaceOp, cacheItem);
            THandleState checked = expected;
            if (!cacheOp.TryBeginErase(ownerRef.Get(), checked)) {
                ownerRef.Disarm();
                expectedRaw = checked.Raw();
                continue;
            }
            cacheOp.Eviction().CompleteColdPageRelease(ownerRef);
            return nullptr;
        }
        Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) && expected.Refs() > 0);
        if (allowKeepCold && expected.IsCold() && expected.IsPageKind() && expected.Refs() == 1 &&
            cacheOp.Eviction().WantsKeepCold(handle) && Cache_.Memory_.TryAddRetainedBytes(PageBytes(handle)))
        {
            if (cacheOp.Eviction().TryReleaseColdPageToKeepCold(cacheItem, expectedRaw)) {
                return nullptr;
            }
            continue;
        }
        if (TryReleaseItemRef(spaceOp, cacheItem, expectedRaw)) {
            return nullptr;
        }
    }
}

template <class TTraits>
Y_FORCE_INLINE bool TCacheItems<TTraits>::TryReleaseItemRef(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, ui64& expectedRaw) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState expected = THandleState::FromRaw(expectedRaw);
    const bool publishCold = expected.IsCold() && expected.Refs() == 1;
    const bool lastKeepColdRef = expected.IsKeepCold() && expected.Refs() == 1;
    const bool wantsKeepCold = lastKeepColdRef && cacheOp.Eviction().WantsKeepCold(handle);
    const ui64 coldBytes = publishCold ? PayloadBytes(handle, expected.Kind()) : 0;
    // Capture while the native ref protects the payload; detach/reclaim may follow the final CAS.
    const bool notifyCollectionRelease =
        expected.IsCollectionKind() && expected.IsSticky() && expected.Refs() == 1 && expected.IsStickyField() &&
        handle.Body.Collection->GetActorState() &&
        handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) != 0;
    const TLogoBlobID collectionId = notifyCollectionRelease ? handle.Body.Collection->Id() : TLogoBlobID{};
    const THandleState desired = expected.DecrementRefs();
    if (publishCold) {
        Cache_.InvokeHook(ESharedCacheHookPoint::BeforeLastColdRefRelease, cacheItem);
    }
    if (handle.State.compare_exchange_weak(
            expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
    {
        if (notifyCollectionRelease) {
            // No native metadata owner remains to hand these writer-held Sticky pages to an executor.
            Cache_.NotifyCollectionReleased(collectionId, TCollectionCacheItem::FromValidated(cacheItem));
        }
        Cache_.UpdateColdReclaimable(coldBytes, expected, desired);
        if (publishCold) {
            // Acquire membership updates carried by earlier reference releases after a refcount ABA
            std::atomic_thread_fence(std::memory_order_acquire);
            cacheOp.Eviction().PublishCold(cacheItem);
        } else if (lastKeepColdRef) {
            Cache_.InvokeHook(ESharedCacheHookPoint::AfterKeepColdRefReleased, cacheItem);
            // Check after releasing the final ref: a concurrent trim may have withdrawn this page's ring entry
            // while the ref was being dropped. Policy was read while the original page was still pinned.
            if (!Cache_.Eviction_.IsKeepColdQueued(handle) || !wantsKeepCold) {
                const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire);
                cacheOp.Eviction().DemoteKeepCold(TColdCacheItem::Make(coldVersion & MaxItemVersion, cacheItem.Index()),
                    TPageCacheItem::FromValidated(cacheItem));
            }
        }
        if (expected.IsReady() && desired.Refs() == 0 && expected.IsStickyNoneField()) {
            Cache_.NotifyResourcesAvailable();
        }
        return true;
    }
    return false;
}

// Inline implementations

template <class TTraits>
inline TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::TryAcquireItemRef(
    TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen())
    {
        return {};
    }

    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || expected.IsFree() || expected.IsCompleting() || expected.IsReplacing() ||
            expected.IsReplaced() || expected.IsTombstone())
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

} // namespace NKikimr::NSharedCache
