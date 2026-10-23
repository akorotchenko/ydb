#include "shared_cache_items.h"
#include "shared_cache.h"

#include <new>

namespace NKikimr::NSharedCache {

template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::TryAcquireHotItemRef(
    TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen()) {
        return {};
    }
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsHot() || !expected.IsStickyNoneField()) {
            return {};
        }
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            return TSharedCacheItemRefImpl<TTraits>(cacheItem);
        }
    }
}

template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::AcquireMorePageRef(
    TSpaceOperation& spaceOp, TCacheItem item) noexcept {
    THandle& handle = spaceOp.Handles()[item.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        // The source owner protects this physical item, including a withdrawn or relocated predecessor.
        Y_ABORT_UNLESS(item.Matches(expected) && expected.IsPageKind() && expected.Refs() != 0);
        Y_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        if (handle.State.compare_exchange_weak(
                expectedRaw, expected.IncrementRefs().Raw(), std::memory_order_relaxed, std::memory_order_relaxed)) {
            return TSharedCacheItemRefImpl<TTraits>(item);
        }
    }
}

template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::AcquireMoreCollectionRef(
    TSpaceOperation& spaceOp, TCacheItem item) noexcept {
    THandle& handle = spaceOp.Handles()[item.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(
            item.Matches(expected) && expected.IsCollectionKind() && expected.Refs() != 0 && handle.Body.Collection);
        Y_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        if (handle.State.compare_exchange_weak(
                expectedRaw, expected.IncrementRefs().Raw(), std::memory_order_relaxed, std::memory_order_relaxed)) {
            return TSharedCacheItemRefImpl<TTraits>(item);
        }
    }
}

template <class TTraits>
TCollection* TCacheItems<TTraits>::ReleaseTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(cacheItem.Matches(expected) && expected.IsTombstone() && expected.Refs() > 0);
        if (expected.Refs() > 1) {
            const THandleState desired = expected.DecrementRefs();
            if (handle.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
            {
                if (cacheItem.IsResizeMarker() && expected.Refs() == 2) {
                    Cache_.NotifyResourcesAvailable();
                }
                return nullptr;
            }
            continue;
        }

        if (cacheItem.IsResizeMarker()) {
            const ui32 nextVersion = AdvanceItemVersion(expected.Version());
            const THandleState desired = expected.WithVersion(nextVersion)
                                             .WithState(EHandleState::Free)
                                             .WithFrequency(0)
                                             .WithSticky(EStickyState::None)
                                             .WithRefs(0);
            if (handle.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                Cache_.NotifyResourcesAvailable();
                return nullptr;
            }
            continue;
        }

        TCollection* owner = expected.IsPageKind() ? cacheOp.Collections().PageCollection(handle) : nullptr;
        const TSharedCacheKey key = LoadSharedCacheKey(handle, expected.Kind());
        cacheOp.FinishErase(cacheItem, key);
        Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() ||
                             handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) == 0);
        const ui32 nextVersion = AdvanceItemVersion(expected.Version());
        const THandleState desired = expected.WithVersion(nextVersion)
                                         .WithState(EHandleState::Free)
                                         .WithFrequency(0)
                                         .WithSticky(EStickyState::None)
                                         .WithRefs(0);
        Cache_.InvokeHook(ESharedCacheHookPoint::BeforeTombstoneFinalCas, cacheItem);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            Y_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_acquire) == 0);
            DeletePayload(handle, expected.Kind(), expected.State(), owner);
            Y_ABORT_UNLESS(spaceOp.ReturnFreeHandle(cacheItem.Index(), nextVersion));
            Cache_.NotifyResourcesAvailable();
            if (expected.IsCollectionKind()) {
                Cache_.NotifyCollectionReleased(
                    TLogoBlobID(key.Word(0), key.Word(1), key.Word(2)), TCollectionCacheItem::FromValidated(cacheItem));
            }
            return owner;
        }
    }
}

template <class TTraits>
ui32 TCacheItems<TTraits>::TryAllocateHandle(TSpaceOperation& spaceOp) noexcept {
    auto evictionOp = Cache_.Eviction_.BindOperation(spaceOp);
    const TCacheItem spare = TCacheItem::FromRaw(spaceOp.Hazard().SpareItem.exchange(0, std::memory_order_relaxed));
    if (spare.IsNull()) {
        if (const ui32 index = spaceOp.TryAllocateHandle()) {
            return index;
        }
        if (spaceOp.ReclaimSpareHandle()) {
            Cache_.NotifyResourcesAvailable();
            if (const ui32 index = spaceOp.TryAllocateHandle()) {
                return index;
            }
        }
        // Byte headroom does not guarantee a free handle when pages are much smaller than the expected size.
        // Reclaim Cold, or shrink Hot to feed Cold, before refusing the admission.
        const ui32 workLimit = static_cast<ui32>(Min<ui64>(spaceOp.HandleCount(), SharedCacheTransitionWorkBatch));
        for (ui32 attempt = 0; attempt < workLimit; ++attempt) {
            bool progress = evictionOp.ReclaimCold();
            if (!progress) {
                progress = evictionOp.EnforceCurrentLimit(0, true);
                // The caller's view may predate growth; drain with a fresh operation under the resize lock.
                progress = Cache_.DrainHotResize() || progress;
                progress = evictionOp.ReclaimCold() || progress;
            }
            if (const ui32 index = spaceOp.TryAllocateHandle()) {
                return index;
            }
            if (!progress) {
                return 0;
            }
            Cache_.InvokeHook(ESharedCacheHookPoint::AfterAdmissionReclaimStep, TCacheItem{});
        }
        Cache_.NotifyResourcesAvailable();
        return 0;
    }

    Y_ABORT_UNLESS(!spare.IsFrozen() && spare.Index() >= 2 && spare.Index() < spaceOp.AccessibleHandleCount());
    const THandle& handle = spaceOp.Handles()[spare.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_ABORT_UNLESS(spare.Matches(state) && state.IsBegin() && state.Refs() == 0);
    return spare.Index();
}

template <class TTraits>
TCacheItem TCacheItems<TTraits>::Allocate(TSpaceOperation& spaceOp) noexcept {
    const ui32 index = TryAllocateHandle(spaceOp);
    if (!index) {
        return {};
    }
    const THandleState state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(state.IsBegin() && state.Refs() == 0);
    spaceOp.Handles()[index].Key0.store(0, std::memory_order_relaxed);
    spaceOp.Handles()[index].Body.Fetch = nullptr;
    return TCacheItem::Make(state.Version(), index);
}

template <class TTraits>
TCacheItem TCacheItems<TTraits>::TryAllocateReservedItem(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages) noexcept {
    if (!Cache_.TryReserve(spaceOp, bytes, pages)) {
        return {};
    }
    const TCacheItem cacheItem = Allocate(spaceOp);
    if (cacheItem.IsNull()) {
        Cache_.ReleaseReservation(bytes, pages);
    }
    return cacheItem;
}

template <class TTraits>
void TCacheItems<TTraits>::RefillSpareItem(TSpaceOperation& spaceOp) noexcept {
    if (spaceOp.Hazard().SpareItem.load(std::memory_order_relaxed) != 0) {
        return;
    }
    const ui32 index = spaceOp.TryAllocateHandle();
    if (!index) {
        return;
    }
    THandle& handle = spaceOp.Handles()[index];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    // A recycled free handle may retain a destroyed buffer's union bits. Begin also represents
    // these uninitialized spares, so pending-page cleanup must see a blank fetch payload.
    handle.Key0.store(0, std::memory_order_relaxed);
    handle.Body.Fetch = nullptr;
    const TCacheItem cacheItem = TCacheItem::Make(state.Version(), index);
    if (spaceOp.TryStoreSpareItem(cacheItem)) {
        Cache_.NotifyResourcesAvailable();
    } else {
        DiscardCandidate(spaceOp, cacheItem, state.Kind(), 0);
    }
}

template <class TTraits>
void TCacheItems<TTraits>::RecycleCandidate(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto collectionsOp = Cache_.Collections_.BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (state.IsPageKind() && state.IsPending() && state.IsStickyField()) {
        collectionsOp.UnlinkPendingStickyPage(cacheItem);
        state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    }
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && !state.IsFree() && !state.IsTombstone() && state.Refs() == 0);
    const bool pending = state.IsPending();
    const bool reserved = pending && !state.IsWaitingForMemory();
    const ui64 reservedBytes = reserved ? PayloadBytes(handle, state.Kind()) : 0;
    if (!pending) {
        Y_DEBUG_ABORT_UNLESS(state.IsReady());
        Y_DEBUG_ABORT_UNLESS(!state.IsPageKind() || !state.IsSticky());
        Y_DEBUG_ABORT_UNLESS(!state.IsCollectionKind() ||
                             handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) == 0);
        Y_DEBUG_ABORT_UNLESS(
            !state.IsCollectionKind() || handle.Body.Collection->Registry_.load(std::memory_order_acquire) == nullptr);
        DeletePayload(
            handle, state.Kind(), state.State(), state.IsPageKind() ? collectionsOp.PageCollection(handle) : nullptr);
        collectionsOp.DropPageItemRef(handle);
        if (state.IsPageKind()) {
            handle.Key0.store(0, std::memory_order_relaxed);
            handle.Body.Fetch = nullptr;
        }
        state = state.WithState(EHandleState::Begin).WithFrequency(0).WithSticky(EStickyState::None);
        handle.State.store(state.Raw(), std::memory_order_relaxed);
    }
    if (pending) {
        const bool page = state.IsPageKind();
        if (page) {
            collectionsOp.DropPageItemRef(handle);
            handle.Key0.store(0, std::memory_order_relaxed);
            if (state.IsStickyField()) {
                Cache_.Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, PageBytes(handle));
            }
            TCacheFetches<TTraits>::DeletePendingFetch(handle);
        }
        if (reserved) {
            Cache_.ReleaseReservation(reservedBytes, page ? 1 : 0);
        }
        state = state.WithState(EHandleState::Begin).WithFrequency(0).WithSticky(EStickyState::None);
        handle.State.store(state.Raw(), std::memory_order_release);
    }
    if (spaceOp.TryStoreSpareItem(cacheItem)) {
        Cache_.NotifyResourcesAvailable();
    } else {
        DiscardCandidate(spaceOp, cacheItem, state.Kind(), 0);
    }
}

template <class TTraits>
void TCacheItems<TTraits>::DiscardCandidate(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes) noexcept {
    auto collectionsOp = Cache_.Collections_.BindOperation(spaceOp);
    const ui32 index = cacheItem.Index();
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AccessibleHandleCount());

    THandle& handle = spaceOp.Handles()[index];
    THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (expected.IsPageKind() && expected.IsPending() && expected.IsStickyField()) {
        collectionsOp.UnlinkPendingStickyPage(cacheItem);
        expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    }
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) && expected.Kind() == kind && !expected.IsFree() &&
                         !expected.IsTombstone() && expected.Refs() == 0);

    Y_DEBUG_ABORT_UNLESS(!expected.IsPageKind() || !expected.IsSticky());
    Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() || !expected.IsReady() ||
                         handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) == 0);
    Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() || !expected.IsReady() ||
                         handle.Body.Collection->Registry_.load(std::memory_order_acquire) == nullptr);

    const THandleState desired = expected.WithState(EHandleState::Free).WithFrequency(0).WithSticky(EStickyState::None);
    TCollection* owner = nullptr;
    if (expected.IsPageKind() && handle.Body.Collection) {
        owner = collectionsOp.PageCollection(handle);
    }
    Cache_.UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired);
    handle.State.store(desired.Raw(), std::memory_order_release);
    if (expected.IsPageKind() && expected.IsStickyField() && !expected.IsReady()) {
        Cache_.Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, PageBytes(handle));
    }
    if (expected.IsReady()) {
        DeletePayload(handle, expected.Kind(), expected.State(), owner);
    } else if (expected.IsPageKind() && expected.IsPending()) {
        TCacheFetches<TTraits>::DeletePendingFetch(handle);
    }
    if (owner) {
        collectionsOp.DropOwnerReference(*owner);
    }
    Y_ABORT_UNLESS(spaceOp.ReturnFreeHandle(index, desired.Version()));
    if (reservedBytes != 0) {
        Cache_.ReleaseReservation(reservedBytes, expected.IsPageKind() ? 1 : 0);
    } else {
        Cache_.NotifyResourcesAvailable();
    }
}

template <class TTraits>
void TCacheItems<TTraits>::InitializePage(TSpaceOperation& spaceOp, TCacheItem cacheItem,
    TCollectionCacheItem collection, ui64 offset, ui64 size, NTable::NPage::EPage type, ui32 crc32, EStickyState sticky,
    bool reserved, bool collectionOwnerHeld) noexcept {
    auto collectionsOp = Cache_.Collections_.BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AccessibleHandleCount());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState claimed = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(claimed) && claimed.IsBegin() && claimed.Refs() == 0);
    handle.Key0.store(collection.Raw(), std::memory_order_relaxed);
    handle.Key1.store(offset, std::memory_order_relaxed);
    handle.Key2OrSize.store(size, std::memory_order_relaxed);
    handle.Metadata.Page.Type = type;
    handle.Metadata.Page.Reserved = 0;
    handle.Metadata.Page.Crc32 = crc32;
    Y_DEBUG_ABORT_UNLESS(!handle.Body.Fetch);
    if (TCollection* owner = collectionsOp.PageCollection(handle)) {
        const THandle& collection = spaceOp.Handles()[owner->CacheItem.CacheItem().Index()];
        const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
        Y_ABORT_UNLESS(state.IsSticky() && (state.IsStickyField() || collectionOwnerHeld));
        Cache_.InvokeHook(ESharedCacheHookPoint::AfterPageCollectionStateChecked, owner->CacheItem.CacheItem());
        const ui64 previous = owner->References.fetch_add(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
    }
    handle.Body.Fetch = new TPageFetchState(TPageCacheItem::FromValidated(cacheItem));
    handle.Body.Fetch->Ref();
    handle.Next.store(TCacheItem::Make(claimed.Version(), 0).Raw(), std::memory_order_relaxed);
    handle.NextInOwner.store(0, std::memory_order_relaxed);
    if (sticky == EStickyState::Sticky) {
        Cache_.Memory_.AddStickyOwnedBytes(AccountedPageBytes(size));
    }
    const THandleState initialized = claimed.WithState(reserved ? EHandleState::Begin : EHandleState::WaitingForMemory)
                                         .WithKind(EItemKind::Page)
                                         .WithFrequency(0)
                                         .WithSticky(sticky);
    handle.State.store(initialized.Raw(), std::memory_order_relaxed);
    if (sticky == EStickyState::Sticky) {
        TOperationItemRef<TTraits> collectionRef(spaceOp);
        collectionsOp.LinkStickyPage(cacheItem, collectionRef.Get());
    }
}

template <class TTraits>
void TCacheItems<TTraits>::InitializeCollection(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept {
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AccessibleHandleCount());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState claimed = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(claimed) && claimed.IsBegin() && claimed.Refs() == 0);
    const ui64* words = id.GetRaw();
    handle.Key0.store(words[0], std::memory_order_relaxed);
    handle.Key1.store(words[1], std::memory_order_relaxed);
    handle.Key2OrSize.store(words[2], std::memory_order_relaxed);
    new (&handle.Body.Collection) TCollection*(nullptr);
    handle.Metadata.PayloadBytes = bytes;
    handle.Next.store(TCacheItem::Make(claimed.Version(), 0).Raw(), std::memory_order_relaxed);
    handle.NextInOwner.store(0, std::memory_order_relaxed);
    const THandleState initialized =
        claimed.WithKind(EItemKind::Collection).WithFrequency(0).WithSticky(EStickyState::Sticky);
    handle.State.store(initialized.Raw(), std::memory_order_relaxed);
}

template <class TTraits>
void TCacheItems<TTraits>::DeletePayload(
    THandle& handle, EItemKind kind, EHandleState state, TCollection* owner) noexcept {
    Y_DEBUG_ABORT_UNLESS(IsReady(state) || state == EHandleState::Tombstone);
    Y_DEBUG_ABORT_UNLESS(kind == EItemKind::Page || handle.Body.Collection);
    const bool hasPayload = kind != EItemKind::Page || bool(handle.Body.PageBuffer);
    Y_DEBUG_ABORT_UNLESS(state == EHandleState::Tombstone || hasPayload);
    const ui64 bytes = hasPayload ? PayloadBytes(handle, kind) : 0;
    if (kind == EItemKind::Page) {
        if (hasPayload && owner) {
            Cache_.Collections_.AddResidentPageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            if (state == EHandleState::Hot || state == EHandleState::Sticky || state == EHandleState::KeepCold) {
                Cache_.Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
        }
        handle.Body.PageBuffer.~TBuffer();
    } else {
        Y_DEBUG_ABORT_UNLESS(hasPayload);
        delete std::exchange(handle.Body.Collection, nullptr);
        Cache_.Memory_.RemoveCollection(bytes);
    }
    if (hasPayload) {
        const i64 estimatedBytes = static_cast<i64>(bytes);
        if (state == EHandleState::Hot) {
            Cache_.Memory_.Hot().RemoveBytes(estimatedBytes);
            Cache_.Memory_.Hot().RemovePage(kind);
        } else if (state == EHandleState::Sticky) {
            Cache_.Memory_.Sticky().RemoveBytes(estimatedBytes);
            Cache_.Memory_.Sticky().RemovePage(kind);
            Cache_.Memory_.ReleaseStickyOwnedBytes(kind, bytes);
        } else if (state == EHandleState::KeepCold) {
            Cache_.Memory_.KeepCold().RemoveBytes(estimatedBytes);
            Cache_.Memory_.KeepCold().RemovePage(kind);
            Cache_.Memory_.ReleaseKeepColdOwnedBytes(bytes);
        } else {
            Cache_.Memory_.Cold().RemoveBytes(estimatedBytes);
            Cache_.Memory_.Cold().RemovePage(kind);
            Cache_.Memory_.RemoveColdItem();
        }
        Cache_.Memory_.RemoveResident(bytes);
        if (state == EHandleState::Hot || state == EHandleState::Sticky || state == EHandleState::KeepCold) {
            Cache_.Memory_.ReleaseRetainedBytes(bytes);
        }
        Cache_.Memory_.ReleasePayloadCharge(bytes, kind);
        if (kind == EItemKind::Page) {
            Cache_.NotifyResourcesAvailable();
        }
    }
}

// Native item operations are instantiated here for both trait variants
template class TCacheItems<TProdTraits>;
template class TCacheItems<TTestTraits>;

} // namespace NKikimr::NSharedCache
