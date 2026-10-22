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
    if (reservedBytes != 0) {
        Cache_.ReleaseReservation(reservedBytes, expected.IsPageKind() ? 1 : 0);
    }
    Y_ABORT_UNLESS(spaceOp.ReturnFreeHandle(index, desired.Version()));
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
