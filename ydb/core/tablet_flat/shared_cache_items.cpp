#include "shared_cache_items.h"
#include "shared_cache_collections.h"
#include "shared_cache_memory.h"
#include "shared_cache_space.h"
#include "shared_cache_traits.h"

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
void TCacheItems<TTraits>::InitializePage(TSpaceOperation& spaceOp, TCacheCollections<TTraits>& collections,
    TCacheItem cacheItem, TCollectionCacheItem collection, ui64 offset, ui64 size, NTable::NPage::EPage type,
    ui32 crc32, EStickyState sticky, bool reserved, bool collectionOwnerHeld) noexcept {
    auto collectionsOp = collections.BindOperation(spaceOp);
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
        Traits_.Invoke(ESharedCacheHookPoint::AfterPageCollectionStateChecked, owner->CacheItem.CacheItem());
        const ui64 previous = owner->References.fetch_add(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
    }
    handle.Body.Fetch = new TPageFetchState(TPageCacheItem::FromValidated(cacheItem));
    handle.Body.Fetch->Ref();
    handle.Next.store(TCacheItem::Make(claimed.Version(), 0).Raw(), std::memory_order_relaxed);
    handle.NextInOwner.store(0, std::memory_order_relaxed);
    if (sticky == EStickyState::Sticky) {
        Memory_.AddStickyOwnedBytes(AccountedPageBytes(size));
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

// Native item operations are instantiated here for both trait variants
template class TCacheItems<TProdTraits>;
template class TCacheItems<TTestTraits>;

} // namespace NKikimr::NSharedCache
