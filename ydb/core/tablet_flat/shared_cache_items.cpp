#include "shared_cache_items.h"

#include <new>

namespace NKikimr::NSharedCache {

template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::TryAcquireHotEvictionPin(
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
TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::AcquireHeldPage(
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
TSharedCacheItemRefImpl<TTraits> TCacheItems<TTraits>::AcquireHeldCollection(
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
