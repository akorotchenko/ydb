#include "shared_cache.h"

namespace NKikimr::NSharedCache {

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TSharedCacheItemRef TSharedCacheItemRefImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef TSharedCache::TryAcquirePageForRelocation(TSpaceOperation& spaceOp, ui32 index) noexcept {
    THandle& source = spaceOp.Handles()[index];
    ui64 expectedRaw = source.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        const bool ordinary =
            (expected.IsHot() || expected.IsCold()) && expected.IsStickyNoneField() && expected.Refs() != 0;
        const bool unsticky = expected.IsSticky() && expected.IsTransitionField();
        if (!expected.IsPageKind() || (!ordinary && !unsticky))
        {
            return {};
        }
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs();
        if (source.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            return TSharedCacheItemRef(TCacheItem::Make(expected.Version(), index));
        }
    }
}

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCache::PreparePageReplacement(TSpaceOperation& spaceOp, TCacheItem sourceItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& source = spaceOp.Handles()[sourceItem.Index()];
    const THandleState sourceState = THandleState::FromRaw(source.State.load(std::memory_order_acquire));
    const bool restoreSticky = sourceState.IsSticky() && sourceState.IsTransitionField();
    if (!restoreSticky && (!(sourceState.IsHot() || sourceState.IsCold()) || !sourceState.IsStickyNoneField())) {
        return {};
    }
    // Allocate may reclaim this Cold source. Its native reference keeps the payload alive,
    // but only the later Replacing claim can exclude logical deletion.
    const TCacheItem replacementItem = cacheOp.Allocate();
    if (replacementItem.IsNull()) {
        return {};
    }

    const TCollectionCacheItem collection =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(source.Key0.load(std::memory_order_relaxed)));
    const ui64 offset = source.Key1.load(std::memory_order_relaxed);
    const ui64 size = source.Key2OrSize.load(std::memory_order_relaxed);
    const NTable::NPage::EPage type = source.Metadata.Page.Type;
    const ui32 crc32 = source.Metadata.Page.Crc32;
    NActors::TSharedData data = source.Body.PageBuffer.Share(size);

    // Relocation transfers the source's sticky charge; claiming a second budget slot would double count it.
    cacheOp.InitializePage(replacementItem, collection, offset, size, type, crc32, EStickyState::None, true, true);
    THandle& replacement = spaceOp.Handles()[replacementItem.Index()];
    TCacheFetches<TTraits>::DeletePendingFetch(replacement);
    new (&replacement.Body.PageBuffer) NActors::TSharedData::TBuffer(std::move(data).Extract());
    const THandleState initialized = THandleState::FromRaw(replacement.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(replacementItem.Matches(initialized) && initialized.IsBegin() && initialized.Refs() == 0);
    const EHandleState replacementState = restoreSticky ? EHandleState::Sticky : sourceState.State();
    replacement.State.store(initialized.WithState(replacementState)
                                .WithSticky(restoreSticky ? EStickyState::Sticky : EStickyState::None)
                                .WithRefs(1)
                                .Raw(), std::memory_order_release);
    if (restoreSticky) {
        Y_DEBUG_ABORT_UNLESS(source.NextInOwner.load(std::memory_order_acquire) == 0);
        TOperationItemRef collectionRef(spaceOp);
        if (!cacheOp.Collections().LinkStickyPage(replacementItem, collectionRef.Get())) {
            cacheOp.DiscardPageReplacement(replacementItem);
            return {};
        }
    }
    return replacementItem;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DiscardPageReplacement(TSpaceOperation& spaceOp, TCacheItem replacementItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& replacement = spaceOp.Handles()[replacementItem.Index()];
    const THandleState state = THandleState::FromRaw(replacement.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(replacementItem.Matches(state) && state.IsPageKind() &&
                         (state.IsSticky() || state.IsHot() || state.IsCold()) && state.Refs() == 1 &&
                         replacement.NextInOwner.load(std::memory_order_relaxed) == 0);
    cacheOp.Collections().DropPageItemRef(replacement);
    replacement.Body.PageBuffer.~TBuffer();
    replacement.Body.Fetch = nullptr;
    replacement.State.store(state.WithState(EHandleState::Begin).WithSticky(EStickyState::None).WithRefs(0).Raw(),
        std::memory_order_relaxed);
    cacheOp.DiscardCandidate(replacementItem, EItemKind::Page);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PublishPageReplacement(
    TSpaceOperation& spaceOp, TCacheItem sourceItem, TCacheItem replacementItem, EHandleState& oldState) noexcept {
    THandle& source = spaceOp.Handles()[sourceItem.Index()];
    THandle& replacement = spaceOp.Handles()[replacementItem.Index()];

    ui64 expectedRaw = source.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        const bool ordinary = (expected.IsHot() || expected.IsCold()) && expected.IsStickyNoneField();
        const bool unsticky = expected.IsSticky() && expected.IsTransitionField();
        Y_DEBUG_ABORT_UNLESS(sourceItem.Matches(expected) && expected.IsPageKind() && expected.Refs() > 0);
        if (!ordinary && !unsticky) {
            return false;
        }
        const THandleState desired = expected.WithState(EHandleState::Replacing).WithFrequency(0);
        if (source.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
            oldState = expected.State();
            break;
        }
    }

    // A held Cold page may have been promoted while the replacement was prepared.
    // A Sticky replacement may already have been drained to Hot by its collection.
    if (oldState != EHandleState::Sticky) {
        const THandleState prepared = THandleState::FromRaw(replacement.State.load(std::memory_order_relaxed));
        replacement.State.store(prepared.WithState(oldState).Raw(), std::memory_order_release);
    }

    // Replacing with an unfrozen Next makes readers retry. Freeze only the forwarder:
    // freezing the old successor would make readers mistake it for the replacement.
    ui64 successorRaw = source.Next.load(std::memory_order_acquire);
    for (;;) {
        const TCacheItem successor = TCacheItem::FromRaw(successorRaw);
        Y_DEBUG_ABORT_UNLESS(!successor.IsFrozen());
        const TCacheItem next = successor.IsNull() ? TCacheItem::Make(replacementItem.Version(), 0) : successor;
        replacement.Next.store(next.Raw(), std::memory_order_relaxed);
        InvokeHook(ESharedCacheHookPoint::BeforePageReplacementLinkCas, sourceItem);
        if (source.Next.compare_exchange_weak(successorRaw, replacementItem.WithFrozen().Raw(),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
        // An insertion that already owned this link may have changed the successor.
    }
    InvokeHook(ESharedCacheHookPoint::AfterReplacingPublished, sourceItem);
    const TSharedCacheKey key = LoadSharedCacheKey(source, EItemKind::Page);
    auto cacheOp = BindOperation(spaceOp);
    cacheOp.PublishPageReplacement(sourceItem, replacementItem, key);
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::CompletePageReplacement(TSpaceOperation& spaceOp, TCacheItem sourceItem) noexcept {
    THandle& source = spaceOp.Handles()[sourceItem.Index()];
    ui64 expectedRaw = source.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(
            sourceItem.Matches(expected) && expected.IsPageKind() && expected.IsReplacing() && expected.Refs() > 0);
        const THandleState desired = expected.WithState(EHandleState::Replaced);
        if (source.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            InvokeHook(ESharedCacheHookPoint::AfterReplacedPublished, sourceItem);
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::RelocatePage(TSpaceOperation& spaceOp, ui32 index) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    TOperationItemRef sourceRef(spaceOp, cacheOp.TryAcquirePageForRelocation(index));
    if (!sourceRef) {
        return false;
    }
    const TCacheItem sourceItem = sourceRef.CacheItem();
    const TCacheItem replacementItem = cacheOp.PreparePageReplacement(sourceItem);
    if (replacementItem.IsNull()) {
        return false;
    }

    THandle& source = spaceOp.Handles()[sourceItem.Index()];
    EHandleState oldState;
    if (!cacheOp.PublishPageReplacement(sourceItem, replacementItem, oldState)) {
        cacheOp.DiscardPageReplacement(replacementItem);
        return false;
    }
    if (oldState == EHandleState::Cold) {
        AdvanceColdMembership(source);
    }
    if (oldState == EHandleState::Hot) {
        cacheOp.Eviction().RouteHot(EHotLevel::L1, replacementItem);
    }
    cacheOp.CompletePageReplacement(sourceItem);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DemoteUnlinkedStickyPage(TSpaceOperation& spaceOp, ui32 index) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    TOperationItemRef pageRef(spaceOp, cacheOp.TryAcquirePageForRelocation(index));
    if (!pageRef) {
        return false;
    }
    const TCacheItem pageItem = pageRef.CacheItem();
    THandle& page = spaceOp.Handles()[index];
    ui64 expectedRaw = page.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        const bool unlinkedCutPage =
            pageItem.Matches(expected) && expected.IsPageKind() && expected.IsSticky() && expected.IsTransitionField();
        if (!unlinkedCutPage) {
            return false;
        }
        Y_ABORT_UNLESS(page.NextInOwner.load(std::memory_order_acquire) == 0);

        if (TCollection* owner = cacheOp.Collections().PageCollection(page)) {
            const THandleState ownerState = THandleState::FromRaw(
                spaceOp.Handles()[owner->CacheItem.Index()].State.load(std::memory_order_acquire));
            if (ownerState.IsTransitionField()) {
                return false; // Wait for attachment or detachment to finish.
            }
            if (ownerState.IsStickyField() &&
                owner->CacheMode_.load(std::memory_order_acquire) != ECacheMode::TryKeepInMemory) {
                return false;
            }
        }

        const THandleState desired =
            expected.WithState(EHandleState::Hot).WithSticky(EStickyState::None).WithFrequency(0);
        if (page.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
            const ui64 bytes = TCacheItems<TTraits>::PageBytes(page);
            Memory_.Sticky().TransferBytesTo(Memory_.Hot(), bytes);
            Memory_.Sticky().TransferPageTo(Memory_.Hot(), EItemKind::Page);
            Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
            cacheOp.Eviction().RouteHot(EHotLevel::L1, pageItem);
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
EShrinkCutStatus TSharedCache::ReclaimShrinkHandle(TSpaceOperation& spaceOp, ui32 index) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const THandleState state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_acquire));
    if (state.IsFree()) {
        Y_DEBUG_ABORT_UNLESS(state.Refs() == 0);
        return EShrinkCutStatus::Complete;
    }
    if (state.IsHot()) {
        if (state.Refs() != 0 && cacheOp.RelocatePage(index)) {
            return EShrinkCutStatus::Progress;
        }
        if (cacheOp.Eviction().EvictFromHot(index)) {
            return EShrinkCutStatus::Progress;
        }
    }
    if (state.IsCold()) {
        if (state.Refs() != 0 && cacheOp.RelocatePage(index)) {
            return EShrinkCutStatus::Progress;
        }
        if (state.Refs() == 0) {
            const ui32 coldVersion = spaceOp.Handles()[index].ColdVersion.load(std::memory_order_acquire);
            if (cacheOp.Eviction().EraseCold(TColdCacheItem::Make(coldVersion & MaxItemVersion, index))) {
                return EShrinkCutStatus::Progress;
            }
        }
    }
    if (state.IsKeepCold()) {
        const ui32 coldVersion = spaceOp.Handles()[index].ColdVersion.load(std::memory_order_acquire);
        if (cacheOp.Eviction().DemoteKeepCold(TColdCacheItem::Make(coldVersion & MaxItemVersion, index))) {
            return EShrinkCutStatus::Progress;
        }
    }
    if (state.IsPageKind() && state.IsSticky()) {
        if (state.IsStickyField()) {
            return cacheOp.Collections().UnstickyCutPages(
                       TCacheItem::Make(state.Version(), index), spaceOp.AllocationLimit())
                       ? EShrinkCutStatus::Progress
                       : EShrinkCutStatus::Blocked;
        }
        if (state.IsTransitionField()) {
            if (cacheOp.RelocatePage(index) || cacheOp.DemoteUnlinkedStickyPage(index)) {
                return EShrinkCutStatus::Progress;
            }
        }
    }
    return EShrinkCutStatus::Blocked;
}

#undef TOperationItemRef
#undef TSharedCacheItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

// Native relocation and shrink kernels are instantiated here for both trait variants

template TSharedCacheItemRefImpl<TProdTraits> TSharedCacheImpl<TProdTraits>::TryAcquirePageForRelocation(
    TSpaceOperation&, ui32) noexcept;
template TCacheItem TSharedCacheImpl<TProdTraits>::PreparePageReplacement(TSpaceOperation&, TCacheItem) noexcept;
template void TSharedCacheImpl<TProdTraits>::DiscardPageReplacement(TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::PublishPageReplacement(
    TSpaceOperation&, TCacheItem, TCacheItem, EHandleState&) noexcept;
template void TSharedCacheImpl<TProdTraits>::CompletePageReplacement(TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::RelocatePage(TSpaceOperation&, ui32) noexcept;
template bool TSharedCacheImpl<TProdTraits>::DemoteUnlinkedStickyPage(TSpaceOperation&, ui32) noexcept;
template EShrinkCutStatus TSharedCacheImpl<TProdTraits>::ReclaimShrinkHandle(TSpaceOperation&, ui32) noexcept;

template TSharedCacheItemRefImpl<TTestTraits> TSharedCacheImpl<TTestTraits>::TryAcquirePageForRelocation(
    TSpaceOperation&, ui32) noexcept;
template TCacheItem TSharedCacheImpl<TTestTraits>::PreparePageReplacement(TSpaceOperation&, TCacheItem) noexcept;
template void TSharedCacheImpl<TTestTraits>::DiscardPageReplacement(TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::PublishPageReplacement(
    TSpaceOperation&, TCacheItem, TCacheItem, EHandleState&) noexcept;
template void TSharedCacheImpl<TTestTraits>::CompletePageReplacement(TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::RelocatePage(TSpaceOperation&, ui32) noexcept;
template bool TSharedCacheImpl<TTestTraits>::DemoteUnlinkedStickyPage(TSpaceOperation&, ui32) noexcept;
template EShrinkCutStatus TSharedCacheImpl<TTestTraits>::ReclaimShrinkHandle(TSpaceOperation&, ui32) noexcept;

} // namespace NKikimr::NSharedCache
