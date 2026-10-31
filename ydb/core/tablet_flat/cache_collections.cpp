#include "cache_collections.h"
#include "cache_items.h"
#include "cache.h"
#include "cache_memory.h"
#include "cache_table.h"
#include "cache_traits.h"

#include <util/system/spinlock.h>

namespace NKikimr::NSharedCache {

template <class TTraits>
void TCacheCollections<TTraits>::SetSkipBTreeIndexV1Shadow(TCollection& owner, bool skip) noexcept {
    const bool previous = owner.PageCollection()->SkipBTreeIndexV1Shadow();
    if (previous != skip) {
        owner.PageCollection()->SetSkipBTreeIndexV1Shadow(skip);
    }
    const bool changed = previous != owner.PageCollection()->SkipBTreeIndexV1Shadow();
    PublishKeepPageBytes(owner);
    if (changed) {
        auto spaceOp = Cache_.Space_->BeginOperation();
        ui64 slots = 0;
        for (const auto& tier : spaceOp.View().Tiers) {
            slots += tier.HotStorage.SlotCount + tier.ColdStorage.SlotCount;
        }
        Cache_.Eviction_.ScheduleModeSweep(slots);
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::PublishKeepPageBytes(TCollection& owner) noexcept {
    constexpr ui32 Publishing = 1;
    constexpr ui32 Changed = 2;
    if (owner.KeepAccountingState_.fetch_or(Publishing | Changed, std::memory_order_acq_rel) & Publishing) {
        return; // The active publisher will pick up this update; other cache operations do not wait.
    }
    for (;;) {
        owner.KeepAccountingState_.fetch_and(~Changed, std::memory_order_acquire);
        Cache_.InvokeHook(ECacheHookPoint::BeforeKeepBytesPublished, owner.CacheItem.CacheItem());
        const bool keep = owner.GetCacheMode() == ECacheMode::TryKeepInMemory;
        const ui64 resident = keep ? owner.KeepResidentPageBytes() : 0;
        const ui64 active = keep ? owner.KeepReadyPageBytes() : 0;
        Cache_.Memory_.UpdateKeepResidentPageBytes(owner.ReportedKeepResidentBytes_, resident);
        Cache_.Memory_.UpdateKeepReadyPageBytes(owner.ReportedKeepReadyBytes_, active);
        owner.ReportedKeepResidentBytes_ = resident;
        owner.ReportedKeepReadyBytes_ = active;
        ui32 expected = Publishing;
        if (owner.KeepAccountingState_.compare_exchange_strong(
                expected, 0, std::memory_order_release, std::memory_order_relaxed)) {
            return;
        }
        // An update raced with this snapshot. Clearing Changed and resampling prevents a lost delta.
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::AddResidentPageBytes(
    TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept {
    const auto add = [&](std::atomic<ui64>& counter) {
        if (bytes >= 0) {
            counter.fetch_add(static_cast<ui64>(bytes), std::memory_order_relaxed);
        } else {
            SubtractExactBytes(counter, static_cast<ui64>(-bytes));
        }
    };
    add(owner.ResidentPageBytes_);
    if (type != NTable::NPage::EPage::Skip) {
        add(owner.KeepResidentPageBytes_);
    }
    if (type == NTable::NPage::EPage::BTreeIndex) {
        add(owner.BTreeIndexV1ResidentBytes_);
    }
    PublishKeepPageBytes(owner);
    if (!owner.BytesNotificationPending_.exchange(true, std::memory_order_acq_rel)) {
        Cache_.NotifyCollectionBytesChanged(owner.Id(), owner.CacheItem);
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::AddReadyPageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept {
    owner.ReadyPageBytes_.fetch_add(bytes, std::memory_order_relaxed);
    if (type != NTable::NPage::EPage::Skip) {
        owner.KeepReadyPageBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }
    if (type == NTable::NPage::EPage::BTreeIndex) {
        owner.BTreeIndexV1ReadyBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }
    PublishKeepPageBytes(owner);
}

template <class TTraits>
bool TCacheCollections<TTraits>::Link(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(collectionItem.Matches(state) && state.IsCollectionKind());
    Y_DEBUG_ABORT_UNLESS((state.IsBegin() && state.IsStickyField()) ||
                         ((state.IsHot() || state.IsCold()) && state.IsTransitionField()) ||
                         (state.IsSticky() && (state.IsStickyField() || state.IsTransitionField())));
    Y_DEBUG_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_relaxed) == 0);
    Y_DEBUG_ABORT_UNLESS(handle.Body.Collection);

    TCollectionRegistry* expectedRegistry = nullptr;
    if (!handle.Body.Collection->Registry_.compare_exchange_strong(
            expectedRegistry, &registry, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        return false;
    }

    ui32 head = registry.CollectionListHead.load(std::memory_order_relaxed);
    for (;;) {
        handle.NextInOwner.store(head, std::memory_order_release);
        if (registry.CollectionListHead.compare_exchange_weak(
                head, collectionItem.Index(), std::memory_order_release, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::Unlink(TSpaceOperation& spaceOp, TCollectionRegistry& registry,
    TCacheItem collectionItem, EHandleState expectedState) noexcept {
    for (;;) {
        std::atomic<ui32>* link = &registry.CollectionListHead;
        ui32 current = link->load(std::memory_order_acquire);
        while (current != 0) {
            Y_DEBUG_ABORT_UNLESS(current < spaceOp.AccessibleHandleCount());
            THandle& handle = spaceOp.Handles()[current];
            const ui32 next = handle.NextInOwner.load(std::memory_order_acquire);
            const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            Y_DEBUG_ABORT_UNLESS(state.IsCollectionKind() && handle.Body.Collection &&
                                 handle.Body.Collection->Registry_.load(std::memory_order_acquire) == &registry);

            if (current == collectionItem.Index()) {
                if (!collectionItem.Matches(state) || state.State() != expectedState) {
                    return false;
                }
                if (!link->compare_exchange_weak(current, next, std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    break;
                }
                handle.NextInOwner.store(0, std::memory_order_release);
                TCollectionRegistry* expectedRegistry = &registry;
                Y_ABORT_UNLESS(handle.Body.Collection->Registry_.compare_exchange_strong(
                    expectedRegistry, nullptr, std::memory_order_release, std::memory_order_relaxed));
                return true;
            }

            link = &handle.NextInOwner;
            current = next;
        }
        if (current == 0) {
            return false;
        }
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::Unlink(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    TOperationItemRef<TTraits> collectionRef(spaceOp, Items(spaceOp).TryAcquireStructural(collectionItem));
    if (!collectionRef) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !state.IsSticky() || !state.IsStickyField() ||
        !handle.Body.Collection || handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &registry)
    {
        return false;
    }

    return Unlink(spaceOp, registry, collectionItem, EHandleState::Sticky);
}

template <class TTraits>
void TCacheCollections<TTraits>::ForEach(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TFunctionRef<void(TCollection&)> callback) noexcept {
    ui32 current = registry.CollectionListHead.load(std::memory_order_acquire);
    while (current) {
        THandle& handle = spaceOp.Handles()[current];
        const ui32 next = handle.NextInOwner.load(std::memory_order_acquire);
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(state.IsCollectionKind() && handle.Body.Collection &&
                             handle.Body.Collection->Registry_.load(std::memory_order_acquire) == &registry);
        TOperationItemRef<TTraits> ref(
            spaceOp, Items(spaceOp).TryAcquireStructural(TCacheItem::Make(state.Version(), current)));
        Y_ABORT_UNLESS(ref);
        callback(*handle.Body.Collection);
        current = next;
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::SetPageAttachment(
    TSpaceOperation& spaceOp, TCollection& owner, bool attached) noexcept {
    THandle& handle = spaceOp.Handles()[owner.CacheItem.Index()];
    ui64 raw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const auto state = THandleState::FromRaw(raw);
        Y_DEBUG_ABORT_UNLESS(owner.CacheItem.Matches(state) && (state.IsSticky() || state.IsTombstone()));
        Y_DEBUG_ABORT_UNLESS(state.Refs() != 0);
        if (state.HasPageAttachment() == attached) {
            return;
        }
        const auto desired = state.WithPageAttachment(attached);
        if (handle.State.compare_exchange_weak(
                raw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_acquire)) {
            if (!attached) {
                Cache_.InvokeHook(ECacheHookPoint::AfterPageAttachmentWithdrawn, owner.CacheItem.CacheItem());
            }
            return;
        }
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::AddPageReference(TSpaceOperation& spaceOp, TCollection& owner) noexcept {
    if (owner.PageReferences.fetch_add(1, std::memory_order_acq_rel) == 0) {
        // The insertion's native owner bridges publication of the first page attachment
        Cache_.InvokeHook(ECacheHookPoint::BeforePageAttachmentPublished, owner.CacheItem.CacheItem());
        SetPageAttachment(spaceOp, owner, true);
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::DropPageReference(TSpaceOperation& spaceOp, TCollection& owner) noexcept {
    // A page already owns metadata, including metadata explicitly unlinked while admission was in flight
    TOperationItemRef<TTraits> collectionRef(
        spaceOp, Items(spaceOp).AcquireMoreCollectionRef(owner.CacheItem.CacheItem()));
    ui64 references = owner.PageReferences.load(std::memory_order_acquire);
    for (;;) {
        Y_DEBUG_ABORT_UNLESS(references != 0);
        if (references > 1) {
            if (owner.PageReferences.compare_exchange_weak(
                    references, references - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
            continue;
        }
        // Withdraw before zero becomes visible; the native guard retains metadata through the handoff
        SetPageAttachment(spaceOp, owner, false);
        if (owner.PageReferences.compare_exchange_strong(
                references, 0, std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
        // A new page joined before the last-page CAS; restore its attachment before releasing the guard
        SetPageAttachment(spaceOp, owner, true);
    }
    Cache_.InvokeHook(ECacheHookPoint::AfterCollectionOwnerReferenceDropped, collectionRef.CacheItem());
    if (references == 1) {
        Cache_.NotifyCollectionReleased(owner.Id(), owner.CacheItem);
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::DropPageItemRef(TSpaceOperation& spaceOp, THandle& page) noexcept {
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_relaxed));
    if (state.Kind() != EItemKind::Page) {
        return;
    }
    if (TCollection* owner = PageCollection(spaceOp, page)) {
        DropPageReference(spaceOp, *owner);
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::FinishCollectionRetain(TSpaceOperation& spaceOp, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    if (handle.Body.Collection->Registry_.load(std::memory_order_acquire)) {
        return;
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() && expected.IsSticky());
        if (expected.IsStickyField()) {
            return;
        }
        const THandleState desired = expected.WithSticky(EStickyState::Sticky)
                                         .WithFrequency(0)
                                         .WithPageAttachment(expected.HasPageAttachment())
                                         .WithShrinkWaiter(expected.HasShrinkWaiter());
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::TryBeginAttach(TSpaceOperation& spaceOp, TCollectionRegistry& registry,
    TCacheItem collectionItem, THandleState& attachState) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind()) {
            return false;
        }
        if (expected.IsSticky() && expected.IsStickyField()) {
            TCollectionRegistry* owner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
            if (owner == &registry) {
                attachState = expected;
                return true;
            }
            if (!owner && Link(spaceOp, registry, collectionItem)) {
                attachState = expected;
                return true;
            }
            return false;
        }
        if ((!expected.IsSticky() && !expected.IsHot() && !expected.IsCold()) || !expected.IsStickyNoneField() ||
            handle.Body.Collection->Registry_.load(std::memory_order_acquire)) {
            return false;
        }

        const THandleState claimed = expected.WithSticky(EStickyState::Transition);
        Cache_.InvokeHook(ECacheHookPoint::BeforeCollectionAttachClaim, collectionItem);
        if (handle.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            Cache_.UpdateColdReclaimable(handle.Metadata.PayloadBytes, expected, claimed);
            attachState = claimed;
            return true;
        }
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::FinishAttach(
    TSpaceOperation& spaceOp, TCacheItem collectionItem, EHandleState detachedState) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const ui64 bytes = handle.Metadata.PayloadBytes;
    if (detachedState == EHandleState::Cold) {
        Cache_.AdvanceColdMembership(handle);
        Cache_.Memory_.AddRetainedBytes(bytes);
        Cache_.Memory_.Cold().TransferBytesTo(Cache_.Memory_.Sticky(), bytes);

    } else if (detachedState == EHandleState::Hot) {
        Cache_.Memory_.Hot().TransferBytesTo(Cache_.Memory_.Sticky(), bytes);
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() &&
                       expected.State() == detachedState && expected.IsTransitionField());
        const THandleState desired = expected.WithState(EHandleState::Sticky)
                                         .WithSticky(EStickyState::Sticky)
                                         .WithFrequency(0)
                                         .WithPageAttachment(expected.HasPageAttachment())
                                         .WithShrinkWaiter(expected.HasShrinkWaiter());
        Cache_.InvokeHook(ECacheHookPoint::BeforeCollectionAttachPublished, collectionItem);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::Attach(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    THandleState attachState;
    if (!TryBeginAttach(spaceOp, registry, collectionItem, attachState)) {
        return false;
    }
    if (attachState.IsStickyField()) {
        return true;
    }

    const EHandleState detachedState = attachState.State();
    Y_ABORT_UNLESS(Link(spaceOp, registry, collectionItem));
    FinishAttach(spaceOp, collectionItem, detachedState);
    Cache_.NotifyResourcesAvailable();
    return true;
}

template <class TTraits>
TCacheItemRefImpl<TTraits> TCacheCollections<TTraits>::AcquireSticky(
    TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem, TCollection*& collectionValue) noexcept {
    collectionValue = nullptr;
    TOperationItemRef<TTraits> collectionRef(spaceOp, Items(spaceOp).TryAcquireStructural(collectionItem.CacheItem()));
    if (!collectionRef) {
        return {};
    }

    THandle& collection = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
    if (!collectionItem.Matches(state) || !state.IsSticky() || !state.IsStickyField() || !collection.Body.Collection)
    {
        return {};
    }
    collectionValue = collection.Body.Collection;
    return collectionRef.Take();
}

template <class TTraits>
void TCacheCollections<TTraits>::RestoreStickyPageList(
    TSpaceOperation& spaceOp, TCollection& collection, ui32 detachedHead) noexcept {
    ui32 tail = detachedHead;
    while (const ui32 next = spaceOp.Handles()[tail].NextInOwner.load(std::memory_order_acquire)) {
        tail = next;
    }
    ui32 concurrentHead = collection.StickyPageListHead.load(std::memory_order_relaxed);
    do {
        spaceOp.Handles()[tail].NextInOwner.store(concurrentHead, std::memory_order_release);
    } while (!collection.StickyPageListHead.compare_exchange_weak(
        concurrentHead, detachedHead, std::memory_order_release, std::memory_order_relaxed));
}

template <class TTraits>
void TCacheCollections<TTraits>::MergeRetainedStickyPages(
    TSpaceOperation& spaceOp, TCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept {
    Y_DEBUG_ABORT_UNLESS(retainedHead != 0 && retainedTail != 0);
    ui32 concurrentHead = collection.StickyPageListHead.load(std::memory_order_relaxed);
    do {
        spaceOp.Handles()[retainedTail].NextInOwner.store(concurrentHead, std::memory_order_release);
    } while (!collection.StickyPageListHead.compare_exchange_weak(
        concurrentHead, retainedHead, std::memory_order_release, std::memory_order_relaxed));
    Cache_.InvokeHook(ECacheHookPoint::AfterStickyPageListHeadPublished, collection.CacheItem.CacheItem());
}

template <class TTraits>
void TCacheCollections<TTraits>::DrainStickyPages(TSpaceOperation& spaceOp, TCollection& value) noexcept {
    const TCollectionCacheItem collectionItem = value.CacheItem;
    if (value.CacheMode_.load(std::memory_order_acquire) == ECacheMode::Sticky) {
        return;
    }
    ui32 current = value.StickyPageListHead.exchange(0, std::memory_order_acq_rel);
    if (current == 0) {
        return;
    }
    // A page linked after this exchange checks the mode and drains its own list.

    Cache_.InvokeHook(ECacheHookPoint::AfterStickyPageListDetached, collectionItem.CacheItem());
    if (value.CacheMode_.load(std::memory_order_acquire) == ECacheMode::Sticky) {
        RestoreStickyPageList(spaceOp, value, current);
        return;
    }

    while (current != 0) {
        Y_DEBUG_ABORT_UNLESS(current < spaceOp.AccessibleHandleCount());
        THandle& page = spaceOp.Handles()[current];
        const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
        page.NextInOwner.store(0, std::memory_order_release);

        ui64 expectedRaw = page.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState expected = THandleState::FromRaw(expectedRaw);
            const bool pending = expected.IsPending() || expected.IsCompleting();
            const bool resident = expected.IsSticky();
            const bool reserved = expected.IsHot() && expected.IsStickyField();
            Y_DEBUG_ABORT_UNLESS(expected.IsPageKind() &&
                                 (((pending || resident) && expected.IsStickyField()) || reserved) &&
                                 page.Key0.load(std::memory_order_relaxed) == collectionItem.Raw());

            const ui64 bytes = TCacheItems<TTraits>::PageBytes(page);
            THandleState desired = expected.WithSticky(EStickyState::None);
            if (!pending) {
                desired = desired.WithFrequency(0);
                if (resident) {
                    desired = desired.WithState(EHandleState::Hot);
                }
            }
            if (!page.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                continue;
            }

            if (!pending) {
                auto& hot = Cache_.Memory_.Hot(desired.Tier());
                Cache_.Memory_.Sticky().TransferBytesTo(hot, bytes);
                Cache_.Memory_.Sticky().TransferPageTo(hot, EItemKind::Page);
            }
            Cache_.Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
            if (!pending) {
                Cache_.Eviction_.BindOperation(spaceOp).RouteHot(
                    EHotLevel::L1, TCacheItem::Make(expected.Version(), current));
            }
            break;
        }
        current = next;
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::LinkStickyPage(
    TSpaceOperation& spaceOp, TCacheItem pageItem, TCacheItemRefImpl<TTraits>& collectionRef) noexcept {
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(pageItem.Matches(state) && state.IsPageKind() &&
                         ((state.IsStickyField() && (state.IsPending() || state.IsCompleting() || state.IsHot())) ||
                             (state.IsSticky() && state.IsStickyField())));
    Y_DEBUG_ABORT_UNLESS(page.NextInOwner.load(std::memory_order_relaxed) == 0);

    const TCollectionCacheItem collectionItem =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed)));
    TCollection* collectionValue = nullptr;
    collectionRef = AcquireSticky(spaceOp, collectionItem, collectionValue);
    const ECacheMode modeAtAdmission =
        collectionValue ? collectionValue->CacheMode_.load(std::memory_order_acquire) : ECacheMode::TryKeepInMemory;
    const auto allowsSticky = [&] {
        if (!collectionRef) {
            return false;
        }
        const ECacheMode mode = collectionValue->CacheMode_.load(std::memory_order_acquire);
        const THandleState collectionState =
            THandleState::FromRaw(spaceOp.Handles()[collectionItem.Index()].State.load(std::memory_order_acquire));
        return collectionState.IsSticky() && collectionState.IsStickyField() && mode != ECacheMode::TryKeepInMemory;
    };
    if (!allowsSticky())
    {
        if (state.IsSticky()) {
            // A replacement borrows the old page's sticky charge until it is published.
            return false;
        }
        CancelStickyLink(spaceOp, pageItem);
        return true;
    }

    Cache_.InvokeHook(ECacheHookPoint::BeforeStickyPageListLink, pageItem);
    ui32 head = collectionValue->StickyPageListHead.load(std::memory_order_relaxed);
    for (;;) {
        page.NextInOwner.store(head, std::memory_order_release);
        if (collectionValue->StickyPageListHead.compare_exchange_weak(
                head, pageItem.Index(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            break;
        }
    }
    Cache_.InvokeHook(ECacheHookPoint::AfterStickyPageLinked, pageItem);
    if (!allowsSticky() || collectionValue->CacheMode_.load(std::memory_order_acquire) != modeAtAdmission)
    {
        DrainStickyPages(spaceOp, *collectionValue);
    }
    return true;
}

template <class TTraits>
void TCacheCollections<TTraits>::CancelStickyLink(TSpaceOperation& spaceOp, TCacheItem pageItem) noexcept {
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    ui64 expectedRaw = page.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(
            pageItem.Matches(expected) && expected.IsPageKind() &&
            ((expected.IsPending() || expected.IsCompleting() || expected.IsHot()) && expected.IsStickyField()));
        const THandleState desired = expected.WithSticky(EStickyState::None);
        if (page.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            if (expected.IsHot()) {
                Cache_.Memory_.Sticky().TransferBytesTo(
                    Cache_.Memory_.Hot(expected.Tier()), TCacheItems<TTraits>::PageBytes(page));
                Cache_.Memory_.Sticky().TransferPageTo(Cache_.Memory_.Hot(expected.Tier()), EItemKind::Page);
                Cache_.Eviction_.BindOperation(spaceOp).RouteHot(EHotLevel::L1, pageItem);
            }
            Cache_.Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, TCacheItems<TTraits>::PageBytes(page));
            return;
        }
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::UnlinkPendingStickyPage(TSpaceOperation& spaceOp, TCacheItem pageItem) noexcept {
    THandle& target = spaceOp.Handles()[pageItem.Index()];
    TCollection* collection = PageCollection(spaceOp, target);
    Y_ABORT_UNLESS(collection);
    const ui64 bytes = TCacheItems<TTraits>::PageBytes(target);
    for (;;) {
        const THandleState state = THandleState::FromRaw(target.State.load(std::memory_order_acquire));
        if (!state.IsStickyField()) {
            return;
        }
        Y_DEBUG_ABORT_UNLESS(pageItem.Matches(state) && (state.IsPending() || state.IsCompleting()));
        const ECacheMode mode = collection->CacheMode_.load(std::memory_order_acquire);
        ui32 current = collection->StickyPageListHead.exchange(0, std::memory_order_acq_rel);
        Cache_.InvokeHook(ECacheHookPoint::AfterStickyPageListDetached, collection->CacheItem.CacheItem());
        ui32 retainedHead = 0;
        ui32 retainedTail = 0;
        bool found = false;
        while (current != 0) {
            THandle& page = spaceOp.Handles()[current];
            const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
            if (current == pageItem.Index()) {
                page.NextInOwner.store(0, std::memory_order_release);
                found = true;
            } else {
                if (retainedTail == 0) {
                    retainedTail = current;
                }
                page.NextInOwner.store(retainedHead, std::memory_order_relaxed);
                retainedHead = current;
            }
            current = next;
        }
        if (retainedTail != 0) {
            MergeRetainedStickyPages(spaceOp, *collection, retainedHead, retainedTail);
            const THandleState owner = THandleState::FromRaw(
                spaceOp.Handles()[collection->CacheItem.Index()].State.load(std::memory_order_acquire));
            if (!owner.IsStickyField() || collection->CacheMode_.load(std::memory_order_acquire) != mode) {
                DrainStickyPages(spaceOp, *collection);
            }
        }
        if (found) {
            ui64 expectedRaw = target.State.load(std::memory_order_relaxed);
            for (;;) {
                const THandleState expected = THandleState::FromRaw(expectedRaw);
                if (!expected.IsStickyField()) {
                    return;
                }
                if (target.State.compare_exchange_weak(expectedRaw, expected.WithSticky(EStickyState::None).Raw(),
                        std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    Cache_.Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
                    return;
                }
            }
        }
        // Another list traversal temporarily owns this node. It will withdraw it or restore it to the head.
        SpinLockPause();
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::MakePageSticky(TSpaceOperation& spaceOp, TCacheItem pageItem) noexcept {
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    TCollection* collection = PageCollection(spaceOp, page);
    if (!collection) {
        return false;
    }
    const ECacheMode admissionMode = collection->CacheMode_.load(std::memory_order_acquire);
    if (admissionMode == ECacheMode::TryKeepInMemory) {
        return false;
    }
    const ui64 bytes = TCacheItems<TTraits>::PageBytes(page);
    if (THandleState::FromRaw(page.State.load(std::memory_order_acquire)).IsCold()) {
        Cache_.Eviction_.BindOperation(spaceOp).PromoteCold(pageItem, true);
    }
    ui64 expectedRaw = page.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!pageItem.Matches(expected) || !expected.IsPageKind()) {
            return false;
        }
        if (expected.IsSticky() && expected.IsStickyField()) {
            return true;
        }
        const bool pending = expected.IsPending();
        if (pending && expected.IsStickyField()) {
            return true;
        }
        if ((!expected.IsHot() && !pending) || !expected.IsStickyNoneField()) {
            return false;
        }
        Cache_.Memory_.AddStickyOwnedBytes(bytes);
        const THandleState claimed = expected.WithSticky(EStickyState::Sticky);
        if (page.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            if (pending) {
                TOperationItemRef<TTraits> collectionRef(spaceOp);
                LinkStickyPage(spaceOp, pageItem, collectionRef.Get());
                return THandleState::FromRaw(page.State.load(std::memory_order_acquire)).IsStickyField();
            }
            break;
        }
        Cache_.Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
    }

    const bool promoted = FinishStickyPromotion(spaceOp, pageItem, bytes);
    Cache_.NotifyResourcesAvailable();
    return promoted;
}

template <class TTraits>
bool TCacheCollections<TTraits>::FinishStickyPromotion(
    TSpaceOperation& spaceOp, TCacheItem pageItem, ui64 bytes) noexcept {
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    const ECacheTier sourceTier = THandleState::FromRaw(page.State.load(std::memory_order_acquire)).Tier();
    Cache_.Memory_.Hot(sourceTier).TransferBytesTo(Cache_.Memory_.Sticky(), bytes);
    Cache_.Memory_.Hot(sourceTier).TransferPageTo(Cache_.Memory_.Sticky(), EItemKind::Page);
    TOperationItemRef<TTraits> collectionRef(spaceOp);
    if (!LinkStickyPage(spaceOp, pageItem, collectionRef.Get())) {
        ui64 claimedRaw = page.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState claimed = THandleState::FromRaw(claimedRaw);
            Y_DEBUG_ABORT_UNLESS(pageItem.Matches(claimed) && claimed.IsHot() && claimed.IsStickyField());
            if (page.State.compare_exchange_weak(claimedRaw, claimed.WithSticky(EStickyState::None).Raw(),
                    std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                Cache_.Memory_.Sticky().TransferBytesTo(Cache_.Memory_.Hot(sourceTier), bytes);
                Cache_.Memory_.Sticky().TransferPageTo(Cache_.Memory_.Hot(sourceTier), EItemKind::Page);
                Cache_.Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
                Cache_.Eviction_.BindOperation(spaceOp).RouteHot(EHotLevel::L1, pageItem);
                return false;
            }
        }
    }

    ui64 claimedRaw = page.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState claimed = THandleState::FromRaw(claimedRaw);
        if (!pageItem.Matches(claimed) || !claimed.IsHot() || !claimed.IsStickyField()) {
            return claimed.IsSticky() && claimed.IsStickyField();
        }
        const THandleState desired = claimed.WithState(EHandleState::Sticky).WithSticky(EStickyState::Sticky);
        if (page.State.compare_exchange_weak(
                claimedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            if (sourceTier == ECacheTier::Keep) {
                Cache_.Memory_.ReleaseKeep(bytes);
                Cache_.NotifyResourcesAvailable();
            }
            return true;
        }
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::PublishPageUnsticky(THandle& page, THandleState state) noexcept {
    ui64 expectedRaw = state.Raw();
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(expected.Version() == state.Version() && expected.IsPageKind() && expected.IsSticky() &&
                             expected.IsStickyField());
        const THandleState desired = expected.WithSticky(EStickyState::Transition);
        if (page.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::UnstickyCutPages(
    TSpaceOperation& spaceOp, TCacheItem pageItem, ui64 allocationLimit) noexcept {
    THandle& trigger = spaceOp.Handles()[pageItem.Index()];
    const TCollectionCacheItem collectionItem =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(trigger.Key0.load(std::memory_order_relaxed)));
    TCollection* collectionValue = nullptr;
    TOperationItemRef<TTraits> collectionRef(spaceOp, AcquireSticky(spaceOp, collectionItem, collectionValue));
    if (!collectionRef) {
        return false;
    }

    const ui32 detachedHead = collectionValue->StickyPageListHead.exchange(0, std::memory_order_acq_rel);
    Cache_.InvokeHook(ECacheHookPoint::AfterStickyPageListDetached, collectionItem.CacheItem());

    ui32 current = detachedHead;
    ui32 retainedHead = 0;
    ui32 retainedTail = 0;
    while (current != 0) {
        Y_DEBUG_ABORT_UNLESS(current < spaceOp.AccessibleHandleCount());
        THandle& page = spaceOp.Handles()[current];
        const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
        const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(
            state.IsPageKind() && (state.IsPending() || state.IsCompleting() || state.IsHot() || state.IsSticky()) &&
            state.IsStickyField() && page.Key0.load(std::memory_order_relaxed) == collectionItem.Raw());

        // Keep in-flight pages linked until they publish the Sticky state
        if (current < allocationLimit || !state.IsSticky()) {
            if (retainedTail == 0) {
                retainedTail = current;
            }
            page.NextInOwner.store(retainedHead, std::memory_order_relaxed);
            retainedHead = current;
        } else {
            page.NextInOwner.store(0, std::memory_order_release);
            PublishPageUnsticky(page, state);
        }
        current = next;
    }
    if (retainedTail != 0) {
        MergeRetainedStickyPages(spaceOp, *collectionValue, retainedHead, retainedTail);
    }
    return true;
}

template <class TTraits>
bool TCacheCollections<TTraits>::SetPagesCacheMode(
    TSpaceOperation& spaceOp, TCollection& value, ECacheMode mode) noexcept {
    const TCollectionCacheItem collection = value.CacheItem;
    THandle& handle = spaceOp.Handles()[collection.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collection.Matches(state) || handle.Body.Collection != &value) {
        return false;
    }
    const bool attached = state.IsSticky() && state.IsStickyField();
    const bool detached = (state.IsHot() || state.IsCold() || (state.IsSticky() && mode == ECacheMode::Regular)) &&
                          state.IsStickyNoneField() && !value.Registry_.load(std::memory_order_acquire);
    if (!attached && !detached) {
        return false;
    }
    if (mode == ECacheMode::Sticky && !attached) {
        return false;
    }
    const ECacheMode previousMode = value.CacheMode_.load(std::memory_order_acquire);
    if (previousMode == mode) {
        return true;
    }
    const bool wasKeep = previousMode == ECacheMode::TryKeepInMemory;
    const bool keep = mode == ECacheMode::TryKeepInMemory;
    if (wasKeep != keep) {
        value.KeepGeneration_.fetch_add(1, std::memory_order_acq_rel);
    }
    value.CacheMode_.store(mode, std::memory_order_release);
    PublishKeepPageBytes(value);
    if (previousMode != mode) {
        Cache_.Eviction_.ScheduleModeSweep(spaceOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount +
                                           spaceOp.View().Tiers[ECacheTier::Regular].ColdStorage.SlotCount +
                                           spaceOp.View().Tiers[ECacheTier::Keep].HotStorage.SlotCount +
                                           spaceOp.View().Tiers[ECacheTier::Keep].ColdStorage.SlotCount);
    }
    if (keep || (previousMode == ECacheMode::Sticky && mode == ECacheMode::Regular)) {
        // Publish the destination mode before withdrawing the previous Sticky ownership.
        DrainStickyPages(spaceOp, value);
    }
    return true;
}

template <class TTraits>
bool TCacheCollections<TTraits>::TryBeginDetach(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    if (!spaceOp.Contains(collectionItem) || collectionItem.IsFrozen()) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind() || !expected.IsSticky() ||
            !expected.IsStickyField() || !handle.Body.Collection ||
            handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &registry)
        {
            return false;
        }
        const THandleState claimed = expected.WithSticky(EStickyState::Transition);
        if (handle.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::Detach(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    if (!TryBeginDetach(spaceOp, registry, collectionItem)) {
        return false;
    }
    FinishDetach(spaceOp, registry, collectionItem);
    return true;
}

template <class TTraits>
bool TCacheCollections<TTraits>::DetachReleased(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 raw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const auto state = THandleState::FromRaw(raw);
        if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !state.IsSticky() ||
            !state.IsStickyField() || state.Refs() != 1 ||
            handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &registry) {
            return false;
        }
        const auto claimed = state.WithSticky(EStickyState::Transition);
        if (handle.State.compare_exchange_weak(
                raw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
            break;
        }
    }
    if (handle.Body.Collection->GetActorState()) {
        // Actor-owned collections finish through their registration and request lifecycle
        raw = handle.State.load(std::memory_order_relaxed);
        for (;;) {
            const auto state = THandleState::FromRaw(raw);
            const auto restored = state.WithSticky(EStickyState::Sticky);
            if (handle.State.compare_exchange_weak(
                    raw, restored.Raw(), std::memory_order_release, std::memory_order_relaxed)) {
                break;
            }
        }
        Cache_.NotifyResourcesAvailable();
        return false;
    }
    FinishDetach(spaceOp, registry, collectionItem);
    return true;
}

template <class TTraits>
void TCacheCollections<TTraits>::FinishDetach(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    // Invalidate last-owner notification snapshots across detach/reattach
    Cache_.AdvanceColdMembership(handle);
    TCollection& value = *handle.Body.Collection;
    const ECacheMode previousMode = value.CacheMode_.exchange(ECacheMode::Regular, std::memory_order_acq_rel);
    PublishKeepPageBytes(value);
    if (previousMode == ECacheMode::TryKeepInMemory) {
        Cache_.Eviction_.ScheduleModeSweep(spaceOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount +
                                           spaceOp.View().Tiers[ECacheTier::Regular].ColdStorage.SlotCount +
                                           spaceOp.View().Tiers[ECacheTier::Keep].HotStorage.SlotCount +
                                           spaceOp.View().Tiers[ECacheTier::Keep].ColdStorage.SlotCount);
    }
    DrainStickyPages(spaceOp, value);
    Y_ABORT_UNLESS(Unlink(spaceOp, registry, collectionItem, EHandleState::Sticky));

    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() && expected.IsSticky() &&
                       expected.IsTransitionField());
        const THandleState desired = expected.WithSticky(EStickyState::None)
                                         .WithFrequency(0)
                                         .WithPageAttachment(expected.HasPageAttachment())
                                         .WithShrinkWaiter(expected.HasShrinkWaiter());
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            break;
        }
    }
    Cache_.NotifyResourcesAvailable();
}

template <class TTraits>
bool TCacheCollections<TTraits>::TryClaimDeletion(TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry,
    TCacheItem collectionItem, THandleState& sourceState, TCacheItemRefImpl<TTraits>& owner) noexcept {
    if (!spaceOp.Contains(collectionItem) || collectionItem.IsFrozen()) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    for (;;) {
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind() || !handle.Body.Collection ||
            handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) != 0)
        {
            return false;
        }
        TCollectionRegistry* registry = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
        if (expected.HasPageAttachment() ||
            handle.Body.Collection->PageReferences.load(std::memory_order_acquire) != 0) {
            return false;
        }
        const bool stickySource = expected.IsSticky() && expected.IsStickyField() && registry == expectedRegistry;
        const bool detachedSource = expected.IsStickyNoneField() && !registry && !expectedRegistry;
        if (!stickySource && !detachedSource) {
            return false;
        }
        Cache_.InvokeHook(ECacheHookPoint::BeforeCollectionEraseClaim, collectionItem);
        if (Cache_.Table_.TryBeginErase(spaceOp, collectionItem, expected, owner)) {
            const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
            Cache_.UpdateColdReclaimable(handle.Metadata.PayloadBytes, expected, desired);
            sourceState = expected;
            Cache_.InvokeHook(ECacheHookPoint::AfterTombstoneOwnerClaim, collectionItem);
            return true;
        }
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::Erase(
    TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry, TCacheItem collectionItem) noexcept {
    TOperationItemRef<TTraits> deletionOwner(spaceOp);
    THandleState sourceState;
    if (!TryClaimDeletion(spaceOp, expectedRegistry, collectionItem, sourceState, deletionOwner.Get())) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    if (sourceState.IsCold()) {
        Cache_.AdvanceColdMembership(handle);
    }

    TCollectionRegistry* registryOwner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
    if (registryOwner) {
        Unlink(*registryOwner, deletionOwner);
    } else {
        Y_DEBUG_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_relaxed) == 0);
    }

    const TCacheKey key = LoadSharedCacheKey(handle, EItemKind::Collection);
    Cache_.Table_.FinishErase(spaceOp, deletionOwner.CacheItem(), key);
    return true;
}

template <class TTraits>
bool TCacheCollections<TTraits>::MoveRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& source,
    TCollectionRegistry& destination, TCacheItem collectionItem) noexcept {
    TOperationItemRef<TTraits> collectionRef(spaceOp, Items(spaceOp).TryAcquireStructural(collectionItem));
    if (!collectionRef) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !state.IsSticky() || !state.IsStickyField() ||
        !handle.Body.Collection || handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &source)
    {
        return false;
    }
    if (&source == &destination) {
        return true;
    }

    if (!Unlink(spaceOp, source, collectionItem, EHandleState::Sticky)) {
        return false;
    }
    Y_ABORT_UNLESS(Link(spaceOp, destination, collectionItem));
    return true;
}

template class TCacheCollections<TProdTraits>;
template class TCacheCollections<TTestTraits>;

} // namespace NKikimr::NSharedCache
