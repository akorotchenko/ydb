#include "shared_cache_collections.h"
#include "shared_cache.h"
#include "shared_cache_memory.h"
#include "shared_cache_table.h"
#include "shared_cache_traits.h"

#include <util/system/spinlock.h>

namespace NKikimr::NSharedCache {

template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheCollectionServices<TTraits>::TryAcquireStructural(
    TSpaceOperation& spaceOp, TCacheItem item) noexcept {
    return Cache_.TryAcquireStructural(spaceOp, item);
}

template <class TTraits>
void TCacheCollectionServices<TTraits>::RouteHotL1(TSpaceOperation& spaceOp, TCacheItem item) noexcept {
    Cache_.RouteHot(spaceOp, TSharedCacheImpl<TTraits>::EHotLevel::L1, item);
}

template <class TTraits>
void TCacheCollectionServices<TTraits>::UpdateColdReclaimable(
    ui64 payloadBytes, THandleState before, THandleState after) noexcept {
    Cache_.UpdateColdReclaimable(payloadBytes, before, after);
}

template <class TTraits>
ui32 TCacheCollectionServices<TTraits>::AdvanceColdMembership(THandle& handle) noexcept {
    return TSharedCacheImpl<TTraits>::AdvanceColdMembership(handle);
}

template <class TTraits>
ui64 TCacheCollectionServices<TTraits>::PageBytes(const THandle& handle) noexcept {
    return TSharedCacheImpl<TTraits>::PageBytes(handle);
}

template <class TTraits>
void TCacheCollectionServices<TTraits>::PromoteColdForSticky(TSpaceOperation& spaceOp, TCacheItem item) noexcept {
    Cache_.PromoteCold(spaceOp, item, true);
}

template <class TTraits>
void TCacheCollectionServices<TTraits>::ScheduleKeepColdSweep(ui64 slots) noexcept {
    Cache_.Eviction_.ScheduleKeepColdSweep(slots);
}

template <class TTraits>
TCollection* TCacheCollections<TTraits>::PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    return PageCollection(
        spaceOp, TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed))));
}

template <class TTraits>
TCollection* TCacheCollections<TTraits>::PageCollection(TSpaceOperation& spaceOp, TCollectionCacheItem item) noexcept {
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

template <class TTraits>
void TCacheCollections<TTraits>::PublishKeepPageBytes(TCollection& owner) noexcept {
    constexpr ui32 Publishing = 1;
    constexpr ui32 Changed = 2;
    if (owner.KeepAccountingState_.fetch_or(Publishing | Changed, std::memory_order_acq_rel) & Publishing) {
        return; // The active publisher will pick up this update; other cache operations do not wait.
    }
    for (;;) {
        owner.KeepAccountingState_.fetch_and(~Changed, std::memory_order_acquire);
        Traits_.Invoke(ESharedCacheHookPoint::BeforeKeepBytesPublished, owner.CacheItem.CacheItem());
        const bool keep = owner.GetCacheMode() == ECacheMode::TryKeepInMemory;
        const ui64 resident = keep ? owner.KeepResidentPageBytes() : 0;
        const ui64 active = keep ? owner.KeepActivePageBytes() : 0;
        Memory_.UpdateKeepResidentPageBytes(owner.ReportedKeepResidentBytes_, resident);
        Memory_.UpdateKeepActivePageBytes(owner.ReportedKeepActiveBytes_, active);
        owner.ReportedKeepResidentBytes_ = resident;
        owner.ReportedKeepActiveBytes_ = active;
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
        Traits_.NotifyCollectionBytesChanged(owner.Id(), owner.CacheItem);
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::AddActivePageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept {
    owner.ActivePageBytes_.fetch_add(bytes, std::memory_order_relaxed);
    if (type != NTable::NPage::EPage::Skip) {
        owner.KeepActivePageBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }
    if (type == NTable::NPage::EPage::BTreeIndex) {
        owner.BTreeIndexV1ActiveBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }
    PublishKeepPageBytes(owner);
}

template <class TTraits>
void TCacheCollections<TTraits>::SetSkipBTreeIndexV1Shadow(TCollection& owner, bool skip) noexcept {
    owner.PageCollection()->SetSkipBTreeIndexV1Shadow(skip);
    PublishKeepPageBytes(owner);
}

template <class TTraits>
void TCacheCollections<TTraits>::SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept {
    const ui64 previous = counter.fetch_sub(bytes, std::memory_order_relaxed);
    Y_ABORT_UNLESS(previous >= bytes);
}

template <class TTraits>
void TCacheCollections<TTraits>::AcknowledgeBytes(TCollection& owner) noexcept {
    // Clear before the actor samples bytes, so a racing update can schedule the next notification.
    owner.BytesNotificationPending_.store(false, std::memory_order_release);
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
void TCacheCollections<TTraits>::Unlink(
    TCollectionRegistry& registry, TOperationItemRef<TTraits>& collectionRef) noexcept {
    const bool unlinked =
        Unlink(collectionRef.SpaceOperation(), registry, collectionRef.CacheItem(), EHandleState::Tombstone);
    Y_DEBUG_ABORT_UNLESS(unlinked);
}

template <class TTraits>
bool TCacheCollections<TTraits>::Unlink(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    TOperationItemRef<TTraits> collectionRef(spaceOp, Services_.TryAcquireStructural(spaceOp, collectionItem));
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
            spaceOp, Services_.TryAcquireStructural(spaceOp, TCacheItem::Make(state.Version(), current)));
        Y_ABORT_UNLESS(ref);
        callback(*handle.Body.Collection);
        current = next;
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::AddReference(TCollection& owner) noexcept {
    owner.References.fetch_add(1, std::memory_order_relaxed);
}

template <class TTraits>
bool TCacheCollections<TTraits>::DropReference(TCollection& owner) noexcept {
    const ui64 previous = owner.References.fetch_sub(1, std::memory_order_relaxed);
    Y_ABORT_UNLESS(previous != 0);
    return previous == 1;
}

template <class TTraits>
void TCacheCollections<TTraits>::DropOwnerReference(TSpaceOperation& spaceOp, TCollection& owner) noexcept {
    // Keep the payload alive after dropping the page's collection ownership.
    TOperationItemRef<TTraits> collectionRef(
        spaceOp, Services_.TryAcquireStructural(spaceOp, owner.CacheItem.CacheItem()));
    Y_ABORT_UNLESS(collectionRef);
    const bool lastOwner = DropReference(owner);
    Traits_.Invoke(ESharedCacheHookPoint::AfterCollectionOwnerReferenceDropped, collectionRef.CacheItem());
    if (lastOwner) {
        const TLogoBlobID id = owner.Id();
        const TCollectionCacheItem item = owner.CacheItem;
        PublishCollectionForReclaim(spaceOp, item.CacheItem());
        Traits_.NotifyCollectionReleased(id, item);
    } else if (owner.Registry_.load(std::memory_order_acquire) &&
               owner.References.load(std::memory_order_acquire) == 1) {
        // The registry is the only remaining structural owner; actor work may now be cleaned up.
        Traits_.NotifyCollectionReleased(owner.Id(), owner.CacheItem);
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::DropPageItemRef(TSpaceOperation& spaceOp, THandle& page) noexcept {
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_relaxed));
    if (state.Kind() != EItemKind::Page) {
        return;
    }
    if (TCollection* owner = PageCollection(spaceOp, page)) {
        DropOwnerReference(spaceOp, *owner);
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::PublishCollectionForReclaim(
    TSpaceOperation& spaceOp, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind() || !expected.IsSticky() ||
            !expected.IsStickyNoneField() || handle.Body.Collection->Registry_.load(std::memory_order_acquire) ||
            handle.Body.Collection->References.load(std::memory_order_acquire) != 0)
        {
            return;
        }
        const THandleState desired = expected.WithState(EHandleState::Hot).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            Memory_.Sticky().TransferBytesTo(Memory_.Hot(), handle.Metadata.PayloadBytes);
            Traits_.Invoke(ESharedCacheHookPoint::BeforeCollectionHotPublished, collectionItem);
            Services_.RouteHotL1(spaceOp, collectionItem);
            return;
        }
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
        const THandleState desired = expected.WithSticky(EStickyState::Sticky).WithFrequency(0);
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
        Traits_.Invoke(ESharedCacheHookPoint::BeforeCollectionAttachClaim, collectionItem);
        if (handle.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            Services_.UpdateColdReclaimable(handle.Metadata.PayloadBytes, expected, claimed);
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
        Services_.AdvanceColdMembership(handle);
        Memory_.AddRetainedBytes(bytes);
        Memory_.Cold().TransferBytesTo(Memory_.Sticky(), bytes);
        Memory_.RemoveColdItem();
    } else if (detachedState == EHandleState::Hot) {
        Memory_.Hot().TransferBytesTo(Memory_.Sticky(), bytes);
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() &&
                       expected.State() == detachedState && expected.IsTransitionField());
        const THandleState desired =
            expected.WithState(EHandleState::Sticky).WithSticky(EStickyState::Sticky).WithFrequency(0);
        Traits_.Invoke(ESharedCacheHookPoint::BeforeCollectionAttachPublished, collectionItem);
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
    AddReference(*spaceOp.Handles()[collectionItem.Index()].Body.Collection);
    FinishAttach(spaceOp, collectionItem, detachedState);
    return true;
}

template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheCollections<TTraits>::AcquireSticky(
    TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem, TCollection*& collectionValue) noexcept {
    collectionValue = nullptr;
    TOperationItemRef<TTraits> collectionRef(
        spaceOp, Services_.TryAcquireStructural(spaceOp, collectionItem.CacheItem()));
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
    Traits_.Invoke(ESharedCacheHookPoint::AfterStickyPageListHeadPublished, collection.CacheItem.CacheItem());
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

    Traits_.Invoke(ESharedCacheHookPoint::AfterStickyPageListDetached, collectionItem.CacheItem());
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

            const ui64 bytes = Services_.PageBytes(page);
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
                Memory_.Sticky().TransferBytesTo(Memory_.Hot(), bytes);
                Memory_.Sticky().TransferPageTo(Memory_.Hot(), EItemKind::Page);
            }
            Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
            if (!pending) {
                Services_.RouteHotL1(spaceOp, TCacheItem::Make(expected.Version(), current));
            }
            break;
        }
        current = next;
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::LinkStickyPage(
    TSpaceOperation& spaceOp, TCacheItem pageItem, TSharedCacheItemRefImpl<TTraits>& collectionRef) noexcept {
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

    Traits_.Invoke(ESharedCacheHookPoint::BeforeStickyPageListLink, pageItem);
    ui32 head = collectionValue->StickyPageListHead.load(std::memory_order_relaxed);
    for (;;) {
        page.NextInOwner.store(head, std::memory_order_release);
        if (collectionValue->StickyPageListHead.compare_exchange_weak(
                head, pageItem.Index(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            break;
        }
    }
    Traits_.Invoke(ESharedCacheHookPoint::AfterStickyPageLinked, pageItem);
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
                Memory_.Sticky().TransferBytesTo(Memory_.Hot(), Services_.PageBytes(page));
                Memory_.Sticky().TransferPageTo(Memory_.Hot(), EItemKind::Page);
                Services_.RouteHotL1(spaceOp, pageItem);
            }
            Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, Services_.PageBytes(page));
            return;
        }
    }
}

template <class TTraits>
void TCacheCollections<TTraits>::UnlinkPendingStickyPage(TSpaceOperation& spaceOp, TCacheItem pageItem) noexcept {
    THandle& target = spaceOp.Handles()[pageItem.Index()];
    TCollection* collection = PageCollection(spaceOp, target);
    Y_ABORT_UNLESS(collection);
    const ui64 bytes = Services_.PageBytes(target);
    for (;;) {
        const THandleState state = THandleState::FromRaw(target.State.load(std::memory_order_acquire));
        if (!state.IsStickyField()) {
            return;
        }
        Y_DEBUG_ABORT_UNLESS(pageItem.Matches(state) && (state.IsPending() || state.IsCompleting()));
        const ECacheMode mode = collection->CacheMode_.load(std::memory_order_acquire);
        ui32 current = collection->StickyPageListHead.exchange(0, std::memory_order_acq_rel);
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
                    Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
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
    const ui64 bytes = Services_.PageBytes(page);
    if (THandleState::FromRaw(page.State.load(std::memory_order_acquire)).IsCold()) {
        Services_.PromoteColdForSticky(spaceOp, pageItem);
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
        Memory_.AddStickyOwnedBytes(bytes);
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
        Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
    }

    return FinishStickyPromotion(spaceOp, pageItem, bytes);
}

template <class TTraits>
bool TCacheCollections<TTraits>::FinishStickyPromotion(
    TSpaceOperation& spaceOp, TCacheItem pageItem, ui64 bytes) noexcept {
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    Memory_.Hot().TransferBytesTo(Memory_.Sticky(), bytes);
    Memory_.Hot().TransferPageTo(Memory_.Sticky(), EItemKind::Page);
    TOperationItemRef<TTraits> collectionRef(spaceOp);
    if (!LinkStickyPage(spaceOp, pageItem, collectionRef.Get())) {
        ui64 claimedRaw = page.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState claimed = THandleState::FromRaw(claimedRaw);
            Y_DEBUG_ABORT_UNLESS(pageItem.Matches(claimed) && claimed.IsHot() && claimed.IsStickyField());
            if (page.State.compare_exchange_weak(claimedRaw, claimed.WithSticky(EStickyState::None).Raw(),
                    std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                Memory_.Sticky().TransferBytesTo(Memory_.Hot(), bytes);
                Memory_.Sticky().TransferPageTo(Memory_.Hot(), EItemKind::Page);
                Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
                Services_.RouteHotL1(spaceOp, pageItem);
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
    Traits_.Invoke(ESharedCacheHookPoint::AfterStickyPageListDetached, collectionItem.CacheItem());

    ui32 current = detachedHead;
    ui32 retainedHead = 0;
    ui32 retainedTail = 0;
    while (current != 0) {
        Y_DEBUG_ABORT_UNLESS(current < spaceOp.AccessibleHandleCount());
        THandle& page = spaceOp.Handles()[current];
        const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
        const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(state.IsPageKind() && (state.IsPending() || state.IsCompleting() || state.IsSticky()) &&
                             state.IsStickyField() &&
                             page.Key0.load(std::memory_order_relaxed) == collectionItem.Raw());

        if (current < allocationLimit || state.IsPending() || state.IsCompleting()) {
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
    const bool detached = (state.IsHot() || state.IsCold()) && state.IsStickyNoneField() &&
                          !value.Registry_.load(std::memory_order_acquire);
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
    if (wasKeep && !keep) {
        Services_.ScheduleKeepColdSweep(spaceOp.KeepCold().Capacity);
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
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    TCollection& value = *handle.Body.Collection;
    const ECacheMode previousMode = value.CacheMode_.exchange(ECacheMode::Regular, std::memory_order_acq_rel);
    PublishKeepPageBytes(value);
    if (previousMode == ECacheMode::TryKeepInMemory) {
        Services_.ScheduleKeepColdSweep(spaceOp.KeepCold().Capacity);
    }
    DrainStickyPages(spaceOp, value);
    Y_ABORT_UNLESS(Unlink(spaceOp, registry, collectionItem, EHandleState::Sticky));

    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() && expected.IsSticky() &&
                       expected.IsTransitionField());
        const THandleState desired = expected.WithSticky(EStickyState::None).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            break;
        }
    }
    if (DropReference(*handle.Body.Collection)) {
        PublishCollectionForReclaim(spaceOp, collectionItem);
    }
    return true;
}

template <class TTraits>
bool TCacheCollections<TTraits>::WantsStickyPage(const TCollection& collection) noexcept {
    return collection.CacheMode_.load(std::memory_order_acquire) == ECacheMode::Sticky;
}

template <class TTraits>
bool TCacheCollections<TTraits>::TryClaimDeletion(TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry,
    TCacheItem collectionItem, THandleState& sourceState, TSharedCacheItemRefImpl<TTraits>& owner) noexcept {
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
        /* the record's own reference is carried by the sticky field: attached means one
         * reference for the record itself, detached means none, and every other
         * reference belongs to a live page item */
        const ui64 ownReference = expected.IsStickyField() ? 1 : 0;
        if (handle.Body.Collection->References.load(std::memory_order_acquire) != ownReference) {
            return false;
        }
        const bool stickySource = expected.IsSticky() && expected.IsStickyField() && registry == expectedRegistry;
        const bool detachedSource = expected.IsStickyNoneField() && !registry && !expectedRegistry;
        if (!stickySource && !detachedSource) {
            return false;
        }
        if (Table_.TryBeginErase(spaceOp, collectionItem, expected, owner)) {
            const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
            Services_.UpdateColdReclaimable(handle.Metadata.PayloadBytes, expected, desired);
            sourceState = expected;
            Traits_.Invoke(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, collectionItem);
            return true;
        }
    }
}

template <class TTraits>
bool TCacheCollections<TTraits>::Delete(
    TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry, TCacheItem collectionItem) noexcept {
    TOperationItemRef<TTraits> deletionOwner(spaceOp);
    THandleState sourceState;
    if (!TryClaimDeletion(spaceOp, expectedRegistry, collectionItem, sourceState, deletionOwner.Get())) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    if (sourceState.IsCold()) {
        Services_.AdvanceColdMembership(handle);
    }

    TCollectionRegistry* registryOwner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
    if (registryOwner) {
        Unlink(*registryOwner, deletionOwner);
    } else {
        Y_DEBUG_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_relaxed) == 0);
    }

    const ui64 bytes = handle.Metadata.PayloadBytes;
    if (sourceState.IsSticky()) {
        Memory_.ReleaseRetainedBytes(bytes);
        Memory_.Sticky().TransferBytesTo(Memory_.Cold(), bytes);
        Memory_.AddColdItem();
    } else if (sourceState.IsHot()) {
        Memory_.ReleaseRetainedBytes(bytes);
        Memory_.Hot().TransferBytesTo(Memory_.Cold(), bytes);
        Memory_.AddColdItem();
    }

    const TSharedCacheKey key = LoadSharedCacheKey(handle, EItemKind::Collection);
    Table_.FinishErase(spaceOp, deletionOwner.CacheItem(), key);
    return true;
}

template <class TTraits>
bool TCacheCollections<TTraits>::MoveRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& source,
    TCollectionRegistry& destination, TCacheItem collectionItem) noexcept {
    TOperationItemRef<TTraits> collectionRef(spaceOp, Services_.TryAcquireStructural(spaceOp, collectionItem));
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

template class TCacheCollectionServices<TProdTraits>;
template class TCacheCollectionServices<TTestTraits>;
template class TCacheCollections<TProdTraits>;
template class TCacheCollections<TTestTraits>;

} // namespace NKikimr::NSharedCache
