#include "shared_cache_eviction.h"
#include "shared_cache_items.h"
#include "shared_cache.h"

#include <util/generic/cast.h>

namespace NKikimr::NSharedCache {

template <class TTraits>
bool TCacheEviction<TTraits>::WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    if (TCollection* collection = Cache_.Collections_.BindOperation(spaceOp).PageCollection(page)) {
        return collection->GetCacheMode() == ECacheMode::TryKeepInMemory &&
               collection->IsKeepAllowedPage(page.Metadata.Page.Type);
    }
    return false;
}

template <class TTraits>
bool TCacheEviction<TTraits>::DemoteKeepCold(
    TSpaceOperation& spaceOp, TColdCacheItem coldItem, TPageCacheItem expectedPage) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return false;
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!expected.IsKeepCold() || expected.Refs() != 0 || (expectedPage && !expectedPage.Matches(expected)) ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion())
        {
            return false;
        }
        // Claim the demotion and a native ref together: an already-matched consumer may still tombstone it.
        const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Cold);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            TOperationItemRef<TTraits> itemRef(spaceOp, TCacheItem::Make(expected.Version(), coldItem.Index()));
            Cache_.InvokeHook(ESharedCacheHookPoint::AfterKeepColdDemoted, TCacheItem::FromRaw(coldItem.Raw()));
            const ui64 bytes = TCacheItems<TTraits>::PageBytes(handle);
            Cache_.AdvanceColdMembership(handle);
            Cache_.Memory_.ReleaseRetainedBytes(bytes);
            Cache_.Memory_.KeepCold().TransferBytesTo(Cache_.Memory_.Cold(), bytes);
            Cache_.Memory_.KeepCold().TransferPageTo(Cache_.Memory_.Cold(), EItemKind::Page);
            Cache_.Memory_.AddColdItem();
            Cache_.Memory_.ReleaseKeepColdOwnedBytes(bytes);
            if (TCollection* owner = Cache_.Collections_.BindOperation(spaceOp).PageCollection(handle)) {
                Cache_.Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
            ReleaseColdDemotionRef(spaceOp, itemRef);
            return true;
        }
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::ReleaseColdDemotionRef(
    TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& itemRef) noexcept {
    const TCacheItem cacheItem = itemRef.CacheItem();
    itemRef.Disarm();
    // A policy demotion must not readmit the page to KeepCold when releasing its temporary ref.
    if (TCollection* owner = Cache_.Items_.BindOperation(spaceOp).ReleaseItem(cacheItem, false)) {
        Cache_.Collections_.BindOperation(spaceOp).DropOwnerReference(*owner);
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    if (DemoteKeepCold(spaceOp, coldItem)) {
        return;
    }
    if (!MatchesKeepColdEntry(spaceOp, coldItem)) {
        return;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    ui32 coldVersion;
    if (!Cache_.AdvanceColdMembership(handle, coldItem.ColdVersion(), false, coldVersion)) {
        return;
    }
    DemoteKeepCold(spaceOp, TColdCacheItem::Make(coldVersion, coldItem.Index()));
}

template <class TTraits>
bool TCacheEviction<TTraits>::MatchesKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return false;
    }
    const THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return false;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    return state.IsKeepCold();
}

template <class TTraits>
bool TCacheEviction<TTraits>::IsKeepColdQueued(const THandle& handle) noexcept {
    return (handle.ColdVersion.load(std::memory_order_acquire) & KeepColdQueuedMask) != 0;
}

template <class TTraits>
void TCacheEviction<TTraits>::RouteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!expected.IsKeepCold() ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
            return;
        }
        if (expected.Refs() != 0) {
            // The caller took this page's ring entry; its last reference will process the withdrawal.
            WithdrawKeepColdEntry(spaceOp, coldItem);
            return;
        }
        if (handle.State.compare_exchange_weak(
                expectedRaw, expected.IncrementRefs().Raw(), std::memory_order_acquire, std::memory_order_relaxed)) {
            break;
        }
    }
    TOperationItemRef<TTraits> itemRef(
        spaceOp, TCacheItem::Make(THandleState::FromRaw(expectedRaw).Version(), coldItem.Index()));
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    if (!WantsKeepCold(spaceOp, handle)) {
        WithdrawKeepColdEntry(spaceOp, coldItem);
        return;
    }
    ui32 queuedVersion;
    if (!Cache_.AdvanceColdMembership(handle, coldItem.ColdVersion(), true, queuedVersion)) {
        return;
    }
    const TColdCacheItem queuedItem = TColdCacheItem::Make(queuedVersion, coldItem.Index());
    const ui64 displaced = spaceOp.KeepCold().PutAndPop(queuedItem.Raw());
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterKeepColdQueued, TCacheItem::FromRaw(queuedItem.Raw()));
    if (displaced != 0) {
        const TColdCacheItem displacedItem = TColdCacheItem::FromRaw(displaced);
        if (!DemoteKeepCold(spaceOp, displacedItem)) {
            // The displaced page stays KeepCold without a ring entry: demote it on its last release.
            WithdrawKeepColdEntry(spaceOp, displacedItem);
        }
    }
    if (!WantsKeepCold(spaceOp, handle)) {
        WithdrawKeepColdEntry(spaceOp, queuedItem);
    }
    itemRef.Drop();
    TrimKeepCold(spaceOp);
}

template <class TTraits>
void TCacheEviction<TTraits>::ReleaseKeepColdRef(
    TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& itemRef, ui32 initialColdVersion) noexcept {
    const TCacheItem cacheItem = itemRef.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsKeepCold() || expected.Refs() != 1 ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != initialColdVersion ||
            IsKeepColdQueued(handle) || !WantsKeepCold(spaceOp, handle))
        {
            itemRef.Drop();
            return;
        }
        const THandleState desired = expected.DecrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            itemRef.Disarm();
            RouteKeepCold(spaceOp, TColdCacheItem::Make(initialColdVersion, cacheItem.Index()));
            Cache_.NotifyResourcesAvailable();
            return;
        }
    }
}

template <class TTraits>
TCacheItem TCacheEviction<TTraits>::PutHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    const TRingExchangeResult exchange =
        spaceOp.Hot(ToUnderlying(level)).PutAndPopWithHook(cacheItem.Raw(), [&]() noexcept {
            Cache_.InvokeHook(ESharedCacheHookPoint::BeforeHotExchange, cacheItem);
        });
    // The operation protects the mapping; a concurrent cut may still exclude this slot.
    if (!spaceOp.IsAllowedHotSlot(exchange.Slot)) {
        ui64 expected = cacheItem.Raw();
        if (exchange.Slot->compare_exchange_strong(expected, 0, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            EvictFromHot(spaceOp, cacheItem);
        }
    }
    return TCacheItem::FromRaw(exchange.Previous);
}

template <class TTraits>
void TCacheEviction<TTraits>::RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    const TCacheItem displaced = PutHot(spaceOp, level, cacheItem);
    if (!displaced.IsNull()) {
        ProcessHot(spaceOp, level, displaced);
    }
}

template <class TTraits>
Y_FORCE_INLINE bool TCacheEviction<TTraits>::TakeHotFrequency(
    THandle& handle, TCacheItem cacheItem, THandleState& state, ui8& frequency) noexcept {
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsHot() || !expected.IsStickyNoneField()) {
            return false;
        }
        const THandleState desired = expected.WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            state = desired;
            frequency = expected.Frequency();
            return true;
        }
    }
}

template <class TTraits>
Y_FORCE_INLINE void TCacheEviction<TTraits>::CompleteHotEviction(TSpaceOperation& spaceOp, THandle& handle,
    THandleState coldState, ui64 bytes, TOperationItemRef<TTraits>& itemRef, ui32 initialColdVersion) noexcept {
    Y_DEBUG_ABORT_UNLESS(coldState.Refs() > 0);
    Cache_.InvokeHook(ESharedCacheHookPoint::AfterHotEvictionPublished, itemRef.CacheItem());
    if (Cache_.Memory_.UpdateColdReclaimable(bytes, coldState.WithState(EHandleState::Hot), coldState)) {
        Cache_.NotifyResourcesAvailable();
    }
    if (coldState.IsKeepCold()) {
        Cache_.Memory_.Hot().TransferBytesTo(Cache_.Memory_.KeepCold(), bytes);
        Cache_.Memory_.Hot().TransferPageTo(Cache_.Memory_.KeepCold(), coldState.Kind());
    } else {
        Cache_.Memory_.ReleaseRetainedBytes(bytes);
        Cache_.Memory_.Hot().TransferBytesTo(Cache_.Memory_.Cold(), bytes);
        Cache_.Memory_.Hot().TransferPageTo(Cache_.Memory_.Cold(), coldState.Kind());
        Cache_.Memory_.AddColdItem();
        if (coldState.IsPageKind()) {
            if (TCollection* owner = Cache_.Collections_.BindOperation(spaceOp).PageCollection(handle)) {
                Cache_.Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
        }
    }
    ReleaseKeepColdRef(spaceOp, itemRef, initialColdVersion);
}

template <class TTraits>
void TCacheEviction<TTraits>::ProcessHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    for (;;) {
        TOperationItemRef<TTraits> evictionRef(
            spaceOp, Cache_.Items_.BindOperation(spaceOp).TryAcquireHotItemRef(cacheItem));
        if (!evictionRef) {
            return;
        }
        THandle& handle = spaceOp.Handles()[cacheItem.Index()];
        THandleState state;
        ui8 frequency;
        if (!TakeHotFrequency(handle, cacheItem, state, frequency)) {
            return;
        }

        if (frequency == 0) {
            if (level != EHotLevel::L0) {
                level = EHotLevel::L0;
            } else {
                ui64 hotRaw = state.Raw();
                const ui64 bytes = TCacheItems<TTraits>::PayloadBytes(handle, state.Kind());
                const bool keep = state.IsPageKind() && state.Refs() == 1 && WantsKeepCold(spaceOp, handle);
                if (keep) {
                    Cache_.Memory_.AddKeepColdOwnedBytes(bytes);
                }
                const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
                const THandleState cold = state.WithState(keep ? EHandleState::KeepCold : EHandleState::Cold);
                if (handle.State.compare_exchange_strong(
                        hotRaw, cold.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
                {
                    CompleteHotEviction(spaceOp, handle, cold, bytes, evictionRef, coldVersion);
                    return;
                }

                if (keep) {
                    Cache_.Memory_.ReleaseKeepColdOwnedBytes(bytes);
                }

                level = EHotLevel::L0;
            }
        } else {
            level = level >= EHotLevel::L2 && frequency > 1 ? EHotLevel::L3 : EHotLevel::L2;
        }

        evictionRef.Drop();
        const TCacheItem displaced = PutHot(spaceOp, level, cacheItem);
        if (displaced.IsNull()) {
            return;
        }
        cacheItem = displaced;
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept {
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AccessibleHandleCount());

    const THandleState state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_relaxed));
    if (!state.IsHot()) {
        return false;
    }
    return EvictFromHot(spaceOp, TCacheItem::Make(state.Version(), index));
}

template <class TTraits>
bool TCacheEviction<TTraits>::EvictFromHot(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem) && !cacheItem.IsFrozen() && cacheItem.Index() >= 2);

    TOperationItemRef<TTraits> evictionRef(
        spaceOp, Cache_.Items_.BindOperation(spaceOp).TryAcquireHotItemRef(cacheItem));
    if (!evictionRef) {
        return false;
    }
    const ui32 index = cacheItem.Index();
    THandle& handle = spaceOp.Handles()[index];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsHot() || !expected.IsStickyNoneField()) {
            return false;
        }
        const ui64 bytes = TCacheItems<TTraits>::PayloadBytes(handle, expected.Kind());
        const bool keep = expected.IsPageKind() && expected.Refs() == 1 && WantsKeepCold(spaceOp, handle);
        if (keep) {
            Cache_.Memory_.AddKeepColdOwnedBytes(bytes);
        }
        const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
        const THandleState desired =
            expected.WithState(keep ? EHandleState::KeepCold : EHandleState::Cold).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
        {
            CompleteHotEviction(spaceOp, handle, desired, bytes, evictionRef, coldVersion);
            return true;
        }
        if (keep) {
            Cache_.Memory_.ReleaseKeepColdOwnedBytes(bytes);
        }
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::PromoteCold(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool force) noexcept {
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsCold()) {
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(expected.IsPageKind() && expected.Refs() != 0);
        const ui64 bytes = TCacheItems<TTraits>::PageBytes(handle);
        TCollection* owner = Cache_.Collections_.BindOperation(spaceOp).PageCollection(handle);
        const bool protectedPage = force || !owner || owner->GetCacheMode() != ECacheMode::Regular;
        if (protectedPage) {
            Cache_.Memory_.AddRetainedBytes(bytes);
        } else if (!Cache_.Memory_.TryAddRetainedBytes(bytes)) {
            return false;
        }
        Cache_.Memory_.Hot().AddBytes(bytes);
        Cache_.Memory_.Hot().AddPage(EItemKind::Page);
        const THandleState desired = expected.WithState(EHandleState::Hot).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
            Cache_.AdvanceColdMembership(handle);
            Cache_.Memory_.Cold().RemoveBytes(bytes);
            Cache_.Memory_.Cold().RemovePage(EItemKind::Page);
            Cache_.Memory_.RemoveColdItem();
            if (owner) {
                Cache_.Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
            }
            return true;
        }
        Cache_.Memory_.Hot().RemoveBytes(bytes);
        Cache_.Memory_.Hot().RemovePage(EItemKind::Page);
        Cache_.Memory_.ReleaseRetainedBytes(bytes);
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::ProcessHotAtMinimum(TSpaceOperation& spaceOp) noexcept {
    bool progress = false;
    // Give every level a share of the bounded work even when the lower rings are large or empty.
    for (ui32 level = 0; level < 4; ++level) {
        const TRingView hot = spaceOp.Hot(level);
        const ui64 attempts = Min<ui64>(hot.Capacity, SharedCacheTransitionWorkBatch / 4);
        for (ui64 attempt = 0; attempt < attempts; ++attempt) {
            const ui64 raw = hot.Pop();
            if (raw != 0) {
                ProcessHot(spaceOp, static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
                progress = true;
            }
        }
    }
    return progress;
}

template <class TTraits>
bool TCacheEviction<TTraits>::EraseCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen())
    {
        return false;
    }

    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return false;
    }
    TCollection* pageCollection = nullptr;

    TOperationItemRef<TTraits> ownerRef(spaceOp);
    THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    for (;;) {
        if ((!expected.IsCold() && !expected.IsKeepCold()) || (expected.IsKeepCold() && expected.Refs() != 0) ||
            !expected.IsStickyNoneField() ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
            return false;
        }
        const TCacheItem cacheItem = TCacheItem::Make(expected.Version(), coldItem.Index());
        if (Cache_.Table_.TryBeginErase(spaceOp, cacheItem, expected, ownerRef.Get())) {
            // The tombstone owner now pins the page and its collection during accounting and notification.
            pageCollection =
                expected.IsPageKind() ? Cache_.Collections_.BindOperation(spaceOp).PageCollection(handle) : nullptr;
            const ui64 bytes = TCacheItems<TTraits>::PayloadBytes(handle, expected.Kind());
            const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
            if (Cache_.Memory_.UpdateColdReclaimable(bytes, expected, desired)) {
                Cache_.NotifyResourcesAvailable();
            }
            if (expected.IsKeepCold()) {
                Cache_.Memory_.ReleaseRetainedBytes(bytes);
                Cache_.Memory_.KeepCold().TransferBytesTo(Cache_.Memory_.Cold(), bytes);
                Cache_.Memory_.KeepCold().TransferPageTo(Cache_.Memory_.Cold(), expected.Kind());
                Cache_.Memory_.AddColdItem();
                Cache_.Memory_.ReleaseKeepColdOwnedBytes(bytes);
                if (pageCollection) {
                    Cache_.Collections_.AddActivePageBytes(
                        *pageCollection, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
                }
            }
            Cache_.InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, TCacheItem::FromRaw(coldItem.Raw()));
            break;
        }
    }

    const THandleState coldState = expected;
    // Stale ring entries may name reattached collections; check only after claiming an eviction owner.
    if (coldState.IsCollectionKind()) {
        Y_DEBUG_ABORT_UNLESS(
            !handle.Body.Collection || Cache_.Collections_.IsUnregisteredAndUnreferenced(*handle.Body.Collection));
    }
    const TCacheItem cacheItem = ownerRef.CacheItem();
    const TSharedCacheKey key = LoadSharedCacheKey(handle, coldState.Kind());
    if (coldState.IsPageKind() && pageCollection && pageCollection->GetCacheMode() == ECacheMode::TryKeepInMemory &&
        pageCollection->IsKeepAllowedPage(handle.Metadata.Page.Type))
    {
        Cache_.NotifyKeepPageEviction(pageCollection->Id(), pageCollection->CacheItem, pageCollection->KeepGeneration(),
            NTable::NPage::TPageLocation(NTable::NPage::TPageOffset::FromRaw(key.Word(1)),
                handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type,
                handle.Metadata.Page.Crc32));
    }
    Cache_.AdvanceColdMembership(handle);
    Cache_.Table_.FinishErase(spaceOp, cacheItem, key);
    return true;
}

template <class TTraits>
void TCacheEviction<TTraits>::RouteCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return;
    }

    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!state.IsCold() || !state.IsStickyNoneField() || state.Refs() != 0) {
        return;
    }

    Cache_.Memory_.AddColdRingEntry();
    Cache_.InvokeHook(ESharedCacheHookPoint::BeforeColdExchange, TCacheItem::FromRaw(coldItem.Raw()));
    const TColdCacheItem displaced = spaceOp.PutCold(coldItem);
    if (!displaced.IsNull()) {
        Cache_.Memory_.RemoveColdRingEntry();
        EraseCold(spaceOp, displaced);
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReclaimCold(TSpaceOperation& spaceOp) noexcept {
    const TRingView cold = spaceOp.Cold();
    for (ui64 attempt = 0; attempt < cold.Capacity; ++attempt) {
        if (Cache_.Memory_.ColdRingEntries() == 0) {
            return false;
        }
        Cache_.InvokeHook(ESharedCacheHookPoint::BeforeColdRingPop, TCacheItem{});
        const ui64 rawItem = cold.Pop();
        if (rawItem != 0) {
            Cache_.Memory_.RemoveColdRingEntry();
            if (EraseCold(spaceOp, TColdCacheItem::FromRaw(rawItem))) {
                return true;
            }
        }
    }
    return false;
}

template <class TTraits>
bool TCacheEviction<TTraits>::DemoteKeepColdForSoftLimit(TSpaceOperation& spaceOp) noexcept {
    const TRingView keep = spaceOp.KeepCold();
    for (ui64 attempt = 0; attempt < keep.Capacity && Cache_.Memory_.KeepColdOwnedBytes() != 0; ++attempt) {
        const ui64 raw = keep.Pop();
        if (raw == 0) {
            continue;
        }
        const TColdCacheItem item = TColdCacheItem::FromRaw(raw);
        if (DemoteKeepCold(spaceOp, item)) {
            return true;
        }
        // The FIFO entry is gone even when a structural ref delays demotion.
        WithdrawKeepColdEntry(spaceOp, item);
    }
    return false;
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReclaimKeepCold(TSpaceOperation& spaceOp) noexcept {
    const TRingView keep = spaceOp.KeepCold();
    for (ui64 attempt = 0; attempt < keep.Capacity && Cache_.Memory_.KeepColdOwnedBytes() != 0; ++attempt) {
        const ui64 raw = keep.Pop();
        if (raw == 0) {
            continue;
        }
        const TColdCacheItem item = TColdCacheItem::FromRaw(raw);
        if (!MatchesKeepColdEntry(spaceOp, item)) {
            continue;
        }
        Cache_.InvokeHook(ESharedCacheHookPoint::AfterKeepColdEntryMatched, TCacheItem::FromRaw(item.Raw()));
        if (EraseCold(spaceOp, item)) {
            return true;
        }
        WithdrawKeepColdEntry(spaceOp, item);
    }
    return false;
}

template class TCacheEviction<TProdTraits>;
template class TCacheEviction<TTestTraits>;

} // namespace NKikimr::NSharedCache
