#include "cache_eviction.h"
#include "cache_items.h"
#include "cache.h"

#include <util/generic/scope.h>

namespace NKikimr::NSharedCache {

template <class TTraits>
bool TCacheEviction<TTraits>::WantsKeepPage(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    if (TCollection* owner = Cache_.Collections_.BindOperation(spaceOp).PageCollection(page)) {
        return owner->GetCacheMode() == ECacheMode::TryKeepInMemory &&
               owner->IsKeepAllowedPage(page.Metadata.Page.Type);
    }
    return false;
}

template <class TTraits>
TKeepReservation TCacheEviction<TTraits>::ReserveKeepForDemand(TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    if (bytes > KeepLimit() || Cache_.Memory_.KeepLimitPages() == 0) {
        return {};
    }
    for (ui32 attempt = 0; attempt < SharedCacheTransitionWorkBatch; ++attempt) {
        auto reservation = Cache_.Memory_.TryReserveKeep(bytes, KeepLimit());
        if (reservation) {
            return reservation;
        }
        if (!ReclaimCold(spaceOp, ECacheTier::Keep) &&
            !HotPolicy_.ProcessBatch(spaceOp, spaceOp.View().Tiers[ECacheTier::Keep])) {
            break;
        }
    }
    return {};
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReconcilePage(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner) noexcept {
    return ReconcilePageImpl<false>(spaceOp, owner);
}

template <class TTraits>
bool TCacheEviction<TTraits>::TouchPage(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner) noexcept {
    return ReconcilePageImpl<true>(spaceOp, owner);
}

template <class TTraits>
template <bool Demand>
bool TCacheEviction<TTraits>::ReconcilePageImpl(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner) noexcept {
    TKeepReservation incoming;
    for (;;) {
        const TCacheItem item = owner.CacheItem();
        THandle& handle = spaceOp.Handles()[item.Index()];
        ui64 raw = handle.State.load(std::memory_order_acquire);
        for (;;) {
            const auto before = THandleState::FromRaw(raw);
            if (item.Matches(before) && (before.IsReplacing() || before.IsReplaced())) {
                auto current = Cache_.AcquirePageItem(spaceOp, TPageCacheItem::FromValidated(item));
                if (!current) {
                    return false;
                }
                owner = std::move(current);
                break;
            }
            if (!item.Matches(before) || !before.IsPageKind() || (!before.IsHot() && !before.IsCold()) ||
                !before.IsStickyNoneField()) {
                return false;
            }
            const ECacheTier desiredTier = WantsKeepPage(spaceOp, handle) ? ECacheTier::Keep : ECacheTier::Regular;
            if (before.Tier() == desiredTier) {
                return false;
            }
            const ui64 bytes = TCacheItems<TTraits>::PageBytes(handle);
            if (desiredTier == ECacheTier::Keep && !incoming) {
                if constexpr (Demand) {
                    incoming = ReserveKeepForDemand(spaceOp, bytes);
                } else {
                    incoming = Cache_.Memory_.TryReserveKeep(bytes, KeepLimit());
                }
                if (!incoming) {
                    THandleState expected = before;
                    if (!Cache_.Table_.TryBeginErase(spaceOp, owner, expected)) {
                        raw = expected.Raw();
                        continue;
                    }
                    FinishPageErase(spaceOp, item);
                    return true;
                }
            }
            const auto after = before.WithState(
                before.IsHot() ? THandleState::HotState(desiredTier) : THandleState::ColdState(desiredTier));
            if (!handle.State.compare_exchange_weak(
                    raw, after.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
                continue;
            }
            Cache_.InvokeHook(ECacheHookPoint::AfterPageRetierClaim, item);
            if (before.IsCold()) {
                Cache_.AdvanceColdMembership(handle);
            }
            auto& source = Cache_.Memory_.ResidentCategory(before.State());
            auto& destination = Cache_.Memory_.ResidentCategory(after.State());
            source.TransferBytesTo(destination, bytes);
            source.TransferPageTo(destination, EItemKind::Page);
            if (desiredTier == ECacheTier::Keep) {
                incoming.Commit();
                Cache_.Capacity_.RequestTierRebalance();
            } else {
                Cache_.Memory_.ReleaseKeep(bytes);
                Cache_.NotifyResourcesAvailable();
            }
            return true;
        }
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::RouteOwnedHot(
    TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>& owner, EHotLevel level) noexcept {
    const ui64 generation = ModeGeneration_.load(std::memory_order_acquire);
    ReconcilePage(spaceOp, owner);
    const TCacheItem item = owner.CacheItem();
    const auto state = THandleState::FromRaw(spaceOp.Handles()[item.Index()].State.load(std::memory_order_acquire));
    if (item.Matches(state) && state.IsHot() && state.IsStickyNoneField()) {
        HotPolicy_.RouteHot(spaceOp, spaceOp.View().Tiers[state.Tier()], level, item);
    }
    CheckModeGeneration(spaceOp, generation);
}

template <class TTraits>
void TCacheEviction<TTraits>::RouteHot(
    TSpaceOperation& spaceOp, ECacheTier tier, EHotLevel level, TCacheItem item) noexcept {
    TOperationItemRef<TTraits> ref(spaceOp, Cache_.Items_.BindOperation(spaceOp).TryAcquireHotItemRef(item, tier));
    if (ref) {
        RouteOwnedHot(spaceOp, ref.Get(), level);
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem item) noexcept {
    const auto state = THandleState::FromRaw(spaceOp.Handles()[item.Index()].State.load(std::memory_order_acquire));
    if (item.Matches(state) && state.IsHot()) {
        RouteHot(spaceOp, state.Tier(), level, item);
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::TryEvictHot(
    THandle& handle, THandleState before, TOperationItemRef<TTraits>& ref) noexcept {
    ui64 raw = before.Raw();
    const auto after = before.WithState(THandleState::ColdState(before.Tier())).WithFrequency(0);
    if (!handle.State.compare_exchange_strong(raw, after.Raw(), std::memory_order_relaxed, std::memory_order_relaxed)) {
        return false;
    }
    Cache_.InvokeHook(ECacheHookPoint::AfterHotEvictionPublished, ref.CacheItem());
    const ui64 bytes = TCacheItems<TTraits>::PayloadBytes(handle, before.Kind());
    Cache_.AdvanceColdMembership(handle);
    Cache_.Memory_.Hot(before.Tier()).TransferBytesTo(Cache_.Memory_.Cold(before.Tier()), bytes);
    Cache_.Memory_.Hot(before.Tier()).TransferPageTo(Cache_.Memory_.Cold(before.Tier()), before.Kind());
    Cache_.Memory_.ReleaseRetainedBytes(bytes);
    ref.Drop();
    return true;
}

template <class TTraits>
bool TCacheEviction<TTraits>::EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept {
    const auto state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_acquire));
    return state.IsHot() && EvictFromHot(spaceOp, TCacheItem::Make(state.Version(), index), state.Tier());
}

template <class TTraits>
bool TCacheEviction<TTraits>::EvictFromHot(
    TSpaceOperation& spaceOp, TCacheItem item, ECacheTier expectedTier) noexcept {
    TOperationItemRef<TTraits> ref(
        spaceOp, Cache_.Items_.BindOperation(spaceOp).TryAcquireHotItemRef(item, expectedTier));
    if (!ref) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[item.Index()];
    for (;;) {
        const auto state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (!item.Matches(state) || !state.IsHotInTier(expectedTier) || !state.IsStickyNoneField()) {
            return false;
        }
        if (TryEvictHot(handle, state, ref)) {
            return true;
        }
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::PromoteCold(TSpaceOperation& spaceOp, TCacheItem item, bool force) noexcept {
    THandle& handle = spaceOp.Handles()[item.Index()];
    TKeepReservation incoming;
    ui64 raw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const auto before = THandleState::FromRaw(raw);
        if (!item.Matches(before) || !before.IsCold() || !before.IsStickyNoneField()) {
            return false;
        }
        const ECacheTier tier = !force && WantsKeepPage(spaceOp, handle) ? ECacheTier::Keep : ECacheTier::Regular;
        const ui64 bytes = TCacheItems<TTraits>::PageBytes(handle);
        if (tier == ECacheTier::Keep && NeedsKeepTrim()) {
            return false;
        }
        if (tier == ECacheTier::Keep && before.Tier() != tier && !incoming) {
            incoming = Cache_.Memory_.TryReserveKeep(bytes, KeepLimit());
            if (!incoming) {
                return false;
            }
        }
        TCollection* owner = Cache_.Collections_.BindOperation(spaceOp).PageCollection(handle);
        const bool guaranteed = force || !owner || owner->GetCacheMode() == ECacheMode::Sticky;
        if (guaranteed) {
            Cache_.Memory_.AddRetainedBytes(bytes);
        } else if (!Cache_.Memory_.TryAddRetainedBytes(bytes)) {
            return false;
        }
        const auto after = before.WithState(THandleState::HotState(tier)).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                raw, after.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
            Cache_.AdvanceColdMembership(handle);
            Cache_.Memory_.Cold(before.Tier()).TransferBytesTo(Cache_.Memory_.Hot(tier), bytes);
            Cache_.Memory_.Cold(before.Tier()).TransferPageTo(Cache_.Memory_.Hot(tier), EItemKind::Page);
            if (before.Tier() != tier) {
                if (tier == ECacheTier::Keep) {
                    incoming.Commit();
                    Cache_.Capacity_.RequestTierRebalance();
                } else {
                    Cache_.Memory_.ReleaseKeep(bytes);
                    Cache_.NotifyResourcesAvailable();
                }
            }
            return true;
        }
        Cache_.Memory_.ReleaseRetainedBytes(bytes);
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::FinishPageErase(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = Cache_.BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    Cache_.InvokeHook(ECacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
    if (TCollection* owner = cacheOp.Collections().PageCollection(handle);
        owner && owner->GetCacheMode() == ECacheMode::TryKeepInMemory &&
        owner->IsKeepAllowedPage(handle.Metadata.Page.Type)) {
        Cache_.NotifyKeepPageEviction(owner->Id(), owner->CacheItem, owner->KeepGeneration(),
            NTable::NPage::TPageLocation(
                NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
                handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type,
                handle.Metadata.Page.Crc32));
    }
    Cache_.AdvanceColdMembership(handle);
    cacheOp.FinishErase(cacheItem, LoadSharedCacheKey(handle, EItemKind::Page));
}

template <class TTraits>
bool TCacheEviction<TTraits>::EraseCold(
    TSpaceOperation& spaceOp, TColdCacheItem coldItem, ECacheTier expectedTier) noexcept {
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
        // Acquire the State observation, including failed-CAS retries, before validating its separate Cold epoch
        std::atomic_thread_fence(std::memory_order_acquire);
        if (!expected.IsColdInTier(expectedTier) || !expected.IsStickyNoneField() ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
            return false;
        }
        const TCacheItem cacheItem = TCacheItem::Make(expected.Version(), coldItem.Index());
        Cache_.InvokeHook(ECacheHookPoint::BeforeColdEraseClaim, TCacheItem::FromRaw(coldItem.Raw()));
        if (Cache_.Table_.TryBeginErase(spaceOp, cacheItem, expected, ownerRef.Get())) {
            // The tombstone owner now pins the page and its collection during accounting and notification.
            pageCollection =
                expected.IsPageKind() ? Cache_.Collections_.BindOperation(spaceOp).PageCollection(handle) : nullptr;
            const ui64 bytes = TCacheItems<TTraits>::PayloadBytes(handle, expected.Kind());
            const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
            if (Cache_.Memory_.UpdateColdReclaimable(bytes, expected, desired)) {
                Cache_.NotifyResourcesAvailable();
            }
            Cache_.InvokeHook(ECacheHookPoint::AfterTombstoneOwnerClaim, TCacheItem::FromRaw(coldItem.Raw()));
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
    const TCacheKey key = LoadSharedCacheKey(handle, coldState.Kind());
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
bool TCacheEviction<TTraits>::TryEnqueueColdForPreload(TSpaceOperation& spaceOp, const TCacheTierView& tier,
    TColdCacheItem coldItem, THandle& handle, THandleState state) noexcept {
    if (spaceOp.Hazard().PreloadReclaim && state.IsPageKind()) {
        TOperationItemRef<TTraits> collectionRef(spaceOp, AcquireRegularCollection(spaceOp, handle));
        if (collectionRef) {
            tier.Rings->AddColdEntry();
            Cache_.InvokeHook(ECacheHookPoint::BeforeColdExchange, TCacheItem::FromRaw(coldItem.Raw()));
            const auto exchange = ExchangeForPreload(spaceOp, tier.Cold().SelectPutSlot(), coldItem.Raw(), true);
            if (!exchange.Slot) {
                tier.Rings->RemoveColdEntry();
                FinishPreloadCold(spaceOp, tier, coldItem);
            } else if (exchange.Previous != 0) {
                tier.Rings->RemoveColdEntry();
                if (exchange.Previous != coldItem.Raw()) {
                    FinishPreloadCold(spaceOp, tier, TColdCacheItem::FromRaw(exchange.Previous));
                }
            }
            return true;
        }
    }
    return false;
}

template <class TTraits>
TCacheItemRefImpl<TTraits> TCacheEviction<TTraits>::AcquireRegularCollection(
    TSpaceOperation& spaceOp, const THandle& page) noexcept {
    const TCacheItem collection = TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed));
    if (!spaceOp.Contains(collection) || collection.IsFrozen()) {
        return {};
    }
    THandle& handle = spaceOp.Handles()[collection.Index()];
    ui64 raw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState state = THandleState::FromRaw(raw);
        if (!collection.Matches(state) || !state.IsCollectionKind() || !state.IsSticky() || !state.IsStickyField() ||
            state.Refs() == MaxHandleRefs) {
            return {};
        }
        if (handle.State.compare_exchange_weak(
                raw, state.IncrementRefs().Raw(), std::memory_order_acquire, std::memory_order_relaxed)) {
            break;
        }
    }
    TOperationItemRef<TTraits> ref(spaceOp, collection);
    return handle.Body.Collection && handle.Body.Collection->GetCacheMode() == ECacheMode::Regular
               ? ref.Take()
               : TCacheItemRefImpl<TTraits>{};
}

template <class TTraits>
bool TCacheEviction<TTraits>::IsPreloadProtected(
    TSpaceOperation& spaceOp, ui64 raw, bool cold, TOperationItemRef<TTraits>& collectionRef) noexcept {
    if (raw == 0) {
        return false;
    }
    const TCacheItem token = TCacheItem::FromRaw(raw);
    if (!spaceOp.Contains(token) || token.IsFrozen()) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[token.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (cold) {
        if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) !=
                TColdCacheItem::FromRaw(raw).ColdVersion() ||
            !state.IsCold()) {
            return false;
        }
    } else if (!token.Matches(state)) {
        return false;
    }
    // A same-version Cold Keep page can reheat before the slot CAS, reviving its old Hot token
    if (!state.IsPageKind() || !state.IsStickyNoneField() || state.Refs() != 0) {
        return true;
    }
    collectionRef.Get() = AcquireRegularCollection(spaceOp, handle);
    return !collectionRef;
}

template <class TTraits>
TRingExchangeResult TCacheEviction<TTraits>::PopForPreload(
    TSpaceOperation& spaceOp, TRingView ring, bool cold) noexcept {
    auto* slot = ring.SelectPopSlot();
    ui64 raw = slot->load(std::memory_order_acquire);
    TOperationItemRef<TTraits> collectionRef(spaceOp);
    const bool protect = IsPreloadProtected(spaceOp, raw, cold, collectionRef);
    if (!cold && raw != 0) {
        Cache_.InvokeHook(ECacheHookPoint::AfterPreloadHotTokenChecked, TCacheItem::FromRaw(raw));
    }
    if (protect || !slot->compare_exchange_strong(raw, 0, std::memory_order_acquire, std::memory_order_relaxed)) {
        return {};
    }
    return { slot, raw };
}

template <class TTraits>
TRingExchangeResult TCacheEviction<TTraits>::ExchangeForPreload(
    TSpaceOperation& spaceOp, std::atomic<ui64>* slot, ui64 incoming, bool cold) noexcept {
    ui64 raw = slot->load(std::memory_order_acquire);
    TOperationItemRef<TTraits> collectionRef(spaceOp);
    if (IsPreloadProtected(spaceOp, raw, cold, collectionRef) ||
        !slot->compare_exchange_strong(raw, incoming, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return {};
    }
    return { slot, raw };
}

template <class TTraits>
bool TCacheEviction<TTraits>::TryErasePreloadPage(
    TSpaceOperation& spaceOp, TColdCacheItem item, ECacheTier expectedTier) noexcept {
    if (!spaceOp.Contains(item) || item.IsFrozen()) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[item.Index()];
    THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!expected.IsCold() || expected.Tier() != expectedTier || !expected.IsPageKind() || expected.Refs() != 0 ||
        !expected.IsStickyNoneField() ||
        (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != item.ColdVersion()) {
        return false;
    }
    TOperationItemRef<TTraits> collectionRef(spaceOp, AcquireRegularCollection(spaceOp, handle));
    if (!collectionRef) {
        return false;
    }
    const TCacheItem page = TCacheItem::Make(expected.Version(), item.Index());
    Cache_.InvokeHook(ECacheHookPoint::BeforePreloadPageErase, page);
    TOperationItemRef<TTraits> pageRef(spaceOp);
    if (!Cache_.Table_.TryBeginErase(spaceOp, page, expected, pageRef.Get())) {
        return false;
    }
    const ui64 bytes = TCacheItems<TTraits>::PageBytes(handle);
    Cache_.UpdateColdReclaimable(bytes, expected, expected.IncrementRefs().WithState(EHandleState::Tombstone));
    FinishPageErase(spaceOp, page);
    return true;
}

template <class TTraits>
bool TCacheEviction<TTraits>::FinishPreloadCold(TSpaceOperation& spaceOp, const TCacheTierView& tier,
    TColdCacheItem item, std::atomic<ui64>* vacatedSlot) noexcept {
    if (TryErasePreloadPage(spaceOp, item, tier.Id)) {
        return true;
    }
    if (!spaceOp.Contains(item) || item.IsFrozen()) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[item.Index()];
    const auto state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!state.IsColdInTier(tier.Id) || !state.IsStickyNoneField() ||
        (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != item.ColdVersion()) {
        return false;
    }
    if (vacatedSlot) {
        tier.Rings->AddColdEntry();
        ui64 empty = 0;
        if (vacatedSlot->compare_exchange_strong(
                empty, item.Raw(), std::memory_order_release, std::memory_order_relaxed)) {
            return false;
        }
        tier.Rings->RemoveColdEntry();
    }
    // Selection already established a Regular victim. Later refs keep its payload alive;
    // a completed retier invalidates the captured membership instead of erasing Keep pages
    return EraseCold(spaceOp, item, tier.Id);
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReclaimRegularPage(TSpaceOperation& spaceOp) noexcept {
    bool& preload = spaceOp.Hazard().PreloadReclaim;
    Y_DEBUG_ABORT_UNLESS(!preload);
    preload = true;
    Y_DEFER {
        preload = false;
    };
    for (ui32 step = 0; step < SharedCacheTransitionWorkBatch; ++step) {
        if (ColdPolicy_.ReclaimForPreload(spaceOp, spaceOp.View().Tiers[ECacheTier::Regular])) {
            return true;
        }
        if (!HotPolicy_.ProcessPreloadBatch(spaceOp, spaceOp.View().Tiers[ECacheTier::Regular])) {
            return false;
        }
        if (spaceOp.AvailableHandleCount() != 0) {
            return true;
        }
    }
    return false;
}

template class TCacheEviction<TProdTraits>;
template class TCacheEviction<TTestTraits>;

} // namespace NKikimr::NSharedCache
