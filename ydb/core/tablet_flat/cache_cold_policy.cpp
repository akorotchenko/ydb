#include "cache_cold_policy.h"
#include "cache.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
void TCacheColdPolicy<TTraits>::PublishCold(
    TSpaceOperation& spaceOp, const TCacheTierView& tier, TCacheItem nativeItem) noexcept {
    if (!spaceOp.Contains(nativeItem) || nativeItem.IsFrozen()) {
        return;
    }
    THandle& handle = spaceOp.Handles()[nativeItem.Index()];
    ui32 membership = handle.ColdVersion.load(std::memory_order_acquire);
    for (;;) {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (!nativeItem.Matches(state) || !state.IsCold() || state.Tier() != tier.Id || !state.IsStickyNoneField() ||
            state.Refs() != 0 || (membership & ColdTicketOwnedMask)) {
            return;
        }
        if (handle.ColdVersion.compare_exchange_weak(
                membership, membership | ColdTicketOwnedMask, std::memory_order_acq_rel, std::memory_order_acquire)) {
            Cache_.InvokeHook(ECacheHookPoint::AfterColdTicketClaim, nativeItem);
            // A structural ref may appear after the claim; the ticket must still be delivered
            RouteCold(spaceOp, tier, TColdCacheItem::Make(membership & MaxItemVersion, nativeItem.Index()));
            return;
        }
    }
}

template <class TTraits>
void TCacheColdPolicy<TTraits>::RouteCold(
    TSpaceOperation& spaceOp, const TCacheTierView& tier, TColdCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return;
    }

    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!state.IsCold() || state.Tier() != tier.Id || !state.IsStickyNoneField()) {
        return;
    }

    Enqueue(spaceOp, tier, coldItem, handle, state);
}

template <class TTraits>
void TCacheColdPolicy<TTraits>::Enqueue(TSpaceOperation& spaceOp, const TCacheTierView& tier, TColdCacheItem coldItem,
    THandle& handle, THandleState state) noexcept {
    if (Cache_.Eviction_.TryEnqueueColdForPreload(spaceOp, tier, coldItem, handle, state)) {
        return;
    }

    tier.Rings->AddColdEntry();
    Cache_.InvokeHook(ECacheHookPoint::BeforeColdExchange, TCacheItem::FromRaw(coldItem.Raw()));
    const TColdCacheItem displaced = TColdCacheItem::FromRaw(tier.Cold().PutAndPop(coldItem.Raw()));
    if (!displaced.IsNull()) {
        tier.Rings->RemoveColdEntry();
        if (displaced != coldItem) {
            Cache_.Eviction_.EraseCold(spaceOp, displaced, tier.Id);
        }
    }
}

template <class TTraits>
template <bool Preload>
bool TCacheColdPolicy<TTraits>::ReclaimImpl(TSpaceOperation& spaceOp, const TCacheTierView& tier) noexcept {
    const TRingView cold = tier.Cold();
    const ui64 attempts = Preload ? Min<ui64>(cold.Capacity, SharedCacheTransitionWorkBatch / 2) : cold.Capacity;
    for (ui64 attempt = 0; attempt < attempts; ++attempt) {
        if (tier.Rings->ColdEntries() == 0) {
            return false;
        }
        Cache_.InvokeHook(ECacheHookPoint::BeforeColdRingPop, TCacheItem{});
        const auto exchange =
            Preload ? Cache_.Eviction_.PopForPreload(spaceOp, cold, true) : TRingExchangeResult{ nullptr, cold.Pop() };
        const ui64 rawItem = exchange.Previous;
        if (rawItem != 0) {
            tier.Rings->RemoveColdEntry();
            if (Preload
                    ? Cache_.Eviction_.FinishPreloadCold(spaceOp, tier, TColdCacheItem::FromRaw(rawItem), exchange.Slot)
                    : Cache_.Eviction_.EraseCold(spaceOp, TColdCacheItem::FromRaw(rawItem), tier.Id)) {
                return true;
            }
        }
    }
    return false;
}

template class TCacheColdPolicy<TProdTraits>;
template bool TCacheColdPolicy<TProdTraits>::ReclaimImpl<false>(TSpaceOperation&, const TCacheTierView&) noexcept;
template bool TCacheColdPolicy<TProdTraits>::ReclaimImpl<true>(TSpaceOperation&, const TCacheTierView&) noexcept;
template class TCacheColdPolicy<TTestTraits>;
template bool TCacheColdPolicy<TTestTraits>::ReclaimImpl<false>(TSpaceOperation&, const TCacheTierView&) noexcept;
template bool TCacheColdPolicy<TTestTraits>::ReclaimImpl<true>(TSpaceOperation&, const TCacheTierView&) noexcept;

} // namespace NKikimr::NSharedCache
