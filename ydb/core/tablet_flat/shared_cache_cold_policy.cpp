#include "shared_cache_cold_policy.h"
#include "shared_cache.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
void TCacheColdPolicy<TTraits>::PublishCold(
    TSpaceOperation& spaceOp, const TCacheTierView& tier, TCacheItem nativeItem) noexcept {
    if (!spaceOp.Contains(nativeItem) || nativeItem.IsFrozen()) {
        return;
    }
    THandle& handle = spaceOp.Handles()[nativeItem.Index()];
    const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!nativeItem.Matches(state) || !state.IsCold() || state.Tier() != tier.Id || !state.IsStickyNoneField() ||
        state.Refs() != 0) {
        return;
    }
    Enqueue(spaceOp, tier, TColdCacheItem::Make(coldVersion, nativeItem.Index()), handle, state);
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
    if (!state.IsCold() || state.Tier() != tier.Id || !state.IsStickyNoneField() || state.Refs() != 0) {
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
    Cache_.InvokeHook(ESharedCacheHookPoint::BeforeColdExchange, TCacheItem::FromRaw(coldItem.Raw()));
    const TColdCacheItem displaced = TColdCacheItem::FromRaw(tier.Cold().PutAndPop(coldItem.Raw()));
    if (!displaced.IsNull()) {
        tier.Rings->RemoveColdEntry();
        Cache_.Eviction_.EraseCold(spaceOp, displaced, tier.Id);
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
        Cache_.InvokeHook(ESharedCacheHookPoint::BeforeColdRingPop, TCacheItem{});
        const ui64 rawItem = Preload ? Cache_.Eviction_.PopForPreload(spaceOp, cold, true).Previous : cold.Pop();
        if (rawItem != 0) {
            tier.Rings->RemoveColdEntry();
            if (Preload ? Cache_.Eviction_.TryErasePreloadPage(spaceOp, TColdCacheItem::FromRaw(rawItem), tier.Id)
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
