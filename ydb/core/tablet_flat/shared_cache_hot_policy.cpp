#include "shared_cache_hot_policy.h"
#include "shared_cache.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
TCacheItem TCacheHotPolicy<TTraits>::PutHot(
    TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level, TCacheItem item) noexcept {
    return PutHotImpl<false>(spaceOp, tier, level, item);
}

template <class TTraits>
template <bool Preload, bool Requeue>
TCacheItem TCacheHotPolicy<TTraits>::PutHotImpl(TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level,
    TCacheItem cacheItem, TRingExchangeResult* placement) noexcept {
    const TRingExchangeResult exchange = [&]() noexcept {
        if constexpr (Preload) {
            const auto hot = tier.Hot(level);
            auto* slot = Requeue ? hot.SelectPushSlot() : hot.SelectPutSlot();
            return Cache_.Eviction_.ExchangeForPreload(spaceOp, slot, cacheItem.Raw(), false);
        } else {
            return tier.Hot(level).PutAndPopWithHook(cacheItem.Raw(), [&]() noexcept {
                Cache_.InvokeHook(ESharedCacheHookPoint::BeforeHotExchange, cacheItem);
            });
        }
    }();
    if constexpr (Preload) {
        if (placement) {
            *placement = exchange;
        }
        if (!exchange.Slot) {
            return cacheItem;
        }
    }
    // The operation protects the mapping; a concurrent cut may still exclude this slot.
    if (!tier.IsAllowedHotSlot(exchange.Slot)) {
        ui64 expected = cacheItem.Raw();
        if (exchange.Slot->compare_exchange_strong(expected, 0, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            Cache_.Eviction_.EvictFromHot(spaceOp, cacheItem, tier.Id);
        }
    }
    return TCacheItem::FromRaw(exchange.Previous);
}

template <class TTraits>
Y_FORCE_INLINE bool TCacheHotPolicy<TTraits>::TakeHotFrequency(
    THandle& handle, TCacheItem cacheItem, ECacheTier expectedTier, THandleState& state, ui8& frequency) noexcept {
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsHot() || expected.Tier() != expectedTier ||
            !expected.IsStickyNoneField()) {
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
void TCacheHotPolicy<TTraits>::ProcessHot(
    TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level, TCacheItem item) noexcept {
    ProcessHotImpl<false>(spaceOp, tier, level, item);
}

template <class TTraits>
template <bool Preload>
void TCacheHotPolicy<TTraits>::ProcessHotImpl(
    TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level, TCacheItem cacheItem) noexcept {
    for (;;) {
        TOperationItemRef<TTraits> evictionRef(
            spaceOp, Cache_.Items_.BindOperation(spaceOp).TryAcquireHotItemRef(cacheItem, tier.Id));
        if (!evictionRef) {
            return;
        }
        THandle& handle = spaceOp.Handles()[cacheItem.Index()];
        THandleState state;
        ui8 frequency;
        if (!TakeHotFrequency(handle, cacheItem, tier.Id, state, frequency)) {
            return;
        }
        const EHotLevel sourceLevel = level;

        if (frequency == 0) {
            if (level != EHotLevel::L0) {
                level = EHotLevel::L0;
            } else {
                if (Cache_.Eviction_.TryEvictHot(spaceOp, handle, state, evictionRef)) {
                    return;
                }

                level = EHotLevel::L0;
            }
        } else {
            level = level >= EHotLevel::L2 && frequency > 1 ? EHotLevel::L3 : EHotLevel::L2;
        }

        evictionRef.Drop();
        TRingExchangeResult placement;
        TCacheItem displaced = PutHotImpl<Preload>(spaceOp, tier, level, cacheItem, &placement);
        if constexpr (Preload) {
            // Retry the source tier when protected pages block promotion
            if (!placement.Slot && frequency != 0) {
                const ui64 attempts = tier.Hot(sourceLevel).Capacity;
                for (ui64 attempt = 0; attempt < attempts; ++attempt) {
                    displaced = PutHotImpl<true, true>(spaceOp, tier, sourceLevel, cacheItem, &placement);
                    if (placement.Slot) {
                        level = sourceLevel;
                        break;
                    }
                }
            }
        }
        // The last ref may have notified while this token was still outside the ring
        Cache_.NotifyResourcesAvailable();
        if (displaced.IsNull()) {
            return;
        }
        cacheItem = displaced;
    }
}

template <class TTraits>
template <bool Preload>
bool TCacheHotPolicy<TTraits>::ProcessBatchImpl(TSpaceOperation& spaceOp, const TCacheTierView& tier) noexcept {
    bool progress = false;
    // Give every level a share of the bounded work even when the lower rings are large or empty.
    for (ui32 index = 0; index < HotLevelCount; ++index) {
        const auto level = static_cast<EHotLevel>(index);
        const TRingView hot = tier.Hot(level);
        const ui64 attempts = Min<ui64>(hot.Capacity, SharedCacheTransitionWorkBatch / HotLevelCount);
        for (ui64 attempt = 0; attempt < attempts; ++attempt) {
            const auto popped = Preload ? Cache_.Eviction_.PopForPreload(spaceOp, hot, false)
                                        : TRingExchangeResult{ nullptr, hot.Pop() };
            if (popped.Previous != 0) {
                ProcessHotImpl<Preload>(spaceOp, tier, level, TCacheItem::FromRaw(popped.Previous));
                progress = true;
            }
        }
    }
    return progress;
}

template class TCacheHotPolicy<TProdTraits>;
template bool TCacheHotPolicy<TProdTraits>::ProcessBatchImpl<false>(TSpaceOperation&, const TCacheTierView&) noexcept;
template bool TCacheHotPolicy<TProdTraits>::ProcessBatchImpl<true>(TSpaceOperation&, const TCacheTierView&) noexcept;
template class TCacheHotPolicy<TTestTraits>;
template bool TCacheHotPolicy<TTestTraits>::ProcessBatchImpl<false>(TSpaceOperation&, const TCacheTierView&) noexcept;
template bool TCacheHotPolicy<TTestTraits>::ProcessBatchImpl<true>(TSpaceOperation&, const TCacheTierView&) noexcept;

} // namespace NKikimr::NSharedCache
