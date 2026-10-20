#include "shared_cache.h"

#include <util/generic/cast.h>

namespace NKikimr::NSharedCache {

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TSharedCacheItemRef TSharedCacheItemRefImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCache::PutHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TRingExchangeResult exchange =
        spaceOp.Hot(ToUnderlying(level)).PutAndPopWithHook(cacheItem.Raw(), [&]() noexcept {
            InvokeHook(ESharedCacheHookPoint::BeforeHotExchange, cacheItem);
        });
    // The operation protects the mapping; a concurrent cut may still exclude this slot.
    if (!spaceOp.IsAllowedHotSlot(exchange.Slot)) {
        ui64 expected = cacheItem.Raw();
        if (exchange.Slot->compare_exchange_strong(expected, 0, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            cacheOp.EvictFromHot(cacheItem);
        }
    }
    return TCacheItem::FromRaw(exchange.Previous);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem displaced = cacheOp.PutHot(level, cacheItem);
    if (!displaced.IsNull()) {
        cacheOp.ProcessHot(level, displaced);
    }
}

SHARED_CACHE_TEMPLATE
Y_FORCE_INLINE bool TSharedCache::TakeHotFrequency(
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

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef TSharedCache::TryAcquireHotEvictionPin(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
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
            return TSharedCacheItemRef(cacheItem);
        }
    }
}

SHARED_CACHE_TEMPLATE
Y_FORCE_INLINE void TSharedCache::CompleteHotEviction(TSpaceOperation& spaceOp, THandle& handle, THandleState coldState,
    ui64 bytes, TOperationItemRef& pin, ui32 initialColdVersion) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(coldState.Refs() > 0);
    InvokeHook(ESharedCacheHookPoint::AfterHotEvictionPublished, pin.CacheItem());
    UpdateColdReclaimable(bytes, coldState.WithState(EHandleState::Hot), coldState);
    if (coldState.IsKeepCold()) {
        Memory_.Hot().TransferBytesTo(Memory_.KeepCold(), bytes);
        Memory_.Hot().TransferPageTo(Memory_.KeepCold(), coldState.Kind());
    } else {
        Memory_.ReleaseRetainedBytes(bytes);
        Memory_.Hot().TransferBytesTo(Memory_.Cold(), bytes);
        Memory_.Hot().TransferPageTo(Memory_.Cold(), coldState.Kind());
        Memory_.AddColdItem();
        if (coldState.IsPageKind()) {
            if (TCollection* owner = cacheOp.PageCollection(handle)) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
        }
    }
    cacheOp.ReleaseKeepColdPin(pin, initialColdVersion);
}

SHARED_CACHE_TEMPLATE
Y_FORCE_INLINE void TSharedCache::TOperationView::CompleteHotEviction(
    THandle& handle, THandleState coldState, ui64 bytes, TOperationItemRef& pin, ui32 initialColdVersion) noexcept {
    Cache_.CompleteHotEviction(SpaceOp_, handle, coldState, bytes, pin, initialColdVersion);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseKeepColdPin(
    TSpaceOperation& spaceOp, TOperationItemRef& pin, ui32 initialColdVersion) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem cacheItem = pin.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsKeepCold() || expected.Refs() != 1 ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != initialColdVersion ||
            IsKeepColdQueued(handle) || !cacheOp.WantsKeepCold(handle))
        {
            pin.Get().Drop(spaceOp);
            return;
        }
        const THandleState desired = expected.DecrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            pin.Disarm();
            cacheOp.RouteKeepCold(TColdCacheItem::Make(initialColdVersion, cacheItem.Index()));
            NotifyResourcesAvailable();
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ProcessHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    for (;;) {
        TOperationItemRef evictionPin(spaceOp, cacheOp.TryAcquireHotEvictionPin(cacheItem));
        if (!evictionPin) {
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
                const ui64 bytes = PayloadBytes(handle, state.Kind());
                const bool keep = state.IsPageKind() && state.Refs() == 1 && cacheOp.WantsKeepCold(handle);
                if (keep) {
                    Memory_.AddKeepColdOwnedBytes(bytes);
                }
                const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
                const THandleState cold = state.WithState(keep ? EHandleState::KeepCold : EHandleState::Cold);
                if (handle.State.compare_exchange_strong(
                        hotRaw, cold.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
                {
                    cacheOp.CompleteHotEviction(handle, cold, bytes, evictionPin, coldVersion);
                    return;
                }

                if (keep) {
                    Memory_.ReleaseKeepColdOwnedBytes(bytes);
                }

                level = EHotLevel::L0;
            }
        } else {
            level = level >= EHotLevel::L2 && frequency > 1 ? EHotLevel::L3 : EHotLevel::L2;
        }

        evictionPin.Get().Drop(spaceOp);
        const TCacheItem displaced = cacheOp.PutHot(level, cacheItem);
        if (displaced.IsNull()) {
            return;
        }
        cacheItem = displaced;
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AccessibleHandleCount());

    const THandleState state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_relaxed));
    if (!state.IsHot()) {
        return false;
    }
    return cacheOp.EvictFromHot(TCacheItem::Make(state.Version(), index));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EvictFromHot(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem) && !cacheItem.IsFrozen() && cacheItem.Index() >= 2);

    TOperationItemRef evictionPin(spaceOp, cacheOp.TryAcquireHotEvictionPin(cacheItem));
    if (!evictionPin) {
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
        const ui64 bytes = PayloadBytes(handle, expected.Kind());
        const bool keep = expected.IsPageKind() && expected.Refs() == 1 && cacheOp.WantsKeepCold(handle);
        if (keep) {
            Memory_.AddKeepColdOwnedBytes(bytes);
        }
        const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
        const THandleState desired =
            expected.WithState(keep ? EHandleState::KeepCold : EHandleState::Cold).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
        {
            cacheOp.CompleteHotEviction(handle, desired, bytes, evictionPin, coldVersion);
            return true;
        }
        if (keep) {
            Memory_.ReleaseKeepColdOwnedBytes(bytes);
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PromoteCold(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool force) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsCold()) {
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(expected.IsPageKind() && expected.Refs() != 0);
        const ui64 bytes = PageBytes(handle);
        TCollection* owner = cacheOp.PageCollection(handle);
        const bool protectedPage =
            force || !owner || owner->CacheMode_.load(std::memory_order_acquire) != ECacheMode::Regular;
        if (protectedPage) {
            Memory_.AddRetainedBytes(bytes);
        } else if (!Memory_.TryAddRetainedBytes(bytes)) {
            return false;
        }
        Memory_.Hot().AddBytes(bytes);
        Memory_.Hot().AddPage(EItemKind::Page);
        const THandleState desired = expected.WithState(EHandleState::Hot).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
            AdvanceColdMembership(handle);
            Memory_.Cold().RemoveBytes(bytes);
            Memory_.Cold().RemovePage(EItemKind::Page);
            Memory_.RemoveColdItem();
            if (owner) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
            }
            return true;
        }
        Memory_.Hot().RemoveBytes(bytes);
        Memory_.Hot().RemovePage(EItemKind::Page);
        Memory_.ReleaseRetainedBytes(bytes);
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ProcessHotAtMinimum(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    bool progress = false;
    // Give every level a share of the bounded work even when the lower rings are large or empty.
    for (ui32 level = 0; level < 4; ++level) {
        const TRingView hot = spaceOp.Hot(level);
        const ui64 attempts = Min<ui64>(hot.Capacity, SharedCacheTransitionWorkBatch / 4);
        for (ui64 attempt = 0; attempt < attempts; ++attempt) {
            const ui64 raw = hot.Pop();
            if (raw != 0) {
                cacheOp.ProcessHot(static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
                progress = true;
            }
        }
    }
    return progress;
}

#undef TOperationItemRef
#undef TSharedCacheItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

// Explicit instantiations of this file's Hot definitions for both trait variants.
template TCacheItem TSharedCacheImpl<TProdTraits>::PutHot(
    TSpaceOperation&, TSharedCacheImpl<TProdTraits>::EHotLevel, TCacheItem) noexcept;
template void TSharedCacheImpl<TProdTraits>::RouteHot(
    TSpaceOperation&, TSharedCacheImpl<TProdTraits>::EHotLevel, TCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::TakeHotFrequency(THandle&, TCacheItem, THandleState&, ui8&) noexcept;
template TSharedCacheItemRefImpl<TProdTraits> TSharedCacheImpl<TProdTraits>::TryAcquireHotEvictionPin(
    TSpaceOperation&, TCacheItem) noexcept;
template void TSharedCacheImpl<TProdTraits>::CompleteHotEviction(
    TSpaceOperation&, THandle&, THandleState, ui64, TOperationItemRef<TProdTraits>&, ui32) noexcept;
template void TSharedCacheImpl<TProdTraits>::TOperationView::CompleteHotEviction(
    THandle&, THandleState, ui64, TOperationItemRef<TProdTraits>&, ui32) noexcept;
template void TSharedCacheImpl<TProdTraits>::ReleaseKeepColdPin(
    TSpaceOperation&, TOperationItemRef<TProdTraits>&, ui32) noexcept;
template void TSharedCacheImpl<TProdTraits>::ProcessHot(
    TSpaceOperation&, TSharedCacheImpl<TProdTraits>::EHotLevel, TCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::EvictFromHot(TSpaceOperation&, ui32) noexcept;
template bool TSharedCacheImpl<TProdTraits>::EvictFromHot(TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheImpl<TProdTraits>::PromoteCold(TSpaceOperation&, TCacheItem, bool) noexcept;
template bool TSharedCacheImpl<TProdTraits>::ProcessHotAtMinimum(TSpaceOperation&) noexcept;

template TCacheItem TSharedCacheImpl<TTestTraits>::PutHot(
    TSpaceOperation&, TSharedCacheImpl<TTestTraits>::EHotLevel, TCacheItem) noexcept;
template void TSharedCacheImpl<TTestTraits>::RouteHot(
    TSpaceOperation&, TSharedCacheImpl<TTestTraits>::EHotLevel, TCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::TakeHotFrequency(THandle&, TCacheItem, THandleState&, ui8&) noexcept;
template TSharedCacheItemRefImpl<TTestTraits> TSharedCacheImpl<TTestTraits>::TryAcquireHotEvictionPin(
    TSpaceOperation&, TCacheItem) noexcept;
template void TSharedCacheImpl<TTestTraits>::CompleteHotEviction(
    TSpaceOperation&, THandle&, THandleState, ui64, TOperationItemRef<TTestTraits>&, ui32) noexcept;
template void TSharedCacheImpl<TTestTraits>::TOperationView::CompleteHotEviction(
    THandle&, THandleState, ui64, TOperationItemRef<TTestTraits>&, ui32) noexcept;
template void TSharedCacheImpl<TTestTraits>::ReleaseKeepColdPin(
    TSpaceOperation&, TOperationItemRef<TTestTraits>&, ui32) noexcept;
template void TSharedCacheImpl<TTestTraits>::ProcessHot(
    TSpaceOperation&, TSharedCacheImpl<TTestTraits>::EHotLevel, TCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::EvictFromHot(TSpaceOperation&, ui32) noexcept;
template bool TSharedCacheImpl<TTestTraits>::EvictFromHot(TSpaceOperation&, TCacheItem) noexcept;
template bool TSharedCacheImpl<TTestTraits>::PromoteCold(TSpaceOperation&, TCacheItem, bool) noexcept;
template bool TSharedCacheImpl<TTestTraits>::ProcessHotAtMinimum(TSpaceOperation&) noexcept;

} // namespace NKikimr::NSharedCache
