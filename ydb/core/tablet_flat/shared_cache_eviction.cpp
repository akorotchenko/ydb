#include "shared_cache_eviction.h"
#include "shared_cache.h"

#include <util/generic/cast.h>

#include <cmath>

namespace NKikimr::NSharedCache {

template <class TTraits>
ui64 TCacheEvictionServices<TTraits>::PageBytes(const THandle& handle) noexcept {
    return TSharedCacheImpl<TTraits>::PageBytes(handle);
}

template <class TTraits>
ui64 TCacheEvictionServices<TTraits>::PayloadBytes(const THandle& handle, EItemKind kind) noexcept {
    return TSharedCacheImpl<TTraits>::PayloadBytes(handle, kind);
}

template <class TTraits>
TSharedCacheItemRefImpl<TTraits> TCacheEvictionServices<TTraits>::TryAcquireHotEvictionPin(
    TSpaceOperation& spaceOp, TCacheItem item) noexcept {
    return Cache_.TryAcquireHotEvictionPin(spaceOp, item);
}

template <class TTraits>
ui32 TCacheEvictionServices<TTraits>::AdvanceColdMembership(THandle& handle) noexcept {
    return TSharedCacheImpl<TTraits>::AdvanceColdMembership(handle);
}

template <class TTraits>
bool TCacheEvictionServices<TTraits>::AdvanceColdMembership(
    THandle& handle, ui32 expectedVersion, bool keepColdQueued, ui32& newVersion) noexcept {
    return TSharedCacheImpl<TTraits>::AdvanceColdMembership(handle, expectedVersion, keepColdQueued, newVersion);
}

template <class TTraits>
TCollection* TCacheEvictionServices<TTraits>::ReleaseForColdDemotion(
    TSpaceOperation& spaceOp, TCacheItem item) noexcept {
    return Cache_.ReleaseItem(spaceOp, item, false);
}

template <class TTraits>
void TCacheEvictionServices<TTraits>::NotifyResourcesAvailable() noexcept {
    Cache_.NotifyResourcesAvailable();
}

template <class TTraits>
bool TCacheEvictionServices<TTraits>::EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    return Cache_.EnforceCurrentLimit(spaceOp, bytes);
}

template <class TTraits>
ui64 TCacheEviction<TTraits>::FractionCeil(ui64 value, double fraction) noexcept {
    Y_DEBUG_ABORT_UNLESS(fraction >= 0.0 && fraction <= 1.0);
    return fraction == 1.0 ? value : static_cast<ui64>(std::ceil(static_cast<double>(value) * fraction));
}

template <class TTraits>
bool TCacheEviction<TTraits>::UpdateKeepColdLimit(ui64 limit) noexcept {
    if (limit > Memory_.HardLimit()) {
        return false;
    }
    const ui64 previousMaximum = KeepColdMaxBytes_.exchange(limit, std::memory_order_relaxed);
    if (previousMaximum == limit) {
        return true;
    }
    ui64 current = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 growth = limit > previousMaximum ? limit - previousMaximum : 0;
        const ui64 target = Min(current, limit) + Min(growth, limit - Min(current, limit));
        if (KeepColdLimit_.compare_exchange_weak(current, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::ScheduleKeepColdSweep(ui64 slots) noexcept {
    KeepColdSweepRemaining_.store(slots, std::memory_order_release);
}

template <class TTraits>
bool TCacheEviction<TTraits>::SweepKeepCold(TSharedCacheSpace& space) noexcept {
    ui64 remaining = KeepColdSweepRemaining_.load(std::memory_order_acquire);
    while (remaining != 0) {
        const ui64 batch = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
        if (KeepColdSweepRemaining_.compare_exchange_weak(
                remaining, remaining - batch, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            auto spaceOp = space.BeginOperation();
            for (ui64 index = 0; index < batch; ++index) {
                const ui64 raw = spaceOp.KeepCold().Pop();
                if (raw != 0) {
                    RouteKeepCold(spaceOp, TColdCacheItem::FromRaw(raw));
                }
            }
            return true;
        }
    }
    return false;
}

template <class TTraits>
bool TCacheEviction<TTraits>::TrimKeepCold(TSpaceOperation& spaceOp) noexcept {
    const ui64 limit = KeepColdLimit();
    if (Memory_.KeepColdOwnedBytes() <= limit && !KeepColdTrimActive_.load(std::memory_order_relaxed))
    {
        return false;
    }
    if (!KeepColdTrimActive_.exchange(true, std::memory_order_acq_rel)) {
        KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
    }
    const ui64 lowWatermark = limit - (limit / 20 + (limit % 20 != 0));
    ui64 remaining = KeepColdTrimScanRemaining_.load(std::memory_order_acquire);
    ui64 attempts;
    for (;;) {
        if (remaining == 0) {
            KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
            remaining = spaceOp.KeepCold().Capacity;
        }
        attempts = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
        if (KeepColdTrimScanRemaining_.compare_exchange_weak(
                remaining, remaining - attempts, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            break;
        }
    }
    bool progress = false;
    for (ui64 index = 0; index < attempts && Memory_.KeepColdOwnedBytes() > lowWatermark; ++index)
    {
        const ui64 raw = spaceOp.KeepCold().Pop();
        if (raw == 0) {
            continue;
        }
        const TColdCacheItem coldItem = TColdCacheItem::FromRaw(raw);
        if (DemoteKeepCold(spaceOp, coldItem)) {
            progress = true;
        } else {
            const ui64 keepColdBytes = Memory_.KeepColdOwnedBytes();
            WithdrawKeepColdEntry(spaceOp, coldItem);
            // A referenced page cannot be demoted yet, but its entry is gone now: advance the membership so
            // that the page leaves KeepCold on its last release. The withdrawal helper also demotes it if the
            // last release raced with the membership advance.
            progress = progress || Memory_.KeepColdOwnedBytes() < keepColdBytes;
        }
    }
    if (Memory_.KeepColdOwnedBytes() <= lowWatermark) {
        KeepColdTrimActive_.store(false, std::memory_order_relaxed);
        KeepColdTrimScanRemaining_.store(0, std::memory_order_relaxed);
    } else if (progress) {
        KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_relaxed);
    }
    return progress || remaining > attempts;
}

template <class TTraits>
bool TCacheEviction<TTraits>::ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept {
    // Finish the previous FIFO trim before reducing the target again: sparse slots or held pages may delay it.
    if (KeepColdTrimActive_.load(std::memory_order_relaxed)) {
        return TrimKeepCold(spaceOp);
    }
    ui64 limit = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 occupied = Min(KeepColdLimit(), Memory_.KeepColdOwnedBytes());
        const ui64 step = Max<ui64>(1, FractionCeil(occupied, Policy_.ResizeStep));
        const ui64 target = occupied - Min(occupied, step);
        if (limit == target) {
            return TrimKeepCold(spaceOp);
        }
        if (KeepColdLimit_.compare_exchange_weak(limit, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            TrimKeepCold(spaceOp);
            return true;
        }
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::GrowKeepCold() noexcept {
    ui64 limit = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 budget = EvictableByteBudget();
        const ui64 maximum =
            Min(KeepColdMaxBytes_.load(std::memory_order_relaxed), budget - FractionCeil(budget, Policy_.HotMin));
        if (limit >= maximum) {
            return false;
        }
        const ui64 step = Max<ui64>(1, FractionCeil(maximum, Policy_.ResizeStep));
        const ui64 target = limit + Min(maximum - limit, step);
        if (KeepColdLimit_.compare_exchange_weak(limit, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    if (TCollection* collection = Collections_.BindOperation(spaceOp).PageCollection(page)) {
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
            TOperationItemRef<TTraits> pin(spaceOp, TCacheItem::Make(expected.Version(), coldItem.Index()));
            Traits_.Invoke(ESharedCacheHookPoint::AfterKeepColdDemoted, TCacheItem::FromRaw(coldItem.Raw()));
            const ui64 bytes = Services_.PageBytes(handle);
            Services_.AdvanceColdMembership(handle);
            Memory_.ReleaseRetainedBytes(bytes);
            Memory_.KeepCold().TransferBytesTo(Memory_.Cold(), bytes);
            Memory_.KeepCold().TransferPageTo(Memory_.Cold(), EItemKind::Page);
            Memory_.AddColdItem();
            Memory_.ReleaseKeepColdOwnedBytes(bytes);
            if (TCollection* owner = Collections_.BindOperation(spaceOp).PageCollection(handle)) {
                Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
            ReleaseColdDemotionPin(spaceOp, pin);
            return true;
        }
    }
}

template <class TTraits>
void TCacheEviction<TTraits>::ReleaseColdDemotionPin(
    TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& pin) noexcept {
    const TCacheItem cacheItem = pin.CacheItem();
    pin.Disarm();
    // A policy demotion must not readmit the page to KeepCold when releasing its temporary ref.
    if (TCollection* owner = Services_.ReleaseForColdDemotion(spaceOp, cacheItem)) {
        Collections_.BindOperation(spaceOp).DropOwnerReference(*owner);
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
    if (!Services_.AdvanceColdMembership(handle, coldItem.ColdVersion(), false, coldVersion)) {
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
    TOperationItemRef<TTraits> pin(
        spaceOp, TCacheItem::Make(THandleState::FromRaw(expectedRaw).Version(), coldItem.Index()));
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return;
    }
    if (!WantsKeepCold(spaceOp, handle)) {
        WithdrawKeepColdEntry(spaceOp, coldItem);
        return;
    }
    ui32 queuedVersion;
    if (!Services_.AdvanceColdMembership(handle, coldItem.ColdVersion(), true, queuedVersion)) {
        return;
    }
    const TColdCacheItem queuedItem = TColdCacheItem::Make(queuedVersion, coldItem.Index());
    const ui64 displaced = spaceOp.KeepCold().PutAndPop(queuedItem.Raw());
    Traits_.Invoke(ESharedCacheHookPoint::AfterKeepColdQueued, TCacheItem::FromRaw(queuedItem.Raw()));
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
    pin.Drop();
    TrimKeepCold(spaceOp);
}

template <class TTraits>
void TCacheEviction<TTraits>::ReleaseKeepColdPin(
    TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& pin, ui32 initialColdVersion) noexcept {
    const TCacheItem cacheItem = pin.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsKeepCold() || expected.Refs() != 1 ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != initialColdVersion ||
            IsKeepColdQueued(handle) || !WantsKeepCold(spaceOp, handle))
        {
            pin.Drop();
            return;
        }
        const THandleState desired = expected.DecrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            pin.Disarm();
            RouteKeepCold(spaceOp, TColdCacheItem::Make(initialColdVersion, cacheItem.Index()));
            Services_.NotifyResourcesAvailable();
            return;
        }
    }
}

template <class TTraits>
TCacheItem TCacheEviction<TTraits>::PutHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    const TRingExchangeResult exchange =
        spaceOp.Hot(ToUnderlying(level)).PutAndPopWithHook(cacheItem.Raw(), [&]() noexcept {
            Traits_.Invoke(ESharedCacheHookPoint::BeforeHotExchange, cacheItem);
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
    THandleState coldState, ui64 bytes, TOperationItemRef<TTraits>& pin, ui32 initialColdVersion) noexcept {
    Y_DEBUG_ABORT_UNLESS(coldState.Refs() > 0);
    Traits_.Invoke(ESharedCacheHookPoint::AfterHotEvictionPublished, pin.CacheItem());
    if (Memory_.UpdateColdReclaimable(bytes, coldState.WithState(EHandleState::Hot), coldState)) {
        Services_.NotifyResourcesAvailable();
    }
    if (coldState.IsKeepCold()) {
        Memory_.Hot().TransferBytesTo(Memory_.KeepCold(), bytes);
        Memory_.Hot().TransferPageTo(Memory_.KeepCold(), coldState.Kind());
    } else {
        Memory_.ReleaseRetainedBytes(bytes);
        Memory_.Hot().TransferBytesTo(Memory_.Cold(), bytes);
        Memory_.Hot().TransferPageTo(Memory_.Cold(), coldState.Kind());
        Memory_.AddColdItem();
        if (coldState.IsPageKind()) {
            if (TCollection* owner = Collections_.BindOperation(spaceOp).PageCollection(handle)) {
                Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
        }
    }
    ReleaseKeepColdPin(spaceOp, pin, initialColdVersion);
}

template <class TTraits>
void TCacheEviction<TTraits>::ProcessHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    for (;;) {
        TOperationItemRef<TTraits> evictionPin(spaceOp, Services_.TryAcquireHotEvictionPin(spaceOp, cacheItem));
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
                const ui64 bytes = Services_.PayloadBytes(handle, state.Kind());
                const bool keep = state.IsPageKind() && state.Refs() == 1 && WantsKeepCold(spaceOp, handle);
                if (keep) {
                    Memory_.AddKeepColdOwnedBytes(bytes);
                }
                const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
                const THandleState cold = state.WithState(keep ? EHandleState::KeepCold : EHandleState::Cold);
                if (handle.State.compare_exchange_strong(
                        hotRaw, cold.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
                {
                    CompleteHotEviction(spaceOp, handle, cold, bytes, evictionPin, coldVersion);
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

        evictionPin.Drop();
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

    TOperationItemRef<TTraits> evictionPin(spaceOp, Services_.TryAcquireHotEvictionPin(spaceOp, cacheItem));
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
        const ui64 bytes = Services_.PayloadBytes(handle, expected.Kind());
        const bool keep = expected.IsPageKind() && expected.Refs() == 1 && WantsKeepCold(spaceOp, handle);
        if (keep) {
            Memory_.AddKeepColdOwnedBytes(bytes);
        }
        const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
        const THandleState desired =
            expected.WithState(keep ? EHandleState::KeepCold : EHandleState::Cold).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
        {
            CompleteHotEviction(spaceOp, handle, desired, bytes, evictionPin, coldVersion);
            return true;
        }
        if (keep) {
            Memory_.ReleaseKeepColdOwnedBytes(bytes);
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
        const ui64 bytes = Services_.PageBytes(handle);
        TCollection* owner = Collections_.BindOperation(spaceOp).PageCollection(handle);
        const bool protectedPage = force || !owner || owner->GetCacheMode() != ECacheMode::Regular;
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
            Services_.AdvanceColdMembership(handle);
            Memory_.Cold().RemoveBytes(bytes);
            Memory_.Cold().RemovePage(EItemKind::Page);
            Memory_.RemoveColdItem();
            if (owner) {
                Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
            }
            return true;
        }
        Memory_.Hot().RemoveBytes(bytes);
        Memory_.Hot().RemovePage(EItemKind::Page);
        Memory_.ReleaseRetainedBytes(bytes);
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
bool TCacheEviction<TTraits>::ExceedsLimit(ui64 usage, ui64 limit, ui64 bytes) noexcept {
    return usage + bytes > limit;
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
        if (Table_.TryBeginErase(spaceOp, cacheItem, expected, ownerRef.Get())) {
            // The tombstone owner now pins the page and its collection during accounting and notification.
            pageCollection =
                expected.IsPageKind() ? Collections_.BindOperation(spaceOp).PageCollection(handle) : nullptr;
            const ui64 bytes = Services_.PayloadBytes(handle, expected.Kind());
            const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
            if (Memory_.UpdateColdReclaimable(bytes, expected, desired)) {
                Services_.NotifyResourcesAvailable();
            }
            if (expected.IsKeepCold()) {
                Memory_.ReleaseRetainedBytes(bytes);
                Memory_.KeepCold().TransferBytesTo(Memory_.Cold(), bytes);
                Memory_.KeepCold().TransferPageTo(Memory_.Cold(), expected.Kind());
                Memory_.AddColdItem();
                Memory_.ReleaseKeepColdOwnedBytes(bytes);
                if (pageCollection) {
                    Collections_.AddActivePageBytes(
                        *pageCollection, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
                }
            }
            Traits_.Invoke(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, TCacheItem::FromRaw(coldItem.Raw()));
            break;
        }
    }

    const THandleState coldState = expected;
    // Stale ring entries may name reattached collections; check only after claiming an eviction owner.
    if (coldState.IsCollectionKind()) {
        Y_DEBUG_ABORT_UNLESS(
            !handle.Body.Collection || Collections_.IsUnregisteredAndUnreferenced(*handle.Body.Collection));
    }
    const TCacheItem cacheItem = ownerRef.CacheItem();
    const TSharedCacheKey key = LoadSharedCacheKey(handle, coldState.Kind());
    if (coldState.IsPageKind() && pageCollection && pageCollection->GetCacheMode() == ECacheMode::TryKeepInMemory &&
        pageCollection->IsKeepAllowedPage(handle.Metadata.Page.Type))
    {
        Traits_.NotifyKeepPageEviction(pageCollection->Id(), pageCollection->CacheItem,
            pageCollection->KeepGeneration(),
            NTable::NPage::TPageLocation(NTable::NPage::TPageOffset::FromRaw(key.Word(1)),
                handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type,
                handle.Metadata.Page.Crc32));
    }
    Services_.AdvanceColdMembership(handle);
    Table_.FinishErase(spaceOp, cacheItem, key);
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

    Memory_.AddColdRingEntry();
    Traits_.Invoke(ESharedCacheHookPoint::BeforeColdExchange, TCacheItem::FromRaw(coldItem.Raw()));
    const TColdCacheItem displaced = spaceOp.PutCold(coldItem);
    if (!displaced.IsNull()) {
        Memory_.RemoveColdRingEntry();
        EraseCold(spaceOp, displaced);
    }
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReclaimCold(TSpaceOperation& spaceOp) noexcept {
    const TRingView cold = spaceOp.Cold();
    for (ui64 attempt = 0; attempt < cold.Capacity; ++attempt) {
        if (Memory_.ColdRingEntries() == 0) {
            return false;
        }
        Traits_.Invoke(ESharedCacheHookPoint::BeforeColdRingPop, TCacheItem{});
        const ui64 rawItem = cold.Pop();
        if (rawItem != 0) {
            Memory_.RemoveColdRingEntry();
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
    for (ui64 attempt = 0; attempt < keep.Capacity && Memory_.KeepColdOwnedBytes() != 0; ++attempt) {
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
    for (ui64 attempt = 0; attempt < keep.Capacity && Memory_.KeepColdOwnedBytes() != 0; ++attempt) {
        const ui64 raw = keep.Pop();
        if (raw == 0) {
            continue;
        }
        const TColdCacheItem item = TColdCacheItem::FromRaw(raw);
        if (!MatchesKeepColdEntry(spaceOp, item)) {
            continue;
        }
        Traits_.Invoke(ESharedCacheHookPoint::AfterKeepColdEntryMatched, TCacheItem::FromRaw(item.Raw()));
        if (EraseCold(spaceOp, item)) {
            return true;
        }
        WithdrawKeepColdEntry(spaceOp, item);
    }
    return false;
}

template <class TTraits>
bool TCacheEviction<TTraits>::Reclaim(TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    if (ReclaimCold(spaceOp) || ReclaimKeepCold(spaceOp)) {
        return true;
    }
    // Cold is empty, so only moving the hot/cold boundary can make room, and that is EnforceCurrentLimit's job.
    return Services_.EnforceCurrentLimit(spaceOp, bytes);
}

template <class TTraits>
bool TCacheEviction<TTraits>::ReclaimToLimits(
    TSpaceOperation& spaceOp, ui64 softLimit, ui32 step, ui64 bytes) noexcept {
    bool progress = false;
    for (ui32 attempt = 0; attempt < step; ++attempt) {
        if (ExceedsLimit(Memory_.PayloadUsage(), Memory_.CurrentLimit(), bytes)) {
            if (!ReclaimCold(spaceOp) && !ReclaimKeepCold(spaceOp)) {
                break;
            }
        } else if (softLimit == 0 && Memory_.ColdReclaimableItems() != 0) {
            if (!ReclaimCold(spaceOp)) {
                break;
            }
        } else if (Memory_.RetainedBytes() > softLimit) {
            if (!DemoteKeepColdForSoftLimit(spaceOp)) {
                break;
            }
        } else {
            break;
        }
        progress = true;
    }
    return progress;
}

template class TCacheEvictionServices<TProdTraits>;
template class TCacheEvictionServices<TTestTraits>;
template class TCacheEviction<TProdTraits>;
template class TCacheEviction<TTestTraits>;

} // namespace NKikimr::NSharedCache
