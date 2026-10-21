#include "shared_cache_eviction.h"
#include "shared_cache.h"

#include <cmath>

namespace NKikimr::NSharedCache {

template <class TTraits>
bool TCacheEvictionServices<TTraits>::DemoteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept {
    return Cache_.DemoteKeepCold(spaceOp, item);
}

template <class TTraits>
void TCacheEvictionServices<TTraits>::WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem item) noexcept {
    Cache_.WithdrawKeepColdEntry(spaceOp, item);
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
        if (Services_.DemoteKeepCold(spaceOp, coldItem)) {
            progress = true;
        } else {
            const ui64 keepColdBytes = Memory_.KeepColdOwnedBytes();
            Services_.WithdrawKeepColdEntry(spaceOp, coldItem);
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

template class TCacheEvictionServices<TProdTraits>;
template class TCacheEvictionServices<TTestTraits>;
template class TCacheEviction<TProdTraits>;
template class TCacheEviction<TTestTraits>;

} // namespace NKikimr::NSharedCache
