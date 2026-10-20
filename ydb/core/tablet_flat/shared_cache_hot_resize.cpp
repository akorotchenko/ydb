#include "shared_cache.h"

#include <cmath>

namespace NKikimr::NSharedCache {

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>

SHARED_CACHE_TEMPLATE
bool TSharedCache::DrainHotResize() noexcept {
    if (auto hotResize = HotResize_.TryLock()) {
        auto cacheOp = BeginOperation();
        return cacheOp.DrainHotResize();
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DrainHotResize(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    bool progress = false;
    while (HotResize_.Phase() == EHotResizePhase::Cut) {
        ui32 level;
        ui64 raw;
        if (!Space_->DrainHotResize(HotResize_, level, raw)) {
            break;
        }
        progress = true;
        if (raw != 0) {
            cacheOp.ProcessHot(static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
        }
    }
    return progress;
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::MinimumHotSlots(const TSpaceOperation& spaceOp, bool underPressure) const noexcept {
    const ui64 hotPages = HotPages();
    const ui64 hotBytes = LoadEstimatedBytes(HotBytes_);
    const ui64 meanPageBytes = hotPages ? Max<ui64>(1, hotBytes / hotPages)
                                        : AccountedPageBytes(Space_->ReservedConfiguration().ExpectedPageSize);
    const ui64 hotMinBytes =
        FractionCeil(EvictableByteBudget(), underPressure ? Policy_.HotMinUnderPressure : Policy_.HotMin);
    const ui64 byteMinimumSlots = hotMinBytes / meanPageBytes + (hotMinBytes % meanPageBytes != 0);
    return static_cast<ui32>(
        Min<ui64>(spaceOp.View().HotSlotCount, Max<ui64>(Policy_.MinHotSlotsUnderPressure, byteMinimumSlots)));
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::ShrinkHotTarget(
    const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step, bool underPressure) const noexcept {
    const ui32 minimum = MinimumHotSlots(spaceOp, underPressure);
    if (effectiveHotSlots <= minimum) {
        return 0;
    }
    return effectiveHotSlots - minimum > step ? effectiveHotSlots - step : minimum;
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::GrowHotTarget(const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step) const noexcept {
    const ui32 maximum = static_cast<ui32>(spaceOp.View().HotSlotCount);
    const ui32 growthStep = Max<ui32>(1, step / 2);
    const ui64 grownHotSlots = ui64(effectiveHotSlots) + growthStep;
    if (grownHotSlots < maximum) {
        return static_cast<ui32>(grownHotSlots);
    }
    return effectiveHotSlots < maximum ? maximum : 0;
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::HotResizeStep(ui32 effectiveHotSlots) const noexcept {
    return Max(Policy_.MinResizeStep, static_cast<ui32>(std::ceil(effectiveHotSlots * Policy_.ResizeStep)));
}

#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

// Hot resize members are instantiated here; other cache members live in their respective .cpp files.
template bool TSharedCacheImpl<TProdTraits>::DrainHotResize() noexcept;
template bool TSharedCacheImpl<TProdTraits>::DrainHotResize(TSpaceOperation&) noexcept;
template ui32 TSharedCacheImpl<TProdTraits>::MinimumHotSlots(const TSpaceOperation&, bool) const noexcept;
template ui32 TSharedCacheImpl<TProdTraits>::ShrinkHotTarget(const TSpaceOperation&, ui32, ui32, bool) const noexcept;
template ui32 TSharedCacheImpl<TProdTraits>::GrowHotTarget(const TSpaceOperation&, ui32, ui32) const noexcept;
template ui32 TSharedCacheImpl<TProdTraits>::HotResizeStep(ui32) const noexcept;

template bool TSharedCacheImpl<TTestTraits>::DrainHotResize() noexcept;
template bool TSharedCacheImpl<TTestTraits>::DrainHotResize(TSpaceOperation&) noexcept;
template ui32 TSharedCacheImpl<TTestTraits>::MinimumHotSlots(const TSpaceOperation&, bool) const noexcept;
template ui32 TSharedCacheImpl<TTestTraits>::ShrinkHotTarget(const TSpaceOperation&, ui32, ui32, bool) const noexcept;
template ui32 TSharedCacheImpl<TTestTraits>::GrowHotTarget(const TSpaceOperation&, ui32, ui32) const noexcept;
template ui32 TSharedCacheImpl<TTestTraits>::HotResizeStep(ui32) const noexcept;

} // namespace NKikimr::NSharedCache
