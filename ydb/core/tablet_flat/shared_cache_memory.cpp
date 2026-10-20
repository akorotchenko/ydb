#include "shared_cache_memory.h"

#include <util/generic/algorithm.h>

#include <cmath>

namespace NKikimr::NSharedCache {

TCacheMemory::TCacheMemory(ui64 staticBytes, ui64 initialLimit, ui64 reservationLimit) noexcept
    : StaticBytes_(staticBytes)
    , OverallUsage_(staticBytes)
    , CurrentLimit_(initialLimit)
    , HardLimit_(initialLimit)
    , ReservationLimit_(reservationLimit)
{}

ui64 TCacheMemory::CalculateSoftLimit(ui64 currentLimit, double gapFraction, ui64 minimumGap) noexcept {
    Y_DEBUG_ABORT_UNLESS(gapFraction >= 0.0 && gapFraction <= 1.0);
    const ui64 fractionGap = gapFraction == 1.0
                                 ? currentLimit
                                 : static_cast<ui64>(std::ceil(static_cast<double>(currentLimit) * gapFraction));
    const ui64 gap = Min(currentLimit, Max(minimumGap, fractionGap));
    return currentLimit - gap;
}

ui64 TCacheMemory::RefreshSoftLimit(double gapFraction, ui64 minimumGap) noexcept {
    const ui64 currentLimit = CurrentLimit_.load(std::memory_order_relaxed);
    const ui64 configured = SoftLimitOverride_.load(std::memory_order_acquire);
    const bool hasOverride = configured != Max<ui64>();
    const ui64 softLimit =
        hasOverride ? Min(configured, currentLimit) : CalculateSoftLimit(currentLimit, gapFraction, minimumGap);
    SoftLimit_.store(softLimit, std::memory_order_relaxed);
    return softLimit;
}

void TCacheMemory::PrepareStaticResize(ui64 bytes) noexcept {
    Y_DEBUG_ABORT_UNLESS(StaticDeltaBytes_.load(std::memory_order_relaxed) == 0);
    StaticDeltaBytes_.fetch_add(bytes, std::memory_order_relaxed);
    OverallUsage_.fetch_add(bytes, std::memory_order_relaxed);
}

void TCacheMemory::CancelStaticResize(ui64 bytes) noexcept {
    SubtractExactBytes(StaticDeltaBytes_, bytes);
    SubtractExactBytes(OverallUsage_, bytes);
}

void TCacheMemory::CommitStaticResize(ui64 oldStaticBytes, ui64 targetStaticBytes) noexcept {
    if (targetStaticBytes > oldStaticBytes) {
        const ui64 delta = targetStaticBytes - oldStaticBytes;
        StaticBytes_.fetch_add(delta, std::memory_order_relaxed);
        SubtractExactBytes(StaticDeltaBytes_, delta);
    } else {
        const ui64 delta = oldStaticBytes - targetStaticBytes;
        SubtractExactBytes(StaticBytes_, delta);
        SubtractExactBytes(OverallUsage_, delta);
    }
}

} // namespace NKikimr::NSharedCache
