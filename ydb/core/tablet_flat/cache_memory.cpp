#include "cache_memory.h"

#include <util/generic/algorithm.h>

#include <cmath>

namespace NKikimr::NSharedCache {

TKeepReservation::~TKeepReservation() {
    Reset();
}

TKeepReservation& TKeepReservation::operator=(TKeepReservation&& other) noexcept {
    if (this != &other) {
        Reset();
        Memory_ = std::exchange(other.Memory_, nullptr);
        Bytes_ = other.Bytes_;
        Pending_ = other.Pending_;
    }
    return *this;
}

void TKeepReservation::MarkPending() noexcept {
    if (Memory_ && !Pending_) {
        auto& pending = Memory_->Tier(ECacheTier::Keep).Pending;
        pending.AddBytes(static_cast<i64>(Bytes_));
        pending.AddPage(EItemKind::Page);
        Pending_ = true;
    }
}

void TKeepReservation::Commit() noexcept {
    if (Memory_ && Pending_) {
        auto& pending = Memory_->Tier(ECacheTier::Keep).Pending;
        pending.RemoveBytes(static_cast<i64>(Bytes_));
        pending.RemovePage(EItemKind::Page);
    }
    Memory_ = nullptr;
    Pending_ = false;
}

void TKeepReservation::Reset() noexcept {
    if (Memory_) {
        Memory_->ReleaseKeep(Bytes_);
        Commit();
    }
}

bool TCacheMemory::TryAddBounded(std::atomic<ui64>& counter, ui64 amount, ui64 limit) noexcept {
    ui64 used = counter.load(std::memory_order_relaxed);
    for (;;) {
        if (used > limit || amount > limit - used) {
            return false;
        }
        if (counter.compare_exchange_weak(used, used + amount, std::memory_order_relaxed, std::memory_order_relaxed)) {
            return true;
        }
    }
}

bool TCacheMemory::PeekKeepReserve(ui64 bytes, ui64 pages, ui64 byteLimit) const noexcept {
    const ui64 usedBytes = KeepOwnedBytes();
    const ui64 usedPages = KeepOwnedPages();
    const ui64 pageLimit = KeepLimitPages();
    return usedBytes <= byteLimit && bytes <= byteLimit - usedBytes && usedPages <= pageLimit &&
           pages <= pageLimit - usedPages;
}

TKeepReservation TCacheMemory::TryReserveKeep(ui64 bytes, ui64 byteLimit) noexcept {
    if (!TryAddBounded(KeepOwnedBytes_, bytes, byteLimit)) {
        return {};
    }
    if (!TryAddBounded(KeepOwnedPages_, 1, KeepLimitPages())) {
        SubtractExactBytes(KeepOwnedBytes_, bytes);
        return {};
    }
    return TKeepReservation(*this, bytes);
}

void TCacheMemory::ReleaseKeep(ui64 bytes) noexcept {
    SubtractExactBytes(KeepOwnedBytes_, bytes);
    SubtractExactBytes(KeepOwnedPages_, 1);
}

TCacheMemory::TCategoryUsage& TCacheMemory::ResidentCategory(EHandleState state) noexcept {
    switch (state) {
        case EHandleState::Hot:
            return Hot(ECacheTier::Regular);
        case EHandleState::Cold:
            return Cold(ECacheTier::Regular);
        case EHandleState::KeepHot:
            return Hot(ECacheTier::Keep);
        case EHandleState::KeepCold:
            return Cold(ECacheTier::Keep);
        case EHandleState::Sticky:
            return Sticky();
        case EHandleState::Tombstone:
            return Retired();
        default:
            Y_ABORT("Invalid resident payload category");
    }
}

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
