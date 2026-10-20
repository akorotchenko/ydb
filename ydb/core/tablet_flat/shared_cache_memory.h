#pragma once

#include "shared_cache_item.h"

#include <util/generic/ylimits.h>
#include <util/system/yassert.h>

#include <atomic>

namespace NKikimr::NSharedCache {

class TCacheMemory {
public:
    TCacheMemory(ui64 staticBytes, ui64 initialLimit, ui64 reservationLimit) noexcept;

    ui64 CurrentLimit() const noexcept {
        return CurrentLimit_.load(std::memory_order_relaxed);
    }

    ui64 SoftLimit() const noexcept {
        return SoftLimit_.load(std::memory_order_relaxed);
    }

    ui64 HardLimit() const noexcept {
        return HardLimit_.load(std::memory_order_relaxed);
    }

    ui64 ReservationLimit() const noexcept {
        return ReservationLimit_;
    }

    void SetCurrentLimit(ui64 limit) noexcept {
        CurrentLimit_.store(limit, std::memory_order_relaxed);
    }

    void SetHardLimit(ui64 limit) noexcept {
        HardLimit_.store(limit, std::memory_order_relaxed);
    }

    void SetSoftLimitOverride(ui64 limit) noexcept {
        SoftLimitOverride_.store(limit, std::memory_order_release);
    }

    ui64 RefreshSoftLimit(double gapFraction, ui64 minimumGap) noexcept;

    ui64 ResidentBytes() const noexcept {
        return ResidentBytes_.load(std::memory_order_relaxed);
    }

    ui64 StaticBytes() const noexcept {
        return StaticBytes_.load(std::memory_order_relaxed);
    }

    ui64 StaticDeltaBytes() const noexcept {
        return StaticDeltaBytes_.load(std::memory_order_relaxed);
    }

    ui64 OverallUsage() const noexcept {
        return OverallUsage_.load(std::memory_order_relaxed);
    }

    ui64 PayloadUsage() const noexcept {
        return PageUsage_.load(std::memory_order_relaxed);
    }

    ui64 ReservedBytes() const noexcept {
        return ReservedBytes_.load(std::memory_order_relaxed);
    }

    ui64 ReservedPages() const noexcept {
        return ReservedPages_.load(std::memory_order_relaxed);
    }

    // The caller checks its current limit against usage before each attempt.
    bool TryReserve(ui64& usage, ui64 bytes, ui64 pages) noexcept {
        if (PageUsage_.compare_exchange_weak(
                usage, usage + bytes, std::memory_order_relaxed, std::memory_order_relaxed)) {
            OverallUsage_.fetch_add(bytes, std::memory_order_relaxed);
            ReservedBytes_.fetch_add(bytes, std::memory_order_relaxed);
            ReservedPages_.fetch_add(pages, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    void AccountWritten(ui64 bytes, ui64 pages) noexcept {
        if (bytes == 0 && pages == 0) {
            return;
        }
        // The writer already owns these buffers; transfer their charge before publishing native page items.
        PageUsage_.fetch_add(bytes, std::memory_order_relaxed);
        OverallUsage_.fetch_add(bytes, std::memory_order_relaxed);
        ReservedBytes_.fetch_add(bytes, std::memory_order_relaxed);
        ReservedPages_.fetch_add(pages, std::memory_order_relaxed);
    }

    void ReleaseReservation(ui64 bytes, ui64 pages) noexcept {
        SubtractExactBytes(ReservedBytes_, bytes);
        SubtractExactBytes(ReservedPages_, pages);
        SubtractExactBytes(OverallUsage_, bytes);
        SubtractExactBytes(PageUsage_, bytes);
    }

    void CompleteReservedPage() noexcept {
        SubtractExactBytes(ReservedPages_, 1);
    }

    void CompleteReservedBytes(ui64 bytes) noexcept {
        SubtractExactBytes(ReservedBytes_, bytes);
    }

    void AddResident(ui64 bytes) noexcept {
        ResidentBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void RemoveResident(ui64 bytes) noexcept {
        SubtractExactBytes(ResidentBytes_, bytes);
    }

    void ReleasePayloadCharge(ui64 bytes, EItemKind kind) noexcept {
        SubtractExactBytes(OverallUsage_, bytes);
        if (kind == EItemKind::Page) {
            SubtractExactBytes(PageUsage_, bytes);
        }
    }

    void PrepareStaticResize(ui64 bytes) noexcept;
    void CancelStaticResize(ui64 bytes) noexcept;
    void CommitStaticResize(ui64 oldStaticBytes, ui64 targetStaticBytes) noexcept;

private:
    static ui64 CalculateSoftLimit(ui64 currentLimit, double gapFraction, ui64 minimumGap) noexcept;

    static void SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept {
        const ui64 previous = counter.fetch_sub(bytes, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous >= bytes);
    }

private: // Data members
    std::atomic<ui64> ResidentBytes_{ 0 };
    std::atomic<ui64> StaticBytes_{ 0 };
    std::atomic<ui64> StaticDeltaBytes_{ 0 };
    std::atomic<ui64> OverallUsage_{ 0 };
    std::atomic<ui64> PageUsage_{ 0 };
    std::atomic<ui64> ReservedBytes_{ 0 };
    std::atomic<ui64> ReservedPages_{ 0 };
    std::atomic<ui64> CurrentLimit_{ 0 };
    std::atomic<ui64> SoftLimitOverride_{ Max<ui64>() };
    std::atomic<ui64> SoftLimit_{ 0 };
    std::atomic<ui64> HardLimit_{ 0 };
    ui64 ReservationLimit_ = 0;
};

} // namespace NKikimr::NSharedCache
