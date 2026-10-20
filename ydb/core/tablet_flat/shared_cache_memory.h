#pragma once

#include "shared_cache_item.h"

#include <util/generic/algorithm.h>
#include <util/generic/ylimits.h>
#include <util/system/yassert.h>

#include <atomic>

namespace NKikimr::NSharedCache {

class TCacheMemory {
public:
    // State transitions can overtake category accounting, temporarily making these estimates negative.
    class TCategoryUsage {
    public:
        i64 BytesEstimate() const noexcept {
            return Bytes_.load(std::memory_order_relaxed);
        }

        i64 PagesEstimate() const noexcept {
            return Pages_.load(std::memory_order_relaxed);
        }

        ui64 Bytes() const noexcept {
            return static_cast<ui64>(Max<i64>(BytesEstimate(), 0));
        }

        ui64 Pages() const noexcept {
            return static_cast<ui64>(Max<i64>(PagesEstimate(), 0));
        }

        void AddBytes(i64 bytes) noexcept {
            Bytes_.fetch_add(bytes, std::memory_order_relaxed);
        }

        void RemoveBytes(i64 bytes) noexcept {
            Bytes_.fetch_sub(bytes, std::memory_order_relaxed);
        }

        void AddPage(EItemKind kind) noexcept {
            if (kind == EItemKind::Page) {
                Pages_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        void RemovePage(EItemKind kind) noexcept {
            if (kind == EItemKind::Page) {
                Pages_.fetch_sub(1, std::memory_order_relaxed);
            }
        }

        void TransferBytesTo(TCategoryUsage& destination, ui64 bytes) noexcept {
            const i64 estimatedBytes = static_cast<i64>(bytes);
            destination.Bytes_.fetch_add(estimatedBytes, std::memory_order_relaxed);
            Bytes_.fetch_sub(estimatedBytes, std::memory_order_relaxed);
        }

        void TransferPageTo(TCategoryUsage& destination, EItemKind kind) noexcept {
            if (kind != EItemKind::Page) {
                return;
            }
            destination.Pages_.fetch_add(1, std::memory_order_relaxed);
            Pages_.fetch_sub(1, std::memory_order_relaxed);
        }

    private:
        std::atomic<i64> Bytes_{ 0 };
        std::atomic<i64> Pages_{ 0 };
    };

    TCacheMemory(ui64 staticBytes, ui64 initialLimit, ui64 reservationLimit) noexcept;

    TCategoryUsage& Hot() noexcept {
        return Hot_;
    }

    const TCategoryUsage& Hot() const noexcept {
        return Hot_;
    }

    TCategoryUsage& Cold() noexcept {
        return Cold_;
    }

    const TCategoryUsage& Cold() const noexcept {
        return Cold_;
    }

    TCategoryUsage& KeepCold() noexcept {
        return KeepCold_;
    }

    const TCategoryUsage& KeepCold() const noexcept {
        return KeepCold_;
    }

    TCategoryUsage& Sticky() noexcept {
        return Sticky_;
    }

    const TCategoryUsage& Sticky() const noexcept {
        return Sticky_;
    }

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

    ui64 RetainedBytes() const noexcept {
        return RetainedBytes_.load(std::memory_order_relaxed);
    }

    ui64 StickyOwnedBytes() const noexcept {
        return StickyOwnedBytes_.load(std::memory_order_relaxed);
    }

    ui64 KeepColdOwnedBytes() const noexcept {
        return KeepColdOwnedBytes_.load(std::memory_order_relaxed);
    }

    bool TryAddRetainedBytes(ui64 bytes) noexcept {
        ui64 retained = RetainedBytes_.load(std::memory_order_relaxed);
        for (;;) {
            if (retained + bytes > SoftLimit()) {
                return false;
            }
            if (RetainedBytes_.compare_exchange_weak(
                    retained, retained + bytes, std::memory_order_relaxed, std::memory_order_relaxed)) {
                return true;
            }
        }
    }

    void AddRetainedBytes(ui64 bytes) noexcept {
        RetainedBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void ReleaseRetainedBytes(ui64 bytes) noexcept {
        SubtractExactBytes(RetainedBytes_, bytes);
    }

    void AddStickyOwnedBytes(ui64 bytes) noexcept {
        StickyOwnedBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    // Sticky ownership counts page payloads; collection records contribute only to the resident-state estimates.
    void ReleaseStickyOwnedBytes(EItemKind kind, ui64 bytes) noexcept {
        if (kind == EItemKind::Page) {
            SubtractExactBytes(StickyOwnedBytes_, bytes);
        }
    }

    void AddKeepColdOwnedBytes(ui64 bytes) noexcept {
        KeepColdOwnedBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void ReleaseKeepColdOwnedBytes(ui64 bytes) noexcept {
        SubtractExactBytes(KeepColdOwnedBytes_, bytes);
    }

    ui64 ColdReclaimableBytes() const noexcept {
        return static_cast<ui64>(Max<i64>(ColdReclaimableBytes_.load(std::memory_order_relaxed), 0));
    }

    ui64 ColdReclaimableItems() const noexcept {
        return static_cast<ui64>(Max<i64>(0, ColdReclaimableItems_.load(std::memory_order_relaxed)));
    }

    // Returns true when the transition makes the item reclaimable.
    bool UpdateColdReclaimable(ui64 payloadBytes, THandleState before, THandleState after) noexcept {
        const auto reclaimable = [](THandleState state) {
            return state.IsCold() && state.IsStickyNoneField() && state.Refs() == 0;
        };
        const bool wasReclaimable = reclaimable(before);
        const bool isReclaimable = reclaimable(after);
        if (wasReclaimable == isReclaimable) {
            return false;
        }
        const i64 bytes = static_cast<i64>(payloadBytes);
        ColdReclaimableBytes_.fetch_add(isReclaimable ? bytes : -bytes, std::memory_order_relaxed);
        ColdReclaimableItems_.fetch_add(isReclaimable ? 1 : -1, std::memory_order_relaxed);
        return isReclaimable;
    }

    void AddColdItem() noexcept {
        ColdItems_.fetch_add(1, std::memory_order_relaxed);
    }

    void RemoveColdItem() noexcept {
        ColdItems_.fetch_sub(1, std::memory_order_relaxed);
    }

    ui64 ColdRingEntries() const noexcept {
        return ColdRingEntries_.load(std::memory_order_relaxed);
    }

    void AddColdRingEntry() noexcept {
        ColdRingEntries_.fetch_add(1, std::memory_order_relaxed);
    }

    void RemoveColdRingEntry() noexcept {
        const ui64 previous = ColdRingEntries_.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
    }

    ui64 Collections() const noexcept {
        return Collections_.load(std::memory_order_relaxed);
    }

    ui64 CollectionBytes() const noexcept {
        return CollectionBytes_.load(std::memory_order_relaxed);
    }

    void AddCollection(ui64 bytes) noexcept {
        CollectionBytes_.fetch_add(bytes, std::memory_order_relaxed);
        Collections_.fetch_add(1, std::memory_order_relaxed);
    }

    void RemoveCollection(ui64 bytes) noexcept {
        const ui64 previous = Collections_.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
        SubtractExactBytes(CollectionBytes_, bytes);
    }

    ui64 KeepResidentPageBytes() const noexcept {
        return KeepResidentPageBytes_.load(std::memory_order_relaxed);
    }

    ui64 KeepActivePageBytes() const noexcept {
        return KeepActivePageBytes_.load(std::memory_order_relaxed);
    }

    void UpdateKeepResidentPageBytes(ui64 previous, ui64 current) noexcept {
        UpdateAggregate(KeepResidentPageBytes_, previous, current);
    }

    void UpdateKeepActivePageBytes(ui64 previous, ui64 current) noexcept {
        UpdateAggregate(KeepActivePageBytes_, previous, current);
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

    static void UpdateAggregate(std::atomic<ui64>& total, ui64 previous, ui64 current) noexcept {
        if (current > previous) {
            total.fetch_add(current - previous, std::memory_order_relaxed);
        } else if (current < previous) {
            SubtractExactBytes(total, previous - current);
        }
    }

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
    std::atomic<ui64> RetainedBytes_{ 0 };
    std::atomic<ui64> StickyOwnedBytes_{ 0 };
    std::atomic<ui64> KeepColdOwnedBytes_{ 0 };
    TCategoryUsage Hot_;
    TCategoryUsage Cold_; // All Cold storage, including held and retiring payloads
    TCategoryUsage KeepCold_;
    TCategoryUsage Sticky_;
    std::atomic<i64> ColdReclaimableBytes_{ 0 }; // Unreferenced ordinary Cold payloads
    std::atomic<i64> ColdReclaimableItems_{ 0 }; // Unreferenced ordinary Cold items
    std::atomic<i64> ColdItems_{ 0 }; // All Cold-state items, including held items
    std::atomic<ui64> ColdRingEntries_{ 0 }; // Occupied ring slots, including stale entries until popped
    std::atomic<ui64> Collections_{ 0 };
    std::atomic<ui64> CollectionBytes_{ 0 };
    std::atomic<ui64> KeepResidentPageBytes_{ 0 };
    std::atomic<ui64> KeepActivePageBytes_{ 0 };
    std::atomic<ui64> CurrentLimit_{ 0 };
    std::atomic<ui64> SoftLimitOverride_{ Max<ui64>() };
    std::atomic<ui64> SoftLimit_{ 0 };
    std::atomic<ui64> HardLimit_{ 0 };
    ui64 ReservationLimit_ = 0;
};

} // namespace NKikimr::NSharedCache
