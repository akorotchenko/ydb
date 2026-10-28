#pragma once

#include "cache_fwd.h"
#include "cache_budget.h"
#include "cache_item.h"

#include <util/generic/algorithm.h>
#include <util/generic/ylimits.h>
#include <util/system/yassert.h>

#include <atomic>
#include <array>

namespace NKikimr::NSharedCache {

class TCacheMemory {
public:
    struct TStats {
        ui64 RequestedPages = 0;
        ui64 RequestedBytes = 0;
        ui64 HitPages = 0;
        ui64 HitBytes = 0;
        ui64 MissPages = 0;
        ui64 MissBytes = 0;

        void AddAdmission(ECacheResultStatus status, ui64 bytes) noexcept;
    };

    TStats Stats() const noexcept {
        return {
            .RequestedPages = RequestedPages_.load(std::memory_order_relaxed),
            .RequestedBytes = RequestedBytes_.load(std::memory_order_relaxed),
            .HitPages = HitPages_.load(std::memory_order_relaxed),
            .HitBytes = HitBytes_.load(std::memory_order_relaxed),
            .MissPages = MissPages_.load(std::memory_order_relaxed),
            .MissBytes = MissBytes_.load(std::memory_order_relaxed),
        };
    }

    void AddStats(const TStats& stats) noexcept;

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

        void AddPage(EItemKind kind) noexcept;

        void RemovePage(EItemKind kind) noexcept;

        void TransferBytesTo(TCategoryUsage& destination, ui64 bytes) noexcept {
            const i64 estimatedBytes = static_cast<i64>(bytes);
            destination.Bytes_.fetch_add(estimatedBytes, std::memory_order_relaxed);
            Bytes_.fetch_sub(estimatedBytes, std::memory_order_relaxed);
        }

        void TransferPageTo(TCategoryUsage& destination, EItemKind kind) noexcept;

    private:
        std::atomic<i64> Bytes_{ 0 };
        std::atomic<i64> Pages_{ 0 };
    };

    Y_FORCE_INLINE TCacheMemory(ui64 staticBytes, ui64 initialLimit, ui64 reservationLimit) noexcept
        : StaticBytes_(staticBytes)
        , OverallUsage_(staticBytes)
        , CurrentLimit_(initialLimit)
        , HardLimit_(initialLimit)
        , ReservationLimit_(reservationLimit)
    {}

    struct TTierUsage {
        TCategoryUsage Hot;
        TCategoryUsage Cold;
        TCategoryUsage Pending;
        std::atomic<i64> ReclaimableBytes{ 0 };
        std::atomic<i64> ReclaimableItems{ 0 };
    };

    TTierUsage& Tier(ECacheTier tier) noexcept {
        return Tiers_[tier];
    }

    const TTierUsage& Tier(ECacheTier tier) const noexcept {
        return Tiers_[tier];
    }

    TCategoryUsage& Hot(ECacheTier tier = ECacheTier::Regular) noexcept {
        return Tier(tier).Hot;
    }

    const TCategoryUsage& Hot(ECacheTier tier = ECacheTier::Regular) const noexcept {
        return Tier(tier).Hot;
    }

    TCategoryUsage& Cold(ECacheTier tier = ECacheTier::Regular) noexcept {
        return Tier(tier).Cold;
    }

    const TCategoryUsage& Cold(ECacheTier tier = ECacheTier::Regular) const noexcept {
        return Tier(tier).Cold;
    }

    TCategoryUsage& Retired() noexcept {
        return Retired_;
    }

    const TCategoryUsage& Retired() const noexcept {
        return Retired_;
    }

    TCategoryUsage& ResidentCategory(EHandleState state) noexcept;

    ui64 ReadyBytes(ECacheTier tier) const noexcept {
        return Hot(tier).Bytes() + Cold(tier).Bytes();
    }

    ui64 ReadyPages(ECacheTier tier) const noexcept {
        return Hot(tier).Pages() + Cold(tier).Pages();
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

    void SetKeepPageLimit(ui64 pages) noexcept {
        KeepLimitPages_.store(pages, std::memory_order_relaxed);
    }

    ui64 KeepLimitPages() const noexcept {
        return KeepLimitPages_.load(std::memory_order_relaxed);
    }

    ui64 KeepOwnedBytes() const noexcept {
        return KeepOwnedBytes_.load(std::memory_order_relaxed);
    }

    ui64 KeepOwnedPages() const noexcept {
        return KeepOwnedPages_.load(std::memory_order_relaxed);
    }

    bool KeepOverLimit(ui64 byteLimit) const noexcept {
        return KeepOwnedBytes() > byteLimit || KeepOwnedPages() > KeepLimitPages();
    }

    bool PeekKeepReserve(ui64 bytes, ui64 pages, ui64 byteLimit) const noexcept;
    TKeepReservation TryReserveKeep(ui64 bytes, ui64 byteLimit) noexcept;
    void ReleaseKeep(ui64 bytes) noexcept;

    bool TryAddRetainedBytes(ui64 bytes) noexcept;

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
    void ReleaseStickyOwnedBytes(EItemKind kind, ui64 bytes) noexcept;

    ui64 ColdReclaimableBytes(ECacheTier tier) const noexcept {
        return static_cast<ui64>(Max<i64>(Tier(tier).ReclaimableBytes.load(std::memory_order_relaxed), 0));
    }

    ui64 ColdReclaimableBytes() const noexcept {
        return ColdReclaimableBytes(ECacheTier::Regular) + ColdReclaimableBytes(ECacheTier::Keep);
    }

    ui64 ColdReclaimableItems(ECacheTier tier) const noexcept {
        return static_cast<ui64>(Max<i64>(Tier(tier).ReclaimableItems.load(std::memory_order_relaxed), 0));
    }

    ui64 ColdReclaimableItems() const noexcept {
        return ColdReclaimableItems(ECacheTier::Regular) + ColdReclaimableItems(ECacheTier::Keep);
    }

    // Returns true when a newly unreferenced Cold item can make progress
    bool UpdateColdReclaimable(ui64 payloadBytes, THandleState before, THandleState after) noexcept;

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

    ui64 KeepReadyPageBytes() const noexcept {
        return KeepReadyPageBytes_.load(std::memory_order_relaxed);
    }

    void UpdateKeepResidentPageBytes(ui64 previous, ui64 current) noexcept {
        UpdateAggregate(KeepResidentPageBytes_, previous, current);
    }

    void UpdateKeepReadyPageBytes(ui64 previous, ui64 current) noexcept {
        UpdateAggregate(KeepReadyPageBytes_, previous, current);
    }

    ui64 AdmissionGeneration() const noexcept {
        return AdmissionGeneration_.load(std::memory_order_acquire);
    }

    // The caller notifies its resource waiter when either operation returns true
    bool NotifyResourcesAvailable() noexcept {
        AdmissionGeneration_.fetch_add(1, std::memory_order_release);
        return ResourceWaitArmed_.exchange(false, std::memory_order_acq_rel);
    }

    bool ArmResourceWait(ui64 generation) noexcept;

    void DisarmResourceWait() noexcept {
        ResourceWaitArmed_.store(false, std::memory_order_release);
    }

    // The caller checks its current limit against usage before each attempt.
    bool TryReserve(ui64& usage, ui64 bytes, ui64 pages) noexcept;

    void AccountWritten(ui64 bytes, ui64 pages) noexcept;

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

    void ReleasePayloadCharge(ui64 bytes, EItemKind kind) noexcept;

    Y_FORCE_INLINE void PrepareStaticResize(ui64 bytes) noexcept {
        Y_DEBUG_ABORT_UNLESS(StaticDeltaBytes_.load(std::memory_order_relaxed) == 0);
        StaticDeltaBytes_.fetch_add(bytes, std::memory_order_relaxed);
        OverallUsage_.fetch_add(bytes, std::memory_order_relaxed);
    }

    Y_FORCE_INLINE void CancelStaticResize(ui64 bytes) noexcept {
        SubtractExactBytes(StaticDeltaBytes_, bytes);
        SubtractExactBytes(OverallUsage_, bytes);
    }

    void CommitStaticResize(ui64 oldStaticBytes, ui64 targetStaticBytes) noexcept;

private:
    static bool TryAddBounded(std::atomic<ui64>& counter, ui64 amount, ui64 limit) noexcept;

    static ui64 CalculateSoftLimit(ui64 currentLimit, double gapFraction, ui64 minimumGap) noexcept;

    static void UpdateAggregate(std::atomic<ui64>& total, ui64 previous, ui64 current) noexcept;

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
    std::atomic<ui64> KeepLimitPages_{ 0 };
    std::atomic<ui64> KeepOwnedBytes_{ 0 };
    std::atomic<ui64> KeepOwnedPages_{ 0 };
    std::array<TTierUsage, CacheTierCount> Tiers_;
    TCategoryUsage Sticky_;
    TCategoryUsage Retired_; // Logical erasure loses the source tier; native owners keep the charge alive
    std::atomic<ui64> Collections_{ 0 };
    std::atomic<ui64> CollectionBytes_{ 0 };
    std::atomic<ui64> KeepResidentPageBytes_{ 0 };
    std::atomic<ui64> KeepReadyPageBytes_{ 0 };
    std::atomic<ui64> RequestedPages_{ 0 };
    std::atomic<ui64> RequestedBytes_{ 0 };
    std::atomic<ui64> HitPages_{ 0 };
    std::atomic<ui64> HitBytes_{ 0 };
    std::atomic<ui64> MissPages_{ 0 };
    std::atomic<ui64> MissBytes_{ 0 };
    std::atomic<ui64> CurrentLimit_{ 0 };
    std::atomic<ui64> SoftLimitOverride_{ Max<ui64>() };
    std::atomic<ui64> SoftLimit_{ 0 };
    std::atomic<ui64> HardLimit_{ 0 };
    ui64 ReservationLimit_ = 0;
    std::atomic<ui64> AdmissionGeneration_{ 0 };
    std::atomic<bool> ResourceWaitArmed_{ false };
};

// Inline implementations

inline void TCacheMemory::AddStats(const TStats& stats) noexcept {
    RequestedPages_.fetch_add(stats.RequestedPages, std::memory_order_relaxed);
    RequestedBytes_.fetch_add(stats.RequestedBytes, std::memory_order_relaxed);
    HitPages_.fetch_add(stats.HitPages, std::memory_order_relaxed);
    HitBytes_.fetch_add(stats.HitBytes, std::memory_order_relaxed);
    MissPages_.fetch_add(stats.MissPages, std::memory_order_relaxed);
    MissBytes_.fetch_add(stats.MissBytes, std::memory_order_relaxed);
}

inline bool TCacheMemory::TryAddRetainedBytes(ui64 bytes) noexcept {
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

inline void TCacheMemory::ReleaseStickyOwnedBytes(EItemKind kind, ui64 bytes) noexcept {
    if (kind == EItemKind::Page) {
        SubtractExactBytes(StickyOwnedBytes_, bytes);
    }
}

inline bool TCacheMemory::UpdateColdReclaimable(ui64 payloadBytes, THandleState before, THandleState after) noexcept {
    const auto reclaimable = [](THandleState state) {
        return state.IsCold() && state.IsStickyNoneField() && state.Refs() == 0;
    };
    const bool wasReclaimable = reclaimable(before);
    const bool isReclaimable = reclaimable(after);
    if (wasReclaimable == isReclaimable && (!wasReclaimable || before.Tier() == after.Tier())) {
        return false;
    }
    const i64 bytes = static_cast<i64>(payloadBytes);
    if (wasReclaimable) {
        auto& tier = Tier(before.Tier());
        tier.ReclaimableBytes.fetch_sub(bytes, std::memory_order_relaxed);
        tier.ReclaimableItems.fetch_sub(1, std::memory_order_relaxed);
    }
    if (isReclaimable) {
        auto& tier = Tier(after.Tier());
        tier.ReclaimableBytes.fetch_add(bytes, std::memory_order_relaxed);
        tier.ReclaimableItems.fetch_add(1, std::memory_order_relaxed);
    }
    return isReclaimable;
}

inline bool TCacheMemory::ArmResourceWait(ui64 generation) noexcept {
    // Acquire the notifier's last exchange even when it has already cleared the flag.
    // This makes its preceding generation update visible and closes the store/load lost-wake race.
    ResourceWaitArmed_.exchange(true, std::memory_order_acq_rel);
    return AdmissionGeneration() != generation && ResourceWaitArmed_.exchange(false, std::memory_order_acq_rel);
}

inline bool TCacheMemory::TryReserve(ui64& usage, ui64 bytes, ui64 pages) noexcept {
    if (PageUsage_.compare_exchange_weak(usage, usage + bytes, std::memory_order_relaxed, std::memory_order_relaxed)) {
        OverallUsage_.fetch_add(bytes, std::memory_order_relaxed);
        ReservedBytes_.fetch_add(bytes, std::memory_order_relaxed);
        ReservedPages_.fetch_add(pages, std::memory_order_relaxed);
        return true;
    }
    return false;
}

inline void TCacheMemory::AccountWritten(ui64 bytes, ui64 pages) noexcept {
    if (bytes == 0 && pages == 0) {
        return;
    }
    // The writer already owns these buffers; transfer their charge before publishing native page items.
    PageUsage_.fetch_add(bytes, std::memory_order_relaxed);
    OverallUsage_.fetch_add(bytes, std::memory_order_relaxed);
    ReservedBytes_.fetch_add(bytes, std::memory_order_relaxed);
    ReservedPages_.fetch_add(pages, std::memory_order_relaxed);
}

inline void TCacheMemory::ReleasePayloadCharge(ui64 bytes, EItemKind kind) noexcept {
    SubtractExactBytes(OverallUsage_, bytes);
    if (kind == EItemKind::Page) {
        SubtractExactBytes(PageUsage_, bytes);
    }
}

inline void TCacheMemory::UpdateAggregate(std::atomic<ui64>& total, ui64 previous, ui64 current) noexcept {
    if (current > previous) {
        total.fetch_add(current - previous, std::memory_order_relaxed);
    } else if (current < previous) {
        SubtractExactBytes(total, previous - current);
    }
}

inline void TCacheMemory::TStats::AddAdmission(ECacheResultStatus status, ui64 bytes) noexcept {
    ++RequestedPages;
    RequestedBytes += bytes;
    if (status == ECacheResultStatus::Hit) {
        ++HitPages;
        HitBytes += bytes;
        return;
    }
    ++MissPages;
    MissBytes += bytes;
}

inline void TCacheMemory::TCategoryUsage::AddPage(EItemKind kind) noexcept {
    if (kind == EItemKind::Page) {
        Pages_.fetch_add(1, std::memory_order_relaxed);
    }
}

inline void TCacheMemory::TCategoryUsage::RemovePage(EItemKind kind) noexcept {
    if (kind == EItemKind::Page) {
        Pages_.fetch_sub(1, std::memory_order_relaxed);
    }
}

inline void TCacheMemory::TCategoryUsage::TransferPageTo(TCategoryUsage& destination, EItemKind kind) noexcept {
    if (kind != EItemKind::Page) {
        return;
    }
    destination.Pages_.fetch_add(1, std::memory_order_relaxed);
    Pages_.fetch_sub(1, std::memory_order_relaxed);
}

} // namespace NKikimr::NSharedCache
