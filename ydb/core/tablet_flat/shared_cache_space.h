#pragma once

#include "shared_cache_cyclic_buffer.h"
#include "shared_cache_item.h"
#include "shared_cache_mapping.h"

#include <util/generic/ptr.h>

#include <array>
#include <atomic>
#include <bit>
#include <utility>

namespace NKikimr::NSharedCache {

class TSharedCacheSpace;
template <class TTraits>
class TSharedCacheImpl;

inline constexpr ui64 MaxSpaceGeneration = Max<ui32>();
inline constexpr ui32 SharedCacheTransitionWorkBatch = 256;

inline constexpr ui8 MinSharedCacheAddressBits = 5;
inline constexpr ui8 MaxSharedCacheAddressBits = 32;

constexpr ui32 SharedCacheBucketMask(ui8 addressBits) noexcept {
    return static_cast<ui32>((ui64{ 1 } << addressBits) - 1);
}

constexpr ui64 SharedCacheBucketCount(ui8 addressBits) noexcept {
    return ui64{ 1 } << addressBits;
}

constexpr bool SharedCacheHotLayoutIsValid(ui64 hotSlotCount) noexcept {
    const ui64 l3Size = hotSlotCount / 15;
    const ui64 l2Size = (hotSlotCount * 2) / 15;
    const ui64 l1Size = (hotSlotCount * 5) / 15;
    const ui64 l0Size = hotSlotCount - l3Size - l2Size - l1Size;
    return l0Size && l1Size && l2Size && l3Size;
}

struct TSharedCacheCapacity {
    ui8 AddressBits = 0;
    ui64 Limit = 0;
    ui64 ExpectedPageSize = 0;
    ui64 FixedBytes = 0;
    ui64 StaticBytes = 0;
    ui64 MinimumPayloadBytes = 0;
    ui64 TotalBytes = 0;

    constexpr ui64 HandleCount() const noexcept {
        return SharedCacheBucketCount(AddressBits);
    }

    constexpr ui64 BucketCount() const noexcept {
        return HandleCount();
    }

    constexpr ui64 HotSlotCount() const noexcept {
        return (HandleCount() * 3) / 4;
    }

    constexpr ui64 ColdSlotCount() const noexcept {
        return HandleCount() / 2;
    }

    constexpr ui64 KeepColdSlotCount() const noexcept {
        return HandleCount() / 2;
    }
};

enum class EBucketResize : i8 {
    Shrinking = -1,
    Growing = 1,
};

enum class EBucketResizePhase : ui8 {
    Stable,
    Prepared,
    Activated,
    CursorPublished,
    Closed,
};

static_assert(static_cast<i8>(EBucketResize::Shrinking) == -1);
static_assert(static_cast<i8>(EBucketResize::Growing) == 1);
static_assert(static_cast<ui8>(EBucketResizePhase::Closed) <= 0x7);

class TBucketResizeState {
public:
    constexpr TBucketResizeState() noexcept = default;

    static constexpr TBucketResizeState Make(
        EBucketResize direction, EBucketResizePhase phase, ui8 oldAddressBits, ui32 cursor = 0) noexcept {
        return FromRaw((ui64(static_cast<ui8>(direction)) & DirectionMask) | (ui64(phase) << PhaseShift) |
                       (ui64(oldAddressBits) << AddressBitsShift) | (ui64(cursor) << CursorShift));
    }

    static constexpr TBucketResizeState FromRaw(ui64 raw) noexcept {
        return TBucketResizeState(raw);
    }

    constexpr ui64 Raw() const noexcept {
        return Raw_;
    }

    constexpr explicit operator bool() const noexcept {
        return Raw_ != 0;
    }

    constexpr EBucketResize Direction() const noexcept {
        const i8 direction = static_cast<i8>(Raw_ & DirectionMask);
        return static_cast<EBucketResize>(direction == 3 ? -1 : direction);
    }

    constexpr EBucketResizePhase Phase() const noexcept {
        return static_cast<EBucketResizePhase>((Raw_ & PhaseMask) >> PhaseShift);
    }

    constexpr ui8 OldAddressBits() const noexcept {
        return static_cast<ui8>((Raw_ & AddressBitsMask) >> AddressBitsShift);
    }

    constexpr ui8 NewAddressBits() const noexcept {
        return static_cast<ui8>(OldAddressBits() + static_cast<i8>(Direction()));
    }

    constexpr ui32 Cursor() const noexcept {
        return static_cast<ui32>(Raw_ >> CursorShift);
    }

    constexpr ui32 SplitBit() const noexcept {
        if (!*this) {
            return 0;
        }
        const ui8 bits = Direction() == EBucketResize::Growing ? OldAddressBits() : NewAddressBits();
        return ui32{ 1 } << bits;
    }

    constexpr ui32 PairCount() const noexcept {
        return SplitBit();
    }

    constexpr TBucketResizeState WithPhase(EBucketResizePhase phase) const noexcept {
        return FromRaw((Raw_ & ~PhaseMask) | (ui64(phase) << PhaseShift));
    }

    constexpr TBucketResizeState WithCursorAndPhase(ui32 cursor, EBucketResizePhase phase) const noexcept {
        return FromRaw(
            (Raw_ & ~(CursorMask | PhaseMask)) | (ui64(cursor) << CursorShift) | (ui64(phase) << PhaseShift));
    }

    friend constexpr bool operator==(const TBucketResizeState&, const TBucketResizeState&) noexcept = default;

private:
    static constexpr ui64 DirectionMask = 0x0000000000000003ULL;
    static constexpr ui32 PhaseShift = 2;
    static constexpr ui64 PhaseMask = 0x000000000000001CULL;
    static constexpr ui32 AddressBitsShift = 5;
    static constexpr ui64 AddressBitsMask = 0x00000000000007E0ULL;
    static constexpr ui32 CursorShift = 11;
    static constexpr ui64 CursorMask = 0x000007FFFFFFF800ULL;

    explicit constexpr TBucketResizeState(ui64 raw) noexcept
        : Raw_(raw)
    {
    }

private:
    ui64 Raw_ = 0;
};

static_assert(sizeof(TBucketResizeState) == sizeof(ui64));
static_assert(std::is_trivially_copyable_v<TBucketResizeState>);

struct TSpaceView {
    TSharedCacheSpace* Space = nullptr;
    ui32 Generation = 0;
    THandle* Handles = nullptr;
    std::atomic<ui64>* Buckets = nullptr;
    std::atomic<ui64>* HotSlots = nullptr;
    std::atomic<ui64>* ColdSlots = nullptr;
    std::atomic<ui64>* KeepColdSlots = nullptr;
    std::atomic<ui64>* FreeSlots = nullptr;

    ui64 HandleCount = 0;
    ui64 AllocationLimit = 0;
    ui64 ColdMinHandles = 0;
    ui64 ColdGrowHandles = 0;
    ui64 HotSlotCount = 0;
    TS5FifoRings* Rings = nullptr;
    TFreeRingCursor* FreeCursor = nullptr;
    std::atomic<ui64>* EffectiveHotSlots = nullptr;

    size_t HandlesBytes = 0;
    size_t BucketsBytes = 0;
    size_t HotSlotBytes = 0;
    size_t ColdSlotBytes = 0;
    size_t KeepColdSlotBytes = 0;
    size_t FreeSlotBytes = 0;

    Y_FORCE_INLINE ui64 EffectiveHotSlotCount() const noexcept {
        const ui64 effective = EffectiveHotSlots ? EffectiveHotSlots->load(std::memory_order_relaxed) : HotSlotCount;
        return Min(effective, HotSlotCount);
    }

    Y_FORCE_INLINE TRingView Hot(ui32 level) const noexcept {
        Y_DEBUG_ABORT_UNLESS(Rings && level < 4);
        const ui64 hotSlotCount = EffectiveHotSlotCount();
        const ui64 l3Size = hotSlotCount / 15;
        const ui64 l2Size = (hotSlotCount * 2) / 15;
        const ui64 l1Size = (hotSlotCount * 5) / 15;
        const ui64 sizes[4] = {
            hotSlotCount - l3Size - l2Size - l1Size,
            l1Size,
            l2Size,
            l3Size,
        };
        const ui64 offsets[4] = {
            l3Size + l2Size + l1Size,
            l3Size + l2Size,
            l3Size,
            0,
        };
        return { &Rings->Hot(level), HotSlots + offsets[level], sizes[level] };
    }

    Y_FORCE_INLINE TRingView Cold() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Rings);
        return { &Rings->Cold(), ColdSlots, HandleCount / 2 };
    }

    Y_FORCE_INLINE TRingView KeepCold() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Rings);
        return { &Rings->KeepCold(), KeepColdSlots, HandleCount / 2 };
    }

    Y_FORCE_INLINE TFreeRingView Free() const noexcept {
        Y_DEBUG_ABORT_UNLESS(FreeCursor);
        return { FreeCursor, FreeSlots, HandleCount };
    }

    void CopyMappings(const TSpaceView& source) noexcept {
        Handles = source.Handles;
        Buckets = source.Buckets;
        HotSlots = source.HotSlots;
        ColdSlots = source.ColdSlots;
        KeepColdSlots = source.KeepColdSlots;
        FreeSlots = source.FreeSlots;
        HandlesBytes = source.HandlesBytes;
        BucketsBytes = source.BucketsBytes;
        HotSlotBytes = source.HotSlotBytes;
        ColdSlotBytes = source.ColdSlotBytes;
        KeepColdSlotBytes = source.KeepColdSlotBytes;
        FreeSlotBytes = source.FreeSlotBytes;
    }
};

enum class ETransitionPhase {
    Idle,
    Prepare,
    InitializeGrowth,
    PublishMigration,
    MigrationDrain,
    Migrate,
    AppendGrowth,
    FinalDrain,
    Release,
    ReleaseRetry,
    Commit,
    AbandonedForShutdown,
};

enum class ESpaceMap : ui8 {
    Handles,
    Buckets,
    Hot,
    Cold,
    KeepCold,
    Free,
    Done,
};

class TTransition {
public:
    ETransitionPhase Phase() const noexcept {
        return Phase_;
    }

    ui32 Generation() const noexcept {
        return Generation_;
    }

    const TSharedCacheCapacity& OldConfiguration() const noexcept {
        return OldConfiguration_;
    }

    const TSharedCacheCapacity& TargetConfiguration() const noexcept {
        return TargetConfiguration_;
    }

    ui8 ResizeDeltaBits() const noexcept {
        return ResizeDeltaBits_;
    }

    ESpaceMap ReleaseStage() const noexcept {
        return ReleaseStage_;
    }

private:
    friend class TSharedCacheSpace;
    friend class TSharedCacheTestAccess;
    template <class TTraits>
    friend class TSharedCacheImpl;

    void Reset() noexcept {
        Phase_ = ETransitionPhase::Idle;
        Generation_ = 0;
        OldConfiguration_ = {};
        TargetConfiguration_ = {};
        ResizeDeltaBits_ = 0;
        ReleaseStage_ = ESpaceMap::Done;
        NextHazardIndex_ = 0;
        NextWorkIndex_ = 0;
        FinalDrainStage_ = ESpaceMap::Done;
        CutBlocked_ = false;
        PreparedView_.Reset();
        OldView_.Reset();
    }

private:
    ETransitionPhase Phase_ = ETransitionPhase::Idle;
    ui32 Generation_ = 0;
    TSharedCacheCapacity OldConfiguration_;
    TSharedCacheCapacity TargetConfiguration_;
    ui8 ResizeDeltaBits_ = 0;
    ESpaceMap ReleaseStage_ = ESpaceMap::Done;
    ui32 NextHazardIndex_ = 0;
    ui64 NextWorkIndex_ = 0;
    ESpaceMap FinalDrainStage_ = ESpaceMap::Done;
    bool CutBlocked_ = false;
    THolder<TSpaceView> PreparedView_;
    THolder<TSpaceView> OldView_;
};

class TSpaceState {
public:
    static constexpr ui64 StableBucketMask = 0x00000000FFFFFFFFULL;
    static constexpr ui32 GenerationShift = 32;

    constexpr TSpaceState() noexcept = default;

    static constexpr TSpaceState Make(ui32 generation, ui8 addressBits) noexcept {
        return FromRaw((ui64(generation) << GenerationShift) | SharedCacheBucketMask(addressBits));
    }

    static constexpr TSpaceState FromRaw(ui64 raw) noexcept {
        return TSpaceState(raw);
    }

    constexpr ui64 Raw() const noexcept {
        return Raw_;
    }

    constexpr ui32 Generation() const noexcept {
        return static_cast<ui32>(Raw_ >> GenerationShift);
    }

    constexpr ui32 BucketMask() const noexcept {
        return static_cast<ui32>(Raw_ & StableBucketMask);
    }

    constexpr ui64 BucketCount() const noexcept {
        return ui64(BucketMask()) + 1;
    }

    constexpr ui8 AddressBits() const noexcept {
        return static_cast<ui8>(std::bit_width(BucketMask()));
    }

    constexpr bool Resizing() const noexcept {
        return !BucketMask();
    }

    constexpr TSpaceState WithGeneration(ui32 generation) const noexcept {
        return FromRaw((Raw_ & ~0xFFFFFFFF00000000ULL) | (ui64(generation) << GenerationShift));
    }

    constexpr TSpaceState WithAddressBits(ui8 addressBits) const noexcept {
        return FromRaw((Raw_ & ~StableBucketMask) | SharedCacheBucketMask(addressBits));
    }

    constexpr TSpaceState BeginResize() const noexcept {
        return FromRaw(Raw_ & ~StableBucketMask);
    }

    friend constexpr bool operator==(const TSpaceState&, const TSpaceState&) noexcept = default;

private:
    explicit constexpr TSpaceState(ui64 raw) noexcept
        : Raw_(raw)
    {
    }

private:
    ui64 Raw_ = 0;
};

static_assert(sizeof(TSpaceState) == sizeof(ui64));
static_assert(std::is_trivially_copyable_v<TSpaceState>);

enum class EHotResizePhase {
    Idle,
    Cut,
};

class THotResize {
public:
    EHotResizePhase Phase() const noexcept {
        return Phase_;
    }

    // The boundary change is exclusive: only one caller may move the hot/cold boundary at a time, every other
    // caller gets an empty guard (and re-tries reclamation from the beginning).
    class TGuard {
    public:
        TGuard() noexcept = default;

        explicit TGuard(THotResize& resize) noexcept
            : Resize_(resize.TryAcquire() ? &resize : nullptr)
        {}

        TGuard(TGuard&& other) noexcept
            : Resize_(other.Resize_)
        {
            other.Resize_ = nullptr;
        }

        TGuard& operator=(TGuard&& other) noexcept {
            if (this != &other) {
                Reset();
                Resize_ = other.Resize_;
                other.Resize_ = nullptr;
            }
            return *this;
        }

        TGuard(const TGuard&) = delete;
        TGuard& operator=(const TGuard&) = delete;

        ~TGuard() {
            Reset();
        }

        explicit operator bool() const noexcept {
            return Resize_ != nullptr;
        }

    private:
        void Reset() noexcept {
            if (Resize_) {
                Resize_->Unlock();
                Resize_ = nullptr;
            }
        }

    private:
        THotResize* Resize_ = nullptr;
    };

    TGuard TryLock() noexcept {
        return TGuard(*this);
    }

private:
    bool TryAcquire() noexcept {
        bool expected = false;
        return Locked_.compare_exchange_strong(expected, true, std::memory_order_acquire, std::memory_order_relaxed);
    }

    void Unlock() noexcept {
        Locked_.store(false, std::memory_order_release);
    }

    friend class TSharedCacheSpace;

    void Reset() noexcept {
        Phase_ = EHotResizePhase::Idle;
        OldEffectiveHotSlots_ = 0;
        NextCutSlot_ = 0;
    }

private:
    std::atomic<bool> Locked_{ false };
    EHotResizePhase Phase_ = EHotResizePhase::Idle;
    ui64 OldEffectiveHotSlots_ = 0;
    ui64 NextCutSlot_ = 0;
};

bool TryCalculateSharedCacheFootprint(
    ui8 addressBits, ui64 expectedPageSize, ui64 fixedBytes, ui32 hazardCount, TSharedCacheCapacity& result) noexcept;

bool TryCalculateSharedCacheCapacity(
    ui64 limit, ui64 expectedPageSize, ui64 fixedBytes, ui32 hazardCount, TSharedCacheCapacity& result) noexcept;

struct alignas(64) TSpaceHazard {
    std::atomic<ui64> State{ 0 }; // {generation:32, active-count:32}
    std::atomic<ui64> SpareItem{ 0 };
    std::atomic<bool> Bound{ false }; // One thread owns this slot while its binding exists.
};

static_assert(sizeof(TSpaceHazard) == 64);
static_assert(alignof(TSpaceHazard) == 64);

inline constexpr ui64 SpaceHazardGeneration(ui64 state) noexcept;
inline constexpr ui32 SpaceHazardActiveCount(ui64 state) noexcept;

class TSpaceOperation {
public:
    TSpaceOperation(const TSpaceOperation&) = delete;
    TSpaceOperation& operator=(const TSpaceOperation&) = delete;
    TSpaceOperation& operator=(TSpaceOperation&&) = delete;

    TSpaceOperation(TSpaceOperation&& other) noexcept
        : Hazard_(std::exchange(other.Hazard_, nullptr))
        , EntryState_(other.EntryState_)
        , View_(other.View_)
    {
    }

    ~TSpaceOperation() {
        if (Hazard_) {
            LeaveHazard(*Hazard_);
        }
    }

    static Y_FORCE_INLINE TSpaceOperation Begin(
        std::atomic<ui64>& spaceState, TSpaceHazard& hazard, const std::array<TSpaceView, 2>& views) noexcept {
        return BeginImpl<false>(spaceState, hazard, views, [] {
        });
    }

    template <class TAfterIncrement>
    static Y_FORCE_INLINE TSpaceOperation BeginWithHook(std::atomic<ui64>& spaceState, TSpaceHazard& hazard,
        const std::array<TSpaceView, 2>& views,
        TAfterIncrement&& afterIncrement) noexcept(noexcept(std::forward<TAfterIncrement>(afterIncrement)()))
    {
        return BeginImpl<true>(spaceState, hazard, views, std::forward<TAfterIncrement>(afterIncrement));
    }

    ui64 EntryGeneration() const noexcept {
        return EntryState_.Generation();
    }

    TSpaceState EntrySpaceState() const noexcept {
        return EntryState_;
    }

    TSpaceHazard& Hazard() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Hazard_);
        return *Hazard_;
    }

    const TSpaceView& View() const noexcept {
        return *View_;
    }

    Y_FORCE_INLINE THandle* Handles() const noexcept {
        return View().Handles;
    }

    Y_FORCE_INLINE std::atomic<ui64>* Buckets() const noexcept {
        return View().Buckets;
    }

    Y_FORCE_INLINE ui64 HandleCount() const noexcept {
        return View().HandleCount;
    }

    Y_FORCE_INLINE ui64 AccessibleHandleCount() const noexcept {
        return View().HandlesBytes / sizeof(THandle);
    }

    Y_FORCE_INLINE bool Contains(TCacheItem cacheItem) const noexcept {
        return !cacheItem.IsNull() && cacheItem.Index() < AccessibleHandleCount();
    }

    Y_FORCE_INLINE ui64 AllocationLimit() const noexcept {
        return View().AllocationLimit;
    }

    Y_FORCE_INLINE ui64 EffectiveHotSlots() const noexcept {
        return View().EffectiveHotSlotCount();
    }

    Y_FORCE_INLINE TRingView Hot(ui32 level) const noexcept {
        return View().Hot(level);
    }

    Y_FORCE_INLINE bool IsAllowedHotSlot(const std::atomic<ui64>* slot) const noexcept {
        const TSpaceView& view = View();
        Y_DEBUG_ABORT_UNLESS(slot >= view.HotSlots && slot < view.HotSlots + view.HotSlotCount);
        return static_cast<ui64>(slot - view.HotSlots) < view.EffectiveHotSlotCount();
    }

    Y_FORCE_INLINE TRingView Cold() const noexcept {
        return View().Cold();
    }

    Y_FORCE_INLINE TRingView KeepCold() const noexcept {
        return View().KeepCold();
    }

    Y_FORCE_INLINE TCacheItem PutCold(TCacheItem cacheItem) const noexcept {
        Y_DEBUG_ABORT_UNLESS(!cacheItem.IsNull() && !cacheItem.IsFrozen());
        return TCacheItem::FromRaw(Cold().PutAndPop(cacheItem.Raw()));
    }

    ui32 TryAllocateHandle() noexcept;

    bool ReturnFreeHandle(ui32 index, ui32 version) noexcept;

    Y_FORCE_INLINE bool TryStoreSpareItem(TCacheItem cacheItem) noexcept {
        Y_DEBUG_ABORT_UNLESS(!cacheItem.IsNull() && !cacheItem.IsFrozen() && cacheItem.Index() >= 2 &&
                             cacheItem.Index() < AllocationLimit());
        const THandle& handle = Handles()[cacheItem.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && state.IsBegin() && state.Refs() == 0);

        ui64 expected = 0;
        return Hazard().SpareItem.compare_exchange_strong(
            expected, cacheItem.Raw(), std::memory_order_release, std::memory_order_relaxed);
    }

private:
    template <bool WithHook, class TAfterIncrement>
    static Y_FORCE_INLINE TSpaceOperation BeginImpl(std::atomic<ui64>& spaceState, TSpaceHazard& hazard,
        const std::array<TSpaceView, 2>& views,
        TAfterIncrement&& afterIncrement) noexcept(noexcept(std::forward<TAfterIncrement>(afterIncrement)()))
    {
        for (;;) {
            TSpaceState entryState = TSpaceState::FromRaw(spaceState.load(std::memory_order_acquire));
            ui64 hazardState = hazard.State.load(std::memory_order_relaxed);
            const ui32 activeCount = SpaceHazardActiveCount(hazardState);
            Y_DEBUG_ABORT_UNLESS(activeCount != Max<ui32>());

            if (activeCount != 0) {
                if (!hazard.State.compare_exchange_weak(
                        hazardState, hazardState + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
                {
                    continue;
                }
                return TSpaceOperation(hazard, entryState, views);
            }

            Y_DEBUG_ABORT_UNLESS(hazardState == 0);
            const ui32 generation = entryState.Generation();
            if (!hazard.State.compare_exchange_weak(hazardState, PackHazardState(generation, activeCount + 1),
                    std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                continue;
            }
            if constexpr (WithHook) {
                std::forward<TAfterIncrement>(afterIncrement)();
            }
            entryState = TSpaceState::FromRaw(spaceState.load(std::memory_order_acquire));
            if (entryState.Generation() != generation) {
                LeaveHazard(hazard);
                continue;
            }

            return TSpaceOperation(hazard, entryState, views);
        }
    }

    static Y_FORCE_INLINE void LeaveHazard(TSpaceHazard& hazard) noexcept {
        ui64 state = hazard.State.load(std::memory_order_relaxed);
        for (;;) {
            const ui32 activeCount = SpaceHazardActiveCount(state);
            Y_DEBUG_ABORT_UNLESS(activeCount != 0);
            const ui64 desired = activeCount == 1 ? 0 : state - 1;
            if (hazard.State.compare_exchange_weak(
                    state, desired, std::memory_order_release, std::memory_order_relaxed))
            {
                return;
            }
        }
    }

    static constexpr ui64 PackHazardState(ui64 generation, ui32 activeCount) noexcept {
        return (generation << 32) | activeCount;
    }

    TSpaceOperation(TSpaceHazard& hazard, TSpaceState entryState, const std::array<TSpaceView, 2>& views) noexcept
        : Hazard_(&hazard)
        , EntryState_(entryState)
        , View_(&views[entryState.Generation() & 1])
    {
        Y_DEBUG_ABORT_UNLESS(View_->Generation == entryState.Generation());
    }

private:
    TSpaceHazard* Hazard_;
    TSpaceState EntryState_;
    const TSpaceView* View_;
};

inline constexpr ui64 SpaceHazardGeneration(ui64 state) noexcept {
    return state >> 32;
}

inline constexpr ui32 SpaceHazardActiveCount(ui64 state) noexcept {
    return static_cast<ui32>(state);
}

inline constexpr bool SpaceHazardIsActive(ui64 state) noexcept {
    return SpaceHazardActiveCount(state) != 0;
}

inline constexpr bool SpaceHazardIsDrained(ui64 state, ui64 generation) noexcept {
    return !SpaceHazardIsActive(state) || SpaceHazardGeneration(state) == generation;
}

class TSharedCacheSpaceTestAccess;
class TSharedCacheTestAccess;

class TSpaceHazardBinding {
public:
    TSpaceHazardBinding() noexcept = default;
    TSpaceHazardBinding(const TSpaceHazardBinding&) = delete;
    TSpaceHazardBinding& operator=(const TSpaceHazardBinding&) = delete;
    TSpaceHazardBinding(TSpaceHazardBinding&& other) noexcept;
    TSpaceHazardBinding& operator=(TSpaceHazardBinding&& other) noexcept;
    ~TSpaceHazardBinding();

private:
    friend class TSharedCacheSpace;

    TSpaceHazardBinding(const TSharedCacheSpace* space, TSpaceHazard* hazard) noexcept;

    void Reset() noexcept;

private:
    const TSharedCacheSpace* Space_ = nullptr;
    TSpaceHazard* Hazard_ = nullptr;
};

class TSharedCacheSpace {
public:
    static THolder<TSharedCacheSpace> Create(
        const TSharedCacheCapacity& current, const TSharedCacheCapacity& reserved, ui32 hazardCount);

    TSharedCacheSpace(const TSharedCacheSpace&) = delete;
    TSharedCacheSpace& operator=(const TSharedCacheSpace&) = delete;
    ~TSharedCacheSpace();

    Y_FORCE_INLINE TSpaceOperation BeginOperation() noexcept {
        return TSpaceOperation::Begin(SpaceState_, CurrentThreadHazard(), SpaceViews_);
    }

    Y_FORCE_INLINE const TSpaceView& SpaceView(TSpaceState state) const noexcept {
        const TSpaceView& view = SpaceViews_[state.Generation() & 1];
        Y_DEBUG_ABORT_UNLESS(view.Generation == state.Generation());
        return view;
    }

    Y_FORCE_INLINE TBucketResizeState BucketResizeState() const noexcept {
        return TBucketResizeState::FromRaw(BucketResizeState_.load(std::memory_order_acquire));
    }

    Y_FORCE_INLINE bool RefreshBucketRoute(TSpaceState& spaceState, TBucketResizeState& resizeState) const noexcept {
        const TSpaceState currentSpaceState = CurrentSpaceState();
        if (!currentSpaceState.Resizing()) {
            const bool unchanged = currentSpaceState == spaceState;
            spaceState = currentSpaceState;
            if (!unchanged) {
                resizeState = {};
            }
            return unchanged;
        }

        const TBucketResizeState currentResizeState = BucketResizeState();
        if (currentSpaceState == spaceState && currentResizeState == resizeState)
        {
            return true;
        }
        spaceState = currentSpaceState;
        resizeState = currentResizeState;
        return false;
    }

    Y_FORCE_INLINE TSpaceState CurrentSpaceState() const noexcept {
        return TSpaceState::FromRaw(SpaceState_.load(std::memory_order_acquire));
    }

    // Serialized controller access; workers use their active space view.
    const TSharedCacheCapacity& CurrentConfiguration() const noexcept {
        return Capacity_;
    }

    const TSharedCacheCapacity& ReservedConfiguration() const noexcept {
        return ReservedCapacity_;
    }

    ui64 EffectiveHotSlots() const noexcept {
        return EffectiveHotSlots_.load(std::memory_order_acquire);
    }

    ui32 HazardCount() const noexcept {
        return HazardCount_;
    }

    bool ThreadHazardBound() const noexcept {
        Y_DEBUG_ABORT_UNLESS(!TlsHazard_ || TlsSpace_ == this);
        return TlsHazard_;
    }

    bool BeginBucketResize(ui8 newAddressBits) noexcept;
    bool AdvanceBucketResizePhase(EBucketResizePhase phase) noexcept;
    bool PublishBucketResizeCursor(ui32 cursor) noexcept;
    bool PrepareNextBucketResizePair() noexcept;
    bool FinishBucketResize() noexcept;

    TSpaceHazardBinding BindThreadHazard(ui32 index) const noexcept;
    TSpaceHazardBinding BindAnyThreadHazard() const noexcept;

    TCacheItem TakeCutSpare(ui64 allocationLimit) noexcept;

    bool BeginHotResize(ui32 targetEffectiveHotSlots, THotResize& resize) noexcept;
    bool DrainHotResize(THotResize& resize, ui32& level, ui64& raw) noexcept;

    bool PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept;

    bool PublishMigrationView(TTransition& transition) noexcept;

    bool TryDrainTransition(TTransition& transition) noexcept;

    template <class THotRouter, class TColdRouter, class TKeepColdRouter>
    bool FinalDrain(TTransition& transition, THotRouter&& hotRouter, TColdRouter&& coldRouter,
        TKeepColdRouter&& keepColdRouter) noexcept {
        if (transition.Phase_ != ETransitionPhase::FinalDrain || transition.NextHazardIndex_ != HazardSlotCount_ ||
            transition.Generation_ != CurrentSpaceState().Generation() || !transition.OldView_)
        {
            return false;
        }

        ui32 remainingSlots = SharedCacheTransitionWorkBatch;
        const ui64 oldHotSlotCount = transition.OldConfiguration_.HotSlotCount();
        const ui64 oldHandleCount = transition.OldConfiguration_.HandleCount();
        const ui64 oldColdSlotCount = transition.OldConfiguration_.ColdSlotCount();
        const ui64 oldKeepColdSlotCount = transition.OldConfiguration_.KeepColdSlotCount();
        const ui64 targetHandleCount = transition.TargetConfiguration_.HandleCount();
        const ui64 targetColdSlotCount = transition.TargetConfiguration_.ColdSlotCount();
        const ui64 targetKeepColdSlotCount = transition.TargetConfiguration_.KeepColdSlotCount();
        while (remainingSlots != 0 && transition.FinalDrainStage_ != ESpaceMap::Done) {
            switch (transition.FinalDrainStage_) {
                case ESpaceMap::Hot:
                    while (remainingSlots != 0 && transition.NextWorkIndex_ < oldHotSlotCount) {
                        const ui64 slot = transition.NextWorkIndex_++;
                        const ui64 raw = transition.OldView_->HotSlots[slot].exchange(0, std::memory_order_relaxed);
                        if (raw != 0) {
                            hotRouter(HotLevelForSlot(slot, oldHotSlotCount), raw);
                        }
                        --remainingSlots;
                    }
                    if (transition.NextWorkIndex_ == oldHotSlotCount) {
                        transition.FinalDrainStage_ = ESpaceMap::Cold;
                        transition.NextWorkIndex_ = targetColdSlotCount;
                    }
                    break;

                case ESpaceMap::Cold:
                    while (remainingSlots != 0 && transition.NextWorkIndex_ < oldColdSlotCount) {
                        const ui64 raw = transition.OldView_->ColdSlots[transition.NextWorkIndex_++].exchange(
                            0, std::memory_order_relaxed);
                        if (raw != 0) {
                            coldRouter(raw);
                        }
                        --remainingSlots;
                    }
                    if (transition.NextWorkIndex_ == oldColdSlotCount) {
                        transition.FinalDrainStage_ = ESpaceMap::KeepCold;
                        transition.NextWorkIndex_ = targetKeepColdSlotCount;
                    }
                    break;

                case ESpaceMap::KeepCold:
                    while (remainingSlots != 0 && transition.NextWorkIndex_ < oldKeepColdSlotCount) {
                        const ui64 raw = transition.OldView_->KeepColdSlots[transition.NextWorkIndex_++].exchange(
                            0, std::memory_order_relaxed);
                        if (raw != 0) {
                            keepColdRouter(raw);
                        }
                        --remainingSlots;
                    }
                    if (transition.NextWorkIndex_ == oldKeepColdSlotCount) {
                        transition.FinalDrainStage_ = ESpaceMap::Free;
                        transition.NextWorkIndex_ = 0;
                    }
                    break;

                case ESpaceMap::Free: {
                    const ui64 cutCount = oldHandleCount - targetHandleCount;
                    const TSpaceView& view = SpaceView(CurrentSpaceState());
                    while (remainingSlots != 0 && transition.NextWorkIndex_ < oldHandleCount) {
                        const ui64 workIndex = transition.NextWorkIndex_++;
                        if (workIndex < cutCount) {
                            const ui64 slot = targetHandleCount + workIndex;
                            RouteFreeWord(view, FreeRingCursor_->Clear(&transition.OldView_->FreeSlots[slot]));
                        } else {
                            std::atomic<ui64>* currentSlot = &view.FreeSlots[workIndex - cutCount];
                            ui64 raw = currentSlot->load(std::memory_order_relaxed);
                            while (raw != 0 && !IsCurrentFreeWord(view, raw)) {
                                if (FreeRingCursor_->ClearSafe(currentSlot, raw)) {
                                    break;
                                }
                                raw = currentSlot->load(std::memory_order_relaxed);
                            }
                        }
                        --remainingSlots;
                    }
                    if (transition.NextWorkIndex_ == oldHandleCount) {
                        transition.FinalDrainStage_ = ESpaceMap::Done;
                        transition.NextWorkIndex_ = 0;
                    }
                    break;
                }

                case ESpaceMap::Handles:
                case ESpaceMap::Buckets:
                case ESpaceMap::Done:
                    Y_ABORT("Invalid shared-cache final-drain stage");
            }
        }
        if (transition.FinalDrainStage_ == ESpaceMap::Done) {
            transition.Phase_ = ETransitionPhase::Release;
        }
        return true;
    }

    bool PublishFinalView(TTransition& transition) noexcept;

    bool AppendFreeHandles(TTransition& transition) noexcept;

    bool TryReleaseTransition(TTransition& transition) noexcept;

    bool CommitTransition(TTransition& transition) noexcept;

    bool AbandonTransition(TTransition& transition) noexcept;

    Y_FORCE_INLINE bool RefreshSpaceState(TSpaceState& spaceState) const noexcept {
        const TSpaceState current = CurrentSpaceState();
        if (current == spaceState) {
            return true;
        }
        spaceState = current;
        return false;
    }

private:
    friend class TSpaceHazardBinding;
    friend class TSharedCacheSpaceTestAccess;
    friend class TSharedCacheTestAccess;
    friend class TSpaceOperation;

    struct TFreeExchangeHook {
        void* Context = nullptr;
        void (*Function)(void*) noexcept = nullptr;

        void Run() const noexcept {
            if (Function) {
                Function(Context);
            }
        }
    };

    struct TPublishMigrationViewHook {
        void* Context = nullptr;
        void (*Function)(void*) noexcept = nullptr;

        void Run() const noexcept {
            Function(Context);
        }
    };

    struct TReleaseHook {
        void* Context = nullptr;
        bool (*Function)(void*, ESpaceMap, const void*, size_t) noexcept = nullptr;

        bool Run(ESpaceMap mapping, const void* begin, size_t size) const noexcept {
            return Function(Context, mapping, begin, size);
        }
    };

    ui32 TryAllocateHandleWithHook(TSpaceOperation& spaceOp, TFreeExchangeHook hook) noexcept;

    bool ReturnFreeHandleWithHook(TSpaceOperation& spaceOp, ui32 index, ui32 version, TFreeExchangeHook hook) noexcept;

    bool PublishMigrationViewWithHook(TTransition& transition, TPublishMigrationViewHook hook) noexcept;

    bool TryReleaseTransitionWithHook(TTransition& transition, TReleaseHook hook) noexcept;

    bool AdvanceSpaceGeneration() noexcept;

    template <bool WithHook>
    Y_FORCE_INLINE ui32 TryAllocateHandleImpl(TSpaceOperation& spaceOp, TFreeExchangeHook hook) noexcept;

    template <bool WithHook>
    Y_FORCE_INLINE bool ReturnFreeHandleImpl(
        TSpaceOperation& spaceOp, ui32 index, ui32 version, TFreeExchangeHook hook) noexcept;

    template <bool WithHook>
    bool PublishMigrationViewImpl(TTransition& transition, TPublishMigrationViewHook hook) noexcept;

    template <bool WithHook>
    bool TryReleaseTransitionImpl(TTransition& transition, TReleaseHook hook) noexcept;

    void ResetHazardScan(TTransition& transition) noexcept;

    static ui32 HotLevelForSlot(ui64 slot, ui64 hotSlotCount) noexcept;

    template <bool WithHook>
    bool TryReleaseOldMapping(ESpaceMap mapping, TTransition& transition, TReleaseHook hook) noexcept;

    bool InitializeGrowth(TTransition& transition) noexcept;

    bool IsCurrentFreeWord(const TSpaceView& view, ui64 raw) const noexcept;

    void RouteFreeWord(const TSpaceView& view, ui64 raw) noexcept;

    void CancelPreparedView(TSpaceView& view) noexcept;

    void PublishPreparedView(THolder<TSpaceView> view, THolder<TSpaceView>& oldView) noexcept;

    void PrepareView(ui32 generation, TSpaceView view) noexcept;

    Y_FORCE_INLINE const TSpaceView& NewestView() const noexcept {
        TSpaceView* view = NewestView_.load(std::memory_order_acquire);
        Y_DEBUG_ABORT_UNLESS(view);
        return *view;
    }

    Y_FORCE_INLINE TSpaceHazard& CurrentThreadHazard() const noexcept {
        Y_DEBUG_ABORT_UNLESS(TlsSpace_ == this);
        Y_DEBUG_ABORT_UNLESS(TlsHazard_);
        return *TlsHazard_;
    }

    TSharedCacheSpace() = default;

    bool Initialize(const TSharedCacheCapacity& current, const TSharedCacheCapacity& reserved, ui32 hazardCount);

private:
    TSharedCacheMapping HandlesMapping_;
    TSharedCacheMapping BucketsMapping_;
    TSharedCacheMapping HotMapping_;
    TSharedCacheMapping ColdMapping_;
    TSharedCacheMapping KeepColdMapping_;
    TSharedCacheMapping FreeMapping_;
    THolder<TSpaceView> NewestViewOwner_;
    std::atomic<TSpaceView*> NewestView_{ nullptr };
    std::array<TSpaceView, 2> SpaceViews_;
    THolder<TS5FifoRings> S5FifoRings_;
    THolder<TFreeRingCursor> FreeRingCursor_;
    TArrayHolder<TSpaceHazard> Hazards_;

    inline static thread_local TSpaceHazard* TlsHazard_ = nullptr;
    inline static thread_local const TSharedCacheSpace* TlsSpace_ = nullptr;

    ui32 HazardCount_ = 0;
    ui32 HazardSlotCount_ = 1;
    TSharedCacheCapacity Capacity_;
    TSharedCacheCapacity ReservedCapacity_;

    std::atomic<ui64> SpaceState_{ 0 };
    std::atomic<ui64> EffectiveHotSlots_{ 0 };
    std::atomic<ui64> BucketResizeState_{ 0 };
};

} // namespace NKikimr::NSharedCache
