#pragma once

#include "defs.h"
#include "flat_sausage_gut.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace NKikimr::NSharedCache {

inline constexpr ui32 MaxItemVersion = (ui32{ 1 } << 31) - 1;
// Item versions wrap inside MaxItemVersion, so the top bit of THandle::ColdVersion is free: it records whether
// the page currently owns an entry in the KeepCold ring.
inline constexpr ui32 KeepColdQueuedMask = ui32{ 1 } << 31;
inline constexpr ui32 MaxHandleRefs = (ui32{ 1 } << 24) - 1;
inline constexpr ui32 ResizeMarkerIndex = 1;

enum class EItemKind : ui8 {
    Page = 0,
    Collection = 1,
};

enum class EStickyState : ui8 {
    None = 0,
    Sticky = 1,
    // Page: withdrawn from the Sticky list for relocation. Collection: attachment/detachment in progress.
    Transition = 2,
};

enum class EHandleState : ui8 {
    Free = 0,
    Begin,
    Queued,
    Requested,
    QueuedRequested,
    Completing,
    Sticky,
    Hot,
    KeepCold,
    Cold,
    Replacing,
    Replaced,
    BucketSplit,
    Tombstone,
    WaitingForMemory,
};

static_assert(static_cast<ui8>(EItemKind::Collection) <= 0x1);
static_assert(static_cast<ui8>(EStickyState::Transition) <= 0x3);
static_assert(static_cast<ui8>(EHandleState::WaitingForMemory) <= 0xF);

constexpr bool IsPending(EHandleState state) noexcept {
    return (state >= EHandleState::Begin && state <= EHandleState::QueuedRequested) ||
           state == EHandleState::WaitingForMemory;
}

constexpr bool IsReady(EHandleState state) noexcept {
    return state == EHandleState::Hot || state == EHandleState::Cold || state == EHandleState::KeepCold ||
           state == EHandleState::Sticky;
}

class THandleState;

class TCacheItem {
public:
    static constexpr ui64 IndexMask = 0x00000000FFFFFFFFULL;
    static constexpr ui64 FrozenMask = 0x0000000100000000ULL;
    static constexpr ui64 VersionMask = 0xFFFFFFFE00000000ULL;
    static constexpr ui32 VersionShift = 33;

    constexpr TCacheItem() noexcept = default;

    static constexpr TCacheItem Make(ui32 version, ui32 index, bool frozen = false) noexcept {
        return FromRaw((ui64(version & MaxItemVersion) << VersionShift) | (frozen ? FrozenMask : 0) | ui64(index));
    }

    static constexpr TCacheItem FromRaw(ui64 raw) noexcept {
        return TCacheItem(raw);
    }

    static constexpr TCacheItem ClosedBucketHead() noexcept {
        return Make(MaxItemVersion, 0);
    }

    constexpr ui64 Raw() const noexcept {
        return Raw_;
    }

    constexpr ui32 Version() const noexcept {
        return static_cast<ui32>((Raw_ & VersionMask) >> VersionShift);
    }

    constexpr ui32 Index() const noexcept {
        return static_cast<ui32>(Raw_ & IndexMask);
    }

    constexpr bool IsNull() const noexcept {
        return Index() == 0;
    }

    constexpr bool IsClosedBucketHead() const noexcept {
        return Raw_ == ClosedBucketHead().Raw_;
    }

    constexpr bool IsResizeMarker() const noexcept {
        return Index() == ResizeMarkerIndex;
    }

    constexpr bool IsFrozen() const noexcept {
        return Raw_ & FrozenMask;
    }

    constexpr TCacheItem WithFrozen() const noexcept {
        return FromRaw(Raw_ | FrozenMask);
    }

    constexpr TCacheItem WithoutFrozen() const noexcept {
        return FromRaw(Raw_ & ~FrozenMask);
    }

    constexpr bool Matches(THandleState state) const noexcept;

    friend constexpr bool operator==(const TCacheItem&, const TCacheItem&) noexcept = default;

private:
    explicit constexpr TCacheItem(ui64 raw) noexcept
        : Raw_(raw)
    {
    }

private:
    ui64 Raw_ = 0;
};

class THandleState {
public:
    static constexpr ui64 RefsMask = 0x0000000000FFFFFFULL;
    static constexpr ui32 RefsShift = 0;
    static constexpr ui64 StickyMask = 0x0000000003000000ULL;
    static constexpr ui32 StickyShift = 24;
    static constexpr ui64 FrequencyMask = 0x000000000C000000ULL;
    static constexpr ui32 FrequencyShift = 26;
    static constexpr ui64 KindMask = 0x0000000010000000ULL;
    static constexpr ui32 KindShift = 28;
    static constexpr ui64 StateMask = 0x00000001E0000000ULL;
    static constexpr ui32 StateShift = 29;
    static constexpr ui64 VersionMask = 0xFFFFFFFE00000000ULL;
    static constexpr ui32 VersionShift = 33;

    constexpr THandleState() noexcept = default;

    static constexpr THandleState Make(
        ui32 version, EHandleState state, EItemKind kind, ui8 frequency, EStickyState sticky, ui32 refs) noexcept {
        return FromRaw((ui64(version & MaxItemVersion) << VersionShift) | (ui64(state) << StateShift) |
                       (ui64(kind) << KindShift) | (ui64(frequency & 0x3) << FrequencyShift) |
                       (ui64(sticky) << StickyShift) | ui64(refs & MaxHandleRefs));
    }

    static constexpr THandleState FromRaw(ui64 raw) noexcept {
        return THandleState(raw);
    }

    constexpr ui64 Raw() const noexcept {
        return Raw_;
    }

    constexpr ui32 Version() const noexcept {
        return static_cast<ui32>((Raw_ & VersionMask) >> VersionShift);
    }

    constexpr EHandleState State() const noexcept {
        return static_cast<EHandleState>((Raw_ & StateMask) >> StateShift);
    }

    constexpr EItemKind Kind() const noexcept {
        return static_cast<EItemKind>((Raw_ & KindMask) >> KindShift);
    }

    constexpr bool IsPageKind() const noexcept {
        return Kind() == EItemKind::Page;
    }

    constexpr bool IsCollectionKind() const noexcept {
        return Kind() == EItemKind::Collection;
    }

    constexpr bool IsFree() const noexcept {
        return State() == EHandleState::Free;
    }

    constexpr bool IsBegin() const noexcept {
        return State() == EHandleState::Begin;
    }

    constexpr bool IsQueued() const noexcept {
        return State() == EHandleState::Queued;
    }

    constexpr bool IsRequested() const noexcept {
        return State() == EHandleState::Requested;
    }

    constexpr bool IsQueuedRequested() const noexcept {
        return State() == EHandleState::QueuedRequested;
    }

    constexpr bool IsCompleting() const noexcept {
        return State() == EHandleState::Completing;
    }

    constexpr bool IsHot() const noexcept {
        return State() == EHandleState::Hot;
    }

    constexpr bool IsCold() const noexcept {
        return State() == EHandleState::Cold;
    }

    constexpr bool IsKeepCold() const noexcept {
        return State() == EHandleState::KeepCold;
    }

    constexpr bool IsSticky() const noexcept {
        return State() == EHandleState::Sticky;
    }

    constexpr bool IsReplacing() const noexcept {
        return State() == EHandleState::Replacing;
    }

    constexpr bool IsReplaced() const noexcept {
        return State() == EHandleState::Replaced;
    }

    constexpr bool IsBucketSplit() const noexcept {
        return State() == EHandleState::BucketSplit;
    }

    constexpr bool IsTombstone() const noexcept {
        return State() == EHandleState::Tombstone;
    }

    constexpr bool IsPending() const noexcept {
        return NSharedCache::IsPending(State());
    }

    constexpr bool IsWaitingForMemory() const noexcept {
        return State() == EHandleState::WaitingForMemory;
    }

    constexpr bool IsInFlight() const noexcept {
        return State() >= EHandleState::Queued && State() <= EHandleState::QueuedRequested;
    }

    constexpr bool IsReady() const noexcept {
        return NSharedCache::IsReady(State());
    }

    constexpr bool IsStickyNoneField() const noexcept {
        return Sticky() == EStickyState::None;
    }

    constexpr bool IsStickyField() const noexcept {
        return Sticky() == EStickyState::Sticky;
    }

    constexpr bool IsTransitionField() const noexcept {
        return Sticky() == EStickyState::Transition;
    }

    constexpr ui8 Frequency() const noexcept {
        return static_cast<ui8>((Raw_ & FrequencyMask) >> FrequencyShift);
    }

    constexpr EStickyState Sticky() const noexcept {
        return static_cast<EStickyState>((Raw_ & StickyMask) >> StickyShift);
    }

    constexpr ui32 Refs() const noexcept {
        return static_cast<ui32>((Raw_ & RefsMask) >> RefsShift);
    }

    constexpr THandleState WithVersion(ui32 value) const noexcept {
        return Replace(VersionMask, VersionShift, value & MaxItemVersion);
    }

    constexpr THandleState WithState(EHandleState value) const noexcept {
        return Replace(StateMask, StateShift, ui64(value));
    }

    constexpr THandleState WithKind(EItemKind value) const noexcept {
        return Replace(KindMask, KindShift, ui64(value));
    }

    constexpr THandleState WithFrequency(ui8 value) const noexcept {
        return Replace(FrequencyMask, FrequencyShift, value & 0x3);
    }

    constexpr THandleState WithSticky(EStickyState value) const noexcept {
        return Replace(StickyMask, StickyShift, ui64(value));
    }

    constexpr THandleState WithRefs(ui32 value) const noexcept {
        return Replace(RefsMask, RefsShift, value & MaxHandleRefs);
    }

    constexpr THandleState IncrementRefs() const noexcept {
        return FromRaw(Raw_ + 1);
    }

    constexpr THandleState DecrementRefs() const noexcept {
        return FromRaw(Raw_ - 1);
    }

    friend constexpr bool operator==(const THandleState&, const THandleState&) noexcept = default;

private:
    explicit constexpr THandleState(ui64 raw) noexcept
        : Raw_(raw)
    {
    }

    constexpr THandleState Replace(ui64 mask, ui32 shift, ui64 value) const noexcept {
        return FromRaw((Raw_ & ~mask) | ((value << shift) & mask));
    }

private:
    ui64 Raw_ = 0;
};

constexpr bool TCacheItem::Matches(THandleState state) const noexcept {
    return Version() == state.Version();
}

class TPageCacheItem {
public:
    constexpr TPageCacheItem() noexcept = default;

    static constexpr TPageCacheItem FromValidated(TCacheItem cacheItem) noexcept {
        return TPageCacheItem(cacheItem);
    }

    constexpr TCacheItem CacheItem() const noexcept {
        return CacheItem_;
    }

    constexpr ui32 Index() const noexcept {
        return CacheItem_.Index();
    }

    constexpr ui32 Version() const noexcept {
        return CacheItem_.Version();
    }

    constexpr bool Matches(THandleState state) const noexcept {
        return CacheItem_.Matches(state) && state.IsPageKind();
    }

    constexpr explicit operator bool() const noexcept {
        return !CacheItem_.IsNull();
    }

    friend constexpr bool operator==(const TPageCacheItem&, const TPageCacheItem&) noexcept = default;

private:
    explicit constexpr TPageCacheItem(TCacheItem cacheItem) noexcept
        : CacheItem_(cacheItem)
    {
    }

private:
    TCacheItem CacheItem_;
};

class TCollectionCacheItem {
public:
    constexpr TCollectionCacheItem() noexcept = default;

    static constexpr TCollectionCacheItem FromValidated(TCacheItem cacheItem) noexcept {
        return TCollectionCacheItem(cacheItem);
    }

    constexpr TCacheItem CacheItem() const noexcept {
        return CacheItem_;
    }

    constexpr ui64 Raw() const noexcept {
        return CacheItem_.Raw();
    }

    constexpr ui32 Index() const noexcept {
        return CacheItem_.Index();
    }

    constexpr ui32 Version() const noexcept {
        return CacheItem_.Version();
    }

    constexpr bool Matches(THandleState state) const noexcept {
        return CacheItem_.Matches(state) && state.IsCollectionKind();
    }

    constexpr explicit operator bool() const noexcept {
        return !CacheItem_.IsNull();
    }

    friend constexpr bool operator==(const TCollectionCacheItem&, const TCollectionCacheItem&) noexcept = default;

private:
    explicit constexpr TCollectionCacheItem(TCacheItem cacheItem) noexcept
        : CacheItem_(cacheItem)
    {
    }

private:
    TCacheItem CacheItem_;
};

constexpr ui32 AdvanceItemVersion(ui32 version) noexcept {
    return (version + 1) & MaxItemVersion;
}

constexpr ui32 AdvanceColdVersion(ui32 version) noexcept {
    return (version + 1) & MaxItemVersion;
}

// Pre-integration stand-in for the tablet-owned collection-list head.
class TCollectionRegistry : public TThrRefBase {
public:
    std::atomic<ui32> CollectionListHead{ 0 };
};

struct TPage;
class TCacheCollection;

template <class TTraits>
class TSharedCacheImpl;

enum class EPageFetchCompletion {
    Pending,
    Ready,
    Failed,
};

class TPageFetchWaiter : public TThrRefBase {
public:
    virtual void Complete(TPageCacheItem, EPageFetchCompletion) noexcept {
    }

private:
    friend class TPageFetchState;

    std::atomic<TPageFetchWaiter*> Next_{ nullptr };
};

class TPageFetchState : public TThrRefBase {
public:
    explicit TPageFetchState(TPageCacheItem page = {}) noexcept
        : Page_(page)
    {
    }

    ~TPageFetchState() {
        CloseWaiters([this](TIntrusivePtr<TPageFetchWaiter> waiter) {
            waiter->Complete(Page_, EPageFetchCompletion::Failed);
        });
    }

    TPageCacheItem Page() const noexcept {
        return Page_;
    }

    bool Subscribe(TIntrusivePtr<TPageFetchWaiter> waiter) noexcept {
        Y_DEBUG_ABORT_UNLESS(waiter);
        TPageFetchWaiter* raw = waiter.Get();
        raw->Ref();
        TPageFetchWaiter* head = Waiters_.load(std::memory_order_acquire);
        for (;;) {
            if (head == Sealed) {
                raw->UnRef();
                return false;
            }
            raw->Next_.store(head, std::memory_order_relaxed);
            if (Waiters_.compare_exchange_weak(head, raw, std::memory_order_release, std::memory_order_acquire))
            {
                return true;
            }
        }
    }

    template <class TConsumer>
    bool CloseWaiters(TConsumer&& consumer) noexcept {
        TPageFetchWaiter* head = DetachWaiters();
        if (head == Sealed) {
            return false;
        }
        DrainWaiters(head, std::forward<TConsumer>(consumer));
        return true;
    }

    bool Closed() const noexcept {
        return Waiters_.load(std::memory_order_acquire) == Sealed;
    }

private:
    template <class>
    friend class TSharedCacheImpl;

    template <class>
    friend class TSharedCacheImpl;

    void SetReady(NActors::TSharedData&& data) noexcept {
        Y_DEBUG_ABORT_UNLESS(data && Completion_.load(std::memory_order_relaxed) == EPageFetchCompletion::Pending);
        Data_ = std::move(data);
        Completion_.store(EPageFetchCompletion::Ready, std::memory_order_release);
    }

    void SetFailed() noexcept {
        Y_DEBUG_ABORT_UNLESS(Completion_.load(std::memory_order_relaxed) == EPageFetchCompletion::Pending);
        Completion_.store(EPageFetchCompletion::Failed, std::memory_order_release);
    }

    EPageFetchCompletion Completion() const noexcept {
        return Completion_.load(std::memory_order_acquire);
    }

    NActors::TSharedData TakeData() noexcept {
        Y_DEBUG_ABORT_UNLESS(Completion() == EPageFetchCompletion::Ready && Data_);
        return std::move(Data_);
    }

    TPageFetchWaiter* DetachWaiters() noexcept {
        return Waiters_.exchange(Sealed, std::memory_order_acq_rel);
    }

    template <class TConsumer>
    static void DrainWaiters(TPageFetchWaiter* head, TConsumer&& consumer) noexcept {
        Y_DEBUG_ABORT_UNLESS(head != Sealed);
        while (head) {
            TPageFetchWaiter* next = head->Next_.load(std::memory_order_relaxed);
            head->Next_.store(nullptr, std::memory_order_relaxed);
            consumer(TIntrusivePtr<TPageFetchWaiter>(head, TIntrusivePtr<TPageFetchWaiter>::TNoIncrement{}));
            head = next;
        }
    }

    inline static TPageFetchWaiter* const Sealed = reinterpret_cast<TPageFetchWaiter*>(~uintptr_t{ 0 });

private:
    const TPageCacheItem Page_;
    std::atomic<TPageFetchWaiter*> Waiters_{ nullptr };
    std::atomic<EPageFetchCompletion> Completion_{ EPageFetchCompletion::Pending };
    NActors::TSharedData Data_;
};

struct alignas(64) THandle {
    std::atomic<ui64> State{ 0 };
    std::atomic<ui64> Next{ 0 };

    std::atomic<ui64> Key0{ 0 };
    std::atomic<ui64> Key1{ 0 };

    std::atomic<ui64> Key2OrSize{ 0 };

    union TBody {
        NActors::TSharedData::TBuffer PageBuffer;
        TPageFetchState* Fetch;
        TCacheCollection* Collection;

        constexpr TBody() noexcept {
        }

        ~TBody() noexcept {
        }
    } Body;

    union TMetadata {
        struct TPageMetadata {
            NTable::NPage::EPage Type = NTable::NPage::EPage::Undef;
            ui16 Reserved = 0;
            ui32 Crc32 = 0;
        } Page;

        ui64 PayloadBytes;

        constexpr TMetadata() noexcept
            : PayloadBytes(0)
        {
        }
    } Metadata;

    std::atomic<ui32> ColdVersion{ 0 };
    std::atomic<ui32> NextInOwner{ 0 };
};

static_assert(std::atomic<ui64>::is_always_lock_free);
static_assert(sizeof(std::atomic<ui64>) == sizeof(ui64));
static_assert(std::atomic<ui32>::is_always_lock_free);
static_assert(sizeof(std::atomic<ui32>) == sizeof(ui32));

static_assert(sizeof(TCacheItem) == sizeof(ui64));
static_assert(std::is_trivially_copyable_v<TCacheItem>);
static_assert(sizeof(THandleState) == sizeof(ui64));
static_assert(std::is_trivially_copyable_v<THandleState>);
static_assert(sizeof(TPageCacheItem) == sizeof(ui64));
static_assert(sizeof(TCollectionCacheItem) == sizeof(ui64));
static_assert(sizeof(THandle::TBody) == sizeof(ui64));

static_assert(alignof(THandle) == 64);
static_assert(sizeof(THandle) == 64);
static_assert(offsetof(THandle, State) == 0);
static_assert(offsetof(THandle, Next) == 8);
static_assert(offsetof(THandle, Key0) == 16);
static_assert(offsetof(THandle, Key1) == 24);
static_assert(offsetof(THandle, Key2OrSize) == 32);
static_assert(offsetof(THandle, Body) == 40);
static_assert(offsetof(THandle, Metadata) == 48);
static_assert(offsetof(THandle, ColdVersion) == 56);
static_assert(offsetof(THandle, NextInOwner) == 60);

} // namespace NKikimr::NSharedCache
