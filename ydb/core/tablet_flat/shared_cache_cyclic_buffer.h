#pragma once

#include "defs.h"

#include <array>
#include <atomic>
#include <utility>

namespace NKikimr::NSharedCache {

struct TRingExchangeResult {
    std::atomic<ui64>* Slot = nullptr;
    ui64 Previous = 0;
};

class TRingCursor {
private:
    struct alignas(64) TCursor {
        explicit TCursor(ui64 value) noexcept
            : Value(value)
        {
        }

        std::atomic<ui64> Value;
    };

public:
    explicit TRingCursor(ui64 initialPosition = 0) noexcept
        : Head_(initialPosition)
        , Tail_(initialPosition)
    {
    }

    TRingCursor(ui64 headPosition, ui64 tailPosition) noexcept
        : Head_(headPosition)
        , Tail_(tailPosition)
    {
    }

    Y_FORCE_INLINE ui64 PutAndPop(std::atomic<ui64>* slots, ui64 capacity, ui64 value) noexcept {
        return PutAndPopImpl<false, std::memory_order_relaxed>(slots, capacity, value, [] {
        }).Previous;
    }

    template <class TOnReserved>
    Y_FORCE_INLINE TRingExchangeResult PutAndPopWithHook(std::atomic<ui64>* slots, ui64 capacity, ui64 value,
        TOnReserved&& onReserved) noexcept(noexcept(std::forward<TOnReserved>(onReserved)())) {
        return PutAndPopImpl<true, std::memory_order_acquire>(
            slots, capacity, value, std::forward<TOnReserved>(onReserved));
    }

    Y_FORCE_INLINE ui64 Pop(std::atomic<ui64>* slots, ui64 capacity) noexcept {
        return PopExchange(slots, capacity).Previous;
    }

    Y_FORCE_INLINE TRingExchangeResult Push(std::atomic<ui64>* slots, ui64 capacity, ui64 value) noexcept {
        Y_DEBUG_ABORT_UNLESS(value != 0);
        Y_DEBUG_ABORT_UNLESS(slots && capacity != 0);
        const ui64 position = Tail_.Value.fetch_add(ui64{ 1 }, std::memory_order_relaxed);
        std::atomic<ui64>* slot = &slots[Map(position, capacity)];
        return { slot, slot->exchange(value, std::memory_order_relaxed) };
    }

    Y_FORCE_INLINE TRingExchangeResult PopExchange(std::atomic<ui64>* slots, ui64 capacity) noexcept {
        Y_DEBUG_ABORT_UNLESS(slots && capacity != 0);
        const ui64 position = Head_.Value.fetch_add(ui64{ 1 }, std::memory_order_relaxed);
        std::atomic<ui64>* slot = &slots[Map(position, capacity)];
        return { slot, slot->exchange(0, std::memory_order_relaxed) };
    }

    template <class TOnReserved>
    ui64 PopWithHook(std::atomic<ui64>* slots, ui64 capacity, TOnReserved&& onReserved) noexcept(
        noexcept(std::forward<TOnReserved>(onReserved)())) {
        return PopImpl<true>(slots, capacity, std::forward<TOnReserved>(onReserved));
    }

private:
    template <bool WithHook, std::memory_order Order, class TOnReserved>
    Y_FORCE_INLINE TRingExchangeResult PutAndPopImpl(std::atomic<ui64>* slots, ui64 capacity, ui64 value,
        TOnReserved&& onReserved) noexcept(noexcept(std::forward<TOnReserved>(onReserved)())) {
        Y_DEBUG_ABORT_UNLESS(value != 0);
        Y_DEBUG_ABORT_UNLESS(slots && capacity != 0);

        const ui64 position = Tail_.Value.fetch_add(ui64{ 1 }, std::memory_order_relaxed);
        Head_.Value.fetch_add(ui64{ 1 }, std::memory_order_relaxed);
        if constexpr (WithHook) {
            std::forward<TOnReserved>(onReserved)();
        }
        std::atomic<ui64>* slot = &slots[Map(position, capacity)];
        return { slot, slot->exchange(value, Order) };
    }

    template <bool WithHook, class TOnReserved>
    Y_FORCE_INLINE ui64 PopImpl(std::atomic<ui64>* slots, ui64 capacity, TOnReserved&& onReserved) noexcept(
        noexcept(std::forward<TOnReserved>(onReserved)())) {
        Y_DEBUG_ABORT_UNLESS(slots && capacity != 0);
        const ui64 position = Head_.Value.fetch_add(ui64{ 1 }, std::memory_order_relaxed);
        if constexpr (WithHook) {
            std::forward<TOnReserved>(onReserved)();
        }
        return slots[Map(position, capacity)].exchange(0, std::memory_order_relaxed);
    }

    static ui32 Map(ui64 position, ui64 capacity) noexcept {
        return static_cast<ui32>(position % capacity);
    }

private:
    TCursor Head_;
    TCursor Tail_;
};

class TS5FifoRings {
public:
    Y_FORCE_INLINE TRingCursor& Hot(ui32 index) noexcept {
        return HotCursors_[index];
    }

    Y_FORCE_INLINE TRingCursor& Cold() noexcept {
        return ColdCursor_;
    }

private:
    std::array<TRingCursor, 4> HotCursors_;
    TRingCursor ColdCursor_;
};

struct TRingView {
    TRingCursor* Cursor = nullptr;
    std::atomic<ui64>* Slots = nullptr;
    ui64 Capacity = 0;

    Y_FORCE_INLINE ui64 PutAndPop(ui64 value) const noexcept {
        Y_DEBUG_ABORT_UNLESS(Cursor);
        return Cursor->PutAndPop(Slots, Capacity, value);
    }

    template <class TOnReserved>
    Y_FORCE_INLINE TRingExchangeResult PutAndPopWithHook(ui64 value, TOnReserved&& onReserved) const
        noexcept(noexcept(std::forward<TOnReserved>(onReserved)())) {
        Y_DEBUG_ABORT_UNLESS(Cursor);
        return Cursor->PutAndPopWithHook(Slots, Capacity, value, std::forward<TOnReserved>(onReserved));
    }

    Y_FORCE_INLINE ui64 Pop() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Cursor);
        return Cursor->Pop(Slots, Capacity);
    }
};

class TFreeRingCursor {
public:
    TFreeRingCursor(ui64 headPosition, ui64 tailPosition, ui64 count) noexcept
        : Cursor_(headPosition, tailPosition)
        , Count_(count)
    {
    }

    ui64 Count() const noexcept {
        return Count_.load(std::memory_order_relaxed);
    }

    Y_FORCE_INLINE TRingExchangeResult Push(std::atomic<ui64>* slots, ui64 capacity, ui64 value) noexcept {
        return Push(slots, capacity, value, [] {
        });
    }

    template <class TOnExchange>
    Y_FORCE_INLINE TRingExchangeResult Pop(std::atomic<ui64>* slots, ui64 capacity, TOnExchange&& onExchange) noexcept(
        noexcept(std::forward<TOnExchange>(onExchange)())) {
        if (Count() == 0) {
            return {};
        }
        const TRingExchangeResult result = Cursor_.PopExchange(slots, capacity);
        std::forward<TOnExchange>(onExchange)();
        if (result.Previous != 0) {
            RemoveOne();
        }
        return result;
    }

    template <class TOnExchange>
    Y_FORCE_INLINE TRingExchangeResult Push(std::atomic<ui64>* slots, ui64 capacity, ui64 value,
        TOnExchange&& onExchange) noexcept(noexcept(std::forward<TOnExchange>(onExchange)())) {
        Count_.fetch_add(1, std::memory_order_relaxed);
        const TRingExchangeResult result = Cursor_.Push(slots, capacity, value);
        std::forward<TOnExchange>(onExchange)();
        if (result.Previous != 0) {
            RemoveOne();
        }
        return result;
    }

    Y_FORCE_INLINE ui64 Clear(std::atomic<ui64>* slot) noexcept {
        Y_DEBUG_ABORT_UNLESS(slot);
        const ui64 previous = slot->exchange(0, std::memory_order_relaxed);
        if (previous != 0) {
            RemoveOne();
        }
        return previous;
    }

    Y_FORCE_INLINE bool ClearSafe(std::atomic<ui64>* slot, ui64 expected) noexcept {
        Y_DEBUG_ABORT_UNLESS(slot && expected != 0);
        if (!slot->compare_exchange_strong(expected, 0, std::memory_order_relaxed)) {
            return false;
        }
        RemoveOne();
        return true;
    }

private:
    Y_FORCE_INLINE void RemoveOne() noexcept {
        const ui64 previous = Count_.fetch_sub(1, std::memory_order_relaxed);
        Y_DEBUG_ABORT_UNLESS(previous > 0);
    }

private:
    TRingCursor Cursor_;
    std::atomic<ui64> Count_;
};

struct TFreeRingView {
    TFreeRingCursor* Cursor = nullptr;
    std::atomic<ui64>* Slots = nullptr;
    ui64 Capacity = 0;

    ui64 Count() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Cursor);
        return Cursor->Count();
    }

    template <class TOnExchange>
    Y_FORCE_INLINE TRingExchangeResult Pop(TOnExchange&& onExchange) const
        noexcept(noexcept(std::forward<TOnExchange>(onExchange)())) {
        Y_DEBUG_ABORT_UNLESS(Cursor);
        return Cursor->Pop(Slots, Capacity, std::forward<TOnExchange>(onExchange));
    }

    template <class TOnExchange>
    Y_FORCE_INLINE TRingExchangeResult Push(ui64 value, TOnExchange&& onExchange) const
        noexcept(noexcept(std::forward<TOnExchange>(onExchange)())) {
        Y_DEBUG_ABORT_UNLESS(Cursor);
        return Cursor->Push(Slots, Capacity, value, std::forward<TOnExchange>(onExchange));
    }

    Y_FORCE_INLINE TRingExchangeResult Push(ui64 value) const noexcept {
        Y_DEBUG_ABORT_UNLESS(Cursor);
        return Cursor->Push(Slots, Capacity, value);
    }
};

} // namespace NKikimr::NSharedCache
