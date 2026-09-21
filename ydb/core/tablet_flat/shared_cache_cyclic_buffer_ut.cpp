#include "shared_cache_cyclic_buffer.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <array>
#include <atomic>
#include <thread>

namespace NKikimr::NSharedCache {

template <size_t Capacity>
class TTestRing {
public:
    explicit TTestRing(ui64 initialPosition = 0) noexcept
        : Cursor_(initialPosition)
        , View_{ &Cursor_, Slots_.data(), Capacity }
    {
    }

    ui64 PutAndPop(ui64 value) noexcept {
        return View_.PutAndPop(value);
    }

    ui64 Pop() noexcept {
        return View_.Pop();
    }

    template <class TOnReserved>
    ui64 PutAndPopWithHook(ui64 value, TOnReserved&& onReserved) noexcept(
        noexcept(std::forward<TOnReserved>(onReserved)())) {
        return Cursor_.PutAndPopWithHook(Slots_.data(), Capacity, value, std::forward<TOnReserved>(onReserved))
            .Previous;
    }

    template <class TOnReserved>
    ui64 PopWithHook(TOnReserved&& onReserved) noexcept(noexcept(std::forward<TOnReserved>(onReserved)())) {
        return Cursor_.PopWithHook(Slots_.data(), Capacity, std::forward<TOnReserved>(onReserved));
    }

private:
    std::array<std::atomic<ui64>, Capacity> Slots_{};
    TRingCursor Cursor_;
    TRingView View_;
};

Y_UNIT_TEST_SUITE(TSharedCacheCyclicBufferTest) {
    Y_UNIT_TEST(InitialAndWrapAround) {
        TTestRing<4> buffer;

        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(1), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(2), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(3), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(4), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(5), 1);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 2);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 3);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 4);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 5);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 0);
    }

    Y_UNIT_TEST(HolesAreAllowed) {
        TTestRing<4> buffer;

        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(1), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(2), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(3), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(4), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(5), 1);
    }

    Y_UNIT_TEST(ExternalViewsKeepCursorPosition) {
        std::atomic<ui64> first[2] = {};
        std::atomic<ui64> second[4] = {};
        TRingCursor cursor;
        TRingView firstView{ &cursor, first, 2 };
        TRingView secondView{ &cursor, second, 4 };

        UNIT_ASSERT_VALUES_EQUAL(firstView.PutAndPop(11), 0);
        UNIT_ASSERT_VALUES_EQUAL(first[0].load(), 11);
        UNIT_ASSERT_VALUES_EQUAL(secondView.PutAndPop(22), 0);
        UNIT_ASSERT_VALUES_EQUAL(second[1].load(), 22);
        UNIT_ASSERT_VALUES_EQUAL(firstView.PutAndPop(33), 11);
        UNIT_ASSERT_VALUES_EQUAL(first[0].load(), 33);
    }

    Y_UNIT_TEST(PausedProducerAfterReservation) {
        TTestRing<1> buffer;
        std::atomic<bool> reserved = false;
        std::atomic<bool> resume = false;
        ui64 firstResult = 0;

        std::thread first([&] {
            firstResult = buffer.PutAndPopWithHook(1, [&] {
                reserved.store(true, std::memory_order_release);
                while (!resume.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
            });
        });

        while (!reserved.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(2), 0);
        resume.store(true, std::memory_order_release);
        first.join();

        UNIT_ASSERT_VALUES_EQUAL(firstResult, 2);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 1);
    }

    Y_UNIT_TEST(PausedConsumerAfterReservation) {
        TTestRing<1> buffer;
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(1), 0);

        std::atomic<bool> reserved = false;
        std::atomic<bool> resume = false;
        ui64 popResult = 0;

        std::thread consumer([&] {
            popResult = buffer.PopWithHook([&] {
                reserved.store(true, std::memory_order_release);
                while (!resume.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
            });
        });

        while (!reserved.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(2), 1);
        resume.store(true, std::memory_order_release);
        consumer.join();

        UNIT_ASSERT_VALUES_EQUAL(popResult, 2);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 0);
    }

    Y_UNIT_TEST(SafeClearRacesPublishedCount) {
        std::atomic<ui64> slot = 0;
        TFreeRingCursor cursor(0, 0, 0);
        std::atomic<bool> exchanged = false;
        std::atomic<bool> resume = false;

        std::thread producer([&] {
            cursor.Push(&slot, 1, 17, [&] {
                exchanged.store(true, std::memory_order_release);
                while (!resume.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
            });
        });
        while (!exchanged.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        const ui64 expected = 17;
        bool cleared = false;
        std::thread clearer([&] {
            cleared = cursor.ClearSafe(&slot, expected);
        });
        while (slot.load(std::memory_order_acquire) != 0) {
            std::this_thread::yield();
        }
        resume.store(true, std::memory_order_release);
        producer.join();
        clearer.join();

        UNIT_ASSERT(cleared);
        UNIT_ASSERT_VALUES_EQUAL(cursor.Count(), 0);
    }

    Y_UNIT_TEST(CounterWrap) {
        TTestRing<4> buffer(Max<ui64>() - 1);

        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(1), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(2), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(3), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.PutAndPop(4), 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 1);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 2);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 3);
        UNIT_ASSERT_VALUES_EQUAL(buffer.Pop(), 4);
    }

    Y_UNIT_TEST(MultipleProducersAndConsumers) {
        constexpr ui32 ThreadCount = 8;
        constexpr ui32 ValuesPerThread = 1000;
        constexpr ui32 ValueCount = ThreadCount * ValuesPerThread;

        TArrayHolder<std::atomic<ui64>> slots(new std::atomic<ui64>[ValueCount]());
        TRingCursor cursor;
        TRingView buffer{ &cursor, slots.Get(), ValueCount };
        std::atomic<bool> start = false;
        std::atomic<bool> failed = false;
        TVector<std::thread> threads;
        threads.reserve(ThreadCount);

        for (ui32 thread = 0; thread < ThreadCount; ++thread) {
            threads.emplace_back([&, thread] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (ui32 offset = 0; offset < ValuesPerThread; ++offset) {
                    const ui64 value = ui64(thread) * ValuesPerThread + offset + 1;
                    if (buffer.PutAndPop(value) != 0) {
                        failed.store(true, std::memory_order_relaxed);
                    }
                }
            });
        }

        start.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT(!failed.load(std::memory_order_relaxed));

        threads.clear();
        start.store(false, std::memory_order_relaxed);
        TVector<TVector<ui64>> results(ThreadCount);
        for (ui32 thread = 0; thread < ThreadCount; ++thread) {
            threads.emplace_back([&, thread] {
                auto& result = results[thread];
                result.reserve(ValuesPerThread);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (ui32 offset = 0; offset < ValuesPerThread; ++offset) {
                    result.push_back(buffer.Pop());
                }
            });
        }

        start.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }

        TVector<bool> seen(ValueCount + 1, false);
        for (const auto& result : results) {
            for (ui64 value : result) {
                UNIT_ASSERT(value > 0 && value <= ValueCount);
                UNIT_ASSERT(!seen[value]);
                seen[value] = true;
            }
        }
        for (ui32 value = 1; value <= ValueCount; ++value) {
            UNIT_ASSERT(seen[value]);
        }
    }
}

} // namespace NKikimr::NSharedCache
