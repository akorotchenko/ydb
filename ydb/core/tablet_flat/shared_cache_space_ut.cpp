#include "shared_cache_space.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>

#include <atomic>
#include <thread>

namespace NKikimr::NSharedCache {

Y_UNIT_TEST_SUITE(TSharedCacheSpaceTest) {
    Y_UNIT_TEST(SpaceStatePacking) {
        constexpr TSpaceState state = TSpaceState::Make(Max<ui32>(), MaxSharedCacheAddressBits);
        static_assert(state.Generation() == Max<ui32>());
        static_assert(state.BucketMask() == Max<ui32>());
        static_assert(state.BucketCount() == (ui64{ 1 } << 32));
        static_assert(!state.Resizing());

        constexpr TSpaceState resizing = state.BeginResize();
        static_assert(resizing.Resizing());
        static_assert(resizing.BucketMask() == 0);

        constexpr TSpaceState stable = resizing.WithGeneration(7).WithAddressBits(6);
        static_assert(stable.Generation() == 7);
        static_assert(stable.BucketMask() == 63);
        static_assert(stable.BucketCount() == 64);
        static_assert(!stable.Resizing());
    }

    Y_UNIT_TEST(BucketResizeStatePacking) {
        constexpr TBucketResizeState inactive;
        static_assert(!inactive);
        static_assert(inactive.Phase() == EBucketResizePhase::Stable);

        constexpr TBucketResizeState growth = TBucketResizeState::Make(
            EBucketResize::Growing, EBucketResizePhase::CursorPublished, MaxSharedCacheAddressBits - 1, Max<ui32>());
        static_assert(growth);
        static_assert(growth.Direction() == EBucketResize::Growing);
        static_assert(growth.Phase() == EBucketResizePhase::CursorPublished);
        static_assert(growth.OldAddressBits() == MaxSharedCacheAddressBits - 1);
        static_assert(growth.NewAddressBits() == MaxSharedCacheAddressBits);
        static_assert(growth.Cursor() == Max<ui32>());

        constexpr TBucketResizeState shrink = TBucketResizeState::Make(
            EBucketResize::Shrinking, EBucketResizePhase::Closed, MaxSharedCacheAddressBits, 17);
        static_assert(shrink);
        static_assert(shrink.Direction() == EBucketResize::Shrinking);
        static_assert(shrink.Phase() == EBucketResizePhase::Closed);
        static_assert(shrink.OldAddressBits() == MaxSharedCacheAddressBits);
        static_assert(shrink.NewAddressBits() == MaxSharedCacheAddressBits - 1);
        static_assert(shrink.Cursor() == 17);
    }

    Y_UNIT_TEST(ActivityRegistration) {
        std::atomic<ui64> spaceState = TSpaceState::Make(7, 5).Raw();
        TSpaceHazard slot;
        std::array<TSpaceView, 2> views;
        views[7 & 1].Generation = 7;

        {
            auto operation = TSpaceOperation::Begin(spaceState, slot, views);
            UNIT_ASSERT_VALUES_EQUAL(operation.EntryGeneration(), 7);
            UNIT_ASSERT(SpaceHazardIsActive(slot.State.load()));
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardGeneration(slot.State.load()), 7);
        }
        UNIT_ASSERT_VALUES_EQUAL(slot.State.load(), 0);
    }

    Y_UNIT_TEST(ActivityRegistrationRetriesGenerationChange) {
        std::atomic<ui64> spaceState = TSpaceState::Make(10, 5).Raw();
        TSpaceHazard slot;
        std::atomic<bool> incremented = false;
        std::atomic<bool> resume = false;
        std::atomic<ui32> hookCalls = 0;
        ui64 registeredGeneration = 0;
        bool workerSawActive = false;
        std::array<TSpaceView, 2> views;
        views[10 & 1].Generation = 10;
        views[11 & 1].Generation = 11;

        std::thread worker([&] {
            auto operation = TSpaceOperation::BeginWithHook(spaceState, slot, views, [&] {
                if (hookCalls.fetch_add(1, std::memory_order_relaxed) == 0) {
                    incremented.store(true, std::memory_order_release);
                    while (!resume.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                }
            });
            registeredGeneration = operation.EntryGeneration();
            const ui64 state = slot.State.load();
            workerSawActive = SpaceHazardIsActive(state) && SpaceHazardGeneration(state) == 11;
        });

        while (!incremented.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        const ui64 activeState = slot.State.load(std::memory_order_acquire);
        UNIT_ASSERT(SpaceHazardIsActive(activeState));
        UNIT_ASSERT_VALUES_EQUAL(SpaceHazardGeneration(activeState), 10);
        spaceState.store(TSpaceState::Make(11, 5).Raw(), std::memory_order_release);
        UNIT_ASSERT(!SpaceHazardIsDrained(slot.State.load(std::memory_order_acquire), 11));
        resume.store(true, std::memory_order_release);
        worker.join();

        UNIT_ASSERT(hookCalls.load() >= 2);
        UNIT_ASSERT_VALUES_EQUAL(registeredGeneration, 11);
        UNIT_ASSERT(workerSawActive);
        UNIT_ASSERT_VALUES_EQUAL(slot.State.load(), 0);
        UNIT_ASSERT(SpaceHazardIsDrained(slot.State.load(), 11));
    }
}

} // namespace NKikimr::NSharedCache
