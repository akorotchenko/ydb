#include "shared_cache_space.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>

#include <atomic>
#include <thread>

namespace NKikimr::NSharedCache {

class TSharedCacheSpaceTestAccess {
public:
    static THandle* Handles(const TSharedCacheSpace& space) noexcept {
        return space.NewestView().Handles;
    }

    static std::atomic<ui64>* Buckets(const TSharedCacheSpace& space) noexcept {
        return space.NewestView().Buckets;
    }

    static std::atomic<ui64>* HotSlots(const TSharedCacheSpace& space) noexcept {
        return space.NewestView().HotSlots;
    }

    static std::atomic<ui64>* ColdSlots(const TSharedCacheSpace& space) noexcept {
        return space.NewestView().ColdSlots;
    }

    static std::atomic<ui64>* KeepColdSlots(const TSharedCacheSpace& space) noexcept {
        return space.NewestView().KeepColdSlots;
    }

    static std::atomic<ui64>* FreeSlots(const TSharedCacheSpace& space) noexcept {
        return space.NewestView().FreeSlots;
    }

    static const TSpaceView& CurrentView(const TSharedCacheSpace& space) noexcept {
        return space.SpaceView(space.CurrentSpaceState());
    }

    static TSpaceState CurrentSpaceState(const TSharedCacheSpace& space) noexcept {
        return space.CurrentSpaceState();
    }

    static const TSpaceView& SpaceView(const TSharedCacheSpace& space, TSpaceState state) noexcept {
        return space.SpaceView(state);
    }

    static TBucketResizeState BucketResizeState(const TSharedCacheSpace& space) noexcept {
        return space.BucketResizeState();
    }

    static const TSharedCacheCapacity& ReservedCapacity(const TSharedCacheSpace& space) noexcept {
        return space.ReservedCapacity_;
    }

    static ui64 BucketCount(const TSharedCacheSpace& space) noexcept {
        const TSpaceState state = space.CurrentSpaceState();
        return state.Resizing() ? SharedCacheBucketCount(space.BucketResizeState().OldAddressBits())
                                : state.BucketCount();
    }

    static ui64 FreeCount(const TSharedCacheSpace& space) noexcept {
        return space.FreeRingCursor_->Count();
    }

    static TSpaceHazard& Hazard(const TSharedCacheSpace& space, ui32 index) noexcept {
        return space.Hazards_[index];
    }

    static ui32 HazardCount(const TSharedCacheSpace& space) noexcept {
        return space.HazardCount_;
    }

    static bool AdvanceSpaceGeneration(TSharedCacheSpace& space) noexcept {
        return space.AdvanceSpaceGeneration();
    }

    template <class TCallback>
    static ui32 TryAllocateHandle(TSharedCacheSpace& space, TSpaceOperation& operation, TCallback& callback) noexcept {
        return space.TryAllocateHandleWithHook(operation, {
                                                              .Context = &callback,
                                                              .Function = [](void* context) noexcept {
                                                                  (*static_cast<TCallback*>(context))();
                                                              },
                                                          });
    }

    template <class TCallback>
    static bool ReturnFreeHandle(
        TSharedCacheSpace& space, TSpaceOperation& operation, ui32 index, ui32 version, TCallback& callback) noexcept {
        return space.ReturnFreeHandleWithHook(operation, index, version,
            {
                .Context = &callback,
                .Function = [](void* context) noexcept {
                    (*static_cast<TCallback*>(context))();
                },
            });
    }

    template <class TCallback>
    static bool PublishMigrationView(TSharedCacheSpace& space, TTransition& transition, TCallback& callback) noexcept {
        return space.PublishMigrationViewWithHook(transition, {
                                                                  .Context = &callback,
                                                                  .Function = [](void* context) noexcept {
                                                                      (*static_cast<TCallback*>(context))();
                                                                  },
                                                              });
    }

    template <class TCallback>
    static bool TryReleaseTransition(TSharedCacheSpace& space, TTransition& transition, TCallback& callback) noexcept {
        return space.TryReleaseTransitionWithHook(
            transition, {
                            .Context = &callback,
                            .Function = [](void* context, ESpaceMap mapping, const void* begin, size_t size) noexcept {
                                return (*static_cast<TCallback*>(context))(mapping, begin, size);
                            },
                        });
    }

    static bool FinalDrain(TSharedCacheSpace& space, TTransition& transition) noexcept {
        bool sawWord = false;
        const bool finished = space.FinalDrain(
            transition,
            [&](ui32, ui64) noexcept {
                sawWord = true;
            },
            [&](ui64) noexcept {
                sawWord = true;
            },
            [&](ui64) noexcept {
                sawWord = true;
            });
        UNIT_ASSERT(!sawWord);
        return finished;
    }
};

void CompleteEmptyBucketResize(TSharedCacheSpace& space, ui8 targetAddressBits) {
    UNIT_ASSERT(space.BeginBucketResize(targetAddressBits));
    for (;;) {
        TBucketResizeState state = TSharedCacheSpaceTestAccess::BucketResizeState(space);
        const ui32 pairCount = state.PairCount();
        UNIT_ASSERT(space.AdvanceBucketResizePhase(EBucketResizePhase::Activated));
        UNIT_ASSERT(space.PublishBucketResizeCursor(state.Cursor() + 1));
        if (state.Direction() == EBucketResize::Shrinking) {
            UNIT_ASSERT(space.AdvanceBucketResizePhase(EBucketResizePhase::Closed));
        }
        state = TSharedCacheSpaceTestAccess::BucketResizeState(space);
        if (state.Cursor() == pairCount) {
            break;
        }
        UNIT_ASSERT(space.PrepareNextBucketResizePair());
    }
    UNIT_ASSERT(space.FinishBucketResize());
}

void CompleteGrowthAppend(TSharedCacheSpace& space, TTransition& transition) {
    while (transition.Phase() == ETransitionPhase::AppendGrowth) {
        UNIT_ASSERT(space.AppendFreeHandles(transition));
    }
    UNIT_ASSERT(transition.Phase() == ETransitionPhase::FinalDrain);
}

void CompleteFinalDrain(TSharedCacheSpace& space, TTransition& transition) {
    while (transition.Phase() == ETransitionPhase::FinalDrain) {
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::FinalDrain(space, transition));
    }
    UNIT_ASSERT(transition.Phase() == ETransitionPhase::Release);
}

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

    Y_UNIT_TEST(Footprint) {
        TSharedCacheCapacity footprint;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 128, 3, footprint));
        UNIT_ASSERT_VALUES_EQUAL(footprint.AddressBits, 5);
        UNIT_ASSERT_VALUES_EQUAL(footprint.HandleCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(footprint.BucketCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(footprint.HotSlotCount(), 24);
        UNIT_ASSERT_VALUES_EQUAL(footprint.KeepColdSlotCount(), 16);

        const ui64 expectedStatic = 128 + 32 * sizeof(THandle) + 32 * sizeof(std::atomic<ui64>) +
                                    24 * sizeof(std::atomic<ui64>) + 32 * sizeof(std::atomic<ui64>) +
                                    16 * sizeof(std::atomic<ui64>) + 32 * sizeof(std::atomic<ui64>) +
                                    3 * sizeof(TSpaceHazard);
        const ui64 expectedPayload = 30 * (4096 + NActors::TSharedData::OverheadSize);
        UNIT_ASSERT_VALUES_EQUAL(footprint.StaticBytes, expectedStatic);
        UNIT_ASSERT_VALUES_EQUAL(footprint.MinimumPayloadBytes, expectedPayload);
        UNIT_ASSERT_VALUES_EQUAL(footprint.TotalBytes, expectedStatic + expectedPayload);
    }

    Y_UNIT_TEST(KeepColdRingHasHalfTheHandleSlots) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, 1);
        UNIT_ASSERT(space);
        auto hazard = space->BindThreadHazard(0);
        auto operation = space->BeginOperation();
        auto ring = operation.KeepCold();
        UNIT_ASSERT_VALUES_EQUAL(ring.Capacity, 16);

        for (ui64 word = 1; word <= ring.Capacity; ++word) {
            UNIT_ASSERT_VALUES_EQUAL(ring.PutAndPop(word), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(ring.PutAndPop(ring.Capacity + 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(ring.Pop(), 2);
    }

    Y_UNIT_TEST(KeepColdTailIsDrainedOnPhysicalShrink) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, large));
        auto space = TSharedCacheSpace::Create(large, large, 0);
        UNIT_ASSERT(space);
        const ui64 word = TCacheItem::Make(1, 2).Raw();
        TSharedCacheSpaceTestAccess::KeepColdSlots(*space)[large.KeepColdSlotCount() - 1].store(word);

        TTransition shrink;
        UNIT_ASSERT(space->PrepareTransition(small, shrink));
        UNIT_ASSERT(space->PublishMigrationView(shrink));
        UNIT_ASSERT(space->TryDrainTransition(shrink));
        CompleteEmptyBucketResize(*space, small.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(shrink));
        UNIT_ASSERT(space->TryDrainTransition(shrink));

        ui64 drained = 0;
        UNIT_ASSERT(space->FinalDrain(
            shrink,
            [](ui32, ui64) noexcept {
            },
            [](ui64) noexcept {
            },
            [&](ui64 raw) noexcept {
                drained = raw;
            }));
        UNIT_ASSERT_VALUES_EQUAL(drained, word);
        UNIT_ASSERT(shrink.Phase() == ETransitionPhase::Release);
    }

    Y_UNIT_TEST(FootprintRejectsInvalidInput) {
        TSharedCacheCapacity footprint;
        UNIT_ASSERT(!TryCalculateSharedCacheFootprint(4, 4096, 0, 0, footprint));
        UNIT_ASSERT(!TryCalculateSharedCacheFootprint(33, 4096, 0, 0, footprint));
        UNIT_ASSERT(!TryCalculateSharedCacheFootprint(5, 0, 0, 0, footprint));
        UNIT_ASSERT(!TryCalculateSharedCacheFootprint(5, Max<ui64>(), 0, 0, footprint));
    }

    Y_UNIT_TEST(CapacitySelectsLargestFit) {
        TSharedCacheCapacity fiveBits;
        TSharedCacheCapacity sixBits;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 2, fiveBits));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 2, sixBits));

        TSharedCacheCapacity exact;
        UNIT_ASSERT(TryCalculateSharedCacheCapacity(fiveBits.TotalBytes, 4096, 0, 2, exact));
        UNIT_ASSERT_VALUES_EQUAL(exact.AddressBits, 5);

        TSharedCacheCapacity next;
        UNIT_ASSERT(TryCalculateSharedCacheCapacity(sixBits.TotalBytes - 1, 4096, 0, 2, next));
        UNIT_ASSERT_VALUES_EQUAL(next.AddressBits, 5);
        UNIT_ASSERT(TryCalculateSharedCacheCapacity(sixBits.TotalBytes, 4096, 0, 2, next));
        UNIT_ASSERT_VALUES_EQUAL(next.AddressBits, 6);

        UNIT_ASSERT(!TryCalculateSharedCacheCapacity(fiveBits.TotalBytes - 1, 4096, 0, 2, next));
    }

    Y_UNIT_TEST(CapacityResizeDeltaBits) {
        TSharedCacheCapacity current;
        TSharedCacheCapacity doubled;
        TSharedCacheCapacity quadrupled;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, doubled));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 0, quadrupled));
        auto space = TSharedCacheSpace::Create(current, quadrupled, 0);
        UNIT_ASSERT(space);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(doubled, transition));
        UNIT_ASSERT_VALUES_EQUAL(transition.ResizeDeltaBits(), 1);
        UNIT_ASSERT(space->AbandonTransition(transition));

        UNIT_ASSERT(space->PrepareTransition(quadrupled, transition));
        UNIT_ASSERT_VALUES_EQUAL(transition.ResizeDeltaBits(), 2);
        UNIT_ASSERT(space->AbandonTransition(transition));
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

    Y_UNIT_TEST(RegisteredThreadHazard) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 2, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, 2);
        UNIT_ASSERT(space);

        {
            auto binding = space->BindThreadHazard(1);
            auto operation = space->BeginOperation();
            UNIT_ASSERT(SpaceHazardIsActive(TSharedCacheSpaceTestAccess::Hazard(*space, 1).State.load()));
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardGeneration(TSharedCacheSpaceTestAccess::Hazard(*space, 1).State.load()),
                operation.EntryGeneration());
        }
        UNIT_ASSERT(!SpaceHazardIsActive(TSharedCacheSpaceTestAccess::Hazard(*space, 1).State.load()));
    }

    Y_UNIT_TEST(HotPolicyResizeDoesNotUseHazards) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, 1);
        UNIT_ASSERT(space);

        std::atomic<bool> active = false;
        std::atomic<bool> resume = false;
        ui64 observedHotSlots = 0;
        std::thread worker([&] {
            auto binding = space->BindThreadHazard(0);
            auto operation = space->BeginOperation();
            observedHotSlots = operation.EffectiveHotSlots();
            active.store(true, std::memory_order_release);
            while (!resume.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        });

        while (!active.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        THotResize resize;
        const ui32 target = static_cast<ui32>(capacity.HotSlotCount() - 1);
        UNIT_ASSERT(space->BeginHotResize(target, resize));
        UNIT_ASSERT_VALUES_EQUAL(space->EffectiveHotSlots(), target);
        while (resize.Phase() == EHotResizePhase::Cut) {
            ui32 level;
            ui64 raw;
            UNIT_ASSERT(space->DrainHotResize(resize, level, raw));
            UNIT_ASSERT_VALUES_EQUAL(raw, 0);
        }
        UNIT_ASSERT(resize.Phase() == EHotResizePhase::Idle);

        resume.store(true, std::memory_order_release);
        worker.join();
        UNIT_ASSERT_VALUES_EQUAL(observedHotSlots, capacity.HotSlotCount());
    }

    Y_UNIT_TEST(NestedOperationsShareHazard) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, 1);
        UNIT_ASSERT(space);
        auto binding = space->BindThreadHazard(0);

        auto outer = space->BeginOperation();
        UNIT_ASSERT_VALUES_EQUAL(
            SpaceHazardActiveCount(TSharedCacheSpaceTestAccess::Hazard(*space, 0).State.load()), 1);
        UNIT_ASSERT_VALUES_EQUAL(outer.EntryGeneration(), 0);
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
        {
            auto nested = space->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(
                SpaceHazardActiveCount(TSharedCacheSpaceTestAccess::Hazard(*space, 0).State.load()), 2);
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardGeneration(TSharedCacheSpaceTestAccess::Hazard(*space, 0).State.load()),
                outer.EntryGeneration());
            UNIT_ASSERT_VALUES_EQUAL(
                nested.EntryGeneration(), TSharedCacheSpaceTestAccess::CurrentSpaceState(*space).Generation());
            UNIT_ASSERT_VALUES_EQUAL(nested.View().Generation, nested.EntryGeneration());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            SpaceHazardActiveCount(TSharedCacheSpaceTestAccess::Hazard(*space, 0).State.load()), 1);
    }

    Y_UNIT_TEST(InitialMappingsAndFreeContents) {
        TSharedCacheCapacity current;
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 3, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 3, reserved));

        auto space = TSharedCacheSpace::Create(current, reserved, 3);
        UNIT_ASSERT(space);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, 32);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, 32);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::ReservedCapacity(*space).HandleCount(), 64);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::BucketCount(*space), 32);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::ReservedCapacity(*space).BucketCount(), 64);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HotSlotCount, 24);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).KeepCold().Capacity, 16);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), 30);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::HazardCount(*space), 3);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::Hazard(*space, 0).State.load(), 0);

        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(TSharedCacheSpaceTestAccess::Handles(*space)) % 64, 0);
        for (ui32 index = 0; index < current.HandleCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::Handles(*space)[index].State.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::ColdSlots(*space)[index].load(), 0);
        }
        for (ui32 index = 0; index < current.BucketCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::Buckets(*space)[index].load(), 0);
        }
        for (ui32 index = 0; index < current.HotSlotCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::HotSlots(*space)[index].load(), 0);
        }
        for (ui32 index = 0; index < current.KeepColdSlotCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::KeepColdSlots(*space)[index].load(), 0);
        }
        for (ui32 index = 2; index < current.HandleCount(); ++index) {
            const auto cacheItem =
                TCacheItem::FromRaw(TSharedCacheSpaceTestAccess::FreeSlots(*space)[index - 2].load());
            UNIT_ASSERT_VALUES_EQUAL(cacheItem.Version(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cacheItem.Index(), index);
            UNIT_ASSERT(!cacheItem.IsFrozen());
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeSlots(*space)[current.HandleCount() - 2].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeSlots(*space)[current.HandleCount() - 1].load(), 0);
    }

    Y_UNIT_TEST(RejectsInvalidMappingConfiguration) {
        TSharedCacheCapacity current;
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, reserved));

        auto invalid = current;
        invalid.AddressBits = reserved.AddressBits + 1;
        UNIT_ASSERT(!TSharedCacheSpace::Create(invalid, reserved, 0));

        invalid = current;
        invalid.AddressBits = MinSharedCacheAddressBits - 1;
        UNIT_ASSERT(!TSharedCacheSpace::Create(invalid, reserved, 0));
    }

    Y_UNIT_TEST(AllocateAndReturnItem) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, 1);
        UNIT_ASSERT(space);
        auto hazard = space->BindThreadHazard(0);
        auto operation = space->BeginOperation();

        TVector<bool> seen(capacity.HandleCount(), false);
        for (ui32 count = 0; count < capacity.HandleCount() - 2; ++count) {
            const auto index = operation.TryAllocateHandle();
            UNIT_ASSERT(index);
            UNIT_ASSERT(index >= 2 && index < capacity.HandleCount());
            UNIT_ASSERT(!seen[index]);
            seen[index] = true;
            const auto state = THandleState::FromRaw(TSharedCacheSpaceTestAccess::Handles(*space)[index].State.load());
            UNIT_ASSERT(state.IsBegin());
            UNIT_ASSERT_VALUES_EQUAL(state.Version(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), 0);
        UNIT_ASSERT(!operation.TryAllocateHandle());

        constexpr ui32 ReturnedIndex = 7;
        auto state = THandleState::FromRaw(TSharedCacheSpaceTestAccess::Handles(*space)[ReturnedIndex].State.load());
        TSharedCacheSpaceTestAccess::Handles(*space)[ReturnedIndex].State.store(
            state.WithState(EHandleState::Free).Raw(), std::memory_order_release);
        UNIT_ASSERT(operation.ReturnFreeHandle(ReturnedIndex, state.Version()));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), 1);
        const auto allocatedAgain = operation.TryAllocateHandle();
        UNIT_ASSERT(allocatedAgain);
        UNIT_ASSERT_VALUES_EQUAL(allocatedAgain, ReturnedIndex);
    }

    Y_UNIT_TEST(ConcurrentAllocationIsUnique) {
        constexpr ui32 HandleCount = 1024;
        constexpr ui32 ThreadCount = 8;

        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(10, 4096, 0, ThreadCount, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, ThreadCount);
        UNIT_ASSERT(space);

        TVector<TVector<ui32>> allocated(ThreadCount);
        TVector<std::thread> threads;
        for (ui32 thread = 0; thread < ThreadCount; ++thread) {
            threads.emplace_back([&, thread] {
                auto hazard = space->BindThreadHazard(thread);
                auto operation = space->BeginOperation();
                for (;;) {
                    const auto index = operation.TryAllocateHandle();
                    if (!index) {
                        return;
                    }
                    allocated[thread].push_back(index);
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }

        TVector<bool> seen(HandleCount, false);
        ui32 count = 0;
        for (const auto& indices : allocated) {
            for (ui32 index : indices) {
                UNIT_ASSERT(index >= 2 && index < HandleCount);
                UNIT_ASSERT(!seen[index]);
                seen[index] = true;
                ++count;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(count, HandleCount - 2);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), 0);
    }

    Y_UNIT_TEST(AllocateReroutesAfterGenerationChange) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, 1);
        UNIT_ASSERT(space);
        auto hazard = space->BindThreadHazard(0);
        auto operation = space->BeginOperation();

        bool advanced = false;
        auto advance = [&]() noexcept {
            if (!advanced) {
                advanced = true;
                UNIT_ASSERT(TSharedCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
            }
        };

        TVector<bool> seen(capacity.HandleCount(), false);
        const auto first = TSharedCacheSpaceTestAccess::TryAllocateHandle(*space, operation, advance);
        UNIT_ASSERT(first);
        seen[first] = true;
        ui32 count = 1;
        while (const auto index = operation.TryAllocateHandle()) {
            UNIT_ASSERT(!seen[index]);
            seen[index] = true;
            ++count;
        }

        UNIT_ASSERT(advanced);
        UNIT_ASSERT_VALUES_EQUAL(count, capacity.HandleCount() - 2);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), 0);
    }

    Y_UNIT_TEST(ReturnReroutesAfterGenerationChange) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, 1);
        UNIT_ASSERT(space);
        auto hazard = space->BindThreadHazard(0);
        auto operation = space->BeginOperation();

        const auto returned = operation.TryAllocateHandle();
        UNIT_ASSERT(returned);
        THandleState state = THandleState::FromRaw(TSharedCacheSpaceTestAccess::Handles(*space)[returned].State.load());
        TSharedCacheSpaceTestAccess::Handles(*space)[returned].State.store(
            state.WithState(EHandleState::Free).Raw(), std::memory_order_release);

        bool advanced = false;
        auto advance = [&]() noexcept {
            if (!advanced) {
                advanced = true;
                UNIT_ASSERT(TSharedCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
            }
        };
        UNIT_ASSERT(
            TSharedCacheSpaceTestAccess::ReturnFreeHandle(*space, operation, returned, state.Version(), advance));
        UNIT_ASSERT(advanced);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), capacity.HandleCount() - 2);

        TVector<bool> seen(capacity.HandleCount(), false);
        ui32 count = 0;
        while (const auto index = operation.TryAllocateHandle()) {
            UNIT_ASSERT(!seen[index]);
            seen[index] = true;
            ++count;
        }
        UNIT_ASSERT(seen[returned]);
        UNIT_ASSERT_VALUES_EQUAL(count, capacity.HandleCount() - 2);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), 0);
    }

    Y_UNIT_TEST(CapacityGrowthAndShrinkAliasTheLivePrefix) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));

        auto space = TSharedCacheSpace::Create(small, large, 1);
        UNIT_ASSERT(space);
        auto binding = space->BindThreadHazard(0);
        THandle* smallHandles = TSharedCacheSpaceTestAccess::Handles(*space);
        smallHandles[2].Key0.store(17, std::memory_order_relaxed);

        TTransition growth;
        UNIT_ASSERT(space->PrepareTransition(large, growth));
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::Prepare);
        UNIT_ASSERT_VALUES_EQUAL(growth.ResizeDeltaBits(), 1);
        {
            auto oldOperation = space->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().Generation, 0);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().Handles, smallHandles);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().HandleCount, small.HandleCount());
            UNIT_ASSERT(space->PublishMigrationView(growth));
            UNIT_ASSERT(growth.Phase() == ETransitionPhase::MigrationDrain);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(
                TSharedCacheSpaceTestAccess::Handles(*space)[2].Key0.load(std::memory_order_relaxed), 17);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().Handles, smallHandles);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().HandleCount, small.HandleCount());
            TSharedCacheSpaceTestAccess::Handles(*space)[2].Key1.store(29, std::memory_order_relaxed);
            UNIT_ASSERT_VALUES_EQUAL(smallHandles[2].Key1.load(std::memory_order_relaxed), 29);
            UNIT_ASSERT(!space->TryDrainTransition(growth));
        }
        UNIT_ASSERT(space->TryDrainTransition(growth));
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::Migrate);
        {
            auto migrationOperation = space->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().Generation, 1);
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().Handles, TSharedCacheSpaceTestAccess::Handles(*space));
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().HandleCount, small.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().AllocationLimit, small.HandleCount());
            CompleteEmptyBucketResize(*space, large.AddressBits);
            UNIT_ASSERT(space->PublishFinalView(growth));
            CompleteGrowthAppend(*space, growth);
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().HandleCount, small.HandleCount());
            UNIT_ASSERT(!space->TryDrainTransition(growth));
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, large.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, large.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), large.HandleCount() - 2);
        UNIT_ASSERT(space->TryDrainTransition(growth));
        CompleteFinalDrain(*space, growth);
        UNIT_ASSERT(space->TryReleaseTransition(growth));
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::Commit);
        UNIT_ASSERT(space->CommitTransition(growth));
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::Idle);

        TTransition shrink;
        UNIT_ASSERT(space->PrepareTransition(small, shrink));
        THandle* largeHandles = TSharedCacheSpaceTestAccess::Handles(*space);
        UNIT_ASSERT(space->PublishMigrationView(shrink));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, small.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, large.HandleCount());
        {
            auto migrationOperation = space->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().Generation, 3);
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().Handles, largeHandles);
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().HandleCount, large.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().AllocationLimit, small.HandleCount());
            UNIT_ASSERT(space->TryDrainTransition(shrink));
            CompleteEmptyBucketResize(*space, small.AddressBits);
            UNIT_ASSERT(space->PublishFinalView(shrink));
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().Handles, largeHandles);
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().HandleCount, large.HandleCount());
            UNIT_ASSERT(!space->TryDrainTransition(shrink));
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheSpaceTestAccess::Handles(*space)[2].Key1.load(std::memory_order_relaxed), 29);
        TSharedCacheSpaceTestAccess::Handles(*space)[2].Key0.store(41, std::memory_order_relaxed);
        UNIT_ASSERT_VALUES_EQUAL(largeHandles[2].Key0.load(std::memory_order_relaxed), 41);
        UNIT_ASSERT(space->TryDrainTransition(shrink));
        CompleteFinalDrain(*space, shrink);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), small.HandleCount() - 2);
        UNIT_ASSERT(space->TryReleaseTransition(shrink));
        UNIT_ASSERT(space->CommitTransition(shrink));
    }

    Y_UNIT_TEST(CapacityGrowthAddsEveryNewHandleToFree) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));

        auto space = TSharedCacheSpace::Create(small, large, 1);
        UNIT_ASSERT(space);
        auto binding = space->BindThreadHazard(0);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(large, transition));
        UNIT_ASSERT(space->PublishMigrationView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteEmptyBucketResize(*space, large.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(transition));
        CompleteGrowthAppend(*space, transition);
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteFinalDrain(*space, transition);
        UNIT_ASSERT(space->TryReleaseTransition(transition));
        UNIT_ASSERT(space->CommitTransition(transition));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), large.HandleCount() - 2);

        auto operation = space->BeginOperation();
        TVector<bool> seen(large.HandleCount(), false);
        ui64 count = 0;
        while (const ui32 index = operation.TryAllocateHandle()) {
            UNIT_ASSERT(index >= 2 && index < large.HandleCount());
            UNIT_ASSERT(!seen[index]);
            seen[index] = true;
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, large.HandleCount() - 2);
        for (ui64 index = 2; index < large.HandleCount(); ++index) {
            UNIT_ASSERT(seen[index]);
        }
    }

    Y_UNIT_TEST(CapacityGrowthWorkIsBatched) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(10, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(11, 4096, 0, 1, large));

        auto space = TSharedCacheSpace::Create(small, large, 1);
        UNIT_ASSERT(space);
        auto binding = space->BindThreadHazard(0);

        TTransition canceled;
        UNIT_ASSERT(space->PrepareTransition(large, canceled));
        UNIT_ASSERT(space->PublishMigrationView(canceled));
        UNIT_ASSERT(canceled.Phase() == ETransitionPhase::InitializeGrowth);
        UNIT_ASSERT(space->AbandonTransition(canceled));
        UNIT_ASSERT(canceled.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(large, transition));
        ui32 initializationCalls = 0;
        do {
            UNIT_ASSERT(space->PublishMigrationView(transition));
            ++initializationCalls;
        } while (transition.Phase() == ETransitionPhase::InitializeGrowth);
        UNIT_ASSERT(initializationCalls > 1);
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::MigrationDrain);

        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteEmptyBucketResize(*space, large.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(transition));
        ui32 appendCalls = 1;
        while (transition.Phase() == ETransitionPhase::AppendGrowth) {
            UNIT_ASSERT(space->AppendFreeHandles(transition));
            ++appendCalls;
        }
        UNIT_ASSERT(appendCalls > 1);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), large.HandleCount() - 2);

        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteFinalDrain(*space, transition);
        UNIT_ASSERT(space->TryReleaseTransition(transition));
        UNIT_ASSERT(space->CommitTransition(transition));
    }

    Y_UNIT_TEST(CapacityShrinkDrainsWrappedFreeRing) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));

        auto space = TSharedCacheSpace::Create(large, large, 1);
        UNIT_ASSERT(space);
        auto binding = space->BindThreadHazard(0);
        {
            auto operation = space->BeginOperation();
            for (ui64 iteration = 0; iteration < large.HandleCount() * 3; ++iteration) {
                const ui32 index = operation.TryAllocateHandle();
                UNIT_ASSERT(index >= 2 && index < large.HandleCount());
                THandle& handle = operation.Handles()[index];
                const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
                UNIT_ASSERT(state.IsBegin() && state.Refs() == 0);
                handle.State.store(state.WithState(EHandleState::Free).Raw(), std::memory_order_release);
                UNIT_ASSERT(operation.ReturnFreeHandle(index, state.Version()));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), large.HandleCount() - 2);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(small, transition));
        UNIT_ASSERT(space->PublishMigrationView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteEmptyBucketResize(*space, small.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteFinalDrain(*space, transition);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), small.HandleCount() - 2);
        UNIT_ASSERT(space->TryReleaseTransition(transition));
        UNIT_ASSERT(space->CommitTransition(transition));

        auto operation = space->BeginOperation();
        TVector<bool> seen(small.HandleCount(), false);
        ui64 count = 0;
        while (const ui32 index = operation.TryAllocateHandle()) {
            UNIT_ASSERT(index >= 2 && index < small.HandleCount());
            UNIT_ASSERT(!seen[index]);
            seen[index] = true;
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, small.HandleCount() - 2);
    }

    Y_UNIT_TEST(MigrationViewPublicationOrdersRestrictionBeforeGeneration) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, large));
        auto space = TSharedCacheSpace::Create(large, large, 0);
        UNIT_ASSERT(space);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(small, transition));
        const ui64 generation = TSharedCacheSpaceTestAccess::CurrentSpaceState(*space).Generation();
        const TSpaceState publishedState =
            TSharedCacheSpaceTestAccess::CurrentSpaceState(*space).WithGeneration(generation + 1);
        bool called = false;
        auto check = [&]() noexcept {
            called = true;
            const TSpaceView& publishedView = TSharedCacheSpaceTestAccess::SpaceView(*space, publishedState);
            UNIT_ASSERT_VALUES_EQUAL(publishedView.AllocationLimit, small.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(publishedView.HandleCount, large.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(
                TSharedCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, large.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, large.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentSpaceState(*space).Generation(), generation);
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::PublishMigration);
        };
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::PublishMigrationView(*space, transition, check));
        UNIT_ASSERT(called);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentSpaceState(*space).Generation(), generation + 1);
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::MigrationDrain);
    }

    Y_UNIT_TEST(CapacityReleaseRetriesFailedStage) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(10, 4096, 0, 0, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(11, 4096, 0, 0, large));
        auto space = TSharedCacheSpace::Create(large, large, 0);
        UNIT_ASSERT(space);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(small, transition));
        UNIT_ASSERT(space->PublishMigrationView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteEmptyBucketResize(*space, small.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::FinalDrain(*space, transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::FinalDrain);
        CompleteFinalDrain(*space, transition);
        UNIT_ASSERT(transition.ReleaseStage() == ESpaceMap::Handles);

        ui32 calls = 0;
        auto failHandles = [&](ESpaceMap mapping, const void* begin, size_t size) noexcept {
            ++calls;
            UNIT_ASSERT(begin);
            UNIT_ASSERT(size != 0);
            return mapping != ESpaceMap::Handles;
        };
        UNIT_ASSERT(!TSharedCacheSpaceTestAccess::TryReleaseTransition(*space, transition, failHandles));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::ReleaseRetry);
        UNIT_ASSERT(transition.ReleaseStage() == ESpaceMap::Handles);
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);

        calls = 0;
        auto succeed = [&](ESpaceMap mapping, const void* begin, size_t size) noexcept {
            ++calls;
            if (calls == 1) {
                UNIT_ASSERT(mapping == ESpaceMap::Handles);
            }
            UNIT_ASSERT(begin);
            UNIT_ASSERT(size != 0);
            return true;
        };
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::TryReleaseTransition(*space, transition, succeed));
        UNIT_ASSERT(calls >= 1);
        UNIT_ASSERT(transition.ReleaseStage() == ESpaceMap::Done);
        UNIT_ASSERT(space->CommitTransition(transition));
    }

    Y_UNIT_TEST(CapacityShutdownCancelsPrivateAndAbandonsPublished) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, large));

        auto space = TSharedCacheSpace::Create(small, large, 0);
        UNIT_ASSERT(space);
        THandle* initialHandles = TSharedCacheSpaceTestAccess::Handles(*space);

        TTransition privateTransition;
        UNIT_ASSERT(space->PrepareTransition(large, privateTransition));
        UNIT_ASSERT_VALUES_EQUAL(privateTransition.ResizeDeltaBits(), 1);
        UNIT_ASSERT(space->AbandonTransition(privateTransition));
        UNIT_ASSERT(privateTransition.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::Handles(*space), initialHandles);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());

        TTransition publishedTransition;
        UNIT_ASSERT(space->PrepareTransition(large, publishedTransition));
        UNIT_ASSERT(space->PublishMigrationView(publishedTransition));
        const ui64 generation = TSharedCacheSpaceTestAccess::CurrentSpaceState(*space).Generation();
        UNIT_ASSERT(space->AbandonTransition(publishedTransition));
        UNIT_ASSERT(publishedTransition.Phase() == ETransitionPhase::AbandonedForShutdown);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentSpaceState(*space).Generation(), generation);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());
        UNIT_ASSERT(!space->TryDrainTransition(publishedTransition));
        UNIT_ASSERT(!space->TryReleaseTransition(publishedTransition));
        UNIT_ASSERT(!space->CommitTransition(publishedTransition));
    }
}

} // namespace NKikimr::NSharedCache
