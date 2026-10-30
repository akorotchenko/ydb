#include <ydb/core/tablet_flat/cache_thread_test.h>
#include "cache_space.h"
#include "cache_mapping_test.h"

#include <util/generic/scope.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/system/info.h>

#include <atomic>
#include <thread>

namespace NKikimr::NSharedCache {

class TCacheSpaceTestAccess {
public:
    static THandle* Handles(const TCacheSpace& space) noexcept {
        return space.NewestView().Handles;
    }

    static std::atomic<ui64>* Buckets(const TCacheSpace& space) noexcept {
        return space.NewestView().Buckets;
    }

    static std::atomic<ui64>* HotSlots(const TCacheSpace& space) noexcept {
        return space.NewestView().Tiers[ECacheTier::Regular].HotStorage.Slots;
    }

    static std::atomic<ui64>* ColdSlots(const TCacheSpace& space) noexcept {
        return space.NewestView().Tiers[ECacheTier::Regular].ColdStorage.Slots;
    }

    static std::atomic<ui64>* KeepColdSlots(const TCacheSpace& space) noexcept {
        return space.NewestView().Tiers[ECacheTier::Keep].ColdStorage.Slots;
    }

    static std::atomic<ui64>* FreeSlots(const TCacheSpace& space) noexcept {
        return space.NewestView().FreeSlots;
    }

    static const TSpaceView& CurrentView(const TCacheSpace& space) noexcept {
        return space.SpaceView(space.CurrentSpaceState());
    }

    static TCacheMapping& RingMapping(TCacheSpace& space, ESpaceMap mapping) noexcept {
        return space.RingMapping(mapping);
    }

    static EHotLevel HotLevelForSlot(ui64 slot, ui64 count) noexcept {
        return TCacheSpace::HotLevelForSlot(slot, count);
    }

    static TCacheMapping& BucketsMapping(TCacheSpace& space) noexcept {
        return space.BucketsMapping_;
    }

    static TSpaceState CurrentSpaceState(const TCacheSpace& space) noexcept {
        return space.CurrentSpaceState();
    }

    static const TSpaceView& SpaceView(const TCacheSpace& space, TSpaceState state) noexcept {
        return space.SpaceView(state);
    }

    static TBucketResizeState BucketResizeState(const TCacheSpace& space) noexcept {
        return space.BucketResizeState();
    }

    static const TCacheCapacityConfig& ReservedCapacity(const TCacheSpace& space) noexcept {
        return space.ReservedCapacity_;
    }

    static ui64 BucketCount(const TCacheSpace& space) noexcept {
        const TSpaceState state = space.CurrentSpaceState();
        return state.Resizing() ? SharedCacheBucketCount(space.BucketResizeState().OldAddressBits())
                                : state.BucketCount();
    }

    static ui64 FreeCount(const TCacheSpace& space) noexcept {
        return space.FreeRingCursor_->Count();
    }

    static TSpaceHazard& Hazard(const TCacheSpace& space, ui32 index) noexcept {
        return *space.Hazards_[index];
    }

    static ui32 HazardCount(const TCacheSpace& space) noexcept {
        return space.HazardCount_;
    }

    static bool AdvanceSpaceGeneration(TCacheSpace& space) noexcept {
        return space.AdvanceSpaceGeneration();
    }

    static bool TryDrainBefore(TCacheSpace& space, ui32 generation, ui32& cursor) noexcept {
        return space.TryDrainBefore(generation, cursor);
    }

    static void SetHazardDrainNotify(TCacheSpace& space, TCacheNotify notify) noexcept {
        space.HazardDrainNotify_ = notify;
    }

    static TSpaceOperation BeginOnHazard(TCacheSpace& space, ui32 index) noexcept {
        return TSpaceOperation::Begin(space.SpaceState_, Hazard(space, index), space.SpaceViews_);
    }

    template <class TCallback>
    static TSpaceOperation BeginOnHazardWithHook(TCacheSpace& space, ui32 index, TCallback&& callback) noexcept {
        return TSpaceOperation::BeginWithHook(
            space.SpaceState_, Hazard(space, index), space.SpaceViews_, std::forward<TCallback>(callback));
    }

    template <class TCallback>
    static ui32 TryAllocateHandle(TCacheSpace& space, TSpaceOperation& operation, TCallback& callback) noexcept {
        return space.TryAllocateHandleWithHook(operation, {
                                                              .Context = &callback,
                                                              .Function = [](void* context) noexcept {
                                                                  (*static_cast<TCallback*>(context))();
                                                              },
                                                          });
    }

    template <class TCallback>
    static bool ReturnFreeHandle(
        TCacheSpace& space, TSpaceOperation& operation, ui32 index, ui32 version, TCallback& callback) noexcept {
        return space.ReturnFreeHandleWithHook(operation, index, version,
            {
                .Context = &callback,
                .Function = [](void* context) noexcept {
                    (*static_cast<TCallback*>(context))();
                },
            });
    }

    template <class TCallback>
    static bool PublishMigrationView(TCacheSpace& space, TTransition& transition, TCallback& callback) noexcept {
        return space.PublishMigrationViewWithHook(transition, {
                                                                  .Context = &callback,
                                                                  .Function = [](void* context) noexcept {
                                                                      (*static_cast<TCallback*>(context))();
                                                                  },
                                                              });
    }

    template <class TCallback>
    static EPrepareResult PrepareTransition(
        TCacheSpace& space, const TCacheCapacityConfig& target, TTransition& transition, TCallback& callback) noexcept {
        return space.PrepareTransitionWithHook(target, transition,
            {
                .Context = &callback,
                .Function = [](void* context, ESpaceMap mapping) noexcept {
                    return (*static_cast<TCallback*>(context))(mapping);
                },
            });
    }

    template <class TCallback>
    static bool PrepareReceiver(TCacheSpace& space, TTransition& transition, TCallback& callback) noexcept {
        return space.PrepareReceiverWithHook(transition, {
                                                             .Context = &callback,
                                                             .Function = [](void* context, ESpaceMap mapping) noexcept {
                                                                 return (*static_cast<TCallback*>(context))(mapping);
                                                             },
                                                         });
    }

    static void SetGeneration(TCacheSpace& space, ui32 generation) noexcept {
        const auto old = space.CurrentSpaceState();
        space.PrepareView(generation, space.SpaceView(old));
        space.SpaceState_.store(old.WithGeneration(generation).Raw(), std::memory_order_release);
    }

    template <class TCallback>
    static bool TryReleaseTransition(TCacheSpace& space, TTransition& transition, TCallback& callback) noexcept {
        return space.TryReleaseTransitionWithHook(
            transition, {
                            .Context = &callback,
                            .Function = [](void* context, ESpaceMap mapping, const void* begin, size_t size) noexcept {
                                return (*static_cast<TCallback*>(context))(mapping, begin, size);
                            },
                        });
    }

    static bool FinalDrain(TCacheSpace& space, TTransition& transition) noexcept {
        bool sawWord = false;
        const bool finished = space.FinalDrain(
            transition,
            [&](ECacheTier, EHotLevel, ui64) noexcept {
                sawWord = true;
            },
            [&](ECacheTier, ui64) noexcept {
                sawWord = true;
            });
        UNIT_ASSERT(!sawWord);
        return finished;
    }
};

void CompleteEmptyBucketResize(TCacheSpace& space, ui8 targetAddressBits) {
    UNIT_ASSERT(space.BeginBucketResize(targetAddressBits));
    for (;;) {
        TBucketResizeState state = TCacheSpaceTestAccess::BucketResizeState(space);
        const ui32 pairCount = state.PairCount();
        UNIT_ASSERT(space.AdvanceBucketResizePhase(EBucketResizePhase::Activated));
        UNIT_ASSERT(space.PublishBucketResizeCursor(state.Cursor() + 1));
        if (state.Direction() == EBucketResize::Shrinking) {
            UNIT_ASSERT(space.AdvanceBucketResizePhase(EBucketResizePhase::Closed));
        }
        state = TCacheSpaceTestAccess::BucketResizeState(space);
        if (state.Cursor() == pairCount) {
            break;
        }
        UNIT_ASSERT(space.PrepareNextBucketResizePair());
    }
    UNIT_ASSERT(space.FinishBucketResize());
}

void CompleteGrowthAppend(TCacheSpace& space, TTransition& transition) {
    while (transition.Phase() == ETransitionPhase::AppendGrowth) {
        UNIT_ASSERT(space.AppendFreeHandles(transition));
    }
    UNIT_ASSERT(transition.Phase() == ETransitionPhase::FinalDrain);
}

void CompleteFinalDrain(TCacheSpace& space, TTransition& transition) {
    while (transition.Phase() == ETransitionPhase::FinalDrain) {
        UNIT_ASSERT(TCacheSpaceTestAccess::FinalDrain(space, transition));
    }
    UNIT_ASSERT(transition.Phase() == ETransitionPhase::Release);
}

Y_UNIT_TEST_SUITE(TSharedCacheHotLayoutTest) {
    Y_UNIT_TEST(StandbyHasOneSlotInEachLevel) {
        constexpr THotRingLayout layout{ HotLevelCount };
        static_assert(layout.IsValid());
        static_assert(layout.Ring(EHotLevel::L3).Offset == 0);
        static_assert(layout.Ring(EHotLevel::L0).End() == HotLevelCount);
        for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
            UNIT_ASSERT_VALUES_EQUAL(layout.Ring(level).Size, 1);
        }
    }

    Y_UNIT_TEST(OneTierHasIndependentHotAndColdStorage) {
        std::array<std::atomic<ui64>, 64> hot{};
        std::array<std::atomic<ui64>, 8> cold{};
        TCacheTierRings cursors;
        const TCacheTierView tier{ &cursors, { hot.data(), sizeof(hot), hot.size() },
            { cold.data(), sizeof(cold), cold.size() } };
        const std::array<ui64, HotLevelCount> offsets{ 33, 12, 4, 0 };
        const std::array<ui64, HotLevelCount> sizes{ 31, 21, 8, 4 };
        for (ui32 index = 0; index < HotLevelCount; ++index) {
            const auto level = static_cast<EHotLevel>(index);
            const auto view = tier.Hot(level);
            UNIT_ASSERT(view.Cursor == &cursors.Hot(level));
            UNIT_ASSERT(view.Slots == hot.data() + offsets[level]);
            UNIT_ASSERT_VALUES_EQUAL(view.Capacity, sizes[level]);
            for (ui64 pos = 0; pos < view.Capacity; ++pos) {
                const ui64 value = 100 * (index + 1) + pos;
                const auto result = view.Cursor->Push(view.Slots, view.Capacity, value);
                UNIT_ASSERT_VALUES_EQUAL(result.Previous, 0);
                UNIT_ASSERT(result.Slot == hot.data() + offsets[level] + pos);
            }
            for (ui64 pos = 0; pos < view.Capacity; ++pos) {
                UNIT_ASSERT_VALUES_EQUAL(view.Pop(), 100 * (index + 1) + pos);
            }
        }
        for (const auto& slot : cold) {
            UNIT_ASSERT_VALUES_EQUAL(slot.load(), 0);
        }
        const auto view = tier.Cold();
        UNIT_ASSERT(view.Cursor == &cursors.Cold());
        UNIT_ASSERT(view.Slots == cold.data());
        UNIT_ASSERT_VALUES_EQUAL(view.Capacity, cold.size());
        for (ui64 pos = 0; pos < cold.size(); ++pos) {
            UNIT_ASSERT_VALUES_EQUAL(view.Cursor->Push(view.Slots, view.Capacity, pos + 1).Previous, 0);
        }
        for (ui64 pos = 0; pos < cold.size(); ++pos) {
            UNIT_ASSERT_VALUES_EQUAL(view.Pop(), pos + 1);
        }
        for (const auto& slot : hot) {
            UNIT_ASSERT_VALUES_EQUAL(slot.load(), 0);
        }
    }

    Y_UNIT_TEST(SmallLayoutsCoverTheTierExactly) {
        for (ui64 count = HotLevelCount; count <= 512; ++count) {
            const THotRingLayout layout{ count };
            std::array<ui8, 512> owners{};
            ui64 total = 0;
            for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
                const auto range = layout.Ring(level);
                UNIT_ASSERT(range.Size > 0 && range.End() <= count);
                total += range.Size;
                for (ui64 pos = range.Offset; pos < range.End(); ++pos) {
                    UNIT_ASSERT_VALUES_EQUAL(owners[pos], 0);
                    owners[pos] = 1;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(total, count);
            for (ui64 pos = 0; pos < count; ++pos) {
                UNIT_ASSERT_VALUES_EQUAL(owners[pos], 1);
            }
        }
    }

    Y_UNIT_TEST(ExistingRegularLayoutIsPreserved) {
        for (ui64 count = 0; count < 15; ++count) {
            UNIT_ASSERT(!SharedCacheHotLayoutIsValid(count));
        }
        std::array<std::atomic<ui64>, 512> slots{};
        TS5FifoRings cursors;
        TSpaceView view;
        view.Tiers[ECacheTier::Regular].HotStorage.Slots = slots.data();
        view.Tiers[ECacheTier::Regular].Rings = &cursors.Tier(ECacheTier::Regular);
        for (ui64 count = 15; count <= slots.size(); ++count) {
            UNIT_ASSERT(SharedCacheHotLayoutIsValid(count));
            view.Tiers[ECacheTier::Regular].HotStorage.SlotCount = count;
            const ui64 l3 = count / 15;
            const ui64 l2 = count * 2 / 15;
            const ui64 l1 = count * 5 / 15;
            const std::array<ui64, HotLevelCount> sizes{ count - l3 - l2 - l1, l1, l2, l3 };
            const std::array<ui64, HotLevelCount> offsets{ l3 + l2 + l1, l3 + l2, l3, 0 };
            for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
                const auto existing = view.Hot(level);
                UNIT_ASSERT_VALUES_EQUAL(existing.Capacity, sizes[level]);
                UNIT_ASSERT(existing.Slots == slots.data() + offsets[level]);
                UNIT_ASSERT(existing.Cursor == &cursors.Tier(ECacheTier::Regular).Hot(level));
            }
        }
    }

    Y_UNIT_TEST(CopiedTierViewsKeepCursorPositions) {
        std::array<std::atomic<ui64>, 64> hot{};
        std::array<std::atomic<ui64>, 8> cold{};
        TCacheTierRings cursors;
        const TCacheTierView first{ &cursors, { hot.data(), sizeof(hot), hot.size() },
            { cold.data(), sizeof(cold), cold.size() } };
        const auto second = first;
        for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
            auto a = first.Hot(level);
            auto b = second.Hot(level);
            UNIT_ASSERT(a.Cursor == b.Cursor && a.Slots == b.Slots);
            UNIT_ASSERT_VALUES_EQUAL(a.PutAndPop(11), 0);
            UNIT_ASSERT_VALUES_EQUAL(b.PutAndPop(22), 0);
            UNIT_ASSERT_VALUES_EQUAL(a.Slots[0].load(), 11);
            UNIT_ASSERT_VALUES_EQUAL(a.Slots[1].load(), 22);
            for (ui64 pos = 2; pos < a.Capacity; ++pos) {
                UNIT_ASSERT_VALUES_EQUAL(a.Pop(), 0);
            }
            UNIT_ASSERT_VALUES_EQUAL(b.Pop(), 11);
            UNIT_ASSERT_VALUES_EQUAL(a.Pop(), 22);
        }
        auto a = first.Cold();
        auto b = second.Cold();
        UNIT_ASSERT(a.Cursor == b.Cursor);
        UNIT_ASSERT_VALUES_EQUAL(a.Cursor->Push(a.Slots, a.Capacity, 33).Previous, 0);
        UNIT_ASSERT_VALUES_EQUAL(b.Cursor->Push(b.Slots, b.Capacity, 44).Previous, 0);
        UNIT_ASSERT_VALUES_EQUAL(a.Pop(), 33);
        UNIT_ASSERT_VALUES_EQUAL(b.Pop(), 44);
    }

    Y_UNIT_TEST(IndependentCoresAndLegacyBindings) {
        static_assert(sizeof(TCacheTierRings) >= (HotLevelCount + 1) * sizeof(TRingCursor) + sizeof(std::atomic<ui64>));
        static_assert(sizeof(TS5FifoRings) == CacheTierCount * sizeof(TCacheTierRings));
        std::array<std::array<std::atomic<ui64>, 64>, CacheTierCount> hot{};
        std::array<std::array<std::atomic<ui64>, 8>, CacheTierCount> cold{};
        TS5FifoRings composed;
        std::array<TRingCursor*, CacheTierCount*(HotLevelCount + 1)> seen{};
        ui32 seenCount = 0;
        for (ui32 index = 0; index < CacheTierCount; ++index) {
            const auto id = static_cast<ECacheTier>(index);
            TCacheTierView tier{ &composed.Tier(id), { hot[index].data(), sizeof(hot[index]), 64 },
                { cold[index].data(), sizeof(cold[index]), 8 } };
            for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
                auto ring = tier.Hot(level);
                UNIT_ASSERT(ring.Cursor == &composed.Hot(id, level));
                if (id == ECacheTier::Regular) {
                    UNIT_ASSERT(ring.Cursor == &composed.Hot(level));
                }
                for (ui32 pos = 0; pos < seenCount; ++pos) {
                    UNIT_ASSERT(ring.Cursor != seen[pos]);
                }
                seen[seenCount++] = ring.Cursor;
                UNIT_ASSERT_VALUES_EQUAL(ring.PutAndPop(index + 1), 0);
                UNIT_ASSERT_VALUES_EQUAL(ring.Slots[0].load(), index + 1);
            }
            auto ring = tier.Cold();
            UNIT_ASSERT(ring.Cursor == (id == ECacheTier::Regular ? &composed.Cold() : &composed.KeepCold()));
            for (ui32 pos = 0; pos < seenCount; ++pos) {
                UNIT_ASSERT(ring.Cursor != seen[pos]);
            }
            seen[seenCount++] = ring.Cursor;
            UNIT_ASSERT_VALUES_EQUAL(ring.Cursor->Push(ring.Slots, ring.Capacity, index + 3).Previous, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(seenCount, seen.size());
        UNIT_ASSERT_VALUES_EQUAL(cold[0][0].load(), 3);
        UNIT_ASSERT_VALUES_EQUAL(cold[1][0].load(), 4);
    }

    Y_UNIT_TEST(HotAndColdUseForwardFifoAcrossWrap) {
        for (ui64 hotSize : { ui64{ 4 }, ui64{ 64 } }) {
            std::array<std::atomic<ui64>, 64> hot{};
            std::array<std::atomic<ui64>, 7> cold{};
            TCacheTierRings cursors;
            const TCacheTierView tier{ &cursors, { hot.data(), sizeof(hot), hotSize },
                { cold.data(), sizeof(cold), cold.size() } };
            const auto check = [](TRingView ring) {
                const ui64 writes = 2 * ring.Capacity + 3;
                for (ui64 pos = 0; pos < writes; ++pos) {
                    const ui64 expected = pos < ring.Capacity ? 0 : pos - ring.Capacity + 1;
                    UNIT_ASSERT_VALUES_EQUAL(ring.PutAndPop(pos + 1), expected);
                    UNIT_ASSERT_VALUES_EQUAL(ring.Slots[pos % ring.Capacity].load(), pos + 1);
                }
                for (ui64 pos = writes - ring.Capacity; pos < writes; ++pos) {
                    UNIT_ASSERT_VALUES_EQUAL(ring.Pop(), pos + 1);
                }
                UNIT_ASSERT_VALUES_EQUAL(ring.Pop(), 0);
            };
            for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
                check(tier.Hot(level));
            }
            check(tier.Cold());
        }
    }

    Y_UNIT_TEST(TailOnlyRequeueKeepsFifoOrder) {
        std::array<std::atomic<ui64>, 8> hot{};
        std::array<std::atomic<ui64>, 4> cold{};
        TCacheTierRings cursors;
        const TCacheTierView tier{ &cursors, { hot.data(), sizeof(hot), hot.size() },
            { cold.data(), sizeof(cold), cold.size() } };
        const auto ring = tier.Hot(EHotLevel::L0);
        for (ui64 value = 1; value <= ring.Capacity; ++value) {
            UNIT_ASSERT_VALUES_EQUAL(ring.Cursor->Push(ring.Slots, ring.Capacity, value).Previous, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(ring.Pop(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ring.Cursor->Push(ring.Slots, ring.Capacity, 1).Previous, 0);
        for (ui64 value = 2; value <= ring.Capacity; ++value) {
            UNIT_ASSERT_VALUES_EQUAL(ring.Pop(), value);
        }
        UNIT_ASSERT_VALUES_EQUAL(ring.Pop(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tier.Cold().Pop(), 0);
    }

    Y_UNIT_TEST(InvalidLayoutsAndLargeCounts) {
        for (ui64 count = 0; count < HotLevelCount; ++count) {
            UNIT_ASSERT(!THotRingLayout{ count }.IsValid());
        }
        constexpr THotRingLayout layout{ Max<ui64>() };
        static_assert(layout.IsValid());
        ui64 total = 0;
        for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
            const auto range = layout.Ring(level);
            UNIT_ASSERT(range.Size > 0 && range.End() <= layout.SlotCount);
            total += range.Size;
        }
        UNIT_ASSERT_VALUES_EQUAL(total, layout.SlotCount);
    }

    Y_UNIT_TEST(TierHotLimitsAreIndependentAndClampedByEachView) {
        std::array<std::atomic<ui64>, 64> firstSlots{};
        std::array<std::atomic<ui64>, 32> secondSlots{};
        TCacheTierRings firstRings;
        TCacheTierRings secondRings;
        std::atomic<ui64> firstLimit = 32;
        std::atomic<ui64> secondLimit = 16;
        const TCacheTierView first{ &firstRings, { firstSlots.data(), sizeof(firstSlots), 64 }, {}, &firstLimit };
        const TCacheTierView second{ &secondRings, { secondSlots.data(), sizeof(secondSlots), 32 }, {}, &secondLimit };
        auto copied = first;
        UNIT_ASSERT_VALUES_EQUAL(first.EffectiveHotSlotCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(second.EffectiveHotSlotCount(), 16);
        firstLimit.store(HotLevelCount);
        for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
            UNIT_ASSERT_VALUES_EQUAL(first.Hot(level).Capacity, 1);
            UNIT_ASSERT_VALUES_EQUAL(copied.Hot(level).Capacity, 1);
        }
        UNIT_ASSERT(first.IsAllowedHotSlot(firstSlots.data() + HotLevelCount - 1));
        UNIT_ASSERT(!first.IsAllowedHotSlot(firstSlots.data() + HotLevelCount));
        UNIT_ASSERT_VALUES_EQUAL(second.EffectiveHotSlotCount(), 16);
        firstLimit.store(128);
        UNIT_ASSERT_VALUES_EQUAL(first.EffectiveHotSlotCount(), 64);
        copied.HotStorage.SlotCount = 16;
        UNIT_ASSERT_VALUES_EQUAL(copied.EffectiveHotSlotCount(), 16);
        secondLimit.store(32);
        UNIT_ASSERT_VALUES_EQUAL(second.EffectiveHotSlotCount(), 32);
    }
}

Y_UNIT_TEST_SUITE(TSharedCacheTierStorageTest) {
    void CheckDefaultMappings(
        const TSpaceView& view, const TCacheCapacityConfig& logical, const TCacheCapacityConfig& mapped) {
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            const auto& actual = view.Tiers[tier];
            UNIT_ASSERT(actual.Id == tier && actual.Rings && actual.HotStorage.Slots && actual.ColdStorage.Slots);
            UNIT_ASSERT_VALUES_EQUAL(actual.HotStorage.SlotCount, logical.Tiers[tier].HotSlots);
            UNIT_ASSERT_VALUES_EQUAL(actual.ColdStorage.SlotCount, logical.Tiers[tier].ColdSlots);
            UNIT_ASSERT_VALUES_EQUAL(actual.HotStorage.Bytes, mapped.Tiers[tier].HotSlots * sizeof(std::atomic<ui64>));
            UNIT_ASSERT_VALUES_EQUAL(
                actual.ColdStorage.Bytes, mapped.Tiers[tier].ColdSlots * sizeof(std::atomic<ui64>));
        }
        UNIT_ASSERT(view.Tiers[ECacheTier::Regular].Rings != view.Tiers[ECacheTier::Keep].Rings);
        UNIT_ASSERT_VALUES_EQUAL(view.Cold().Capacity, logical.Tiers[ECacheTier::Regular].ColdSlots);
        UNIT_ASSERT_VALUES_EQUAL(view.KeepCold().Capacity, logical.Tiers[ECacheTier::Keep].ColdSlots);
    }

    TCacheCapacityConfig PairedCapacity(ui8 bits, bool asymmetric = false) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(bits, 4096, 0, 2, capacity));
        const ui64 keepHot = asymmetric ? capacity.HotSlotCount() / 6 : capacity.HotSlotCount() / 2;
        const ui64 regularCold = asymmetric ? capacity.HandleCount() / 4 : capacity.HandleCount() / 2;
        capacity.Tiers = { TCacheTierCapacity{ capacity.HotSlotCount() - keepHot, regularCold },
            TCacheTierCapacity{ keepHot, capacity.HandleCount() - regularCold } };
        UNIT_ASSERT(capacity.HasValidTierCapacity());
        return capacity;
    }

    void CheckPairedMappings(
        const TSpaceView& view, const TCacheCapacityConfig& logical, const TCacheCapacityConfig& mapped) {
        ui64 hot = 0;
        ui64 cold = 0;
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            const auto& actual = view.Tiers[tier];
            UNIT_ASSERT(actual.Id == tier && actual.Rings && actual.HotStorage.Slots && actual.ColdStorage.Slots);
            UNIT_ASSERT_VALUES_EQUAL(actual.HotStorage.SlotCount, logical.Tiers[tier].HotSlots);
            UNIT_ASSERT_VALUES_EQUAL(actual.ColdStorage.SlotCount, logical.Tiers[tier].ColdSlots);
            UNIT_ASSERT_VALUES_EQUAL(actual.HotStorage.Bytes, mapped.Tiers[tier].HotSlots * sizeof(std::atomic<ui64>));
            UNIT_ASSERT_VALUES_EQUAL(
                actual.ColdStorage.Bytes, mapped.Tiers[tier].ColdSlots * sizeof(std::atomic<ui64>));
            UNIT_ASSERT(actual.EffectiveHotSlotCount() <= actual.HotStorage.SlotCount);
            hot += actual.HotStorage.SlotCount;
            cold += actual.ColdStorage.SlotCount;
        }
        UNIT_ASSERT_VALUES_EQUAL(hot, logical.HotSlotCount());
        UNIT_ASSERT_VALUES_EQUAL(cold, logical.HandleCount());
        UNIT_ASSERT(view.Tiers[ECacheTier::Regular].Rings != view.Tiers[ECacheTier::Keep].Rings);
        UNIT_ASSERT(view.Tiers[ECacheTier::Regular].HotStorage.Slots != view.Tiers[ECacheTier::Keep].HotStorage.Slots);
        UNIT_ASSERT(
            view.Tiers[ECacheTier::Regular].ColdStorage.Slots != view.Tiers[ECacheTier::Keep].ColdStorage.Slots);
    }

    Y_UNIT_TEST(PairedCapacityValidatesSharedEnvelopes) {
        const auto capacity = PairedCapacity(5, true);
        auto invalid = capacity;
        ++invalid.Tiers[ECacheTier::Keep].HotSlots;
        UNIT_ASSERT(!invalid.HasValidTierCapacity());
        invalid = capacity;
        ++invalid.Tiers[ECacheTier::Keep].ColdSlots;
        UNIT_ASSERT(!invalid.HasValidTierCapacity());
        invalid = capacity;
        invalid.Tiers[ECacheTier::Regular].HotSlots += invalid.Tiers[ECacheTier::Keep].HotSlots - 3;
        invalid.Tiers[ECacheTier::Keep].HotSlots = 3;
        UNIT_ASSERT(!invalid.HasValidTierCapacity());
        invalid = capacity;
        invalid.Tiers[ECacheTier::Keep].ColdSlots += invalid.Tiers[ECacheTier::Regular].ColdSlots;
        invalid.Tiers[ECacheTier::Regular].ColdSlots = 0;
        UNIT_ASSERT(!invalid.HasValidTierCapacity());
        auto [threads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 2);
        UNIT_ASSERT(space);
        CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), capacity, capacity);
        auto [badThreads, badSpace] = TCacheThreadTestAccess::MakeSpace(invalid, capacity, 2);
        UNIT_ASSERT(!badSpace);
    }

    Y_UNIT_TEST(PairedMappingsGrowAndShrinkWithIndependentBounds) {
        for (bool asymmetric : { false, true }) {
            const auto small = PairedCapacity(5, asymmetric);
            const auto large = PairedCapacity(6, asymmetric);
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 2);
            UNIT_ASSERT(space);
            const auto before = TCacheSpaceTestAccess::CurrentView(*space);
            CheckPairedMappings(before, small, small);
            for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                before.Tiers[tier].HotStorage.Slots[0].store(11 + tier);
                before.Tiers[tier].ColdStorage.Slots[0].store(21 + tier);
            }
            TTransition growth;
            UNIT_ASSERT(space->PrepareTransition(large, growth) == EPrepareResult::Prepared);
            {
                auto old = TCacheSpaceTestAccess::BeginOnHazard(*space, 0);
                UNIT_ASSERT(space->PublishMigrationView(growth));
                CheckPairedMappings(old.View(), small, small);
                CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), small, large);
                UNIT_ASSERT(!space->TryDrainTransition(growth));
            }
            UNIT_ASSERT(space->TryDrainTransition(growth));
            CompleteEmptyBucketResize(*space, large.AddressBits);
            {
                auto migration = TCacheSpaceTestAccess::BeginOnHazard(*space, 1);
                UNIT_ASSERT(space->PublishFinalView(growth));
                CompleteGrowthAppend(*space, growth);
                CheckPairedMappings(migration.View(), small, large);
                CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), large, large);
                UNIT_ASSERT(!space->TryDrainTransition(growth));
            }
            UNIT_ASSERT(space->TryDrainTransition(growth));
            CompleteFinalDrain(*space, growth);
            UNIT_ASSERT(space->TryReleaseTransition(growth));
            UNIT_ASSERT(space->CommitTransition(growth));
            const auto grown = TCacheSpaceTestAccess::CurrentView(*space);
            for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                UNIT_ASSERT_VALUES_EQUAL(grown.Tiers[tier].HotStorage.Slots[0].load(), 11 + tier);
                UNIT_ASSERT_VALUES_EQUAL(grown.Tiers[tier].ColdStorage.Slots[0].load(), 21 + tier);
                UNIT_ASSERT_VALUES_EQUAL(grown.Tiers[tier].HotStorage.Slots[large.Tiers[tier].HotSlots - 1].load(), 0);
                UNIT_ASSERT_VALUES_EQUAL(
                    grown.Tiers[tier].ColdStorage.Slots[large.Tiers[tier].ColdSlots - 1].load(), 0);
                grown.Tiers[tier].HotStorage.Slots[large.Tiers[tier].HotSlots - 1].store(31 + tier);
                grown.Tiers[tier].ColdStorage.Slots[large.Tiers[tier].ColdSlots - 1].store(41 + tier);
            }
            TTransition shrink;
            UNIT_ASSERT(space->PrepareTransition(small, shrink) == EPrepareResult::Prepared);
            UNIT_ASSERT(space->PublishMigrationView(shrink));
            UNIT_ASSERT(space->TryDrainTransition(shrink));
            CompleteEmptyBucketResize(*space, small.AddressBits);
            {
                auto migration = TCacheSpaceTestAccess::BeginOnHazard(*space, 0);
                UNIT_ASSERT(space->PublishFinalView(shrink));
                CheckPairedMappings(migration.View(), large, large);
                CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), small, small);
                UNIT_ASSERT(!space->TryDrainTransition(shrink));
            }
            UNIT_ASSERT(space->TryDrainTransition(shrink));
            std::array<ui64, CacheTierCount> hot{};
            std::array<ui64, CacheTierCount> cold{};
            while (shrink.Phase() == ETransitionPhase::FinalDrain) {
                UNIT_ASSERT(space->FinalDrain(
                    shrink,
                    [&](ECacheTier tier, EHotLevel level, ui64 raw) noexcept {
                        UNIT_ASSERT(level == EHotLevel::L0);
                        UNIT_ASSERT_VALUES_EQUAL(hot[tier], 0);
                        hot[tier] = raw;
                    },
                    [&](ECacheTier tier, ui64 raw) noexcept {
                        UNIT_ASSERT_VALUES_EQUAL(cold[tier], 0);
                        cold[tier] = raw;
                    }));
            }
            for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                UNIT_ASSERT_VALUES_EQUAL(hot[tier], 31 + tier);
                UNIT_ASSERT_VALUES_EQUAL(cold[tier], 41 + tier);
            }
            UNIT_ASSERT(space->TryReleaseTransition(shrink));
            UNIT_ASSERT(space->CommitTransition(shrink));
            const auto& after = TCacheSpaceTestAccess::CurrentView(*space);
            CheckPairedMappings(after, small, small);
            for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                UNIT_ASSERT_VALUES_EQUAL(after.Tiers[tier].HotStorage.Slots[0].load(), 11 + tier);
                UNIT_ASSERT_VALUES_EQUAL(after.Tiers[tier].ColdStorage.Slots[0].load(), 21 + tier);
                UNIT_ASSERT_VALUES_EQUAL(after.Tiers[tier].Rings, before.Tiers[tier].Rings);
            }
        }
    }

    Y_UNIT_TEST(PairedPreparationCancellationRetriesEveryRing) {
        using EStep = TCacheMappingTestAccess::EStep;
        const auto small = PairedCapacity(15);
        const auto large = PairedCapacity(16);
        for (auto failure :
            { ESpaceMap::RegularHot, ESpaceMap::RegularCold, ESpaceMap::KeepHot, ESpaceMap::KeepCold }) {
            for (bool initialize : { false, true }) {
                auto [threads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 2);
                UNIT_ASSERT(space);
                const auto before = TCacheSpaceTestAccess::CurrentView(*space);
                const auto state = space->CurrentSpaceState();
                auto& mapping = TCacheSpaceTestAccess::RingMapping(*space, failure);
                bool fail = true;
                auto cleanup = [&](EStep step) noexcept {
                    return !fail ||
                           (step != EStep::Unmap && step != EStep::ShrinkBacking && step != EStep::DiscardTail);
                };
                TCacheMappingTestAccess::SetHook(mapping, cleanup);
                TTransition transition;
                Y_DEFER {
                    TCacheMappingTestAccess::ClearHook(mapping);
                    if (transition.Phase() != ETransitionPhase::Idle) {
                        space->AbandonTransition(transition);
                    }
                };
                if (initialize) {
                    UNIT_ASSERT(space->PrepareTransition(large, transition) == EPrepareResult::Prepared);
                    UNIT_ASSERT(space->PublishMigrationView(transition));
                    UNIT_ASSERT(transition.Phase() == ETransitionPhase::InitializeGrowth);
                    UNIT_ASSERT(!space->AbandonTransition(transition));
                } else {
                    const auto next = static_cast<ESpaceMap>(static_cast<ui8>(failure) + 1);
                    auto prepare = [&](ESpaceMap stage) noexcept {
                        return stage != next;
                    };
                    UNIT_ASSERT(TCacheSpaceTestAccess::PrepareTransition(*space, large, transition, prepare) ==
                                EPrepareResult::CleanupPending);
                }
                UNIT_ASSERT(transition.Phase() == ETransitionPhase::CancelRetry);
                UNIT_ASSERT(transition.ReleaseStage() == failure);
                UNIT_ASSERT(space->CurrentSpaceState() == state);
                CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), small, small);
                fail = false;
                UNIT_ASSERT(space->AbandonTransition(transition));
                UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
                UNIT_ASSERT(space->CurrentSpaceState() == state);
                UNIT_ASSERT_VALUES_EQUAL(
                    TCacheSpaceTestAccess::CurrentView(*space).Tiers[ECacheTier::Keep].HotStorage.Slots,
                    before.Tiers[ECacheTier::Keep].HotStorage.Slots);
            }
        }
    }

    Y_UNIT_TEST(PairedReleaseRetriesEveryRing) {
        const auto small = PairedCapacity(15);
        const auto large = PairedCapacity(16);
        for (auto failure :
            { ESpaceMap::RegularHot, ESpaceMap::RegularCold, ESpaceMap::KeepHot, ESpaceMap::KeepCold }) {
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 2);
            UNIT_ASSERT(space);
            TTransition transition;
            UNIT_ASSERT(space->PrepareTransition(small, transition) == EPrepareResult::Prepared);
            UNIT_ASSERT(space->PublishMigrationView(transition));
            UNIT_ASSERT(space->TryDrainTransition(transition));
            CompleteEmptyBucketResize(*space, small.AddressBits);
            UNIT_ASSERT(space->PublishFinalView(transition));
            UNIT_ASSERT(space->TryDrainTransition(transition));
            CompleteFinalDrain(*space, transition);
            auto release = [&](ESpaceMap mapping, const void*, size_t) noexcept {
                return mapping != failure;
            };
            UNIT_ASSERT(!TCacheSpaceTestAccess::TryReleaseTransition(*space, transition, release));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::ReleaseRetry);
            UNIT_ASSERT(transition.ReleaseStage() == failure);
            ESpaceMap first = ESpaceMap::Done;
            auto retry = [&](ESpaceMap mapping, const void*, size_t) noexcept {
                if (first == ESpaceMap::Done) {
                    first = mapping;
                }
                return true;
            };
            UNIT_ASSERT(TCacheSpaceTestAccess::TryReleaseTransition(*space, transition, retry));
            UNIT_ASSERT(first == failure);
            UNIT_ASSERT(space->CommitTransition(transition));
            CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), small, small);
        }
    }

    Y_UNIT_TEST(SmallTierHotLevelsUseCommonLayout) {
        for (ui64 count = HotLevelCount; count < 32; ++count) {
            const THotRingLayout layout{ count };
            for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
                const auto range = layout.Ring(level);
                for (ui64 slot = range.Offset; slot < range.End(); ++slot) {
                    UNIT_ASSERT(TCacheSpaceTestAccess::HotLevelForSlot(slot, count) == level);
                }
            }
        }
        const auto capacity = PairedCapacity(6);
        auto [threads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 2);
        UNIT_ASSERT(space);
        UNIT_ASSERT_VALUES_EQUAL(space->EffectiveHotSlots(), capacity.Tiers[ECacheTier::Regular].HotSlots);
        THotResize resize;
        UNIT_ASSERT(
            !space->BeginHotResize(ECacheTier::Regular, capacity.Tiers[ECacheTier::Regular].HotSlots + 1, resize));
    }

    Y_UNIT_TEST(CacheAcceptsPairedLayoutAndRejectsMissingHotTier) {
        const auto capacity = PairedCapacity(6);
        auto [threads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 2);
        UNIT_ASSERT(space);
        auto cache = TSharedCacheImpl<TTestTraits>::Create(std::move(space), capacity.Limit);
        UNIT_ASSERT(cache);
        auto invalid = capacity;
        invalid.Tiers[ECacheTier::Regular].HotSlots += invalid.Tiers[ECacheTier::Keep].HotSlots;
        invalid.Tiers[ECacheTier::Keep].HotSlots = 0;
        auto [badThreads, badSpace] = TCacheThreadTestAccess::MakeSpace(invalid, invalid, 2);
        UNIT_ASSERT(badSpace);
        UNIT_ASSERT(!TSharedCacheImpl<TTestTraits>::Create(std::move(badSpace), invalid.Limit));
    }

    Y_UNIT_TEST(PairedCapacityRejectsMixedCapacityAndPresenceChanges) {
        const auto small = PairedCapacity(5);
        const auto large = PairedCapacity(6);
        auto [threads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 2);
        UNIT_ASSERT(space);
        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(small, transition) == EPrepareResult::Rejected);
        auto mixed = large;
        mixed.Tiers[ECacheTier::Regular].HotSlots = small.Tiers[ECacheTier::Regular].HotSlots - 1;
        mixed.Tiers[ECacheTier::Keep].HotSlots = large.HotSlotCount() - mixed.Tiers[ECacheTier::Regular].HotSlots;
        UNIT_ASSERT(mixed.HasValidTierCapacity());
        UNIT_ASSERT(space->PrepareTransition(mixed, transition) == EPrepareResult::Rejected);
        auto presence = large;
        presence.Tiers[ECacheTier::Regular].HotSlots = large.HotSlotCount();
        presence.Tiers[ECacheTier::Keep].HotSlots = 0;
        UNIT_ASSERT(presence.HasValidTierCapacity());
        UNIT_ASSERT(space->PrepareTransition(presence, transition) == EPrepareResult::Rejected);
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
    }

    TCacheCapacityConfig ReverseTierShares(TCacheCapacityConfig capacity) {
        std::swap(capacity.Tiers[ECacheTier::Regular], capacity.Tiers[ECacheTier::Keep]);
        UNIT_ASSERT(capacity.HasValidTierCapacity());
        return capacity;
    }

    void PublishRingStep(TCacheSpace & space, TTransition & transition) {
        do {
            UNIT_ASSERT(space.PublishMigrationView(transition));
        } while (transition.Phase() == ETransitionPhase::InitializeGrowth);
        UNIT_ASSERT(space.TryDrainTransition(transition));
        UNIT_ASSERT(space.PublishFinalView(transition));
        UNIT_ASSERT(space.TryDrainTransition(transition));
        CompleteFinalDrain(space, transition);
    }

    void CompleteReceiver(TCacheSpace & space, TTransition & transition) {
        UNIT_ASSERT(space.PrepareReceiver(transition));
        PublishRingStep(space, transition);
        UNIT_ASSERT(space.TryReleaseTransition(transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Commit);
        UNIT_ASSERT(space.CommitTransition(transition));
    }

    Y_UNIT_TEST(TierTransferFencesLateDonorWritesAndInitializesReceiverTails) {
        const auto source = PairedCapacity(6, true);
        const auto target = ReverseTierShares(source);
        auto [threads, space] = TCacheThreadTestAccess::MakeSpace(source, source, 2);
        UNIT_ASSERT(space);
        const auto initial = TCacheSpaceTestAccess::CurrentView(*space);
        const ui64 free = TCacheSpaceTestAccess::FreeCount(*space);
        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(target, transition) == EPrepareResult::Prepared);
        UNIT_ASSERT_VALUES_EQUAL(transition.ResizeDeltaBits(), 0);
        {
            auto old = TCacheSpaceTestAccess::BeginOnHazard(*space, 0);
            UNIT_ASSERT(space->PublishMigrationView(transition));
            UNIT_ASSERT(!space->TryDrainTransition(transition));
            CheckPairedMappings(old.View(), source, source);
        }
        UNIT_ASSERT(space->TryDrainTransition(transition));
        {
            auto migration = TCacheSpaceTestAccess::BeginOnHazard(*space, 0);
            UNIT_ASSERT(space->PublishFinalView(transition));
            UNIT_ASSERT(!space->TryDrainTransition(transition));
            // The held old view may still exchange excluded words after the donor cut
            migration.View()
                .Tiers[ECacheTier::Regular]
                .HotStorage.Slots[source.Tiers[ECacheTier::Regular].HotSlots - 1]
                .store(31);
            migration.View()
                .Tiers[ECacheTier::Keep]
                .ColdStorage.Slots[source.Tiers[ECacheTier::Keep].ColdSlots - 1]
                .store(41);
        }
        UNIT_ASSERT(space->TryDrainTransition(transition));
        ui64 hot = 0;
        ui64 cold = 0;
        while (transition.Phase() == ETransitionPhase::FinalDrain) {
            UNIT_ASSERT(space->FinalDrain(
                transition,
                [&](ECacheTier tier, EHotLevel level, ui64 raw) noexcept {
                    UNIT_ASSERT(tier == ECacheTier::Regular && level == EHotLevel::L0);
                    hot = raw;
                },
                [&](ECacheTier tier, ui64 raw) noexcept {
                    UNIT_ASSERT(tier == ECacheTier::Keep);
                    cold = raw;
                }));
        }
        UNIT_ASSERT_VALUES_EQUAL(hot, 31);
        UNIT_ASSERT_VALUES_EQUAL(cold, 41);
        UNIT_ASSERT(!space->PrepareReceiver(transition));
        UNIT_ASSERT(!space->CommitTransition(transition));
        UNIT_ASSERT(space->TryReleaseTransition(transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::PrepareReceiver);
        UNIT_ASSERT(space->CurrentConfiguration().Tiers == source.Tiers);
        const auto donor = TCacheSpaceTestAccess::CurrentView(*space);
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            UNIT_ASSERT_VALUES_EQUAL(
                donor.Tiers[tier].HotStorage.SlotCount, Min(source.Tiers[tier].HotSlots, target.Tiers[tier].HotSlots));
            UNIT_ASSERT_VALUES_EQUAL(donor.Tiers[tier].ColdStorage.SlotCount,
                Min(source.Tiers[tier].ColdSlots, target.Tiers[tier].ColdSlots));
        }
        UNIT_ASSERT(space->PrepareReceiver(transition));
        {
            auto old = TCacheSpaceTestAccess::BeginOnHazard(*space, 0);
            do {
                UNIT_ASSERT(space->PublishMigrationView(transition));
            } while (transition.Phase() == ETransitionPhase::InitializeGrowth);
            UNIT_ASSERT(!space->TryDrainTransition(transition));
            UNIT_ASSERT_VALUES_EQUAL(old.View().Tiers[ECacheTier::Keep].HotStorage.SlotCount,
                donor.Tiers[ECacheTier::Keep].HotStorage.SlotCount);
        }
        UNIT_ASSERT(space->TryDrainTransition(transition));
        {
            auto migration = TCacheSpaceTestAccess::BeginOnHazard(*space, 1);
            UNIT_ASSERT(space->PublishFinalView(transition));
            UNIT_ASSERT(!space->TryDrainTransition(transition));
            CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), target, target);
        }
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteFinalDrain(*space, transition);
        UNIT_ASSERT(space->TryReleaseTransition(transition));
        UNIT_ASSERT(space->CommitTransition(transition));
        UNIT_ASSERT(space->CurrentConfiguration().Tiers == target.Tiers);
        const auto& result = TCacheSpaceTestAccess::CurrentView(*space);
        UNIT_ASSERT_VALUES_EQUAL(
            result.Tiers[ECacheTier::Regular].ColdStorage.Slots[target.Tiers[ECacheTier::Regular].ColdSlots - 1].load(),
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            result.Tiers[ECacheTier::Keep].HotStorage.Slots[target.Tiers[ECacheTier::Keep].HotSlots - 1].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result.Handles, initial.Handles);
        UNIT_ASSERT_VALUES_EQUAL(result.Buckets, initial.Buckets);
        UNIT_ASSERT_VALUES_EQUAL(result.FreeSlots, initial.FreeSlots);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), free);
        UNIT_ASSERT_VALUES_EQUAL(space->CurrentSpaceState().Generation(), 4);
        UNIT_ASSERT(TCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
        UNIT_ASSERT_VALUES_EQUAL(space->CurrentSpaceState().Generation(), 5);
        // The same controller can transfer the shares back without replacing identities
        TTransition reverse;
        UNIT_ASSERT(space->PrepareTransition(source, reverse) == EPrepareResult::Prepared);
        PublishRingStep(*space, reverse);
        UNIT_ASSERT(space->TryReleaseTransition(reverse));
        CompleteReceiver(*space, reverse);
        CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), source, source);
    }

    Y_UNIT_TEST(TierTransferCannotGrowReceiverBeforeDonorReleaseAcknowledgement) {
        const auto source = PairedCapacity(16, true);
        const auto target = ReverseTierShares(source);
        for (auto failure : { ESpaceMap::RegularHot, ESpaceMap::KeepCold }) {
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(source, source, 2);
            UNIT_ASSERT(space);
            TTransition transition;
            UNIT_ASSERT(space->PrepareTransition(target, transition) == EPrepareResult::Prepared);
            PublishRingStep(*space, transition);
            auto release = [&](ESpaceMap mapping, const void*, size_t) noexcept {
                return mapping != failure;
            };
            UNIT_ASSERT(!TCacheSpaceTestAccess::TryReleaseTransition(*space, transition, release));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::ReleaseRetry);
            ui32 prepareCalls = 0;
            auto prepare = [&](ESpaceMap) noexcept {
                ++prepareCalls;
                return true;
            };
            UNIT_ASSERT(!TCacheSpaceTestAccess::PrepareReceiver(*space, transition, prepare));
            UNIT_ASSERT_VALUES_EQUAL(prepareCalls, 0);
            UNIT_ASSERT(space->TryReleaseTransition(transition));
            UNIT_ASSERT(TCacheSpaceTestAccess::PrepareReceiver(*space, transition, prepare));
            UNIT_ASSERT(prepareCalls != 0);
            // This private candidate belongs to an already-published transition
            UNIT_ASSERT(space->AbandonTransition(transition));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::AbandonedForShutdown);
        }
    }

    Y_UNIT_TEST(TierTransferReceiverCleanupRetainsPublishedIntermediateState) {
        using EStep = TCacheMappingTestAccess::EStep;
        const auto source = PairedCapacity(16, true);
        const auto target = ReverseTierShares(source);
        for (auto failure : { ESpaceMap::RegularCold, ESpaceMap::KeepHot }) {
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(source, source, 2);
            UNIT_ASSERT(space);
            TTransition transition;
            UNIT_ASSERT(space->PrepareTransition(target, transition) == EPrepareResult::Prepared);
            PublishRingStep(*space, transition);
            UNIT_ASSERT(space->TryReleaseTransition(transition));
            const auto intermediate = TCacheSpaceTestAccess::CurrentView(*space);
            const auto generation = space->CurrentSpaceState().Generation();
            auto& mapping = TCacheSpaceTestAccess::RingMapping(*space, failure);
            bool failCleanup = true;
            ui32 unmaps = 0;
            auto cleanup = [&](EStep step) noexcept {
                if (step == EStep::Unmap) {
                    ++unmaps;
                }
                return !failCleanup || (step != EStep::ShrinkBacking && step != EStep::DiscardTail);
            };
            TCacheMappingTestAccess::SetHook(mapping, cleanup);
            Y_DEFER {
                TCacheMappingTestAccess::ClearHook(mapping);
            };
            const auto next = static_cast<ESpaceMap>(static_cast<ui8>(failure) + 1);
            auto prepare = [&](ESpaceMap stage) noexcept {
                return stage != next;
            };
            UNIT_ASSERT(!TCacheSpaceTestAccess::PrepareReceiver(*space, transition, prepare));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::CancelReceiver);
            UNIT_ASSERT_VALUES_EQUAL(space->CurrentSpaceState().Generation(), generation);
            UNIT_ASSERT(space->CurrentConfiguration().Tiers == source.Tiers);
            UNIT_ASSERT(transition.TargetConfiguration().Tiers == target.Tiers);
            const ui32 completedUnmaps = unmaps;
            UNIT_ASSERT(!space->PrepareReceiver(transition));
            UNIT_ASSERT_VALUES_EQUAL(unmaps, completedUnmaps);
            failCleanup = false;
            UNIT_ASSERT(!space->PrepareReceiver(transition));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::PrepareReceiver);
            UNIT_ASSERT_VALUES_EQUAL(unmaps, completedUnmaps);
            UNIT_ASSERT_VALUES_EQUAL(
                TCacheSpaceTestAccess::CurrentView(*space).Tiers[ECacheTier::Regular].HotStorage.Slots,
                intermediate.Tiers[ECacheTier::Regular].HotStorage.Slots);
            TCacheMappingTestAccess::ClearHook(mapping);
            CompleteReceiver(*space, transition);
            CheckPairedMappings(TCacheSpaceTestAccess::CurrentView(*space), target, target);
        }
    }

    Y_UNIT_TEST(TierTransferAbandonAfterPrivateReceiverInitializationStaysClosed) {
        const auto source = PairedCapacity(16, true);
        const auto target = ReverseTierShares(source);
        auto [threads, space] = TCacheThreadTestAccess::MakeSpace(source, source, 2);
        UNIT_ASSERT(space);
        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(target, transition) == EPrepareResult::Prepared);
        PublishRingStep(*space, transition);
        UNIT_ASSERT(space->TryReleaseTransition(transition));
        UNIT_ASSERT(space->PrepareReceiver(transition));
        UNIT_ASSERT(space->PublishMigrationView(transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::InitializeGrowth);
        UNIT_ASSERT(space->AbandonTransition(transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::AbandonedForShutdown);
        UNIT_ASSERT(space->CurrentConfiguration().Tiers == source.Tiers);
        UNIT_ASSERT(!space->PrepareReceiver(transition));
    }

    Y_UNIT_TEST(TierTransferReservesFourGenerationsBeforeSideEffects) {
        const auto source = PairedCapacity(6, true);
        const auto target = ReverseTierShares(source);
        auto [threads, space] = TCacheThreadTestAccess::MakeSpace(source, source, 2);
        UNIT_ASSERT(space);
        TTransition transition;
        TCacheSpaceTestAccess::SetGeneration(*space, MaxSpaceGeneration - 3);
        UNIT_ASSERT(space->PrepareTransition(target, transition) == EPrepareResult::Rejected);
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
        TCacheSpaceTestAccess::SetGeneration(*space, MaxSpaceGeneration - 4);
        UNIT_ASSERT(space->PrepareTransition(target, transition) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->AbandonTransition(transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
    }

    Y_UNIT_TEST(CopyMappingsPreservesTierBoundsAndCursors) {
        std::array<std::array<std::atomic<ui64>, 16>, CacheTierCount * 2> oldSlots{};
        std::array<std::array<std::atomic<ui64>, 16>, CacheTierCount * 2> newSlots{};
        TS5FifoRings oldRings;
        TS5FifoRings newRings;
        std::array<std::atomic<ui64>, CacheTierCount> oldLimits{};
        std::array<std::atomic<ui64>, CacheTierCount> newLimits{};
        TSpaceView oldView;
        TSpaceView newView;
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            const ui32 index = tier * 2;
            oldView.Tiers[tier] = {
                &oldRings.Tier(tier),
                { oldSlots[index].data(), sizeof(oldSlots[index]), 4 + index },
                { oldSlots[index + 1].data(), sizeof(oldSlots[index + 1]), 5 + index },
            };
            newView.Tiers[tier] = {
                &newRings.Tier(tier),
                { newSlots[index].data(), sizeof(newSlots[index]), 16 },
                { newSlots[index + 1].data(), sizeof(newSlots[index + 1]), 16 },
            };
            oldView.Tiers[tier].EffectiveHotSlots = &oldLimits[tier];
            newView.Tiers[tier].EffectiveHotSlots = &newLimits[tier];
            oldView.Tiers[tier].Id = tier;
            newView.Tiers[tier].Id = tier == ECacheTier::Regular ? ECacheTier::Keep : ECacheTier::Regular;
        }
        oldView.CopyMappings(newView);
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            const ui32 index = tier * 2;
            const auto& view = oldView.Tiers[tier];
            UNIT_ASSERT(view.Rings == &oldRings.Tier(tier));
            UNIT_ASSERT(view.EffectiveHotSlots == &oldLimits[tier]);
            UNIT_ASSERT(view.Id == tier);
            UNIT_ASSERT(view.HotStorage.Slots == newSlots[index].data());
            UNIT_ASSERT(view.ColdStorage.Slots == newSlots[index + 1].data());
            UNIT_ASSERT_VALUES_EQUAL(view.HotStorage.Bytes, sizeof(newSlots[index]));
            UNIT_ASSERT_VALUES_EQUAL(view.ColdStorage.Bytes, sizeof(newSlots[index + 1]));
            UNIT_ASSERT_VALUES_EQUAL(view.HotStorage.SlotCount, 4 + index);
            UNIT_ASSERT_VALUES_EQUAL(view.ColdStorage.SlotCount, 5 + index);
        }
    }

    Y_UNIT_TEST(TierMappingsKeepLogicalBoundsAcrossGrowthAndShrink) {
        const ui64 keepWord = TColdCacheItem::Make(33, 3).Raw();
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));
        for (bool growing : { true, false }) {
            const auto& current = growing ? small : large;
            const auto& target = growing ? large : small;
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(current, large, 1);
            UNIT_ASSERT(space);
            const auto before = TCacheSpaceTestAccess::CurrentView(*space);
            CheckDefaultMappings(before, current, current);
            before.Tiers[ECacheTier::Regular].HotStorage.Slots[0].store(11);
            before.Tiers[ECacheTier::Regular].ColdStorage.Slots[0].store(22);
            before.Tiers[ECacheTier::Keep].ColdStorage.Slots[0].store(keepWord);
            TTransition transition;
            UNIT_ASSERT(space->PrepareTransition(target, transition) == EPrepareResult::Prepared);
            {
                auto oldOperation = TCacheSpaceTestAccess::BeginOnHazard(*space, 0);
                UNIT_ASSERT(space->PublishMigrationView(transition));
                CheckDefaultMappings(TCacheSpaceTestAccess::CurrentView(*space), current, large);
                CheckDefaultMappings(oldOperation.View(), current, current);
                UNIT_ASSERT(!space->TryDrainTransition(transition));
            }
            UNIT_ASSERT(space->TryDrainTransition(transition));
            CompleteEmptyBucketResize(*space, target.AddressBits);
            {
                auto migrationOperation = TCacheSpaceTestAccess::BeginOnHazard(*space, 0);
                UNIT_ASSERT(space->PublishFinalView(transition));
                if (growing) {
                    CompleteGrowthAppend(*space, transition);
                }
                CheckDefaultMappings(TCacheSpaceTestAccess::CurrentView(*space), target, target);
                CheckDefaultMappings(migrationOperation.View(), current, large);
                UNIT_ASSERT(!space->TryDrainTransition(transition));
            }
            UNIT_ASSERT(space->TryDrainTransition(transition));
            CompleteFinalDrain(*space, transition);
            UNIT_ASSERT(space->TryReleaseTransition(transition));
            UNIT_ASSERT(space->CommitTransition(transition));
            const auto& after = TCacheSpaceTestAccess::CurrentView(*space);
            CheckDefaultMappings(after, target, target);
            UNIT_ASSERT(before.Tiers[ECacheTier::Regular].Rings == after.Tiers[ECacheTier::Regular].Rings);
            UNIT_ASSERT(before.Tiers[ECacheTier::Keep].Rings == after.Tiers[ECacheTier::Keep].Rings);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Regular].HotStorage.Slots[0].load(), 11);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Regular].ColdStorage.Slots[0].load(), 22);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Keep].ColdStorage.Slots[0].load(), keepWord);
        }
    }

    Y_UNIT_TEST(PartialPreparationKeepsThePublishedTierMappings) {
        const ui64 keepWord = TColdCacheItem::Make(33, 3).Raw();
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));
        const std::array stages{ ESpaceMap::Handles, ESpaceMap::Buckets, ESpaceMap::RegularHot, ESpaceMap::RegularCold,
            ESpaceMap::KeepHot, ESpaceMap::KeepCold, ESpaceMap::Free };
        for (ui32 failure = 0; failure < stages.size(); ++failure) {
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 1);
            UNIT_ASSERT(space);
            const auto state = space->CurrentSpaceState();
            const auto before = TCacheSpaceTestAccess::CurrentView(*space);
            before.Tiers[ECacheTier::Regular].HotStorage.Slots[0].store(11);
            before.Tiers[ECacheTier::Regular].ColdStorage.Slots[0].store(22);
            before.Tiers[ECacheTier::Keep].ColdStorage.Slots[0].store(keepWord);
            ui32 calls = 0;
            bool ordered = true;
            auto prepare = [&](ESpaceMap mapping) noexcept {
                ordered &= calls < stages.size() && mapping == stages[calls];
                ++calls;
                return mapping != stages[failure];
            };
            TTransition failed;
            UNIT_ASSERT(
                TCacheSpaceTestAccess::PrepareTransition(*space, large, failed, prepare) == EPrepareResult::Rejected);
            UNIT_ASSERT(ordered);
            UNIT_ASSERT_VALUES_EQUAL(calls, failure + 1);
            UNIT_ASSERT(failed.Phase() == ETransitionPhase::Idle);
            UNIT_ASSERT(space->CurrentSpaceState() == state);
            TTransition retry;
            UNIT_ASSERT(space->PrepareTransition(large, retry) == EPrepareResult::Prepared);
            UNIT_ASSERT(space->AbandonTransition(retry));
            const auto& after = TCacheSpaceTestAccess::CurrentView(*space);
            CheckDefaultMappings(after, small, small);
            UNIT_ASSERT(after.Tiers[ECacheTier::Regular].HotStorage.Slots ==
                        before.Tiers[ECacheTier::Regular].HotStorage.Slots);
            UNIT_ASSERT(after.Tiers[ECacheTier::Regular].ColdStorage.Slots ==
                        before.Tiers[ECacheTier::Regular].ColdStorage.Slots);
            UNIT_ASSERT(
                after.Tiers[ECacheTier::Keep].ColdStorage.Slots == before.Tiers[ECacheTier::Keep].ColdStorage.Slots);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Regular].HotStorage.Slots[0].load(), 11);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Regular].ColdStorage.Slots[0].load(), 22);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Keep].ColdStorage.Slots[0].load(), keepWord);
        }
    }

    Y_UNIT_TEST(TierReleaseRetriesEachRingStage) {
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(14, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(15, 4096, 0, 1, large));
        for (auto* capacity : { &small, &large }) {
            const ui64 hot = capacity->HotSlotCount();
            const ui64 cold = capacity->HandleCount();
            capacity->Tiers = { TCacheTierCapacity{ hot / 2, cold / 2 },
                TCacheTierCapacity{ hot - hot / 2, cold - cold / 2 } };
        }
        for (auto failure :
            { ESpaceMap::RegularHot, ESpaceMap::RegularCold, ESpaceMap::KeepHot, ESpaceMap::KeepCold }) {
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 1);
            UNIT_ASSERT(space);
            TTransition transition;
            UNIT_ASSERT(space->PrepareTransition(small, transition) == EPrepareResult::Prepared);
            UNIT_ASSERT(space->PublishMigrationView(transition));
            UNIT_ASSERT(space->TryDrainTransition(transition));
            CompleteEmptyBucketResize(*space, small.AddressBits);
            UNIT_ASSERT(space->PublishFinalView(transition));
            UNIT_ASSERT(space->TryDrainTransition(transition));
            CompleteFinalDrain(*space, transition);
            bool reached = false;
            auto release = [&](ESpaceMap mapping, const void*, size_t) noexcept {
                reached |= mapping == failure;
                return mapping != failure;
            };
            UNIT_ASSERT(!TCacheSpaceTestAccess::TryReleaseTransition(*space, transition, release));
            UNIT_ASSERT(reached);
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::ReleaseRetry);
            UNIT_ASSERT(transition.ReleaseStage() == failure);
            CheckDefaultMappings(TCacheSpaceTestAccess::CurrentView(*space), small, small);
            ESpaceMap first = ESpaceMap::Done;
            auto retry = [&](ESpaceMap mapping, const void*, size_t) noexcept {
                if (first == ESpaceMap::Done) {
                    first = mapping;
                }
                return true;
            };
            UNIT_ASSERT(TCacheSpaceTestAccess::TryReleaseTransition(*space, transition, retry));
            UNIT_ASSERT(first == failure);
            UNIT_ASSERT(space->CommitTransition(transition));
            CheckDefaultMappings(TCacheSpaceTestAccess::CurrentView(*space), small, small);
        }
    }
}

Y_UNIT_TEST_SUITE(TSharedCacheSpaceTest) {
    Y_UNIT_TEST(MappingCancellationRetainsPartialAliasAndBackingProgress) {
        using EStep = TCacheMappingTestAccess::EStep;
        const size_t small = NSystemInfo::GetPageSize() * 2;
        const size_t large = small * 2;
        for (bool failAfterMap : { false, true }) {
            TCacheMapping mapping;
            void* base = nullptr;
            UNIT_ASSERT(mapping.Allocate(small, large, base));
            static_cast<char*>(base)[0] = 17;
            static_cast<char*>(base)[small - 1] = 29;
            bool failUnmap = true;
            bool failTruncate = true;
            ui32 maps = 0;
            ui32 unmaps = 0;
            ui32 truncates = 0;
            auto hook = [&](EStep step) noexcept {
                switch (step) {
                    case EStep::Map:
                        ++maps;
                        return failAfterMap;
                    case EStep::AfterMap:
                        return false;
                    case EStep::Unmap:
                        ++unmaps;
                        return !failUnmap;
                    case EStep::ShrinkBacking:
                        ++truncates;
                        return !failTruncate;
                    default:
                        return true;
                }
            };
            TCacheMappingTestAccess::SetHook(mapping, hook);
            char* candidate = nullptr;
            const bool prepared = mapping.Prepare(large, candidate);
            Y_DEFER {
                TCacheMappingTestAccess::ClearHook(mapping);
                mapping.Cancel(candidate, large);
            };
            if (maps == 0) {
                // Stable reserved backends have no disposable aliases
                continue;
            }
            UNIT_ASSERT(!prepared);
            UNIT_ASSERT_VALUES_EQUAL(bool(candidate), failAfterMap);
            const auto partial = candidate;
            UNIT_ASSERT(!mapping.Cancel(candidate, large));
            UNIT_ASSERT_VALUES_EQUAL(candidate, partial);
            if (failAfterMap) {
                UNIT_ASSERT_VALUES_EQUAL(unmaps, 1);
                UNIT_ASSERT_VALUES_EQUAL(truncates, 0);
                failUnmap = false;
                UNIT_ASSERT(!mapping.Cancel(candidate, large));
            }
            UNIT_ASSERT(!candidate);
            UNIT_ASSERT_VALUES_EQUAL(truncates, 1);
            const ui32 completedUnmaps = unmaps;
            failTruncate = false;
            UNIT_ASSERT(mapping.Cancel(candidate, large));
            UNIT_ASSERT_VALUES_EQUAL(unmaps, completedUnmaps);
            UNIT_ASSERT_VALUES_EQUAL(truncates, 2);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<char*>(base)[0], 17);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<char*>(base)[small - 1], 29);
            TCacheMappingTestAccess::ClearHook(mapping);
            UNIT_ASSERT(mapping.Prepare(large, candidate));
            UNIT_ASSERT(mapping.Cancel(candidate, large));
        }
    }

    Y_UNIT_TEST(SameSizeAndShrinkCancellationPreservePublishedMapping) {
        const size_t small = NSystemInfo::GetPageSize() * 2;
        const size_t large = small * 2;
        for (auto mode : { TCacheMapping::EAddressMode::Default, TCacheMapping::EAddressMode::Stable }) {
            TCacheMapping mapping(mode);
            void* base = nullptr;
            UNIT_ASSERT(mapping.Allocate(large, large * 2, base));
            auto* bytes = static_cast<char*>(base);
            bytes[large - 1] = 53;
            for (size_t target : { large, small }) {
                void* candidate = nullptr;
                UNIT_ASSERT(mapping.Prepare(target, candidate));
                UNIT_ASSERT(mapping.Cancel(candidate, target));
                UNIT_ASSERT(!candidate);
                UNIT_ASSERT_VALUES_EQUAL(bytes[large - 1], 53);
            }
        }
    }

    Y_UNIT_TEST(StableMappingCancellationRetriesTailCleanup) {
        using EStep = TCacheMappingTestAccess::EStep;
        const size_t small = NSystemInfo::GetPageSize() * 2;
        const size_t large = small * 2;
        TCacheMapping mapping(TCacheMapping::EAddressMode::Stable);
        void* base = nullptr;
        UNIT_ASSERT(mapping.Allocate(small, large, base));
        static_cast<char*>(base)[small - 1] = 29;
        void* candidate = nullptr;
        UNIT_ASSERT(mapping.Prepare(large, candidate));
        static_cast<char*>(candidate)[large - 1] = 41;
        bool fail = true;
        ui32 cleanups = 0;
        auto hook = [&](EStep step) noexcept {
            if (step == EStep::ShrinkBacking || step == EStep::DiscardTail) {
                ++cleanups;
                return !fail;
            }
            return true;
        };
        TCacheMappingTestAccess::SetHook(mapping, hook);
        Y_DEFER {
            TCacheMappingTestAccess::ClearHook(mapping);
            mapping.Cancel(candidate, large);
        };
        UNIT_ASSERT(!mapping.Cancel(candidate, large));
        UNIT_ASSERT_VALUES_EQUAL(cleanups, 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<char*>(base)[small - 1], 29);
        fail = false;
        UNIT_ASSERT(mapping.Cancel(candidate, large));
        UNIT_ASSERT_VALUES_EQUAL(cleanups, 2);
        UNIT_ASSERT(!candidate);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<char*>(base)[small - 1], 29);
    }

    Y_UNIT_TEST(CancellationAfterPartialInitializationRetainsItsCursor) {
        using EStep = TCacheMappingTestAccess::EStep;
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        // Bucket backing grows from 64 KiB to 128 KiB, crossing even 64 KiB OS pages
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(13, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(14, 4096, 0, 1, large));
        auto [threads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 1);
        UNIT_ASSERT(space);
        auto& mapping = TCacheSpaceTestAccess::BucketsMapping(*space);
        const auto state = space->CurrentSpaceState();
        const auto before = TCacheSpaceTestAccess::CurrentView(*space);
        before.Buckets[0].store(17);
        TTransition transition;
        Y_DEFER {
            TCacheMappingTestAccess::ClearHook(mapping);
            if (transition.Phase() != ETransitionPhase::Idle) {
                space->AbandonTransition(transition);
            }
        };
        UNIT_ASSERT(space->PrepareTransition(large, transition) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->PublishMigrationView(transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::InitializeGrowth);
        bool fail = true;
        auto hook = [&](EStep step) noexcept {
            return !fail || (step != EStep::Unmap && step != EStep::ShrinkBacking && step != EStep::DiscardTail);
        };
        TCacheMappingTestAccess::SetHook(mapping, hook);
        UNIT_ASSERT(!space->AbandonTransition(transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::CancelRetry);
        UNIT_ASSERT(transition.ReleaseStage() == ESpaceMap::Buckets);
        UNIT_ASSERT(space->CurrentSpaceState() == state);
        UNIT_ASSERT_VALUES_EQUAL(before.Buckets[0].load(), 17);
        fail = false;
        UNIT_ASSERT(space->AbandonTransition(transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT(space->CurrentSpaceState() == state);
        UNIT_ASSERT_VALUES_EQUAL(before.Buckets[0].load(), 17);
    }

    Y_UNIT_TEST(StableMappingCancellationAndRegrowthPreserveLivePrefix) {
        const size_t small = NSystemInfo::GetPageSize() * 2;
        const size_t large = small * 2;
        TCacheMapping mapping(TCacheMapping::EAddressMode::Stable);
        void* base = nullptr;
        UNIT_ASSERT(mapping.Allocate(small, large * 2, base));
        auto* bytes = static_cast<char*>(base);
        bytes[0] = 17;
        bytes[small - 1] = 29;

        void* prepared = nullptr;
        UNIT_ASSERT(mapping.Prepare(large, prepared));
        UNIT_ASSERT_VALUES_EQUAL(prepared, base);
        static_cast<char*>(prepared)[large - 1] = 41;
        UNIT_ASSERT(mapping.Cancel(prepared, large));
        UNIT_ASSERT_VALUES_EQUAL(bytes[0], 17);
        UNIT_ASSERT_VALUES_EQUAL(bytes[small - 1], 29);

        UNIT_ASSERT(mapping.Prepare(large, prepared));
        UNIT_ASSERT_VALUES_EQUAL(prepared, base);
        bytes[large - 1] = 53;
        mapping.Publish(prepared, large);
        void* old = base;
        UNIT_ASSERT(mapping.Release(old, small, base, large));
        UNIT_ASSERT_VALUES_EQUAL(bytes[small - 1], 29);

        UNIT_ASSERT(mapping.Prepare(small, prepared));
        UNIT_ASSERT(mapping.Cancel(prepared, small));
        UNIT_ASSERT_VALUES_EQUAL(bytes[large - 1], 53);

        UNIT_ASSERT(mapping.Prepare(small, prepared));
        mapping.Publish(prepared, small);
        const void* begin = nullptr;
        size_t size = 0;
        mapping.ReleaseRange(base, large, base, small, begin, size);
        UNIT_ASSERT_VALUES_EQUAL(begin, bytes + small);
        UNIT_ASSERT_VALUES_EQUAL(size, large - small);
        old = base;
        UNIT_ASSERT(mapping.Release(old, large, base, small));
        UNIT_ASSERT_VALUES_EQUAL(bytes[0], 17);
        UNIT_ASSERT_VALUES_EQUAL(bytes[small - 1], 29);

        UNIT_ASSERT(mapping.Prepare(large, prepared));
        UNIT_ASSERT_VALUES_EQUAL(prepared, base);
        bytes[large - 1] = 67;
        mapping.Publish(prepared, large);
        old = base;
        UNIT_ASSERT(mapping.Release(old, small, base, large));
        UNIT_ASSERT_VALUES_EQUAL(bytes[small - 1], 29);
        UNIT_ASSERT_VALUES_EQUAL(bytes[large - 1], 67);
    }

    Y_UNIT_TEST(StableMappingReleaseRangeUsesBackingExtent) {
        const size_t small = NSystemInfo::GetPageSize() * 2;
        const size_t middle = small * 2;
        const size_t large = small * 4;
        TCacheMapping mapping(TCacheMapping::EAddressMode::Stable);
        void* base = nullptr;
        UNIT_ASSERT(mapping.Allocate(small, large, base));
        auto* bytes = static_cast<char*>(base);
        bytes[small - 1] = 17;

        // A failed growth cancellation can leave a backing larger than the published prefix
        void* prepared = nullptr;
        UNIT_ASSERT(mapping.Prepare(large, prepared));
        UNIT_ASSERT(mapping.Prepare(middle, prepared));
        UNIT_ASSERT_VALUES_EQUAL(prepared, base);
        bytes[middle - 1] = 29;
        mapping.Publish(prepared, middle);
        const void* begin = nullptr;
        size_t size = 0;
        if (mapping.NeedsRelease(base, small, base, middle)) {
            mapping.ReleaseRange(base, small, base, middle, begin, size);
            UNIT_ASSERT_VALUES_EQUAL(begin, bytes + middle);
            UNIT_ASSERT(size > 0 && size <= large - middle);
        }
        void* old = base;
        UNIT_ASSERT(mapping.Release(old, small, base, middle));
        UNIT_ASSERT_VALUES_EQUAL(bytes[small - 1], 17);
        UNIT_ASSERT_VALUES_EQUAL(bytes[middle - 1], 29);
    }

    Y_UNIT_TEST(HotResizeCoalescesContendedUnlockNotification) {
        ui32 notifications = 0;
        THotResize resize({
            .Context = &notifications,
            .Function = [](void* context) noexcept {
                ++*static_cast<ui32*>(context);
            },
        });
        {
            auto owner = resize.TryLock();
            UNIT_ASSERT(owner);
            bool contended = false;
            std::thread worker([&] {
                contended = !resize.TryLock() && !resize.TryLock();
            });
            worker.join();
            UNIT_ASSERT(contended);
            UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        {
            auto owner = resize.TryLock();
            UNIT_ASSERT(owner);
        }
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
    }

    Y_UNIT_TEST(HotResizeMovedGuardNotifiesAfterUnlock) {
        struct TNotification {
            THotResize* Resize = nullptr;
            ui32 Count = 0;
            bool Acquired = false;
        } notification;

        THotResize resize({
            .Context = &notification,
            .Function = [](void* context) noexcept {
                auto& notification = *static_cast<TNotification*>(context);
                ++notification.Count;
                auto owner = notification.Resize->TryLock();
                notification.Acquired = bool(owner);
            },
        });
        notification.Resize = &resize;
        THotResize::TGuard owner;
        {
            auto first = resize.TryLock();
            UNIT_ASSERT(first);
            UNIT_ASSERT(!resize.TryLock());
            auto second = std::move(first);
            UNIT_ASSERT(!first);
            owner = std::move(second);
            UNIT_ASSERT(!second);
        }
        UNIT_ASSERT_VALUES_EQUAL(notification.Count, 0);
        owner = {};
        UNIT_ASSERT_VALUES_EQUAL(notification.Count, 1);
        UNIT_ASSERT(notification.Acquired);
    }

    Y_UNIT_TEST(HazardWaiterPreservesGenerationAndActiveCount) {
        constexpr ui64 state = (ui64(Max<ui32>()) << 32) | SpaceHazardDrainWaiter | MaxSpaceHazardActiveCount;
        static_assert(SpaceHazardGeneration(state) == Max<ui32>());
        static_assert(SpaceHazardActiveCount(state) == MaxSpaceHazardActiveCount);
        static_assert(SpaceHazardIsActive(state));
        static_assert(SpaceHazardIsDrained(state, Max<ui32>()));
        static_assert(!SpaceHazardIsDrained(state, Max<ui32>() - 1));
    }

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
        TCacheCapacityConfig footprint;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 128, 3, footprint));
        UNIT_ASSERT_VALUES_EQUAL(footprint.FixedBytes, 128);
        UNIT_ASSERT_VALUES_EQUAL(footprint.AddressBits, 5);
        UNIT_ASSERT_VALUES_EQUAL(footprint.HandleCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(footprint.BucketCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(footprint.HotSlotCount(), 24);
        UNIT_ASSERT_VALUES_EQUAL(footprint.ColdSlotCount(), 31);
        UNIT_ASSERT_VALUES_EQUAL(footprint.KeepColdSlotCount(), 1);

        const ui64 expectedStatic =
            128 + sizeof(TS5FifoRings) + sizeof(TFreeRingCursor) + sizeof(THotResize) + sizeof(TSpaceViews) +
            MaxOwnedSpaceViews * sizeof(TSpaceView) + 32 * sizeof(THandle) + 32 * sizeof(std::atomic<ui64>) +
            24 * sizeof(std::atomic<ui64>) + 16 * sizeof(std::atomic<ui64>) + 16 * sizeof(std::atomic<ui64>) +
            32 * sizeof(std::atomic<ui64>) + 3 * (sizeof(TSpaceHazard) + sizeof(TSpaceHazard*));
        const ui64 expectedPayload = 30 * (4096 + NActors::TSharedData::OverheadSize);
        UNIT_ASSERT_VALUES_EQUAL(footprint.StaticBytes, expectedStatic);
        UNIT_ASSERT_VALUES_EQUAL(footprint.MinimumPayloadBytes, expectedPayload);
        UNIT_ASSERT_VALUES_EQUAL(footprint.TotalBytes, expectedStatic + expectedPayload);
    }

    Y_UNIT_TEST(KeepColdRingUsesConfiguredSlots) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };
        auto operation = space->BeginOperation();
        auto ring = operation.KeepCold();
        UNIT_ASSERT_VALUES_EQUAL(ring.Capacity, capacity.Tiers[ECacheTier::Keep].ColdSlots);

        for (ui64 word = 1; word <= ring.Capacity; ++word) {
            UNIT_ASSERT_VALUES_EQUAL(ring.PutAndPop(word), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(ring.PutAndPop(ring.Capacity + 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(ring.Pop(), 2);
    }

    Y_UNIT_TEST(KeepColdTailIsDrainedOnPhysicalShrink) {
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, large));
        small.Tiers[ECacheTier::Regular].ColdSlots = small.HandleCount() / 2;
        small.Tiers[ECacheTier::Keep].ColdSlots = small.HandleCount() / 2;
        large.Tiers[ECacheTier::Regular].ColdSlots = large.HandleCount() / 2;
        large.Tiers[ECacheTier::Keep].ColdSlots = large.HandleCount() / 2;
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 0);
        UNIT_ASSERT(space);
        const ui64 word = TCacheItem::Make(1, 2).Raw();
        TCacheSpaceTestAccess::KeepColdSlots(*space)[large.KeepColdSlotCount() - 1].store(word);

        TTransition shrink;
        UNIT_ASSERT(space->PrepareTransition(small, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->PublishMigrationView(shrink));
        UNIT_ASSERT(space->TryDrainTransition(shrink));
        CompleteEmptyBucketResize(*space, small.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(shrink));
        UNIT_ASSERT(space->TryDrainTransition(shrink));

        ui64 drained = 0;
        UNIT_ASSERT(space->FinalDrain(
            shrink,
            [](ECacheTier, EHotLevel, ui64) noexcept {
            },
            [&](ECacheTier tier, ui64 raw) noexcept {
                UNIT_ASSERT(tier == ECacheTier::Keep);
                drained = raw;
            }));
        UNIT_ASSERT_VALUES_EQUAL(drained, word);
        UNIT_ASSERT(shrink.Phase() == ETransitionPhase::Release);
    }

    Y_UNIT_TEST(PhysicalShrinkCleansAssignedPrefixesWithoutMovingLowWords) {
        for (bool replaceKeepPrefix : { false, true }) {
            TCacheCapacityConfig small;
            TCacheCapacityConfig large;
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 2, small));
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(8, 4096, 0, 2, large));
            for (auto* capacity : { &small, &large }) {
                capacity->Tiers = { TCacheTierCapacity{ capacity->HotSlotCount() / 2, capacity->HandleCount() / 2 },
                    TCacheTierCapacity{ capacity->HotSlotCount() / 2, capacity->HandleCount() / 2 } };
                UNIT_ASSERT(capacity->HasValidTierCapacity());
            }
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 2);
            UNIT_ASSERT(space);
            const auto before = TCacheSpaceTestAccess::CurrentView(*space);
            const ui32 highIndex = static_cast<ui32>(small.HandleCount());
            const auto lowHot = TCacheItem::Make(11, 2);
            const auto highHot = TCacheItem::Make(13, highIndex);
            const auto tailHot = TCacheItem::Make(17, highIndex);
            const auto lowCold = TColdCacheItem::Make(19, 2);
            const auto highCold = TColdCacheItem::Make(23, highIndex);
            const auto tailCold = TColdCacheItem::Make(29, highIndex);
            const auto replacementHot = TCacheItem::Make(31, 3);
            const auto replacementCold = TColdCacheItem::Make(37, 3);
            constexpr ui64 cursorProbeCapacity = 7;
            for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                const auto& ring = before.Tiers[tier];
                ring.EffectiveHotSlots->store(HotLevelCount);
                const auto hotEnd = small.Tiers[tier].HotSlots;
                const auto coldEnd = small.Tiers[tier].ColdSlots;
                ring.HotStorage.Slots[0].store(lowHot.Raw());
                ring.HotStorage.Slots[hotEnd - 1].store(highHot.Raw());
                ring.ColdStorage.Slots[0].store(lowCold.Raw());
                ring.ColdStorage.Slots[coldEnd - 1].store(highCold.Raw());
                ring.Rings->AddColdEntry();
                ring.Rings->AddColdEntry();
                // Probe counters without exchanging any words; the pass must leave them unchanged
                for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
                    UNIT_ASSERT(ring.Rings->Hot(level).SelectPopSlot(ring.HotStorage.Slots, cursorProbeCapacity) ==
                                ring.HotStorage.Slots);
                    UNIT_ASSERT(ring.Rings->Hot(level).SelectPushSlot(ring.HotStorage.Slots, cursorProbeCapacity) ==
                                ring.HotStorage.Slots);
                }
                UNIT_ASSERT(ring.Rings->Cold().SelectPopSlot(ring.ColdStorage.Slots, cursorProbeCapacity) ==
                            ring.ColdStorage.Slots);
                UNIT_ASSERT(ring.Rings->Cold().SelectPushSlot(ring.ColdStorage.Slots, cursorProbeCapacity) ==
                            ring.ColdStorage.Slots);
            }
            TTransition shrink;
            UNIT_ASSERT(space->PrepareTransition(small, shrink) == EPrepareResult::Prepared);
            UNIT_ASSERT(space->PublishMigrationView(shrink));
            UNIT_ASSERT(space->TryDrainTransition(shrink));
            CompleteEmptyBucketResize(*space, small.AddressBits);
            {
                auto oldPublisher = TCacheSpaceTestAccess::BeginOnHazard(*space, 0);
                UNIT_ASSERT(space->PublishFinalView(shrink));
                for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                    const auto& old = oldPublisher.View().Tiers[tier];
                    old.HotStorage.Slots[small.Tiers[tier].HotSlots + 1].store(tailHot.Raw());
                    old.Rings->AddColdEntry();
                    old.ColdStorage.Slots[small.Tiers[tier].ColdSlots + 1].store(tailCold.Raw());
                }
                UNIT_ASSERT(!space->TryDrainTransition(shrink));
                UNIT_ASSERT(!TCacheSpaceTestAccess::FinalDrain(*space, shrink));
            }
            UNIT_ASSERT(space->TryDrainTransition(shrink));
            std::array<ui32, CacheTierCount> hotCleared{};
            std::array<ui32, CacheTierCount> coldCleared{};
            const auto hot = [&](ECacheTier tier, EHotLevel, ui64 raw) {
                UNIT_ASSERT(TCacheItem::FromRaw(raw).Index() >= small.HandleCount());
                ++hotCleared[tier];
                if (replaceKeepPrefix && tier == ECacheTier::Keep && raw == tailHot.Raw()) {
                    TCacheSpaceTestAccess::CurrentView(*space)
                        .Tiers[tier]
                        .HotStorage.Slots[small.Tiers[tier].HotSlots - 1]
                        .store(replacementHot.Raw());
                }
            };
            const auto cold = [&](ECacheTier tier, ui64 raw) {
                UNIT_ASSERT(TColdCacheItem::FromRaw(raw).Index() >= small.HandleCount());
                auto& ring = TCacheSpaceTestAccess::CurrentView(*space).Tiers[tier];
                ring.Rings->RemoveColdEntry();
                ++coldCleared[tier];
                if (replaceKeepPrefix && tier == ECacheTier::Keep && raw == tailCold.Raw()) {
                    ring.ColdStorage.Slots[small.Tiers[tier].ColdSlots - 1].store(replacementCold.Raw());
                }
            };
            UNIT_ASSERT(space->FinalDrain(shrink, hot, cold));
            UNIT_ASSERT(shrink.Phase() == ETransitionPhase::FinalDrain);
            while (shrink.Phase() == ETransitionPhase::FinalDrain) {
                UNIT_ASSERT(space->FinalDrain(shrink, hot, cold));
            }
            UNIT_ASSERT(shrink.Phase() == ETransitionPhase::Release);
            UNIT_ASSERT(space->TryReleaseTransition(shrink));
            UNIT_ASSERT(space->CommitTransition(shrink));
            const auto& after = TCacheSpaceTestAccess::CurrentView(*space);
            for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                const bool replaced = replaceKeepPrefix && tier == ECacheTier::Keep;
                const auto& ring = after.Tiers[tier];
                UNIT_ASSERT_VALUES_EQUAL(hotCleared[tier], replaced ? 1 : 2);
                UNIT_ASSERT_VALUES_EQUAL(coldCleared[tier], replaced ? 1 : 2);
                UNIT_ASSERT_VALUES_EQUAL(ring.HotStorage.Slots[0].load(), lowHot.Raw());
                UNIT_ASSERT_VALUES_EQUAL(ring.ColdStorage.Slots[0].load(), lowCold.Raw());
                UNIT_ASSERT_VALUES_EQUAL(
                    ring.HotStorage.Slots[ring.HotStorage.SlotCount - 1].load(), replaced ? replacementHot.Raw() : 0);
                UNIT_ASSERT_VALUES_EQUAL(ring.ColdStorage.Slots[ring.ColdStorage.SlotCount - 1].load(),
                    replaced ? replacementCold.Raw() : 0);
                UNIT_ASSERT_VALUES_EQUAL(ring.Rings->ColdEntries(), replaced ? 2 : 1);
                UNIT_ASSERT(!ring.IsAllowedHotSlot(ring.HotStorage.Slots + ring.HotStorage.SlotCount - 1));
                for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
                    UNIT_ASSERT(ring.Rings->Hot(level).SelectPopSlot(ring.HotStorage.Slots, cursorProbeCapacity) ==
                                ring.HotStorage.Slots + 1);
                    UNIT_ASSERT(ring.Rings->Hot(level).SelectPushSlot(ring.HotStorage.Slots, cursorProbeCapacity) ==
                                ring.HotStorage.Slots + 1);
                }
                UNIT_ASSERT(ring.Rings->Cold().SelectPopSlot(ring.ColdStorage.Slots, cursorProbeCapacity) ==
                            ring.ColdStorage.Slots + 1);
                UNIT_ASSERT(ring.Rings->Cold().SelectPushSlot(ring.ColdStorage.Slots, cursorProbeCapacity) ==
                            ring.ColdStorage.Slots + 1);
            }
        }
    }

    Y_UNIT_TEST(FootprintRejectsInvalidInput) {
        TCacheCapacityConfig footprint;
        UNIT_ASSERT(!TryCalculateSharedCacheFootprint(4, 4096, 0, 0, footprint));
        UNIT_ASSERT(!TryCalculateSharedCacheFootprint(33, 4096, 0, 0, footprint));
        UNIT_ASSERT(!TryCalculateSharedCacheFootprint(5, 0, 0, 0, footprint));
        UNIT_ASSERT(!TryCalculateSharedCacheFootprint(5, Max<ui64>(), 0, 0, footprint));
    }

    Y_UNIT_TEST(CapacitySelectsLargestFit) {
        TCacheCapacityConfig fiveBits;
        TCacheCapacityConfig sixBits;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 2, fiveBits));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 2, sixBits));

        TCacheCapacityConfig exact;
        UNIT_ASSERT(TryCalculateSharedCacheCapacity(fiveBits.TotalBytes, 4096, 0, 2, exact));
        UNIT_ASSERT_VALUES_EQUAL(exact.AddressBits, 5);

        TCacheCapacityConfig next;
        UNIT_ASSERT(TryCalculateSharedCacheCapacity(sixBits.TotalBytes - 1, 4096, 0, 2, next));
        UNIT_ASSERT_VALUES_EQUAL(next.AddressBits, 5);
        UNIT_ASSERT(TryCalculateSharedCacheCapacity(sixBits.TotalBytes, 4096, 0, 2, next));
        UNIT_ASSERT_VALUES_EQUAL(next.AddressBits, 6);

        UNIT_ASSERT(!TryCalculateSharedCacheCapacity(fiveBits.TotalBytes - 1, 4096, 0, 2, next));
    }

    Y_UNIT_TEST(CapacityResizeDeltaBits) {
        TCacheCapacityConfig current;
        TCacheCapacityConfig doubled;
        TCacheCapacityConfig quadrupled;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, doubled));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 0, quadrupled));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(current, quadrupled, 0);
        UNIT_ASSERT(space);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(doubled, transition) == EPrepareResult::Prepared);
        UNIT_ASSERT_VALUES_EQUAL(transition.ResizeDeltaBits(), 1);
        UNIT_ASSERT(space->AbandonTransition(transition));

        UNIT_ASSERT(space->PrepareTransition(quadrupled, transition) == EPrepareResult::Prepared);
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
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 2, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 2);
        UNIT_ASSERT(space);

        {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            auto operation = space->BeginOperation();
            UNIT_ASSERT(SpaceHazardIsActive(TCacheSpaceTestAccess::Hazard(*space, 1).State.load()));
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardGeneration(TCacheSpaceTestAccess::Hazard(*space, 1).State.load()),
                operation.EntryGeneration());
        }
        UNIT_ASSERT(!SpaceHazardIsActive(TCacheSpaceTestAccess::Hazard(*space, 1).State.load()));
    }

    Y_UNIT_TEST(HotPolicyResizeDoesNotUseHazards) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);

        std::atomic<bool> active = false;
        std::atomic<bool> resume = false;
        ui64 observedHotSlots = 0;
        std::thread worker([&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
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
        const ui32 target = static_cast<ui32>(capacity.Tiers[ECacheTier::Regular].HotSlots - 1);
        UNIT_ASSERT(space->BeginHotResize(ECacheTier::Regular, target, resize));
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
        UNIT_ASSERT_VALUES_EQUAL(observedHotSlots, capacity.Tiers[ECacheTier::Regular].HotSlots);
    }

    Y_UNIT_TEST(NestedOperationsShareHazard) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };

        auto outer = space->BeginOperation();
        UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(TCacheSpaceTestAccess::Hazard(*space, 0).State.load()), 1);
        UNIT_ASSERT_VALUES_EQUAL(outer.EntryGeneration(), 0);
        UNIT_ASSERT(TCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
        {
            auto nested = space->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(
                SpaceHazardActiveCount(TCacheSpaceTestAccess::Hazard(*space, 0).State.load()), 2);
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardGeneration(TCacheSpaceTestAccess::Hazard(*space, 0).State.load()),
                outer.EntryGeneration());
            UNIT_ASSERT_VALUES_EQUAL(
                nested.EntryGeneration(), TCacheSpaceTestAccess::CurrentSpaceState(*space).Generation());
            UNIT_ASSERT_VALUES_EQUAL(nested.View().Generation, nested.EntryGeneration());
        }
        UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(TCacheSpaceTestAccess::Hazard(*space, 0).State.load()), 1);
    }

    Y_UNIT_TEST(GenerationDrainResumesAtFirstBlockedHazard) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 3, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 3);
        UNIT_ASSERT(space);
        ui32 notifications = 0;
        TCacheSpaceTestAccess::SetHazardDrainNotify(*space, { &notifications, [](void* context) noexcept {
                                                                 ++*static_cast<ui32*>(context);
                                                             } });
        auto first = MakeHolder<TSpaceOperation>(TCacheSpaceTestAccess::BeginOnHazard(*space, 0));
        auto second = MakeHolder<TSpaceOperation>(TCacheSpaceTestAccess::BeginOnHazard(*space, 1));
        auto third = MakeHolder<TSpaceOperation>(TCacheSpaceTestAccess::BeginOnHazard(*space, 2));
        first.Reset();
        UNIT_ASSERT(TCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));

        ui32 cursor = 0;
        UNIT_ASSERT(!TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 1);
        UNIT_ASSERT(TCacheSpaceTestAccess::Hazard(*space, 1).State.load() & SpaceHazardDrainWaiter);
        second.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT(!TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 2);
        UNIT_ASSERT(TCacheSpaceTestAccess::Hazard(*space, 2).State.load() & SpaceHazardDrainWaiter);
        third.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 2);
        UNIT_ASSERT(TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 3);

        auto current = MakeHolder<TSpaceOperation>(TCacheSpaceTestAccess::BeginOnHazard(*space, 0));
        UNIT_ASSERT(TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT(TCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
        cursor = 0;
        UNIT_ASSERT(!TCacheSpaceTestAccess::TryDrainBefore(*space, 2, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 0);
        current.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 3);
        UNIT_ASSERT(TCacheSpaceTestAccess::TryDrainBefore(*space, 2, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 3);
    }

    Y_UNIT_TEST(GenerationDrainKeepsNestedWaiterUntilLastExit) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        ui32 notifications = 0;
        TCacheSpaceTestAccess::SetHazardDrainNotify(*space, { &notifications, [](void* context) noexcept {
                                                                 ++*static_cast<ui32*>(context);
                                                             } });
        auto outer = MakeHolder<TSpaceOperation>(TCacheSpaceTestAccess::BeginOnHazard(*space, 0));
        UNIT_ASSERT(TCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
        auto nested = MakeHolder<TSpaceOperation>(TCacheSpaceTestAccess::BeginOnHazard(*space, 0));
        UNIT_ASSERT_VALUES_EQUAL(nested->EntryGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(SpaceHazardGeneration(outer->Hazard().State.load()), 0);

        ui32 cursor = 0;
        UNIT_ASSERT(!TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        nested.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(outer->Hazard().State.load()), 1);
        UNIT_ASSERT(outer->Hazard().State.load() & SpaceHazardDrainWaiter);
        UNIT_ASSERT(!TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        outer.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT(TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 1);
    }

    Y_UNIT_TEST(GenerationDrainWakesAfterRejectedOldRegistration) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        ui32 notifications = 0;
        TCacheSpaceTestAccess::SetHazardDrainNotify(*space, { &notifications, [](void* context) noexcept {
                                                                 ++*static_cast<ui32*>(context);
                                                             } });
        ui32 hookCalls = 0;
        ui32 cursor = 0;
        bool advanced = false;
        bool blocked = false;
        auto operation = TCacheSpaceTestAccess::BeginOnHazardWithHook(*space, 0, [&]() noexcept {
            if (++hookCalls == 1) {
                advanced = TCacheSpaceTestAccess::AdvanceSpaceGeneration(*space);
                blocked = !TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor);
            }
        });
        UNIT_ASSERT(advanced && blocked);
        UNIT_ASSERT_VALUES_EQUAL(hookCalls, 2);
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT_VALUES_EQUAL(operation.EntryGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cursor, 0);
        UNIT_ASSERT(!(operation.Hazard().State.load() & SpaceHazardDrainWaiter));
        UNIT_ASSERT(TCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 1);
    }

    Y_UNIT_TEST(InitialMappingsAndFreeContents) {
        TCacheCapacityConfig current;
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 3, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 3, reserved));

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(current, reserved, 3);
        UNIT_ASSERT(space);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, 32);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, 32);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::ReservedCapacity(*space).HandleCount(), 64);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::BucketCount(*space), 32);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::ReservedCapacity(*space).BucketCount(), 64);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheSpaceTestAccess::CurrentView(*space).Tiers[ECacheTier::Regular].HotStorage.SlotCount,
            current.Tiers[ECacheTier::Regular].HotSlots);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheSpaceTestAccess::CurrentView(*space).Cold().Capacity, current.Tiers[ECacheTier::Regular].ColdSlots);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheSpaceTestAccess::CurrentView(*space).KeepCold().Capacity, current.Tiers[ECacheTier::Keep].ColdSlots);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), 30);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::HazardCount(*space), 3);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::Hazard(*space, 0).State.load(), 0);

        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(TCacheSpaceTestAccess::Handles(*space)) % 64, 0);
        for (ui32 index = 0; index < current.HandleCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::Handles(*space)[index].State.load(), 0);
        }
        for (ui32 index = 0; index < current.ColdSlotCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::ColdSlots(*space)[index].load(), 0);
        }
        for (ui32 index = 0; index < current.BucketCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::Buckets(*space)[index].load(), 0);
        }
        for (ui32 index = 0; index < current.Tiers[ECacheTier::Regular].HotSlots; ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::HotSlots(*space)[index].load(), 0);
        }
        for (ui32 index = 0; index < current.KeepColdSlotCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::KeepColdSlots(*space)[index].load(), 0);
        }
        for (ui32 index = 2; index < current.HandleCount(); ++index) {
            const auto cacheItem = TCacheItem::FromRaw(TCacheSpaceTestAccess::FreeSlots(*space)[index - 2].load());
            UNIT_ASSERT_VALUES_EQUAL(cacheItem.Version(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cacheItem.Index(), index);
            UNIT_ASSERT(!cacheItem.IsFrozen());
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeSlots(*space)[current.HandleCount() - 2].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeSlots(*space)[current.HandleCount() - 1].load(), 0);
    }

    Y_UNIT_TEST(RejectsInvalidMappingConfiguration) {
        TCacheCapacityConfig current;
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, reserved));

        auto invalid = current;
        invalid.AddressBits = reserved.AddressBits + 1;
        TCacheThreadSubSystem threadHazards(1);
        UNIT_ASSERT(!TCacheSpace::Create(invalid, reserved, threadHazards));

        invalid = current;
        invalid.AddressBits = MinSharedCacheAddressBits - 1;
        UNIT_ASSERT(!TCacheSpace::Create(invalid, reserved, threadHazards));
    }

    Y_UNIT_TEST(TransitionRejectsChangedPageSizeEstimate) {
        TCacheCapacityConfig current;
        TCacheCapacityConfig reserved;
        TCacheCapacityConfig changedEstimate;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, reserved));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 2048, 0, 1, changedEstimate));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(current, reserved, 1);
        UNIT_ASSERT(space);
        const auto state = space->CurrentSpaceState();
        const auto* handles = TCacheSpaceTestAccess::Handles(*space);
        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(changedEstimate, transition) == EPrepareResult::Rejected);
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT(space->CurrentSpaceState() == state);
        UNIT_ASSERT(TCacheSpaceTestAccess::Handles(*space) == handles);
        UNIT_ASSERT(space->PrepareTransition(reserved, transition) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->AbandonTransition(transition));
    }

    Y_UNIT_TEST(AllocateAndReturnItem) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };
        auto operation = space->BeginOperation();

        TVector<bool> seen(capacity.HandleCount(), false);
        for (ui32 count = 0; count < capacity.HandleCount() - 2; ++count) {
            const auto index = operation.TryAllocateHandle();
            UNIT_ASSERT(index);
            UNIT_ASSERT(index >= 2 && index < capacity.HandleCount());
            UNIT_ASSERT(!seen[index]);
            seen[index] = true;
            const auto state = THandleState::FromRaw(TCacheSpaceTestAccess::Handles(*space)[index].State.load());
            UNIT_ASSERT(state.IsBegin());
            UNIT_ASSERT_VALUES_EQUAL(state.Version(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), 0);
        UNIT_ASSERT(!operation.TryAllocateHandle());

        constexpr ui32 ReturnedIndex = 7;
        auto state = THandleState::FromRaw(TCacheSpaceTestAccess::Handles(*space)[ReturnedIndex].State.load());
        TCacheSpaceTestAccess::Handles(*space)[ReturnedIndex].State.store(
            state.WithState(EHandleState::Free).Raw(), std::memory_order_release);
        UNIT_ASSERT(operation.ReturnFreeHandle(ReturnedIndex, state.Version()));
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), 1);
        const auto allocatedAgain = operation.TryAllocateHandle();
        UNIT_ASSERT(allocatedAgain);
        UNIT_ASSERT_VALUES_EQUAL(allocatedAgain, ReturnedIndex);
    }

    Y_UNIT_TEST(ConcurrentAllocationIsUnique) {
        constexpr ui32 HandleCount = 1024;
        constexpr ui32 ThreadCount = 8;

        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(10, 4096, 0, ThreadCount, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, ThreadCount);
        UNIT_ASSERT(space);

        TVector<TVector<ui32>> allocated(ThreadCount);
        TVector<std::thread> threads;
        for (ui32 thread = 0; thread < ThreadCount; ++thread) {
            threads.emplace_back([&, thread] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, thread);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
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
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), 0);
    }

    Y_UNIT_TEST(AllocateReroutesAfterGenerationChange) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };
        auto operation = space->BeginOperation();

        bool advanced = false;
        auto advance = [&]() noexcept {
            if (!advanced) {
                advanced = true;
                UNIT_ASSERT(TCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
            }
        };

        TVector<bool> seen(capacity.HandleCount(), false);
        const auto first = TCacheSpaceTestAccess::TryAllocateHandle(*space, operation, advance);
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
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), 0);
    }

    Y_UNIT_TEST(ReturnReroutesAfterGenerationChange) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };
        auto operation = space->BeginOperation();

        const auto returned = operation.TryAllocateHandle();
        UNIT_ASSERT(returned);
        THandleState state = THandleState::FromRaw(TCacheSpaceTestAccess::Handles(*space)[returned].State.load());
        TCacheSpaceTestAccess::Handles(*space)[returned].State.store(
            state.WithState(EHandleState::Free).Raw(), std::memory_order_release);

        bool advanced = false;
        auto advance = [&]() noexcept {
            if (!advanced) {
                advanced = true;
                UNIT_ASSERT(TCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
            }
        };
        UNIT_ASSERT(TCacheSpaceTestAccess::ReturnFreeHandle(*space, operation, returned, state.Version(), advance));
        UNIT_ASSERT(advanced);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), capacity.HandleCount() - 2);

        TVector<bool> seen(capacity.HandleCount(), false);
        ui32 count = 0;
        while (const auto index = operation.TryAllocateHandle()) {
            UNIT_ASSERT(!seen[index]);
            seen[index] = true;
            ++count;
        }
        UNIT_ASSERT(seen[returned]);
        UNIT_ASSERT_VALUES_EQUAL(count, capacity.HandleCount() - 2);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), 0);
    }

    Y_UNIT_TEST(CapacityGrowthAndShrinkAliasTheLivePrefix) {
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };
        THandle* smallHandles = TCacheSpaceTestAccess::Handles(*space);
        smallHandles[2].Key0.store(17, std::memory_order_relaxed);

        TTransition growth;
        UNIT_ASSERT(space->PrepareTransition(large, growth) == EPrepareResult::Prepared);
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::Prepare);
        UNIT_ASSERT_VALUES_EQUAL(growth.ResizeDeltaBits(), 1);
        {
            auto oldOperation = space->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().Generation, 0);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().Handles, smallHandles);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().HandleCount, small.HandleCount());
            UNIT_ASSERT(space->PublishMigrationView(growth));
            UNIT_ASSERT(growth.Phase() == ETransitionPhase::MigrationDrain);
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(
                TCacheSpaceTestAccess::Handles(*space)[2].Key0.load(std::memory_order_relaxed), 17);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().Handles, smallHandles);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.View().HandleCount, small.HandleCount());
            TCacheSpaceTestAccess::Handles(*space)[2].Key1.store(29, std::memory_order_relaxed);
            UNIT_ASSERT_VALUES_EQUAL(smallHandles[2].Key1.load(std::memory_order_relaxed), 29);
            UNIT_ASSERT(!space->TryDrainTransition(growth));
        }
        UNIT_ASSERT(space->TryDrainTransition(growth));
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::Migrate);
        {
            auto migrationOperation = space->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().Generation, 1);
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().Handles, TCacheSpaceTestAccess::Handles(*space));
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().HandleCount, small.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().AllocationLimit, small.HandleCount());
            CompleteEmptyBucketResize(*space, large.AddressBits);
            UNIT_ASSERT(space->PublishFinalView(growth));
            CompleteGrowthAppend(*space, growth);
            UNIT_ASSERT_VALUES_EQUAL(migrationOperation.View().HandleCount, small.HandleCount());
            UNIT_ASSERT(!space->TryDrainTransition(growth));
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, large.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, large.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), large.HandleCount() - 2);
        UNIT_ASSERT(space->TryDrainTransition(growth));
        CompleteFinalDrain(*space, growth);
        UNIT_ASSERT(space->TryReleaseTransition(growth));
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::Commit);
        UNIT_ASSERT(space->CommitTransition(growth));
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::Handles(*space), smallHandles);

        TTransition shrink;
        UNIT_ASSERT(space->PrepareTransition(small, shrink) == EPrepareResult::Prepared);
        THandle* largeHandles = TCacheSpaceTestAccess::Handles(*space);
        UNIT_ASSERT(space->PublishMigrationView(shrink));
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, small.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, large.HandleCount());
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
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::Handles(*space)[2].Key1.load(std::memory_order_relaxed), 29);
        TCacheSpaceTestAccess::Handles(*space)[2].Key0.store(41, std::memory_order_relaxed);
        UNIT_ASSERT_VALUES_EQUAL(largeHandles[2].Key0.load(std::memory_order_relaxed), 41);
        UNIT_ASSERT(space->TryDrainTransition(shrink));
        CompleteFinalDrain(*space, shrink);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), small.HandleCount() - 2);
        UNIT_ASSERT(space->TryReleaseTransition(shrink));
        UNIT_ASSERT(space->CommitTransition(shrink));
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::Handles(*space), smallHandles);
    }

    Y_UNIT_TEST(CapacityGrowthAddsEveryNewHandleToFree) {
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(large, transition) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->PublishMigrationView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteEmptyBucketResize(*space, large.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(transition));
        CompleteGrowthAppend(*space, transition);
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteFinalDrain(*space, transition);
        UNIT_ASSERT(space->TryReleaseTransition(transition));
        UNIT_ASSERT(space->CommitTransition(transition));
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), large.HandleCount() - 2);

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
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(10, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(11, 4096, 0, 1, large));

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };

        TTransition canceled;
        UNIT_ASSERT(space->PrepareTransition(large, canceled) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->PublishMigrationView(canceled));
        UNIT_ASSERT(canceled.Phase() == ETransitionPhase::InitializeGrowth);
        UNIT_ASSERT(space->AbandonTransition(canceled));
        UNIT_ASSERT(canceled.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(large, transition) == EPrepareResult::Prepared);
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
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), large.HandleCount() - 2);

        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteFinalDrain(*space, transition);
        UNIT_ASSERT(space->TryReleaseTransition(transition));
        UNIT_ASSERT(space->CommitTransition(transition));
    }

    Y_UNIT_TEST(CapacityShrinkDrainsWrappedFreeRing) {
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };
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
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), large.HandleCount() - 2);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(small, transition) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->PublishMigrationView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteEmptyBucketResize(*space, small.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteFinalDrain(*space, transition);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::FreeCount(*space), small.HandleCount() - 2);
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
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, large));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 0);
        UNIT_ASSERT(space);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(small, transition) == EPrepareResult::Prepared);
        const ui64 generation = TCacheSpaceTestAccess::CurrentSpaceState(*space).Generation();
        const TSpaceState publishedState =
            TCacheSpaceTestAccess::CurrentSpaceState(*space).WithGeneration(generation + 1);
        bool called = false;
        auto check = [&]() noexcept {
            called = true;
            const TSpaceView& publishedView = TCacheSpaceTestAccess::SpaceView(*space, publishedState);
            UNIT_ASSERT_VALUES_EQUAL(publishedView.AllocationLimit, small.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(publishedView.HandleCount, large.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, large.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, large.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentSpaceState(*space).Generation(), generation);
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::PublishMigration);
        };
        UNIT_ASSERT(TCacheSpaceTestAccess::PublishMigrationView(*space, transition, check));
        UNIT_ASSERT(called);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentSpaceState(*space).Generation(), generation + 1);
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::MigrationDrain);
    }

    Y_UNIT_TEST(CapacityReleaseRetriesFailedStage) {
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(10, 4096, 0, 0, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(11, 4096, 0, 0, large));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 0);
        UNIT_ASSERT(space);

        TTransition transition;
        UNIT_ASSERT(space->PrepareTransition(small, transition) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->PublishMigrationView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        CompleteEmptyBucketResize(*space, small.AddressBits);
        UNIT_ASSERT(space->PublishFinalView(transition));
        UNIT_ASSERT(space->TryDrainTransition(transition));
        UNIT_ASSERT(TCacheSpaceTestAccess::FinalDrain(*space, transition));
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
        UNIT_ASSERT(!TCacheSpaceTestAccess::TryReleaseTransition(*space, transition, failHandles));
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
        UNIT_ASSERT(TCacheSpaceTestAccess::TryReleaseTransition(*space, transition, succeed));
        UNIT_ASSERT(calls >= 1);
        UNIT_ASSERT(transition.ReleaseStage() == ESpaceMap::Done);
        UNIT_ASSERT(space->CommitTransition(transition));
    }

    Y_UNIT_TEST(CapacityShutdownCancelsPrivateAndAbandonsPublished) {
        TCacheCapacityConfig small;
        TCacheCapacityConfig large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 0, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 0, large));

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 0);
        UNIT_ASSERT(space);
        THandle* initialHandles = TCacheSpaceTestAccess::Handles(*space);

        TTransition privateTransition;
        UNIT_ASSERT(space->PrepareTransition(large, privateTransition) == EPrepareResult::Prepared);
        UNIT_ASSERT_VALUES_EQUAL(privateTransition.ResizeDeltaBits(), 1);
        UNIT_ASSERT(space->AbandonTransition(privateTransition));
        UNIT_ASSERT(privateTransition.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::Handles(*space), initialHandles);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());

        TTransition publishedTransition;
        UNIT_ASSERT(space->PrepareTransition(large, publishedTransition) == EPrepareResult::Prepared);
        UNIT_ASSERT(space->PublishMigrationView(publishedTransition));
        const ui64 generation = TCacheSpaceTestAccess::CurrentSpaceState(*space).Generation();
        UNIT_ASSERT(space->AbandonTransition(publishedTransition));
        UNIT_ASSERT(publishedTransition.Phase() == ETransitionPhase::AbandonedForShutdown);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentSpaceState(*space).Generation(), generation);
        UNIT_ASSERT_VALUES_EQUAL(TCacheSpaceTestAccess::CurrentView(*space).HandleCount, small.HandleCount());
        UNIT_ASSERT(!space->TryDrainTransition(publishedTransition));
        UNIT_ASSERT(!space->TryReleaseTransition(publishedTransition));
        UNIT_ASSERT(!space->CommitTransition(publishedTransition));
    }
}

} // namespace NKikimr::NSharedCache
