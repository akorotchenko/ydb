#include <ydb/core/tablet_flat/shared_cache_thread_test.h>
#include "shared_cache_space.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/system/info.h>

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
        return space.NewestView().Tiers[ECacheTier::Regular].HotStorage.Slots;
    }

    static std::atomic<ui64>* ColdSlots(const TSharedCacheSpace& space) noexcept {
        return space.NewestView().Tiers[ECacheTier::Regular].ColdStorage.Slots;
    }

    static std::atomic<ui64>* KeepColdSlots(const TSharedCacheSpace& space) noexcept {
        return space.NewestView().Tiers[ECacheTier::Keep].ColdStorage.Slots;
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
        return *space.Hazards_[index];
    }

    static ui32 HazardCount(const TSharedCacheSpace& space) noexcept {
        return space.HazardCount_;
    }

    static bool AdvanceSpaceGeneration(TSharedCacheSpace& space) noexcept {
        return space.AdvanceSpaceGeneration();
    }

    static bool TryDrainBefore(TSharedCacheSpace& space, ui32 generation, ui32& cursor) noexcept {
        return space.TryDrainBefore(generation, cursor);
    }

    static void SetHazardDrainNotify(TSharedCacheSpace& space, TCacheNotify notify) noexcept {
        space.HazardDrainNotify_ = notify;
    }

    static TSpaceOperation BeginOnHazard(TSharedCacheSpace& space, ui32 index) noexcept {
        return TSpaceOperation::Begin(space.SpaceState_, Hazard(space, index), space.SpaceViews_);
    }

    template <class TCallback>
    static TSpaceOperation BeginOnHazardWithHook(TSharedCacheSpace& space, ui32 index, TCallback&& callback) noexcept {
        return TSpaceOperation::BeginWithHook(
            space.SpaceState_, Hazard(space, index), space.SpaceViews_, std::forward<TCallback>(callback));
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
    static bool PrepareTransition(TSharedCacheSpace& space, const TSharedCacheCapacity& target, TTransition& transition,
        TCallback& callback) noexcept {
        return space.PrepareTransitionWithHook(target, transition,
            {
                .Context = &callback,
                .Function = [](void* context, ESpaceMap mapping) noexcept {
                    return (*static_cast<TCallback*>(context))(mapping);
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
    void CheckLegacyMappings(
        const TSpaceView& view, const TSharedCacheCapacity& logical, const TSharedCacheCapacity& mapped) {
        const auto& regular = view.Tiers[ECacheTier::Regular];
        const auto& keep = view.Tiers[ECacheTier::Keep];
        UNIT_ASSERT(regular.Id == ECacheTier::Regular);
        UNIT_ASSERT(keep.Id == ECacheTier::Keep);
        UNIT_ASSERT(regular.Rings && keep.Rings && regular.Rings != keep.Rings);
        UNIT_ASSERT(regular.HotStorage.Slots && regular.ColdStorage.Slots && keep.ColdStorage.Slots);
        UNIT_ASSERT_VALUES_EQUAL(regular.HotStorage.SlotCount, logical.HotSlotCount());
        UNIT_ASSERT_VALUES_EQUAL(regular.ColdStorage.SlotCount, logical.ColdSlotCount());
        UNIT_ASSERT_VALUES_EQUAL(keep.ColdStorage.SlotCount, logical.KeepColdSlotCount());
        UNIT_ASSERT_VALUES_EQUAL(regular.HotStorage.Bytes, mapped.HotSlotCount() * sizeof(std::atomic<ui64>));
        UNIT_ASSERT_VALUES_EQUAL(regular.ColdStorage.Bytes, mapped.ColdSlotCount() * sizeof(std::atomic<ui64>));
        UNIT_ASSERT_VALUES_EQUAL(keep.ColdStorage.Bytes, mapped.KeepColdSlotCount() * sizeof(std::atomic<ui64>));
        UNIT_ASSERT(!keep.HotStorage.Slots);
        UNIT_ASSERT_VALUES_EQUAL(keep.HotStorage.Bytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(keep.HotStorage.SlotCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(view.Cold().Capacity, logical.ColdSlotCount());
        UNIT_ASSERT_VALUES_EQUAL(view.KeepCold().Capacity, logical.KeepColdSlotCount());
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
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));
        for (bool growing : { true, false }) {
            const auto& current = growing ? small : large;
            const auto& target = growing ? large : small;
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(current, large, 1);
            UNIT_ASSERT(space);
            const auto before = TSharedCacheSpaceTestAccess::CurrentView(*space);
            CheckLegacyMappings(before, current, current);
            before.Tiers[ECacheTier::Regular].HotStorage.Slots[0].store(11);
            before.Tiers[ECacheTier::Regular].ColdStorage.Slots[0].store(22);
            before.Tiers[ECacheTier::Keep].ColdStorage.Slots[0].store(33);
            TTransition transition;
            UNIT_ASSERT(space->PrepareTransition(target, transition));
            {
                auto oldOperation = TSharedCacheSpaceTestAccess::BeginOnHazard(*space, 0);
                UNIT_ASSERT(space->PublishMigrationView(transition));
                CheckLegacyMappings(TSharedCacheSpaceTestAccess::CurrentView(*space), current, large);
                CheckLegacyMappings(oldOperation.View(), current, current);
                UNIT_ASSERT(!space->TryDrainTransition(transition));
            }
            UNIT_ASSERT(space->TryDrainTransition(transition));
            CompleteEmptyBucketResize(*space, target.AddressBits);
            {
                auto migrationOperation = TSharedCacheSpaceTestAccess::BeginOnHazard(*space, 0);
                UNIT_ASSERT(space->PublishFinalView(transition));
                if (growing) {
                    CompleteGrowthAppend(*space, transition);
                }
                CheckLegacyMappings(TSharedCacheSpaceTestAccess::CurrentView(*space), target, target);
                CheckLegacyMappings(migrationOperation.View(), current, large);
                UNIT_ASSERT(!space->TryDrainTransition(transition));
            }
            UNIT_ASSERT(space->TryDrainTransition(transition));
            CompleteFinalDrain(*space, transition);
            UNIT_ASSERT(space->TryReleaseTransition(transition));
            UNIT_ASSERT(space->CommitTransition(transition));
            const auto& after = TSharedCacheSpaceTestAccess::CurrentView(*space);
            CheckLegacyMappings(after, target, target);
            UNIT_ASSERT(before.Tiers[ECacheTier::Regular].Rings == after.Tiers[ECacheTier::Regular].Rings);
            UNIT_ASSERT(before.Tiers[ECacheTier::Keep].Rings == after.Tiers[ECacheTier::Keep].Rings);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Regular].HotStorage.Slots[0].load(), 11);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Regular].ColdStorage.Slots[0].load(), 22);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Keep].ColdStorage.Slots[0].load(), 33);
        }
    }

    Y_UNIT_TEST(PartialPreparationKeepsThePublishedTierMappings) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, large));
        const std::array stages{ ESpaceMap::Handles, ESpaceMap::Buckets, ESpaceMap::Hot, ESpaceMap::Cold,
            ESpaceMap::KeepCold, ESpaceMap::Free };
        for (ui32 failure = 0; failure < stages.size(); ++failure) {
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 1);
            UNIT_ASSERT(space);
            const auto state = space->CurrentSpaceState();
            const auto before = TSharedCacheSpaceTestAccess::CurrentView(*space);
            before.Tiers[ECacheTier::Regular].HotStorage.Slots[0].store(11);
            before.Tiers[ECacheTier::Regular].ColdStorage.Slots[0].store(22);
            before.Tiers[ECacheTier::Keep].ColdStorage.Slots[0].store(33);
            ui32 calls = 0;
            bool ordered = true;
            auto prepare = [&](ESpaceMap mapping) noexcept {
                ordered &= calls < stages.size() && mapping == stages[calls];
                ++calls;
                return mapping != stages[failure];
            };
            TTransition failed;
            UNIT_ASSERT(!TSharedCacheSpaceTestAccess::PrepareTransition(*space, large, failed, prepare));
            UNIT_ASSERT(ordered);
            UNIT_ASSERT_VALUES_EQUAL(calls, failure + 1);
            UNIT_ASSERT(failed.Phase() == ETransitionPhase::Idle);
            UNIT_ASSERT(space->CurrentSpaceState() == state);
            TTransition retry;
            UNIT_ASSERT(space->PrepareTransition(large, retry));
            UNIT_ASSERT(space->AbandonTransition(retry));
            const auto& after = TSharedCacheSpaceTestAccess::CurrentView(*space);
            CheckLegacyMappings(after, small, small);
            UNIT_ASSERT(after.Tiers[ECacheTier::Regular].HotStorage.Slots ==
                        before.Tiers[ECacheTier::Regular].HotStorage.Slots);
            UNIT_ASSERT(after.Tiers[ECacheTier::Regular].ColdStorage.Slots ==
                        before.Tiers[ECacheTier::Regular].ColdStorage.Slots);
            UNIT_ASSERT(
                after.Tiers[ECacheTier::Keep].ColdStorage.Slots == before.Tiers[ECacheTier::Keep].ColdStorage.Slots);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Regular].HotStorage.Slots[0].load(), 11);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Regular].ColdStorage.Slots[0].load(), 22);
            UNIT_ASSERT_VALUES_EQUAL(after.Tiers[ECacheTier::Keep].ColdStorage.Slots[0].load(), 33);
        }
    }

    Y_UNIT_TEST(TierReleaseRetriesEachRingStage) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(14, 4096, 0, 1, small));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(15, 4096, 0, 1, large));
        for (auto failure : { ESpaceMap::Hot, ESpaceMap::Cold, ESpaceMap::KeepCold }) {
            auto [threads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 1);
            UNIT_ASSERT(space);
            TTransition transition;
            UNIT_ASSERT(space->PrepareTransition(small, transition));
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
            UNIT_ASSERT(!TSharedCacheSpaceTestAccess::TryReleaseTransition(*space, transition, release));
            UNIT_ASSERT(reached);
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::ReleaseRetry);
            UNIT_ASSERT(transition.ReleaseStage() == failure);
            CheckLegacyMappings(TSharedCacheSpaceTestAccess::CurrentView(*space), small, small);
            ESpaceMap first = ESpaceMap::Done;
            auto retry = [&](ESpaceMap mapping, const void*, size_t) noexcept {
                if (first == ESpaceMap::Done) {
                    first = mapping;
                }
                return true;
            };
            UNIT_ASSERT(TSharedCacheSpaceTestAccess::TryReleaseTransition(*space, transition, retry));
            UNIT_ASSERT(first == failure);
            UNIT_ASSERT(space->CommitTransition(transition));
            CheckLegacyMappings(TSharedCacheSpaceTestAccess::CurrentView(*space), small, small);
        }
    }
}

Y_UNIT_TEST_SUITE(TSharedCacheSpaceTest) {
    Y_UNIT_TEST(StableMappingCancellationAndRegrowthPreserveLivePrefix) {
        const size_t small = NSystemInfo::GetPageSize() * 2;
        const size_t large = small * 2;
        TSharedCacheMapping mapping(TSharedCacheMapping::EAddressMode::Stable);
        void* base = nullptr;
        UNIT_ASSERT(mapping.Allocate(small, large * 2, base));
        auto* bytes = static_cast<char*>(base);
        bytes[0] = 17;
        bytes[small - 1] = 29;

        void* prepared = nullptr;
        UNIT_ASSERT(mapping.Prepare(large, prepared));
        UNIT_ASSERT_VALUES_EQUAL(prepared, base);
        static_cast<char*>(prepared)[large - 1] = 41;
        mapping.Cancel(prepared, large);
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
        mapping.Cancel(prepared, small);
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
        TSharedCacheMapping mapping(TSharedCacheMapping::EAddressMode::Stable);
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
        TSharedCacheCapacity footprint;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 128, 3, footprint));
        UNIT_ASSERT_VALUES_EQUAL(footprint.FixedBytes, 128);
        UNIT_ASSERT_VALUES_EQUAL(footprint.AddressBits, 5);
        UNIT_ASSERT_VALUES_EQUAL(footprint.HandleCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(footprint.BucketCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(footprint.HotSlotCount(), 24);
        UNIT_ASSERT_VALUES_EQUAL(footprint.ColdSlotCount(), 16);
        UNIT_ASSERT_VALUES_EQUAL(footprint.KeepColdSlotCount(), 16);

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

    Y_UNIT_TEST(KeepColdRingHasHalfTheHandleSlots) {
        TSharedCacheCapacity capacity;
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
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 0);
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
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(current, quadrupled, 0);
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
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 2);
        UNIT_ASSERT(space);

        {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
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
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };

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

    Y_UNIT_TEST(GenerationDrainResumesAtFirstBlockedHazard) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 3, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 3);
        UNIT_ASSERT(space);
        ui32 notifications = 0;
        TSharedCacheSpaceTestAccess::SetHazardDrainNotify(*space, { &notifications, [](void* context) noexcept {
                                                                       ++*static_cast<ui32*>(context);
                                                                   } });
        auto first = MakeHolder<TSpaceOperation>(TSharedCacheSpaceTestAccess::BeginOnHazard(*space, 0));
        auto second = MakeHolder<TSpaceOperation>(TSharedCacheSpaceTestAccess::BeginOnHazard(*space, 1));
        auto third = MakeHolder<TSpaceOperation>(TSharedCacheSpaceTestAccess::BeginOnHazard(*space, 2));
        first.Reset();
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));

        ui32 cursor = 0;
        UNIT_ASSERT(!TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 1);
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::Hazard(*space, 1).State.load() & SpaceHazardDrainWaiter);
        second.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT(!TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 2);
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::Hazard(*space, 2).State.load() & SpaceHazardDrainWaiter);
        third.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 2);
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 3);

        auto current = MakeHolder<TSpaceOperation>(TSharedCacheSpaceTestAccess::BeginOnHazard(*space, 0));
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
        cursor = 0;
        UNIT_ASSERT(!TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 2, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 0);
        current.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 3);
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 2, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 3);
    }

    Y_UNIT_TEST(GenerationDrainKeepsNestedWaiterUntilLastExit) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        ui32 notifications = 0;
        TSharedCacheSpaceTestAccess::SetHazardDrainNotify(*space, { &notifications, [](void* context) noexcept {
                                                                       ++*static_cast<ui32*>(context);
                                                                   } });
        auto outer = MakeHolder<TSpaceOperation>(TSharedCacheSpaceTestAccess::BeginOnHazard(*space, 0));
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::AdvanceSpaceGeneration(*space));
        auto nested = MakeHolder<TSpaceOperation>(TSharedCacheSpaceTestAccess::BeginOnHazard(*space, 0));
        UNIT_ASSERT_VALUES_EQUAL(nested->EntryGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(SpaceHazardGeneration(outer->Hazard().State.load()), 0);

        ui32 cursor = 0;
        UNIT_ASSERT(!TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        nested.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(outer->Hazard().State.load()), 1);
        UNIT_ASSERT(outer->Hazard().State.load() & SpaceHazardDrainWaiter);
        UNIT_ASSERT(!TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        outer.Reset();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 1);
    }

    Y_UNIT_TEST(GenerationDrainWakesAfterRejectedOldRegistration) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        ui32 notifications = 0;
        TSharedCacheSpaceTestAccess::SetHazardDrainNotify(*space, { &notifications, [](void* context) noexcept {
                                                                       ++*static_cast<ui32*>(context);
                                                                   } });
        ui32 hookCalls = 0;
        ui32 cursor = 0;
        bool advanced = false;
        bool blocked = false;
        auto operation = TSharedCacheSpaceTestAccess::BeginOnHazardWithHook(*space, 0, [&]() noexcept {
            if (++hookCalls == 1) {
                advanced = TSharedCacheSpaceTestAccess::AdvanceSpaceGeneration(*space);
                blocked = !TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor);
            }
        });
        UNIT_ASSERT(advanced && blocked);
        UNIT_ASSERT_VALUES_EQUAL(hookCalls, 2);
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT_VALUES_EQUAL(operation.EntryGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cursor, 0);
        UNIT_ASSERT(!(operation.Hazard().State.load() & SpaceHazardDrainWaiter));
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::TryDrainBefore(*space, 1, cursor));
        UNIT_ASSERT_VALUES_EQUAL(cursor, 1);
    }

    Y_UNIT_TEST(InitialMappingsAndFreeContents) {
        TSharedCacheCapacity current;
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 3, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 3, reserved));

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(current, reserved, 3);
        UNIT_ASSERT(space);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).HandleCount, 32);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).AllocationLimit, 32);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::ReservedCapacity(*space).HandleCount(), 64);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::BucketCount(*space), 32);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::ReservedCapacity(*space).BucketCount(), 64);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheSpaceTestAccess::CurrentView(*space).Tiers[ECacheTier::Regular].HotStorage.SlotCount, 24);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).Cold().Capacity, 16);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::CurrentView(*space).KeepCold().Capacity, 16);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), 30);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::HazardCount(*space), 3);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::Hazard(*space, 0).State.load(), 0);

        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(TSharedCacheSpaceTestAccess::Handles(*space)) % 64, 0);
        for (ui32 index = 0; index < current.HandleCount(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::Handles(*space)[index].State.load(), 0);
        }
        for (ui32 index = 0; index < current.ColdSlotCount(); ++index) {
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
        TCacheThreadSubSystem threadHazards(1);
        UNIT_ASSERT(!TSharedCacheSpace::Create(invalid, reserved, threadHazards));

        invalid = current;
        invalid.AddressBits = MinSharedCacheAddressBits - 1;
        UNIT_ASSERT(!TSharedCacheSpace::Create(invalid, reserved, threadHazards));
    }

    Y_UNIT_TEST(TransitionRejectsChangedPageSizeEstimate) {
        TSharedCacheCapacity current;
        TSharedCacheCapacity reserved;
        TSharedCacheCapacity changedEstimate;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 1, current));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 1, reserved));
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 2048, 0, 1, changedEstimate));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(current, reserved, 1);
        UNIT_ASSERT(space);
        const auto state = space->CurrentSpaceState();
        const auto* handles = TSharedCacheSpaceTestAccess::Handles(*space);
        TTransition transition;
        UNIT_ASSERT(!space->PrepareTransition(changedEstimate, transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT(space->CurrentSpaceState() == state);
        UNIT_ASSERT(TSharedCacheSpaceTestAccess::Handles(*space) == handles);
        UNIT_ASSERT(space->PrepareTransition(reserved, transition));
        UNIT_ASSERT(space->AbandonTransition(transition));
    }

    Y_UNIT_TEST(AllocateAndReturnItem) {
        TSharedCacheCapacity capacity;
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
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::FreeCount(*space), 0);
    }

    Y_UNIT_TEST(AllocateReroutesAfterGenerationChange) {
        TSharedCacheCapacity capacity;
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

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };
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
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::Handles(*space), smallHandles);

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
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheSpaceTestAccess::Handles(*space), smallHandles);
    }

    Y_UNIT_TEST(CapacityGrowthAddsEveryNewHandleToFree) {
        TSharedCacheCapacity small;
        TSharedCacheCapacity large;
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

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 1);
        UNIT_ASSERT(space);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*space, 0);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };

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
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 0);
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
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(large, large, 0);
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

        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(small, large, 0);
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
