#include "shared_cache_item.h"

#include <library/cpp/testing/unittest/registar.h>

#include <type_traits>

namespace NKikimr::NSharedCache {

Y_UNIT_TEST_SUITE(TSharedCacheItemTest) {
    Y_UNIT_TEST(CacheItemFields) {
        constexpr auto cacheItem = TCacheItem::Make(MaxItemVersion, Max<ui32>(), true);

        static_assert(cacheItem.Version() == MaxItemVersion);
        static_assert(cacheItem.Index() == Max<ui32>());
        static_assert(cacheItem.IsFrozen());
        static_assert(!cacheItem.IsNull());
        static_assert(cacheItem.WithoutFrozen().WithFrozen() == cacheItem);

        UNIT_ASSERT_VALUES_EQUAL(cacheItem.Raw(), Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(TCacheItem{}.Raw(), 0);
        UNIT_ASSERT(TCacheItem{}.IsNull());
    }

    Y_UNIT_TEST(HandleStateFields) {
        constexpr auto state = THandleState::Make(
            MaxItemVersion, EHandleState::Tombstone, EItemKind::Collection, 3, EStickyState::Reserved, MaxHandleRefs);

        static_assert(state.Version() == MaxItemVersion);
        static_assert(state.State() == EHandleState::Tombstone);
        static_assert(state.Kind() == EItemKind::Collection);
        static_assert(state.Frequency() == 3);
        static_assert(state.Sticky() == EStickyState::Reserved);
        static_assert(state.Refs() == MaxHandleRefs);

        constexpr ui64 expected = THandleState::VersionMask |
                                  (ui64(EHandleState::Tombstone) << THandleState::StateShift) | THandleState::KindMask |
                                  THandleState::FrequencyMask | THandleState::StickyMask | THandleState::RefsMask;
        UNIT_ASSERT_VALUES_EQUAL(state.Raw(), expected);
    }

    Y_UNIT_TEST(HandleStateFieldIsolation) {
        constexpr auto state =
            THandleState::Make(123456, EHandleState::Hot, EItemKind::Page, 2, EStickyState::Sticky, 42);

        constexpr auto updated = state.WithVersion(765432)
                                     .WithState(EHandleState::Cold)
                                     .WithKind(EItemKind::Collection)
                                     .WithFrequency(1)
                                     .WithSticky(EStickyState::Unsticky)
                                     .WithRefs(24);

        static_assert(updated.Version() == 765432);
        static_assert(updated.State() == EHandleState::Cold);
        static_assert(updated.Kind() == EItemKind::Collection);
        static_assert(updated.Frequency() == 1);
        static_assert(updated.Sticky() == EStickyState::Unsticky);
        static_assert(updated.Refs() == 24);

        UNIT_ASSERT_VALUES_EQUAL(state.Version(), 123456);
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 42);
    }

    Y_UNIT_TEST(HandleStateRefs) {
        const auto base = THandleState::Make(7, EHandleState::Hot, EItemKind::Page, 2, EStickyState::None, 1);

        const THandleState incremented = base.IncrementRefs();
        UNIT_ASSERT_VALUES_EQUAL(incremented.Refs(), 2);
        UNIT_ASSERT_VALUES_EQUAL(incremented.Version(), base.Version());
        UNIT_ASSERT_VALUES_EQUAL(incremented.Frequency(), base.Frequency());

        const THandleState decremented = incremented.DecrementRefs();
        UNIT_ASSERT_VALUES_EQUAL(decremented.Raw(), base.Raw());

        UNIT_ASSERT_VALUES_EQUAL(base.WithRefs(MaxHandleRefs).Refs(), MaxHandleRefs);
        UNIT_ASSERT_VALUES_EQUAL(base.WithRefs(0).Refs(), 0);
    }

    Y_UNIT_TEST(HandleStateClassification) {
        constexpr TCacheItem cacheItem = TCacheItem::Make(7, 42);
        constexpr auto make = [](EHandleState state, EStickyState sticky = EStickyState::None) {
            return THandleState::Make(7, state, EItemKind::Page, 0, sticky, 1);
        };
        constexpr THandleState ready = make(EHandleState::Hot);
        constexpr THandleState pending = make(EHandleState::Queued);

        static_assert(cacheItem.Matches(ready));
        static_assert(!TCacheItem::Make(8, 42).Matches(ready));
        static_assert(ready.IsPageKind());
        static_assert(!ready.IsCollectionKind());
        static_assert(ready.IsReady());
        static_assert(!ready.IsPending());
        static_assert(!pending.IsReady());
        static_assert(pending.IsPending());
        static_assert(make(EHandleState::Free).IsFree());
        static_assert(make(EHandleState::Begin).IsBegin());
        static_assert(make(EHandleState::Queued).IsQueued());
        static_assert(make(EHandleState::Requested).IsRequested());
        static_assert(make(EHandleState::QueuedRequested).IsQueuedRequested());
        static_assert(make(EHandleState::Completing).IsCompleting());
        static_assert(make(EHandleState::Hot).IsHot());
        static_assert(make(EHandleState::Cold).IsCold());
        static_assert(make(EHandleState::Sticky).IsSticky());
        static_assert(make(EHandleState::Replacing).IsReplacing());
        static_assert(make(EHandleState::Replaced).IsReplaced());
        static_assert(make(EHandleState::BucketSplit).IsBucketSplit());
        static_assert(make(EHandleState::Tombstone).IsTombstone());
        static_assert(make(EHandleState::Begin).IsStickyNoneField());
        static_assert(make(EHandleState::Begin, EStickyState::Sticky).IsStickyField());
        static_assert(make(EHandleState::Begin, EStickyState::Unsticky).IsUnstickyField());
        static_assert(make(EHandleState::Begin, EStickyState::Reserved).IsStickyReservedField());
    }

    Y_UNIT_TEST(VersionWrap) {
        UNIT_ASSERT_VALUES_EQUAL(AdvanceItemVersion(MaxItemVersion), 0);
        UNIT_ASSERT_VALUES_EQUAL(AdvanceColdVersion(MaxItemVersion), 0);
        UNIT_ASSERT_VALUES_EQUAL(AdvanceItemVersion(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(AdvanceColdVersion(0), 1);
    }

    Y_UNIT_TEST(TypedCacheItems) {
        static_assert(!std::is_convertible_v<TPageCacheItem, TCollectionCacheItem>);
        static_assert(!std::is_convertible_v<TCollectionCacheItem, TPageCacheItem>);
        static_assert(!std::is_constructible_v<TPageCacheItem, TCacheItem>);
        static_assert(!std::is_constructible_v<TCollectionCacheItem, TCacheItem>);

        constexpr auto first = TCollectionCacheItem::FromValidated(TCacheItem::Make(11, 42));
        constexpr auto second = TCollectionCacheItem::FromValidated(TCacheItem::Make(12, 42));

        static_assert(first.CacheItem() == TCacheItem::Make(11, 42));
        static_assert(first != second);
        UNIT_ASSERT_VALUES_UNEQUAL(first.Raw(), second.Raw());
    }

    Y_UNIT_TEST(ItemInitialization) {
        THandle handle;

        UNIT_ASSERT_VALUES_EQUAL(handle.State.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(handle.Next.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(handle.Key0.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(handle.Key1.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(handle.Metadata.PayloadBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(handle.ColdVersion.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(handle.NextInOwner.load(), 0);
    }
}

} // namespace NKikimr::NSharedCache
