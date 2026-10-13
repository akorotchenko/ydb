#include "shared_cache_table.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>

namespace NKikimr::NSharedCache {
namespace {
    TCollectionCacheItem MakeCollectionCacheItem(ui32 version, ui32 index) {
        return TCollectionCacheItem::FromValidated(TCacheItem::Make(version, index));
    }
} // namespace

Y_UNIT_TEST_SUITE(TSharedCacheTableKeyTest) {
    Y_UNIT_TEST(PageKeyRoundTripIgnoresSizeWord) {
        THandle handle;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(5, 17), 1234);
        StoreSharedCacheKey(handle, key);
        handle.Key2OrSize.store(98765);

        UNIT_ASSERT(LoadSharedCacheKey(handle, EItemKind::Page) == key);
    }
    Y_UNIT_TEST(CollectionKeyRoundTripUsesAllWords) {
        THandle handle;
        const auto key = TSharedCacheKey::Collection(TLogoBlobID(11, 22, 33));
        StoreSharedCacheKey(handle, key);

        UNIT_ASSERT(LoadSharedCacheKey(handle, EItemKind::Collection) == key);
        UNIT_ASSERT_VALUES_EQUAL(handle.Key2OrSize.load(), 33);
    }
    Y_UNIT_TEST(HashUsesEveryTypedKeyWord) {
        const auto token = MakeCollectionCacheItem(1, 2);
        UNIT_ASSERT_VALUES_UNEQUAL(HashPageKey(token, 10), HashPageKey(token, 11));
        UNIT_ASSERT_VALUES_UNEQUAL(HashPageKey(token, 10), HashPageKey(MakeCollectionCacheItem(2, 2), 10));

        const auto base = HashCollectionKey(TLogoBlobID(1, 2, 3));
        UNIT_ASSERT_VALUES_UNEQUAL(base, HashCollectionKey(TLogoBlobID(2, 2, 3)));
        UNIT_ASSERT_VALUES_UNEQUAL(base, HashCollectionKey(TLogoBlobID(1, 3, 3)));
        UNIT_ASSERT_VALUES_UNEQUAL(base, HashCollectionKey(TLogoBlobID(1, 2, 4)));
    }
    Y_UNIT_TEST(CrossKindHashTieUsesKind) {
        const auto page = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 3);
        const auto collection = TSharedCacheKey::Collection(TLogoBlobID(1, 2, 3));

        UNIT_ASSERT(CompareSharedCacheKeys(page, 42, collection, 42) < 0);
        UNIT_ASSERT(CompareSharedCacheKeys(collection, 42, page, 42) > 0);
    }
    Y_UNIT_TEST(TypedWordOrderBreaksHashTie) {
        const auto first = TSharedCacheKey::Collection(TLogoBlobID(1, 2, 3));
        const auto second = TSharedCacheKey::Collection(TLogoBlobID(1, 2, 4));

        UNIT_ASSERT(CompareSharedCacheKeys(first, 42, second, 42) < 0);
        UNIT_ASSERT(CompareSharedCacheKeys(second, 42, first, 42) > 0);
        UNIT_ASSERT_VALUES_EQUAL(CompareSharedCacheKeys(first, 42, first, 42), 0);
    }
    Y_UNIT_TEST(ComparatorDefinesTotalOrder) {
        TVector<TSharedCacheKey> keys = {
            TSharedCacheKey::Page(MakeCollectionCacheItem(1, 7), 11),
            TSharedCacheKey::Collection(TLogoBlobID(3, 2, 1)),
            TSharedCacheKey::Page(MakeCollectionCacheItem(1, 7), 10),
            TSharedCacheKey::Collection(TLogoBlobID(1, 2, 3)),
            TSharedCacheKey::Page(MakeCollectionCacheItem(2, 7), 10),
        };

        Sort(keys.begin(), keys.end(), [](const auto& left, const auto& right) {
            return CompareSharedCacheKeys(left, right) < 0;
        });

        for (size_t index = 1; index < keys.size(); ++index) {
            UNIT_ASSERT(CompareSharedCacheKeys(keys[index - 1], keys[index]) < 0);
        }
    }
}

} // namespace NKikimr::NSharedCache
