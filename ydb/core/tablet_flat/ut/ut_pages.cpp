#include <ydb/core/tablet_flat/test/libs/table/cache_fixture.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <util/generic/xrange.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

namespace NKikimr {
namespace NTable {

    static_assert(std::is_same_v<NPage::TDataPage::TRecord, NPage::TBorrowedDataPage::TRecord>);
    static_assert(std::is_same_v<NPage::TDataPage::TIter, NPage::TBorrowedDataPage::TIter>);
    static_assert(std::is_same_v<NPage::TFlatIndex::TRecord, NPage::TBorrowedFlatIndex::TRecord>);
    static_assert(std::is_same_v<NPage::TFlatIndex::TIter, NPage::TBorrowedFlatIndex::TIter>);

    template <class TPage>
    concept THasOwnedPageData = requires(const TPage& page) { page.GetData(); };

    static_assert(THasOwnedPageData<NPage::TDataPage>);
    static_assert(!THasOwnedPageData<NPage::TBorrowedDataPage>);

namespace {
    /* Sample rows set for old pages compatability tests, should be
        updated with assosiated raw page collections (see data/ directory).
     */

    const NTest::TMass MassZ(new NTest::TModelStd(false), 128);
}

Y_UNIT_TEST_SUITE_F(TPageParserOwnership, ::NKikimr::NTable::NTest::TTableCacheTest) {
    NTest::TPartEggs MakePages(NPage::ECodec codec) {
        NTest::TLayoutCook lay;
        lay.Col(0, 0, NScheme::NTypeIds::Uint32).Col(0, 1, NScheme::NTypeIds::String).Key({ 0 });
        NPage::TConf conf;
        conf.Group(0).PageRows = 2;
        conf.Group(0).Codec = codec;
        conf.Group(0).ForceCompression = codec != NPage::ECodec::Plain;
        NTest::TPartCook cook(lay, conf);
        for (ui32 row : xrange(6u)) {
            cook.Add(*NTest::TSchemedCookRow(*lay).Col(row * 2, TString(128, 'x') + ToString(row)));
        }
        return cook.Finish();
    }

    void CheckDataPage(NPage::ECodec codec) {
        const auto eggs = MakePages(codec);
        const auto& part = *eggs.Lone();
        NTest::TTestEnv env;
        auto input = env.TryGetPage(&part, NTest::IndexTools::GetFirstPageLocation(part, {}), {});
        UNIT_ASSERT(input);
        UNIT_ASSERT(NPage::TLabelWrapper().Read({ input.data(), input.size() }).Codec == codec);
        auto ownedInput = input.Acquire();
        NPage::TDataPage owned(std::move(ownedInput));
        UNIT_ASSERT(!ownedInput);
        NPage::TBorrowedDataPage borrowed(input.Borrow());
        UNIT_ASSERT(owned);
        UNIT_ASSERT(borrowed);
        if (codec == NPage::ECodec::Plain) {
            UNIT_ASSERT(borrowed->Base == input.data());
            UNIT_ASSERT(owned.GetData().data() == input.data());
            owned.Set(&owned.GetData());
            UNIT_ASSERT(owned->Base == input.data());
        } else {
            UNIT_ASSERT(borrowed->Base != input.data());
            input.Drop();
        }
        UNIT_ASSERT_VALUES_EQUAL(borrowed.BaseRow(), owned.BaseRow());
        UNIT_ASSERT_VALUES_EQUAL(borrowed->Count, owned->Count);
        UNIT_ASSERT_VALUES_EQUAL(borrowed.Label().Format, owned.Label().Format);
        const auto& layout = part.Scheme->GetLayout({});
        for (ui32 row : xrange(owned->Count)) {
            const auto* actual = borrowed->Record(row);
            const auto* expected = owned->Record(row);
            UNIT_ASSERT(actual->GetRop() == expected->GetRop());
            for (const auto& column : layout.Columns) {
                UNIT_ASSERT_VALUES_EQUAL(
                    CompareTypedCells(actual->Cell(column), expected->Cell(column), column.TypeInfo), 0);
            }
        }
        for (ui32 keyValue : xrange(5u)) {
            const TCell key[] = { TCell::Make(keyValue) };
            for (ESeek seek : { ESeek::Exact, ESeek::Lower, ESeek::Upper }) {
                UNIT_ASSERT_VALUES_EQUAL(borrowed.LookupKey(key, layout, seek, eggs.Scheme->Keys.Get()).Off(),
                    owned.LookupKey(key, layout, seek, eggs.Scheme->Keys.Get()).Off());
                UNIT_ASSERT_VALUES_EQUAL(borrowed.LookupKeyReverse(key, layout, seek, eggs.Scheme->Keys.Get()).Off(),
                    owned.LookupKeyReverse(key, layout, seek, eggs.Scheme->Keys.Get()).Off());
            }
        }
        borrowed.Set();
        UNIT_ASSERT(!borrowed);
        UNIT_ASSERT_VALUES_EQUAL(borrowed->Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(borrowed.BaseRow(), Max<TRowId>());
        if (input) {
            borrowed.Set(input.Borrow());
            UNIT_ASSERT(borrowed->Base == input.data());
        }
    }

    Y_UNIT_TEST(PlainDataPageBorrowsPinnedBytes) {
        CheckDataPage(NPage::ECodec::Plain);
    }

    Y_UNIT_TEST(CompressedDataPageOwnsDecodedBytes) {
        CheckDataPage(NPage::ECodec::LZ4);
    }

    Y_UNIT_TEST(BorrowedFlatIndexMatchesOwnedIndex) {
        const auto eggs = MakePages(NPage::ECodec::Plain);
        const auto& part = *eggs.Lone();
        NTest::TTestEnv env;
        auto owner = env.TryGetIndexPage(&part, part.GetIndexPageLocation(part.IndexPages.GetFlat({}), {}), {});
        UNIT_ASSERT(owner);
        NPage::TFlatIndex original(owner.Acquire());
        NPage::TBorrowedFlatIndex borrowed(owner.Borrow());
        NPage::TFlatIndex raw(owner.BuildSharedData());
        auto owned = std::move(original);
        UNIT_ASSERT(borrowed.GetPageRef().data() == owned.GetPageRef().data());
        const auto& layout = part.Scheme->GetLayout({});
        const auto check = [&](const auto& index) {
            UNIT_ASSERT_VALUES_EQUAL(index.Rows(), owned.Rows());
            UNIT_ASSERT_VALUES_EQUAL(index.GetEndRowId(), owned.GetEndRowId());
            for (TRowId row : xrange(part.Stat.Rows + 1)) {
                auto actual = index.LookupRow(row);
                auto expected = owned.LookupRow(row);
                UNIT_ASSERT_VALUES_EQUAL(actual.Off(), expected.Off());
                if (actual) {
                    UNIT_ASSERT_VALUES_EQUAL(actual->GetPageId(), expected->GetPageId());
                    UNIT_ASSERT_VALUES_EQUAL(index.GetLastRowId(actual), owned.GetLastRowId(expected));
                }
            }
            for (ui32 keyValue : xrange(14u)) {
                const TCell key[] = { TCell::Make(keyValue) };
                for (ESeek seek : { ESeek::Exact, ESeek::Lower, ESeek::Upper }) {
                    UNIT_ASSERT_VALUES_EQUAL(index.LookupKey(key, layout, seek, eggs.Scheme->Keys.Get()).Off(),
                        owned.LookupKey(key, layout, seek, eggs.Scheme->Keys.Get()).Off());
                    UNIT_ASSERT_VALUES_EQUAL(index.LookupKeyReverse(key, layout, seek, eggs.Scheme->Keys.Get()).Off(),
                        owned.LookupKeyReverse(key, layout, seek, eggs.Scheme->Keys.Get()).Off());
                }
            }
        };
        check(borrowed);
        check(raw);
    }
}

Y_UNIT_TEST_SUITE_F(NPage, ::NKikimr::NTable::NTest::TTableCacheTest) {

    Y_UNIT_TEST(Encoded)
    {
        using namespace NTable::NTest;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({ 0 });

        const TRow foo = *TSchemedCookRow(*lay).Col(555_u32, "foo");
        const TRow bar = *TSchemedCookRow(*lay).Col(777_u32, "bar");

        NPage::TConf conf{ true, 2 * 1024 };

        conf.Group(0).Codec = NPage::ECodec::LZ4;
        conf.Group(0).ForceCompression = true; /* required for this UT only */

        TCheckIter wrap(TPartCook(lay, conf).Add(foo).Finish(), { });

        wrap.To(1).Has(foo).To(2).NoKey(bar);

        auto &part = dynamic_cast<const NTest::TPartStore&>(*(*wrap).Eggs.Lone());

        for (auto page: xrange(part.Store->PageCollectionPagesCount(0))) {
            auto *raw = part.Store->GetPage(0, page);
            auto got = NPage::TLabelWrapper().Read(*raw, NPage::EPage::Undef);

            if (got.Type != NPage::EPage::DataPage) {
                /* Have to check for compression only rows page */
            } else if (got.Codec == NPage::ECodec::Plain) {
                UNIT_FAIL("Test has failed to cook compressed pages");
            }
        }
    }

    Y_UNIT_TEST(ABI_002)
    {
        const auto raw = NResource::Find("abi/002_full_part.pages");

        TStringInput input(raw);

        {
            using namespace NTest;

            const TLogoBlobID label(1,2,3);

            auto part = NTest::TLoader(TStore::Restore(input), { }).Load(label);

            TPartEggs eggs{ nullptr, MassZ.Model->Scheme, { std::move(part) } };

            TWreck<TCheckIter, TPartEggs>(MassZ, 666).Do(EWreck::Cached, eggs);
        }
    }

    Y_UNIT_TEST(GroupIdEncoding) {
        NPage::TGroupId main;
        UNIT_ASSERT_VALUES_EQUAL(main.Raw(), 0u);
        NPage::TGroupId alt(1);
        UNIT_ASSERT_VALUES_EQUAL(alt.Raw(), 1u);
        NPage::TGroupId mainHist(0, true);
        UNIT_ASSERT_VALUES_EQUAL(mainHist.Raw(), 0x80000000u);
        NPage::TGroupId altHist(1, true);
        UNIT_ASSERT_VALUES_EQUAL(altHist.Raw(), 0x80000001u);
        UNIT_ASSERT(main == main);
        UNIT_ASSERT(main < alt);
        UNIT_ASSERT(main < mainHist);
        UNIT_ASSERT(alt < mainHist);
        UNIT_ASSERT(alt < altHist);
        UNIT_ASSERT(mainHist < altHist);
    }

}

}
}
