#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_iter.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_warm.h>
#include <ydb/core/tablet_flat/test/libs/table/test_cooker.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <ydb/core/tablet_flat/flat_table_committed.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

namespace {
    const NTest::TMass Mass(new NTest::TModelStd(false), 999);
}

using TCheckIter = NTest::TChecker<NTest::TWrapMemtable, TIntrusiveConstPtr<TMemTable>>;
using TCheckReverseIter = NTest::TChecker<NTest::TWrapReverseMemtable, TIntrusiveConstPtr<TMemTable>>;

Y_UNIT_TEST_SUITE(Memtable)
{
    using namespace NTest;

    TIntrusiveConstPtr<TRowScheme> BasicRowLayout()
    {
        return
            TLayoutCook()
                .Col(0, 0,  NScheme::NTypeIds::Uint32)
                .Col(0, 1,  NScheme::NTypeIds::String)
                .Col(0, 2,  NScheme::NTypeIds::Double, Cimple(3.14))
                .Col(0, 3,  NScheme::NTypeIds::Bool)
                .Key({0, 1})
                    .RowScheme();
    }

    Y_UNIT_TEST(Basics)
    {
        const auto lay = BasicRowLayout();

        /* Test copied from TPart::Basics with except of iteration */

        const auto foo = *TSchemedCookRow(*lay).Col(555_u32, "foo", 3.14, nullptr);
        const auto bar = *TSchemedCookRow(*lay).Col(777_u32, "bar", 2.72, true);

        TCheckIter wrap(TCooker(lay).Add(foo).Add(bar).Unwrap(), { });

        wrap.To(10).Has(foo).Has(bar);
        wrap.To(11).NoVal(*TSchemedCookRow(*lay).Col(555_u32, "foo", 10.));
        wrap.To(12).NoKey(*TSchemedCookRow(*lay).Col(888_u32, "foo", 3.14));

        /*_ Basic lower and upper bounds lookup semantic  */

        wrap.To(20).Seek(foo, ESeek::Lower).Is(foo);
        wrap.To(21).Seek(foo, ESeek::Upper).Is(bar);
        wrap.To(22).Seek(bar, ESeek::Upper).Is(EReady::Gone);

        /*_ The empty key is interpreted depending on ESeek mode... */

        wrap.To(30).Seek({ }, ESeek::Lower).Is(foo);
        wrap.To(31).Seek({ }, ESeek::Exact).Is(EReady::Gone);
        wrap.To(32).Seek({ }, ESeek::Upper).Is(EReady::Gone);

        /* ... but incomplete keys are padded with +inf instead of nulls
            on lookup. Check that it really happens for Seek()'s */

        wrap.To(33).Seek(*TSchemedCookRow(*lay).Col(555_u32), ESeek::Lower).Is(bar);
        wrap.To(34).Seek(*TSchemedCookRow(*lay).Col(555_u32), ESeek::Upper).Is(bar);

        /*_ Basic iteration over two rows in tables */

        wrap.To(40).Seek({ }, ESeek::Lower);
        wrap.To(41).Is(foo).Next().Is(bar).Next().Is(EReady::Gone);
    }

    Y_UNIT_TEST(BasicsReverse)
    {
        const auto lay = BasicRowLayout();

        /* Test copied from TPart::Basics with except of iteration */

        const auto foo = *TSchemedCookRow(*lay).Col(555_u32, "foo", 3.14, nullptr);
        const auto bar = *TSchemedCookRow(*lay).Col(777_u32, "bar", 2.72, true);

        TCheckReverseIter wrap(TCooker(lay).Add(foo).Add(bar).Unwrap(), { });

        wrap.To(10).Has(foo).Has(bar);
        wrap.To(11).NoVal(*TSchemedCookRow(*lay).Col(555_u32, "foo", 10.));
        wrap.To(12).NoKey(*TSchemedCookRow(*lay).Col(888_u32, "foo", 3.14));

        /*_ Basic lower and upper bounds lookup semantic  */

        wrap.To(20).Seek(foo, ESeek::Lower).Is(foo);
        wrap.To(21).Seek(foo, ESeek::Upper).Is(EReady::Gone);
        wrap.To(22).Seek(bar, ESeek::Lower).Is(bar);
        wrap.To(22).Seek(bar, ESeek::Upper).Is(foo);

        /*_ The empty key is interpreted depending on ESeek mode... */

        wrap.To(30).Seek({ }, ESeek::Lower).Is(bar);
        wrap.To(31).Seek({ }, ESeek::Exact).Is(EReady::Gone);
        wrap.To(32).Seek({ }, ESeek::Upper).Is(EReady::Gone);

        /* ... but incomplete keys are padded with +inf instead of nulls
            on lookup. Check that it really happens for Seek()'s */

        wrap.To(33).Seek(*TSchemedCookRow(*lay).Col(555_u32), ESeek::Lower).Is(foo);
        wrap.To(34).Seek(*TSchemedCookRow(*lay).Col(555_u32), ESeek::Upper).Is(foo);

        /*_ Basic iteration over two rows in tables */

        wrap.To(40).Seek({ }, ESeek::Lower);
        wrap.To(41).Is(bar).Next().Is(foo).Next().Is(EReady::Gone);
    }

    Y_UNIT_TEST(Markers)
    {
        const auto lay = BasicRowLayout();

        auto reset = *TSchemedCookRow(*lay).Col(555_u32, "foo", ECellOp::Reset);
        auto null  = *TSchemedCookRow(*lay).Col(556_u32, "foo", ECellOp::Null);

        TCooker cooker(lay);

        TCheckIter(*cooker.Add(reset, ERowOp::Upsert), { }).To(10).Has(reset);
        TCheckIter(*cooker.Add(null, ERowOp::Upsert), { }).To(11).Has(null);
    }

    Y_UNIT_TEST(Overlap)
    {
        const auto lay = BasicRowLayout();

        auto r0W = *TSchemedCookRow(*lay).Col(555_u32, "foo", 3.14, nullptr);
        auto r1W = *TSchemedCookRow(*lay).Col(555_u32, "foo", nullptr, false);
        auto r2W = *TSchemedCookRow(*lay).Col(555_u32, "foo", 2.72);
        auto r2R = *TSchemedCookRow(*lay).Col(555_u32, "foo", 2.72, false);
        auto r3W = *TSchemedCookRow(*lay).Col(555_u32, "foo", nullptr, true);
        auto r4W = *TSchemedCookRow(*lay).Col(555_u32, "foo", ECellOp::Reset);
        auto r4R = *TSchemedCookRow(*lay).Col(555_u32, "foo", 3.14, true);

        TCooker cooker(lay);

        TCheckIter(*cooker.Add(r0W, ERowOp::Upsert), { }).To(10).Has(r0W);
        TCheckIter(*cooker.Add(r1W, ERowOp::Upsert), { }).To(11).Has(r1W);
        TCheckIter(*cooker.Add(r2W, ERowOp::Upsert), { }).To(12).Has(r2R);
        TCheckIter(*cooker.Add(r3W, ERowOp::Upsert), { }).To(13).Has(r3W);
        TCheckIter(*cooker.Add(r4W, ERowOp::Upsert), { }).To(14).Has(r4R);
        TCheckIter(*cooker.Add(r0W, ERowOp::Erase), { }).To(19).NoKey(r0W, false);
    }

    Y_UNIT_TEST(Wreck)
    {
        auto egg = *TCooker(Mass.Model->Scheme).Add(Mass.Saved, ERowOp::Upsert);

        TWreck<TCheckIter, TIntrusiveConstPtr<TMemTable>>(Mass, 666).Do(EWreck::Cached, egg);
    }

    Y_UNIT_TEST(Erased)
    {
        auto egg =
            *TCooker(Mass.Model->Scheme)
                .Add(Mass.Saved, ERowOp::Upsert)
                .Add(Mass.Holes, ERowOp::Upsert)
                .Add(Mass.Holes, ERowOp::Erase);

        TWreck<TCheckIter, TIntrusiveConstPtr<TMemTable>>(Mass, 666).Do(EWreck::Cached, egg);
    }

    Y_UNIT_TEST(UncommittedDeltaLimit)
    {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        auto scheme = lay.RowScheme();
        NTest::TRowTool tool(*scheme);

        TCooker cooker(scheme);
        auto table = cooker.Unwrap();

        const ui32 limit = 4;

        // Distinct TxIds: each write adds a separate delta node (no coalescing)
        for (ui32 i = 0; i < limit; ++i) {
            const auto row = *TSchemedCookRow(*scheme).Col(1_u32, "v");
            auto pair = tool.Split(row, true, true);
            table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                TRowVersion(Max<ui64>(), 100 + i), /* committed */ nullptr, {}, limit);
        }

        // The (limit+1)-th distinct TxId throws; the write is not applied to the memtable
        {
            const auto row = *TSchemedCookRow(*scheme).Col(1_u32, "v");
            auto pair = tool.Split(row, true, true);
            UNIT_ASSERT_EXCEPTION(
                table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                    TRowVersion(Max<ui64>(), 999), /* committed */ nullptr, {}, limit),
                TDeltaChainException);
        }

        // Same-TxId writes must NOT throw regardless of the limit (no new delta row)
        for (ui32 i = 0; i < limit + 10; ++i) {
            const auto row = *TSchemedCookRow(*scheme).Col(1_u32, "w");
            auto pair = tool.Split(row, true, true);
            table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                TRowVersion(Max<ui64>(), 100), /* committed */ nullptr, {}, limit);
        }
    }

    Y_UNIT_TEST(UncommittedDeltaLimitDisabled)
    {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        auto scheme = lay.RowScheme();
        NTest::TRowTool tool(*scheme);

        TCooker cooker(scheme);
        auto table = cooker.Unwrap();

        // 0 = no limit
        for (ui32 i = 0; i < 100; ++i) {
            const auto row = *TSchemedCookRow(*scheme).Col(1_u32, "v");
            auto pair = tool.Split(row, true, true);
            table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                TRowVersion(Max<ui64>(), 100 + i), /* committed */ nullptr);
        }
    }

    Y_UNIT_TEST(UncommittedDeltaLimitIgnoresLocks)
    {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        auto scheme = lay.RowScheme();
        NTest::TRowTool tool(*scheme);

        TCooker cooker(scheme);
        auto table = cooker.Unwrap();

        const ui32 limit = 4;

        const auto row = *TSchemedCookRow(*scheme).Col(1_u32, "v");
        auto pair = tool.Split(row, true, true);

        // Add many lock-only (Absent) deltas — they must NOT count toward the limit
        for (ui32 i = 0; i < limit + 10; ++i) {
            table->LockRow(ELockMode::Exclusive, pair.Key, 100 + i);
        }

        // Now add real data deltas up to the limit — should still succeed
        for (ui32 i = 0; i < limit; ++i) {
            table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                TRowVersion(Max<ui64>(), 200 + i), /* committed */ nullptr, {}, limit);
        }

        // The (limit+1)-th data delta throws
        UNIT_ASSERT_EXCEPTION(
            table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                TRowVersion(Max<ui64>(), 999), /* committed */ nullptr, {}, limit),
            TDeltaChainException);
    }

    Y_UNIT_TEST(UncommittedDeltaLimitCountsErase)
    {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        auto scheme = lay.RowScheme();
        NTest::TRowTool tool(*scheme);

        TCooker cooker(scheme);
        auto table = cooker.Unwrap();

        const ui32 limit = 4;

        const auto row = *TSchemedCookRow(*scheme).Col(1_u32, "v");
        auto pair = tool.Split(row, true, true);

        // Erase (non-Absent) deltas count toward the limit, like data deltas
        for (ui32 i = 0; i < limit; ++i) {
            table->Update(ERowOp::Erase, pair.Key, {}, {},
                TRowVersion(Max<ui64>(), 100 + i), /* committed */ nullptr, {}, limit);
        }

        // The (limit+1)-th Erase delta throws
        UNIT_ASSERT_EXCEPTION(
            table->Update(ERowOp::Erase, pair.Key, {}, {},
                TRowVersion(Max<ui64>(), 999), /* committed */ nullptr, {}, limit),
            TDeltaChainException);
    }

    Y_UNIT_TEST(UncommittedDeltaLimitIgnoresCommittedByMap)
    {
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        auto scheme = lay.RowScheme();
        NTest::TRowTool tool(*scheme);

        TCooker cooker(scheme);
        auto table = cooker.Unwrap();

        const ui32 limit = 1;

        const auto row = *TSchemedCookRow(*scheme).Col(1_u32, "v");
        auto pair = tool.Split(row, true, true);

        // Live delta from tx 100
        table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
            TRowVersion(Max<ui64>(), 100), /* committed */ nullptr, {}, limit);

        // Commit tx 100: its delta becomes committed-by-map and must not count
        auto committedMap = TSingleTransactionMap::Create(100, TRowVersion(1, 1));
        NTable::ITransactionMapSimplePtr committed(committedMap);

        // A second live tx is allowed (only 1 live delta after exclusion)
        table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
            TRowVersion(Max<ui64>(), 101), committed, {}, limit);

        // A third live tx trips the limit (2 live deltas > limit 1)
        UNIT_ASSERT_EXCEPTION(
            table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                TRowVersion(Max<ui64>(), 102), committed, {}, limit),
            TDeltaChainException);
    }

    Y_UNIT_TEST(UncommittedDeltaLimitIgnoresIneligible)
    {
        // Writes that don't enforce the limit produce ineligible deltas that never count
        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        auto scheme = lay.RowScheme();
        NTest::TRowTool tool(*scheme);

        TCooker cooker(scheme);
        auto table = cooker.Unwrap();

        const ui32 limit = 1;
        const auto row = *TSchemedCookRow(*scheme).Col(1_u32, "v");
        auto pair = tool.Split(row, true, true);

        // Ineligible (limit 0) deltas never count toward the limit
        for (ui32 i = 0; i < 10; ++i) {
            table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                TRowVersion(Max<ui64>(), 100 + i), /* committed */ nullptr, {}, 0);
        }

        // An eligible (limit-enforcing) delta is allowed despite the ineligible ones
        table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
            TRowVersion(Max<ui64>(), 200), /* committed */ nullptr, {}, limit);

        // A second eligible delta trips the limit
        UNIT_ASSERT_EXCEPTION(
            table->Update(ERowOp::Upsert, pair.Key, pair.Ops, {},
                TRowVersion(Max<ui64>(), 201), /* committed */ nullptr, {}, limit),
            TDeltaChainException);
    }

}
}
}
