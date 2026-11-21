#include <benchmark/benchmark.h>

#include <ydb/core/tablet_flat/cache.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_dbase_naked.h>
#include <ydb/core/tablet_flat/flat_executor_tx_env.h>
#include <ydb/core/tablet_flat/flat_part_store_cache.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/cache_context.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <algorithm>
#include <numeric>
#include <random>

namespace NKikimr::NTable {
namespace {

    constexpr ui32 TransactionRows = 1u << 22;
    constexpr ui32 TransactionValueBytes = 64;
    constexpr ui64 TransactionAccessSeed = 20261114;
    const TTag TransactionTags[] = { 0, 1 };

    class TBenchmarkTransaction final : public NTabletFlatExecutor::ITransaction {
        bool Execute(TTransactionContext&, const NActors::TActorContext&) override {
            return true;
        }

        void Complete(const NActors::TActorContext&) override {}
    };

    class TTransactionFixture {
    public:
        TTransactionFixture() {
            NSharedCache::TCacheCapacityConfig capacity;
            Y_ENSURE(NSharedCache::TryCalculateCacheCapacity(
                2_GB, TCache::ExpectedPageSize, 0, NActors::MaxWorkers, capacity));
            Context = MakeHolder<NTest::TTableCacheContext>(capacity);
            NTest::TLayoutCook layout;
            layout.Col(0, 0, NScheme::NTypeIds::Uint32).Col(0, 1, NScheme::NTypeIds::String).Key({ 0 });
            NPage::TConf conf;
            conf.WriteFlatIndex = false;
            conf.WriteBTreeIndexV1 = false;
            conf.WriteBTreeIndexV2 = true;
            conf.Group(0).Codec = NPage::ECodec::Plain;
            conf.Group(0).PageSize = 8_KB;
            NTest::TPartCook cook(layout, conf);
            const TString value(TransactionValueBytes, 'x');
            for (ui32 key = 0; key < TransactionRows; ++key) {
                cook.AddN(key, value);
            }
            Eggs = cook.Finish();
            const auto& source = *Eggs.Lone();
            TPart::TParams params{
                .Epoch = source.Epoch,
                .Scheme = source.Scheme,
                .IndexPages = source.IndexPages,
                .IndexesRawSize = source.IndexesRawSize,
                .MinRowVersion = source.MinRowVersion,
                .MaxRowVersion = source.MaxRowVersion,
            };
            Part = new TCachePartStore(source.Label, std::move(params), source.Stat);
            for (ui32 room = 0; room < source.Store->GetRoomCount(); ++room) {
                const auto& collection = source.PageColls.at(room);
                Part->PageCollections.push_back(NTest::AdmitCollection(TCache::Get(), collection));
                for (ui32 page = 0; page < collection->Total(); ++page) {
                    const auto location = collection->GetLocation(page);
                    const auto* body = source.Store->GetPage(room, location.Offset);
                    auto cached = NTest::AdmitPage(TCache::Get(), collection, location, TSharedData(*body));
                    Y_ENSURE(cached);
                }
            }
            TAutoPtr<TScheme> scheme = new TScheme;
            auto& table = scheme->Tables.emplace(1, TScheme::TTableInfo("benchmark", 1)).first->second;
            for (const auto& col : Eggs.Scheme->Cols) {
                auto& column = table.Columns[col.Tag];
                column.Id = col.Tag;
                column.PType = col.TypeInfo;
                column.KeyOrder = col.Key;
                column.Family = Eggs.Scheme->Families[col.Group];
            }
            Database = MakeHolder<TDatabase>(new TDatabaseImpl(0, scheme, nullptr));
            Database->Merge(1, TPartView{ Part, nullptr, source.Slices });
            Order.resize(TransactionRows);
            std::iota(Order.begin(), Order.end(), 0u);
            std::mt19937_64 random(TransactionAccessSeed);
            std::shuffle(Order.begin(), Order.end(), random);
        }

        void Counters(benchmark::State& state, const NTabletFlatExecutor::TPageCollectionReadEnv& env,
            const NTabletFlatExecutor::TPageCollectionReadEnv::TStats& warmed) const {
            state.counters["Rows"] = TransactionRows;
            state.counters["PayloadBytes"] = ui64(TransactionRows) * TransactionValueBytes;
            state.counters["PartBytes"] = Part->Stat.Bytes;
            state.counters["PinnedPages"] = env.GetStats().NewlyPinnedPages;
            state.counters["PinnedBytes"] = env.GetStats().NewlyPinnedBytes;
            state.counters["PageFaults"] = env.GetStats().ToLoadPages;
            Y_ENSURE(env.GetStats().ToLoadPages == 0);
            Y_ENSURE(env.GetStats().NewlyPinnedPages == warmed.NewlyPinnedPages);
            Y_ENSURE(env.GetStats().NewlyPinnedBytes == warmed.NewlyPinnedBytes);
        }

        THolder<NTest::TTableCacheContext> Context;
        NTest::TPartEggs Eggs;
        TIntrusivePtr<TCachePartStore> Part;
        THolder<TDatabase> Database;
        TVector<ui32> Order;
    };

    void TransactionPointRead(benchmark::State& state) {
        TTransactionFixture fixture;
        NTabletFlatExecutor::TCacheSeat seat(1, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TBenchmarkTransaction));
        NTabletFlatExecutor::TPageCollectionReadEnv env(seat);
        TDatabase& database = *fixture.Database;
        database.Begin(1, env);
        Y_ENSURE(database.Precharge(1, {}, {}, TransactionTags, 0, Max<ui64>(), Max<ui64>()).Ready);
        const auto warmed = env.GetStats();
        TRowState row;
        ui32 index = 0;
        ui64 checksum = 0, expectedChecksum = 0;
        for (auto _ : state) {
            const ui32 key = fixture.Order[index++ & (TransactionRows - 1)];
            const TRawTypeValue rawKey(&key, sizeof(key), NScheme::NTypeIds::Uint32);
            if (database.Select(1, { &rawKey, 1 }, TransactionTags, row) != EReady::Data) {
                state.SkipWithError("Warmed transaction point read did not return data");
                break;
            }
            checksum += row.Get(0).AsValue<ui32>() + row.Get(1).AsBuf()[key % TransactionValueBytes];
            expectedChecksum += key + 'x';
            benchmark::DoNotOptimize(checksum);
        }
        Y_ENSURE(checksum == expectedChecksum);
        fixture.Counters(state, env, warmed);
        database.Commit(1, false);
    }

    void TransactionRangeRead(benchmark::State& state) {
        TTransactionFixture fixture;
        NTabletFlatExecutor::TCacheSeat seat(1, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TBenchmarkTransaction));
        NTabletFlatExecutor::TPageCollectionReadEnv env(seat);
        TDatabase& database = *fixture.Database;
        database.Begin(1, env);
        Y_ENSURE(database.Precharge(1, {}, {}, TransactionTags, 0, Max<ui64>(), Max<ui64>()).Ready);
        const auto warmed = env.GetStats();
        ui32 index = 0;
        constexpr ui32 RangeRows = 64;
        ui64 checksum = 0, expectedChecksum = 0;
        for (auto _ : state) {
            const ui32 key = fixture.Order[index++ & (TransactionRows - 1)] & ~(RangeRows - 1);
            const TRawTypeValue rawKey(&key, sizeof(key), NScheme::NTypeIds::Uint32);
            auto iter = database.Iterate(1, { &rawKey, 1 }, TransactionTags, ELookup::GreaterOrEqualThan);
            bool ready = true;
            for (ui32 row = 0; row < RangeRows; ++row) {
                if (iter->Next(ENext::Data) != EReady::Data) {
                    state.SkipWithError("Warmed transaction range read did not return data");
                    ready = false;
                    break;
                }
                const TCell keyCell = iter->Row().Get(0);
                checksum += keyCell.AsValue<ui32>() + iter->Row().Get(1).AsBuf()[row % TransactionValueBytes];
                expectedChecksum += key + row + 'x';
                benchmark::DoNotOptimize(checksum);
            }
            if (!ready) {
                break;
            }
        }
        Y_ENSURE(checksum == expectedChecksum);
        state.SetItemsProcessed(state.iterations() * RangeRows);
        fixture.Counters(state, env, warmed);
        database.Commit(1, false);
    }

    BENCHMARK(TransactionPointRead);
    BENCHMARK(TransactionRangeRead);

} // namespace
} // namespace NKikimr::NTable
