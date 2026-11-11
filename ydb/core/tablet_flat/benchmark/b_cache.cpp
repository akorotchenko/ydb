#include <benchmark/benchmark.h>

#include <ydb/core/tablet_flat/cache.h>
#include <ydb/core/tablet_flat/test/libs/table/cache_context.h>
#include <ydb/library/actors/core/defs.h>

#include <util/generic/hash.h>
#include <util/system/env.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <numeric>
#include <random>
#include <unordered_map>

namespace NKikimr::NSharedCache {
namespace {

    using NTable::NPage::EPage;
    using NTable::NPage::TPageLocation;
    using NTable::NPage::TPageOffset;

    constexpr ui32 ResidentPages = 1u << 22;
    constexpr ui32 BucketsPerPage = 4;
    constexpr ui32 PageSize = 64;
    constexpr ui64 AccessSeed = 20261023;

    struct TKeyHash {
        size_t operator()(const TCacheKey& key) const noexcept {
            return key.Hash();
        }
    };

    using TStdRawMap = std::unordered_map<TCacheKey, TCacheItem, TKeyHash>;
    using TYdbRawMap = THashMap<TCacheKey, TCacheItem, TKeyHash>;
    using TStdOwnedMap = std::unordered_map<TCacheKey, NActors::TSharedData, TKeyHash>;
    using TYdbOwnedMap = THashMap<TCacheKey, NActors::TSharedData, TKeyHash>;

    class TBenchmarkCollection final : public NPageCollection::IPageCollection {
    public:
        explicit TBenchmarkCollection(ui32 pages)
            : Pages_(pages)
        {}

        const TLogoBlobID& Label() const noexcept override {
            return Id_;
        }

        ui32 Total() const noexcept override {
            return Pages_;
        }

        ui32 MetaPages() const noexcept override {
            return 0;
        }

        NPageCollection::TInfo Page(ui32 page) const override {
            Y_DEBUG_ABORT_UNLESS(page < Pages_);
            return { PageSize, ui32(EPage::DataPage) };
        }

        NPageCollection::TBorder Bounds(ui32 page) const override {
            return { Page(page).Size, { page, 0 }, { page, PageSize } };
        }

        NPageCollection::TBorder Bounds(const TPageLocation& location) const override {
            return Bounds(location.Offset.AsPageIndex());
        }

        NPageCollection::TGlobId Glob(ui32) const override {
            Y_ABORT("Benchmark pages have no blob backing");
        }

        bool Verify(ui32 page, TArrayRef<const char> data) const override {
            return page < Pages_ && data.size() == PageSize;
        }

        bool Verify(const TPageLocation& location, TArrayRef<const char> data) const override {
            return location.Offset.IsPageIndex() && Verify(location.Offset.AsPageIndex(), data) &&
                   location.Size == PageSize;
        }

        size_t BackingSize() const noexcept override {
            return 0;
        }

        i64 GetMetadataMemoryUsage() const noexcept override {
            return sizeof(TBenchmarkCollection);
        }

        const TSharedData* GetMetadataBuffer() const noexcept override {
            return nullptr;
        }

        TPageLocation GetLocation(ui32 page) const override {
            Y_DEBUG_ABORT_UNLESS(page < Pages_);
            return TPageLocation::FromPageIndex(page, PageSize, EPage::DataPage, 0);
        }

    private:
        const TLogoBlobID Id_{ 301, 302, 303 };
        const ui32 Pages_;
    };

    class TCacheFixture : public benchmark::Fixture {
    public:
        void SetUp(benchmark::State& state) override {
            const TString noOperation = GetEnv("LFCACHE_BENCH_NO_OPERATION");
            Y_ENSURE(noOperation.empty() || noOperation == "1");
            NoOperation_ = noOperation == "1";
            PageCount_ = static_cast<ui32>(state.range(0));
            Y_ENSURE(PageCount_ && !(PageCount_ & (PageCount_ - 1)));
            const ui8 addressBits = static_cast<ui8>(std::bit_width(ui64(PageCount_) * BucketsPerPage - 1));
            TCacheCapacityConfig capacity;
            Y_ENSURE(TryCalculateCacheFootprint(addressBits, PageSize, 0, NActors::MaxWorkers, capacity));
            Context_ = MakeHolder<NTable::NTest::TTableCacheContext>(capacity);
            Cache_ = &TCache::Get();
            PageCollection_ = MakeIntrusiveConst<TBenchmarkCollection>(PageCount_ * 2);
            Collection_ = Cache_->AdmitCollection(PageCollection_);
            const ui64 buckets = capacity.BucketCount();
            StdRaw_.reserve(buckets);
            YdbRaw_.reserve(buckets);
            StdOwned_.reserve(buckets);
            YdbOwned_.reserve(buckets);
            if (state.range(1)) {
                Pinned_.reserve(PageCount_);
            }
            for (ui32 page = 0; page < PageCount_; ++page) {
                const auto location = PageCollection_->GetLocation(page);
                const auto key = TCacheKey::Page(Collection_.CacheItem(), static_cast<ui64>(location.Offset));
                auto data = NActors::TSharedData::Uninitialized(PageSize);
                std::memset(data.mutable_data(), page & 0xff, PageSize);
                auto shared = data;
                auto ref = Cache_->AdmitPage(PageCollection_, location, std::move(data));
                Y_ENSURE(ref);
                const auto item = ref.CacheItem().CacheItem();
                StdRaw_.emplace(key, item);
                YdbRaw_.emplace(key, item);
                StdOwned_.emplace(key, shared);
                YdbOwned_.emplace(key, std::move(shared));
                if (state.range(1)) {
                    Pinned_.push_back(std::move(ref));
                }
            }
            for (ui32 page = 0; page < PageCount_; ++page) {
                TCachePageRef ref;
                if (Cache_->Find(Collection_.CacheItem(), static_cast<ui64>(TPageOffset::FromPageIndex(page)), ref) !=
                    ECacheResultStatus::Hit) {
                    state.SkipWithError("Cache does not retain the complete comparison working set");
                    return;
                }
            }
            if (!CheckHotWorkingSet(state, "before timing")) {
                return;
            }
            if (NoOperation_) {
                // Amortize entry/exit outside the timed loop while keeping the mapping protected
                Operation_ = MakeHolder<TCache::TOperation>(Cache_->BeginOperation());
            }
            state.counters["OperationWrap"] = NoOperation_ ? 0 : 1;
            Order_.resize(PageCount_);
            std::iota(Order_.begin(), Order_.end(), 0u);
            std::mt19937_64 random(AccessSeed);
            std::shuffle(Order_.begin(), Order_.end(), random);
            state.counters["LookupValueMiB"] =
                double(PageCount_) * sizeof(std::pair<const TCacheKey, TCacheItem>) / (1024 * 1024);
            state.counters["PageBytes"] = PageSize;
            state.counters["CacheHandles"] = capacity.HandleCount();
            state.counters["CacheBuckets"] = capacity.BucketCount();
            state.counters["StdBuckets"] = StdRaw_.bucket_count();
            state.counters["YdbBuckets"] = YdbRaw_.bucket_count();
        }

        void TearDown(benchmark::State&) override {
            Operation_.Reset();
            TStdRawMap().swap(StdRaw_);
            TYdbRawMap().swap(YdbRaw_);
            TStdOwnedMap().swap(StdOwned_);
            TYdbOwnedMap().swap(YdbOwned_);
            TVector<TCachePageRef>().swap(Pinned_);
            TVector<ui32>().swap(Order_);
            Collection_.Drop();
            PageCollection_.Drop();
            Cache_ = nullptr;
            Context_.Reset();
        }

    protected:
        bool CheckHotWorkingSet(benchmark::State& state, const char* phase) const {
            const ui64 hot = Cache_->HotPages();
            const ui64 cold = Cache_->ColdPages();
            const bool before = TStringBuf(phase) == "before timing";
            state.counters[before ? "HotPagesBefore" : "HotPagesAfter"] = hot;
            state.counters[before ? "ColdPagesBefore" : "ColdPagesAfter"] = cold;
            if (hot != PageCount_ || cold != 0) {
                state.SkipWithError(phase);
                return false;
            }
            return true;
        }

        template <class TLookup>
        void RunLookups(benchmark::State& state, bool miss, TLookup&& lookup) {
            if (state.skipped()) {
                return;
            }
            const auto expected = miss ? ECacheResultStatus::Miss : ECacheResultStatus::Hit;
            ui32 cursor = 0;
            for (auto _ : state) {
                const ui32 page = Order_[cursor] + (miss ? PageCount_ : 0);
                const ui64 offset = static_cast<ui64>(TPageOffset::FromPageIndex(page));
                if (lookup(offset) != expected) {
                    state.SkipWithError("Lookup returned an unexpected result");
                    break;
                }
                cursor = (cursor + 1) & (PageCount_ - 1);
            }
            state.SetItemsProcessed(state.iterations());
            CheckHotWorkingSet(state, "after timing");
        }

        template <class TMap>
        ECacheResultStatus LookupRaw(TMap& map, ui64 offset) {
            const auto it = map.find(TCacheKey::Page(Collection_.CacheItem(), offset));
            if (it == map.end()) {
                return ECacheResultStatus::Miss;
            }
            ui64 value = it->second.Raw();
            benchmark::DoNotOptimize(value);
            return ECacheResultStatus::Hit;
        }

        template <bool Read, class TMap>
        ECacheResultStatus LookupOwned(TMap& map, ui64 offset) {
            const auto it = map.find(TCacheKey::Page(Collection_.CacheItem(), offset));
            if (it == map.end()) {
                return ECacheResultStatus::Miss;
            }
            auto data = it->second;
            if constexpr (Read) {
                char value = data.data()[0];
                benchmark::DoNotOptimize(value);
            } else {
                auto* value = data.data();
                benchmark::DoNotOptimize(value);
            }
            return ECacheResultStatus::Hit;
        }

        template <bool Read>
        static ECacheResultStatus ConsumePage(TCachePageRef& page, ECacheResultStatus status) {
            if (status == ECacheResultStatus::Hit) {
                if constexpr (Read) {
                    char value = page.data()[0];
                    benchmark::DoNotOptimize(value);
                } else {
                    auto* value = page.data();
                    benchmark::DoNotOptimize(value);
                }
            }
            return status;
        }

        template <bool Read>
        ECacheResultStatus LookupCache(ui64 offset) {
            TCachePageRef page;
            const auto status = Cache_->Find(Collection_.CacheItem(), offset, page);
            return ConsumePage<Read>(page, status);
        }

        template <bool Read>
        ECacheResultStatus LookupCache(TCache::TOperation& operation, ui64 offset) {
            TCachePageRef page;
            TOperationItemRef<TProdTraits> ref(operation);
            const auto status = operation.FindPage(Collection_.CacheItem(), offset, ref.Get());
            if (status == ECacheResultStatus::Hit) {
                page = operation.BuildPageRef(ref.Take());
            }
            return ConsumePage<Read>(page, status);
        }

        ECacheResultStatus LookupTable(TCache::TOperation& operation, ui64 offset) {
            TCacheItem item;
            const auto status = operation.FindPage(Collection_.CacheItem(), offset, item);
            if (status == ECacheResultStatus::Hit) {
                ui64 value = item.Raw();
                benchmark::DoNotOptimize(value);
            }
            return status;
        }

        template <bool Read>
        void RunCacheLookups(benchmark::State& state, bool miss = false) {
            if (state.skipped()) {
                return;
            }
            if (NoOperation_) {
                auto& operation = *Operation_;
                RunLookups(state, miss, [this, &operation](ui64 offset) {
                    return LookupCache<Read>(operation, offset);
                });
            } else {
                RunLookups(state, miss, [this](ui64 offset) {
                    return LookupCache<Read>(offset);
                });
            }
        }

        void RunTableLookups(benchmark::State& state, bool miss = false) {
            if (state.skipped()) {
                return;
            }
            if (NoOperation_) {
                auto& operation = *Operation_;
                RunLookups(state, miss, [this, &operation](ui64 offset) {
                    return LookupTable(operation, offset);
                });
            } else {
                RunLookups(state, miss, [this](ui64 offset) {
                    auto operation = Cache_->BeginOperation();
                    return LookupTable(operation, offset);
                });
            }
        }

        THolder<NTable::NTest::TTableCacheContext> Context_;
        TCache* Cache_ = nullptr;
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection_;
        TCacheCollectionRef Collection_;
        TStdRawMap StdRaw_;
        TYdbRawMap YdbRaw_;
        TStdOwnedMap StdOwned_;
        TYdbOwnedMap YdbOwned_;
        TVector<TCachePageRef> Pinned_;
        TVector<ui32> Order_;
        ui32 PageCount_ = 0;
        bool NoOperation_ = false;
        THolder<TCache::TOperation> Operation_;
    };

    BENCHMARK_DEFINE_F(TCacheFixture, StdRawHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupRaw(StdRaw_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TCacheFixture, YdbRawHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupRaw(YdbRaw_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TCacheFixture, CacheTableHit)(benchmark::State& state) {
        RunTableLookups(state);
    }

    BENCHMARK_DEFINE_F(TCacheFixture, StdOwnedHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupOwned<false>(StdOwned_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TCacheFixture, YdbOwnedHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupOwned<false>(YdbOwned_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TCacheFixture, CacheHit)(benchmark::State& state) {
        RunCacheLookups<false>(state);
    }

    BENCHMARK_DEFINE_F(TCacheFixture, StdOwnedRead)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupOwned<true>(StdOwned_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TCacheFixture, YdbOwnedRead)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupOwned<true>(YdbOwned_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TCacheFixture, CacheRead)(benchmark::State& state) {
        RunCacheLookups<true>(state);
    }

    BENCHMARK_DEFINE_F(TCacheFixture, StdRawMiss)(benchmark::State& state) {
        RunLookups(state, true, [this](ui64 offset) {
            return LookupRaw(StdRaw_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TCacheFixture, YdbRawMiss)(benchmark::State& state) {
        RunLookups(state, true, [this](ui64 offset) {
            return LookupRaw(YdbRaw_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TCacheFixture, CacheTableMiss)(benchmark::State& state) {
        RunTableLookups(state, true);
    }

    BENCHMARK_DEFINE_F(TCacheFixture, CacheMiss)(benchmark::State& state) {
        RunCacheLookups<false>(state, true);
    }

    BENCHMARK_REGISTER_F(TCacheFixture, StdRawHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, YdbRawHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, CacheTableHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, StdOwnedHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, YdbOwnedHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, CacheHit)
        ->Args({ ResidentPages, 0 })
        ->Args({ ResidentPages, 1 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, StdOwnedRead)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, YdbOwnedRead)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, CacheRead)
        ->Args({ ResidentPages, 0 })
        ->Args({ ResidentPages, 1 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, StdRawMiss)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, YdbRawMiss)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, CacheTableMiss)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TCacheFixture, CacheMiss)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);

} // namespace
} // namespace NKikimr::NSharedCache
