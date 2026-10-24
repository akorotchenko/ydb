#include <benchmark/benchmark.h>

#include <ydb/core/tablet_flat/shared_cache.h>
#include <ydb/core/tablet_flat/test/libs/table/shared_cache_context.h>
#include <ydb/library/actors/core/defs.h>

#include <util/generic/hash.h>

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
        size_t operator()(const TSharedCacheKey& key) const noexcept {
            return key.Hash();
        }
    };

    using TStdRawMap = std::unordered_map<TSharedCacheKey, TCacheItem, TKeyHash>;
    using TYdbRawMap = THashMap<TSharedCacheKey, TCacheItem, TKeyHash>;
    using TStdOwnedMap = std::unordered_map<TSharedCacheKey, NActors::TSharedData, TKeyHash>;
    using TYdbOwnedMap = THashMap<TSharedCacheKey, NActors::TSharedData, TKeyHash>;

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

        TPageLocation GetLocation(ui32 page) const override {
            Y_DEBUG_ABORT_UNLESS(page < Pages_);
            return TPageLocation::FromPageIndex(page, PageSize, EPage::DataPage, 0);
        }

    private:
        const TLogoBlobID Id_{ 301, 302, 303 };
        const ui32 Pages_;
    };

    class TSharedCacheFixture : public benchmark::Fixture {
    public:
        void SetUp(benchmark::State& state) override {
            PageCount_ = static_cast<ui32>(state.range(0));
            Y_ENSURE(PageCount_ && !(PageCount_ & (PageCount_ - 1)));
            const ui8 addressBits = static_cast<ui8>(std::bit_width(ui64(PageCount_) * BucketsPerPage - 1));
            TSharedCacheCapacity capacity;
            Y_ENSURE(TryCalculateSharedCacheFootprint(addressBits, PageSize, 0, NActors::MaxWorkers, capacity));
            Context_ = MakeHolder<NTable::NTest::TTableCacheContext>(capacity);
            Cache_ = &TSharedCache::Get();
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
                const auto key = TSharedCacheKey::Page(Collection_.CacheItem(), static_cast<ui64>(location.Offset));
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
                TSharedPageRef ref;
                if (Cache_->Find(Collection_.CacheItem(), static_cast<ui64>(TPageOffset::FromPageIndex(page)), ref) !=
                    ESharedCacheResultStatus::Hit) {
                    state.SkipWithError("Cache does not retain the complete comparison working set");
                    return;
                }
            }
            Order_.resize(PageCount_);
            std::iota(Order_.begin(), Order_.end(), 0u);
            std::mt19937_64 random(AccessSeed);
            std::shuffle(Order_.begin(), Order_.end(), random);
            state.counters["LookupValueMiB"] =
                double(PageCount_) * sizeof(std::pair<const TSharedCacheKey, TCacheItem>) / (1024 * 1024);
            state.counters["PageBytes"] = PageSize;
            state.counters["CacheHandles"] = capacity.HandleCount();
            state.counters["CacheBuckets"] = capacity.BucketCount();
            state.counters["StdBuckets"] = StdRaw_.bucket_count();
            state.counters["YdbBuckets"] = YdbRaw_.bucket_count();
        }

        void TearDown(benchmark::State&) override {
            TStdRawMap().swap(StdRaw_);
            TYdbRawMap().swap(YdbRaw_);
            TStdOwnedMap().swap(StdOwned_);
            TYdbOwnedMap().swap(YdbOwned_);
            TVector<TSharedPageRef>().swap(Pinned_);
            TVector<ui32>().swap(Order_);
            Collection_.Drop();
            PageCollection_.Drop();
            Cache_ = nullptr;
            Context_.Reset();
        }

    protected:
        template <class TLookup>
        void RunLookups(benchmark::State& state, bool miss, TLookup&& lookup) {
            const auto expected = miss ? ESharedCacheResultStatus::Miss : ESharedCacheResultStatus::Hit;
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
        }

        template <class TMap>
        ESharedCacheResultStatus LookupRaw(TMap& map, ui64 offset) {
            const auto it = map.find(TSharedCacheKey::Page(Collection_.CacheItem(), offset));
            if (it == map.end()) {
                return ESharedCacheResultStatus::Miss;
            }
            ui64 value = it->second.Raw();
            benchmark::DoNotOptimize(value);
            return ESharedCacheResultStatus::Hit;
        }

        template <bool Read, class TMap>
        ESharedCacheResultStatus LookupOwned(TMap& map, ui64 offset) {
            const auto it = map.find(TSharedCacheKey::Page(Collection_.CacheItem(), offset));
            if (it == map.end()) {
                return ESharedCacheResultStatus::Miss;
            }
            auto data = it->second;
            if constexpr (Read) {
                char value = data.data()[0];
                benchmark::DoNotOptimize(value);
            } else {
                auto* value = data.data();
                benchmark::DoNotOptimize(value);
            }
            return ESharedCacheResultStatus::Hit;
        }

        ESharedCacheResultStatus LookupTable(ui64 offset) {
            auto operation = Cache_->BeginOperation();
            TCacheItem item;
            const auto status = operation.Find(TSharedCacheKey::Page(Collection_.CacheItem(), offset), item);
            if (status == ESharedCacheResultStatus::Hit) {
                ui64 value = item.Raw();
                benchmark::DoNotOptimize(value);
            }
            return status;
        }

        template <bool Read>
        ESharedCacheResultStatus LookupCache(ui64 offset) {
            TSharedPageRef page;
            const auto status = Cache_->Find(Collection_.CacheItem(), offset, page);
            if (status == ESharedCacheResultStatus::Hit) {
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

        THolder<NTable::NTest::TTableCacheContext> Context_;
        TSharedCache* Cache_ = nullptr;
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection_;
        TSharedCacheCollectionRef Collection_;
        TStdRawMap StdRaw_;
        TYdbRawMap YdbRaw_;
        TStdOwnedMap StdOwned_;
        TYdbOwnedMap YdbOwned_;
        TVector<TSharedPageRef> Pinned_;
        TVector<ui32> Order_;
        ui32 PageCount_ = 0;
    };

    BENCHMARK_DEFINE_F(TSharedCacheFixture, StdRawHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupRaw(StdRaw_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, YdbRawHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupRaw(YdbRaw_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, CacheTableHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupTable(offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, StdOwnedHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupOwned<false>(StdOwned_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, YdbOwnedHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupOwned<false>(YdbOwned_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, CacheHit)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupCache<false>(offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, StdOwnedRead)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupOwned<true>(StdOwned_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, YdbOwnedRead)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupOwned<true>(YdbOwned_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, CacheRead)(benchmark::State& state) {
        RunLookups(state, false, [this](ui64 offset) {
            return LookupCache<true>(offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, StdRawMiss)(benchmark::State& state) {
        RunLookups(state, true, [this](ui64 offset) {
            return LookupRaw(StdRaw_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, YdbRawMiss)(benchmark::State& state) {
        RunLookups(state, true, [this](ui64 offset) {
            return LookupRaw(YdbRaw_, offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, CacheTableMiss)(benchmark::State& state) {
        RunLookups(state, true, [this](ui64 offset) {
            return LookupTable(offset);
        });
    }

    BENCHMARK_DEFINE_F(TSharedCacheFixture, CacheMiss)(benchmark::State& state) {
        RunLookups(state, true, [this](ui64 offset) {
            return LookupCache<false>(offset);
        });
    }

    BENCHMARK_REGISTER_F(TSharedCacheFixture, StdRawHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, YdbRawHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, CacheTableHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, StdOwnedHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, YdbOwnedHit)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, CacheHit)
        ->Args({ ResidentPages, 0 })
        ->Args({ ResidentPages, 1 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, StdOwnedRead)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, YdbOwnedRead)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, CacheRead)
        ->Args({ ResidentPages, 0 })
        ->Args({ ResidentPages, 1 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, StdRawMiss)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, YdbRawMiss)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, CacheTableMiss)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);
    BENCHMARK_REGISTER_F(TSharedCacheFixture, CacheMiss)
        ->Args({ ResidentPages, 0 })
        ->ArgNames({ "pages", "native_pins" })
        ->Iterations(ResidentPages)
        ->Unit(benchmark::kNanosecond);

} // namespace
} // namespace NKikimr::NSharedCache
