#include "cache_context.h"

#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/tablet_flat/cache.h>
#include <ydb/core/testlib/actor_helpers.h>

namespace NKikimr::NTable::NTest {

namespace {
    NSharedCache::TCacheCapacityConfig DefaultCapacity() {
        NSharedCache::TCacheCapacityConfig capacity;
        Y_ENSURE(NSharedCache::TryCalculateCacheCapacity(
            128_MB, NSharedCache::TCache::ExpectedPageSize, 0, NActors::MaxWorkers, capacity));
        return capacity;
    }
} // namespace

TCacheCollectionRef AdmitCollection(TCache& cache, TIntrusiveConstPtr<NPageCollection::IPageCollection> collection) {
    TCacheCollectionRef ready;
    const auto status = cache.BeginOperation().AdmitCollection(collection, ready);
    Y_ENSURE(status != NSharedCache::ECacheResultStatus::NoResource,
        "NoResource admitting test collection " << collection->Label());
    return ready;
}

TCachePageRef AdmitPage(TCache& cache, TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
    NPage::TPageLocation location, NActors::TSharedData&& data, bool sticky) {
    auto buffer = std::move(data);
    auto cacheOp = cache.BeginOperation();
    TCacheCollectionRef owner;
    const auto status = cacheOp.AdmitCollection(collection, owner);
    Y_ENSURE(status != NSharedCache::ECacheResultStatus::NoResource,
        "NoResource admitting test collection " << collection->Label());
    return cacheOp.AdmitPage(owner.CacheItem(), location, std::move(buffer), sticky);
}

TTableCacheContext::TTableCacheContext()
    : TTableCacheContext(DefaultCapacity())
{}

TTableCacheContext::TTableCacheContext(const NSharedCache::TCacheCapacityConfig& capacity) {
    Y_ABORT_UNLESS(!NActors::TlsActivationContext && !NSharedCache::TlsCacheHazards);
    Context_ = MakeHolder<TActorSystemStub>();
    NKikimrConfig::TFeatureFlags flags;
    flags.SetEnableLockFreePageCache(true);
    Context_->AppData.InitFeatureFlags(flags);
    constexpr ui32 hazards = NActors::MaxWorkers;
    Threads_ = MakeHolder<NSharedCache::TCacheThreadSubSystem>(hazards);
    NSharedCache::TlsCacheHazards = { &Threads_->Hazard(), 1 };
    try {
        auto cache = NSharedCache::TCache::Create(
            NSharedCache::TCacheSpace::Create(capacity, capacity, *Threads_), capacity.Limit);
        Y_ENSURE(cache);
    } catch (...) {
        NSharedCache::TlsCacheHazards = {};
        throw;
    }
}

TTableCacheContext::~TTableCacheContext() {
    NSharedCache::TlsCacheHazards = {};
    Context_.Reset();
}

NActors::TActorSystem& TTableCacheContext::ActorSystem() const noexcept {
    return *Context_->System;
}

} // namespace NKikimr::NTable::NTest
