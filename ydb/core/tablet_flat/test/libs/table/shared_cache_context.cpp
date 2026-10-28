#include "shared_cache_context.h"

#include <ydb/core/tablet_flat/cache.h>
#include <ydb/core/testlib/actor_helpers.h>

namespace NKikimr::NTable::NTest {

namespace {
    NSharedCache::TCacheCapacityConfig DefaultCapacity() {
        NSharedCache::TCacheCapacityConfig capacity;
        Y_ENSURE(NSharedCache::TryCalculateSharedCacheCapacity(
            128_MB, NSharedCache::TSharedCache::ExpectedPageSize, 0, NActors::MaxWorkers, capacity));
        return capacity;
    }
} // namespace

TTableCacheContext::TTableCacheContext()
    : TTableCacheContext(DefaultCapacity())
{}

TTableCacheContext::TTableCacheContext(const NSharedCache::TCacheCapacityConfig& capacity) {
    Y_ABORT_UNLESS(!NActors::TlsActivationContext && !NSharedCache::TlsCacheHazard);
    Context_ = MakeHolder<TActorSystemStub>();
    constexpr ui32 hazards = NActors::MaxWorkers;
    Threads_ = MakeHolder<NSharedCache::TCacheThreadSubSystem>(hazards);
    NSharedCache::TlsCacheHazard = &Threads_->Hazard();
    try {
        auto cache = NSharedCache::TSharedCache::Create(
            NSharedCache::TCacheSpace::Create(capacity, capacity, *Threads_), capacity.Limit);
        Y_ENSURE(cache);
    } catch (...) {
        NSharedCache::TlsCacheHazard = nullptr;
        throw;
    }
}

TTableCacheContext::~TTableCacheContext() {
    NSharedCache::TlsCacheHazard = nullptr;
    Context_.Reset();
}

NActors::TActorSystem& TTableCacheContext::ActorSystem() const noexcept {
    return *Context_->System;
}

} // namespace NKikimr::NTable::NTest
