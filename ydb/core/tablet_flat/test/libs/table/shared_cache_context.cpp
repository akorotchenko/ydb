#include "shared_cache_context.h"

#include <ydb/core/tablet_flat/shared_cache.h>
#include <ydb/core/testlib/actor_helpers.h>

namespace NKikimr::NTable::NTest {

TTableCacheContext::TTableCacheContext() {
    Y_ABORT_UNLESS(!NActors::TlsActivationContext && !NSharedCache::TlsCacheHazard);
    Context_ = MakeHolder<TActorSystemStub>();
    NSharedCache::TSharedCacheCapacity capacity;
    constexpr ui32 hazards = NActors::MaxWorkers;
    Threads_ = MakeHolder<NSharedCache::TCacheThreadSubSystem>(hazards);
    NSharedCache::TlsCacheHazard = &Threads_->Hazard();
    try {
        Y_ENSURE(NSharedCache::TryCalculateSharedCacheCapacity(
            128_MB, NSharedCache::TSharedCache::ExpectedPageSize, 0, hazards, capacity));
        auto cache = NSharedCache::TSharedCache::Create(
            NSharedCache::TSharedCacheSpace::Create(capacity, capacity, *Threads_), capacity.Limit);
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
