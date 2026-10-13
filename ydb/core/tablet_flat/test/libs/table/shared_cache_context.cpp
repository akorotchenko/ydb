#include <ydb/core/tablet_flat/shared_cache.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/hook/hook.h>

namespace NKikimr::NTable::NTest {
namespace {

    THolder<TActorSystemStub> ApplicationContext;

}

Y_TEST_HOOK_BEFORE_RUN(InitializeTablePageCache) {
    ApplicationContext = MakeHolder<TActorSystemStub>();
    NSharedCache::TSharedCacheCapacity capacity;
    constexpr ui32 hazards = NActors::MaxWorkers;
    Y_ENSURE(NSharedCache::TryCalculateSharedCacheCapacity(
        128_MB, NSharedCache::TSharedCache::ExpectedPageSize, 0, hazards, capacity));
    auto core = NSharedCache::TSharedCache::Create(
        NSharedCache::TSharedCacheSpace::Create(capacity, capacity, hazards), capacity.Limit);
    Y_ENSURE(core);
}

Y_TEST_HOOK_AFTER_RUN(ReleaseTablePageCache) {
    ApplicationContext.Reset();
}

} // namespace NKikimr::NTable::NTest
