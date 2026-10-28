#pragma once

#include <ydb/core/tablet_flat/cache_thread.h>

#include <util/generic/ptr.h>

namespace NActors {
class TActorSystem;
}

namespace NKikimr {
struct TActorSystemStub;
}

namespace NKikimr::NSharedCache {
struct TCacheCapacityConfig;
}

namespace NKikimr::NTable::NTest {

class TTableCacheContext {
public:
    TTableCacheContext();
    explicit TTableCacheContext(const NSharedCache::TCacheCapacityConfig& capacity);
    ~TTableCacheContext();

    NActors::TActorSystem& ActorSystem() const noexcept;

private:
    THolder<NSharedCache::TCacheThreadSubSystem> Threads_;
    THolder<TActorSystemStub> Context_;
};

} // namespace NKikimr::NTable::NTest
