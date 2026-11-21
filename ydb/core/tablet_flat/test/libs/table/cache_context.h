#pragma once

#include <ydb/core/tablet_flat/cache_thread.h>
#include <ydb/core/tablet_flat/cache_fwd.h>

#include <util/generic/ptr.h>

namespace NActors {
class TActorSystem;
class TSharedData;
}

namespace NKikimr {
struct TActorSystemStub;

namespace NPageCollection {
    class IPageCollection;
}

namespace NTable::NPage {
    struct TPageLocation;
}
}

namespace NKikimr::NSharedCache {
struct TCacheCapacityConfig;
}

namespace NKikimr::NTable::NTest {

TCacheCollectionRef AdmitCollection(TCache& cache, TIntrusiveConstPtr<NPageCollection::IPageCollection> collection);
TCachePageRef AdmitPage(TCache& cache, TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
    NPage::TPageLocation location, NActors::TSharedData&& data, bool sticky = false);

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
