#pragma once
#include "defs.h"
#include "shared_cache_item.h"
#include "shared_cache_fwd.h"

namespace NKikimr::NSharedCache {

class TSharedCachePages : public TThrRefBase {
public:
    // Loaders and writers use the owner of their execution context.
    static TSharedCachePages& Get();
    TSharedCacheCollectionRef AdmitCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection);
    TSharedCachePageRef AdmitPage(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        NTable::NPage::TPageLocation location, NActors::TSharedData&& data, bool sticky = false);

    TIntrusivePtr<TCollectionRegistry> Registry = new TCollectionRegistry;
    TIntrusivePtr<TThrRefBase> Cache;
};

} // namespace NKikimr::NSharedCache
