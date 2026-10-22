#include "flat_sausagecache.h"
#include "shared_cache.h"

#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr::NSharedCache {

TSharedPageRef TCollection::TryGetPage(const NTable::NPage::TPageLocation& location, bool sticky) const {
    auto* cache = TSharedCache::TryGet();
    if (!cache) {
        return {};
    }
    TSharedCacheCollectionRef collection;
    if (cache->Find(Id(), collection) != ESharedCacheResultStatus::Hit) {
        return {};
    }
    const TCollectionCacheItem item = collection.CacheItem();
    TSharedPageRef page;
    if (cache->Find(item, static_cast<ui64>(location.Offset), page) != ESharedCacheResultStatus::Hit) {
        return {};
    }
    if (sticky && !page.IsSticky()) {
        // A warm metadata hit must receive the same admission hint as a loader fetch.
        // Reattach a collection retained across reboot before linking its Sticky pages.
        if (!HasAppData()) {
            return {};
        }
        TCollectionCacheItem inserted;
        TSharedCacheCollectionRef attached;
        const TCollectionLocation collectionLocation{ .Id = Id(), .BackingSize = PageCollection_->BackingSize() };
        const auto status = cache->FindOrInsert(collectionLocation, inserted, attached);
        if (status == ESharedCacheResultStatus::Inserted) {
            Y_ENSURE(cache->MakeReady(inserted, MakeHolder<TCollection>(PageCollection_, inserted)));
            Y_ENSURE(cache->SetCollectionPagesCacheMode(inserted, ECacheMode::Regular));
            return {};
        }
        if (status != ESharedCacheResultStatus::Hit || attached.CacheItem() != item) {
            return {};
        }
        TSharedCachePageRequest request(location, MakeIntrusive<TPageFetchWaiter>(), true);
        if (!cache->FindOrInsertBatch(item, { &request, 1 }, false) ||
            request.Status() != ESharedCacheResultStatus::Hit) {
            return {};
        }
        page = std::move(request.Page());
    }
    return page;
}

} // namespace NKikimr::NSharedCache
