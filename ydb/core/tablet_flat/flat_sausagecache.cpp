#include "flat_sausagecache.h"
#include "cache.h"

#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr::NSharedCache {

TCachePageRef TCollection::TryGetPage(const NTable::NPage::TPageLocation& location, bool sticky) const {
    auto* cache = TSharedCache::TryGet();
    if (!cache) {
        return {};
    }
    TCacheCollectionRef collection;
    if (cache->Find(Id(), collection) != ECacheResultStatus::Hit) {
        return {};
    }
    const TCollectionCacheItem item = collection.CacheItem();
    TCachePageRef page;
    if (cache->Find(item, static_cast<ui64>(location.Offset), page) != ECacheResultStatus::Hit) {
        return {};
    }
    if (sticky && !page.IsSticky()) {
        // A warm metadata hit must receive the same admission hint as a loader fetch.
        // Reattach a collection retained across reboot before linking its Sticky pages.
        if (!HasAppData()) {
            return {};
        }
        TCollectionCacheItem inserted;
        TCacheCollectionRef attached;
        const TCollectionLocation collectionLocation{ .Id = Id(), .BackingSize = PageCollection_->BackingSize() };
        const auto status = cache->FindOrInsert(collectionLocation, inserted, attached);
        if (status == ECacheResultStatus::Inserted) {
            Y_ENSURE(
                cache->MakeReady(inserted, MakeHolder<TCollection>(PageCollection_, inserted, ECacheMode::Regular)));
            return {};
        }
        if (status != ECacheResultStatus::Hit || attached.CacheItem() != item) {
            return {};
        }
        TCachePageRequest request(location, MakeIntrusive<TPageFetchWaiter>(), true);
        if (!cache->FindOrInsertBatch(item, { &request, 1 }, false) || request.Status() != ECacheResultStatus::Hit) {
            return {};
        }
        page = std::move(request.Page());
    }
    return page;
}

} // namespace NKikimr::NSharedCache
