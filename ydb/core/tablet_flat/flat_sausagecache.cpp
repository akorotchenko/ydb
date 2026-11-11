#include "flat_sausagecache.h"
#include "cache.h"

#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr::NSharedCache {

TCachePageRef TCollection::TryGetPage(
    TCacheOperationView<TProdTraits>& cacheOp, const NTable::NPage::TPageLocation& location, bool sticky) const {
    TOperationItemRef<TProdTraits> collection(cacheOp.SpaceOperation());
    if (cacheOp.FindCollection(Id(), collection.Get()) != ECacheResultStatus::Hit) {
        return {};
    }
    const TCollectionCacheItem item = TCollectionCacheItem::FromValidated(collection.CacheItem());
    TOperationItemRef<TProdTraits> pageRef(cacheOp.SpaceOperation());
    if (cacheOp.FindPage(item, static_cast<ui64>(location.Offset), pageRef.Get()) != ECacheResultStatus::Hit) {
        return {};
    }
    TCachePageRef page = cacheOp.BuildPageRef(pageRef.Take());
    if (sticky && !page.IsSticky()) {
        // A warm metadata hit must receive the same admission hint as a loader fetch.
        // Reattach a collection retained across reboot before linking its Sticky pages.
        if (!HasAppData()) {
            return {};
        }
        auto& cache = TCache::Get();
        THolder<TCollection> value;
        TCollectionCacheItem inserted;
        TCacheCollectionRef attached;
        const auto status = cache.FindOrInsert(PageCollection(), value, inserted, attached);
        if (status == ECacheResultStatus::Inserted) {
            Y_ENSURE(cache.MakeReady(inserted, std::move(value)));
            return {};
        }
        if (status != ECacheResultStatus::Hit || attached.CacheItem() != item) {
            return {};
        }
        TCachePageRequest request(location, MakeIntrusive<TPageFetchWaiter>(), true);
        if (!cache.FindOrInsertBatch(item, { &request, 1 }, false) || request.Status() != ECacheResultStatus::Hit) {
            return {};
        }
        page = std::move(request.Page());
    }
    return page;
}

} // namespace NKikimr::NSharedCache
