#include "shared_cache_collection.h"

namespace NKikimr::NSharedCache {

TCacheCollection::TCacheCollection(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TCollectionCacheItem cacheItem) noexcept
    : CacheItem(cacheItem)
    , PageCollection_(std::move(pageCollection))
{
    Y_DEBUG_ABORT_UNLESS(PageCollection_);
}

TCacheCollection::~TCacheCollection() = default;

} // namespace NKikimr::NSharedCache
