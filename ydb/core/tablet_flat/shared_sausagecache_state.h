#pragma once

#include "shared_cache_events.h"

#include <util/generic/set.h>

namespace NKikimr::NSharedCache {

class TCollection {
public:
    TLogoBlobID Id;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    TCollectionCacheItem CacheItem;
    TSet<TActorId> InMemoryOwners;
    TSet<TActorId> StickyOwners;
    TSet<TActorId> Owners;
    ui64 TotalSize = 0;
    ui64 TotalPages = 0;
    ui64 PendingRequests = 0;
    bool SavedToCore = false;
    TMonotonic CoreSavedAt;
    bool CoreKeepsPages = false;
    bool CoreStickyPages = false;
    ui64 CoreKeepGeneration = 0;

    ECacheMode GetCacheMode() const {
        return InMemoryOwners ? ECacheMode::TryKeepInMemory : ECacheMode::Regular;
    }
};

} // namespace NKikimr::NSharedCache
