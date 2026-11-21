#pragma once

#include "flat_exec_seat.h"
#include "cache.h"

#include <util/generic/hash.h>

namespace NKikimr::NTabletFlatExecutor {

struct TCacheSeat final : public TSeat {
    using TSeat::TSeat;

    using TPageKey = std::pair<NSharedCache::TCollectionCacheItem, NTable::NPage::TPageOffset>;

    struct TPageKeyHash {
        size_t operator()(const TPageKey& key) const noexcept {
            return HashPageKey(key.first, static_cast<ui64>(key.second));
        }
    };

    THashMap<TPageKey, TCachePageRef, TPageKeyHash> Pinned;
};

} // namespace NKikimr::NTabletFlatExecutor
