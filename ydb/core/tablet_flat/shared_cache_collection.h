#pragma once

#include "defs.h"
#include "shared_cache_item.h"

#include <util/generic/ptr.h>

namespace NKikimr::NSharedCache {

// The cache collection owns its page metadata and Sticky list. Legacy actor bookkeeping stays in the legacy actor.

struct TCollectionLocation {
    TLogoBlobID Id;
    ui64 BackingSize = 0;

    ui64 AccountedBytes() const noexcept;
};

template <class TTraits>
class TSharedCacheImpl;

enum class ECollectionCacheMode : ui8 {
    Regular,
    Sticky,
    Keep,
};

class TCacheCollection {
public:
    explicit TCacheCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection,
        TCollectionCacheItem cacheItem = {}) noexcept;

    TCacheCollection(const TCacheCollection&) = delete;
    TCacheCollection& operator=(const TCacheCollection&) = delete;
    ~TCacheCollection();

    const TLogoBlobID& Id() const noexcept {
        return PageCollection_->Label();
    }

    const TIntrusiveConstPtr<NPageCollection::IPageCollection>& PageCollection() const noexcept {
        return PageCollection_;
    }

    NTable::NPage::TPageLocation GetLocation(ui32 pageId) const {
        return PageCollection_->GetLocation(pageId);
    }

    ui64 AccountedBytes() const noexcept {
        // Restored when collection metadata joins cache-limit accounting.
        return 0;
    }

    ui64 KeepGeneration() const noexcept {
        return KeepGeneration_.load(std::memory_order_acquire);
    }

public:
    TCollectionCacheItem CacheItem;
    std::atomic<ui32> StickyPageListHead{ 0 };
    std::atomic<ui64> References{ 0 };

private:
    template <class>
    friend class TSharedCacheImpl;

    const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection_;
    std::atomic<TCollectionRegistry*> Registry_{ nullptr };
    std::atomic<ECollectionCacheMode> Mode_{ ECollectionCacheMode::Sticky };
    std::atomic<ui64> KeepGeneration_{ 0 };
};

inline ui64 TCollectionLocation::AccountedBytes() const noexcept {
    // Restored when collection metadata joins cache-limit accounting.
    return 0;
}

} // namespace NKikimr::NSharedCache
