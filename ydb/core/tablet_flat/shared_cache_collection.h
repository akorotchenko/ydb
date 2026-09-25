#pragma once

#include "defs.h"
#include "shared_cache_item.h"

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSharedCache {

// Host-side collection bookkeeping: the cache and the sausage cache actor own these objects, they are not part of
// the mapped item layout in shared_cache_item.h. Keep them out of that header so code that only needs the item
// layout -- notably the legacy actor, which has its own TCollection/TPageSet -- does not pull them in.

struct TPageByOffsetHash {
    size_t operator()(const TIntrusivePtr<TPage>& ptr) const;
    size_t operator()(NTable::NPage::TPageOffset offset) const;
};

struct TPageByOffsetEq {
    bool operator()(const TIntrusivePtr<TPage>& left, const TIntrusivePtr<TPage>& right) const;
    bool operator()(const TIntrusivePtr<TPage>& left, NTable::NPage::TPageOffset right) const;
    bool operator()(NTable::NPage::TPageOffset left, const TIntrusivePtr<TPage>& right) const;
};

using TPageSetBase = THashSet<TIntrusivePtr<TPage>, TPageByOffsetHash, TPageByOffsetEq>;

class TPageSet : public TPageSetBase {
public:
    using TPageSetBase::TPageSetBase;

    TPage* FindPage(NTable::NPage::TPageOffset offset) const;
    bool ErasePage(NTable::NPage::TPageOffset offset);
};

struct TCollectionLocation {
    TLogoBlobID Id;
    ui64 BackingSize = 0;

    ui64 AccountedBytes() const noexcept;
};

template <class TTraits>
class TSharedCacheImpl;

class TCollection {
public:
    explicit TCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection,
        TCollectionCacheItem cacheItem = {}) noexcept;

    TCollection(const TCollection&) = delete;
    TCollection& operator=(const TCollection&) = delete;
    ~TCollection();

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

    NTable::NPage::ECacheMode GetCacheMode() const noexcept {
        return CacheMode;
    }

public:
    TCollectionCacheItem CacheItem;
    NTable::NPage::ECacheMode CacheMode = NTable::NPage::ECacheMode::Regular;
    // Who asked to keep this collection in memory. The keep authority is the cache (SetCollectionKeepPages); this
    // set only says which actors receive the loaded pages so their preload walk can continue.
    TSet<TActorId> KeepOwners;
    // Pages of a kept collection that still have to be loaded, drained by the cache actor's preload driver.
    TVector<NTable::NPage::TPageLocation> KeepPending;
    TSet<TActorId> Owners;
    THashMap<TActorId, TCollectionRegistry*> RegistryOwners;
    TPageSet PageSet;
    ui64 TotalSize = 0;
    ui64 AliveBytes = 0;
    ui64 TotalPages = 0;

    std::atomic<ui32> KeepPageListHead{ 0 };
    std::atomic<ui64> References{ 0 };

private:
    template <class>
    friend class TSharedCacheImpl;

    const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection_;
    std::atomic<TCollectionRegistry*> Registry_{ nullptr };
    std::atomic<bool> KeepPages_{ true };
};

inline ui64 TCollectionLocation::AccountedBytes() const noexcept {
    // Restored when collection metadata joins cache-limit accounting.
    return 0;
}

} // namespace NKikimr::NSharedCache
