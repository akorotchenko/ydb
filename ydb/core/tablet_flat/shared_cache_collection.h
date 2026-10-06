#pragma once

#include "defs.h"
#include "shared_cache_item.h"

#include <util/generic/ptr.h>

namespace NKikimr::NSharedCache {

// The cache collection owns its page metadata, Sticky list, and resident-byte estimates.

struct TCollectionLocation {
    TLogoBlobID Id;
    ui64 BackingSize = 0;

    ui64 AccountedBytes() const noexcept;
};

template <class TTraits>
class TSharedCacheImpl;

using ECacheMode = NTable::NPage::ECacheMode;

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

    ui64 ResidentPageBytes() const noexcept {
        return ResidentPageBytes_.load(std::memory_order_relaxed);
    }

    ui64 ActivePageBytes() const noexcept {
        const i64 bytes = ActivePageBytes_.load(std::memory_order_relaxed);
        return bytes > 0 ? static_cast<ui64>(bytes) : 0;
    }

    bool IsKeepAllowedPage(NTable::NPage::EPage type) const noexcept {
        return !NPageCollection::IsDeadPage(type, PageCollection_->SkipBTreeIndexV1Shadow());
    }

    ui64 KeepResidentPageBytes() const noexcept {
        return KeepResidentPageBytes_.load(std::memory_order_relaxed);
    }

    ui64 KeepActivePageBytes() const noexcept {
        const i64 bytes = KeepActivePageBytes_.load(std::memory_order_relaxed);
        return bytes > 0 ? static_cast<ui64>(bytes) : 0;
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
    std::atomic<ECacheMode> CacheMode_{ ECacheMode::Sticky };
    std::atomic<ui64> KeepGeneration_{ 0 };
    std::atomic<ui64> ResidentPageBytes_{ 0 };
    // Ring transitions update this estimate after publishing their page state.
    std::atomic<i64> ActivePageBytes_{ 0 };
    std::atomic<ui64> KeepResidentPageBytes_{ 0 };
    std::atomic<i64> KeepActivePageBytes_{ 0 };

    void AddActivePageBytes(NTable::NPage::EPage type, i64 bytes) noexcept {
        ActivePageBytes_.fetch_add(bytes, std::memory_order_relaxed);
        if (IsKeepAllowedPage(type)) {
            KeepActivePageBytes_.fetch_add(bytes, std::memory_order_relaxed);
        }
    }
};

inline ui64 TCollectionLocation::AccountedBytes() const noexcept {
    // Restored when collection metadata joins cache-limit accounting.
    return 0;
}

} // namespace NKikimr::NSharedCache
