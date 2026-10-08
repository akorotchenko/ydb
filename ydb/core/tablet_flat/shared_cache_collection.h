#pragma once

#include "defs.h"
#include "shared_cache_item.h"
#include "shared_cache_fwd.h"

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

struct TPageCollectionStats {
    ui64 PageCollections = 0;
    ui64 SharedBodyBytes = 0;
    ui64 StickyBytes = 0;
    ui64 TryKeepInMemoryBytes = 0;
};

// One shared accumulator per executor, updated by the cache actor's owner registration.
struct TCollectionOwnerStats : public TThrRefBase {
    std::atomic<ui64> PageCollections{ 0 };
    std::atomic<ui64> SharedBodyBytes{ 0 };
    std::atomic<ui64> StickyBytes{ 0 };
    std::atomic<ui64> TryKeepInMemoryBytes{ 0 };

    TPageCollectionStats Read() const noexcept {
        return {
            .PageCollections = PageCollections.load(std::memory_order_relaxed),
            .SharedBodyBytes = SharedBodyBytes.load(std::memory_order_relaxed),
            .StickyBytes = StickyBytes.load(std::memory_order_relaxed),
            .TryKeepInMemoryBytes = TryKeepInMemoryBytes.load(std::memory_order_relaxed),
        };
    }
};

struct TCollectionActorState;

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

    NTable::NPage::EPage GetPageType(ui32 pageId) const noexcept {
        return NTable::NPage::EPage(PageCollection_->Page(pageId).Type);
    }

    ui64 GetPageSize(ui32 pageId) const noexcept {
        return PageCollection_->Page(pageId).Size;
    }

    ECacheMode GetCacheMode() const noexcept {
        return CacheMode_.load(std::memory_order_acquire);
    }

    void SetSkipBTreeIndexV1Shadow(bool skip);
    void DrainStickyPages();

    TSharedCachePageRef TryGetPage(const NTable::NPage::TPageLocation& location, bool sticky = false) const;

    ui64 TotalSize() const noexcept {
        return NActors::TSharedData::OverheadSize * PageCollection_->Total() + PageCollection_->BackingSize();
    }

    // Actor-only bookkeeping; native readers and writers do not access it.
    TCollectionActorState* GetActorState() const noexcept {
        return ActorState_.load(std::memory_order_acquire);
    }

    TCollectionActorState& EnsureActorState();
    void ResetActorState() noexcept;

    // Page items retain metadata through pending I/O, residency and retirement.
    bool HasPageItems() const noexcept {
        return References.load(std::memory_order_acquire) > (Registry_.load(std::memory_order_acquire) ? 1 : 0);
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
        const ui64 bytes = KeepResidentPageBytes_.load(std::memory_order_relaxed);
        const ui64 shadowBytes =
            PageCollection_->SkipBTreeIndexV1Shadow() ? BTreeIndexV1ResidentBytes_.load(std::memory_order_relaxed) : 0;
        return bytes - Min(bytes, shadowBytes);
    }

    ui64 KeepActivePageBytes() const noexcept {
        const i64 bytes =
            KeepActivePageBytes_.load(std::memory_order_relaxed) -
            (PageCollection_->SkipBTreeIndexV1Shadow() ? BTreeIndexV1ActiveBytes_.load(std::memory_order_relaxed) : 0);
        return bytes > 0 ? static_cast<ui64>(bytes) : 0;
    }

public:
    TCollectionCacheItem CacheItem;
    std::atomic<ui32> StickyPageListHead{ 0 };
    std::atomic<ui64> References{ 0 };

private:
    template <class>
    friend class TSharedCacheImpl;

    std::atomic<TCollectionActorState*> ActorState_{ nullptr };
    const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection_;
    std::atomic<TCollectionRegistry*> Registry_{ nullptr };
    std::atomic<ECacheMode> CacheMode_{ ECacheMode::Sticky };
    std::atomic<ui64> KeepGeneration_{ 0 };
    std::atomic<ui64> ResidentPageBytes_{ 0 };
    // Ring transitions update this estimate after publishing their page state.
    std::atomic<i64> ActivePageBytes_{ 0 };
    std::atomic<ui64> KeepResidentPageBytes_{ 0 };
    std::atomic<i64> KeepActivePageBytes_{ 0 };
    // Keep accounting excludes Skip permanently. Track V1 bytes independently so
    // changing the reader's shadow selection cannot change publication/reclaim deltas.
    std::atomic<ui64> BTreeIndexV1ResidentBytes_{ 0 };
    std::atomic<i64> BTreeIndexV1ActiveBytes_{ 0 };

    // A nonblocking publisher folds concurrent counter/mode changes into core totals.
    std::atomic<ui32> KeepAccountingState_{ 0 };
    std::atomic<bool> BytesNotificationPending_{ false };
    // Only the current publisher accesses these contributions.
    ui64 ReportedKeepResidentBytes_ = 0;
    ui64 ReportedKeepActiveBytes_ = 0;
};

inline ui64 TCollectionLocation::AccountedBytes() const noexcept {
    // Restored when collection metadata joins cache-limit accounting.
    return 0;
}

} // namespace NKikimr::NSharedCache
