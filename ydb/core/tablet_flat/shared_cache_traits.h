#pragma once

#include "shared_cache_item.h"

namespace NActors {
class TActorSystem;
}

namespace NKikimr::NSharedCache {

enum class ESharedCacheHookPoint {
    AfterInsertPositionFound, // Payload is the link owner, before candidate preparation or publication.
    BeforeTableLinkCas, // Payload is the link owner, may be null.
    AfterTableLinkCas, // Payload is the link owner, may be null.
    AfterTombstoneOwnerClaim, // Payload is the claimed item.
    AfterTombstoneHelperClaim, // Payload is the claimed item.
    BeforeTombstoneFinalCas, // Payload is the item at its final CAS.
    BeforeHotExchange, // Payload is the Hot item; the target slot has been reserved but not exchanged.
    AfterGrowthBucketPublished, // Payload is the resize marker.
    AfterBucketSplitActivated, // Payload is the resize marker.
    AfterBucketResizeCursorPublished, // Payload is the resize marker.
    AfterShrinkBucketClosed, // Payload is the resize marker.
    AfterBucketSplitRemoved, // Payload is the resize marker.
    AfterStickyPageLinked, // Payload is the linked Sticky page before its ready State is published.
    AfterStickyPageListDetached, // Payload is the owning collection while the detached list is private.
    AfterStickyPageListHeadExchanged, // Payload is the owning collection before its retained tail is connected.
    AfterFetchWaiterSubscribed, // Payload is the pending page while the subscriber still owns its structural ref.
    BeforeCollectionHotPublished, // Payload is unlinked and no longer owned, but still has Sticky State.
    AfterReplacingPublished, // Payload is the old forwarding item before its incoming link is replaced.
    AfterReplacedPublished, // Payload is the completed old replacement before its owner ref is dropped.
};

struct TProdTraits {
    NActors::TActorSystem* KeepEvictionActorSystem = nullptr;
    TActorId KeepEvictionActor;

    static void* TrySharedCachePages() noexcept;
    static bool InstallSharedCachePages(void* cache) noexcept;
    static void BindSharedCachePages(void* cache) noexcept;
    static void UnbindSharedCachePages(void* cache) noexcept;
    static ui32 CurrentWorkerIndex() noexcept;

    void NotifyKeepPageEviction(
        const TLogoBlobID& collectionId, ui64 generation, NTable::NPage::TPageLocation location) const noexcept;

    Y_FORCE_INLINE void Invoke(ESharedCacheHookPoint, TCacheItem) const noexcept {
    }
};

struct TTestTraits {
    inline static thread_local void* CurrentSharedCachePages = nullptr;

    void* Context = nullptr;
    // The TCacheItem argument follows the payload rules documented on ESharedCacheHookPoint.
    void (*Function)(void*, ESharedCacheHookPoint, TCacheItem) noexcept = nullptr;

    void Invoke(ESharedCacheHookPoint point, TCacheItem cacheItem) const noexcept {
        if (Function) {
            Function(Context, point, cacheItem);
        }
    }

    void NotifyKeepPageEviction(const TLogoBlobID&, ui64, NTable::NPage::TPageLocation) const noexcept {
    }

    static void* TrySharedCachePages() noexcept {
        return CurrentSharedCachePages;
    }

    static bool InstallSharedCachePages(void*) noexcept {
        return true;
    }

    static void BindSharedCachePages(void* cache) noexcept {
        Y_DEBUG_ABORT_UNLESS(!CurrentSharedCachePages);
        CurrentSharedCachePages = cache;
    }

    static void UnbindSharedCachePages(void* cache) noexcept {
        Y_DEBUG_ABORT_UNLESS(CurrentSharedCachePages == cache);
        CurrentSharedCachePages = nullptr;
    }

    static ui32 CurrentWorkerIndex() noexcept {
        return 0;
    }
};

struct TSharedCachePolicy {
    double CurrentLimitGap = 0.10;
    ui64 MinCurrentLimitGap = ui64{ 100 } << 20;
    double ColdMin = 0.20;
    double Grow = 0.30;
    double HotMin = 0.25;
    double ResizeStep = 0.05;
    ui32 MinHotSlots = 1024;
    ui32 MinHotSlotsUnderPressure = 15;
    ui32 MinResizeStep = 256;
};

template <class TTraits>
inline constexpr TSharedCachePolicy SharedCachePolicyFor{};

template <>
inline constexpr TSharedCachePolicy SharedCachePolicyFor<TTestTraits>{
    .MinCurrentLimitGap = 0,
    .MinHotSlots = 15,
    .MinResizeStep = 1,
};

} // namespace NKikimr::NSharedCache
