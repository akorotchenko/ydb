#pragma once

#include "shared_cache_item.h"

namespace NActors {
class TActorSystem;
}

namespace NKikimr::NSharedCache {

enum class ESharedCacheHookPoint {
    BeforeKeepBytesPublished, // Payload is the collection whose contribution is being published.
    AfterPageBatchPrepared, // Payload is the collection after the batch presence checks.
    AfterPageCollectionStateChecked, // Collection, before InitializePage increments its owner count.
    AfterInsertPositionFound, // Payload is the link owner, before candidate preparation or publication.
    BeforeTableLinkCas, // Payload is the link owner, may be null.
    AfterTableLinkCas, // Payload is the link owner, may be null.
    AfterTombstoneOwnerClaim, // Payload is the claimed item.
    AfterTombstoneHelperClaim, // Payload is the claimed item.
    BeforeTombstoneFinalCas, // Payload is the item at its final CAS.
    BeforeHotExchange, // Payload is the Hot item; the target slot has been reserved but not exchanged.
    AfterHotEvictionPublished, // Payload is the evicted item, before category accounting.
    AfterKeepColdDemoted, // Cold membership token, immediately after the demotion CAS.
    AfterKeepColdPromotion, // Native page identity, after Cold becomes KeepCold with its reference still held.
    AfterKeepColdEntryMatched, // Cold membership token, before ReclaimKeepCold calls EraseCold.
    AfterKeepColdQueued, // Cold membership token, after ring publication while the routing pin is held.
    AfterKeepColdRefReleased, // Native page identity, after releasing its last KeepCold reference.
    BeforeColdExchange, // Payload is the Cold membership after occupancy accounting, before ring publication.
    BeforeColdRingPop, // No item payload; an occupied Cold ring is about to be probed.
    AfterGrowthBucketPublished, // Payload is the resize marker.
    AfterBucketSplitActivated, // Payload is the resize marker.
    AfterBucketResizeCursorPublished, // Payload is the resize marker.
    AfterShrinkBucketClosed, // Payload is the resize marker.
    AfterBucketSplitRemoved, // Payload is the resize marker.
    BeforeStickyPageListLink, // Payload is the page after its collection mode check, before list publication.
    AfterStickyPageLinked, // Payload is the linked Sticky page, including pending admissions.
    BeforePageReadyState, // Payload is the completing page before its payload is published.
    BeforeReadyStatePublication, // Payload is the accounted item before the common ready CAS.
    AfterStickyPageListDetached, // Payload is the owning collection while the detached list is private.
    AfterStickyPageListHeadPublished, // Payload is the owning collection after its retained list is published.
    AfterFetchWaiterSubscribed, // Payload is the pending page while the subscriber still owns its structural ref.
    BeforeCollectionHotPublished, // Payload is the collection before insertion into a Hot ring.
    BeforePageReplacementRetry, // Source, while a held-page reader waits for its forwarder.
    BeforePageReplacementLinkCas, // Source, after preparing the successor but before freezing the forwarder.
    AfterReplacingPublished, // Payload is the old forwarding item before its incoming link is replaced.
    AfterReplacedPublished, // Payload is the completed old replacement before its owner ref is dropped.
    AfterCollectionOwnerReferenceDropped, // Payload is the collection after its owner count is decremented.
    BeforeCollectionAttachClaim, // Payload is the collection before its transition claim CAS.
    BeforeCollectionAttachPublished, // Payload is the collection before its attached-state CAS.
};

struct TProdTraits {
    NActors::TActorSystem* KeepEvictionActorSystem = nullptr;
    TActorId KeepEvictionActor;

    static void* TryGetCache() noexcept;
    static bool InstallCache(void* cache) noexcept;
    static ui32 CurrentWorkerIndex() noexcept;

    void NotifyResourcesAvailable() const noexcept;
    void NotifyCollectionReleased(const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem) const noexcept;
    void NotifyCollectionBytesChanged(const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem) const noexcept;

    void NotifyKeepPageEviction(const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem, ui64 generation,
        NTable::NPage::TPageLocation location) const noexcept;

    Y_FORCE_INLINE void Invoke(ESharedCacheHookPoint, TCacheItem) const noexcept {}
};

struct TTestTraits {
    inline static thread_local void* CurrentCache = nullptr;

    void* Context = nullptr;
    // The TCacheItem argument follows the payload rules documented on ESharedCacheHookPoint.
    void (*Function)(void*, ESharedCacheHookPoint, TCacheItem) noexcept = nullptr;

    void Invoke(ESharedCacheHookPoint point, TCacheItem cacheItem) const noexcept {
        if (Function) {
            Function(Context, point, cacheItem);
        }
    }

    void NotifyKeepPageEviction(
        const TLogoBlobID&, TCollectionCacheItem, ui64, NTable::NPage::TPageLocation) const noexcept {}

    void NotifyResourcesAvailable() const noexcept {}

    void NotifyCollectionReleased(const TLogoBlobID&, TCollectionCacheItem) const noexcept {}

    void NotifyCollectionBytesChanged(const TLogoBlobID&, TCollectionCacheItem) const noexcept {}

    static void* TryGetCache() noexcept {
        return CurrentCache;
    }

    static bool InstallCache(void*) noexcept {
        return true;
    }

    static void BindCache(void* cache) noexcept {
        Y_DEBUG_ABORT_UNLESS(!CurrentCache);
        CurrentCache = cache;
    }

    static void UnbindCache(void* cache) noexcept {
        Y_DEBUG_ABORT_UNLESS(CurrentCache == cache);
        CurrentCache = nullptr;
    }

    static ui32 CurrentWorkerIndex() noexcept {
        return 0;
    }
};

struct TSharedCachePolicy {
    double CurrentLimitGap = 0.15;
    ui64 MinCurrentLimitGap = 0; // An absolute floor would drain the payload of a small cache on pressure.
    double ColdMin = 0.20;
    double Grow = 0.30;
    double HotMin = 0.60;
    double HotMinUnderPressure = 0.25;
    double ResizeStep = 0.05;
    ui32 MinHotSlots = 25;
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
