#pragma once

#include "cache_item.h"
#include "cache_thread.h"
#include "cache_tier.h"

#include <array>

namespace NActors {
class TActorSystem;
}

namespace NKikimr::NSharedCache {

enum class ECacheHookPoint {
    AfterPageRetierClaim,
    BeforeColdEraseClaim,
    AfterLastColdRefReleased,
    AfterColdTicketClaim,
    BeforeRetirementAccounting,
    BeforeReadyWaiters,
    BeforeReadyReconcile,
    AfterColdRefAcquired,
    BeforeLastColdModeCheck,

    AfterAdmissionReclaimStep, // No payload; a successful admission reclamation step has completed.
    ResourcesAvailable, // No item payload; test traits observe a resource-waiter notification.
    BeforeKeepBytesPublished, // Payload is the collection whose contribution is being published.
    AfterPageBatchPrepared, // Payload is the collection after the batch presence checks.
    AfterPageCollectionStateChecked, // Collection, before InitializePage increments its owner count.
    BeforeInsertOwnerAcquire, // Payload is the insertion predecessor, before acquiring its structural ref.
    AfterInsertPositionFound, // Payload is the link owner, before candidate preparation or publication.
    BeforeTableLinkCas, // Payload is the link owner, may be null.
    AfterTableLinkCas, // Payload is the link owner, may be null.
    AfterTombstoneOwnerClaim, // Payload is the claimed item.
    AfterTombstoneHelperClaim, // Payload is the claimed item.
    BeforeTombstoneFinalCas, // Payload is the item at its final CAS.
    BeforePreloadPageErase, // Page, after protecting its Regular collection and before the zero-ref erase CAS
    AfterPreloadHotTokenChecked, // Hot token, after protection classification and before the slot CAS
    BeforeHotExchange, // Payload is the Hot item; the target slot has been reserved but not exchanged.
    AfterHotEvictionPublished, // Payload is the evicted item, before category accounting.
    BeforeLastColdRefRelease, // Native identity, before the final Cold reference CAS
    BeforeColdExchange, // Payload is the Cold membership after occupancy accounting, before ring publication.
    BeforeColdRingPop, // No item payload; an occupied Cold ring is about to be probed.
    AfterGrowthBucketPublished, // Payload is the resize marker.
    AfterBucketSplitActivated, // Payload is the resize marker.
    AfterBucketResizeCursorPublished, // Payload is the resize marker.
    AfterShrinkBucketClosed, // Payload is the resize marker.
    BeforeBucketSplitLinkClaim, // Payload is the marker's link owner, before acquiring its structural ref.
    AfterBucketSplitOwnerCheck, // Tombstone marker, after observing the resize owner's sole reference.
    AfterBucketSplitRemoved, // Payload is the resize marker.
    BeforeStickyPageListLink, // Payload is the page after its collection mode check, before list publication.
    AfterStickyPageLinked, // Payload is the linked Sticky page, including pending admissions.
    BeforePageReadyState, // Payload is the completing page before its payload is published.
    BeforeReadyStatePublication, // Payload is the accounted item before the common ready CAS.
    AfterStickyPageListDetached, // Payload is the owning collection while the detached list is private.
    AfterStickyPageListHeadPublished, // Payload is the owning collection after its retained list is published.
    AfterFetchWaiterSubscribed, // Payload is the pending page while the subscriber still owns its structural ref.
    AfterFetchCompletingClaim, // Payload is the page after the last pending releaser claims exclusive finalization.
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

    void NotifyResourcesAvailable() const noexcept;
    void NotifyCollectionReleased(const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem) const noexcept;
    void NotifyCollectionBytesChanged(const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem) const noexcept;

    void NotifyKeepPageEviction(const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem, ui64 generation,
        NTable::NPage::TPageLocation location) const noexcept;

    Y_FORCE_INLINE void Invoke(ECacheHookPoint, TCacheItem) const noexcept {}
};

struct TTestTraits {
    void* Context = nullptr;
    // The TCacheItem argument follows the payload rules documented on ECacheHookPoint.
    void (*Function)(void*, ECacheHookPoint, TCacheItem) noexcept = nullptr;

    void Invoke(ECacheHookPoint point, TCacheItem cacheItem) const noexcept;

    void NotifyKeepPageEviction(
        const TLogoBlobID&, TCollectionCacheItem, ui64, NTable::NPage::TPageLocation) const noexcept {}

    void NotifyResourcesAvailable() const noexcept {
        Invoke(ECacheHookPoint::ResourcesAvailable, {});
    }

    void NotifyCollectionReleased(const TLogoBlobID&, TCollectionCacheItem) const noexcept {}

    void NotifyCollectionBytesChanged(const TLogoBlobID&, TCollectionCacheItem) const noexcept {}

    static void* TryGetCache() noexcept {
        return TlsCacheHazard ? TlsCacheHazard->TestCache : nullptr;
    }

    static bool InstallCache(void*) noexcept {
        return true;
    }
};

struct TCachePolicy {
    double CurrentLimitGap = 0.15;
    ui64 MinCurrentLimitGap = 0; // An absolute floor would drain the payload of a small cache on pressure.
    double ColdMin = 0.20;
    double Grow = 0.30;
    double KeepMaxShare = 0.40;
    double ResizeStep = 0.05;
    ui32 MinHotSlots = 128; // Shared minimum for active tiers
    ui32 MinResizeStep = 256;

    constexpr std::array<ui32, CacheTierCount> HotMinimums(bool keepActive) const noexcept;
};

template <class TTraits>
inline constexpr TCachePolicy SharedCachePolicyFor{};

template <>
inline constexpr TCachePolicy SharedCachePolicyFor<TTestTraits>{
    .MinCurrentLimitGap = 0,
    .MinHotSlots = 15, // Scaled geometry for small native fixtures
    .MinResizeStep = 1,
};

// Inline implementations

inline constexpr std::array<ui32, CacheTierCount> TCachePolicy::HotMinimums(bool keepActive) const noexcept {
    if (!keepActive) {
        return { MinHotSlots, HotLevelCount };
    }
    const ui32 keep = Max<ui32>(HotLevelCount, MinHotSlots / CacheTierCount);
    return { Max<ui32>(HotLevelCount, MinHotSlots - keep), keep };
}

inline void TTestTraits::Invoke(ECacheHookPoint point, TCacheItem cacheItem) const noexcept {
    if (Function) {
        Function(Context, point, cacheItem);
    }
}

} // namespace NKikimr::NSharedCache
