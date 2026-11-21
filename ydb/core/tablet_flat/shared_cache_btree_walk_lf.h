#pragma once

#include "flat_bio_eggs.h"
#include "shared_cache_btree_walk.h"
#include "shared_cache_events_lf.h"

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/array_ref.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NKikimr::NSharedCache {

class TCollection;

inline constexpr size_t MaxPreloadBatchLocations = 1024;

class ICacheBTreeWalkHost {
public:
    virtual ~ICacheBTreeWalkHost() = default;

    virtual TCollection* FindCollection(const TLogoBlobID& id) = 0;
    virtual TPendingInMemoryPages& PendingPages() = 0;
    virtual TCache::TOperation BeginOperation() = 0;
    virtual NActors::TSharedData FindPage(
        TCacheOperationView<TProdTraits>& cacheOp, TCollection& collection, TPageOffset offset) = 0;
    // Regular-mode index reads; in-memory index reads go through PendingPages.
    virtual void FetchIndexLevel(TCollection& collection, TVector<NTable::NPage::TPageLocation>&& locations,
        const TLogoBlobID& walkCollectionId) = 0;
    virtual void RequestStickyPages(
        TCollection& collection, const TActorId& owner, const TVector<NTable::NPage::TPageLocation>& locations) = 0;
    virtual void CancelQueuedRequestsAndPump(const TLogoBlobID& walkCollectionId) = 0;
    virtual void TryDropExpiredCollection(TCollection& collection) = 0;
    virtual void ScheduleContinuation() = 0;
};

struct TLFWalkCollectionState : TWalkCollectionState {
    ui64 IndexOnlySeedCount = 0;
};

class TCacheBTreeWalkController : private TWalkScheduler<TLFWalkCollectionState> {
public:
    using TWalkScheduler::EraseCollection;
    using TWalkScheduler::HasActiveWalks;
    using TWalkScheduler::IndexPagesChanged;
    using TWalkScheduler::IsIdle;

    explicit TCacheBTreeWalkController(ICacheBTreeWalkHost& host)
        : Host(host)
    {}

    void UpdateSeeds(TCollection& collection, const TActorId& owner, TVector<TEvAttach::TBtreeSeed> seeds,
        bool replayStickyWalk = false);

    // Any path making a waiting/yielded run runnable must set its NeedsAdvance and the common AdvanceNeeded.
    void Advance();
    void FinishReady();
    void FinishFetch(const TLogoBlobID& walkCollectionId);

    Y_FORCE_INLINE void FetchStarted(const TLogoBlobID& walkCollectionId) {
        auto* collection = Host.FindCollection(walkCollectionId);
        Y_ENSURE(collection && State(collection->Id()).Run &&
                 State(collection->Id()).Run->State != EWalkRunState::Cancelled);
        ++State(collection->Id()).Run->FetchesInFlight;
    }

    Y_FORCE_INLINE void InvalidateRun(const TLogoBlobID& walkCollectionId) {
        auto* collection = Host.FindCollection(walkCollectionId);
        Y_ENSURE(collection);
        InvalidateWalkRun(*collection);
    }

    void InvalidateDataCollection(const TLogoBlobID& collectionId);
    void InvalidateIndexCollection(const TLogoBlobID& collectionId);
    void DropForIndexCollection(const TLogoBlobID& indexCollectionId);
    void RestartForIndexCollection(const TLogoBlobID& indexCollectionId);
    void DropIndexOnlyWalks(const TLogoBlobID& indexCollectionId);

private:
    enum class EBatchResult {
        Flushed,
        Queued,
    };

    void FinishWalkRunIfDrained(TLogoBlobID collectionId);
    void CancelWalkRun(TCollection& collection);

    Y_FORCE_INLINE void RestartWalkRun(TCollection& collection) {
        State(collection.Id()).ControllerState = EWalkControllerState::Pending;
        CancelWalkRun(collection);
    }

    Y_FORCE_INLINE void InvalidateWalkRun(TCollection& collection) {
        State(collection.Id()).ControllerState = EWalkControllerState::Blocked;
        CancelWalkRun(collection);
    }

    bool QueueInMemoryPages(TCollection& collection, TArrayRef<const TPageLocation> locations);
    void AdvanceWalk(TCacheBTreeWalk& walk, const TLogoBlobID& walkCollectionId);
    bool AdvanceIndexPage(TCacheBTreeWalk& walk, TCollection& indexCollection, const TLogoBlobID& walkCollectionId,
        TPageLocation location);
    void HandOverIndexLevel(TCacheBTreeWalk& walk, TArrayRef<const NTable::NPage::TPageLocation> locations, ui32 level);
    EBatchResult FlushDataPageBatch(TCacheBTreeWalk& walk, TCollection& dataCollection);
    bool AddNodeDataPagesToBatch(TCacheBTreeWalk& walk, TCollection& dataCollection);
    void AppendPageToNotify(
        TCacheBTreeWalk& walk, const TLogoBlobID& collectionId, const NTable::NPage::TPageLocation& location);
    void FlushPagesToNotify(TCacheBTreeWalk& walk);

private:
    ICacheBTreeWalkHost& Host;
};

} // namespace NKikimr::NSharedCache
