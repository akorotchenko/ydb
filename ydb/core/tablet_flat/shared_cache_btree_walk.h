#pragma once

#include "flat_bio_eggs.h"
#include "shared_cache_events.h"

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/array_ref.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NSharedCache {

class TCollection;

inline constexpr size_t MaxPreloadBatchLocations = 1024;

using TPendingInMemoryPages = THashMap<TLogoBlobID, TSet<NTable::NPage::TPageLocation>>;

// One B-tree being walked by the cache itself, one index level at a time in write order.
struct TCacheBTreeWalk {
    TActorId Owner;
    NSharedCache::TEvAttach::TBtreeSeed Seed;

    TVector<TPageLocation> CurrentLevel;
    TVector<TPageLocation> NextLevel;
    TVector<TPageLocation> PendingDataPages;
    TVector<TPageLocation> DataPageBatch;
    ui64 DataPageBatchBytes = 0;
    size_t Next = 0;
    size_t BatchEnd = 0;
    ui32 Level = 0;
    TLogoBlobID NotifyCollectionId;
    TVector<TPageLocation> PagesToNotify;
    bool Started = false;
    bool Done = false;
    bool Invalid = false;
};

enum class EWalkRunState {
    Walking,
    Draining,
    Cancelled,
};

enum class EWalkControllerState {
    Idle,
    Pending,
    Blocked,
};

struct TWalkRun {
    ui32 FetchesInFlight = 0;
    EWalkRunState State = EWalkRunState::Walking;
    bool NeedsAdvance = true;
    TVector<TCacheBTreeWalk> Walks;
};

struct TWalkCollectionState {
    TMap<TActorId, TVector<TEvAttach::TBtreeSeed>> SeedsByOwner;
    TLogoBlobID IndexCollectionId;
    ui64 IndexOnlySeedCount = 0;
    std::optional<TWalkRun> Run;
    EWalkControllerState ControllerState = EWalkControllerState::Idle;
};

class ICacheBTreeWalkHost {
public:
    virtual ~ICacheBTreeWalkHost() = default;

    virtual TCollection* FindCollection(const TLogoBlobID& id) = 0;
    virtual TPendingInMemoryPages& PendingPages() = 0;
    virtual NActors::TSharedData FindPage(TCollection& collection, TPageOffset offset) = 0;
    // Regular-mode index reads; in-memory index reads go through PendingPages.
    virtual void FetchIndexLevel(TCollection& collection, TVector<NTable::NPage::TPageLocation>&& locations,
        const TLogoBlobID& walkCollectionId) = 0;
    virtual void RequestStickyPages(
        TCollection& collection, const TActorId& owner, const TVector<NTable::NPage::TPageLocation>& locations) = 0;
    virtual void CancelQueuedRequestsAndPump(const TLogoBlobID& walkCollectionId) = 0;
    virtual void TryDropExpiredCollection(TCollection& collection) = 0;
    virtual void ScheduleContinuation() = 0;
};

class TCacheBTreeWalkController {
public:
    explicit TCacheBTreeWalkController(ICacheBTreeWalkHost& host)
        : Host(host)
    {}

    void UpdateSeeds(TCollection& collection, const TActorId& owner, TVector<TEvAttach::TBtreeSeed> seeds,
        bool replayStickyWalk = false);

    bool HasActiveWalks() const {
        // Include cancelled runs waiting for dispatched I/O.
        return ActiveRunCount != 0;
    }

    // Any path making a waiting/yielded run runnable must set its NeedsAdvance and the common AdvanceNeeded.
    void Advance();
    void IndexPagesChanged(const TLogoBlobID& collectionId);
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

    Y_FORCE_INLINE bool IsIdle(const TLogoBlobID& collectionId) const {
        const auto* state = States.FindPtr(collectionId);
        // Keep the collection alive while its index association exists, or WalkCollectionsByIndex would dangle.
        return !state || (!state->Run && !state->IndexCollectionId);
    }

    Y_FORCE_INLINE void EraseCollection(const TLogoBlobID& collectionId) {
        Y_DEBUG_ABORT_UNLESS(IsIdle(collectionId));
        States.erase(collectionId);
    }

private:
    enum class EBatchResult {
        Flushed,
        Queued,
    };

    Y_FORCE_INLINE TWalkCollectionState& State(const TLogoBlobID& collectionId) {
        return States[collectionId];
    }

    void UpdateWalkIndex(TCollection& collection);

    Y_FORCE_INLINE TVector<TLogoBlobID> GetWalkCollections(const TLogoBlobID& indexCollectionId) const {
        if (const auto* walkCollections = WalkCollectionsByIndex.FindPtr(indexCollectionId)) {
            return *walkCollections;
        }
        return {};
    }

    void StartWalkRun(TCollection& collection);
    void FinishWalkRun(TCollection& collection);
    bool FinishWalkRunIfDrained(TCollection& collection);
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
    void HandOverIndexLevel(TCacheBTreeWalk& walk, TArrayRef<const NTable::NPage::TPageLocation> locations, ui32 level);
    EBatchResult FlushDataPageBatch(TCacheBTreeWalk& walk, TCollection& dataCollection);
    bool AddNodeDataPagesToBatch(TCacheBTreeWalk& walk, TCollection& dataCollection);
    void AppendPageToNotify(
        TCacheBTreeWalk& walk, const TLogoBlobID& collectionId, const NTable::NPage::TPageLocation& location);
    void FlushPagesToNotify(TCacheBTreeWalk& walk);

private:
    ICacheBTreeWalkHost& Host;
    // Erase this entry whenever the matching collection leaves the actor's Collections map.
    THashMap<TLogoBlobID, TWalkCollectionState> States;
    THashSet<TLogoBlobID> WalkRunsInProgress;
    // Only recheck completion after a run stops walking or its pending queue drains.
    THashSet<TLogoBlobID> RunsToFinish;
    // Waiting for I/O or cache capacity does not make an ordinary actor message useful to the walk.
    bool AdvanceNeeded = false;
    THashMap<TLogoBlobID, TVector<TLogoBlobID>> WalkCollectionsByIndex;
    ui64 ActiveRunCount = 0;
    // A queued data-page batch yields; another actor turn must continue discovery.
    bool ContinuationNeeded = false;
    static constexpr ui64 MaxWalkBatchBytes = NTabletFlatExecutor::NBlockIO::BlockSize;
};

} // namespace NKikimr::NSharedCache
