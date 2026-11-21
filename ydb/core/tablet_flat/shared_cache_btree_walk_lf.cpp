#include "flat_page_btree_index.h"
#include "shared_cache_btree_walk_lf.h"
#include "cache.h"
#include "cache_collection.h"

#include <util/generic/algorithm.h>

#include <utility>

namespace NKikimr::NSharedCache {

void TCacheBTreeWalkController::FinishWalkRunIfDrained(TLogoBlobID collectionId) {
    if (FinishDrainedRun(collectionId)) {
        if (auto* current = Host.FindCollection(collectionId)) {
            Host.TryDropExpiredCollection(*current);
        }
    }
}

void TCacheBTreeWalkController::CancelWalkRun(TCollection& collection) {
    const TLogoBlobID collectionId = collection.Id();
    if (CancelRun(collectionId, [&] {
            Host.CancelQueuedRequestsAndPump(collectionId);
        })) {
        FinishWalkRunIfDrained(collectionId);
    }
}

void TCacheBTreeWalkController::UpdateSeeds(TCollection& collection, const TActorId& owner,
    TVector<NSharedCache::TEvAttach::TBtreeSeed> seeds, bool replayStickyWalk) {
    auto* current = State(collection.Id()).SeedsByOwner.FindPtr(owner);
    // Production seeds are ordered current then historic, with at most two per owner.
    const bool same = current ? *current == seeds : seeds.empty();
    const bool unblock = State(collection.Id()).ControllerState == EWalkControllerState::Blocked;
    if (same && !unblock && !replayStickyWalk) {
        return;
    }

    const auto countIndexOnlySeeds = [](const auto& ownerSeeds) {
        return CountIf(ownerSeeds, [](const auto& seed) {
            return !seed.QueueDataPages && !seed.Sticky && !seed.IndexCollectionSticky;
        });
    };
    auto& state = State(collection.Id());
    // Only this owner's old and new seeds are counted, at most two production seeds each.
    const ui64 oldIndexOnlySeeds = current ? countIndexOnlySeeds(*current) : 0;
    Y_ENSURE(state.IndexOnlySeedCount >= oldIndexOnlySeeds);
    state.IndexOnlySeedCount -= oldIndexOnlySeeds;
    state.IndexOnlySeedCount += countIndexOnlySeeds(seeds);

    if (seeds) {
        State(collection.Id()).SeedsByOwner[owner] = std::move(seeds);
    } else {
        State(collection.Id()).SeedsByOwner.erase(owner);
    }
    State(collection.Id()).ControllerState = EWalkControllerState::Pending;
    UpdateWalkIndex(collection.Id());
    CancelWalkRun(collection);
}

void TCacheBTreeWalkController::FinishFetch(const TLogoBlobID& walkCollectionId) {
    if (!walkCollectionId) {
        return;
    }

    // A run outlives every fetch it starts; the last arriving fetch may finish the run.
    auto* collection = Host.FindCollection(walkCollectionId);
    Y_ENSURE(collection && State(collection->Id()).Run);
    auto& run = *State(collection->Id()).Run;
    Y_ENSURE(run.FetchesInFlight > 0);
    --run.FetchesInFlight;

    FinishWalkRunIfDrained(collection->Id());
}

void TCacheBTreeWalkController::InvalidateDataCollection(const TLogoBlobID& collectionId) {
    if (auto* collection = Host.FindCollection(collectionId)) {
        if (HasWalkRun(collectionId)) {
            InvalidateWalkRun(*collection);
        }
    }
}

void TCacheBTreeWalkController::InvalidateIndexCollection(const TLogoBlobID& collectionId) {
    for (const TLogoBlobID& id : GetWalkCollections(collectionId)) {
        InvalidateDataCollection(id);
    }
}

void TCacheBTreeWalkController::AdvanceWalk(TCacheBTreeWalk& walk, const TLogoBlobID& walkCollectionId) {
    auto* indexCollection = Host.FindCollection(walk.Seed.IndexCollectionId);
    auto* dataCollection = Host.FindCollection(walk.Seed.DataCollectionId);
    if (!indexCollection || !indexCollection->PageCollection() || !dataCollection) {
        // Only an attach creates a walk, so a missing collection means the part is gone.
        walk.Invalid = true;
        walk.Done = true;
        return;
    }

    if (!walk.Started) {
        walk.Started = true;
        walk.CurrentLevel.push_back(walk.Seed.Root);
        HandOverIndexLevel(walk, walk.CurrentLevel, walk.Level);
    }

    walk.AdvanceLevels(
        [&] {
            return AddNodeDataPagesToBatch(walk, *dataCollection);
        },
        [&](const TPageLocation& location) {
            return AdvanceIndexPage(walk, *indexCollection, walkCollectionId, location);
        },
        [&] {
            if (FlushDataPageBatch(walk, *dataCollection) != EBatchResult::Flushed) {
                return false;
            }
            FlushPagesToNotify(walk);
            return true;
        });
}

bool TCacheBTreeWalkController::AdvanceIndexPage(
    TCacheBTreeWalk& walk, TCollection& indexCollection, const TLogoBlobID& walkCollectionId, TPageLocation location) {
    NActors::TSharedData coreData;
    TVector<TPageLocation> toRequest;
    {
        auto cacheOp = Host.BeginOperation();
        coreData = Host.FindPage(cacheOp, indexCollection, location.Offset);
        if (!coreData) {
            for (size_t i = walk.Next; i < walk.BatchEnd; ++i) {
                const auto& batchLocation = walk.CurrentLevel[i];
                if (!Host.FindPage(cacheOp, indexCollection, batchLocation.Offset)) {
                    toRequest.push_back(batchLocation);
                }
            }
        }
    }

    if (!coreData) {
        // Index pages always live in the main group; its mode decides how they are fetched.
        if (indexCollection.GetCacheMode() == ECacheMode::TryKeepInMemory) {
            // Wait for loader progress; a self-continuation cannot unblock a capacity-limited index read.
            QueueInMemoryPages(indexCollection, toRequest);
        } else {
            Host.FetchIndexLevel(indexCollection, std::move(toRequest), walkCollectionId);
        }
        return false; // wait for the pages to arrive, the next drive continues the walk
    }

    ++walk.Next;

    const bool childrenAreData = (walk.Level + 1 >= walk.Seed.LevelCount);
    if (childrenAreData && !walk.Seed.QueueDataPages && !walk.Seed.Sticky) {
        // This walk only keeps the index level resident, its data pages are of no use.
        return true;
    }

    NTable::NPage::TBtreeIndexNode node(coreData, /*v2Format=*/true);

    walk.AcceptChildren(node, childrenAreData, [&](TArrayRef<const TPageLocation> children, ui32 level) {
        HandOverIndexLevel(walk, children, level);
    });
    return true;
}

void TCacheBTreeWalkController::HandOverIndexLevel(
    TCacheBTreeWalk& walk, TArrayRef<const TPageLocation> locations, ui32 level) {
    if ((!walk.Seed.Sticky && !walk.Seed.IndexCollectionSticky) || level >= walk.Seed.LevelCount) {
        return; // not sticky, or these are the data pages
    }

    for (const auto& location : locations) {
        AppendPageToNotify(walk, walk.Seed.IndexCollectionId, location);
    }
}

bool TCacheBTreeWalkController::QueueInMemoryPages(TCollection& collection, TArrayRef<const TPageLocation> locations) {
    auto& queue = Host.PendingPages()[collection.Id()];
    bool queued = false;
    for (const auto& location : locations) {
        if (queue.emplace(location).second) {
            queued = true;
        }
    }
    return queued;
}

TCacheBTreeWalkController::EBatchResult TCacheBTreeWalkController::FlushDataPageBatch(
    TCacheBTreeWalk& walk, TCollection& dataCollection) {
    if (!walk.DataPageBatch) {
        return EBatchResult::Flushed;
    }

    const bool queueDataPages =
        walk.Seed.QueueDataPages && dataCollection.GetCacheMode() == ECacheMode::TryKeepInMemory;
    // Like V1's mode-switch scan, keep discovering locations while the loader is out of cache budget.
    const bool queued = queueDataPages && QueueInMemoryPages(dataCollection, walk.DataPageBatch);

    if (walk.Seed.Sticky) {
        // Index notifications are parents-first; flush them before handing over data pages.
        FlushPagesToNotify(walk);
        Host.RequestStickyPages(dataCollection, walk.Owner, walk.DataPageBatch);
    }

    walk.DataPageBatch.clear();
    walk.DataPageBatchBytes = 0;
    if (queued) {
        ContinueWalk(dataCollection.Id());
    }
    return queued ? EBatchResult::Queued : EBatchResult::Flushed;
}

bool TCacheBTreeWalkController::AddNodeDataPagesToBatch(TCacheBTreeWalk& walk, TCollection& dataCollection) {
    const bool queueDataPages =
        walk.Seed.QueueDataPages && dataCollection.GetCacheMode() == ECacheMode::TryKeepInMemory;
    return walk.AppendNodeDataPages(queueDataPages, [&] {
        return FlushDataPageBatch(walk, dataCollection) == EBatchResult::Flushed;
    });
}

void TCacheBTreeWalkController::AppendPageToNotify(
    TCacheBTreeWalk& walk, const TLogoBlobID& collectionId, const TPageLocation& location) {
    if (walk.NotifyCollectionId != collectionId) {
        FlushPagesToNotify(walk);
        walk.NotifyCollectionId = collectionId;
    }

    walk.PagesToNotify.push_back(location);
    if (walk.PagesToNotify.size() >= MaxPreloadBatchLocations) {
        FlushPagesToNotify(walk);
    }
}

void TCacheBTreeWalkController::FlushPagesToNotify(TCacheBTreeWalk& walk) {
    if (walk.PagesToNotify.empty()) {
        return;
    }

    if (auto* collection = Host.FindCollection(walk.NotifyCollectionId)) {
        Host.RequestStickyPages(*collection, walk.Owner, walk.PagesToNotify);
    }
    walk.PagesToNotify.clear();
    walk.NotifyCollectionId = TLogoBlobID();
}

void TCacheBTreeWalkController::Advance() {
    const bool schedule = AdvanceRuns(
        [&](const TLogoBlobID& id) {
            auto* collection = Host.FindCollection(id);
            Y_ENSURE(collection && State(collection->Id()).Run);
            return State(collection->Id()).Run->Advance([&](TCacheBTreeWalk& walk) {
                AdvanceWalk(walk, collection->Id());
            });
        },
        [&](const TLogoBlobID& id) {
            if (auto* collection = Host.FindCollection(id)) {
                InvalidateWalkRun(*collection);
            }
        });
    if (schedule) {
        Host.ScheduleContinuation();
    }
}

void TCacheBTreeWalkController::FinishReady() {
    FinishReadyRuns([&](const TLogoBlobID& id) {
        if (auto* collection = Host.FindCollection(id)) {
            FinishWalkRunIfDrained(collection->Id());
        }
    });
}

void TCacheBTreeWalkController::DropIndexOnlyWalks(const TLogoBlobID& indexCollectionId) {
    for (const TLogoBlobID& id : GetWalkCollections(indexCollectionId)) {
        auto* collection = Host.FindCollection(id);
        Y_ENSURE(collection);
        if (State(collection->Id()).IndexOnlySeedCount != 0) {
            InvalidateWalkRun(*collection);
        }
    }
}

void TCacheBTreeWalkController::DropForIndexCollection(const TLogoBlobID& indexCollectionId) {
    for (const TLogoBlobID& id : GetWalkCollections(indexCollectionId)) {
        auto* collection = Host.FindCollection(id);
        Y_ENSURE(collection);
        InvalidateWalkRun(*collection);
    }
}

void TCacheBTreeWalkController::RestartForIndexCollection(const TLogoBlobID& indexCollectionId) {
    for (const TLogoBlobID& id : GetWalkCollections(indexCollectionId)) {
        auto* collection = Host.FindCollection(id);
        Y_ENSURE(collection);
        RestartWalkRun(*collection);
    }
}
} // namespace NKikimr::NSharedCache
