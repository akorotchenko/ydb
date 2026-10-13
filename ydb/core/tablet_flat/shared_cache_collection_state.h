#pragma once

#include "flat_page_iface.h"

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NKikimr::NSharedCache {

struct TCoreKeepPreloadRequest {
    ui64 Generation = 0;
    ui64 ChargedBytes = 0;
    TVector<NTable::NPage::TPageLocation> Locations;
    bool Admitted = false;
};

// Only the shared-cache actor accesses this optional collection state.
struct TCollectionActorState {
    TSet<TActorId> InMemoryOwners;
    TSet<TActorId> StickyOwners;
    TSet<TActorId> Owners;
    THashMap<ui64, TCoreKeepPreloadRequest> KeepPreloadRequests;
    ui64 PendingRequests = 0;
    ui64 ReportedResidentBytes = 0;
};

} // namespace NKikimr::NSharedCache
