#pragma once

#include "shared_cache_collection_load.h"

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NSharedCache {

// Temporary work only; these limits do not change canonical metadata accounting
struct TCollectionLoadLimits {
    ui64 Bytes = 512_MB; // Normal serialized-byte allowance; one known job may exceed it
    ui32 Count = 64;
};

class TCollectionLoadController {
public:
    TCollectionLoadController(TSharedCache& cache, const TActorId& owner, TCollectionLoadLimits limits = {});

    void Request(const TActorId& sender, ui64 cookie, const TEvCollectionRequest& message);
    void Update(const TCollectionRequest& request);
    void Ready(const TLogoBlobID& id);
    void RawReady(const TActorId& sender, const TEvCollectionRawReady& result);
    void Data(const TActorId& sender, TEvCollectionData& result);

    // The owner samples AdmissionGeneration before Pump and arms the union of both wait queues
    void Pump();
    void Stop();

    bool HasResourceWaits() const noexcept {
        return !ResourceWaits_.empty();
    }

private:
    struct TWaiter {
        TIntrusivePtr<TCollectionRequest> Request;
        TActorId Sender;
        ui64 Cookie;
    };

    struct TLoad {
        ui64 Id = 0;
        NPageCollection::TLargeGlobId Descriptor;
        EPriority Priority = EPriority::Fast;
        ECollectionLoadKind Kind = ECollectionLoadKind::Unknown;
        TVector<TWaiter> Waiters;
        TActorId Worker;
        TCacheCollectionRef Canonical;
        bool Queued = false;
        bool Retained = false;
    };

    struct TJob {
        ui64 LoadId;
        TLogoBlobID Key;
        ui64 Bytes;
        bool Known;
        bool Raw = false;
        bool Parsing = false;
        bool Terminal = false;
        bool Revoked = false;
        THolder<TCollection> Candidate;
    };

    struct TQueuedLoad {
        TLogoBlobID Key;
        ui64 LoadId;
    };

    void Queue(TLoad& load);
    void Queue(const TLogoBlobID& id);
    void WakeWorkWaits();
    void WakeParserWaits();
    void Advance(TLoad& load);
    bool CheckWaiters(TLoad& load);
    void Deliver(TLoad& load);
    void Retain(TLoad& load);
    void Forget(const TLoad& load, const TWaiter& waiter);
    void Fail(TLoad& load, NKikimrProto::EReplyStatus status, const TString& error);
    void Reply(TWaiter& waiter, NKikimrProto::EReplyStatus status, TCacheCollectionRef ref, const TString& error);
    void Erase(TLoad& load);
    bool StartJob(TLoad& load);
    void GrantParser(TLoad& load, TJob& job);
    void StopJob(TLoad& load);
    void ReleaseJob(const TActorId& worker);
    void FinishParsing(TJob& job);
    TLoad* Current(const TActorId& worker, const TJob& job);
    TJob* Job(const TActorId& worker, ui64 loadId);

private:
    TSharedCache& Cache_;
    const TActorId Owner_;
    const TCollectionLoadLimits Limits_;
    THashMap<TLogoBlobID, TLoad> Loads_;
    // Completed snapshots awaiting role confirmation cannot accept new requests
    THashMap<ui64, TLoad> Retained_;
    THashMap<const TCollectionRequest*, ui64> RetainedRequests_;
    // Revoked workers remain here until their physical terminal outcome arrives
    THashMap<TActorId, TJob> Jobs_;
    TDeque<TQueuedLoad> Runnable_;
    THashSet<TLogoBlobID> ResourceWaits_;
    THashSet<TLogoBlobID> WorkWaits_;
    THashSet<TLogoBlobID> ParserWaits_;
    ui64 NextLoadId_ = 1;
    ui64 ChargedBytes_ = 0;
    ui64 UnknownBytes_ = 0;
    ui32 KnownJobs_ = 0;
    ui32 Parsers_ = 0;
};

} // namespace NKikimr::NSharedCache
