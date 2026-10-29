#include "shared_cache_collections.h"
#include "flat_sausage_packet.h"

#include <ydb/library/actors/core/actor.h>

#include <util/generic/algorithm.h>

#include <utility>

namespace NKikimr::NSharedCache {
namespace {

    void Send(const TActorId& owner, const TActorId& recipient, IEventBase* event, ui64 cookie = 0) {
        NActors::TActivationContext::Send(new NActors::IEventHandle(recipient, owner, event, 0, cookie));
    }

    ECollectionLoadKind CollectionKind(const NPageCollection::IPageCollection& collection) {
        return dynamic_cast<const NPageCollection::TOuterPageCollection*>(&collection) ? ECollectionLoadKind::Outer
                                                                                       : ECollectionLoadKind::Regular;
    }

} // anonymous namespace

bool TCollectionRequest::Resolve(ECollectionLoadKind kind) noexcept {
    if (kind != ECollectionLoadKind::Regular && kind != ECollectionLoadKind::Outer) {
        return false;
    }
    auto expected = ECollectionLoadKind::Unknown;
    return Kind_.compare_exchange_strong(expected, kind, std::memory_order_acq_rel, std::memory_order_acquire) ||
           expected == kind;
}

bool TCollectionRequest::TryComplete() noexcept {
    return !Cancelled_.load(std::memory_order_acquire) && !Completed_.exchange(true, std::memory_order_acq_rel);
}

TCollectionLoadController::TCollectionLoadController(
    TSharedCache& cache, const TActorId& owner, TCollectionLoadLimits limits)
    : Cache_(cache)
    , Owner_(owner)
    , Limits_(limits)
{
    Y_ENSURE(Limits_.Count >= 2 && Limits_.Bytes > 0, "Invalid collection-load work limits");
}

void TCollectionLoadController::Request(const TActorId& sender, ui64 cookie, const TEvCollectionRequest& message) {
    Y_ENSURE(message.Request);
    auto& request = *message.Request;
    if (!request.IsActive() || std::exchange(request.Joined_, true)) {
        return;
    }
    TWaiter waiter{ message.Request, sender, cookie };
    const auto& descriptor = request.Descriptor;
    if (!descriptor || !descriptor.Lead || !descriptor.Lead.BlobSize() ||
        descriptor.Bytes < descriptor.Lead.BlobSize() || descriptor.Bytes > Max<ui32>() ||
        descriptor.Group == NPageCollection::TLargeGlobId::InvalidGroup) {
        Reply(waiter, NKikimrProto::ERROR, {}, "Invalid collection metadata descriptor");
        return;
    }
    auto [it, inserted] = Loads_.try_emplace(request.Descriptor.Lead);
    auto& load = it->second;
    if (inserted) {
        load.Id = NextLoadId_++;
        load.Descriptor = request.Descriptor;
        load.Priority = message.Priority;
    } else if (!(load.Descriptor == request.Descriptor)) {
        Reply(waiter, NKikimrProto::ERROR, {}, "Conflicting collection metadata descriptor");
        return;
    }
    load.Waiters.push_back(std::move(waiter));
    Queue(load);
}

void TCollectionLoadController::Update(const TCollectionRequest& request) {
    if (const auto* id = RetainedRequests_.FindPtr(&request)) {
        if (auto* load = Retained_.FindPtr(*id)) {
            Queue(*load);
        }
        return;
    }
    auto* load = Loads_.FindPtr(request.Descriptor.Lead);
    if (!load) {
        return;
    }
    for (const auto& waiter : load->Waiters) {
        if (waiter.Request.Get() == &request) {
            Queue(*load);
            return;
        }
    }
}

void TCollectionLoadController::CancelRequests(const TActorId& owner, const TLogoBlobID& collectionId) {
    const auto cancel = [&](auto& loads) {
        for (auto& [_, load] : loads) {
            if (collectionId && load.Descriptor.Lead != collectionId) {
                continue;
            }
            bool matched = false;
            for (auto& waiter : load.Waiters) {
                if (waiter.Sender == owner) {
                    Reply(waiter, NKikimrProto::RACE, {}, "Collection metadata request cancelled by owner");
                    matched = true;
                }
            }
            if (matched) {
                Queue(load);
            }
        }
    };
    cancel(Loads_);
    cancel(Retained_);
}

void TCollectionLoadController::Ready(const TLogoBlobID& id) {
    Queue(id);
}

void TCollectionLoadController::Queue(TLoad& load) {
    if (!std::exchange(load.Queued, true)) {
        Runnable_.push_back({ load.Descriptor.Lead, load.Id });
    }
}

void TCollectionLoadController::Queue(const TLogoBlobID& id) {
    if (auto* load = Loads_.FindPtr(id)) {
        Queue(*load);
    }
}

void TCollectionLoadController::WakeWorkWaits() {
    auto waiting = std::exchange(WorkWaits_, {});
    for (bool known : { true, false }) {
        for (const auto& id : waiting) {
            if (auto* load = Loads_.FindPtr(id); load && (load->Kind != ECollectionLoadKind::Unknown) == known) {
                Queue(*load);
            }
        }
    }
}

void TCollectionLoadController::WakeParserWaits() {
    auto waiting = std::exchange(ParserWaits_, {});
    for (const auto& id : waiting) {
        Queue(id);
    }
}

void TCollectionLoadController::Pump() {
    auto waiting = std::exchange(ResourceWaits_, {});
    for (const auto& id : waiting) {
        Queue(id);
    }
    while (!Runnable_.empty()) {
        const auto next = Runnable_.front();
        Runnable_.pop_front();
        auto* load = Loads_.FindPtr(next.Key);
        if (!load || load->Id != next.LoadId) {
            load = Retained_.FindPtr(next.LoadId);
        }
        if (load) {
            load->Queued = false;
            Advance(*load);
        }
    }
}

bool TCollectionLoadController::CheckWaiters(TLoad& load) {
    EraseIf(load.Waiters, [&](TWaiter& waiter) {
        if (!waiter.Request->IsActive()) {
            Forget(load, waiter);
            return true;
        }
        const auto kind = waiter.Request->Kind();
        if (kind == ECollectionLoadKind::Unknown) {
            return false;
        }
        if (load.Kind == ECollectionLoadKind::Unknown) {
            load.Kind = kind;
        } else if (kind != load.Kind) {
            Reply(waiter, NKikimrProto::ERROR, {}, "Conflicting collection metadata representation");
            Forget(load, waiter);
            return true;
        }
        return false;
    });
    if (load.Waiters.empty()) {
        Erase(load);
        return false;
    }
    return true;
}

void TCollectionLoadController::Advance(TLoad& load) {
    if (load.Retained) {
        if (CheckWaiters(load)) {
            Deliver(load);
        }
        return;
    }
    EraseIf(load.Waiters, [](const TWaiter& waiter) {
        return !waiter.Request->IsActive();
    });
    if (load.Waiters.empty()) {
        Erase(load);
        return;
    }
    if (!load.Canonical) {
        switch (Cache_.FindCollection(load.Descriptor.Lead, load.Canonical)) {
            case ECacheCollectionLookupStatus::Pending:
                ResourceWaits_.insert(load.Descriptor.Lead);
                return;
            case ECacheCollectionLookupStatus::Miss:
                break;
            case ECacheCollectionLookupStatus::Hit:
                break;
        }
    }
    if (load.Canonical) {
        const auto& collection = *load.Canonical->PageCollection();
        const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(&collection);
        if (!packet || !(packet->LargeGlobId == load.Descriptor)) {
            Fail(load, NKikimrProto::ERROR, "Canonical collection metadata descriptor does not match");
            return;
        }
        load.Kind = CollectionKind(collection);
        // Keep the native owner before revoking work or closing any request
        StopJob(load);
        if (CheckWaiters(load)) {
            Deliver(load);
        }
        return;
    }
    if (!CheckWaiters(load)) {
        return;
    }
    if (!load.Worker && !StartJob(load)) {
        return;
    }
    auto& job = Jobs_.at(load.Worker);
    if (!job.Terminal) {
        GrantParser(load, job);
        return;
    }

    TCollectionCacheItem inserted;
    TCacheCollectionRef canonical;
    const TCollectionLocation location{ .Id = load.Descriptor.Lead,
        .BackingSize = job.Candidate->PageCollection()->BackingSize() };
    switch (Cache_.FindOrInsert(location, inserted, canonical)) {
        case ECacheResultStatus::Inserted: {
            // The holder was allocated before inserting the short-lived native publisher
            Y_ENSURE(Cache_.MakeReady(inserted, std::move(job.Candidate)));
            const auto worker = std::exchange(load.Worker, {});
            ReleaseJob(worker);
            Queue(load);
            return;
        }
        case ECacheResultStatus::Hit:
            load.Canonical = std::move(canonical);
            Queue(load);
            return;
        case ECacheResultStatus::Pending:
            // Reclassify failed attachment versus an active native publisher without parking blindly
            Queue(load);
            return;
        case ECacheResultStatus::Miss:
            ResourceWaits_.insert(load.Descriptor.Lead);
            return;
    }
}

void TCollectionLoadController::Reply(
    TWaiter& waiter, NKikimrProto::EReplyStatus status, TCacheCollectionRef ref, const TString& error) {
    if (waiter.Request->TryComplete()) {
        Send(Owner_, waiter.Sender, new TEvCollectionResult(waiter.Request, status, std::move(ref), error),
            waiter.Cookie);
    }
}

void TCollectionLoadController::Deliver(TLoad& load) {
    TVector<TWaiter> unresolved;
    for (size_t index = 0; index < load.Waiters.size(); ++index) {
        auto& waiter = load.Waiters[index];
        const auto kind = waiter.Request->Kind();
        if (kind == ECollectionLoadKind::Unknown) {
            unresolved.push_back(std::move(waiter));
            continue;
        }
        Forget(load, waiter);
        if (kind != load.Kind) {
            Reply(waiter, NKikimrProto::ERROR, {}, "Conflicting collection metadata representation");
        } else {
            auto ref = index + 1 == load.Waiters.size() && unresolved.empty() ? std::move(load.Canonical)
                                                                              : load.Canonical.Acquire();
            Reply(waiter, NKikimrProto::OK, std::move(ref), {});
        }
    }
    load.Waiters = std::move(unresolved);
    EraseIf(load.Waiters, [&](const TWaiter& waiter) {
        if (waiter.Request->IsActive()) {
            return false;
        }
        Forget(load, waiter);
        return true;
    });
    if (load.Waiters.empty()) {
        Erase(load);
    } else if (!load.Retained) {
        Retain(load);
    }
}

void TCollectionLoadController::Retain(TLoad& load) {
    Y_DEBUG_ABORT_UNLESS(load.Canonical && !load.Worker);
    const auto key = load.Descriptor.Lead;
    const ui64 id = load.Id;
    auto [it, inserted] = Retained_.emplace(id, std::move(load));
    Y_DEBUG_ABORT_UNLESS(inserted);
    auto& retained = it->second;
    retained.Retained = true;
    for (const auto& waiter : retained.Waiters) {
        RetainedRequests_.emplace(waiter.Request.Get(), id);
    }
    ResourceWaits_.erase(key);
    WorkWaits_.erase(key);
    ParserWaits_.erase(key);
    Loads_.erase(key);
}

void TCollectionLoadController::Forget(const TLoad& load, const TWaiter& waiter) {
    if (load.Retained) {
        auto it = RetainedRequests_.find(waiter.Request.Get());
        if (it != RetainedRequests_.end() && it->second == load.Id) {
            RetainedRequests_.erase(it);
        }
    }
}

void TCollectionLoadController::Fail(TLoad& load, NKikimrProto::EReplyStatus status, const TString& error) {
    for (auto& waiter : load.Waiters) {
        Reply(waiter, status, {}, error);
    }
    Erase(load);
}

void TCollectionLoadController::Erase(TLoad& load) {
    const auto id = load.Descriptor.Lead;
    StopJob(load);
    if (load.Retained) {
        for (const auto& waiter : load.Waiters) {
            Forget(load, waiter);
        }
        Retained_.erase(load.Id);
        return;
    }
    const auto it = Loads_.find(id);
    Y_DEBUG_ABORT_UNLESS(it != Loads_.end() && it->second.Id == load.Id);
    ResourceWaits_.erase(id);
    WorkWaits_.erase(id);
    ParserWaits_.erase(id);
    Loads_.erase(it);
}

bool TCollectionLoadController::StartJob(TLoad& load) {
    const ui64 bytes = load.Descriptor.Bytes;
    const bool known = load.Kind != ECollectionLoadKind::Unknown;
    const bool fitsBytes = bytes <= Limits_.Bytes && ChargedBytes_ <= Limits_.Bytes - bytes;
    const bool fitsUnknown = bytes <= Limits_.Bytes / 2 && UnknownBytes_ <= Limits_.Bytes / 2 - bytes;
    if (Jobs_.size() >= Limits_.Count ||
        (!known && (Jobs_.size() >= Limits_.Count - 1 || !fitsUnknown || !fitsBytes)) ||
        (known && !fitsBytes && KnownJobs_)) {
        WorkWaits_.insert(load.Descriptor.Lead);
        return false;
    }
    // Unresolved tails cannot consume the known/root slot or its single oversized-job allowance
    const auto worker = NActors::TActivationContext::Register(
        CreateCollectionLoadActor(Owner_, load.Id, load.Descriptor, load.Priority), Owner_);
    Jobs_.emplace(worker, TJob{ .LoadId = load.Id, .Key = load.Descriptor.Lead, .Bytes = bytes, .Known = known });
    load.Worker = worker;
    ChargedBytes_ += bytes;
    if (known) {
        ++KnownJobs_;
    } else {
        UnknownBytes_ += bytes;
    }
    WorkWaits_.erase(load.Descriptor.Lead);
    return true;
}

void TCollectionLoadController::GrantParser(TLoad& load, TJob& job) {
    if (!job.Raw || job.Parsing || load.Kind == ECollectionLoadKind::Unknown) {
        return;
    }
    if (Parsers_) {
        ParserWaits_.insert(load.Descriptor.Lead);
        return;
    }
    job.Parsing = true;
    ++Parsers_;
    ParserWaits_.erase(load.Descriptor.Lead);
    Send(Owner_, load.Worker, new TEvCollectionParse(load.Kind));
}

TCollectionLoadController::TJob* TCollectionLoadController::Job(const TActorId& worker, ui64 loadId) {
    auto* job = Jobs_.FindPtr(worker);
    return job && job->LoadId == loadId ? job : nullptr;
}

TCollectionLoadController::TLoad* TCollectionLoadController::Current(const TActorId& worker, const TJob& job) {
    auto* load = Loads_.FindPtr(job.Key);
    return load && load->Id == job.LoadId && load->Worker == worker && !job.Revoked ? load : nullptr;
}

void TCollectionLoadController::RawReady(const TActorId& sender, const TEvCollectionRawReady& result) {
    auto* job = Job(sender, result.LoadId);
    if (!job || job->Terminal) {
        return;
    }
    if (auto* load = Current(sender, *job)) {
        Y_ENSURE(result.Bytes == job->Bytes);
        job->Raw = true;
        Queue(*load);
    }
}

void TCollectionLoadController::Data(const TActorId& sender, TEvCollectionData& result) {
    auto* job = Job(sender, result.LoadId);
    if (!job || job->Terminal) {
        return;
    }
    job->Terminal = true;
    FinishParsing(*job);
    if (auto* load = Current(sender, *job)) {
        if (result.Status != NKikimrProto::OK) {
            // Existing metadata readers treat storage failures as terminal for this load
            Fail(*load, result.Status, result.Error);
            return;
        }
        // Allocate before any native Begin can become visible
        job->Candidate =
            MakeHolder<TCollection>(std::move(result.PageCollection), TCollectionCacheItem{}, ECacheMode::Regular);
        Queue(*load);
    } else {
        result.PageCollection.Reset();
        ReleaseJob(sender);
    }
}

void TCollectionLoadController::FinishParsing(TJob& job) {
    if (std::exchange(job.Parsing, false)) {
        Y_DEBUG_ABORT_UNLESS(Parsers_ == 1);
        --Parsers_;
        WakeParserWaits();
    }
}

void TCollectionLoadController::ReleaseJob(const TActorId& worker) {
    auto it = Jobs_.find(worker);
    Y_DEBUG_ABORT_UNLESS(it != Jobs_.end() && it->second.Terminal);
    auto& job = it->second;
    FinishParsing(job);
    ChargedBytes_ -= job.Bytes;
    if (job.Known) {
        --KnownJobs_;
    } else {
        UnknownBytes_ -= job.Bytes;
    }
    Jobs_.erase(it);
    WakeWorkWaits();
}

void TCollectionLoadController::StopJob(TLoad& load) {
    const auto worker = std::exchange(load.Worker, {});
    if (!worker) {
        return;
    }
    auto& job = Jobs_.at(worker);
    if (job.Terminal) {
        ReleaseJob(worker);
    } else {
        job.Revoked = true;
        Send(Owner_, worker, new NActors::TEvents::TEvPoison);
    }
}

void TCollectionLoadController::Stop() {
    while (!Loads_.empty()) {
        Fail(Loads_.begin()->second, NKikimrProto::RACE, "Shared cache stopped");
    }
    while (!Retained_.empty()) {
        Fail(Retained_.begin()->second, NKikimrProto::RACE, "Shared cache stopped");
    }
}

} // namespace NKikimr::NSharedCache
