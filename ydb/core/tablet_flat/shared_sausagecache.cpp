#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "cache.h"
#include "shared_cache_btree_walk.h"
#include "shared_cache_collections.h"
#include "shared_cache_events.h"
#include "shared_sausagecache_state.h"
#include "shared_cache_counters.h"
#include "shared_sausagecache.h"
#include "util_fmt_abort.h"
#include <util/stream/format.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/process_stats.h>
#include <util/system/getpid.h>
#include <util/generic/algorithm.h>
#include <util/generic/set.h>
#include <util/generic/scope.h>

namespace NKikimr::NSharedCache {

using namespace NTabletFlatExecutor;
using TPageLocation = NTable::NPage::TPageLocation;

::NFormatPrivate::THumanReadableSize HumanReadableBytes(ui64 bytes) {
    return HumanReadableSize(bytes, SF_BYTES);
}

namespace {

enum class EBlockIOFetchTypeCookie {
    NoQueue = 1,
    AsyncQueue = 2,
    ScanQueue = 3,
    Fetch = 5,
};

struct TFetch {
    TLogoBlobID CollectionId;
    ui64 Bytes = 0;
    EBlockIOFetchTypeCookie QueueCookie = EBlockIOFetchTypeCookie::NoQueue;
    TVector<TPageFetch> Pages;
};

struct TRequest {
    TLogoBlobID CollectionId;
    TActorId Sender;
    ui64 EventCookie = 0;
    ui64 Cookie = 0;
    TIntrusivePtr<TRequestCompletion> Completion;
    bool Cancelled = false;
    bool CollectionTracked = false;
};

struct TRegisteredCollection {
    TCacheCollectionRef Ref;
    mutable bool ReportedStats = false; // Already included in the owner's totals; not part of the set's key.

    struct THash {
        size_t operator()(const TRegisteredCollection& entry) const {
            return operator()(entry.Ref.CacheItem());
        }

        size_t operator()(TCollectionCacheItem item) const {
            return ::THash<ui64>()(item.Raw());
        }
    };

    struct TEqual {
        bool operator()(const TRegisteredCollection& a, const TRegisteredCollection& b) const {
            return a.Ref.CacheItem() == b.Ref.CacheItem();
        }

        bool operator()(const TRegisteredCollection& a, TCollectionCacheItem b) const {
            return a.Ref.CacheItem() == b;
        }

        bool operator()(TCollectionCacheItem a, const TRegisteredCollection& b) const {
            return a == b.Ref.CacheItem();
        }
    };
};

struct TCollectionOwner {
    THashSet<TRegisteredCollection, TRegisteredCollection::THash, TRegisteredCollection::TEqual> Collections;
    TIntrusivePtr<TCollectionOwnerStats> Stats;
};

struct TQueuedRequest {
    TActorId Sender;
    TIntrusivePtr<TRequestCompletion> Completion;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    NBlockIO::EPriority Priority;
    NWilson::TTraceId TraceId;
    TDeque<TPageFetch> Pages;
    TCollectionCacheItem CollectionItem;
    TVector<ui32> AdmissionIndices;
    TVector<bool> Sticky;
    TVector<TIntrusivePtr<TRequestCompletion>> HandleRequirements;
    bool WaitingTransaction = false;
};

struct TRequestQueue {
    explicit TRequestQueue(EBlockIOFetchTypeCookie cookie)
        : Cookie(cookie)
    {}

    EBlockIOFetchTypeCookie Cookie;

    TMap<TActorId, TDeque<THolder<TQueuedRequest>>> Requests;

    ui64 Limit = 0;
    ui64 InFly = 0;

    TActorId NextToRequest;
};

class TSharedPageCache : public TActorBootstrapped<TSharedPageCache>, private ICacheBTreeWalkHost {
    static constexpr ui8 MinimumAddressBits = 8;
    static constexpr ui64 WalkResultCookie = Max<ui64>();
    static constexpr ui64 KeepPreloadResultCookie = Max<ui64>() - 1;

    TActorId Owner;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    TSharedCacheConfig Config;
    TCacheCounters Counters;
    TIntrusivePtr<TSharedCache> Cache;
    THolder<TCollectionLoadController> CollectionLoads;
    TCacheBTreeWalkController Walks{ *this };
    TPendingInMemoryPages PendingInMemoryPages;
    THashMap<ui64, TLogoBlobID> WalkRequests;
    ui64 KeepPreloadBytes = 0;
    THashSet<TLogoBlobID> KeepPreloadRetryBlocked;
    // I/O owners stay here until the matching fetch actor completes.
    THashMap<TActorId, TFetch> Fetches;
    // Each first owner registration holds one native metadata ref until detach/unregister.
    THashMap<TActorId, TCollectionOwner> Owners;
    THashMap<ui64, TRequest> Requests;
    ui64 NextRequestId = 1;
    TDeque<THolder<TQueuedRequest>> ResourceWaits;
    TRequestQueue AsyncRequests{EBlockIOFetchTypeCookie::AsyncQueue};
    TRequestQueue ScanRequests{EBlockIOFetchTypeCookie::ScanQueue};

    ui64 LastKeepResidentBytes = 0;

    ui64 MemLimitBytes = 0;
    ui64 ControllerMaxBytes = 0;
    ui64 ControllerCurrentBytes = 0;
    bool HasControllerAllocation = false;
    bool LimitReady = false;
    bool LimitMaintenanceScheduled = false;
    TVector<TAutoPtr<IEventHandle>> DeferredEvents;
    ui64 TargetInMemoryBytes = 0;
    bool WalkContinuationScheduled = false;
    ui64 NextWalkRequestId = 1;
    ui64 NextKeepPreloadRequestId = 1;

    TCollection* FindCollection(const TLogoBlobID& id) override {
        if (!Cache) {
            return nullptr;
        }
        TCacheCollectionRef ref;
        if (Cache->Find(id, ref) != ECacheResultStatus::Hit || !ref->GetActorState()) {
            return nullptr;
        }
        return ref.Get();
    }

    void ForEachCollection(TFunctionRef<void(TCollection&)> callback) {
        if (!Cache) {
            return;
        }
        Cache->ForEachCollection([&](TCollection& collection) {
            if (collection.GetActorState()) {
                callback(collection);
            }
        });
    }

    TPendingInMemoryPages& PendingPages() override {
        return PendingInMemoryPages;
    }

    void ScheduleContinuation() override {
        if (!std::exchange(WalkContinuationScheduled, true)) {
            Send(SelfId(), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::ContinueBTreeWalk)));
        }
    }

    NActors::TSharedData FindPage(TCollection& collection, TPageOffset offset) override {
        if (!collection.CacheItem) {
            return {};
        }
        TCachePageRef page;
        if (Cache->Find(collection.CacheItem, static_cast<ui64>(offset), page) == ECacheResultStatus::Hit) {
            return page.BuildSharedData();
        }
        return {};
    }

    size_t AllowedFetchPageCount(TArrayRef<const TPageLocation> locations, ui64& bytes) const {
        const ui64 byteLimit = Cache->CurrentLimit();
        const ui64 pageLimit = Cache->TargetHandleCount() - TSharedCache::RequestHandleOverhead;
        size_t count = 0;
        bytes = 0;
        // Keep the first page so an insufficient limit uses the normal resource-wait path
        for (const auto& location : locations) {
            const ui64 pageBytes = KeepPageBytes(location);
            if (count && (count >= pageLimit || pageBytes > byteLimit - Min(byteLimit, bytes))) {
                break;
            }
            bytes += pageBytes;
            ++count;
            if (bytes >= byteLimit) {
                break;
            }
        }
        return count;
    }

    void FetchIndexLevel(
        TCollection& collection, TVector<TPageLocation>&& locations, const TLogoBlobID& walkCollectionId) override {
        Y_DEBUG_ABORT_UNLESS(collection.GetCacheMode() != ECacheMode::TryKeepInMemory);
        ui64 bytes = 0;
        locations.resize(AllowedFetchPageCount(locations, bytes));
        if (locations.size() > 1) {
            const TCacheHandleRequirement requirement{ collection.Id(), locations };
            // Independent residency can leave less room than the configured ceilings
            if (!Cache->CanReserve(bytes) || !Cache->CanAllocateHandles({ &requirement, 1 })) {
                locations.resize(1);
            }
        }
        const ui64 requestId = NextWalkRequestId++;
        Y_ENSURE(WalkRequests.emplace(requestId, walkCollectionId).second);
        Walks.FetchStarted(walkCollectionId);
        NSharedCache::TEvRequest request(
            NBlockIO::EPriority::Bkgr, collection.PageCollection(), std::move(locations), requestId);
        RequestPages(SelfId(), WalkResultCookie, request);
    }

    void RequestStickyPages(
        TCollection& collection, const TActorId& owner, const TVector<TPageLocation>& locations) override {
        RequestStickyCollectionPages(collection, owner, locations);
    }

    void CancelQueuedRequestsAndPump(const TLogoBlobID& walkCollectionId) override {
        CancelQueuedWalkRequests(walkCollectionId);
        RequestFromQueue(AsyncRequests);
        RequestFromQueue(ScanRequests);
    }

    ui64 GetTargetCacheHardLimitBytes() const {
        if (ControllerMaxBytes) {
            return Config.HasMemoryLimit() ? Min(ControllerMaxBytes, Config.GetMemoryLimit()) : ControllerMaxBytes;
        }
        if (Config.HasMemoryLimit()) {
            return Config.GetMemoryLimit();
        }
        return Cache ? Cache->HardLimit() : 0;
    }

    ui64 GetTargetCacheCurrentLimitBytes() const {
        const ui64 maximum = GetTargetCacheHardLimitBytes();
        return ControllerCurrentBytes ? Min(ControllerCurrentBytes, maximum) : maximum;
    }

    void UpdateSoftLimitOverride() {
        if (HasControllerAllocation) {
            Y_ENSURE(Cache->UpdateSoftLimitOverride(MemLimitBytes));
        } else {
            Y_ENSURE(Cache->UpdateSoftLimitOverride(Max<ui64>()));
        }
    }

    ui64 GetCacheReservationLimitBytes(ui64 allocationLimit) const {
        ui64 nodeLimit = AppData()->MemoryControllerConfig.GetHardLimitBytes();
        if (!nodeLimit) {
            NActors::TProcStat process;
            if (process.Fill(static_cast<pid_t>(GetPID()))) {
                nodeLimit = process.CGroupMemLim;
                if (process.MemTotal) {
                    nodeLimit = nodeLimit ? Min(nodeLimit, process.MemTotal) : process.MemTotal;
                }
            }
            if (!nodeLimit) {
                nodeLimit = 2_GB;
            }
        }
        return Max(Max(allocationLimit, nodeLimit), Max(ControllerMaxBytes, Config.GetMemoryLimit()));
    }

    bool InitializeCache() {
        if (Cache) {
            return true;
        }
        const auto* threads = TActivationContext::ActorSystem()->GetSubSystem<TCacheThreadSubSystem>();
        Y_ENSURE(threads, "Shared-cache thread records must be prepared before cache initialization");
        const ui32 hazardCount = threads->ThreadCount();
        // Small metadata pages can exhaust handles while the byte budget still has room.
        ui8 minimumAddressBits = Max<ui8>(MinSharedCacheAddressBits, MinimumAddressBits);
        while (TCacheCapacityConfig{ .AddressBits = minimumAddressBits }.HotSlotCount() <
               SharedCachePolicyFor<TProdTraits>.MinHotSlots)
        {
            ++minimumAddressBits;
        }
        TCacheCapacityConfig minimum;
        if (!TryCalculateSharedCacheFootprint(
                minimumAddressBits, TSharedCache::ExpectedPageSize, 0, hazardCount, minimum))
        {
            return false;
        }
        const ui64 targetLimit = GetTargetCacheHardLimitBytes();
        const ui64 allocationLimit = Max(targetLimit, minimum.TotalBytes);
        TCacheCapacityConfig current;
        if (!TryCalculateSharedCacheCapacity(allocationLimit, TSharedCache::ExpectedPageSize, 0, hazardCount, current))
        {
            return false;
        }
        TCacheCapacityConfig reserved;
        if (!TryCalculateSharedCacheCapacity(GetCacheReservationLimitBytes(allocationLimit),
                TSharedCache::ExpectedPageSize, 0, hazardCount, reserved)) {
            return false;
        }
        auto space = TCacheSpace::Create(current, reserved, *threads);
        TProdTraits traits;
        traits.KeepEvictionActorSystem = TActivationContext::ActorSystem();
        traits.KeepEvictionActor = SelfId();
        Cache = TSharedCache::Create(std::move(space), targetLimit, std::move(traits));
        if (Cache) {
            Y_ENSURE(Cache->UpdateCurrentLimit(GetTargetCacheCurrentLimitBytes()));
            UpdateSoftLimitOverride();
        }
        return bool(Cache);
    }

    TCollectionLoadController& EnsureCollectionLoads() {
        if (!CollectionLoads) {
            Y_ENSURE(InitializeCache(), "Cannot initialize shared cache");
            CollectionLoads = MakeHolder<TCollectionLoadController>(*Cache, SelfId());
        }
        return *CollectionLoads;
    }

    void Handle(TEvCollectionRequest::TPtr& ev, const TActorContext&) {
        EnsureCollectionLoads().Request(ev->Sender, ev->Cookie, *ev->Get());
        RetryResourceWaits();
    }

    void Handle(TEvCollectionRequestUpdate::TPtr& ev, const TActorContext&) {
        if (CollectionLoads) {
            CollectionLoads->Update(*ev->Get()->Request);
            RetryResourceWaits();
        }
    }

    void Handle(TEvCollectionRawReady::TPtr& ev, const TActorContext&) {
        if (CollectionLoads) {
            CollectionLoads->RawReady(ev->Sender, *ev->Get());
            RetryResourceWaits();
        }
    }

    void Handle(TEvCollectionData::TPtr& ev, const TActorContext&) {
        if (CollectionLoads) {
            CollectionLoads->Data(ev->Sender, *ev->Get());
            RetryResourceWaits();
        }
    }

    TCollection* TryEnsureCollection(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        const TLogoBlobID& pageCollectionId = pageCollection->Label();
        Y_ENSURE(pageCollectionId);
        Y_ENSURE(InitializeCache(), "Cannot initialize shared cache");
        const TCollectionLocation location{ .Id = pageCollectionId, .BackingSize = pageCollection->BackingSize() };
        TCollectionCacheItem inserted;
        TCacheCollectionRef ref;
        const auto status = Cache->FindOrInsert(location, inserted, ref);
        if (status == ECacheResultStatus::Miss || status == ECacheResultStatus::Pending) {
            return nullptr;
        }
        if (status == ECacheResultStatus::Inserted) {
            auto value = MakeHolder<TCollection>(pageCollection, inserted, ECacheMode::Regular);
            Y_ENSURE(Cache->MakeReady(inserted, std::move(value)));
            Y_ENSURE(Cache->Find(pageCollectionId, ref) == ECacheResultStatus::Hit);
        } else {
            Y_ENSURE(status == ECacheResultStatus::Hit);
        }
        TCollection& collection = *ref;
        Y_ENSURE(collection.PageCollection()->Total() == pageCollection->Total(),
            "Page collection " << pageCollectionId << " changed number of pages from "
                               << collection.PageCollection()->Total() << " to " << pageCollection->Total()
                               << " by " << owner);
        collection.SetSkipBTreeIndexV1Shadow(pageCollection->SkipBTreeIndexV1Shadow());
        if (!collection.GetActorState()) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE,
                "Add page collection " << pageCollectionId);
            collection.EnsureActorState().ReportedResidentBytes = collection.ResidentPageBytes();
            Counters.PageCollections->Inc();
            if (collection.GetCacheMode() != ECacheMode::Regular) {
                Y_ENSURE(Cache->SetCollectionPagesCacheMode(collection.CacheItem, ECacheMode::Regular));
            }
        }
        return &collection;
    }

    TCollection& EnsureCollection(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        auto* collection = TryEnsureCollection(std::move(pageCollection), owner);
        Y_ENSURE(collection, "Cannot admit page collection for " << owner);
        return *collection;
    }

    void UpdateKeepLimit() {
        // Sticky ownership reduces the budget available to Keep pages
        Y_ENSURE(Cache->UpdateKeepLimit(Min(TargetInMemoryBytes, Cache->HardLimit())));
    }

    void ActualizeCacheSizeLimit() {
        Counters.ConfigLimitBytes->Set(Config.HasMemoryLimit() ? Config.GetMemoryLimit() : 0);
        if (LimitReady && !Cache) {
            Y_ENSURE(InitializeCache(), "Cannot initialize shared cache");
        }
        if (Cache) {
            const ui64 hardLimit = GetTargetCacheHardLimitBytes();
            const ui64 currentLimit = GetTargetCacheCurrentLimitBytes();
            const bool limitsChanged = hardLimit != Cache->HardLimit() || currentLimit != Cache->CurrentLimit();
            if (hardLimit < Cache->CurrentLimit()) {
                Y_ENSURE(Cache->UpdateCurrentLimit(Min(currentLimit, hardLimit)));
            }
            if (hardLimit != Cache->HardLimit()) {
                Y_ENSURE(Cache->UpdateHardLimit(hardLimit),
                    "Shared-cache capacity change exceeds reserved space: requested " << hardLimit << " reserved "
                                                                                      << Cache->ReservationLimit());
            }
            Y_ENSURE(Cache->UpdateCurrentLimit(currentLimit));
            UpdateSoftLimitOverride();
            UpdateKeepLimit();
            Counters.ActiveLimitBytes->Set(currentLimit);
            if (limitsChanged) {
                KeepPreloadRetryBlocked.clear();
                CancelOversizedWalkRequests();
            }
        } else {
            Counters.ActiveLimitBytes->Set(GetTargetCacheCurrentLimitBytes());
        }
    }

    void DoGC() {
        if (Cache) {
            const ui64 staticBytes = Cache->StaticBytes();
            const bool pressure = Cache->RetainedBytes() > Cache->SoftLimit();
            const ui32 workLimit =
                pressure || Cache->NeedsCapacityMaintenance() ? SharedCacheTransitionWorkBatch : 1;
            bool moreWork = false;
            for (ui32 work = 0; work < workLimit; ++work) {
                const bool reclaimed = Cache->EnforceCurrentLimit();
                moreWork = Cache->RunMaintenance();
                if (!reclaimed && !moreWork) {
                    break;
                }
                if (!Cache->NeedsCapacityMaintenance() && Cache->RetainedBytes() <= Cache->SoftLimit()) {
                    break;
                }
            }
            if (moreWork && Cache->NeedsCapacityMaintenance() && !LimitMaintenanceScheduled) {
                LimitMaintenanceScheduled = true;
            Send(SelfId(), new TKikimrEvents::TEvWakeup(ui64(EWakeupTag::DoLimitMaintenance)));
        }
            if (staticBytes != Cache->StaticBytes()) {
                UpdateKeepLimit();
            }

            Counters.ActivePages->Set(Cache->HotPages() + Cache->StickyPages());
            Counters.ActiveBytes->Set(Cache->HotBytes() + Cache->StickyBytes());
            Counters.ActiveInMemoryBytes->Set(Cache->KeepReadyPageBytes());
            Counters.PassivePages->Set(Cache->ColdPages());
            Counters.PassiveBytes->Set(Cache->ColdBytes());
        }
        LastKeepResidentBytes = Cache ? Cache->KeepResidentPageBytes() : 0;
        if (MemoryConsumer) {
            MemoryConsumer->SetConsumption(Cache ? Cache->OverallUsage() : 0);
        }
    }

    void Handle(NMemory::TEvConsumerRegistered::TPtr &ev, const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Register memory consumer");

        auto *msg = ev->Get();
        MemoryConsumer = std::move(msg->Consumer);
    }

    void ReplayDeferredEvents() {
        if (!LimitReady) {
            return;
        }
        for (auto& pending : DeferredEvents) {
            TActivationContext::Send(pending.Release());
        }
        DeferredEvents.clear();
    }

    void Handle(NMemory::TEvConsumerLimit::TPtr &ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        if (msg->MaxLimitBytes) {
            ControllerMaxBytes = msg->MaxLimitBytes;
        }
        Y_ENSURE(ControllerMaxBytes || Config.HasMemoryLimit() || Cache,
            "Shared-cache controller must supply a maximum allocation");
        HasControllerAllocation = true;
        if (msg->CurrentLimitBytes) {
            ControllerCurrentBytes = msg->CurrentLimitBytes;
        }
        LimitReady = true;

        LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Limit memory consumer"
            << " with " << HumanReadableBytes(msg->LimitBytes));

        MemLimitBytes = msg->LimitBytes;
        Counters.MemLimitBytes->Set(MemLimitBytes);

        ActualizeCacheSizeLimit();
        ReplayDeferredEvents();
    }

    void Registered(TActorSystem* sys, const TActorId& owner) override {
        NActors::TActorBootstrapped<TSharedPageCache>::Registered(sys, owner);
        Owner = owner;
    }

    void DetachCollection(TCollection& collection) {
        CancelRequests(collection.Id());
        Y_ENSURE(Cache->SetCollectionPagesCacheMode(collection.CacheItem, ECacheMode::Regular));
        // The registry keeps the unified object alive until replies and walks drain.
        ActualizeCacheSizeLimit();
    }

    void TakePoison(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Poison shared cache");
        if (CollectionLoads) {
            CollectionLoads->Stop();
            CollectionLoads.Reset();
        }
        for (auto& [_, request] : Requests) {
            request.Completion->Cancel();
        }
        PruneQueues();
        AsyncRequests.Requests.clear();
        ScanRequests.Requests.clear();
        ResourceWaits.clear();
        Fetches.clear();
        if (Cache) {
            ForEachCollection([&](TCollection& collection) {
                collection.ResetActorState();
                Y_ENSURE(Cache->DetachCollection(collection.CacheItem));
            });
        }
        KeepPreloadBytes = 0;
        Requests.clear();

        if (auto owner = std::exchange(Owner, { }))
            Send(owner, new TEvents::TEvGone);
        PassAway();
    }

    TCollection* TryAttachCollection(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        auto* admitted = TryEnsureCollection(std::move(pageCollection), owner);
        if (!admitted) {
            return nullptr;
        }
        TCollection& collection = *admitted;

        if (collection.GetActorState()->Owners.insert(owner).second) {
            auto [ownerIt, inserted] = Owners.try_emplace(owner);
            if (inserted) {
                Counters.Owners->Inc();
            }
            TCacheCollectionRef ref;
            Y_ENSURE(Cache->Find(collection.Id(), ref) == ECacheResultStatus::Hit);
            Y_ENSURE(ownerIt->second.Collections.emplace(TRegisteredCollection{ .Ref = std::move(ref) }).second);
            Counters.PageCollectionOwners->Inc();
        }

        return &collection;
    }

    TCollection& AttachCollection(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        auto* collection = TryAttachCollection(std::move(pageCollection), owner);
        Y_ENSURE(collection, "Cannot attach page collection for " << owner);
        return *collection;
    }

    static void UpdateStat(std::atomic<ui64>& total, ui64 previous, ui64 current) {
        if (current > previous) {
            total.fetch_add(current - previous, std::memory_order_relaxed);
        } else if (current < previous) {
            const ui64 old = total.fetch_sub(previous - current, std::memory_order_relaxed);
            Y_ENSURE(old >= previous - current);
        }
    }

    void RegisterOwnerStats(
        TCollection& collection, const TActorId& owner, const TIntrusivePtr<TCollectionOwnerStats>& stats) {
        if (!stats) {
            return;
        }
        auto& entry = Owners.at(owner);
        Y_ENSURE(!entry.Stats || entry.Stats == stats);
        entry.Stats = stats;
        const auto& registration = *entry.Collections.find(collection.CacheItem);
        if (registration.ReportedStats) {
            return;
        }
        registration.ReportedStats = true;
        const auto* state = collection.GetActorState();
        stats->PageCollections.fetch_add(1, std::memory_order_relaxed);
        stats->SharedBodyBytes.fetch_add(state->ReportedResidentBytes, std::memory_order_relaxed);
        if (state->StickyOwners.contains(owner)) {
            stats->StickyBytes.fetch_add(state->ReportedResidentBytes, std::memory_order_relaxed);
        }
        if (state->InMemoryOwners.contains(owner)) {
            stats->TryKeepInMemoryBytes.fetch_add(
                collection.PageCollection()->BackingSize(), std::memory_order_relaxed);
        }
    }

    void RemoveOwnerStats(TCollection& collection, const TActorId& owner, const TCollectionOwner& entry,
        const TRegisteredCollection& registration) {
        if (!registration.ReportedStats) {
            return;
        }
        const auto* state = collection.GetActorState();
        UpdateStat(entry.Stats->PageCollections, 1, 0);
        UpdateStat(entry.Stats->SharedBodyBytes, state->ReportedResidentBytes, 0);
        if (state->StickyOwners.contains(owner)) {
            UpdateStat(entry.Stats->StickyBytes, state->ReportedResidentBytes, 0);
        }
        if (state->InMemoryOwners.contains(owner)) {
            UpdateStat(entry.Stats->TryKeepInMemoryBytes, collection.PageCollection()->BackingSize(), 0);
        }
    }

    void UpdateCollectionStats(TCollection& collection) {
        auto& state = *collection.GetActorState();
        const ui64 previous = state.ReportedResidentBytes;
        const ui64 current = collection.ResidentPageBytes();
        if (previous == current) {
            return;
        }
        state.ReportedResidentBytes = current;
        for (const auto& owner : state.Owners) {
            auto& entry = Owners.at(owner);
            if (entry.Collections.find(collection.CacheItem)->ReportedStats) {
                UpdateStat(entry.Stats->SharedBodyBytes, previous, current);
                if (state.StickyOwners.contains(owner)) {
                    UpdateStat(entry.Stats->StickyBytes, previous, current);
                }
            }
        }
    }

    bool RestartBlockedWalk(TQueuedRequest& waiting) {
        if (waiting.Completion->Locations().size() <= 1) {
            return false;
        }
        auto request = Requests.find(waiting.Completion->CompletionId());
        if (request == Requests.end() || request->second.Cancelled || !waiting.Completion->IsActive() ||
            request->second.Sender != SelfId() || request->second.EventCookie != WalkResultCookie) {
            return false;
        }
        request->second.Cancelled = true;
        waiting.Completion->Cancel();
        return true;
    }

    void CancelOversizedWalkRequests() {
        bool cancelled = false;
        for (auto& [_, request] : Requests) {
            if (request.Cancelled || request.Sender != SelfId() || request.EventCookie != WalkResultCookie) {
                continue;
            }
            ui64 bytes = 0;
            if (AllowedFetchPageCount(request.Completion->Locations(), bytes) <
                request.Completion->Locations().size()) {
                request.Cancelled = true;
                // Drain dispatched callbacks before the existing RACE reply restarts the walk
                request.Completion->Cancel();
                cancelled = true;
            }
        }
        if (cancelled) {
            PruneQueues();
        }
    }

    void CancelQueuedWalkRequests(const TLogoBlobID& walkCollectionId) {
        for (const auto& [requestId, collectionId] : WalkRequests) {
            if (collectionId != walkCollectionId) {
                continue;
            }
            for (auto& [_, request] : Requests) {
                if (request.Sender == SelfId() && request.EventCookie == WalkResultCookie &&
                    request.Cookie == requestId) {
                    request.Cancelled = true;
                    request.Completion->Cancel();
                }
            }
        }
        PruneQueues();
    }

    void CancelRequests(const TLogoBlobID& collectionId, const TActorId& owner = {}) {
        for (auto& [_, request] : Requests) {
            if ((!collectionId || request.CollectionId == collectionId) && (!owner || request.Sender == owner)) {
                request.Cancelled = true;
                request.Completion->Cancel(request.Sender != SelfId());
                            }
                        }
        for (auto it = ResourceWaits.begin(); it != ResourceWaits.end();) {
            const auto& waiting = **it;
            const bool requiredCollection =
                !collectionId || waiting.PageCollection->Label() == collectionId ||
                (waiting.WaitingTransaction &&
                    (waiting.Completion->WaitPad()->WorkingSetPages.contains(collectionId) ||
                        AnyOf(waiting.HandleRequirements, [&](const auto& completion) {
                            return completion->PageCollection()->Label() == collectionId;
                        })));
            if (waiting.WaitingTransaction && requiredCollection && (!owner || waiting.Sender == owner)) {
                waiting.Completion->NotifyResourcesReady();
                it = ResourceWaits.erase(it);
            } else {
                ++it;
            }
        }
        PruneQueues();
    }

    void Handle(NSharedCache::TEvCacheReadyRequest::TPtr& ev, const TActorContext&) {
        // StateFunc defers this request until the configured cache owner is fully initialized.
        Send(ev->Sender, new NSharedCache::TEvCacheReady, 0, ev->Cookie);
    }

    void Handle(NSharedCache::TEvAttach::TPtr &ev, const TActorContext& ctx) {
        auto*msg = ev->Get();
        TCollection& collection = AttachCollection(msg->PageCollection, ev->Sender);
        UpdateCollectionStats(collection);
        RegisterOwnerStats(collection, ev->Sender, msg->OwnerStats);
        const bool wasSticky = collection.GetActorState()->StickyOwners.contains(ev->Sender);
        const bool wasKeep = collection.GetActorState()->InMemoryOwners.contains(ev->Sender);
        if (msg->CacheMode == ECacheMode::Sticky) {
            collection.GetActorState()->StickyOwners.insert(ev->Sender);
        } else {
            collection.GetActorState()->StickyOwners.erase(ev->Sender);
        }
        if (msg->CacheMode == ECacheMode::TryKeepInMemory) {
            TryMoveToTryKeepInMemoryCache(collection, msg->PageCollection, ev->Sender);
        } else {
            TryMoveToRegularCache(collection, ev->Sender);
        }
        const auto& entry = Owners.at(ev->Sender);
        if (entry.Collections.find(collection.CacheItem)->ReportedStats) {
            const bool sticky = collection.GetActorState()->StickyOwners.contains(ev->Sender);
            const bool keep = collection.GetActorState()->InMemoryOwners.contains(ev->Sender);
            UpdateStat(entry.Stats->StickyBytes, wasSticky ? collection.GetActorState()->ReportedResidentBytes : 0,
                sticky ? collection.GetActorState()->ReportedResidentBytes : 0);
            UpdateStat(entry.Stats->TryKeepInMemoryBytes, wasKeep ? collection.PageCollection()->BackingSize() : 0,
                keep ? collection.PageCollection()->BackingSize() : 0);
        }
        const bool resumedSticky = ApplyCollectionMode(collection, ctx);
        Walks.UpdateSeeds(collection, ev->Sender, std::move(msg->BtreeSeeds), msg->ReplayStickyWalk || resumedSticky);
        if (ev->Cookie) {
            Send(ev->Sender, new NSharedCache::TEvAttached(collection.Id(), collection.CacheItem), 0, ev->Cookie);
        }
    }

    void Handle(NSharedCache::TEvSaveCompactedPages::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvSaveCompactedPages *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Save page collection " << pageCollectionId
            << " owner " << ev->Sender
            << " compacted pages " << msg->Pages);

        Y_ENSURE(pageCollectionId);
        Y_ENSURE(!FindCollection(pageCollectionId), "Only new collections can save compacted pages");
        auto& collection = EnsureCollection(msg->PageCollection, ev->Sender);
        const TCollectionCacheItem collectionItem = collection.CacheItem;
        Y_ENSURE(collectionItem, "Cannot create page collection " << pageCollectionId);
        Y_ENSURE(!msg->CacheItem || msg->CacheItem == collectionItem);
        for (const auto& location : msg->Pages) {
            if (pageCollection.Total() != pageCollection.MetaPages() &&
                !NPageCollection::IsDeadPage(location.Type, pageCollection.SkipBTreeIndexV1Shadow())) {
                PendingInMemoryPages[collection.Id()].emplace(location);
            }
        }

        TCacheCollectionRef ownership;
        {
            Y_ENSURE(Cache->Find(pageCollectionId, ownership) == ECacheResultStatus::Hit);
            if (ownership.IsSoleReference()) {
                // Preserve writer Sticky pages through part handoff, and demote abandoned ones.
                collection.DrainStickyPages();
            }
        }
        // Cached page items protect their metadata. Normal page reclamation schedules its cleanup.
        if (!collection.HasPageItems()) {
            TryDropExpiredCollection(collection);
        }
        ActualizeCacheSizeLimit();
    }

    bool RequestPages(TActorId sender, ui64 eventCookie, NSharedCache::TEvRequest& msg) {
        const TLogoBlobID id = msg.PageCollection->Label();
        TCollection* owner = FindCollection(id);
        const ui64 maxPageHandles = Cache->TargetHandleCount() - TSharedCache::RequestHandleOverhead;
        if (msg.Pages.size() > maxPageHandles) {
            THashSet<TPageOffset> distinctOffsets;
            distinctOffsets.reserve(msg.Pages.size());
            for (const TPageLocation& page : msg.Pages) {
                distinctOffsets.insert(page.Offset);
            }
            if (distinctOffsets.size() > maxPageHandles) {
                Y_TABLET_ERROR("Cache request for " << id << " needs " << distinctOffsets.size()
                                                    << " distinct page handles but physical capacity is "
                                                   << maxPageHandles);
            }
        }
        const bool stickyRequest = eventCookie == ui64(ERequestTypeCookie::StickyPages);
        Counters.PendingRequests->Inc();
        const ui64 completionId = NextRequestId++;
        if (owner) {
            ++owner->GetActorState()->PendingRequests;
        }
        auto completion = MakeIntrusive<TRequestCompletion>(TRequestCompletionParams{
            .ActorSystem = TActivationContext::ActorSystem(),
            .ReplyTo = sender,
            .EventCookie = eventCookie,
            .PageCollection = msg.PageCollection,
            .Pages = std::move(msg.Pages),
            .WaitPad = std::move(msg.WaitPad),
            .Cookie = msg.Cookie,
            .Notify = SelfId(),
            .CoreRoute = true,
            .CompletionId = completionId,
        });

        auto [requestIt, inserted] =
            Requests.emplace(completionId, TRequest{ id, sender, eventCookie, msg.Cookie, completion });
        Y_ENSURE(inserted);
        requestIt->second.CollectionTracked = bool(owner);
        if (const auto& pad = completion->WaitPad(); pad && pad->PostponedForResources) {
            Requests.at(completionId).Cancelled = true;
            completion->PostponeForResources();
            return true;
        }
        if (completion->Locations().empty()) {
            return true;
        }
        auto admission = MakeHolder<TQueuedRequest>();
        admission->Sender = sender;
        admission->Completion = completion;
        admission->PageCollection = msg.PageCollection;
        admission->CollectionItem = owner ? owner->CacheItem : TCollectionCacheItem{};
        admission->Priority = msg.Priority;
        admission->TraceId = std::move(msg.TraceId);
        const auto& locations = completion->Locations();
        Y_ENSURE(msg.Sticky.empty() || msg.Sticky.size() == locations.size());
        admission->AdmissionIndices.reserve(locations.size());
        admission->Sticky.reserve(locations.size());
        for (ui32 index = 0; index < locations.size(); ++index) {
            admission->AdmissionIndices.push_back(index);
            admission->Sticky.push_back(stickyRequest || (!msg.Sticky.empty() && msg.Sticky[index]));
        }
        if (!TryAdmitPages(*admission)) {
            if (eventCookie == KeepPreloadResultCookie) {
                requestIt->second.Cancelled = true;
                completion->Cancel(true);
                ActualizeCacheSizeLimit();
                return false;
            } else if (RestartBlockedWalk(*admission)) {
                TryAdmitPages(*admission);
            } else {
                ResourceWaits.push_back(std::move(admission));
            }
        }
        RetryResourceWaits();
        ActualizeCacheSizeLimit();
        return true;
    }

    bool TryAdmitPages(TQueuedRequest& admission) {
        if (admission.Completion->IsCancelled() || admission.Completion->HasFailed()) {
            // These indices have no native waiter; drain them before retiring the request
            for (const ui32 index : admission.AdmissionIndices) {
                admission.Completion->Complete(index, {}, EPageFetchCompletion::Failed);
            }
            admission.AdmissionIndices.clear();
            return true;
        }
        TCollection* owner = FindCollection(admission.PageCollection->Label());
        if (admission.CollectionItem) {
            if (!owner || owner->CacheItem != admission.CollectionItem) {
                admission.Completion->Cancel(true);
                return true;
            }
        } else {
            owner = TryAttachCollection(admission.PageCollection, admission.Sender);
            if (!owner) {
                return false;
            }
            admission.CollectionItem = owner->CacheItem;
            Requests.at(admission.Completion->CompletionId()).CollectionTracked = true;
            ++owner->GetActorState()->PendingRequests;
            RecordPendingInMemoryPages(*owner, admission.Completion->Locations());
        }
        auto& request = Requests.at(admission.Completion->CompletionId());
        const bool preload = request.EventCookie == KeepPreloadResultCookie;
        const bool recordStats =
            request.EventCookie != KeepPreloadResultCookie && request.EventCookie != WalkResultCookie;
        const bool keepPages = owner->GetCacheMode() == ECacheMode::TryKeepInMemory;
        const auto& locations = admission.Completion->Locations();
        TVector<TCachePageRequest> requests;
        requests.reserve(admission.AdmissionIndices.size());
        for (const ui32 index : admission.AdmissionIndices) {
            requests.emplace_back(
                locations[index], new TRequestPageWaiter(admission.Completion, index), admission.Sticky[index]);
        }
        const bool admitted = preload ? Cache->FindOrInsertPreloadBatch(admission.CollectionItem, requests)
                                      : Cache->FindOrInsertBatch(admission.CollectionItem, requests, recordStats, true);
        if (!admitted) {
            return false;
        }

        // Optional work must not hold hits while waiting for missing handles or another request's admission
        const bool retryPreload = preload && AnyOf(requests, [](const auto& page) {
            return page.Status() == ECacheResultStatus::Miss || page.Status() == ECacheResultStatus::Pending;
        });
        if (retryPreload) {
            request.Cancelled = true;
            admission.Completion->Cancel(true);
        }

        TVector<ui32> unresolved;
        TVector<TPageFetch> toFetch;
        for (ui32 localIndex = 0; localIndex < requests.size(); ++localIndex) {
            auto& page = requests[localIndex];
            const ui32 index = admission.AdmissionIndices[localIndex];
            if (page.Status() == ECacheResultStatus::Miss) {
                // A presence snapshot can become absent before candidate insertion.
                unresolved.push_back(index);
                continue;
            }
            const ui64 bytes = locations[index].Size;
            if (recordStats) {
                Counters.RequestedPages->Inc();
                Counters.RequestedBytes->Add(bytes);
            }
            switch (page.Status()) {
                case ECacheResultStatus::Hit:
                    if (recordStats) {
                        Counters.CacheHitPages->Inc();
                        Counters.CacheHitBytes->Add(bytes);
                    }
                    admission.Completion->Complete(index, std::move(page.Page()), EPageFetchCompletion::Ready);
                    break;
                case ECacheResultStatus::Inserted:
                    toFetch.push_back(std::move(page.Fetch()));
                    [[fallthrough]];
                case ECacheResultStatus::Pending:
                    if (!QueueForPriority(admission.Priority) && page.Status() == ECacheResultStatus::Pending) {
                        if (TPageFetch promoted =
                                TakeQueuedPage(admission.PageCollection->Label(), locations[index].Offset)) {
                            toFetch.push_back(std::move(promoted));
                        }
                    }
                    if (recordStats) {
                        Counters.CacheMissPages->Inc();
                        Counters.CacheMissBytes->Add(bytes);
                        if (keepPages) {
                            Counters.CacheMissInMemoryPages->Inc();
                            Counters.CacheMissInMemoryBytes->Add(bytes);
                        }
                    }
                    break;
                case ECacheResultStatus::Miss:
                    Y_UNREACHABLE();
            }
        }
        admission.AdmissionIndices = std::move(unresolved);
        if (toFetch) {
            QueueFetch(admission.PageCollection, std::move(toFetch), admission.Priority, admission.Sender,
                NWilson::TTraceId(admission.TraceId), admission.Completion);
        }
        return admission.AdmissionIndices.empty() && !retryPreload;
    }

    void RecordPendingInMemoryPages(TCollection& collection, TArrayRef<const TPageLocation> locations) {
        const auto& pageCollection = *collection.PageCollection();
        for (const auto& location : locations) {
            if (pageCollection.Total() != pageCollection.MetaPages() &&
                !NPageCollection::IsDeadPage(location.Type, pageCollection.SkipBTreeIndexV1Shadow())) {
                PendingInMemoryPages[collection.Id()].emplace(location);
            }
        }
    }

    void QueueFetch(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, TVector<TPageFetch>&& pages,
        NBlockIO::EPriority priority, const TActorId& sender, NWilson::TTraceId traceId,
        TIntrusivePtr<TRequestCompletion> completion) {
        auto waiting = MakeHolder<TQueuedRequest>();
        waiting->Sender = sender;
        waiting->Completion = completion;
        waiting->PageCollection = collection;
        waiting->Priority = priority;
        waiting->TraceId = NWilson::TTraceId(traceId);
        TVector<TPageFetch> ready;
        const bool reserved = Cache->TryReserveFetchBatch(pages);

        for (auto &page : pages) {
            if (reserved) {
                ready.push_back(std::move(page));
            } else {
                waiting->Pages.push_back(std::move(page));
            }
        }
        if (waiting->Pages) {
            if (RestartBlockedWalk(*waiting)) {
                for (auto page = waiting->Pages.begin(); page != waiting->Pages.end();) {
                    if (page->HasActiveWaiters()) {
                        ++page;
                    } else {
                        page = waiting->Pages.erase(page);
                    }
                }
            }
            if (waiting->Pages) {
                ResourceWaits.push_back(std::move(waiting));
            }
        }
        if (!ready) {
            return;
        }
        if (auto* queue = QueueForPriority(priority)) {
            auto queued = MakeHolder<TQueuedRequest>();
            queued->Sender = sender;
            queued->Completion = std::move(completion);
            queued->PageCollection = collection;
            queued->Priority = priority;
            queued->TraceId = std::move(traceId);
            for (auto& page : ready) {
                queued->Pages.push_back(std::move(page));
            }
            queue->Requests[sender].push_back(std::move(queued));
            RequestFromQueue(*queue);
        } else {
            StartFetch(collection, std::move(ready), priority, sender, std::move(traceId));
        }
    }

    void PostponeTransaction(
        const TIntrusivePtr<NPageCollection::TPagesWaitPad>& pad, THolder<TQueuedRequest> gate = {}) {
        const bool createGate = !std::exchange(pad->PostponedForResources, true);
        for (auto& [_, request] : Requests) {
            if (!request.Cancelled && request.Completion->WaitPad() == pad) {
                if (createGate && !gate) {
                    gate = MakeHolder<TQueuedRequest>();
                    gate->Completion = request.Completion;
                    gate->Sender = request.Sender;
                    gate->PageCollection = request.Completion->PageCollection();
                }
                if (createGate) {
                    gate->HandleRequirements.push_back(request.Completion);
                }
                request.Cancelled = true;
                request.Completion->PostponeForResources();
            }
        }
        PruneQueues();
        if (createGate && gate) {
            gate->WaitingTransaction = true;
            ResourceWaits.push_back(std::move(gate));
        }
    }

    bool PrepareAdmission(const TQueuedRequest& waiting) {
        TVector<TCacheHandleRequirement> requirements;
        TVector<TPageLocation> locations;
        ui64 bytes = 0;
        if (waiting.WaitingTransaction) {
            const auto& pad = waiting.Completion->WaitPad();
            bytes = pad->WorkingSetBytes;
            requirements.reserve(pad->WorkingSetPages.size() + waiting.HandleRequirements.size());
            for (const auto& [collectionId, locations] : pad->WorkingSetPages) {
                requirements.push_back({ collectionId, locations });
            }
            for (const auto& completion : waiting.HandleRequirements) {
                requirements.push_back({ completion->PageCollection()->Label(), completion->Locations() });
            }
        } else {
            locations.reserve(waiting.AdmissionIndices.size());
            for (const ui32 index : waiting.AdmissionIndices) {
                locations.push_back(waiting.Completion->Locations()[index]);
            }
            requirements.push_back({ waiting.PageCollection->Label(), locations });
        }
        bool progress = true;
        for (ui32 step = 0;; ++step) {
            // Recheck both snapshots: reclamation can evict a required resident page
            const bool handlesReady = Cache->CanAllocateHandles(requirements);
            if (handlesReady && Cache->CanReserve(bytes)) {
                return true;
            }
            if (!progress) {
                return false;
            }
            if (step == SharedCacheTransitionWorkBatch) {
                Send(SelfId(), new TEvResourcesAvailable);
                return false;
            }
            progress = (!handlesReady && Cache->ReclaimSpareHandle()) || Cache->ReclaimForAdmission(bytes);
        }
    }

    bool WorkingSetTooLarge(const TQueuedRequest& waiting) {
        const auto& pad = waiting.Completion->WaitPad();
        if (!pad || pad->RetainedCollections.empty()) {
            return false;
        }
        TVector<TCacheHandleRequirement> requirements;
        for (const auto& [id, pages] : pad->WorkingSetPages) {
            requirements.push_back({ id, pages });
        }
        return Cache->WorkingSetExceedsCapacity(pad->RetainedCollections, requirements);
    }

    void RetryResourceWaits() {
        if (!Cache) {
            return;
        }
        const ui64 generation = Cache->AdmissionGeneration();
        if (CollectionLoads) {
            CollectionLoads->Pump();
        }
        const size_t count = ResourceWaits.size();
        // Demand promotion during admission can remove another waiting entry
        for (size_t index = 0; index < count && !ResourceWaits.empty(); ++index) {
            auto waiting = std::move(ResourceWaits.front());
            ResourceWaits.pop_front();
            if (waiting->WaitingTransaction) {
                if (PrepareAdmission(*waiting)) {
                    waiting->Completion->NotifyResourcesReady();
                } else if (WorkingSetTooLarge(*waiting)) {
                    waiting->Completion->NotifyWorkingSetTooLarge();
                } else {
                    ResourceWaits.push_back(std::move(waiting));
                }
                continue;
            }
            if (!waiting->AdmissionIndices.empty()) {
                if (waiting->Completion->IsCancelled() || waiting->Completion->HasFailed()) {
                    TryAdmitPages(*waiting);
                    continue;
                }
                if (PrepareAdmission(*waiting) && TryAdmitPages(*waiting)) {
                    continue;
                }
                if (RestartBlockedWalk(*waiting)) {
                    TryAdmitPages(*waiting);
                    continue;
                }
                const auto& pad = waiting->Completion->WaitPad();
                if (pad && (pad->HasPinnedPages.load(std::memory_order_acquire) || !pad->RetainedCollections.empty()) &&
                    !pad->PostponedForResources) {
                    PostponeTransaction(pad, std::move(waiting));
                } else {
                    ResourceWaits.push_back(std::move(waiting));
                }
                continue;
            }
            TVector<TPageFetch> pages;
            while (waiting->Pages) {
            auto page = std::move(waiting->Pages.front());
                waiting->Pages.pop_front();
                if (page.HasActiveWaiters()) {
                    pages.push_back(std::move(page));
                }
            }
            if (pages) {
                const size_t before = ResourceWaits.size();
                QueueFetch(waiting->PageCollection, std::move(pages), waiting->Priority, waiting->Sender,
                    std::move(waiting->TraceId), waiting->Completion);
                if (ResourceWaits.size() > before) {
                    const auto& blocked = *ResourceWaits.back();
                    for (const auto& page : blocked.Pages) {
                        page.ForEachActiveWaiter([&](const TPageFetchWaiter& waiter) {
                            const auto* requestWaiter = dynamic_cast<const TRequestPageWaiter*>(&waiter);
                            if (!requestWaiter) {
                                return;
                            }
                            const auto& pad = requestWaiter->GetCompletion().WaitPad();
                            if (!pad || pad->PostponedForResources) {
                                return;
                            }
                            const bool held = pad->HasPinnedPages.load(std::memory_order_acquire) ||
                                              !pad->RetainedCollections.empty();
                            if (held) {
                                PostponeTransaction(pad);
                            }
                        });
                    }
                }
            }
        }
        if (ResourceWaits.empty() && (!CollectionLoads || !CollectionLoads->HasResourceWaits())) {
            Cache->DisarmResourceWait();
        } else {
            // Arm after observing the generation before reservation attempts; a concurrent release wakes us.
            Cache->ArmResourceWait(generation);
        }
    }

    void Handle(TEvResourcesAvailable::TPtr&, const TActorContext&) {
        RetryResourceWaits();
    }

    static ui64 KeepPageBytes(const TPageLocation& location) {
        Y_ENSURE(location.Size <= Max<ui64>() - NActors::TSharedData::OverheadSize);
        return location.Size + NActors::TSharedData::OverheadSize;
    }

    ui64 KeepRemainingBytes() const {
        const ui64 limit = Config.GetInMemoryInFlyLimit();
        return limit - Min(limit, KeepPreloadBytes);
    }

    bool IsKeepPreloadInFlight(const TCollection& collection, TPageOffset offset) const {
        const ui64 generation = collection.KeepGeneration();
        for (const auto& [_, request] : collection.GetActorState()->KeepPreloadRequests) {
            if (request.Generation != generation) {
                continue;
            }
            const auto found = LowerBound(request.Locations.begin(), request.Locations.end(), offset,
                [](const TPageLocation& location, TPageOffset value) {
                    return location.Offset < value;
                });
            if (found != request.Locations.end() && found->Offset == offset) {
                return true;
            }
        }
        return false;
    }

    void RequestKeepPages(TCollection& collection,
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TVector<TPageLocation>&& locations,
        NBlockIO::EPriority priority) {
        Y_ENSURE(locations);
        Y_DEBUG_ABORT_UNLESS(IsSorted(locations.begin(), locations.end()));
        ui64 chargedBytes = 0;
        for (const auto& location : locations) {
            const ui64 bytes = KeepPageBytes(location);
            Y_ENSURE(chargedBytes <= Max<ui64>() - bytes);
            chargedBytes += bytes;
        }

        const ui64 requestId = NextKeepPreloadRequestId++;
        auto [requestIt, inserted] = collection.GetActorState()->KeepPreloadRequests.emplace(
            requestId, TKeepPreloadRequest{
                           .Generation = collection.KeepGeneration(),
                           .ChargedBytes = chargedBytes,
                           .Locations = locations,
                       });
        Y_ENSURE(inserted);
        Y_ENSURE(KeepPreloadBytes <= Max<ui64>() - chargedBytes);
        KeepPreloadBytes += chargedBytes;

        NSharedCache::TEvRequest request(priority, std::move(pageCollection), std::move(locations), requestId);
        requestIt->second.Admitted = RequestPages(SelfId(), KeepPreloadResultCookie, request);
    }

    void Handle(NSharedCache::TEvRequest::TPtr &ev, const TActorContext&) {
        auto*msg = ev->Get();
        if (ev->Cookie == ui64(ERequestTypeCookie::StickyPages)) {
            const auto* collection = FindCollection(msg->PageCollection->Label());
            if (!collection || !collection->GetActorState()->Owners.contains(ev->Sender)) {
                // A delayed executor notification must not attach a collection that owner has dropped.
                auto result = MakeHolder<TEvResult>(msg->PageCollection, NKikimrProto::RACE, msg->Cookie);
                result->WaitPad = std::move(msg->WaitPad);
                Send(ev->Sender, result.Release(), 0, ev->Cookie);
                return;
            }
        }
        if (auto* collection = TryAttachCollection(msg->PageCollection, ev->Sender)) {
            RecordPendingInMemoryPages(*collection, msg->Pages);
        }
        RequestPages(ev->Sender, ev->Cookie, *msg);
    }

    void Handle(NSharedCache::TEvCollectionBytesChanged::TPtr& ev, const TActorContext&) {
        const auto* msg = ev->Get();

        if (Cache->AcknowledgeCollectionBytes(msg->CacheItem)) {
            if (auto* collection = FindCollection(msg->CollectionId);
                collection && collection->CacheItem == msg->CacheItem) {
                UpdateCollectionStats(*collection);
            }
        }
    }

    void Handle(NSharedCache::TEvCollectionReleased::TPtr& ev, const TActorContext&) {
        const auto* msg = ev->Get();
        TCacheCollectionRef collection;
        const auto status = Cache->Find(msg->CollectionId, collection);
        if (status == ECacheResultStatus::Hit) {
            if (collection.CacheItem() != msg->CacheItem) {
                return;
            }
            if (!collection->GetActorState()) {
                // Metadata-only readers do not register an actor owner
                Cache->TryDetachReleasedCollection(collection);
                return;
            }
            if (collection->GetActorState()->Owners.empty() && collection.IsSoleReference()) {
                collection->DrainStickyPages();
            }

            if (!collection->HasPageItems()) {
                TryDropExpiredCollection(*collection);
            }
        } else if (status == ECacheResultStatus::Miss) {
            // A late release from an older generation must not remove a replacement's offsets.
            PendingInMemoryPages.erase(msg->CollectionId);
            KeepPreloadRetryBlocked.erase(msg->CollectionId);
        }
    }

    void Handle(NSharedCache::TEvRequestAnswered::TPtr& ev, const TActorContext&) {
        auto requestIt = Requests.find(ev->Get()->CompletionId);
        Y_ENSURE(requestIt != Requests.end());
        const TLogoBlobID collectionId = requestIt->second.CollectionId;
        const bool collectionTracked = requestIt->second.CollectionTracked;
        Requests.erase(requestIt);
        if (auto* collection = FindCollection(collectionId); collection && collectionTracked) {
            Y_ENSURE(collection->GetActorState()->PendingRequests != 0);
            --collection->GetActorState()->PendingRequests;
            TryDropExpiredCollection(*collection);
        }
        Counters.PendingRequests->Dec();
        if (ev->Get()->Status == NKikimrProto::ERROR) {
            PruneQueues();
        }
        RetryResourceWaits();
        if (ev->Get()->Status == NKikimrProto::OK) {
            Counters.SucceedRequests->Inc();
        } else {
            Counters.FailedRequests->Inc();
        }
    }

    void Handle(NSharedCache::TEvKeepPageEvicted::TPtr& ev, const TActorContext&) {
        const auto* msg = ev->Get();
        if (auto *collection = FindCollection(msg->CollectionId);
            collection && collection->CacheItem == msg->CacheItem &&
            (collection->GetCacheMode() == ECacheMode::TryKeepInMemory) &&
            collection->GetActorState()->InMemoryOwners && collection->KeepGeneration() == msg->Generation)
        {
            PendingInMemoryPages[msg->CollectionId].emplace(msg->Location);
        }
    }

    void Handle(NSharedCache::TEvSync::TPtr &, const TActorContext&) {
        //  references carry their own lifetime; private-page sync is obsolete after attach.
    }

    void Handle(NSharedCache::TEvUnregister::TPtr &ev, const TActorContext& ctx) {
        Y_DEFER {
            if (CollectionLoads) {
                RetryResourceWaits();
            }
        };
        if (CollectionLoads) {
            CollectionLoads->CancelRequests(ev->Sender);
        }
        CancelRequests({}, ev->Sender);
        auto ownerIt = Owners.find(ev->Sender);
        if (ownerIt == Owners.end()) {
            return;
        }
        const auto entry = std::move(ownerIt->second);
        Owners.erase(ownerIt);
        Counters.Owners->Dec();
        for (const auto& registration : entry.Collections) {
            auto* collection = registration.Ref.Get();
            RemoveOwnerStats(*collection, ev->Sender, entry, registration);
            Y_ENSURE(collection->GetActorState()->Owners.erase(ev->Sender));
            collection->GetActorState()->StickyOwners.erase(ev->Sender);
            Counters.PageCollectionOwners->Dec();

            TryMoveToRegularCache(*collection, ev->Sender);
            const bool resumedSticky = ApplyCollectionMode(*collection, ctx);
            const TLogoBlobID collectionId = collection->Id();
            Walks.UpdateSeeds(*collection, ev->Sender, {}, resumedSticky);
            if (auto* current = FindCollection(collectionId)) {
                if (current->GetActorState()->Owners.empty()) {
                    DetachCollection(*current);
                }
                TryDropExpiredCollection(*current);
            }
        }
    }

    void Handle(NSharedCache::TEvDetach::TPtr &ev, const TActorContext& ctx) {
        const TLogoBlobID collectionId = ev->Get()->PageCollectionId;
        Y_DEFER {
            if (CollectionLoads) {
                RetryResourceWaits();
            }
        };
        if (CollectionLoads) {
            CollectionLoads->CancelRequests(ev->Sender, collectionId);
        }
        CancelRequests(collectionId, ev->Sender);
        auto* collection = FindCollection(collectionId);

        if (!collection || !collection->GetActorState()->Owners.erase(ev->Sender)) {
            return;
        }
        auto ownerIt = Owners.find(ev->Sender);
        Y_ENSURE(ownerIt != Owners.end());
        const auto registration = ownerIt->second.Collections.find(collection->CacheItem);
        Y_ENSURE(registration != ownerIt->second.Collections.end());
        RemoveOwnerStats(*collection, ev->Sender, ownerIt->second, *registration);
        collection->GetActorState()->StickyOwners.erase(ev->Sender);
        ownerIt->second.Collections.erase(registration);
        Counters.PageCollectionOwners->Dec();

        TryMoveToRegularCache(*collection, ev->Sender);
        const bool resumedSticky = ApplyCollectionMode(*collection, ctx);
        Walks.UpdateSeeds(*collection, ev->Sender, {}, resumedSticky);
        if (auto* current = FindCollection(collectionId)) {
            if (current->GetActorState()->Owners.empty()) {
                DetachCollection(*current);
            }
            TryDropExpiredCollection(*current);
        }
    }

    void Handle(NSharedCache::TEvResult::TPtr& ev, const TActorContext&) {
        if (ev->Cookie == KeepPreloadResultCookie) {
            const ui64 requestId = ev->Get()->Cookie;
            auto* collection = FindCollection(ev->Get()->PageCollection->Label());
            Y_ENSURE(collection && collection->GetActorState());
            auto& requests = collection->GetActorState()->KeepPreloadRequests;
            auto requestIt = requests.find(requestId);
            Y_ENSURE(requestIt != requests.end());
            TKeepPreloadRequest request = std::move(requestIt->second);
            requests.erase(requestIt);
            Y_ENSURE(KeepPreloadBytes >= request.ChargedBytes);
            KeepPreloadBytes -= request.ChargedBytes;

            if ((collection->GetCacheMode() == ECacheMode::TryKeepInMemory) &&
                request.Generation == collection->KeepGeneration())
            {
                if (ev->Get()->Status == NKikimrProto::OK) {
                    Walks.IndexPagesChanged(collection->Id());
                } else if (!request.Admitted) {
                    auto& pending = PendingInMemoryPages[collection->Id()];
                    pending.insert(request.Locations.begin(), request.Locations.end());
                    KeepPreloadRetryBlocked.insert(collection->Id());
                } else {
                    if (auto pendingIt = PendingInMemoryPages.find(collection->Id());
                        pendingIt != PendingInMemoryPages.end())
                    {
                        for (const auto& location : request.Locations) {
                            pendingIt->second.erase(location);
                        }
                        if (pendingIt->second.empty()) {
                            PendingInMemoryPages.erase(pendingIt);
                        }
                    }
                    Walks.InvalidateDataCollection(collection->Id());
                    Walks.InvalidateIndexCollection(collection->Id());
                    for (const auto& owner : collection->GetActorState()->InMemoryOwners) {
                        NotifyInMemOwnerAboutError(collection->PageCollection(), ev->Get()->Status, owner);
                    }
                }
            }
            TryDropExpiredCollection(*collection);
            return;
        }

        Y_ENSURE(ev->Cookie == WalkResultCookie);
        const ui64 requestId = ev->Get()->Cookie;
        auto requestIt = WalkRequests.find(requestId);
        Y_ENSURE(requestIt != WalkRequests.end());
        const TLogoBlobID walkCollectionId = requestIt->second;
        WalkRequests.erase(requestIt);
        if (ev->Get()->Status != NKikimrProto::OK && ev->Get()->Status != NKikimrProto::RACE) {
            Walks.InvalidateRun(walkCollectionId);
        } else {
            Walks.IndexPagesChanged(ev->Get()->PageCollection->Label());
        }
        Walks.FinishFetch(walkCollectionId);
    }

    void Handle(NBlockIO::TEvData::TPtr &ev, const TActorContext&) {
        CompleteFetch(ev->Sender, *ev->Get());
    }

    void TryDropExpiredCollection(TCollection& collection) override {
        auto* state = collection.GetActorState();
        // Keep preload records until their results arrive, even after the request-answered notification.
        if (state && state->Owners.empty() && state->PendingRequests == 0 && state->KeepPreloadRequests.empty() &&
            Walks.IsIdle(collection.Id()))
        {
            Y_DEBUG_ABORT_UNLESS(state->InMemoryOwners.empty());
            const TLogoBlobID id = collection.Id();
            Walks.EraseCollection(id);
            KeepPreloadRetryBlocked.erase(id);
            // Detach releases the registry hold; no access to collection follows it.
            collection.ResetActorState();
            Y_ENSURE(Cache->DetachCollection(collection.CacheItem));
            Counters.PageCollections->Dec();
        }
    }

    void Wakeup(TKikimrEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
        if (tag != EWakeupTag::ContinueBTreeWalk) {
            LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Wakeup " << tag);
        }

        switch (tag) {
            case EWakeupTag::DoGCScheduled:
                // Periodic GC also retries OS mapping release and background capacity work
                KeepPreloadRetryBlocked.clear();
                ScheduleGC();
                break;
            case EWakeupTag::DoGCManual:
                break;
            case EWakeupTag::DoLimitMaintenance:
                LimitMaintenanceScheduled = false;
                ActualizeCacheSizeLimit();
                break;
            case EWakeupTag::ContinueBTreeWalk:
                WalkContinuationScheduled = false;
                break;
        }

        // DoGC will be called at the end of StateFunc
    }

    void ScheduleGC() {
        TActivationContext::AsActorContext().Schedule(TDuration::Seconds(15), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::DoGCScheduled)));
    }

    void NotifyInMemOwnerAboutError(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, NKikimrProto::EReplyStatus error, const TActorId& owner) {
        TAutoPtr<NSharedCache::TEvResult> result = new NSharedCache::TEvResult(std::move(pageCollection), error, 0);

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Send page collection error " << result->PageCollection->Label()
            << " owner " << owner
            << " class " << NBlockIO::EPriority::Bulk
            << " error " << error
            << " cookie " << static_cast<ui64>(ERequestTypeCookie::TryKeepInMemPages)
            << " (in-memory preload)");

        Send(owner, result.Release(), 0, static_cast<ui64>(ERequestTypeCookie::TryKeepInMemPages));
    }

    TRequestQueue* QueueForPriority(NBlockIO::EPriority priority) {
        switch (priority) {
            case NBlockIO::EPriority::Bkgr:
                return &AsyncRequests;
            case NBlockIO::EPriority::Bulk:
            case NBlockIO::EPriority::Low:
                return &ScanRequests;
            case NBlockIO::EPriority::None:
            case NBlockIO::EPriority::Fast:
                return nullptr;
        }
        Y_UNREACHABLE();
    }

    TPageFetch TakeQueuedPage(const TLogoBlobID& collectionId, TPageOffset offset) {
        for (TRequestQueue* queue : { &AsyncRequests, &ScanRequests }) {
            for (auto ownerIt = queue->Requests.begin(); ownerIt != queue->Requests.end(); ++ownerIt) {
                auto& requests = ownerIt->second;
                for (auto requestIt = requests.begin(); requestIt != requests.end(); ++requestIt) {
                    auto& request = **requestIt;
                    if (request.PageCollection->Label() != collectionId) {
                        continue;
                    }
                    for (auto pageIt = request.Pages.begin(); pageIt != request.Pages.end(); ++pageIt) {
        if (pageIt->Location().Offset != offset) {
                            continue;
                        }
                        TPageFetch page = std::move(*pageIt);
                        request.Pages.erase(pageIt);
                        if (request.Pages.empty()) {
                            requests.erase(requestIt);
                            if (requests.empty()) {
                                queue->Requests.erase(ownerIt);
                            }
                        }
                        return page;
                    }
                }
            }
        }
        for (auto requestIt = ResourceWaits.begin(); requestIt != ResourceWaits.end(); ++requestIt) {
            auto& request = **requestIt;
            if (request.PageCollection->Label() != collectionId) {
                continue;
            }
            for (auto pageIt = request.Pages.begin(); pageIt != request.Pages.end(); ++pageIt) {
                if (pageIt->Location().Offset != offset) {
                    continue;
                }
                TPageFetch page = std::move(*pageIt);
                request.Pages.erase(pageIt);
                if (request.Pages.empty() && request.AdmissionIndices.empty() && !request.WaitingTransaction) {
                    ResourceWaits.erase(requestIt);
                }
                return page;
            }
        }
        return {};
    }

    void RequestFromQueue(TRequestQueue& queue) {
        if (queue.Requests.empty()) {
            return;
        }
        auto it = queue.NextToRequest ? queue.Requests.find(queue.NextToRequest) : queue.Requests.begin();
        if (it == queue.Requests.end()) {
            it = queue.Requests.begin();
        }
        while (!queue.Requests.empty() && queue.InFly <= queue.Limit) {
            if (it == queue.Requests.end()) {
                it = queue.Requests.begin();
            }
            TQueuedRequest& request = *it->second.front();
            ui64 bytes = 0;
            ui32 count = 0;
            for (const TPageFetch& page : request.Pages) {
                const ui64 pageBytes = page.Size();
                Y_ENSURE(bytes <= Max<ui64>() - pageBytes && queue.InFly <= Max<ui64>() - bytes - pageBytes);
                bytes += pageBytes;
                ++count;
                if (queue.InFly + bytes > queue.Limit) {
                    break;
                }
            }
            Y_ENSURE(count != 0);
            TVector<TPageFetch> batch;
            batch.reserve(count);
            for (ui32 index = 0; index < count; ++index) {
                batch.push_back(std::move(request.Pages.front()));
                request.Pages.pop_front();
            }
            Y_ENSURE(queue.InFly <= Max<ui64>() - bytes);
            queue.InFly += bytes;
            StartFetch(request.PageCollection, std::move(batch), request.Priority, request.Sender,
                NWilson::TTraceId(request.TraceId), queue.Cookie);
            if (request.Pages.empty()) {
                it->second.pop_front();
                if (it->second.empty()) {
                    it = queue.Requests.erase(it);
                    continue;
                }
            }
            ++it;
        }
        queue.NextToRequest = it == queue.Requests.end() ? TActorId() : it->first;
    }

    void PruneQueues() {
        for (const auto& waiting : ResourceWaits) {
            if (!waiting->WaitingTransaction && !waiting->AdmissionIndices.empty() &&
                (waiting->Completion->IsCancelled() || waiting->Completion->HasFailed())) {
                TryAdmitPages(*waiting);
            }
        }
        for (TRequestQueue* queue : { &AsyncRequests, &ScanRequests }) {
            for (auto it = queue->Requests.begin(); it != queue->Requests.end();) {
                auto& requests = it->second;
                for (auto requestIt = requests.begin(); requestIt != requests.end();) {
                    auto& queued = **requestIt;
                    for (auto pageIt = queued.Pages.begin(); pageIt != queued.Pages.end();) {
                        if (pageIt->HasActiveWaiters()) {
                            ++pageIt;
        } else {
                            pageIt = queued.Pages.erase(pageIt);
                        }
                    }
                    if (queued.Pages.empty()) {
                        requestIt = requests.erase(requestIt);
                    } else {
                        ++requestIt;
                    }
                }
                it = requests.empty() ? queue->Requests.erase(it) : ++it;
            }
        }
    }

    void StartFetch(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection,
        TVector<TPageFetch>&& pages, NBlockIO::EPriority priority, const TActorId& statActor,
        NWilson::TTraceId traceId, EBlockIOFetchTypeCookie queueCookie = EBlockIOFetchTypeCookie::NoQueue) {
        Y_ENSURE(pages);
        TVector<TPageLocation> locations;
        locations.reserve(pages.size());
        ui64 bytes = 0;
        for (TPageFetch& page : pages) {
            const TPageLocation location = page.Location();
            Y_ENSURE(bytes <= Max<ui64>() - location.Size);
            bytes += location.Size;
            locations.push_back(location);
            Y_ENSURE(page.Dispatch());
        }
        auto* request = new NBlockIO::TEvFetch(priority, pageCollection, std::move(locations), bytes);
        request->TraceId = std::move(traceId);
        const TLogoBlobID collectionId = pageCollection->Label();
        const ui32 pageCount = pages.size();
        Counters.LoadInFlyPages->Add(pageCount);
        Counters.LoadInFlyBytes->Add(bytes);
        const TActorId fetchActor =
            NBlockIO::Start(this, statActor, static_cast<ui64>(EBlockIOFetchTypeCookie::Fetch), request);
        Y_ENSURE(Fetches
                     .emplace(fetchActor,
                         TFetch{ .CollectionId = collectionId, .Bytes = bytes, .QueueCookie = queueCookie,
                             .Pages = std::move(pages) })
                     .second);
    }

    void CompleteFetch(const TActorId& fetchActor, NBlockIO::TEvData& data) {
        auto fetchIt = Fetches.find(fetchActor);
        if (fetchIt == Fetches.end()) {
            return; // stale or duplicate completion from an already resolved fetch actor
        }
        TFetch fetch = std::move(fetchIt->second);
        Fetches.erase(fetchIt);
        Counters.LoadInFlyPages->Sub(fetch.Pages.size());
        Counters.LoadInFlyBytes->Sub(fetch.Bytes);

        const bool valid = data.Status == NKikimrProto::OK && data.PageCollection->Label() == fetch.CollectionId &&
                           data.Pages.size() == fetch.Pages.size();
        bool failed = false;
        for (ui32 index = 0; index < fetch.Pages.size(); ++index) {
            TPageFetch& page = fetch.Pages[index];
            if (valid && data.Pages[index].Offset == page.Location().Offset &&
                data.Pages[index].Data.size() == page.Size())
            {
                page.MakeReady(std::move(data.Pages[index].Data));
            } else {
                failed = true;
                page.FailReady();
                }
            }

            if (failed) {
                PruneQueues();
            }
            if (fetch.QueueCookie == EBlockIOFetchTypeCookie::AsyncQueue) {
            Y_ENSURE(AsyncRequests.InFly >= fetch.Bytes);
            AsyncRequests.InFly -= fetch.Bytes;
            RequestFromQueue(AsyncRequests);
        } else if (fetch.QueueCookie == EBlockIOFetchTypeCookie::ScanQueue) {
            Y_ENSURE(ScanRequests.InFly >= fetch.Bytes);
            ScanRequests.InFly -= fetch.Bytes;
            RequestFromQueue(ScanRequests);
        }

        ActualizeCacheSizeLimit();
    }

    bool ApplyCollectionMode(TCollection& collection, const TActorContext& ctx) {
        const bool keepPages = bool(collection.GetActorState()->InMemoryOwners);
        const bool stickyPages = !keepPages && bool(collection.GetActorState()->StickyOwners);
        const ECacheMode mode = keepPages     ? ECacheMode::TryKeepInMemory
                                : stickyPages ? ECacheMode::Sticky
                                              : ECacheMode::Regular;
        const ECacheMode previous = collection.GetCacheMode();
        if (mode == previous) {
            return false;
        }
        if (!Cache->SetCollectionPagesCacheMode(collection.CacheItem, mode)) {
            LOG_WARN_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE,
                "Cannot apply cache mode " << mode << " to page collection " << collection.Id());
            return false;
        }
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE,
            "Set page collection " << collection.Id() << " mode to " << mode);
        if (keepPages != (previous == ECacheMode::TryKeepInMemory)) {
            KeepPreloadRetryBlocked.erase(collection.Id());
        }
        return stickyPages && previous != ECacheMode::Sticky;
    }

    void TryMoveToRegularCache(TCollection& collection, const TActorId& owner) {
        if (!collection.GetActorState()->InMemoryOwners.erase(owner) ||
            collection.GetActorState()->InMemoryOwners) {
            return;
        }
        Y_ENSURE(TargetInMemoryBytes >= collection.TotalSize());
        TargetInMemoryBytes -= collection.TotalSize();
        Counters.TargetInMemoryBytes->Set(TargetInMemoryBytes);
    for (auto& [_, request] : Requests) {
        if (request.CollectionId == collection.Id() && request.EventCookie == KeepPreloadResultCookie) {
            request.Cancelled = true;
                request.Completion->Cancel();
            }
        }
    PruneQueues();
    ActualizeCacheSizeLimit();
        Walks.DropIndexOnlyWalks(collection.Id());
    }

    void TryMoveToTryKeepInMemoryCache(TCollection& collection,
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        if (!collection.GetActorState()->InMemoryOwners.insert(owner).second ||
            collection.GetActorState()->InMemoryOwners.size() != 1) {
            return;
        }
        Walks.RestartForIndexCollection(collection.Id());
        TargetInMemoryBytes += collection.TotalSize();
        Counters.TargetInMemoryBytes->Set(TargetInMemoryBytes);
        const bool skipShadow = pageCollection->SkipBTreeIndexV1Shadow();
        auto& pagesToLoad = PendingInMemoryPages[collection.Id()];
        for (const auto pageId : xrange(pageCollection->MetaPages())) {
            const auto type = static_cast<EPage>(pageCollection->Page(pageId).Type);
            if (!NPageCollection::IsDeadPage(type, skipShadow)) {
                pagesToLoad.emplace(pageCollection->GetLocation(pageId));
            }
        }
        ActualizeCacheSizeLimit();
    }

    // The walk submits Sticky preloads directly; results/errors retain their original owner.
    void RequestStickyCollectionPages(
        TCollection& collection, const TActorId& owner, const TVector<TPageLocation>& locations) {
        if (!collection.GetActorState()->Owners.contains(owner)) {
            return;
        }

        auto send = [&](auto first, auto last) {
            if (first == last) {
                return;
            }

            TVector<TPageLocation> batch(first, last);
            TEvRequest request(NBlockIO::EPriority::Bkgr, collection.PageCollection(), std::move(batch));
        RequestPages(owner, ui64(ERequestTypeCookie::StickyPages), request);
    };

        for (auto it = locations.begin(); it != locations.end(); ) {
            auto last = it + Min<size_t>(MaxPreloadBatchLocations, locations.end() - it);
            send(it, last);
            it = last;
        }
    }

    void TryLoadInMemoryCollections() {
        if (PendingInMemoryPages.empty()) {
            return;
        }

        const ui64 committedKeepBytes = LastKeepResidentBytes + KeepPreloadBytes;
        const ui64 targetRoom = TargetInMemoryBytes > committedKeepBytes ? TargetInMemoryBytes - committedKeepBytes : 0;
        ui64 remainingBytes = Min(KeepRemainingBytes(), targetRoom);
        ui64 remainingHandles = Cache->AvailableHandleCount();
        THashSet<TLogoBlobID> waitingForInFlight;

        while (PendingInMemoryPages) {
            auto it = PendingInMemoryPages.begin();
            while (it != PendingInMemoryPages.end()) {
                const TCollection* candidate = FindCollection(it->first);
            // Routed Regular collections retain known hidden offsets until a Keep owner appears.
            if (!KeepPreloadRetryBlocked.contains(it->first) && !waitingForInFlight.contains(it->first) &&
                candidate && candidate->GetActorState()->InMemoryOwners)
                {
                    break;
                }
                ++it;
            }
            if (it == PendingInMemoryPages.end()) {
                break;
            }

            const TLogoBlobID collectionId = it->first;
            auto& pagesToLoad = it->second;

            auto* collection = FindCollection(collectionId);
            if (!collection) {
                PendingInMemoryPages.erase(it);
                continue;
            }

            if (!(collection->GetCacheMode() == ECacheMode::TryKeepInMemory)) {
                PendingInMemoryPages.erase(it);
                continue;
            }

            const ui64 pageLimit =
                Min<ui64>(MaxPreloadBatchLocations, Cache->TargetHandleCount() - TSharedCache::RequestHandleOverhead);
            TVector<TPageLocation> pagesToRequest;
            ui64 requestBytes = 0;
            bool budgetBlocked = false;
            auto locationIt = pagesToLoad.begin();
            while (locationIt != pagesToLoad.end() && pagesToRequest.size() < pageLimit)
            {
                const TPageLocation location = *locationIt;
                ECacheResultStatus status;
                {
                    TCachePageRef page;
                    status = Cache->Find(collection->CacheItem, static_cast<ui64>(location.Offset), page);
            }
            if (status == ECacheResultStatus::Hit) {
                locationIt = pagesToLoad.erase(locationIt);
                Walks.IndexPagesChanged(collectionId);
                ScheduleContinuation();
                continue;
            }
            if (status == ECacheResultStatus::Pending || IsKeepPreloadInFlight(*collection, location.Offset))
            {
                    ++locationIt;
                    continue;
                }
                const ui64 pageBytes = KeepPageBytes(location);
                // Existing hits may acquire Keep credits; exclude this batch's unsubmitted promises
                const auto room = Cache->KeepLoadRoom();
                if (pageBytes > remainingBytes || pageBytes > room.Bytes - Min(room.Bytes, requestBytes) ||
                    pagesToRequest.size() >= room.Pages) {
                    budgetBlocked = true;
                    break;
                }
                if (remainingHandles == 0) {
                    const ui64 available = Cache->AvailableHandleCount();
                    remainingHandles = available - Min<ui64>(available, pagesToRequest.size());
                }
                if (remainingHandles == 0 && (Cache->ReclaimSpareHandle() || Cache->ReclaimForPreload())) {
                    const ui64 available = Cache->AvailableHandleCount();
                    remainingHandles = available - Min<ui64>(available, pagesToRequest.size());
                }
                if (remainingHandles == 0) {
                    budgetBlocked = true;
                    break;
                }
                remainingBytes -= pageBytes;
                --remainingHandles;
                requestBytes += pageBytes;
                pagesToRequest.push_back(location);
                locationIt = pagesToLoad.erase(locationIt);
            }

            // A trailing hit may have adopted a Keep page after the last missing-page probe
            const auto room = Cache->KeepLoadRoom();
            while (pagesToRequest.size() > room.Pages || requestBytes > room.Bytes) {
                const auto location = pagesToRequest.back();
                pagesToRequest.pop_back();
                requestBytes -= KeepPageBytes(location);
                pagesToLoad.emplace(location);
                budgetBlocked = true;
            }

            const bool submitted = bool(pagesToRequest);

            if (submitted) {
            RequestKeepPages(
                *collection, collection->PageCollection(), std::move(pagesToRequest), NBlockIO::EPriority::Bulk);
            }

            if (pagesToLoad) {
                if (!submitted && !budgetBlocked) {
                    waitingForInFlight.insert(collectionId);
                    continue;
                }
                break;
            }

            PendingInMemoryPages.erase(it);
            continue;
        }
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        {
            auto* appData = AppData(ctx);
            NKikimrSharedCache::TSharedCacheConfig config;
            if (record.GetConfig().HasBootstrapConfig() && record.GetConfig().GetBootstrapConfig().HasSharedCacheConfig()) {
                config.MergeFrom(record.GetConfig().GetBootstrapConfig().GetSharedCacheConfig());
            } else if (appData->BootstrapConfig.HasSharedCacheConfig()) {
                config.MergeFrom(appData->BootstrapConfig.GetSharedCacheConfig());
            }
            if (record.GetConfig().HasSharedCacheConfig()) {
                config.MergeFrom(record.GetConfig().GetSharedCacheConfig());
            } else {
                config.MergeFrom(appData->SharedCacheConfig);
            }
            Y_ENSURE(!config.HasMemoryLimit() || config.GetMemoryLimit() > 0,
                "Shared-cache MemoryLimit must be positive");
            LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Update config " << config.ShortDebugString());
            Config.Swap(&config);
        }

    LimitReady = ControllerMaxBytes || Config.HasMemoryLimit() || bool(Cache);
    ActualizeCacheSizeLimit();
        ReplayDeferredEvents();

        AsyncRequests.Limit = Config.GetAsyncQueueInFlyLimit();
        ScanRequests.Limit = Config.GetScanQueueInFlyLimit();
        RequestFromQueue(AsyncRequests);
        RequestFromQueue(ScanRequests);
    }

public:
    TSharedPageCache(const TSharedCacheConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Config(config)
        , Counters(counters)
    {
        Y_ENSURE(
            !Config.HasMemoryLimit() || Config.GetMemoryLimit() > 0, "Shared-cache MemoryLimit must be positive");
        AsyncRequests.Limit = Config.GetAsyncQueueInFlyLimit();
        ScanRequests.Limit = Config.GetScanQueueInFlyLimit();
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Bootstrap with config " << Config.ShortDebugString());

        LimitReady = Config.HasMemoryLimit();
        MemLimitBytes = Config.GetMemoryLimit();
        if (LimitReady) {
            ActualizeCacheSizeLimit();
        }

        Send(NMemory::MakeMemoryControllerId(), new NMemory::TEvConsumerRegister(NMemory::EMemoryConsumerKind::SharedCache));

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
                NKikimrConsole::TConfigItem::BootstrapConfigItem, NKikimrConsole::TConfigItem::SharedCacheConfigItem}));

        Become(&TThis::StateFunc);

        ScheduleGC();
    }

    STFUNC(StateFunc) {
        if (!LimitReady && ev->GetTypeRewrite() != NMemory::EvConsumerRegistered &&
            ev->GetTypeRewrite() != NMemory::EvConsumerLimit &&
            ev->GetTypeRewrite() != NConsole::TEvConsole::EvConfigNotificationRequest &&
            ev->GetTypeRewrite() != TEvents::TSystem::PoisonPill)
        {
            DeferredEvents.emplace_back(ev.Release());
            return;
        }
        switch (ev->GetTypeRewrite()) {
            HFunc(NSharedCache::TEvCacheReadyRequest, Handle);
            HFunc(NSharedCache::TEvCollectionRequest, Handle);
            HFunc(NSharedCache::TEvCollectionRequestUpdate, Handle);
            HFunc(NSharedCache::TEvCollectionRawReady, Handle);
            HFunc(NSharedCache::TEvCollectionData, Handle);
            HFunc(NSharedCache::TEvAttach, Handle);
            HFunc(NSharedCache::TEvSaveCompactedPages, Handle);
            HFunc(NSharedCache::TEvRequest, Handle);
            HFunc(NSharedCache::TEvRequestAnswered, Handle);
            HFunc(NSharedCache::TEvCollectionReleased, Handle);
            HFunc(NSharedCache::TEvCollectionBytesChanged, Handle);
            HFunc(NSharedCache::TEvKeepPageEvicted, Handle);
            HFunc(NSharedCache::TEvResourcesAvailable, Handle);
            HFunc(NSharedCache::TEvResult, Handle);
            HFunc(NSharedCache::TEvSync, Handle);
            HFunc(NSharedCache::TEvUnregister, Handle);
            HFunc(NSharedCache::TEvDetach, Handle);

            HFunc(NBlockIO::TEvData, Handle);
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(TKikimrEvents::TEvWakeup, Wakeup);
            CFunc(TEvents::TSystem::PoisonPill, TakePoison);

            HFunc(NMemory::TEvConsumerRegistered, Handle);
            HFunc(NMemory::TEvConsumerLimit, Handle);
            case NSharedCache::TEvCollectionReady::EventType:
                if (!CollectionLoads) {
                    return;
                }
                CollectionLoads->Ready(ev->Get<TEvCollectionReady>()->CollectionId);
                RetryResourceWaits();
                break;
        }

        // Advance is a no-op without new work; FinishReady only checks runs whose state changed.
        // This ordering is load-bearing: walks parse arrivals before GC and may become Draining
        // after queueing data pages; the loader must submit those pages before completion is checked.
        if (Walks.HasActiveWalks()) {
            Walks.Advance();
        }
        DoGC();
        TryLoadInMemoryCollections();
        if (Walks.HasActiveWalks()) {
            Walks.FinishReady();
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SAUSAGE_CACHE;
    }
};
} // namespace

IActor* CreateSharedPageCache(
    const TSharedCacheConfig& config,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters
) {
    return new TSharedPageCache(config,
        GetServiceCounters(counters, "tablets")->GetSubgroup("type", "S_CACHE"));
}

} // namespace NKikimr::NSharedCache

template<> inline
void Out<TVector<ui32>>(IOutputStream& o, const TVector<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<TVector<NKikimr::NSharedCache::TPageOffset>>(IOutputStream& o, const TVector<NKikimr::NSharedCache::TPageOffset> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<TDeque<ui32>>(IOutputStream& o, const TDeque<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<THashSet<ui32>>(IOutputStream& o, const THashSet<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<TVector<NKikimr::NSharedCache::TEvResult::TLoaded>>(IOutputStream& o, const TVector<NKikimr::NSharedCache::TEvResult::TLoaded> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x.Offset << ' ';
    o << "]";
}

template<> inline
void Out<TVector<NKikimr::NPageCollection::TPageData>>(IOutputStream& o, const TVector<NKikimr::NPageCollection::TPageData> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x.Location << ' ';
    o << "]";
}

template<> inline
void Out<TVector<NKikimr::NPageCollection::TLoadedPageData>>(IOutputStream& o, const TVector<NKikimr::NPageCollection::TLoadedPageData> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x.Offset << ' ';
    o << "]";
}
