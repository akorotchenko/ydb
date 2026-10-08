#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "shared_cache.h"
#include "shared_cache_btree_walk.h"
#include "shared_cache_events.h"
#include "shared_sausagecache_state.h"
#include "shared_cache_pages.h"
#include "shared_cache_counters.h"
#include "shared_sausagecache.h"
#include "util_fmt_abort.h"
#include <util/stream/format.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/algorithm.h>
#include <util/generic/set.h>

namespace NKikimr::NSharedCache {

using namespace NTabletFlatExecutor;
using TPageLocation = NTable::NPage::TPageLocation;

::NFormatPrivate::THumanReadableSize HumanReadableBytes(ui64 bytes) {
    return HumanReadableSize(bytes, SF_BYTES);
}

enum class EBlockIOFetchTypeCookie {
    NoQueue = 1,
    AsyncQueue = 2,
    ScanQueue = 3,
    CoreFetch = 5,
};

struct TCoreFetch {
    TLogoBlobID CollectionId;
    ui64 Bytes = 0;
    EBlockIOFetchTypeCookie QueueCookie = EBlockIOFetchTypeCookie::NoQueue;
    TVector<TPageFetch> Pages;
};

struct TCoreRequest {
    TLogoBlobID CollectionId;
    TActorId Sender;
    ui64 EventCookie = 0;
    ui64 Cookie = 0;
    TIntrusivePtr<TRequestCompletion> Completion;
    bool Cancelled = false;
};

struct TRegisteredCollection {
    TSharedCacheCollectionRef Ref;
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

struct TCoreQueuedRequest {
    TActorId Sender;
    TIntrusivePtr<TRequestCompletion> Completion;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    NBlockIO::EPriority Priority;
    NWilson::TTraceId TraceId;
    TDeque<TPageFetch> Pages;
    bool WaitingTransaction = false;
};

struct TRequestQueue {
    explicit TRequestQueue(EBlockIOFetchTypeCookie cookie)
        : Cookie(cookie)
    {}

    EBlockIOFetchTypeCookie Cookie;

    TMap<TActorId, TDeque<THolder<TCoreQueuedRequest>>> Requests;

    ui64 Limit = 0;
    ui64 InFly = 0;

    TActorId NextToRequest;
};

namespace {

class TSharedPageCache : public TActorBootstrapped<TSharedPageCache>, private ICacheBTreeWalkHost {
    static constexpr ui8 CoreMinimumAddressBits = 8;
    static constexpr ui64 CoreWalkResultCookie = Max<ui64>();
    static constexpr ui64 CoreKeepPreloadResultCookie = Max<ui64>() - 1;

    TActorId Owner;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    NSharedCache::TSharedCachePages* SharedCachePages;
    TSharedCacheConfig Config;
    TSharedPageCacheCounters Counters;
    TIntrusivePtr<TSharedCache> CacheCore;
    TIntrusivePtr<TCollectionRegistry> CoreRegistry;
    TCacheBTreeWalkController Walks{ *this };
    TPendingInMemoryPages PendingInMemoryPages;
    THashMap<ui64, TLogoBlobID> CoreWalkRequests;
    ui64 KeepPreloadBytes = 0;
    THashSet<TLogoBlobID> CoreKeepPreloadRetryBlocked;
    // I/O owners stay here until the matching fetch actor completes.
    THashMap<TActorId, TCoreFetch> CoreFetches;
    // Each first owner registration holds one native metadata ref until detach/unregister.
    THashMap<TActorId, TCollectionOwner> Owners;
    THashMap<ui64, TCoreRequest> CoreRequests;
    ui64 NextCoreRequestId = 1;
    TDeque<THolder<TCoreQueuedRequest>> CoreMemoryWaits;
    bool ResourceRetryScheduled = false;
    TRequestQueue AsyncRequests{EBlockIOFetchTypeCookie::AsyncQueue};
    TRequestQueue ScanRequests{EBlockIOFetchTypeCookie::ScanQueue};

    ui64 LastCoreKeepResidentBytes = 0;

    ui64 MemLimitBytes = 0;
    ui64 ControllerMaxBytes = 0;
    ui64 ControllerCurrentBytes = 0;
    bool HasControllerAllocation = false;
    bool LimitReady = false;
    bool LimitMaintenanceScheduled = false;
    TVector<TAutoPtr<IEventHandle>> DeferredEvents;
    ui64 TargetInMemoryBytes = 0;
    bool WalkContinuationScheduled = false;
    ui64 NextCoreWalkRequestId = 1;
    ui64 NextCoreKeepPreloadRequestId = 1;

    TCacheCollection* FindCollection(const TLogoBlobID& id) {
        if (!CacheCore) {
            return nullptr;
        }
        auto binding = CacheCore->BindCurrentThreadHazard();
        TSharedCacheCollectionRef ref;
        if (CacheCore->Find(id, ref) != ESharedCacheResultStatus::Hit || !ref->GetActorState()) {
            return nullptr;
        }
        return ref.Get();
    }

    void ForEachCollection(TFunctionRef<void(TCacheCollection&)> callback) {
        if (!CacheCore) {
            return;
        }
        auto binding = CacheCore->BindCurrentThreadHazard();
        CacheCore->ForEachCollection(*CoreRegistry, [&](TCacheCollection& collection) {
            if (collection.GetActorState()) {
                callback(collection);
            }
        });
    }

    TCacheCollection* FindWalkCollection(const TLogoBlobID& id) override {
        return FindCollection(id);
    }

    TPendingInMemoryPages& PendingWalkPages() override {
        return PendingInMemoryPages;
    }

    void ScheduleWalkContinuation() override {
        if (!std::exchange(WalkContinuationScheduled, true)) {
            Send(SelfId(), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::ContinueBTreeWalk)));
        }
    }

    NActors::TSharedData FindCoreWalkPage(TCacheCollection& collection, TPageOffset offset) override {
        if (!collection.CacheItem) {
            return {};
        }
        auto binding = CacheCore->BindCurrentThreadHazard();
        TSharedCachePageRef page;
        if (CacheCore->Find(collection.CacheItem, static_cast<ui64>(offset), page) ==
            ESharedCacheResultStatus::Hit) {
            return page.BuildSharedData();
        }
        return {};
    }

    void FetchWalkIndexLevel(TCacheCollection& collection, TVector<TPageLocation>&& locations, const TLogoBlobID& walkCollectionId) override {
        Y_DEBUG_ABORT_UNLESS(collection.GetCacheMode() != ECacheMode::TryKeepInMemory);
        const ui64 requestId = NextCoreWalkRequestId++;
        Y_ENSURE(CoreWalkRequests.emplace(requestId, walkCollectionId).second);
        Walks.FetchStarted(walkCollectionId);
        NSharedCache::TEvRequest request(
            NBlockIO::EPriority::Bkgr, collection.PageCollection(), std::move(locations), requestId);
        RequestCorePages(SelfId(), CoreWalkResultCookie, request);
    }

    void RequestWalkStickyPages(
        TCacheCollection& collection, const TActorId& owner, const TVector<TPageLocation>& locations) override {
        RequestStickyCollectionPages(collection, owner, locations);
    }

    void CancelQueuedWalkRequestsAndPump(const TLogoBlobID& walkCollectionId) override {
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
        return CacheCore ? CacheCore->HardLimit() : 0;
    }

    ui64 GetTargetCacheCurrentLimitBytes() const {
        const ui64 maximum = GetTargetCacheHardLimitBytes();
        return ControllerCurrentBytes ? Min(ControllerCurrentBytes, maximum) : maximum;
    }

    void UpdateCoreSoftLimit() {
        if (HasControllerAllocation) {
            Y_ENSURE(CacheCore->UpdateSoftLimit(MemLimitBytes));
        } else {
            Y_ENSURE(CacheCore->UpdateSoftLimit(Max<ui64>()));
        }
    }

    bool InitializeCacheCore() {
        if (CacheCore) {
            return true;
        }
        constexpr ui32 HazardCount = NActors::MaxWorkers;
        // Small metadata pages can exhaust handles while the byte budget still has room.
        ui8 minimumAddressBits = Max<ui8>(MinSharedCacheAddressBits, CoreMinimumAddressBits);
        while (TSharedCacheCapacity{ .AddressBits = minimumAddressBits }.HotSlotCount() <
               SharedCachePolicyFor<TProdTraits>.MinHotSlots)
        {
            ++minimumAddressBits;
        }
        TSharedCacheCapacity minimum;
        if (!TryCalculateSharedCacheFootprint(
                minimumAddressBits, TSharedCache::ExpectedPageSize, 0, HazardCount, minimum))
        {
            return false;
        }
        const ui64 targetLimit = GetTargetCacheHardLimitBytes();
        const ui64 allocationLimit = Max(targetLimit, minimum.TotalBytes);
        TSharedCacheCapacity current;
        if (!TryCalculateSharedCacheCapacity(
                allocationLimit, TSharedCache::ExpectedPageSize, 0, HazardCount, current))
        {
            return false;
        }
        TSharedCacheCapacity reserved;
        if (!TryCalculateSharedCacheFootprint(
                MaxSharedCacheAddressBits, TSharedCache::ExpectedPageSize, 0, HazardCount, reserved)) {
            return false;
        }
        auto space = TSharedCacheSpace::Create(current, reserved, HazardCount);
        TProdTraits traits;
        traits.KeepEvictionActorSystem = TActivationContext::ActorSystem();
        traits.KeepEvictionActor = SelfId();
        CacheCore = TSharedCache::Create(std::move(space), targetLimit, std::move(traits));
        if (CacheCore) {
            Y_ENSURE(CacheCore->UpdateCurrentLimit(GetTargetCacheCurrentLimitBytes()));
            UpdateCoreSoftLimit();
            Y_ENSURE(CacheCore->UpdateStickyLimit(CacheCore->HardLimit()));
        }
        return bool(CacheCore);
    }

    TCacheCollection& EnsureCollection(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        const TLogoBlobID& pageCollectionId = pageCollection->Label();
        Y_ENSURE(pageCollectionId);
        Y_ENSURE(InitializeCacheCore(), "Cannot initialize shared-cache core");
        auto binding = CacheCore->BindCurrentThreadHazard();
        const TCollectionLocation location{ .Id = pageCollectionId, .BackingSize = pageCollection->BackingSize() };
        TCollectionCacheItem inserted;
        TSharedCacheCollectionRef ref;
        const auto status = CacheCore->FindOrInsert(*CoreRegistry, location, inserted, ref);
        if (status == ESharedCacheResultStatus::Inserted) {
            auto value = MakeHolder<TCacheCollection>(pageCollection, inserted);
            Y_ENSURE(CacheCore->MakeReady(*CoreRegistry, inserted, std::move(value)));
            Y_ENSURE(CacheCore->Find(pageCollectionId, ref) == ESharedCacheResultStatus::Hit);
        } else {
            Y_ENSURE(status == ESharedCacheResultStatus::Hit);
        }
        TCacheCollection& collection = *ref;
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
                Y_ENSURE(CacheCore->SetCollectionPagesCacheMode(collection.CacheItem, ECacheMode::Regular));
            }
        }
        return collection;
    }

    void UpdateCoreKeepColdLimit() {
        // The core applies the live 50% ceiling after excluding Sticky bytes.
        Y_ENSURE(CacheCore->UpdateKeepColdLimit(Min(TargetInMemoryBytes, CacheCore->HardLimit())));
    }

    void ActualizeCacheSizeLimit() {
        Counters.ConfigLimitBytes->Set(Config.HasMemoryLimit() ? Config.GetMemoryLimit() : 0);
        if (LimitReady && !CacheCore) {
            Y_ENSURE(InitializeCacheCore(), "Cannot initialize shared-cache core");
        }
        if (CacheCore) {
            const ui64 hardLimit = GetTargetCacheHardLimitBytes();
        const ui64 coreLimit = GetTargetCacheCurrentLimitBytes();
            if (hardLimit < CacheCore->CurrentLimit()) {
                Y_ENSURE(CacheCore->UpdateCurrentLimit(Min(coreLimit, hardLimit)));
            }
            if (hardLimit != CacheCore->HardLimit()) {
                Y_ENSURE(CacheCore->UpdateHardLimit(hardLimit),
                    "Shared-cache capacity change exceeds reserved space: requested "
                        << hardLimit << " reserved " << CacheCore->ReservationLimit());
                Y_ENSURE(CacheCore->UpdateStickyLimit(hardLimit));
            }
            Y_ENSURE(CacheCore->UpdateCurrentLimit(coreLimit));
            UpdateCoreSoftLimit();
            UpdateCoreKeepColdLimit();
            Counters.ActiveLimitBytes->Set(coreLimit);
        } else {
            Counters.ActiveLimitBytes->Set(GetTargetCacheCurrentLimitBytes());
        }
    }

    void DoGC() {
        if (CacheCore) {
            auto binding = CacheCore->BindCurrentThreadHazard();
            const ui64 staticBytes = CacheCore->StaticBytes();
            const bool pressure = CacheCore->RetainedBytes() > CacheCore->SoftLimit();
            const ui32 workLimit =
                pressure || CacheCore->NeedsCapacityMaintenance() ? SharedCacheTransitionWorkBatch : 1;
            bool moreWork = false;
            for (ui32 work = 0; work < workLimit; ++work) {
                const bool reclaimed = CacheCore->EnforceCurrentLimit();
                moreWork = CacheCore->RunMaintenance();
                if (!reclaimed && !moreWork) {
                    break;
                }
                if (!CacheCore->NeedsCapacityMaintenance() &&
                    CacheCore->RetainedBytes() <= CacheCore->SoftLimit()) {
                    break;
                }
            }
            if (moreWork && CacheCore->NeedsCapacityMaintenance() && !LimitMaintenanceScheduled) {
                LimitMaintenanceScheduled = true;
            Send(SelfId(), new TKikimrEvents::TEvWakeup(ui64(EWakeupTag::DoLimitMaintenance)));
        }
            if (staticBytes != CacheCore->StaticBytes()) {
                UpdateCoreKeepColdLimit();
            }

            Counters.ActivePages->Set(
                CacheCore->HotPages() + CacheCore->StickyPages() + CacheCore->KeepColdPages());
        Counters.ActiveBytes->Set(
                CacheCore->HotBytes() + CacheCore->StickyBytes() + CacheCore->KeepColdBytes());
            Counters.ActiveInMemoryBytes->Set(CacheCore->KeepActivePageBytes());
            Counters.PassivePages->Set(CacheCore->ColdPages());
            Counters.PassiveBytes->Set(CacheCore->ColdBytes());
        }
        LastCoreKeepResidentBytes = CacheCore ? CacheCore->KeepResidentPageBytes() : 0;
        if (MemoryConsumer) {
            MemoryConsumer->SetConsumption(CacheCore ? CacheCore->OverallUsage() : 0);
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
        Y_ENSURE(ControllerMaxBytes || Config.HasMemoryLimit() || CacheCore,
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

        SharedCachePages = sys->AppData<TAppData>()->SharedCachePages.Get();
        CoreRegistry = SharedCachePages->Registry;
    }

    void DetachCoreCollection(TCacheCollection& collection) {
        CancelCoreRequests(collection.Id());
        auto binding = CacheCore->BindCurrentThreadHazard();
        Y_ENSURE(CacheCore->SetCollectionPagesCacheMode(collection.CacheItem, ECacheMode::Regular));
        // The registry keeps the unified object alive until replies and walks drain.
        ActualizeCacheSizeLimit();
    }

    void TakePoison(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Poison shared cache");
        for (auto& [_, request] : CoreRequests) {
            request.Completion->Cancel();
        }
        AsyncRequests.Requests.clear();
        ScanRequests.Requests.clear();
        CoreMemoryWaits.clear();
        CoreFetches.clear();
        if (CacheCore) {
            auto binding = CacheCore->BindCurrentThreadHazard();
            ForEachCollection([&](TCacheCollection& collection) {
                collection.ResetActorState();
                Y_ENSURE(CacheCore->DetachCollection(*CoreRegistry, collection.CacheItem));
            });
        }
        KeepPreloadBytes = 0;
        CoreRequests.clear();

        if (auto owner = std::exchange(Owner, { }))
            Send(owner, new TEvents::TEvGone);
        PassAway();
    }

    TCacheCollection& AttachCollection(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        TCacheCollection& collection = EnsureCollection(std::move(pageCollection), owner);

        if (collection.GetActorState()->Owners.insert(owner).second) {
            auto [ownerIt, inserted] = Owners.try_emplace(owner);
            if (inserted) {
                Counters.Owners->Inc();
            }
            auto binding = CacheCore->BindCurrentThreadHazard();
            TSharedCacheCollectionRef ref;
            Y_ENSURE(CacheCore->Find(collection.Id(), ref) == ESharedCacheResultStatus::Hit);
            Y_ENSURE(ownerIt->second.Collections.emplace(TRegisteredCollection{ .Ref = std::move(ref) }).second);
            Counters.PageCollectionOwners->Inc();
        }

        return collection;
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
        TCacheCollection& collection, const TActorId& owner, const TIntrusivePtr<TCollectionOwnerStats>& stats) {
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

    void RemoveOwnerStats(TCacheCollection& collection, const TActorId& owner, const TCollectionOwner& entry,
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

    void UpdateCollectionStats(TCacheCollection& collection) {
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

    void CancelQueuedWalkRequests(const TLogoBlobID& walkCollectionId) {
        for (const auto& [requestId, collectionId] : CoreWalkRequests) {
            if (collectionId != walkCollectionId) {
                continue;
            }
            for (auto& [_, request] : CoreRequests) {
                if (request.Sender == SelfId() && request.EventCookie == CoreWalkResultCookie &&
                    request.Cookie == requestId) {
                    request.Cancelled = true;
                    request.Completion->Cancel();
                }
            }
        }
        PruneCoreQueues();
    }

    void CancelCoreRequests(const TLogoBlobID& collectionId, const TActorId& owner = {}) {
        for (auto& [_, request] : CoreRequests) {
            if (request.CollectionId == collectionId && (!owner || request.Sender == owner)) {
                request.Cancelled = true;
                request.Completion->Cancel(request.Sender != SelfId());
                            }
                        }
        for (auto it = CoreMemoryWaits.begin(); it != CoreMemoryWaits.end();) {
            const auto& waiting = **it;
            if (waiting.WaitingTransaction && waiting.PageCollection->Label() == collectionId &&
                (!owner || waiting.Sender == owner)) {
                waiting.Completion->NotifyResourcesReady();
                it = CoreMemoryWaits.erase(it);
            } else {
                ++it;
            }
        }
        PruneCoreQueues();
    }

    void Handle(NSharedCache::TEvAttach::TPtr &ev, const TActorContext& ctx) {
        auto*msg = ev->Get();
        TCacheCollection& collection = AttachCollection(msg->PageCollection, ev->Sender);
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
        const bool resumedSticky = ApplyCoreCollectionMode(collection, ctx);
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
        const TCollectionCacheItem coreItem = collection.CacheItem;
        Y_ENSURE(coreItem, "Cannot create core page collection " << pageCollectionId);
        Y_ENSURE(!msg->CacheItem || msg->CacheItem == coreItem);
        for (const auto& location : msg->Pages) {
            if (pageCollection.Total() != pageCollection.MetaPages() &&
                !NPageCollection::IsDeadPage(location.Type, pageCollection.SkipBTreeIndexV1Shadow())) {
                PendingInMemoryPages[collection.Id()].emplace(location);
            }
        }

        TSharedCacheCollectionRef ownership;
        {
            auto binding = CacheCore->BindCurrentThreadHazard();
            Y_ENSURE(CacheCore->Find(pageCollectionId, ownership) == ESharedCacheResultStatus::Hit);
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

    bool RequestCorePages(TActorId sender, ui64 eventCookie, NSharedCache::TEvRequest& msg) {
        const TLogoBlobID id = msg.PageCollection->Label();
        TCacheCollection* owner = FindCollection(id);
        Y_ENSURE(owner);
        const TCollectionCacheItem collection = owner->CacheItem;
        const ui64 maxPageHandles = CacheCore->TargetHandleCount() - 3;
        if (msg.Pages.size() > maxPageHandles) {
            THashSet<TPageOffset> distinctOffsets;
            distinctOffsets.reserve(msg.Pages.size());
            for (const TPageLocation& page : msg.Pages) {
                distinctOffsets.insert(page.Offset);
            }
            if (distinctOffsets.size() > maxPageHandles) {
                Y_TABLET_ERROR("Core request for " << id << " needs " << distinctOffsets.size()
                                                   << " distinct page handles but physical capacity is "
                                                   << maxPageHandles);
            }
        }
        const bool stickyRequest = eventCookie == ui64(ERequestTypeCookie::StickyPages);
        const bool recordStats = eventCookie != CoreKeepPreloadResultCookie && eventCookie != CoreWalkResultCookie;
        const bool keepPages = owner->GetCacheMode() == ECacheMode::TryKeepInMemory;
        Counters.PendingRequests->Inc();
        const ui64 completionId = NextCoreRequestId++;
        ++owner->GetActorState()->PendingRequests;
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

        Y_ENSURE(CoreRequests.emplace(completionId, TCoreRequest{ id, sender, eventCookie, msg.Cookie, completion })
                     .second);
        if (const auto& pad = completion->WaitPad(); pad && pad->PostponedForResources) {
            CoreRequests.at(completionId).Cancelled = true;
            completion->PostponeForResources();
            return true;
        }
        const TVector<TPageLocation>& locations = completion->Locations();
        TVector<TSharedCachePageRequest> requests;
        requests.reserve(locations.size());
        Y_ENSURE(msg.Sticky.empty() || msg.Sticky.size() == locations.size());
        for (ui32 index = 0; index < locations.size(); ++index) {
            const bool sticky = stickyRequest || (!msg.Sticky.empty() && msg.Sticky[index]);
            requests.emplace_back(locations[index], new TRequestPageWaiter(completion, index), sticky);
        }

        auto binding = CacheCore->BindCurrentThreadHazard();
        if (!CacheCore->FindOrInsertBatch(collection, requests, recordStats, true)) {
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE,
                "Core batch admission failed for "
                    << id << " pages " << locations.size() << " usage " << CacheCore->OverallUsage() << " current "
                    << CacheCore->CurrentLimit() << " hard " << CacheCore->HardLimit() << " static "
                    << CacheCore->StaticBytes() << " live pages "
                    << CacheCore->HotPages() + CacheCore->ColdPages() + CacheCore->KeepColdPages() +
                           CacheCore->StickyPages()
                    << " hot " << CacheCore->HotPages() << " cold " << CacheCore->ColdPages() << " cold ring "
                    << CacheCore->ColdRingEntries() << " sticky " << CacheCore->StickyPages() << " collections "
                    << CacheCore->Collections());
            for (ui32 index = 0; index < locations.size(); ++index) {
                completion->Complete(index, {}, EPageFetchCompletion::Failed);
            }
            return false;
        }

        TVector<TPageFetch> toFetch;
        for (ui32 index = 0; index < requests.size(); ++index) {
            auto& request = requests[index];
            const ui64 bytes = locations[index].Size;
            if (recordStats) {
                Counters.RequestedPages->Inc();
                Counters.RequestedBytes->Add(bytes);
            }
            switch (request.Status()) {
                case ESharedCacheResultStatus::Hit:
                    if (recordStats) {
                        Counters.CacheHitPages->Inc();
                        Counters.CacheHitBytes->Add(bytes);
                    }
                    completion->Complete(index, std::move(request.Page()), EPageFetchCompletion::Ready);
                    break;
                case ESharedCacheResultStatus::Inserted:
                    toFetch.push_back(std::move(request.Fetch()));
                [[fallthrough]];
            case ESharedCacheResultStatus::Pending:
                    if (!CoreQueueForPriority(msg.Priority) &&
                        request.Status() == ESharedCacheResultStatus::Pending) {
                        if (TPageFetch promoted = TakeQueuedCorePage(id, locations[index].Offset)) {
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
                case ESharedCacheResultStatus::Miss:
                    completion->Complete(index, {}, EPageFetchCompletion::Failed);
                    break;
            }
        }
        if (toFetch) {
            QueueCoreFetch(msg.PageCollection, std::move(toFetch), msg.Priority, sender,
                std::move(msg.TraceId), completion);
        }
        RetryCoreMemoryWaits();
        ActualizeCacheSizeLimit();
        return true;
    }

    void QueueCoreFetch(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        TVector<TPageFetch>&& pages, NBlockIO::EPriority priority, const TActorId& sender,
        NWilson::TTraceId traceId, TIntrusivePtr<TRequestCompletion> completion) {
        auto waiting = MakeHolder<TCoreQueuedRequest>();
        waiting->Sender = sender;
        waiting->Completion = completion;
        waiting->PageCollection = collection;
        waiting->Priority = priority;
        waiting->TraceId = NWilson::TTraceId(traceId);
        TVector<TPageFetch> ready;
        const bool reserved = CacheCore->TryReserveFetchBatch(pages);

        for (auto &page : pages) {
            if (reserved) {
                ready.push_back(std::move(page));
            } else {
                waiting->Pages.push_back(std::move(page));
            }
        }
        if (waiting->Pages) {
            CoreMemoryWaits.push_back(std::move(waiting));
        }
        if (!ready) {
            return;
        }
        if (auto* queue = CoreQueueForPriority(priority)) {
            auto queued = MakeHolder<TCoreQueuedRequest>();
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
            StartCoreFetch(collection, std::move(ready), priority, sender, std::move(traceId));
        }
    }

    void PostponeCoreTransaction(const TIntrusivePtr<NPageCollection::TPagesWaitPad>& pad) {
        const bool createGate = !std::exchange(pad->PostponedForResources, true);
        THolder<TCoreQueuedRequest> gate;
        for (auto& [_, request] : CoreRequests) {
            if (!request.Cancelled && request.Completion->WaitPad() == pad) {
                if (createGate && !gate) {
                    gate = MakeHolder<TCoreQueuedRequest>();
                    gate->Completion = request.Completion;
                    gate->Sender = request.Sender;
                    auto* collection = FindCollection(request.CollectionId);
                    Y_ENSURE(collection);
                    gate->PageCollection = collection->PageCollection();
                    gate->WaitingTransaction = true;
                }
                request.Cancelled = true;
                request.Completion->PostponeForResources();
            }
        }
        PruneCoreQueues();
        if (gate) {
            CoreMemoryWaits.push_back(std::move(gate));
        }
    }

    void RetryCoreMemoryWaits() {
        if (!CacheCore || CoreMemoryWaits.empty()) {
            if (CacheCore) {
                CacheCore->DisarmResourceWait();
            }
            return;
        }
        auto binding = CacheCore->BindCurrentThreadHazard();
        const ui64 generation = CacheCore->ResourceGeneration();
        const size_t count = CoreMemoryWaits.size();
        for (size_t index = 0; index < count; ++index) {
            auto waiting = std::move(CoreMemoryWaits.front());
            CoreMemoryWaits.pop_front();
            if (waiting->WaitingTransaction) {
                if (CacheCore->CanAdmitWorkingSet(waiting->Completion->WaitPad()->WorkingSetBytes)) {
                    waiting->Completion->NotifyResourcesReady();
                } else {
                    CoreMemoryWaits.push_back(std::move(waiting));
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
                const size_t before = CoreMemoryWaits.size();
                QueueCoreFetch(waiting->PageCollection, std::move(pages), waiting->Priority, waiting->Sender,
                    std::move(waiting->TraceId), waiting->Completion);
                if (CoreMemoryWaits.size() > before) {
                    const auto& blocked = *CoreMemoryWaits.back();
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
                            const bool held = pad->HasPinnedPages.load(std::memory_order_acquire);
                            if (held) {
                                PostponeCoreTransaction(pad);
                            }
                        });
                    }
                }
            }
        }
        if (CoreMemoryWaits.empty()) {
            CacheCore->DisarmResourceWait();
        } else {
            // Arm after observing the generation before reservation attempts; a concurrent release wakes us.
            CacheCore->ArmResourceWait(generation);
            // Also retry maintenance contention, which need not change resource counters.
            if (!ResourceRetryScheduled) {
                ResourceRetryScheduled = true;
                Schedule(
                    TDuration::MilliSeconds(10), new TKikimrEvents::TEvWakeup(ui64(EWakeupTag::RetryResources)));
            }
        }
    }

    void Handle(TEvResourcesAvailable::TPtr&, const TActorContext&) {
        RetryCoreMemoryWaits();
    }

    static ui64 CoreKeepPageBytes(const TPageLocation& location) {
        Y_ENSURE(location.Size <= Max<ui64>() - NActors::TSharedData::OverheadSize);
        return location.Size + NActors::TSharedData::OverheadSize;
    }

    ui64 CoreKeepRemainingBytes() const {
        const ui64 limit = Config.GetInMemoryInFlyLimit();
        return limit - Min(limit, KeepPreloadBytes);
    }

    bool IsCoreKeepPreloadInFlight(const TCacheCollection& collection, TPageOffset offset) const {
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

    void RequestCoreKeepPages(TCacheCollection& collection,
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TVector<TPageLocation>&& locations,
        NBlockIO::EPriority priority) {
        Y_ENSURE(locations);
        Y_DEBUG_ABORT_UNLESS(IsSorted(locations.begin(), locations.end()));
        ui64 chargedBytes = 0;
        for (const auto& location : locations) {
            const ui64 bytes = CoreKeepPageBytes(location);
            Y_ENSURE(chargedBytes <= Max<ui64>() - bytes);
            chargedBytes += bytes;
        }

        const ui64 requestId = NextCoreKeepPreloadRequestId++;
        auto [requestIt, inserted] = collection.GetActorState()->KeepPreloadRequests.emplace(
            requestId, TCoreKeepPreloadRequest{
                           .Generation = collection.KeepGeneration(),
                           .ChargedBytes = chargedBytes,
                           .Locations = locations,
                       });
        Y_ENSURE(inserted);
        Y_ENSURE(KeepPreloadBytes <= Max<ui64>() - chargedBytes);
        KeepPreloadBytes += chargedBytes;

        NSharedCache::TEvRequest request(priority, std::move(pageCollection), std::move(locations), requestId);
        requestIt->second.Admitted = RequestCorePages(SelfId(), CoreKeepPreloadResultCookie, request);
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
        TCacheCollection& collection = AttachCollection(msg->PageCollection, ev->Sender);
        if (collection.PageCollection()->Total() != collection.PageCollection()->MetaPages()) {
            for (const auto& location : msg->Pages) {
                if (!NPageCollection::IsDeadPage(
                        location.Type, collection.PageCollection()->SkipBTreeIndexV1Shadow())) {
                    PendingInMemoryPages[collection.Id()].emplace(location);
                }
            }
        }
        RequestCorePages(ev->Sender, ev->Cookie, *msg);
    }

    void Handle(NSharedCache::TEvCollectionBytesChanged::TPtr& ev, const TActorContext&) {
        const auto* msg = ev->Get();
        auto binding = CacheCore->BindCurrentThreadHazard();

        if (CacheCore->AcknowledgeCollectionBytes(msg->CacheItem)) {
            if (auto* collection = FindCollection(msg->CollectionId);
                collection && collection->CacheItem == msg->CacheItem) {
                UpdateCollectionStats(*collection);
            }
        }
    }

    void Handle(NSharedCache::TEvCollectionReleased::TPtr& ev, const TActorContext&) {
        const auto* msg = ev->Get();
        auto binding = CacheCore->BindCurrentThreadHazard();
        TSharedCacheCollectionRef collection;
        const auto status = CacheCore->Find(msg->CollectionId, collection);
        if (status == ESharedCacheResultStatus::Hit) {
            if (collection.CacheItem() == msg->CacheItem && collection->GetActorState()) {
                if (collection->GetActorState()->Owners.empty() && collection.IsSoleReference()) {
                    collection->DrainStickyPages();
            }

            if (!collection->HasPageItems()) {
                    TryDropExpiredCollection(*collection);
                }
            }
        } else if (status == ESharedCacheResultStatus::Miss) {
            // A late release from an older generation must not remove a replacement's offsets.
            PendingInMemoryPages.erase(msg->CollectionId);
            CoreKeepPreloadRetryBlocked.erase(msg->CollectionId);
        }
    }

    void Handle(NSharedCache::TEvRequestAnswered::TPtr& ev, const TActorContext&) {
        auto requestIt = CoreRequests.find(ev->Get()->CompletionId);
        Y_ENSURE(requestIt != CoreRequests.end());
        const TLogoBlobID collectionId = requestIt->second.CollectionId;
        CoreRequests.erase(requestIt);
        if (auto* collection = FindCollection(collectionId)) {
            Y_ENSURE(collection->GetActorState()->PendingRequests != 0);
            --collection->GetActorState()->PendingRequests;
            TryDropExpiredCollection(*collection);
        }
        Counters.PendingRequests->Dec();
        RetryCoreMemoryWaits();
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
        // Core references carry their own lifetime; private-page sync is obsolete after attach.
    }

    void Handle(NSharedCache::TEvUnregister::TPtr &ev, const TActorContext& ctx) {
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
            CancelCoreRequests(collection->Id(), ev->Sender);
            Y_ENSURE(collection->GetActorState()->Owners.erase(ev->Sender));
            collection->GetActorState()->StickyOwners.erase(ev->Sender);
            Counters.PageCollectionOwners->Dec();

            TryMoveToRegularCache(*collection, ev->Sender);
            const bool resumedSticky = ApplyCoreCollectionMode(*collection, ctx);
            const TLogoBlobID collectionId = collection->Id();
            Walks.UpdateSeeds(*collection, ev->Sender, {}, resumedSticky);
            if (auto* current = FindCollection(collectionId)) {
                if (current->GetActorState()->Owners.empty()) {
                    DetachCoreCollection(*current);
                }
                TryDropExpiredCollection(*current);
            }
        }
    }

    void Handle(NSharedCache::TEvDetach::TPtr &ev, const TActorContext& ctx) {
        const TLogoBlobID collectionId = ev->Get()->PageCollectionId;
        auto* collection = FindCollection(collectionId);

        if (!collection || !collection->GetActorState()->Owners.erase(ev->Sender)) {
            return;
        }
        CancelCoreRequests(collectionId, ev->Sender);
        auto ownerIt = Owners.find(ev->Sender);
        Y_ENSURE(ownerIt != Owners.end());
        const auto registration = ownerIt->second.Collections.find(collection->CacheItem);
        Y_ENSURE(registration != ownerIt->second.Collections.end());
        RemoveOwnerStats(*collection, ev->Sender, ownerIt->second, *registration);
        collection->GetActorState()->StickyOwners.erase(ev->Sender);
        ownerIt->second.Collections.erase(registration);
        Counters.PageCollectionOwners->Dec();

        TryMoveToRegularCache(*collection, ev->Sender);
        const bool resumedSticky = ApplyCoreCollectionMode(*collection, ctx);
        Walks.UpdateSeeds(*collection, ev->Sender, {}, resumedSticky);
        if (auto* current = FindCollection(collectionId)) {
            if (current->GetActorState()->Owners.empty()) {
                DetachCoreCollection(*current);
            }
            TryDropExpiredCollection(*current);
        }
    }

    void Handle(NSharedCache::TEvResult::TPtr& ev, const TActorContext&) {
        if (ev->Cookie == CoreKeepPreloadResultCookie) {
            const ui64 requestId = ev->Get()->Cookie;
            auto* collection = FindCollection(ev->Get()->PageCollection->Label());
            Y_ENSURE(collection && collection->GetActorState());
            auto& requests = collection->GetActorState()->KeepPreloadRequests;
            auto requestIt = requests.find(requestId);
            Y_ENSURE(requestIt != requests.end());
            TCoreKeepPreloadRequest request = std::move(requestIt->second);
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
                    CoreKeepPreloadRetryBlocked.insert(collection->Id());
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

        Y_ENSURE(ev->Cookie == CoreWalkResultCookie);
        const ui64 requestId = ev->Get()->Cookie;
        auto requestIt = CoreWalkRequests.find(requestId);
        Y_ENSURE(requestIt != CoreWalkRequests.end());
        const TLogoBlobID walkCollectionId = requestIt->second;
        CoreWalkRequests.erase(requestIt);
        if (ev->Get()->Status != NKikimrProto::OK && ev->Get()->Status != NKikimrProto::RACE) {
            Walks.InvalidateRun(walkCollectionId);
        } else {
            Walks.IndexPagesChanged(ev->Get()->PageCollection->Label());
        }
        Walks.FinishFetch(walkCollectionId);
    }

    void Handle(NBlockIO::TEvData::TPtr &ev, const TActorContext&) {
        CompleteCoreFetch(ev->Sender, *ev->Get());
    }

    void TryDropExpiredCollection(TCacheCollection& collection) override {
        auto* state = collection.GetActorState();
        // Keep preload records until their results arrive, even after the request-answered notification.
        if (state && state->Owners.empty() && state->PendingRequests == 0 && state->KeepPreloadRequests.empty() &&
            Walks.IsIdle(collection.Id()))
        {
            Y_DEBUG_ABORT_UNLESS(state->InMemoryOwners.empty());
            const TLogoBlobID id = collection.Id();
            Walks.EraseCollection(id);
            CoreKeepPreloadRetryBlocked.erase(id);
            auto binding = CacheCore->BindCurrentThreadHazard();
            // Detach releases the registry hold; no access to collection follows it.
            collection.ResetActorState();
            Y_ENSURE(CacheCore->DetachCollection(*CoreRegistry, collection.CacheItem));
            Counters.PageCollections->Dec();
        }
    }

    void Wakeup(TKikimrEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
        if (tag != EWakeupTag::ContinueBTreeWalk && tag != EWakeupTag::RetryResources) {
            LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Wakeup " << tag);
        }

        switch (tag) {
        case EWakeupTag::DoGCScheduled:
                CoreKeepPreloadRetryBlocked.clear();
                ScheduleGC();
            break;
        case EWakeupTag::DoGCManual:
            break;
        case EWakeupTag::DoLimitMaintenance:
                LimitMaintenanceScheduled = false;
            ActualizeCacheSizeLimit();
                break;
            case EWakeupTag::RetryResources:
                ResourceRetryScheduled = false;
                RetryCoreMemoryWaits();
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

    TRequestQueue* CoreQueueForPriority(NBlockIO::EPriority priority) {
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

    TPageFetch TakeQueuedCorePage(const TLogoBlobID& collectionId, TPageOffset offset) {
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
            TCoreQueuedRequest& request = *it->second.front();
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
            StartCoreFetch(request.PageCollection,
            std::move(batch), request.Priority, request.Sender,
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

    void PruneCoreQueues() {
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

    void StartCoreFetch(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection,
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
        const TActorId fetchActor = NBlockIO::Start(this, statActor, static_cast<ui64>(EBlockIOFetchTypeCookie::CoreFetch), request);
        Y_ENSURE(CoreFetches
                     .emplace(fetchActor,
                         TCoreFetch{ .CollectionId = collectionId, .Bytes = bytes, .QueueCookie = queueCookie,
                             .Pages = std::move(pages) })
                     .second);
    }

    void CompleteCoreFetch(const TActorId& fetchActor, NBlockIO::TEvData& data) {
        auto fetchIt = CoreFetches.find(fetchActor);
        if (fetchIt == CoreFetches.end()) {
            return; // stale or duplicate completion from an already resolved fetch actor
        }
        TCoreFetch fetch = std::move(fetchIt->second);
        CoreFetches.erase(fetchIt);
        Counters.LoadInFlyPages->Sub(fetch.Pages.size());
        Counters.LoadInFlyBytes->Sub(fetch.Bytes);

        const bool valid = data.Status == NKikimrProto::OK && data.PageCollection->Label() == fetch.CollectionId &&
                           data.Pages.size() == fetch.Pages.size();
        for (ui32 index = 0; index < fetch.Pages.size(); ++index) {
            TPageFetch& page = fetch.Pages[index];
            if (valid && data.Pages[index].Offset == page.Location().Offset &&
                data.Pages[index].Data.size() == page.Size())
            {
                page.MakeReady(std::move(data.Pages[index].Data));
            } else {
                page.FailReady();
                }
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

    bool ApplyCoreCollectionMode(TCacheCollection& collection, const TActorContext& ctx) {
        const bool keepPages = bool(collection.GetActorState()->InMemoryOwners);
        const bool stickyPages = !keepPages && bool(collection.GetActorState()->StickyOwners);
        const ECacheMode mode = keepPages     ? ECacheMode::TryKeepInMemory
                                : stickyPages ? ECacheMode::Sticky
                                              : ECacheMode::Regular;
        const ECacheMode previous = collection.GetCacheMode();
        if (mode == previous) {
            return false;
        }
        auto binding = CacheCore->BindCurrentThreadHazard();
        if (!CacheCore->SetCollectionPagesCacheMode(collection.CacheItem, mode)) {
            LOG_WARN_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE,
                "Cannot apply cache mode " << mode << " to core page collection " << collection.Id());
            return false;
        }
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE,
            "Set core page collection " << collection.Id() << " mode to " << mode);
        if (keepPages != (previous == ECacheMode::TryKeepInMemory)) {
            CoreKeepPreloadRetryBlocked.erase(collection.Id());
        }
        return stickyPages && previous != ECacheMode::Sticky;
    }

    void TryMoveToRegularCache(TCacheCollection& collection, const TActorId& owner) {
        if (!collection.GetActorState()->InMemoryOwners.erase(owner) ||
            collection.GetActorState()->InMemoryOwners) {
            return;
        }
        Y_ENSURE(TargetInMemoryBytes >= collection.TotalSize());
        TargetInMemoryBytes -= collection.TotalSize();
        Counters.TargetInMemoryBytes->Set(TargetInMemoryBytes);
        for (auto& [_, request] : CoreRequests) {
            if (request.CollectionId == collection.Id() && request.EventCookie == CoreKeepPreloadResultCookie) {
                request.Cancelled = true;
                request.Completion->Cancel();
            }
        }
        PruneCoreQueues();
        ActualizeCacheSizeLimit();
        Walks.DropIndexOnlyWalks(collection.Id());
    }

    void TryMoveToTryKeepInMemoryCache(TCacheCollection& collection, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
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
        TCacheCollection& collection, const TActorId& owner, const TVector<TPageLocation>& locations) {
        if (!collection.GetActorState()->Owners.contains(owner)) {
            return;
        }

        auto send = [&](auto first, auto last) {
            if (first == last) {
                return;
            }

            TVector<TPageLocation> batch(first, last);
            TEvRequest request(NBlockIO::EPriority::Bkgr, collection.PageCollection(), std::move(batch));
            RequestCorePages(owner, ui64(ERequestTypeCookie::StickyPages), request);
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

        const ui64 keepColdLimit = CacheCore ? CacheCore->KeepColdLimit() : 0;
        Y_ENSURE(LastCoreKeepResidentBytes <= Max<ui64>() - KeepPreloadBytes);
        const ui64 committedKeepBytes = LastCoreKeepResidentBytes + KeepPreloadBytes;
        const ui64 pendingKeepBytes = KeepPreloadBytes;
        const ui64 protectedRoom = keepColdLimit > committedKeepBytes ? keepColdLimit - committedKeepBytes : 0;
        const ui64 softLimit = CacheCore ? CacheCore->SoftLimit() : 0;
        const ui64 usage = CacheCore ? CacheCore->RetainedBytes() : 0;
        const ui64 freeRoom = softLimit > usage ? softLimit - usage : 0;
        // Preload commits its future retained buffers; demand's transient loads do not consume this room.
        const ui64 freePreloadRoom = freeRoom - Min(freeRoom, pendingKeepBytes);
        const ui64 targetRoom =
            TargetInMemoryBytes > committedKeepBytes ? TargetInMemoryBytes - committedKeepBytes : 0;
        // KeepCold reserves protected space; Hot and Cold may hold more Keep pages when memory is free.
        // Use the reclaim target for extra preload room so eviction does not immediately restart the same fetch.
        ui64 coreRemainBytes = Min(CoreKeepRemainingBytes(), Min(targetRoom, Max(protectedRoom, freePreloadRoom)));
        THashSet<TLogoBlobID> waitingForInFlight;

        while (PendingInMemoryPages) {
            auto it = PendingInMemoryPages.begin();
            while (it != PendingInMemoryPages.end()) {
                const TCacheCollection* candidate = FindCollection(it->first);
                // Routed Regular collections retain known hidden offsets until a Keep owner appears.
                if (!CoreKeepPreloadRetryBlocked.contains(it->first) && !waitingForInFlight.contains(it->first) &&
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

            TVector<TPageLocation> pagesToRequest;
            bool budgetBlocked = false;
            auto locationIt = pagesToLoad.begin();
            while (locationIt != pagesToLoad.end() && pagesToRequest.size() < MaxPreloadBatchLocations)
            {
                const TPageLocation location = *locationIt;
                ESharedCacheResultStatus status;
                {
                    auto binding = CacheCore->BindCurrentThreadHazard();
                    TSharedCachePageRef page;
                    status = CacheCore->Find(collection->CacheItem, static_cast<ui64>(location.Offset), page);
                }
                if (status == ESharedCacheResultStatus::Hit) {
                    locationIt = pagesToLoad.erase(locationIt);
                    Walks.IndexPagesChanged(collectionId);
                    ScheduleWalkContinuation();
                    continue;
                }
                if (status == ESharedCacheResultStatus::Pending ||
                    IsCoreKeepPreloadInFlight(*collection, location.Offset))
                {
                    ++locationIt;
                    continue;
                }
                const ui64 pageBytes = CoreKeepPageBytes(location);
                if (pageBytes > coreRemainBytes) {
                    budgetBlocked = true;
                        break;
                    }
                coreRemainBytes -= pageBytes;
                    pagesToRequest.push_back(location);
                locationIt = pagesToLoad.erase(locationIt);
            }

            const bool submitted = bool(pagesToRequest);

            if (submitted) {
                RequestCoreKeepPages(*collection, collection->PageCollection(), std::move(pagesToRequest),
                    NBlockIO::EPriority::Bulk);
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

        LimitReady = ControllerMaxBytes || Config.HasMemoryLimit() || bool(CacheCore);
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
