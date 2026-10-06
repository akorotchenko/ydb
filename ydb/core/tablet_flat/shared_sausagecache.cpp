#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "shared_cache.h"
#include "shared_cache_btree_walk.h"
#include "shared_cache_counters.h"
#include "shared_cache_events.h"
#include "shared_cache_pages.h"
#include "shared_page.h"
#include "shared_sausagecache.h"
#include "shared_sausagecache_state.h"
#include "util_fmt_abort.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/bootstrap.pb.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/set.h>
#include <util/stream/format.h>

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

struct TCoreKeepPreloadRequest {
    TLogoBlobID CollectionId;
    TCollectionCacheItem CacheItem;
    ui64 Generation = 0;
    ui64 ChargedBytes = 0;
    TVector<TPageLocation> Locations;
    bool Admitted = false;
};

struct TCoreRequest {
    TLogoBlobID CollectionId;
    TActorId Sender;
    ui64 EventCookie = 0;
    ui64 Cookie = 0;
    TIntrusivePtr<TRequestCompletion> Completion;
    bool Cancelled = false;
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

class TSeedWaiter final : public TPageFetchWaiter {
public:
    void Complete(TPageCacheItem, EPageFetchCompletion) noexcept override {
    }
};

struct TCoreQueue {
    explicit TCoreQueue(EBlockIOFetchTypeCookie cookie)
        : Cookie(cookie)
    {
    }

    EBlockIOFetchTypeCookie Cookie;
    TMap<TActorId, TDeque<THolder<TCoreQueuedRequest>>> Requests;
    ui64 Limit = 0;
    ui64 InFly = 0;
    TActorId NextToRequest;
};

namespace {

    class TSharedPageCache : public TActorBootstrapped<TSharedPageCache>, private ICacheBTreeWalkHost {
        static constexpr ui8 CoreMinimumAddressBits = 8;
        static constexpr ui64 CoreExpectedPageSize = 3 * 1024;
        static constexpr ui64 CoreWalkResultCookie = Max<ui64>();
        static constexpr ui64 CoreKeepPreloadResultCookie = Max<ui64>() - 1;

        TActorId Owner;
        TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
        NSharedCache::TSharedCachePages* SharedCachePages;
        TSharedCacheConfig Config;
        TSharedPageCacheCounters Counters;
        TIntrusivePtr<TSharedCache> CacheCore;
        TIntrusivePtr<TCollectionRegistry> CoreRegistry = new TCollectionRegistry;

        THashMap<TLogoBlobID, TCollection> Collections;
        TCacheBTreeWalkController Walks{ *this };
        TPendingInMemoryPages PendingInMemoryPages;
        THashMap<ui64, TLogoBlobID> CoreWalkRequests;
        THashMap<ui64, TCoreKeepPreloadRequest> CoreKeepPreloadRequests;
        THashSet<TLogoBlobID> CoreKeepPreloadRetryBlocked;
        // I/O owners stay here until the matching fetch actor completes.
        THashMap<TActorId, TCoreFetch> CoreFetches;
        THashMap<TActorId, THashSet<TCollection*>> Owners;
        THashMap<ui64, TCoreRequest> CoreRequests;
        ui64 NextCoreRequestId = 1;
        TDeque<THolder<TCoreQueuedRequest>> CoreMemoryWaits;
        bool ResourceRetryScheduled = false;
        TCoreQueue AsyncCoreRequests{ EBlockIOFetchTypeCookie::AsyncQueue };
        TCoreQueue ScanCoreRequests{ EBlockIOFetchTypeCookie::ScanQueue };

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

        TCollection* FindWalkCollection(const TLogoBlobID& id) override {
            return Collections.FindPtr(id);
        }

        TPendingInMemoryPages& PendingWalkPages() override {
            return PendingInMemoryPages;
        }

        void ScheduleWalkContinuation() override {
            if (!std::exchange(WalkContinuationScheduled, true)) {
                Send(SelfId(), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::ContinueBTreeWalk)));
            }
        }

        NActors::TSharedData FindCoreWalkPage(TCollection& collection, TPageOffset offset) override {
            if (!collection.CacheItem) {
                return {};
            }
            auto binding = CacheCore->BindCurrentThreadHazard();
            TSharedCachePageRef page;
            if (CacheCore->Find(collection.CacheItem, static_cast<ui64>(offset), page) ==
                ESharedCacheResultStatus::Hit) {
                return page.ShareData();
            }
            return {};
        }

        void FetchWalkIndexLevel(
            TCollection& collection, TVector<TPageLocation>&& locations, const TLogoBlobID& walkCollectionId) override {
            Y_DEBUG_ABORT_UNLESS(collection.GetCacheMode() == ECacheMode::Regular);
            const ui64 requestId = NextCoreWalkRequestId++;
            Y_ENSURE(CoreWalkRequests.emplace(requestId, walkCollectionId).second);
            Walks.FetchStarted(walkCollectionId);
            NSharedCache::TEvRequest request(
                NBlockIO::EPriority::Bkgr, collection.PageCollection, std::move(locations), requestId);
            RequestCorePages(SelfId(), CoreWalkResultCookie, request);
        }

        void SendWalkStickyPages(
            TCollection& collection, const TActorId& owner, const TVector<TPageLocation>& locations) override {
            SendStickyCollectionPages(collection, owner, locations);
        }

        void CancelQueuedWalkRequestsAndPump(const TLogoBlobID& walkCollectionId) override {
            CancelQueuedWalkRequests(walkCollectionId);
            PumpCoreQueue(AsyncCoreRequests);
            PumpCoreQueue(ScanCoreRequests);
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
            if (!TryCalculateSharedCacheFootprint(minimumAddressBits, CoreExpectedPageSize, 0, HazardCount, minimum))
            {
                return false;
            }
            const ui64 targetLimit = GetTargetCacheHardLimitBytes();
            const ui64 allocationLimit = Max(targetLimit, minimum.TotalBytes);
            TSharedCacheCapacity current;
            if (!TryCalculateSharedCacheCapacity(allocationLimit, CoreExpectedPageSize, 0, HazardCount, current))
            {
                return false;
            }
            TSharedCacheCapacity reserved;
            if (!TryCalculateSharedCacheFootprint(
                    MaxSharedCacheAddressBits, CoreExpectedPageSize, 0, HazardCount, reserved)) {
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

        TCollectionCacheItem EnsureCoreCollection(TCollection& collection) {
            if (collection.CacheItem) {
                return collection.CacheItem;
            }
            Y_ENSURE(InitializeCacheCore(), "Cannot initialize shared-cache core");
            auto binding = CacheCore->BindCurrentThreadHazard();
            const TCollectionLocation location{ .Id = collection.Id,
                .BackingSize = collection.PageCollection->BackingSize() };
            TCollectionCacheItem inserted;
            TSharedCacheCollectionRef hit;
            const ESharedCacheResultStatus status = CacheCore->FindOrInsert(*CoreRegistry, location, inserted, hit);
            if (status == ESharedCacheResultStatus::Inserted) {
                auto value = MakeHolder<TCacheCollection>(collection.PageCollection, inserted);
                Y_ENSURE(CacheCore->MakeReady(*CoreRegistry, inserted, std::move(value)));
                collection.CacheItem = inserted;
            } else {
                Y_ENSURE(status == ESharedCacheResultStatus::Hit);
                collection.CacheItem = hit.CacheItem();
            }
            Y_ENSURE(CacheCore->SetCollectionPagesCacheMode(collection.CacheItem, ECacheMode::Regular));
            return collection.CacheItem;
        }

        void UpdateCoreKeepColdLimit() {
            // The core applies the live 50% ceiling after excluding Sticky bytes.
            Y_ENSURE(CacheCore->UpdateKeepColdLimit(Min(TargetInMemoryBytes, CacheCore->HardLimit())));
        }

        void ActualizeCacheSizeLimit() {
            Counters.ConfigLimitBytes->Set(Config.HasMemoryLimit() ? Config.GetMemoryLimit() : 0);
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
            ProcessGCList();
            TVector<TLogoBlobID> expiredSaves;
            const TMonotonic now = TActivationContext::Monotonic();
            for (const auto& [id, collection] : Collections) {
                if (collection.SavedToCore && collection.Owners.empty() &&
                    now - collection.CoreSavedAt >= TDuration::Minutes(1)) {
                    expiredSaves.push_back(id);
                }
            }
            for (const TLogoBlobID& id : expiredSaves) {
                TCollection& collection = Collections.at(id);
                DetachCoreCollection(collection);
                collection.SavedToCore = false;
                TryDropExpiredCollection(collection);
            }
            ui64 coreInMemoryBytes = 0;
            ui64 coreKeepResidentBytes = 0;
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
                for (const auto& [_, collection] : Collections) {
                    if (collection.CacheItem && collection.CoreKeepsPages) {
                        TSharedCacheCollectionRef coreCollection;
                        Y_ENSURE(CacheCore->Find(collection.Id, coreCollection) == ESharedCacheResultStatus::Hit);
                        coreInMemoryBytes += coreCollection.GetCollection().KeepActivePageBytes();
                        coreKeepResidentBytes += coreCollection.GetCollection().KeepResidentPageBytes();
                    }
                }
                Counters.ActivePages->Set(
                    CacheCore->HotPages() + CacheCore->StickyPages() + CacheCore->KeepColdPages());
                Counters.ActiveBytes->Set(
                    CacheCore->HotBytes() + CacheCore->StickyBytes() + CacheCore->KeepColdBytes());
                Counters.ActiveInMemoryBytes->Set(coreInMemoryBytes);
                Counters.PassivePages->Set(CacheCore->ColdPages());
                Counters.PassiveBytes->Set(CacheCore->ColdBytes());
            }
            LastCoreKeepResidentBytes = coreKeepResidentBytes;
            if (MemoryConsumer) {
                MemoryConsumer->SetConsumption(CacheCore ? CacheCore->OverallUsage() : 0);
            }
        }

        void Handle(NMemory::TEvConsumerRegistered::TPtr& ev, const TActorContext& ctx) {
            LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Register memory consumer");

            auto* msg = ev->Get();
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

        void Handle(NMemory::TEvConsumerLimit::TPtr& ev, const TActorContext& ctx) {
            auto* msg = ev->Get();
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

            LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE,
                "Limit memory consumer" << " with " << HumanReadableBytes(msg->LimitBytes));

            MemLimitBytes = msg->LimitBytes;
            Counters.MemLimitBytes->Set(MemLimitBytes);

            ActualizeCacheSizeLimit();
            ReplayDeferredEvents();
        }

        void Registered(TActorSystem* sys, const TActorId& owner) override {
            NActors::TActorBootstrapped<TSharedPageCache>::Registered(sys, owner);
            Owner = owner;

            SharedCachePages = sys->AppData<TAppData>()->SharedCachePages.Get();
        }

        void DetachCoreCollection(TCollection& collection) {
            if (!collection.CacheItem) {
                return;
            }
            CancelCoreRequests(collection.Id);
            auto binding = CacheCore->BindCurrentThreadHazard();
            Y_ENSURE(CacheCore->DetachCollection(*CoreRegistry, collection.CacheItem));
            collection.CacheItem = {};
            collection.CoreKeepsPages = false;
            collection.CoreStickyPages = false;
            ActualizeCacheSizeLimit();
        }

        void TakePoison(const TActorContext& ctx) {
            LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Poison shared cache");
            for (auto& [_, request] : CoreRequests) {
                request.Completion->Cancel();
            }
            AsyncCoreRequests.Requests.clear();
            ScanCoreRequests.Requests.clear();
            CoreMemoryWaits.clear();
            CoreFetches.clear();
            if (CacheCore) {
                auto binding = CacheCore->BindCurrentThreadHazard();
                for (auto& [_, collection] : Collections) {
                    if (collection.CacheItem) {
                        CacheCore->DetachCollection(*CoreRegistry, collection.CacheItem);
                        collection.CacheItem = {};
                        collection.CoreKeepsPages = false;
                        collection.CoreStickyPages = false;
                    }
                }
            }
            CoreRequests.clear();
            if (auto owner = std::exchange(Owner, {})) {
                Send(owner, new TEvents::TEvGone);
            }
            PassAway();
        }

        TCollection& AttachCollection(const TLogoBlobID& pageCollectionId,
            TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
            TCollection& collection = EnsureCollection(pageCollectionId, *pageCollection, owner);
            collection.PageCollection = std::move(pageCollection);
            EnsureCoreCollection(collection);
            if (collection.Owners.insert(owner).second) {
                auto [ownerIt, inserted] = Owners.try_emplace(owner);
                if (inserted) {
                    Counters.Owners->Inc();
                }
                Y_ENSURE(ownerIt->second.insert(&collection).second);
                Counters.PageCollectionOwners->Inc();
            }
            return collection;
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

        void Handle(NSharedCache::TEvAttach::TPtr& ev, const TActorContext& ctx) {
            auto* msg = ev->Get();
            TCollection& collection = AttachCollection(msg->PageCollection->Label(), msg->PageCollection, ev->Sender);
            collection.SavedToCore = false;
            if (msg->CacheMode == ECacheMode::Sticky) {
                collection.StickyOwners.insert(ev->Sender);
            } else {
                collection.StickyOwners.erase(ev->Sender);
            }
            if (msg->CacheMode == ECacheMode::TryKeepInMemory) {
                TryMoveToTryKeepInMemoryCache(collection, msg->PageCollection, ev->Sender);
            } else {
                TryMoveToRegularCache(collection, ev->Sender);
            }
            const bool resumedSticky = ApplyCoreCollectionMode(collection, ctx);
            Walks.UpdateSeeds(
                collection, ev->Sender, std::move(msg->BtreeSeeds), msg->ReplayStickyWalk || resumedSticky);
            Send(ev->Sender, new NSharedCache::TEvAttached(collection.Id, collection.CacheItem), 0, ev->Cookie);
        }

        void Handle(NSharedCache::TEvSaveCompactedPages::TPtr& ev, const TActorContext& ctx) {
            NSharedCache::TEvSaveCompactedPages* msg = ev->Get();
            const auto& pageCollection = *msg->PageCollection;
            const TLogoBlobID pageCollectionId = pageCollection.Label();

            LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE,
                "Save page collection " << pageCollectionId << " owner " << ev->Sender << " compacted pages "
                                        << msg->Pages);

            Y_ENSURE(pageCollectionId);
            Y_ENSURE(!Collections.contains(pageCollectionId), "Only new collections can save compacted pages");
            auto& collection = EnsureCollection(pageCollectionId, pageCollection, ev->Sender);
            collection.PageCollection = msg->PageCollection;

            const TCollectionCacheItem coreItem = EnsureCoreCollection(collection);
            Y_ENSURE(coreItem, "Cannot create core page collection " << pageCollectionId);
            auto binding = CacheCore->BindCurrentThreadHazard();
            Y_ENSURE(CacheCore->SetCollectionPagesCacheMode(coreItem, ECacheMode::Regular));

            TVector<TSharedCachePageRequest> requests;
            requests.reserve(1);
            for (const auto& page : msg->Pages) {
                if (pageCollection.Total() != pageCollection.MetaPages() &&
                    !NPageCollection::IsDeadPage(page->Type, pageCollection.SkipBTreeIndexV1Shadow())) {
                    PendingInMemoryPages[collection.Id].emplace(page->Offset, page->Size, page->Type, page->Crc32);
                }
                const bool sticky = page->CacheMode == ECacheMode::Sticky;
                requests.clear();
                requests.emplace_back(
                    TPageLocation(page->Offset, page->Size, page->Type, page->Crc32), new TSeedWaiter, sticky);
                if (!CacheCore->FindOrInsertBatch(coreItem, requests, false)) {
                    // Regular pages are on disk and can be fetched when first requested.
                    Y_ENSURE(
                        !sticky, "Cannot admit selected Sticky page " << page->Offset << " in " << pageCollectionId);
                    continue;
                }
                switch (requests.front().Status()) {
                    case ESharedCacheResultStatus::Inserted: {
                        TSharedData data = page->Pin();
                        page->UnPin();
                        Y_ENSURE(requests.front().Fetch().MakeReady(std::move(data)));
                        break;
                    }
                    case ESharedCacheResultStatus::Hit:
                    case ESharedCacheResultStatus::Pending:
                        break;
                    case ESharedCacheResultStatus::Miss:
                        Y_TABLET_ERROR("Cannot save compacted page after accepted admission");
                }
            }

            collection.SavedToCore = true;
            collection.CoreSavedAt = TActivationContext::Monotonic();
            ActualizeCacheSizeLimit();
        }

        bool RequestCorePages(TActorId sender, ui64 eventCookie, NSharedCache::TEvRequest& msg) {
            const TLogoBlobID id = msg.PageCollection->Label();
            const TCollectionCacheItem collection = Collections.at(id).CacheItem;
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
            const bool keepPages = Collections.at(id).CoreKeepsPages;
            Counters.PendingRequests->Inc();
            const ui64 completionId = NextCoreRequestId++;
            ++Collections.at(id).PendingRequests;
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
                        completion->Complete(
                            index, MakeSharedPageRef(std::move(request.Page())), EPageFetchCompletion::Ready);
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
            for (auto& page : pages) {
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
                PumpCoreQueue(*queue);
            } else {
                StartCoreFetch(collection, std::move(ready), priority, sender, std::move(traceId));
            }
        }

        bool CorePageNeeded(const TLogoBlobID& collectionId, TPageOffset offset) const {
            return AnyOf(CoreRequests, [&](const auto& item) {
                const auto& request = item.second;
                return !request.Cancelled && request.CollectionId == collectionId &&
                       AnyOf(request.Completion->Locations(), [&](const auto& location) {
                           return location.Offset == offset;
                       });
            });
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
                        gate->PageCollection = Collections.at(request.CollectionId).PageCollection;
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
                    if (CorePageNeeded(waiting->PageCollection->Label(), page.Location().Offset)) {
                        pages.push_back(std::move(page));
                    }
                }
                if (pages) {
                    const size_t before = CoreMemoryWaits.size();
                    QueueCoreFetch(waiting->PageCollection, std::move(pages), waiting->Priority, waiting->Sender,
                        std::move(waiting->TraceId), waiting->Completion);
                    if (CoreMemoryWaits.size() > before) {
                        const auto& blocked = *CoreMemoryWaits.back();
                        for (auto& [_, request] : CoreRequests) {
                            const auto pad = request.Completion->WaitPad();
                            if (request.Cancelled || !pad || request.CollectionId != blocked.PageCollection->Label() ||
                                !AnyOf(blocked.Pages, [&](const auto& page) {
                                    return AnyOf(request.Completion->Locations(), [&](const auto& location) {
                                        return location.Offset == page.Location().Offset;
                                    });
                                })) {
                                continue;
                            }
                            const bool held = pad->HasPinnedPages.load(std::memory_order_acquire) ||
                                              AnyOf(CoreRequests, [&](const auto& item) {
                                                  return !item.second.Cancelled &&
                                                         item.second.Completion->WaitPad() == pad &&
                                                         item.second.Completion->HasReadyPages();
                                              });
                            if (held) {
                                PostponeCoreTransaction(pad);
                            }
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
            ui64 remaining = Config.GetInMemoryInFlyLimit();
            for (const auto& [_, request] : CoreKeepPreloadRequests) {
                if (request.ChargedBytes >= remaining) {
                    return 0;
                }
                remaining -= request.ChargedBytes;
            }
            return remaining;
        }

        bool IsCoreKeepPreloadInFlight(const TLogoBlobID& collectionId, ui64 generation, TPageOffset offset) const {
            for (const auto& [_, request] : CoreKeepPreloadRequests) {
                if (request.CollectionId == collectionId && request.Generation == generation &&
                    AnyOf(request.Locations,
                        [offset](const auto& location) {
                            return location.Offset == offset;
                        }))
                {
                    return true;
                }
            }
            return false;
        }

        void RequestCoreKeepPages(TCollection& collection,
            TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TVector<TPageLocation>&& locations,
            NBlockIO::EPriority priority) {
            Y_ENSURE(locations);
            ui64 chargedBytes = 0;
            for (const auto& location : locations) {
                const ui64 bytes = CoreKeepPageBytes(location);
                Y_ENSURE(chargedBytes <= Max<ui64>() - bytes);
                chargedBytes += bytes;
            }

            const ui64 requestId = NextCoreKeepPreloadRequestId++;
            auto [requestIt, inserted] =
                CoreKeepPreloadRequests.emplace(requestId, TCoreKeepPreloadRequest{
                                                               .CollectionId = collection.Id,
                                                               .CacheItem = collection.CacheItem,
                                                               .Generation = collection.CoreKeepGeneration,
                                                               .ChargedBytes = chargedBytes,
                                                               .Locations = locations,
                                                           });
            Y_ENSURE(inserted);

            NSharedCache::TEvRequest request(priority, std::move(pageCollection), std::move(locations), requestId);
            requestIt->second.Admitted = RequestCorePages(SelfId(), CoreKeepPreloadResultCookie, request);
        }

        void Handle(NSharedCache::TEvRequest::TPtr& ev, const TActorContext&) {
            auto* msg = ev->Get();
            TCollection& collection = AttachCollection(msg->PageCollection->Label(), msg->PageCollection, ev->Sender);
            if (collection.PageCollection->Total() != collection.PageCollection->MetaPages()) {
                for (const auto& location : msg->Pages) {
                    if (!NPageCollection::IsDeadPage(
                            location.Type, collection.PageCollection->SkipBTreeIndexV1Shadow())) {
                        PendingInMemoryPages[collection.Id].emplace(location);
                    }
                }
            }
            RequestCorePages(ev->Sender, ev->Cookie, *msg);
        }

        void Handle(NSharedCache::TEvRequestAnswered::TPtr& ev, const TActorContext&) {
            auto requestIt = CoreRequests.find(ev->Get()->CompletionId);
            Y_ENSURE(requestIt != CoreRequests.end());
            const TLogoBlobID collectionId = requestIt->second.CollectionId;
            CoreRequests.erase(requestIt);
            if (auto* collection = Collections.FindPtr(collectionId)) {
                Y_ENSURE(collection->PendingRequests != 0);
                --collection->PendingRequests;
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
            if (auto* collection = Collections.FindPtr(msg->CollectionId);
                collection && collection->CacheItem == msg->CacheItem && collection->CoreKeepsPages &&
                collection->InMemoryOwners && collection->CoreKeepGeneration == msg->Generation)
            {
                PendingInMemoryPages[msg->CollectionId].emplace(msg->Location);
            }
        }

        void Handle(NSharedCache::TEvSync::TPtr&, const TActorContext&) {
            // Core references carry their own lifetime; private-page sync is obsolete after attach.
        }

        void Handle(NSharedCache::TEvUnregister::TPtr& ev, const TActorContext& ctx) {
            auto ownerIt = Owners.find(ev->Sender);
            if (ownerIt == Owners.end()) {
                return;
            }
            const auto collections = std::move(ownerIt->second);
            Owners.erase(ownerIt);
            Counters.Owners->Dec();
            for (TCollection* collection : collections) {
                CancelCoreRequests(collection->Id, ev->Sender);
                Y_ENSURE(collection->Owners.erase(ev->Sender));
                collection->StickyOwners.erase(ev->Sender);
                Counters.PageCollectionOwners->Dec();
                TryMoveToRegularCache(*collection, ev->Sender);
                const bool resumedSticky = ApplyCoreCollectionMode(*collection, ctx);
                const TLogoBlobID collectionId = collection->Id;
                Walks.UpdateSeeds(*collection, ev->Sender, {}, resumedSticky);
                if (auto* current = Collections.FindPtr(collectionId)) {
                    if (current->Owners.empty()) {
                        DetachCoreCollection(*current);
                    }
                    TryDropExpiredCollection(*current);
                }
            }
        }

        void Handle(NSharedCache::TEvDetach::TPtr& ev, const TActorContext& ctx) {
            const TLogoBlobID collectionId = ev->Get()->PageCollectionId;
            auto* collection = Collections.FindPtr(collectionId);
            if (!collection || !collection->Owners.erase(ev->Sender)) {
                return;
            }
            CancelCoreRequests(collectionId, ev->Sender);
            collection->StickyOwners.erase(ev->Sender);
            auto ownerIt = Owners.find(ev->Sender);
            Y_ENSURE(ownerIt != Owners.end() && ownerIt->second.erase(collection));
            Counters.PageCollectionOwners->Dec();
            TryMoveToRegularCache(*collection, ev->Sender);
            const bool resumedSticky = ApplyCoreCollectionMode(*collection, ctx);
            Walks.UpdateSeeds(*collection, ev->Sender, {}, resumedSticky);
            if (auto* current = Collections.FindPtr(collectionId)) {
                if (current->Owners.empty()) {
                    DetachCoreCollection(*current);
                }
                TryDropExpiredCollection(*current);
            }
        }

        void Handle(NSharedCache::TEvResult::TPtr& ev, const TActorContext&) {
            if (ev->Cookie == CoreKeepPreloadResultCookie) {
                const ui64 requestId = ev->Get()->Cookie;
                auto requestIt = CoreKeepPreloadRequests.find(requestId);
                Y_ENSURE(requestIt != CoreKeepPreloadRequests.end());
                TCoreKeepPreloadRequest request = std::move(requestIt->second);
                CoreKeepPreloadRequests.erase(requestIt);

                if (auto* collection = Collections.FindPtr(request.CollectionId);
                    collection && collection->CacheItem == request.CacheItem && collection->CoreKeepsPages &&
                    request.Generation == collection->CoreKeepGeneration)
                {
                    if (ev->Get()->Status == NKikimrProto::OK) {
                        Walks.IndexPagesChanged(collection->Id);
                    } else if (!request.Admitted) {
                        auto& pending = PendingInMemoryPages[collection->Id];
                        pending.insert(request.Locations.begin(), request.Locations.end());
                        CoreKeepPreloadRetryBlocked.insert(collection->Id);
                    } else {
                        if (auto pendingIt = PendingInMemoryPages.find(collection->Id);
                            pendingIt != PendingInMemoryPages.end())
                        {
                            for (const auto& location : request.Locations) {
                                pendingIt->second.erase(location);
                            }
                            if (pendingIt->second.empty()) {
                                PendingInMemoryPages.erase(pendingIt);
                            }
                        }
                        Walks.InvalidateDataCollection(collection->Id);
                        Walks.InvalidateIndexCollection(collection->Id);
                        for (const auto& owner : collection->InMemoryOwners) {
                            NotifyInMemOwnerAboutError(collection->PageCollection, ev->Get()->Status, owner);
                        }
                    }
                }
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

        void Handle(NBlockIO::TEvData::TPtr& ev, const TActorContext&) {
            CompleteCoreFetch(ev->Sender, *ev->Get());
        }

        TCollection& EnsureCollection(const TLogoBlobID& pageCollectionId,
            const NPageCollection::IPageCollection& pageCollection, const TActorId& owner) {
            TCollection& collection = Collections[pageCollectionId];
            if (!collection.Id) {
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE,
                    "Add page collection " << pageCollectionId);
                Counters.PageCollections->Inc();
                Y_ENSURE(pageCollectionId);
                collection.Id = pageCollectionId;
                collection.TotalPages = pageCollection.Total();
                collection.TotalSize =
                    NActors::TSharedData::OverheadSize * collection.TotalPages + pageCollection.BackingSize();
            } else {
                Y_DEBUG_ABORT_UNLESS(collection.Id == pageCollectionId);
                Y_ENSURE(collection.TotalPages == pageCollection.Total(),
                    "Page collection " << pageCollectionId << " changed number of pages from " << collection.TotalPages
                                       << " to " << pageCollection.Total() << " by " << owner);
            }
            return collection;
        }

        void TryDropExpiredCollection(TCollection& collection) override {
            if (!collection.Owners && !collection.CacheItem && !collection.SavedToCore &&
                collection.PendingRequests == 0 && Walks.IsIdle(collection.Id)) {
                Y_DEBUG_ABORT_UNLESS(collection.InMemoryOwners.empty());
                const TLogoBlobID id = collection.Id;
                Walks.EraseCollection(id);
                PendingInMemoryPages.erase(id);
                CoreKeepPreloadRetryBlocked.erase(id);
                Collections.erase(id);
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

        void ProcessGCList() {
            // Writer and loader references can still release their original page handles after core admission.
            while (auto page = SharedCachePages->GCList->PopGC()) {
                page->TryDrop();
            }
        }

        void ScheduleGC() {
            TActivationContext::AsActorContext().Schedule(
                TDuration::Seconds(15), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::DoGCScheduled)));
        }

        void NotifyInMemOwnerAboutError(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection,
            NKikimrProto::EReplyStatus error, const TActorId& owner) {
            TAutoPtr<NSharedCache::TEvResult> result = new NSharedCache::TEvResult(std::move(pageCollection), error, 0);

            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE,
                "Send page collection error " << result->PageCollection->Label() << " owner " << owner << " class "
                                              << NBlockIO::EPriority::Bulk << " error " << error << " cookie "
                                              << static_cast<ui64>(ERequestTypeCookie::TryKeepInMemPages)
                                              << " (in-memory preload)");

            Send(owner, result.Release(), 0, static_cast<ui64>(ERequestTypeCookie::TryKeepInMemPages));
        }

        TCoreQueue* CoreQueueForPriority(NBlockIO::EPriority priority) {
            switch (priority) {
                case NBlockIO::EPriority::Bkgr:
                    return &AsyncCoreRequests;
                case NBlockIO::EPriority::Bulk:
                case NBlockIO::EPriority::Low:
                    return &ScanCoreRequests;
                case NBlockIO::EPriority::None:
                case NBlockIO::EPriority::Fast:
                    return nullptr;
            }
            Y_UNREACHABLE();
        }

        TPageFetch TakeQueuedCorePage(const TLogoBlobID& collectionId, TPageOffset offset) {
            for (TCoreQueue* queue : { &AsyncCoreRequests, &ScanCoreRequests }) {
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

        void PumpCoreQueue(TCoreQueue& queue) {
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
                StartCoreFetch(request.PageCollection, std::move(batch), request.Priority, request.Sender,
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
            for (TCoreQueue* queue : { &AsyncCoreRequests, &ScanCoreRequests }) {
                for (auto it = queue->Requests.begin(); it != queue->Requests.end();) {
                    auto& requests = it->second;
                    for (auto requestIt = requests.begin(); requestIt != requests.end();) {
                        auto& queued = **requestIt;
                        const TLogoBlobID collectionId = queued.PageCollection->Label();
                        for (auto pageIt = queued.Pages.begin(); pageIt != queued.Pages.end();) {
                            const TPageOffset offset = pageIt->Location().Offset;
                            const bool needed = AnyOf(CoreRequests, [&](const auto& item) {
                                const TCoreRequest& request = item.second;
                                return !request.Cancelled && request.CollectionId == collectionId &&
                                       AnyOf(request.Completion->Locations(), [&](const TPageLocation& location) {
                                           return location.Offset == offset;
                                       });
                            });
                            if (needed) {
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
            Counters.InFlightPages->Add(pageCount);
            Counters.InFlightBytes->Add(bytes);
            const TActorId fetchActor =
                NBlockIO::Start(this, statActor, static_cast<ui64>(EBlockIOFetchTypeCookie::CoreFetch), request);
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
            Counters.InFlightPages->Sub(fetch.Pages.size());
            Counters.InFlightBytes->Sub(fetch.Bytes);

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
                Y_ENSURE(AsyncCoreRequests.InFly >= fetch.Bytes);
                AsyncCoreRequests.InFly -= fetch.Bytes;
                PumpCoreQueue(AsyncCoreRequests);
            } else if (fetch.QueueCookie == EBlockIOFetchTypeCookie::ScanQueue) {
                Y_ENSURE(ScanCoreRequests.InFly >= fetch.Bytes);
                ScanCoreRequests.InFly -= fetch.Bytes;
                PumpCoreQueue(ScanCoreRequests);
            }
            ActualizeCacheSizeLimit();
        }

        bool ApplyCoreCollectionMode(TCollection& collection, const TActorContext& ctx) {
            if (!collection.CacheItem) {
                return false;
            }

            const bool keepPages = bool(collection.InMemoryOwners);
            const bool stickyPages = !keepPages && bool(collection.StickyOwners);
            if (keepPages == collection.CoreKeepsPages && stickyPages == collection.CoreStickyPages) {
                return false;
            }

            const TCollectionCacheItem coreItem = collection.CacheItem;
            auto binding = CacheCore->BindCurrentThreadHazard();
            const ECacheMode mode = keepPages     ? ECacheMode::TryKeepInMemory
                                    : stickyPages ? ECacheMode::Sticky
                                                  : ECacheMode::Regular;
            const bool applied = CacheCore->SetCollectionPagesCacheMode(coreItem, mode);
            if (applied) {
                const bool resumedSticky = stickyPages && !collection.CoreStickyPages;
                const bool keepChanged = keepPages != collection.CoreKeepsPages;
                LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE,
                    "Set core page collection " << collection.Id << " mode to "
                                                << (keepPages        ? "Keep"
                                                       : stickyPages ? "Sticky"
                                                                     : "Regular"));
                collection.CoreKeepsPages = keepPages;
                collection.CoreStickyPages = stickyPages;
                if (keepChanged) {
                    TSharedCacheCollectionRef coreCollection;
                    Y_ENSURE(CacheCore->Find(collection.Id, coreCollection) == ESharedCacheResultStatus::Hit);
                    collection.CoreKeepGeneration = coreCollection.GetCollection().KeepGeneration();
                    CoreKeepPreloadRetryBlocked.erase(collection.Id);
                }
                return resumedSticky;
            } else {
                LOG_WARN_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE,
                    "Cannot apply cache mode " << (keepPages ? ECacheMode::TryKeepInMemory : ECacheMode::Regular)
                                               << " to core page collection " << collection.Id);
                return false;
            }
        }

        void TryMoveToRegularCache(TCollection& collection, const TActorId& owner) {
            if (!collection.InMemoryOwners.erase(owner) || collection.InMemoryOwners) {
                return;
            }
            Y_ENSURE(TargetInMemoryBytes >= collection.TotalSize);
            TargetInMemoryBytes -= collection.TotalSize;
            Counters.TargetInMemoryBytes->Set(TargetInMemoryBytes);
            for (auto& [_, request] : CoreRequests) {
                if (request.CollectionId == collection.Id && request.EventCookie == CoreKeepPreloadResultCookie) {
                    request.Cancelled = true;
                    request.Completion->Cancel();
                }
            }
            PruneCoreQueues();
            ActualizeCacheSizeLimit();
            Walks.DropIndexOnlyWalks(collection.Id);
        }

        void TryMoveToTryKeepInMemoryCache(TCollection& collection,
            TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
            if (!collection.InMemoryOwners.insert(owner).second || collection.InMemoryOwners.size() != 1) {
                return;
            }
            Walks.RestartForIndexCollection(collection.Id);
            TargetInMemoryBytes += collection.TotalSize;
            Counters.TargetInMemoryBytes->Set(TargetInMemoryBytes);
            const bool skipShadow = pageCollection->SkipBTreeIndexV1Shadow();
            auto& pagesToLoad = PendingInMemoryPages[collection.Id];
            for (const auto pageId : xrange(pageCollection->MetaPages())) {
                const auto type = static_cast<EPage>(pageCollection->Page(pageId).Type);
                if (!NPageCollection::IsDeadPage(type, skipShadow)) {
                    pagesToLoad.emplace(pageCollection->GetLocation(pageId));
                }
            }
            ActualizeCacheSizeLimit();
        }

        // The cache only enumerates the pages of a sticky collection, the owner fetches and keeps them.
        void SendStickyCollectionPages(
            TCollection& collection, const TActorId& owner, const TVector<TPageLocation>& locations) {
            if (!collection.Owners.contains(owner)) {
                return;
            }

            auto send = [&](auto first, auto last) {
                if (first == last) {
                    return;
                }

                TVector<TPageLocation> batch(first, last);
                Send(owner, new NSharedCache::TEvStickyCollectionPages(collection.Id, batch));
            };

            for (auto it = locations.begin(); it != locations.end();) {
                auto last = it + Min<size_t>(TEvStickyCollectionPages::MaxBatchLocations, locations.end() - it);
                send(it, last);
                it = last;
            }
        }

        void TryLoadInMemoryCollections() {
            if (PendingInMemoryPages.empty()) {
                return;
            }

            const ui64 keepColdLimit = CacheCore ? CacheCore->KeepColdLimit() : 0;
            ui64 committedKeepBytes = LastCoreKeepResidentBytes;
            ui64 pendingKeepBytes = 0;
            for (const auto& [_, request] : CoreKeepPreloadRequests) {
                Y_ENSURE(committedKeepBytes <= Max<ui64>() - request.ChargedBytes);
                committedKeepBytes += request.ChargedBytes;
                pendingKeepBytes += request.ChargedBytes;
            }
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
                    const TCollection* candidate = Collections.FindPtr(it->first);
                    // Routed Regular collections retain known hidden offsets until a Keep owner appears.
                    if (!CoreKeepPreloadRetryBlocked.contains(it->first) && !waitingForInFlight.contains(it->first) &&
                        (!candidate || candidate->InMemoryOwners))
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

                auto* collection = Collections.FindPtr(collectionId);
                if (!collection) {
                    PendingInMemoryPages.erase(it);
                    continue;
                }

                if (!collection->CoreKeepsPages) {
                    PendingInMemoryPages.erase(it);
                    continue;
                }

                TVector<TPageLocation> pagesToRequest;
                bool budgetBlocked = false;
                auto locationIt = pagesToLoad.begin();
                while (locationIt != pagesToLoad.end() &&
                       pagesToRequest.size() < TEvStickyCollectionPages::MaxBatchLocations)
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
                        IsCoreKeepPreloadInFlight(collectionId, collection->CoreKeepGeneration, location.Offset))
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
                    RequestCoreKeepPages(
                        *collection, collection->PageCollection, std::move(pagesToRequest), NBlockIO::EPriority::Bulk);
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
                if (record.GetConfig().HasBootstrapConfig() &&
                    record.GetConfig().GetBootstrapConfig().HasSharedCacheConfig()) {
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

            AsyncCoreRequests.Limit = Config.GetAsyncQueueInFlyLimit();
            ScanCoreRequests.Limit = Config.GetScanQueueInFlyLimit();
            PumpCoreQueue(AsyncCoreRequests);
            PumpCoreQueue(ScanCoreRequests);
        }

    public:
        TSharedPageCache(
            const TSharedCacheConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
            : Config(config)
            , Counters(counters)
        {
            Y_ENSURE(
                !Config.HasMemoryLimit() || Config.GetMemoryLimit() > 0, "Shared-cache MemoryLimit must be positive");
            AsyncCoreRequests.Limit = Config.GetAsyncQueueInFlyLimit();
            ScanCoreRequests.Limit = Config.GetScanQueueInFlyLimit();
        }

        void Bootstrap(const TActorContext& ctx) {
            LOG_NOTICE_S(
                ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Bootstrap with config " << Config.ShortDebugString());

            LimitReady = Config.HasMemoryLimit();
            MemLimitBytes = Config.GetMemoryLimit();
            if (LimitReady) {
                ActualizeCacheSizeLimit();
            }

            Send(NMemory::MakeMemoryControllerId(),
                new NMemory::TEvConsumerRegister(NMemory::EMemoryConsumerKind::SharedCache));

            Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
                new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
                    { NKikimrConsole::TConfigItem::BootstrapConfigItem,
                        NKikimrConsole::TConfigItem::SharedCacheConfigItem }));

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
    const TSharedCacheConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TSharedPageCache(config, GetServiceCounters(counters, "tablets")->GetSubgroup("type", "S_CACHE"));
}

} // namespace NKikimr::NSharedCache

template <>
inline void Out<TVector<ui32>>(IOutputStream& o, const TVector<ui32>& vec) {
    o << "[ ";
    for (const auto& x : vec) {
        o << x << ' ';
    }
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
void Out<TVector<NKikimr::NPageCollection::TLoadedPage>>(IOutputStream& o, const TVector<NKikimr::NPageCollection::TLoadedPage> &vec) {
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

template<> inline
void Out<TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>>>(IOutputStream& o, const TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x->Offset << ' ';
    o << "]";
}
