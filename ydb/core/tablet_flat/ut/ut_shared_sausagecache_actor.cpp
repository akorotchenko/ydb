#include <ydb/core/tablet_flat/cache_thread_test.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/tablet_flat/test/libs/table/shared_cache_fixture.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

#include <flat_sausagecache.h>
#include <flat_executor_bootlogic.h>
#include <flat_page_btree_index_writer.h>
#include <cache.h>
#include <shared_cache_btree_walk.h>
#include <shared_cache_counters.h>
#include <shared_cache_events.h>
#include <shared_sausagecache.h>
#include <shared_sausagecache_state.h>
#include <atomic>
#include <functional>
#include <new>
#include <thread>

namespace NKikimr::NSharedCache {
using namespace NActors;
using namespace NTabletFlatExecutor;
using namespace NPageCollection;

static const ui64 NO_QUEUE_COOKIE = 1;
static const ui64 ASYNC_QUEUE_COOKIE = 2;
static const ui64 TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE = 4;

static const ui64 CORE_FETCH_COOKIE = 5;

static const ui64 PAGE_TOTAL_SIZE = NActors::TSharedData::OverheadSize + 10;

static const ui64 DefaultMemoryLimit = 8 * PAGE_TOTAL_SIZE;

struct TFetch {
    ui64 Cookie;
    TIntrusiveConstPtr<IPageCollection> PageCollection;
    TVector<TPageLocation> Pages;

    TString DebugString() {
        TStringBuilder result;
        result << "PageCollection: " << PageCollection->Label();
        result << " Cookie: " << Cookie;
        result << " Pages: [";
        for (auto& page : Pages) {
            result << " " << page;
        }
        result << " ]";
        return result;
    }
};

static TPageLocation _P(ui32 id, EPage type = EPage::DataPage) {
    return TPageLocation::FromPageIndex(id, 10, type, id + 1);
}

struct TPageCollectionMock : public IPageCollection {
    TPageCollectionMock(ui64 id, ui32 totalPages)
        : Id(1, 1, id)
        , TotalPages(totalPages)
    {}

    const TLogoBlobID& Label() const noexcept override {
        return Id;
    }

    ui32 Total() const noexcept override {
        return TotalPages;
    }

    ui32 MetaPages() const noexcept override {
        Y_DEBUG_ABORT_UNLESS(PageTypes.size() <= TotalPages);
        return MetaPagesOverride.value_or(PageTypes.empty() ? TotalPages : static_cast<ui32>(PageTypes.size()));
    }

    TInfo Page(ui32 page) const override {
        auto type = page < PageTypes.size() ? PageTypes[page] : NTable::NPage::EPage::DataPage;
        return { 10, ui32(type) };
    }

    TBorder Bounds(ui32 page) const override {
        return { Page(page).Size, { page, 0 }, { page, ui32(Page(page).Size) } };
    }

    TBorder Bounds(const TPageLocation& location) const override {
        return Bounds(location.Offset.AsPageIndex());
    }

    TGlobId Glob(ui32) const override {
        Y_TABLET_ERROR("Unexpected Glob(...) call");
    }

    bool Verify(ui32, TArrayRef<const char>) const override {
        Y_TABLET_ERROR("Unexpected Verify(...) call");
    }

    bool Verify(const TPageLocation& location, TArrayRef<const char> data) const override {
        return data.size() == location.Size;
    }

    size_t BackingSize() const noexcept override {
        return 10 * TotalPages;
    }

    TPageLocation GetLocation(ui32 pageId) const override {
        return TPageLocation::FromPageIndex(pageId, 10, static_cast<EPage>(Page(pageId).Type), pageId + 1);
    }

    bool SkipBTreeIndexV1Shadow() const noexcept override {
        return SkipV1Shadow;
    }

    TVector<NTable::NPage::EPage> PageTypes;
    bool SkipV1Shadow = false;
    mutable std::optional<ui32> MetaPagesOverride;

private:
    TLogoBlobID Id;
    ui32 TotalPages;
};

struct TExecutorMock : public TActorBootstrapped<TExecutorMock> {
public:
    TExecutorMock(std::deque<NSharedCache::TEvResult::TPtr>& results, std::deque<NSharedCache::TEvResult::TPtr>& stickyResults)
        : Results(results)
        , StickyResults(stickyResults)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSharedCache::TEvResult, Handle);
        }
    }

    void Handle(NSharedCache::TEvResult::TPtr& ev) {
        if (ev->Cookie == ui64(ERequestTypeCookie::StickyPages) && ev->Get()->Cookie == 0) {
            StickyResults.push_back(ev);
        } else {
            Results.push_back(ev);
        }
    }

    std::deque<NSharedCache::TEvResult::TPtr>& Results;
    std::deque<NSharedCache::TEvResult::TPtr>& StickyResults;
};

struct TBootReadinessState {
    bool Started = false;
    bool Completed = false;
    bool Cancelled = false;
    bool CacheAvailable = false;
    ui32 ReadyMessages = 0;
    ui32 UnhandledReadyMessages = 0;
};

class TBootReadinessActor : public TActorBootstrapped<TBootReadinessActor> {
public:
    static constexpr ui64 BootAttempt = 42;

    TBootReadinessActor(TBootReadinessState& state, bool follower)
        : State(state)
        , Follower(follower)
        , Info(CreateTestTabletInfo(1, TTabletTypes::DataShard))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Logic.Reset(new TExecutorBootLogic(this, SelfId(), BootAttempt, Info.Get(), 1_MB));
        if (Follower) {
            Send(SelfId(), new TEvTablet::TEvFBoot(
                               Info->TabletID, 1, 1, TActorId{}, new TEvTablet::TDependencyGraph({ 0, 0 }), Info));
        } else {
            Send(SelfId(), new TEvTablet::TEvBoot(Info->TabletID, 1, nullptr, TActorId{}, Info));
        }
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTablet::TEvBoot, Handle);
            hFunc(TEvTablet::TEvFBoot, Handle);
            hFunc(TEvTablet::TEvRestored, Handle);
            cFunc(TEvents::TEvPoison::EventType, HandlePoison);
            default:
                if (ev->GetTypeRewrite() == TEvCacheReady::EventType) {
                    ++State.ReadyMessages;
                    const auto result = Logic->Receive(*ev);
                    State.UnhandledReadyMessages += result == TExecutorBootLogic::OpResultUnhandled;
                    CheckResult(result);
                }
                break;
        }
    }

    void Handle(TEvTablet::TEvBoot::TPtr& ev) {
        CheckResult(Logic->ReceiveBoot(ev, {}));
        State.Started = true;
        Send(SelfId(), new TEvTablet::TEvRestored(Info->TabletID, 1, SelfId(), false));
    }

    void Handle(TEvTablet::TEvFBoot::TPtr& ev) {
        CheckResult(Logic->ReceiveFollowerBoot(ev, {}));
        State.Started = true;
    }

    void Handle(TEvTablet::TEvRestored::TPtr& ev) {
        CheckResult(Logic->ReceiveRestored(ev));
    }

    void CheckResult(TExecutorBootLogic::EOpResult result) {
        Y_ABORT_UNLESS(result != TExecutorBootLogic::OpResultBroken);
        if (result == TExecutorBootLogic::OpResultComplete) {
            State.Completed = true;
            State.CacheAvailable = TSharedCache::TryGet() != nullptr;
        }
    }

    void HandlePoison() {
        Logic->Cancel();
        Logic.Reset();
        State.Cancelled = true;
        PassAway();
    }

    TBootReadinessState& State;
    const bool Follower;
    TIntrusivePtr<TTabletStorageInfo> Info;
    THolder<TExecutorBootLogic> Logic;
};

struct TSharedPageCacheMock {
    static TSharedCacheConfig DefaultConfig() {
        TSharedCacheConfig config;
        config.SetMemoryLimit(DefaultMemoryLimit);
        config.SetAsyncQueueInFlyLimit(19); // 2 in-fly pages
        config.SetInMemoryInFlyLimit(10 * PAGE_TOTAL_SIZE);
        return config;
    }

    TSharedPageCacheMock(
        const TSharedCacheConfig& config = DefaultConfig(), bool enableSchedule = false, ui64 nodeMemoryLimit = 0) {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        TCacheThreadTestAccess::Setup(Runtime);
        Runtime.Initialize(app->Unwrap());
        if (nodeMemoryLimit) {
            Runtime.GetAppData().MemoryControllerConfig.SetHardLimitBytes(nodeMemoryLimit);
        }
        Runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_TRACE);
        Runtime.SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_TRACE);

        ActorId = Runtime.Register(CreateSharedPageCache(config, Runtime.GetDynamicCounters()));
        if (enableSchedule) {
            Runtime.EnableScheduleForActor(ActorId);
        }

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);

        Sender1 = Runtime.Register(new TExecutorMock(Results, StickyResults));
        Sender2 = Runtime.Register(new TExecutorMock(Results, StickyResults));
        BlockIoSender = Runtime.AllocateEdgeActor();

        Fetches = MakeHolder<TBlockEvents<NBlockIO::TEvFetch>>(Runtime);

        Counters = MakeHolder<TCacheCounters>(
            GetServiceCounters(Runtime.GetDynamicCounters(), "tablets")->GetSubgroup("type", "S_CACHE"));
        if (config.HasMemoryLimit()) {
            SetLimit(config.GetMemoryLimit());
        }
    }

    ~TSharedPageCacheMock() {
        Send(Sender1, new TEvents::TEvPoison());
        TWaitForFirstEvent<TEvents::TEvPoison> waiter(Runtime);
        waiter.Wait();
        Results.clear();
        StickyResults.clear();
    }

    TSharedPageCacheMock& Wakeup() {
        auto wakeup = new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::DoGCManual));
        Send(Sender1, wakeup);

        TWaitForFirstEvent<TKikimrEvents::TEvWakeup> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& SetLimit(ui64 limitBytes, ui64 maximumBytes = 0, ui64 currentBytes = 0) {
        auto limit = new NMemory::TEvConsumerLimit(limitBytes, maximumBytes, currentBytes);
        Send(Sender1, limit);

        TWaitForFirstEvent<NMemory::TEvConsumerLimit> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    void PrepareKeepStorage(ui64 bytes) {
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        auto& cache = TSharedCache::Get();
        UNIT_ASSERT(cache.UpdateKeepLimit(bytes));
        for (ui64 step = 0; step < cache.TargetHandleCount() * 4 + 4096; ++step) {
            const bool work = cache.RunMaintenance();
            if (!work && !cache.NeedsCapacityMaintenance()) {
                return;
            }
        }
        UNIT_FAIL("Keep storage preparation did not converge");
    }

    TSharedPageCacheMock& UpdateConfig(const NKikimrConfig::TAppConfig& appConfig) {
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        request->Record.MutableConfig()->CopyFrom(appConfig);
        Send(Sender1, request.Release());

        TWaitForFirstEvent<NConsole::TEvConsole::TEvConfigNotificationRequest> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Request(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection, TVector<TPageLocation> locations, EPriority priority = EPriority::Fast, ui64 eventCookie = 0,
        TIntrusivePtr<TPagesWaitPad> waitPad = {}) {
        auto request = new TEvRequest(priority, collection, std::move(locations), ++RequestId);
        request->WaitPad = std::move(waitPad);
        Send(sender, request, eventCookie ? eventCookie : ui64(ERequestTypeCookie::Transaction));

        TWaitForFirstEvent<TEvRequest> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    THashMap<TActorId, TVector<TPageLocation>> TakeFetchActors(
        TIntrusiveConstPtr<TPageCollectionMock> collection, const TVector<TPageLocation>& locations) {
        THashMap<TActorId, TVector<TPageLocation>> groups;
        auto* actors = FetchActors.FindPtr(collection->Label());
        for (const auto& location : locations) {
            TActorId sender = BlockIoSender;
            if (actors) {
                if (auto it = actors->find(location.Offset); it != actors->end()) {
                    sender = it->second;
                    actors->erase(it);
                }
            }
            groups[sender].push_back(location);
        }
        return groups;
    }

    TSharedPageCacheMock& Provide(TIntrusiveConstPtr<TPageCollectionMock> collection, TVector<TPageLocation> locations,
        ui64 eventCookie = CORE_FETCH_COOKIE) { // event cookie -> queue type
        for (const auto& [sender, pages] : TakeFetchActors(collection, locations)) {
            auto data = new NBlockIO::TEvData(
                NKikimrProto::OK, collection, pages.size() * 10); // fetch cookie -> requested size
            for (const auto& loc : pages) {
                data->Pages.emplace_back(loc.Offset, TSharedData::Copy(TString(10, 'x')));
            }
            Send(sender, data, eventCookie);
        }

        // TODO: why this broke everything?
        // TWaitForFirstEvent<NBlockIO::TEvData> waiter(Runtime);
        // waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Fail(TIntrusiveConstPtr<TPageCollectionMock> collection, TVector<TPageLocation> locations,
        ui64 eventCookie = CORE_FETCH_COOKIE) {
        for (const auto& [sender, pages] : TakeFetchActors(collection, locations)) {
            auto data = new NBlockIO::TEvData(NKikimrProto::ERROR, collection, pages.size() * 10);
            for (const auto& location : pages) {
                data->Pages.emplace_back(location.Offset, TSharedData{});
            }
            Send(sender, data, eventCookie);
        }

        return *this;
    }

    TSharedPageCacheMock& Attach(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection,
        ECacheMode cacheMode = ECacheMode::Regular, TVector<TEvAttach::TBtreeSeed> btreeSeeds = {},
        bool metadataOnly = false, bool replayStickyWalk = false, bool stickyPages = true,
        TIntrusivePtr<TCollectionOwnerStats> ownerStats = {}) {
        if (metadataOnly) {
            collection->MetaPagesOverride = static_cast<ui32>(collection->PageTypes.size());
        }
        const ECacheMode mode =
            cacheMode == ECacheMode::Regular && metadataOnly && stickyPages ? ECacheMode::Sticky : cacheMode;
        auto attach = new TEvAttach(collection, mode, std::move(btreeSeeds), replayStickyWalk);
        attach->OwnerStats = std::move(ownerStats);
        Send(sender, attach, 1); // These tests inspect the core item in the optional acknowledgement.

        TWaitForFirstEvent<TEvAttach> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Detach(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection) {
        auto detach = new TEvDetach(collection->Label());
        Send(sender, detach);

        TWaitForFirstEvent<TEvDetach> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Unregister(TActorId sender) {
        auto unregister = new TEvUnregister();
        Send(sender, unregister);

        TWaitForFirstEvent<TEvUnregister> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& CheckFetches(const TVector<TFetch>& expected) {
        if (expected.empty()) {
            Runtime.SimulateSleep(TDuration::Seconds(1));
        } else {
            Runtime.WaitFor(TStringBuilder() << "fetches #" << RequestId, 
                [&]{return Fetches->size() >= expected.size();}, TDuration::Seconds(5));
        }

        TVector<TFetch> actual;
        for (auto& f : *Fetches) {
            auto &fetch = *f->Get();
            actual.push_back({fetch.Cookie, fetch.PageCollection, fetch.Pages});
            for (const auto& page : fetch.Pages) {
                FetchActors[fetch.PageCollection->Label()][page.Offset] = f->GetRecipientRewrite();
            }
        }
        Fetches->clear();

        Cerr << "Checking fetches#" << RequestId << Endl;
        CheckFetches(expected, actual);

        return *this;
    }

    THashMap<TPageId, TCachePageRef> CheckResults(
        TVector<TFetch> expected, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
        if (expected.empty()) {
            Runtime.SimulateSleep(TDuration::Seconds(1));
        } else {
            Runtime.WaitFor(TStringBuilder() << "results #" << RequestId, 
                [&]{return Results.size() >= expected.size();}, TDuration::Seconds(5));
        }

        TVector<TFetch> actual;
        THashMap<TPageId, TCachePageRef> pages;
        for (auto& r : Results) {
            UNIT_ASSERT_VALUES_EQUAL(r->Get()->Status, status);
            auto& result = *r->Get();
            actual.push_back(TFetch{result.Cookie, result.PageCollection, {}});
            for (auto& p : r->Get()->Pages) {
                actual.back().Pages.push_back(_P(p.Offset.AsPageIndex()));
                pages.emplace(p.Offset.AsPageIndex(), std::move(p.Page));
            }
        }
        Results.clear();
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        Cerr << "Checking results#" << RequestId << Endl;
        CheckFetches(expected, actual);

        return pages;
    }

    void CheckFetches(TVector<TFetch> expected, TVector<TFetch> actual) {
        // blocked results to different senders may be reordered, sort them before check:
        auto cmp = [](const auto& l, const auto& r){
            if (l.PageCollection->Label() != r.PageCollection->Label()) {
                return l.PageCollection->Label() < r.PageCollection->Label();
            }
            if (l.Cookie != r.Cookie) {
                return l.Cookie < r.Cookie;
            }
            return l.Pages < r.Pages;
        };
        Sort(expected, cmp);
        Sort(actual, cmp);
        // pages within a single fetch may also be in any order (e.g. from THashSet iteration)
        for (auto& f : expected)
            Sort(f.Pages);
        for (auto& f : actual)
            Sort(f.Pages);

        Cerr << "Expected:" << Endl;
        for (auto f : expected) {
            Cerr << "  " << f.DebugString() << Endl;
        }
        Cerr << "Actual:" << Endl;
        for (auto f : actual) {
            Cerr << "  " << f.DebugString() << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (auto i : xrange(expected.size())) {
            UNIT_ASSERT_VALUES_EQUAL(actual[i].PageCollection->Label(), expected[i].PageCollection->Label());
            UNIT_ASSERT_VALUES_EQUAL(actual[i].Pages, expected[i].Pages);
            UNIT_ASSERT_VALUES_EQUAL(actual[i].Cookie, expected[i].Cookie);
        }
    }

    void Send(TActorId sender, IEventBase* ev, ui64 cookie = 0) {
        Runtime.Send(new IEventHandle(ActorId, sender, ev, 0, cookie), 0, true);
    }

    TTestActorRuntime Runtime;
    TActorId ActorId;
    ui64 RequestId = 0;
    THolder<TCacheCounters> Counters;

    THolder<TBlockEvents<NBlockIO::TEvFetch>> Fetches;
    std::deque<NSharedCache::TEvResult::TPtr> Results;
    std::deque<NSharedCache::TEvResult::TPtr> StickyResults;
    THashMap<TLogoBlobID, THashMap<TPageOffset, TActorId>> FetchActors;

    TActorId Sender1;
    TActorId Sender2;
    TActorId BlockIoSender;
    TIntrusiveConstPtr<TPageCollectionMock> Collection1 = new TPageCollectionMock(1, 100);
    TIntrusiveConstPtr<TPageCollectionMock> Collection2 = new TPageCollectionMock(2, 100);
};

Y_UNIT_TEST_SUITE(TSharedPageCache_Actor) {
    void CheckInitialCollectionMode(ECacheMode ownerMode, bool actorCreates) {
        TSharedPageCacheMock sharedCache;
        auto& cache = TSharedCache::Get();
        auto source = MakeIntrusiveConst<TPageCollectionMock>(ui64{ 19800 }, ui32{ 0 });
        const auto id = source->Label();
        bool seenReady = false;
        bool ownerModePublished = false;
        ECacheMode initialMode = ECacheMode::Sticky;
        NActors::TTestActorRuntimeBase::TEventFilter previousFilter;
        previousFilter = sharedCache.Runtime.SetEventFilter([&](NActors::TTestActorRuntimeBase& runtime,
                                                                TAutoPtr<IEventHandle>& event) {
            if (event->GetRecipientRewrite() == sharedCache.ActorId &&
                event->GetTypeRewrite() == TEvResourcesAvailable::EventType) {
                if (!seenReady) {
                    TCacheCollectionRef published;
                    if (cache.Find(id, published) == ECacheResultStatus::Hit) {
                        seenReady = true;
                        initialMode = published->GetCacheMode();
                        if (!actorCreates) {
                            // Empty metadata lets this native controller update send no nested events
                            ownerModePublished = cache.SetCollectionPagesCacheMode(published.CacheItem(), ownerMode);
                        }
                    } else {
                        cache.ArmResourceWait(cache.AdmissionGeneration());
                    }
                }
                return true; // Consume the test-owned synchronous publication notification
            }
            return previousFilter(runtime, event);
        });
        Y_DEFER {
            sharedCache.Runtime.SetEventFilter(std::move(previousFilter));
        };
        cache.ArmResourceWait(cache.AdmissionGeneration());
        TCacheCollectionRef result;
        if (actorCreates) {
            sharedCache.Attach(sharedCache.Sender1, source, ownerMode);
            UNIT_ASSERT(cache.Find(id, result) == ECacheResultStatus::Hit);
        } else {
            result = cache.AdmitCollection(source);
        }
        UNIT_ASSERT(seenReady);
        UNIT_ASSERT(actorCreates || ownerModePublished);
        UNIT_ASSERT(result->GetCacheMode() == ownerMode);
        UNIT_ASSERT(initialMode == ECacheMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(result->KeepGeneration(), ownerMode == ECacheMode::TryKeepInMemory ? 1 : 0);
        auto joined = cache.AdmitCollection(source);
        UNIT_ASSERT(joined.CacheItem() == result.CacheItem());
        UNIT_ASSERT(joined->GetCacheMode() == ownerMode);
    }

    Y_UNIT_TEST(LoaderPublishesRegularBeforeKeepOwnerMode) {
        CheckInitialCollectionMode(ECacheMode::TryKeepInMemory, false);
    }

    Y_UNIT_TEST(LoaderPublishesRegularBeforeStickyOwnerMode) {
        CheckInitialCollectionMode(ECacheMode::Sticky, false);
    }

    Y_UNIT_TEST(ActorPublishesRegularBeforeKeepOwnerMode) {
        CheckInitialCollectionMode(ECacheMode::TryKeepInMemory, true);
    }

    Y_UNIT_TEST(ActorPublishesRegularBeforeStickyOwnerMode) {
        CheckInitialCollectionMode(ECacheMode::Sticky, true);
    }

    Y_UNIT_TEST(QueuedResultIsAbandonedDuringRuntimeShutdown) {
        UNIT_ASSERT(!TlsActivationContext && !TlsCacheHazard);
        {
            TSharedPageCacheMock sharedCache;
            UNIT_ASSERT_VALUES_EQUAL(TActivationContext::ActorSystem(), sharedCache.Runtime.GetActorSystem(0));
            UNIT_ASSERT_VALUES_EQUAL(&TSharedCache::Get(), sharedCache.Runtime.GetAppData().SharedCache.Get());
            auto source = MakeIntrusiveConst<TPageCollectionMock>(ui64{ 19762 }, ui32{ 1 });
            auto page = TSharedCache::Get().AdmitPage(source, _P(0), TSharedData::Copy(TString(10, 'q')));
            UNIT_ASSERT(page);
            auto* result = new TEvResult(source, NKikimrProto::OK, 0);
            result->Pages.emplace_back(_P(0).Offset, page.size(), std::move(page));
            const auto recipient = sharedCache.Runtime.AllocateEdgeActor();
            sharedCache.Runtime.Send(new IEventHandle(recipient, {}, result), 0, true);
        }
        UNIT_ASSERT(!TlsActivationContext && !TlsCacheHazard);
    }

    void ReviewBootWaitsForCache(bool follower) {
        TBootReadinessState state;
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.ClearMemoryLimit();
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Runtime.RegisterService(MakeSharedPageCacheId(), sharedCache.ActorId);
        const auto bootActor = sharedCache.Runtime.Register(new TBootReadinessActor(state, follower));
        sharedCache.Runtime.WaitFor("boot readiness request", [&] {
            return state.Started;
        });
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(!state.Completed);
        UNIT_ASSERT(!TSharedCache::TryGet());

        sharedCache.Runtime.Send(new IEventHandle(
            bootActor, sharedCache.Sender1, new TEvCacheReady, 0, TBootReadinessActor::BootAttempt - 1));
        sharedCache.Runtime.WaitFor("stale cache acknowledgement", [&] {
            return state.ReadyMessages == 1;
        });
        UNIT_ASSERT_VALUES_EQUAL(state.UnhandledReadyMessages, 1);
        UNIT_ASSERT(!state.Completed);
        UNIT_ASSERT(!TSharedCache::TryGet());

        sharedCache.SetLimit(0, 32_MB);
        sharedCache.Runtime.WaitFor("boot after cache initialization", [&] {
            return state.Completed;
        });
        UNIT_ASSERT(state.CacheAvailable);
        UNIT_ASSERT(TSharedCache::TryGet());

        sharedCache.Runtime.Send(
            new IEventHandle(bootActor, sharedCache.Sender1, new TEvCacheReady, 0, TBootReadinessActor::BootAttempt));
        sharedCache.Runtime.WaitFor("duplicate cache acknowledgement", [&] {
            return state.ReadyMessages == 3;
        });
        UNIT_ASSERT_VALUES_EQUAL(state.UnhandledReadyMessages, 2);
    }

    Y_UNIT_TEST(ReviewR9LeaderBootWaitsForCacheInitialization) {
        ReviewBootWaitsForCache(false);
    }

    Y_UNIT_TEST(ReviewR9FollowerBootWaitsForCacheInitialization) {
        ReviewBootWaitsForCache(true);
    }

    Y_UNIT_TEST(ReviewR9CancellationDropsPendingReadinessStage) {
        TBootReadinessState state;
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.ClearMemoryLimit();
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Runtime.RegisterService(MakeSharedPageCacheId(), sharedCache.ActorId);
        const auto bootActor = sharedCache.Runtime.Register(new TBootReadinessActor(state, true));
        sharedCache.Runtime.WaitFor("boot waiting for cache", [&] {
            return state.Started;
        });
        sharedCache.Runtime.Send(new IEventHandle(bootActor, sharedCache.Sender1, new TEvents::TEvPoison));
        sharedCache.Runtime.WaitFor("boot cancelled before cache initialization", [&] {
            return state.Cancelled;
        });
        UNIT_ASSERT(!state.Completed);
        UNIT_ASSERT(!TSharedCache::TryGet());
        sharedCache.SetLimit(0, 32_MB);
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(!state.Completed);
        UNIT_ASSERT(TSharedCache::TryGet());
    }

    Y_UNIT_TEST(ZeroMemoryLimitRejected) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(0);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TSharedPageCacheMock(config), yexception, "Shared-cache MemoryLimit must be positive");
        UNIT_ASSERT(!TlsActivationContext && !TlsCacheHazard);
    }

    Y_UNIT_TEST(ReservationUsesNodeCeilingAndAllowsLaterGrowth) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(8_MB);
        TSharedPageCacheMock sharedCache(config, false, 128_MB);
        auto* core = TSharedCache::TryGet();
        UNIT_ASSERT(core);
        UNIT_ASSERT_VALUES_EQUAL(core->ReservationLimit(), 128_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 8_MB);

        sharedCache.SetLimit(0, 64_MB);
        NKikimrConfig::TAppConfig update;
        update.MutableSharedCacheConfig()->CopyFrom(config);
        update.MutableSharedCacheConfig()->SetMemoryLimit(48_MB);
        sharedCache.UpdateConfig(update);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 48_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->ReservationLimit(), 128_MB);

        update.MutableSharedCacheConfig()->ClearMemoryLimit();
        sharedCache.UpdateConfig(update);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 64_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->ReservationLimit(), 128_MB);
    }

    Y_UNIT_TEST(ReservationPreservesKnownControllerCeiling) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.ClearMemoryLimit();
        TSharedPageCacheMock sharedCache(config, false, 16_MB);
        UNIT_ASSERT(!TSharedCache::TryGet());
        sharedCache.SetLimit(0, 64_MB);
        auto* core = TSharedCache::TryGet();
        UNIT_ASSERT(core);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 64_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->ReservationLimit(), 64_MB);
    }

    Y_UNIT_TEST(ControllerMaximumSizesCoreBeforeZeroAllocationAdmission) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.ClearMemoryLimit();
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0) });
        sharedCache.CheckFetches({});
        UNIT_ASSERT(!TSharedCache::TryGet());
        sharedCache.Send(sharedCache.Sender1, new NMemory::TEvConsumerLimit(0, 32_MB));
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        UNIT_ASSERT(TSharedCache::TryGet() == sharedCache.Runtime.GetAppData().SharedCache.Get());
        sharedCache.Provide(sharedCache.Collection1, { _P(0) });
        auto pages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0) } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 32_MB);
        UNIT_ASSERT_GE(core->HardLimit(), core->CurrentLimit());
        UNIT_ASSERT_VALUES_EQUAL(pages.size(), 1);
    }

    Y_UNIT_TEST(ClearingConfiguredCeilingBeforeControllerRetainsInitializedCache) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(32_MB);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0) });
        auto pages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0) } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        NKikimrConfig::TAppConfig update;
        update.MutableSharedCacheConfig()->CopyFrom(config);
        update.MutableSharedCacheConfig()->ClearMemoryLimit();
        sharedCache.UpdateConfig(update);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 32_MB);
        sharedCache.SetLimit(0);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 0);
        sharedCache.SetLimit(0, 64_MB, 16_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 64_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 16_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 0);
        TCachePageRef pinned = std::move(pages.at(0));
        UNIT_ASSERT_VALUES_EQUAL(pinned.BuildSharedData().size(), 10);
        sharedCache.SetLimit(0, 64_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 16_MB);
    }

    Y_UNIT_TEST(ClearingConfiguredCeilingBeforeUseRetainsInitializedCache) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(32_MB);
        TSharedPageCacheMock sharedCache(config);
        NKikimrConfig::TAppConfig update;
        update.MutableSharedCacheConfig()->CopyFrom(config);
        update.MutableSharedCacheConfig()->ClearMemoryLimit();
        sharedCache.UpdateConfig(update);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0) });
        auto pages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0) } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(pages.size(), 1);
    }

    Y_UNIT_TEST(ConfiguredCeilingReleasesDeferredRequestsWithoutController) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.ClearMemoryLimit();
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0) });
        sharedCache.CheckFetches({});
        NKikimrConfig::TAppConfig update;
        update.MutableSharedCacheConfig()->CopyFrom(config);
        update.MutableSharedCacheConfig()->SetMemoryLimit(32_MB);
        sharedCache.UpdateConfig(update);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0) });
        auto pages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0) } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(pages.size(), 1);
    }

    Y_UNIT_TEST(CurrentAndSoftLimitsDoNotResizeCapacity) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(32_MB);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.SetLimit(4_MB, 64_MB, 16_MB);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0) });
        auto pages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0) } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        const ui64 handles = core->PhysicalHandleCount();
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 16_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 4_MB);
        sharedCache.SetLimit(0, 64_MB, 8_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 8_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(core->PhysicalHandleCount(), handles);
        NKikimrConfig::TAppConfig update;
        update.MutableSharedCacheConfig()->CopyFrom(config);
        update.MutableSharedCacheConfig()->SetMemoryLimit(16_MB);
        sharedCache.UpdateConfig(update);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 16_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 8_MB);
        sharedCache.SetLimit(6_MB, 64_MB, 24_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 16_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 16_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 6_MB);
        update.MutableSharedCacheConfig()->SetMemoryLimit(48_MB);
        sharedCache.UpdateConfig(update);
        UNIT_ASSERT_VALUES_EQUAL(core->HardLimit(), 48_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->CurrentLimit(), 24_MB);
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 6_MB);
    }

    Y_UNIT_TEST(HardGrowthPreservesKeepBudgetWithPendingPreload) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(1_MB);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.SetLimit(20 * PAGE_TOTAL_SIZE, 4_MB, 1_MB);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches(
            { TFetch{ 80, sharedCache.Collection1, { _P(0), _P(1), _P(2), _P(3), _P(4), _P(5), _P(6), _P(7) } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        const ui64 keepLimit = core->KeepLimit();
        UNIT_ASSERT(keepLimit > 0);
        NKikimrConfig::TAppConfig update;
        update.MutableSharedCacheConfig()->CopyFrom(config);
        update.MutableSharedCacheConfig()->SetMemoryLimit(4_MB);
        sharedCache.UpdateConfig(update);
        for (ui32 step = 0; step < 200 && core->PhysicalHandleCount() != core->TargetHandleCount(); ++step) {
            sharedCache.Send(sharedCache.Sender1, new TKikimrEvents::TEvWakeup(ui64(EWakeupTag::DoGCManual)));
            TWaitForFirstEvent<TKikimrEvents::TEvWakeup> maintenance(sharedCache.Runtime);
            maintenance.Wait();
        }
        UNIT_ASSERT_VALUES_EQUAL(core->PhysicalHandleCount(), core->TargetHandleCount());
        UNIT_ASSERT(core->StaticBytes() > core->SoftLimit());
        UNIT_ASSERT_VALUES_EQUAL(core->KeepLimit(), keepLimit);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 8);
    }

    Y_UNIT_TEST(ZeroConsumerLimitPreservesHardAdmissionLimit) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(0);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0) });
        auto pages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0) } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        UNIT_ASSERT_VALUES_EQUAL(core->SoftLimit(), 0);
        UNIT_ASSERT(core->CurrentLimit() > 0);
        UNIT_ASSERT_VALUES_EQUAL(pages.size(), 1);
    }

    void CheckOwnerHoldsReleased(bool unregister) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(ui64{ 7 }, ui32{ 1 });
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1); // Idempotent mode/update registration.
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1);
        auto* core = &NSharedCache::TSharedCache::Get();
        TCacheCollectionRef collection;
        UNIT_ASSERT(core->Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
        const auto item = collection.CacheItem();
        collection.Drop();
        UNIT_ASSERT_VALUES_EQUAL(core->Collections(), 1);

        if (unregister) {
            sharedCache.Unregister(sharedCache.Sender1);
            sharedCache.Unregister(sharedCache.Sender1);
        } else {
            sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
            sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        }
        UNIT_ASSERT(core->Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
        collection.Drop();
        UNIT_ASSERT(!core->EraseCollection(item)); // The surviving owner's registry attachment is still active.

        if (unregister) {
            sharedCache.Unregister(sharedCache.Sender2);
        } else {
            sharedCache.Detach(sharedCache.Sender2, sharedCache.Collection1);
        }
        UNIT_ASSERT(core->EraseCollection(item));
        // No owner ref was leaked by the repeated attach or released twice by duplicate cleanup.
        UNIT_ASSERT_VALUES_EQUAL(core->Collections(), 0);
    }

    Y_UNIT_TEST(OwnerHoldsReleasedOnFinalDetach) {
        CheckOwnerHoldsReleased(false);
    }

    Y_UNIT_TEST(OwnerHoldsReleasedOnUnregister) {
        CheckOwnerHoldsReleased(true);
    }

    Y_UNIT_TEST_F(ApplicationCacheSharesOwnersAcrossThreads, NTable::NTest::TTableCacheTest) {
        auto& pages = TSharedCache::Get();
        auto* actorSystem = &CacheContext().ActorSystem();
        TIntrusiveConstPtr<TPageCollectionMock> source = new TPageCollectionMock(19761, 1);
        auto collection = pages.AdmitCollection(source);
        auto page = pages.AdmitPage(source, _P(0), TSharedData::Copy(TString(10, 's')));
        UNIT_ASSERT(page);
        const auto item = page.CacheItem();
        const char* data = page.data();
        std::atomic<bool> start{ false };
        TVector<std::thread> threads;
        for (ui32 index = 0; index < 8; ++index) {
            threads.emplace_back([&, index, owner = page.Acquire(), metadata = collection.Acquire()]() mutable {
                TCacheTestThreadContext context(*actorSystem, TCacheThreadTestAccess::Hazard(pages, index + 1));
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                UNIT_ASSERT_VALUES_EQUAL(&TSharedCache::Get(), &pages);
                for (ui32 repeat = 0; repeat < 100; ++repeat) {
                    auto acquired = owner.Acquire();
                    UNIT_ASSERT(acquired.CacheItem() == item);
                    UNIT_ASSERT_VALUES_EQUAL(acquired.data(), data);
                    UNIT_ASSERT_VALUES_EQUAL(acquired.data()[0], 's');
                    UNIT_ASSERT_VALUES_EQUAL(metadata->Id(), source->Label());
                }
                owner.Drop();
                metadata.Drop();
            });
        }
        page.Drop();
        collection.Drop();
        start.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }
    }

    Y_UNIT_TEST(CollectionMetadataReadsCoreWithoutAttachAcknowledgement) {
        TSharedPageCacheMock sharedCache;
        const TPageLocation location = _P(1, EPage::DataPage);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { location });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { location } } });
        sharedCache.Provide(sharedCache.Collection1, { location });
        auto loaded = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });
        auto collection = TSharedCache::Get().AdmitCollection(sharedCache.Collection1);
        auto page = collection->TryGetPage(location);
        UNIT_ASSERT(page);
        UNIT_ASSERT_VALUES_EQUAL(page.size(), 10);
    }

    Y_UNIT_TEST(CoreStickyFetchIgnoresDuplicateCompletion) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        const TActorId fetchActor = sharedCache.FetchActors[sharedCache.Collection1->Label()][page.Offset];
        UNIT_ASSERT(fetchActor);
        sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);

        auto duplicate = new NBlockIO::TEvData(NKikimrProto::OK, sharedCache.Collection1, 10);
        duplicate->Pages.emplace_back(page.Offset, TSharedData::Copy(TString(10, 'x')));
        sharedCache.Send(fetchActor, duplicate, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({});

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);

        const TPageLocation failedPage = _P(2, EPage::DataPage);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { failedPage }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { failedPage } } });
        sharedCache.Fail(sharedCache.Collection1, { failedPage }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 3, sharedCache.Collection1, {} } }, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
    }

    Y_UNIT_TEST(CoreStickyFetchAfterDetachFails) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
    }

    Y_UNIT_TEST(OwnerStatsFollowRegistrationModeAndResidentBytes) {
        TSharedPageCacheMock sharedCache;
        auto first = MakeIntrusive<TCollectionOwnerStats>();
        auto second = MakeIntrusive<TCollectionOwnerStats>();
        const auto attach = [&](TActorId owner, ECacheMode mode, const TIntrusivePtr<TCollectionOwnerStats>& stats) {
            sharedCache.Attach(owner, sharedCache.Collection1, mode, {}, true, false, false, stats);
        };
        attach(sharedCache.Sender1, ECacheMode::Sticky, first);
        attach(sharedCache.Sender1, ECacheMode::Sticky, first);
        attach(sharedCache.Sender2, ECacheMode::Regular, second);
        UNIT_ASSERT_VALUES_EQUAL(first->Read().PageCollections, 1);
        UNIT_ASSERT_VALUES_EQUAL(second->Read().PageCollections, 1);
        const auto page = _P(1, EPage::DataPage);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page });
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });
        sharedCache.Runtime.WaitFor("resident owner statistics", [&] {
            return first->Read().SharedBodyBytes == PAGE_TOTAL_SIZE &&
                   second->Read().SharedBodyBytes == PAGE_TOTAL_SIZE;
        });
        UNIT_ASSERT_VALUES_EQUAL(first->Read().StickyBytes, PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(second->Read().StickyBytes, 0);
        attach(sharedCache.Sender2, ECacheMode::TryKeepInMemory, second);
        UNIT_ASSERT_VALUES_EQUAL(second->Read().TryKeepInMemoryBytes, sharedCache.Collection1->BackingSize());
        attach(sharedCache.Sender1, ECacheMode::Regular, first);
        UNIT_ASSERT_VALUES_EQUAL(first->Read().StickyBytes, 0);
        attach(sharedCache.Sender2, ECacheMode::Regular, second);
        UNIT_ASSERT_VALUES_EQUAL(second->Read().TryKeepInMemoryBytes, 0);
        sharedCache.SetLimit(0).Wakeup();
        sharedCache.Runtime.WaitFor("evicted owner statistics", [&] {
            return first->Read().SharedBodyBytes == 0 && second->Read().SharedBodyBytes == 0;
        });
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(first->Read().PageCollections, 0);
        sharedCache.Unregister(sharedCache.Sender2);
        UNIT_ASSERT_VALUES_EQUAL(second->Read().PageCollections, 0);
    }

    Y_UNIT_TEST(LateStickyRequestDoesNotReattachDetachedOwner) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1);
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(1, EPage::DataPage) }, EPriority::Bkgr,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);
    }

    Y_UNIT_TEST(CoreCollectionReattachWhileCancellationDrains) {
        for (bool unregister : { false, true }) {
            TSharedPageCacheMock sharedCache;
            const TPageLocation page = _P(1, EPage::DataPage);
            sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
            // Native completions have no sender actor; block them without actor-name logging.
            TVector<TEvRequestAnswered::TPtr> answered;
            auto blocker = sharedCache.Runtime.AddObserver<TEvRequestAnswered>([&](auto& ev) {
                answered.push_back(std::move(ev));
            });
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
            sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
            auto& core = TSharedCache::Get();
            TCacheCollectionRef collection;
            UNIT_ASSERT(core.Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
            const auto item = collection.CacheItem();
            UNIT_ASSERT_VALUES_EQUAL(collection->GetActorState()->PendingRequests, 1);

            if (unregister) {
                sharedCache.Unregister(sharedCache.Sender1);
            } else {
                sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
            }
            sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
            sharedCache.Runtime.WaitFor("cancelled request notification", [&] {
                return !answered.empty();
            });
            UNIT_ASSERT(collection->GetActorState());
            UNIT_ASSERT(collection->GetActorState()->Owners.empty());
            UNIT_ASSERT(collection->GetCacheMode() == ECacheMode::Regular);

            sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
            TCacheCollectionRef reattached;
            UNIT_ASSERT(core.Find(sharedCache.Collection1->Label(), reattached) == ECacheResultStatus::Hit);
            UNIT_ASSERT(reattached.CacheItem() == item);
            UNIT_ASSERT(reattached.Get() == collection.Get());
            blocker.Remove();
            for (auto& ev : answered) {
                sharedCache.Runtime.Send(ev.Release(), 0, true);
            }
            sharedCache.Runtime.WaitFor("old request drained", [&] {
                return collection->GetActorState()->PendingRequests == 0;
            });
            UNIT_ASSERT(collection->GetActorState()->Owners.contains(sharedCache.Sender1));
            sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
            UNIT_ASSERT(!collection->GetActorState());
            // The original fetch still owns its native item after actor bookkeeping is removed.
            sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
            sharedCache.Runtime.WaitFor("late fetch completed", [&] {
                return sharedCache.Counters->LoadInFlyPages->Val() == 0;
            });
            UNIT_ASSERT(!collection->GetActorState());
        }
    }

    Y_UNIT_TEST(CoreStickyAsyncQueueCompletes) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation first = _P(1, EPage::DataPage);
        const TPageLocation second = _P(2, EPage::DataPage);
        const TPageLocation third = _P(3, EPage::DataPage);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { first, second, third }, EPriority::Bkgr,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { first, second } } });
        sharedCache.Provide(sharedCache.Collection1, { first, second }, CORE_FETCH_COOKIE);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { third } } });
        sharedCache.Provide(sharedCache.Collection1, { third }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1), _P(2), _P(3) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
    }

    Y_UNIT_TEST(CoreStickyQueuedDetachReportsRace) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation first = _P(1, EPage::DataPage);
        const TPageLocation second = _P(2, EPage::DataPage);
        const TPageLocation third = _P(3, EPage::DataPage);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { first, second, third }, EPriority::Bkgr,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { first, second } } });
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.CheckFetches({});
        sharedCache.Provide(sharedCache.Collection1, { first, second }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
    }

    Y_UNIT_TEST(CoreStickyBtreeWalkFailureCompletes) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation root = _P(0, EPage::BTreeIndexV2);
        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = root;
        seed.LevelCount = 1;
        seed.QueueDataPages = false;
        seed.Sticky = true;
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { seed }, true);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { root } } });
        sharedCache.Fail(sharedCache.Collection1, { root }, CORE_FETCH_COOKIE);
        sharedCache.Runtime.WaitFor("failed core walk fetch", [&] {
            return sharedCache.Counters->LoadInFlyPages->Val() == 0;
        }, TDuration::Seconds(5));
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(CoreStickyAdmitsCompactedPageBeforeAttach) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation location = _P(1, EPage::DataPage);
        auto saved = MakeHolder<TEvSaveCompactedPages>(sharedCache.Collection1);
        auto page = saved->AddPage(TSharedCache::Get(), { location, TSharedData::Copy(TString(10, 'x')) }, true);
        UNIT_ASSERT(page && page.IsSticky());
        page.Drop();
        sharedCache.Send(sharedCache.Sender1, saved.Release());
        TWaitForFirstEvent<TEvSaveCompactedPages> waiter(sharedCache.Runtime);
        waiter.Wait();

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { location });
        sharedCache.CheckFetches({});
        UNIT_ASSERT(!sharedCache.Results.empty() && sharedCache.Results.front()->Get()->CoreRoute);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.SetLimit(1);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { location }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
    }

    void ReviewWrittenAdmission(bool stickyInput) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(2 * PAGE_TOTAL_SIZE, 0, 2 * PAGE_TOTAL_SIZE);
        if (stickyInput) {
            sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Sticky, {}, true);
        }
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1) } } });
        if (stickyInput) {
            held.erase(1); // Sticky retention protects this page without a reader reference.
        }
        auto& cache = TSharedCache::Get();
        const ui64 handles = cache.PhysicalHandleCount();
        const ui64 overallBefore = cache.OverallUsage();
        TEvSaveCompactedPages saved(sharedCache.Collection2);
        TPageData refused(_P(0), TSharedData::Copy(TString(10, 'w')));
        UNIT_ASSERT(!saved.AddPage(cache, std::move(refused), false));
        UNIT_ASSERT_VALUES_EQUAL(refused.Data.size(), 10);
        const char* pointer = refused.Data.data();
        auto sticky = saved.AddWrittenPage(cache, std::move(refused), true);
        UNIT_ASSERT(sticky && sticky.IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(sticky.data(), pointer);
        auto regular = saved.AddWrittenPage(cache, { _P(1), TSharedData::Copy(TString(10, 'n')) }, false);
        UNIT_ASSERT(regular && !regular.IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(cache.PayloadUsage(), 4 * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(cache.OverallUsage(), overallBefore + 2 * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(cache.ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.ReservedPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.CurrentLimit(), 2 * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(cache.PhysicalHandleCount(), handles);
        UNIT_ASSERT(!cache.AdmitPage(sharedCache.Collection2, _P(2), TSharedData::Copy(TString(10, 'b'))));
        UNIT_ASSERT_VALUES_EQUAL(cache.PayloadUsage(), 4 * PAGE_TOTAL_SIZE);
        regular.Drop();
        sharedCache.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(cache.PayloadUsage(), 3 * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(held.at(0).data(), held.at(0).size()), TStringBuf("xxxxxxxxxx"));
        if (stickyInput) {
            TCacheCollectionRef collection;
            UNIT_ASSERT(cache.Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
            TCachePageRef unreferenced;
            UNIT_ASSERT(cache.Find(collection.CacheItem(), static_cast<ui64>(_P(1).Offset), unreferenced) ==
                        ECacheResultStatus::Hit);
            UNIT_ASSERT(unreferenced.IsSticky());
        }
    }

    Y_UNIT_TEST(ReviewR13WrittenAdmissionPreservesStickyInput) {
        ReviewWrittenAdmission(true);
    }

    Y_UNIT_TEST(ReviewR13WrittenAdmissionPreservesPinnedInput) {
        ReviewWrittenAdmission(false);
    }

    Y_UNIT_TEST(CoreCompactedBufferRemainsAccountedUntilLastReference) {
        TSharedPageCacheMock sharedCache;
        auto saved = MakeHolder<TEvSaveCompactedPages>(sharedCache.Collection1);
        const TPageLocation location = _P(1, EPage::DataPage);
        auto page = saved->AddPage(TSharedCache::Get(), { location, TSharedData::Copy(TString(10, 'x')) }, false);
        UNIT_ASSERT(page && !page.IsSticky());
        TCachePageRef pinned = std::move(page);
        UNIT_ASSERT(!page && pinned);
        sharedCache.Send(sharedCache.Sender1, saved.Release());
        TWaitForFirstEvent<TEvSaveCompactedPages> waiter(sharedCache.Runtime);
        waiter.Wait();
        sharedCache.SetLimit(0);
        sharedCache.Wakeup();
        auto* core = &NSharedCache::TSharedCache::Get();
        UNIT_ASSERT_GT(core->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(pinned.BuildSharedData().Slice(), TStringBuf("xxxxxxxxxx"));
        TCachePageRef last = std::move(pinned);
        UNIT_ASSERT(!pinned);
        sharedCache.Wakeup();
        UNIT_ASSERT_GT(core->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(last.BuildSharedData().Slice(), TStringBuf("xxxxxxxxxx"));
        last.Drop();
        sharedCache.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(core->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(CoreRegularCollectionRetainsSelectedBTreeIndexPage) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::BTreeIndexV2);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true, false, false);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });

        sharedCache.SetLimit(1);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
    }

    Y_UNIT_TEST(CoreSavedCollectionFollowsPageReclamation) {
        for (bool retainPartOwnership : { false, true }) {
            TSharedPageCacheMock sharedCache(TSharedPageCacheMock::DefaultConfig(), true);
            auto ownership = TSharedCache::Get().AdmitCollection(sharedCache.Collection1);
            const TPageLocation location = _P(1, EPage::DataPage);
            auto saved = MakeHolder<TEvSaveCompactedPages>(sharedCache.Collection1);
            auto page = saved->AddPage(TSharedCache::Get(), { location, TSharedData::Copy(TString(10, 'x')) }, true);
            UNIT_ASSERT(page && page.IsSticky());
            if (!retainPartOwnership) {
                ownership.Drop();
            }
            sharedCache.Send(sharedCache.Sender1, saved.Release());
            TWaitForFirstEvent<TEvSaveCompactedPages> waiter(sharedCache.Runtime);
            waiter.Wait();
            auto& core = TSharedCache::Get();
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(page.IsSticky(), retainPartOwnership);

            sharedCache.Runtime.SimulateSleep(TDuration::Minutes(2));
            sharedCache.Wakeup();
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
            ownership.Drop();
            sharedCache.Runtime.WaitFor("saved part ownership released", [&] {
                return !page.IsSticky();
            });
            TCacheCollectionRef collection;
            UNIT_ASSERT(core.Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
            const auto item = collection.CacheItem();
            sharedCache.SetLimit(0).Wakeup();
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(page.data(), page.size()), TStringBuf("xxxxxxxxxx"));
            UNIT_ASSERT(collection->HasPageItems());
            page.Drop();
            sharedCache.Runtime.WaitFor("saved page items drained", [&] {
                return sharedCache.Counters->PageCollections->Val() == 0;
            });
            UNIT_ASSERT(!collection->GetActorState());
            UNIT_ASSERT(!collection->HasPageItems());

            ui32 reclaimed = 0;
            auto observer = sharedCache.Runtime.AddObserver<TEvCollectionReleased>([&](const auto& ev) {
                if (ev->Get()->CacheItem == item) {
                    ++reclaimed;
                }
            });
            UNIT_ASSERT(core.EraseCollection(item));
            UNIT_ASSERT_VALUES_EQUAL(core.Collections(), 1); // The held metadata ref still protects its payload.
            collection.Drop();
            sharedCache.Runtime.WaitFor("saved collection reclaimed", [&] {
                return reclaimed != 0;
            });
            UNIT_ASSERT_VALUES_EQUAL(core.Collections(), 0);
        }
    }

    Y_UNIT_TEST(CoreCompactedPagesRouteUnderHandlePressure) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(1_MB);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 160u);
        sharedCache.Collection2 = MakeIntrusiveConst<TPageCollectionMock>(2ul, 160u);
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2, ECacheMode::Regular, {}, true, false, false);
        TVector<TPageLocation> residentPages;
        TVector<TPageLocation> expectedPages;
        for (ui32 index = 0; index < 150; ++index) {
            residentPages.push_back(_P(index, EPage::DataPage));
            expectedPages.push_back(_P(index));
        }
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, residentPages);
        sharedCache.CheckFetches({ TFetch{ 1500, sharedCache.Collection2, residentPages } });
        sharedCache.Provide(sharedCache.Collection2, residentPages, CORE_FETCH_COOKIE);
        auto heldPages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection2, expectedPages } });
        UNIT_ASSERT_VALUES_EQUAL(heldPages.size(), 150);

        auto saved = MakeHolder<TEvSaveCompactedPages>(sharedCache.Collection1);
        for (ui32 index = 0; index < 160; ++index) {
            const TPageLocation location = _P(index, EPage::DataPage);
            auto page = saved->AddPage(TSharedCache::Get(), { location, TSharedData::Copy(TString(10, 'x')) }, false);
            UNIT_ASSERT(!page || !page.IsSticky());
        }
        sharedCache.Send(sharedCache.Sender1, saved.Release());
        TWaitForFirstEvent<TEvSaveCompactedPages> waiter(sharedCache.Runtime);
        waiter.Wait();

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {});
        sharedCache.CheckFetches({});
        sharedCache.Runtime.WaitFor("core saved collection result", [&] {
            return !sharedCache.Results.empty();
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->CoreRoute);
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, {} } });

        ui32 fetched = 0;
        for (ui32 index = 0; index < 160; ++index) {
            const TPageLocation location = _P(index, EPage::DataPage);
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { location });
            sharedCache.Runtime.WaitFor("core saved-page response", [&] {
                return sharedCache.Fetches->size() || !sharedCache.Results.empty();
            }, TDuration::Seconds(5));
            if (sharedCache.Fetches->size()) {
                sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { location } } });
                sharedCache.Provide(sharedCache.Collection1, { location }, CORE_FETCH_COOKIE);
                ++fetched;
            }
            sharedCache.Runtime.WaitFor("core saved-page result", [&] {
                return !sharedCache.Results.empty();
            }, TDuration::Seconds(5));
            UNIT_ASSERT(sharedCache.Results.front()->Get()->CoreRoute);
            sharedCache.CheckResults({ TFetch{ 3 + index, sharedCache.Collection1, { _P(index) } } });
        }
        UNIT_ASSERT_GT(fetched, 0);
        heldPages.clear();
    }

    Y_UNIT_TEST(CoreRouteWorksBelowMinimumPayloadCapacity) {
        TSharedPageCacheMock sharedCache;
        const TPageLocation page = _P(1, EPage::DataPage);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
    }

    Y_UNIT_TEST(CoreRouteServesOrdinaryCollection) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(1_MB);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 100000u);
        const TPageLocation page = _P(1, EPage::DataPage);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true, false, false);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
        sharedCache.Runtime.WaitFor("core result", [&] {
            return !sharedCache.Results.empty();
        }, TDuration::Seconds(5));
        UNIT_ASSERT(!sharedCache.Results.empty() && sharedCache.Results.front()->Get()->CoreRoute);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
    }

    Y_UNIT_TEST(CoreOversizedDemandFailsBeforeFetch) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(1_MB);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 300u);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true, false, false);

        TVector<TPageLocation> pages;
        for (ui32 index = 0; index < 300; ++index) {
            pages.push_back(_P(index, EPage::DataPage));
        }
        UNIT_ASSERT_EXCEPTION_CONTAINS(sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1,
                                           std::move(pages)), NUtil::TTabletError, "physical capacity");
    }

    Y_UNIT_TEST(CoreStickyPromotesAlreadyLoadedPage) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page });
        auto heldPages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
        heldPages.clear();
        sharedCache.SetLimit(1);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page }, EPriority::Fast,
            ui64(ERequestTypeCookie::StickyPages));
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 3, sharedCache.Collection1, { _P(1) } } });
    }
    Y_UNIT_TEST(CorePageReferenceSurvivesOwnerDetach) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page });
        auto heldPages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        TCachePageRef pinned = std::move(heldPages.at(1));
        UNIT_ASSERT_VALUES_EQUAL(pinned.BuildSharedData().size(), 10);

        heldPages.clear();
        sharedCache.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
    }
    Y_UNIT_TEST(CoreSharedPageRefStickyQueryTracksModeChanges) {
        TSharedPageCacheMock sharedCache;
        const auto page = _P(0);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Sticky);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page });
        auto pages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { page } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        UNIT_ASSERT(pages.at(0).IsSticky());
        TCachePageRef copy;
        TCacheCollectionRef collection;
        UNIT_ASSERT(core->Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
        UNIT_ASSERT(
            core->Find(collection.CacheItem(), static_cast<ui64>(page.Offset), copy) == ECacheResultStatus::Hit);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular);
        UNIT_ASSERT(!pages.at(0).IsSticky());
        UNIT_ASSERT(!copy.IsSticky());
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Sticky);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { page } } });
        UNIT_ASSERT(pages.at(0).IsSticky());
        UNIT_ASSERT(copy.IsSticky());
        copy.Drop();
        pages.clear();
    }

    Y_UNIT_TEST(CoreInMemoryCollectionKeepsPages) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });

        // The protected Keep share still fits this page after reducing the soft limit
        sharedCache.SetLimit(3 * PAGE_TOTAL_SIZE);
        sharedCache.Wakeup();
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
    }

    Y_UNIT_TEST(CoreKeepAttachRetainsReferencedPage) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        config.SetMaxLimitDecreaseStepBytes(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page });
        auto usedPages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });
        UNIT_ASSERT(usedPages.contains(1));

        sharedCache.SetLimit(1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 1);
        sharedCache.SetLimit(128_MB);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, {}, true);
        sharedCache.CheckFetches({});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
    }

    Y_UNIT_TEST(CoreKeepModeChangeUsesKnownHiddenOffset) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        config.SetMaxLimitDecreaseStepBytes(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page });
        auto usedPages = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });
        UNIT_ASSERT(usedPages.contains(1));

        sharedCache.SetLimit(1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 1);
        sharedCache.SetLimit(128_MB);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true);
        sharedCache.CheckFetches({});
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });
    }

    Y_UNIT_TEST(CoreInMemoryCollectionKeepsCoreRouteOnModeChange) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });
        sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(1) } } });

        // A cache mode change is applied to the core record instead of moving the collection back to this cache.
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, false);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1) } } });

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, {}, false);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { page });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 3, sharedCache.Collection1, { _P(1) } } });
    }

    Y_UNIT_TEST(CoreInMemoryEvictionUsesPendingPages) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        config.SetInMemoryInFlyLimit(2 * (10 + NActors::TSharedData::OverheadSize));
        config.SetScanQueueInFlyLimit(1_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation page = _P(1, EPage::DataPage);
        const TPageLocation second = _P(2, EPage::DataPage);
        TBlockEvents<TEvAttached> attached(sharedCache.Runtime);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, {}, true);
        sharedCache.Runtime.WaitFor("core attach result", [&] {
            return !attached.empty();
        });
        const TCollectionCacheItem cacheItem = attached.front()->Get()->CacheItem;

        sharedCache.Send(
            sharedCache.Sender1, new TEvKeepPageEvicted(sharedCache.Collection1->Label(), cacheItem, 0, page));
        sharedCache.CheckFetches({});

        sharedCache.Send(
            sharedCache.Sender1, new TEvKeepPageEvicted(sharedCache.Collection1->Label(), cacheItem, 1, page));
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { page } } });

        sharedCache.Send(
            sharedCache.Sender1, new TEvKeepPageEvicted(sharedCache.Collection1->Label(), cacheItem, 1, page));
        sharedCache.CheckFetches({});
        sharedCache.Send(
            sharedCache.Sender1, new TEvKeepPageEvicted(sharedCache.Collection1->Label(), cacheItem, 1, second));
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { second } } });

        sharedCache.Provide(sharedCache.Collection1, { page }, CORE_FETCH_COOKIE);
        sharedCache.Provide(sharedCache.Collection1, { second }, CORE_FETCH_COOKIE);
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(CoreKeepPreloadRecordsSurviveDetachUntilResult) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetInMemoryInFlyLimit(PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 1u);
        sharedCache.Collection2 = MakeIntrusiveConst<TPageCollectionMock>(2ul, 1u);
        std::deque<TEvResult::TPtr> results;
        auto observer = sharedCache.Runtime.AddObserver<TEvResult>([&](TEvResult::TPtr& event) {
            if (event->GetRecipientRewrite() == sharedCache.ActorId) {
                results.emplace_back(std::move(event));
            }
        });

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        auto* core = &NSharedCache::TSharedCache::Get();
        TCacheCollectionRef collection;
        { UNIT_ASSERT(core->Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit); }
        UNIT_ASSERT_VALUES_EQUAL(collection->GetActorState()->KeepPreloadRequests.size(), 1);
        const auto& preload = collection->GetActorState()->KeepPreloadRequests.begin()->second;
        UNIT_ASSERT_VALUES_EQUAL(preload.ChargedBytes, PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(preload.Locations.size(), 1);

        sharedCache.Provide(sharedCache.Collection1, { _P(0) });
        sharedCache.Runtime.WaitFor("preload result held after request acknowledgement", [&] {
            return results.size() == 1 && collection->GetActorState()->PendingRequests == 0;
        }, TDuration::Seconds(5));
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT(collection->GetActorState());
        UNIT_ASSERT(collection->GetActorState()->Owners.empty());
        UNIT_ASSERT_VALUES_EQUAL(collection->GetActorState()->KeepPreloadRequests.size(), 1);

        // The completed first batch remains charged until its result is consumed.
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});
        observer.Remove();
        while (!results.empty()) {
            sharedCache.Runtime.Send(results.front().Release(), 0, true);
            results.pop_front();
        }
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection2, { _P(0) } } });
        UNIT_ASSERT(!collection->GetActorState());
        sharedCache.Provide(sharedCache.Collection2, { _P(0) });
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(CoreKeepIgnoresEvictionFromRecreatedCollection) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 1u);
        TBlockEvents<TEvAttached> attached(sharedCache.Runtime);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, {}, true);
        sharedCache.Runtime.WaitFor("first core attach", [&] {
            return !attached.empty();
        });
        const TCollectionCacheItem oldItem = attached.front()->Get()->CacheItem;
        attached.clear();
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        auto* core = &NSharedCache::TSharedCache::Get();
        {
            UNIT_ASSERT(core->EraseCollection(oldItem));
        }
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, {}, true);
        sharedCache.Runtime.WaitFor("new core attach", [&] {
            return !attached.empty();
        });
        UNIT_ASSERT(attached.front()->Get()->CacheItem != oldItem);
        sharedCache.Send(
            sharedCache.Sender1, new TEvKeepPageEvicted(sharedCache.Collection1->Label(), oldItem, 1, _P(0)));
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(CoreInMemoryWalkPreloadsPagesThroughCore) {
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        TSharedPageCacheMock sharedCache(config);
        const TPageLocation root = _P(0, EPage::DataPage);
        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = root;
        seed.LevelCount = 0; // the root of a zero-level tree is the data page itself
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, { seed }, true);

        // An in-memory collection preloads its pages through the core, without a request from the tablet.
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { root } } });
        sharedCache.Provide(sharedCache.Collection1, { root }, CORE_FETCH_COOKIE);
        sharedCache.Runtime.WaitFor("preloaded leaf", [&] {
            return sharedCache.Counters->LoadInFlyPages->Val() == 0;
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);
    }

    Y_UNIT_TEST(CoreInMemoryLeafWalkWaitsForPreloadBudget) {
        const ui64 pageBytes = 10 + NActors::TSharedData::OverheadSize;
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        config.SetInMemoryInFlyLimit(pageBytes - 1);
        TSharedPageCacheMock sharedCache(config);

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0, EPage::DataPage);
        seed.LevelCount = 0;
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, { seed }, true);
        sharedCache.CheckFetches({});

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableSharedCacheConfig()->CopyFrom(config);
        appConfig.MutableSharedCacheConfig()->SetInMemoryInFlyLimit(pageBytes);
        sharedCache.UpdateConfig(appConfig);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { seed.Root } } });
        sharedCache.Provide(sharedCache.Collection1, { seed.Root }, CORE_FETCH_COOKIE);
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(CoreInMemoryIndexWalkWaitsForPreloadBudget) {
        const ui64 pageBytes = 10 + NActors::TSharedData::OverheadSize;
        TSharedCacheConfig config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(128_MB);
        config.SetInMemoryInFlyLimit(pageBytes - 1);
        TSharedPageCacheMock sharedCache(config);

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0, EPage::BTreeIndexV2);
        seed.LevelCount = 1;
        seed.QueueDataPages = false;
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, { seed }, true);
        sharedCache.CheckFetches({});

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableSharedCacheConfig()->CopyFrom(config);
        appConfig.MutableSharedCacheConfig()->SetInMemoryInFlyLimit(pageBytes);
        sharedCache.UpdateConfig(appConfig);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { seed.Root } } });
        sharedCache.Fail(sharedCache.Collection1, { seed.Root }, CORE_FETCH_COOKIE);
        sharedCache.CheckResults({ TFetch{ 0, sharedCache.Collection1, {} } }, NKikimrProto::ERROR);
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(RequestCompletionSendsOneResultAfterLastPage) {
        constexpr ui32 PageCount = 8;
        TSharedPageCacheMock sharedCache;
        TVector<TPageLocation> locations;
        locations.reserve(PageCount);
        for (ui32 index = 0; index < PageCount; ++index) {
            locations.push_back(_P(index + 1));
        }

        TVector<TPageLocation> expected;
        for (ui32 index = 0; index < PageCount; ++index) {
            expected.push_back(_P(index + 1));
        }
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, locations);
        sharedCache.CheckFetches({ TFetch{ 10 * PageCount, sharedCache.Collection1, locations } });
        sharedCache.Provide(sharedCache.Collection1, locations);
        auto admitted = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, expected } });
        auto* core = &NSharedCache::TSharedCache::Get();

        TIntrusivePtr<TRequestCompletion> completion = new TRequestCompletion(TRequestCompletionParams{
            .ActorSystem = sharedCache.Runtime.GetActorSystem(0),
            .ReplyTo = sharedCache.Sender1,
            .EventCookie = 17,
            .ExecutorGeneration = 19,
            .RequestId = 23,
            .PageCollection = sharedCache.Collection1,
            .Pages = locations,
            .Cookie = 29,
        });

        TVector<std::thread> threads;
        threads.reserve(PageCount);
        for (ui32 index = 0; index < PageCount; ++index) {
            auto page = std::move(admitted.at(index + 1));
            threads.emplace_back([completion, index, core, actorSystem = sharedCache.Runtime.GetActorSystem(0),
                                     page = std::move(page)]() mutable {
                TCacheTestThreadContext context(*actorSystem, TCacheThreadTestAccess::Hazard(*core, index + 1));
                completion->Complete(index, std::move(page), EPageFetchCompletion::Ready);
            });
        }
        for (std::thread& thread : threads) {
            thread.join();
        }

        sharedCache.Runtime.WaitFor("request completion", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Results.size(), 1);
        const TEvResult::TPtr& event = sharedCache.Results.front();
        UNIT_ASSERT_VALUES_EQUAL(event->Cookie, 17);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->Cookie, 29);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->ExecutorGeneration, 19);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->RequestId, 23);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->Pages.size(), PageCount);
        for (ui32 index = 0; index < PageCount; ++index) {
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Pages[index].Offset, locations[index].Offset);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Pages[index].Size, locations[index].Size);
            UNIT_ASSERT(event->Get()->Pages[index].Page);
        }
    }

    enum class ECompletionAction {
        Fail,
        Cancel,
        CancelImmediately,
        Postpone,
    };

    void CheckCompletionPageRelease(ECompletionAction action) {
        TSharedPageCacheMock sharedCache;
        auto& cache = TSharedCache::Get();
        UNIT_ASSERT(cache.UpdateSoftLimitOverride(0));

        struct TReleaseProbe final : TSharedData::IOwner {
            using THeader = TSharedData::THeader;

            TRequestCompletion* Completion = nullptr;
            ui32 Releases = 0;

            TSharedData Allocate(size_t size) {
                char* raw = new char[sizeof(THeader) + size];
                new (raw) THeader(this);
                return TSharedData::AttachUnsafe(raw + sizeof(THeader), size);
            }

            void Deallocate(char* data) noexcept override {
                if (Completion) {
                    std::atomic<bool> returned = false;
                    std::thread other([&] {
                        Completion->Complete(2, {}, EPageFetchCompletion::Failed);
                        returned.store(true, std::memory_order_release);
                    });
                    const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
                    while (!returned.load(std::memory_order_acquire)) {
                        Y_ABORT_UNLESS(TInstant::Now() < deadline,
                            "Native page destruction must not hold the request-completion mutex");
                        std::this_thread::yield();
                    }
                    other.join();
                }
                ++Releases;
                char* raw = data - sizeof(THeader);
                reinterpret_cast<THeader*>(raw)->~THeader();
                delete[] raw;
            }
        } probe;

        constexpr ui64 cookie = 93;
        auto completion = MakeIntrusive<TRequestCompletion>(TRequestCompletionParams{
            .ActorSystem = sharedCache.Runtime.GetActorSystem(0),
            .ReplyTo = sharedCache.Sender1,
            .PageCollection = sharedCache.Collection1,
            .Pages = { _P(0), _P(1), _P(2) },
            .Cookie = cookie,
        });
        probe.Completion = completion.Get();
        Y_DEFER {
            probe.Completion = nullptr;
            completion->Cancel();
        };
        auto page = cache.AdmitPage(sharedCache.Collection1, _P(0), probe.Allocate(_P(0).Size));
        UNIT_ASSERT(page);
        completion->Complete(0, std::move(page), EPageFetchCompletion::Ready);
        UNIT_ASSERT_VALUES_EQUAL(probe.Releases, 0);

        switch (action) {
            case ECompletionAction::Fail:
                completion->Complete(1, {}, EPageFetchCompletion::Failed);
                break;
            case ECompletionAction::Cancel:
                completion->Cancel();
                break;
            case ECompletionAction::CancelImmediately:
                completion->Cancel(true);
                break;
            case ECompletionAction::Postpone:
                completion->PostponeForResources();
                break;
        }
        UNIT_ASSERT_VALUES_EQUAL(probe.Releases, 1);
        if (action != ECompletionAction::Fail) {
            completion->Complete(1, {}, EPageFetchCompletion::Failed);
        }
        sharedCache.Runtime.WaitFor("completion after releasing native pages", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            sharedCache.Results.front()->Get()->ResourcePressure, action == ECompletionAction::Postpone);
        const auto status = action == ECompletionAction::Fail ? NKikimrProto::ERROR : NKikimrProto::RACE;
        sharedCache.CheckResults({ TFetch{ cookie, sharedCache.Collection1, {} } }, status);
        sharedCache.CheckResults({});
    }

    Y_UNIT_TEST(RequestCompletionFailureReleasesPagesOutsideMutex) {
        CheckCompletionPageRelease(ECompletionAction::Fail);
    }
    Y_UNIT_TEST(RequestCompletionCancelReleasesPagesOutsideMutex) {
        CheckCompletionPageRelease(ECompletionAction::Cancel);
    }
    Y_UNIT_TEST(RequestCompletionImmediateCancelReleasesPagesOutsideMutex) {
        CheckCompletionPageRelease(ECompletionAction::CancelImmediately);
    }
    Y_UNIT_TEST(RequestCompletionPostponeReleasesPagesOutsideMutex) {
        CheckCompletionPageRelease(ECompletionAction::Postpone);
    }

    Y_UNIT_TEST(RequestPageWaiterPropagatesFailure) {
        TSharedPageCacheMock sharedCache;
        TIntrusivePtr<TRequestCompletion> completion = new TRequestCompletion(TRequestCompletionParams{
            .ActorSystem = sharedCache.Runtime.GetActorSystem(0),
            .ReplyTo = sharedCache.Sender1,
            .PageCollection = sharedCache.Collection1,
            .Pages = { _P(1) },
        });
        TIntrusivePtr<TPageFetchWaiter> waiter = new TRequestPageWaiter(completion, 0);
        waiter->Complete({}, EPageFetchCompletion::Failed);

        sharedCache.Runtime.WaitFor("failed request completion", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Results.front()->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->Pages.empty());
    }

    Y_UNIT_TEST(ReviewR19CompletionReportsFailureBeforeRemainingPages) {
        TSharedPageCacheMock sharedCache;
        TIntrusivePtr<TRequestCompletion> completion = new TRequestCompletion(TRequestCompletionParams{
            .ActorSystem = sharedCache.Runtime.GetActorSystem(0),
            .ReplyTo = sharedCache.Sender1,
            .PageCollection = sharedCache.Collection1,
            .Pages = { _P(1), _P(2), _P(3) },
            .Cookie = 91,
        });
        TIntrusivePtr<TPageFetchWaiter> failed = new TRequestPageWaiter(completion, 0);
        TIntrusivePtr<TPageFetchWaiter> firstPending = new TRequestPageWaiter(completion, 1);
        TIntrusivePtr<TPageFetchWaiter> secondPending = new TRequestPageWaiter(completion, 2);
        failed->Complete({}, EPageFetchCompletion::Failed);
        sharedCache.CheckResults({ TFetch{ 91, sharedCache.Collection1, {} } }, NKikimrProto::ERROR);
        UNIT_ASSERT(!firstPending->IsActive());
        UNIT_ASSERT(!secondPending->IsActive());

        std::thread first([&] {
            firstPending->Complete({}, EPageFetchCompletion::Failed);
        });
        std::thread second([&] {
            secondPending->Complete({}, EPageFetchCompletion::Failed);
        });
        first.join();
        second.join();
        sharedCache.CheckResults({});
    }

    void ReviewFailedRequestWithMemoryWait(bool sharedBlockedPage) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(3 * PAGE_TOTAL_SIZE, 0, 3 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection2, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection2, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection2, { _P(0), _P(1) } } });

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(0), _P(1), _P(2) });
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        if (sharedBlockedPage) {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(1) });
            sharedCache.CheckFetches({});
            UNIT_ASSERT(sharedCache.Results.empty());
        }

        sharedCache.Fail(sharedCache.Collection1, { _P(0) });
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, {} }, TFetch{ 3, sharedCache.Collection1, {} } },
            NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(held.size(), 2);
        if (sharedBlockedPage) {
            sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(1) } } });
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);
            sharedCache.Provide(sharedCache.Collection1, { _P(1) });
            sharedCache.CheckResults({ TFetch{ 4, sharedCache.Collection1, { _P(1) } } });
        } else {
            sharedCache.CheckFetches({});
        }
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCache::Get().ReservedBytes(), 0);
        sharedCache.CheckResults({});
    }

    Y_UNIT_TEST(ReviewR19FailedRequestDoesNotWaitForPageMemory) {
        ReviewFailedRequestWithMemoryWait(false);
    }

    Y_UNIT_TEST(ReviewR19FailurePreservesAnotherBlockedPageConsumer) {
        ReviewFailedRequestWithMemoryWait(true);
    }

    Y_UNIT_TEST(Request_Basics) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
    }

    TSharedCacheConfig ReviewHandlePressureConfig() {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(64_KB);
        return config;
    }

    TVector<TCachePageRef> HoldAllReviewPageHandles(TSharedPageCacheMock & sharedCache) {
        sharedCache.Collection1 = new TPageCollectionMock(1, 10000);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true, false, false);
        auto& core = TSharedCache::Get();
        TVector<TCachePageRef> held;
        for (ui32 index = 0; index < core.TargetHandleCount(); ++index) {
            auto page = core.AdmitPage(sharedCache.Collection1, _P(index), TSharedData::Copy(TString(10, 'x')));
            if (!page) {
                break;
            }
            held.push_back(std::move(page));
        }
        UNIT_ASSERT_GT(held.size(), 2);
        UNIT_ASSERT_LT(core.PayloadUsage() + PAGE_TOTAL_SIZE, core.CurrentLimit());
        return held;
    }

    Y_UNIT_TEST(ReviewR20IndependentStickyPressureCanClearWithSubsetAlive) {
        TSharedPageCacheMock sharedCache(ReviewHandlePressureConfig(), true);
        auto& core = TSharedCache::Get();
        auto pseudo = core.AdmitCollection(sharedCache.Collection2);
        sharedCache.Collection1 = new TPageCollectionMock(1, 10000);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {}, true, false, false);
        TCacheCollectionRef main;
        { UNIT_ASSERT(core.Find(sharedCache.Collection1->Label(), main) == ECacheResultStatus::Hit); }
        for (ui32 index = 0; index < core.TargetHandleCount() - 4; ++index) {
            auto page = core.AdmitPage(sharedCache.Collection1, _P(index), TSharedData::Copy(TString(10, 'x')), true);
            UNIT_ASSERT(page && page.IsSticky());
        }
        UNIT_ASSERT_LT(core.PayloadUsage() + PAGE_TOTAL_SIZE, core.CurrentLimit());
        auto pad = MakeIntrusive<TPagesWaitPad>();
        pad->RetainedCollections = { main.CacheItem(), pseudo.CacheItem() };
        pad->WorkingSetPages[main->Id()] = { _P(9000) };
        pad->WorkingSetBytes = PAGE_TOTAL_SIZE;
        UNIT_ASSERT(!pad->HasPinnedPages.load());
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(9000) }, EPriority::Fast, 0, pad);
        sharedCache.Runtime.WaitFor("independent Sticky pressure", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcePressure);
        UNIT_ASSERT(!sharedCache.Results.front()->Get()->WorkingSetTooLarge);
        sharedCache.Results.clear();
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        main->DrainStickyPages();
        sharedCache.Runtime.WaitFor("Sticky pressure clears without releasing retained collections", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(main && pseudo);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcesReady);
        UNIT_ASSERT(!sharedCache.Results.front()->Get()->WorkingSetTooLarge);
        sharedCache.Results.clear();
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(ReviewR20ForeignHandlesRemainRetryableOnFirstPage) {
        TSharedPageCacheMock sharedCache(ReviewHandlePressureConfig(), true);
        auto held = HoldAllReviewPageHandles(sharedCache);
        TCacheCollectionRef main;
        { UNIT_ASSERT(TSharedCache::Get().Find(sharedCache.Collection1->Label(), main) == ECacheResultStatus::Hit); }
        auto pad = MakeIntrusive<TPagesWaitPad>();
        pad->RetainedCollections = { main.CacheItem() };
        pad->WorkingSetPages[main->Id()] = { _P(9000) };
        pad->WorkingSetBytes = PAGE_TOTAL_SIZE;
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(9000) }, EPriority::Fast, 0, pad);
        sharedCache.Runtime.WaitFor("first-page pressure releases the attempt", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcePressure);
        UNIT_ASSERT(!sharedCache.Results.front()->Get()->WorkingSetTooLarge);
        UNIT_ASSERT(!pad->HasPinnedPages.load());
        sharedCache.Results.clear();
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        held.pop_back();
        sharedCache.Runtime.WaitFor("foreign handle release wakes statistics", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcesReady);
        UNIT_ASSERT(!sharedCache.Results.front()->Get()->WorkingSetTooLarge);
        sharedCache.Results.clear();
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(ReviewR15ReadWaitsForHandleWithoutStorageFailure) {
        TSharedPageCacheMock sharedCache(ReviewHandlePressureConfig(), true);
        auto held = HoldAllReviewPageHandles(sharedCache);
        const auto requestedBefore = sharedCache.Counters->RequestedPages->Val();
        const auto coreRequestedBefore = TSharedCache::Get().Stats().RequestedPages;
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(9000) });
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->RequestedPages->Val(), requestedBefore);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCache::Get().Stats().RequestedPages, coreRequestedBefore);

        held.pop_back();
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(9000) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(9000) });
        auto loaded = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(9000) } } });
        UNIT_ASSERT_VALUES_EQUAL(loaded.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->RequestedPages->Val(), requestedBefore + 1);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCache::Get().Stats().RequestedPages, coreRequestedBefore + 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(ReviewR15ReadWaitsBeforeCollectionRegistration) {
        TSharedPageCacheMock sharedCache(ReviewHandlePressureConfig(), true);
        auto held = HoldAllReviewPageHandles(sharedCache);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, { _P(0) });
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        held.pop_back();
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        // The new collection and its page need two handles together.
        held.pop_back();
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection2, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection2, { _P(0) });
        auto loaded = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection2, { _P(0) } } });
        UNIT_ASSERT_VALUES_EQUAL(loaded.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    void ReviewCancelUnregisteredHandleWait(bool unregister) {
        TSharedPageCacheMock sharedCache(ReviewHandlePressureConfig(), true);
        auto held = HoldAllReviewPageHandles(sharedCache);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, { _P(0) });
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        if (unregister) {
            sharedCache.Unregister(sharedCache.Sender2);
        } else {
            sharedCache.Detach(sharedCache.Sender2, sharedCache.Collection2);
        }
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection2, {} } }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        held.pop_back();
        held.pop_back();
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        auto& core = TSharedCache::Get();
        TCacheCollectionRef collection;
        UNIT_ASSERT(core.Find(sharedCache.Collection2->Label(), collection) == ECacheResultStatus::Miss);
    }

    Y_UNIT_TEST(ReviewR15UnregisterCancelsUnregisteredHandleWait) {
        ReviewCancelUnregisteredHandleWait(true);
    }

    Y_UNIT_TEST(ReviewR15DetachCancelsUnregisteredHandleWait) {
        ReviewCancelUnregisteredHandleWait(false);
    }

    Y_UNIT_TEST(ReviewR15HandleReadinessIncludesReleasedTransactionPins) {
        TSharedPageCacheMock sharedCache(ReviewHandlePressureConfig(), true);
        auto held = HoldAllReviewPageHandles(sharedCache);
        const auto pinnedLocation = held.back().GetLocation();
        auto pad = MakeIntrusive<TPagesWaitPad>();
        pad->PendingRequests = 1;
        pad->WorkingSetBytes = 2 * PAGE_TOTAL_SIZE;
        pad->WorkingSetPages[sharedCache.Collection1->Label()] = { pinnedLocation, _P(9000) };
        pad->HasPinnedPages.store(true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(9000) }, EPriority::Fast, 0, pad);
        sharedCache.Runtime.WaitFor("handle pressure releases transaction pins", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcePressure);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pad);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, {} } }, NKikimrProto::RACE);

        held.pop_back(); // Unwind the attempt; foreign pins still leave only one handle.
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        held.pop_back();
        sharedCache.Runtime.WaitFor("two-page handle working set fits", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcesReady);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pad);
        sharedCache.Results.clear();

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { pinnedLocation, _P(9000) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { pinnedLocation, _P(9000) } } });
        sharedCache.Provide(sharedCache.Collection1, { pinnedLocation, _P(9000) });
        auto loaded = sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { pinnedLocation, _P(9000) } } });
        UNIT_ASSERT_VALUES_EQUAL(loaded.size(), 2);
    }

    Y_UNIT_TEST(ReviewR15DetachRetiresGateForAnotherWorkingSetCollection) {
        TSharedPageCacheMock sharedCache(ReviewHandlePressureConfig(), true);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2);
        auto held = HoldAllReviewPageHandles(sharedCache);
        auto pad = MakeIntrusive<TPagesWaitPad>();
        pad->PendingRequests = 2;
        pad->WorkingSetBytes = 3 * PAGE_TOTAL_SIZE;
        pad->WorkingSetPages[sharedCache.Collection1->Label()] = { held.back().GetLocation(), _P(9000) };
        pad->WorkingSetPages[sharedCache.Collection2->Label()] = { _P(0) };
        pad->HasPinnedPages.store(true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(9000) }, EPriority::Fast, 0, pad);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, { _P(0) }, EPriority::Fast, 0, pad);
        sharedCache.CheckResults(
            { TFetch{ 1, sharedCache.Collection1, {} }, TFetch{ 2, sharedCache.Collection2, {} } }, NKikimrProto::RACE);
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection2);
        sharedCache.Runtime.WaitFor("detaching a non-anchor collection retires the resource gate", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcesReady);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pad);
        sharedCache.Results.clear();
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(ReviewR15KeepCancellationDropsDeferredPreload) {
        TSharedPageCacheMock sharedCache(ReviewHandlePressureConfig(), true);
        auto collection = MakeIntrusive<TPageCollectionMock>(2ul, 1u);
        collection->PageTypes = { EPage::Skip };
        sharedCache.Collection2 = collection;
        sharedCache.Attach(sharedCache.Sender2, collection);
        auto held = HoldAllReviewPageHandles(sharedCache);
        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = collection->Label();
        seed.DataCollectionId = collection->Label();
        seed.Root = _P(0, EPage::DataPage);
        seed.LevelCount = 0;
        seed.QueueDataPages = true;
        sharedCache.Attach(sharedCache.Sender2, collection, ECacheMode::TryKeepInMemory, { seed });
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT(sharedCache.Results.empty());

        sharedCache.Attach(sharedCache.Sender2, collection, ECacheMode::Regular);
        sharedCache.Runtime.WaitFor("cancelled Keep request releases its bookkeeping", [&] {
            return sharedCache.Counters->PendingRequests->Val() == 0;
        }, TDuration::Seconds(5));
        held.pop_back();
        auto& cache = TSharedCache::Get();
        UNIT_ASSERT(cache.AvailableHandleCount() != 0 || cache.ReclaimForPreload());
        sharedCache.Send(sharedCache.Sender2, new TKikimrEvents::TEvWakeup(ui64(EWakeupTag::DoGCScheduled)));
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
    }

    Y_UNIT_TEST(ResourceWaitResumesAfterHeldPagesAreReleased) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(2 * PAGE_TOTAL_SIZE, 0, 2 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(2) });
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(2) });
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        held.erase(0);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(2) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(2) });
        sharedCache.CheckResults(
            { TFetch{ 2, sharedCache.Collection1, { _P(2) } }, TFetch{ 3, sharedCache.Collection1, { _P(2) } } });
    }

    void ReviewStatsResourceWait(bool joinWaitingFetch) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(2 * PAGE_TOTAL_SIZE, 0, 2 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1) } } });
        if (joinWaitingFetch) {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(2) });
            sharedCache.CheckFetches({});
        }
        auto pad = MakeIntrusive<TPagesWaitPad>();
        pad->WorkingSetBytes = 2 * PAGE_TOTAL_SIZE;
        pad->HasPinnedPages.store(true);
        const ui64 cookie = sharedCache.RequestId + 1;
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(2) }, EPriority::Fast, 0, pad);
        sharedCache.Runtime.WaitFor("statistics resource-pressure reply", [&] {
            return !sharedCache.Results.empty();
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcePressure);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pad);
        sharedCache.CheckResults({ TFetch{ cookie, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
        // Release the stats attempt's pin; an unrelated owner still occupies half the budget.
        held.erase(0);
        if (joinWaitingFetch) {
            sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(2) } } });
            sharedCache.Provide(sharedCache.Collection1, { _P(2) });
            sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(2) } } });
        }
        sharedCache.Wakeup();
        UNIT_ASSERT(sharedCache.Results.empty());
        held.clear();
        sharedCache.Runtime.WaitFor("statistics working-set readiness", [&] {
            return !sharedCache.Results.empty();
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcesReady);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pad);
        sharedCache.Results.clear();
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(ReviewR11NewFetchWaitsForResources) {
        ReviewStatsResourceWait(false);
    }

    Y_UNIT_TEST(ReviewR11JoinedFetchWaitsForResources) {
        ReviewStatsResourceWait(true);
    }

    Y_UNIT_TEST(ResourceWaitKeepsFetchAfterOriginalRequesterUnregisters) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(2 * PAGE_TOTAL_SIZE, 0, 2 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1) } } });

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(2) });
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(2) });
        sharedCache.CheckFetches({});
        sharedCache.Unregister(sharedCache.Sender1);
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());

        held.clear();
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(2) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(2) });
        sharedCache.CheckResults({ TFetch{ 3, sharedCache.Collection1, { _P(2) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 2);
    }

    Y_UNIT_TEST(ResourceWaitDropsFetchAfterLastRequesterUnregisters) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(2 * PAGE_TOTAL_SIZE, 0, 2 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1) } } });

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(2) });
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(2) });
        sharedCache.CheckFetches({});
        sharedCache.Unregister(sharedCache.Sender1);
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
        sharedCache.Unregister(sharedCache.Sender2);
        sharedCache.CheckResults({ TFetch{ 3, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
        sharedCache.CheckFetches({});

        auto* core = &NSharedCache::TSharedCache::Get();
        {
            TCacheCollectionRef collection;
            UNIT_ASSERT(core->Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
            TCachePageRef page;
            UNIT_ASSERT(
                core->Find(collection.CacheItem(), static_cast<ui64>(_P(2).Offset), page) == ECacheResultStatus::Miss);
        }
        held.clear();
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(ResourceWaitWithNoFreeHandlesDoesNotWakeItself) {
        TSharedPageCacheMock sharedCache(TSharedPageCacheMock::DefaultConfig(), true);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        auto& cache = TSharedCache::Get();
        TCacheCollectionRef collection;
        UNIT_ASSERT(cache.Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
        const TPageLocation location = _P(0);
        const TCacheHandleRequirement requirement{ sharedCache.Collection1->Label(), { &location, 1 } };
        TVector<TCacheCollectionRef> foreignCollections;
        for (ui64 id = 100; cache.CanAllocateHandles({ &requirement, 1 }); ++id) {
            UNIT_ASSERT(foreignCollections.size() < cache.TargetHandleCount());
            foreignCollections.push_back(cache.AdmitCollection(MakeIntrusiveConst<TPageCollectionMock>(id, 1u)));
        }
        for (const auto& foreign : foreignCollections) {
            UNIT_ASSERT(cache.DetachCollection(foreign.CacheItem()));
        }
        UNIT_ASSERT(cache.CanReserve(PAGE_TOTAL_SIZE));
        ui32 notifications = 0;
        ui32 wakeups = 0;
        auto wakeupObserver = sharedCache.Runtime.AddObserver<TKikimrEvents::TEvWakeup>([&](auto& event) {
            if (event->GetRecipientRewrite() == sharedCache.ActorId) {
                ++wakeups;
            }
        });
        TVector<TEvResourcesAvailable::TPtr> blocked;
        auto observer = sharedCache.Runtime.AddObserver<TEvResourcesAvailable>([&](auto& event) {
            // Stop an accidental notification loop so the assertions can report it
            if (++notifications > 16) {
                blocked.push_back(std::move(event));
            }
        });
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { location });
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT(blocked.empty());
        UNIT_ASSERT_VALUES_EQUAL(wakeups, 0);
        // The request's equal-limit update may notify once; the parked waiter must then stay quiet
        const ui32 settledNotifications = notifications;
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(notifications, settledNotifications);
        UNIT_ASSERT_VALUES_EQUAL(wakeups, 0);
        UNIT_ASSERT(sharedCache.Fetches->empty());
        UNIT_ASSERT(sharedCache.Results.empty());
        const ui64 generation = cache.AdmissionGeneration();
        sharedCache.Send(sharedCache.Sender1, new TEvResourcesAvailable);
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(notifications, settledNotifications + 1);
        UNIT_ASSERT_VALUES_EQUAL(wakeups, 0);
        UNIT_ASSERT(blocked.empty());
        UNIT_ASSERT_VALUES_EQUAL(cache.AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(cache.ReservedBytes(), 0);
        UNIT_ASSERT(sharedCache.Fetches->empty());
        observer.Remove();
        wakeupObserver.Remove();
        foreignCollections.clear();
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { location } } });
        sharedCache.Provide(sharedCache.Collection1, { location });
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { location } } });
    }

    Y_UNIT_TEST(PendingResourceEventSurvivesOrdinaryRequest) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        auto& cache = TSharedCache::Get();
        TCacheCollectionRef collection;
        UNIT_ASSERT(cache.Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
        const TPageLocation location = _P(0);
        const TCacheHandleRequirement requirement{ sharedCache.Collection1->Label(), { &location, 1 } };
        TVector<TCacheCollectionRef> foreignCollections;
        for (ui64 id = 100; cache.CanAllocateHandles({ &requirement, 1 }); ++id) {
            UNIT_ASSERT(foreignCollections.size() < cache.TargetHandleCount());
            foreignCollections.push_back(cache.AdmitCollection(MakeIntrusiveConst<TPageCollectionMock>(id, 1u)));
        }
        for (const auto& foreign : foreignCollections) {
            UNIT_ASSERT(cache.DetachCollection(foreign.CacheItem()));
        }
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { location });
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(sharedCache.Fetches->empty());
        TVector<TEvResourcesAvailable::TPtr> resourceEvents;
        auto observer = sharedCache.Runtime.AddObserver<TEvResourcesAvailable>([&](auto& event) {
            resourceEvents.push_back(std::move(event));
        });
        UNIT_ASSERT(!foreignCollections.empty());
        foreignCollections.pop_back();
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(!resourceEvents.empty());
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { location });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { location } } });
        observer.Remove();
        for (auto& event : resourceEvents) {
            sharedCache.Runtime.Send(event.Release(), 0, /* viaActorSystem */ true);
        }
        resourceEvents.clear();
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        sharedCache.CheckFetches({});
        sharedCache.Provide(sharedCache.Collection1, { location });
        sharedCache.CheckResults(
            { TFetch{ 1, sharedCache.Collection1, { location } }, TFetch{ 2, sharedCache.Collection1, { location } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(ResourceWaitSharedFetchPostponesPinnedTransactionOnce) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(3 * PAGE_TOTAL_SIZE, 0, 3 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1), _P(4) });
        sharedCache.CheckFetches({ TFetch{ 30, sharedCache.Collection1, { _P(0), _P(1), _P(4) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1), _P(4) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1), _P(4) } } });

        // The fetch owner has no transaction pad; another transaction joins both blocked fetches.
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(2), _P(3) });
        {
            auto* core = &NSharedCache::TSharedCache::Get();
            TCacheCollectionRef collection;
            UNIT_ASSERT(core->Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
            TCachePageRequest producer(_P(2), MakeIntrusive<TPageFetchWaiter>());
            UNIT_ASSERT(core->FindOrInsertBatch(collection.CacheItem(), { &producer, 1 }));
            UNIT_ASSERT(producer.Status() == ECacheResultStatus::Pending);
        }
        TIntrusivePtr<TPagesWaitPad> pinnedPad = new TPagesWaitPad;
        pinnedPad->PendingRequests = 1;
        pinnedPad->WorkingSetBytes = 3 * PAGE_TOTAL_SIZE;
        pinnedPad->HasPinnedPages.store(true);
        sharedCache.Request(
            sharedCache.Sender2, sharedCache.Collection1, { _P(2), _P(3) }, EPriority::Fast, 0, pinnedPad);
        sharedCache.Runtime.WaitFor("shared-fetch transaction pressure", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcePressure);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pinnedPad);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Results.front()->Get()->Cookie, 3);
        UNIT_ASSERT(pinnedPad->PostponedForResources);
        sharedCache.Results.clear();

        TIntrusivePtr<TPagesWaitPad> unpinnedPad = new TPagesWaitPad;
        unpinnedPad->PendingRequests = 1;
        unpinnedPad->WorkingSetBytes = 2 * PAGE_TOTAL_SIZE;
        sharedCache.Request(
            sharedCache.Sender1, sharedCache.Collection1, { _P(2), _P(3) }, EPriority::Fast, 0, unpinnedPad);
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        UNIT_ASSERT(!unpinnedPad->PostponedForResources);

        held.clear();
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(2), _P(3) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(2), _P(3) });
        // Working-set readiness can arrive before either fetch completion
        sharedCache.Runtime.WaitFor("shared-fetch replies and working-set readiness", [&] {
            return sharedCache.Results.size() >= 3;
        }, TDuration::Seconds(5));
        ui32 readinessReplies = 0;
        for (auto reply = sharedCache.Results.begin(); reply != sharedCache.Results.end();) {
            const auto& result = *(*reply)->Get();
            if (!result.ResourcesReady) {
                ++reply;
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(result.Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result.Cookie, 3);
            UNIT_ASSERT(result.WaitPad == pinnedPad);
            UNIT_ASSERT(result.Pages.empty());
            ++readinessReplies;
            reply = sharedCache.Results.erase(reply);
        }
        UNIT_ASSERT_VALUES_EQUAL(readinessReplies, 1);
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(2), _P(3) } },
            TFetch{ 4, sharedCache.Collection1, { _P(2), _P(3) } } });
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
    }

    Y_UNIT_TEST(ResourceWaitPartialBatchPublishesPinnedPages) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(2 * PAGE_TOTAL_SIZE, 0, 2 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1) } } });

        TIntrusivePtr<TPagesWaitPad> pad = new TPagesWaitPad;
        pad->PendingRequests = 1;
        pad->WorkingSetBytes = 2 * PAGE_TOTAL_SIZE;
        UNIT_ASSERT(!pad->HasPinnedPages.load());
        // The cached page is retained by the completion while the other page waits for memory.
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(2) }, EPriority::Fast, 0, pad);
        sharedCache.Runtime.WaitFor("partial batch releases its ready page under pressure", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(pad->HasPinnedPages.load());
        UNIT_ASSERT(pad->PostponedForResources);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcePressure);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pad);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->Pages.empty());
        sharedCache.Results.clear();
        sharedCache.CheckFetches({});

        held.clear();
        sharedCache.Runtime.WaitFor("partial-batch working-set room", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcesReady);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pad);
        sharedCache.Results.clear();
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(ReviewR14StickyPartialBatchesResumeAfterDemotion) {
        TSharedPageCacheMock sharedCache(TSharedPageCacheMock::DefaultConfig(), true);
        sharedCache.SetLimit(0, 0, 2 * PAGE_TOTAL_SIZE);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Sticky, {}, true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1) } } });
        UNIT_ASSERT(held.at(0).IsSticky());
        UNIT_ASSERT(held.at(1).IsSticky());
        held.clear();

        auto firstPad = MakeIntrusive<TPagesWaitPad>();
        auto secondPad = MakeIntrusive<TPagesWaitPad>();
        for (const auto& pad : { firstPad, secondPad }) {
            pad->PendingRequests = 1;
            pad->WorkingSetBytes = 2 * PAGE_TOTAL_SIZE;
            UNIT_ASSERT(!pad->HasPinnedPages.load());
        }
        // Each attempt fits alone; together their partial Sticky hits block both missing pages.
        sharedCache.Request(
            sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(2) }, EPriority::Fast, 0, firstPad);
        sharedCache.Request(
            sharedCache.Sender2, sharedCache.Collection1, { _P(1), _P(3) }, EPriority::Fast, 0, secondPad);
        sharedCache.Runtime.WaitFor("Sticky partial batches release their pins", [&] {
            return sharedCache.Results.size() == 2;
        }, TDuration::Seconds(5));
        for (const auto& result : sharedCache.Results) {
            const auto& pad = result->Get()->WaitPad;
            UNIT_ASSERT(pad == firstPad || pad == secondPad);
            UNIT_ASSERT(pad->HasPinnedPages.load());
            UNIT_ASSERT(pad->PostponedForResources);
            UNIT_ASSERT(result->Get()->ResourcePressure);
            UNIT_ASSERT(!result->Get()->ResourcesReady);
        }
        sharedCache.CheckResults(
            { TFetch{ 2, sharedCache.Collection1, {} }, TFetch{ 3, sharedCache.Collection1, {} } }, NKikimrProto::RACE);
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCache::Get().StickyBytes(), 2 * PAGE_TOTAL_SIZE);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular);
        sharedCache.Runtime.WaitFor("both attempts resume after Sticky demotion", [&] {
            return sharedCache.Results.size() == 2;
        }, TDuration::Seconds(5));
        for (const auto& pad : { firstPad, secondPad }) {
            ui32 replies = 0;
            for (const auto& result : sharedCache.Results) {
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT(result->Get()->ResourcesReady);
                UNIT_ASSERT(!result->Get()->ResourcePressure);
                UNIT_ASSERT(result->Get()->Pages.empty());
                replies += result->Get()->WaitPad == pad;
            }
            UNIT_ASSERT_VALUES_EQUAL(replies, 1);
        }
        sharedCache.Results.clear();

        // Readiness is advisory: finish one retry and release its pins before running the other.
        for (ui32 index = 0; index < 2; ++index) {
            const TVector<TPageLocation> pages{ _P(index), _P(index + 2) };
            sharedCache.Request(index ? sharedCache.Sender2 : sharedCache.Sender1, sharedCache.Collection1, pages);
            sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, pages } });
            sharedCache.Provide(sharedCache.Collection1, pages);
            auto loaded = sharedCache.CheckResults({ TFetch{ 4 + index, sharedCache.Collection1, pages } });
            UNIT_ASSERT_VALUES_EQUAL(loaded.size(), 2);
        }
    }

    Y_UNIT_TEST(ResourceWaitReportsTransactionPinsWithoutStorageFailure) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(2 * PAGE_TOTAL_SIZE, 0, 2 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(0), _P(1) });
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, { _P(0), _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0), _P(1) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0), _P(1) } } });
        TIntrusivePtr<TPagesWaitPad> pad = new TPagesWaitPad;
        pad->PendingRequests = 2;
        pad->WorkingSetBytes = 2 * PAGE_TOTAL_SIZE;
        pad->HasPinnedPages.store(true);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(2) }, EPriority::Fast, 0, pad);
        sharedCache.Runtime.WaitFor("transaction resource response", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        const auto* result = sharedCache.Results.front()->Get();
        UNIT_ASSERT(result->ResourcePressure);
        UNIT_ASSERT(result->WaitPad == pad);
        UNIT_ASSERT(result->Pages.empty());
        UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::RACE);
        sharedCache.Results.clear();
        // A request sent by the executor before it saw the pressure reply can arrive late.
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(2) }, EPriority::Fast, 0, pad);
        sharedCache.Runtime.WaitFor("late request belongs to postponed attempt", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcePressure);
        sharedCache.Results.clear();
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        held.erase(0);
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.Results.empty());
        held.clear();
        sharedCache.Runtime.WaitFor("transaction working set room", [&] {
            return sharedCache.Results.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(sharedCache.Results.front()->Get()->ResourcesReady);
        UNIT_ASSERT(sharedCache.Results.front()->Get()->WaitPad == pad);
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Results.size(), 1);
        sharedCache.Results.clear();
    }

    Y_UNIT_TEST(Request_Failed) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2) });
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(1), _P(2)});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {_P(3)});
        sharedCache.CheckFetches({
            TFetch{ 20, sharedCache.Collection1, {_P(1), _P(2) }},
            TFetch{ 10, sharedCache.Collection2, {_P(3) } } });
        sharedCache.Fail(sharedCache.Collection1, {_P(1), _P(2) });
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}},
            TFetch{ 2, sharedCache.Collection1, {}}
        }, NKikimrProto::ERROR);
        sharedCache.Provide(sharedCache.Collection2, { _P(3) });
        sharedCache.CheckResults({ TFetch{ 3, sharedCache.Collection2, { _P(3) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
    }

    Y_UNIT_TEST(Request_Queue) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4), _P(5)}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {_P(1), _P(2), _P(3)}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {_P(1), _P(2)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 8);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2)}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {_P(3), _P(4)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(3), _P(4)}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            // TODO: shouldn't we finish with Collection1 page 5?
            TFetch{20, sharedCache.Collection2, {_P(1), _P(2)}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection2, {_P(1), _P(2)}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {_P(3)}},
            TFetch{10, sharedCache.Collection1, {_P(5)}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection2, {_P(3)});
        sharedCache.Provide(sharedCache.Collection1, {_P(5)});
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {_P(1), _P(2), _P(3)}},
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4), _P(5)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 2);
    }

    Y_UNIT_TEST(Request_Queue_Failed) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {_P(4)}, EPriority::Bkgr);
        sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, {_P(1), _P(2) } } });
        sharedCache.Fail(sharedCache.Collection1, {_P(1), _P(2) });
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, {} } }, NKikimrProto::ERROR);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, { _P(4) } } });
        sharedCache.Provide(sharedCache.Collection2, {_P(4)});
        sharedCache.CheckResults({
            TFetch{ 2, sharedCache.Collection2, {_P(4)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
    }

    Y_UNIT_TEST(Request_Queue_Fast) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4), _P(5)}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {_P(1), _P(2)}}
        });
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4), _P(6)}, EPriority::Fast);
        sharedCache.CheckFetches({
            TFetch{ 30, sharedCache.Collection1, { _P(3), _P(4), _P(6)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 5);
        sharedCache.Provide(sharedCache.Collection1, { _P(3), _P(4), _P(6) });
        sharedCache.CheckResults({});

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(5) } } });
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4), _P(6)}} });

        sharedCache.Provide(sharedCache.Collection1, {_P(5)});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4), _P(5)}} });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(Request_Sequential) {
        TSharedPageCacheMock sharedCache;
        
        {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
            sharedCache.CheckFetches({
                TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 3);
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

            sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
            sharedCache.CheckResults({
                TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        }

        {
            sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {_P(1), _P(2)});
            sharedCache.CheckFetches({
                TFetch{20, sharedCache.Collection2, {_P(1), _P(2)}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

            sharedCache.Provide(sharedCache.Collection2, {_P(1), _P(2)});
            sharedCache.CheckResults({
                TFetch{2, sharedCache.Collection2, {_P(1), _P(2)}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        }
    }

    Y_UNIT_TEST(Request_Cached) {
        TSharedPageCacheMock sharedCache;
        TVector<TPageLocation> pages;
        for (TPageId pageId : xrange(1, 8)) {
            pages.push_back(_P(pageId));
        }
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, pages);
        sharedCache.CheckFetches({
            TFetch{ 70, sharedCache.Collection1, pages }
            });

            sharedCache.Provide(sharedCache.Collection1, pages);
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, pages } });
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, pages);
        sharedCache.CheckFetches({});
            sharedCache.CheckResults({
                TFetch{ 2, sharedCache.Collection1, pages }
            });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 7);
    }

    Y_UNIT_TEST(Request_Different_Collections) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {_P(1), _P(2)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{20, sharedCache.Collection2, {_P(1), _P(2)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Provide(sharedCache.Collection2, {_P(1), _P(2)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection2, {_P(1), _P(2)}}
        });
    }

    Y_UNIT_TEST(Request_Different_Pages) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(4), _P(5)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{20, sharedCache.Collection1, {_P(4), _P(5)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Provide(sharedCache.Collection1, {_P(4), _P(5)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection1, {_P(4), _P(5)}}
        });
    }

    Y_UNIT_TEST(Request_Different_Pages_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(4), _P(5)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{20, sharedCache.Collection1, {_P(4), _P(5)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(4), _P(5)});
        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});

        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection1, {_P(4), _P(5)}},
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });
    }

    Y_UNIT_TEST(Request_Subset) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1), _P(2)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection1, {_P(1), _P(2)}}
        });
    }

    Y_UNIT_TEST(Request_Subset_Shuffled) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(3), _P(1)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection1, {_P(3), _P(1)}}
        });
    }

    Y_UNIT_TEST(Request_Superset) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{10, sharedCache.Collection1, {_P(4)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Provide(sharedCache.Collection1, {_P(4)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4)}}
        });
    }

    Y_UNIT_TEST(Request_Superset_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{10, sharedCache.Collection1, {_P(4)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(4)});
        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4)}}
        });
    }

    Y_UNIT_TEST(Request_Crossing) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(3), _P(4)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{10, sharedCache.Collection1, {_P(4)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Provide(sharedCache.Collection1, {_P(4)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection1, {_P(3), _P(4)}}
        });
    }

    Y_UNIT_TEST(Request_Crossing_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(3), _P(4)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{10, sharedCache.Collection1, {_P(4)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(4)});
        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection1, {_P(3), _P(4)}}
        });
    }

    Y_UNIT_TEST(Request_Crossing_Shuffled) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(4), _P(3)});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{10, sharedCache.Collection1, {_P(4)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.Provide(sharedCache.Collection1, {_P(4)});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}},
            TFetch{2, sharedCache.Collection1, {_P(4), _P(3)}}
        });
    }

    Y_UNIT_TEST(Attach_Basics) {
        TSharedPageCacheMock sharedCache;
        
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        // call again
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4);
    }

    Y_UNIT_TEST(Attach_Request) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {_P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);
    }

    Y_UNIT_TEST(Detach_Basics) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2);
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        // call again
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Detach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
    }

    Y_UNIT_TEST(Detach_Cached) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
    }

    Y_UNIT_TEST(Detach_InFly) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1)});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {_P(2)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1), _P(3)});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(1)}},
            TFetch{10, sharedCache.Collection2, {_P(2)}},
            TFetch{10, sharedCache.Collection1, {_P(3)}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}}
        }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {_P(1)});
        sharedCache.CheckResults({
            TFetch{3, sharedCache.Collection1, {_P(1)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(3)});
        sharedCache.CheckResults({
            TFetch{4, sharedCache.Collection1, {_P(1), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection2, {_P(2)});
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {_P(2)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);
    }

    Y_UNIT_TEST(Detach_Queued) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4) }, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1), _P(3)}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {_P(1), _P(2)}}
        });
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}} }, NKikimrProto::RACE);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2)});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(3) } } });
        sharedCache.Provide(sharedCache.Collection1, {_P(3) });
        sharedCache.CheckResults({
            TFetch{ 2, sharedCache.Collection1, {_P(1), _P(3) } } });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
    }

    Y_UNIT_TEST(Unregister_Basics) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2);
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Unregister(sharedCache.Sender1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        // call again
        sharedCache.Unregister(sharedCache.Sender1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Unregister(sharedCache.Sender2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
    }

    Y_UNIT_TEST(Unregister_Cached) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Unregister(sharedCache.Sender1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
    }

    Y_UNIT_TEST(Unregister_InFly) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1)});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {_P(2)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1)});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1), _P(3)});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(1)}},
            TFetch{10, sharedCache.Collection2, {_P(2)}},
            TFetch{10, sharedCache.Collection1, {_P(3)}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Unregister(sharedCache.Sender1);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}},
            TFetch{2, sharedCache.Collection2, {}}
        }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {_P(1)});
        sharedCache.Provide(sharedCache.Collection2, {_P(2)});
        sharedCache.CheckResults({
            TFetch{3, sharedCache.Collection1, {_P(1)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(3)});
        sharedCache.CheckResults({
            TFetch{4, sharedCache.Collection1, {_P(1), _P(3)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);
    }

    Y_UNIT_TEST(Unregister_Queued) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3), _P(4) }, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {_P(1), _P(3)}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {_P(1), _P(2)}}
        });
        sharedCache.Unregister(sharedCache.Sender1);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}} }, NKikimrProto::RACE);

        sharedCache.Provide(sharedCache.Collection1, {_P(1), _P(2)});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(3) } } });
        sharedCache.Provide(sharedCache.Collection1, {_P(3) });
        sharedCache.CheckResults({
            TFetch{ 2, sharedCache.Collection1, {_P(1), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
    }

    Y_UNIT_TEST(Unregister_Queued_Pending) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1)}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {_P(10), _P(11)}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {_P(12)}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(1)}},
            TFetch{10, sharedCache.Collection2, {_P(10)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Unregister(sharedCache.Sender2);
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {}},
            TFetch{3, sharedCache.Collection2, {}}
        }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {_P(1)}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1)}},
        });
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Provide(sharedCache.Collection2, {_P(10)}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);
    }

    Y_UNIT_TEST(InMemory_Basics) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * PAGE_TOTAL_SIZE;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Provide(sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(1), _P(2), _P(3)});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_RegularReadsShareCoreCapacity) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        TVector<TPageLocation> pages{ _P(0), _P(1), _P(2), _P(3) };
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({ TFetch{ 40, sharedCache.Collection1, pages } });
        sharedCache.Provide(sharedCache.Collection1, pages);
        sharedCache.CheckResults({});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {_P(1)});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {_P(1)}} });
        sharedCache.Provide(sharedCache.Collection2, {_P(1)});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection2, {_P(1)}}
        });
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, pages);
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, pages } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), 4 * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 4 * PAGE_TOTAL_SIZE);
    }

    Y_UNIT_TEST(InMemory_NotEnoughMemory) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        TSharedPageCacheMock sharedCache(config);
        sharedCache.SetLimit(4 * PAGE_TOTAL_SIZE);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 6u);
        ui64 collection1TotalSize = 6 * PAGE_TOTAL_SIZE;
        ui64 collection1FitInMemory = PAGE_TOTAL_SIZE;

        // The hard Keep share admits one of the six pages
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(0) }, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1FitInMemory);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), DefaultMemoryLimit);

        // Pin the resident prefix so demand allocation cannot turn it into another miss
        TCacheCollectionRef collection;
        auto& cache = TSharedCache::Get();
        UNIT_ASSERT(cache.Find(sharedCache.Collection1->Label(), collection) == ECacheResultStatus::Hit);
        TCachePageRef pinned;
        UNIT_ASSERT(
            cache.Find(collection.CacheItem(), static_cast<ui64>(_P(0).Offset), pinned) == ECacheResultStatus::Hit);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3), _P(4), _P(5)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 5);
    }

    Y_UNIT_TEST(InMemory_StaleBtreeSeed) {
        // A seed that references a collection which is not attached (its part is already gone)
        // must not wedge the walk and must not try to fetch anything.
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 2u);
        auto missing = MakeIntrusiveConst<TPageCollectionMock>(2ul, 2u);

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = missing->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0);
        seed.LevelCount = 1;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {seed});
        sharedCache.CheckFetches({});

        // The collection itself stays usable.
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0)});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(0)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(0)});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(0)}}
        });
    }

    Y_UNIT_TEST(InMemory_StickyWalksAreOwnerSpecific) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 1u);
        TEvAttach::TBtreeSeed regularSeed;
        regularSeed.IndexCollectionId = sharedCache.Collection1->Label();
        regularSeed.DataCollectionId = sharedCache.Collection1->Label();
        regularSeed.Root = _P(0, EPage::DataPage);
        regularSeed.QueueDataPages = false;

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1, ECacheMode::Regular, { regularSeed });

        auto stickySeed = regularSeed;
        stickySeed.QueueDataPages = true;
        stickySeed.Sticky = true;
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { stickySeed });

        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { stickySeed.Root } } });
        sharedCache.Provide(sharedCache.Collection1, { stickySeed.Root });
        sharedCache.Runtime.WaitFor("sticky preload result", [&] {
            return !sharedCache.StickyResults.empty();
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.StickyResults.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.StickyResults.front()->GetRecipientRewrite(), sharedCache.Sender1);
        UNIT_ASSERT(sharedCache.StickyResults.front()->Get()->Pages.front().Page.IsSticky());
        sharedCache.StickyResults.clear();
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1, ECacheMode::Regular, { regularSeed });
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { stickySeed });
        sharedCache.Runtime.SimulateSleep(TDuration::Seconds(1));
        sharedCache.CheckFetches({});
        UNIT_ASSERT(sharedCache.StickyResults.empty());
    }

    Y_UNIT_TEST(StickyWalkReplaysIdenticalSeedsOnReattach) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 1u);

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0, EPage::DataPage);
        seed.Sticky = true;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { seed });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { seed.Root } } });
        sharedCache.Provide(sharedCache.Collection1, { seed.Root });
        sharedCache.Runtime.WaitFor("initial sticky preload", [&] {
            return !sharedCache.StickyResults.empty();
        });
        sharedCache.StickyResults.clear();
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { seed });
        sharedCache.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(sharedCache.StickyResults.empty());

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { seed }, false, true);
        sharedCache.Runtime.WaitFor("replayed sticky preload", [&] {
            return !sharedCache.StickyResults.empty();
        });
        sharedCache.CheckFetches({}); // Replaying a ready Sticky page must not duplicate I/O.
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.StickyResults.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.StickyResults.front()->GetRecipientRewrite(), sharedCache.Sender1);
        UNIT_ASSERT_VALUES_EQUAL(
            sharedCache.StickyResults.front()->Get()->PageCollection->Label(), seed.DataCollectionId);
    }

    Y_UNIT_TEST(InMemory_IndexOnlyDropKeepsOtherOwnerWalk) {
        TSharedPageCacheMock sharedCache;
        auto indexCollection = MakeIntrusive<TPageCollectionMock>(1ul, 1u);
        indexCollection->PageTypes = { EPage::Skip };
        sharedCache.Collection1 = indexCollection;
        sharedCache.Collection2 = MakeIntrusiveConst<TPageCollectionMock>(2ul, 2u);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});

        TEvAttach::TBtreeSeed indexOnlySeed;
        indexOnlySeed.IndexCollectionId = sharedCache.Collection1->Label();
        indexOnlySeed.DataCollectionId = sharedCache.Collection2->Label();
        indexOnlySeed.Root = _P(0, EPage::DataPage);
        indexOnlySeed.LevelCount = 0;
        indexOnlySeed.QueueDataPages = false;
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2, ECacheMode::Regular, { indexOnlySeed });

        auto stickySeed = indexOnlySeed;
        stickySeed.Root = _P(1, EPage::DataPage);
        stickySeed.QueueDataPages = true;
        stickySeed.Sticky = true;
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2, ECacheMode::Regular, { stickySeed });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection2, { stickySeed.Root } } });
        sharedCache.Provide(sharedCache.Collection2, { stickySeed.Root });
        sharedCache.Runtime.WaitFor("first sticky preload", [&] {
            return !sharedCache.StickyResults.empty();
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.StickyResults.front()->GetRecipientRewrite(), sharedCache.Sender2);
        sharedCache.StickyResults.clear();

        // The main collection leaving the in-memory tier invalidates the mixed run. The following
        // authoritative group attach withdraws Sender1's obsolete index-only seed and must re-arm
        // Sender2's still-valid sticky walk.
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2, ECacheMode::Regular);
        sharedCache.Runtime.WaitFor("re-armed sticky preload", [&] {
            return !sharedCache.StickyResults.empty();
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.StickyResults.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.StickyResults.front()->GetRecipientRewrite(), sharedCache.Sender2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.StickyResults.front()->Get()->Pages.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(
            sharedCache.StickyResults.front()->Get()->Pages.front().Offset, stickySeed.Root.Offset);
    }

    Y_UNIT_TEST(InMemory_ChangedWalkDrainsParallelFetches) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 6u);

        auto makeSeed = [&](ui32 root) {
            TEvAttach::TBtreeSeed seed;
            seed.IndexCollectionId = sharedCache.Collection1->Label();
            seed.DataCollectionId = sharedCache.Collection1->Label();
            seed.Root = _P(root, EPage::BTreeIndexV2);
            seed.LevelCount = 1;
            seed.QueueDataPages = false;
            return seed;
        };

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular,
            { makeSeed(0), makeSeed(1), makeSeed(2), makeSeed(3), makeSeed(4) });
        sharedCache.CheckFetches({
            TFetch{ 10, sharedCache.Collection1, { _P(0, EPage::BTreeIndexV2) } },
            TFetch{ 10, sharedCache.Collection1, { _P(1, EPage::BTreeIndexV2) } },
        });

        // Root 2 is also needed by a queued reader; root 4 belongs only to the cancelled walk.
        sharedCache.Request(
            sharedCache.Sender2, sharedCache.Collection1, { _P(2, EPage::BTreeIndexV2) }, EPriority::Bkgr);
        sharedCache.CheckFetches({});

        // A direct reader takes ownership of Pending root 3 immediately and bypasses the async limit.
        sharedCache.Request(
            sharedCache.Sender2, sharedCache.Collection1, { _P(3, EPage::BTreeIndexV2) }, EPriority::Fast);
        sharedCache.CheckFetches({
            TFetch{ 10, sharedCache.Collection1, { _P(3, EPage::BTreeIndexV2) } },
        });

        // The replacement is held until both physical fetches of the cancelled generation drain.
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { makeSeed(5) });
        sharedCache.CheckFetches({});

        sharedCache.Provide(sharedCache.Collection1, { _P(3, EPage::BTreeIndexV2) }, NO_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{ 2, sharedCache.Collection1, { _P(3) } },
        });

        sharedCache.Provide(sharedCache.Collection1, { _P(0, EPage::BTreeIndexV2) }, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{ 10, sharedCache.Collection1, { _P(2, EPage::BTreeIndexV2) } },
        });

        sharedCache.Provide(sharedCache.Collection1, { _P(2, EPage::BTreeIndexV2) }, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{ 1, sharedCache.Collection1, { _P(2) } },
        });
        sharedCache.CheckFetches({});

        sharedCache.Provide(sharedCache.Collection1, { _P(1, EPage::BTreeIndexV2) }, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{ 10, sharedCache.Collection1, { _P(5, EPage::BTreeIndexV2) } },
        });
    }

    Y_UNIT_TEST(InMemory_CancelledWalkDropsUnsentPages) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetAsyncQueueInFlyLimit(0);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 1u);

        // Occupy the async queue, so the walk below cannot submit its root.
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, { _P(0) }, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{ 10, sharedCache.Collection2, { _P(0) } },
        });

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0, EPage::BTreeIndexV2);
        seed.LevelCount = 1;
        seed.QueueDataPages = false;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { seed });
        sharedCache.CheckFetches({});

        // Withdrawing the walk must erase its unsent bodyless page, so the final detach can expire
        // the collection even while the unrelated fetch remains in flight.
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {});
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
    }

    Y_UNIT_TEST(InMemory_ModeChangeWaitsForOldRegularIndexFetch) {
        TSharedPageCacheMock sharedCache;
        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 2u);
        collection->PageTypes = { EPage::Skip, EPage::Skip };
        sharedCache.Collection1 = collection;

        TEvAttach::TBtreeSeed regularSeed;
        regularSeed.IndexCollectionId = collection->Label();
        regularSeed.DataCollectionId = collection->Label();
        regularSeed.Root = _P(0, EPage::BTreeIndexV2);
        regularSeed.LevelCount = 1;
        regularSeed.QueueDataPages = false;
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::Regular, { regularSeed });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { regularSeed.Root } } });

        auto inMemorySeed = regularSeed;
        inMemorySeed.Root = _P(1, EPage::DataPage);
        inMemorySeed.LevelCount = 0;
        inMemorySeed.QueueDataPages = true;
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::TryKeepInMemory, { inMemorySeed });
        sharedCache.CheckFetches({});

        // The old response finishes its cancelled run; only then can the new seed queue its page.
        sharedCache.Provide(collection, { regularSeed.Root }, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { inMemorySeed.Root } } });
        sharedCache.Provide(collection, { inMemorySeed.Root }, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(InMemory_LastOwnerRemovalCancelsQueuedWalk) {
        for (bool unregister : { false, true }) {
            auto config = TSharedPageCacheMock::DefaultConfig();
            config.SetAsyncQueueInFlyLimit(0);
            TSharedPageCacheMock sharedCache(config);
            sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 1u);

            // Keep the walk root queued with no fetch in flight for its collection.
            sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, { _P(0) }, EPriority::Bkgr);
            sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection2, { _P(0) } } });

            TEvAttach::TBtreeSeed seed;
            seed.IndexCollectionId = sharedCache.Collection1->Label();
            seed.DataCollectionId = sharedCache.Collection1->Label();
            seed.Root = _P(0, EPage::BTreeIndexV2);
            seed.LevelCount = 1;
            seed.Sticky = true;
            sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, { seed });
            sharedCache.CheckFetches({});
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);

            // Remove the owner directly: cancellation expires the collection inside UpdateSeeds.
            if (unregister) {
                sharedCache.Unregister(sharedCache.Sender1);
            } else {
                sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
            }
            sharedCache.Runtime.WaitFor("cancelled walk collection expires", [&] {
                return sharedCache.Counters->PageCollections->Val() == 1;
            });

            sharedCache.Provide(sharedCache.Collection2, { _P(0) }, ASYNC_QUEUE_COOKIE);
            sharedCache.CheckFetches({});
        }
    }

    Y_UNIT_TEST(InMemory_IndexWalkWaitsForCacheCapacity) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.SetLimit(10 * PAGE_TOTAL_SIZE);
        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 5u);
        collection->PageTypes = { EPage::DataPage, EPage::DataPage, EPage::DataPage, EPage::DataPage, EPage::Skip };
        sharedCache.Collection1 = collection;

        // Fill the cache without exposing the V2 index root through metadata preload.
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({ TFetch{ 40, sharedCache.Collection1, { _P(0), _P(1), _P(2), _P(3) } } });
        sharedCache.Provide(collection, { _P(0), _P(1), _P(2), _P(3) }, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveBytes->Val(), 4 * PAGE_TOTAL_SIZE);

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = collection->Label();
        seed.DataCollectionId = collection->Label();
        seed.Root = _P(4, EPage::BTreeIndexV2);
        seed.LevelCount = 1;
        seed.QueueDataPages = false;
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::TryKeepInMemory, { seed });
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->EvictedPages->Val(), 0);
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableSharedCacheConfig()->CopyFrom(config);
        appConfig.MutableSharedCacheConfig()->SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        sharedCache.UpdateConfig(appConfig);
        sharedCache.SetLimit(13 * PAGE_TOTAL_SIZE);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { seed.Root } } });
        sharedCache.Provide(collection, { seed.Root }, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->EvictedPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_CancelledPreloadClearsOnModeExit) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetInMemoryInFlyLimit(PAGE_TOTAL_SIZE - 1);
        TSharedPageCacheMock sharedCache(config);
        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 1u);
        collection->PageTypes = { EPage::Skip };
        sharedCache.Collection1 = collection;

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0, EPage::BTreeIndexV2);
        seed.LevelCount = 1;
        seed.QueueDataPages = false;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, { seed });
        sharedCache.CheckFetches({});

        // Cancellation leaves the queued root for in-memory preload; leaving the mode clears it.
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, {});
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);

        // Keep the collection alive: the loader's missing-collection cleanup cannot hide a stale queue.
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableSharedCacheConfig()->CopyFrom(config);
        appConfig.MutableSharedCacheConfig()->SetInMemoryInFlyLimit(PAGE_TOTAL_SIZE);
        sharedCache.UpdateConfig(appConfig);
        sharedCache.CheckFetches({});

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_QueuedDataPagesLoadAfterWalkCompletes) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetInMemoryInFlyLimit(PAGE_TOTAL_SIZE - 1);
        TSharedPageCacheMock sharedCache(config);

        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 1u);
        collection->PageTypes = { EPage::Skip };
        sharedCache.Collection1 = collection;

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0, EPage::DataPage);
        seed.LevelCount = 0;
        seed.QueueDataPages = true;

        // The walk finishes traversal, but its data page cannot be submitted under the initial limit.
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, { seed });
        sharedCache.CheckFetches({});

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableSharedCacheConfig()->CopyFrom(config);
        appConfig.MutableSharedCacheConfig()->SetInMemoryInFlyLimit(PAGE_TOTAL_SIZE);
        sharedCache.UpdateConfig(appConfig);
        sharedCache.CheckFetches({
            TFetch{ 10, sharedCache.Collection1, { seed.Root } },
        });

        sharedCache.Provide(sharedCache.Collection1, { seed.Root }, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckFetches({});
    }

    void CheckKeepPreloadBatchLimit(ui64 initialLimit, ui32 pageCount, bool growBeforeCompletion) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(initialLimit);
        config.SetInMemoryInFlyLimit(1_MB);
        TSharedPageCacheMock sharedCache(config);
        const ui64 keepBytes = pageCount * PAGE_TOTAL_SIZE;
        const ui64 softLimit = keepBytes * 5 / 2;
        sharedCache.SetLimit(softLimit);
        sharedCache.PrepareKeepStorage(keepBytes);
        auto& cache = TSharedCache::Get();
        const ui64 pageLimit = Min<ui64>(MaxPreloadBatchLocations, cache.KeepLoadRoom().Pages);
        UNIT_ASSERT(pageCount > pageLimit);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, pageCount);
        TVector<TPageLocation> first;
        TVector<TPageLocation> remaining;
        for (ui32 page = 0; page < pageCount; ++page) {
            (page < pageLimit ? first : remaining).push_back(_P(page));
        }

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({ TFetch{ 10 * first.size(), sharedCache.Collection1, first } });

        if (growBeforeCompletion) {
            // Grow before completing I/O so every page can remain resident
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableSharedCacheConfig()->CopyFrom(config);
            appConfig.MutableSharedCacheConfig()->SetMemoryLimit(4_MB);
            sharedCache.UpdateConfig(appConfig);
            sharedCache.CheckFetches({ TFetch{ 10 * remaining.size(), sharedCache.Collection1, remaining } });
            sharedCache.PrepareKeepStorage(keepBytes);
            sharedCache.Provide(sharedCache.Collection1, first, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        } else {
            sharedCache.Provide(sharedCache.Collection1, first, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
            sharedCache.CheckFetches({ TFetch{ 10 * remaining.size(), sharedCache.Collection1, remaining } });
        }
        sharedCache.Provide(sharedCache.Collection1, remaining, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});
        UNIT_ASSERT_VALUES_EQUAL(cache.KeepResidentPageBytes(), pageCount * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(cache.ReservedPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(KeepPreloadSplitsAtHandleCapacity) {
        CheckKeepPreloadBatchLimit(1_MB, 300, true);
    }

    Y_UNIT_TEST(KeepPreloadPreservesMaximumBatchSize) {
        CheckKeepPreloadBatchLimit(32_MB, 1100, false);
    }

    Y_UNIT_TEST(KeepPreloadStopsAtHandleCapacityAndResumesAfterGrowth) {
        constexpr ui32 InFlightPageCount = 100;
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(1_MB);
        config.SetInMemoryInFlyLimit(InFlightPageCount * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        const ui32 pageCount = 300;
        const ui64 keepBytes = pageCount * PAGE_TOTAL_SIZE;
        sharedCache.SetLimit(keepBytes * 5 / 2);
        sharedCache.PrepareKeepStorage(keepBytes);
        auto& cache = TSharedCache::Get();
        const ui64 pageLimit = cache.KeepLoadRoom().Pages;
        UNIT_ASSERT(pageLimit > InFlightPageCount && pageLimit <= 2 * InFlightPageCount);
        UNIT_ASSERT(pageLimit < pageCount);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, pageCount);
        TVector<TPageLocation> first, second, remaining;
        for (ui32 page = 0; page < pageCount; ++page) {
            (page < InFlightPageCount ? first : page < pageLimit ? second : remaining).push_back(_P(page));
        }
        ui32 evictions = 0;
        TBlockEvents<TEvKeepPageEvicted> observer(sharedCache.Runtime, [&](const auto&) {
            ++evictions;
            return false;
        });

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({ TFetch{ 10 * first.size(), sharedCache.Collection1, first } });
        sharedCache.Provide(sharedCache.Collection1, first, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckFetches({ TFetch{ 10 * second.size(), sharedCache.Collection1, second } });
        sharedCache.Provide(sharedCache.Collection1, second, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckFetches({});
        for (ui32 retry = 0; retry < 3; ++retry) {
            sharedCache.Send(sharedCache.Sender1, new TEvResourcesAvailable);
            sharedCache.Send(sharedCache.Sender1, new TKikimrEvents::TEvWakeup(ui64(EWakeupTag::DoGCScheduled)));
            sharedCache.CheckFetches({});
        }
        UNIT_ASSERT_VALUES_EQUAL(evictions, 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.KeepResidentPageBytes(), pageLimit * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(cache.ReservedPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableSharedCacheConfig()->CopyFrom(config);
        appConfig.MutableSharedCacheConfig()->SetMemoryLimit(4_MB);
        sharedCache.UpdateConfig(appConfig);
        THashSet<TPageId> loaded;
        while (loaded.size() < remaining.size()) {
            sharedCache.Runtime.WaitFor("preload progress after growth", [&] {
                return !sharedCache.Fetches->empty();
            }, TDuration::Seconds(5));
            TVector<TFetch> expected;
            TVector<TPageLocation> provided;
            for (const auto& fetch : *sharedCache.Fetches) {
                TVector<TPageLocation> pages;
                for (const auto& location : fetch->Get()->Pages) {
                    const TPageId page = location.Offset.AsPageIndex();
                    UNIT_ASSERT_C(page >= pageLimit && page < pageCount,
                        TStringBuilder() << "unexpected page=" << page << " first missing=" << pageLimit);
                    UNIT_ASSERT(loaded.insert(page).second);
                    pages.push_back(_P(page));
                    provided.push_back(_P(page));
                }
                expected.push_back({ 10 * pages.size(), sharedCache.Collection1, std::move(pages) });
            }
            UNIT_ASSERT(provided.size() <= InFlightPageCount);
            sharedCache.CheckFetches(expected);
            sharedCache.Provide(sharedCache.Collection1, std::move(provided), TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        }
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});
        UNIT_ASSERT_VALUES_EQUAL(evictions, 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.KeepResidentPageBytes(), pageCount * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(cache.ReservedPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(KeepPreloadReplacesRegularPagesWhenAllHandlesAreOccupied) {
        for (ui32 adoptedOffset : { 0, 3 }) {
            auto config = TSharedPageCacheMock::DefaultConfig();
            config.SetMemoryLimit(1_MB);
            config.SetInMemoryInFlyLimit(1_MB);
            TSharedPageCacheMock sharedCache(config);
            auto& cache = TSharedCache::Get();
            sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
            sharedCache.Collection2 =
                MakeIntrusiveConst<TPageCollectionMock>(2ul, static_cast<ui32>(cache.TargetHandleCount()));
            sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular);
            sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2, ECacheMode::Regular);
            auto retained =
                cache.AdmitPage(sharedCache.Collection1, _P(adoptedOffset), TSharedData::Copy(TString(10, 'x')));
            UNIT_ASSERT(retained);
            retained.Drop();
            for (ui32 page = 0; cache.AvailableHandleCount() != 0; ++page) {
                auto regular = cache.AdmitPage(sharedCache.Collection2, _P(page), TSharedData::Copy(TString(10, 'x')));
                UNIT_ASSERT(regular);
            }
            ui32 keepEvictions = 0;
            TBlockEvents<TEvKeepPageEvicted> observer(sharedCache.Runtime, [&](const auto&) {
                ++keepEvictions;
                return false;
            });
            sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
            // The adopted hit consumes one of the three slots available to fresh Keep pages
            TVector<TPageLocation> missing;
            for (ui32 page = 0; page < 4 && missing.size() < 2; ++page) {
                if (page != adoptedOffset) {
                    missing.push_back(_P(page));
                }
            }
            sharedCache.CheckFetches({ TFetch{ 20, sharedCache.Collection1, missing } });
            sharedCache.Provide(sharedCache.Collection1, missing, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
            sharedCache.CheckFetches({});
            sharedCache.CheckResults({});
            UNIT_ASSERT_VALUES_EQUAL(keepEvictions, 0);
            UNIT_ASSERT_VALUES_EQUAL(cache.KeepResidentPageBytes(), 3 * PAGE_TOTAL_SIZE);
            UNIT_ASSERT_VALUES_EQUAL(cache.ReservedPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        }
    }

    void CheckKeepPreloadAdmissionRace(bool pendingPage) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(1_MB);
        config.SetInMemoryInFlyLimit(1_MB);
        TSharedPageCacheMock sharedCache(config);
        auto& cache = TSharedCache::Get();

        struct TRacingCollection final : TPageCollectionMock {
            using TPageCollectionMock::TPageCollectionMock;

            const TLogoBlobID& Label() const noexcept override {
                if (OnRequest) {
                    auto callback = std::move(OnRequest);
                    if (!callback()) {
                        OnRequest = std::move(callback);
                    }
                }
                return TPageCollectionMock::Label();
            }

            mutable std::function<bool()> OnRequest;
        };

        TIntrusiveConstPtr<TRacingCollection> collection = MakeIntrusive<TRacingCollection>(1ul, pendingPage ? 3u : 2u);
        const TLogoBlobID id = collection->Label();
        sharedCache.Collection1 = collection;
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::Regular);
        TVector<TCacheCollectionRef> foreignCollections;
        TPageFetch foreignFetch;
        TPageCacheItem readyItem;
        bool raced = false;
        collection->OnRequest = [&] {
            TCacheCollectionRef owner;
            Y_ABORT_UNLESS(cache.Find(id, owner) == ECacheResultStatus::Hit);
            if (owner->GetActorState()->KeepPreloadRequests.empty()) {
                return false;
            }
            raced = true;
            if (pendingPage) {
                auto ready = cache.AdmitPage(collection, _P(0), TSharedData::Copy(TString(10, 'x')));
                Y_ABORT_UNLESS(ready);
                readyItem = ready.CacheItem();
                TCachePageRequest request(_P(1), MakeIntrusive<TPageFetchWaiter>());
                Y_ABORT_UNLESS(cache.FindOrInsertBatch(owner.CacheItem(), { &request, 1 }));
                Y_ABORT_UNLESS(request.Status() == ECacheResultStatus::Inserted);
                foreignFetch = std::move(request.Fetch());
            } else {
                for (ui64 foreignId = 100; cache.AvailableHandleCount() > 1; ++foreignId) {
                    foreignCollections.push_back(
                        cache.AdmitCollection(MakeIntrusiveConst<TPageCollectionMock>(foreignId, 1u)));
                }
            }
            return true;
        };
        Y_DEFER {
            collection->OnRequest = {};
        };
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::TryKeepInMemory);
        if (pendingPage) {
            sharedCache.CheckFetches({ TFetch{ 10, collection, { _P(2) } } });
            sharedCache.Provide(collection, { _P(2) }, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        }
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});
        UNIT_ASSERT(raced);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        if (pendingPage) {
            auto cacheOp = cache.BeginOperation();
            const auto state =
                THandleState::FromRaw(cacheOp.Handles()[readyItem.Index()].State.load(std::memory_order_relaxed));
            UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
            UNIT_ASSERT(foreignFetch.FailReady());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(cache.ReservedPages(), 0);
            for (auto& owner : foreignCollections) {
                UNIT_ASSERT(cache.DetachCollection(owner.CacheItem()));
                UNIT_ASSERT(cache.EraseCollection(owner.CacheItem()));
            }
            foreignCollections.clear();
        }
        sharedCache.Send(sharedCache.Sender1, new TEvResourcesAvailable);
        sharedCache.CheckFetches({});
        sharedCache.Send(sharedCache.Sender1, new TKikimrEvents::TEvWakeup(ui64(EWakeupTag::DoGCScheduled)));
        const TVector<TPageLocation> missing = pendingPage ? TVector{ _P(1) } : TVector{ _P(0), _P(1) };
        sharedCache.CheckFetches({ TFetch{ 10 * missing.size(), collection, missing } });
        sharedCache.Provide(collection, missing, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});
        UNIT_ASSERT_VALUES_EQUAL(cache.KeepResidentPageBytes(), (pendingPage ? 3 : 2) * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(cache.ReservedPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(KeepPreloadAbandonsBatchWhenHandlesDisappearAfterPreflight) {
        CheckKeepPreloadAdmissionRace(false);
    }

    Y_UNIT_TEST(KeepPreloadAbandonsPartialHitsWhenAnotherRequestOwnsPendingPage) {
        CheckKeepPreloadAdmissionRace(true);
    }

    enum class EIndexFetchLimit {
        Handles,
        Bytes,
        QueuedShrink,
        ByteAdmissionRace,
        HandleAdmissionRace,
    };

    void CheckBoundedIndexFetch(EIndexFetchLimit limit) {
        const bool handleLimit = limit == EIndexFetchLimit::Handles || limit == EIndexFetchLimit::HandleAdmissionRace;
        const bool admissionRace =
            limit == EIndexFetchLimit::ByteAdmissionRace || limit == EIndexFetchLimit::HandleAdmissionRace;
        const ui64 pageSize = handleLimit ? 10 : 4096;
        const ui64 pageBytes = pageSize + TSharedData::OverheadSize;
        auto config = TSharedPageCacheMock::DefaultConfig();
        ui64 byteLimit = 8 * pageBytes;
        if (handleLimit) {
            byteLimit = 1_MB;
        } else if (limit == EIndexFetchLimit::Bytes) {
            byteLimit = 2 * pageSize + TSharedData::OverheadSize;
        } else if (admissionRace) {
            byteLimit = 2 * pageBytes + pageBytes / 2;
        }
        config.SetMemoryLimit(byteLimit);
        config.SetAsyncQueueInFlyLimit(limit == EIndexFetchLimit::QueuedShrink ? 0 : 8_MB);
        TSharedPageCacheMock sharedCache(config);
        auto& cache = TSharedCache::Get();
        const ui32 pageCount = limit == EIndexFetchLimit::Handles ? static_cast<ui32>(cache.TargetHandleCount() + 8)
                                                                  : (limit == EIndexFetchLimit::Bytes ? 6 : 4);

        struct TRacingIndexCollection final : TPageCollectionMock {
            using TPageCollectionMock::TPageCollectionMock;

            mutable ui32 LabelReads = 0;
            mutable std::function<bool()> AfterPreflight;

            const TLogoBlobID& Label() const noexcept override {
                // Ignore nested eviction notifications between preflight and the next request-label read
                if (AfterPreflight && ++LabelReads > 1) {
                    auto consume = std::move(AfterPreflight);
                    if (!consume()) {
                        AfterPreflight = std::move(consume);
                    }
                }
                return TPageCollectionMock::Label();
            }
        };

        auto indexCollection = MakeIntrusive<TRacingIndexCollection>(1ul, pageCount + 1);
        indexCollection->PageTypes.assign(pageCount + 1, EPage::BTreeIndexV2);
        indexCollection->MetaPagesOverride = 0;
        TIntrusiveConstPtr<TRacingIndexCollection> index = std::move(indexCollection);
        auto data = MakeIntrusive<TPageCollectionMock>(2ul, 1u);
        data->MetaPagesOverride = 0;
        sharedCache.Collection1 = index;
        sharedCache.Collection2 = data;
        sharedCache.Attach(sharedCache.Sender1, index, ECacheMode::Regular);

        auto blocker = MakeIntrusiveConst<TPageCollectionMock>(3ul, 1u);
        if (limit == EIndexFetchLimit::QueuedShrink) {
            sharedCache.Request(sharedCache.Sender2, blocker, { _P(0) }, EPriority::Bkgr);
            sharedCache.CheckFetches({ TFetch{ 10, blocker, { _P(0) } } });
        }

        NTable::NTest::TLayoutCook layout;
        layout.Col(0, 0, NScheme::NTypeIds::Uint32).Key({ 0 });
        NTable::NPage::TBtreeIndexNodeWriter<NTable::NPage::TBtreeIndexNode::TChildV2> writer(
            new NTable::TPartScheme(layout.RowScheme()->Cols), {});
        for (ui32 child = 1; child <= pageCount; ++child) {
            const auto location = TPageLocation::FromPageIndex(child, pageSize, EPage::BTreeIndexV2, child + 1);
            if (child > 1) {
                const TCell key = TCell::Make(child);
                writer.AddKey({ &key, 1 });
            }
            writer.AddChild({ location.Offset, location.Size, location.Crc32, 1, location.Size, 0, 0 });
        }
        auto body = writer.Finish();
        if (limit == EIndexFetchLimit::ByteAdmissionRace) {
            UNIT_ASSERT(body.size() + TSharedData::OverheadSize <= pageBytes / 2);
        }
        const auto rootLocation = TPageLocation::FromPageIndex(0, body.size(), EPage::BTreeIndexV2, 1);
        auto root = cache.AdmitPage(index, rootLocation, std::move(body));
        UNIT_ASSERT(root);
        root.Drop();
        sharedCache.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = index->Label();
        seed.DataCollectionId = data->Label();
        seed.Root = rootLocation;
        seed.LevelCount = 2;
        seed.QueueDataPages = false;
        TCachePageRef foreignPage;
        TVector<TCacheCollectionRef> foreignCollections;
        bool consumedAfterPreflight = false;
        ui32 restarts = 0;
        TBlockEvents<TEvResult> results(sharedCache.Runtime, [&](const TEvResult::TPtr& event) {
            if (event->Cookie == Max<ui64>() && event->Get()->Status == NKikimrProto::RACE) {
                ++restarts;
            }
            return false;
        });
        if (admissionRace) {
            const auto indexItem = cache.AdmitCollection(index).CacheItem();
            const auto indexRefs = [&cache, indexItem] {
                auto cacheOp = cache.BeginOperation();
                return THandleState::FromRaw(cacheOp.Handles()[indexItem.Index()].State.load(std::memory_order_relaxed))
                    .Refs();
            };
            const auto stableIndexRefs = indexRefs();
            index->AfterPreflight = [&, indexRefs, stableIndexRefs] {
                if (indexRefs() != stableIndexRefs) {
                    return false;
                }
                consumedAfterPreflight = true;
                if (handleLimit) {
                    const auto first = TPageLocation::FromPageIndex(1, pageSize, EPage::BTreeIndexV2, 2);
                    const auto second = TPageLocation::FromPageIndex(2, pageSize, EPage::BTreeIndexV2, 3);
                    const TPageLocation missing[]{ first, second };
                    const TCacheHandleRequirement requirement{ seed.IndexCollectionId, missing };
                    for (ui64 id = 100; cache.CanAllocateHandles({ &requirement, 1 }); ++id) {
                        foreignCollections.push_back(
                            cache.AdmitCollection(MakeIntrusiveConst<TPageCollectionMock>(id, 1u)));
                    }
                    const TCacheHandleRequirement one{ seed.IndexCollectionId, { missing, 1 } };
                    Y_ABORT_UNLESS(cache.CanAllocateHandles({ &one, 1 }));
                } else {
                    Y_ABORT_UNLESS(cache.CanReserve(2 * pageBytes));
                    auto foreign = MakeIntrusiveConst<TPageCollectionMock>(100ul, 1u);
                    const auto location = TPageLocation::FromPageIndex(0, pageSize, EPage::DataPage, 1);
                    foreignPage = cache.AdmitPage(foreign, location, TSharedData::Copy(TString(pageSize, 'p')));
                    Y_ABORT_UNLESS(foreignPage);
                }
                return true;
            };
        }
        Y_DEFER {
            index->AfterPreflight = {};
        };
        sharedCache.Attach(sharedCache.Sender1, data, ECacheMode::TryKeepInMemory, { seed });
        if (limit == EIndexFetchLimit::QueuedShrink) {
            sharedCache.CheckFetches({});
            sharedCache.SetLimit(0, 0, 2 * pageBytes);
            sharedCache.Provide(blocker, { _P(0) }, ASYNC_QUEUE_COOKIE);
            sharedCache.CheckResults({ TFetch{ 1, blocker, { _P(0) } } });
        }

        THashSet<TPageOffset> observed;
        ui32 batches = 0;
        while (observed.size() < pageCount) {
            UNIT_ASSERT(++batches <= pageCount);
            sharedCache.Runtime.WaitFor("bounded index fetch", [&] {
                return !sharedCache.Fetches->empty();
            }, TDuration::Seconds(5));
            auto event = std::move(sharedCache.Fetches->front());
            sharedCache.Fetches->pop_front();
            const auto& fetch = *event->Get();
            UNIT_ASSERT_VALUES_EQUAL(fetch.PageCollection->Label(), index->Label());
            UNIT_ASSERT(fetch.Pages.size() <= cache.TargetHandleCount() - TSharedCache::RequestHandleOverhead);
            ui64 bytes = 0;
            auto response = MakeHolder<NBlockIO::TEvData>(NKikimrProto::OK, index, fetch.Cookie);
            for (const auto& location : fetch.Pages) {
                UNIT_ASSERT(location.Offset.AsPageIndex() > 0 && location.Offset.AsPageIndex() <= pageCount);
                UNIT_ASSERT(observed.insert(location.Offset).second);
                bytes += location.Size + TSharedData::OverheadSize;
                response->Pages.emplace_back(location.Offset, TSharedData::Copy(TString(location.Size, 'x')));
            }
            UNIT_ASSERT(bytes <= cache.CurrentLimit());
            sharedCache.Send(event->GetRecipientRewrite(), response.Release(), ASYNC_QUEUE_COOKIE);
        }
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});
        if (admissionRace) {
            UNIT_ASSERT(consumedAfterPreflight);
            UNIT_ASSERT(restarts > 0);
        }
    }

    Y_UNIT_TEST(IndexWalkSplitsLevelsAtHandleCapacity) {
        CheckBoundedIndexFetch(EIndexFetchLimit::Handles);
    }
    Y_UNIT_TEST(IndexWalkSplitsLevelsAtByteCapacityIncludingHeaders) {
        CheckBoundedIndexFetch(EIndexFetchLimit::Bytes);
    }
    Y_UNIT_TEST(IndexWalkResplitsQueuedWindowAfterLimitShrinks) {
        CheckBoundedIndexFetch(EIndexFetchLimit::QueuedShrink);
    }
    Y_UNIT_TEST(IndexWalkRestartsWhenBytesDisappearAfterPreflight) {
        CheckBoundedIndexFetch(EIndexFetchLimit::ByteAdmissionRace);
    }
    Y_UNIT_TEST(IndexWalkRestartsWhenHandlesDisappearAfterPreflight) {
        CheckBoundedIndexFetch(EIndexFetchLimit::HandleAdmissionRace);
    }

    Y_UNIT_TEST(DemandReadPromotesPageFromResourceWaitingBatch) {
        TSharedPageCacheMock sharedCache;
        sharedCache.SetLimit(0, 0, 2 * PAGE_TOTAL_SIZE);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, { _P(0) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection2, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection2, { _P(0) });
        auto held = sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection2, { _P(0) } } });

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, { _P(1), _P(2) }, EPriority::Bkgr);
        sharedCache.CheckFetches({});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, { _P(1) }, EPriority::Fast);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(1) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(1) }, NO_QUEUE_COOKIE);
        auto demanded = sharedCache.CheckResults({ TFetch{ 3, sharedCache.Collection1, { _P(1) } } });
        UNIT_ASSERT_VALUES_EQUAL(demanded.size(), 1);

        held.clear();
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(2) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(2) }, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, { _P(1), _P(2) } } });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});
    }

    Y_UNIT_TEST(InMemory_FailedBtreeIndexFetch) {
        // The tree page cannot be read, so the walk must give up instead of re-requesting it.
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0);
        seed.LevelCount = 2;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular, {seed});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(0)}}
        });

        sharedCache.Fail(sharedCache.Collection1, { _P(0) }, ASYNC_QUEUE_COOKIE);

        sharedCache.Wakeup();
        sharedCache.CheckFetches({});
    }

    Y_UNIT_TEST(InMemory_FailedPreloadFetch) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(3 * PAGE_TOTAL_SIZE);
        config.SetInMemoryInFlyLimit(PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 1u);
        collection->PageTypes = { EPage::Skip };
        sharedCache.Collection1 = collection;

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = sharedCache.Collection1->Label();
        seed.DataCollectionId = sharedCache.Collection1->Label();
        seed.Root = _P(0, EPage::DataPage);
        seed.LevelCount = 0;
        seed.QueueDataPages = true;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory, { seed });
        sharedCache.CheckFetches({
            TFetch{ 10, sharedCache.Collection1, { seed.Root } },
        });

        sharedCache.Fail(sharedCache.Collection1, { seed.Root }, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);

        sharedCache.CheckResults(
            { TFetch{ 0, sharedCache.Collection1, {} } },
            NKikimrProto::ERROR);
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);

        sharedCache.Unregister(sharedCache.Sender1);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableSharedCacheConfig()->CopyFrom(config);
        appConfig.MutableSharedCacheConfig()->SetInMemoryInFlyLimit(0);
        sharedCache.UpdateConfig(appConfig);

        // A new in-memory collection with no resident pages must not inherit the failed page's reservation.
        auto emptyCollection = MakeIntrusive<TPageCollectionMock>(3ul, 1u);
        emptyCollection->PageTypes = { EPage::Skip };
        sharedCache.Attach(sharedCache.Sender1, emptyCollection, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, { _P(0) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection2, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection2, { _P(0) });
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection2, { _P(0) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);
    }

    Y_UNIT_TEST(InMemory_ResidentSinglePageV2ChangesTier) {
        TSharedPageCacheMock sharedCache;
        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 1u);
        collection->PageTypes = { EPage::Skip };
        sharedCache.Collection1 = collection;
        const auto dataPage = _P(0, EPage::DataPage);

        sharedCache.Request(sharedCache.Sender1, collection, { dataPage });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { dataPage } } });
        sharedCache.Provide(collection, { dataPage });
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);

        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = collection->Label();
        seed.DataCollectionId = collection->Label();
        seed.Root = dataPage;
        seed.LevelCount = 0;
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::TryKeepInMemory, { seed });
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), PAGE_TOTAL_SIZE);

        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);
    }

    Y_UNIT_TEST(InMemory_ResidentV1ShadowDoesNotReserveMemory) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(4 * PAGE_TOTAL_SIZE);
        config.SetInMemoryInFlyLimit(0);
        TSharedPageCacheMock sharedCache(config);
        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 1u);
        collection->PageTypes = { EPage::BTreeIndex };
        collection->SkipV1Shadow = true;
        sharedCache.Collection1 = collection;
        const auto shadow = _P(0, EPage::BTreeIndex);

        sharedCache.Request(sharedCache.Sender1, collection, { shadow });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { shadow } } });
        sharedCache.Provide(collection, { shadow });
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, { _P(0) } } });

        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::TryKeepInMemory);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);

        // Both pages must fit in the regular tier: the shadow contributes no in-memory reservation.
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, { _P(0) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection2, { _P(0) } } });
        sharedCache.Provide(sharedCache.Collection2, { _P(0) });
        sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection2, { _P(0) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 2);

        // Leaving the mode while the regular shadow is still alive must not subtract its bytes.
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::Regular);
        sharedCache.Attach(sharedCache.Sender1, collection, ECacheMode::TryKeepInMemory);
        sharedCache.SetLimit(0);
        for (ui32 pass = 0;
             pass < 400 && sharedCache.Counters->ActivePages->Val() + sharedCache.Counters->PassivePages->Val() != 0;
             ++pass) {
            sharedCache.Wakeup();
        }
        UNIT_ASSERT_VALUES_EQUAL(
            sharedCache.Counters->ActivePages->Val() + sharedCache.Counters->PassivePages->Val(), 0);
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        sharedCache.Detach(sharedCache.Sender1, collection);

        // After eviction, no stale reservation may steal capacity from another regular collection.
        sharedCache.SetLimit(2 * PAGE_TOTAL_SIZE);
        auto emptyCollection = MakeIntrusive<TPageCollectionMock>(3ul, 1u);
        emptyCollection->PageTypes = { EPage::Skip };
        sharedCache.Attach(sharedCache.Sender1, emptyCollection, ECacheMode::TryKeepInMemory);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, { _P(1) });
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection2, { _P(1) } } });
        sharedCache.Provide(sharedCache.Collection2, { _P(1) });
        sharedCache.CheckResults({ TFetch{ 3, sharedCache.Collection2, { _P(1) } } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);
    }

    Y_UNIT_TEST(InMemory_SkipV1ShadowPages) {
        // A dual-root part keeps a V1 shadow the readers do not use; moving the collection in memory must
        // not spend the in-memory budget on it (nor on the pages the meta marks as excluded).
        TSharedPageCacheMock sharedCache;
        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 4u);
        collection->PageTypes = {EPage::BTreeIndex, EPage::Skip, EPage::DataPage, EPage::DataPage};
        collection->SkipV1Shadow = true;
        sharedCache.Collection1 = collection;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {_P(2), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(2), _P(3)}, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 2 * PAGE_TOTAL_SIZE);
    }

    Y_UNIT_TEST(InMemory_SkipExcludedPagesOnly) {
        // Without the V1 shadow flag only the excluded pages are skipped: the V1 index is preloaded.
        TSharedPageCacheMock sharedCache;
        auto collection = MakeIntrusive<TPageCollectionMock>(1ul, 4u);
        collection->PageTypes = {EPage::BTreeIndex, EPage::Skip, EPage::DataPage, EPage::DataPage};
        sharedCache.Collection1 = collection;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(0, EPage::BTreeIndex), _P(2), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(0, EPage::BTreeIndex), _P(2), _P(3)}, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 3 * PAGE_TOTAL_SIZE);
    }

    Y_UNIT_TEST(ConfigUpdate_PartialBootstrapPreservesBootstrapSharedCacheConfig) {
        TSharedPageCacheMock sharedCache;

        const ui64 bootstrapSharedCacheLimit = 2 * PAGE_TOTAL_SIZE;
        sharedCache.Runtime.GetAppData().BootstrapConfig.MutableSharedCacheConfig()->SetMemoryLimit(bootstrapSharedCacheLimit);

        NKikimrConfig::TAppConfig update;
        auto* tablet = update.MutableBootstrapConfig()->AddTablet();
        tablet->MutableInfo()->SetTabletID(1);

        sharedCache.UpdateConfig(update);

        UNIT_ASSERT_VALUES_EQUAL_C(
            sharedCache.Counters->ConfigLimitBytes->Val(),
            bootstrapSharedCacheLimit,
            "Partial bootstrap update should preserve bootstrap-level shared cache config");
    }

    Y_UNIT_TEST(InMemory_Enabling) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * PAGE_TOTAL_SIZE;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(2)});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(2)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(2)});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(2)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {_P(0), _P(1), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(0), _P(1), _P(3)}, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_Enabling_AllRequested) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * PAGE_TOTAL_SIZE;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)});
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_Disabling) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * PAGE_TOTAL_SIZE;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_Detach) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * PAGE_TOTAL_SIZE;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Detach(sharedCache.Sender2, sharedCache.Collection1);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_Unregister) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * PAGE_TOTAL_SIZE;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Unregister(sharedCache.Sender1);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Unregister(sharedCache.Sender2);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_PromotesReferencedResidentPages) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        TVector<TPageLocation> pages{ _P(0), _P(1), _P(2), _P(3) };
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, pages);
        sharedCache.CheckFetches({
            TFetch{ 40, sharedCache.Collection1, pages }
        });
        sharedCache.Provide(sharedCache.Collection1, pages);
        auto held = sharedCache.CheckResults({
            TFetch{ 1, sharedCache.Collection1, pages } });

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, pages);
        sharedCache.CheckFetches({});
        auto kept = sharedCache.CheckResults({ TFetch{ 2, sharedCache.Collection1, pages } });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 4 * PAGE_TOTAL_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(held.at(0).data(), kept.at(0).data());
    }

    Y_UNIT_TEST(InMemory_ResidentPagesReturnToRegular) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        TVector<TPageLocation> pages{ _P(0), _P(1), _P(2), _P(3) };
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({ TFetch{ 40, sharedCache.Collection1, pages } });
        sharedCache.Provide(sharedCache.Collection1, pages);
        sharedCache.CheckResults({});
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), 0);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, pages);
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({ TFetch{ 1, sharedCache.Collection1, pages } });
    }

    Y_UNIT_TEST(InMemory_ReloadPages) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.SetLimit(10 * PAGE_TOTAL_SIZE);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 6u);
        TVector<TPageLocation> first{ _P(0), _P(1), _P(2), _P(3) };
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({ TFetch{ 40, sharedCache.Collection1, first } });
        sharedCache.Provide(sharedCache.Collection1, first);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});
        sharedCache.SetLimit(13 * PAGE_TOTAL_SIZE);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(4) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(4) });
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});
        sharedCache.SetLimit(15 * PAGE_TOTAL_SIZE);
        sharedCache.CheckFetches({ TFetch{ 10, sharedCache.Collection1, { _P(5) } } });
        sharedCache.Provide(sharedCache.Collection1, { _P(5) });
        sharedCache.CheckResults({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 6 * PAGE_TOTAL_SIZE);
        sharedCache.SetLimit(0);
        for (ui32 pass = 0;
             pass < 400 && sharedCache.Counters->ActivePages->Val() + sharedCache.Counters->PassivePages->Val() != 0;
             ++pass) {
            sharedCache.Wakeup();
        }
        UNIT_ASSERT_VALUES_EQUAL(
            sharedCache.Counters->ActivePages->Val() + sharedCache.Counters->PassivePages->Val(), 0);
        sharedCache.CheckFetches({});
        sharedCache.SetLimit(10 * PAGE_TOTAL_SIZE);
        sharedCache.CheckFetches({ TFetch{ 40, sharedCache.Collection1, first } });
        sharedCache.Provide(sharedCache.Collection1, first);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 4 * PAGE_TOTAL_SIZE);
    }

    Y_UNIT_TEST(InMemory_ReloadPagesLimitedInFly) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        config.SetInMemoryInFlyLimit(2 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);
        sharedCache.SetLimit(13 * PAGE_TOTAL_SIZE);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 5u);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {_P(0), _P(1)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(0), _P(1)});
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {_P(2), _P(3)}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        sharedCache.Provide(sharedCache.Collection1, {_P(2), _P(3)});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {_P(4)}}
        });
        sharedCache.Provide(sharedCache.Collection1, {_P(4)});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 5 * PAGE_TOTAL_SIZE);
    }

    Y_UNIT_TEST(InMemory_AttachRepeated) {
        auto config = TSharedPageCacheMock::DefaultConfig();
        config.SetMemoryLimit(20 * PAGE_TOTAL_SIZE);
        TSharedPageCacheMock sharedCache(config);

        sharedCache.SetLimit(10 * PAGE_TOTAL_SIZE);
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * PAGE_TOTAL_SIZE;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        sharedCache.Provide(sharedCache.Collection1, {_P(0), _P(1), _P(2), _P(3)}, TRY_KEEP_IN_MEMORY_PRELOAD_COOKIE);
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);

        // Attach same collection again
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissInMemoryPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TargetInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), 20 * PAGE_TOTAL_SIZE);
    }
}
} // namespace NKikimr::NSharedCache
