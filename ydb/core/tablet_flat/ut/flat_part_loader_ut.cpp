#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_sausagecache.h>
#include <ydb/core/tablet_flat/flat_part_store_cache.h>
#include <ydb/core/tablet_flat/flat_part_store.h>
#include <ydb/core/tablet_flat/flat_executor.h>
#include <ydb/core/tablet_flat/flat_fwd_conf.h>
#include <ydb/core/tablet_flat/flat_fwd_iface.h>
#include <ydb/core/tablet_flat/flat_part_index_iter_iface.h>
#include <ydb/core/tablet_flat/flat_part_writer.h>
#include <ydb/core/tablet_flat/flat_table_subset.h>
#include <ydb/core/tablet_flat/flat_writer_bundle.h>
#include <ydb/core/tablet_flat/flat_scan_actor.h>
#include <ydb/core/tablet_flat/cache_thread_test.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tablet_flat/shared_cache_pages.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/cache_fixture.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NTabletFlatExecutor::NOps {

class TColdPartLoadTestAccess {
public:
    using TEvLoaded = TDriver::TEvPrivate::TEvPartLoaded;
    using TEvFailed = TDriver::TEvPrivate::TEvPartFailed;
    using TEvLoadBlob = TDriver::TEvPrivate::TEvLoadBlob;
    using TEvBlobLoaded = TDriver::TEvPrivate::TEvBlobLoaded;

    static NActors::IActor* Create(
        TActorId owner, TIntrusiveConstPtr<NTable::TColdPartStore> part, THolder<NTable::IPartLoaderEnv> environment) {
        auto backend = MakeLFExecutorPageCache();
        if (!environment) {
            environment = backend->MakeLoaderEnvironment();
        }
        return new TDriver::TColdPartLoader(owner, std::move(part), NBlockIO::EPriority::Fast, std::move(environment),
            backend->MakeMetadataWorkingSet());
    }
};

} // namespace NKikimr::NTabletFlatExecutor::NOps

namespace NKikimr::NTable {
namespace {

    using NTabletFlatExecutor::NWriter::TBundle;

    struct TWriteOptions {
        bool Outer = false;
        bool Grouped = false;
        bool Dual = false;
        bool External = false;
        ui32 Step = 1;
        TPartLayout Layout;
    };

    TBundle::TResult WritePart(TWriteOptions options = {}, TVector<NPageCollection::TGlob>* blobs = nullptr) {
        NTest::TLayoutCook layout;
        layout.Col(0, 1, NScheme::NTypeIds::Uint64)
            .Col(options.Grouped ? 1 : 0, 2, NScheme::NTypeIds::String)
            .Key({ 1 });
        const auto scheme = layout.RowScheme();
        const auto tags = scheme->Tags();
        NTabletFlatExecutor::NWriter::TConf conf;
        const ui32 groups = options.Grouped ? 2 : 1;
        const TPartSlots slots(groups, options.Layout);
        conf.Groups.resize(slots.SlotsCount());
        for (auto& group : conf.Groups) {
            group.Channel = 1;
            group.MaxBlobSize = 4096;
            group.Cache = NPage::ECache::Ever;
        }
        conf.BlobsChannels = { 1 };
        conf.OuterChannel = 1;
        conf.ExtraChannel = 1;
        conf.ChannelsShares = NUtil::TChannelsShares({ { 1, 1.0f } });
        conf.Slots = { { 1, 0 } };
        conf.WriteBTreeIndexV2 = options.Dual;
        TBundle bundle(TLogoBlobID(77, 1, options.Step, 0, 0, 0), conf);

        NPage::TConf pages{ true, 512 };
        pages.Groups.resize(groups);
        pages.Layout = options.Layout;
        pages.WriteBTreeIndexV2 = options.Dual;
        pages.WriteFlatIndex = !options.Dual;
        for (auto& group : pages.Groups) {
            group.PageSize = 512;
            group.BTreeIndexNodeTargetSize = 512;
        }
        if (options.Outer || options.External) {
            pages.SmallEdge = 8;
            pages.LargeEdge = options.External ? 16 : 128;
        }
        TPartWriter writer(new TPartScheme(scheme->Cols), tags, bundle, pages, TEpoch::Zero());
        const TString value(32, 'x');
        for (ui64 key = 0; key < 128; ++key) {
            const TCell keyCell(reinterpret_cast<const char*>(&key), sizeof(key));
            writer.BeginKey({ &keyCell, 1 });
            TRowState row(tags.size());
            row.Touch(ERowOp::Upsert);
            row.Set(1, ECellOp::Set, TCell(value.data(), value.size()));
            writer.AddKeyVersion(row, TRowVersion::Min());
            writer.EndKey();
        }
        UNIT_ASSERT_VALUES_EQUAL(writer.Finish().Parts, 1);
        if (blobs) {
            *blobs = bundle.GetBlobsToSave();
        }
        auto results = bundle.Results();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        return std::move(results.front());
    }

    TPartComponents Components(const TBundle::TResult& written, bool rawMetadata = false) {
        TPartComponents components{ .Epoch = TEpoch::Zero() };
        components.Opaque = written.Overlay;
        for (const auto& collection : written.PageCollections) {
            const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(collection.PageCollection.Get());
            UNIT_ASSERT(packet);
            auto& component = components.PageCollectionComponents.emplace_back();
            component.LargeGlobId = packet->LargeGlobId;
            if (rawMetadata) {
                component.RawMeta = packet->Meta.Raw;
            }
        }
        return components;
    }

    TCacheCollectionRef AdmitCollection(const TBundle::TResult& written, ui32 slot) {
        return NTable::NTest::AdmitCollection(
            NSharedCache::TCache::Get(), written.PageCollections.at(slot).PageCollection);
    }

    TCacheCollectionRef AdmitReadCollection(const TBundle::TResult& written) {
        const auto* source =
            dynamic_cast<const NPageCollection::TPageCollection*>(written.PageCollections.front().PageCollection.Get());
        UNIT_ASSERT(source);
        auto id = source->LargeGlobId;
        id.Lead = TLogoBlobID(77, 1, 201, 0, id.Lead.BlobSize(), id.Lead.Cookie());
        auto& cache = TCache::Get();
        auto collection = NTable::NTest::AdmitCollection(
            cache, MakeIntrusiveConst<NPageCollection::TPageCollection>(id, TSharedData(source->Meta.Raw)));
        UNIT_ASSERT(cache.SetCollectionPagesCacheMode(collection.CacheItem(), NPage::ECacheMode::Regular));
        return collection;
    }

    TVector<const NPageCollection::TPageData*> DataPages(const TBundle::TResult& written) {
        TVector<const NPageCollection::TPageData*> result;
        for (const auto* pages :
            { &written.PageCollections.front().StickyPages, &written.PageCollections.front().RegularPages }) {
            for (const auto& page : *pages) {
                if (page.Location.Type == EPage::DataPage) {
                    result.push_back(&page);
                }
            }
        }
        UNIT_ASSERT(result.size() >= 2);
        return result;
    }

    TLoader::TLoaderEnv MakeReadEnv(TCacheCollectionRef collection) {
        TVector<TCacheCollectionRef> collections;
        collections.push_back(std::move(collection));
        return TLoader::TLoaderEnv(std::move(collections), TPartSlots(1, {}));
    }

    ui32 ActiveLoaderOperations() {
        return NSharedCache::SpaceHazardActiveCount(
            NSharedCache::TlsCacheHazards[0].State.load(std::memory_order_relaxed));
    }

    const NPageCollection::TPageData& FindWrittenPage(
        const TBundle::TResult& written, const TLogoBlobID& collectionId, const TPageLocation& location) {
        const NPageCollection::TPageData* source = nullptr;
        for (const auto& collection : written.PageCollections) {
            if (collection.PageCollection->Label() != collectionId) {
                continue;
            }
            for (const auto* group : { &collection.StickyPages, &collection.RegularPages }) {
                for (const auto& page : *group) {
                    if (page.Location == location) {
                        source = &page;
                    }
                }
            }
        }
        UNIT_ASSERT_C(source, "Requested page was not captured by the real writer");
        return *source;
    }

    TVector<NSharedCache::TEvResult::TLoaded> LoadWrittenPages(const TBundle::TResult& written,
        const TIntrusiveConstPtr<NPageCollection::IPageCollection>& pageCollection,
        TConstArrayRef<TPageLocation> locations) {
        TVector<NSharedCache::TEvResult::TLoaded> pages;
        for (const auto& location : locations) {
            const auto& source = FindWrittenPage(written, pageCollection->Label(), location);
            auto page = NTable::NTest::AdmitPage(
                NSharedCache::TCache::Get(), pageCollection, location, TSharedData(source.Data), true);
            UNIT_ASSERT(page);
            pages.emplace_back(location.Offset, location.Size, std::move(page));
        }
        return pages;
    }

    TPartView Finish(TLoader& loader, const TBundle::TResult& written) {
        while (auto fetch = loader.Run({ .PreloadIndex = false, .PreloadData = false })) {
            UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 0);
            NSharedCache::TEvResult reply(fetch.PageCollection, NKikimrProto::OK, 0);
            reply.Pages = LoadWrittenPages(written, fetch.PageCollection, fetch.Pages);
            loader.Save(reply);
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 0);
        return loader.Result();
    }

    struct TMetadataHost {
        NActors::TActorSystem* System;
        TActorId ReplyTo{ 1, "metadata" };
        NTabletFlatExecutor::IPageMetadataWorkingSet* MetadataWorkingSet = nullptr;
        THashMap<ui32, TIntrusivePtr<NSharedCache::TCollectionRequest>> Requests;
        TVector<TIntrusivePtr<NSharedCache::TCollectionRequest>> Updates;
        bool SendAllowed = true;

        TMetadataLoadContext Context() {
            return { *this, System, ReplyTo, 42, NTabletFlatExecutor::NBlockIO::EPriority::Fast, MetadataWorkingSet };
        }

        void LoadRawMetadata(ui32, const NPageCollection::TLargeGlobId&) {
            UNIT_FAIL("LF metadata must be requested through the shared cache");
        }

        bool SendMetadataRequest(ui32 slot, const void* identity, THolder<NActors::IEventBase> event) {
            const auto* request = CheckedCast<NSharedCache::TEvCollectionRequest*>(event.Get());
            UNIT_ASSERT(request->Request.Get() == identity);
            UNIT_ASSERT(Requests.emplace(slot, request->Request).second);
            return SendAllowed;
        }

        void SendMetadataUpdate(THolder<NActors::IEventBase> event) {
            Updates.push_back(CheckedCast<NSharedCache::TEvCollectionRequestUpdate*>(event.Get())->Request);
        }

        EMetadataLoadResult Reply(TLoader& loader, ui32 slot, TCacheCollectionRef collection,
            NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
            const auto& request = Requests.at(slot);
            NSharedCache::TEvCollectionResult reply(request, status, std::move(collection));
            NTabletFlatExecutor::TPageCacheMetadataResult metadata{ .Request = request.Get(), .Status = status };
            return loader.AcceptMetadata(slot, metadata, reply, Context());
        }
    };

    struct TRawMetadataIO {
        TVector<std::pair<ui32, TLogoBlobID>> Requests;

        TMetadataLoadContext Context() {
            return { *this, nullptr, {}, 42, NTabletFlatExecutor::NBlockIO::EPriority::Fast };
        }

        void LoadRawMetadata(ui32 slot, const NPageCollection::TLargeGlobId& id) {
            Requests.emplace_back(slot, id.Lead);
        }

        bool SendMetadataRequest(ui32, const void*, THolder<NActors::IEventBase>) {
            UNIT_FAIL("Private metadata must be fetched as raw blobs");
            return false;
        }

        void SendMetadataUpdate(THolder<NActors::IEventBase>) {
            UNIT_FAIL("Private metadata must not update the shared cache");
        }
    };

} // anonymous namespace

namespace {

    using TColdAccess = NTabletFlatExecutor::NOps::TColdPartLoadTestAccess;

    struct TColdFixture {
        TColdFixture() {
            NSharedCache::TCacheThreadTestAccess::Setup(Runtime);
            TAutoPtr<TAppPrepare> app = new TAppPrepare;
            app->SetEnableLockFreePageCache(true);
            Runtime.Initialize(app->Unwrap());
            First = Runtime.AllocateEdgeActor();
            Second = Runtime.AllocateEdgeActor();
            Proxy = Runtime.AllocateEdgeActor();
            Runtime.RegisterService(MakeBlobStorageProxyID(0), Proxy);
            NSharedCache::TSharedCacheConfig config;
            config.SetMemoryLimit(16_MB);
            Cache = Runtime.Register(NSharedCache::CreateSharedPageCache(config, Runtime.GetDynamicCounters()));
            Runtime.RegisterService(NSharedCache::MakeSharedPageCacheId(), Cache);
            Runtime.Send(new NActors::IEventHandle(Cache, First, new NSharedCache::TEvCacheReadyRequest), 0, true);
            Runtime.GrabEdgeEvent<NSharedCache::TEvCacheReady>(First, TDuration::Seconds(5));
            PreviousFilter = Runtime.SetEventFilter(
                [this](NActors::TTestActorRuntimeBase& runtime, TAutoPtr<NActors::IEventHandle>& event) {
                    switch (event->GetTypeRewrite()) {
                        case NSharedCache::TEvCollectionRequest::EventType:
                            ++Requests;
                            break;
                        case TColdAccess::TEvFailed::EventType:
                            ++Failures;
                            break;
                        case TEvBlobStorage::TEvGet::EventType:
                            ReadWorkers.insert(event->Sender);
                            break;
                    }
                    return PreviousFilter(runtime, event);
                });
        }

        ~TColdFixture() {
            Runtime.Send(new NActors::IEventHandle(Cache, First, new NActors::TEvents::TEvPoison), 0, true);
            Drain();
            Runtime.SetEventFilter(std::move(PreviousFilter));
        }

        TIntrusiveConstPtr<TColdPartStore> Cold(const TBundle::TResult& written, bool withOverlay = true) {
            TVector<NPageCollection::TLargeGlobId> descriptors;
            for (const auto& collection : written.PageCollections) {
                const auto* packet =
                    dynamic_cast<const NPageCollection::TPageCollection*>(collection.PageCollection.Get());
                UNIT_ASSERT(packet);
                descriptors.push_back(packet->LargeGlobId);
            }
            return MakeIntrusiveConst<TColdPartStore>(
                std::move(descriptors), TString{}, withOverlay ? written.Overlay : TString{}, TEpoch::Zero());
        }

        void Warm(const TBundle::TResult& written) {
            auto& cache = NSharedCache::TCache::Get();
            for (const auto& collection : written.PageCollections) {
                auto metadata = NTable::NTest::AdmitCollection(cache, collection.PageCollection);
                for (const auto* group : { &collection.StickyPages, &collection.RegularPages }) {
                    for (const auto& data : *group) {
                        auto page = NTable::NTest::AdmitPage(
                            cache, collection.PageCollection, data.Location, TSharedData(data.Data), true);
                        UNIT_ASSERT(page);
                    }
                }
            }
        }

        TActorId Start(
            TActorId owner, const TIntrusiveConstPtr<TColdPartStore>& part, THolder<IPartLoaderEnv> environment = {}) {
            return Runtime.Register(TColdAccess::Create(owner, part, std::move(environment)));
        }

        TEvBlobStorage::TEvGet::TPtr ReadRoot(const TBundle::TResult& written) {
            const auto& label = written.PageCollections.front().PageCollection->Label();
            for (;;) {
                auto read = Runtime.GrabEdgeEvent<TEvBlobStorage::TEvGet>(Proxy, TDuration::Seconds(5));
                for (ui32 index = 0; index < read->Get()->QuerySize; ++index) {
                    if (read->Get()->Queries[index].Id == label) {
                        return read;
                    }
                }
            }
        }

        void NoData(const TEvBlobStorage::TEvGet::TPtr& read) {
            Runtime.Send(new NActors::IEventHandle(read->Sender, Proxy,
                             new TEvBlobStorage::TEvGetResult(NKikimrProto::NODATA, 0, 0)), 0, true);
        }

        void Drain() {
            Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }

        NActors::TTestBasicRuntime Runtime;
        TActorId First;
        TActorId Second;
        TActorId Proxy;
        TActorId Cache;
        NActors::TTestActorRuntimeBase::TEventFilter PreviousFilter;
        THashSet<TActorId> ReadWorkers;
        ui32 Requests = 0;
        ui32 Failures = 0;
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(TColdPartCollectionMetadata) {
    Y_UNIT_TEST(RawMetadataRestoresFragmentsBeforeLoadingPages) {
        struct TRawLoaderEnv : TLoader::TLoaderEnv {
            TRawLoaderEnv(TStringBuf expected, ui32& saves)
                : TLoaderEnv(TVector<TCacheCollectionRef>{})
                , Expected(expected)
                , Saves(saves)
            {}

            bool BeginMetadata(TArrayRef<TPageCollectionComponents> components, const TMetadataLoadContext& context,
                TFunctionRef<bool(ui32)>) override {
                UNIT_ASSERT_VALUES_EQUAL(components.size(), 1);
                context.LoadRaw(0, components[0].LargeGlobId);
                return true;
            }

            EMetadataLoadResult SaveMetadata(
                TArrayRef<TPageCollectionComponents> components, ui32 slot, TSharedData data) override {
                UNIT_ASSERT_VALUES_EQUAL(slot, 0);
                UNIT_ASSERT_VALUES_EQUAL(TStringBuf(data.data(), data.size()), Expected);
                UNIT_ASSERT_VALUES_EQUAL(++Saves, 1);
                components[slot].RawMeta = std::move(data);
                return EMetadataLoadResult::Ready;
            }

            bool MetadataReady() const noexcept override {
                return Saves != 0;
            }

            const TStringBuf Expected;
            ui32& Saves;
        };

        ui32 saves = 0;
        TSharedData raw;
        TColdFixture fixture;
        auto written = WritePart();
        UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 1);
        auto& collection = written.PageCollections.front().PageCollection;
        const auto* packet = CheckedCast<const NPageCollection::TPageCollection*>(collection.Get());
        raw = packet->Meta.Raw;
        UNIT_ASSERT(raw.size() > 1);
        const auto original = packet->Label();
        const TLogoBlobID lead(original.TabletID(), original.Generation(), original.Step(), original.Channel(),
            raw.size() / 2, original.Cookie());
        const NPageCollection::TLargeGlobId descriptor(packet->LargeGlobId.Group, lead, raw.size());
        collection = MakeIntrusiveConst<NPageCollection::TPageCollection>(descriptor, TSharedData(raw));
        fixture.Warm(written);

        const auto child = fixture.Start(
            fixture.First, fixture.Cold(written), MakeHolder<TRawLoaderEnv>(TStringBuf(raw.data(), raw.size()), saves));
        TVector<TLogoBlobID> fragments;
        descriptor.MaterializeTo(fragments);
        UNIT_ASSERT(fragments.size() > 1);
        for (const auto& id : fragments) {
            auto request =
                fixture.Runtime.GrabEdgeEvent<TColdAccess::TEvLoadBlob>(fixture.First, TDuration::Seconds(5));
            UNIT_ASSERT(request->Sender == child);
            UNIT_ASSERT_VALUES_EQUAL(request->Cookie, 0);
            UNIT_ASSERT(request->Get()->BlobId == id);
            UNIT_ASSERT_VALUES_EQUAL(request->Get()->Group, descriptor.Group);
        }

        for (size_t remaining = fragments.size(); remaining > 0; --remaining) {
            const auto& id = fragments[remaining - 1];
            const size_t offset = (remaining - 1) * lead.BlobSize();
            fixture.Runtime.Send(
                new NActors::IEventHandle(child, fixture.First,
                    new TColdAccess::TEvBlobLoaded(id, TString(raw.data() + offset, id.BlobSize())), 0, 0), 0, true);
        }
        auto loaded = fixture.Runtime.GrabEdgeEvent<TColdAccess::TEvLoaded>(fixture.First, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(loaded->Get()->Part->Stat.Rows, 128);
        UNIT_ASSERT_VALUES_EQUAL(saves, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests, 0);
    }

    Y_UNIT_TEST(MissRequestsPreserveMixedStickyHints) {
        TColdFixture fixture;
        TVector<NPageCollection::TGlob> blobs;
        auto written = WritePart({}, &blobs);
        THashMap<TLogoBlobID, TString> bodies;
        for (auto& blob : blobs) {
            UNIT_ASSERT(bodies.emplace(blob.GId.Logo, std::move(blob.Data)).second);
        }
        auto& cache = NSharedCache::TCache::Get();
        TVector<TCacheCollectionRef> metadata;
        for (const auto& collection : written.PageCollections) {
            metadata.push_back(NTable::NTest::AdmitCollection(cache, collection.PageCollection));
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.PayloadUsage(), 0);

        struct TSeen {
            TLogoBlobID Id;
            TPageLocation Location;
            bool Sticky;
        };

        TVector<TSeen> seen;
        ui32 reads = 0;
        NActors::TTestActorRuntimeBase::TEventFilter previous;
        previous = fixture.Runtime.SetEventFilter(
            [&](NActors::TTestActorRuntimeBase& runtime, TAutoPtr<NActors::IEventHandle>& event) {
                if (event->GetTypeRewrite() == NSharedCache::TEvRequest::EventType) {
                    const auto* request = event->Get<NSharedCache::TEvRequest>();
                    UNIT_ASSERT_VALUES_EQUAL(request->Sticky.size(), request->Pages.size());
                    for (ui32 index = 0; index < request->Pages.size(); ++index) {
                        const auto& location = request->Pages[index];
                        const bool required = TLoader::NeedIn(location.Type) || location.Type == EPage::FlatIndex;
                        UNIT_ASSERT_VALUES_EQUAL(request->Sticky[index], required);
                        seen.push_back({ request->PageCollection->Label(), location, required });
                    }
                } else if (event->GetTypeRewrite() == TEvBlobStorage::TEvGet::EventType) {
                    const auto* request = event->Get<TEvBlobStorage::TEvGet>();
                    auto response =
                        MakeHolder<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, request->QuerySize, ui32{ 0 });
                    for (ui32 index = 0; index < request->QuerySize; ++index) {
                        const auto& query = request->Queries[index];
                        const auto* body = bodies.FindPtr(query.Id);
                        UNIT_ASSERT(body && query.Shift <= body->size());
                        const size_t size = query.Size ? query.Size : body->size() - query.Shift;
                        UNIT_ASSERT(size <= body->size() - query.Shift);
                        auto& result = response->Responses[index];
                        result.Id = query.Id;
                        result.Status = NKikimrProto::OK;
                        result.Shift = query.Shift;
                        result.RequestedSize = query.Size;
                        result.Buffer = TRope(TString(body->data() + query.Shift, size));
                    }
                    ++reads;
                    fixture.Runtime.Send(new NActors::IEventHandle(event->Sender, fixture.Proxy, response.Release(), 0,
                                             event->Cookie), 0, true);
                    return true;
                }
                return previous(runtime, event);
            });
        Y_DEFER {
            fixture.Runtime.SetEventFilter(std::move(previous));
        };

        TActorId loader;
        TBlockEvents<NSharedCache::TEvUnregister> unregister(fixture.Runtime, [&](const auto& event) {
            return event->Sender == loader;
        });
        loader = fixture.Start(fixture.First, fixture.Cold(written, false));
        TVector<TCachePageRef> retainedPages;
        {
            auto loaded = fixture.Runtime.GrabEdgeEvent<TColdAccess::TEvLoaded>(fixture.First, TDuration::Seconds(5));
            const auto* part = loaded->Get()->Part.As<TCachePartStore>();
            UNIT_ASSERT(part && part->PageCollections.size() == metadata.size());
            metadata.clear();
            UNIT_ASSERT(reads != 0);
            ui32 required = 0;
            ui32 optional = 0;
            for (const auto& entry : seen) {
                TCacheCollectionRef collection;
                UNIT_ASSERT(cache.Find(entry.Id, collection) == NSharedCache::ECacheResultStatus::Hit);
                TCachePageRef page;
                UNIT_ASSERT(cache.Find(collection.CacheItem(), static_cast<ui64>(entry.Location.Offset), page) ==
                            NSharedCache::ECacheResultStatus::Hit);
                UNIT_ASSERT_VALUES_EQUAL(page.IsSticky(), entry.Sticky);
                required += entry.Sticky;
                optional += !entry.Sticky;
                if (entry.Sticky) {
                    retainedPages.push_back(std::move(page));
                }
            }
            UNIT_ASSERT(required != 0 && optional != 0);
            unregister.Stop().Unblock();
            fixture.Runtime.Send(new NActors::IEventHandle(
                                     fixture.Cache, fixture.First, new NSharedCache::TEvCacheReadyRequest), 0, true);
            fixture.Runtime.GrabEdgeEvent<NSharedCache::TEvCacheReady>(fixture.First, TDuration::Seconds(5));
            for (const auto& page : retainedPages) {
                UNIT_ASSERT(page.IsSticky());
            }
            // The completed part is the remaining real metadata owner
        }
        fixture.Runtime.WaitFor("last part owner releases Sticky policy", [&] {
            for (const auto& page : retainedPages) {
                if (page.IsSticky()) {
                    return false;
                }
            }
            return true;
        }, TDuration::Seconds(5));
        retainedPages.clear();
        fixture.Runtime.Send(
            new NActors::IEventHandle(fixture.Cache, fixture.First, new NMemory::TEvConsumerLimit(0)), 0, true);
        ui64 registered = 0;
        for (ui32 step = 0; step < 400; ++step) {
            fixture.Drain();
            registered = 0;
            cache.ForEachCollection([&](NSharedCache::TCollection&) {
                ++registered;
            });
            if (!cache.PayloadUsage() && !registered) {
                break;
            }
            fixture.Runtime.Send(
                new NActors::IEventHandle(fixture.Cache, fixture.First,
                    new TKikimrEvents::TEvWakeup(ui64(NSharedCache::EWakeupTag::DoGCManual))), 0, true);
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(registered, 0);
    }

    Y_UNIT_TEST(ConcurrentReadersReuseCanonicalMetadataAndOuterCollection) {
        TColdFixture fixture;
        auto written = WritePart({ .Outer = true });
        fixture.Warm(written);
        auto cold = fixture.Cold(written);
        fixture.Start(fixture.First, cold);
        fixture.Start(fixture.Second, cold);
        auto a = fixture.Runtime.GrabEdgeEvent<TColdAccess::TEvLoaded>(fixture.First, TDuration::Seconds(5));
        auto b = fixture.Runtime.GrabEdgeEvent<TColdAccess::TEvLoaded>(fixture.Second, TDuration::Seconds(5));
        const auto* first = a->Get()->Part.As<TCachePartStore>();
        const auto* second = b->Get()->Part.As<TCachePartStore>();
        UNIT_ASSERT(first && second && first->Small && second->Small);
        UNIT_ASSERT(first->PageCollections[0].Get() == second->PageCollections[0].Get());
        const auto* one =
            dynamic_cast<const NPageCollection::TPageCollection*>(first->PageCollections[0]->PageCollection().Get());
        const auto* two =
            dynamic_cast<const NPageCollection::TPageCollection*>(second->PageCollections[0]->PageCollection().Get());
        UNIT_ASSERT(one && two && &one->Meta == &two->Meta);
        UNIT_ASSERT(dynamic_cast<const NPageCollection::TOuterPageCollection*>(
            first->PageCollections.back()->PageCollection().Get()));
        UNIT_ASSERT(first->PageCollections.back().Get() == second->PageCollections.back().Get());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests, 4);
        UNIT_ASSERT(fixture.ReadWorkers.empty());
    }

    Y_UNIT_TEST(MetadataFailureReportsPartFailure) {
        TColdFixture fixture;
        auto written = WritePart();
        fixture.Start(fixture.First, fixture.Cold(written));
        fixture.NoData(fixture.ReadRoot(written));
        auto failed = fixture.Runtime.GrabEdgeEvent<TColdAccess::TEvFailed>(fixture.First, TDuration::Seconds(5));
        UNIT_ASSERT(failed->Get()->Label == written.PageCollections.front().PageCollection->Label());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Failures, 1);
    }

    Y_UNIT_TEST(PoisoningOneReaderPreservesAnotherReadersPendingLoad) {
        TColdFixture fixture;
        auto written = WritePart();
        auto cold = fixture.Cold(written);
        const auto first = fixture.Start(fixture.First, cold);
        fixture.Start(fixture.Second, cold);
        auto read = fixture.ReadRoot(written);
        fixture.Runtime.WaitFor("both metadata readers", [&] {
            return fixture.Requests == 2;
        }, TDuration::Seconds(5));
        fixture.Runtime.Send(new NActors::IEventHandle(first, fixture.First, new NActors::TEvents::TEvPoison), 0, true);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Failures, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        fixture.NoData(read);
        auto failed = fixture.Runtime.GrabEdgeEvent<TColdAccess::TEvFailed>(fixture.Second, TDuration::Seconds(5));
        UNIT_ASSERT(failed->Get()->Label == cold->Label);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Failures, 1);
    }
}

Y_UNIT_TEST_SUITE_F(TPartLoaderReadScopes, NTest::TTableCacheTest) {
    Y_UNIT_TEST(SavedPagesShareReadScopeOperation) {
        const auto written = WritePart();
        const auto pages = DataPages(written);
        auto collection = AdmitReadCollection(written);
        auto env = MakeReadEnv(collection.Acquire());
        UNIT_ASSERT(!env.TryGetSlotPage(0, pages[0]->Location));
        UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 0);
        const auto fetch = env.GetFetch();
        UNIT_ASSERT_VALUES_EQUAL(fetch.Pages.size(), 1);
        auto page = NTable::NTest::AdmitPage(
            TCache::Get(), collection->PageCollection(), pages[0]->Location, TSharedData(pages[0]->Data));
        UNIT_ASSERT(page);
        NSharedCache::TEvResult::TLoaded loaded(pages[0]->Location.Offset, pages[0]->Location.Size, std::move(page));
        env.Save(std::move(loaded));
        env.EnsureNoNeedPages();

        {
            auto scope = env.BeginOpBatch();
            UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
            UNIT_ASSERT(env.TryGetSlotPage(0, pages[0]->Location));
            UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 0);
    }

    Y_UNIT_TEST(NestedScopesReuseOneOperation) {
        const auto written = WritePart();
        const auto pages = DataPages(written);
        auto collection = AdmitReadCollection(written);
        auto page = NTable::NTest::AdmitPage(
            TCache::Get(), collection->PageCollection(), pages[0]->Location, TSharedData(pages[0]->Data));
        UNIT_ASSERT(page);
        auto env = MakeReadEnv(collection.Acquire());
        {
            auto outer = env.BeginOpBatch();
            UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
            {
                auto inner = env.BeginOpBatch();
                UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
                UNIT_ASSERT(env.TryGetSlotPage(0, pages[0]->Location));
                UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
            UNIT_ASSERT(env.TryGetSlotPage(0, pages[0]->Location));
            UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 0);
        UNIT_ASSERT(env.TryGetSlotPage(0, pages[0]->Location));
        UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 0);
    }

    Y_UNIT_TEST(SavedPageVisitsRenewOperationWithoutEndingReadScope) {
        const auto written = WritePart();
        const auto pages = DataPages(written);
        auto collection = AdmitReadCollection(written);
        auto page = NTable::NTest::AdmitPage(
            TCache::Get(), collection->PageCollection(), pages[0]->Location, TSharedData(pages[0]->Data));
        UNIT_ASSERT(page);
        auto env = MakeReadEnv(collection.Acquire());
        {
            auto scope = env.BeginOpBatch();
            for (ui32 visit = 0; visit < NSharedCache::CacheOperationBatchSize; ++visit) {
                UNIT_ASSERT(env.TryGetSlotPage(0, pages[0]->Location));
                UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
            }
            {
                auto inner = env.BeginOpBatch();
                UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
                UNIT_ASSERT(env.TryGetSlotPage(0, pages[0]->Location));
            }
            UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
            UNIT_ASSERT(!env.TryGetSlotPage(0, pages[1]->Location));
            UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 0);
        const auto fetch = env.GetFetch();
        UNIT_ASSERT_VALUES_EQUAL(fetch.Pages.size(), 1);
        UNIT_ASSERT(fetch.Pages.front() == pages[1]->Location);
    }
}

Y_UNIT_TEST_SUITE_F(TPartLoaderMetadata, NTest::TTableCacheTest) {
    Y_UNIT_TEST(WrittenPageCacheKeepsOwnersAndTransfersBuffers) {
        using namespace NTabletFlatExecutor;

        auto written = WritePart({ .Outer = true });
        UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 2);
        auto& main = written.PageCollections.front();
        auto& outer = written.PageCollections.back();
        const auto it = std::find_if(main.StickyPages.begin(), main.StickyPages.end(), [](const auto& page) {
            return page.Location.Type == EPage::DataPage;
        });
        UNIT_ASSERT(it != main.StickyPages.end());
        main.RegularPages.push_back(std::move(*it));
        main.StickyPages.erase(it);
        UNIT_ASSERT(!main.StickyPages.empty());
        outer.StickyPages.clear();
        outer.RegularPages.clear();

        const auto stickyLocation = main.StickyPages.front().Location;
        const auto regularLocation = main.RegularPages.back().Location;
        const auto* stickyData = main.StickyPages.front().Data.data();
        const auto* regularData = main.RegularPages.back().Data.data();

        TVector<TPageLocation> locations;
        for (const auto& page : main.StickyPages) {
            locations.push_back(page.Location);
        }
        for (const auto& page : main.RegularPages) {
            locations.push_back(page.Location);
        }
        TVector<THolder<NActors::IEventBase>> events;
        auto environment =
            GetWrittenPageCache().AddWrittenPages(written.PageCollections, [&](THolder<NActors::IEventBase> event) {
                UNIT_ASSERT_VALUES_EQUAL(ActiveLoaderOperations(), 0);
                events.push_back(std::move(event));
            });
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(environment->GetCollectionCount(), 2);

        const auto& mainEvent = *CheckedCast<NSharedCache::TEvSaveCompactedPages*>(events[0].Get());
        const auto& outerEvent = *CheckedCast<NSharedCache::TEvSaveCompactedPages*>(events[1].Get());
        UNIT_ASSERT(mainEvent.PageCollection.Get() == main.PageCollection.Get());
        UNIT_ASSERT(outerEvent.PageCollection.Get() == outer.PageCollection.Get());
        UNIT_ASSERT(mainEvent.Pages == locations);
        UNIT_ASSERT(outerEvent.Pages.empty());
        const auto* mainOwner = mainEvent.Collection().Get();
        const auto* outerOwner = outerEvent.Collection().Get();
        events.clear();

        auto& cache = NSharedCache::TCache::Get();
        TCacheCollectionRef mainRef;
        TCacheCollectionRef outerRef;
        UNIT_ASSERT(
            cache.Find(environment->GetCollection(0)->Label(), mainRef) == NSharedCache::ECacheResultStatus::Hit);
        UNIT_ASSERT(
            cache.Find(environment->GetCollection(1)->Label(), outerRef) == NSharedCache::ECacheResultStatus::Hit);
        UNIT_ASSERT(mainRef.Get() == mainOwner);
        UNIT_ASSERT(outerRef.Get() == outerOwner);

        TCachePageRef sticky;
        TCachePageRef regular;
        UNIT_ASSERT(cache.Find(mainRef.CacheItem(), static_cast<ui64>(stickyLocation.Offset), sticky) ==
                    NSharedCache::ECacheResultStatus::Hit);
        UNIT_ASSERT(cache.Find(mainRef.CacheItem(), static_cast<ui64>(regularLocation.Offset), regular) ==
                    NSharedCache::ECacheResultStatus::Hit);
        UNIT_ASSERT(sticky.GetData().data() == stickyData && sticky.IsSticky());
        UNIT_ASSERT(regular.GetData().data() == regularData && !regular.IsSticky());
    }

    Y_UNIT_TEST(ForwardEnvironmentPreservesReadAheadAndReset) {
        using NTabletFlatExecutor::NBlockIO::EPriority;

        const auto written = WritePart();
        TLoader initial(Components(written, true));
        const auto part = Finish(initial, written);
        NTest::TLayoutCook layout;
        layout.Col(0, 1, NScheme::NTypeIds::Uint64).Col(0, 2, NScheme::NTypeIds::String).Key({ 1 });
        TSubset subset(TEpoch::Max(), layout.RowScheme(), TVector<TPartView>{ part });
        NFwd::TConf conf{ .AheadHi = 512, .AheadLo = 256 };
        auto environment = NFwd::MakeEnv(conf, subset);
        auto index = CreateIndexIter(part.Part.Get(), environment.Get(), {});
        UNIT_ASSERT(index->Seek(0) == EReady::Page);

        auto fetch = environment->GetFetch();
        UNIT_ASSERT(fetch);
        const auto* locations = fetch.Pages.data();
        const auto* collection = fetch.PageCollection.Get();
        const auto cookie = fetch.Cookie;
        auto request = environment->MakeRequest(EPriority::Bulk, std::move(fetch));
        const auto& typed = *CheckedCast<NSharedCache::TEvRequest*>(request.Get());
        UNIT_ASSERT(typed.PageCollection.Get() == collection);
        UNIT_ASSERT(typed.Pages.data() == locations);
        UNIT_ASSERT_VALUES_EQUAL(typed.Cookie, cookie);
        UNIT_ASSERT(typed.Priority == EPriority::Bulk);
        UNIT_ASSERT(typed.ReadAhead);
        UNIT_ASSERT(typed.Sticky.empty());

        NSharedCache::TEvResult stale(typed.PageCollection, NKikimrProto::OK, typed.Cookie);
        stale.Pages = LoadWrittenPages(written, typed.PageCollection, typed.Pages);
        UNIT_ASSERT(!environment->MayProgress());
        index.Reset();
        UNIT_ASSERT(environment->Reset() == environment.Get());
        environment->Save(stale);
        UNIT_ASSERT(stale.Pages.empty());
        UNIT_ASSERT(environment->MayProgress());

        auto savePending = [&] {
            while (auto pending = environment->GetFetch()) {
                auto event = environment->MakeRequest(EPriority::Bulk, std::move(pending));
                const auto& pendingRequest = *CheckedCast<NSharedCache::TEvRequest*>(event.Get());
                NSharedCache::TEvResult reply(pendingRequest.PageCollection, NKikimrProto::OK, pendingRequest.Cookie);
                reply.Pages = LoadWrittenPages(written, pendingRequest.PageCollection, pendingRequest.Pages);
                environment->Save(reply);
            }
        };
        index = CreateIndexIter(part.Part.Get(), environment.Get(), {});
        TPageLocation dataLocation;
        const TSharedData* data = nullptr;
        for (ui32 attempt = 0; attempt < 32 && !data; ++attempt) {
            if (index->Seek(0) == EReady::Data) {
                dataLocation = index->GetLocation();
                data = environment->TryGetPage(part.Part.Get(), dataLocation, {});
            }
            if (!data) {
                savePending();
            }
        }
        UNIT_ASSERT(data);
        UNIT_ASSERT_VALUES_EQUAL(data->size(), dataLocation.Size);
        UNIT_ASSERT(environment->MayProgress());

        auto loader = environment->MakeLoaderEnvironment();
        const auto* protocol = &environment->Protocol();
        index.Reset();
        environment.Reset();
        UNIT_ASSERT(&loader->Protocol() == protocol);
        UNIT_ASSERT(protocol->EventKind(NSharedCache::EvResult) == NTabletFlatExecutor::EPageCacheEvent::Result);
        auto loaderRequest = loader->MakeRequest(EPriority::Fast,
            TLoaderFetch{ .PageCollection = typed.PageCollection, .Pages = { dataLocation }, .Sticky = { true } }, 91);
        const auto& typedLoader = *CheckedCast<NSharedCache::TEvRequest*>(loaderRequest.Get());
        UNIT_ASSERT(!typedLoader.ReadAhead);
        UNIT_ASSERT(typedLoader.Sticky == TVector<bool>{ true });
    }

    Y_UNIT_TEST(AsyncRootResolvesRawTailWithoutParsingItEarly) {
        for (bool outer : { false, true }) {
            for (bool cachedTail : { false, true }) {
                const auto written =
                    WritePart({ .Outer = outer, .Grouped = !outer, .Step = 1u + ui32(outer) + 2u * ui32(cachedTail) });
                auto components = Components(written);
                const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(
                    written.PageCollections.back().PageCollection.Get());
                UNIT_ASSERT(packet);
                components.PageCollectionComponents.back().RawMeta = packet->Meta.Raw;
                TCacheCollectionRef cached;
                if (cachedTail) {
                    cached = AdmitCollection(written, 1);
                }
                TLoader loader(std::move(components));
                TMetadataHost host{ .System = &CacheContext().ActorSystem() };
                UNIT_ASSERT(loader.BeginMetadata(host.Context()));
                UNIT_ASSERT_VALUES_EQUAL(host.Requests.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(host.Requests.at(0)->WorkingSet->CollectionCount(), 2);
                UNIT_ASSERT(!loader.MetadataReady());
                UNIT_ASSERT(!loader.BeginMetadata(host.Context()));
                UNIT_ASSERT(loader.SaveMetadata(1, TSharedData(packet->Meta.Raw)) == EMetadataLoadResult::Ignored);
                UNIT_ASSERT(!loader.MetadataReady());
                UNIT_ASSERT(host.Reply(loader, 0, AdmitCollection(written, 0)) == EMetadataLoadResult::Ready);
                UNIT_ASSERT_VALUES_EQUAL(loader.HasOuterCollection(), outer);
                if (!cachedTail) {
                    TCacheCollectionRef absent;
                    UNIT_ASSERT(TCache::Get().Find(packet->Label(), absent) == NSharedCache::ECacheResultStatus::Miss);
                }
                const auto result = Finish(loader, written);
                UNIT_ASSERT_VALUES_EQUAL(bool(result.Part->Small), outer);
                UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
            }
        }
    }

    Y_UNIT_TEST(PrebuiltOwnersSupplyMissingDescriptors) {
        const auto written = WritePart({ .Outer = true });
        TVector<TCacheCollectionRef> collections;
        for (ui32 slot = 0; slot < written.PageCollections.size(); ++slot) {
            collections.push_back(AdmitCollection(written, slot));
        }
        TPartComponents components{ .PageCollectionComponents = TVector<TPageCollectionComponents>(collections.size()),
            .Opaque = written.Overlay,
            .Epoch = TEpoch::Zero() };
        TLoader loader(std::move(components), std::move(collections));
        TMetadataHost host{ .System = &CacheContext().ActorSystem() };
        UNIT_ASSERT(loader.BeginMetadata(host.Context()));
        UNIT_ASSERT(loader.MetadataReady());
        UNIT_ASSERT(host.Requests.empty());
        UNIT_ASSERT(loader.HasOuterCollection());
        const auto result = Finish(loader, written);
        UNIT_ASSERT(result.Part->Small);
    }

    Y_UNIT_TEST(RestartReacquiresCanonicalCollectionsWithoutSnapshot) {
        using namespace NTabletFlatExecutor;

        const auto written = WritePart({ .External = true });
        TLoader initial(Components(written, true));
        const auto part = Finish(initial, written);
        const auto* initialStore = part.As<TCachePartStore>();
        UNIT_ASSERT(initialStore && initialStore->Pseudo);

        auto backend = MakeLFExecutorPageCache();
        TExecutorCaches caches;
        backend->CaptureForRestart(caches.PageCollections);
        UNIT_ASSERT(!caches.PageCollections);

        THolder<IPageCacheLoadedCollections> loaded;
        backend->CaptureForRestart(*initialStore, loaded);
        UNIT_ASSERT(!loaded);
        backend->MergeForRestart(caches.PageCollections, loaded.Get());
        UNIT_ASSERT(!caches.PageCollections);

        auto components = Components(written, true);
        TVector<NPageCollection::TLargeGlobId> ids;
        for (const auto& component : components.PageCollectionComponents) {
            ids.push_back(component.LargeGlobId);
        }
        auto environment = backend->MakeLoaderEnvironment(ids, caches.PageCollections.Get());
        UNIT_ASSERT_VALUES_EQUAL(environment->GetCollectionCount(), ids.size());
        for (ui32 slot = 0; slot < ids.size(); ++slot) {
            UNIT_ASSERT(!environment->HasCollection(slot));
        }

        TLoader restarted(std::move(environment), std::move(components));
        TMetadataHost host{ .System = &CacheContext().ActorSystem() };
        UNIT_ASSERT(restarted.BeginMetadata(host.Context()));
        UNIT_ASSERT(restarted.MetadataReady());
        UNIT_ASSERT(host.Requests.empty());
        const auto restartedPart = Finish(restarted, written);
        const auto* restartedStore = restartedPart.As<TCachePartStore>();
        UNIT_ASSERT(restartedStore);
        UNIT_ASSERT_VALUES_EQUAL(restartedStore->PageCollections.size(), ids.size());
        for (ui32 slot = 0; slot < ids.size(); ++slot) {
            UNIT_ASSERT(restartedStore->PageCollections[slot].Get() == initialStore->PageCollections[slot].Get());
        }
    }

    Y_UNIT_TEST(PrebuiltRootGivesAsyncTailItsRoleBeforeSending) {
        const auto written = WritePart({ .Outer = true });
        TVector<TCacheCollectionRef> collections(written.PageCollections.size());
        collections[0] = AdmitCollection(written, 0);
        TLoader loader(Components(written), std::move(collections));
        TMetadataHost host{ .System = &CacheContext().ActorSystem() };
        UNIT_ASSERT(loader.BeginMetadata(host.Context()));
        UNIT_ASSERT_VALUES_EQUAL(host.Requests.size(), 1);
        UNIT_ASSERT(host.Requests.at(1)->Kind() == NSharedCache::ECollectionLoadKind::Outer);
        UNIT_ASSERT_VALUES_EQUAL(host.Requests.at(1)->WorkingSet->CollectionCount(), 2);
        UNIT_ASSERT(host.Updates.empty());
        UNIT_ASSERT(host.Reply(loader, 1, AdmitCollection(written, 1)) == EMetadataLoadResult::Ready);
        const auto result = Finish(loader, written);
        UNIT_ASSERT(result.Part->Small);
    }

    Y_UNIT_TEST(LateRepliesUseTheFrozenIndexSelectionAndRequestIdentity) {
        auto& flags = AppData()->FeatureFlags;
        flags.SetEnableLocalDBBtreeIndex(true);
        flags.SetEnableLocalDBFlatIndex(true);
        flags.SetEnableLocalDBBtreeIndexV2(true);
        const auto written = WritePart({ .Grouped = true, .Dual = true,
            .Layout = { .SeparateDescriptorCollection = true, .KeyIndexInDescriptor = false,
                .GroupIndexesInDescriptor = false } });

        struct TTrackingLoaderEnv : TLoader::TLoaderEnv {
            explicit TTrackingLoaderEnv(TVector<ui32>& publishedSlots)
                : TLoaderEnv(TVector<TCacheCollectionRef>{})
                , PublishedSlots(publishedSlots)
            {}

            void SetSkipBTreeIndexV1Shadow(ui32 slot, bool skip) override {
                PublishedSlots.push_back(slot);
                TLoaderEnv::SetSkipBTreeIndexV1Shadow(slot, skip);
            }

            TVector<ui32>& PublishedSlots;
        };

        TVector<ui32> publishedSlots;
        TLoader loader(MakeHolder<TTrackingLoaderEnv>(publishedSlots), Components(written));
        TMetadataHost host{ .System = &CacheContext().ActorSystem() };
        UNIT_ASSERT(loader.BeginMetadata(host.Context()));
        UNIT_ASSERT_VALUES_EQUAL(host.Requests.size(), 3);
        UNIT_ASSERT(host.Requests.at(2)->Kind() == NSharedCache::ECollectionLoadKind::Unknown);

        auto wrong = MakeIntrusive<NSharedCache::TCollectionRequest>(host.Requests.at(2)->Descriptor);
        NSharedCache::TEvCollectionResult stale(wrong, NKikimrProto::OK, AdmitCollection(written, 2));
        NTabletFlatExecutor::TPageCacheMetadataResult metadata{ .Request = wrong.Get() };
        UNIT_ASSERT(loader.AcceptMetadata(2, metadata, stale, host.Context()) == EMetadataLoadResult::Ignored);
        UNIT_ASSERT(stale.Collection);

        auto index = AdmitCollection(written, 1);
        UNIT_ASSERT(host.Reply(loader, 1, index.Acquire()) == EMetadataLoadResult::Pending);
        UNIT_ASSERT(!index->PageCollection()->SkipBTreeIndexV1Shadow());
        UNIT_ASSERT(publishedSlots.empty());
        UNIT_ASSERT(host.Reply(loader, 0, AdmitCollection(written, 0)) == EMetadataLoadResult::Pending);
        UNIT_ASSERT(index->PageCollection()->SkipBTreeIndexV1Shadow());
        UNIT_ASSERT(publishedSlots == TVector<ui32>({ 0, 1 }));
        UNIT_ASSERT(host.Requests.at(2)->Kind() == NSharedCache::ECollectionLoadKind::Regular);
        UNIT_ASSERT_VALUES_EQUAL(host.Updates.size(), 1);
        flags.SetEnableLocalDBBtreeIndexV2(false);
        auto last = AdmitCollection(written, 2);
        UNIT_ASSERT(host.Reply(loader, 2, last.Acquire()) == EMetadataLoadResult::Ready);
        UNIT_ASSERT(last->PageCollection()->SkipBTreeIndexV1Shadow());
        UNIT_ASSERT(host.Reply(loader, 2, last.Acquire()) == EMetadataLoadResult::Ignored);
        UNIT_ASSERT(publishedSlots == TVector<ui32>({ 0, 1, 2 }));
        const auto result = Finish(loader, written);
        for (const auto& tree : result.Part->IndexPages.BTreeGroups) {
            UNIT_ASSERT(tree.HasRootV2() && !tree.HasRootV1());
        }
    }

    Y_UNIT_TEST(SharedWorkingSetAllowsDifferentRequestersAndIncludesPseudo) {
        auto backend = NTabletFlatExecutor::MakeLFExecutorPageCache();
        auto metadataWorkingSet = backend->MakeMetadataWorkingSet();
        const auto written = WritePart({ .External = true });
        const auto next = WritePart({ .Step = 2 });
        TLoader first(Components(written));
        TLoader second(Components(next));
        TMetadataHost firstHost{ .System = &CacheContext().ActorSystem(),
            .MetadataWorkingSet = metadataWorkingSet.Get() };
        TMetadataHost secondHost{ .System = &CacheContext().ActorSystem(), .ReplyTo = TActorId(1, "metadata2"),
            .MetadataWorkingSet = metadataWorkingSet.Get() };
        UNIT_ASSERT(!backend->BeginPartMetadata(first, firstHost.Context(), false));
        UNIT_ASSERT(firstHost.Requests.empty());
        UNIT_ASSERT(backend->BeginPartMetadata(first, firstHost.Context(), true));
        UNIT_ASSERT(backend->BeginPartMetadata(second, secondHost.Context(), true));
        const auto& workingSet = firstHost.Requests.at(0)->WorkingSet;
        UNIT_ASSERT(workingSet == secondHost.Requests.at(0)->WorkingSet);
        UNIT_ASSERT_VALUES_EQUAL(workingSet->CollectionCount(), 2);
        metadataWorkingSet->AddCollections(Components(written).PageCollectionComponents);
        UNIT_ASSERT_VALUES_EQUAL(workingSet->CollectionCount(), 2);
        UNIT_ASSERT(firstHost.Reply(first, 0, AdmitCollection(written, 0)) == EMetadataLoadResult::Ready);
        auto result = Finish(first, written);
        UNIT_ASSERT(result.Part->Blobs);
        UNIT_ASSERT_VALUES_EQUAL(workingSet->CollectionCount(), 3);
        result = {};
        first.CancelMetadata();
        second.CancelMetadata();
        UNIT_ASSERT(!secondHost.Requests.at(0)->IsActive());
        second.CancelMetadata();
        UNIT_ASSERT(secondHost.Reply(second, 0, AdmitCollection(next, 0)) == EMetadataLoadResult::Ignored);
    }

    Y_UNIT_TEST(FailureAndCancellationCannotCompleteMetadata) {
        const auto written = WritePart({ .Grouped = true });
        TLoader loader(Components(written));
        TMetadataHost host{ .System = &CacheContext().ActorSystem() };
        UNIT_ASSERT(loader.BeginMetadata(host.Context()));
        UNIT_ASSERT(host.Reply(loader, 0, {}, NKikimrProto::NODATA) == EMetadataLoadResult::Error);
        UNIT_ASSERT(!loader.MetadataReady());
        loader.CancelMetadata();
        UNIT_ASSERT(!host.Requests.at(1)->IsActive());
        UNIT_ASSERT(!loader.BeginMetadata(host.Context()));

        TLoader declined(Components(written));
        TMetadataHost rejected{ .System = &CacheContext().ActorSystem(), .SendAllowed = false };
        UNIT_ASSERT(declined.BeginMetadata(rejected.Context()));
        UNIT_ASSERT(!declined.MetadataReady());
        UNIT_ASSERT_VALUES_EQUAL(rejected.Requests.size(), 1);
        UNIT_ASSERT(!rejected.Requests.at(0)->IsActive());

        TLoader offline(Components(written));
        TMetadataHost noSystem{ .System = nullptr };
        UNIT_ASSERT_EXCEPTION(offline.BeginMetadata(noSystem.Context()), yexception);
        UNIT_ASSERT(noSystem.Requests.empty());

        TLoader partial(Components(written));
        TMetadataHost pending{ .System = &CacheContext().ActorSystem() };
        UNIT_ASSERT(partial.BeginMetadata(pending.Context()));
        UNIT_ASSERT(pending.Reply(partial, 0, AdmitCollection(written, 0)) == EMetadataLoadResult::Pending);
        partial.CancelMetadata();
        UNIT_ASSERT(!partial.MetadataReady());
        UNIT_ASSERT(!pending.Requests.at(1)->IsActive());
        UNIT_ASSERT(pending.Reply(partial, 1, AdmitCollection(written, 1)) == EMetadataLoadResult::Ignored);
    }
}

Y_UNIT_TEST_SUITE(TPrivatePartLoader) {
    Y_UNIT_TEST(ForwardTraceKeepsBytesAndReleasesLegacyHandle) {
        using namespace NTabletFlatExecutor;
        using NSharedCache::NLegacy::TEvRequest;
        using NSharedCache::NLegacy::TEvResult;

        const auto written = WritePart();
        const auto& source = written.PageCollections.front();
        auto collection = MakeIntrusive<TPrivatePageCache::TPageCollection>(source.PageCollection);
        for (const auto* group : { &source.StickyPages, &source.RegularPages }) {
            for (const auto& page : *group) {
                collection->AddPage(page.Location.Offset, page.Location.Size,
                    NSharedCache::TSharedPageRef::MakePrivate(TSharedData(page.Data)));
            }
        }
        TLoader initial(MakePrivateLoaderEnvironment({ collection }), Components(written));
        UNIT_ASSERT(!initial.Run({}));
        const auto part = initial.Result();
        NTest::TLayoutCook layout;
        layout.Col(0, 1, NScheme::NTypeIds::Uint64).Col(0, 2, NScheme::NTypeIds::String).Key({ 1 });
        TSubset subset(TEpoch::Max(), layout.RowScheme(), TVector<TPartView>{ part });
        auto environment = NFwd::MakePrivateEnv(NFwd::TConf{ .AheadHi = 512, .AheadLo = 256 }, subset);
        UNIT_ASSERT(!environment->MetadataWorkingSet());
        const auto* protocol = &environment->Protocol();
        UNIT_ASSERT(protocol == &GetPrivatePageCacheProtocol());

        const auto* firstSource = DataPages(written).front();
        TIntrusivePtr<NSharedCache::TSharedPageHandle> firstHandle;
        const auto makeReply = [&](NFwd::TFetch fetch) {
            auto request = environment->MakeRequest(NBlockIO::EPriority::Bulk, std::move(fetch));
            const auto& typed = *CheckedCast<TEvRequest*>(request.Get());
            UNIT_ASSERT(typed.Priority == NBlockIO::EPriority::Bulk);
            auto reply = MakeHolder<TEvResult>(typed.PageCollection, NKikimrProto::OK, typed.Cookie);
            for (const auto& location : typed.Pages) {
                const auto& writtenPage = FindWrittenPage(written, typed.PageCollection->Label(), location);
                auto page = NSharedCache::TSharedPageRef::MakePrivate(TSharedData(writtenPage.Data));
                page.SetType(location.Type);
                if (location == firstSource->Location) {
                    firstHandle = page.GetHandle();
                }
                reply->Pages.emplace_back(location.Offset, location.Size, std::move(page));
            }
            return reply;
        };
        auto index = CreateIndexIter(part.Part.Get(), environment.Get(), {});
        UNIT_ASSERT(index->Seek(0) == EReady::Page);
        auto staleFetch = environment->GetFetch();
        UNIT_ASSERT(staleFetch);
        auto stale = makeReply(std::move(staleFetch));
        index.Reset();
        UNIT_ASSERT(environment->Reset() == environment.Get());
        environment->Save(*stale);
        UNIT_ASSERT(stale->Pages.empty() && environment->MayProgress());
        firstHandle.Reset();

        const auto savePending = [&] {
            while (auto fetch = environment->GetFetch()) {
                environment->Save(*makeReply(std::move(fetch)));
            }
        };
        index = CreateIndexIter(part.Part.Get(), environment.Get(), {});
        const auto seek = [&](TRowId row) {
            for (ui32 attempt = 0; attempt < 32; ++attempt) {
                if (index->Seek(row) == EReady::Data) {
                    return index->GetLocation();
                }
                savePending();
            }
            UNIT_FAIL("The private forward cache did not load the index");
            return TPageLocation::Max();
        };
        const auto first = seek(0);
        UNIT_ASSERT(first == firstSource->Location);
        if (!environment->TryGetPage(part.Part.Get(), first, {})) {
            savePending();
        }
        const auto* firstBody = environment->TryGetPage(part.Part.Get(), first, {});
        UNIT_ASSERT(firstBody && firstBody->data() == firstSource->Data.data());
        UNIT_ASSERT(firstHandle && firstHandle->UseCount() == 2);

        const auto second = seek(index->GetNextRowId());
        environment->TryGetPage(part.Part.Get(), second, {});
        UNIT_ASSERT_VALUES_EQUAL(firstHandle->UseCount(), 1);
        const auto* traced = environment->TryGetPage(part.Part.Get(), first, {});
        UNIT_ASSERT(traced && traced->data() == firstSource->Data.data());

        auto loader = environment->MakeLoaderEnvironment();
        index.Reset();
        environment.Reset();
        UNIT_ASSERT(&loader->Protocol() == protocol);
    }

    Y_UNIT_TEST(WrittenPagesTransferBodiesToLegacyActorAndLoader) {
        using namespace NTabletFlatExecutor;

        auto written = WritePart({ .Outer = true });
        UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 2);
        auto& collection = written.PageCollections.front();
        auto regular = std::find_if(collection.StickyPages.begin(), collection.StickyPages.end(), [](const auto& page) {
            return page.Location.Type == EPage::DataPage;
        });
        UNIT_ASSERT(regular != collection.StickyPages.end());
        auto sticky = std::find_if(collection.StickyPages.begin(), collection.StickyPages.end(), [](const auto& page) {
            return TLoader::NeedIn(page.Location.Type);
        });
        UNIT_ASSERT(sticky != collection.StickyPages.end());

        TVector<NWriter::TWrittenPageCollection> input(2);
        input[1].PageCollection = written.PageCollections[1].PageCollection;
        input.front().PageCollection = collection.PageCollection;
        input.front().RegularPages.push_back(std::move(*regular));
        input.front().StickyPages.push_back(std::move(*sticky));
        const auto regularLocation = input.front().RegularPages.front().Location;
        const auto stickyLocation = input.front().StickyPages.front().Location;
        const auto* regularBytes = input.front().RegularPages.front().Data.data();
        const auto* stickyBytes = input.front().StickyPages.front().Data.data();
        auto pages = MakeIntrusive<NSharedCache::TSharedCachePages>();
        TVector<THolder<NActors::IEventBase>> events;
        auto environment =
            MakePrivateWrittenPageCache(*pages).AddWrittenPages(input, [&](THolder<NActors::IEventBase> message) {
                events.push_back(std::move(message));
            });
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(environment->GetCollectionCount(), 2);
        const auto& saved = *CheckedCast<NSharedCache::NLegacy::TEvSaveCompactedPages*>(events[0].Get());
        const auto& outer = *CheckedCast<NSharedCache::NLegacy::TEvSaveCompactedPages*>(events[1].Get());
        UNIT_ASSERT(outer.PageCollection == input[1].PageCollection);
        UNIT_ASSERT(outer.Pages.empty());
        UNIT_ASSERT(environment->GetCollection(1) == input[1].PageCollection);
        UNIT_ASSERT(saved.PageCollection == collection.PageCollection);
        UNIT_ASSERT_VALUES_EQUAL(saved.Pages.size(), 2);
        UNIT_ASSERT(
            saved.Pages[0]->Offset == stickyLocation.Offset && saved.Pages[1]->Offset == regularLocation.Offset);
        UNIT_ASSERT_VALUES_EQUAL(saved.Pages[0]->UseCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(saved.Pages[1]->UseCount(), 1);
        UNIT_ASSERT(!input.front().RegularPages.front().Data && !input.front().StickyPages.front().Data);
        events.clear();

        environment->Initialize(TPartSlots(1));
        const auto* cachedRegular = environment->TryGetSlotPage(0, regularLocation);
        const auto* cachedSticky = environment->TryGetSlotPage(0, stickyLocation);
        UNIT_ASSERT(cachedRegular && cachedRegular->data() == regularBytes);
        UNIT_ASSERT(cachedSticky && cachedSticky->data() == stickyBytes);
    }

    Y_UNIT_TEST(RawMetadataAndPrebuiltOwnersLoadThroughLegacyEvents) {
        using namespace NTabletFlatExecutor;
        using NSharedCache::NLegacy::TEvRequest;
        using NSharedCache::NLegacy::TEvResult;

        const auto written = WritePart({ .Outer = true, .Grouped = true });
        auto components = Components(written, true);
        UNIT_ASSERT_VALUES_EQUAL(components.PageCollectionComponents.size(), 3);
        const auto outerMeta = std::move(components.PageCollectionComponents[2].RawMeta);
        components.PageCollectionComponents[1].RawMeta = {};
        auto group = MakeIntrusive<TPrivatePageCache::TPageCollection>(written.PageCollections[1].PageCollection);
        auto environment = MakePrivateLoaderEnvironment({ {}, group });
        auto* pages = environment.Get();
        TLoader loader(std::move(environment), std::move(components));
        TRawMetadataIO io;
        UNIT_ASSERT(loader.BeginMetadata(io.Context()));
        UNIT_ASSERT(!loader.BeginMetadata(io.Context()));
        UNIT_ASSERT_VALUES_EQUAL(io.Requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(io.Requests.front().first, 2);
        UNIT_ASSERT_VALUES_EQUAL(io.Requests.front().second, written.PageCollections.back().PageCollection->Label());
        UNIT_ASSERT(!loader.MetadataReady());
        UNIT_ASSERT(loader.SaveMetadata(0, TSharedData(outerMeta)) == EMetadataLoadResult::Ignored);
        UNIT_ASSERT(loader.SaveMetadata(1, TSharedData(outerMeta)) == EMetadataLoadResult::Ignored);
        UNIT_ASSERT(loader.SaveMetadata(2, {}) == EMetadataLoadResult::Error);
        UNIT_ASSERT(!loader.MetadataReady());
        UNIT_ASSERT(loader.SaveMetadata(2, TSharedData(outerMeta)) == EMetadataLoadResult::Ready);
        UNIT_ASSERT(loader.MetadataReady());
        UNIT_ASSERT(loader.SaveMetadata(2, TSharedData(outerMeta)) == EMetadataLoadResult::Ignored);

        while (auto fetch = loader.Run({ .PreloadIndex = false, .PreloadData = true })) {
            auto event = loader.MakeRequest(NBlockIO::EPriority::Fast, std::move(fetch), 17);
            UNIT_ASSERT_VALUES_EQUAL(event->Type(), TEvRequest::EventType);
            const auto* request = CheckedCast<TEvRequest*>(event.Get());
            UNIT_ASSERT_VALUES_EQUAL(request->Cookie, 17);
            UNIT_ASSERT(std::is_sorted(request->Pages.begin(), request->Pages.end()));
            TEvResult reply(request->PageCollection, NKikimrProto::OK, request->Cookie);
            for (const auto& location : request->Pages) {
                const auto& source = FindWrittenPage(written, request->PageCollection->Label(), location);
                auto page = NSharedCache::TSharedPageRef::MakePrivate(TSharedData(source.Data));
                page.SetType(location.Type);
                reply.Pages.emplace_back(location.Offset, location.Size, std::move(page));
            }
            loader.Save(reply);
        }

        const auto result = loader.Result();
        const auto* part = result.As<TPrivatePartStore>();
        UNIT_ASSERT(part && part->Small);
        UNIT_ASSERT_VALUES_EQUAL(part->Stat.Rows, 128);
        UNIT_ASSERT(part->PageCollections[1].Get() == group.Get());
        UNIT_ASSERT_VALUES_EQUAL(group.RefCount(), 2);
        UNIT_ASSERT(dynamic_cast<const NPageCollection::TOuterPageCollection*>(part->GetPageCollection(2)));
        const auto descriptorSlot = part->Slots.DescriptorSlot();
        const auto* descriptor = part->GetPageCollection(descriptorSlot);
        const auto scheme = descriptor->GetLocation(descriptor->MetaPages() - 1);
        UNIT_ASSERT(part->PageCollections[descriptorSlot]->IsStickyPage(scheme.Offset));
        const auto& data = *DataPages(written).front();
        const auto dataSlot = part->DataSlot({});
        UNIT_ASSERT(!part->PageCollections[dataSlot]->IsStickyPage(data.Location.Offset));
        const auto* saved = pages->TryGetPage(part, data.Location, {});
        UNIT_ASSERT(saved && saved->data() == data.Data.data());
    }

    Y_UNIT_TEST(CancelledRawMetadataIgnoresLateReplies) {
        const auto written = WritePart({ .Grouped = true });
        auto components = Components(written, true);
        const auto last = std::move(components.PageCollectionComponents.back().RawMeta);
        const auto first = std::move(components.PageCollectionComponents.front().RawMeta);
        TLoader loader(NTabletFlatExecutor::MakePrivateLoaderEnvironment(), std::move(components));
        TRawMetadataIO io;
        UNIT_ASSERT(loader.BeginMetadata(io.Context()));
        UNIT_ASSERT_VALUES_EQUAL(io.Requests.size(), 2);
        UNIT_ASSERT(loader.SaveMetadata(1, TSharedData(last)) == EMetadataLoadResult::Pending);
        UNIT_ASSERT(loader.SaveMetadata(1, TSharedData(last)) == EMetadataLoadResult::Ignored);
        loader.CancelMetadata();
        loader.CancelMetadata();
        UNIT_ASSERT(!loader.MetadataReady());
        UNIT_ASSERT(!loader.BeginMetadata(io.Context()));
        UNIT_ASSERT(loader.SaveMetadata(0, TSharedData(first)) == EMetadataLoadResult::Ignored);
        UNIT_ASSERT(!loader.MetadataReady());
    }

    Y_UNIT_TEST(InvalidOverlayReleasesPrebuiltCollection) {
        using namespace NTabletFlatExecutor;

        const auto written = WritePart();
        const auto& source = written.PageCollections.front();
        auto collection = MakeIntrusive<TPrivatePageCache::TPageCollection>(source.PageCollection);
        for (const auto* group : { &source.StickyPages, &source.RegularPages }) {
            for (const auto& page : *group) {
                collection->AddPage(page.Location.Offset, page.Location.Size,
                    NSharedCache::TSharedPageRef::MakePrivate(TSharedData(page.Data)));
            }
        }

        auto components = Components(written);
        components.Legacy = TString(1, '\x80');
        {
            TLoader loader(MakePrivateLoaderEnvironment({ collection }), std::move(components));
            UNIT_ASSERT_EXCEPTION_CONTAINS(loader.Run({}), NUtil::TTabletError, "Failed to parse part overlay");
        }
        UNIT_ASSERT_VALUES_EQUAL(collection.RefCount(), 1);
    }
}

Y_UNIT_TEST_SUITE_F(TPartLoaderRoot, NTest::TTableCacheTest) {
    Y_UNIT_TEST(EnvironmentBorrowsCollectionsAfterPartCreation) {
        class TEnvironment final : public TLoader::TLoaderEnv {
        public:
            TEnvironment()
                : TLoaderEnv(TVector<TCacheCollectionRef>{})
            {}

            void SetSkipBTreeIndexV1Shadow(ui32 slot, bool skip) override {
                UNIT_ASSERT(!GetCollection(slot)->SkipBTreeIndexV1Shadow());
                UNIT_ASSERT_VALUES_EQUAL(skip, slot == 0);
                TLoaderEnv::SetSkipBTreeIndexV1Shadow(slot, skip);
                ++ShadowUpdates;
            }

            TPartStore* CreatePart(TPart::TParams params, TPart::TStat stat) override {
                auto* part = TLoaderEnv::CreatePart(std::move(params), stat);
                UNIT_ASSERT_VALUES_EQUAL(GetCollectionCount(), part->GetPageCollectionCount());
                const auto* cachePart = CheckedCast<const TCachePartStore*>(part);
                auto cacheOp = TCache::Get().BeginOperation();
                for (ui32 slot = 0; slot < GetCollectionCount(); ++slot) {
                    UNIT_ASSERT(GetCollection(slot).Get() == part->GetPageCollection(slot));
                    const auto item = cachePart->PageCollections[slot].CacheItem();
                    const auto& handle = cacheOp.SpaceOperation().Handles()[item.Index()];
                    const auto state =
                        NSharedCache::THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
                    UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
                }
                ++CreatedParts;
                return part;
            }

            ui32 ShadowUpdates = 0;
            ui32 CreatedParts = 0;
        };

        auto& flags = AppData()->FeatureFlags;
        flags.SetEnableLocalDBBtreeIndex(true);
        flags.SetEnableLocalDBFlatIndex(true);
        flags.SetEnableLocalDBBtreeIndexV2(true);
        const auto written = WritePart({ .Grouped = true, .Dual = true });
        UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 2);
        TPartView result;
        {
            auto env = MakeHolder<TEnvironment>();
            auto* environment = env.Get();
            TLoader loader(std::move(env), Components(written, true));
            result = Finish(loader, written);
            UNIT_ASSERT_VALUES_EQUAL(environment->CreatedParts, 1);
            UNIT_ASSERT_VALUES_EQUAL(environment->ShadowUpdates, written.PageCollections.size());
            const auto* descriptor = result.Part->GetPageCollection(0);
            const auto location = descriptor->GetLocation(descriptor->MetaPages() - 1);
            const auto* saved = environment->TryGetSlotPage(0, location);
            UNIT_ASSERT(saved);
            UNIT_ASSERT_VALUES_EQUAL(saved->size(), location.Size);
        }
        const auto* part = result.As<TPartStore>();
        UNIT_ASSERT_VALUES_EQUAL(part->GetPageCollectionCount(), written.PageCollections.size());
        for (ui32 slot = 0; slot < part->GetPageCollectionCount(); ++slot) {
            UNIT_ASSERT_VALUES_EQUAL(result.Part->GetPageCollection(slot)->SkipBTreeIndexV1Shadow(), slot == 0);
        }
    }

    Y_UNIT_TEST(PrepareRootBeforeOuterCollectionArrives) {
        auto written = WritePart({ .Outer = true });
        UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 2);
        TLoader loader(Components(written));
        UNIT_ASSERT_EXCEPTION(loader.HasOuterCollection(), yexception);
        auto root = AdmitCollection(written, 0);
        loader.SetPageCollection(0, root.Acquire());
        loader.PrepareRoot();
        UNIT_ASSERT(loader.HasOuterCollection());
        UNIT_ASSERT_EXCEPTION(loader.SetPageCollection(0, root.Acquire()), yexception);
        TCacheCollectionRef missing;
        UNIT_ASSERT(NSharedCache::TCache::Get().Find(written.PageCollections[1].PageCollection->Label(), missing) ==
                    NSharedCache::ECacheResultStatus::Miss);
        UNIT_ASSERT_EXCEPTION(loader.Run({ .PreloadIndex = false, .PreloadData = false }), yexception);
        loader.SetPageCollection(1, AdmitCollection(written, 1));
        auto result = Finish(loader, written);
        UNIT_ASSERT(result.Part->Small);
        UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
        UNIT_ASSERT_EXCEPTION(loader.SetPageCollection(0, root.Acquire()), yexception);
    }

    Y_UNIT_TEST(PrepareRootBeforeOrdinaryGroupArrives) {
        auto written = WritePart({ .Grouped = true });
        UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 2);
        TLoader loader(Components(written));
        auto root = AdmitCollection(written, 0);
        loader.SetPageCollection(0, root.Acquire());
        loader.PrepareRoot();
        UNIT_ASSERT(!loader.HasOuterCollection());
        UNIT_ASSERT_EXCEPTION(loader.SetPageCollection(1, root.Acquire()), yexception);
        loader.SetPageCollection(1, AdmitCollection(written, 1));
        auto result = Finish(loader, written);
        UNIT_ASSERT(!result.Part->Small);
        UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
    }

    Y_UNIT_TEST(RepeatedPreparationKeepsTheFirstIndexSelection) {
        auto& flags = AppData()->FeatureFlags;
        flags.SetEnableLocalDBBtreeIndex(true);
        flags.SetEnableLocalDBFlatIndex(true);
        flags.SetEnableLocalDBBtreeIndexV2(true);
        auto written = WritePart({ .Dual = true });
        TLoader loader(Components(written));
        auto root = AdmitCollection(written, 0);
        loader.SetPageCollection(0, root.Acquire());
        loader.PrepareRoot();
        UNIT_ASSERT(root->PageCollection()->SkipBTreeIndexV1Shadow());
        flags.SetEnableLocalDBBtreeIndexV2(false);
        loader.PrepareRoot();
        UNIT_ASSERT(root->PageCollection()->SkipBTreeIndexV1Shadow());
        auto result = Finish(loader, written);
        const auto& index = result.Part->IndexPages.BTreeGroups.at(0);
        UNIT_ASSERT(index.HasRootV2() && !index.HasRootV1());
        UNIT_ASSERT(root->PageCollection()->SkipBTreeIndexV1Shadow());
    }

    Y_UNIT_TEST(LateIndexCollectionsUseThePreparedRootSelection) {
        for (bool rawTail : { false, true }) {
            auto& flags = AppData()->FeatureFlags;
            flags.SetEnableLocalDBBtreeIndex(true);
            flags.SetEnableLocalDBFlatIndex(true);
            flags.SetEnableLocalDBBtreeIndexV2(true);
            auto written = WritePart({ .Grouped = true, .Dual = true, .Step = rawTail ? 2u : 1u,
                .Layout = { .SeparateDescriptorCollection = true, .KeyIndexInDescriptor = false,
                    .GroupIndexesInDescriptor = false } });
            UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 3);
            TLoader loader(Components(written, rawTail));
            auto root = AdmitCollection(written, 0);
            loader.SetPageCollection(0, root.Acquire());
            loader.PrepareRoot();
            UNIT_ASSERT(!root->PageCollection()->SkipBTreeIndexV1Shadow());
            flags.SetEnableLocalDBBtreeIndexV2(false);
            if (!rawTail) {
                for (ui32 slot = 1; slot < written.PageCollections.size(); ++slot) {
                    auto collection = AdmitCollection(written, slot);
                    loader.SetPageCollection(slot, collection.Acquire());
                    UNIT_ASSERT(collection->PageCollection()->SkipBTreeIndexV1Shadow());
                }
            }
            auto result = Finish(loader, written);
            UNIT_ASSERT(!result.Part->GetPageCollection(0)->SkipBTreeIndexV1Shadow());
            for (ui32 slot = 1; slot < written.PageCollections.size(); ++slot) {
                UNIT_ASSERT(result.Part->GetPageCollection(slot)->SkipBTreeIndexV1Shadow());
            }
            for (const auto& index : result.Part->IndexPages.BTreeGroups) {
                UNIT_ASSERT(index.HasRootV2() && !index.HasRootV1());
            }
        }
    }

    Y_UNIT_TEST(PreservesRawAndFullyPrebuiltRunPaths) {
        for (bool prebuilt : { false, true }) {
            auto written = WritePart({ .Step = prebuilt ? 2u : 1u });
            TPartComponents components{ .Epoch = TEpoch::Zero() };
            TVector<TCacheCollectionRef> collections;
            if (prebuilt) {
                components.Opaque = written.Overlay;
                for (ui32 slot = 0; slot < written.PageCollections.size(); ++slot) {
                    collections.push_back(AdmitCollection(written, slot));
                }
            } else {
                components = Components(written, true);
            }
            TLoader loader(std::move(components), std::move(collections));
            auto result = Finish(loader, written);
            UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
        }
    }

    Y_UNIT_TEST(RuntimeSwitchKeepsOneLoaderAcrossMetadataPhase) {
        auto written = WritePart({ .Outer = true });
        auto backend = NTabletFlatExecutor::MakeLFExecutorPageCache();
        NTabletFlatExecutor::TPendingPartSwitch::TNewBundle bundle(
            backend->MakeLoaderEnvironment(), Components(written));
        auto* stage = bundle.GetLoaderStage();
        UNIT_ASSERT(stage);
        UNIT_ASSERT(!bundle.GetResult());
        auto* original = &stage->Loader;
        TMetadataHost host{ .System = &CacheContext().ActorSystem() };
        UNIT_ASSERT(backend->BeginPartMetadata(stage->Loader, host.Context(), true));
        UNIT_ASSERT(host.Reply(stage->Loader, 0, AdmitCollection(written, 0)) == EMetadataLoadResult::Pending);
        UNIT_ASSERT(stage->Loader.HasOuterCollection());
        UNIT_ASSERT(host.Reply(stage->Loader, 1, AdmitCollection(written, 1)) == EMetadataLoadResult::Ready);
        UNIT_ASSERT(&stage->Loader == original);
        auto result = Finish(stage->Loader, written);
        bundle.SetResult(std::move(result));
        UNIT_ASSERT(!bundle.GetLoaderStage());
        const auto* partView = bundle.GetResult();
        UNIT_ASSERT(partView && *partView);
        UNIT_ASSERT_VALUES_EQUAL(partView->Part->Stat.Rows, 128);
    }

    Y_UNIT_TEST(RuntimeSwitchPreservesMixedRawMetadataInputs) {
        for (bool rootRaw : { false, true }) {
            auto written = WritePart({ .Outer = true, .Step = rootRaw ? 2u : 1u });
            auto components = Components(written, true);
            components.PageCollectionComponents[rootRaw ? 1 : 0].RawMeta = {};
            auto backend = NTabletFlatExecutor::MakeLFExecutorPageCache();
            NTabletFlatExecutor::TPendingPartSwitch::TNewBundle bundle(
                backend->MakeLoaderEnvironment(), std::move(components));
            auto* stage = bundle.GetLoaderStage();
            TMetadataHost host{ .System = &CacheContext().ActorSystem() };
            UNIT_ASSERT(backend->BeginPartMetadata(stage->Loader, host.Context(), true));
            UNIT_ASSERT(host.Requests.size() == 1);
            const ui32 slot = rootRaw ? 1 : 0;
            UNIT_ASSERT(host.Reply(stage->Loader, slot, AdmitCollection(written, slot)) == EMetadataLoadResult::Ready);
            UNIT_ASSERT(stage->Loader.HasOuterCollection());
            auto result = Finish(stage->Loader, written);
            UNIT_ASSERT(result.Part->Small);
            UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
        }
    }
}

} // namespace NKikimr::NTable
