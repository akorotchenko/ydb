#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_executor.h>
#include <ydb/core/tablet_flat/flat_part_writer.h>
#include <ydb/core/tablet_flat/flat_writer_bundle.h>
#include <ydb/core/tablet_flat/flat_scan_actor.h>
#include <ydb/core/tablet_flat/cache_thread_test.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/shared_cache_fixture.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTabletFlatExecutor::NOps {

class TColdPartLoadTestAccess {
public:
    using TEvLoaded = TDriver::TEvPrivate::TEvPartLoaded;
    using TEvFailed = TDriver::TEvPrivate::TEvPartFailed;

    static NActors::IActor* Create(TActorId owner, TIntrusiveConstPtr<NTable::TColdPartStore> part) {
        return new TDriver::TColdPartLoader(owner, std::move(part), NBlockIO::EPriority::Fast);
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
        ui32 Step = 1;
    };

    TBundle::TResult WritePart(TWriteOptions options = {}, TVector<NPageCollection::TGlob>* blobs = nullptr) {
        NTest::TLayoutCook layout;
        layout.Col(0, 1, NScheme::NTypeIds::Uint64)
            .Col(options.Grouped ? 1 : 0, 2, NScheme::NTypeIds::String)
            .Key({ 1 });
        const auto scheme = layout.RowScheme();
        const auto tags = scheme->Tags();
        NTabletFlatExecutor::NWriter::TConf conf;
        conf.Groups.resize(options.Grouped ? 2 : 1);
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
        pages.Groups.resize(conf.Groups.size());
        pages.WriteBTreeIndexV2 = options.Dual;
        pages.WriteFlatIndex = !options.Dual;
        for (auto& group : pages.Groups) {
            group.PageSize = 512;
            group.BTreeIndexNodeTargetSize = 512;
        }
        if (options.Outer) {
            pages.SmallEdge = 8;
            pages.LargeEdge = 128;
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
        return NSharedCache::TSharedCache::Get().AdmitCollection(written.PageCollections.at(slot).PageCollection);
    }

    TPartView Finish(TLoader& loader, const TBundle::TResult& written) {
        while (auto fetch = loader.Run({ .PreloadIndex = false, .PreloadData = false })) {
            TVector<NSharedCache::TEvResult::TLoaded> pages;
            for (const auto& location : fetch.Pages) {
                const NPageCollection::TPageData* source = nullptr;
                for (const auto& collection : written.PageCollections) {
                    if (collection.PageCollection->Label() != fetch.PageCollection->Label()) {
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
                auto page = NSharedCache::TSharedCache::Get().AdmitPage(
                    fetch.PageCollection, location, TSharedData(source->Data), true);
                UNIT_ASSERT(page);
                pages.emplace_back(location.Offset, location.Size, std::move(page));
            }
            loader.Save(std::move(pages));
        }
        return loader.Result();
    }

} // anonymous namespace

namespace {

    using TColdAccess = NTabletFlatExecutor::NOps::TColdPartLoadTestAccess;

    struct TColdFixture {
        TColdFixture() {
            NSharedCache::TCacheThreadTestAccess::Setup(Runtime);
            TAutoPtr<TAppPrepare> app = new TAppPrepare;
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
            auto& cache = NSharedCache::TSharedCache::Get();
            for (const auto& collection : written.PageCollections) {
                auto metadata = cache.AdmitCollection(collection.PageCollection);
                for (const auto* group : { &collection.StickyPages, &collection.RegularPages }) {
                    for (const auto& data : *group) {
                        auto page =
                            cache.AdmitPage(collection.PageCollection, data.Location, TSharedData(data.Data), true);
                        UNIT_ASSERT(page);
                    }
                }
            }
        }

        TActorId Start(TActorId owner, const TIntrusiveConstPtr<TColdPartStore>& part) {
            return Runtime.Register(TColdAccess::Create(owner, part));
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
    Y_UNIT_TEST(MissRequestsPreserveMixedStickyHints) {
        TColdFixture fixture;
        TVector<NPageCollection::TGlob> blobs;
        auto written = WritePart({}, &blobs);
        THashMap<TLogoBlobID, TString> bodies;
        for (auto& blob : blobs) {
            UNIT_ASSERT(bodies.emplace(blob.GId.Logo, std::move(blob.Data)).second);
        }
        auto& cache = NSharedCache::TSharedCache::Get();
        TVector<TCacheCollectionRef> metadata;
        for (const auto& collection : written.PageCollections) {
            metadata.push_back(cache.AdmitCollection(collection.PageCollection));
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
            const auto* part = loaded->Get()->Part.As<TPartStore>();
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
        const auto* first = a->Get()->Part.As<TPartStore>();
        const auto* second = b->Get()->Part.As<TPartStore>();
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

Y_UNIT_TEST_SUITE_F(TPartLoaderRoot, NTest::TTableCacheTest) {
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
        UNIT_ASSERT(NSharedCache::TSharedCache::Get().Find(written.PageCollections[1].PageCollection->Label(),
                        missing) == NSharedCache::ECacheResultStatus::Miss);
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
        NTabletFlatExecutor::TPendingPartSwitch::TNewBundle bundle(Components(written));
        auto* original = &*bundle.Loader;
        bundle.Loader->SetPageCollection(0, AdmitCollection(written, 0));
        bundle.Loader->PrepareRoot();
        UNIT_ASSERT(bundle.Loader->HasOuterCollection());
        bundle.Loader->SetPageCollection(1, AdmitCollection(written, 1));
        bundle.Stage.emplace<NTabletFlatExecutor::TPendingPartSwitch::TLoaderStage>();
        UNIT_ASSERT(&*bundle.Loader == original);
        auto result = Finish(*bundle.Loader, written);
        bundle.Loader.reset();
        bundle.Stage.emplace<NTabletFlatExecutor::TPendingPartSwitch::TResultStage>(std::move(result));
        UNIT_ASSERT(!bundle.Loader);
        UNIT_ASSERT_VALUES_EQUAL(
            bundle.GetStage<NTabletFlatExecutor::TPendingPartSwitch::TResultStage>()->PartView.Part->Stat.Rows, 128);
    }

    Y_UNIT_TEST(RuntimeSwitchPreservesMixedRawMetadataInputs) {
        for (bool rootRaw : { false, true }) {
            auto written = WritePart({ .Outer = true, .Step = rootRaw ? 2u : 1u });
            auto components = Components(written, true);
            components.PageCollectionComponents[rootRaw ? 1 : 0].RawMeta = {};
            NTabletFlatExecutor::TPendingPartSwitch::TNewBundle bundle(std::move(components));
            if (!rootRaw) {
                bundle.Loader->SetPageCollection(0, AdmitCollection(written, 0));
            }
            bundle.Loader->PrepareRoot();
            UNIT_ASSERT(bundle.Loader->HasOuterCollection());
            if (rootRaw) {
                bundle.Loader->SetPageCollection(1, AdmitCollection(written, 1));
            }
            bundle.Stage.emplace<NTabletFlatExecutor::TPendingPartSwitch::TLoaderStage>();
            auto result = Finish(*bundle.Loader, written);
            UNIT_ASSERT(result.Part->Small);
            UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
        }
    }
}

} // namespace NKikimr::NTable
