#include <ydb/core/tablet_flat/shared_cache_collection_load.h>
#include <ydb/core/tablet_flat/flat_sausage_packet.h>
#include <ydb/core/tablet_flat/flat_sausage_record.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSharedCache {
namespace {

    using namespace NActors;

    struct TCollectionLoadFixture {
        static constexpr ui32 Group = 77;
        static constexpr ui64 LoadId = 42;

        explicit TCollectionLoadFixture(ui32 chunkSize = Max<ui32>())
            : Runtime(1)
        {
            TAutoPtr<TAppPrepare> app = new TAppPrepare;
            Runtime.Initialize(app->Unwrap());
            Owner = Runtime.AllocateEdgeActor();
            Proxy = Runtime.AllocateEdgeActor();
            Runtime.RegisterService(MakeBlobStorageProxyID(Group), Proxy);
            NPageCollection::TRecord record(Group);
            const TString body(64, 'x');
            record.Push(TLogoBlobID(50, 1, 1, 0, body.size(), 1));
            record.Push(ui32(NTable::NPage::EPage::DataPage), body);
            Meta = record.Finish();
            Descriptor = { Group, TLogoBlobID(50, 1, 1, 0, Min<ui32>(chunkSize, Meta.size()), 100), Meta.size() };
            PreviousFilter =
                Runtime.SetEventFilter([this](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                    if (event->GetRecipientRewrite() == Owner) {
                        switch (event->GetTypeRewrite()) {
                            case TEvCollectionRawReady::EventType:
                                ++RawEvents;
                                break;
                            case TEvCollectionLoaded::EventType:
                                ++LoadedEvents;
                                break;
                            case TEvCollectionLoadFailed::EventType:
                                ++FailedEvents;
                                break;
                            case TEvCollectionLoadStopped::EventType:
                                ++StoppedEvents;
                                break;
                        }
                    }
                    return PreviousFilter(runtime, event);
                });
        }

        ~TCollectionLoadFixture() {
            Runtime.SetEventFilter(std::move(PreviousFilter));
        }

        void Start() {
            Worker = Runtime.Register(CreateCollectionLoadActor(Owner, LoadId, Descriptor));
        }

        TEvBlobStorage::TEvGet::TPtr ReadRequest() {
            return Runtime.GrabEdgeEvent<TEvBlobStorage::TEvGet>(Proxy, TDuration::Seconds(5));
        }

        void Reply(const TEvBlobStorage::TEvGet::TPtr& request, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
            auto response =
                MakeHolder<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, request->Get()->QuerySize, Group);
            for (ui32 index = 0; index < request->Get()->QuerySize; ++index) {
                auto& item = response->Responses[index];
                item.Id = request->Get()->Queries[index].Id;
                item.Status = index == 0 ? status : NKikimrProto::OK;
                if (item.Status == NKikimrProto::OK) {
                    const ui64 offset = (item.Id.Cookie() - Descriptor.Lead.Cookie()) * Descriptor.Lead.BlobSize();
                    item.Buffer = TRope(TString(Meta.data() + offset, item.Id.BlobSize()));
                }
            }
            Runtime.Send(new IEventHandle(Worker, Proxy, response.Release()), 0, true);
        }

        void Read() {
            auto request = ReadRequest();
            UNIT_ASSERT(request->Sender == Worker);
            Reply(request);
            auto ready = Runtime.GrabEdgeEvent<TEvCollectionRawReady>(Owner, TDuration::Seconds(5));
            UNIT_ASSERT(ready->Sender == Worker);
            UNIT_ASSERT_VALUES_EQUAL(ready->Get()->LoadId, LoadId);
            UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Bytes, Meta.size());
        }

        void Grant(ECollectionLoadKind kind, TActorId sender = {}) {
            Runtime.Send(new IEventHandle(Worker, sender ? sender : Owner, new TEvParseCollection(kind)), 0, true);
        }

        TEvCollectionLoaded::TPtr Loaded() {
            auto event = Runtime.GrabEdgeEvent<TEvCollectionLoaded>(Owner, TDuration::Seconds(5));
            UNIT_ASSERT(event->Sender == Worker);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->LoadId, LoadId);
            UNIT_ASSERT(event->Get()->PageCollection);
            UNIT_ASSERT(event->Get()->PageCollection->Label() == Descriptor.Lead);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->PageCollection->Total(), 1);
            UNIT_ASSERT_VALUES_EQUAL(LoadedEvents, 1);
            UNIT_ASSERT_VALUES_EQUAL(FailedEvents, 0);
            return event;
        }

        void Stop() {
            Runtime.Send(new IEventHandle(Worker, Owner, new TEvents::TEvPoison), 0, true);
            auto stopped = Runtime.GrabEdgeEvent<TEvCollectionLoadStopped>(Owner, TDuration::Seconds(5));
            UNIT_ASSERT(stopped->Sender == Worker);
            UNIT_ASSERT_VALUES_EQUAL(stopped->Get()->LoadId, LoadId);
            UNIT_ASSERT_VALUES_EQUAL(StoppedEvents, 1);
            UNIT_ASSERT_VALUES_EQUAL(LoadedEvents, 0);
            UNIT_ASSERT_VALUES_EQUAL(FailedEvents, 0);
        }

        TTestBasicRuntime Runtime;
        TActorId Owner;
        TActorId Proxy;
        TActorId Worker;
        NActors::TSharedData Meta;
        NPageCollection::TLargeGlobId Descriptor;
        TTestActorRuntimeBase::TEventFilter PreviousFilter;
        ui32 RawEvents = 0;
        ui32 LoadedEvents = 0;
        ui32 FailedEvents = 0;
        ui32 StoppedEvents = 0;
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSharedCacheCollectionLoad) {
    Y_UNIT_TEST(ReadsChunkedMetadataBeforeRegularConstruction) {
        TCollectionLoadFixture fixture(16);
        fixture.Start();
        fixture.Read();
        UNIT_ASSERT_VALUES_EQUAL(fixture.RawEvents, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedEvents, 0);
        fixture.Grant(ECollectionLoadKind::Regular);
        auto event = fixture.Loaded();
        const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(event->Get()->PageCollection.Get());
        UNIT_ASSERT(packet && !dynamic_cast<const NPageCollection::TOuterPageCollection*>(packet));
        UNIT_ASSERT(packet->LargeGlobId == fixture.Descriptor);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(packet->Meta.Raw.data(), packet->Meta.Raw.size()),
            TStringBuf(fixture.Meta.data(), fixture.Meta.size()));
    }

    Y_UNIT_TEST(DefersOuterRepresentationUntilParseGrant) {
        TCollectionLoadFixture fixture;
        fixture.Start();
        fixture.Read();
        UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedEvents, 0);
        fixture.Grant(ECollectionLoadKind::Outer);
        auto event = fixture.Loaded();
        const auto* packet =
            dynamic_cast<const NPageCollection::TOuterPageCollection*>(event->Get()->PageCollection.Get());
        UNIT_ASSERT(packet && packet->LargeGlobId == fixture.Descriptor);
        UNIT_ASSERT(packet->GetLocation(0).Offset.IsPageIndex());
    }

    Y_UNIT_TEST(DefersEarlyGrantUntilRawDataArrives) {
        TCollectionLoadFixture fixture;
        fixture.Start();
        fixture.Grant(ECollectionLoadKind::Regular);
        fixture.Read();
        auto event = fixture.Loaded();
        UNIT_ASSERT(
            dynamic_cast<const NPageCollection::TOuterPageCollection*>(event->Get()->PageCollection.Get()) == nullptr);
    }

    Y_UNIT_TEST(StopsAnOutstandingReadWithoutParsing) {
        TCollectionLoadFixture fixture;
        fixture.Start();
        auto request = fixture.ReadRequest();
        fixture.Stop();
        fixture.Reply(request);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.RawEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.FailedEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.StoppedEvents, 1);
    }

    Y_UNIT_TEST(StopsWhileRawDataAwaitsRole) {
        TCollectionLoadFixture fixture;
        fixture.Start();
        fixture.Read();
        fixture.Stop();
    }

    Y_UNIT_TEST(PropagatesBlobFailureBeforeRawCompletion) {
        TCollectionLoadFixture fixture(16);
        fixture.Start();
        fixture.Reply(fixture.ReadRequest(), NKikimrProto::NODATA);
        auto event = fixture.Runtime.GrabEdgeEvent<TEvCollectionLoadFailed>(fixture.Owner, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->LoadId, fixture.LoadId);
        UNIT_ASSERT(event->Get()->Status == NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(fixture.RawEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.FailedEvents, 1);
    }

    Y_UNIT_TEST(ReportsCorruptMetadataAfterGrant) {
        TCollectionLoadFixture fixture;
        fixture.Meta.mutable_data()[0] ^= 1;
        fixture.Start();
        fixture.Read();
        fixture.Grant(ECollectionLoadKind::Regular);
        auto event = fixture.Runtime.GrabEdgeEvent<TEvCollectionLoadFailed>(fixture.Owner, TDuration::Seconds(5));
        UNIT_ASSERT(event->Get()->Status == NKikimrProto::CORRUPTED);
        UNIT_ASSERT_VALUES_EQUAL(fixture.FailedEvents, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.LoadedEvents, 0);
    }

    Y_UNIT_TEST(IgnoresParseGrantFromAnotherActor) {
        TCollectionLoadFixture fixture;
        fixture.Start();
        fixture.Read();
        fixture.Grant(ECollectionLoadKind::Outer, fixture.Proxy);
        fixture.Grant(ECollectionLoadKind::Regular);
        auto event = fixture.Loaded();
        UNIT_ASSERT(
            dynamic_cast<const NPageCollection::TOuterPageCollection*>(event->Get()->PageCollection.Get()) == nullptr);
    }
}

} // namespace NKikimr::NSharedCache
