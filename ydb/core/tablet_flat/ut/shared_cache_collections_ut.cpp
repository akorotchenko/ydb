#include <ydb/core/tablet_flat/shared_cache_collections.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tablet_flat/cache_thread_test.h>
#include <ydb/core/tablet_flat/flat_sausage_packet.h>
#include <ydb/core/tablet_flat/flat_sausage_record.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSharedCache {
namespace {

    using namespace NActors;

    struct TMetadata {
        NPageCollection::TLargeGlobId Descriptor;
        TSharedData Raw;
    };

    struct TCollectionFixture {
        static constexpr ui32 Group = 77;

        TCollectionFixture() {
            TCacheThreadTestAccess::Setup(Runtime);
            TAutoPtr<TAppPrepare> app = new TAppPrepare;
            Runtime.Initialize(app->Unwrap());
            Client = Runtime.AllocateEdgeActor();
            Proxy = Runtime.AllocateEdgeActor();
            Runtime.RegisterService(MakeBlobStorageProxyID(Group), Proxy);
            TSharedCacheConfig config;
            config.SetMemoryLimit(8_MB);
            CacheActor = Runtime.Register(CreateSharedPageCache(config, Runtime.GetDynamicCounters()));
            Send(new TEvCacheReadyRequest);
            Runtime.GrabEdgeEvent<TEvCacheReady>(Client, TDuration::Seconds(5));
            PreviousFilter =
                Runtime.SetEventFilter([this](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                    switch (event->GetTypeRewrite()) {
                        case TEvBlobStorage::TEvGet::EventType:
                            ReadWorkers.insert(event->Sender);
                            break;
                        case TEvCollectionRawReady::EventType:
                            RawWorkers.insert(event->Sender);
                            break;
                        case TEvCollectionParse::EventType:
                            ParseWorkers.insert(event->GetRecipientRewrite());
                            break;
                        case TEvCollectionResult::EventType:
                            ++Results;
                            break;
                    }
                    return PreviousFilter(runtime, event);
                });
        }

        ~TCollectionFixture() {
            Send(new TEvents::TEvPoison);
            Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            Runtime.SetEventFilter(std::move(PreviousFilter));
        }

        TMetadata Metadata(ui32 cookie = 1) {
            NPageCollection::TRecord record(Group);
            const TString body(64, 'x');
            record.Push(TLogoBlobID(50, 1, 1, 0, body.size(), cookie + 10000));
            record.Push(ui32(NTable::NPage::EPage::DataPage), body);
            auto raw = record.Finish();
            const NPageCollection::TLargeGlobId descriptor{ Group, TLogoBlobID(50, 1, 1, 0, raw.size(), cookie),
                raw.size() };
            return { descriptor, std::move(raw) };
        }

        void Send(IEventBase* event, ui64 cookie = 0, TActorId sender = {}) {
            Runtime.Send(new IEventHandle(CacheActor, sender ? sender : Client, event, 0, cookie), 0, true);
        }

        TIntrusivePtr<TCollectionRequest> Load(
            const TMetadata& metadata, ECollectionLoadKind kind = ECollectionLoadKind::Regular, ui64 cookie = 1,
            TActorId sender = {}) {
            auto request = MakeIntrusive<TCollectionRequest>(metadata.Descriptor, kind);
            Send(new TEvCollectionRequest(request), cookie, sender);
            return request;
        }

        void Update(const TIntrusivePtr<TCollectionRequest>& request, TActorId sender = {}) {
            Send(new TEvCollectionRequestUpdate(request), 0, sender);
        }

        TEvBlobStorage::TEvGet::TPtr Read(const TMetadata& metadata) {
            if (auto it = DeferredReads.find(metadata.Descriptor.Lead); it != DeferredReads.end()) {
                auto request = std::move(it->second);
                DeferredReads.erase(it);
                return request;
            }
            for (;;) {
                auto request = Runtime.GrabEdgeEvent<TEvBlobStorage::TEvGet>(Proxy, TDuration::Seconds(5));
                if (request->Get()->Queries[0].Id == metadata.Descriptor.Lead) {
                    return request;
                }
                const auto id = request->Get()->Queries[0].Id;
                DeferredReads.emplace(id, std::move(request));
            }
        }

        void Reply(const TEvBlobStorage::TEvGet::TPtr& request, const TMetadata& metadata,
            NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
            auto response =
                MakeHolder<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, request->Get()->QuerySize, Group);
            for (ui32 index = 0; index < request->Get()->QuerySize; ++index) {
                auto& item = response->Responses[index];
                item.Id = request->Get()->Queries[index].Id;
                item.Status = index == 0 ? status : NKikimrProto::OK;
                if (item.Status == NKikimrProto::OK) {
                    const ui64 offset =
                        (item.Id.Cookie() - metadata.Descriptor.Lead.Cookie()) * metadata.Descriptor.Lead.BlobSize();
                    item.Buffer = TRope(TString(metadata.Raw.data() + offset, item.Id.BlobSize()));
                }
            }
            Runtime.Send(new IEventHandle(request->Sender, Proxy, response.Release()), 0, true);
        }

        TEvCollectionResult::TPtr Result(
            NKikimrProto::EReplyStatus status = NKikimrProto::OK, TActorId recipient = {}) {
            auto result =
                Runtime.GrabEdgeEvent<TEvCollectionResult>(recipient ? recipient : Client, TDuration::Seconds(5));
            UNIT_ASSERT(result->Sender == CacheActor);
            UNIT_ASSERT(result->Get()->Status == status);
            UNIT_ASSERT(bool(result->Get()->Collection) == (status == NKikimrProto::OK));
            return result;
        }

        void Drain() {
            Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }

        ui64 RegisteredCollections() {
            ui64 count = 0;
            TSharedCache::Get().ForEachCollection([&](TCollection&) {
                ++count;
            });
            return count;
        }

        TTestBasicRuntime Runtime;
        TActorId Client;
        TActorId Proxy;
        TActorId CacheActor;
        TTestActorRuntimeBase::TEventFilter PreviousFilter;
        THashSet<TActorId> ReadWorkers;
        THashSet<TActorId> RawWorkers;
        THashSet<TActorId> ParseWorkers;
        THashMap<TLogoBlobID, TEvBlobStorage::TEvGet::TPtr> DeferredReads;
        ui32 Results = 0;
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSharedCacheCollections) {
    Y_UNIT_TEST(RejectsInvalidDescriptorBeforeLookupOrRead) {
        TCollectionFixture fixture;
        NPageCollection::TLargeGlobId descriptor;
        descriptor.Bytes = 1;
        auto request = MakeIntrusive<TCollectionRequest>(descriptor, ECollectionLoadKind::Regular);
        fixture.Send(new TEvCollectionRequest(request));
        auto result = fixture.Result(NKikimrProto::ERROR);
        UNIT_ASSERT(result->Get()->Request == request);
        UNIT_ASSERT(fixture.ReadWorkers.empty() && fixture.ParseWorkers.empty());
    }

    Y_UNIT_TEST(CoalescesReadersAndBypassesConstructionOnReadyHit) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto first = fixture.Load(metadata, ECollectionLoadKind::Regular, 1);
        auto request = fixture.Read(metadata);
        auto second = fixture.Load(metadata, ECollectionLoadKind::Regular, 2);
        fixture.Reply(request, metadata);
        auto a = fixture.Result();
        auto b = fixture.Result();
        UNIT_ASSERT(a->Get()->Request != b->Get()->Request);
        UNIT_ASSERT(a->Get()->Collection.Get() == b->Get()->Collection.Get());
        UNIT_ASSERT(a->Get()->Collection.CacheItem() == b->Get()->Collection.CacheItem());
        auto third = fixture.Load(metadata, ECollectionLoadKind::Regular, 3);
        auto hit = fixture.Result();
        UNIT_ASSERT(hit->Get()->Request == third);
        UNIT_ASSERT(hit->Get()->Collection.Get() == a->Get()->Collection.Get());
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
    }

    Y_UNIT_TEST(MetadataOnlyCollectionReleasesItsRegistryHold) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto request = fixture.Load(metadata);
        fixture.Reply(fixture.Read(metadata), metadata);
        auto result = fixture.Result();
        auto retained = std::move(result->Get()->Collection);
        result.Reset();
        fixture.Send(new TEvUnregister);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.RegisteredCollections(), 1);

        retained.Drop();
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.RegisteredCollections(), 0);
    }

    Y_UNIT_TEST(LateMetadataReleasePreservesANewNativeOwner) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto request = fixture.Load(metadata);
        fixture.Reply(fixture.Read(metadata), metadata);
        auto result = fixture.Result();
        TVector<TEvCollectionReleased::TPtr> releases;
        auto observer = fixture.Runtime.AddObserver<TEvCollectionReleased>([&](TEvCollectionReleased::TPtr& event) {
            releases.push_back(std::move(event));
        });
        result.Reset();
        fixture.Runtime.WaitFor("metadata release", [&] {
            return !releases.empty();
        }, TDuration::Seconds(5));

        TCacheCollectionRef retained;
        UNIT_ASSERT(TSharedCache::Get().FindCollection(metadata.Descriptor.Lead, retained) ==
                    ECacheCollectionLookupStatus::Hit);
        observer.Remove();
        for (auto& release : releases) {
            fixture.Runtime.Send(release.Release(), 0, true);
        }
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.RegisteredCollections(), 1);
        retained.Drop();
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.RegisteredCollections(), 0);
    }

    Y_UNIT_TEST(LastMetadataOwnerReleaseUnblocksHandleAdmission) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto request = fixture.Load(metadata);
        fixture.Reply(fixture.Read(metadata), metadata);
        auto result = fixture.Result();
        auto retained = std::move(result->Get()->Collection);
        result.Reset();

        auto& cache = TSharedCache::Get();
        TVector<TCacheCollectionRef> held;
        for (ui32 cookie = 100; cache.AvailableHandleCount() != 0; ++cookie) {
            auto item = fixture.Metadata(cookie);
            auto packet = MakeIntrusiveConst<NPageCollection::TPageCollection>(item.Descriptor, std::move(item.Raw));
            held.push_back(cache.AdmitCollection(std::move(packet)));
        }

        const auto waitingMetadata = fixture.Metadata(1000000);
        auto waiting = fixture.Load(waitingMetadata);
        fixture.Reply(fixture.Read(waitingMetadata), waitingMetadata);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 1);

        retained.Drop();
        auto admitted = fixture.Result();
        UNIT_ASSERT(admitted->Get()->Request == waiting);
        UNIT_ASSERT_VALUES_EQUAL(admitted->Get()->Collection->Id(), waitingMetadata.Descriptor.Lead);
    }

    Y_UNIT_TEST(WriterRetainsMetadataAcrossAnUnsuccessfulPageAdmission) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto packet =
            MakeIntrusiveConst<NPageCollection::TPageCollection>(metadata.Descriptor, TSharedData(metadata.Raw));
        const auto location = packet->GetLocation(0);
        auto& cache = TSharedCache::Get();
        TEvSaveCompactedPages writer(packet);
        UNIT_ASSERT(cache.UpdateCurrentLimit(1));
        UNIT_ASSERT(
            !writer.AddPage(cache, NPageCollection::TPageData(location, TSharedData::Copy(TString(64, 'x'))), false));
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.RegisteredCollections(), 1);

        UNIT_ASSERT(cache.UpdateCurrentLimit(8_MB));
        auto page =
            writer.AddPage(cache, NPageCollection::TPageData(location, TSharedData::Copy(TString(64, 'x'))), false);
        UNIT_ASSERT(page);
    }

    Y_UNIT_TEST(LastWriterOwnerDetachPreservesAHeldPage) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto packet =
            MakeIntrusiveConst<NPageCollection::TPageCollection>(metadata.Descriptor, TSharedData(metadata.Raw));
        const auto location = packet->GetLocation(0);
        TCachePageRef page;
        {
            TEvSaveCompactedPages writer(packet);
            page = writer.AddWrittenPage(
                TSharedCache::Get(), NPageCollection::TPageData(location, TSharedData::Copy(TString(64, 'x'))), true);
            UNIT_ASSERT(page);
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.RegisteredCollections(), 1);
            TCacheCollectionRef retained;
            UNIT_ASSERT(TSharedCache::Get().FindCollection(metadata.Descriptor.Lead, retained) ==
                        ECacheCollectionLookupStatus::Hit);
            UNIT_ASSERT(!TSharedCache::Get().TryDetachReleasedCollection(retained));
            UNIT_ASSERT(page.IsSticky());
        }
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.RegisteredCollections(), 0);
        UNIT_ASSERT(!page.IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(page.data(), page.size()), TString(64, 'x'));
    }

    Y_UNIT_TEST(RetainsCanonicalOwnerForAnEarlierUnresolvedSubscriber) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto unknown = fixture.Load(metadata, ECollectionLoadKind::Unknown, 1);
        auto known = fixture.Load(metadata, ECollectionLoadKind::Regular, 2);
        fixture.Reply(fixture.Read(metadata), metadata);
        auto first = fixture.Result();
        UNIT_ASSERT(first->Get()->Request == known);
        auto* canonical = first->Get()->Collection.Get();
        const auto item = first->Get()->Collection.CacheItem();
        UNIT_ASSERT(TSharedCache::Get().DetachCollection(item));
        UNIT_ASSERT(TSharedCache::Get().EraseCollection(item));
        first.Reset();

        auto packet =
            MakeIntrusiveConst<NPageCollection::TPageCollection>(metadata.Descriptor, TSharedData(metadata.Raw));
        auto replacement = TSharedCache::Get().AdmitCollection(packet);
        UNIT_ASSERT(replacement.CacheItem() != item);
        UNIT_ASSERT(TSharedCache::Get().DetachCollection(replacement.CacheItem()));
        auto newUnknown = fixture.Load(metadata, ECollectionLoadKind::Unknown, 3);
        auto newKnown = fixture.Load(metadata, ECollectionLoadKind::Regular, 4);
        auto current = fixture.Result();
        UNIT_ASSERT(current->Get()->Request == newKnown);
        UNIT_ASSERT(current->Get()->Collection.Get() == replacement.Get());
        UNIT_ASSERT(current->Get()->Collection.Get() != canonical);
        UNIT_ASSERT(TSharedCache::Get().DetachCollection(replacement.CacheItem()));

        UNIT_ASSERT(unknown->Resolve(ECollectionLoadKind::Regular));
        fixture.Update(unknown);
        auto second = fixture.Result();
        UNIT_ASSERT(second->Get()->Request == unknown);
        UNIT_ASSERT(second->Get()->Collection.Get() == canonical);
        UNIT_ASSERT(newUnknown->Resolve(ECollectionLoadKind::Regular));
        fixture.Update(newUnknown);
        auto resolved = fixture.Result();
        UNIT_ASSERT(resolved->Get()->Request == newUnknown);
        UNIT_ASSERT(resolved->Get()->Collection.Get() == replacement.Get());
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
    }

    Y_UNIT_TEST(RetainedCleanupPreservesANewLoadsResourceWait) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto unknown = fixture.Load(metadata, ECollectionLoadKind::Unknown);
        auto known = fixture.Load(metadata, ECollectionLoadKind::Regular, 2);
        fixture.Reply(fixture.Read(metadata), metadata);
        auto old = fixture.Result();
        const auto item = old->Get()->Collection.CacheItem();
        UNIT_ASSERT(TSharedCache::Get().DetachCollection(item));
        UNIT_ASSERT(TSharedCache::Get().EraseCollection(item));
        old.Reset();

        TCollectionCacheItem replacement;
        TCacheCollectionRef found;
        UNIT_ASSERT(TSharedCache::Get().FindOrInsert({ .Id = metadata.Descriptor.Lead }, replacement, found) ==
                    ECacheResultStatus::Inserted);
        TVector<TEvCollectionReady::TPtr> hints;
        auto hintObserver = fixture.Runtime.AddObserver<TEvCollectionReady>([&hints](TEvCollectionReady::TPtr& event) {
            hints.push_back(std::move(event));
        });
        auto fresh = fixture.Load(metadata, ECollectionLoadKind::Regular, 3);
        fixture.Drain();
        unknown->Cancel();
        fixture.Update(unknown);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 1);

        auto packet =
            MakeIntrusiveConst<NPageCollection::TPageCollection>(metadata.Descriptor, TSharedData(metadata.Raw));
        UNIT_ASSERT(TSharedCache::Get().MakeReady(replacement, MakeHolder<TCollection>(packet)));
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == fresh);
        UNIT_ASSERT(result->Get()->Collection.CacheItem() == replacement);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        UNIT_ASSERT(!hints.empty());
        hintObserver.Remove();
        for (auto& hint : hints) {
            fixture.Runtime.Send(hint.Release(), 0, true);
        }
    }

    Y_UNIT_TEST(QueuedSuccessfulReplyKeepsOwnershipAfterCancellation) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        TBlockEvents<TEvCollectionResult> replies(fixture.Runtime);
        auto request = fixture.Load(metadata);
        fixture.Reply(fixture.Read(metadata), metadata);
        fixture.Runtime.WaitFor("queued result", [&] {
            return replies.size() == 1;
        }, TDuration::Seconds(5));
        const auto item = replies.front()->Get()->Collection.CacheItem();
        request->Cancel();
        fixture.Update(request);
        fixture.Drain();
        UNIT_ASSERT(TSharedCache::Get().DetachCollection(item));
        UNIT_ASSERT(TSharedCache::Get().EraseCollection(item));
        replies.Unblock();
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == request);
        UNIT_ASSERT(!request->IsActive());
        UNIT_ASSERT(result->Get()->Collection->Id() == metadata.Descriptor.Lead);
    }

    Y_UNIT_TEST(ObservesCancellationAndRoleResolutionBeforeJoin) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto cancelled = MakeIntrusive<TCollectionRequest>(metadata.Descriptor);
        cancelled->Cancel();
        fixture.Update(cancelled);
        fixture.Send(new TEvCollectionRequest(cancelled));
        fixture.Drain();
        UNIT_ASSERT(fixture.ReadWorkers.empty());
        auto outer = MakeIntrusive<TCollectionRequest>(metadata.Descriptor);
        UNIT_ASSERT(outer->Resolve(ECollectionLoadKind::Outer));
        fixture.Update(outer);
        fixture.Send(new TEvCollectionRequest(outer));
        fixture.Reply(fixture.Read(metadata), metadata);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == outer);
        UNIT_ASSERT(dynamic_cast<const NPageCollection::TOuterPageCollection*>(
            result->Get()->Collection->PageCollection().Get()));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 1);
    }

    Y_UNIT_TEST(DefersParsingUntilRawDataAndRoleAreAvailable) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto request = fixture.Load(metadata, ECollectionLoadKind::Unknown);
        fixture.Reply(fixture.Read(metadata), metadata);
        fixture.Runtime.WaitFor("raw metadata", [&] {
            return fixture.RawWorkers.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(fixture.ParseWorkers.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 0);
        UNIT_ASSERT(request->Resolve(ECollectionLoadKind::Outer));
        fixture.Update(request);
        auto result = fixture.Result();
        UNIT_ASSERT(dynamic_cast<const NPageCollection::TOuterPageCollection*>(
            result->Get()->Collection->PageCollection().Get()));
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
    }

    Y_UNIT_TEST(RejectsConflictingSubscriptionsWithoutFailingTheLoad) {
        TCollectionFixture fixture;
        auto metadata = fixture.Metadata();
        auto valid = fixture.Load(metadata);
        auto request = fixture.Read(metadata);
        auto conflicting = metadata;
        ++conflicting.Descriptor.Group;
        auto wrongDescriptor = fixture.Load(conflicting, ECollectionLoadKind::Regular, 2);
        auto wrongRole = fixture.Load(metadata, ECollectionLoadKind::Outer, 3);
        auto a = fixture.Result(NKikimrProto::ERROR);
        auto b = fixture.Result(NKikimrProto::ERROR);
        UNIT_ASSERT(a->Get()->Request != b->Get()->Request);
        UNIT_ASSERT(a->Get()->Request == wrongDescriptor || a->Get()->Request == wrongRole);
        UNIT_ASSERT(b->Get()->Request == wrongDescriptor || b->Get()->Request == wrongRole);
        fixture.Reply(request, metadata);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == valid);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
    }

    Y_UNIT_TEST(FansOutStorageFailureAndAllowsAFreshLoad) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto first = fixture.Load(metadata);
        auto request = fixture.Read(metadata);
        auto second = fixture.Load(metadata, ECollectionLoadKind::Regular, 2);
        fixture.Reply(request, metadata, NKikimrProto::NODATA);
        auto a = fixture.Result(NKikimrProto::NODATA);
        auto b = fixture.Result(NKikimrProto::NODATA);
        UNIT_ASSERT(a->Get()->Request != b->Get()->Request);
        auto fresh = fixture.Load(metadata, ECollectionLoadKind::Regular, 3);
        auto retry = fixture.Read(metadata);
        UNIT_ASSERT(retry->Sender != request->Sender);
        fixture.Reply(retry, metadata);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == fresh);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
    }

    Y_UNIT_TEST(CancellingOneSubscriberPreservesSharedWork) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto cancelled = fixture.Load(metadata);
        auto request = fixture.Read(metadata);
        auto live = fixture.Load(metadata, ECollectionLoadKind::Regular, 2);
        cancelled->Cancel();
        fixture.Update(cancelled);
        fixture.Reply(request, metadata);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == live);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
    }

    Y_UNIT_TEST(LateStopAcknowledgmentCannotChangeARecreatedLoad) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        auto old = fixture.Load(metadata);
        auto oldRequest = fixture.Read(metadata);
        TBlockEvents<TEvCollectionData> stopped(fixture.Runtime, [](const auto& ev) {
            return ev->Get()->Status == NKikimrProto::RACE;
        });
        old->Cancel();
        fixture.Update(old);
        fixture.Runtime.WaitFor("stopped worker", [&] {
            return stopped.size() == 1;
        }, TDuration::Seconds(5));
        auto fresh = fixture.Load(metadata, ECollectionLoadKind::Regular, 2);
        auto request = fixture.Read(metadata);
        UNIT_ASSERT(request->Sender != oldRequest->Sender);
        fixture.Reply(request, metadata);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == fresh);
        stopped.Unblock();
        fixture.Reply(oldRequest, metadata);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 1);
        TCacheCollectionRef found;
        UNIT_ASSERT(
            TSharedCache::Get().FindCollection(metadata.Descriptor.Lead, found) == ECacheCollectionLookupStatus::Hit);
        UNIT_ASSERT(found.Get() == result->Get()->Collection.Get());
    }

    Y_UNIT_TEST(WriterWinsAndLateParsedCandidateCannotOverwriteIt) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        TBlockEvents<TEvCollectionData> loaded(fixture.Runtime);
        auto request = fixture.Load(metadata);
        fixture.Reply(fixture.Read(metadata), metadata);
        fixture.Runtime.WaitFor("parsed candidate", [&] {
            return loaded.size() == 1;
        }, TDuration::Seconds(5));
        auto writer =
            MakeIntrusiveConst<NPageCollection::TPageCollection>(metadata.Descriptor, TSharedData(metadata.Raw));
        writer->SetSkipBTreeIndexV1Shadow(true);
        auto canonical = TSharedCache::Get().AdmitCollection(writer);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == request);
        UNIT_ASSERT(result->Get()->Collection.Get() == canonical.Get());
        loaded.Unblock();
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 1);
        UNIT_ASSERT(result->Get()->Collection->PageCollection()->SkipBTreeIndexV1Shadow());
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
    }

    Y_UNIT_TEST(NativePublicationWakesMetadataWhenThePageWaitQueueIsEmpty) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        TCollectionCacheItem item;
        TCacheCollectionRef found;
        UNIT_ASSERT(TSharedCache::Get().FindOrInsert({ .Id = metadata.Descriptor.Lead }, item, found) ==
                    ECacheResultStatus::Inserted);
        TVector<TEvCollectionReady::TPtr> hints;
        auto hintObserver = fixture.Runtime.AddObserver<TEvCollectionReady>([&hints](TEvCollectionReady::TPtr& event) {
            hints.push_back(std::move(event));
        });
        auto request = fixture.Load(metadata);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 0);
        auto packet =
            MakeIntrusiveConst<NPageCollection::TPageCollection>(metadata.Descriptor, TSharedData(metadata.Raw));
        UNIT_ASSERT(TSharedCache::Get().MakeReady(item, MakeHolder<TCollection>(packet)));
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == request);
        UNIT_ASSERT(result->Get()->Collection.CacheItem() == item);
        UNIT_ASSERT(fixture.ReadWorkers.empty() && fixture.ParseWorkers.empty());
        UNIT_ASSERT(!hints.empty());
        hintObserver.Remove();
        for (auto& hint : hints) {
            fixture.Runtime.Send(hint.Release(), 0, true);
        }
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Results, 1);
    }

    Y_UNIT_TEST(SerializesParserGrantsAcrossCollections) {
        TCollectionFixture fixture;
        const auto a = fixture.Metadata(1);
        const auto b = fixture.Metadata(2);
        TBlockEvents<TEvCollectionParse> grants(fixture.Runtime);
        auto first = fixture.Load(a);
        auto second = fixture.Load(b, ECollectionLoadKind::Regular, 2);
        auto requestA = fixture.Read(a);
        auto requestB = fixture.Read(b);
        fixture.Reply(requestA, a);
        fixture.Reply(requestB, b);
        fixture.Runtime.WaitFor("both raw collections", [&] {
            return fixture.RawWorkers.size() == 2 && grants.size() == 1;
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
        grants.Unblock(1);
        auto resultA = fixture.Result();
        fixture.Runtime.WaitFor("second parser grant", [&] {
            return grants.size() == 1;
        }, TDuration::Seconds(5));
        grants.Unblock();
        auto resultB = fixture.Result();
        UNIT_ASSERT(resultA->Get()->Request != resultB->Get()->Request);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 2);
    }

    Y_UNIT_TEST(ParserFailureReleasesTheNextQueuedGrant) {
        TCollectionFixture fixture;
        auto corrupt = fixture.Metadata(1);
        corrupt.Raw.mutable_data()[0] ^= 1;
        const auto valid = fixture.Metadata(2);
        TBlockEvents<TEvCollectionParse> grants(fixture.Runtime);
        auto failed = fixture.Load(corrupt);
        auto live = fixture.Load(valid, ECollectionLoadKind::Regular, 2);
        auto requestCorrupt = fixture.Read(corrupt);
        auto requestValid = fixture.Read(valid);
        fixture.Reply(requestCorrupt, corrupt);
        fixture.Runtime.WaitFor("first parser grant", [&] {
            return grants.size() == 1;
        }, TDuration::Seconds(5));
        fixture.Reply(requestValid, valid);
        fixture.Runtime.WaitFor("both raw collections", [&] {
            return fixture.RawWorkers.size() == 2;
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
        grants.Unblock(1);
        auto error = fixture.Result(NKikimrProto::CORRUPTED);
        UNIT_ASSERT(error->Get()->Request == failed);
        fixture.Runtime.WaitFor("parser grant after failure", [&] {
            return grants.size() == 1;
        }, TDuration::Seconds(5));
        grants.Unblock();
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == live);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 2);
    }

    Y_UNIT_TEST(UnresolvedJobsLeaveAReadSlotForRootProgress) {
        TCollectionFixture fixture;
        TVector<TIntrusivePtr<TCollectionRequest>> unknown;
        const ui32 unknownCount = TCollectionLoadLimits{}.Count - 1;
        for (ui32 index = 0; index < unknownCount; ++index) {
            unknown.push_back(fixture.Load(fixture.Metadata(index + 1), ECollectionLoadKind::Unknown));
        }
        fixture.Runtime.WaitFor("unresolved read capacity", [&] {
            return fixture.ReadWorkers.size() == unknownCount;
        }, TDuration::Seconds(5));
        auto waiting = fixture.Load(fixture.Metadata(unknownCount + 1), ECollectionLoadKind::Unknown);
        const auto root = fixture.Metadata(unknownCount + 2);
        auto known = fixture.Load(root);
        fixture.Reply(fixture.Read(root), root);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == known);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), unknownCount + 1);
        unknown.front()->Cancel();
        fixture.Update(unknown.front());
        fixture.Runtime.WaitFor("next unresolved read", [&] {
            return fixture.ReadWorkers.size() == unknownCount + 2;
        }, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(StopAcknowledgmentRetainsByteChargeWhileRootCanProgress) {
        TCollectionFixture fixture;
        auto large = fixture.Metadata(1);
        large.Descriptor = { TCollectionFixture::Group, TLogoBlobID(50, 1, 1, 0, 1_MB, 1),
            TCollectionLoadLimits{}.Bytes / 2 };
        auto old = fixture.Load(large, ECollectionLoadKind::Unknown);
        fixture.Read(large);
        TBlockEvents<TEvCollectionData> stopped(fixture.Runtime, [](const auto& ev) {
            return ev->Get()->Status == NKikimrProto::RACE;
        });
        old->Cancel();
        fixture.Update(old);
        fixture.Runtime.WaitFor("stopped large read", [&] {
            return stopped.size() == 1;
        }, TDuration::Seconds(5));
        auto waiting = fixture.Load(fixture.Metadata(1000), ECollectionLoadKind::Unknown);
        const auto root = fixture.Metadata(2000);
        auto known = fixture.Load(root);
        fixture.Reply(fixture.Read(root), root);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == known);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 2);
        stopped.Unblock();
        fixture.Runtime.WaitFor("byte credit after acknowledgment", [&] {
            return fixture.ReadWorkers.size() == 3;
        }, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(UnregisterCancelsRequestsBeforeCollectionRegistration) {
        TCollectionFixture fixture;
        const auto a = fixture.Metadata(1);
        const auto b = fixture.Metadata(2);
        auto first = fixture.Load(a);
        auto second = fixture.Load(b, ECollectionLoadKind::Unknown, 2);
        auto oldRead = fixture.Read(a);
        fixture.Read(b);
        TBlockEvents<TEvCollectionData> stopped(fixture.Runtime, [](const auto& ev) {
            return ev->Get()->Status == NKikimrProto::RACE;
        });
        fixture.Send(new TEvUnregister);
        auto cancelledA = fixture.Result(NKikimrProto::RACE);
        auto cancelledB = fixture.Result(NKikimrProto::RACE);
        UNIT_ASSERT(cancelledA->Get()->Request != cancelledB->Get()->Request);
        UNIT_ASSERT(!first->IsActive() && !second->IsActive());
        fixture.Runtime.WaitFor("stopped owner reads", [&] {
            return stopped.size() == 2;
        }, TDuration::Seconds(5));
        stopped.Unblock();
        fixture.Drain();
        auto fresh = fixture.Load(a, ECollectionLoadKind::Regular, 3);
        auto read = fixture.Read(a);
        UNIT_ASSERT(read->Sender != oldRead->Sender);
        fixture.Reply(read, a);
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == fresh);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
    }

    Y_UNIT_TEST(UnregisterPreservesAnotherOwnersSharedLoad) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        const auto other = fixture.Runtime.AllocateEdgeActor();
        auto removed = fixture.Load(metadata);
        auto read = fixture.Read(metadata);
        auto kept = fixture.Load(metadata, ECollectionLoadKind::Regular, 2, other);
        fixture.Send(new TEvUnregister);
        auto cancelled = fixture.Result(NKikimrProto::RACE);
        UNIT_ASSERT(cancelled->Get()->Request == removed);
        UNIT_ASSERT(kept->IsActive());
        fixture.Reply(read, metadata);
        auto result = fixture.Result(NKikimrProto::OK, other);
        UNIT_ASSERT(result->Get()->Request == kept);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 1);
    }

    Y_UNIT_TEST(DetachCancelsOnlyTheOwnersMatchingCollection) {
        TCollectionFixture fixture;
        const auto a = fixture.Metadata(1);
        const auto b = fixture.Metadata(2);
        const auto other = fixture.Runtime.AllocateEdgeActor();
        auto removed = fixture.Load(a);
        auto shared = fixture.Load(a, ECollectionLoadKind::Regular, 2, other);
        auto independent = fixture.Load(b, ECollectionLoadKind::Regular, 3);
        auto readA = fixture.Read(a);
        auto readB = fixture.Read(b);
        fixture.Send(new TEvDetach(a.Descriptor.Lead));
        auto cancelled = fixture.Result(NKikimrProto::RACE);
        UNIT_ASSERT(cancelled->Get()->Request == removed);
        UNIT_ASSERT(shared->IsActive() && independent->IsActive());
        fixture.Reply(readA, a);
        fixture.Reply(readB, b);
        auto resultA = fixture.Result(NKikimrProto::OK, other);
        auto resultB = fixture.Result();
        UNIT_ASSERT(resultA->Get()->Request == shared);
        UNIT_ASSERT(resultB->Get()->Request == independent);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ParseWorkers.size(), 2);
    }

    Y_UNIT_TEST(UnregisterRetainedOwnerPreservesANewLoadsWait) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        const auto other = fixture.Runtime.AllocateEdgeActor();
        auto retained = fixture.Load(metadata, ECollectionLoadKind::Unknown);
        auto known = fixture.Load(metadata, ECollectionLoadKind::Regular, 2, other);
        fixture.Reply(fixture.Read(metadata), metadata);
        auto old = fixture.Result(NKikimrProto::OK, other);
        const auto item = old->Get()->Collection.CacheItem();
        UNIT_ASSERT(TSharedCache::Get().DetachCollection(item));
        UNIT_ASSERT(TSharedCache::Get().EraseCollection(item));
        old.Reset();

        TCollectionCacheItem replacement;
        TCacheCollectionRef found;
        UNIT_ASSERT(TSharedCache::Get().FindOrInsert({ .Id = metadata.Descriptor.Lead }, replacement, found) ==
                    ECacheResultStatus::Inserted);
        TVector<TEvCollectionReady::TPtr> hints;
        auto hintObserver = fixture.Runtime.AddObserver<TEvCollectionReady>([&hints](TEvCollectionReady::TPtr& event) {
            hints.push_back(std::move(event));
        });
        auto fresh = fixture.Load(metadata, ECollectionLoadKind::Regular, 3, other);
        fixture.Drain();
        fixture.Send(new TEvUnregister);
        auto cancelled = fixture.Result(NKikimrProto::RACE);
        UNIT_ASSERT(cancelled->Get()->Request == retained);
        UNIT_ASSERT(!retained->IsActive() && fresh->IsActive());
        auto packet =
            MakeIntrusiveConst<NPageCollection::TPageCollection>(metadata.Descriptor, TSharedData(metadata.Raw));
        UNIT_ASSERT(TSharedCache::Get().MakeReady(replacement, MakeHolder<TCollection>(packet)));
        auto result = fixture.Result(NKikimrProto::OK, other);
        UNIT_ASSERT(result->Get()->Request == fresh);
        UNIT_ASSERT(result->Get()->Collection.CacheItem() == replacement);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        UNIT_ASSERT(!hints.empty());
        hintObserver.Remove();
        for (auto& hint : hints) {
            fixture.Runtime.Send(hint.Release(), 0, true);
        }
    }

    Y_UNIT_TEST(OwnerRemovalPreservesAQueuedSuccessfulReply) {
        TCollectionFixture fixture;
        const auto metadata = fixture.Metadata();
        TBlockEvents<TEvCollectionResult> replies(fixture.Runtime);
        auto request = fixture.Load(metadata);
        fixture.Reply(fixture.Read(metadata), metadata);
        fixture.Runtime.WaitFor("queued result", [&] {
            return replies.size() == 1;
        }, TDuration::Seconds(5));
        const auto item = replies.front()->Get()->Collection.CacheItem();
        fixture.Send(new TEvUnregister);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT(TSharedCache::Get().DetachCollection(item));
        UNIT_ASSERT(TSharedCache::Get().EraseCollection(item));
        replies.Unblock();
        auto result = fixture.Result();
        UNIT_ASSERT(result->Get()->Request == request);
        UNIT_ASSERT(result->Get()->Collection->Id() == metadata.Descriptor.Lead);
    }

    Y_UNIT_TEST(RequiredKnownJobLargerThanTheNormalAllowanceCanStart) {
        TCollectionFixture fixture;
        auto metadata = fixture.Metadata();
        metadata.Descriptor = { TCollectionFixture::Group, TLogoBlobID(50, 1, 1, 0, 1_MB, 1),
            TCollectionLoadLimits{}.Bytes + 1_MB };
        auto request = fixture.Load(metadata);
        auto read = fixture.Read(metadata);
        UNIT_ASSERT_VALUES_EQUAL(read->Get()->Queries[0].Id, metadata.Descriptor.Lead);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadWorkers.size(), 1);
        UNIT_ASSERT(fixture.ParseWorkers.empty());
    }
}

} // namespace NKikimr::NSharedCache
