#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tablet_flat/flat_transaction_pages.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_exec_seat_cache.h>
#include <ydb/core/tablet_flat/flat_sausagecache.h>
#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_part_store_cache.h>
#include <ydb/core/tablet_flat/flat_part_store.h>
#include <ydb/core/tablet_flat/flat_scan_events.h>
#include <ydb/core/tablet_flat/flat_executor_tx_env.h>
#include <ydb/core/tablet_flat/shared_cache_events_lf.h>
#include <ydb/core/tablet_flat/shared_cache_collection_load.h>
#include <ydb/core/tablet_flat/shared_sausagecache_state.h>
#include <ydb/core/tablet_flat/test/libs/table/cache_fixture.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/cast.h>
#include <util/stream/str.h>

#include <type_traits>

namespace NKikimr::NTabletFlatExecutor {
namespace {

    class TReadTransaction final : public ITransaction {
        bool Execute(TTransactionContext&, const NActors::TActorContext&) override {
            return true;
        }

        void Complete(const NActors::TActorContext&) override {}
    };

    template <class TStore, class TMakeCollection>
    TIntrusivePtr<TStore> MakeRegistrationPart(
        bool sharedIndex, TMakeCollection makeCollection, TSharedData* indexSlotPage = nullptr) {
        NTable::NTest::TLayoutCook layout;
        layout.Col(0, 0, NScheme::NTypeIds::Uint32).Col(0, 1, NScheme::NTypeIds::String);
        if (sharedIndex) {
            layout.Col(1, 2, NScheme::NTypeIds::String);
        }
        layout.Key({ 0 });
        NTable::NPage::TConf conf;
        conf.SmallEdge = 8;
        conf.LargeEdge = 16;
        if (sharedIndex) {
            conf.Groups.resize(2);
            conf.Layout.SeparateDescriptorCollection = true;
            conf.Layout.KeyIndexInDescriptor = true;
            conf.Layout.GroupIndexesInDescriptor = true;
            conf.WriteFlatIndex = false;
            conf.WriteBTreeIndexV1 = true;
            conf.WriteBTreeIndexV2 = false;
        }
        NTable::NTest::TPartCook cook(layout, conf);
        if (sharedIndex) {
            cook.AddN(ui32(1), TString(64, 'x'), TString(64, 'y'));
        } else {
            cook.AddN(ui32(1), TString(64, 'x'));
        }
        const auto source = cook.Finish().Lone();
        if (indexSlotPage) {
            const ui32 slot = source->IndexSlot({});
            const auto location = source->PageColls[slot]->GetLocation(0);
            *indexSlotPage = TSharedData(*source->Store->GetPage(slot, location.Offset));
        }
        NTable::TPart::TParams params{
            .Epoch = source->Epoch,
            .Scheme = source->Scheme,
            .IndexPages = source->IndexPages,
            .Blobs = source->Blobs,
            .ByKeyPrefixes = source->ByKeyPrefixes,
            .Large = source->Large,
            .Small = source->Small,
            .IndexesRawSize = source->IndexesRawSize,
            .MinRowVersion = source->MinRowVersion,
            .MaxRowVersion = source->MaxRowVersion,
            .GarbageStats = source->GarbageStats,
            .TxIdStats = source->TxIdStats,
            .Layout = source->Slots.GetLayout(),
        };
        auto part = MakeIntrusive<TStore>(source->PageColls.front()->Label(), std::move(params), source->Stat);
        for (const auto& descriptor : source->PageColls) {
            part->PageCollections.push_back(makeCollection(descriptor));
        }
        UNIT_ASSERT(source->Blobs);
        part->Pseudo = makeCollection(source->Blobs);
        return part;
    }

    TIntrusivePtr<NTable::TCachePartStore> MakeRegistrationPart(
        bool sharedIndex = false, TSharedData* indexSlotPage = nullptr) {
        return MakeRegistrationPart<NTable::TCachePartStore>(sharedIndex, [](const auto& collection) {
            return NTable::NTest::AdmitCollection(TCache::Get(), collection);
        }, indexSlotPage);
    }

    TIntrusivePtr<NTable::TPrivatePartStore> MakePrivateRegistrationPart(bool sharedIndex = false) {
        return MakeRegistrationPart<NTable::TPrivatePartStore>(sharedIndex, [](const auto& collection) {
            return MakeIntrusive<TPrivatePageCache::TPageCollection>(collection);
        });
    }

} // namespace

Y_UNIT_TEST_SUITE_F(TLFPageCacheBackend, ::NKikimr::NTable::NTest::TTableCacheTest) {
    Y_UNIT_TEST(ModeResolutionPreservesOwnerRequestsAndStickyIndexes) {
        using NTable::NPage::ECacheMode;
        auto backend = MakeLFExecutorPageCache();
        auto part = MakeRegistrationPart(true);
        UNIT_ASSERT(!backend->PrepareRegistration());
        UNIT_ASSERT(part->IndexPages.HasBTree());
        const ui32 dataSlot = part->DataSlot({});
        const ui32 otherDataSlot = part->DataSlot(NTable::NPage::TGroupId(1));
        const ui32 indexSlot = part->IndexSlot({});
        UNIT_ASSERT_VALUES_EQUAL(indexSlot, part->IndexSlot(NTable::NPage::TGroupId(1)));
        const ui32 count = part->GetPageCollectionCount();
        TVector<ECacheMode> modes(count, ECacheMode::Regular);
        TVector<bool> stickyGroups(part->GroupsCount, false);
        stickyGroups[1] = true;
        backend->ReadCollectionModes(*part, modes, stickyGroups);
        UNIT_ASSERT(modes[indexSlot] == ECacheMode::Sticky);
        UNIT_ASSERT(modes[dataSlot] == ECacheMode::Regular);
        UNIT_ASSERT(modes[otherDataSlot] == ECacheMode::Sticky);

        modes.assign(count, ECacheMode::Regular);
        modes[indexSlot] = ECacheMode::TryKeepInMemory;
        backend->PrepareCollectionModes(*part, modes, stickyGroups);
        UNIT_ASSERT(modes[indexSlot] == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(modes[otherDataSlot] == ECacheMode::Sticky);

        modes.assign(count, ECacheMode::Regular);
        modes[dataSlot] = ECacheMode::TryKeepInMemory;
        stickyGroups[0] = true;
        const auto notify = backend->UpdateCollectionModes(*part, modes, stickyGroups);
        UNIT_ASSERT_VALUES_EQUAL(notify.size(), count);
        for (bool required : notify) {
            UNIT_ASSERT(required);
        }
        UNIT_ASSERT(modes[dataSlot] == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(modes[indexSlot] == ECacheMode::Sticky);

        UNIT_ASSERT(TCache::Get().SetCollectionPagesCacheMode(
            part->PageCollections[dataSlot].CacheItem(), ECacheMode::TryKeepInMemory));
        modes.assign(count, ECacheMode::Regular);
        stickyGroups.assign(part->GroupsCount, false);
        backend->ReadCollectionModes(*part, modes, stickyGroups);
        for (auto mode : modes) {
            UNIT_ASSERT(mode == ECacheMode::Regular);
        }
        UNIT_ASSERT(part->PageCollections[dataSlot]->GetCacheMode() == ECacheMode::TryKeepInMemory);
    }

    Y_UNIT_TEST(RegistrationKeepsOwnerStatsAcrossReplayAndModeUpdates) {
        auto backend = MakeLFExecutorPageCache();
        TPageCacheStats stats;
        UNIT_ASSERT(!backend->ReadStats(stats));
        UNIT_ASSERT(!backend->PrepareRegistration());
        UNIT_ASSERT(backend->ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, 0);
        auto part = MakeRegistrationPart();
        const auto& descriptor = part->GetPageCollectionRef(0);
        TVector<NPageCollection::TBtreeSeed> seeds{
            { .IndexCollectionId = descriptor->Label(), .DataCollectionId = descriptor->Label(), .Sticky = true },
            { .IndexCollectionId = descriptor->Label(), .DataCollectionId = descriptor->Label(), .Sticky = false },
        };
        TVector<THolder<NActors::IEventBase>> sent;
        const auto send = [&](THolder<NActors::IEventBase> event) {
            sent.push_back(std::move(event));
        };
        backend->RegisterCollection(*part, 0, NTable::NPage::ECacheMode::TryKeepInMemory, seeds, true, send);
        backend->RegisterPseudoCollection(*part, send);
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), 2);
        const auto* attached = CheckedCast<NSharedCache::TEvAttach*>(sent[0].Get());
        const auto* pseudo = CheckedCast<NSharedCache::TEvAttach*>(sent[1].Get());
        UNIT_ASSERT(attached->PageCollection.Get() == descriptor.Get());
        UNIT_ASSERT(attached->CacheMode == NTable::NPage::ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(attached->BtreeSeeds == seeds);
        UNIT_ASSERT(attached->ReplayStickyWalk && attached->OwnerStats);
        UNIT_ASSERT(pseudo->PageCollection.Get() == part->GetPseudoPageCollection());
        UNIT_ASSERT(pseudo->CacheMode == NTable::NPage::ECacheMode::Regular);
        UNIT_ASSERT(pseudo->BtreeSeeds.empty() && !pseudo->ReplayStickyWalk);
        UNIT_ASSERT(pseudo->OwnerStats == attached->OwnerStats);

        auto* owner = attached->OwnerStats.Get();
        owner->PageCollections.store(2, std::memory_order_relaxed);
        owner->SharedBodyBytes.store(101, std::memory_order_relaxed);
        owner->StickyBytes.store(23, std::memory_order_relaxed);
        owner->TryKeepInMemoryBytes.store(47, std::memory_order_relaxed);
        UNIT_ASSERT(backend->PrepareRegistration());
        backend->UpdateAttachment(*part, 0, NTable::NPage::ECacheMode::Regular, {}, EAttachmentUpdate::CacheMode, send);
        const auto* update = CheckedCast<NSharedCache::TEvAttach*>(sent.back().Get());
        UNIT_ASSERT(update->OwnerStats.Get() == owner);
        UNIT_ASSERT(update->CacheMode == NTable::NPage::ECacheMode::Regular);
        UNIT_ASSERT(update->BtreeSeeds.empty() && !update->ReplayStickyWalk);
        UNIT_ASSERT(backend->ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.SharedBodyBytes, 101);
        UNIT_ASSERT_VALUES_EQUAL(stats.StickyBytes, 23);
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, 47);
        auto detach = backend->DetachCollection(descriptor->Label());
        UNIT_ASSERT(CheckedCast<NSharedCache::TEvDetach*>(detach.Get())->PageCollectionId == descriptor->Label());
        backend.Reset();
        UNIT_ASSERT_VALUES_EQUAL(owner->Read().SharedBodyBytes, 101);
    }

    Y_UNIT_TEST(TransactionRequestKeepsCollectionAliveThroughSend) {
        using NTable::NPage::ECacheMode;
        using NTable::NPage::TGroupId;

        auto backend = MakeLFExecutorPageCache();
        TSharedData retainedData;
        auto part = MakeRegistrationPart(true, &retainedData);
        const auto& regular = part->PageCollections[part->DataSlot({})];
        const auto& sticky = part->PageCollections[part->DataSlot(TGroupId(1))];
        const auto& retained = part->PageCollections[part->IndexSlot({})];
        UNIT_ASSERT(regular->GetCacheMode() == ECacheMode::Regular);
        UNIT_ASSERT(TCache::Get().SetCollectionPagesCacheMode(sticky.CacheItem(), ECacheMode::Sticky));
        const auto regularLocation = regular->PageCollection()->GetLocation(0);
        const auto stickyLocation = sticky->PageCollection()->GetLocation(0);
        const auto retainedLocation = retained->PageCollection()->GetLocation(0);

        auto seat = backend->MakeSeat(1, TAutoPtr<ITransaction>(new TReadTransaction));
        auto retainedPage = NTable::NTest::AdmitPage(
            TCache::Get(), retained->PageCollection(), retainedLocation, std::move(retainedData));
        UNIT_ASSERT(retainedPage);
        NSharedCache::TEvResult loaded(retained->PageCollection(), NKikimrProto::OK, 0);
        loaded.Pages.emplace_back(retainedLocation.Offset, retainedLocation.Size, std::move(retainedPage));
        UNIT_ASSERT(backend->AdoptLoadedPages(*seat, &loaded));

        TPageCollectionReadEnv readEnv(*CheckedCast<TCacheSeat*>(seat.get()));
        ITransactionPages& pages = readEnv;
        UNIT_ASSERT(!pages.TryGetPage(part.Get(), regularLocation, {}));
        UNIT_ASSERT(!pages.TryGetPage(part.Get(), stickyLocation, TGroupId(1)));
        auto waitPad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
        auto requests = backend->PreparePageRequests(*seat, pages, *waitPad);
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);

        auto& regularPages = requests.at(regular->Id());
        const auto* locations = regularPages.data();
        const auto trace = NWilson::TTraceId::NewTraceId(15, 4095);
        ui32 sent = 0;
        UNIT_ASSERT(regular.IsSoleReference());
        backend->SendTransactionRequest(regular->Id(), std::move(regularPages), waitPad, NWilson::TTraceId(trace),
            [&](THolder<NActors::IEventBase> event) {
                ++sent;
                UNIT_ASSERT(!regular.IsSoleReference());
                const auto& request = *CheckedCast<NSharedCache::TEvRequest*>(event.Get());
                UNIT_ASSERT(request.PageCollection.Get() == regular->PageCollection().Get());
                UNIT_ASSERT(request.Pages.data() == locations);
                UNIT_ASSERT_VALUES_EQUAL(request.Pages.size(), 1);
                UNIT_ASSERT(request.Priority == NBlockIO::EPriority::Fast);
                UNIT_ASSERT_VALUES_EQUAL(request.Cookie, 0);
                UNIT_ASSERT(request.WaitPad == waitPad && request.TraceId == trace);
                UNIT_ASSERT(request.Sticky.empty() && !request.ReadAhead);

                UNIT_ASSERT(waitPad->WorkingSetPages.at(retained->Id()).front() == retainedLocation);
                UNIT_ASSERT(waitPad->WorkingSetPages.at(sticky->Id()).front() == stickyLocation);
                UNIT_ASSERT(waitPad->WorkingSetPages.at(regular->Id()).front() == regularLocation);
                UNIT_ASSERT_VALUES_EQUAL(waitPad->WorkingSetBytes,
                    retainedLocation.Size + regularLocation.Size + 2 * NActors::TSharedData::OverheadSize);
                UNIT_ASSERT(waitPad->HasPinnedPages.load(std::memory_order_acquire));
            });
        UNIT_ASSERT_VALUES_EQUAL(sent, 1);
        UNIT_ASSERT(regular.IsSoleReference());
    }

    Y_UNIT_TEST(LegacyAndLFEventsKeepDistinctTypesAndPayloads) {
        using TLegacyRequest = NSharedCache::NLegacy::TEvRequest;
        using TLegacyResult = NSharedCache::NLegacy::TEvResult;
        using TLegacySaved = NSharedCache::NLegacy::TEvSaveCompactedPages;
        static_assert(ui32(NSharedCache::EvEnd) <= ui32(NSharedCache::NLegacy::EvTouch));
        static_assert(ui32(NSharedCache::NLegacy::EvEnd) <= ui32(NOps::EEv::Base_));
        static_assert(
            std::is_same_v<decltype(TLegacySaved::Pages), TVector<TIntrusivePtr<NSharedCache::NLegacy::TPage>>>);

        auto part = MakeRegistrationPart();
        const auto& collection = part->GetPageCollectionRef(0);
        const auto location = NTable::NPage::TPageLocation::FromPageIndex(0, 4, NTable::NPage::EPage::DataPage);
        NActors::IEventHandle native(
            {}, {}, new NSharedCache::TEvRequest(NBlockIO::EPriority::Fast, collection, { location }, 7));
        UNIT_ASSERT(native.CastAsLocal<NSharedCache::TEvRequest>());
        UNIT_ASSERT(!native.CastAsLocal<TLegacyRequest>());

        auto result = MakeHolder<TLegacyResult>(collection, NKikimrProto::OK, 19);
        auto waitPad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
        result->WaitPad = waitPad;
        auto page = NSharedCache::TSharedPageRef::MakePrivate(TSharedData::Copy("page", 4));
        page.SetType(location.Type);
        result->Pages.emplace_back(location.Offset, location.Size, std::move(page));
        NActors::IEventHandle legacy({}, {}, result.Release(), 0, 22);
        UNIT_ASSERT(!legacy.CastAsLocal<NSharedCache::TEvResult>());
        const auto* message = legacy.CastAsLocal<TLegacyResult>();
        UNIT_ASSERT(message);
        UNIT_ASSERT(!message->IsSerializable());
        UNIT_ASSERT(message->PageCollection.Get() == collection.Get());
        UNIT_ASSERT(message->WaitPad == waitPad);
        UNIT_ASSERT_VALUES_EQUAL(message->Cookie, 19);
        UNIT_ASSERT_VALUES_EQUAL(legacy.Cookie, 22);
        UNIT_ASSERT_VALUES_EQUAL(message->Bytes(), 4);
        UNIT_ASSERT(message->Pages.front().Page.GetType() == location.Type);
        const NSharedCache::TPinnedPageRef pinned(message->Pages.front().Page);
        UNIT_ASSERT_VALUES_EQUAL(pinned.GetData().Slice(), "page");

        const auto& protocol = GetLFPageCacheProtocol();
        UNIT_ASSERT(protocol.EventKind(legacy.GetTypeRewrite()) == EPageCacheEvent::Unhandled);
        TPageCacheMetadataResult metadata;
        UNIT_ASSERT(!protocol.ReadMetadataResult(legacy, metadata));

        const auto& privateProtocol = GetPrivatePageCacheProtocol();
        UNIT_ASSERT(privateProtocol.EventKind(NSharedCache::EvResult) == EPageCacheEvent::Unhandled);
        UNIT_ASSERT(privateProtocol.EventKind(NSharedCache::NLegacy::EvResult) == EPageCacheEvent::Result);
        UNIT_ASSERT(privateProtocol.EventKind(NSharedCache::NLegacy::EvUpdated) == EPageCacheEvent::Updated);
        UNIT_ASSERT(
            privateProtocol.EventKind(NSharedCache::NLegacy::EvStickyCollectionPages) == EPageCacheEvent::StickyPages);
        UNIT_ASSERT(!privateProtocol.MakeReadyRequest());
        UNIT_ASSERT(!privateProtocol.ReadMetadataResult(legacy, metadata));
        auto decoded = privateProtocol.ReadResult(*legacy.Get<TLegacyResult>());
        UNIT_ASSERT(decoded.Kind == EPageCacheResultKind::Pages);
        UNIT_ASSERT(decoded.Status == NKikimrProto::OK && decoded.PageCollection == collection.Get());
        UNIT_ASSERT(&decoded.WaitPad == &message->WaitPad);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Cookie, 19);
        UNIT_ASSERT_VALUES_EQUAL(decoded.PageCount, 1);

        auto request = privateProtocol.MakePageRequest(NBlockIO::EPriority::Bkgr, collection, { location }, 41);
        const auto* legacyRequest = CheckedCast<TLegacyRequest*>(request.Get());
        UNIT_ASSERT(legacyRequest->Priority == NBlockIO::EPriority::Bkgr);
        UNIT_ASSERT(legacyRequest->PageCollection == collection);
        UNIT_ASSERT(legacyRequest->Pages == TVector<NTable::NPage::TPageLocation>{ location });
        UNIT_ASSERT_VALUES_EQUAL(legacyRequest->Cookie, 41);
        auto update = privateProtocol.MakeCollectionUpdate(collection, NTable::NPage::ECacheMode::TryKeepInMemory,
            { NPageCollection::TBtreeSeed{ .Root = location } });
        const auto* attach = CheckedCast<NSharedCache::NLegacy::TEvAttach*>(update.Get());
        UNIT_ASSERT(
            attach->PageCollection == collection && attach->CacheMode == NTable::NPage::ECacheMode::TryKeepInMemory);
        UNIT_ASSERT_VALUES_EQUAL(attach->BtreeSeeds.size(), 1);
        UNIT_ASSERT(attach->BtreeSeeds.front().Root == location && !attach->ReplayStickyWalk);
        UNIT_ASSERT_VALUES_EQUAL(privateProtocol.MakeUnregisterRequest()->Type(), NSharedCache::NLegacy::EvUnregister);
    }

    Y_UNIT_TEST(LegacyCollectionKeepsPagesAndPendingRequestsByOffset) {
        using NSharedCache::NLegacy::TCollection;
        using NSharedCache::NLegacy::TPage;
        using NSharedCache::NLegacy::TRequest;
        using NTable::NPage::ECacheMode;
        using NTable::NPage::EPage;
        using NTable::NPage::TPageOffset;
        static_assert(!std::is_same_v<TCollection, NSharedCache::TCollection>);

        TCollection collection{};
        const auto offset = TPageOffset::FromPageIndex(7);
        auto page = MakeIntrusive<TPage>(offset, size_t{ 4 }, EPage::DataPage, ui32{ 0 }, &collection);
        auto* inserted = page.Get();
        UNIT_ASSERT(collection.PageSet.insert(std::move(page)).second);
        UNIT_ASSERT(!collection.PageSet
                         .insert(MakeIntrusive<TPage>(offset, size_t{ 8 }, EPage::FlatIndex, ui32{ 0 }, &collection))
                         .second);
        UNIT_ASSERT(collection.PageSet.FindPage(offset) == inserted);
        UNIT_ASSERT(collection.GetCacheMode() == ECacheMode::Regular);

        auto body = TSharedData::Copy("page", 4);
        const auto* bodyData = body.data();
        inserted->ProvideBody(std::move(body));
        auto request = MakeIntrusive<TRequest>();
        request->Sender = TActorId(1, "legacy-cache");
        request->RequestCookie = 19;
        request->ReadyPages.emplace_back(
            offset, 4, NSharedCache::TSharedPageRef::MakeUsed(TIntrusivePtr<NSharedCache::TSharedPageHandle>(inserted),
                           MakeIntrusive<NSharedCache::TSharedPageGCList>(), EPage::DataPage));
        UNIT_ASSERT(!inserted->UnUse());
        collection.InMemoryOwners.insert(request->Sender);
        auto& pending = collection.PendingRequests[offset];
        UNIT_ASSERT(pending.emplace(request, 0).second);
        request.Drop();
        UNIT_ASSERT(collection.GetCacheMode() == ECacheMode::TryKeepInMemory);

        auto& waiting = *pending.begin()->first;
        UNIT_ASSERT(!waiting.IsResponded());
        NSharedCache::NLegacy::TEvResult result({}, NKikimrProto::OK, waiting.RequestCookie);
        result.Pages = std::move(waiting.ReadyPages);
        waiting.MarkResponded();
        UNIT_ASSERT(waiting.IsResponded());
        UNIT_ASSERT_VALUES_EQUAL(result.Cookie, 19);
        UNIT_ASSERT(collection.PageSet.ErasePage(offset));
        UNIT_ASSERT(!collection.PageSet.FindPage(offset));
        UNIT_ASSERT(!collection.PageSet.ErasePage(offset));
        const NSharedCache::TPinnedPageRef pinned(result.Pages.front().Page);
        UNIT_ASSERT(pinned.GetData().data() == bodyData);
    }

    Y_UNIT_TEST(PrivatePartCloneRetainsOwnersAndDetachCopiesPageMaps) {
        auto part = MakePrivateRegistrationPart(true);
        TPrivatePageCache cache;
        for (const auto& collection : part->PageCollections) {
            cache.AddPageCollection(collection);
        }
        cache.AddPageCollection(part->Pseudo);
        const auto& regularCollection = part->PageCollections[part->DataSlot({})];
        const auto& stickyCollection = part->PageCollections[part->DataSlot(NTable::NPage::TGroupId(1))];
        const auto regularLocation = regularCollection->GetLocation(0);
        const auto stickyLocation = stickyCollection->GetLocation(0);
        const TString regularData(regularLocation.Size, 'r');
        const TString stickyData(stickyLocation.Size, 's');
        const auto regular = NSharedCache::TSharedPageRef::MakePrivate(TSharedData::Copy(regularData));
        const auto sticky = NSharedCache::TSharedPageRef::MakePrivate(TSharedData::Copy(stickyData));
        cache.AddPage(regularLocation.Offset, regularLocation.Size, regular, regularCollection.Get());
        cache.AddStickyPage(stickyLocation.Offset, stickyLocation.Size, sticky, stickyCollection.Get());

        const NTable::TPartStore& common = *part;
        const NTable::TEpoch epoch(i64(42));
        const auto clone = common.CloneWithEpoch(epoch);
        const auto* privateClone = CheckedCast<const NTable::TPrivatePartStore*>(clone.Get());
        UNIT_ASSERT(clone->Epoch == epoch);
        UNIT_ASSERT(privateClone->BundleId() == common.BundleId());
        UNIT_ASSERT_VALUES_EQUAL(privateClone->BackingSize(), common.BackingSize());
        UNIT_ASSERT_VALUES_EQUAL(privateClone->GetPageCollectionCount(), common.GetPageCollectionCount());
        for (ui32 slot = 0; slot < common.GetPageCollectionCount(); ++slot) {
            UNIT_ASSERT(privateClone->PageCollections[slot].Get() == part->PageCollections[slot].Get());
            UNIT_ASSERT(privateClone->GetPageCollectionRef(slot).Get() == common.GetPageCollectionRef(slot).Get());
        }
        UNIT_ASSERT(part->Pseudo && privateClone->Pseudo.Get() == part->Pseudo.Get());
        UNIT_ASSERT(privateClone->GetPseudoPageCollection() == common.GetPseudoPageCollection());

        const auto detached = cache.DetachPrivatePageCache();
        const auto& detachedRegular = detached.at(regularCollection->Id);
        const auto& detachedSticky = detached.at(stickyCollection->Id);
        UNIT_ASSERT(cache.FindPageCollection(regularCollection->Id) == regularCollection.Get());
        UNIT_ASSERT(detachedRegular.Get() != regularCollection.Get());
        UNIT_ASSERT(detachedSticky.Get() != stickyCollection.Get());
        UNIT_ASSERT(detachedRegular->PageCollection.Get() == regularCollection->PageCollection.Get());
        UNIT_ASSERT(detachedSticky->PageCollection.Get() == stickyCollection->PageCollection.Get());
        const auto* regularPage = detachedRegular->FindPage(regularLocation.Offset);
        const auto* stickyPage = detachedSticky->FindPage(stickyLocation.Offset);
        UNIT_ASSERT(regularPage && regularPage->SharedBody.GetHandle() == regular.GetHandle());
        UNIT_ASSERT(stickyPage && stickyPage->SharedBody.GetHandle() == sticky.GetHandle());
        UNIT_ASSERT(detachedSticky->IsStickyPage(stickyLocation.Offset));
        UNIT_ASSERT(regularPage->PageCollection == detachedRegular.Get());
        cache.DropPage(regularLocation.Offset, regularCollection.Get());
        UNIT_ASSERT(!regularCollection->FindPage(regularLocation.Offset));
        UNIT_ASSERT(detachedRegular->FindPage(regularLocation.Offset) == regularPage);
    }

    Y_UNIT_TEST(ProtocolKeepsReplyIdentityAndControlPriority) {
        auto backend = MakeLFExecutorPageCache();
        const auto& protocol = backend->Protocol();
        UNIT_ASSERT(&protocol == &GetLFPageCacheProtocol());
        UNIT_ASSERT(protocol.EventKind(NSharedCache::EvResult) == EPageCacheEvent::Result);
        UNIT_ASSERT(protocol.EventKind(NSharedCache::EvCacheReady) == EPageCacheEvent::Ready);
        UNIT_ASSERT(protocol.EventKind(NSharedCache::EvCollectionResult) == EPageCacheEvent::Unhandled);

        NSharedCache::TEvResult result({}, NKikimrProto::OK, 37, 19, 23, true);
        result.WaitPad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
        auto view = protocol.ReadResult(result);
        UNIT_ASSERT(view.Kind == EPageCacheResultKind::Pages);
        UNIT_ASSERT_VALUES_EQUAL(view.Cookie, 37);
        UNIT_ASSERT(&view.WaitPad == &result.WaitPad);
        view.WaitPad.Drop();
        UNIT_ASSERT(!result.WaitPad);
        UNIT_ASSERT(result.CoreRoute);
        UNIT_ASSERT_VALUES_EQUAL(result.ExecutorGeneration, 19);
        UNIT_ASSERT_VALUES_EQUAL(result.RequestId, 23);

        result.ResourcePressure = true;
        UNIT_ASSERT(protocol.ReadResult(result).Kind == EPageCacheResultKind::ResourcePressure);
        result.ResourcesReady = true;
        UNIT_ASSERT(protocol.ReadResult(result).Kind == EPageCacheResultKind::ResourcesReady);
        result.WorkingSetTooLarge = true;
        UNIT_ASSERT(protocol.ReadResult(result).Kind == EPageCacheResultKind::WorkingSetTooLarge);
        TStringStream description;
        protocol.ReadResult(result).Describe(description);
        UNIT_ASSERT_VALUES_EQUAL(description.Str(), "TEvResult{WorkingSetTooLarge}");
    }

    Y_UNIT_TEST(ProtocolPreservesRequestFieldsAndBorrowsMetadata) {
        auto backend = MakeLFExecutorPageCache();
        const auto& protocol = backend->Protocol();
        NTable::NTest::TLayoutCook layout;
        layout.Col(0, 0, NScheme::NTypeIds::Uint32).Key({ 0 });
        NTable::NPage::TConf conf;
        NTable::NTest::TPartCook cook(layout, conf);
        cook.AddN(ui32(1));
        auto eggs = cook.Finish();
        const auto& part = *eggs.Lone();
        auto descriptor = part.PageColls.front();
        const auto location = NTable::NTest::IndexTools::GetFirstPageLocation(part, {});
        auto request = protocol.MakePageRequest(NBlockIO::EPriority::Bkgr, descriptor, { location }, 41);
        auto* pages = CheckedCast<NSharedCache::TEvRequest*>(request.Get());
        UNIT_ASSERT(pages->Priority == NBlockIO::EPriority::Bkgr);
        UNIT_ASSERT(pages->PageCollection.Get() == descriptor.Get());
        UNIT_ASSERT_VALUES_EQUAL(pages->Pages.size(), 1);
        UNIT_ASSERT(pages->Pages.front() == location);
        UNIT_ASSERT_VALUES_EQUAL(pages->Cookie, 41);
        UNIT_ASSERT(pages->Sticky.empty());
        auto update = protocol.MakeCollectionUpdate(descriptor, NTable::NPage::ECacheMode::TryKeepInMemory,
            { NPageCollection::TBtreeSeed{ .Root = location } });
        const auto* attach = CheckedCast<NSharedCache::TEvAttach*>(update.Get());
        UNIT_ASSERT(attach->PageCollection.Get() == descriptor.Get());
        UNIT_ASSERT(attach->CacheMode == NTable::NPage::ECacheMode::TryKeepInMemory);
        UNIT_ASSERT_VALUES_EQUAL(attach->BtreeSeeds.size(), 1);
        UNIT_ASSERT(attach->BtreeSeeds.front().Root == location);
        UNIT_ASSERT(!attach->OwnerStats && !attach->ReplayStickyWalk);
        UNIT_ASSERT(protocol.MakeReadyRequest()->Type() == NSharedCache::TEvCacheReadyRequest::EventType);
        UNIT_ASSERT(backend->MakeUnregisterRequest()->Type() == NSharedCache::TEvUnregister::EventType);

        auto token = MakeIntrusive<NSharedCache::TCollectionRequest>(
            NPageCollection::TLargeGlobId(1, TLogoBlobID(1, 2, 3, 1, 1, 0)));
        NActors::IEventHandle metadataEvent(
            {}, {}, new NSharedCache::TEvCollectionResult(token, NKikimrProto::ERROR, {}, "metadata failed"));
        TPageCacheMetadataResult metadata;
        UNIT_ASSERT(protocol.ReadMetadataResult(metadataEvent, metadata));
        UNIT_ASSERT(metadata.Request == token.Get());
        UNIT_ASSERT_VALUES_EQUAL(metadata.CollectionId, token->Descriptor.Lead);
        UNIT_ASSERT(metadata.Status == NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(metadata.Error, "metadata failed");
        UNIT_ASSERT(metadata.Error.data() == metadataEvent.Get<NSharedCache::TEvCollectionResult>()->Error.data());
        NActors::IEventHandle unrelated({}, {}, new NSharedCache::TEvCacheReady);
        UNIT_ASSERT(!protocol.ReadMetadataResult(unrelated, metadata));
        UNIT_ASSERT(metadata.Request == token.Get());
    }

    Y_UNIT_TEST(RollbackClearsSharedEffects) {
        auto backend = MakeLFExecutorPageCache();
        auto seat = backend->MakeSeat(1, TAutoPtr<ITransaction>(new TReadTransaction));
        NTable::TDatabase database;
        ui32 calls = 0;
        backend->WithTransactionPages(database, *seat, [&](ITransactionPages& pages, TTransactionEnv& txEnv) {
            ++calls;
            database.Begin(1, pages);
            txEnv.BorrowUpdates[TLogoBlobID(1, 2, 3)].StoppedLoans.push_back(7);
            UNIT_ASSERT(txEnv.HasChanges());
            database.RollbackChanges();
            UNIT_ASSERT(!txEnv.HasChanges());
            database.Commit(1, false);
        });
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
    }

    Y_UNIT_TEST(CallbackCanDestroyItsSeat) {
        auto backend = MakeLFExecutorPageCache();
        auto seat = backend->MakeSeat(1, TAutoPtr<ITransaction>(new TReadTransaction));
        NTable::TDatabase database;
        backend->WithTransactionPages(database, *seat, [&](ITransactionPages& pages, TTransactionEnv&) {
            database.Begin(1, pages);
            database.Commit(1, false);
            seat.reset();
        });
        UNIT_ASSERT(!seat);
    }

    Y_UNIT_TEST(ReplyAcceptanceKeepsActiveCollectionAlive) {
        auto backend = MakeLFExecutorPageCache();
        const auto part = MakeRegistrationPart();
        const auto& owner = part->PageCollections.front();
        const std::pair<NKikimrProto::EReplyStatus, EPageResultStatus> cases[] = {
            { NKikimrProto::OK, EPageResultStatus::Ready },
            { NKikimrProto::NODATA, EPageResultStatus::Error },
            { NKikimrProto::RACE, EPageResultStatus::Ignored },
        };
        for (const auto& [status, expected] : cases) {
            NSharedCache::TEvResult event(owner->PageCollection(), status, 0, 0, 0, status == NKikimrProto::OK);
            bool called = false;
            UNIT_ASSERT(owner.IsSoleReference());
            backend->ProcessLoadedPages(event, {}, [&](EPageResultStatus result) {
                UNIT_ASSERT(!called);
                called = true;
                UNIT_ASSERT(result == expected);
                UNIT_ASSERT(owner.IsSoleReference() == (result == EPageResultStatus::Ignored));
            });
            UNIT_ASSERT(called);
            UNIT_ASSERT(owner.IsSoleReference());
        }

        NSharedCache::TEvResult unrouted(owner->PageCollection(), NKikimrProto::OK, 0);
        bool called = false;
        UNIT_ASSERT_EXCEPTION(backend->ProcessLoadedPages(unrouted, { .Sticky = true }, [&](EPageResultStatus) {
            called = true;
        }), yexception);
        UNIT_ASSERT(!called);
        UNIT_ASSERT(owner.IsSoleReference());

        backend->ProcessLoadedPages(unrouted, { .AbandonedTransaction = true }, [&](EPageResultStatus result) {
            UNIT_ASSERT(result == EPageResultStatus::Ignored && owner.IsSoleReference());
        });
    }

    Y_UNIT_TEST(LoadedPagesKeepCacheOwnersUntilRelease) {
        auto backend = MakeLFExecutorPageCache();
        auto seat = backend->MakeSeat(1, TAutoPtr<ITransaction>(new TReadTransaction));
        TPageCollectionReadEnv pages(*CheckedCast<TCacheSeat*>(seat.get()));
        for (bool sticky : { false, true }) {
            UNIT_ASSERT(!backend->AdoptLoadedPages(*seat, nullptr));
            NTable::NTest::TLayoutCook layout;
            layout.Col(0, 0, NScheme::NTypeIds::Uint32).Key({ 0 });
            NTable::NPage::TConf conf;
            NTable::NTest::TPartCook cook(layout, conf);
            cook.AddN(ui32(1));
            auto eggs = cook.Finish();
            const auto& part = *eggs.Lone();
            const auto location = NTable::NTest::IndexTools::GetFirstPageLocation(part, {});
            auto owner = NTable::NTest::AdmitPage(TCache::Get(), part.PageColls.front(), location,
                TSharedData(*part.Store->GetPage(0, location.Offset)), sticky);
            UNIT_ASSERT(owner);
            const auto* data = &owner.GetData();
            NSharedCache::TEvResult loaded(part.PageColls.front(), NKikimrProto::OK, 0);
            loaded.Pages.emplace_back(location.Offset, location.Size, std::move(owner));
            const auto decoded = backend->Protocol().ReadResult(loaded);
            UNIT_ASSERT_VALUES_EQUAL(decoded.PageCount, 1);
            UNIT_ASSERT(loaded.Pages.front().Page);
            UNIT_ASSERT(backend->AdoptLoadedPages(*seat, &loaded));
            UNIT_ASSERT(!loaded.Pages.front().Page);
            UNIT_ASSERT(backend->AdoptLoadedPages(*seat, nullptr));
            auto waitPad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
            UNIT_ASSERT(backend->PreparePageRequests(*seat, pages, *waitPad).empty());
            UNIT_ASSERT_VALUES_EQUAL(waitPad->WorkingSetPages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(waitPad->WorkingSetPages.at(part.PageColls.front()->Label()).size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(waitPad->WorkingSetBytes, sticky ? 0 : location.Size + TSharedData::OverheadSize);
            UNIT_ASSERT(waitPad->HasPinnedPages.load(std::memory_order_acquire));
            UNIT_ASSERT_VALUES_EQUAL(data->size(), location.Size);
            backend->ReleasePins(*seat);
            auto released = MakeIntrusive<NPageCollection::TPagesWaitPad>();
            UNIT_ASSERT(backend->PreparePageRequests(*seat, pages, *released).empty());
            UNIT_ASSERT(released->WorkingSetPages.empty());
            UNIT_ASSERT(!released->HasPinnedPages.load(std::memory_order_acquire));
        }
    }
}

Y_UNIT_TEST_SUITE(TPrivateExecutorPageCache) {
    namespace NLegacy = NSharedCache::NLegacy;

    Y_UNIT_TEST(RefreshAndRestartPreservePrivateRegistration) {
        auto backend = MakePrivateExecutorPageCache();
        UNIT_ASSERT(backend->Backend() == EPageCacheBackend::Legacy);
        TPageCacheStats stats;
        UNIT_ASSERT(!backend->ReadStats(stats));
        UNIT_ASSERT(!backend->PrepareRegistration());

        const auto part = MakePrivateRegistrationPart(true);
        const ui32 count = part->GetPageCollectionCount();
        const auto descriptorSlot = part->DescriptorSlot();
        const auto dataSlot = part->DataSlot({});
        const auto& data = part->PageCollections[dataSlot];
        const auto location = NTable::NPage::TPageLocation::FromByteOffset(0, 4, EPage::DataPage);
        auto body = TSharedData::Copy("body", 4);
        data->AddPage(location.Offset, location.Size, NSharedCache::TSharedPageRef::MakePrivate(body));

        TVector<bool> stickyGroups(part->GroupsCount, true);
        TVector<ECacheMode> modes(count, ECacheMode::Regular);
        backend->PrepareCollectionModes(*part, modes, stickyGroups);
        TVector<THolder<NActors::IEventBase>> events;
        const auto send = [&](THolder<NActors::IEventBase> event) {
            events.push_back(std::move(event));
        };
        for (ui32 slot : xrange(count)) {
            backend->RegisterCollection(
                *part, slot, modes[slot], {}, slot == dataSlot, [&](THolder<NActors::IEventBase> event) {
                    UNIT_ASSERT(backend->ReadStats(stats));
                    UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, slot + 1);
                    send(std::move(event));
                });
        }
        backend->RegisterPseudoCollection(*part, send);
        UNIT_ASSERT_VALUES_EQUAL(events.size(), count + 2);
        const auto& attach = *CheckedCast<NLegacy::TEvAttach*>(events[dataSlot].Get());
        UNIT_ASSERT(attach.ReplayStickyWalk && attach.PageCollection == data->PageCollection);
        const auto& sync = *CheckedCast<NLegacy::TEvSync*>(events[dataSlot + 1].Get());
        UNIT_ASSERT(sync.Pages.at(data->Id).contains(location.Offset));

        modes[descriptorSlot] = ECacheMode::TryKeepInMemory;
        const auto toUpdate = backend->UpdateCollectionModes(*part, modes, stickyGroups);
        UNIT_ASSERT(toUpdate[descriptorSlot]);
        for (ui32 group : xrange(part->GroupsCount)) {
            const NTable::NPage::TGroupId groupId(group);
            UNIT_ASSERT_VALUES_EQUAL(part->IndexSlot(groupId), descriptorSlot);
            UNIT_ASSERT(toUpdate[part->DataSlot(groupId)]);
            UNIT_ASSERT(modes[part->DataSlot(groupId)] == ECacheMode::Regular);
        }
        events.clear();
        backend->UpdateAttachment(*part, dataSlot, modes[dataSlot], {}, EAttachmentUpdate::StickyPages, send);
        UNIT_ASSERT(events.empty());
        backend->UpdateAttachment(*part, dataSlot, modes[dataSlot], {}, EAttachmentUpdate::CacheMode, send);
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(events.front()->Type(), NLegacy::EvAttach);
        UNIT_ASSERT(backend->ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, count + 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.SharedBodyBytes, location.Size);
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, part->GetPageCollection(descriptorSlot)->BackingSize());

        THolder<IPageCacheRestartState> restart;
        backend->CaptureForRestart(restart);
        UNIT_ASSERT(restart);
        backend->DetachCollection(data->Id);
        UNIT_ASSERT(!data->FindPage(location.Offset));
        TVector<NPageCollection::TLargeGlobId> ids(count + 1);
        for (ui32 slot : xrange(count)) {
            ids[slot].Lead = part->PageCollections[slot]->Id;
        }
        // The fixture uses an empty label for its Pseudo collection.
        ids.back().Lead = TLogoBlobID(77, 1, 1, 0, 1, 0);
        auto environment = backend->MakeLoaderEnvironment(ids, restart.Get());
        restart.Reset();
        environment->Initialize(part->Slots);
        UNIT_ASSERT(!environment->HasCollection(count));
        const auto* saved = environment->TryGetSlotPage(dataSlot, location);
        UNIT_ASSERT(saved && saved->data() == body.data());

        THolder<IPageCacheLoadedCollections> loaded;
        backend->CaptureForRestart(*part, loaded);
        backend->MergeForRestart(restart, loaded.Get());
        ids.back().Lead = part->Pseudo->Id;
        auto merged = backend->MakeLoaderEnvironment(ids, restart.Get());
        loaded.Reset();
        restart.Reset();
        UNIT_ASSERT(merged->GetCollection(count) == part->Pseudo->PageCollection);
        UNIT_ASSERT(backend->PrepareRegistration());
        UNIT_ASSERT(backend->ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, 0);
    }

    Y_UNIT_TEST(TransactionRepliesRetainPrivatePinsAcrossEviction) {
        auto backend = MakePrivateExecutorPageCache();
        UNIT_ASSERT(!backend->PrepareRegistration());
        const auto part = MakePrivateRegistrationPart();
        const auto dataSlot = part->DataSlot({});
        const auto& owner = part->PageCollections[dataSlot];
        backend->RegisterCollection(*part, dataSlot, ECacheMode::Regular, {}, false, [](THolder<NActors::IEventBase>) {
        });
        auto seat = backend->MakeSeat(1, TAutoPtr<ITransaction>(new TReadTransaction));
        NTable::TDatabase database;
        const auto regular = NTable::NPage::TPageLocation::FromByteOffset(0, 4, EPage::DataPage);
        const auto sticky = NTable::NPage::TPageLocation::FromByteOffset(4, 4, EPage::DataPage);
        backend->WithTransactionPages(database, *seat, [&](ITransactionPages& pages, TTransactionEnv&) {
            UNIT_ASSERT(!pages.TryGetPage(part.Get(), regular, {}));
            UNIT_ASSERT(!pages.TryGetPage(part.Get(), regular, {}));
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 1);
            auto requests = pages.ObtainToLoad();
            auto waitPad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
            backend->SendTransactionRequest(
                owner->Id, std::move(requests.at(owner->Id)), waitPad, {}, [&](THolder<NActors::IEventBase> event) {
                    const auto& request = *CheckedCast<NLegacy::TEvRequest*>(event.Get());
                    UNIT_ASSERT(request.Priority == NBlockIO::EPriority::Fast);
                    UNIT_ASSERT(request.PageCollection == owner->PageCollection && request.WaitPad == waitPad);
                    UNIT_ASSERT_VALUES_EQUAL(request.Pages.size(), 1);
                    UNIT_ASSERT(request.Pages.front() == regular);
                });
        });

        auto body = TSharedData::Copy("body", 4);
        NLegacy::TEvResult reply(owner->PageCollection, NKikimrProto::OK, 0);
        reply.Pages.emplace_back(regular.Offset, regular.Size, NSharedCache::TSharedPageRef::MakePrivate(body));
        backend->ProcessLoadedPages(reply, { .AbandonedTransaction = true }, [&](EPageResultStatus status) {
            UNIT_ASSERT(status == EPageResultStatus::Ready);
            UNIT_ASSERT(owner->FindPage(regular.Offset));
        });
        UNIT_ASSERT(reply.Pages.front().Page);
        UNIT_ASSERT(backend->AdoptLoadedPages(*seat, &reply));
        UNIT_ASSERT(!reply.Pages.front().Page);

        NLegacy::TEvStickyCollectionPages notice(owner->Id, { sticky });
        auto request = backend->MakeStickyPageRequest(notice);
        UNIT_ASSERT(request);
        UNIT_ASSERT(CheckedCast<NLegacy::TEvRequest*>(request.Get())->Priority == NBlockIO::EPriority::Bkgr);
        NLegacy::TEvResult stickyReply(owner->PageCollection, NKikimrProto::OK, 0);
        stickyReply.Pages.emplace_back(sticky.Offset, sticky.Size, NSharedCache::TSharedPageRef::MakePrivate(body));
        backend->ProcessLoadedPages(stickyReply, { .Sticky = true }, [&](EPageResultStatus status) {
            UNIT_ASSERT(status == EPageResultStatus::Ready && owner->IsStickyPage(sticky.Offset));
        });

        NLegacy::TEvUpdated update;
        update.DroppedPages[owner->Id].insert(regular.Offset);
        backend->ProcessCacheUpdate(update);
        UNIT_ASSERT(!owner->FindPage(regular.Offset));
        backend->WithTransactionPages(database, *seat, [&](ITransactionPages& pages, TTransactionEnv&) {
            const auto* retained = pages.TryGetPage(part.Get(), regular, {});
            UNIT_ASSERT(retained && retained->data() == body.data());
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedPages, 0);
            UNIT_ASSERT(pages.TryGetPage(part.Get(), sticky, {}));
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedPages, 1);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedBytes, 0);
            backend->ReleasePins(*seat);
            UNIT_ASSERT(!backend->AdoptLoadedPages(*seat, nullptr));
            seat.reset();
        });

        NLegacy::TEvResult race(owner->PageCollection, NKikimrProto::RACE, 0);
        backend->ProcessLoadedPages(race, {}, [](EPageResultStatus status) {
            UNIT_ASSERT(status == EPageResultStatus::Error);
        });
        NLegacy::TEvResult error(owner->PageCollection, NKikimrProto::NODATA, 0);
        const auto references = owner.RefCount();
        backend->ProcessLoadedPages(error, { .AbandonedTransaction = true }, [&](EPageResultStatus status) {
            UNIT_ASSERT(status == EPageResultStatus::Error);
            auto detach = backend->DetachCollection(owner->Id);
            UNIT_ASSERT_VALUES_EQUAL(detach->Type(), NLegacy::EvDetach);
            UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), references);
        });
        UNIT_ASSERT_VALUES_EQUAL(owner.RefCount(), references - 1);
        UNIT_ASSERT(!backend->MakeStickyPageRequest(notice));
        backend->ProcessLoadedPages(race, {}, [](EPageResultStatus status) {
            UNIT_ASSERT(status == EPageResultStatus::Ignored);
        });
    }
}

} // namespace NKikimr::NTabletFlatExecutor
