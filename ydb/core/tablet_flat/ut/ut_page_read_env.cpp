#include <ydb/core/tablet_flat/flat_exec_seat.h>
#include <ydb/core/tablet_flat/flat_sausagecache.h>
#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_part_store_cache.h>
#include <ydb/core/tablet_flat/flat_part_store.h>
#include <ydb/core/tablet_flat/flat_executor_tx_env.h>
#include <ydb/core/tablet_flat/flat_writer_blocks.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tablet_flat/shared_cache_pages.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_dbase_sz_env.h>
#include <ydb/core/tablet_flat/test/libs/table/cache_fixture.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/testlib/helpers.h>

#include <type_traits>

namespace NKikimr::NTabletFlatExecutor {
namespace {

    class TReadTransaction final : public ITransaction {
        bool Execute(TTransactionContext&, const NActors::TActorContext&) override {
            return true;
        }

        void Complete(const NActors::TActorContext&) override {}
    };

    class TReadPageCollection final : public NTable::NTest::TStorePageCollection {
    public:
        TReadPageCollection(TIntrusiveConstPtr<NTable::NTest::TStore> store, ui32 room, TLogoBlobID label)
            : TStorePageCollection(std::move(store), room)
            , Label_(label)
        {}

        const TLogoBlobID& Label() const noexcept override {
            return Label_;
        }

    private:
        const TLogoBlobID Label_;
    };

    template <bool LockFree = false>
    struct TReadFixture {
        using TStore = std::conditional_t<LockFree, NTable::TCachePartStore, NTable::TPrivatePartStore>;

        explicit TReadFixture(bool registerCollections = true) {
            NTable::NTest::TLayoutCook layout;
            layout.Col(0, 0, NScheme::NTypeIds::Uint32).Col(0, 1, NScheme::NTypeIds::String).Key({ 0 });
            NTable::NPage::TConf conf;
            conf.Group(0).PageSize = 128;
            conf.SmallEdge = 8;
            conf.LargeEdge = 128;
            NTable::NTest::TPartCook cook(layout, conf);
            for (ui32 key : xrange(32)) {
                cook.AddN(key, TString(32, 'x'));
            }
            Source = cook.Finish().Lone();
            NTable::TPart::TParams params{
                .Epoch = Source->Epoch,
                .Scheme = Source->Scheme,
                .IndexPages = Source->IndexPages,
                .Blobs = Source->Blobs,
                .ByKeyPrefixes = Source->ByKeyPrefixes,
                .Large = Source->Large,
                .Small = Source->Small,
                .IndexesRawSize = Source->IndexesRawSize,
                .MinRowVersion = Source->MinRowVersion,
                .MaxRowVersion = Source->MaxRowVersion,
                .GarbageStats = Source->GarbageStats,
                .TxIdStats = Source->TxIdStats,
                .Layout = Source->Slots.GetLayout(),
            };
            Part = MakeHolder<TStore>(Source->Label, std::move(params), Source->Stat);
            for (ui32 room : xrange(Source->PageColls.size())) {
                const auto& label = Source->Label;
                auto descriptor = MakeIntrusive<TReadPageCollection>(
                    Source->Store, room, TLogoBlobID(label.TabletID(), label.Generation(), label.Step(),
                                             label.Channel(), label.BlobSize(), label.Cookie() + room));
                if constexpr (LockFree) {
                    Part->PageCollections.push_back(
                        NTable::NTest::AdmitCollection(TCache::Get(), std::move(descriptor)));
                } else {
                    Part->PageCollections.push_back(
                        MakeIntrusive<TPrivatePageCache::TPageCollection>(std::move(descriptor)));
                }
            }
            const ui32 slot = Part->DataSlot({});
            for (ui32 id : xrange(Source->Store->PageCollectionPagesCount(slot))) {
                if (Source->Store->GetPageType(slot, id) == NTable::NPage::EPage::DataPage) {
                    Locations.push_back(Source->Store->GetPageLocation(slot, id));
                }
            }
            UNIT_ASSERT(Locations.size() >= 3);
            UNIT_ASSERT(Part->Small && Part->OuterSlot() < Part->PageCollections.size());
            if (registerCollections) {
                UNIT_ASSERT(!Backend->PrepareRegistration());
                Register();
            }
        }

        void Register() {
            for (ui32 slot : xrange(Part->GetPageCollectionCount())) {
                auto mode = ECacheMode::Regular;
                if constexpr (!LockFree) {
                    mode = Part->PageCollections[slot]->GetCacheMode();
                }
                Backend->RegisterCollection(*Part, slot, mode, {}, false, [](THolder<NActors::IEventBase>) {
                });
            }
        }

        auto Load(const NTable::TPageLocation& location) {
            const auto slot = Part->DataSlot({});
            const auto& bytes = *Source->Store->GetPage(slot, location.Offset);
            if constexpr (LockFree) {
                auto page = NTable::NTest::AdmitPage(
                    TCache::Get(), Part->GetPageCollectionRef(slot), location, TSharedData(bytes));
                UNIT_ASSERT(page);
                return page;
            } else {
                auto page = NSharedCache::TSharedPageRef::MakePrivate(bytes);
                NSharedCache::NLegacy::TEvResult result(Part->GetPageCollectionRef(slot), NKikimrProto::OK, 0);
                result.Pages.emplace_back(location.Offset, location.Size, page);
                Backend->ProcessLoadedPages(result, {}, [](EPageResultStatus status) {
                    UNIT_ASSERT(status == EPageResultStatus::Ready);
                });
                return page;
            }
        }

        THolder<IExecutorPageCache> Backend = LockFree ? MakeLFExecutorPageCache() : MakePrivateExecutorPageCache();
        std::unique_ptr<TSeat> Seat = Backend->MakeSeat(1, TAutoPtr<ITransaction>(new TReadTransaction));
        NTable::TDatabase Database;
        TIntrusiveConstPtr<NTable::NTest::TPartStore> Source;
        THolder<TStore> Part;
        TVector<NTable::TPageLocation> Locations;
    };

} // namespace

Y_UNIT_TEST_SUITE_F(TPageReadEnv, NTable::NTest::TTableCacheTest) {
    Y_UNIT_TEST_TWIN(BackendSupportsPrebootSeatsAndSynchronousFrames, LockFree) {
        auto backend = LockFree ? MakeLFExecutorPageCache() : MakePrivateExecutorPageCache();
        auto& pageCache = *backend;
        auto seat = pageCache.MakeSeat(1, TAutoPtr<ITransaction>(new TReadTransaction));
        UNIT_ASSERT(seat);
        pageCache.ReleasePins(*seat);
        TPageCacheStats stats;
        UNIT_ASSERT(!pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(bool(pageCache.Protocol().MakeReadyRequest()), LockFree);
        UNIT_ASSERT_VALUES_EQUAL(bool(pageCache.MakeMetadataWorkingSet()), LockFree);

        NTable::TDatabase database;
        ui32 calls = 0;
        for (ui32 attempt : xrange(2)) {
            UNIT_ASSERT_VALUES_EQUAL(pageCache.PrepareRegistration(), attempt != 0);
            UNIT_ASSERT(pageCache.ReadStats(stats));
            UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, 0);
            pageCache.WithTransactionPages(database, *seat, [&](ITransactionPages& pages, TTransactionEnv& txEnv) {
                ++calls;
                database.Begin(1, pages);
                IExecuting& executing = txEnv;
                executing.CleanupLoan(TLogoBlobID(1, 2, 3), 7);
                UNIT_ASSERT(txEnv.HasChanges());
                database.RollbackChanges();
                UNIT_ASSERT(!txEnv.HasChanges());
                database.Commit(1, false);
                if (attempt == 1) {
                    seat.reset();
                }
            });
        }
        UNIT_ASSERT_VALUES_EQUAL(calls, 2);
        UNIT_ASSERT(!seat);
    }

    Y_UNIT_TEST_TWIN(SizeCountingDeduplicatesSharedDescriptors, LockFree) {
        TReadFixture<LockFree> f;
        f.Backend->WithTransactionPages(f.Database, *f.Seat, [&](ITransactionPages& pages, TTransactionEnv&) {
            const ui32 slot = f.Part->DataSlot({});
            const auto& location = f.Locations.front();
            const NTable::TPartStore& common = *f.Part;
            const auto* descriptor = common.GetPageCollection(slot);
            UNIT_ASSERT(descriptor == common.GetPageCollectionRef(slot).Get());
            NTable::TSizeEnv env(&pages);
            UNIT_ASSERT(!env.TryGetPage(f.Part.Get(), location, {}));
            UNIT_ASSERT(!env.TryGetPage(f.Part.Get(), location, {}));
            UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);

            auto clone = common.CloneWithEpoch(common.Epoch);
            UNIT_ASSERT(!env.TryGetPage(clone.Get(), location, {}));
            UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);
            if constexpr (!LockFree) {
                TReadFixture<> separatePart;
                auto separate = MakeIntrusive<TPrivatePageCache::TPageCollection>(common.GetPageCollectionRef(slot));
                separatePart.Part->PageCollections[slot] = separate;
                UNIT_ASSERT(separate->PageCollection.Get() == descriptor);
                UNIT_ASSERT(!env.TryGetPage(separatePart.Part.Get(), location, {}));
                UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);
            }
        });
    }

    Y_UNIT_TEST_TWIN(MissingPagesAreDeduplicatedAndSorted, LockFree) {
        TReadFixture<LockFree> f;
        f.Backend->WithTransactionPages(f.Database, *f.Seat, [&](ITransactionPages& pages, TTransactionEnv&) {
            for (ui32 index : { 2, 0, 1, 2, 0 }) {
                UNIT_ASSERT(!pages.TryGetPage(f.Part.Get(), f.Locations[index], {}));
            }
            const ui64 bytes = f.Locations[0].Size + f.Locations[1].Size + f.Locations[2].Size;
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 3);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadBytes, bytes);
            const auto requests = pages.ObtainToLoad();
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
            const auto& locations = requests.at(f.Part->GetPageCollection(f.Part->DataSlot({}))->Label());
            UNIT_ASSERT_VALUES_EQUAL(locations.size(), 3);
            for (ui32 index : xrange(3)) {
                UNIT_ASSERT(locations[index].Offset == f.Locations[index].Offset);
            }
            UNIT_ASSERT(pages.ObtainToLoad().empty());
            UNIT_ASSERT(!pages.TryGetPage(f.Part.Get(), f.Locations[0], {}));
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 4);
        });
    }

    Y_UNIT_TEST_TWIN(MissingReferenceControlsPreserveNeedAndResetBytes, LockFree) {
        TReadFixture<LockFree> f;
        f.Backend->WithTransactionPages(f.Database, *f.Seat, [&](ITransactionPages& pages, TTransactionEnv&) {
            pages.EnableReadMissingReferences();
            const ui64 bytes = Max<ui64>(1, f.Part->GetPageSize(NTable::ELargeObj::Outer, 0));
            for (ui32 call = 1; call <= 2; ++call) {
                const auto result = pages.Locate(f.Part.Get(), 0, NTable::ELargeObj::Outer);
                UNIT_ASSERT(!result.Need && !result.Page);
                UNIT_ASSERT_VALUES_EQUAL(pages.MissingReferencesSize(), call * bytes);
            }
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 1);
            pages.DisableReadMissingReferences();
            UNIT_ASSERT_VALUES_EQUAL(pages.MissingReferencesSize(), 0);
            const auto result = pages.Locate(f.Part.Get(), 0, NTable::ELargeObj::Outer);
            UNIT_ASSERT(result.Need && !result.Page);
            UNIT_ASSERT_VALUES_EQUAL(pages.MissingReferencesSize(), 0);
        });
    }

    Y_UNIT_TEST_TWIN(TransactionRollbackClearsEffectsAndPreservesReadState, LockFree) {
        TReadFixture<LockFree> f;
        const auto& location = f.Locations.front();
        auto owner = f.Load(location);
        auto& database = f.Database;
        f.Backend->WithTransactionPages(database, *f.Seat, [&](ITransactionPages& pages, TTransactionEnv& env) {
            IExecuting& executing = env;
            database.Begin(1, pages);
            const auto* data = pages.TryGetPage(f.Part.Get(), location, {});
            UNIT_ASSERT(data);
            executing.CleanupLoan(f.Part->Label, 7);
            UNIT_ASSERT(env.HasChanges());
            executing.EnableReadMissingReferences();
            const auto missing = pages.Locate(f.Part.Get(), 0, NTable::ELargeObj::Outer);
            UNIT_ASSERT(!missing.Need && !missing.Page);
            const auto missingBytes = executing.MissingReferencesSize();
            UNIT_ASSERT(missingBytes > 0);

            database.RollbackChanges();
            UNIT_ASSERT(!env.HasChanges());
            UNIT_ASSERT(pages.TryGetPage(f.Part.Get(), location, {}) == data);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedPages, 1);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedBytes, location.Size);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 1);
            UNIT_ASSERT_VALUES_EQUAL(executing.MissingReferencesSize(), missingBytes);
            executing.DisableReadMissingReferences();
            UNIT_ASSERT_VALUES_EQUAL(pages.MissingReferencesSize(), 0);
            database.Commit(1, false);
        });
    }

    Y_UNIT_TEST(BackendModesPreserveLegacyAccountingAndOuterMode) {
        TReadFixture<> f(false);
        auto& pageCache = *f.Backend;
        UNIT_ASSERT(!pageCache.PrepareRegistration());
        const ui32 dataSlot = f.Part->DataSlot({});
        const ui32 outerSlot = f.Part->OuterSlot();
        const ui32 count = f.Part->GetPageCollectionCount();
        f.Part->PageCollections[outerSlot]->SetCacheMode(ECacheMode::TryKeepInMemory);
        TVector<bool> stickyGroups(f.Part->GroupsCount, true);
        TVector<ECacheMode> modes(count, ECacheMode::Regular);
        modes[dataSlot] = ECacheMode::TryKeepInMemory;
        pageCache.ReadCollectionModes(*f.Part, modes, stickyGroups);
        UNIT_ASSERT(modes[dataSlot] == ECacheMode::Regular);
        UNIT_ASSERT(modes[outerSlot] == ECacheMode::TryKeepInMemory);
        modes[dataSlot] = ECacheMode::TryKeepInMemory;
        modes[outerSlot] = ECacheMode::Regular;
        pageCache.PrepareCollectionModes(*f.Part, modes, stickyGroups);
        UNIT_ASSERT(modes[dataSlot] == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(modes[outerSlot] == ECacheMode::TryKeepInMemory);
        TPageCacheStats stats;
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, 0);
        const auto send = [](THolder<IEventBase>) {
        };
        for (ui32 slot : xrange(count)) {
            pageCache.RegisterCollection(*f.Part, slot, modes[slot], {}, false, send);
        }
        const ui64 dataBytes = f.Part->GetPageCollection(dataSlot)->BackingSize();
        const ui64 outerBytes = f.Part->GetPageCollection(outerSlot)->BackingSize();
        UNIT_ASSERT(dataBytes > 0 && outerBytes > 0);
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, dataBytes + outerBytes);

        modes.assign(count, ECacheMode::Regular);
        auto changed = pageCache.UpdateCollectionModes(*f.Part, modes, stickyGroups);
        UNIT_ASSERT_VALUES_EQUAL(changed.size(), count);
        for (ui32 slot : xrange(count)) {
            UNIT_ASSERT_VALUES_EQUAL(changed[slot], slot == dataSlot);
        }
        UNIT_ASSERT(modes[dataSlot] == ECacheMode::Regular);
        UNIT_ASSERT(modes[outerSlot] == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, outerBytes);
        modes.assign(count, ECacheMode::Regular);
        changed = pageCache.UpdateCollectionModes(*f.Part, modes, stickyGroups);
        for (bool value : changed) {
            UNIT_ASSERT(!value);
        }
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, outerBytes);
    }

    Y_UNIT_TEST(PinsAreReusedAcrossTransactionsWithoutExtraHeat) {
        TReadFixture<> f;
        const auto& location = f.Locations.front();
        const auto owner = f.Load(location);
        const auto startFrequency = owner.GetHandle()->GetFrequency();
        const TSharedData* first = nullptr;
        ui32 pinnedFrequency = 0;
        f.Backend->WithTransactionPages(f.Database, *f.Seat, [&](ITransactionPages& pages, TTransactionEnv&) {
            first = pages.TryGetPage(f.Part.Get(), location, {});
            UNIT_ASSERT(first);
            UNIT_ASSERT_VALUES_EQUAL(first->size(), location.Size);
            pinnedFrequency = owner.GetHandle()->GetFrequency();
            UNIT_ASSERT(pinnedFrequency == startFrequency || pinnedFrequency == startFrequency + 1);
            UNIT_ASSERT(pages.TryGetPage(f.Part.Get(), location, {}) == first);
            UNIT_ASSERT_VALUES_EQUAL(owner.GetHandle()->GetFrequency(), pinnedFrequency);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedPages, 1);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedBytes, location.Size);
        });

        const auto& collection = f.Part->PageCollections[f.Part->DataSlot({})];
        NSharedCache::NLegacy::TEvUpdated update;
        update.DroppedPages[collection->Id].insert(location.Offset);
        f.Backend->ProcessCacheUpdate(update);
        UNIT_ASSERT(!collection->FindPage(location.Offset));
        f.Backend->WithTransactionPages(f.Database, *f.Seat, [&](ITransactionPages& pages, TTransactionEnv&) {
            UNIT_ASSERT(pages.TryGetPage(f.Part.Get(), location, {}) == first);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedPages, 0);
            UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedBytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(owner.GetHandle()->GetFrequency(), pinnedFrequency);
        });
    }

    Y_UNIT_TEST(RestartMergeReusesLoadedOwnersAndKeepsNewestDescriptor) {
        TReadFixture<> prior;
        TReadFixture<> newer(false);
        const auto slot = prior.Part->DataSlot({});
        const auto& earlier = prior.Part->PageCollections[slot];
        const auto& latest = newer.Part->PageCollections[slot];
        UNIT_ASSERT(earlier->Id == latest->Id && earlier->PageCollection != latest->PageCollection);
        TVector<NPageCollection::TLargeGlobId> ids(1);
        ids[0].Lead = earlier->Id;

        TExecutorCaches saved;
        auto txStatus = TSharedData::Copy("tx", 2);
        saved.TxStatusCaches[earlier->Id] = txStatus;
        prior.Backend->CaptureForRestart(saved.PageCollections);
        const auto* restart = saved.PageCollections.Get();
        UNIT_ASSERT(restart);
        auto initial = prior.Backend->MakeLoaderEnvironment(ids, restart);
        UNIT_ASSERT(initial->GetCollection(0) == earlier->PageCollection);

        THolder<IPageCacheLoadedCollections> loaded;
        newer.Backend->CaptureForRestart(*newer.Part, loaded);
        prior.Backend->MergeForRestart(saved.PageCollections, loaded.Get());
        THolder<IPageCacheRestartState> duplicate;
        prior.Backend->MergeForRestart(duplicate, loaded.Get());
        auto latestEnv = prior.Backend->MakeLoaderEnvironment(ids, saved.PageCollections.Get());
        auto duplicateEnv = prior.Backend->MakeLoaderEnvironment(ids, duplicate.Get());
        UNIT_ASSERT(latestEnv->GetCollection(0) == latest->PageCollection);
        UNIT_ASSERT(duplicateEnv->GetCollection(0) == latest->PageCollection);

        prior.Backend->MergeForRestart(saved.PageCollections, nullptr);
        newer.Backend->CaptureForRestart(saved.PageCollections);
        UNIT_ASSERT(saved.PageCollections.Get() == restart);
        UNIT_ASSERT(saved.TxStatusCaches.at(earlier->Id).data() == txStatus.data());
    }

    Y_UNIT_TEST(WrittenOuterPageTransfersItsBuffer) {
        TReadFixture<> f(false);
        const auto outerSlot = f.Part->OuterSlot();
        const auto location = f.Source->Store->GetPageLocation(outerSlot, 0);
        TVector<NWriter::TWrittenPageCollection> written(2);
        written[0].PageCollection = f.Part->GetPageCollectionRef(f.Part->DataSlot({}));
        written[1].PageCollection = f.Part->GetPageCollectionRef(outerSlot);
        written[1].RegularPages.emplace_back(location, *f.Source->Store->GetPage(outerSlot, location.Offset));

        auto pages = MakeIntrusive<NSharedCache::TSharedCachePages>();
        TVector<THolder<NActors::IEventBase>> events;
        auto env =
            MakePrivateWrittenPageCache(*pages).AddWrittenPages(written, [&](THolder<NActors::IEventBase> event) {
                events.push_back(std::move(event));
            });
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env->GetCollectionCount(), 2);
        UNIT_ASSERT(env->GetCollection(1) == written[1].PageCollection);
        const auto* outer = CheckedCast<NSharedCache::NLegacy::TEvSaveCompactedPages*>(events[1].Get());
        UNIT_ASSERT(outer->PageCollection == written[1].PageCollection);
        UNIT_ASSERT_VALUES_EQUAL(outer->Pages.size(), 1);
        UNIT_ASSERT(outer->Pages.front()->Offset == location.Offset);
        UNIT_ASSERT_VALUES_EQUAL(outer->Pages.front()->Size, location.Size);
        UNIT_ASSERT(!written[1].RegularPages.front().Data);
    }

    Y_UNIT_TEST(TransactionRequestsUseTheCurrentRegisteredDescriptor) {
        TReadFixture<> prior;
        TReadFixture<> newer(false);
        auto& backend = *prior.Backend;
        const auto slot = prior.Part->DataSlot({});
        const auto& first = prior.Part->PageCollections[slot];
        const auto& latest = newer.Part->PageCollections[slot];
        UNIT_ASSERT(first->Id == latest->Id);
        auto pad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
        pad->PendingRequests = 7;
        const auto traceId = NWilson::TTraceId::NewTraceId(3, 100);
        auto request = [&](const auto& expected) {
            auto locations = prior.Locations;
            const auto* storage = locations.data();
            THolder<NActors::IEventBase> event;
            backend.SendTransactionRequest(
                first->Id, std::move(locations), pad, traceId.Clone(), [&](THolder<NActors::IEventBase> sent) {
                    const auto* typed = CheckedCast<NSharedCache::NLegacy::TEvRequest*>(sent.Get());
                    UNIT_ASSERT(typed->PageCollection == expected);
                    UNIT_ASSERT(typed->Pages.data() == storage);
                    UNIT_ASSERT(typed->Pages == prior.Locations);
                    UNIT_ASSERT(typed->Priority == NBlockIO::EPriority::Fast);
                    UNIT_ASSERT_VALUES_EQUAL(typed->Cookie, 0);
                    UNIT_ASSERT(typed->WaitPad == pad);
                    UNIT_ASSERT(typed->TraceId == traceId);
                    UNIT_ASSERT_VALUES_EQUAL(pad->PendingRequests, 7);
                    event = std::move(sent);
                });
            UNIT_ASSERT(event);
            return event;
        };

        auto before = request(first->PageCollection);
        UNIT_ASSERT(backend.PrepareRegistration());
        backend.RegisterCollection(*newer.Part, slot, ECacheMode::Regular, {}, false, [](THolder<NActors::IEventBase>) {
        });
        request(latest->PageCollection);
        auto* old = CheckedCast<NSharedCache::NLegacy::TEvRequest*>(before.Get());
        UNIT_ASSERT(old->PageCollection == first->PageCollection);
        UNIT_ASSERT(old->PageCollection != latest->PageCollection);
    }
}

} // namespace NKikimr::NTabletFlatExecutor
