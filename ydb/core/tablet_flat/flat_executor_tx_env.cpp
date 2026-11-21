#include "flat_executor_tx_env.h"
#include "flat_transaction_pages.h"
#include "flat_database.h"
#include "flat_fwd_env.h"
#include "flat_part_loader.h"
#include "flat_part_store.h"
#include "flat_writer_blocks.h"
#include "shared_cache_events.h"
#include "shared_cache_pages.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace {
    namespace NLegacy = NSharedCache::NLegacy;
    using NTable::TPartStore;
    using NTable::TPrivatePartStore;
    using NTable::NPage::TGroupId;
    using NTable::NPage::TPageLocation;

    class TPrivatePageCacheProtocol final : public IPageCacheProtocol {
    public:
        EPageCacheEvent EventKind(ui32 eventType) const noexcept override {
            switch (eventType) {
                case NLegacy::EvResult:
                    return EPageCacheEvent::Result;
                case NLegacy::EvUpdated:
                    return EPageCacheEvent::Updated;
                case NLegacy::EvStickyCollectionPages:
                    return EPageCacheEvent::StickyPages;
                default:
                    return EPageCacheEvent::Unhandled;
            }
        }

        TPageCacheResult ReadResult(NActors::IEventBase& event) const override {
            auto& result = *CheckedCast<NLegacy::TEvResult*>(&event);
            return {
                .Status = result.Status,
                .PageCollection = result.PageCollection.Get(),
                .WaitPad = result.WaitPad,
                .Cookie = result.Cookie,
                .PageCount = result.Pages.size(),
            };
        }

        THolder<NActors::IEventBase> MakePageRequest(NBlockIO::EPriority priority,
            TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, TVector<TPageLocation> locations,
            ui64 cookie) const override {
            return MakeHolder<NLegacy::TEvRequest>(priority, std::move(collection), std::move(locations), cookie);
        }

        THolder<NActors::IEventBase> MakeCollectionUpdate(
            TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, ECacheMode mode,
            TVector<NPageCollection::TBtreeSeed> btreeSeeds) const override {
            return MakeHolder<NLegacy::TEvAttach>(std::move(collection), mode, std::move(btreeSeeds));
        }

        THolder<NActors::IEventBase> MakeUnregisterRequest() const override {
            return MakeHolder<NLegacy::TEvUnregister>();
        }
    };

    using TPrivatePageCollection = TPrivatePageCache::TPageCollection;

    THolder<NTable::IPartLoaderEnv> AddPrivateWrittenPages(
        void* context, TArrayRef<NWriter::TWrittenPageCollection> collections, TPageCacheEventSender send) {
        auto& pages = *static_cast<NSharedCache::TSharedCachePages*>(context);
        TVector<TIntrusivePtr<TPrivatePageCollection>> prebuilt;
        prebuilt.reserve(collections.size());
        for (auto& collection : collections) {
            auto cached = MakeIntrusive<TPrivatePageCollection>(collection.PageCollection);
            auto event = MakeHolder<NLegacy::TEvSaveCompactedPages>(collection.PageCollection);
            const auto addPage = [&](NPageCollection::TPageData& loaded, bool sticky) {
                const auto& location = loaded.Location;
                auto page = MakeIntrusive<NLegacy::TPage>(
                    location.Offset, location.Size, location.Type, location.Crc32, nullptr);
                page->ProvideBody(std::move(loaded.Data));
                event->Pages.push_back(page);
                auto ref = NSharedCache::TSharedPageRef::MakeUsed(std::move(page), pages.GCList, location.Type);
                if (sticky) {
                    cached->AddStickyPage(location.Offset, location.Size, std::move(ref));
                } else {
                    cached->AddPage(location.Offset, location.Size, std::move(ref));
                }
            };
            for (auto& loaded : collection.StickyPages) {
                addPage(loaded, true);
            }
            for (auto& loaded : collection.RegularPages) {
                addPage(loaded, false);
            }
            send(std::move(event));
            prebuilt.push_back(std::move(cached));
        }
        return MakePrivateLoaderEnvironment(std::move(prebuilt));
    }

    const TPrivatePartStore& PrivatePart(const TPartStore& part) {
        return *CheckedCast<const TPrivatePartStore*>(&part);
    }

    struct TPrivateSeat final : TSeat {
        using TSeat::TSeat;

        THashMap<TLogoBlobID, THashMap<TPageOffset, TPrivatePageCache::TPinnedPage>> Pinned;
    };

    class TPrivatePageCacheAccess {
    public:
        using TBackend = TPrivatePageCache;
        using TSeat = TPrivateSeat;
        using TPageCollection = TPrivatePageCollection;

        TPrivatePageCacheAccess(TBackend& cache, TSeat& seat)
            : Cache(cache)
            , Seat(seat)
        {}

        static TPageCollection* Locate(const TPartStore* part, NTable::ELargeObj lob, ui64 ref) {
            return PrivatePart(*part).Locate(lob, ref);
        }

        static TPageCollection* GetPageCollection(const TPartStore* part, ui32 slot) {
            return PrivatePart(*part).PageCollections.at(slot).Get();
        }

        static const TLogoBlobID& GetCollectionId(const TPageCollection* collection) noexcept {
            return collection->Id;
        }

        static void CheckMissingPage(const TPageLocation& location, const TPageCollection* collection) noexcept {
            Y_ASSERT(!collection->IsStickyPage(location.Offset));
        }

        template <class TEnv>
        const TSharedData* TryGetPage(const TPageLocation& location, TPageCollection* collection, TEnv& env) {
            auto& pinned = Seat.Pinned[collection->Id];
            if (const auto* page = pinned.FindPtr(location.Offset)) {
                return &page->PinnedBody;
            }
            auto body = Cache.TryGetPage(location.Offset, collection);
            if (!body) {
                env.ToLoadPage(location, collection);
                return nullptr;
            }

            body.IncrementFrequency();
            const auto [it, inserted] = pinned.try_emplace(location.Offset, std::move(body));
            Y_ENSURE(inserted);
            const auto& pinnedBody = it->second.PinnedBody;
            ++env.Stats.NewlyPinnedPages;
            if (!collection->IsStickyPage(location.Offset)) {
                env.Stats.NewlyPinnedBytes += pinnedBody.size();
            }
            return &pinnedBody;
        }

    private:
        TBackend& Cache;
        TSeat& Seat;
    };

    struct TPrivatePageCacheRestartState final : IPageCacheRestartState {
        THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCollection>> PageCollections;
    };

    struct TPrivateLoadedCollections final : IPageCacheLoadedCollections {
        TVector<TIntrusivePtr<TPrivatePageCollection>> PageCollections;
    };

    class TPrivateExecutorPageCache final : public IExecutorPageCache {
    public:
        bool PrepareRegistration() override {
            const bool replayStickyWalks = bool(Cache);
            Cache = MakeHolder<TPrivatePageCache>();
            return replayStickyWalks;
        }

        void CaptureForRestart(THolder<IPageCacheRestartState>& restartState) const override {
            if (Cache) {
                auto saved = MakeHolder<TPrivatePageCacheRestartState>();
                saved->PageCollections = Cache->DetachPrivatePageCache();
                restartState = std::move(saved);
            }
        }

        void CaptureForRestart(
            const TPartStore& partStore, THolder<IPageCacheLoadedCollections>& loaded) const override {
            const auto& part = PrivatePart(partStore);
            if (!loaded) {
                loaded = MakeHolder<TPrivateLoadedCollections>();
            }
            auto& collections = CheckedCast<TPrivateLoadedCollections*>(loaded.Get())->PageCollections;
            collections.insert(collections.end(), part.PageCollections.begin(), part.PageCollections.end());
            if (part.Pseudo) {
                collections.push_back(part.Pseudo);
            }
        }

        void MergeForRestart(
            THolder<IPageCacheRestartState>& restartState, const IPageCacheLoadedCollections* loaded) const override {
            if (!loaded) {
                return;
            }
            if (!restartState) {
                restartState = MakeHolder<TPrivatePageCacheRestartState>();
            }
            auto& collections = CheckedCast<TPrivatePageCacheRestartState*>(restartState.Get())->PageCollections;
            for (const auto& collection : CheckedCast<const TPrivateLoadedCollections*>(loaded)->PageCollections) {
                collections[collection->Id] = collection;
            }
        }

        THolder<NTable::IPartLoaderEnv> MakeLoaderEnvironment(TConstArrayRef<NPageCollection::TLargeGlobId> collections,
            const IPageCacheRestartState* restartState) const override {
            TVector<TIntrusivePtr<TPrivatePageCollection>> prebuilt(collections.size());
            if (restartState) {
                const auto& saved = CheckedCast<const TPrivatePageCacheRestartState*>(restartState)->PageCollections;
                for (size_t slot : xrange(collections.size())) {
                    if (const auto* collection = saved.FindPtr(collections[slot].Lead)) {
                        prebuilt[slot] = *collection;
                    }
                }
            }
            return MakePrivateLoaderEnvironment(std::move(prebuilt));
        }

        void PrepareCollectionModes(
            const TPartStore& part, TVector<ECacheMode>& modes, const TVector<bool>& stickyGroups) override {
            const auto& collections = PrivatePart(part).PageCollections;
            for (ui32 slot : xrange(part.Slots.SlotsCount())) {
                collections[slot]->SetCacheMode(modes[slot]);
            }
            ReadCollectionModes(part, modes, stickyGroups);
        }

        TVector<bool> UpdateCollectionModes(
            const TPartStore& part, TVector<ECacheMode>& modes, const TVector<bool>& stickyGroups) override {
            TVector<bool> changed(part.GetPageCollectionCount(), false);
            const auto& collections = PrivatePart(part).PageCollections;
            for (ui32 slot : xrange(part.Slots.SlotsCount())) {
                changed[slot] = Cache->UpdateCacheMode(modes[slot], collections[slot].Get());
            }
            ReadCollectionModes(part, modes, stickyGroups);

            // Walks are registered through each group's data slot even if the index lives elsewhere
            auto updates = changed;
            for (ui32 group : xrange(part.GroupsCount)) {
                const TGroupId groupId(group);
                if (changed[part.IndexSlot(groupId)]) {
                    updates[part.DataSlot(groupId)] = true;
                }
            }
            return updates;
        }

        void ReadCollectionModes(
            const TPartStore& part, TVector<ECacheMode>& modes, const TVector<bool>&) const override {
            const auto& collections = PrivatePart(part).PageCollections;
            for (size_t slot : xrange(modes.size())) {
                modes[slot] = collections[slot]->GetCacheMode();
            }
        }

        void RegisterCollection(const TPartStore& part, ui32 slot, ECacheMode mode,
            TVector<NPageCollection::TBtreeSeed> seeds, bool replayStickyWalk, TPageCacheEventSender send) override {
            RegisterCollection(PrivatePart(part).PageCollections[slot], mode, std::move(seeds), replayStickyWalk, send);
        }

        void UpdateAttachment(const TPartStore& part, ui32 slot, ECacheMode mode,
            TVector<NPageCollection::TBtreeSeed> seeds, EAttachmentUpdate reason, TPageCacheEventSender send) override {
            if (reason == EAttachmentUpdate::StickyPages && seeds.empty()) {
                return;
            }
            send(Protocol().MakeCollectionUpdate(part.GetPageCollectionRef(slot), mode, std::move(seeds)));
        }

        void RegisterPseudoCollection(const TPartStore& part, TPageCacheEventSender send) override {
            const auto& pseudo = PrivatePart(part).Pseudo;
            RegisterCollection(pseudo, pseudo->GetCacheMode(), {}, false, send);
        }

        THolder<NActors::IEventBase> DetachCollection(const TLogoBlobID& collectionId) override {
            TIntrusivePtr<TPrivatePageCollection> collection(Cache->GetPageCollection(collectionId));
            Cache->DropPageCollection(collection.Get());
            return MakeHolder<NLegacy::TEvDetach>(collectionId);
        }

        bool ReadStats(TPageCacheStats& stats) const noexcept override {
            if (!Cache) {
                return false;
            }
            stats = Cache->GetStats();
            return true;
        }

        EPageCacheBackend Backend() const noexcept override {
            return EPageCacheBackend::Legacy;
        }

        const IPageCacheProtocol& Protocol() const noexcept override {
            return GetPrivatePageCacheProtocol();
        }

        std::unique_ptr<TSeat> MakeSeat(ui64 uniqueId, TAutoPtr<ITransaction> transaction) override {
            return std::make_unique<TPrivateSeat>(uniqueId, std::move(transaction));
        }

        void WithTransactionPages(NTable::TDatabase& database, TSeat& seat,
            TFunctionRef<void(ITransactionPages&, TTransactionEnv&)> execute) override {
            using TPrivateReadEnv = TPageCollectionReadEnvImpl<TPrivatePageCacheAccess>;
            TPageCollectionTxEnvImpl<TPrivateReadEnv> frame(database, *Cache, *CheckedCast<TPrivateSeat*>(&seat));
            execute(frame, frame);
        }

        void ProcessCacheUpdate(const NActors::IEventBase& event) override {
            const auto& update = *CheckedCast<const NLegacy::TEvUpdated*>(&event);
            for (const auto& [id, offsets] : update.DroppedPages) {
                if (auto* collection = Cache->FindPageCollection(id)) {
                    for (auto offset : offsets) {
                        Cache->DropPage(offset, collection);
                    }
                }
            }
        }

        THolder<NActors::IEventBase> MakeStickyPageRequest(const NActors::IEventBase& event) override {
            const auto& notification = *CheckedCast<const NLegacy::TEvStickyCollectionPages*>(&event);
            if (auto* collection = Cache->FindPageCollection(notification.CollectionId)) {
                return Protocol().MakePageRequest(
                    NBlockIO::EPriority::Bkgr, collection->PageCollection, notification.Locations);
            }
            return {};
        }

        void ProcessLoadedPages(NActors::IEventBase& event, TPageResultOptions options,
            TFunctionRef<void(EPageResultStatus)> consume) override {
            auto& result = *CheckedCast<NLegacy::TEvResult*>(&event);
            TIntrusivePtr<TPrivatePageCollection> collection(Cache->FindPageCollection(result.PageCollection->Label()));
            if (!collection) {
                return consume(EPageResultStatus::Ignored);
            }
            if (result.Status != NKikimrProto::OK) {
                return consume(EPageResultStatus::Error);
            }
            for (auto& loaded : result.Pages) {
                if (options.Sticky) {
                    Cache->AddStickyPage(loaded.Offset, loaded.Size, std::move(loaded.Page), collection.Get());
                } else {
                    Cache->AddPage(loaded.Offset, loaded.Size, loaded.Page, collection.Get());
                }
            }
            consume(EPageResultStatus::Ready);
        }

        bool AdoptLoadedPages(TSeat& seat, NActors::IEventBase* event) override {
            auto& pinned = CheckedCast<TPrivateSeat*>(&seat)->Pinned;
            if (event) {
                auto& result = *CheckedCast<NLegacy::TEvResult*>(event);
                if (auto* collection = Cache->FindPageCollection(result.PageCollection->Label())) {
                    auto& pages = pinned[collection->Id];
                    for (auto& loaded : result.Pages) {
                        const auto [it, inserted] = pages.try_emplace(loaded.Offset, std::move(loaded.Page));
                        Y_ENSURE(inserted);
                    }
                    if (pages) {
                        return true;
                    }
                }
            }
            for (const auto& [id, pages] : pinned) {
                if (pages) {
                    return true;
                }
            }
            return false;
        }

        void SendTransactionRequest(const TLogoBlobID& collectionId, TVector<TPageLocation> locations,
            TIntrusivePtr<NPageCollection::TPagesWaitPad> waitPad, NWilson::TTraceId traceId,
            TPageCacheEventSender send) override {
            TIntrusivePtr<TPrivatePageCollection> collection(Cache->GetPageCollection(collectionId));
            auto request = MakeHolder<NLegacy::TEvRequest>(
                NBlockIO::EPriority::Fast, collection->PageCollection, std::move(locations));
            request->WaitPad = std::move(waitPad);
            request->TraceId = std::move(traceId);
            send(std::move(request));
        }

        void ReleasePins(TSeat& seat) override {
            CheckedCast<TPrivateSeat*>(&seat)->Pinned.clear();
        }

    private:
        void RegisterCollection(const TIntrusivePtr<TPrivatePageCollection>& collection, ECacheMode mode,
            TVector<NPageCollection::TBtreeSeed> seeds, bool replayStickyWalk, TPageCacheEventSender send) {
            auto syncPages = Cache->AddPageCollection(collection);
            send(MakeHolder<NLegacy::TEvAttach>(collection->PageCollection, mode, std::move(seeds), replayStickyWalk));
            if (syncPages) {
                send(MakeHolder<NLegacy::TEvSync>(std::move(syncPages)));
            }
        }

        THolder<TPrivatePageCache> Cache;
    };
} // namespace

    const IPageCacheProtocol& GetPrivatePageCacheProtocol() noexcept {
        static const TPrivatePageCacheProtocol protocol;
        return protocol;
    }

    THolder<IExecutorPageCache> MakePrivateExecutorPageCache() {
        return MakeHolder<TPrivateExecutorPageCache>();
    }

    TWrittenPageCache MakePrivateWrittenPageCache(NSharedCache::TSharedCachePages& pages) noexcept {
        return TWrittenPageCache(&pages, AddPrivateWrittenPages);
    }

    THolder<NTable::IPartLoaderEnv> TWrittenPageCache::AddWrittenPages(
        TArrayRef<NWriter::TWrittenPageCollection> collections, TPageCacheEventSender send) const {
        return AddWrittenPages_(Context_, collections, send);
    }

    bool IExecutorPageCache::BeginPartMetadata(
        NTable::TLoader& loader, const NTable::TMetadataLoadContext& context, bool) const {
        return loader.BeginMetadata(context);
    }

    void TPageCacheResult::Describe(IOutputStream& out) const {
        switch (Kind) {
            case EPageCacheResultKind::WorkingSetTooLarge:
                out << "TEvResult{WorkingSetTooLarge}";
                break;
            case EPageCacheResultKind::ResourcesReady:
                out << "TEvResult{ResourcesReady}";
                break;
            case EPageCacheResultKind::ResourcePressure:
                out << "TEvResult{ResourcePressure}";
                break;
            case EPageCacheResultKind::Pages:
                out << "TEvResult{" << PageCount << " pages"
                    << " " << PageCollection->Label() << " " << (Status == NKikimrProto::OK ? "ok" : "fail") << " "
                    << NKikimrProto::EReplyStatus_Name(Status) << "}";
                break;
        }
    }

    void TTransactionEnv::MakeSnapshot(TIntrusivePtr<TTableSnapshotContext> snap)
    {
        auto tables = snap->TablesToSnapshot();
        Y_ENSURE(tables);

        for (ui32 table : tables) {
            auto& entry = MakeSnap[table];
            entry.Context.push_back(snap);
            auto epoch = DB.TxSnapTable(table);
            if (entry.Epoch) {
                Y_ENSURE(*entry.Epoch == epoch, "Table snapshot changed unexpectedly");
            } else {
                entry.Epoch.emplace(epoch);
            }
        }
    }

} // namespace NTabletFlatExecutor
} // namespace NKikimr

namespace NKikimr::NTable::NFwd {
namespace {
    namespace NLegacy = NSharedCache::NLegacy;

    class TPrivateScanPageRef : private TMoveOnly {
    public:
        TPrivateScanPageRef() = default;

        explicit TPrivateScanPageRef(NLegacy::TEvResult::TLoaded& loaded)
            : Offset(loaded.Offset)
            , Ref(std::move(loaded.Page))
            , Data(NSharedCache::TPinnedPageRef(Ref).GetData())
        {}

        explicit operator bool() const noexcept {
            return bool(Data);
        }

        TPageOffset GetOffset() const noexcept {
            return Offset;
        }

        EPage GetType() const noexcept {
            return Ref.GetType();
        }

        TPageLocation GetLocation() const noexcept {
            return { Offset, size(), GetType() };
        }

        size_t size() const noexcept {
            return Data.size();
        }

        const TSharedData& GetData() const noexcept {
            return Data;
        }

        TSharedData BuildSharedData() const noexcept {
            return Data;
        }

        void Drop() {
            Data = {};
            Ref.Drop();
        }

        void ReleaseTraceHandle() {
            Ref.Drop();
        }

    private:
        TPageOffset Offset;
        NSharedCache::TSharedPageRef Ref;
        TSharedData Data;
    };

    class TPrivateEnv final : public TEnvImpl<TPrivateScanPageRef> {
    public:
        using TEnvImpl::TEnvImpl;

        const NTabletFlatExecutor::IPageCacheProtocol& Protocol() const noexcept override {
            return NTabletFlatExecutor::GetPrivatePageCacheProtocol();
        }

        THolder<NActors::IEventBase> MakeRequest(
            NTabletFlatExecutor::NBlockIO::EPriority priority, TFetch fetch) const override {
            return Protocol().MakePageRequest(
                priority, std::move(fetch.PageCollection), std::move(fetch.Pages), fetch.Cookie);
        }

        THolder<IPartLoaderEnv> MakeLoaderEnvironment() const override {
            return NTabletFlatExecutor::MakePrivateLoaderEnvironment();
        }

        void Save(NActors::IEventBase& event) override {
            auto& result = *CheckedCast<NLegacy::TEvResult*>(&event);
            SavePages(result.PageCollection.Get(), result.Cookie, std::move(result.Pages), [](auto& loaded) {
                return TPrivateScanPageRef(loaded);
            });
        }
    };
} // namespace

THolder<IEnv> MakePrivateEnv(const TConf& conf, const TSubset& subset) {
    return MakeHolder<TPrivateEnv>(conf, subset);
}

} // namespace NKikimr::NTable::NFwd
