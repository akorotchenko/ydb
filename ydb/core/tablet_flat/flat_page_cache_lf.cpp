#include "flat_transaction_pages.h"
#include "flat_exec_seat_cache.h"
#include "flat_executor_tx_env.h"
#include "flat_fwd_env.h"
#include "flat_sausagecache.h"
#include "flat_part_loader.h"
#include "flat_part_store_cache.h"
#include "flat_writer_blocks.h"
#include "shared_cache_events_lf.h"
#include "shared_cache_collection_load.h"
#include "shared_sausagecache.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/cast.h>

namespace NKikimr::NTabletFlatExecutor {
namespace {

    THolder<NTable::IPartLoaderEnv> AddLFWrittenPages(
        void*, TArrayRef<NWriter::TWrittenPageCollection> collections, TPageCacheEventSender send) {
        TVector<TCacheCollectionRef> prebuilt;
        prebuilt.reserve(collections.size());
        auto& cache = NSharedCache::TCache::Get();
        for (auto& collection : collections) {
            auto event = MakeHolder<NSharedCache::TEvSaveCompactedPages>(collection.PageCollection);
            event->PrepareCollection(cache);
            auto owner = event->Collection().Acquire();
            {
                auto batch = cache.BeginOpBatch();
                event->AddWrittenBatch(batch, collection.StickyPages, true);
                event->AddWrittenBatch(batch, collection.RegularPages, false);
            }
            send(std::move(event));
            prebuilt.push_back(std::move(owner));
        }
        return MakeHolder<NTable::TLoader::TLoaderEnv>(std::move(prebuilt));
    }

    // Shared by one mailbox, including its cold-loader actors
    class TLFMetadataWorkingSet final : public IPageMetadataWorkingSet {
    public:
        void Bind(const NTable::TMetadataLoadContext& context) noexcept {
            if (context.System) {
                System_ = context.System;
                ReplyTo_ = context.ReplyTo;
            }
        }

        void AddCollections(TConstArrayRef<NTable::TPageCollectionComponents> components) override {
            bool changed = false;
            for (const auto& component : components) {
                changed |= WorkingSet->AddCollection(component.LargeGlobId.Lead);
            }
            NotifyChanged(changed);
        }

        void AddPart(const NTable::TPartStore& part) override {
            bool changed = false;
            for (ui32 slot = 0; slot < part.GetPageCollectionCount(); ++slot) {
                changed |= WorkingSet->AddCollection(part.GetPageCollection(slot)->Label());
            }
            if (const auto* pseudo = part.GetPseudoPageCollection()) {
                changed |= WorkingSet->AddCollection(pseudo->Label());
            }
            NotifyChanged(changed);
        }

        void NotifyChanged(bool changed) const {
            if (changed && System_) {
                System_->Send(new NActors::IEventHandle(
                    MakeSharedPageCacheId(), ReplyTo_, new NSharedCache::TEvCollectionRequestUpdate(WorkingSet)));
            }
        }

        const TIntrusivePtr<NSharedCache::TCollectionLoadWorkingSet> WorkingSet =
            MakeIntrusive<NSharedCache::TCollectionLoadWorkingSet>();

    private:
        NActors::TActorSystem* System_ = nullptr;
        NActors::TActorId ReplyTo_;
    };

    class TLFPageCacheProtocol final : public IPageCacheProtocol {
    public:
        EPageCacheEvent EventKind(ui32 eventType) const noexcept override;
        TPageCacheResult ReadResult(NActors::IEventBase& event) const override;
        THolder<NActors::IEventBase> MakeReadyRequest() const override;
        bool ReadMetadataResult(NActors::IEventHandle& event, TPageCacheMetadataResult& result) const override;
        THolder<NActors::IEventBase> MakePageRequest(NBlockIO::EPriority priority,
            TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
            TVector<NTable::NPage::TPageLocation> locations, ui64 cookie) const override;
        THolder<NActors::IEventBase> MakeCollectionUpdate(
            TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, NTable::NPage::ECacheMode mode,
            TVector<NPageCollection::TBtreeSeed> btreeSeeds) const override;
        THolder<NActors::IEventBase> MakeUnregisterRequest() const override;
    };

    class TLFExecutorPageCache final : public IExecutorPageCache {
    public:
        // LF reacquires collections through the global cache on each boot
        void CaptureForRestart(THolder<IPageCacheRestartState>&) const override {}

        void CaptureForRestart(const NTable::TPartStore&, THolder<IPageCacheLoadedCollections>&) const override {}

        void MergeForRestart(THolder<IPageCacheRestartState>&, const IPageCacheLoadedCollections*) const override {}

        THolder<NTable::IPartLoaderEnv> MakeLoaderEnvironment(
            TConstArrayRef<NPageCollection::TLargeGlobId> collections, const IPageCacheRestartState*) const override {
            // Metadata requests reuse canonical collections in the global cache
            return MakeHolder<NTable::TLoader::TLoaderEnv>(TVector<TCacheCollectionRef>(collections.size()));
        }

        TIntrusivePtr<IPageMetadataWorkingSet> MakeMetadataWorkingSet() const override {
            return MakeIntrusive<TLFMetadataWorkingSet>();
        }

        bool BeginPartMetadata(
            NTable::TLoader& loader, const NTable::TMetadataLoadContext& context, bool frontStep) const override {
            return frontStep && loader.BeginMetadata(context);
        }

        bool PrepareRegistration() override;
        void PrepareCollectionModes(const NTable::TPartStore& partStore, TVector<NTable::NPage::ECacheMode>& modes,
            const TVector<bool>& stickyGroups) override;
        TVector<bool> UpdateCollectionModes(const NTable::TPartStore& partStore,
            TVector<NTable::NPage::ECacheMode>& modes, const TVector<bool>& stickyGroups) override;
        void ReadCollectionModes(const NTable::TPartStore& partStore, TVector<NTable::NPage::ECacheMode>& modes,
            const TVector<bool>& stickyGroups) const override;
        void RegisterCollection(const NTable::TPartStore& partStore, ui32 slot, NTable::NPage::ECacheMode cacheMode,
            TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk,
            TPageCacheEventSender send) override;

        void UpdateAttachment(const NTable::TPartStore& partStore, ui32 slot, NTable::NPage::ECacheMode mode,
            TVector<NPageCollection::TBtreeSeed> btreeSeeds, EAttachmentUpdate, TPageCacheEventSender send) override {
            RegisterCollection(partStore, slot, mode, std::move(btreeSeeds), false, send);
        }
        void RegisterPseudoCollection(const NTable::TPartStore& partStore, TPageCacheEventSender send) override;
        THolder<NActors::IEventBase> DetachCollection(const TLogoBlobID& collectionId) override;
        bool ReadStats(TPageCacheStats& stats) const noexcept override;

        EPageCacheBackend Backend() const noexcept override {
            return EPageCacheBackend::LF;
        }

        const IPageCacheProtocol& Protocol() const noexcept override {
            return GetLFPageCacheProtocol();
        }

        std::unique_ptr<TSeat> MakeSeat(ui64 uniqueId, TAutoPtr<ITransaction> transaction) override {
            return std::make_unique<TCacheSeat>(uniqueId, std::move(transaction));
        }

        void WithTransactionPages(NTable::TDatabase& database, TSeat& seat,
            TFunctionRef<void(ITransactionPages&, TTransactionEnv&)> execute) override {
            TPageCollectionTxEnv frame(database, *CheckedCast<TCacheSeat*>(&seat));
            execute(frame, frame);
            // Execute can complete and destroy the seat; frame destruction must not access it
        }

        void ProcessLoadedPages(NActors::IEventBase& event, TPageResultOptions options,
            TFunctionRef<void(EPageResultStatus)> consume) override {
            const auto& result = *CheckedCast<NSharedCache::TEvResult*>(&event);
            // Detach cancels the request even when another owner retains the same collection
            if (result.Status == NKikimrProto::RACE || options.AbandonedTransaction) {
                return consume(EPageResultStatus::Ignored);
            }
            TCacheCollectionRef collection;
            NSharedCache::TCache::Get().Find(result.PageCollection->Label(), collection);
            if (!collection) {
                return consume(EPageResultStatus::Ignored);
            }
            if (result.Status != NKikimrProto::OK) {
                return consume(EPageResultStatus::Error);
            }
            Y_ENSURE(result.CoreRoute, "Page reads must be served by the core cache");
            consume(EPageResultStatus::Ready);
        }

        bool AdoptLoadedPages(TSeat& seat, NActors::IEventBase* event) override {
            auto& cacheSeat = *CheckedCast<TCacheSeat*>(&seat);
            if (event) {
                auto& result = *CheckedCast<NSharedCache::TEvResult*>(event);
                for (auto& loaded : result.Pages) {
                    const TCacheSeat::TPageKey key{ loaded.Page.GetCollectionItem(), loaded.Page.GetOffset() };
                    cacheSeat.Pinned.emplace(key, std::move(loaded.Page));
                }
            }
            return !cacheSeat.Pinned.empty();
        }

        TPageRequests PreparePageRequests(
            const TSeat& seat, ITransactionPages& pages, NPageCollection::TPagesWaitPad& waitPad) const override {
            const auto& cacheSeat = *CheckedCast<const TCacheSeat*>(&seat);
            for (const auto& [key, page] : cacheSeat.Pinned) {
                waitPad.WorkingSetPages[page.GetCollectionId()].push_back(page.GetLocation());
                if (!page.IsSticky()) {
                    waitPad.WorkingSetBytes += page.size() + TSharedData::OverheadSize;
                }
            }
            waitPad.HasPinnedPages.store(!waitPad.WorkingSetPages.empty(), std::memory_order_release);

            auto requests = pages.ObtainToLoad();
            for (const auto& [collectionId, locations] : requests) {
                TCacheCollectionRef collection;
                NSharedCache::TCache::Get().Find(collectionId, collection);
                Y_ENSURE(collection, "Trying to get unknown page collection " << collectionId);
                for (const auto& location : locations) {
                    waitPad.WorkingSetPages[collectionId].push_back(location);
                    if (collection->GetCacheMode() != NTable::NPage::ECacheMode::Sticky) {
                        waitPad.WorkingSetBytes += location.Size + NActors::TSharedData::OverheadSize;
                    }
                }
            }
            return requests;
        }

        void SendTransactionRequest(const TLogoBlobID& collectionId, TVector<NTable::NPage::TPageLocation> locations,
            TIntrusivePtr<NPageCollection::TPagesWaitPad> waitPad, NWilson::TTraceId traceId,
            TPageCacheEventSender send) override {
            TCacheCollectionRef collection;
            NSharedCache::TCache::Get().Find(collectionId, collection);
            Y_ENSURE(collection, "Trying to get unknown page collection " << collectionId);
            auto request = MakeHolder<NSharedCache::TEvRequest>(
                NBlockIO::EPriority::Fast, collection->PageCollection(), std::move(locations));
            request->TraceId = std::move(traceId);
            request->WaitPad = std::move(waitPad);
            send(std::move(request));
        }

        void ReleasePins(TSeat& seat) override {
            CheckedCast<TCacheSeat*>(&seat)->Pinned.clear();
        }

    private:
        void RegisterCollection(const TIntrusiveConstPtr<NPageCollection::IPageCollection>& collection,
            NTable::NPage::ECacheMode mode, TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk,
            TPageCacheEventSender send);

        bool Initialized_ = false;
        TIntrusivePtr<NSharedCache::TCollectionOwnerStats> OwnerStats_ = new NSharedCache::TCollectionOwnerStats;
    };

    bool TLFExecutorPageCache::PrepareRegistration() {
        const bool replayStickyWalks = Initialized_;
        Initialized_ = true;
        return replayStickyWalks;
    }

    void TLFExecutorPageCache::PrepareCollectionModes(const NTable::TPartStore& partStore,
        TVector<NTable::NPage::ECacheMode>& modes, const TVector<bool>& stickyGroups) {
        ReadCollectionModes(partStore, modes, stickyGroups);
    }

    TVector<bool> TLFExecutorPageCache::UpdateCollectionModes(const NTable::TPartStore& partStore,
        TVector<NTable::NPage::ECacheMode>& modes, const TVector<bool>& stickyGroups) {
        ReadCollectionModes(partStore, modes, stickyGroups);
        // Each owner re-registers its complete modes and B-tree seeds
        return TVector<bool>(partStore.GetPageCollectionCount(), true);
    }

    void TLFExecutorPageCache::ReadCollectionModes(const NTable::TPartStore& partStore,
        TVector<NTable::NPage::ECacheMode>& modes, const TVector<bool>& stickyGroups) const {
        using NTable::NPage::ECacheMode;
        const ui32 physicalSlots = partStore.Slots.SlotsCount();
        for (ui32 slot : xrange(physicalSlots)) {
            const size_t groupIndex = partStore.Slots.SlotLeaderGroup(slot);
            if (modes[slot] != ECacheMode::TryKeepInMemory && stickyGroups[groupIndex]) {
                modes[slot] = ECacheMode::Sticky;
            }
        }
        for (size_t slot = physicalSlots; slot < modes.size(); ++slot) {
            modes[slot] = ECacheMode::Regular;
        }
        if (partStore.IndexPages.HasBTree()) {
            for (size_t groupIndex : xrange(partStore.GroupsCount)) {
                if (stickyGroups[groupIndex]) {
                    const ui32 indexSlot = partStore.IndexSlot(NTable::NPage::TGroupId(groupIndex));
                    if (modes[indexSlot] != ECacheMode::TryKeepInMemory) {
                        modes[indexSlot] = ECacheMode::Sticky;
                    }
                }
            }
        }
    }

    void TLFExecutorPageCache::RegisterCollection(const NTable::TPartStore& partStore, ui32 slot,
        NTable::NPage::ECacheMode mode, TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk,
        TPageCacheEventSender send) {
        RegisterCollection(partStore.GetPageCollectionRef(slot), mode, std::move(btreeSeeds), replayStickyWalk, send);
    }

    void TLFExecutorPageCache::RegisterPseudoCollection(
        const NTable::TPartStore& partStore, TPageCacheEventSender send) {
        RegisterCollection(partStore.GetPseudoPageCollectionRef(), NTable::NPage::ECacheMode::Regular, {}, false, send);
    }

    void TLFExecutorPageCache::RegisterCollection(
        const TIntrusiveConstPtr<NPageCollection::IPageCollection>& collection, NTable::NPage::ECacheMode mode,
        TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk, TPageCacheEventSender send) {
        auto event = MakeHolder<NSharedCache::TEvAttach>(collection, mode, std::move(btreeSeeds), replayStickyWalk);
        event->OwnerStats = OwnerStats_;
        send(std::move(event));
    }

    THolder<NActors::IEventBase> TLFExecutorPageCache::DetachCollection(const TLogoBlobID& collectionId) {
        return MakeHolder<NSharedCache::TEvDetach>(collectionId);
    }

    bool TLFExecutorPageCache::ReadStats(TPageCacheStats& stats) const noexcept {
        if (!Initialized_) {
            return false;
        }
        const auto current = OwnerStats_->Read();
        stats = {
            .PageCollections = current.PageCollections,
            .SharedBodyBytes = current.SharedBodyBytes,
            .StickyBytes = current.StickyBytes,
            .TryKeepInMemoryBytes = current.TryKeepInMemoryBytes,
        };
        return true;
    }

    EPageCacheEvent TLFPageCacheProtocol::EventKind(ui32 eventType) const noexcept {
        switch (eventType) {
            case NSharedCache::EvResult:
                return EPageCacheEvent::Result;
            case NSharedCache::EvCacheReady:
                return EPageCacheEvent::Ready;
            default:
                return EPageCacheEvent::Unhandled;
        }
    }

    TPageCacheResult TLFPageCacheProtocol::ReadResult(NActors::IEventBase& event) const {
        auto& result = *CheckedCast<NSharedCache::TEvResult*>(&event);
        EPageCacheResultKind kind = EPageCacheResultKind::Pages;
        if (result.WorkingSetTooLarge) {
            kind = EPageCacheResultKind::WorkingSetTooLarge;
        } else if (result.ResourcesReady) {
            kind = EPageCacheResultKind::ResourcesReady;
        } else if (result.ResourcePressure) {
            kind = EPageCacheResultKind::ResourcePressure;
        }
        return {
            .Status = result.Status,
            .PageCollection = result.PageCollection.Get(),
            .WaitPad = result.WaitPad,
            .Cookie = result.Cookie,
            .PageCount = result.Pages.size(),
            .Kind = kind,
        };
    }

    THolder<NActors::IEventBase> TLFPageCacheProtocol::MakeReadyRequest() const {
        return MakeHolder<NSharedCache::TEvCacheReadyRequest>();
    }

    bool TLFPageCacheProtocol::ReadMetadataResult(
        NActors::IEventHandle& event, TPageCacheMetadataResult& metadata) const {
        if (event.GetTypeRewrite() != NSharedCache::TEvCollectionResult::EventType) {
            return false;
        }
        const auto* result = event.Get<NSharedCache::TEvCollectionResult>();
        metadata = {
            .Request = result->Request.Get(),
            .Status = result->Status,
            .Error = result->Error,
            .CollectionId = result->Request ? result->Request->Descriptor.Lead : TLogoBlobID{},
        };
        return true;
    }

    THolder<NActors::IEventBase> TLFPageCacheProtocol::MakePageRequest(NBlockIO::EPriority priority,
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        TVector<NTable::NPage::TPageLocation> locations, ui64 cookie) const {
        return MakeHolder<NSharedCache::TEvRequest>(priority, std::move(collection), std::move(locations), cookie);
    }

    THolder<NActors::IEventBase> TLFPageCacheProtocol::MakeCollectionUpdate(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, NTable::NPage::ECacheMode mode,
        TVector<NPageCollection::TBtreeSeed> btreeSeeds) const {
        return MakeHolder<NSharedCache::TEvAttach>(std::move(collection), mode, std::move(btreeSeeds));
    }

    THolder<NActors::IEventBase> TLFPageCacheProtocol::MakeUnregisterRequest() const {
        return MakeHolder<NSharedCache::TEvUnregister>();
    }

} // namespace

const IPageCacheProtocol& GetLFPageCacheProtocol() noexcept {
    static const TLFPageCacheProtocol protocol;
    return protocol;
}

TWrittenPageCache GetWrittenPageCache() {
    if (HasAppData() && !AppData()->UseLockFreePageCache()) {
        return MakePrivateWrittenPageCache(*AppData()->LegacySharedCachePages);
    }
    return TWrittenPageCache(nullptr, AddLFWrittenPages);
}

THolder<IExecutorPageCache> MakeLFExecutorPageCache() {
    return MakeHolder<TLFExecutorPageCache>();
}

THolder<IExecutorPageCache> MakeExecutorPageCache(const TAppData& appData) {
    return appData.UseLockFreePageCache() ? MakeLFExecutorPageCache() : MakePrivateExecutorPageCache();
}

} // namespace NKikimr::NTabletFlatExecutor

namespace NKikimr::NTable {
namespace {

    TCacheCollectionRef AdmitPartCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection) {
        auto& cache = TCache::Get();
        TCacheCollectionRef ready;
        const auto status = cache.BeginOperation().AdmitCollection(collection, ready);
        Y_ENSURE(status != NSharedCache::ECacheResultStatus::NoResource,
            "NoResource admitting part collection " << collection->Label() << ", handle capacity "
                                                    << cache.TargetHandleCount());
        return ready;
    }

} // namespace

TLoader::TLoader(TPartComponents components, TVector<TCacheCollectionRef> prebuiltPageCollections)
    : TLoader(MakeHolder<TLoaderEnv>(std::move(prebuiltPageCollections)), std::move(components))
{}

void TLoader::SetPageCollection(ui32 slot, TCacheCollectionRef collection)
{
    Y_ENSURE(Stage == EStage::Meta, "Part metadata loading has already completed");
    Y_ENSURE(collection && slot < LoaderEnv->GetCollectionCount() && !LoaderEnv->HasCollection(slot),
        "Invalid or duplicate page collection at slot " << slot);
    if (slot < Components.PageCollectionComponents.size()) {
        Y_ENSURE(collection->Id() == Components.PageCollectionComponents[slot].LargeGlobId.Lead,
            "Page collection does not match slot " << slot);
    }
    CheckedCast<TLoaderEnv*>(LoaderEnv.Get())->SetCollection(slot, std::move(collection));
    if (RootPrepared) {
        ApplyIndexSelection(slot);
    }
}

void TLoader::Save(TVector<NSharedCache::TEvResult::TLoaded>&& pages)
{
    if (Stage == EStage::PartView || Stage == EStage::Slice || Stage == EStage::PreloadData) {
        auto& env = *CheckedCast<TLoaderEnv*>(LoaderEnv.Get());
        for (auto& page : pages) {
            env.Save(std::move(page));
        }
    } else {
        Y_TABLET_ERROR("Unexpected pages save on stage " << int(Stage));
    }
}

const NTabletFlatExecutor::IPageCacheProtocol& TLoader::TLoaderEnv::Protocol() const noexcept {
    return NTabletFlatExecutor::GetLFPageCacheProtocol();
}

THolder<NActors::IEventBase> TLoader::TLoaderEnv::MakeRequest(
    NTabletFlatExecutor::NBlockIO::EPriority priority, TFetch fetch, ui64 cookie) const {
    auto request =
        MakeHolder<NSharedCache::TEvRequest>(priority, std::move(fetch.PageCollection), std::move(fetch.Pages), cookie);
    request->Sticky = std::move(fetch.Sticky);
    return request;
}

const TVector<TCacheCollectionRef>& TLoader::TLoaderEnv::Collections() const noexcept {
    if (Part && PageCollections.empty()) {
        return static_cast<const TCachePartStore*>(Part)->PageCollections;
    }
    return PageCollections;
}

void TLoader::TLoaderEnv::SetCollection(ui32 slot, TCacheCollectionRef collection) {
    PageCollections.at(slot) = std::move(collection);
}

TLoader::TLoaderEnv::~TLoaderEnv() {
    CancelMetadata();
}

bool TLoader::TLoaderEnv::TryReuseRawMetadata(ui32 slot, NSharedCache::ECollectionLoadKind kind) {
    TCacheCollectionRef collection;
    if (TCache::Get().FindCollection(MetadataSlots[slot].Descriptor.Lead, collection) !=
        NSharedCache::ECacheCollectionLookupStatus::Hit) {
        return false;
    }
    SetCollection(slot, std::move(collection));
    ValidateMetadataCollection(slot, kind);
    return true;
}

void TLoader::TLoaderEnv::ValidateMetadataCollection(ui32 slot, NSharedCache::ECollectionLoadKind kind) const {
    const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(GetCollection(slot).Get());
    Y_ENSURE(packet && packet->LargeGlobId == MetadataSlots[slot].Descriptor &&
                 bool(dynamic_cast<const NPageCollection::TOuterPageCollection*>(packet)) ==
                     (kind == NSharedCache::ECollectionLoadKind::Outer),
        "Cached collection does not match metadata at slot " << slot);
}

void TLoader::TLoaderEnv::PrepareMetadataRoot(
    ui32 attachedSlot, const TMetadataLoadContext& context, TFunctionRef<bool(ui32 attachedSlot)> prepareRoot) {
    LastCollectionKind = prepareRoot(attachedSlot) ? NSharedCache::ECollectionLoadKind::Outer
                                                   : NSharedCache::ECollectionLoadKind::Regular;
    if (MetadataSlots.size() > 1) {
        const ui32 slot = MetadataSlots.size() - 1;
        auto& last = MetadataSlots[slot];
        if (HasCollection(slot)) {
            ValidateMetadataCollection(slot, LastCollectionKind);
        } else if (last.RawPresent && TryReuseRawMetadata(slot, LastCollectionKind)) {
            prepareRoot(slot);
        }
        if (last.Request && last.Request->Kind() == NSharedCache::ECollectionLoadKind::Unknown) {
            Y_ENSURE(last.Request->Resolve(LastCollectionKind));
            context.SendUpdate(MakeHolder<NSharedCache::TEvCollectionRequestUpdate>(last.Request));
        }
    }
}

bool TLoader::TLoaderEnv::BeginMetadata(TArrayRef<TPageCollectionComponents> components,
    const TMetadataLoadContext& context, TFunctionRef<bool(ui32 attachedSlot)> prepareRoot) {
    if (MetadataState != EMetadataState::Idle) {
        return false;
    }
    MetadataState = EMetadataState::Loading;
    MetadataSystem = context.System;
    MetadataReplyTo = context.ReplyTo;
    if (GetCollectionCount() < components.size()) {
        ResizeCollections(components.size());
    }
    if (context.MetadataWorkingSet) {
        MetadataWorkingSet = TIntrusivePtr<NTabletFlatExecutor::IPageMetadataWorkingSet>(context.MetadataWorkingSet);
    } else {
        MetadataWorkingSet = MakeIntrusive<NTabletFlatExecutor::TLFMetadataWorkingSet>();
    }
    auto& metadataWorkingSet = *CheckedCast<NTabletFlatExecutor::TLFMetadataWorkingSet*>(MetadataWorkingSet.Get());
    metadataWorkingSet.Bind(context);
    MetadataSlots.resize(GetCollectionCount());
    Y_ENSURE(MetadataSlots, "Part metadata requires at least one collection");
    bool workingSetChanged = false;
    for (ui32 slot = 0; slot < MetadataSlots.size(); ++slot) {
        auto& metadata = MetadataSlots[slot];
        if (slot < components.size()) {
            metadata.Descriptor = components[slot].LargeGlobId;
            metadata.RawPresent = bool(components[slot].RawMeta);
        }
        if (metadata.Descriptor.Lead == TLogoBlobID{}) {
            Y_ENSURE(HasCollection(slot), "Missing collection metadata descriptor at slot " << slot);
            const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(GetCollection(slot).Get());
            Y_ENSURE(packet, "Unexpected prebuilt metadata collection at slot " << slot);
            metadata.Descriptor = packet->LargeGlobId;
        }
        workingSetChanged |= metadataWorkingSet.WorkingSet->AddCollection(metadata.Descriptor.Lead);
    }
    metadataWorkingSet.NotifyChanged(workingSetChanged);
    if (!HasCollection(0) && MetadataSlots[0].RawPresent) {
        TryReuseRawMetadata(0, NSharedCache::ECollectionLoadKind::Regular);
    }
    if (HasCollection(0) || MetadataSlots[0].RawPresent) {
        if (HasCollection(0)) {
            ValidateMetadataCollection(0, NSharedCache::ECollectionLoadKind::Regular);
        }
        LastCollectionKind =
            prepareRoot(0) ? NSharedCache::ECollectionLoadKind::Outer : NSharedCache::ECollectionLoadKind::Regular;
    }
    for (ui32 slot = 0; slot < MetadataSlots.size(); ++slot) {
        auto& metadata = MetadataSlots[slot];
        const auto kind =
            slot && slot + 1 == MetadataSlots.size() ? LastCollectionKind : NSharedCache::ECollectionLoadKind::Regular;
        if (HasCollection(slot)) {
            if (kind != NSharedCache::ECollectionLoadKind::Unknown) {
                ValidateMetadataCollection(slot, kind);
            }
        } else if (metadata.RawPresent) {
            if (kind != NSharedCache::ECollectionLoadKind::Unknown && TryReuseRawMetadata(slot, kind) &&
                LastCollectionKind != NSharedCache::ECollectionLoadKind::Unknown) {
                prepareRoot(slot);
            }
        } else {
            Y_ENSURE(MetadataSystem, "Asynchronous collection metadata loading requires an actor system");
            metadata.Request = MakeIntrusive<NSharedCache::TCollectionRequest>(
                metadata.Descriptor, kind, metadataWorkingSet.WorkingSet);
            ++MetadataRemaining;
        }
    }
    for (ui32 slot = 0; slot < MetadataSlots.size(); ++slot) {
        const auto& request = MetadataSlots[slot].Request;
        if (request && !context.SendRequest(slot, request.Get(),
                           MakeHolder<NSharedCache::TEvCollectionRequest>(request, context.Priority))) {
            CancelMetadata();
            break;
        }
    }
    if (MetadataReady()) {
        MetadataSlots.clear();
    }
    return true;
}

EMetadataLoadResult TLoader::TLoaderEnv::AcceptMetadata(ui32 slot,
    const NTabletFlatExecutor::TPageCacheMetadataResult& result, NActors::IEventBase& event,
    const TMetadataLoadContext& context, TFunctionRef<bool(ui32 attachedSlot)> prepareRoot) {
    if (MetadataState != EMetadataState::Loading || slot >= MetadataSlots.size() || !MetadataSlots[slot].Request ||
        MetadataSlots[slot].Request.Get() != result.Request ||
        event.Type() != NSharedCache::TEvCollectionResult::EventType) {
        return EMetadataLoadResult::Ignored;
    }
    auto& response = *CheckedCast<NSharedCache::TEvCollectionResult*>(&event);
    if (response.Request != MetadataSlots[slot].Request) {
        return EMetadataLoadResult::Ignored;
    }
    MetadataSlots[slot].Request.Reset();
    if (response.Status != NKikimrProto::OK) {
        return EMetadataLoadResult::Error;
    }
    Y_ENSURE(response.Collection, "Empty collection metadata result at slot " << slot);
    SetCollection(slot, std::move(response.Collection));
    const auto kind =
        slot && slot + 1 == MetadataSlots.size() ? LastCollectionKind : NSharedCache::ECollectionLoadKind::Regular;
    Y_ENSURE(
        kind != NSharedCache::ECollectionLoadKind::Unknown, "Unresolved collection metadata role at slot " << slot);
    ValidateMetadataCollection(slot, kind);
    if (HasCollection(0)) {
        PrepareMetadataRoot(slot, context, prepareRoot);
    }
    --MetadataRemaining;
    if (MetadataReady()) {
        MetadataSlots.clear();
        return EMetadataLoadResult::Ready;
    }
    return EMetadataLoadResult::Pending;
}

void TLoader::TLoaderEnv::CancelMetadata() noexcept {
    if (MetadataState == EMetadataState::Cancelled) {
        return;
    }
    MetadataState = EMetadataState::Cancelled;
    for (const auto& metadata : MetadataSlots) {
        if (const auto& request = metadata.Request; request && request->IsActive()) {
            request->Cancel();
            MetadataSystem->Send(new NActors::IEventHandle(NSharedCache::MakeSharedPageCacheId(), MetadataReplyTo,
                new NSharedCache::TEvCollectionRequestUpdate(request)));
        }
    }
    MetadataSlots.clear();
}

void TLoader::TLoaderEnv::PrepareCollection(ui32 slot, TPageCollectionComponents& components, bool outer) {
    if (outer) {
        Y_ENSURE(components.RawMeta, "Outer blob slot has no raw meta data");
    } else {
        Y_ENSURE(components.RawMeta, "Slot " << slot << " has no raw meta data");
    }
    SetCollection(slot, AdmitPartCollection(components.MakePageCollection(outer)));
}

void TLoader::TLoaderEnv::Initialize(TPartSlots slots) {
    Slots = slots;
    Y_ENSURE(PageCollections.size() >= Slots.SlotsCount());
    SavedPages.resize(Slots.SlotsCount());
    NeedPages.resize(Slots.SlotsCount());
}

TPartStore* TLoader::TLoaderEnv::CreatePart(TPart::TParams params, TPart::TStat stat) {
    auto partStore = MakeHolder<TCachePartStore>(Collections().front()->Id(), std::move(params), stat);
    partStore->PageCollections = std::move(PageCollections);
    if (partStore->Blobs) {
        Y_ENSURE(partStore->Large, "Cannot use blobs without frames");
        partStore->Pseudo = AdmitPartCollection(partStore->Blobs);
    }
    if (MetadataWorkingSet) {
        MetadataWorkingSet->AddPart(*partStore);
    }
    Part = partStore.Get();
    return partStore.Release();
}

void TLoader::TLoaderEnv::Save(NActors::IEventBase& event) {
    auto& result = *CheckedCast<NSharedCache::TEvResult*>(&event);
    for (auto& page : result.Pages) {
        Save(std::move(page));
    }
}

void TLoader::TLoaderEnv::Save(NPageCollection::TPageData&& page) {
    auto& needPages = NeedPages.at(FetchSlot);
    auto it = needPages.find(TPageLocation(page.Location.Offset));
    Y_ENSURE(it != needPages.end(), "Got unknown decoder page");
    auto& cache = TCache::Get();
    auto cacheOp = cache.BeginOperation();
    TCacheCollectionRef collection;
    const auto status = cacheOp.AdmitCollection(Collections().at(FetchSlot)->PageCollection(), collection);
    Y_ENSURE(status != NSharedCache::ECacheResultStatus::NoResource,
        "NoResource admitting fetched collection " << Collections().at(FetchSlot)->Id() << ", handle capacity "
                                                   << cache.TargetHandleCount());
    needPages.erase(it);
    auto data = std::move(page.Data);
    SavedPages.at(FetchSlot)[page.Location.Offset] =
        cacheOp.AdmitPage(collection.CacheItem(), page.Location, std::move(data), ShouldKeepSticky(page.Location.Type));
}

} // namespace NKikimr::NTable

namespace NKikimr::NTable::NFwd {

THolder<IEnv> MakeEnv(const TConf& conf, const TSubset& subset) {
    if (HasAppData() && !AppData()->UseLockFreePageCache()) {
        return MakePrivateEnv(conf, subset);
    }
    return MakeHolder<TEnv>(conf, subset, MakeIntrusive<NTabletFlatExecutor::TLFMetadataWorkingSet>());
}

const NTabletFlatExecutor::IPageCacheProtocol& TEnv::Protocol() const noexcept {
    return NTabletFlatExecutor::GetLFPageCacheProtocol();
}

THolder<NActors::IEventBase> TEnv::MakeRequest(NTabletFlatExecutor::NBlockIO::EPriority priority, TFetch fetch) const {
    auto request = MakeHolder<NSharedCache::TEvRequest>(
        priority, std::move(fetch.PageCollection), std::move(fetch.Pages), fetch.Cookie);
    request->ReadAhead = true;
    return request;
}

THolder<IPartLoaderEnv> TEnv::MakeLoaderEnvironment() const {
    return MakeHolder<TLoader::TLoaderEnv>(TVector<TCacheCollectionRef>{});
}

void TEnv::Save(NActors::IEventBase& event) {
    auto& result = *CheckedCast<NSharedCache::TEvResult*>(&event);
    SavePages(result.PageCollection.Get(), result.Cookie, std::move(result.Pages), [](auto& loaded) -> TCachePageRef&& {
        return std::move(loaded.Page);
    });
}

} // namespace NKikimr::NTable::NFwd
