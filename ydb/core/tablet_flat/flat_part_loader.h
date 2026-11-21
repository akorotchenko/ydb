#pragma once
#include "defs.h"
#include "flat_part_store.h"
#include "flat_part_walker.h"
#include "flat_sausagecache_lf.h"
#include "shared_cache_events_lf.h"
#include "shared_cache_collection_load.h"
#include "cache.h"
#include "flat_transaction_pages.h"
#include "util_fmt_abort.h"
#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>
#include <ydb/core/util/pb.h>
#include <util/generic/bitmap.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/stream/mem.h>
#include <optional>

namespace NKikimr {
namespace NTable {

    class TLoader;

    enum class EMetadataLoadResult {
        Ignored,
        Pending,
        Ready,
        Error,
    };

    // Hosts and callbacks are borrowed for this call; only address data may be retained
    class TMetadataLoadContext {
    public:
        template <class THost>
        TMetadataLoadContext(THost& host, NActors::TActorSystem* system, NActors::TActorId replyTo, ui64 cookie,
            NTabletFlatExecutor::NBlockIO::EPriority priority,
            NTabletFlatExecutor::IPageMetadataWorkingSet* workingSet = nullptr) noexcept
            : Host_(&host)
            , LoadRaw_(&InvokeLoadRaw<THost>)
            , SendRequest_(&InvokeSendRequest<THost>)
            , SendUpdate_(&InvokeSendUpdate<THost>)
            , System(system)
            , ReplyTo(replyTo)
            , Cookie(cookie)
            , Priority(priority)
            , MetadataWorkingSet(workingSet)
        {}

        void LoadRaw(ui32 slot, const NPageCollection::TLargeGlobId& id) const {
            LoadRaw_(Host_, slot, id);
        }

        bool SendRequest(ui32 slot, const void* request, THolder<NActors::IEventBase> event) const {
            return SendRequest_(Host_, slot, request, std::move(event));
        }

        void SendUpdate(THolder<NActors::IEventBase> event) const {
            SendUpdate_(Host_, std::move(event));
        }

    private:
        template <class THost>
        static void InvokeLoadRaw(void* context, ui32 slot, const NPageCollection::TLargeGlobId& id) {
            static_cast<THost*>(context)->LoadRawMetadata(slot, id);
        }

        template <class THost>
        static bool InvokeSendRequest(
            void* context, ui32 slot, const void* request, THolder<NActors::IEventBase> event) {
            return static_cast<THost*>(context)->SendMetadataRequest(slot, request, std::move(event));
        }

        template <class THost>
        static void InvokeSendUpdate(void* context, THolder<NActors::IEventBase> event) {
            static_cast<THost*>(context)->SendMetadataUpdate(std::move(event));
        }

        void* Host_;
        void (*LoadRaw_)(void*, ui32, const NPageCollection::TLargeGlobId&);
        bool (*SendRequest_)(void*, ui32, const void*, THolder<NActors::IEventBase>);
        void (*SendUpdate_)(void*, THolder<NActors::IEventBase>);

    public:
        NActors::TActorSystem* const System;
        const NActors::TActorId ReplyTo;
        const ui64 Cookie;
        const NTabletFlatExecutor::NBlockIO::EPriority Priority;
        // The environment retains the working set if it needs it beyond this call
        NTabletFlatExecutor::IPageMetadataWorkingSet* const MetadataWorkingSet;
    };

    struct TLoaderFetch : TMoveOnly {
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TPageLocation> Pages;
        TVector<bool> Sticky;

        explicit operator bool() const {
            return bool(Pages);
        }
    };

    struct IPartLoaderEnv : public IPages {
        // Returns true only when this call starts the metadata phase
        // The callback prepares the root and publishes the frozen selection for available slots
        // After root preparation, it publishes only the newly attached slot
        // The callback is borrowed for the call
        virtual bool BeginMetadata(TArrayRef<TPageCollectionComponents> components, const TMetadataLoadContext& context,
            TFunctionRef<bool(ui32 attachedSlot)> prepareRoot) = 0;
        virtual EMetadataLoadResult SaveMetadata(
            TArrayRef<TPageCollectionComponents> components, ui32 slot, TSharedData data) = 0;
        virtual EMetadataLoadResult AcceptMetadata(ui32 slot,
            const NTabletFlatExecutor::TPageCacheMetadataResult& result, NActors::IEventBase& event,
            const TMetadataLoadContext& context, TFunctionRef<bool(ui32 attachedSlot)> prepareRoot) = 0;
        virtual bool MetadataReady() const noexcept = 0;
        virtual void CancelMetadata() noexcept = 0;
        virtual const NTabletFlatExecutor::IPageCacheProtocol& Protocol() const noexcept = 0;
        virtual THolder<NActors::IEventBase> MakeRequest(
            NTabletFlatExecutor::NBlockIO::EPriority priority, TLoaderFetch fetch, ui64 cookie) const = 0;
        virtual ui32 GetCollectionCount() const noexcept = 0;
        virtual void ResizeCollections(ui32 count) = 0;
        virtual bool HasCollection(ui32 slot) const noexcept = 0;
        virtual const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetCollection(ui32 slot) const = 0;
        // Apply the shared index choice with backend accounting
        virtual void SetSkipBTreeIndexV1Shadow(ui32 slot, bool skip) = 0;
        virtual void PrepareCollection(ui32 slot, TPageCollectionComponents& components, bool outer) = 0;
        virtual void Initialize(TPartSlots slots) = 0;
        virtual TPartStore* CreatePart(TPart::TParams params, TPart::TStat stat) = 0;
        virtual void ProvidePart(const TPart* part) = 0;
        virtual const TSharedData* TryGetSlotPage(ui32 slot, const TPageLocation& location) = 0;
        virtual void EnsureNoNeedPages() const = 0;
        virtual TLoaderFetch GetFetch() = 0;
        virtual void Save(NActors::IEventBase& event) = 0;
    };

    class TPartLoaderPages : public IPartLoaderEnv {
    public:
        TResult Locate(const TMemTable*, ui64, ui32) override {
            Y_TABLET_ERROR("IPages::Locate(TMemTable*, ...) shouldn't be used here");
        }

        TResult Locate(const TPart*, ui64, ELargeObj) override {
            Y_TABLET_ERROR("IPages::Locate(TPart*, ...) shouldn't be used here");
        }

        void EnsureNoNeedPages() const override {
            for (const auto& needPages : NeedPages) {
                Y_ENSURE(!needPages);
            }
        }

        TLoaderFetch GetFetch() override {
            return BuildFetch([](TLoaderFetch&) {
            });
        }

    protected:
        TPartLoaderPages() = default;

        explicit TPartLoaderPages(TPartSlots slots)
            : Slots(slots)
            , NeedPages(Slots.SlotsCount())
        {}

        ui32 DataSlot(const TPart* part, TGroupId groupId) const {
            Y_ENSURE(part == Part, "Unsupported part");
            return Slots.DataSlot(groupId);
        }

        ui32 IndexSlot(const TPart* part, TGroupId groupId) const {
            Y_ENSURE(part == Part, "Unsupported part");
            return Slots.IndexSlot(groupId);
        }

        void RecordMissingPage(ui32 slot, const TPageLocation& location) {
            auto [it, inserted] = NeedPages[slot].emplace(location);
            Y_DEBUG_ABORT_UNLESS(inserted || *it == location);
        }

        // One slot per fetch. The stage is run again after the reply to request any other slots
        template <class TPrepare>
        TLoaderFetch BuildFetch(TPrepare prepare) {
            for (ui32 slot : xrange(NeedPages.size())) {
                if (const auto& needPages = NeedPages[slot]) {
                    TLoaderFetch fetch{ .Pages = TVector<TPageLocation>(needPages.begin(), needPages.end()) };
                    std::sort(fetch.Pages.begin(), fetch.Pages.end());
                    prepare(fetch);
                    FetchSlot = slot;
                    fetch.PageCollection = GetCollection(slot);
                    return fetch;
                }
            }
            return {};
        }

        struct TPageLocationByOffsetEq {
            bool operator()(const TPageLocation& a, const TPageLocation& b) const noexcept {
                return a.Offset == b.Offset;
            }
        };

        const TPart* Part = nullptr;
        TPartSlots Slots{ 0 };
        // Dedup by offset while keeping Size and Type for the fetch request
        TVector<THashSet<TPageLocation, NPage::TPageLocationByOffsetHash, TPageLocationByOffsetEq>> NeedPages;
        ui32 FetchSlot = Max<ui32>();
    };

    class TLoader {
    public:
        enum class EStage : ui8 {
            Meta,
            PartView,
            Slice,
            Deltas,
            PreloadData,
            Result,
        };

        using TFetch = TLoaderFetch;
        using IEnv = IPartLoaderEnv;

        struct TLoaderEnv : public TPartLoaderPages {
            enum class EMetadataState : ui8 {
                Idle,
                Loading,
                Cancelled,
            };

            ~TLoaderEnv() override;

            bool BeginMetadata(TArrayRef<TPageCollectionComponents> components, const TMetadataLoadContext& context,
                TFunctionRef<bool(ui32 attachedSlot)> prepareRoot) override;
            EMetadataLoadResult AcceptMetadata(ui32 slot, const NTabletFlatExecutor::TPageCacheMetadataResult& result,
                NActors::IEventBase& event, const TMetadataLoadContext& context,
                TFunctionRef<bool(ui32 attachedSlot)> prepareRoot) override;

            EMetadataLoadResult SaveMetadata(TArrayRef<TPageCollectionComponents>, ui32, TSharedData) override {
                // LF metadata replies carry collection owners through AcceptMetadata
                return EMetadataLoadResult::Ignored;
            }

            bool MetadataReady() const noexcept override {
                return MetadataState == EMetadataState::Loading && MetadataRemaining == 0 &&
                       LastCollectionKind != NSharedCache::ECollectionLoadKind::Unknown;
            }

            void CancelMetadata() noexcept override;

            explicit TLoaderEnv(TVector<TCacheCollectionRef> pageCollections)
                : PageCollections(std::move(pageCollections))
            {}

            TLoaderEnv(TVector<TCacheCollectionRef> pageCollections, TPartSlots slots)
                : TPartLoaderPages(slots)
                , PageCollections(std::move(pageCollections))
                , SavedPages(Slots.SlotsCount())
            {
                Y_ENSURE(PageCollections.size() >= Slots.SlotsCount());
            }

            const NTabletFlatExecutor::IPageCacheProtocol& Protocol() const noexcept override;
            THolder<NActors::IEventBase> MakeRequest(
                NTabletFlatExecutor::NBlockIO::EPriority priority, TFetch fetch, ui64 cookie) const override;

            ui32 GetCollectionCount() const noexcept override {
                return Collections().size();
            }

            void ResizeCollections(ui32 count) override {
                PageCollections.resize(count);
            }

            bool HasCollection(ui32 slot) const noexcept override {
                return bool(Collections()[slot]);
            }

            const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetCollection(ui32 slot) const override {
                return Collections()[slot]->PageCollection();
            }

            void SetSkipBTreeIndexV1Shadow(ui32 slot, bool skip) override {
                Collections()[slot]->SetSkipBTreeIndexV1Shadow(skip);
            }

            void SetCollection(ui32 slot, TCacheCollectionRef collection);
            void PrepareCollection(ui32 slot, TPageCollectionComponents& components, bool outer) override;
            void Initialize(TPartSlots slots) override;
            TPartStore* CreatePart(TPart::TParams params, TPart::TStat stat) override;
            void Save(NActors::IEventBase& event) override;

            void ProvidePart(const TPart* part) override {
                Y_ENSURE(!Part || Part == part);
                Part = part;
            }

            const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override {
                return TryGetSlotPage(DataSlot(part, groupId), location);
            }

            const TSharedData* TryGetIndexPage(
                const TPart* part, const TPageLocation& location, TGroupId groupId) override {
                return TryGetSlotPage(IndexSlot(part, groupId), location);
            }

            /**
             * Loads a page of the page collection, the outer page collection is not supported
             */
            const TSharedData* TryGetSlotPage(ui32 slot, const TPageLocation& location) override {
                Y_ENSURE(slot < Slots.SlotsCount(), "Unsupported page collection " << slot);

                auto readScope = BeginOpBatch();
                return OpBatch_->ProcessPage([&](auto& cacheOp) {
                    auto& savedPages = SavedPages[slot];
                    auto savedPage = savedPages.find(location.Offset);

                    if (savedPage == savedPages.end() || !savedPage->second) {
                        if (auto page =
                                Collections()[slot]->TryGetPage(cacheOp, location, ShouldKeepSticky(location.Type))) {
                            // Retain the page while decoding, even if its cache entry is withdrawn.
                            AddSavedPage(slot, location.Offset, std::move(page));
                            savedPage = savedPages.find(location.Offset);
                        }
                    }

                    const TSharedData* result = nullptr;
                    if (savedPage != savedPages.end() && savedPage->second) {
                        result = &savedPage->second.GetData();
                    } else {
                        RecordMissingPage(slot, location);
                    }
                    return result;
                });
            }

            TFetch GetFetch() override {
                return BuildFetch([](TLoaderFetch& fetch) {
                    fetch.Sticky.reserve(fetch.Pages.size());
                    for (const auto& page : fetch.Pages) {
                        fetch.Sticky.push_back(ShouldKeepSticky(page.Type));
                    }
                });
            }

            void Save(NPageCollection::TPageData&& page);

            void Save(NSharedCache::TEvResult::TLoaded&& loaded)
            {
                auto& needPages = NeedPages.at(FetchSlot);
                auto it = needPages.find(TPageLocation(loaded.Offset));
                Y_ENSURE(it != needPages.end(), "Got unknown page at " << loaded.Offset << " of page collection " << FetchSlot);
                needPages.erase(it);

                AddSavedPage(FetchSlot, loaded.Offset, std::move(loaded.Page));
            }

        private:
            struct TMetadataSlot {
                NPageCollection::TLargeGlobId Descriptor;
                bool RawPresent = false;
                TIntrusivePtr<NSharedCache::TCollectionRequest> Request;
            };

            bool TryReuseRawMetadata(ui32 slot, NSharedCache::ECollectionLoadKind kind);
            void ValidateMetadataCollection(ui32 slot, NSharedCache::ECollectionLoadKind kind) const;
            void PrepareMetadataRoot(ui32 attachedSlot, const TMetadataLoadContext& context,
                TFunctionRef<bool(ui32 attachedSlot)> prepareRoot);

            const TVector<TCacheCollectionRef>& Collections() const noexcept;

            bool DoBeginOpBatch() noexcept override {
                if (OpBatch_) {
                    return false;
                }
                OpBatch_.emplace(TCache::Get().BeginOpBatch());
                return true;
            }

            void DoEndOpBatch() noexcept override {
                OpBatch_.reset();
            }

            void AddSavedPage(ui32 slot, TPageOffset offset, TCachePageRef&& page)
            {
                SavedPages[slot][offset] = std::move(page);
            }

            TVector<TCacheCollectionRef> PageCollections;
            EMetadataState MetadataState = EMetadataState::Idle;
            ui32 MetadataRemaining = 0;
            NSharedCache::ECollectionLoadKind LastCollectionKind = NSharedCache::ECollectionLoadKind::Unknown;
            TVector<TMetadataSlot> MetadataSlots;
            TIntrusivePtr<NTabletFlatExecutor::IPageMetadataWorkingSet> MetadataWorkingSet;
            NActors::TActorSystem* MetadataSystem = nullptr;
            NActors::TActorId MetadataReplyTo;
            std::optional<TCache::TOpBatch> OpBatch_;
            TVector<THashMap<TPageOffset, TCachePageRef>> SavedPages;
        };

        struct TRunOptions {
            // Marks that optional index pages should be loaded
            //
            // Effects only b-tree index as flat index is kept as sticky
            bool PreloadIndex = true;

            // Marks that all data pages from the main group should be loaded
            bool PreloadData = false;
        };

        TLoader(TPartComponents components, TVector<TCacheCollectionRef> prebuiltPageCollections = {});
        TLoader(THolder<IEnv> environment, TPartComponents components);
        ~TLoader();

        bool BeginMetadata(const TMetadataLoadContext& context);
        EMetadataLoadResult SaveMetadata(ui32 slot, TSharedData data);
        EMetadataLoadResult AcceptMetadata(ui32 slot, const NTabletFlatExecutor::TPageCacheMetadataResult& result,
            NActors::IEventBase& event, const TMetadataLoadContext& context);

        bool MetadataReady() const noexcept {
            return LoaderEnv->MetadataReady();
        }

        void CancelMetadata() noexcept {
            LoaderEnv->CancelMetadata();
        }

        void SetPageCollection(ui32 slot, TCacheCollectionRef collection);
        // Prepare the part layout from slot 0 before other collection metadata arrives
        void PrepareRoot();

        bool HasOuterCollection() const {
            Y_ENSURE(RootPrepared, "Root metadata must be prepared before resolving collection roles");
            return SmallId != Max<TPageId>();
        }

        TFetch Run(TRunOptions options)
        {
            while (Stage < EStage::Result) {
                TFetch fetch;

                switch (Stage) {
                    case EStage::Meta:
                        StageParseMeta();
                        break;
                    case EStage::PartView:
                        fetch = StageCreatePartView(options.PreloadIndex);
                        break;
                    case EStage::Slice:
                        fetch = StageSliceBounds();
                        break;
                    case EStage::Deltas:
                        StageDeltas();
                        break;
                    case EStage::PreloadData:
                        if (options.PreloadData) {
                            fetch = StagePreloadData();
                        }
                        break;
                    default:
                        break;
                }

                if (fetch) {
                    return fetch;
                }

                Stage = EStage(ui8(Stage) + 1);
            }

            return { };
        }

        void Save(TVector<NSharedCache::TEvResult::TLoaded>&& pages);
        void Save(NActors::IEventBase& event);

        THolder<NActors::IEventBase> MakeRequest(
            NTabletFlatExecutor::NBlockIO::EPriority priority, TFetch fetch, ui64 cookie = 0) const {
            return LoaderEnv->MakeRequest(priority, std::move(fetch), cookie);
        }

        constexpr static bool NeedIn(EPage page) noexcept
        {
            return
                page == EPage::Scheme
                || page == EPage::Frames || page == EPage::Globs
                || page == EPage::Schem2 || page == EPage::Bloom
                || page == EPage::GarbageStats
                || page == EPage::TxIdStats;
        }

        static constexpr bool ShouldKeepSticky(EPage page) noexcept {
            return NeedIn(page) || page == EPage::FlatIndex;
        }

        TPartView Result()
        {
            Y_ENSURE(Stage == EStage::Result);
            Y_ENSURE(PartView, "Result may only be grabbed once");
            Y_ENSURE(PartView.Slices, "Missing slices in Result stage");

            return std::move(PartView);
        }

        static TEpoch GrabEpoch(const TPartComponents &pc)
        {
            Y_ENSURE(pc.PageCollectionComponents, "PartComponents should have at least one pageCollectionComponent");
            Y_ENSURE(pc.PageCollectionComponents[0].RawMeta, "PartComponents should have raw meta data");

            const auto &comp = pc.PageCollectionComponents[0];
            NPageCollection::TMeta meta(TSharedData(comp.RawMeta), comp.LargeGlobId.Group);

            for (ui32 page = meta.TotalPages(); page--;) {
                if (meta.GetPageType(page) == ui32(EPage::Schem2)
                    || meta.GetPageType(page) == ui32(EPage::Scheme))
                {
                    TProtoBox<NProto::TRoot> root(meta.GetPageInplaceData(page));

                    Y_ENSURE(root.HasEpoch());

                    return TEpoch(root.GetEpoch());
                }
            }

            Y_TABLET_ERROR("Cannot locate part metadata in page collections of PartComponents");
        }

        static TLogoBlobID BlobsLabelFor(const TLogoBlobID &base) noexcept
        {
            /* By convention IPageCollection blobs label for page collection has the same logo
                as the meta logo but Cookie + 1. Blocks writer always make a
                gap between two subsequent meta TLargeGlobIds thus this value does
                not overlap any real TLargeGlobId leader blob.
             */

            return
                TLogoBlobID(
                    base.TabletID(), base.Generation(), base.Step(),
                    base.Channel(), 0 /* size */, base.Cookie() + 1);
        }

    private:
        TPartSlots GetSlots() const noexcept
        {
            return TPartSlots(Max(BTreeGroupIndexes.size(), FlatGroupIndexes.size()), PartLayout);
        }

        bool HasBasics() const noexcept
        {
            return SchemeId != Max<TPageId>() &&
                (FlatGroupIndexes || BTreeGroupIndexes);
        }

        void ParseMeta(TArrayRef<const char> plain)
        {
            TMemoryInput stream(plain.data(), plain.size());
            bool parsed = Root.ParseFromArcadiaStream(&stream);
            Y_ENSURE(parsed && stream.Skip(1) == 0, "Cannot parse TPart meta");
            Y_ENSURE(Root.HasEpoch(), "TPart meta has no epoch info");
        }

        void ApplyIndexSelection(ui32 slot);
        bool PrepareMetadataRoot(ui32 attachedSlot);
        void StageParseMeta();
        TFetch StageCreatePartView(bool preloadIndex);
        TFetch StageSliceBounds();
        void StageDeltas();
        TFetch StagePreloadData();

    private:
        TPartComponents Components;
        const TString Legacy;
        const TString Opaque;
        const TVector<TString> Deltas;
        const TEpoch Epoch;
        EStage Stage = EStage::Meta;
        bool RootPrepared = false;
        bool Rooted = false; /* Has full topology metablob */
        TPageId SchemeId = Max<TPageId>();
        TPageId GlobsId = Max<TPageId>();
        TPageId LargeId = Max<TPageId>();
        TPageId SmallId = Max<TPageId>();
        TPageId ByKeyId = Max<TPageId>();
        struct TByKeyPrefixMeta {
            TPageId PageId = Max<TPageId>();
            ui32 PrefixColumns = 0;
        };
        TVector<TByKeyPrefixMeta> ByKeyPrefixMetas;
        TPageId GarbageStatsId = Max<TPageId>();
        TPageId TxIdStatsId = Max<TPageId>();
        TVector<TPageId> FlatGroupIndexes;
        TVector<TPageId> FlatHistoricIndexes;
        TVector<NPage::TBtreeIndexMeta> BTreeGroupIndexes;
        TVector<NPage::TBtreeIndexMeta> BTreeHistoricIndexes;
        TRowVersion MinRowVersion;
        TRowVersion MaxRowVersion;
        TPartLayout PartLayout;
        TDynBitMap StrippedV1ShadowSlots;
        NProto::TRoot Root;
        TPartView PartView;
        THolder<IEnv> LoaderEnv;
        struct TPreloadBTreeWalker {
            THolder<TBTreePartWalker> Walker;
            NPage::TGroupId GroupId;
            bool SkipDataPages = false;
        };
        TVector<TPreloadBTreeWalker> PreloadBTreeWalkers;
    };
}}
