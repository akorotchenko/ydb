#pragma once
#include "defs.h"
#include "flat_part_store.h"
#include "flat_part_walker.h"
#include "flat_sausagecache.h"
#include "shared_cache_events.h"
#include "cache.h"
#include "util_fmt_abort.h"
#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>
#include <ydb/core/util/pb.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/stream/mem.h>
#include <optional>

namespace NKikimr {
namespace NTable {

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

        struct TFetch : TMoveOnly {
            TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
            TVector<TPageLocation> Pages;
            TVector<bool> Sticky;

            explicit operator bool() const {
                return bool(Pages);
            }
        };

        struct TLoaderEnv : public IPages {
            TLoaderEnv(TVector<TCacheCollectionRef> pageCollections, TPartSlots slots)
                : PageCollections(std::move(pageCollections))
                , Slots(slots)
                , SavedPages(Slots.SlotsCount())
                , NeedPages(Slots.SlotsCount())
            {
                Y_ENSURE(PageCollections.size() >= Slots.SlotsCount());
            }

            TResult Locate(const TMemTable*, ui64, ui32) override
            {
                Y_TABLET_ERROR("IPages::Locate(TMemTable*, ...) shouldn't be used here");
            }

            TResult Locate(const TPart*, ui64, ELargeObj) override
            {
                Y_TABLET_ERROR("IPages::Locate(TPart*, ...) shouldn't be used here");
            }

            void ProvidePart(const TPart* part)
            {
                Y_ENSURE(!Part);
                Part = part;
            }

            TCachePageRef TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override {
                Y_ENSURE(part == Part, "Unsupported part");
                return TryGetSlotPage(Slots.DataSlot(groupId), location);
            }

            TCachePageRef TryGetIndexPage(
                const TPart* part, const TPageLocation& location, TGroupId groupId) override {
                Y_ENSURE(part == Part, "Unsupported part");
                return TryGetSlotPage(Slots.IndexSlot(groupId), location);
            }

            /**
             * Loads a page of the page collection, the outer page collection is not supported
             */
            TCachePageRef TryGetSlotPage(ui32 slot, const TPageLocation& location)
            {
                Y_ENSURE(slot < Slots.SlotsCount(), "Unsupported page collection " << slot);

                auto& savedPages = SavedPages[slot];
                auto savedPage = savedPages.find(location.Offset);

                if (savedPage == savedPages.end() || !savedPage->second) {
                    auto readScope = BeginRead();
                    if (!ReadOperation_) {
                        ReadOperation_.emplace(TCache::Get().BeginOperation());
                    }
                    if (auto page = PageCollections[slot]->TryGetPage(
                            *ReadOperation_, location, NeedIn(location.Type) || location.Type == EPage::FlatIndex)) {
                        // Retain the page while decoding, even if its cache entry is withdrawn.
                        AddSavedPage(slot, location.Offset, std::move(page));
                        savedPage = savedPages.find(location.Offset);
                    }
                }

                TCachePageRef result;
                if (savedPage != savedPages.end() && savedPage->second) {
                    result = savedPage->second.Acquire();
                } else {
                    auto [it, inserted] = NeedPages[slot].emplace(location);
                    Y_DEBUG_ABORT_UNLESS(inserted || *it == location);
                }
                // Saved-page visits also bound how long an opened native view stays active
                if (ReadOperation_ && ++ReadPageCount_ == ReadBatchSize) {
                    ReleaseReadOperation();
                }
                return result;
            }

            void EnsureNoNeedPages() const
            {
                for (const auto& needPages : NeedPages) {
                    Y_ENSURE(!needPages);
                }
            }

            /**
             * Returns missing pages of a single page collection: only one fetch may be outstanding and its pages
             * are saved before the next fetch. Pages of other page collections are requested by the next fetches
             * when the stage is run again, so stages must be safe to re-run
             */
            TFetch GetFetch()
            {
                for (ui32 slot : xrange(NeedPages.size())) {
                    if (const auto& needPages = NeedPages[slot]) {
                        TVector<TPageLocation> pages(needPages.begin(), needPages.end());
                        std::sort(pages.begin(), pages.end());
                        TVector<bool> sticky;
                        sticky.reserve(pages.size());
                        for (const auto& page : pages) {
                            sticky.push_back(NeedIn(page.Type) || page.Type == EPage::FlatIndex);
                        }
                        FetchSlot = slot;
                        return { .PageCollection = PageCollections[slot]->PageCollection(), .Pages = std::move(pages),
                            .Sticky = std::move(sticky) };
                    }
                }
                return {};
            }

            void Save(NPageCollection::TPageData&& page) {
                auto& needPages = NeedPages.at(FetchSlot);
                auto it = needPages.find(TPageLocation(page.Location.Offset));
                Y_ENSURE(it != needPages.end(), "Got unknown decoder page");
                needPages.erase(it);
                SavedPages.at(FetchSlot)[page.Location.Offset] =
                    TCache::Get().AdmitPage(PageCollections.at(FetchSlot)->PageCollection(), page.Location,
                        std::move(page.Data), NeedIn(page.Location.Type) || page.Location.Type == EPage::FlatIndex);
            }

            void Save(NSharedCache::TEvResult::TLoaded&& loaded)
            {
                auto& needPages = NeedPages.at(FetchSlot);
                auto it = needPages.find(TPageLocation(loaded.Offset));
                Y_ENSURE(it != needPages.end(), "Got unknown page at " << loaded.Offset << " of page collection " << FetchSlot);
                needPages.erase(it);

                AddSavedPage(FetchSlot, loaded.Offset, std::move(loaded.Page));
            }

        private:
            bool DoBeginRead() noexcept override {
                if (ReadScopeActive_) {
                    return false;
                }
                ReadScopeActive_ = true;
                return true;
            }

            void DoEndRead() noexcept override {
                ReleaseReadOperation();
                ReadScopeActive_ = false;
            }

            void ReleaseReadOperation() noexcept {
                ReadOperation_.reset();
                ReadPageCount_ = 0;
            }

            void AddSavedPage(ui32 slot, TPageOffset offset, TCachePageRef&& page)
            {
                SavedPages[slot][offset] = std::move(page);
            }

            struct TPageLocationByOffsetEq {
                bool operator()(const TPageLocation& a, const TPageLocation& b) const noexcept {
                    return a.Offset == b.Offset;
                }
            };

            const TPart* Part = nullptr;
            const TVector<TCacheCollectionRef> PageCollections;
            const TPartSlots Slots;
            // A logical scope may contain only saved pages and need no native operation
            bool ReadScopeActive_ = false;
            std::optional<TCache::TOperation> ReadOperation_;
            ui32 ReadPageCount_ = 0;
            static constexpr ui32 ReadBatchSize = 1024;
            TVector<THashMap<TPageOffset, TCachePageRef>> SavedPages;
            // Dedup by offset — keep Size/Type for the fetch request
            TVector<THashSet<TPageLocation, NPage::TPageLocationByOffsetHash, TPageLocationByOffsetEq>> NeedPages;
            ui32 FetchSlot = Max<ui32>(); // page collection of the last fetch
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
        ~TLoader();

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

        constexpr static bool NeedIn(EPage page) noexcept
        {
            return
                page == EPage::Scheme
                || page == EPage::Frames || page == EPage::Globs
                || page == EPage::Schem2 || page == EPage::Bloom
                || page == EPage::GarbageStats
                || page == EPage::TxIdStats;
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
        void StageParseMeta();
        TFetch StageCreatePartView(bool preloadIndex);
        TFetch StageSliceBounds();
        void StageDeltas();
        TFetch StagePreloadData();

    private:
        TVector<TCacheCollectionRef> PageCollections;
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
        TVector<NPage::TGroupId> StrippedV1Shadows; // groups with a stripped V1 shadow of the V2 b-tree index
        NProto::TRoot Root;
        TPartView PartView;
        THolder<TLoaderEnv> LoaderEnv;
        struct TPreloadBTreeWalker {
            THolder<TBTreePartWalker> Walker;
            NPage::TGroupId GroupId;
            bool SkipDataPages = false;
        };
        TVector<TPreloadBTreeWalker> PreloadBTreeWalkers;
    };
}}
