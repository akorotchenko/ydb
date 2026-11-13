#pragma once
#include "defs.h"
#include "flat_exec_seat.h"
#include "flat_table_misc.h"
#include "flat_part_store.h"
#include "flat_store_hotdog.h"
#include "flat_store_solid.h"
#include "flat_sausagecache.h"
#include "tablet_flat_executor.h"
#include "flat_executor_snapshot.h"
#include "flat_direct_part_writer.h"
#include <ydb/core/util/pb.h>
#include <util/generic/hash_set.h>
#include <ydb/core/tablet_flat/protos/flat_executor.pb.h>
#include <optional>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TPageCollectionReadEnv : public NTable::IPages {
        class TBorrowedPages final : public NTable::IBorrowedPages {
        public:
            explicit TBorrowedPages(TPageCollectionReadEnv& env) noexcept
                : Env_(env)
            {}

            TResult Locate(const TMemTable* table, ui64 ref, ui32 tag) override {
                return Env_.Locate(table, ref, tag);
            }

            TResult Locate(const TPart* part, ui64 ref, ELargeObj lob) override {
                return Env_.Locate(part, ref, lob);
            }

            TCachePageConstRef TryGetPage(const TPart* part, const TPageLocation& location, TGroupId group) override {
                Y_ENSURE(!NTable::NPage::IsIndexPage(location.Type));
                const auto* store = CheckedCast<const NTable::TPartStore*>(part);
                return Env_.TryGetBorrowedPage(location, store->PageCollections.at(part->DataSlot(group)).Get());
            }

            TCachePageConstRef TryGetIndexPage(
                const TPart* part, const TPageLocation& location, TGroupId group) override {
                Y_ENSURE(location.Type == NTable::NPage::EPage::Undef || NTable::NPage::IsIndexPage(location.Type));
                const auto* store = CheckedCast<const NTable::TPartStore*>(part);
                return Env_.TryGetBorrowedPage(location, store->PageCollections.at(part->IndexSlot(group)).Get());
            }

            void OnRollbackChanges() override {
                Env_.OnRollbackChanges();
            }

        private:
            bool DoBeginOpBatch() noexcept override {
                return Env_.DoBeginOpBatch();
            }

            void DoEndOpBatch() noexcept override {
                Env_.DoEndOpBatch();
            }

            TPageCollectionReadEnv& Env_;
        };

        TPageCollectionReadEnv(TSeat& seat)
            : Seat(seat)
            , BorrowedPages_(*this)
        {}

        TPageCollectionReadEnv(const TPageCollectionReadEnv&) = delete;
        TPageCollectionReadEnv& operator=(const TPageCollectionReadEnv&) = delete;
        TPageCollectionReadEnv(TPageCollectionReadEnv&&) = delete;
        TPageCollectionReadEnv& operator=(TPageCollectionReadEnv&&) = delete;

        // Borrowed pages stay valid while the seat retains its native pins
        NTable::IBorrowedPages* BorrowedPages() & noexcept override {
            return &BorrowedPages_;
        }

        NTable::IBorrowedPages* BorrowedPages() && = delete;

        struct TStats {
            size_t NewlyPinnedPages = 0;
            ui64 NewlyPinnedBytes = 0;
    
            size_t ToLoadPages = 0;
            ui64 ToLoadBytes = 0;
        };

        const TStats& GetStats() const { return Stats; }

    protected: /* NTable::IPages, page collection backend implementation */
        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) override
        {
            return NTable::MemTableRefLookup(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            auto *info = partStore->Locate(lob, ref);
            auto page = TryGetPage(info->GetLocation(ref), info);

            if (!page && ReadMissingReferences) {
                MissingReferencesSize_ += Max<ui64>(1, part->GetPageSize(lob, ref));
            }

            return { !ReadMissingReferences, std::move(page) };
        }

        TCachePageRef TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
        {
            Y_ENSURE(!NTable::NPage::IsIndexPage(location.Type),
                "Index page " << location << " is requested as a data page");

            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            return TryGetPage(location, partStore->PageCollections.at(part->DataSlot(groupId)).Get());
        }

        TCachePageRef TryGetIndexPage(
            const TPart* part, const TPageLocation& location, TGroupId groupId) override {
            Y_ENSURE(location.Type == NTable::NPage::EPage::Undef || NTable::NPage::IsIndexPage(location.Type),
                "Page " << location << " is requested as an index page");

            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            return TryGetPage(location, partStore->PageCollections.at(part->IndexSlot(groupId)).Get());
        }

        void EnableReadMissingReferences() {
            ReadMissingReferences = true;
        }

        void DisableReadMissingReferences() {
            ReadMissingReferences = false;
            MissingReferencesSize_ = 0;
        }

        ui64 MissingReferencesSize() const
        {
            return MissingReferencesSize_;
        }

    private:
        using THashSetOfLocation = THashSet<TPageLocation, NTable::NPage::TPageLocationByOffsetHash>;

        TCachePageConstRef TryGetBorrowedPage(const TPageLocation& location, TCollection* pageCollection) {
            auto readScope = BeginOpBatch();
            return OpBatch_->ProcessPage([&](auto& cacheOp) -> TCachePageConstRef {
                NSharedCache::NDetail::TFoundItem collection, page;
                if (cacheOp.FindCollection(pageCollection->Id(), collection) == NSharedCache::ECacheResultStatus::Hit &&
                    collection.Item == pageCollection->CacheItem.CacheItem() && collection.State.IsReady() &&
                    collection.State.IsSticky() &&
                    cacheOp.FindPage(pageCollection->CacheItem, static_cast<ui64>(location.Offset), page) ==
                        NSharedCache::ECacheResultStatus::Hit &&
                    page.State.IsHot() && page.State.Frequency() == 2) {
                    if (const auto pinned = Seat.Pinned.find(NSharedCache::TPageCacheItem::FromValidated(page.Item));
                        pinned != Seat.Pinned.end()) {
                        return pinned->Borrow();
                    }
                }

                auto owned = pageCollection->TryGetPage(cacheOp, location);
                if (!owned) {
                    ToLoadPage(location, pageCollection);
                    return {};
                }
                const auto [pinned, inserted] = Seat.Pinned.emplace(std::move(owned));
                if (inserted) {
                    ++Stats.NewlyPinnedPages;
                    if (!pinned->IsSticky()) {
                        Stats.NewlyPinnedBytes += pinned->size();
                    }
                }
                return pinned->Borrow();
            });
        }

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

        void ToLoadPage(const TPageLocation& location, TCollection* pageCollection) {
            auto res = ToLoad[pageCollection->Id()].insert(location);
            if (res.second) {
                Stats.ToLoadPages++;
                Stats.ToLoadBytes += location.Size;
            } else {
                Y_ASSERT(res.first->Type == location.Type && res.first->Size == location.Size && res.first->Crc32 == location.Crc32);
            }
        }

        TCachePageRef TryGetPage(const TPageLocation& location, TCollection* pageCollection)
        {
            auto readScope = BeginOpBatch();
            auto sharedBody = OpBatch_->ProcessPage([&](auto& cacheOp) {
                return pageCollection->TryGetPage(cacheOp, location);
            });

            if (!sharedBody) {
                ToLoadPage(location, pageCollection);
                return {};
            }

            if (!Seat.Pinned.contains(sharedBody)) {
                Seat.Pinned.emplace(sharedBody.Acquire());

                Stats.NewlyPinnedPages++;
                if (!sharedBody.IsSticky()) {
                    Stats.NewlyPinnedBytes += sharedBody.size();
                }
            }

            return sharedBody;
        }

    public:
        auto ObtainToLoad() {
            THashMap<TLogoBlobID, TVector<TPageLocation>> result;
            for (auto& [pageCollectionId, pages_] : ToLoad) {
                TVector<TPageLocation> pages(pages_.begin(), pages_.end());
                std::sort(pages.begin(), pages.end());
                result.emplace(pageCollectionId, std::move(pages));
            }
            ToLoad.clear();
            return result;
        }

    private:
        TSeat& Seat;
        std::optional<TCache::TOpBatch> OpBatch_;

        THashMap<TLogoBlobID, THashSetOfLocation> ToLoad;

        bool ReadMissingReferences = false;
        ui64 MissingReferencesSize_ = 0;

        TStats Stats;
        TBorrowedPages BorrowedPages_;
    };

    struct TPageCollectionTxEnv : public TPageCollectionReadEnv, public IExecuting {
        TPageCollectionTxEnv(NTable::TDatabase& db, TSeat& seat)
            : TPageCollectionReadEnv(seat)
            , DB(db)
        { }

        using TLogoId = TLogoBlobID;

        struct TBorrowSnap {
            TIntrusivePtr<TTableSnapshotContext> SnapContext;
        };

        struct TBorrowUpdate {
            TDeque<ui64> StoppedLoans;
        };

        struct TLoanConfirmation {
            const TLogoId BorrowId;
        };

        struct TLoanBundle {
            TLoanBundle(ui32 sourceTableId, ui32 localTableId, ui64 lender, NTable::TPartComponents&& pc)
                : SourceTableId(sourceTableId)
                , LocalTableId(localTableId)
                , Lender(lender)
                , PartComponents(std::move(pc))
            {}

            const ui32 SourceTableId;
            const ui32 LocalTableId;
            const ui64 Lender;

            NTable::TPartComponents PartComponents;
        };

        struct TLoanTxStatus {
            TLoanTxStatus(ui32 sourceTableId, ui32 localTableId, ui64 lender,
                          const NPageCollection::TLargeGlobId& dataId, NTable::TEpoch epoch,
                          const TString& data)
                : SourceTableId(sourceTableId)
                , LocalTableId(localTableId)
                , Lender(lender)
                , DataId(dataId)
                , Epoch(epoch)
                , Data(data)
            {}

            const ui32 SourceTableId;
            const ui32 LocalTableId;
            const ui64 Lender;
            const NPageCollection::TLargeGlobId DataId;
            const NTable::TEpoch Epoch;
            const TString Data;
        };

        struct TSnapshot {
            TVector<TIntrusivePtr<TTableSnapshotContext>> Context;
            std::optional<NTable::TEpoch> Epoch;
        };

        using TPageCollectionReadEnv::TPageCollectionReadEnv;

        bool HasChanges() const
        {
            return
                DropSnap
                || MakeSnap
                || LoanBundle
                || LoanTxStatus
                || BorrowUpdates
                || LoanConfirmation
                || AttachedParts;
        }

    protected:
        void OnRollbackChanges() override {
            MakeSnap.clear();
            DropSnap.Reset();
            BorrowUpdates.clear();
            LoanBundle.clear();
            LoanTxStatus.clear();
            LoanConfirmation.clear();
            AttachedParts.clear();
        }

    protected: /* IExecuting, tx stage func implementation */
        void MakeSnapshot(TIntrusivePtr<TTableSnapshotContext> snap) override;

        void DropSnapshot(TIntrusivePtr<TTableSnapshotContext> snap) override
        {
            Y_ENSURE(!DropSnap, "only one snapshot per transaction");

            DropSnap.Reset(new TBorrowSnap{ snap });
        }

        void MoveSnapshot(const TTableSnapshotContext &snap, ui32 src, ui32 dst) override
        {
            snap.Impl->Moved(src, dst);
        }

        void ClearSnapshot(const TTableSnapshotContext &snap) override
        {
            snap.Impl->Clear();
        }

        // NOTE: It's allowed to add parts in the same Tx where the table gets created (and is not visible yet)
        void LoanTable(ui32 tableId, const TString &raw) override
        {
            TProtoBox<NKikimrExecutorFlat::TDatabaseBorrowPart> proto(raw);

            const ui64 lender = proto.GetLenderTablet();
            const ui32 source = proto.GetSourceTable();

            for (auto &part : proto.GetParts()) {
                Y_ENSURE(part.HasBundle(), "Cannot find attached hotdogs in borrow");

                LoanBundle.emplace_back(new TLoanBundle(source, tableId, lender,
                        TPageCollectionProtoHelper::MakePageCollectionComponents(part.GetBundle(), /* unsplit */ true)));
            }

            for (auto &part : proto.GetTxStatusParts()) {
                LoanTxStatus.emplace_back(new TLoanTxStatus(
                    source, tableId, lender,
                    TLargeGlobIdProto::Get(part.GetDataId()),
                    NTable::TEpoch(part.GetEpoch()),
                    part.GetData()));
            }
        }

        void AttachPart(ui32 tableId, THolder<TDirectPartResult> result) override
        {
            Y_ENSURE(result, "AttachPart called with an empty result");
            AttachedParts.emplace_back(TAttachedPart{ tableId, std::move(result) });
        }

        void CleanupLoan(const TLogoId &bundle, ui64 from) override
        {
            Y_ENSURE(!DropSnap, "must not drop snapshot and update loan in same transaction");
            BorrowUpdates[bundle].StoppedLoans.push_back(from);
        }

        void ConfirmLoan(const TLogoId &bundle, const TLogoId &borrow) override
        {
            LoanConfirmation.insert(std::make_pair(bundle, TLoanConfirmation{borrow}));
        }

        void EnableReadMissingReferences() override
        {
            TPageCollectionReadEnv::EnableReadMissingReferences();
        }

        void DisableReadMissingReferences() override
        {
            TPageCollectionReadEnv::DisableReadMissingReferences();
        }

        ui64 MissingReferencesSize() const override
        {
            return TPageCollectionReadEnv::MissingReferencesSize();
        }
    protected:
        NTable::TDatabase& DB;

    public:
        /*_ Pending database snapshots      */

        TMap<ui32, TSnapshot> MakeSnap;

        /*_ In tx tables borrow proto API   */

        THolder<TBorrowSnap> DropSnap;
        THashMap<TLogoId, TBorrowUpdate> BorrowUpdates;
        TVector<THolder<TLoanBundle>> LoanBundle;
        TVector<THolder<TLoanTxStatus>> LoanTxStatus;
        THashMap<TLogoId, TLoanConfirmation> LoanConfirmation;

        /*_ Directly written parts to attach as bottom layers */

        struct TAttachedPart {
            ui32 TableId;
            THolder<TDirectPartResult> Result;
        };
        TVector<TAttachedPart> AttachedParts;
    };

}
}
