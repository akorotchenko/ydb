#include "schemeshard_audit_log.h"
#include "schemeshard_impl.h"
#include "schemeshard_import.h"
#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_xxport__tx_base.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/ptr.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

void TSchemeShard::TXxport::TTxBase::CancelImportChecksumItem(
    NIceDb::TNiceDb& db, TImportInfo& info, ui32 itemIdx, TStringBuf issue)
{
    auto& item = info.Items.at(itemIdx);
    item.ChecksumState = TImportInfo::TItem::EChecksumState::None;
    item.State = TImportInfo::EState::Cancelled;
    item.WaitTxId = InvalidTxId;
    if (issue) {
        item.Issue = issue;
    }
    Self->PersistImportItemState(db, info, itemIdx);
}

void TSchemeShard::TXxport::TTxBase::StopImportChecksums(NIceDb::TNiceDb& db, TImportInfo& info) {
    for (ui32 i : xrange(info.Items.size())) {
        if (info.Items[i].ChecksumState != TImportInfo::TItem::EChecksumState::None) {
            CancelImportChecksumItem(db, info, i);
        }
    }
    StopImportChecksum(info.Id);
}

void TSchemeShard::TXxport::TTxBase::TryFinishImportCancellation(NIceDb::TNiceDb& db, const TImportInfo::TPtr& info) {
    using EState = TImportInfo::EState;
    using ESubState = TImportInfo::TItem::ESubState;
    if (info->State != EState::Cancellation || AnyOf(info->Items, [](const auto& item) {
            return (item.State == EState::Transferring || item.State == EState::BuildIndexes) &&
                   (item.WaitTxId != InvalidTxId || item.SubState == ESubState::Proposed);
        })) {
        return;
    }

    info->State = EState::Cancelled;
    info->EndTime = TAppData::TimeProvider->Now();
    Self->PersistImportState(db, *info);
    Self->EraseEncryptionKey(db, *info);
    SendNotificationsIfFinished(info);
    AuditLogImportEnd(*info.Get(), Self);
}

struct TSchemeShard::TImport::TTxCancel: public TSchemeShard::TXxport::TTxBase {
    TEvImport::TEvCancelImportRequest::TPtr Request;

    explicit TTxCancel(TSelf *self, TEvImport::TEvCancelImportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_IMPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        auto respond = [this, &request](Ydb::StatusIds::StatusCode status) -> bool {
            auto response = MakeHolder<TEvImport::TEvCancelImportResponse>(request.GetTxId());
            auto& proto = *response->Record.MutableResponse();

            proto.SetStatus(status);
            Send(Request->Sender, std::move(response), 0, Request->Cookie);

            return true;
        };

        auto it = Self->Imports.find(request.GetRequest().GetId());
        if (it == Self->Imports.end() || !IsSameDomain(it->second, request.GetDatabaseName())) {
            return respond(Ydb::StatusIds::NOT_FOUND);
        }

        TImportInfo::TPtr importInfo = it->second;
        NIceDb::TNiceDb db(txc.DB);

        // A manual cancel must stop the import's helper actors: each of them holds
        // a TImportInfo::TPtr, so leftover actors pin the whole import state in RAM
        // and their late replies would otherwise resurrect the cancelled import.
        auto killHelperActors = [&]() {
            if (auto schemaMappingGetter = std::exchange(importInfo->SchemaMappingGetter, {})) {
                Send(schemaMappingGetter, new TEvents::TEvPoisonPill());
                Self->RunningImportSchemeGetters.erase(schemaMappingGetter);
            }
            for (ui32 idx = 0; idx < importInfo->Items.size(); ++idx) {
                auto& item = importInfo->Items[idx];
                if (auto schemeGetter = std::exchange(item.SchemeGetter, {})) {
                    Send(schemeGetter, new TEvents::TEvPoisonPill());
                    Self->RunningImportSchemeGetters.erase(schemeGetter);
                }
                if (auto schemeQueryExecutor = std::exchange(item.SchemeQueryExecutor, {})) {
                    Send(schemeQueryExecutor, new TEvents::TEvPoisonPill());
                    Self->RunningImportSchemeQueryExecutors.erase(schemeQueryExecutor);
                }
                if (auto restorer = std::exchange(item.KesusRestorer, {})) {
                    Send(restorer, new TEvents::TEvPoisonPill());
                    Self->RunningImportKesusRestorers.erase(restorer);
                }
            }
        };

        switch (importInfo->State) {
        case TImportInfo::EState::Done:
        case TImportInfo::EState::Cancelled:
            return respond(Ydb::StatusIds::SUCCESS);

        case TImportInfo::EState::DownloadExportMetadata:
        case TImportInfo::EState::Waiting:
        case TImportInfo::EState::Cancellation:
            importInfo->Issue = "Cancelled manually";
            importInfo->State = TImportInfo::EState::Cancelled;
            StopImportChecksums(db, *importInfo);

            for (ui32 itemIdx : xrange(importInfo->Items.size())) {
                const auto& item = importInfo->Items.at(itemIdx);

                switch (item.State) {
                case TImportInfo::EState::Transferring:
                    if (item.WaitTxId != InvalidTxId) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                        Send(Self->SelfId(), CancelRestoreTableDataPropose(*importInfo, item.WaitTxId), 0, importInfo->Id);
                    } else if (item.SubState == TImportInfo::TItem::ESubState::Proposed) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                    }
                    break;

                case TImportInfo::EState::BuildIndexes:
                    if (item.WaitTxId != InvalidTxId) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                        Send(Self->SelfId(), CancelIndexBuildPropose(Self, *importInfo, item.WaitTxId), 0, importInfo->Id);
                    } else if (item.SubState == TImportInfo::TItem::ESubState::Proposed) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                    }
                    break;

                default:
                    break;
                }
            }

            killHelperActors();

            if (importInfo->State == TImportInfo::EState::Cancelled) {
                importInfo->EndTime = TAppData::TimeProvider->Now();
            }

            Self->PersistImportState(db, *importInfo);
            Self->EraseEncryptionKey(db, *importInfo);
            SendNotificationsIfFinished(importInfo);

            if (importInfo->IsFinished()) {
                AuditLogImportEnd(*importInfo.Get(), Self);
            }

            return respond(Ydb::StatusIds::SUCCESS);

        default:
            return respond(Ydb::StatusIds::UNDETERMINED);
        }
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxCancel

struct TSchemeShard::TImport::TTxCancelAck: public TSchemeShard::TXxport::TTxBase {
    const ui64 ImportId;
    const TTxId TxId;

    explicit TTxCancelAck(TSelf* self, ui64 importId, TTxId txId)
        : TXxport::TTxBase(self)
        , ImportId(importId)
        , TxId(txId)
    {
    }

    explicit TTxCancelAck(TSelf* self, TEvSchemeShard::TEvCancelTxResult::TPtr& ev)
        : TTxCancelAck(self, ev->Cookie, TTxId(ev->Get()->Record.GetTargetTxId()))
    {
    }

    explicit TTxCancelAck(TSelf* self, TEvIndexBuilder::TEvCancelResponse::TPtr& ev)
        : TTxCancelAck(self, ev->Cookie, TTxId(ev->Get()->Record.GetTxId()))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_IMPORT_ACK;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        if (!Self->Imports.contains(ImportId)) {
            return true;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(ImportId);
        NIceDb::TNiceDb db(txc.DB);

        if (importInfo->State != TImportInfo::EState::Cancellation) {
            return true;
        }

        for (ui32 i : xrange(importInfo->Items.size())) {
            auto& item = importInfo->Items.at(i);

            if ((item.State == TImportInfo::EState::Transferring || item.State == TImportInfo::EState::BuildIndexes) &&
                item.WaitTxId == TxId && item.ChecksumState == TImportInfo::TItem::EChecksumState::None) {
                item.State = TImportInfo::EState::Cancelled;
                Self->TxIdToImport.erase(TxId);
                Self->PersistImportItemState(db, *importInfo, i);
                TryFinishImportCancellation(db, importInfo);
                break;
            }
        }

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxCancelAck

ITransaction* TSchemeShard::CreateTxCancelImport(TEvImport::TEvCancelImportRequest::TPtr& ev) {
    return new TImport::TTxCancel(this, ev);
}

ITransaction* TSchemeShard::CreateTxCancelImportAck(TEvSchemeShard::TEvCancelTxResult::TPtr& ev) {
    return new TImport::TTxCancelAck(this, ev);
}

ITransaction* TSchemeShard::CreateTxCancelImportAck(TEvIndexBuilder::TEvCancelResponse::TPtr& ev) {
    return new TImport::TTxCancelAck(this, ev);
}

} // NSchemeShard
} // NKikimr
