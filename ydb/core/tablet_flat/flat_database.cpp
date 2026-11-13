#include "defs.h"
#include "flat_abi_evol.h"
#include "flat_database.h"
#include "flat_redo_writer.h"
#include "flat_redo_player.h"
#include "flat_dbase_naked.h"
#include "flat_dbase_apply.h"
#include "flat_dbase_annex.h"
#include "flat_dbase_sz_env.h"
#include "flat_part_shrink.h"
#include "flat_util_misc.h"
#include "flat_sausage_grind.h"
#include "util_fmt_abort.h"
#include <ydb/core/util/pb.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/stream/output.h>

namespace NKikimr {
namespace NTable {

bool TDatabaseCore::TChangeCounter::operator<(const TChangeCounter& rhs) const {
    if (Serial && rhs.Serial) {
        // When both counters have serial they can be compared directly
        return Serial < rhs.Serial;
    }

    if (Epoch == rhs.Epoch) {
        // When this counter is (0, epoch) but rhs is (non-zero, epoch), it
        // indicates rhs may have more changes. When serial is zero it means
        // the current memtable is empty, but rhs epoch is the same, so it
        // cannot have fewer changes.
        return Serial < rhs.Serial;
    }

    // The one with the smaller epoch must have fewer changes. In the worst
    // case that change may have been a flush (incrementing epoch and serial)
    // and then compact (possibly resetting serial to zero).
    return Epoch < rhs.Epoch;
}

TDatabaseState::TDatabaseState(TDatabaseImpl* databaseImpl)
    : DatabaseImpl(databaseImpl ? databaseImpl : new TDatabaseImpl(0, new TScheme, nullptr))
{}

TDatabaseState::~TDatabaseState() = default;

TDatabase::TDatabase(TDatabaseImpl* databaseImpl)
    : TDatabaseState(databaseImpl)
    , TDatabaseCore(static_cast<TDatabaseState&>(*this))
    , TOwningDatabaseReader(*this)
{}

TDatabase::~TDatabase() {
    TempIterators.clear();
}

const TScheme& TDatabaseCore::GetScheme() const noexcept
{
    return *State_.DatabaseImpl->Scheme;
}

TIntrusiveConstPtr<TRowScheme> TDatabaseCore::GetRowScheme(ui32 table) const
{
    return Require(table)->GetScheme();
}

template <class TPageRef>
TAutoPtr<TTableIterImpl<TPageRef>> TDatabaseReader<TPageRef>::Iterate(
    ui32 table, TRawVals key, TTagsRef tags, ELookup mode) const {
    Database_.CheckReadAllowed(table);

    const auto seekBy = [](TRawVals key, ELookup mode) {
        if (!key && mode != ELookup::ExactMatch) {
            /* Compatability mode with outer iterator interface that yields
             * begin() for non-exact lookups with empty key. Inner iterator
             * yields begin() for ({}, Lower) and end() for ({}, Upper).
             */

            return ESeek::Lower;

        } else {
            switch (mode) {
            case ELookup::ExactMatch:
                return ESeek::Exact;
            case ELookup::GreaterThan:
                return ESeek::Upper;
            case ELookup::GreaterOrEqualThan:
                return ESeek::Lower;
            }
        }

        Y_TABLET_ERROR("Don't know how to convert ELookup to ESeek mode");
    };

    return Database_.Require(table)->Iterate(key, tags, Env, seekBy(key, mode), TRowVersion::Max());
}

template <class TPageRef>
TAutoPtr<TTableIterImpl<TPageRef>> TDatabaseReader<TPageRef>::IterateExact(ui32 table, TRawVals key, TTagsRef tags,
    TRowVersion snapshot, const ITransactionMapPtr& visible, const ITransactionObserverPtr& observer) const {
    Database_.CheckReadAllowed(table);

    auto iter = Database_.Require(table)->Iterate(key, tags, Env, ESeek::Exact, snapshot, visible, observer);

    // N.B. ESeek::Exact produces iterators with limit=1

    return iter;
}

namespace {
    // There are many places that use a non-inclusive -inf/+inf
    // We special case empty keys anyway, so the requirement is temporarily relaxed
    static constexpr bool RelaxEmptyKeys = true;

    const char* IsAmbiguousRangeReason(const TKeyRange& range, ui32 keyColumnsCount) {
        if (!range.MinKey) {
            if (Y_UNLIKELY(!range.MinInclusive) && !RelaxEmptyKeys) {
                return "Ambiguous table range: empty MinKey must be inclusive";
            }
        } else if (range.MinKey.size() < keyColumnsCount) {
            if (Y_UNLIKELY(range.MinInclusive)) {
                return "Ambiguous table range: incomplete MinKey must be non-inclusive (any/+inf is ambiguous otherwise)";
            }
        } else if (Y_UNLIKELY(range.MinKey.size() > keyColumnsCount)) {
            return "Ambiguous table range: MinKey is too large";
        }

        if (!range.MaxKey) {
            if (Y_UNLIKELY(!range.MaxInclusive) && !RelaxEmptyKeys) {
                return "Ambiguous table range: empty MaxKey must be inclusive";
            }
        } else if (range.MaxKey.size() < keyColumnsCount) {
            if (Y_UNLIKELY(!range.MaxInclusive)) {
                return "Ambiguous table range: incomplete MaxKey must be inclusive (any/+inf is ambiguous otherwise)";
            }
        } else if (Y_UNLIKELY(range.MaxKey.size() > keyColumnsCount)) {
            return "Ambiguous table range: MaxKey is too large";
        }

        return nullptr;
    }

    bool IsAmbiguousRange(const TKeyRange& range, ui32 keyColumnsCount) {
        return IsAmbiguousRangeReason(range, keyColumnsCount) != nullptr;
    }
}

template <class TPageRef>
TAutoPtr<TTableIterImpl<TPageRef>> TDatabaseReader<TPageRef>::IterateRange(ui32 table, const TKeyRange& range,
    TTagsRef tags, TRowVersion snapshot, const ITransactionMapPtr& visible,
    const ITransactionObserverPtr& observer) const {
    Database_.CheckReadAllowed(table);

    Y_DEBUG_ABORT_UNLESS(!IsAmbiguousRange(range, Database_.Require(table)->GetScheme()->Keys->Size()), "%s",
        IsAmbiguousRangeReason(range, Database_.Require(table)->GetScheme()->Keys->Size()));

    ESeek seek = !range.MinKey || range.MinInclusive ? ESeek::Lower : ESeek::Upper;

    auto iter = Database_.Require(table)->Iterate(range.MinKey, tags, Env, seek, snapshot, visible, observer);

    if (range.MaxKey) {
        TCelled maxKey(range.MaxKey, *iter->Scheme->Keys, false);

        if (range.MaxInclusive) {
            iter->StopAfter(maxKey);
        } else {
            iter->StopBefore(maxKey);
        }
    }

    return iter;
}

template <class TPageRef>
TAutoPtr<TTableReverseIterImpl<TPageRef>> TDatabaseReader<TPageRef>::IterateRangeReverse(ui32 table,
    const TKeyRange& range, TTagsRef tags, TRowVersion snapshot, const ITransactionMapPtr& visible,
    const ITransactionObserverPtr& observer) const {
    Database_.CheckReadAllowed(table);

    Y_DEBUG_ABORT_UNLESS(!IsAmbiguousRange(range, Database_.Require(table)->GetScheme()->Keys->Size()), "%s",
        IsAmbiguousRangeReason(range, Database_.Require(table)->GetScheme()->Keys->Size()));

    ESeek seek = !range.MaxKey || range.MaxInclusive ? ESeek::Lower : ESeek::Upper;

    auto iter = Database_.Require(table)->IterateReverse(range.MaxKey, tags, Env, seek, snapshot, visible, observer);

    if (range.MinKey) {
        TCelled minKey(range.MinKey, *iter->Scheme->Keys, false);

        if (range.MinInclusive) {
            iter->StopAfter(minKey);
        } else {
            iter->StopBefore(minKey);
        }
    }

    return iter;
}

template <class TPageRef>
EReady TDatabaseReader<TPageRef>::Select(ui32 table, TRawVals key, TTagsRef tags, TRowState& row, ui64 flg,
    TRowVersion snapshot, const ITransactionMapPtr& visible, const ITransactionObserverPtr& observer) const& {
    TSelectStats stats;
    return Select(table, key, tags, row, stats, flg, snapshot, visible, observer);
}

template <class TPageRef>
EReady TDatabaseReader<TPageRef>::Select(ui32 table, TRawVals key, TTagsRef tags, TRowState& row, TSelectStats& stats,
    ui64 flg, TRowVersion snapshot, const ITransactionMapPtr& visible, const ITransactionObserverPtr& observer) const& {
    TempIterators.clear();
    Database_.CheckReadAllowed(table);

    auto prevSieved = stats.Sieved;
    auto prevWeeded = stats.Weeded;
    auto prevNoKey = stats.NoKey;
    auto prevInvisible = stats.InvisibleRowSkips;

    auto ready =
        Database_.Require(table)->Select(key, tags, Env, row, flg, snapshot, TempIterators, stats, visible, observer);
    Database_.State_.Change->Stats.SelectSieved += stats.Sieved - prevSieved;
    Database_.State_.Change->Stats.SelectWeeded += stats.Weeded - prevWeeded;
    Database_.State_.Change->Stats.SelectNoKey += stats.NoKey - prevNoKey;
    Database_.State_.Change->Stats.SelectInvisible += stats.InvisibleRowSkips - prevInvisible;

    return ready;
}

template <class TPageRef>
TSelectRowVersionResult TDatabaseReader<TPageRef>::SelectRowVersion(ui32 table, TRawVals key, ui64 readFlags,
    const ITransactionMapPtr& visible, const ITransactionObserverPtr& observer) const {
    return Database_.Require(table)->SelectRowVersion(key, Env, readFlags, visible, observer);
}

template <class TPageRef>
TSelectRowVersionResult TDatabaseReader<TPageRef>::SelectRowVersion(ui32 table, TArrayRef<const TCell> key,
    ui64 readFlags, const ITransactionMapPtr& visible, const ITransactionObserverPtr& observer) const {
    return Database_.Require(table)->SelectRowVersion(key, Env, readFlags, visible, observer);
}

template <class TPageRef>
TSelectRowVersionResult TDatabaseReader<TPageRef>::SelectRowVersionByKeyPrefix(
    ui32 table, TArrayRef<const TCell> key, const ITransactionObserverPtr& observer) const {
    return Database_.Require(table)->SelectRowVersionByKeyPrefix(key, Env, observer);
}

TSizeEnv TDatabase::CreateSizeEnv()
{
    return TSizeEnv(Env);
}


void TDatabase::CalculateReadSize(TSizeEnv& env, ui32 table, TRawVals minKey, TRawVals maxKey,
                                  TTagsRef tags, ui64 flg, ui64 items, ui64 bytes,
                                  EDirection direction, TRowVersion snapshot)
{
    CheckReadAllowed(table);

    auto readScope = Env->BeginOpBatch();
    TSelectStats stats;
    Require(table)->Precharge(minKey, maxKey, tags, &env, flg, items, bytes, direction, snapshot, stats);
}

template <class TPageRef>
TPrechargeResult TDatabaseReader<TPageRef>::Precharge(ui32 table, TRawVals minKey, TRawVals maxKey, TTagsRef tags,
    ui64 flg, ui64 items, ui64 bytes, EDirection direction, TRowVersion snapshot)
{
    Database_.CheckPrechargeAllowed(table, minKey, maxKey);

    TSelectStats stats;
    auto result =
        Database_.Require(table)->Precharge(minKey, maxKey, tags, Env, flg, items, bytes, direction, snapshot, stats);
    Database_.State_.Change->Stats.ChargeSieved += stats.Sieved;
    Database_.State_.Change->Stats.ChargeWeeded += stats.Weeded;
    return result;
}

void TDatabaseCore::Update(ui32 table, ERowOp rop, TRawVals key, TArrayRef<const TUpdateOp> ops, TRowVersion rowVersion)
{
    Y_DEBUG_ABORT_UNLESS(rowVersion != TRowVersion::Max(), "Updates cannot have v{max} as row version");

    for (size_t index = 0; index < key.size(); ++index) {
        if (auto error = NScheme::HasUnexpectedValueSize(key[index])) {
            Y_TABLET_ERROR("Key index " << index << " validation failure: " << error);
        }
    }

    NRedo::IAnnex* annex = State_.Annex.Get();
    NRedo::IAnnex::TLimit limit = annex->Limit(table);
    limit.MinExternSize = Max(limit.MinExternSize, State_.DatabaseImpl->AnnexByteLimit());

    if (State_.ModifiedRefs.size() < ops.size()) {
        State_.ModifiedRefs.resize(FastClp2(ops.size()));
    }
    State_.ModifiedOps.assign(ops.begin(), ops.end());
    for (size_t index = 0; index < State_.ModifiedOps.size(); ++index) {
        TUpdateOp& op = State_.ModifiedOps[index];
        if (auto error = NScheme::HasUnexpectedValueSize(op.Value)) {
            Y_TABLET_ERROR("Op index " << index << " tag " << op.Tag << " validation failure: " << error);
        }

        if (op.Value.IsEmpty()) {
            // Null values lose type id in redo log
            op.Value = {};
            // Null values must change ECellOp::Set to ECellOp::Null in redo log
            op.Op = op.NormalizedCellOp();
        }

        Y_ENSURE(op.Op == ELargeObj::Inline, "User provided an invalid ECellOp");

        TArrayRef<const char> raw = op.AsRef();
        if (limit.IsExtern(raw.size())) {
            if (auto got = annex->Place(table, op.Tag, raw)) {
                State_.ModifiedRefs[index] = got.Ref;
                const auto payload = NUtil::NBin::ToRef(State_.ModifiedRefs[index]);
                op.Value = TRawTypeValue(payload, op.Value.Type());
                op.Op = ELargeObj::Extern;
            }
        }
    }

    State_.Redo->EvUpdate(table, rop, key, State_.ModifiedOps, rowVersion);
    RequireForUpdate(table)->Update(rop, key, State_.ModifiedOps, State_.Annex->Current(), rowVersion);
}

void TDatabaseCore::UpdateTx(ui32 table, ERowOp rop, TRawVals key, TArrayRef<const TUpdateOp> ops, ui64 txId, ui32 savepointSeqNum)
{
    for (size_t index = 0; index < key.size(); ++index) {
        if (auto error = NScheme::HasUnexpectedValueSize(key[index])) {
            Y_TABLET_ERROR("Key index " << index << " validation failure: " << error);
        }
    }

    State_.ModifiedOps.assign(ops.begin(), ops.end());
    for (size_t index = 0; index < State_.ModifiedOps.size(); ++index) {
        TUpdateOp& op = State_.ModifiedOps[index];
        if (auto error = NScheme::HasUnexpectedValueSize(op.Value)) {
            Y_TABLET_ERROR("Op index " << index << " tag " << op.Tag << " validation failure: " << error);
        }

        if (op.Value.IsEmpty()) {
            // Null values lose type id in redo log
            op.Value = {};
            // Null values must change ECellOp::Set to ECellOp::Null in redo log
            op.Op = op.NormalizedCellOp();
        }

        Y_ENSURE(op.Op == ELargeObj::Inline, "User provided an invalid ECellOp");

        // FIXME: we cannot handle blob references during scans, so we
        //        avoid creating large objects when they are in deltas
    }

    State_.Redo->EvUpdateTx(table, rop, key, State_.ModifiedOps, txId, savepointSeqNum);
    RequireForUpdate(table)->UpdateTx(rop, key, State_.ModifiedOps, State_.Annex->Current(), txId, savepointSeqNum);
}

void TDatabaseCore::LockRowTx(ui32 table, ELockMode mode, TRawVals key, ui64 txId)
{
    Y_ENSURE(mode != ELockMode::None);

    for (size_t index = 0; index < key.size(); ++index) {
        if (auto error = NScheme::HasUnexpectedValueSize(key[index])) {
            Y_TABLET_ERROR("Key index " << index << " validation failure: " << error);
        }
    }

    State_.Redo->EvLockRowTx(table, mode, key, txId);
    RequireForUpdate(table)->LockRowTx(mode, key, txId);
}

void TDatabaseCore::RemoveTx(ui32 table, ui64 txId)
{
    State_.Redo->EvRemoveTx(table, txId);
    RequireForUpdate(table)->RemoveTx(txId);
}

void TDatabaseCore::CommitTx(ui32 table, ui64 txId, TRowVersion rowVersion)
{
    State_.Redo->EvCommitTx(table, txId, rowVersion);
    RequireForUpdate(table)->CommitTx(txId, rowVersion);
}

bool TDatabaseCore::HasOpenTx(ui32 table, ui64 txId) const
{
    return Require(table)->HasOpenTx(txId);
}

bool TDatabaseCore::HasTxData(ui32 table, ui64 txId) const
{
    return Require(table)->HasTxData(txId);
}

bool TDatabaseCore::HasCommittedTx(ui32 table, ui64 txId) const
{
    return Require(table)->HasCommittedTx(txId);
}

bool TDatabaseCore::HasRemovedTx(ui32 table, ui64 txId) const
{
    return Require(table)->HasRemovedTx(txId);
}

const absl::flat_hash_set<ui64>& TDatabaseCore::GetOpenTxs(ui32 table) const
{
    return Require(table)->GetOpenTxs();
}

size_t TDatabaseCore::GetOpenTxCount(ui32 table) const
{
    return Require(table)->GetOpenTxCount();
}

size_t TDatabaseCore::GetTxsWithDataCount(ui32 table) const
{
    return Require(table)->GetTxsWithDataCount();
}

size_t TDatabaseCore::GetTxsWithStatusCount(ui32 table) const
{
    return Require(table)->GetTxsWithStatusCount();
}

size_t TDatabaseCore::GetCommittedTxCount(ui32 table) const
{
    return Require(table)->GetCommittedTxCount();
}

size_t TDatabaseCore::GetRemovedTxCount(ui32 table) const
{
    return Require(table)->GetRemovedTxCount();
}

void TDatabaseCore::RemoveRowVersions(ui32 table, const TRowVersion& lower, const TRowVersion& upper)
{
    if (Y_LIKELY(lower < upper)) {
        State_.Change->RemovedRowVersions[table].push_back({ lower, upper });
    }
}

const TRowVersionRanges& TDatabaseCore::GetRemovedRowVersions(ui32 table) const
{
    if (auto& wrap = State_.DatabaseImpl->Get(table, false)) {
        return wrap->GetRemovedRowVersions();
    }

    static const TRowVersionRanges empty;
    return empty;
}

void TDatabaseCore::NoMoreReadsForTx() {
    State_.NoMoreReadsFlag = true;
}

void TDatabaseCore::NoMoreUnprechargedReadsForTx() {
    State_.NoMoreUnprechargedReadsFlag = true;
}

void TDatabase::Begin(TTxStamp stamp, IPages& env)
{
    Y_ENSURE(!State_.Redo, "Transaction already in progress");
    Y_ENSURE(!Env);
    State_.Annex = new TAnnex(*State_.DatabaseImpl->Scheme, State_.DatabaseImpl->Stats.NormalizedFreeSpaceShareByChannel);
    State_.Redo = new NRedo::TWriter;
    State_.DatabaseImpl->BeginTransaction();
    State_.Change = MakeHolder<TChange>(stamp, State_.DatabaseImpl->Serial());
    Env = &env;
    State_.NoMoreReadsFlag = false;
    State_.NoMoreUnprechargedReadsFlag = false;
    State_.PrechargedTables.clear();
}

void TDatabase::RollbackChanges()
{
    Y_ENSURE(State_.Redo, "Transaction is not in progress");
    Y_ENSURE(Env);

    TTxStamp stamp = State_.Change->Stamp;
    IPages& env = *Env;

    auto prod = Commit(stamp, false, nullptr);
    FinishRollback(stamp, env, std::move(prod));
}

void TDatabase::FinishRollback(TTxStamp stamp, IPages& env, TProd&& prod) {
    env.OnRollbackChanges();
    Begin(stamp, env);
    State_.Change->RollbackAffects = std::move(prod.Change->RollbackAffects);
}

TPartView TDatabaseCore::GetPartView(ui32 tableId, const TLogoBlobID &bundle) const {
    return Require(tableId)->GetPartView(bundle);
}

TVector<TPartView> TDatabaseCore::GetTableParts(ui32 tableId) const {
    return Require(tableId)->GetAllParts();
}

TVector<TIntrusiveConstPtr<TColdPart>> TDatabaseCore::GetTableColdParts(ui32 tableId) const {
    return Require(tableId)->GetColdParts();
}

void TDatabaseCore::EnumerateTableParts(ui32 tableId, const std::function<void(const TPartView&)>& callback) const {
    Require(tableId)->EnumerateParts(std::move(callback));
}

void TDatabaseCore::EnumerateTableColdParts(ui32 tableId, const std::function<void(const TIntrusiveConstPtr<TColdPart>&)>& callback) const {
    Require(tableId)->EnumerateColdParts(callback);
}

void TDatabaseCore::EnumerateTableTxStatusParts(ui32 tableId, const std::function<void(const TIntrusiveConstPtr<TTxStatusPart>&)>& callback) const {
    Require(tableId)->EnumerateTxStatusParts(callback);
}

void TDatabaseCore::EnumerateTxStatusParts(const std::function<void(const TIntrusiveConstPtr<TTxStatusPart>&)>& callback) const {
    State_.DatabaseImpl->EnumerateTxStatusParts(callback);
}

ui64 TDatabaseCore::GetTableMemSize(ui32 tableId, TEpoch epoch) const {
    return Require(tableId)->GetMemSize(epoch);
}

ui64 TDatabaseCore::GetTableMemAllocatedSize(ui32 tableId) const {
    return Require(tableId)->GetMemAllocatedSize();
}

ui64 TDatabaseCore::GetTableMemRowCount(ui32 tableId) const {
    return Require(tableId)->GetMemRowCount();
}

ui64 TDatabaseCore::GetTableMemOpsCount(ui32 tableId) const {
    return Require(tableId)->GetOpsCount();
}

ui64 TDatabaseCore::GetTableIndexSize(ui32 tableId) const {
    const auto& partStats = Require(tableId)->Stat().Parts;
    return partStats.FlatIndexBytes + partStats.BTreeIndexBytes;
}

ui64 TDatabaseCore::GetTableSearchHeight(ui32 tableId) const {
    return Require(tableId)->GetSearchHeight();
}

ui64 TDatabaseCore::EstimateRowSize(ui32 tableId) const {
    return Require(tableId)->EstimateRowSize();
}

const TDbStats& TDatabaseCore::Counters() const noexcept
{
    return State_.DatabaseImpl->Stats;
}

TDbRuntimeStats TDatabaseCore::RuntimeCounters() const noexcept
{
    return State_.DatabaseImpl->GetRuntimeStats();
}

void TDatabaseCore::UpdateApproximateFreeSharesByChannel(const THashMap<ui32, float>& approximateFreeSpaceShareByChannel)
{
    for (auto& [channel, value] : approximateFreeSpaceShareByChannel) {
        State_.DatabaseImpl->Stats.NormalizedFreeSpaceShareByChannel[channel] = NUtil::NormalizeFreeSpaceShare(value);
    }
}

void TDatabaseCore::SetTableObserver(ui32 table, TIntrusivePtr<ITableObserver> ptr)
{
    Require(table)->SetTableObserver(std::move(ptr));
}

TDatabaseCore::TChangeCounter TDatabaseCore::Head(ui32 table) const
{
    if (table == Max<ui32>()) {
        return { State_.DatabaseImpl->StableSerial(), TEpoch::Max() };
    } else {
        auto &wrap = State_.DatabaseImpl->Get(table, true);

        return { wrap.StableSerial(), wrap.StableHead() };
    }
}

TString TDatabaseCore::SnapshotToLog(ui32 table, TTxStamp stamp)
{
    Y_ENSURE(!State_.Redo, "Cannot SnapshotToLog inside a transaction");

    auto scn = State_.DatabaseImpl->Serial() + 1;
    auto epoch = State_.DatabaseImpl->Get(table, true)->Snapshot();

    State_.DatabaseImpl->Rewind(scn);

    NRedo::TWriter writer;
    writer.EvBegin(ui32(ECompatibility::Head), ui32(ECompatibility::Edge), scn, stamp);
    writer.EvFlush(table, stamp, epoch);
    return std::move(writer).Finish();
}

TEpoch TDatabaseCore::TxSnapTable(ui32 table)
{
    Y_ENSURE(State_.Redo, "Cannot TxSnapTable outside a transaction");
    ++State_.Change->Snapshots;
    return State_.DatabaseImpl->FlushTable(table);
}

void TDatabaseCore::Truncate(ui32 table)
{
    Y_ENSURE(State_.Redo, "Cannot Truncate outside a transaction");
    ++State_.Change->Snapshots;
    State_.DatabaseImpl->TruncateTable(table);
}

TAutoPtr<TSubset> TDatabaseCore::CompactionSubset(ui32 table, TEpoch before, TArrayRef<const TLogoBlobID> bundle) const
{
    return Require(table)->CompactionSubset(before, bundle);
}

TAutoPtr<TSubset> TDatabaseCore::PartSwitchSubset(ui32 table, TEpoch before, TArrayRef<const TLogoBlobID> bundle, TArrayRef<const TLogoBlobID> txStatus) const
{
    return Require(table)->PartSwitchSubset(before, bundle, txStatus);
}

TAutoPtr<TSubset> TDatabase::Subset(ui32 table, TEpoch before, TRawVals from, TRawVals to) const
{
    auto subset = Require(table)->Subset(before);

    if (from || to) {
        Y_ENSURE(!subset->Frozen, "Got subset with frozens, cannot shrink it");
        Y_ENSURE(!subset->ColdParts, "Got subset with cold parts, cannot shrink it");

        TShrink shrink(Env, subset->Scheme->Keys);

        if (shrink.Put(subset->Flatten, from, to).Skipped) {
            return nullptr; /* Cannot shrink due to lack of some pages */
        } else {
            subset->Flatten = std::move(shrink.PartView);
        }
    }

    return subset;
}

TAutoPtr<TSubset> TDatabaseCore::ScanSnapshot(ui32 table, TRowVersion snapshot)
{
    return Require(table)->ScanSnapshot(snapshot);
}

bool TDatabaseCore::HasBorrowed(ui32 table, ui64 selfTabletId) const
{
    return Require(table)->HasBorrowed(selfTabletId);
}

TBundleSlicesMap TDatabaseCore::LookupSlices(ui32 table, TArrayRef<const TLogoBlobID> bundles) const
{
    return Require(table)->LookupSlices(bundles);
}

void TDatabaseCore::ReplaceSlices(ui32 table, TBundleSlicesMap slices)
{
    return State_.DatabaseImpl->ReplaceSlices(table, std::move(slices));
}

void TDatabaseCore::Replace(
    ui32 table,
    const TSubset& subset,
    TArrayRef<const TPartView> newParts,
    TArrayRef<const TIntrusiveConstPtr<TTxStatusPart>> newTxStatus)
{
    return State_.DatabaseImpl->Replace(table, subset, newParts, newTxStatus);
}

void TDatabaseCore::Merge(ui32 table, TPartView partView)
{
    return State_.DatabaseImpl->Merge(table, std::move(partView));
}

void TDatabaseCore::Merge(ui32 table, TIntrusiveConstPtr<TColdPart> part)
{
    return State_.DatabaseImpl->Merge(table, std::move(part));
}

void TDatabaseCore::Merge(ui32 table, TIntrusiveConstPtr<TTxStatusPart> txStatus)
{
    return State_.DatabaseImpl->Merge(table, std::move(txStatus));
}

void TDatabaseCore::MergeDone(ui32 table)
{
    return State_.DatabaseImpl->MergeDone(table);
}

TAlter& TDatabaseCore::Alter()
{
    Y_ENSURE(State_.Redo, "Scheme change must be done within a transaction");

    return *(State_.Alter_ ? State_.Alter_ : (State_.Alter_ = new TAlter(State_.DatabaseImpl.Get())));
}

void TDatabase::DebugDumpTable(ui32 table, IOutputStream& str, const NScheme::TTypeRegistry& typeRegistry) const {
    str << "Table " << table << Endl;
    if (auto &wrap = State_.DatabaseImpl->Get(table, false))
        wrap->DebugDump(str, Env, typeRegistry);
    else
        str << "unknown" << Endl;
}

void TDatabase::DebugDump(IOutputStream& str, const NScheme::TTypeRegistry& typeRegistry) const {
    for (const auto& it: State_.DatabaseImpl->Scheme->Tables) {
        if (State_.DatabaseImpl->Get(it.first, false)) {
            str << "======= " << it.second.Name << " ======\n";
            DebugDumpTable(it.first, str, typeRegistry);
        }
    }
}

TKeyRangeCache* TDatabaseCore::DebugGetTableErasedKeysCache(ui32 table) const {
    if (auto &wrap = State_.DatabaseImpl->Get(table, false)) {
        return wrap->GetErasedKeysCache();
    } else {
        return nullptr;
    }
}

size_t TDatabaseCore::GetCommitRedoBytes() const
{
    Y_ENSURE(State_.Redo, "Transaction is not in progress");
    return State_.Redo->Bytes();
}

bool TDatabaseCore::HasChanges() const
{
    Y_ENSURE(State_.Redo, "Transaction is not in progress");

    return *State_.Redo || (State_.Alter_ && *State_.Alter_) || State_.Change->Snapshots || State_.Change->RemovedRowVersions;
}

TDatabase::TProd TDatabase::Commit(TTxStamp stamp, bool commit, TCookieAllocator* cookieAllocator)
{
    auto onPersistent = PrepareCommit(commit);
    return FinishCommit(stamp, commit, cookieAllocator, std::move(onPersistent));
}

TVector<std::function<void()>> TDatabase::PrepareCommit(bool commit)
{
    auto onPersistent = TDatabaseCore::PrepareCommit(commit);
    TempIterators.clear();
    return onPersistent;
}

TVector<std::function<void()>> TDatabaseCore::PrepareCommit(bool commit)
{
    TVector<std::function<void()>> onPersistent;

    if (commit) {
        for (auto& callback : State_.OnCommit_) {
            callback();
        }
        onPersistent = std::move(State_.OnPersistent_);
    } else {
        auto it = State_.OnRollback_.rbegin();
        auto end = State_.OnRollback_.rend();
        while (it != end) {
            (*it)();
            ++it;
        }
    }

    State_.OnCommit_.clear();
    State_.OnRollback_.clear();
    State_.OnPersistent_.clear();

    return onPersistent;
}

TDatabase::TProd TDatabase::FinishCommit(
    TTxStamp stamp, bool commit, TCookieAllocator* cookieAllocator, TVector<std::function<void()>> onPersistent)
{
    auto prod = TDatabaseCore::FinishCommit(stamp, commit, cookieAllocator, std::move(onPersistent));
    Env = nullptr;
    return prod;
}

TDatabaseCore::TProd TDatabaseCore::FinishCommit(
    TTxStamp stamp, bool commit, TCookieAllocator* cookieAllocator, TVector<std::function<void()>> onPersistent)
{
    if (commit && HasChanges()) {
        Y_ENSURE(stamp >= State_.Change->Stamp);
        Y_ENSURE(State_.DatabaseImpl->Serial() == State_.Change->Serial);

        // FIXME: Temporary hack for using up to date change stamp when scan
        //        is queued inside a transaction. In practice we just need to
        //        stop using empty commits for scans. See KIKIMR-5366 for
        //        details.
        const_cast<TTxStamp&>(State_.Change->Stamp) = stamp;

        NRedo::TWriter prefix{ };

        {
            // Chunks with events unknown to older versions require a newer evolution to read
            const ui32 head = Max(ui32(ECompatibility::Head), State_.Redo->RequiredEvolution());
            const ui32 edge = ui32(ECompatibility::Edge);

            prefix.EvBegin(head, edge, State_.Change->Serial, State_.Change->Stamp);
        }

        const auto offset = prefix.Bytes(); /* useful payload starts here */

        auto annex = State_.Annex->Unwrap();
        if (annex) {
            Y_ENSURE(cookieAllocator, "Have to provide TCookieAllocator with enabled annex");

            TVector<NPageCollection::TGlobId> blobs;

            blobs.reserve(annex.size());

            for (auto &one: annex) {
                auto glob = cookieAllocator->Do(one.GId.Logo.Channel(), one.Data.size());

                blobs.emplace_back(one.GId = glob);

                Y_ENSURE(glob.Logo.BlobSize(), "Blob cannot have zero bytes");
            }

            prefix.EvAnnex(blobs);
        }

        State_.DatabaseImpl->CommitTransaction(State_.Change->Stamp, annex, prefix);

        if (State_.Alter_ && *State_.Alter_) {
            auto delta = State_.Alter_->Flush();
            Y_PROTOBUF_SUPPRESS_NODISCARD delta->SerializeToString(&State_.Change->Scheme);
        }

        prefix.Join(std::move(*State_.Redo));

        State_.Change->Redo = std::move(prefix).Finish();

        for (const auto& xpair : State_.Change->RemovedRowVersions) {
            if (auto& wrap = State_.DatabaseImpl->Get(xpair.first, false)) {
                for (const auto& range : xpair.second) {
                    wrap->RemoveRowVersions(range.Lower, range.Upper);
                }
            }
        }

        State_.Change->Garbage = std::move(State_.DatabaseImpl->Garbage);
        State_.Change->Deleted = std::move(State_.DatabaseImpl->Deleted);
        State_.Change->Truncated = std::move(State_.DatabaseImpl->Truncated);
        State_.Change->Affects = State_.DatabaseImpl->GrabAffects();
        State_.Change->Annex = std::move(annex);

        if (State_.Change->Redo.size() == offset && !State_.Change->Affects) {
            std::exchange(State_.Change->Redo, { }); /* omit complete NOOP redo */
        }

        if (State_.Change->Redo.size() > offset && !State_.Change->Affects) {
            Y_TABLET_ERROR(
                NFmt::Do(*State_.Change) << " produced " << (State_.Change->Redo.size() - offset)
                << "b of non technical redo without leaving effects on data");
        } else if (State_.Change->Redo && State_.Change->Serial != State_.DatabaseImpl->Serial()) {
            Y_TABLET_ERROR(
                NFmt::Do(*State_.Change) << " serial diverged from current db "
                << State_.DatabaseImpl->Serial() << " after rolling up redo log");
        } else if (State_.Change->Deleted.size() != State_.Change->Garbage.size()) {
            Y_TABLET_ERROR(NFmt::Do(*State_.Change) << " has inconsistent garbage data");
        }
    } else {
        auto affects = State_.DatabaseImpl->RollbackTransaction();
        State_.Change->RollbackAffects.insert(State_.Change->RollbackAffects.end(), affects.begin(), affects.end());
        SortUnique(State_.Change->RollbackAffects);
    }

    State_.DatabaseImpl->RunGC();

    State_.Redo = nullptr;
    State_.Annex = nullptr;
    State_.Alter_ = nullptr;

    return { std::move(State_.Change), std::move(onPersistent) };
}

TTable* TDatabaseCore::Require(ui32 table) const
{
    return State_.DatabaseImpl->Get(table, true).Self.Get();
}

TTable* TDatabaseCore::RequireForUpdate(ui32 table) const
{
    return State_.DatabaseImpl->GetForUpdate(table).Self.Get();
}

void TDatabaseCore::CheckReadAllowed(ui32 table) const
{
    Y_ENSURE(!State_.NoMoreReadsFlag, "Trying to read after reads prohibited, table " << table);
    if (State_.NoMoreUnprechargedReadsFlag) [[unlikely]] {
        Y_DEBUG_ABORT_UNLESS(
            std::find(State_.PrechargedTables.begin(), State_.PrechargedTables.end(), table) != State_.PrechargedTables.end(),
            "Trying to read a previously unprecharged table %u", table);
    }
}

void TDatabaseCore::CheckPrechargeAllowed(ui32 table, TRawVals minKey, TRawVals maxKey) const
{
    Y_ENSURE(!State_.NoMoreReadsFlag, "Trying to precharge after reads prohibited, table " << table);
    if (State_.NoMoreUnprechargedReadsFlag) [[unlikely]] {
        Y_DEBUG_ABORT_UNLESS(
            std::find(State_.PrechargedTables.begin(), State_.PrechargedTables.end(), table) != State_.PrechargedTables.end(),
            "Trying to precharge a previously unprecharged table %u", table);
    } else if (!minKey && !maxKey) {
        // Note: tables are usually fully precharged only in init transcations
        // This vector is small or empty most of the time and not worth using a hash table
        State_.PrechargedTables.push_back(table);
    }
}

TGarbage TDatabaseCore::RollUp(TTxStamp stamp, TArrayRef<const char> delta, TArrayRef<const char> redo,
                                TMemGlobs annex, TArrayRef<const ui32> truncatedTables, IRedoObserver* observer)
{
    Y_ENSURE(!annex || redo, "Annex have to be rolled up with redo log");

    State_.DatabaseImpl->Switch(stamp);

    if (delta) {
        TSchemeChanges changes;
        bool parseOk = ParseFromStringNoSizeLimit(changes, delta);
        Y_ENSURE(parseOk);

        State_.DatabaseImpl->ApplySchema(changes);
    }

    if (observer) {
        for (ui32 table : truncatedTables) {
            observer->OnTruncate(table);
        }
    }

    if (redo) {
        State_.DatabaseImpl->Assign(std::move(annex));
        State_.DatabaseImpl->ApplyRedo(redo, observer);
        State_.DatabaseImpl->GrabAnnex();
    }

    return std::move(State_.DatabaseImpl->Garbage);
}

void TDatabaseCore::RollUpRemoveRowVersions(ui32 table, const TRowVersion& lower, const TRowVersion& upper)
{
    if (auto& wrap = State_.DatabaseImpl->Get(table, false)) {
        wrap->RemoveRowVersions(lower, upper);
    }
}

TCompactionStats TDatabaseCore::GetCompactionStats(ui32 table) const
{
    return Require(table)->GetCompactionStats();
}

// NOTE: This helper should be used only to dump local DB contents in GDB
void DebugDumpDb(const TDatabase &db) {
    NScheme::TTypeRegistry typeRegistry;
    db.DebugDump(Cout, typeRegistry);
}

template class TDatabaseReader<TCachePageRef>;
template class TDatabaseReader<TCachePageConstRef>;
}}

Y_DECLARE_OUT_SPEC(, NKikimr::NTable::TDatabase::TChangeCounter, stream, value) {
    stream << "TChangeCounter{serial=";
    stream << value.Serial;
    stream << ", epoch=";
    stream << value.Epoch;
    stream << "}";
}
