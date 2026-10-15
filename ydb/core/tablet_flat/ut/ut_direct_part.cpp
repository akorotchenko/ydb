#include "flat_executor_ut_common.h"
#include "flat_direct_part_writer.h"
#include "shared_sausagecache_state.h"
#include "test/libs/rows/layout.h"

#include <ydb/core/testlib/actors/wait_events.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

namespace NKikimr::NTable {

using namespace NTabletFlatExecutor;

namespace {

enum : ui32 {
    KeyColumnId = 1,
    ValueColumnId = 2,
};

enum EWriteMode {
    ModeCommit = 0,  // attach the built part within a transaction
    ModeAbort = 1,   // finalize, then release the barrier without committing
};

enum EEv {
    EvStartDirectWrite = EventSpaceBegin(TKikimrEvents::ES_USERSPACE) + 700,
    EvDirectWriteDone,
    EvCancelDirectWrite,
    EvReadinessHandled,
};

struct TEvStartDirectWrite : public TEventLocal<TEvStartDirectWrite, EvStartDirectWrite> {
    TEvStartDirectWrite(ui32 tableId, TVector<std::pair<i64, TString>> rows, EWriteMode mode)
        : TableId(tableId)
        , Rows(std::move(rows))
        , Mode(mode)
    { }

    ui32 TableId;
    TVector<std::pair<i64, TString>> Rows;
    EWriteMode Mode;
    // Optional row to write atomically into another table in the attach tx.
    bool HasCross = false;
    ui32 CrossTableId = 0;
    i64 CrossKey = 0;
    TString CrossValue;
    // Rewrite every blob put result to a non-OK status, forcing the writer to fail.
    bool FailPuts = false;
    // Respect CanFeed() backpressure: feed only while the writer accepts rows and
    // resume on blob put results, instead of feeding the whole batch at once.
    bool Throttle = false;
};

struct TEvCancelDirectWrite : public TEventLocal<TEvCancelDirectWrite, EvCancelDirectWrite> {};

struct TEvReadinessHandled : public TEventLocal<TEvReadinessHandled, EvReadinessHandled> {
    TEvReadinessHandled(TIntrusivePtr<NPageCollection::TPagesWaitPad> pad, bool accepted)
        : Pad(std::move(pad))
        , Accepted(accepted)
    {}

    TIntrusivePtr<NPageCollection::TPagesWaitPad> Pad;
    bool Accepted;
};

struct TEvDirectWriteDone : public TEventLocal<TEvDirectWriteDone, EvDirectWriteDone> {
    bool Ok;
    TString Error;
    // Set when the writer refused a row via CanFeed() at least once (throttle mode).
    bool SawBackpressure = false;

    explicit TEvDirectWriteDone(bool ok, TString error = {})
        : Ok(ok)
        , Error(std::move(error))
    { }
};

class TTxInitSchema : public ITransaction {
public:
    explicit TTxInitSchema(TVector<ui32> tableIds, ui32 extraColumns = 0)
        : TableIds(std::move(tableIds))
        , ExtraColumns(extraColumns)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TCompactionPolicy policy;
        policy.MinBTreeIndexNodeSize = 128;

        for (ui32 tableId : TableIds) {
            if (txc.DB.GetScheme().GetTableInfo(tableId)) {
                continue;
            }
            txc.DB.Alter()
                .AddTable("test" + ToString(tableId), tableId)
                .AddColumn(tableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false, false)
                .AddColumn(tableId, "value", ValueColumnId, NScheme::TString::TypeId, false, false)
                .AddColumnToKey(tableId, KeyColumnId)
                .SetCompactionPolicy(tableId, policy);
            for (ui32 index = 0; index < ExtraColumns; ++index) {
                txc.DB.Alter().AddColumn(
                    tableId, "padding_" + ToString(index), 100 + index, NScheme::TUint64::TypeId, false, false);
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    TVector<ui32> TableIds;
    const ui32 ExtraColumns;
};

// Reads the whole table at HEAD into a vector of (key, value) pairs.
class TTxReadAll : public ITransaction {
public:
    TTxReadAll(ui32 tableId, TVector<std::pair<i64, TString>>& out)
        : TableId(tableId)
        , Out(out)
    {
        Out.clear();
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Out.clear();
        TVector<NTable::TTag> tags{ KeyColumnId, ValueColumnId };
        auto iter = txc.DB.IterateRange(TableId, { }, tags);
        while (iter->Next(ENext::Data) == EReady::Data) {
            const auto& row = iter->Row();
            i64 key = row.Get(0).AsValue<i64>();
            TString value(row.Get(1).AsBuf());
            Out.emplace_back(key, std::move(value));
        }
        return iter->Last() != EReady::Page;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    ui32 TableId;
    TVector<std::pair<i64, TString>>& Out;
};

// Regular writes going through the memtable (upserts or erases), used to verify
// that they layer on top of a directly-written bottom part.
class TTxUpsertRows : public ITransaction {
public:
    TTxUpsertRows(ui32 tableId, TVector<std::pair<i64, TString>> rows, bool erase)
        : TableId(tableId)
        , Rows(std::move(rows))
        , Erase(erase)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        for (const auto& [k, v] : Rows) {
            i64 key = k;
            const auto keyCell = NScheme::TInt64::TInstance(key);
            if (Erase) {
                txc.DB.Update(TableId, NTable::ERowOp::Erase, { keyCell }, { });
            } else {
                TString value = v;
                const auto valCell = NScheme::TString::TInstance(value);
                NTable::TUpdateOp op{ ValueColumnId, NTable::ECellOp::Set, valCell };
                txc.DB.Update(TableId, NTable::ERowOp::Upsert, { keyCell }, { op });
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    ui32 TableId;
    TVector<std::pair<i64, TString>> Rows;
    bool Erase;
};

// Attaches a directly-written part as a bottom layer, optionally also writing a
// row to another table in the same transaction (cross-table atomicity).
class TTxAttachPart : public ITransaction {
public:
    TTxAttachPart(TActorId owner, ui32 tableId, THolder<TDirectPartResult> result,
                  bool hasCross, ui32 crossTableId, i64 crossKey, TString crossValue,
                  bool sawBackpressure)
        : Owner(owner)
        , TableId(tableId)
        , Result(std::move(result))
        , HasCross(hasCross)
        , CrossTableId(crossTableId)
        , CrossKey(crossKey)
        , CrossValue(std::move(crossValue))
        , SawBackpressure(sawBackpressure)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (HasCross) {
            const auto key = NScheme::TInt64::TInstance(CrossKey);
            const auto val = NScheme::TString::TInstance(CrossValue);
            NTable::TUpdateOp op{ ValueColumnId, NTable::ECellOp::Set, val };
            txc.DB.Update(CrossTableId, NTable::ERowOp::Upsert, { key }, { op });
        }
        txc.Env.AttachPart(TableId, std::move(Result));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto* done = new TEvDirectWriteDone(true);
        done->SawBackpressure = SawBackpressure;
        ctx.Send(Owner, done);
    }

private:
    TActorId Owner;
    ui32 TableId;
    THolder<TDirectPartResult> Result;
    bool HasCross;
    ui32 CrossTableId;
    i64 CrossKey;
    TString CrossValue;
    bool SawBackpressure;
};

// A test tablet that owns a TDirectPartWriter, feeds it rows, routes blob put
// results to it, and commits (or aborts) the built part.
class TDirectWriteTablet : public TActor<TDirectWriteTablet>, public TTabletExecutedFlat {
public:
    TDirectWriteTablet(const TActorId& tablet, TTabletStorageInfo* info, const TActorId& owner)
        : TActor(&TDirectWriteTablet::Inbox)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , Owner(owner)
    { }

private:
    using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;

    void Inbox(TEventHandlePtr& eh) {
        if (auto* ev = eh->CastAsLocal<NFake::TEvExecute>()) {
            for (auto& tx : ev->Txs) {
                Execute(tx.Release(), ActorContext());
            }
            for (auto& lambda : ev->Lambdas) {
                std::move(lambda)(Executor(), ActorContext());
            }
        } else if (eh->CastAsLocal<NFake::TEvReturn>()) {
            Send(Owner, new TEvents::TEvWakeup);
        } else if (auto* ev = eh->CastAsLocal<TEvStartDirectWrite>()) {
            StartWrite(ev, ActorContext());
        } else if (eh->CastAsLocal<TEvCancelDirectWrite>()) {
            Y_ENSURE(Writer);
            Executor()->ReleaseWritePart(Writer->Step());
            Writer.Reset();
            Send(Owner, new TEvDirectWriteDone(false, "cancelled"));
        } else if (auto* ev = eh->CastAsLocal<TEvBlobStorage::TEvPutResult>()) {
            if (Writer) {
                if (FailPuts) {
                    // Simulate a storage failure for a direct-write blob.
                    ev->Status = NKikimrProto::ERROR;
                }
                Writer->Handle(*ev, ActorContext());
                if (Feeding && !Writer->HasError()) {
                    FeedSome(ActorContext());
                } else {
                    TryComplete(ActorContext());
                }
            }
        } else if (auto* ev = eh->CastAsLocal<NSharedCache::TEvResult>()) {
            if (eh->Cookie == TWrittenPartLoader::EventCookie) {
                const bool accepted = Writer && Writer->Handle(*ev, ActorContext());
                if (ev->ResourcesReady) {
                    Send(Owner, new TEvReadinessHandled(ev->WaitPad, accepted));
                }
                if (accepted) {
                    TryComplete(ActorContext());
                }
            } else if (!TTabletExecutedFlat::HandleDefaultEvents(eh, SelfId())) {
                Y_TABLET_ERROR("Unexpected cache reply");
            }
        } else if (eh->CastAsLocal<TEvents::TEvPoison>()) {
            if (std::exchange(Stopping, true) != true) {
                auto ctx = ActorContext();
                Executor()->DetachTablet(), Detach(ctx);
            }
        } else if (State == EBoot) {
            TTabletExecutedFlat::StateInitImpl(eh, SelfId());
        } else if (eh->CastAsLocal<TEvTabletPipe::TEvServerConnected>()) {
        } else if (eh->CastAsLocal<TEvTabletPipe::TEvServerDisconnected>()) {
        } else if (!TTabletExecutedFlat::HandleDefaultEvents(eh, SelfId())) {
            Y_TABLET_ERROR("Unexpected event " << eh->GetTypeName());
        }
    }

    void StartWrite(TEvStartDirectWrite* msg, const TActorContext& ctx) {
        WriteTableId = msg->TableId;
        Mode = msg->Mode;
        HasCross = msg->HasCross;
        CrossTableId = msg->CrossTableId;
        CrossKey = msg->CrossKey;
        CrossValue = msg->CrossValue;
        FailPuts = msg->FailPuts;
        Throttle = msg->Throttle;

        Writer = Executor()->BeginWritePart(WriteTableId);
        Y_ENSURE(Writer, "BeginWritePart returned no writer");

        if (Throttle) {
            // Feed lazily, respecting CanFeed() backpressure (see FeedSome).
            PendingRows = msg->Rows;
            Cursor = 0;
            Feeding = true;
            FeedSome(ctx);
            return;
        }

        bool failed = false;
        for (const auto& [k, v] : msg->Rows) {
            if (!FeedRow(k, v, ctx)) {
                failed = true;
                break;
            }
        }

        if (failed) {
            ReportFailure(ctx);
            return;
        }

        Writer->Finalize(ctx);
        TryComplete(ctx);
    }

    // Builds a single row from (key, value) and appends it to the writer.
    bool FeedRow(i64 key, const TString& value, const TActorContext& ctx) {
        TCell keyCell = TCell::Make(key);
        TCell valCell(value.data(), value.size());
        const ui32 valueTag = ValueColumnId;
        TArrayRef<const TCell> keyRef(&keyCell, 1);
        return Writer->AddRow(keyRef, { &valCell, 1 }, { &valueTag, 1 }, TRowVersion::Min(), ctx);
    }

    // Feeds pending rows while the writer accepts them; stops on backpressure and
    // resumes from the blob-put-result handler once in-flight blobs drain.
    void FeedSome(const TActorContext& ctx) {
        while (Cursor < PendingRows.size() && Writer->CanFeed()) {
            const auto& [k, v] = PendingRows[Cursor];
            if (!FeedRow(k, v, ctx)) {
                ReportFailure(ctx);
                return;
            }
            ++Cursor;
        }

        if (Cursor < PendingRows.size()) {
            // The writer refused more rows: the in-flight blob budget is exhausted.
            Y_ENSURE(!Writer->CanFeed(), "Feeding stopped while the writer still accepts rows");
            SawBackpressure = true;
            return;
        }

        Feeding = false;
        Writer->Finalize(ctx);
        TryComplete(ctx);
    }

    void ReportFailure(const TActorContext& ctx) {
        TString error = Writer->Error();
        Executor()->ReleaseWritePart(Writer->Step());
        Writer.Reset();
        ctx.Send(Owner, new TEvDirectWriteDone(false, error));
    }

    void TryComplete(const TActorContext& ctx) {
        if (!Writer) {
            return;
        }
        if (Writer->HasError()) {
            ReportFailure(ctx);
            return;
        }
        if (!Writer->IsComplete()) {
            return;
        }

        if (Mode == ModeAbort) {
            Executor()->ReleaseWritePart(Writer->Step());
            Writer.Reset();
            ctx.Send(Owner, new TEvDirectWriteDone(true));
            return;
        }

        auto result = Writer->ExtractResult(ctx);
        Writer.Reset();
        Execute(new TTxAttachPart(Owner, WriteTableId, std::move(result),
                                  HasCross, CrossTableId, CrossKey, CrossValue,
                                  SawBackpressure), ctx);
    }

    void OnActivateExecutor(const TActorContext&) override {
        State = EWork;
        SignalTabletActive(SelfId());
        Send(Owner, new NFake::TEvReady(TabletID(), SelfId()));
    }

    void DefaultSignalTabletActive(const TActorContext&) override { }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) override {
        OnDetach(ctx);
    }

    void OnDetach(const TActorContext&) override {
        PassAway();
    }

    void Enqueue(TEventHandlePtr& eh) override {
        Y_TABLET_ERROR("Got unexpected event " << eh->GetTypeName() << " on tablet booting");
    }

private:
    enum EState { EBoot, EWork };

    TActorId Owner;
    EState State = EBoot;
    bool Stopping = false;

    THolder<TDirectPartWriter> Writer;
    ui32 WriteTableId = 0;
    EWriteMode Mode = ModeCommit;
    bool HasCross = false;
    ui32 CrossTableId = 0;
    i64 CrossKey = 0;
    TString CrossValue;
    bool FailPuts = false;

    // Throttled (backpressure-respecting) feeding state.
    bool Throttle = false;
    bool Feeding = false;
    bool SawBackpressure = false;
    size_t Cursor = 0;
    TVector<std::pair<i64, TString>> PendingRows;
};

struct TDirectWriteEnv : public TMyEnvBase {
    using TMyEnvBase::TMyEnvBase;
    void FireWriteTablet() {
        FireTablet(Edge, Tablet, [this](const TActorId& tablet, TTabletStorageInfo* info) {
            return new TDirectWriteTablet(tablet, info, Edge);
        });
        WaitFor<NFake::TEvReady>();
    }

    void RestartWriteTablet() {
        SendSync(new TEvents::TEvPoison, false, true);
        FireWriteTablet();
    }

    TVector<std::pair<i64, TString>> ReadAll(ui32 tableId) {
        TVector<std::pair<i64, TString>> rows;
        SendSync(new NFake::TEvExecute{ new TTxReadAll(tableId, rows) });
        return rows;
    }

    void Upsert(ui32 tableId, TVector<std::pair<i64, TString>> rows) {
        SendSync(new NFake::TEvExecute{ new TTxUpsertRows(tableId, std::move(rows), /* erase */ false) });
    }

    void Erase(ui32 tableId, const TVector<i64>& keys) {
        TVector<std::pair<i64, TString>> rows;
        for (i64 k : keys) {
            rows.emplace_back(k, TString());
        }
        SendSync(new NFake::TEvExecute{ new TTxUpsertRows(tableId, std::move(rows), /* erase */ true) });
    }
};

TVector<std::pair<i64, TString>> MakeRows(i64 count) {
    TVector<std::pair<i64, TString>> rows;
    for (i64 i = 0; i < count; ++i) {
        rows.emplace_back(i, TStringBuilder() << "value_" << i);
    }
    return rows;
}

// Rows whose values are large enough that feeding many of them exceeds the
// writer's in-flight blob budget (TDirectPartWriter::MaxFlight).
TVector<std::pair<i64, TString>> MakeBigRows(i64 count, size_t valueSize) {
    TVector<std::pair<i64, TString>> rows;
    for (i64 i = 0; i < count; ++i) {
        rows.emplace_back(i, TString(valueSize, char('a' + (i % 26))));
    }
    return rows;
}

struct TMetadataWaitEnv : TDirectWriteEnv {
    static constexpr ui64 InitialLimit = 512;
    using TCacheResult = NSharedCache::TEvResult;

    TMetadataWaitEnv()
        : TDirectWriteEnv(8_MB, 0)
    {
        auto& appData = Env.GetAppData();
        appData.FeatureFlags.SetEnableLocalDBBtreeIndex(false);
        appData.FeatureFlags.SetEnableLocalDBBtreeIndexV2(false);
        appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);
        FireWriteTablet();
        SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }, 128) });
        OriginalLimit = appData.SharedCache->CurrentLimit();
        BaselineUsage = appData.SharedCache->PageUsage();
        SetLimit(InitialLimit);
        Saves = Env.AddObserver<NSharedCache::TEvSaveCompactedPages>([&](const auto& ev) {
            for (const auto& page : ev->Get()->Pages) {
                if (page.Type == NPage::EPage::FlatIndex) {
                    FlatBytes = page.Size + TSharedData::OverheadSize;
                } else if (page.Type == NPage::EPage::Schem2) {
                    SchemeBytes = page.Size + TSharedData::OverheadSize;
                }
            }
        });
        Requests = Env.AddObserver<NSharedCache::TEvRequest>([&](const auto& ev) {
            if (ev->Cookie == TWrittenPartLoader::EventCookie && ev->Get()->WaitPad) {
                WriterActor = ev->Sender;
                Collection = ev->Get()->PageCollection;
                Pads.push_back(ev->Get()->WaitPad);
            }
        });
        Results = Env.AddObserver<TCacheResult>([&](TCacheResult::TPtr& ev) {
            if (ev->Cookie != TWrittenPartLoader::EventCookie || ev->GetRecipientRewrite() != WriterActor) {
                return;
            }
            if (ev->Get()->ResourcePressure) {
                ++PressureCount;
                if (HoldPressure) {
                    Pressure.push_back(std::move(ev));
                }
            } else if (ev->Get()->ResourcesReady) {
                ++ReadyCount;
            } else if (HoldLoaded && ev->Get()->Status == NKikimrProto::OK) {
                Loaded.push_back(std::move(ev));
            }
        });
    }

    void SetLimit(ui64 limit) {
        TWaitForFirstEvent<NMemory::TEvConsumerLimit> wait(Env);
        Env.Send(NSharedCache::MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(0, 0, limit));
        wait.Wait();
    }

    void Start() {
        SendAsync(new TEvStartDirectWrite(101, MakeRows(1), ModeCommit));
        Env.WaitFor("partial metadata pressure", [&] {
            return !Pressure.empty();
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(Pads.size(), 1);
        UNIT_ASSERT(Pads.front()->HasPinnedPages.load());
        UNIT_ASSERT(FlatBytes && FlatBytes <= InitialLimit);
        UNIT_ASSERT_C(SchemeBytes > InitialLimit, "schema bytes=" << SchemeBytes);
        UNIT_ASSERT_GT(Env.GetAppData().SharedCache->PageUsage(), BaselineUsage);
    }

    void DeliverPressure() {
        HoldPressure = false;
        auto ev = std::move(Pressure.front());
        Pressure.pop_front();
        Env.Send(ev.Release(), 0, true);
    }

    void SendReadiness(TIntrusivePtr<NPageCollection::TPagesWaitPad> pad) {
        auto* ready = new TCacheResult(Collection, NKikimrProto::OK, 0);
        ready->WaitPad = std::move(pad);
        ready->ResourcesReady = true;
        Env.Send(new IEventHandle(WriterActor, {}, ready, 0, TWrittenPartLoader::EventCookie), 0, true);
    }

    void Barrier() {
        SendSync(new NFake::TEvExecute{ [](auto*, const auto& ctx) {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
    }

    void CheckCompleted() {
        auto done = GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);
        UNIT_ASSERT(ReadAll(101) == MakeRows(1));
        RestartWriteTablet();
        UNIT_ASSERT(ReadAll(101) == MakeRows(1));
    }

    ui64 OriginalLimit = 0;
    ui64 BaselineUsage = 0;
    ui64 FlatBytes = 0, SchemeBytes = 0;
    ui32 PressureCount = 0, ReadyCount = 0;
    bool HoldPressure = true, HoldLoaded = false;
    TActorId WriterActor;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> Collection;
    TVector<TIntrusivePtr<NPageCollection::TPagesWaitPad>> Pads;
    TDeque<TCacheResult::TPtr> Pressure, Loaded;
    TTestActorRuntime::TEventObserverHolder Saves, Requests, Results;
};

} // namespace

Y_UNIT_TEST_SUITE(TDirectPartWrite) {

    Y_UNIT_TEST(ReviewR13PartialMetadataHitReleasesPinsBeforeRestart) {
        TMetadataWaitEnv env;
        env.Start();
        auto oldPad = env.Pads.front();
        env.DeliverPressure();
        env->WaitFor("metadata pins released", [&] {
            return env->GetAppData().SharedCache->PageUsage() == env.BaselineUsage;
        }, TDuration::Seconds(5));
        env.SetLimit(env.OriginalLimit);
        env.CheckCompleted();
        UNIT_ASSERT_GE(env.Pads.size(), 2);
        UNIT_ASSERT(env.Pads[1] != oldPad);
    }

    Y_UNIT_TEST(ReviewR13HandlesEarlyAndStaleReadiness) {
        TMetadataWaitEnv env;
        env.HoldLoaded = true;
        env.Start();
        auto oldPad = env.Pads.front();
        env.SetLimit(env.OriginalLimit);
        env->WaitFor("readiness before pressure", [&] {
            return env.ReadyCount != 0;
        }, TDuration::Seconds(5));
        auto earlyReady = env.GrabEdgeEvent<TEvReadinessHandled>();
        UNIT_ASSERT(earlyReady->Get()->Accepted);
        UNIT_ASSERT(earlyReady->Get()->Pad == oldPad);
        env.Barrier();
        UNIT_ASSERT_VALUES_EQUAL(env.Pads.size(), 1);
        env.DeliverPressure();
        env->WaitFor("restarted metadata result", [&] {
            return !env.Loaded.empty();
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(env.Pads.size(), 2);
        UNIT_ASSERT(env.Pads.back() != oldPad);
        auto unrelatedPad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
        env.SendReadiness(unrelatedPad);
        env.SendReadiness(oldPad);
        for (ui32 index = 0; index < 2; ++index) {
            auto rejected = env.GrabEdgeEvent<TEvReadinessHandled>();
            UNIT_ASSERT(!rejected->Get()->Accepted);
            UNIT_ASSERT(rejected->Get()->Pad == unrelatedPad || rejected->Get()->Pad == oldPad);
        }
        env.Barrier();
        UNIT_ASSERT_VALUES_EQUAL(env.Pads.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Loaded.size(), 1);
        env.HoldLoaded = false;
        env->Send(env.Loaded.front().Release(), 0, true);
        env.Loaded.pop_front();
        env.CheckCompleted();
    }

    Y_UNIT_TEST(ReviewR13CancelWhileWaitingReleasesRegistration) {
        TMetadataWaitEnv env;
        env.Start();
        auto oldPad = env.Pads.front();
        env.DeliverPressure();
        env->WaitFor("metadata pins released before cancellation", [&] {
            return env->GetAppData().SharedCache->PageUsage() == env.BaselineUsage;
        }, TDuration::Seconds(5));
        TWaitForFirstEvent<NSharedCache::TEvDetach> detached(*env);
        env.SendAsync(new TEvCancelDirectWrite);
        auto cancelled = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT(!cancelled->Get()->Ok);
        detached.Wait();
        env.SendSync(new NFake::TEvExecute{ [&](auto*, const auto& ctx) {
            auto& cache = NSharedCache::TSharedCache::Get();
            auto binding = cache.BindCurrentThreadHazard();
            TSharedCacheCollectionRef collection;
            if (cache.Find(env.Collection->Label(), collection) == NSharedCache::ESharedCacheResultStatus::Hit) {
                if (const auto* state = collection->GetActorState()) {
                    UNIT_ASSERT(!state->Owners.contains(env.WriterActor));
                    UNIT_ASSERT_VALUES_EQUAL(state->PendingRequests, 0);
                }
            }
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        env.SetLimit(env.OriginalLimit);
        env.SendReadiness(oldPad);
        env.Barrier();
        UNIT_ASSERT(env.ReadAll(101).empty());
        env.SendAsync(new TEvStartDirectWrite(101, MakeRows(1), ModeCommit));
        env.CheckCompleted();
    }

    Y_UNIT_TEST(ReviewR13ReleasesMetadataBetweenOutputParts) {
        TDirectWriteEnv env(8_MB, 0);
        env.FireWriteTablet();
        env.SendSync(new NFake::TEvExecute{ [&](auto*, const auto& ctx) {
            NTest::TLayoutCook layout;
            layout.Col(0, KeyColumnId, NScheme::TInt64::TypeId)
                .Col(0, ValueColumnId, NScheme::TString::TypeId)
                .Key({ KeyColumnId });
            auto scheme = layout.RowScheme();
            NWriter::TConf writerConf;
            writerConf.WriteBTreeIndexV1 = false;
            writerConf.WriteBTreeIndexV2 = false;
            writerConf.Slots = { { 1, 0 } };
            writerConf.Groups[0].Channel = 1;
            writerConf.OuterChannel = writerConf.ExtraChannel = 1;
            NPage::TConf pageConf;
            pageConf.WriteBTreeIndexV1 = false;
            pageConf.WriteBTreeIndexV2 = false;
            TVector<NWriter::TBundle::TResult> written;
            ui64 metadataBytes = 0, totalBytes = 0;
            for (ui32 index = 0; index < 2; ++index) {
                NWriter::TBundle bundle(TLogoBlobID(env.Tablet, 1, 100 + index, 0, 0, 0), writerConf);
                auto writer = MakeIntrusive<TPartWriter>(
                    new TPartScheme(scheme->Cols), scheme->Tags(), bundle, pageConf, TEpoch::Zero() + 1);
                i64 key = index + 1;
                TCell keyCell = TCell::Make(key);
                TRowState row(2);
                row.Touch(ERowOp::Upsert);
                row.Set(0, ECellOp::Set, keyCell);
                row.Set(1, ECellOp::Set, TCell("value", 5));
                writer->BeginKey({ &keyCell, 1 });
                writer->AddKeyVersion(row, TRowVersion::Min());
                writer->EndKey();
                writer->Finish();
                auto result = bundle.Results();
                UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
                ui64 bytes = 0;
                for (auto& pages : result.front().PageCollections) {
                    pages.RegularPages.clear(); // This regression only warms mandatory metadata.
                    for (const auto& page : pages.StickyPages) {
                        bytes += page.Data.size() + TSharedData::OverheadSize;
                    }
                }
                metadataBytes = Max(metadataBytes, bytes);
                totalBytes += bytes;
                written.push_back(std::move(result.front()));
            }
            UNIT_ASSERT(metadataBytes && metadataBytes < totalBytes);
            auto& cache = NSharedCache::TSharedCache::Get();
            const ui64 previousLimit = cache.CurrentLimit();
            UNIT_ASSERT(cache.UpdateCurrentLimit(metadataBytes));
            Y_DEFER {
                UNIT_ASSERT(cache.UpdateCurrentLimit(previousLimit));
            };
            TWrittenPartLoader loader(std::move(written), ctx);
            UNIT_ASSERT(loader.IsComplete());
            UNIT_ASSERT_VALUES_EQUAL(cache.CurrentLimit(), metadataBytes);
            auto results = loader.ExtractResults();
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
            for (size_t index = 0; index < results.size(); ++index) {
                const auto& part = results[index].Part;
                UNIT_ASSERT(part.Part);
                UNIT_ASSERT_VALUES_EQUAL(part.Slices->size(), 1);
                const auto& slice = part.Slices->front();
                UNIT_ASSERT_VALUES_EQUAL(slice.EndRowId() - slice.BeginRowId(), 1);
                UNIT_ASSERT_VALUES_EQUAL(slice.FirstKey.GetCells().front().AsValue<i64>(), index + 1);
            }
            auto binding = cache.BindCurrentThreadHazard();
            for (ui32 attempt = 0; attempt < 128 && cache.PageUsage(); ++attempt) {
                cache.EnforceCurrentLimit();
                cache.RunMaintenance();
                cache.ReclaimCold();
            }
            UNIT_ASSERT_VALUES_EQUAL(cache.PageUsage(), 0);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
    }

    Y_UNIT_TEST(ReviewR13WaitsForMetadataCapacity) {
        for (bool bTreeIndex : { false, true }) {
            TDirectWriteEnv env(8_MB, 0);
            auto& appData = env->GetAppData();
            appData.FeatureFlags.SetEnableLocalDBBtreeIndex(bTreeIndex);
            appData.FeatureFlags.SetEnableLocalDBBtreeIndexV2(bTreeIndex);
            appData.FeatureFlags.SetEnableLocalDBFlatIndex(true);
            env.FireWriteTablet();
            env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

            const ui64 currentLimit = appData.SharedCache->CurrentLimit();
            {
                TWaitForFirstEvent<NMemory::TEvConsumerLimit> wait(*env);
                env->Send(NSharedCache::MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(0, 0, 1));
                wait.Wait();
            }
            bool metadataRequested = false, completed = false;
            auto requests = env->AddObserver<NSharedCache::TEvRequest>([&](const auto& ev) {
                metadataRequested |= bool(ev->Get()->WaitPad);
            });
            auto completion = env->AddObserver<TEvDirectWriteDone>([&](const auto&) {
                completed = true;
            });
            TWaitForFirstEvent<NSharedCache::TEvRequest> waiting(*env);
            const auto rows = MakeRows(100);
            env.SendAsync(new TEvStartDirectWrite(101, rows, ModeCommit));
            waiting.Wait();
            UNIT_ASSERT(metadataRequested);
            UNIT_ASSERT(!completed);
            UNIT_ASSERT_VALUES_EQUAL(appData.SharedCache->CurrentLimit(), 1);
            {
                TWaitForFirstEvent<NMemory::TEvConsumerLimit> wait(*env);
                env->Send(NSharedCache::MakeSharedPageCacheId(), TActorId{},
                    new NMemory::TEvConsumerLimit(0, 0, currentLimit));
                wait.Wait();
            }
            auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
            UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);
            UNIT_ASSERT(env.ReadAll(101) == rows);
            env.RestartWriteTablet();
            UNIT_ASSERT(env.ReadAll(101) == rows);
        }
    }

    Y_UNIT_TEST(HappyPath) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        auto rows = MakeRows(500);
        auto* start = new TEvStartDirectWrite(101, rows, ModeCommit);
        env.SendAsync(start);
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read[i].first, rows[i].first);
            UNIT_ASSERT_VALUES_EQUAL(read[i].second, rows[i].second);
        }
    }

    Y_UNIT_TEST(UnsortedInputFails) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        TVector<std::pair<i64, TString>> rows{ {5, "a"}, {3, "b"} };
        env.SendAsync(new TEvStartDirectWrite(101, rows, ModeCommit));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT(!done->Get()->Ok);
        UNIT_ASSERT(!done->Get()->Error.empty());

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), 0u);
    }

    Y_UNIT_TEST(AbortDoesNotCommit) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        auto rows = MakeRows(300);
        env.SendAsync(new TEvStartDirectWrite(101, rows, ModeAbort));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), 0u);
    }

    Y_UNIT_TEST(SurvivesRestart) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        auto rows = MakeRows(400);
        env.SendAsync(new TEvStartDirectWrite(101, rows, ModeCommit));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        env.RestartWriteTablet();

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read[i].first, rows[i].first);
            UNIT_ASSERT_VALUES_EQUAL(read[i].second, rows[i].second);
        }
    }

    Y_UNIT_TEST(CrossTableAtomicity) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101, 102 }) });

        auto rows = MakeRows(200);
        auto* start = new TEvStartDirectWrite(101, rows, ModeCommit);
        start->HasCross = true;
        start->CrossTableId = 102;
        start->CrossKey = 777;
        start->CrossValue = "marker";
        env.SendAsync(start);
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        env.RestartWriteTablet();

        auto target = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(target.size(), rows.size());

        auto cross = env.ReadAll(102);
        UNIT_ASSERT_VALUES_EQUAL(cross.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cross[0].first, 777);
        UNIT_ASSERT_VALUES_EQUAL(cross[0].second, "marker");
    }

    Y_UNIT_TEST(BlobPutFailureFailsWrite) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        // Enough rows to produce at least one data blob whose put we fail.
        auto rows = MakeRows(500);
        auto* start = new TEvStartDirectWrite(101, rows, ModeCommit);
        start->FailPuts = true;
        env.SendAsync(start);

        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT(!done->Get()->Ok);
        UNIT_ASSERT(!done->Get()->Error.empty());

        // Nothing was attached and the tablet stays healthy across a restart.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadAll(101).size(), 0u);
        env.RestartWriteTablet();
        UNIT_ASSERT_VALUES_EQUAL(env.ReadAll(101).size(), 0u);
    }

    Y_UNIT_TEST(EmptyInputCommits) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        // Finalize with zero rows: the commit advances the epoch but attaches no part.
        env.SendAsync(new TEvStartDirectWrite(101, { }, ModeCommit));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadAll(101).size(), 0u);

        // The table is still usable afterwards.
        env.Upsert(101, { {1, "one"}, {2, "two"} });
        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(read[0].second, "one");
        UNIT_ASSERT_VALUES_EQUAL(read[1].second, "two");
    }

    Y_UNIT_TEST(BackpressureThrottlesFeeding) {
        TDirectWriteEnv env(32_MB, 32_MB);
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        // 200 x 128 KiB = 25 MiB > MaxFlight (20 MiB): feeding must stall at least once.
        const size_t valueSize = 128 * 1024;
        auto rows = MakeBigRows(200, valueSize);
        auto* start = new TEvStartDirectWrite(101, rows, ModeCommit);
        start->Throttle = true;
        env.SendAsync(start);

        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);
        UNIT_ASSERT_C(done->Get()->SawBackpressure,
            "expected CanFeed() to refuse rows at least once");

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read[i].first, rows[i].first);
            UNIT_ASSERT_VALUES_EQUAL(read[i].second, rows[i].second);
        }
    }

    Y_UNIT_TEST(RegularWritesLayerOnTop) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        // Bottom layer: direct-write keys [0, 100) as "value_<k>".
        auto rows = MakeRows(100);
        env.SendAsync(new TEvStartDirectWrite(101, rows, ModeCommit));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        // Regular writes on top: overwrite even keys, add new keys [100, 110).
        TVector<std::pair<i64, TString>> top;
        for (i64 i = 0; i < 100; i += 2) {
            top.emplace_back(i, TStringBuilder() << "top_" << i);
        }
        for (i64 i = 100; i < 110; ++i) {
            top.emplace_back(i, TStringBuilder() << "top_" << i);
        }
        env.Upsert(101, top);
        // ... and erase one key that only exists in the bottom part.
        env.Erase(101, { 1 });

        // Expected view after layering the memtable over the bottom part.
        TMap<i64, TString> expected;
        for (i64 i = 0; i < 100; ++i) {
            expected[i] = TStringBuilder() << "value_" << i;
        }
        for (const auto& [k, v] : top) {
            expected[k] = v;
        }
        expected.erase(1);

        auto verify = [&](const TString& stage) {
            auto read = env.ReadAll(101);
            UNIT_ASSERT_VALUES_EQUAL_C(read.size(), expected.size(), stage);
            for (const auto& [k, v] : read) {
                auto it = expected.find(k);
                UNIT_ASSERT_C(it != expected.end(), stage << ": unexpected key " << k);
                UNIT_ASSERT_VALUES_EQUAL_C(v, it->second, stage << ": key " << k);
            }
        };

        verify("before restart");
        env.RestartWriteTablet();
        verify("after restart");
    }
}

} // namespace NKikimr::NTable
