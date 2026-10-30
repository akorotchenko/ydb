#include <ydb/core/tablet_flat/flat_executor_bootlogic.h>
#include <ydb/core/tablet_flat/flat_boot_env.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTabletFlatExecutor {

struct TBootMetadataState {
    ui32 Starts = 0;
    ui32 Submitted = 0;
    ui32 Destroyed = 0;
    TVector<ui32> Slots;
    TExecutorBootLogic::EOpResult Last = TExecutorBootLogic::OpResultContinue;
};

class TBootMetadataTestAccess {
public:
    static void Prepare(TExecutorBootLogic& logic) {
        logic.PrepareEnv(true, 1, {});
    }

    static NBoot::TSpawned Load(TExecutorBootLogic& logic, NBoot::IStep* step, ui32 slot,
        TIntrusivePtr<NSharedCache::TCollectionRequest> request) {
        return logic.LoadCollection(step, slot, std::move(request));
    }

    static void Queue(TExecutorBootLogic& logic, TBootMetadataState& state,
        TVector<TIntrusivePtr<NSharedCache::TCollectionRequest>> requests, bool fail);

    static void Execute(TExecutorBootLogic& logic) {
        logic.Steps->Execute();
    }
};

namespace {

    using namespace NActors;
    using NSharedCache::TCollectionRequest;
    using NSharedCache::TEvCollectionRequest;
    using NSharedCache::TEvCollectionRequestUpdate;
    using NSharedCache::TEvCollectionResult;

    class TMetadataStep final : public NBoot::IStep {
    public:
        TMetadataStep(
            IStep* owner, TBootMetadataState& state, TVector<TIntrusivePtr<TCollectionRequest>> requests, bool fail)
            : IStep(owner, NBoot::EStep::None)
            , State(state)
            , Requests(std::move(requests))
            , Fail(fail)
        {}

        ~TMetadataStep() override {
            ++State.Destroyed;
        }

        void Start() override {
            ++State.Starts;
            for (ui32 slot = 0; slot < Requests.size(); ++slot) {
                if (TBootMetadataTestAccess::Load(*Logic, this, slot, Requests[slot])) {
                    ++State.Submitted;
                }
            }
        }

        bool HandleCollection(ui32 slot, TEvCollectionResult& result) override {
            UNIT_ASSERT(Requests.at(slot) == result.Request);
            State.Slots.push_back(slot);
            return !Fail || result.Status == NKikimrProto::OK;
        }

    private:
        TBootMetadataState& State;
        TVector<TIntrusivePtr<TCollectionRequest>> Requests;
        const bool Fail;
    };

} // anonymous namespace

void TBootMetadataTestAccess::Queue(TExecutorBootLogic& logic, TBootMetadataState& state,
    TVector<TIntrusivePtr<NSharedCache::TCollectionRequest>> requests, bool fail) {
    logic.Steps->Spawn<TMetadataStep>(state, std::move(requests), fail);
}

namespace {

    class TBootMetadataActor final : public TActorBootstrapped<TBootMetadataActor> {
    public:
        TBootMetadataActor(TBootMetadataState& first, TBootMetadataState& second,
            TVector<TIntrusivePtr<TCollectionRequest>> initial, TVector<TIntrusivePtr<TCollectionRequest>> restart,
            bool queued, bool fail)
            : First(first)
            , Second(second)
            , Initial(std::move(initial))
            , Restart(std::move(restart))
            , Queued(queued)
            , Fail(fail)
            , Info(CreateTestTabletInfo(1, TTabletTypes::DataShard))
        {}

        void Bootstrap() {
            Become(&TThis::StateWork);
            Start(First, Initial);
            if (Queued) {
                Logic.Reset();
            }
        }

    private:
        void Start(TBootMetadataState& state, const TVector<TIntrusivePtr<TCollectionRequest>>& requests) {
            Current = &state;
            Logic = MakeHolder<TExecutorBootLogic>(this, SelfId(), Attempt, Info.Get(), 1_MB);
            TBootMetadataTestAccess::Prepare(*Logic);
            TBootMetadataTestAccess::Queue(*Logic, state, requests, Fail);
            if (!Queued) {
                TBootMetadataTestAccess::Execute(*Logic);
            }
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvCollectionResult, Handle);
                hFunc(TEvents::TEvWakeup, Handle);
                cFunc(TEvents::TSystem::PoisonPill, Stop);
            }
        }

        void Handle(TEvCollectionResult::TPtr& ev) {
            if (Logic) {
                Current->Last = Logic->Receive(*ev);
            }
        }

        void Handle(TEvents::TEvWakeup::TPtr&) {
            Logic->Cancel();
            Logic.Reset();
            ++Attempt;
            Start(Second, Restart);
        }

        void Stop() {
            Logic.Reset();
            PassAway();
        }

        TBootMetadataState& First;
        TBootMetadataState& Second;
        TBootMetadataState* Current = nullptr;
        TVector<TIntrusivePtr<TCollectionRequest>> Initial;
        TVector<TIntrusivePtr<TCollectionRequest>> Restart;
        const bool Queued;
        const bool Fail;
        ui64 Attempt = 1;
        TIntrusivePtr<TTabletStorageInfo> Info;
        THolder<TExecutorBootLogic> Logic;
    };

    TIntrusivePtr<TCollectionRequest> Request(ui32 step) {
        return MakeIntrusive<TCollectionRequest>(NPageCollection::TLargeGlobId{ 1, TLogoBlobID(1, 1, step, 0, 64, 1),
                                                     64 }, NSharedCache::ECollectionLoadKind::Regular);
    }

    struct TFixture {
        TFixture(TVector<TIntrusivePtr<TCollectionRequest>> initial,
            TVector<TIntrusivePtr<TCollectionRequest>> restart = {}, bool queued = false, bool fail = false) {
            TAutoPtr<TAppPrepare> app = new TAppPrepare;
            Runtime.Initialize(app->Unwrap());
            Cache = Runtime.AllocateEdgeActor();
            Runtime.RegisterService(NSharedCache::MakeSharedPageCacheId(), Cache);
            Actor = Runtime.Register(
                new TBootMetadataActor(First, Second, std::move(initial), std::move(restart), queued, fail));
        }

        ~TFixture() {
            Runtime.Send(new IEventHandle(Actor, Cache, new TEvents::TEvPoison), 0, true);
            Drain();
        }

        TEvCollectionRequest::TPtr Read() {
            auto request = Runtime.GrabEdgeEvent<TEvCollectionRequest>(Cache, TDuration::Seconds(5));
            UNIT_ASSERT(request->Sender == Actor);
            return request;
        }

        void Reply(const TIntrusivePtr<TCollectionRequest>& request, ui64 attempt,
            NKikimrProto::EReplyStatus status = NKikimrProto::RACE) {
            // The simulated terminal reply leaves the request inactive, as the real cache does
            request->Cancel();
            Runtime.Send(new IEventHandle(Actor, Cache, new TEvCollectionResult(request, status), 0, attempt), 0, true);
            Drain();
        }

        void Drain() {
            Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }

        TBootMetadataState First;
        TBootMetadataState Second;
        TTestBasicRuntime Runtime;
        TActorId Cache;
        TActorId Actor;
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(TBootCollectionMetadata) {
    Y_UNIT_TEST(RoutesByRequestIdentityAndSlot) {
        auto a = Request(1);
        auto b = Request(2);
        TFixture fixture({ a, b });
        auto first = fixture.Read();
        auto second = fixture.Read();
        UNIT_ASSERT_VALUES_EQUAL(first->Cookie, 1);
        UNIT_ASSERT_VALUES_EQUAL(second->Cookie, 1);
        fixture.Reply(b, 1);
        fixture.Reply(b, 1);
        fixture.Reply(a, 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.First.Slots.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.First.Slots[0], 1);
        fixture.Reply(a, 1);
        UNIT_ASSERT(fixture.First.Slots == TVector<ui32>({ 1, 0 }));
        UNIT_ASSERT_VALUES_EQUAL(fixture.First.Destroyed, 1);
    }

    Y_UNIT_TEST(RestartRejectsOldAttemptAndOldRequest) {
        auto old = Request(1);
        auto fresh = Request(1);
        TFixture fixture({ old }, { fresh });
        fixture.Read();
        fixture.Runtime.Send(new IEventHandle(fixture.Actor, fixture.Cache, new TEvents::TEvWakeup), 0, true);
        auto update = fixture.Runtime.GrabEdgeEvent<TEvCollectionRequestUpdate>(fixture.Cache, TDuration::Seconds(5));
        UNIT_ASSERT(update->Get()->Request == old);
        UNIT_ASSERT(!old->IsActive());
        auto request = fixture.Read();
        UNIT_ASSERT(request->Get()->Request == fresh);
        UNIT_ASSERT_VALUES_EQUAL(request->Cookie, 2);
        fixture.Reply(old, 1);
        fixture.Reply(old, 2);
        UNIT_ASSERT(fixture.Second.Slots.empty());
        fixture.Reply(fresh, 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Second.Slots.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.First.Destroyed, 1);
    }

    Y_UNIT_TEST(DestructorFlushCannotLaunchNewMetadata) {
        auto request = Request(1);
        TFixture fixture({ request }, {}, true);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.First.Starts, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.First.Submitted, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.First.Destroyed, 1);
        UNIT_ASSERT(!request->IsActive());
    }

    Y_UNIT_TEST(FailureCancelsTheRemainingRequests) {
        auto failed = Request(1);
        auto pending = Request(2);
        TFixture fixture({ failed, pending }, {}, false, true);
        fixture.Read();
        fixture.Read();
        fixture.Reply(failed, 1, NKikimrProto::NODATA);
        UNIT_ASSERT(fixture.First.Last == TExecutorBootLogic::OpResultBroken);
        auto update = fixture.Runtime.GrabEdgeEvent<TEvCollectionRequestUpdate>(fixture.Cache, TDuration::Seconds(5));
        UNIT_ASSERT(update->Get()->Request == pending);
        UNIT_ASSERT(!pending->IsActive());
        fixture.Reply(pending, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.First.Slots.size(), 1);
        UNIT_ASSERT(fixture.First.Last == TExecutorBootLogic::OpResultUnhandled);
    }
}

} // namespace NKikimr::NTabletFlatExecutor
