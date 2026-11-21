#include <ydb/core/tablet_flat/cache_thread.h>
#include <ydb/core/tablet_flat/cache_space.h>
#include <ydb/core/tablet_flat/cache.h>
#include <ydb/core/tablet_flat/cache_thread_test.h>
#include <ydb/library/actors/core/actor_coroutine.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/thread_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/system/event.h>

#include <algorithm>
#include <array>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>

namespace NKikimr::NSharedCache {

Y_UNIT_TEST_SUITE(TCacheThreadTest) {
    struct TSample {
        TSpaceHazard* Hazard;
        ui32 Worker;
        ui32 Pool;
        bool HasThreadContext;
        ui32 HazardArraySize;
    };

    struct TProbeState {
        std::mutex Mutex;
        TVector<TSample> Started;
        TVector<TSample> Stopped;
    };

    class TProbeSubSystem final : public NActors::ISubSystem {
    public:
        explicit TProbeSubSystem(TProbeState& state)
            : State_(state)
        {}

        NActors::TSubSystemDependencies GetDependencies() const override {
            return NActors::DependsOn<TCacheThreadSubSystem>();
        }

        void OnExecutorThreadStart(NActors::TThreadContext* context) override {
            std::lock_guard lock(State_.Mutex);
            State_.Started.push_back(Sample(context));
        }

        void OnExecutorThreadStop(NActors::TThreadContext* context) override {
            std::lock_guard lock(State_.Mutex);
            State_.Stopped.push_back(Sample(context));
        }

    private:
        static TSample Sample(NActors::TThreadContext* context) {
            return { TlsCacheHazards.data(), context->WorkerId(), context->PoolId(),
                NActors::TlsThreadContext == context, static_cast<ui32>(TlsCacheHazards.size()) };
        }

        TProbeState& State_;
    };

    THolder<NActors::TActorSystemSetup> MakeWorkerSetup() {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 3;
        setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[setup->ExecutorsCount]);
        setup->Executors[0] = new NActors::TBasicExecutorPool(0, 1, 10, "first");
        setup->Executors[1] = new NActors::TBasicExecutorPool(1, 1, 10, "second");
        setup->Executors[2] = new NActors::TIOExecutorPool(2, 1, "io", nullptr);
        setup->Scheduler = new NActors::TBasicSchedulerThread;

        return setup;
    }

    Y_UNIT_TEST(HazardsAreUniqueAcrossPoolsWithEqualWorkerIds) {
        auto setup = MakeWorkerSetup();
        auto indices = std::make_unique<TCacheThreadSubSystem>();
        auto* domain = indices.get();
        setup->RegisterSubSystem(std::move(indices));
        TProbeState state;
        setup->RegisterSubSystem(std::make_unique<TProbeSubSystem>(state));
        NActors::TActorSystem actorSystem(setup);
        actorSystem.Start();
        actorSystem.Stop();
        actorSystem.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(domain->ThreadCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state.Started.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state.Stopped.size(), 3);
        for (const auto* samples : { &state.Started, &state.Stopped }) {
            TVector<TSpaceHazard*> hazards;
            TVector<ui32> pools;
            for (const auto& sample : *samples) {
                UNIT_ASSERT_VALUES_EQUAL(sample.Worker, 0);
                UNIT_ASSERT(sample.Hazard);
                UNIT_ASSERT(sample.HasThreadContext);
                UNIT_ASSERT_VALUES_EQUAL(sample.HazardArraySize, 1);
                hazards.push_back(sample.Hazard);
                pools.push_back(sample.Pool);
            }
            std::sort(hazards.begin(), hazards.end());
            std::sort(pools.begin(), pools.end());
            UNIT_ASSERT(std::adjacent_find(hazards.begin(), hazards.end()) == hazards.end());
            UNIT_ASSERT_VALUES_EQUAL(pools, (TVector<ui32>{ 0, 1, 2 }));
        }
    }

    Y_UNIT_TEST(ThreadStopClearsPointerAndPreservesRecord) {
        UNIT_ASSERT(!TlsCacheHazards);
        Y_DEFER {
            TlsCacheHazards = {};
        };
        TCacheThreadSubSystem indices;
        NActors::TThreadContext context(0, nullptr, nullptr);
        indices.OnExecutorThreadPrepare(&context);
        auto* previous = std::exchange(NActors::TlsThreadContext, &context);
        Y_DEFER {
            NActors::TlsThreadContext = previous;
        };

        UNIT_ASSERT(!TlsCacheHazards);
        for (ui32 pass = 0; pass < 2; ++pass) {
            indices.OnExecutorThreadStart(&context);
            UNIT_ASSERT(TlsCacheHazards);
            auto* hazard = TlsCacheHazards.data();
            hazard->SpareItem.store(17, std::memory_order_relaxed);
            indices.OnExecutorThreadStop(&context);
            UNIT_ASSERT_VALUES_EQUAL(hazard->SpareItem.load(std::memory_order_relaxed), 17);
            UNIT_ASSERT(!TlsCacheHazards);
        }
    }

    Y_UNIT_TEST(SimulatedContextsDoNotInstallWorkerRegistration) {
        UNIT_ASSERT(!TlsCacheHazards);
        Y_DEFER {
            TlsCacheHazards = {};
        };
        TCacheThreadSubSystem indices;
        NActors::TThreadContext first(0, nullptr, nullptr);
        NActors::TThreadContext second(0, nullptr, nullptr);
        indices.OnExecutorThreadPrepare(&first);
        indices.OnExecutorThreadPrepare(&second);
        auto* previous = NActors::TlsThreadContext;
        Y_DEFER {
            NActors::TlsThreadContext = previous;
        };

        for (auto* context : { &first, &second }) {
            NActors::TlsThreadContext = context;
            UNIT_ASSERT(!TlsCacheHazards);
        }
    }

    Y_UNIT_TEST(IndependentSystemsUseDifferentRecords) {
        UNIT_ASSERT(!TlsCacheHazards);
        Y_DEFER {
            TlsCacheHazards = {};
        };
        TCacheThreadSubSystem first;
        TCacheThreadSubSystem second;
        NActors::TThreadContext context(0, nullptr, nullptr);
        first.OnExecutorThreadPrepare(&context);
        second.OnExecutorThreadPrepare(&context);

        first.OnExecutorThreadStart(&context);
        auto* firstHazard = TlsCacheHazards.data();
        UNIT_ASSERT(firstHazard);
        first.OnExecutorThreadStop(&context);

        second.OnExecutorThreadStart(&context);
        UNIT_ASSERT(TlsCacheHazards);
        UNIT_ASSERT(TlsCacheHazards.data() != firstHazard);
        second.OnExecutorThreadStop(&context);
        UNIT_ASSERT(!TlsCacheHazards);
    }

    class TSimulatedProbe final : public NActors::TActor<TSimulatedProbe> {
    public:
        explicit TSimulatedProbe(std::function<void()> probe)
            : TActor(&TThis::StateWork)
            , Probe_(std::move(probe))
        {}

        STFUNC(StateWork) {
            Y_UNUSED(ev);
            Probe_();
        }

    private:
        std::function<void()> Probe_;
    };

    struct TSimulatedCaches {
        static constexpr ui32 CacheCount = 2;

        TSimulatedCaches()
            : Runtime(CacheCount, false)
        {
            TCacheThreadTestAccess::Setup(Runtime);
            TCacheThreadTestAccess::Setup(Runtime);
            Runtime.Initialize();
            TCacheCapacityConfig capacity;
            auto& first = *Runtime.GetActorSystem(0)->GetSubSystem<TCacheThreadSubSystem>();
            UNIT_ASSERT(TryCalculateCacheFootprint(5, 4096, 0, first.ThreadCount(), capacity));
            for (ui32 node = 0; node < CacheCount; ++node) {
                auto& threads = *Runtime.GetActorSystem(node)->GetSubSystem<TCacheThreadSubSystem>();
                Spaces[node] = TCacheSpace::Create(capacity, capacity, threads);
                UNIT_ASSERT(&TlsCacheHazards[node] == &threads.Hazard());
            }
        }

        NActors::TTestActorRuntimeBase Runtime;
        std::array<THolder<TCacheSpace>, CacheCount> Spaces;
    };

    Y_UNIT_TEST(SimulatedNodesKeepDistinctHazardsWithoutTlsRebinding) {
        UNIT_ASSERT(!NActors::TlsActivationContext && !TlsCacheHazards);
        {
            TSimulatedCaches fixture;
            auto& runtime = fixture.Runtime;
            auto* array = TlsCacheHazards.data();
            UNIT_ASSERT_VALUES_EQUAL(TlsCacheHazards.size(), TSimulatedCaches::CacheCount);
            ui32 visits = 0;
            const auto inner = runtime.Register(new TSimulatedProbe([&] {
                UNIT_ASSERT(TlsCacheHazards.data() == array);
                auto operation = fixture.Spaces[1]->BeginOperation();
                UNIT_ASSERT(&operation.Hazard() == &array[1]);
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(array[0].State.load()), 1);
                ++visits;
            }), 1);
            const auto outer = runtime.Register(new TSimulatedProbe([&] {
                auto operation = fixture.Spaces[0]->BeginOperation();
                runtime.Send(new NActors::IEventHandle(inner, {}, new NActors::TEvents::TEvPing), 1);
                UNIT_ASSERT(TlsCacheHazards.data() == array);
                UNIT_ASSERT(&operation.Hazard() == &array[0]);
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(array[1].State.load()), 0);
                ++visits;
            }));
            runtime.Send(new NActors::IEventHandle(outer, {}, new NActors::TEvents::TEvPing));
            UNIT_ASSERT_VALUES_EQUAL(visits, 2);
            UNIT_ASSERT(!NActors::TlsActivationContext && TlsCacheHazards.data() == array);
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(array[0].State.load()), 0);
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(array[1].State.load()), 0);
        }
        UNIT_ASSERT(!NActors::TlsActivationContext && !TlsCacheHazards);
    }

    Y_UNIT_TEST(EachRegisteredThreadHasItsOwnArray) {
        TSimulatedCaches fixture;
        auto* mainArray = TlsCacheHazards.data();
        auto* workerArray = &TCacheThreadTestAccess::Hazard(*fixture.Spaces[0], 1);
        UNIT_ASSERT(workerArray != mainArray);
        TManualEvent started;
        TManualEvent finish;
        bool matched = false;
        std::thread worker([&] {
            Y_ABORT_UNLESS(!TlsCacheHazards);
            TlsCacheHazards = { workerArray, TSimulatedCaches::CacheCount };
            {
                auto first = fixture.Spaces[0]->BeginOperation();
                auto second = fixture.Spaces[1]->BeginOperation();
                matched = &first.Hazard() == &workerArray[0] && &second.Hazard() == &workerArray[1];
                started.Signal();
                finish.WaitI();
            }
            TlsCacheHazards = {};
        });
        Y_DEFER {
            finish.Signal();
            if (worker.joinable()) {
                worker.join();
            }
        };
        UNIT_ASSERT(started.WaitT(TDuration::Seconds(30)));
        {
            auto first = fixture.Spaces[0]->BeginOperation();
            auto second = fixture.Spaces[1]->BeginOperation();
            UNIT_ASSERT(&first.Hazard() == &mainArray[0] && &second.Hazard() == &mainArray[1]);
            for (ui32 cache = 0; cache < TSimulatedCaches::CacheCount; ++cache) {
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(mainArray[cache].State.load()), 1);
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(workerArray[cache].State.load()), 1);
            }
        }
        finish.Signal();
        worker.join();
        UNIT_ASSERT(matched);
        UNIT_ASSERT(TlsCacheHazards.data() == mainArray);
    }

    struct TTestThreadState {
        TManualEvent Started;
        TManualEvent Ready;
        THolder<TCacheSpace> Space;
        std::mutex Mutex;
        ui32 Workers = 0;
        ui32 Visited = 0;
        bool CreatedLate = false;
        bool Matched = true;
    };

    class TTestThreadProbe final : public NActors::ISubSystem {
    public:
        TTestThreadProbe(TTestThreadState& state, const TCacheThreadSubSystem& threads)
            : State_(state)
            , Threads_(threads)
        {}

        NActors::TSubSystemDependencies GetDependencies() const override {
            return NActors::DependsOn<TCacheThreadSubSystem>();
        }

        void OnExecutorThreadStart(NActors::TThreadContext*) override {
            {
                std::lock_guard lock(State_.Mutex);
                if (++State_.Workers == Threads_.ThreadCount()) {
                    State_.Started.Signal();
                }
            }
            State_.Ready.WaitI();
            if (State_.Space) {
                auto outer = State_.Space->BeginOperation();
                auto inner = State_.Space->BeginOperation();
                auto moved = std::move(inner);
                std::lock_guard lock(State_.Mutex);
                State_.Matched &= &outer.Hazard() == TlsCacheHazards.data() &&
                                  &moved.Hazard() == TlsCacheHazards.data() &&
                                  SpaceHazardActiveCount(TlsCacheHazards[0].State.load(std::memory_order_relaxed)) == 2;
                ++State_.Visited;
            }
        }

        void OnAfterStart(NActors::TActorSystem&) override {
            State_.CreatedLate = State_.Started.WaitT(TDuration::Seconds(30));
            TCacheCapacityConfig capacity;
            if (TryCalculateCacheFootprint(5, 4096, 0, Threads_.ThreadCount(), capacity)) {
                State_.Space = TCacheSpace::Create(capacity, capacity, Threads_);
            }
            State_.Ready.Signal();
        }

    private:
        TTestThreadState& State_;
        const TCacheThreadSubSystem& Threads_;
    };

    Y_UNIT_TEST(ThreadGuardsUsePointersAfterLateCacheCreation) {
        auto setup = MakeWorkerSetup();
        auto threads = std::make_unique<TCacheThreadSubSystem>();
        auto* owner = threads.get();
        setup->RegisterSubSystem(std::move(threads));
        TTestThreadState state;
        setup->RegisterSubSystem(std::make_unique<TTestThreadProbe>(state, *owner));
        NActors::TActorSystem actorSystem(setup);
        Y_DEFER {
            state.Ready.Signal();
            actorSystem.Stop();
        };
        actorSystem.Start();
        actorSystem.Stop();
        actorSystem.Cleanup();
        UNIT_ASSERT(state.CreatedLate);
        UNIT_ASSERT(state.Space);
        UNIT_ASSERT(state.Matched);
        UNIT_ASSERT_VALUES_EQUAL(state.Visited, 3);
        UNIT_ASSERT_VALUES_EQUAL(state.Space->HazardCount(), owner->ThreadCount());
    }

    Y_UNIT_TEST(TestResolverFollowsThreadPointer) {
        UNIT_ASSERT(!TlsCacheHazards);
        TCacheThreadSubSystem threadHazards(1);
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateCacheFootprint(5, 4096, 0, 1, capacity));
        auto cache =
            TCacheImpl<TTestTraits>::Create(TCacheSpace::Create(capacity, capacity, threadHazards), capacity.Limit);
        UNIT_ASSERT(cache);
        TlsCacheHazards = { &threadHazards.Hazard(), 1 };
        Y_DEFER {
            TlsCacheHazards = {};
        };
        auto operation = cache->BeginOperation();
        UNIT_ASSERT_VALUES_EQUAL(&operation.Hazard(), TlsCacheHazards.data());
        UNIT_ASSERT_VALUES_EQUAL(&TCacheImpl<TTestTraits>::Get(), cache.Get());
    }

    struct TCoroState {
        THolder<TCacheSpace> Space;
        TManualEvent Waiting;
        TManualEvent Finished;
        ui32 Visits = 0;
        ui32 Restores = 0;
        ui32 Stores = 0;
        ui32 UnexpectedEvents = 0;
        bool RegistrationMatched = true;
        bool RestoredEmpty = true;
        bool UnwoundWithoutRegistration = false;
    };

    class TRegisteredCoro final : public NActors::TActorCoroImpl {
    public:
        explicit TRegisteredCoro(TCoroState& state)
            : TActorCoroImpl(64 * 1024, true)
            , State_(state)
        {}

        void BeforeResume() override {
            ExpectedHazards_ = TlsCacheHazards;
        }

        void Run() override {
#if CORO_THROUGH_THREADS
            RestoreCacheThreadState();
            Y_DEFER {
                StoreCacheThreadState();
            };
#endif
            try {
                Visit();
                State_.Waiting.Signal();
                for (;;) {
                    THolder<NActors::IEventHandle> event;
                    {
#if CORO_THROUGH_THREADS
                        StoreCacheThreadState();
                        Y_DEFER {
                            RestoreCacheThreadState();
                        };
#endif
                        event = WaitForEvent();
                    }
                    Visit();
                    if (event->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType) {
                        break;
                    }
                    ++State_.UnexpectedEvents;
                }
            } catch (const TDtorException&) {
                State_.UnwoundWithoutRegistration = !TlsCacheHazards;
                return;
            }
            State_.Finished.Signal();
        }

    private:
        void Visit() {
            auto operation = State_.Space->BeginOperation();
            State_.RegistrationMatched &= ExpectedHazards_ && ExpectedHazards_.data() == TlsCacheHazards.data() &&
                                          ExpectedHazards_.size() == TlsCacheHazards.size() &&
                                          &operation.Hazard() == TlsCacheHazards.data();
            ++State_.Visits;
        }

#if CORO_THROUGH_THREADS
        void StoreCacheThreadState() {
            TlsCacheHazards = {};
            ++State_.Stores;
            State_.RestoredEmpty &= !TlsCacheHazards;
        }

        void RestoreCacheThreadState() {
            TlsCacheHazards = ExpectedHazards_;
            ++State_.Restores;
        }
#endif
        TCoroState& State_;
        TArrayRef<TSpaceHazard> ExpectedHazards_;
    };

    void CheckCoroutineRegistration(bool finish) {
        UNIT_ASSERT(!TlsCacheHazards);
        Y_DEFER {
            TlsCacheHazards = {};
        };
        auto setup = MakeWorkerSetup();
        auto indices = std::make_unique<TCacheThreadSubSystem>();
        auto* domain = indices.get();
        setup->RegisterSubSystem(std::move(indices));
        TCoroState state;
        NActors::TActorSystem actorSystem(setup);
        Y_DEFER {
            actorSystem.Stop();
            actorSystem.Cleanup();
        };
        actorSystem.Start();
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateCacheFootprint(5, 4096, 0, domain->ThreadCount(), capacity));
        state.Space = TCacheSpace::Create(capacity, capacity, *domain);
        UNIT_ASSERT(state.Space);
        const auto actor = actorSystem.Register(new NActors::TActorCoro(MakeHolder<TRegisteredCoro>(state)));
        const bool waiting = state.Waiting.WaitT(TDuration::Seconds(30));
        if (waiting && finish) {
            actorSystem.Send(actor, new NActors::TEvents::TEvPing);
            actorSystem.Send(actor, new NActors::TEvents::TEvWakeup);
        }
        const bool finished = waiting && finish && state.Finished.WaitT(TDuration::Seconds(30));
        actorSystem.Stop();
        actorSystem.Cleanup();
        UNIT_ASSERT(waiting);
        UNIT_ASSERT_VALUES_EQUAL(finished, finish);
        UNIT_ASSERT_VALUES_EQUAL(state.Visits, finish ? 3 : 1);
        UNIT_ASSERT_VALUES_EQUAL(state.UnexpectedEvents, finish ? 1 : 0);
        UNIT_ASSERT(state.RegistrationMatched);
        UNIT_ASSERT_VALUES_EQUAL(state.UnwoundWithoutRegistration, !finish);
        UNIT_ASSERT(state.RestoredEmpty);
#if CORO_THROUGH_THREADS
        UNIT_ASSERT_VALUES_EQUAL(state.Stores, finish ? 3 : 2);
        UNIT_ASSERT_VALUES_EQUAL(state.Restores, finish ? 3 : 2);
#endif
        UNIT_ASSERT_VALUES_EQUAL(state.Space->HazardCount(), domain->ThreadCount());
    }

    Y_UNIT_TEST(CoroutineUsesRegisteredSlotOnEntryAndResume) {
        CheckCoroutineRegistration(true);
    }

    Y_UNIT_TEST(SuspendedCoroutineUnwindsWithoutWorkerRegistration) {
        CheckCoroutineRegistration(false);
    }
}

} // namespace NKikimr::NSharedCache
