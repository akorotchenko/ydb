#include <ydb/core/tablet_flat/shared_cache_thread.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/thread_context.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <algorithm>
#include <mutex>
#include <utility>

namespace NKikimr::NSharedCache {

Y_UNIT_TEST_SUITE(TSharedCacheThreadTest) {
    struct TSample {
        ui32 Index;
        ui32 Worker;
        ui32 Pool;
        const TCacheThreadSubSystem* Domain;
        bool HasThreadContext;
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
            return { TCacheThreadContext::Index(), context->WorkerId(), context->PoolId(),
                TCacheThreadContext::Domain(), NActors::TlsThreadContext == context };
        }

        TProbeState& State_;
    };

    Y_UNIT_TEST(IndicesAreUniqueAcrossPoolsWithEqualWorkerIds) {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 3;
        setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[setup->ExecutorsCount]);
        setup->Executors[0] = new NActors::TBasicExecutorPool(0, 1, 10, "first");
        setup->Executors[1] = new NActors::TBasicExecutorPool(1, 1, 10, "second");
        setup->Executors[2] = new NActors::TIOExecutorPool(2, 1, "io", nullptr);
        setup->Scheduler = new NActors::TBasicSchedulerThread;

        auto indices = std::make_unique<TCacheThreadSubSystem>();
        auto* domain = indices.get();
        setup->RegisterSubSystem(std::move(indices));
        TProbeState state;
        setup->RegisterSubSystem(std::make_unique<TProbeSubSystem>(state));
        NActors::TActorSystem actorSystem(setup);
        actorSystem.Start();
        actorSystem.Stop();
        actorSystem.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(domain->WorkerCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state.Started.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state.Stopped.size(), 3);
        for (const auto* samples : { &state.Started, &state.Stopped }) {
            TVector<ui32> actualIndices;
            TVector<ui32> pools;
            for (const auto& sample : *samples) {
                UNIT_ASSERT_VALUES_EQUAL(sample.Worker, 0);
                UNIT_ASSERT_VALUES_EQUAL(sample.Domain, domain);
                UNIT_ASSERT(sample.HasThreadContext);
                actualIndices.push_back(sample.Index);
                pools.push_back(sample.Pool);
            }
            std::sort(actualIndices.begin(), actualIndices.end());
            std::sort(pools.begin(), pools.end());
            UNIT_ASSERT_VALUES_EQUAL(actualIndices, (TVector<ui32>{ 0, 1, 2 }));
            UNIT_ASSERT_VALUES_EQUAL(pools, (TVector<ui32>{ 0, 1, 2 }));
        }
    }

    Y_UNIT_TEST(ThreadStopClearsIndexAndDomain) {
        TCacheThreadSubSystem indices;
        NActors::TThreadContext context(0, nullptr, nullptr);
        indices.OnExecutorThreadPrepare(&context);
        auto* previous = std::exchange(NActors::TlsThreadContext, &context);
        Y_DEFER {
            NActors::TlsThreadContext = previous;
        };

        UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Index(), TCacheThreadContext::InvalidIndex);
        UNIT_ASSERT(!TCacheThreadContext::Domain());
        for (ui32 pass = 0; pass < 2; ++pass) {
            indices.OnExecutorThreadStart(&context);
            UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Index(), 0);
            UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Domain(), &indices);
            indices.OnExecutorThreadStop(&context);
            UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Index(), TCacheThreadContext::InvalidIndex);
            UNIT_ASSERT(!TCacheThreadContext::Domain());
        }
    }

    Y_UNIT_TEST(SimulatedContextsDoNotInstallWorkerRegistration) {
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
            UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Index(), TCacheThreadContext::InvalidIndex);
            UNIT_ASSERT(!TCacheThreadContext::Domain());
        }
    }

    Y_UNIT_TEST(IndependentSystemsCanReuseAnIndexWithoutSharingADomain) {
        TCacheThreadSubSystem first;
        TCacheThreadSubSystem second;
        NActors::TThreadContext context(0, nullptr, nullptr);
        first.OnExecutorThreadPrepare(&context);
        second.OnExecutorThreadPrepare(&context);

        first.OnExecutorThreadStart(&context);
        UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Index(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Domain(), &first);
        first.OnExecutorThreadStop(&context);

        second.OnExecutorThreadStart(&context);
        UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Index(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheThreadContext::Domain(), &second);
        second.OnExecutorThreadStop(&context);
        UNIT_ASSERT(!TCacheThreadContext::Domain());
    }
}

} // namespace NKikimr::NSharedCache
