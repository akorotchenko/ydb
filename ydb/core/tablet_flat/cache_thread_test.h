#pragma once

#include "cache.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>

#include <util/generic/scope.h>

#include <memory>
#include <thread>
#include <vector>

namespace NKikimr::NSharedCache {

class TCacheTestThreadContext {
public:
    TCacheTestThreadContext(NActors::TActorSystem& system, TSpaceHazard& hazard)
        : Thread_(0, &system, nullptr, "cache-test")
        , Context_(Mailbox_, Thread_, GetCycleCountFast(), NActors::TActorId{})
    {
        Y_ABORT_UNLESS(!NActors::TlsActivationContext && !TlsCacheHazards);
        NActors::TlsActivationContext = &Context_;
        TlsCacheHazards = { &hazard, 1 };
    }

    ~TCacheTestThreadContext() {
        Y_DEBUG_ABORT_UNLESS(NActors::TlsActivationContext == &Context_);
        Y_DEBUG_ABORT_UNLESS(
            TlsCacheHazards && !SpaceHazardIsActive(TlsCacheHazards[0].State.load(std::memory_order_relaxed)));
        NActors::TlsActivationContext = nullptr;
        TlsCacheHazards = {};
    }

private:
    NActors::TMailbox Mailbox_;
    NActors::TExecutorThread Thread_;
    NActors::TActorContext Context_;
};

class TCacheThreadTestAccess {
    class TApplicationThreadSubSystem final : public NActors::ISubSystem {
    public:
        NActors::TSubSystemDependencies GetDependencies() const override {
            return NActors::DependsOn<TCacheThreadSubSystem>();
        }

        void OnAfterStart(NActors::TActorSystem& system) override {
            auto& threads = *system.GetSubSystem<TCacheThreadSubSystem>();
            Context_ = MakeHolder<TCacheTestThreadContext>(system, threads.Hazard());
        }

        void OnBeforeStop(NActors::TActorSystem&) override {
            Context_.Reset();
        }

    private:
        THolder<TCacheTestThreadContext> Context_;
    };

    class TSimulatedThreadHazards {
    public:
        explicit TSimulatedThreadHazards(ui32 cacheCount)
            : CacheCount_(cacheCount)
        {
            Y_ABORT_UNLESS(!TlsCacheHazards);
            Hazards.reserve(NActors::MaxWorkers);
            for (ui32 thread = 0; thread < NActors::MaxWorkers; ++thread) {
                Hazards.push_back(std::make_shared<TSpaceHazard[]>(cacheCount));
            }
            TlsCacheHazards = { Hazards.front().get(), cacheCount };
        }

        ~TSimulatedThreadHazards() {
            Y_ABORT_UNLESS(Thread_ == std::this_thread::get_id());
            Y_DEBUG_ABORT_UNLESS(TlsCacheHazards.data() == Hazards.front().get());
            for (ui32 index = 0; index < CacheCount_; ++index) {
                Y_DEBUG_ABORT_UNLESS(
                    !SpaceHazardIsActive(TlsCacheHazards[index].State.load(std::memory_order_relaxed)));
            }
            TlsCacheHazards = {};
        }

        std::vector<std::shared_ptr<TSpaceHazard[]>> Hazards;

    private:
        const ui32 CacheCount_;
        const std::thread::id Thread_ = std::this_thread::get_id();
    };

public:
    static auto MakeSpace(const TCacheCapacityConfig& current, const TCacheCapacityConfig& reserved, ui32 count) {
        auto threads = MakeHolder<TCacheThreadSubSystem>(Max<ui32>(count, 1));
        auto space = TCacheSpace::Create(current, reserved, *threads);
        return std::pair(std::move(threads), std::move(space));
    }

    static TSpaceHazard& Hazard(const TCacheSpace& space, ui32 index = 0) {
        return *space.Hazards_.at(index);
    }

    template <class TTraits>
    static TSpaceHazard& Hazard(const TCacheImpl<TTraits>& cache, ui32 index = 0) {
        return Hazard(*cache.Space_, index);
    }

    template <class TRuntime>
    static void Setup(TRuntime& runtime);
};

// Inline implementations

template <class TRuntime>
inline void TCacheThreadTestAccess::Setup(TRuntime& runtime) {
    auto previous = std::move(runtime.SetupNodeSubSystems);
    const ui32 count = runtime.IsRealThreads() ? 1 : NActors::MaxWorkers;
    const ui32 cacheCount = runtime.IsRealThreads() ? 1 : runtime.GetNodeCount();
    const bool applicationThread = !runtime.IsRealThreads() && runtime.GetNodeCount() == 1;
    runtime.SetupNodeSubSystems = [count, cacheCount, applicationThread, previous = std::move(previous),
                                      hazards = std::shared_ptr<TSimulatedThreadHazards>{}](
                                      ui32 node, NActors::TActorSystemSetup* setup) mutable {
        if (previous) {
            previous(node, setup);
        }
        if (auto* registered = NActors::GetSubSystem<TCacheThreadSubSystem>(setup->SubSystems)) {
            Y_ABORT_UNLESS(cacheCount == 1 || (registered->HazardIndex_ == node && !registered->Hazards_.empty() &&
                                                  registered->Hazards_.front().get() == TlsCacheHazards.data() &&
                                                  TlsCacheHazards.size() == cacheCount),
                "Simulated nodes must share the cache test hazard arrays");
        } else {
            auto threads = std::make_unique<TCacheThreadSubSystem>(cacheCount > 1 ? 0 : count);
            if (cacheCount > 1) {
                // The setup callback retains the arrays until every simulated node has been cleaned up
                if (!hazards) {
                    hazards = std::make_shared<TSimulatedThreadHazards>(cacheCount);
                }
                threads->Hazards_ = hazards->Hazards;
                threads->HazardIndex_ = node;
            }
            setup->RegisterSubSystem(std::move(threads));
        }
        if (applicationThread && !NActors::GetSubSystem<TApplicationThreadSubSystem>(setup->SubSystems)) {
            setup->RegisterSubSystem(std::make_unique<TApplicationThreadSubSystem>());
        }
    };
}

} // namespace NKikimr::NSharedCache
