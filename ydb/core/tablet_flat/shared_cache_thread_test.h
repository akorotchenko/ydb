#pragma once

#include "shared_cache.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>

#include <util/generic/scope.h>

namespace NKikimr::NSharedCache {

class TCacheTestThreadContext {
public:
    TCacheTestThreadContext(NActors::TActorSystem& system, TSpaceHazard& hazard)
        : Thread_(0, &system, nullptr, "cache-test")
        , Context_(Mailbox_, Thread_, GetCycleCountFast(), NActors::TActorId{})
    {
        Y_ABORT_UNLESS(!NActors::TlsActivationContext && !TlsCacheHazard);
        NActors::TlsActivationContext = &Context_;
        TlsCacheHazard = &hazard;
    }

    ~TCacheTestThreadContext() {
        Y_DEBUG_ABORT_UNLESS(NActors::TlsActivationContext == &Context_);
        Y_DEBUG_ABORT_UNLESS(TlsCacheHazard && TlsCacheHazard->State.load(std::memory_order_relaxed) == 0);
        NActors::TlsActivationContext = nullptr;
        TlsCacheHazard = nullptr;
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

public:
    static auto MakeSpace(const TSharedCacheCapacity& current, const TSharedCacheCapacity& reserved, ui32 count) {
        auto threads = MakeHolder<TCacheThreadSubSystem>(Max<ui32>(count, 1));
        auto space = TSharedCacheSpace::Create(current, reserved, *threads);
        return std::pair(std::move(threads), std::move(space));
    }

    static TSpaceHazard& Hazard(const TSharedCacheSpace& space, ui32 index = 0) {
        return *space.Hazards_.at(index);
    }

    template <class TTraits>
    static TSpaceHazard& Hazard(const TSharedCacheImpl<TTraits>& cache, ui32 index = 0) {
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
    const bool applicationThread = !runtime.IsRealThreads() && runtime.GetNodeCount() == 1;
    runtime.SetupNodeSubSystems = [count, applicationThread, previous = std::move(previous)](
                                      ui32 node, NActors::TActorSystemSetup* setup) {
        if (previous) {
            previous(node, setup);
        }
        if (!NActors::GetSubSystem<TCacheThreadSubSystem>(setup->SubSystems)) {
            auto threads = std::make_unique<TCacheThreadSubSystem>(count);
            setup->RegisterSubSystem(std::move(threads));
        }
        if (applicationThread && !NActors::GetSubSystem<TApplicationThreadSubSystem>(setup->SubSystems)) {
            setup->RegisterSubSystem(std::make_unique<TApplicationThreadSubSystem>());
        }
    };
}

} // namespace NKikimr::NSharedCache
