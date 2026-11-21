#pragma once

#include <ydb/library/actors/core/subsystem.h>

#include <util/generic/array_ref.h>
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <atomic>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace NKikimr::NSharedCache {

class TCacheSpace;
struct TTestTraits;
template <class TTraits>
class TCacheImpl;

inline constexpr ui64 SpaceHazardDrainWaiter = ui64{ 1 } << 31;
inline constexpr ui32 MaxSpaceHazardActiveCount = static_cast<ui32>(SpaceHazardDrainWaiter - 1);

inline constexpr ui64 SpaceHazardGeneration(ui64 state) noexcept {
    return state >> 32;
}

inline constexpr ui32 SpaceHazardActiveCount(ui64 state) noexcept {
    return static_cast<ui32>(state) & MaxSpaceHazardActiveCount;
}

inline constexpr bool SpaceHazardIsActive(ui64 state) noexcept {
    return SpaceHazardActiveCount(state) != 0;
}

inline constexpr bool SpaceHazardIsDrained(ui64 state, ui64 generation) noexcept {
    return !SpaceHazardIsActive(state) || SpaceHazardGeneration(state) == generation;
}

struct alignas(64) TSpaceHazard {
    // Threads update counts; drainers retag idle slots and arm old active slots for last-exit notification
    std::atomic<ui64> State{ 0 }; // {generation:32, drain-waiter:1, active-count:31}
    std::atomic<ui64> SpareItem{ 0 };
    TCacheImpl<TTestTraits>* TestCache = nullptr;
    const TCacheSpace* Space = nullptr;
    bool PreloadReclaim = false; // Current thread's synchronous handle reclamation
};

inline void SetHazardSpace(TConstArrayRef<TSpaceHazard*> hazards, const TCacheSpace* space) noexcept {
    for (TSpaceHazard* hazard : hazards) {
        Y_DEBUG_ABORT_UNLESS(hazard->State.load(std::memory_order_relaxed) == 0);
        hazard->Space = space;
    }
}

static_assert(sizeof(TSpaceHazard) == 64);
static_assert(alignof(TSpaceHazard) == 64);

// One record per cache instance on this OS thread; production workers use index zero
inline thread_local constinit TArrayRef<TSpaceHazard> TlsCacheHazards;

class TCacheThreadSubSystem final : public NActors::ISubSystem {
public:
    explicit TCacheThreadSubSystem(ui32 threadCount = 0);

    TCacheThreadSubSystem(const TCacheThreadSubSystem&) = delete;
    TCacheThreadSubSystem& operator=(const TCacheThreadSubSystem&) = delete;

    Y_FORCE_INLINE void OnExecutorThreadPrepare(NActors::TThreadContext* context) override {
        Y_DEBUG_ABORT_UNLESS(HazardIndex_ == 0);
        auto hazard = std::make_shared<TSpaceHazard[1]>();
        Y_ABORT_UNLESS(Threads_.emplace(context, hazard.get()).second);
        Hazards_.push_back(std::move(hazard));
    }

    Y_FORCE_INLINE void OnExecutorThreadStart(NActors::TThreadContext* context) override {
        Y_ABORT_UNLESS(!TlsCacheHazards);
        TlsCacheHazards = { Threads_.at(context), 1 };
    }

    Y_FORCE_INLINE void OnExecutorThreadStop(NActors::TThreadContext* context) override {
        Y_DEBUG_ABORT_UNLESS(TlsCacheHazards.data() == Threads_.at(context));
        for (const auto& hazard : TlsCacheHazards) {
            Y_DEBUG_ABORT_UNLESS(!SpaceHazardIsActive(hazard.State.load(std::memory_order_relaxed)));
        }
        TlsCacheHazards = {};
    }

    ui32 ThreadCount() const noexcept {
        return static_cast<ui32>(Hazards_.size());
    }

    TSpaceHazard& Hazard() noexcept {
        return Hazards_.front()[HazardIndex_];
    }

private:
    friend class TCacheSpace;
    friend class TCacheThreadTestAccess;

    // Prepared before workers start; records survive individual thread stops
    std::vector<std::shared_ptr<TSpaceHazard[]>> Hazards_;
    std::unordered_map<NActors::TThreadContext*, TSpaceHazard*> Threads_;
    ui32 HazardIndex_ = 0;
};

} // namespace NKikimr::NSharedCache
