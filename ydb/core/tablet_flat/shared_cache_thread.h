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

class TSharedCacheSpace;
struct TTestTraits;
template <class TTraits>
class TSharedCacheImpl;

inline constexpr ui64 SpaceHazardDrainWaiter = ui64{ 1 } << 31;
inline constexpr ui32 MaxSpaceHazardActiveCount = static_cast<ui32>(SpaceHazardDrainWaiter - 1);

struct alignas(64) TSpaceHazard {
    std::atomic<ui64> State{ 0 }; // {generation:32, drain-waiter:1, active-count:31}
    std::atomic<ui64> SpareItem{ 0 };
    TSharedCacheImpl<TTestTraits>* TestCache = nullptr;
    const TSharedCacheSpace* Space = nullptr;
};

inline void SetHazardSpace(TConstArrayRef<TSpaceHazard*> hazards, const TSharedCacheSpace* space) noexcept {
    for (TSpaceHazard* hazard : hazards) {
        hazard->Space = space;
    }
}

static_assert(sizeof(TSpaceHazard) == 64);
static_assert(alignof(TSpaceHazard) == 64);

inline thread_local TSpaceHazard* TlsCacheHazard = nullptr;

class TCacheThreadSubSystem final : public NActors::ISubSystem {
public:
    explicit TCacheThreadSubSystem(ui32 threadCount = 0);

    Y_FORCE_INLINE void OnExecutorThreadPrepare(NActors::TThreadContext* context) override {
        auto hazard = std::make_unique<TSpaceHazard>();
        Y_ABORT_UNLESS(Threads_.emplace(context, hazard.get()).second);
        Hazards_.push_back(std::move(hazard));
    }

    Y_FORCE_INLINE void OnExecutorThreadStart(NActors::TThreadContext* context) override {
        Y_ABORT_UNLESS(!TlsCacheHazard);
        TlsCacheHazard = Threads_.at(context);
    }

    Y_FORCE_INLINE void OnExecutorThreadStop(NActors::TThreadContext* context) override {
        Y_DEBUG_ABORT_UNLESS(TlsCacheHazard == Threads_.at(context));
        Y_DEBUG_ABORT_UNLESS(TlsCacheHazard->State.load(std::memory_order_relaxed) == 0);
        TlsCacheHazard = nullptr;
    }

    ui32 ThreadCount() const noexcept {
        return static_cast<ui32>(Hazards_.size());
    }

    TSpaceHazard& Hazard() noexcept {
        return *Hazards_.front();
    }

private:
    friend class TSharedCacheSpace;
    friend class TCacheThreadTestAccess;

    // Prepared before workers start; records survive individual thread stops
    std::vector<std::unique_ptr<TSpaceHazard>> Hazards_;
    std::unordered_map<NActors::TThreadContext*, TSpaceHazard*> Threads_;
};

} // namespace NKikimr::NSharedCache
