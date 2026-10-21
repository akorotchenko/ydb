#pragma once

#include <ydb/library/actors/core/subsystem.h>

#include <util/generic/ylimits.h>
#include <util/system/types.h>

#include <unordered_map>

namespace NKikimr::NSharedCache {

class TCacheThreadSubSystem;

class TCacheThreadContext {
public:
    static constexpr ui32 InvalidIndex = Max<ui32>();

    static ui32 Index() noexcept {
        return Index_;
    }

    static const TCacheThreadSubSystem* Domain() noexcept {
        return Domain_;
    }

private:
    friend class TCacheThreadSubSystem;

    inline static thread_local ui32 Index_ = InvalidIndex;
    inline static thread_local const TCacheThreadSubSystem* Domain_ = nullptr;
};

class TCacheThreadSubSystem final : public NActors::ISubSystem {
public:
    void OnExecutorThreadPrepare(NActors::TThreadContext* context) override;
    void OnExecutorThreadStart(NActors::TThreadContext* context) override;
    void OnExecutorThreadStop(NActors::TThreadContext* context) override;

    ui32 WorkerCount() const noexcept {
        return static_cast<ui32>(Indices_.size());
    }

private:
    // Prepared before any worker starts and immutable while workers run
    std::unordered_map<NActors::TThreadContext*, ui32> Indices_;
};

} // namespace NKikimr::NSharedCache
