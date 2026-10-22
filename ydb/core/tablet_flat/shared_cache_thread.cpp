#include "shared_cache_thread.h"

namespace NKikimr::NSharedCache {

TCacheThreadSubSystem::TCacheThreadSubSystem(ui32 threadCount) {
    Hazards_.reserve(threadCount);
    while (Hazards_.size() < threadCount) {
        Hazards_.push_back(std::make_unique<TSpaceHazard>());
    }
}

void TCacheThreadSubSystem::OnExecutorThreadPrepare(NActors::TThreadContext* context) {
    auto hazard = std::make_unique<TSpaceHazard>();
    Y_ABORT_UNLESS(Threads_.emplace(context, hazard.get()).second);
    Hazards_.push_back(std::move(hazard));
}

void TCacheThreadSubSystem::OnExecutorThreadStart(NActors::TThreadContext* context) {
    Y_ABORT_UNLESS(!TlsCacheHazard);
    TlsCacheHazard = Threads_.at(context);
}

void TCacheThreadSubSystem::OnExecutorThreadStop(NActors::TThreadContext* context) {
    Y_DEBUG_ABORT_UNLESS(TlsCacheHazard == Threads_.at(context));
    Y_DEBUG_ABORT_UNLESS(TlsCacheHazard->State.load(std::memory_order_relaxed) == 0);
    TlsCacheHazard = nullptr;
}

} // namespace NKikimr::NSharedCache
