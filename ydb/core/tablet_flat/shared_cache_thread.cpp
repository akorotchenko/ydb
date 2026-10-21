#include "shared_cache_thread.h"

namespace NKikimr::NSharedCache {

void TCacheThreadSubSystem::OnExecutorThreadPrepare(NActors::TThreadContext* context) {
    const ui32 index = static_cast<ui32>(Indices_.size());
    Y_ABORT_UNLESS(Indices_.emplace(context, index).second);
}

void TCacheThreadSubSystem::OnExecutorThreadStart(NActors::TThreadContext* context) {
    Y_ABORT_UNLESS(TCacheThreadContext::Index_ == TCacheThreadContext::InvalidIndex);
    const auto it = Indices_.find(context);
    Y_ABORT_UNLESS(it != Indices_.end());
    TCacheThreadContext::Domain_ = this;
    TCacheThreadContext::Index_ = it->second;
}

void TCacheThreadSubSystem::OnExecutorThreadStop(NActors::TThreadContext* context) {
    Y_DEBUG_ABORT_UNLESS(TCacheThreadContext::Domain_ == this);
    Y_DEBUG_ABORT_UNLESS(TCacheThreadContext::Index_ == Indices_.at(context));
    TCacheThreadContext::Index_ = TCacheThreadContext::InvalidIndex;
    TCacheThreadContext::Domain_ = nullptr;
}

} // namespace NKikimr::NSharedCache
