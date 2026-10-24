#include "shared_cache_thread.h"

namespace NKikimr::NSharedCache {

TCacheThreadSubSystem::TCacheThreadSubSystem(ui32 threadCount) {
    Hazards_.reserve(threadCount);
    while (Hazards_.size() < threadCount) {
        Hazards_.push_back(std::make_unique<TSpaceHazard>());
    }
}

} // namespace NKikimr::NSharedCache
