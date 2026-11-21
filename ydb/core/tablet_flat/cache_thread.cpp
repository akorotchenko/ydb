#include "cache_thread.h"

namespace NKikimr::NSharedCache {

TCacheThreadSubSystem::TCacheThreadSubSystem(ui32 threadCount) {
    Hazards_.reserve(threadCount);
    while (Hazards_.size() < threadCount) {
        Hazards_.push_back(std::make_shared<TSpaceHazard[1]>());
    }
}

} // namespace NKikimr::NSharedCache
