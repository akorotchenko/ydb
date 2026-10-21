#pragma once

#include "shared_cache_space.h"

namespace NKikimr::NSharedCache {

class TCacheMemory;
template <class TTraits>
class TCacheEviction;

template <class TTraits>
class TCacheResize {
public:
    TCacheResize(TSharedCacheSpace& space, TCacheMemory& memory, TCacheEviction<TTraits>& eviction) noexcept
        : Space_(space)
        , Memory_(memory)
        , Eviction_(eviction)
    {}

    bool PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept;
    bool PublishMigrationView(TTransition& transition, THotResize& hotResize) noexcept;
    bool TryDrainTransition(TTransition& transition) noexcept;
    bool PublishFinalView(TTransition& transition, THotResize& hotResize) noexcept;
    bool AppendFreeHandles(TTransition& transition) noexcept;
    bool FinalDrain(TTransition& transition) noexcept;
    bool TryReleaseTransition(TTransition& transition) noexcept;
    bool AbandonTransition(TTransition& transition) noexcept;

private:
    TSharedCacheSpace& Space_;
    TCacheMemory& Memory_;
    TCacheEviction<TTraits>& Eviction_;
};

} // namespace NKikimr::NSharedCache
