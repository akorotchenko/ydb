#pragma once

#include "shared_cache_item.h"
#include "shared_cache_tier.h"

namespace NKikimr::NSharedCache {

class TSpaceOperation;

// One tier's ordinary Cold queue; the coordinator owns eligibility and payload erasure
template <class TTraits>
class TCacheColdPolicy {
public:
    explicit TCacheColdPolicy(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    // Follows a final decrement; the caller acquired prior releases before this membership read
    void PublishCold(TSpaceOperation& spaceOp, const TCacheTierView& tier, TCacheItem nativeItem) noexcept;

    // Routes an already removed word without refreshing its membership version
    void RouteCold(TSpaceOperation& spaceOp, const TCacheTierView& tier, TColdCacheItem item) noexcept;

    Y_FORCE_INLINE bool Reclaim(TSpaceOperation& spaceOp, const TCacheTierView& tier) noexcept {
        return ReclaimImpl<false>(spaceOp, tier);
    }

    Y_FORCE_INLINE bool ReclaimForPreload(TSpaceOperation& spaceOp, const TCacheTierView& tier) noexcept {
        return ReclaimImpl<true>(spaceOp, tier);
    }

private:
    void Enqueue(TSpaceOperation& spaceOp, const TCacheTierView& tier, TColdCacheItem item, THandle& handle,
        THandleState state) noexcept;

    template <bool Preload>
    bool ReclaimImpl(TSpaceOperation& spaceOp, const TCacheTierView& tier) noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
};

} // namespace NKikimr::NSharedCache
