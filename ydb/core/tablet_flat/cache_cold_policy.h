#pragma once

#include "cache_item.h"
#include "cache_tier.h"

namespace NKikimr::NSharedCache {

class TSpaceOperation;

// One tier's ordinary Cold queue; the coordinator owns eligibility and payload erasure
template <class TTraits>
class TCacheColdPolicy {
public:
    explicit TCacheColdPolicy(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    // Claims one ticket per membership after the final native decrement
    void PublishCold(TSpaceOperation& spaceOp, const TCacheTierView& tier, TCacheItem nativeItem) noexcept;

    // Transfers an owned ticket, including held Cold pages, without refreshing its membership
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
