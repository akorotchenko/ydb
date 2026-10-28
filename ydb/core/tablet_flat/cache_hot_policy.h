#pragma once

#include "cache_item.h"
#include "cache_tier.h"

namespace NKikimr::NSharedCache {

class TSpaceOperation;

// One tier's Hot algorithm; the eviction coordinator owns Cold routing and retention policy
template <class TTraits>
class TCacheHotPolicy {
public:
    explicit TCacheHotPolicy(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    TCacheItem PutHot(TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level, TCacheItem item) noexcept;

    Y_FORCE_INLINE void RouteHot(
        TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level, TCacheItem item) noexcept {
        const TCacheItem displaced = PutHot(spaceOp, tier, level, item);
        if (!displaced.IsNull()) {
            ProcessHot(spaceOp, tier, level, displaced);
        }
    }

    void ProcessHot(TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level, TCacheItem item) noexcept;

    Y_FORCE_INLINE bool ProcessBatch(TSpaceOperation& spaceOp, const TCacheTierView& tier) noexcept {
        return ProcessBatchImpl<false>(spaceOp, tier);
    }

    Y_FORCE_INLINE bool ProcessPreloadBatch(TSpaceOperation& spaceOp, const TCacheTierView& tier) noexcept {
        return ProcessBatchImpl<true>(spaceOp, tier);
    }

private:
    template <bool Preload>
    bool ProcessBatchImpl(TSpaceOperation& spaceOp, const TCacheTierView& tier) noexcept;

    template <bool Preload, bool Requeue = false>
    TCacheItem PutHotImpl(TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level, TCacheItem item,
        TRingExchangeResult* placement = nullptr) noexcept;

    static Y_FORCE_INLINE bool TakeHotFrequency(
        THandle& handle, TCacheItem item, ECacheTier expectedTier, THandleState& state, ui8& frequency) noexcept;

    template <bool Preload>
    void ProcessHotImpl(
        TSpaceOperation& spaceOp, const TCacheTierView& tier, EHotLevel level, TCacheItem item) noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
};

} // namespace NKikimr::NSharedCache
