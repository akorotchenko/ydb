#pragma once

#include "shared_cache_fwd.h"
#include "shared_cache_space.h"

#include <util/generic/array_ref.h>

namespace NKikimr::NSharedCache {

// Coordinator services until the remaining component owners are extracted
template <class TTraits>
class TCacheFetchServices {
public:
    explicit TCacheFetchServices(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    bool TryReserve(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages) noexcept;
    void ReleaseReservation(ui64 bytes, ui64 pages) noexcept;
    static ui64 PageBytes(const THandle& handle) noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
};

template <class TTraits>
class TCacheFetches {
public:
    class TOperation {
    public:
        bool ReserveMemory(TCacheItem item, TPageFetchState& fetch) noexcept {
            return Fetches_.ReserveMemory(SpaceOp_, item, fetch);
        }

        bool TryReserveBatch(TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept {
            return Fetches_.TryReserveBatch(SpaceOp_, pages);
        }

        bool Dispatch(TCacheItem item, TPageFetchState& fetch) noexcept {
            return Fetches_.Dispatch(SpaceOp_, item, fetch);
        }

    private:
        friend class TCacheFetches;

        TOperation(TCacheFetches& fetches, TSpaceOperation& spaceOp) noexcept
            : Fetches_(fetches)
            , SpaceOp_(spaceOp)
        {}

    private:
        TCacheFetches& Fetches_;
        TSpaceOperation& SpaceOp_;
    };

    explicit TCacheFetches(TCacheFetchServices<TTraits> services) noexcept
        : Services_(services)
    {}

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

    // Shared with the pending completion path until its migration
    static bool MatchesPendingFetch(
        const THandle& handle, THandleState state, TCacheItem cacheItem, const TPageFetchState& fetch) noexcept;

private:
    bool ReserveMemory(TSpaceOperation& spaceOp, TCacheItem item, TPageFetchState& fetch) noexcept;
    bool TryReserveBatch(TSpaceOperation& spaceOp, TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept;
    bool Dispatch(TSpaceOperation& spaceOp, TCacheItem cacheItem, TPageFetchState& fetch) noexcept;

private:
    TCacheFetchServices<TTraits> Services_;
};

} // namespace NKikimr::NSharedCache
