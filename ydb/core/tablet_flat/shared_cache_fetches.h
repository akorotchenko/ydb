#pragma once

#include "shared_cache_fwd.h"
#include "shared_cache_space.h"

#include <util/generic/array_ref.h>
#include <util/generic/function_ref.h>
#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>

#include <utility>

namespace NKikimr::NSharedCache {

template <class TTraits>
class TOperationItemRef;
template <class TTraits>
class TSharedCacheTableImpl;
template <class TTraits>
class TCacheCollections;
class TCacheMemory;
class TCollection;

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
    // A false result may leave a Ready reference in owner for the caller to release.
    bool TryAcquirePending(TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef<TTraits>& owner) noexcept;
    void UnlinkPendingStickyPage(TSpaceOperation& spaceOp, TCacheItem page) noexcept;
    void MakeReadyState(TSpaceOperation& spaceOp, TCacheItem item, EHandleState state, TCollection* owner) noexcept;

private:
    TSharedCacheImpl<TTraits>& Cache_;
};

template <class TTraits>
class TCacheFetches {
public:
    class TOperation {
    public:
        TIntrusivePtr<TPageFetchState> TryAcquire(TPageCacheItem page, TOperationItemRef<TTraits>& owner) noexcept {
            return Fetches_.TryAcquire(SpaceOp_, page, owner);
        }

        bool Complete(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch, NActors::TSharedData&& data) noexcept {
            return Fetches_.Complete(owner, fetch, std::move(data));
        }

        bool Fail(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept {
            return Fetches_.Fail(owner, fetch);
        }

        void Release(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept {
            Fetches_.Release(owner, fetch);
        }

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

    TCacheFetches(TCacheFetchServices<TTraits> services, TCacheMemory& memory, TSharedCacheTableImpl<TTraits>& table,
        TCacheCollections<TTraits>& collections, const TTraits& traits) noexcept
        : Services_(services)
        , Memory_(memory)
        , Table_(table)
        , Collections_(collections)
        , Traits_(traits)
    {}

    TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { *this, spaceOp };
    }

    static void DeletePendingFetch(THandle& handle) noexcept;

private:
    static bool MatchesPendingFetch(
        const THandle& handle, THandleState state, TCacheItem cacheItem, const TPageFetchState& fetch) noexcept;

    TIntrusivePtr<TPageFetchState> TryAcquire(
        TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef<TTraits>& owner) noexcept;
    THandle* TryGetPendingHandle(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept;
    bool Complete(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch, NActors::TSharedData&& data) noexcept;
    bool Fail(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept;
    void Release(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept;

    void Finish(TOperationItemRef<TTraits>& finalizerRef, TPageFetchState& fetch, bool reserved) noexcept;
    void FinishFailed(TOperationItemRef<TTraits>& finalizerRef, TPageFetchState& fetch, TPageFetchWaiter* waiters,
        bool reserved) noexcept;

    bool ReserveMemory(TSpaceOperation& spaceOp, TCacheItem item, TPageFetchState& fetch) noexcept;
    bool TryReserveBatch(TSpaceOperation& spaceOp, TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept;
    bool Dispatch(TSpaceOperation& spaceOp, TCacheItem cacheItem, TPageFetchState& fetch) noexcept;

private:
    TCacheFetchServices<TTraits> Services_;
    TCacheMemory& Memory_;
    TSharedCacheTableImpl<TTraits>& Table_;
    TCacheCollections<TTraits>& Collections_;
    const TTraits& Traits_;
};

template <class TTraits = TProdTraits>
class TPageFetchImpl : private TMoveOnly {
public:
    TPageFetchImpl() noexcept = default;
    TPageFetchImpl(TPageFetchImpl&& other) noexcept;
    TPageFetchImpl& operator=(TPageFetchImpl&& other) noexcept;
    ~TPageFetchImpl();

    explicit operator bool() const noexcept {
        return bool(FetchState_);
    }

    TPageCacheItem CacheItem() const noexcept;
    NTable::NPage::TPageLocation Location() const noexcept;
    ui64 Size() const noexcept;
    bool HasActiveWaiters() const noexcept;
    void ForEachActiveWaiter(TFunctionRef<void(const TPageFetchWaiter&)> visitor) const noexcept;
    bool TryReserveMemory() noexcept;
    bool Dispatch() noexcept;
    bool MakeReady(NActors::TSharedData&& data) noexcept;
    bool FailReady() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheFetches<TTraits>;

    explicit TPageFetchImpl(TPageFetchState* fetch) noexcept;

private:
    TIntrusivePtr<TPageFetchState> FetchState_;
};

} // namespace NKikimr::NSharedCache
