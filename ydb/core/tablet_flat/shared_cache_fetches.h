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
class TCollection;

template <class TTraits>
class TCacheFetches {
public:
    using TOperation = TCacheFetchesOperation<TTraits>;

    explicit TCacheFetches(TSharedCacheImpl<TTraits>& cache) noexcept
        : Cache_(cache)
    {}

    Y_FORCE_INLINE TOperation BindOperation(TSpaceOperation& spaceOp) noexcept {
        return { Cache_, spaceOp };
    }

    static Y_FORCE_INLINE void DeletePendingFetch(THandle& handle) noexcept {
        if (TPageFetchState* fetch = std::exchange(handle.Body.Fetch, nullptr)) {
            fetch->UnRef();
        }
    }

private:
    friend class TCacheFetchesOperation<TTraits>;

    static Y_FORCE_INLINE bool MatchesPendingFetch(
        const THandle& handle, THandleState state, TCacheItem cacheItem, const TPageFetchState& fetch) noexcept {
        return cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() && handle.Body.Fetch == &fetch &&
               fetch.Page().CacheItem() == cacheItem;
    }

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
    TSharedCacheImpl<TTraits>& Cache_;
};

// Leave operation constructors implicitly instantiated
template <class TTraits>
class TCacheFetchesOperation {
public:
    Y_FORCE_INLINE TIntrusivePtr<TPageFetchState> TryAcquire(
        TPageCacheItem page, TOperationItemRef<TTraits>& owner) noexcept {
        return Cache_.Fetches_.TryAcquire(SpaceOp_, page, owner);
    }

    Y_FORCE_INLINE bool Complete(
        TOperationItemRef<TTraits>& owner, TPageFetchState& fetch, NActors::TSharedData&& data) noexcept {
        return Cache_.Fetches_.Complete(owner, fetch, std::move(data));
    }

    Y_FORCE_INLINE bool Fail(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept {
        return Cache_.Fetches_.Fail(owner, fetch);
    }

    Y_FORCE_INLINE void Release(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept {
        Cache_.Fetches_.Release(owner, fetch);
    }

    Y_FORCE_INLINE bool ReserveMemory(TCacheItem item, TPageFetchState& fetch) noexcept {
        return Cache_.Fetches_.ReserveMemory(SpaceOp_, item, fetch);
    }

    Y_FORCE_INLINE bool TryReserveBatch(TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept {
        return Cache_.Fetches_.TryReserveBatch(SpaceOp_, pages);
    }

    Y_FORCE_INLINE bool Dispatch(TCacheItem item, TPageFetchState& fetch) noexcept {
        return Cache_.Fetches_.Dispatch(SpaceOp_, item, fetch);
    }

private:
    friend class TCacheFetches<TTraits>;
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheOperationView<TTraits>;

    Y_FORCE_INLINE TCacheFetchesOperation(TSharedCacheImpl<TTraits>& cache, TSpaceOperation& spaceOp) noexcept
        : Cache_(cache)
        , SpaceOp_(spaceOp)
    {}

private:
    TSharedCacheImpl<TTraits>& Cache_;
    TSpaceOperation& SpaceOp_;
};

template <class TTraits = TProdTraits>
class TPageFetchImpl : private TMoveOnly {
public:
    TPageFetchImpl() noexcept = default;

    Y_FORCE_INLINE TPageFetchImpl(TPageFetchImpl&& other) noexcept
        : FetchState_(std::move(other.FetchState_))
    {}

    Y_FORCE_INLINE TPageFetchImpl& operator=(TPageFetchImpl&& other) noexcept;

    Y_FORCE_INLINE ~TPageFetchImpl() {
        FailReady();
    }

    explicit operator bool() const noexcept {
        return bool(FetchState_);
    }

    Y_FORCE_INLINE TPageCacheItem CacheItem() const noexcept {
        Y_DEBUG_ABORT_UNLESS(FetchState_);
        return FetchState_->Page();
    }

    Y_FORCE_INLINE NTable::NPage::TPageLocation Location() const noexcept;

    Y_FORCE_INLINE ui64 Size() const noexcept {
        return Location().Size;
    }

    Y_FORCE_INLINE bool HasActiveWaiters() const noexcept {
        return FetchState_ && FetchState_->HasActiveWaiters();
    }

    Y_FORCE_INLINE void ForEachActiveWaiter(TFunctionRef<void(const TPageFetchWaiter&)> visitor) const noexcept {
        if (FetchState_) {
            FetchState_->ForEachActiveWaiter(visitor);
        }
    }

    Y_FORCE_INLINE bool TryReserveMemory() noexcept {
        if (!FetchState_) {
            return false;
        }
        TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
        auto cacheOp = cache.BeginOperation();
        return cacheOp.Fetches().ReserveMemory(FetchState_->Page().CacheItem(), *FetchState_);
    }

    Y_FORCE_INLINE bool Dispatch() noexcept {
        if (!FetchState_) {
            return false;
        }
        TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
        auto cacheOp = cache.BeginOperation();
        return cacheOp.Fetches().Dispatch(FetchState_->Page().CacheItem(), *FetchState_);
    }

    Y_FORCE_INLINE bool MakeReady(NActors::TSharedData&& data) noexcept;
    Y_FORCE_INLINE bool FailReady() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheFetches<TTraits>;

    Y_FORCE_INLINE explicit TPageFetchImpl(TPageFetchState* fetch) noexcept
        : FetchState_(fetch)
    {
        Y_DEBUG_ABORT_UNLESS(FetchState_ && FetchState_->Page());
    }

private:
    TIntrusivePtr<TPageFetchState> FetchState_;
};

template <class TTraits>
Y_FORCE_INLINE TPageFetchImpl<TTraits>& TPageFetchImpl<TTraits>::operator=(TPageFetchImpl<TTraits>&& other) noexcept {
    if (this != &other) {
        FailReady();
        FetchState_ = std::move(other.FetchState_);
    }
    return *this;
}

template <class TTraits>
Y_FORCE_INLINE NTable::NPage::TPageLocation TPageFetchImpl<TTraits>::Location() const noexcept {
    Y_DEBUG_ABORT_UNLESS(FetchState_);
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    auto spaceOp = cache.BeginOperation();
    const TCacheItem cacheItem = FetchState_->Page().CacheItem();
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(
        cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() && handle.Body.Fetch == FetchState_.Get());
    return {
        NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
        handle.Key2OrSize.load(std::memory_order_relaxed),
        handle.Metadata.Page.Type,
        handle.Metadata.Page.Crc32,
    };
}

template <class TTraits>
Y_FORCE_INLINE bool TPageFetchImpl<TTraits>::MakeReady(NActors::TSharedData&& data) noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    bool ready;
    {
        auto cacheOp = cache.BeginOperation();
        // The fetch owns the insertion ref represented by FetchState_, without storing a second item identity.
        TOperationItemRef<TTraits> owner(cacheOp, FetchState_->Page().CacheItem());
        ready = cacheOp.Fetches().Complete(owner, *FetchState_, std::move(data));
    }
    FetchState_.Drop();
    return ready;
}

template <class TTraits>
Y_FORCE_INLINE bool TPageFetchImpl<TTraits>::FailReady() noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCacheImpl<TTraits>* cache = TSharedCacheImpl<TTraits>::TryGet();
    if (!cache) {
        // Missing actor context is terminal shutdown, as for native item references.
        FetchState_->AbandonWaiters();
        FetchState_.Drop();
        return false;
    }
    bool failed;
    {
        auto cacheOp = cache->BeginOperation();
        TOperationItemRef<TTraits> owner(cacheOp, FetchState_->Page().CacheItem());
        failed = cacheOp.Fetches().Fail(owner, *FetchState_);
    }
    FetchState_.Drop();
    return failed;
}

} // namespace NKikimr::NSharedCache
