#pragma once

#include "shared_cache_fwd.h"
#include "shared_cache_item.h"

#include <util/generic/noncopyable.h>
#include <util/system/compiler.h>

#include <utility>

namespace NKikimr::NSharedCache {

class TCollection;
class TSpaceOperation;
template <class TTraits>
class TSharedCacheTableImpl;
template <class TTraits>
class TCacheItems;
template <class TTraits>
class TOperationItemRef;

template <class TTraits = TProdTraits>
class TSharedCacheItemRefImpl {
public:
    TSharedCacheItemRefImpl() noexcept = default;
    TSharedCacheItemRefImpl(const TSharedCacheItemRefImpl&) = delete;
    TSharedCacheItemRefImpl& operator=(const TSharedCacheItemRefImpl&) = delete;

    Y_FORCE_INLINE TSharedCacheItemRefImpl(TSharedCacheItemRefImpl&& other) noexcept
        : CacheItem_(other.Take())
    {}

    Y_FORCE_INLINE TSharedCacheItemRefImpl& operator=(TSharedCacheItemRefImpl&& other) noexcept;

    Y_FORCE_INLINE ~TSharedCacheItemRefImpl() {
        Drop();
    }

    explicit operator bool() const noexcept {
        return !CacheItem_.IsNull();
    }

    ui32 Index() const noexcept {
        return CacheItem_.Index();
    }

    ui32 Version() const noexcept {
        return CacheItem_.Version();
    }

    TCacheItem CacheItem() const noexcept {
        return CacheItem_;
    }

    Y_FORCE_INLINE void Drop() noexcept;

private:
    friend class TSharedCacheTableImpl<TTraits>;
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheItems<TTraits>;
    friend class TSharedPageRefImpl<TTraits>;
    friend class TSharedCacheCollectionRefImpl<TTraits>;
    friend class TOperationItemRef<TTraits>;

    Y_FORCE_INLINE explicit TSharedCacheItemRefImpl(TCacheItem cacheItem) noexcept
        : CacheItem_(cacheItem)
    {}

    Y_FORCE_INLINE TCacheItem Take() noexcept {
        return std::exchange(CacheItem_, {});
    }

    Y_FORCE_INLINE void Disarm() noexcept {
        Y_DEBUG_ABORT_UNLESS(!CacheItem_.IsNull());
        CacheItem_ = {};
    }

    Y_FORCE_INLINE void Drop(TSpaceOperation& spaceOp) noexcept;

    static Y_FORCE_INLINE void Release(TCacheItem item) noexcept {
        // A missing resolver is the explicit terminal-shutdown abandonment path
        if (TSharedCacheImpl<TTraits>* cache = TSharedCacheImpl<TTraits>::TryGet()) {
            auto cacheOp = cache->BeginOperation();
            cacheOp.Release(item);
        }
    }

    static Y_FORCE_INLINE void Release(TSpaceOperation& spaceOp, TCacheItem item) noexcept {
        auto cacheOp = TSharedCacheImpl<TTraits>::Get().BindOperation(spaceOp);
        cacheOp.Release(item);
    }

private:
    TCacheItem CacheItem_;
};

template <class TTraits>
class TOperationItemRef {
public:
    Y_FORCE_INLINE explicit TOperationItemRef(TSpaceOperation& spaceOp) noexcept
        : SpaceOp_(spaceOp)
    {}

    Y_FORCE_INLINE TOperationItemRef(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept
        : SpaceOp_(spaceOp)
        , Ref_(cacheItem)
    {}

    Y_FORCE_INLINE TOperationItemRef(TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) noexcept
        : SpaceOp_(spaceOp)
        , Ref_(std::move(ref))
    {}

    TOperationItemRef(const TOperationItemRef&) = delete;
    TOperationItemRef& operator=(const TOperationItemRef&) = delete;
    TOperationItemRef(TOperationItemRef&&) = delete;
    TOperationItemRef& operator=(TOperationItemRef&&) = delete;

    Y_FORCE_INLINE ~TOperationItemRef() {
        Ref_.Drop(SpaceOp_);
    }

    Y_FORCE_INLINE explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    Y_FORCE_INLINE TCacheItem CacheItem() const noexcept {
        return Ref_.CacheItem();
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits>& Get() noexcept {
        return Ref_;
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> Take() noexcept {
        return std::move(Ref_);
    }

    Y_FORCE_INLINE void Drop() noexcept {
        Ref_.Drop(SpaceOp_);
    }

    Y_FORCE_INLINE TSpaceOperation& SpaceOperation() const noexcept {
        return SpaceOp_;
    }

    Y_FORCE_INLINE void Disarm() noexcept {
        Ref_.Disarm();
    }

private:
    TSpaceOperation& SpaceOp_;
    TSharedCacheItemRefImpl<TTraits> Ref_;
};

template <class TTraits = TProdTraits>
class TSharedPageRefImpl : private TMoveOnly {
public:
    TSharedPageRefImpl() noexcept = default;

    Y_FORCE_INLINE TSharedPageRefImpl(TSharedPageRefImpl&& other) noexcept
        : Ref_(std::move(other.Ref_))
    {}

    Y_FORCE_INLINE TSharedPageRefImpl& operator=(TSharedPageRefImpl&& other) noexcept;

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    Y_FORCE_INLINE TPageCacheItem CacheItem() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Ref_);
        return TPageCacheItem::FromValidated(Ref_.CacheItem_);
    }

    Y_FORCE_INLINE const char* data() const noexcept {
        return WithHandle([](const THandle& handle) {
            return handle.Body.PageBuffer.data();
        });
    }

    Y_FORCE_INLINE size_t size() const noexcept {
        if (!Ref_) {
            return 0;
        }
        return WithHandle([](const THandle& handle) {
            return handle.Key2OrSize.load(std::memory_order_relaxed);
        });
    }

    Y_FORCE_INLINE NActors::TSharedData BuildSharedData() const noexcept {
        return WithHandle([](const THandle& handle) {
            return handle.Body.PageBuffer.Share(handle.Key2OrSize.load(std::memory_order_relaxed));
        });
    }
    // Construct an independent owner only when moving or borrowing cannot preserve the required lifetime.
    Y_FORCE_INLINE TSharedPageRefImpl Acquire() const& noexcept {
        if (!Ref_) {
            return {};
        }
        TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
        return cache.AcquireMorePageRef(Ref_.CacheItem());
    }

    Y_FORCE_INLINE NTable::NPage::TPageOffset GetOffset() const noexcept {
        return WithHandle([](const THandle& handle) {
            return NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed));
        });
    }

    Y_FORCE_INLINE NTable::NPage::TPageLocation GetLocation() const noexcept {
        return WithHandle([](const THandle& handle) {
            return NTable::NPage::TPageLocation(
                NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
                handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type,
                handle.Metadata.Page.Crc32);
        });
    }

    Y_FORCE_INLINE TLogoBlobID GetCollectionId() const noexcept {
        TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
        auto cacheOp = cache.BeginOperation();
        const THandle& page = cacheOp.Handles()[CacheItem().Index()];
        return cacheOp.Collections().PageCollection(page)->Id();
    }

    Y_FORCE_INLINE NTable::NPage::EPage GetType() const noexcept {
        return WithHandle([](const THandle& handle) {
            return handle.Metadata.Page.Type;
        });
    }

    Y_FORCE_INLINE bool IsSticky() const noexcept {
        if (!Ref_) {
            return false;
        }
        TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
        return cache.IsPageSticky(CacheItem());
    }

    Y_FORCE_INLINE void Drop() noexcept {
        Ref_.Drop();
    }

private:
    friend class TCacheItems<TTraits>;

    Y_FORCE_INLINE explicit TSharedPageRefImpl(TSharedCacheItemRefImpl<TTraits>&& ref) noexcept
        : Ref_(std::move(ref))
    {
        Y_DEBUG_ABORT_UNLESS(Ref_);
    }

    template <class TCallback>
    Y_FORCE_INLINE decltype(auto) WithHandle(TCallback&& callback) const noexcept;

private:
    TSharedCacheItemRefImpl<TTraits> Ref_;
};

template <class TTraits = TProdTraits>
class TSharedCacheCollectionRefImpl : private TMoveOnly {
public:
    TSharedCacheCollectionRefImpl() noexcept = default;

    Y_FORCE_INLINE TSharedCacheCollectionRefImpl(TSharedCacheCollectionRefImpl&& other) noexcept
        : Ref_(std::move(other.Ref_))
    {}

    Y_FORCE_INLINE TSharedCacheCollectionRefImpl& operator=(TSharedCacheCollectionRefImpl&& other) noexcept;

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    Y_FORCE_INLINE TCollectionCacheItem CacheItem() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Ref_);
        return TCollectionCacheItem::FromValidated(Ref_.CacheItem_);
    }

    Y_FORCE_INLINE TCollection& GetCollection() const noexcept;

    Y_FORCE_INLINE TSharedCacheCollectionRefImpl Acquire() const& noexcept {
        if (!Ref_) {
            return {};
        }
        TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
        return cache.AcquireMoreCollectionRef(Ref_.CacheItem());
    }

    Y_FORCE_INLINE bool IsSoleReference() const noexcept;

    TCollection* Get() const noexcept {
        return Ref_ ? &GetCollection() : nullptr;
    }

    TCollection* operator->() const noexcept {
        return &GetCollection();
    }

    TCollection& operator*() const noexcept {
        return GetCollection();
    }

    Y_FORCE_INLINE void Drop() noexcept {
        Ref_.Drop();
    }

private:
    friend class TCacheItems<TTraits>;

    Y_FORCE_INLINE explicit TSharedCacheCollectionRefImpl(TSharedCacheItemRefImpl<TTraits>&& ref) noexcept
        : Ref_(std::move(ref))
    {
        Y_DEBUG_ABORT_UNLESS(Ref_);
    }

private:
    TSharedCacheItemRefImpl<TTraits> Ref_;
};

// Inline implementations

template <class TTraits>
Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits>& TSharedCacheItemRefImpl<TTraits>::operator=(
    TSharedCacheItemRefImpl&& other) noexcept {
    if (this != &other) {
        Drop();
        CacheItem_ = other.Take();
    }
    return *this;
}

template <class TTraits>
Y_FORCE_INLINE void TSharedCacheItemRefImpl<TTraits>::Drop() noexcept {
    const TCacheItem item = Take();
    if (!item.IsNull()) {
        Release(item);
    }
}

template <class TTraits>
Y_FORCE_INLINE void TSharedCacheItemRefImpl<TTraits>::Drop(TSpaceOperation& spaceOp) noexcept {
    const TCacheItem item = Take();
    if (!item.IsNull()) {
        Release(spaceOp, item);
    }
}

template <class TTraits>
Y_FORCE_INLINE TSharedPageRefImpl<TTraits>& TSharedPageRefImpl<TTraits>::operator=(
    TSharedPageRefImpl&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
    }
    return *this;
}

template <class TTraits>
template <class TCallback>
Y_FORCE_INLINE decltype(auto) TSharedPageRefImpl<TTraits>::WithHandle(TCallback&& callback) const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    const TCacheItem item = Ref_.CacheItem();
    const THandle& handle = cache.Space_->OwnedHandle(item);
    Y_DEBUG_ABORT_UNLESS([&] {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        return item.Matches(state) && state.IsPageKind() && state.Refs() != 0;
    }());
    return std::forward<TCallback>(callback)(handle);
}

template <class TTraits>
Y_FORCE_INLINE TSharedCacheCollectionRefImpl<TTraits>& TSharedCacheCollectionRefImpl<TTraits>::operator=(
    TSharedCacheCollectionRefImpl<TTraits>&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
    }
    return *this;
}

template <class TTraits>
Y_FORCE_INLINE TCollection& TSharedCacheCollectionRefImpl<TTraits>::GetCollection() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    auto spaceOp = cache.BeginOperation();
    const THandle& handle = spaceOp.Handles()[Ref_.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(Ref_.CacheItem().Matches(state) && state.IsCollectionKind() && state.Refs() != 0);
    return *handle.Body.Collection;
}

template <class TTraits>
Y_FORCE_INLINE bool TSharedCacheCollectionRefImpl<TTraits>::IsSoleReference() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    auto& cache = TSharedCacheImpl<TTraits>::Get();
    auto spaceOp = cache.BeginOperation();
    const THandleState state =
        THandleState::FromRaw(spaceOp.Handles()[Ref_.Index()].State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(Ref_.CacheItem().Matches(state) && state.IsCollectionKind() && state.Refs() != 0);
    return state.Refs() == 1;
}

} // namespace NKikimr::NSharedCache
