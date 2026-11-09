#pragma once

#include "cache_fwd.h"
#include "cache_item.h"

#include <util/generic/noncopyable.h>
#include <util/system/compiler.h>

#include <utility>

namespace NKikimr::NSharedCache {

class TCollection;
class TSpaceOperation;
template <class TTraits>
class TCacheTableImpl;
template <class TTraits>
class TCacheItems;
template <class TTraits>
class TOperationItemRef;

template <class TTraits = TProdTraits>
class TCacheItemRefImpl {
public:
    TCacheItemRefImpl() noexcept = default;
    TCacheItemRefImpl(const TCacheItemRefImpl&) = delete;
    TCacheItemRefImpl& operator=(const TCacheItemRefImpl&) = delete;

    Y_FORCE_INLINE TCacheItemRefImpl(TCacheItemRefImpl&& other) noexcept
        : CacheItem_(other.Take())
    {}

    Y_FORCE_INLINE TCacheItemRefImpl& operator=(TCacheItemRefImpl&& other) noexcept;

    Y_FORCE_INLINE ~TCacheItemRefImpl() {
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
    friend class TCacheTableImpl<TTraits>;
    friend class TCacheImpl<TTraits>;
    friend class TCacheItems<TTraits>;
    friend class TCachePageRefImpl<TTraits>;
    friend class TCacheCollectionRefImpl<TTraits>;
    friend class TOperationItemRef<TTraits>;

    Y_FORCE_INLINE explicit TCacheItemRefImpl(TCacheItem cacheItem) noexcept
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

    template <class TItem>
    static Y_FORCE_INLINE void Release(TItem item) noexcept {
        // A missing resolver is the explicit terminal-shutdown abandonment path
        if (TCacheImpl<TTraits>* cache = TCacheImpl<TTraits>::TryGet()) {
            cache->Items_.Release(item);
        }
    }

    static Y_FORCE_INLINE void Release(TSpaceOperation& spaceOp, TCacheItem item) noexcept {
        auto cacheOp = TCacheImpl<TTraits>::Get().BindOperation(spaceOp);
        cacheOp.ReleaseCleanup(item);
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

    Y_FORCE_INLINE TOperationItemRef(TSpaceOperation& spaceOp, TCacheItemRefImpl<TTraits>&& ref) noexcept
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

    Y_FORCE_INLINE TCacheItemRefImpl<TTraits>& Get() noexcept {
        return Ref_;
    }

    Y_FORCE_INLINE TCacheItemRefImpl<TTraits> Take() noexcept {
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
    TCacheItemRefImpl<TTraits> Ref_;
};

template <class TTraits = TProdTraits>
class TCachePageRefImpl : private TMoveOnly {
public:
    TCachePageRefImpl() noexcept = default;

    Y_FORCE_INLINE ~TCachePageRefImpl() noexcept {
        Drop();
    }

    Y_FORCE_INLINE TCachePageRefImpl(TCachePageRefImpl&& other) noexcept
        : Ref_(std::move(other.Ref_))
    {}

    Y_FORCE_INLINE TCachePageRefImpl& operator=(TCachePageRefImpl&& other) noexcept;

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
    Y_FORCE_INLINE TCachePageRefImpl Acquire() const& noexcept {
        if (!Ref_) {
            return {};
        }
        TCacheImpl<TTraits>& cache = TCacheImpl<TTraits>::Get();
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
        TCacheImpl<TTraits>& cache = TCacheImpl<TTraits>::Get();
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
        TCacheImpl<TTraits>& cache = TCacheImpl<TTraits>::Get();
        return cache.IsPageSticky(CacheItem());
    }

    Y_FORCE_INLINE void Drop() noexcept {
        const TCacheItem item = Ref_.Take();
        if (!item.IsNull()) {
            TCacheItemRefImpl<TTraits>::Release(TPageCacheItem::FromValidated(item));
        }
    }

private:
    friend class TCacheItems<TTraits>;
    friend class TRequestCompletion;

    // Transfer an existing native owner without acquiring another reference
    Y_FORCE_INLINE TCacheItem TakeNativeRef() noexcept {
        return Ref_.Take();
    }

    static Y_FORCE_INLINE TCachePageRefImpl AdoptNativeRef(TCacheItem item) noexcept {
        return TCachePageRefImpl(TCacheItemRefImpl<TTraits>(item));
    }

    Y_FORCE_INLINE explicit TCachePageRefImpl(TCacheItemRefImpl<TTraits>&& ref) noexcept
        : Ref_(std::move(ref))
    {
        Y_DEBUG_ABORT_UNLESS(Ref_);
    }

    template <class TCallback>
    Y_FORCE_INLINE decltype(auto) WithHandle(TCallback&& callback) const noexcept;

private:
    TCacheItemRefImpl<TTraits> Ref_;
};

template <class TTraits = TProdTraits>
class TCacheCollectionRefImpl : private TMoveOnly {
public:
    TCacheCollectionRefImpl() noexcept = default;

    Y_FORCE_INLINE TCacheCollectionRefImpl(TCacheCollectionRefImpl&& other) noexcept
        : Ref_(std::move(other.Ref_))
    {}

    Y_FORCE_INLINE TCacheCollectionRefImpl& operator=(TCacheCollectionRefImpl&& other) noexcept;

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    Y_FORCE_INLINE TCollectionCacheItem CacheItem() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Ref_);
        return TCollectionCacheItem::FromValidated(Ref_.CacheItem_);
    }

    Y_FORCE_INLINE TCollection& GetCollection() const noexcept;

    Y_FORCE_INLINE TCacheCollectionRefImpl Acquire() const& noexcept {
        if (!Ref_) {
            return {};
        }
        TCacheImpl<TTraits>& cache = TCacheImpl<TTraits>::Get();
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

    Y_FORCE_INLINE explicit TCacheCollectionRefImpl(TCacheItemRefImpl<TTraits>&& ref) noexcept
        : Ref_(std::move(ref))
    {
        Y_DEBUG_ABORT_UNLESS(Ref_);
    }

private:
    TCacheItemRefImpl<TTraits> Ref_;
};

// Inline implementations

template <class TTraits>
Y_FORCE_INLINE TCacheItemRefImpl<TTraits>& TCacheItemRefImpl<TTraits>::operator=(TCacheItemRefImpl&& other) noexcept {
    if (this != &other) {
        Drop();
        CacheItem_ = other.Take();
    }
    return *this;
}

template <class TTraits>
Y_FORCE_INLINE void TCacheItemRefImpl<TTraits>::Drop() noexcept {
    const TCacheItem item = Take();
    if (!item.IsNull()) {
        Release(item);
    }
}

template <class TTraits>
Y_FORCE_INLINE void TCacheItemRefImpl<TTraits>::Drop(TSpaceOperation& spaceOp) noexcept {
    const TCacheItem item = Take();
    if (!item.IsNull()) {
        Release(spaceOp, item);
    }
}

template <class TTraits>
Y_FORCE_INLINE TCachePageRefImpl<TTraits>& TCachePageRefImpl<TTraits>::operator=(TCachePageRefImpl&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
    }
    return *this;
}

template <class TTraits>
template <class TCallback>
Y_FORCE_INLINE decltype(auto) TCachePageRefImpl<TTraits>::WithHandle(TCallback&& callback) const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    TCacheImpl<TTraits>& cache = TCacheImpl<TTraits>::Get();
    const TCacheItem item = Ref_.CacheItem();
    const THandle& handle = cache.Space_->OwnedHandle(item);
    Y_DEBUG_ABORT_UNLESS([&] {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        return item.Matches(state) && state.IsPageKind() && state.Refs() != 0;
    }());
    return std::forward<TCallback>(callback)(handle);
}

template <class TTraits>
Y_FORCE_INLINE TCacheCollectionRefImpl<TTraits>& TCacheCollectionRefImpl<TTraits>::operator=(
    TCacheCollectionRefImpl<TTraits>&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
    }
    return *this;
}

template <class TTraits>
Y_FORCE_INLINE TCollection& TCacheCollectionRefImpl<TTraits>::GetCollection() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    TCacheImpl<TTraits>& cache = TCacheImpl<TTraits>::Get();
    const THandle& handle = cache.Space_->OwnedHandle(Ref_.CacheItem());
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(Ref_.CacheItem().Matches(state) && state.IsCollectionKind() && state.Refs() != 0);
    return *handle.Body.Collection;
}

template <class TTraits>
Y_FORCE_INLINE bool TCacheCollectionRefImpl<TTraits>::IsSoleReference() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    auto& cache = TCacheImpl<TTraits>::Get();
    const THandleState state =
        THandleState::FromRaw(cache.Space_->OwnedHandle(Ref_.CacheItem()).State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(Ref_.CacheItem().Matches(state) && state.IsCollectionKind() && state.Refs() != 0);
    return state.Refs() == 1;
}

} // namespace NKikimr::NSharedCache
