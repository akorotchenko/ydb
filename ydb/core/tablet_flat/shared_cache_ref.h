#pragma once

#include "shared_cache_fwd.h"
#include "shared_cache_item.h"

#include <util/generic/noncopyable.h>

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
    TSharedCacheItemRefImpl(TSharedCacheItemRefImpl&& other) noexcept;
    TSharedCacheItemRefImpl& operator=(TSharedCacheItemRefImpl&& other) noexcept;
    ~TSharedCacheItemRefImpl();

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

    void Drop() noexcept;

private:
    friend class TSharedCacheTableImpl<TTraits>;
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheItems<TTraits>;
    friend class TSharedPageRefImpl<TTraits>;
    friend class TSharedCacheCollectionRefImpl<TTraits>;
    friend class TOperationItemRef<TTraits>;

    explicit TSharedCacheItemRefImpl(TCacheItem cacheItem) noexcept;

    TCacheItem Take() noexcept;
    void Disarm() noexcept;
    void Drop(TSpaceOperation& spaceOp) noexcept;

private:
    TCacheItem CacheItem_;
};

template <class TTraits>
class TOperationItemRef {
public:
    explicit TOperationItemRef(TSpaceOperation& spaceOp) noexcept
        : SpaceOp_(spaceOp)
    {}

    TOperationItemRef(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept
        : SpaceOp_(spaceOp)
        , Ref_(cacheItem)
    {}

    TOperationItemRef(TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) noexcept
        : SpaceOp_(spaceOp)
        , Ref_(std::move(ref))
    {}

    TOperationItemRef(const TOperationItemRef&) = delete;
    TOperationItemRef& operator=(const TOperationItemRef&) = delete;
    TOperationItemRef(TOperationItemRef&&) = delete;
    TOperationItemRef& operator=(TOperationItemRef&&) = delete;

    ~TOperationItemRef() {
        Ref_.Drop(SpaceOp_);
    }

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    TCacheItem CacheItem() const noexcept {
        return Ref_.CacheItem();
    }

    TSharedCacheItemRefImpl<TTraits>& Get() noexcept {
        return Ref_;
    }

    TSharedCacheItemRefImpl<TTraits> Take() noexcept {
        return std::move(Ref_);
    }

    void Drop() noexcept {
        Ref_.Drop(SpaceOp_);
    }

    TSpaceOperation& SpaceOperation() const noexcept {
        return SpaceOp_;
    }

    void Disarm() noexcept {
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
    TSharedPageRefImpl(TSharedPageRefImpl&& other) noexcept;
    TSharedPageRefImpl& operator=(TSharedPageRefImpl&& other) noexcept;

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    TPageCacheItem CacheItem() const noexcept;
    const char* data() const noexcept;
    size_t size() const noexcept;
    NActors::TSharedData BuildSharedData() const noexcept;
    // Construct an independent owner only when moving or borrowing cannot preserve the required lifetime.
    TSharedPageRefImpl Acquire() const& noexcept;
    NTable::NPage::TPageOffset GetOffset() const noexcept;
    NTable::NPage::TPageLocation GetLocation() const noexcept;
    TLogoBlobID GetCollectionId() const noexcept;
    NTable::NPage::EPage GetType() const noexcept;
    bool IsSticky() const noexcept;
    void Drop() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;

    explicit TSharedPageRefImpl(TSharedCacheItemRefImpl<TTraits>&& ref) noexcept;

    template <class TCallback>
    decltype(auto) WithHandle(TCallback&& callback) const noexcept;

private:
    TSharedCacheItemRefImpl<TTraits> Ref_;
};

template <class TTraits = TProdTraits>
class TSharedCacheCollectionRefImpl : private TMoveOnly {
public:
    TSharedCacheCollectionRefImpl() noexcept = default;
    TSharedCacheCollectionRefImpl(TSharedCacheCollectionRefImpl&& other) noexcept;
    TSharedCacheCollectionRefImpl& operator=(TSharedCacheCollectionRefImpl&& other) noexcept;

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    TCollectionCacheItem CacheItem() const noexcept;
    TCollection& GetCollection() const noexcept;
    TSharedCacheCollectionRefImpl Acquire() const& noexcept;
    bool IsSoleReference() const noexcept;

    TCollection* Get() const noexcept {
        return Ref_ ? &GetCollection() : nullptr;
    }

    TCollection* operator->() const noexcept {
        return &GetCollection();
    }

    TCollection& operator*() const noexcept {
        return GetCollection();
    }

    void Drop() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;

    explicit TSharedCacheCollectionRefImpl(TSharedCacheItemRefImpl<TTraits>&& ref) noexcept;

private:
    TSharedCacheItemRefImpl<TTraits> Ref_;
};

} // namespace NKikimr::NSharedCache
