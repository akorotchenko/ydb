#pragma once

#include "shared_cache_items.h"
#include "shared_cache_space.h"

namespace NKikimr::NSharedCache {

template <class TTraits>
TSharedPageRefImpl<TTraits> TCacheItems<TTraits>::BuildPageRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
    Y_DEBUG_ABORT_UNLESS(ref);
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && state.IsPageKind() &&
                         (state.IsReady() || state.IsReplacing() || state.IsReplaced() || state.IsTombstone()) &&
                         state.Refs() > 0 && handle.Body.PageBuffer.data());
    return TSharedPageRefImpl<TTraits>(std::move(ref));
}

template <class TTraits>
TSharedCacheCollectionRefImpl<TTraits> TCacheItems<TTraits>::BuildCollectionRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    Y_DEBUG_ABORT_UNLESS([&] {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        return cacheItem.Matches(state) && state.IsCollectionKind() &&
               (state.IsSticky() || state.IsHot() || state.IsCold() || state.IsTombstone()) && state.Refs() > 0 &&
               handle.Body.Collection;
    }());
    return TSharedCacheCollectionRefImpl<TTraits>(std::move(ref));
}

} // namespace NKikimr::NSharedCache
