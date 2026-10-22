#include "shared_cache.h"
#include "shared_cache_collection.h"
#include "shared_sausagecache_state.h"

namespace NKikimr::NSharedCache {

TCollection::TCollection(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TCollectionCacheItem cacheItem) noexcept
    : CacheItem(cacheItem)
    , PageCollection_(std::move(pageCollection))
{
    Y_DEBUG_ABORT_UNLESS(PageCollection_);
}

TCollection::~TCollection() {
    ResetActorState();
}

void TCollection::SetSkipBTreeIndexV1Shadow(bool skip) {
    auto& cache = TSharedCache::Get();
    cache.Collections_.SetSkipBTreeIndexV1Shadow(*this, skip);
}

void TCollection::DrainStickyPages() {
    auto& cache = TSharedCache::Get();
    auto cacheOp = cache.BeginOperation();
    cacheOp.Collections().DrainStickyPages(*this);
}

TCollectionActorState& TCollection::EnsureActorState() {
    auto* state = GetActorState();
    if (!state) {
        state = new TCollectionActorState;
        ActorState_.store(state, std::memory_order_release);
    }
    return *state;
}

void TCollection::ResetActorState() noexcept {
    delete ActorState_.exchange(nullptr, std::memory_order_acq_rel);
}

} // namespace NKikimr::NSharedCache
