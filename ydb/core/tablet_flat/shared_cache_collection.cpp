#include "shared_cache_collection.h"
#include "shared_cache_collection_state.h"

namespace NKikimr::NSharedCache {

TCacheCollection::TCacheCollection(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TCollectionCacheItem cacheItem) noexcept
    : CacheItem(cacheItem)
    , PageCollection_(std::move(pageCollection))
{
    Y_DEBUG_ABORT_UNLESS(PageCollection_);
}

TCacheCollection::~TCacheCollection() {
    ResetActorState();
}

TCollectionActorState& TCacheCollection::EnsureActorState() {
    auto* state = GetActorState();
    if (!state) {
        state = new TCollectionActorState;
        ActorState_.store(state, std::memory_order_release);
    }
    return *state;
}

void TCacheCollection::ResetActorState() noexcept {
    delete ActorState_.exchange(nullptr, std::memory_order_acq_rel);
}

} // namespace NKikimr::NSharedCache
