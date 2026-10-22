#include "shared_cache.h"
#include "shared_cache_items_ref_impl.h"
#include "shared_cache_events.h"
#include "shared_cache_traits.h"

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/cast.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/system/spinlock.h>

#include <type_traits>
#include <utility>

namespace NKikimr::NSharedCache {
namespace {

    // usage + bytes exceeds limit
    inline bool ExceedsLimit(ui64 usage, ui64 limit, ui64 bytes) noexcept {
        return usage + bytes > limit;
    }

} // anonymous namespace

void* TProdTraits::TryGetCache() noexcept {
    if (HasAppData()) {
        const TIntrusivePtr<TSharedCache>& owner = AppData()->SharedCache;
        return owner.Get();
    }
    return nullptr;
}

bool TProdTraits::InstallCache(void* cache) noexcept {
    if (!HasAppData()) {
        return false;
    }
    auto& owner = AppData()->SharedCache;
    auto* core = static_cast<TSharedCache*>(cache);
    if (owner && owner.Get() != core) {
        return false;
    }
    owner = core;
    return true;
}

void TProdTraits::NotifyKeepPageEviction(const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem,
    ui64 generation, NTable::NPage::TPageLocation location) const noexcept {
    if (KeepEvictionActorSystem && KeepEvictionActor) {
        KeepEvictionActorSystem->Send(
            KeepEvictionActor, new TEvKeepPageEvicted(collectionId, cacheItem, generation, std::move(location)), 0, 0);
    }
}

void TProdTraits::NotifyCollectionReleased(
    const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem) const noexcept {
    if (KeepEvictionActorSystem && KeepEvictionActor) {
        KeepEvictionActorSystem->Send(KeepEvictionActor, new TEvCollectionReleased(collectionId, cacheItem), 0, 0);
    }
}

void TProdTraits::NotifyCollectionBytesChanged(
    const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem) const noexcept {
    if (KeepEvictionActorSystem && KeepEvictionActor) {
        KeepEvictionActorSystem->Send(KeepEvictionActor, new TEvCollectionBytesChanged(collectionId, cacheItem), 0, 0);
    }
}

void TProdTraits::NotifyResourcesAvailable() const noexcept {
    if (KeepEvictionActorSystem && KeepEvictionActor) {
        KeepEvictionActorSystem->Send(KeepEvictionActor, new TEvResourcesAvailable, 0, 0);
    }
}

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TSharedCacheItemRef TSharedCacheItemRefImpl<TTraits>
#define TSharedCacheCollectionRef TSharedCacheCollectionRefImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
TSharedCache::TSharedCacheImpl(THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits) noexcept
    : TTraits(std::move(traits))
    , Space_(std::move(space))
    , Table_(*this)
    , Memory_(Space_->CurrentConfiguration().StaticBytes, hardLimit, Space_->ReservedConfiguration().Limit)
    , Items_(*this)
    , Collections_(*this)
    , Eviction_(*this)
    , Capacity_(*this)
    , Fetches_(*this)
{
    if constexpr (std::is_same_v<TTraits, TTestTraits>) {
        for (TSpaceHazard* hazard : Space_->Hazards_) {
            hazard->TestCache = this;
        }
    }
    RefreshSoftLimit();
}

SHARED_CACHE_TEMPLATE
TIntrusivePtr<TSharedCache> TSharedCache::Create(THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits)
{
    if (!space || hardLimit == 0 || hardLimit > space->ReservedConfiguration().Limit) {
        return {};
    }
    Y_DEBUG_ABORT_UNLESS(Policy_.CurrentLimitGap >= 0.0 && Policy_.CurrentLimitGap < 1.0 && Policy_.ColdMin >= 0.0 &&
                         Policy_.ColdMin < Policy_.Grow && Policy_.Grow <= 1.0 && Policy_.HotMin > 0.0 &&
                         Policy_.HotMin < 1.0 && Policy_.HotMinUnderPressure > 0.0 &&
                         Policy_.HotMinUnderPressure <= Policy_.HotMin && Policy_.ResizeStep > 0.0 &&
                         Policy_.ResizeStep <= 1.0 && Policy_.MinHotSlots > 0 &&
                         Policy_.MinHotSlotsUnderPressure <= Policy_.MinHotSlots && Policy_.MinResizeStep > 0 &&
                         Policy_.MinHotSlots <= space->CurrentConfiguration().HotSlotCount() &&
                         SharedCacheHotLayoutIsValid(Policy_.MinHotSlots) &&
                         SharedCacheHotLayoutIsValid(Policy_.MinHotSlotsUnderPressure));
    TIntrusivePtr<TSharedCache> cache(new TSharedCache(std::move(space), hardLimit, std::move(traits)));
    if (!TTraits::InstallCache(cache.Get())) {
        return {};
    }
    return cache;
}

SHARED_CACHE_TEMPLATE
TSharedPageRefImpl<TTraits>::TSharedPageRefImpl(TSharedCacheItemRef&& ref) noexcept
    : Ref_(std::move(ref))
{
    Y_DEBUG_ABORT_UNLESS(Ref_);
}

SHARED_CACHE_TEMPLATE
TSharedPageRefImpl<TTraits>::TSharedPageRefImpl(TSharedPageRefImpl&& other) noexcept
    : Ref_(std::move(other.Ref_))
{}

SHARED_CACHE_TEMPLATE
TSharedPageRefImpl<TTraits>& TSharedPageRefImpl<TTraits>::operator=(TSharedPageRefImpl&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TPageCacheItem TSharedPageRefImpl<TTraits>::CacheItem() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    return TPageCacheItem::FromValidated(Ref_.CacheItem_);
}

SHARED_CACHE_TEMPLATE
template <class TCallback>
decltype(auto) TSharedPageRefImpl<TTraits>::WithHandle(TCallback&& callback) const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    TSharedCache& cache = TSharedCache::Get();
    auto spaceOp = cache.BeginOperation();
    const TCacheItem item = Ref_.CacheItem();
    const THandle& handle = spaceOp.Handles()[item.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(item.Matches(state) && state.IsPageKind() && state.Refs() != 0);
    return std::forward<TCallback>(callback)(handle);
}

SHARED_CACHE_TEMPLATE
const char* TSharedPageRefImpl<TTraits>::data() const noexcept {
    return WithHandle([](const THandle& handle) {
        return handle.Body.PageBuffer.data();
    });
}

SHARED_CACHE_TEMPLATE
size_t TSharedPageRefImpl<TTraits>::size() const noexcept {
    if (!Ref_) {
        return 0;
    }
    return WithHandle([](const THandle& handle) {
        return handle.Key2OrSize.load(std::memory_order_relaxed);
    });
}

SHARED_CACHE_TEMPLATE
NActors::TSharedData TSharedPageRefImpl<TTraits>::BuildSharedData() const noexcept {
    return WithHandle([](const THandle& handle) {
        return handle.Body.PageBuffer.Share(handle.Key2OrSize.load(std::memory_order_relaxed));
    });
}

SHARED_CACHE_TEMPLATE
TSharedPageRefImpl<TTraits> TSharedPageRefImpl<TTraits>::Acquire() const& noexcept {
    if (!Ref_) {
        return {};
    }
    TSharedCache& cache = TSharedCache::Get();
    return cache.AcquireMorePageRef(Ref_.CacheItem());
}

SHARED_CACHE_TEMPLATE
NTable::NPage::TPageOffset TSharedPageRefImpl<TTraits>::GetOffset() const noexcept {
    return WithHandle([](const THandle& handle) {
        return NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed));
    });
}

SHARED_CACHE_TEMPLATE
NTable::NPage::TPageLocation TSharedPageRefImpl<TTraits>::GetLocation() const noexcept {
    return WithHandle([](const THandle& handle) {
        return NTable::NPage::TPageLocation(
            NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
            handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type, handle.Metadata.Page.Crc32);
    });
}

SHARED_CACHE_TEMPLATE
NTable::NPage::EPage TSharedPageRefImpl<TTraits>::GetType() const noexcept {
    return WithHandle([](const THandle& handle) {
        return handle.Metadata.Page.Type;
    });
}

SHARED_CACHE_TEMPLATE
TLogoBlobID TSharedPageRefImpl<TTraits>::GetCollectionId() const noexcept {
    TSharedCache& cache = TSharedCache::Get();
    auto cacheOp = cache.BeginOperation();
    const THandle& page = cacheOp.Handles()[CacheItem().Index()];
    return cacheOp.Collections().PageCollection(page)->Id();
}

SHARED_CACHE_TEMPLATE
bool TSharedPageRefImpl<TTraits>::IsSticky() const noexcept {
    if (!Ref_) {
        return false;
    }
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    return cache.IsPageSticky(CacheItem());
}

SHARED_CACHE_TEMPLATE
void TSharedPageRefImpl<TTraits>::Drop() noexcept {
    Ref_.Drop();
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef::TSharedCacheCollectionRefImpl(TSharedCacheItemRef&& ref) noexcept
    : Ref_(std::move(ref))
{
    Y_DEBUG_ABORT_UNLESS(Ref_);
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef::TSharedCacheCollectionRefImpl(TSharedCacheCollectionRef&& other) noexcept
    : Ref_(std::move(other.Ref_))
{}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef& TSharedCacheCollectionRef::operator=(TSharedCacheCollectionRef&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TCollectionCacheItem TSharedCacheCollectionRef::CacheItem() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    return TCollectionCacheItem::FromValidated(Ref_.CacheItem_);
}

SHARED_CACHE_TEMPLATE
TCollection& TSharedCacheCollectionRef::GetCollection() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    TSharedCache& cache = TSharedCache::Get();
    auto spaceOp = cache.BeginOperation();
    const THandle& handle = spaceOp.Handles()[Ref_.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(Ref_.CacheItem().Matches(state) && state.IsCollectionKind() && state.Refs() != 0);
    return *handle.Body.Collection;
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef TSharedCacheCollectionRef::Acquire() const& noexcept {
    if (!Ref_) {
        return {};
    }
    TSharedCache& cache = TSharedCache::Get();
    return cache.AcquireMoreCollectionRef(Ref_.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheCollectionRef::IsSoleReference() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    auto& cache = TSharedCache::Get();
    auto spaceOp = cache.BeginOperation();
    const THandleState state =
        THandleState::FromRaw(spaceOp.Handles()[Ref_.Index()].State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(Ref_.CacheItem().Matches(state) && state.IsCollectionKind() && state.Refs() != 0);
    return state.Refs() == 1;
}

SHARED_CACHE_TEMPLATE
void TSharedCacheCollectionRef::Drop() noexcept {
    Ref_.Drop();
}

SHARED_CACHE_TEMPLATE
TSharedCache::~TSharedCacheImpl() {
    const TSpaceView& space = Space_->SpaceView(Space_->CurrentSpaceState());
    // Release every page payload and pending fetch before destroying collection metadata.
    for (ui64 index = 2; index < space.HandleCount; ++index) {
        THandle& handle = space.Handles[index];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        if (!state.IsPageKind()) {
            continue;
        }
        if (state.IsReplacing() || state.IsReplaced()) {
            DeleteReplacedPage(handle);
        } else if (state.IsPending() || state.IsCompleting()) {
            // The actor system may already be gone; final cleanup cannot deliver completions.
            if (TPageFetchState* fetch = handle.Body.Fetch) {
                fetch->AbandonWaiters();
            }
            TCacheFetches<TTraits>::DeletePendingFetch(handle);
        } else if (state.IsReady() || state.IsTombstone()) {
            Items_.DeletePayload(handle, state.Kind(), state.State());
        }
    }
    for (ui64 index = 2; index < space.HandleCount; ++index) {
        THandle& handle = space.Handles[index];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        if (state.IsCollectionKind() && (state.IsReady() || state.IsTombstone())) {
            Items_.DeletePayload(handle, state.Kind(), state.State());
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DeleteReplacedPage(THandle& handle) noexcept {
    Y_DEBUG_ABORT_UNLESS(handle.Body.PageBuffer);
    handle.Body.PageBuffer.~TBuffer();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MatchesCandidateMetadata(
    const TSpaceOperation& spaceOp, TCacheItem candidate, TCacheItem existing) const noexcept {
    const THandle& candidateHandle = spaceOp.Handles()[candidate.Index()];
    const THandle& existingHandle = spaceOp.Handles()[existing.Index()];
    const THandleState candidateState = THandleState::FromRaw(candidateHandle.State.load(std::memory_order_relaxed));
    const THandleState existingState = THandleState::FromRaw(existingHandle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(candidate.Matches(candidateState));
    Y_DEBUG_ABORT_UNLESS(existing.Matches(existingState));
    Y_DEBUG_ABORT_UNLESS(candidateState.Kind() == existingState.Kind());

    return !candidateState.IsPageKind() ||
           (candidateHandle.Key2OrSize.load(std::memory_order_relaxed) ==
                   existingHandle.Key2OrSize.load(std::memory_order_relaxed) &&
               candidateHandle.Metadata.Page.Type == existingHandle.Metadata.Page.Type &&
               candidateHandle.Metadata.Page.Crc32 == existingHandle.Metadata.Page.Crc32);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MatchesCandidateMetadata(
    const TSpaceOperation& spaceOp, const TPageInsertCandidate& candidate, TCacheItem existing) const noexcept {
    const THandle& handle = spaceOp.Handles()[existing.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(existing.Matches(state) && state.IsPageKind());
    return handle.Key2OrSize.load(std::memory_order_relaxed) == candidate.Size &&
           handle.Metadata.Page.Type == candidate.Type && handle.Metadata.Page.Crc32 == candidate.Crc32;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MatchesCandidateMetadata(
    const TSpaceOperation& spaceOp, const TCollectionInsertCandidate& candidate, TCacheItem existing) const noexcept {
    const THandle& handle = spaceOp.Handles()[existing.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(existing.Matches(state) && state.IsCollectionKind());
    return handle.Metadata.PayloadBytes == candidate.Bytes;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::UpdateColdReclaimable(ui64 payloadBytes, THandleState before, THandleState after) noexcept {
    if (Memory_.UpdateColdReclaimable(payloadBytes, before, after)) {
        NotifyResourcesAvailable();
    }
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::AdvanceColdMembership(THandle& handle) noexcept {
    ui32 expected = handle.ColdVersion.load(std::memory_order_relaxed);
    for (;;) {
        const ui32 desired = AdvanceColdVersion(expected & MaxItemVersion);
        if (handle.ColdVersion.compare_exchange_weak(
                expected, desired, std::memory_order_release, std::memory_order_relaxed))
        {
            return desired;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AdvanceColdMembership(
    THandle& handle, ui32 expectedVersion, bool keepColdQueued, ui32& newVersion) noexcept {
    ui32 expected = handle.ColdVersion.load(std::memory_order_relaxed);
    for (;;) {
        if ((expected & MaxItemVersion) != (expectedVersion & MaxItemVersion)) {
            return false;
        }
        const ui32 advanced = AdvanceColdVersion(expected & MaxItemVersion);
        const ui32 desired = keepColdQueued ? advanced | KeepColdQueuedMask : advanced;
        if (handle.ColdVersion.compare_exchange_weak(
                expected, desired, std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            newVersion = desired & MaxItemVersion;
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
TPageCacheItem TSharedCache::AllocatePage(TCollectionCacheItem collection, ui64 offset, ui64 size,
    NTable::NPage::EPage type, ui32 crc32, EStickyState sticky) noexcept {
    Y_DEBUG_ABORT_UNLESS(type != NTable::NPage::EPage::Undef);
    Y_DEBUG_ABORT_UNLESS(sticky == EStickyState::None || sticky == EStickyState::Sticky);
    const ui64 bytes = TCacheItems<TTraits>::AccountedPageBytes(size);
    auto cacheOp = BeginOperation();
    const TCacheItem cacheItem = cacheOp.TryAllocateReservedItem(bytes, 1);
    if (!cacheItem.IsNull()) {
        TCollection* owner = cacheOp.Collections().PageCollection(collection);
        const ECacheMode admissionMode =
            owner ? owner->CacheMode_.load(std::memory_order_acquire) : ECacheMode::Regular;
        const EStickyState pageSticky = admissionMode == ECacheMode::TryKeepInMemory ? EStickyState::None : sticky;
        cacheOp.InitializePage(cacheItem, collection, offset, size, type, crc32, pageSticky);
        cacheOp.RefillSpareItem();
    }
    return TPageCacheItem::FromValidated(cacheItem);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(TPageCacheItem page, NActors::TSharedData&& data) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef owner(cacheOp);
    TIntrusivePtr<TPageFetchState> fetch = cacheOp.Fetches().TryAcquire(page, owner);
    if (!fetch) {
        return false;
    }
    return cacheOp.Fetches().Complete(owner, *fetch, std::move(data));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FailReady(TPageCacheItem page) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef owner(cacheOp);
    TIntrusivePtr<TPageFetchState> fetch = cacheOp.Fetches().TryAcquire(page, owner);
    if (!fetch) {
        return false;
    }
    return cacheOp.Fetches().Fail(owner, *fetch);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
    return MakeReady(Collections_.Registry(), collection, std::move(value));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(
    TCollectionRegistry& registry, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
    return MakeReady(&registry, collection, std::move(value));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(
    TCollectionRegistry* registry, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.MakeReady(registry, collection.CacheItem(), std::move(value));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DetachCollection(TCollectionCacheItem collection) noexcept {
    return DetachCollection(Collections_.Registry(), collection);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DetachCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef collectionRef(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    return cacheOp.Collections().Detach(registry, collection.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DeleteCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
    return DeleteCollection(&registry, collection);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DeleteCollection(TCollectionCacheItem collection) noexcept {
    return DeleteCollection(nullptr, collection);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DeleteCollection(TCollectionRegistry* registry, TCollectionCacheItem collection) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef collectionRef(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    TCollection* value = cacheOp.Collections().PageCollection(collection);
    if (!value || !cacheOp.Collections().SetPagesCacheMode(*value, ECacheMode::Regular)) {
        return false;
    }
    return cacheOp.Collections().Delete(registry, collection.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::SetCollectionPagesCacheMode(TCollectionCacheItem collection, ECacheMode mode) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef collectionRef(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    THandle& handle = cacheOp.Handles()[collection.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collection.Matches(state) || !handle.Body.Collection) {
        return false;
    }
    return cacheOp.Collections().SetPagesCacheMode(*handle.Body.Collection, mode);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::SetCollectionSkipBTreeIndexV1Shadow(TCollectionCacheItem collection, bool skip) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef ref(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (!ref) {
        return false;
    }
    TCollection* owner = cacheOp.Collections().PageCollection(collection);
    if (!owner) {
        return false;
    }
    Collections_.SetSkipBTreeIndexV1Shadow(*owner, skip);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AcknowledgeCollectionBytes(TCollectionCacheItem collection) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef ref(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (auto* owner = ref ? cacheOp.Collections().PageCollection(collection) : nullptr) {
        Collections_.AcknowledgeBytes(*owner);
        return true;
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryReserveFetchBatch(TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.Fetches().TryReserveBatch(pages);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::CanAdmitWorkingSet(ui64 bytes) noexcept {
    auto cacheOp = BeginOperation();
    if (!cacheOp.TryReserve(bytes)) {
        return false;
    }
    // A readiness check, not an allocation owned by the transaction. Its subsequent admissions
    // still use TryReserve and may wait again if another requester consumes the released room.
    ReleaseReservation(bytes);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::CanAdmitPageHandles(TArrayRef<const TSharedCacheHandleRequirement> requirements) noexcept {
    auto cacheOp = BeginOperation();
    TVector<TCacheItem> allocated;
    Y_DEFER {
        for (const TCacheItem item : allocated) {
            const THandleState state =
                THandleState::FromRaw(cacheOp.Handles()[item.Index()].State.load(std::memory_order_relaxed));
            cacheOp.DiscardCandidate(item, state.Kind(), 0);
        }
    };
    const auto countMissing = [&](ui64& count) {
        count = 0;
        THashMap<TLogoBlobID, THashSet<ui64>> seen;
        for (const auto& requirement : requirements) {
            TCacheItem item;
            TCollectionCacheItem collection;
            if (cacheOp.Find(TSharedCacheKey::Collection(requirement.CollectionId), item) !=
                ESharedCacheResultStatus::Miss) {
                const THandleState state =
                    THandleState::FromRaw(cacheOp.Handles()[item.Index()].State.load(std::memory_order_acquire));
                if (!item.Matches(state) || !state.IsCollectionKind() || !state.IsReady()) {
                    return false;
                }
                collection = TCollectionCacheItem::FromValidated(item);
            }
            auto [it, first] = seen.try_emplace(requirement.CollectionId);
            count += first && !collection;
            for (const auto& location : requirement.Pages) {
                const ui64 offset = static_cast<ui64>(location.Offset);
                if (!it->second.insert(offset).second) {
                    continue;
                }
                TCacheItem present;
                count += !collection || cacheOp.Find(TSharedCacheKey::Page(collection, offset), present) ==
                                            ESharedCacheResultStatus::Miss;
            }
        }
        return true;
    };
    for (;;) {
        ui64 missing = 0;
        if (!countMissing(missing)) {
            return false;
        }
        if (missing <= allocated.size()) {
            return true;
        }
        while (allocated.size() < missing) {
            const TCacheItem item = cacheOp.Allocate();
            if (item.IsNull()) {
                return false;
            }
            allocated.push_back(item);
        }
        // Cooperative reclamation may have evicted another required page; include it before reporting readiness.
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::WorkingSetExceedsCapacity(TArrayRef<const TCollectionCacheItem> retainedCollections,
    TArrayRef<const TSharedCacheHandleRequirement> requirements) noexcept {
    auto cacheOp = BeginOperation();
    const ui64 handleLimit = TargetHandleCount() - 2;
    const ui64 byteLimit = CurrentLimit();
    THashSet<ui64> collections;
    for (const auto collection : retainedCollections) {
        if (!cacheOp.Contains(collection.CacheItem())) {
            continue;
        }
        const auto state =
            THandleState::FromRaw(cacheOp.Handles()[collection.Index()].State.load(std::memory_order_acquire));
        if (collection.Matches(state) && state.IsCollectionKind() && !state.IsFree() && state.Refs() > 0) {
            collections.insert(collection.Raw());
        }
    }
    ui64 handles = collections.size();
    ui64 bytes = 0;
    bool exceeds = handles > handleLimit;
    THashMap<TLogoBlobID, THashSet<ui64>> pages;
    for (const auto& requirement : requirements) {
        auto [it, first] = pages.try_emplace(requirement.CollectionId);
        if (first) {
            TCacheItem current;
            if (cacheOp.Find(TSharedCacheKey::Collection(requirement.CollectionId), current) ==
                    ESharedCacheResultStatus::Miss ||
                !collections.contains(current.Raw())) {
                exceeds |= ++handles > handleLimit;
            }
        }
        for (const auto& location : requirement.Pages) {
            if (!it->second.insert(static_cast<ui64>(location.Offset)).second) {
                continue;
            }
            exceeds |= ++handles > handleLimit;
            const ui64 charge = TCacheItems<TTraits>::AccountedPageBytes(location.Size);
            if (charge > byteLimit - bytes) {
                exceeds = true;
            } else {
                bytes += charge;
            }
        }
    }
    return exceeds && handleLimit == TargetHandleCount() - 2 && byteLimit == CurrentLimit();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(TSpaceOperation& spaceOp, TCollectionRegistry* registry, TCacheItem cacheItem,
    THolder<TCollection>&& value) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(value);
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AccessibleHandleCount());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && state.IsCollectionKind() && state.IsBegin());
    Y_DEBUG_ABORT_UNLESS(LoadSharedCacheKey(handle, EItemKind::Collection) == TSharedCacheKey::Collection(value->Id()));
    Y_DEBUG_ABORT_UNLESS(handle.Metadata.PayloadBytes == value->AccountedBytes());

    const ui64 bytes = handle.Metadata.PayloadBytes;
    Memory_.AddCollection(bytes);
    handle.Body.Collection = value.Get();
    value->CacheItem = TCollectionCacheItem::FromValidated(cacheItem);
    Collections_.AddReference(*value);
    if (registry) {
        Y_ABORT_UNLESS(cacheOp.Collections().Link(*registry, cacheItem));
    } else {
        Y_DEBUG_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_relaxed) == 0 &&
                             !handle.Body.Collection->Registry_.load(std::memory_order_relaxed));
    }
    cacheOp.MakeReadyState(cacheItem);
    Y_UNUSED(value.Release());
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::MakeReadyState(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, EHandleState readyState, TCollection* pageOwner) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const ui32 index = cacheItem.Index();
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AccessibleHandleCount());

    THandle& handle = spaceOp.Handles()[index];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    const THandleState initial = THandleState::FromRaw(expectedRaw);
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(initial) && (initial.IsPending() || initial.IsCompleting()));

    const ui64 bytes = TCacheItems<TTraits>::PayloadBytes(handle, initial.Kind());
    const i64 estimatedBytes = static_cast<i64>(bytes);
    EStickyState accountedSticky = initial.Sticky();
    Y_DEBUG_ABORT_UNLESS(accountedSticky == EStickyState::None || accountedSticky == EStickyState::Sticky);
    if (initial.IsStickyField() && readyState == EHandleState::Cold) {
        Memory_.AddRetainedBytes(bytes);
        readyState = EHandleState::Hot;
    }
    // Withdrawal may have happened after the caller charged retention but before this state was read.
    if (!initial.IsStickyField() && readyState != EHandleState::Cold && pageOwner &&
        pageOwner->CacheMode_.load(std::memory_order_acquire) == ECacheMode::Regular && RetainedBytes() > SoftLimit()) {
        Memory_.ReleaseRetainedBytes(bytes);
        readyState = EHandleState::Cold;
    }
    Memory_.AddResident(bytes);
    if (readyState == EHandleState::Cold) {
        Memory_.Cold().AddBytes(estimatedBytes);
        Memory_.Cold().AddPage(initial.Kind());
        Memory_.AddColdItem();
    } else if (accountedSticky == EStickyState::Sticky) {
        Memory_.Sticky().AddBytes(estimatedBytes);
        Memory_.Sticky().AddPage(initial.Kind());
    } else {
        Memory_.Hot().AddBytes(estimatedBytes);
        Memory_.Hot().AddPage(initial.Kind());
    }
    if (pageOwner && readyState != EHandleState::Cold) {
        Collections_.AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
    }
    Memory_.CompleteReservedBytes(bytes);
    InvokeHook(ESharedCacheHookPoint::BeforeReadyStatePublication, cacheItem);

    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) && (expected.IsPending() || expected.IsCompleting()));
        Y_DEBUG_ABORT_UNLESS(expected.IsStickyNoneField() || expected.IsStickyField());
        if (expected.Sticky() != accountedSticky) {
            if (expected.IsStickyField()) {
                if (readyState == EHandleState::Cold) {
                    Memory_.AddRetainedBytes(bytes);
                    Memory_.Cold().TransferBytesTo(Memory_.Hot(), bytes);
                    Memory_.Cold().TransferPageTo(Memory_.Hot(), initial.Kind());
                    Memory_.RemoveColdItem();
                    if (pageOwner) {
                        Collections_.AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
                    }
                    readyState = EHandleState::Hot;
                }
                Memory_.Hot().TransferBytesTo(Memory_.Sticky(), bytes);
                Memory_.Hot().TransferPageTo(Memory_.Sticky(), expected.Kind());
            } else if (pageOwner && pageOwner->CacheMode_.load(std::memory_order_acquire) == ECacheMode::Regular &&
                       RetainedBytes() > SoftLimit()) {
                Memory_.ReleaseRetainedBytes(bytes);
                Memory_.Sticky().TransferBytesTo(Memory_.Cold(), bytes);
                Memory_.Sticky().TransferPageTo(Memory_.Cold(), expected.Kind());
                Memory_.AddColdItem();
                if (pageOwner) {
                    Collections_.AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
                }
                readyState = EHandleState::Cold;
            } else {
                Memory_.Sticky().TransferBytesTo(Memory_.Hot(), bytes);
                Memory_.Sticky().TransferPageTo(Memory_.Hot(), expected.Kind());
            }
            accountedSticky = expected.Sticky();
        }

        const EHandleState destination = expected.IsStickyField() ? EHandleState::Sticky : readyState;
        const THandleState desired = expected.WithState(destination).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            if (desired.IsHot()) {
                cacheOp.Eviction().RouteHot(EHotLevel::L1, TCacheItem::Make(expected.Version(), index));
            }
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
TCollectionCacheItem TSharedCache::AllocateCollection(const TLogoBlobID& id, ui64 bytes) noexcept {
    Y_DEBUG_ABORT_UNLESS(bytes == 0);
    auto cacheOp = BeginOperation();
    const TCacheItem cacheItem = cacheOp.TryAllocateReservedItem(bytes);
    if (!cacheItem.IsNull()) {
        cacheOp.Items().InitializeCollection(cacheItem, id, bytes);
        cacheOp.RefillSpareItem();
    }
    return TCollectionCacheItem::FromValidated(cacheItem);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DiscardCandidate(TPageCacheItem page) noexcept {
    DiscardCandidate(page.CacheItem(), EItemKind::Page);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DiscardCandidate(TCollectionCacheItem collection) noexcept {
    DiscardCandidate(collection.CacheItem(), EItemKind::Collection);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DiscardCandidate(TCacheItem cacheItem, EItemKind kind) noexcept {
    auto cacheOp = BeginOperation();
    const THandle& handle = cacheOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    const ui64 reservedBytes =
        state.IsPending() && !state.IsWaitingForMemory() ? TCacheItems<TTraits>::PayloadBytes(handle, kind) : 0;
    cacheOp.DiscardCandidate(cacheItem, kind, reservedBytes);
}

SHARED_CACHE_TEMPLATE
typename TSharedCache::EAcquireStatus TSharedCache::TryAcquire(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowPending, TSharedCacheItemRef& ref) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (!spaceOp.Contains(cacheItem)) {
        return EAcquireStatus::Stale;
    }

    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || expected.IsFree() || expected.IsReplacing() || expected.IsReplaced() ||
            expected.IsTombstone())
        {
            return EAcquireStatus::Stale;
        }

        if (expected.IsPending() && !allowPending) {
            return EAcquireStatus::Pending;
        }
        if (!expected.IsPending() && !expected.IsReady()) {
            return EAcquireStatus::Stale;
        }

        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        THandleState desired = expected.IncrementRefs();
        const bool pending = expected.IsPending();
        if (expected.IsHot()) {
            desired = desired.WithFrequency(Min<ui8>(expected.Frequency() + 1, 2));
        }

        if (expected.IsKeepCold()) {
            desired = desired.WithState(EHandleState::Hot).WithFrequency(0);
        }
        if (!handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            continue;
        }
        UpdateColdReclaimable(TCacheItems<TTraits>::PayloadBytes(handle, expected.Kind()), expected, desired);
        if (expected.IsKeepCold()) {
            AdvanceColdMembership(handle);
            const ui64 bytes = TCacheItems<TTraits>::PageBytes(handle);
            Memory_.KeepCold().TransferBytesTo(Memory_.Hot(), bytes);
            Memory_.KeepCold().TransferPageTo(Memory_.Hot(), EItemKind::Page);
            Memory_.ReleaseKeepColdOwnedBytes(bytes);
            cacheOp.Eviction().RouteHot(EHotLevel::L1, cacheItem);
        } else if (expected.IsCold() && expected.IsPageKind()) {
            if (cacheOp.Eviction().PromoteCold(cacheItem)) {
                cacheOp.Eviction().RouteHot(EHotLevel::L1, cacheItem);
            } else {
                AdvanceColdMembership(handle);
            }
        }
        ref = TSharedCacheItemRef(cacheItem);
        return pending ? EAcquireStatus::Pending : EAcquireStatus::Ready;
    }
}

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef TSharedCache::BuildExternalRef(EItemKind kind, TCacheItem cacheItem) noexcept {
    auto spaceOp = BeginOperation();
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem) && !cacheItem.IsFrozen());

    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        const bool validState =
            kind == EItemKind::Page ? expected.IsReady() : expected.IsSticky() || expected.IsHot() || expected.IsCold();
        Y_DEBUG_ABORT_UNLESS(
            cacheItem.Matches(expected) && expected.Kind() == kind && validState && expected.Refs() > 0);
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return TSharedCacheItemRef(cacheItem);
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::IsPageSticky(TPageCacheItem page) const noexcept {
    auto spaceOp = Space_->BeginOperation();
    TCacheItem item = page.CacheItem();
    for (;;) {
        if (!spaceOp.Contains(item)) {
            return false;
        }
        const THandle& handle = spaceOp.Handles()[item.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (!item.Matches(state) || !state.IsPageKind()) {
            return false;
        }
        if (state.IsReplacing() || state.IsReplaced()) {
            const TCacheItem next = TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire));
            if (!next.IsFrozen()) {
                InvokeHook(ESharedCacheHookPoint::BeforePageReplacementRetry, item);
                SpinLockPause();
                continue;
            }
            if (next.WithoutFrozen().IsNull()) {
                return false;
            }
            // The held predecessor ref keeps its replacement chain alive.
            item = next.WithoutFrozen();
            continue;
        }
        return state.IsSticky();
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AcquirePage(TPageCacheItem page, TSharedPageRefImpl<TTraits>& result) noexcept {
    result.Drop();
    TCacheItem cacheItem = page.CacheItem();
    auto cacheOp = BeginOperation();
    for (;;) {
        if (!cacheOp.Contains(cacheItem)) {
            return false;
        }

        THandle& handle = cacheOp.Handles()[cacheItem.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (!cacheItem.Matches(state) || !state.IsPageKind()) {
            return false;
        }
        if (state.IsReplacing() || state.IsReplaced()) {
            const TCacheItem replacement = TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire));
            if (!replacement.IsFrozen()) {
                InvokeHook(ESharedCacheHookPoint::BeforePageReplacementRetry, cacheItem);
                SpinLockPause();
                continue;
            }
            if (replacement.WithoutFrozen().IsNull()) {
                return false;
            }
            cacheItem = replacement.WithoutFrozen();
            continue;
        }
        if (!state.IsReady()) {
            return false;
        }

        // The completion finalizer or replacement hold keeps this generation alive until the new ref is acquired.
        TOperationItemRef ref(cacheOp, cacheOp.TryAcquireStructural(cacheItem));
        if (!ref) {
            continue;
        }
        result = cacheOp.BuildPageRef(ref.Take());
        return true;
    }
}

SHARED_CACHE_TEMPLATE
TSharedPageRefImpl<TTraits> TSharedCache::AcquireMorePageRef(TCacheItem item) noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.BuildPageRef(cacheOp.Items().AcquireMorePageRef(item));
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef TSharedCache::AcquireMoreCollectionRef(TCacheItem item) noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.BuildCollectionRef(cacheOp.Items().AcquireMoreCollectionRef(item));
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::Find(
    TCollectionCacheItem collection, ui64 offset, TSharedPageRefImpl<TTraits>& page) noexcept {
    page.Drop();
    auto cacheOp = BeginOperation();
    TOperationItemRef ref(cacheOp);
    const ESharedCacheResultStatus status = cacheOp.Find(TSharedCacheKey::Page(collection, offset), ref.Get());
    if (status == ESharedCacheResultStatus::Hit) {
        page = cacheOp.BuildPageRef(ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::Find(const TLogoBlobID& id, TSharedCacheCollectionRef& collection) noexcept {
    collection.Drop();
    auto cacheOp = BeginOperation();
    TOperationItemRef ref(cacheOp);
    const ESharedCacheResultStatus status = cacheOp.Find(TSharedCacheKey::Collection(id), ref.Get());
    if (status == ESharedCacheResultStatus::Hit) {
        collection = cacheOp.BuildCollectionRef(ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ForEachCollection(TFunctionRef<void(TCollection&)> callback) noexcept {
    auto cacheOp = BeginOperation();
    cacheOp.Collections().ForEach(callback);
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::Find(
    TOperationView& cacheOp, const TSharedCacheKey& key, TSharedCacheItemRef& ref) noexcept {
    for (;;) {
        TCacheItem cacheItem;
        const ESharedCacheResultStatus status = cacheOp.Find(key, cacheItem);
        if (status == ESharedCacheResultStatus::Miss) {
            return ESharedCacheResultStatus::Miss;
        }
        Y_DEBUG_ABORT_UNLESS(status == ESharedCacheResultStatus::Hit);
        TOperationItemRef acquired(cacheOp.SpaceOperation());
        switch (cacheOp.TryAcquire(cacheItem, false, acquired.Get())) {
            case EAcquireStatus::Stale:
                continue;
            case EAcquireStatus::Pending:
                return ESharedCacheResultStatus::Pending;
            case EAcquireStatus::Ready:
                ref = acquired.Take();
                return ESharedCacheResultStatus::Hit;
        }
        Y_UNREACHABLE();
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PrepareCandidate(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const ui64 reservedBytes = TCacheItems<TTraits>::AccountedPageBytes(candidate.Size);
    bool reserved = true;
    TCacheItem cacheItem;
    if (candidate.Admission == EPageAdmission::Written) {
        Memory_.AccountWritten(reservedBytes, 1);
        cacheItem = cacheOp.Allocate();
        if (cacheItem.IsNull()) {
            ReleaseReservation(reservedBytes, 1);
        }
    } else if (candidate.WaitForMemory) {
        reserved = cacheOp.TryReserve(reservedBytes, 1);
        cacheItem = cacheOp.Allocate();
        if (cacheItem.IsNull() && reserved) {
            ReleaseReservation(reservedBytes, 1);
        }
    } else {
        cacheItem = cacheOp.TryAllocateReservedItem(reservedBytes, 1);
    }
    if (cacheItem.IsNull()) {
        return false;
    }
    const TCollectionCacheItem collection =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(candidate.Key.Word(0)));
    TCollection* owner = cacheOp.Collections().PageCollection(collection);
    const ECacheMode admissionMode = owner ? owner->CacheMode_.load(std::memory_order_acquire) : ECacheMode::Regular;
    const bool stickyCollection =
        (candidate.WaitForMemory || candidate.CollectionOwnerHeld) && admissionMode == ECacheMode::Sticky;
    const EStickyState sticky = stickyCollection                               ? EStickyState::Sticky
                                : admissionMode == ECacheMode::TryKeepInMemory ? EStickyState::None
                                                                               : candidate.Sticky;
    cacheOp.InitializePage(cacheItem, collection, candidate.Key.Word(1), candidate.Size, candidate.Type,
        candidate.Crc32, sticky, reserved, candidate.CollectionOwnerHeld);
    candidate.CacheItem = cacheItem;
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PrepareCandidate(TSpaceOperation& spaceOp, TCollectionInsertCandidate& candidate) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(candidate.Bytes == 0);
    const TCacheItem cacheItem = cacheOp.TryAllocateReservedItem(candidate.Bytes);
    if (cacheItem.IsNull()) {
        return false;
    }
    cacheOp.Items().InitializeCollection(
        cacheItem, TLogoBlobID(candidate.Key.Word(0), candidate.Key.Word(1), candidate.Key.Word(2)), candidate.Bytes);
    candidate.CacheItem = cacheItem;
    return true;
}

SHARED_CACHE_TEMPLATE
typename TSharedCache::TPageInsertCandidate TSharedCache::BuildPageInsertCandidate(
    TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page, EStickyState sticky) noexcept {
    return {
        .Key = TSharedCacheKey::Page(collection, static_cast<ui64>(page.Offset)),
        .Size = page.Size,
        .Type = page.Type,
        .Crc32 = page.Crc32,
        .Sticky = sticky,
    };
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(TCollectionCacheItem collection,
    const NTable::NPage::TPageLocation& page, EStickyState sticky, TPageCacheItem& inserted,
    TSharedPageRefImpl<TTraits>& hit) noexcept {
    Y_DEBUG_ABORT_UNLESS(page && page.Type != NTable::NPage::EPage::Undef);
    Y_DEBUG_ABORT_UNLESS(sticky == EStickyState::None || sticky == EStickyState::Sticky);
    inserted = {};
    hit.Drop();
    auto cacheOp = BeginOperation();
    TPageInsertCandidate candidate = BuildPageInsertCandidate(collection, page, sticky);
    TOperationItemRef ref(cacheOp);
    const ESharedCacheResultStatus status = cacheOp.FindOrInsert(candidate, ref.Get());
    switch (status) {
        case ESharedCacheResultStatus::Miss:
        case ESharedCacheResultStatus::Pending:
            return status;
        case ESharedCacheResultStatus::Inserted:
            inserted = TPageCacheItem::FromValidated(ref.CacheItem());
            return status;
        case ESharedCacheResultStatus::Hit:
            hit = cacheOp.BuildPageRef(ref.Take());
            return status;
    }
    Y_UNREACHABLE();
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(TCollectionCacheItem collection,
    const NTable::NPage::TPageLocation& page, EStickyState sticky, TIntrusivePtr<TPageFetchWaiter> waiter,
    TSharedPageRefImpl<TTraits>& hit, TPageFetchImpl<TTraits>& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(page && page.Type != NTable::NPage::EPage::Undef && waiter);
    Y_DEBUG_ABORT_UNLESS(sticky == EStickyState::None || sticky == EStickyState::Sticky);
    auto cacheOp = BeginOperation();
    TPageInsertCandidate candidate = BuildPageInsertCandidate(collection, page, sticky);
    const ESharedCacheResultStatus status = cacheOp.FindOrInsertPage(candidate, std::move(waiter), hit, fetch);
    TStats stats;
    stats.AddAdmission(status, page.Size);
    Memory_.AddStats(stats);
    return status;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsertPage(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate,
    TIntrusivePtr<TPageFetchWaiter> waiter, TSharedPageRefImpl<TTraits>& hit, TPageFetchImpl<TTraits>& fetch) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    hit.Drop();
    fetch = {};
    TOperationItemRef ref(spaceOp);
    const ESharedCacheResultStatus status = cacheOp.FindOrInsert(candidate, ref.Get());
    switch (status) {
        case ESharedCacheResultStatus::Miss:
            return status;
        case ESharedCacheResultStatus::Inserted:
        case ESharedCacheResultStatus::Pending: {
            const TCacheItem cacheItem = ref.CacheItem();
            THandle& handle = spaceOp.Handles()[cacheItem.Index()];
            const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() &&
                                 handle.Body.Fetch && handle.Body.Fetch->Page().CacheItem() == cacheItem);
            TPageFetchState* pageFetch = handle.Body.Fetch;
            if (status == ESharedCacheResultStatus::Pending) {
                TCollection* collection = cacheOp.Collections().PageCollection(handle);
                if (candidate.Sticky == EStickyState::Sticky ||
                    (collection && Collections_.WantsStickyPage(*collection))) {
                    cacheOp.Collections().MakePageSticky(cacheItem);
                }
            }
            // The acquired pending ref prevents waiter-list closure until this operation releases it.
            Y_ABORT_UNLESS(pageFetch->Subscribe(waiter));
            InvokeHook(ESharedCacheHookPoint::AfterFetchWaiterSubscribed, cacheItem);
            if (status == ESharedCacheResultStatus::Inserted) {
                fetch = TPageFetchImpl<TTraits>(pageFetch);
                // The fetch owns this structural count; FetchState_->Page() supplies its exact identity.
                ref.Disarm();
            }
            return status;
        }
        case ESharedCacheResultStatus::Hit:
            hit = cacheOp.BuildPageRef(ref.Take());
            return status;
    }
    Y_UNREACHABLE();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PreparePageBatch(TSpaceOperation& spaceOp, TCollection& collection,
    TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests, TVector<TPageInsertCandidate>& candidates,
    ui64& reservedBytes) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (requests.size() > Max<ui32>()) {
        return false;
    }
    reservedBytes = 0;
    candidates.reserve(requests.size());
    THashSet<ui64> preparedOffsets;
    for (const TSharedCachePageRequestImpl<TTraits>& request : requests) {
        Y_DEBUG_ABORT_UNLESS(
            request.Location && request.Location.Type != NTable::NPage::EPage::Undef && request.Waiter);
        Y_DEBUG_ABORT_UNLESS(request.Location.Size <= Max<ui64>() - NActors::TSharedData::OverheadSize);

        TPageInsertCandidate candidate = BuildPageInsertCandidate(
            collection.CacheItem, request.Location, request.Sticky ? EStickyState::Sticky : EStickyState::None);
        candidate.CollectionOwnerHeld = true;
        // Only pages that are absent from the table need admission budget: a page that is already resident is
        // a hit or a pending subscription and must be admitted even when the cache is at its limit.
        TCacheItem resident;
        candidate.Prepare = cacheOp.Find(candidate.Key, resident) == ESharedCacheResultStatus::Miss &&
                            preparedOffsets.insert(static_cast<ui64>(request.Location.Offset)).second;
        if (candidate.Prepare) {
            const ui64 pageBytes = TCacheItems<TTraits>::AccountedPageBytes(candidate.Size);
            if (reservedBytes > Max<ui64>() - pageBytes) {
                return false;
            }
            reservedBytes += pageBytes;
        }
        candidates.push_back(candidate);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AllocatePageBatch(TSpaceOperation& spaceOp, TCollection& collection,
    TVector<TPageInsertCandidate>& candidates, ui64 reservedBytes, bool waitForMemory,
    EPageAdmission admission) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    ui64 preparedPages = 0;
    for (const TPageInsertCandidate& candidate : candidates) {
        preparedPages += candidate.Prepare ? 1 : 0;
    }
    bool reserved = true;
    if (admission == EPageAdmission::Written) {
        Memory_.AccountWritten(reservedBytes, preparedPages);
    } else {
        reserved = cacheOp.TryReserve(reservedBytes, preparedPages);
    }
    if (!reserved && !waitForMemory) {
        return false;
    }

    ui64 initializedBytes = 0;
    ui64 initializedPages = 0;
    for (TPageInsertCandidate& candidate : candidates) {
        candidate.WaitForMemory = waitForMemory;
        if (!candidate.Prepare) {
            continue;
        }
        const TCacheItem cacheItem = cacheOp.Allocate();
        if (cacheItem.IsNull()) {
            for (TPageInsertCandidate& initialized : candidates) {
                if (!initialized.CacheItem.IsNull()) {
                    cacheOp.RecycleCandidate(initialized.CacheItem);
                    initialized.CacheItem = {};
                }
            }
            if (reserved) {
                ReleaseReservation(reservedBytes - initializedBytes, preparedPages - initializedPages);
            }
            return false;
        }
        const ECacheMode admissionMode = collection.CacheMode_.load(std::memory_order_acquire);
        const EStickyState sticky = admissionMode == ECacheMode::Sticky || (admissionMode == ECacheMode::Regular &&
                                                                               candidate.Sticky == EStickyState::Sticky)
                                        ? EStickyState::Sticky
                                        : EStickyState::None;
        cacheOp.InitializePage(cacheItem, collection.CacheItem, candidate.Key.Word(1), candidate.Size, candidate.Type,
            candidate.Crc32, sticky, reserved, true);
        candidate.CacheItem = cacheItem;
        initializedBytes += TCacheItems<TTraits>::AccountedPageBytes(candidate.Size);
        ++initializedPages;
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FindOrInsertBatch(TCollectionCacheItem collection,
    TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests, bool recordStats, bool waitForMemory) noexcept {
    return FindOrInsertBatch(collection, requests, recordStats, waitForMemory, EPageAdmission::Bounded);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FindOrInsertWrittenBatch(
    TCollectionCacheItem collection, TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests) noexcept {
    return FindOrInsertBatch(collection, requests, false, false, EPageAdmission::Written);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FindOrInsertBatch(TCollectionCacheItem collection,
    TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests, bool recordStats, bool waitForMemory,
    EPageAdmission admission) noexcept {
    if (requests.empty()) {
        return true;
    }

    TVector<TPageInsertCandidate> candidates;
    ui64 reservedBytes;

    auto cacheOp = BeginOperation();
    TOperationItemRef collectionRef(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    THandle& collectionHandle = cacheOp.Handles()[collection.Index()];
    const THandleState collectionState = THandleState::FromRaw(collectionHandle.State.load(std::memory_order_acquire));
    if (!collection.Matches(collectionState) || !collectionState.IsSticky() || !collectionState.IsStickyField() ||
        !collectionHandle.Body.Collection) {
        return false;
    }
    TCollection& owner = *collectionHandle.Body.Collection;
    // A handle ref protects the object, but only an owner ref prevents detach from making it reclaimable.
    // Never revive zero: detach may have already dropped the last owner after the state check above.
    ui64 owners = owner.References.load(std::memory_order_relaxed);
    do {
        if (owners == 0) {
            return false;
        }
    } while (!owner.References.compare_exchange_weak(
        owners, owners + 1, std::memory_order_acquire, std::memory_order_relaxed));
    Y_DEFER {
        cacheOp.Collections().DropOwnerReference(owner);
    };
    const THandleState admittedState = THandleState::FromRaw(collectionHandle.State.load(std::memory_order_acquire));
    if (!collection.Matches(admittedState) || !admittedState.IsSticky() || !admittedState.IsStickyField()) {
        return false;
    }
    if (!cacheOp.PreparePageBatch(owner, requests, candidates, reservedBytes)) {
        return false;
    }
    for (auto& candidate : candidates) {
        candidate.Admission = admission;
    }
    InvokeHook(ESharedCacheHookPoint::AfterPageBatchPrepared, collection.CacheItem());
    if (!cacheOp.AllocatePageBatch(owner, candidates, reservedBytes, waitForMemory, admission)) {
        return false;
    }

    // A rejected batch returns before this point, so every requested page reports exactly one admission outcome.
    TStats stats;
    for (ui32 index = 0; index < requests.size(); ++index) {
        TSharedCachePageRequestImpl<TTraits>& request = requests[index];
        request.Reset();
        TSharedPageRefImpl<TTraits> page;
        TPageFetchImpl<TTraits> fetch;
        const ESharedCacheResultStatus status =
            cacheOp.FindOrInsertPage(candidates[index], std::move(request.Waiter), page, fetch);
        if (status == ESharedCacheResultStatus::Hit) {
            if (request.Sticky || Collections_.WantsStickyPage(owner)) {
                cacheOp.Collections().MakePageSticky(page.CacheItem().CacheItem());
            }
            request.SetHit(std::move(page));
        } else if (status == ESharedCacheResultStatus::Inserted) {
            request.SetInserted(std::move(fetch));
        } else if (status == ESharedCacheResultStatus::Pending) {
            request.SetPending();
        }
        if (status != ESharedCacheResultStatus::Miss || !waitForMemory) {
            stats.AddAdmission(status, request.Location.Size);
        }
    }
    if (recordStats) {
        Memory_.AddStats(stats);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsertUnregistered(
    const TCollectionLocation& collection, TCollectionCacheItem& inserted, TSharedCacheCollectionRef& hit) noexcept {
    inserted = {};
    hit.Drop();
    auto cacheOp = BeginOperation();
    return cacheOp.FindOrInsertCollection(nullptr, collection, inserted, hit);
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(
    const TCollectionLocation& collection, TCollectionCacheItem& inserted, TSharedCacheCollectionRef& hit) noexcept {
    return FindOrInsert(Collections_.Registry(), collection, inserted, hit);
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(TCollectionRegistry& registry,
    const TCollectionLocation& collection, TCollectionCacheItem& inserted, TSharedCacheCollectionRef& hit) noexcept {
    inserted = {};
    hit.Drop();
    auto cacheOp = BeginOperation();
    return cacheOp.FindOrInsertCollection(&registry, collection, inserted, hit);
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsertCollection(TSpaceOperation& spaceOp, TCollectionRegistry* registry,
    const TCollectionLocation& collection, TCollectionCacheItem& inserted, TSharedCacheCollectionRef& hit) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    TCollectionInsertCandidate candidate{
        .Key = TSharedCacheKey::Collection(collection.Id),
        .Bytes = collection.AccountedBytes(),
    };
    TOperationItemRef ref(spaceOp);
    const ESharedCacheResultStatus status = cacheOp.FindOrInsert(candidate, ref.Get());
    if (status == ESharedCacheResultStatus::Inserted) {
        inserted = TCollectionCacheItem::FromValidated(ref.CacheItem());
    } else if (status == ESharedCacheResultStatus::Hit) {
        if (registry) {
            THandle& handle = spaceOp.Handles()[ref.CacheItem().Index()];
            TCollectionRegistry* owner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
            if (owner && owner != registry && !cacheOp.Collections().MoveRegistry(*owner, *registry, ref.CacheItem()))
            {
                return ESharedCacheResultStatus::Pending;
            }
            if (!cacheOp.Collections().Attach(*registry, ref.CacheItem())) {
                return ESharedCacheResultStatus::Pending;
            }
        }
        hit = cacheOp.BuildCollectionRef(ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(
    TSpaceOperation& spaceOp, TCacheItem candidate, TSharedCacheItemRef& ref) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const THandle& handle = spaceOp.Handles()[candidate.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    TCacheKeyedItem keyedCandidate{
        .Key = LoadSharedCacheKey(handle, state.Kind()),
        .CacheItem = candidate,
    };
    return cacheOp.FindOrInsert(keyedCandidate, ref);
}

SHARED_CACHE_TEMPLATE
template <class TKeyedCandidate>
ESharedCacheResultStatus TSharedCache::FindOrInsert(
    TSpaceOperation& spaceOp, TKeyedCandidate& candidate, TSharedCacheItemRef& ref) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    for (;;) {
        typename TOperation::TInsertPosition insertPosition;
        TCacheItem cacheItem;
        const ESharedCacheResultStatus status = cacheOp.FindForInsert(candidate.Key, cacheItem, insertPosition);
        bool inserted = false;
        if (status == ESharedCacheResultStatus::Miss) {
            if (candidate.CacheItem.IsNull()) {
                if (!cacheOp.PrepareCandidate(candidate)) {
                    cacheOp.DropInsertOwner(insertPosition);
                    return ESharedCacheResultStatus::Miss;
                }
                cacheOp.DropInsertOwner(insertPosition);
                continue;
            }
            if (!cacheOp.InsertAt(insertPosition, candidate.CacheItem)) {
                continue;
            }
            cacheItem = candidate.CacheItem;
            inserted = true;
        } else {
            Y_DEBUG_ABORT_UNLESS(status == ESharedCacheResultStatus::Hit);
        }

        TOperationItemRef acquired(spaceOp);
        const EAcquireStatus acquireStatus = cacheOp.TryAcquire(cacheItem, true, acquired.Get());
        if (acquireStatus == EAcquireStatus::Stale) {
            Y_DEBUG_ABORT_UNLESS(!inserted);
            continue;
        }
        Y_DEBUG_ABORT_UNLESS(acquireStatus == EAcquireStatus::Pending || acquireStatus == EAcquireStatus::Ready);

        if (inserted) {
            cacheOp.RefillSpareItem();
            ref = acquired.Take();
            return ESharedCacheResultStatus::Inserted;
        }

        Y_DEBUG_ABORT_UNLESS(cacheOp.MatchesCandidateMetadata(candidate, cacheItem));
        if (!candidate.CacheItem.IsNull()) {
            cacheOp.RecycleCandidate(candidate.CacheItem);
            candidate.CacheItem = {};
        }
        ref = acquired.Take();
        return acquireStatus == EAcquireStatus::Pending ? ESharedCacheResultStatus::Pending
                                                        : ESharedCacheResultStatus::Hit;
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateCurrentLimit(ui64 limit) noexcept {
    if (limit == 0 || limit > HardLimit()) {
        return false;
    }

    Memory_.SetCurrentLimit(limit);
    RefreshSoftLimit();
    NotifyResourcesAvailable();
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateSoftLimit(ui64 limit) noexcept {
    Memory_.SetSoftLimitOverride(limit);
    RefreshSoftLimit();
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RefreshSoftLimit() noexcept {
    const ui64 softLimit = Memory_.RefreshSoftLimit(Policy_.CurrentLimitGap, Policy_.MinCurrentLimitGap);
    if (auto hotResize = HotResize_.TryLock()) {
        Eviction_.RefreshEvictableByteBudget();
    }
    if (RetainedBytes() >= softLimit) {
        Eviction_.MarkSoftLimitPressure();
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateKeepColdLimit(ui64 limit) noexcept {
    return Eviction_.UpdateKeepColdLimit(limit);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::NotifyResourcesAvailable() noexcept {
    if (Memory_.NotifyResourcesAvailable()) {
        TTraits::NotifyResourcesAvailable();
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ArmResourceWait(ui64 generation) noexcept {
    if (Memory_.ArmResourceWait(generation)) {
        TTraits::NotifyResourcesAvailable();
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryReserve(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (bytes == 0 && pages == 0) {
        return true;
    }

    ui64 usage = Memory_.PayloadUsage();
    ui32 reclamationAttempts = 0;
    for (;;) {
        const ui64 currentLimit = CurrentLimit();
        if (ExceedsLimit(usage, currentLimit, bytes)) {
            Eviction_.MarkSoftLimitPressure();
            // Aging/resizing can make progress without freeing bytes. Bound cooperative work so
            // concurrent activity cannot keep a requester here instead of putting it on the wait path.
            if (reclamationAttempts++ < SharedCacheTransitionWorkBatch && cacheOp.Eviction().Reclaim(bytes)) {
                usage = Memory_.PayloadUsage();
                continue;
            }
            return false;
        }
        if (Memory_.TryReserve(usage, bytes, pages)) {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseReservation(ui64 bytes, ui64 pages) noexcept {
    Memory_.ReleaseReservation(bytes, pages);
    NotifyResourcesAvailable();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateHardLimit(ui64 limit) noexcept {
    if (limit == 0 || limit < CurrentLimit() || limit > ReservationLimit()) {
        return false;
    }

    const TSharedCacheCapacity& current = Space_->CurrentConfiguration();
    TSharedCacheCapacity target;
    ui8 minimumAddressBits = MinSharedCacheAddressBits;
    while (TSharedCacheCapacity{ .AddressBits = minimumAddressBits }.HotSlotCount() < Policy_.MinHotSlots) {
        ++minimumAddressBits;
    }
    if (!TryCalculateSharedCacheCapacity(
            limit, current.ExpectedPageSize, current.FixedBytes, Space_->HazardCount(), target) ||
        target.AddressBits < minimumAddressBits)
    {
        if (!TryCalculateSharedCacheFootprint(
                minimumAddressBits, current.ExpectedPageSize, current.FixedBytes, Space_->HazardCount(), target)) {
            return false;
        }
    }
    if (target.AddressBits > Space_->ReservedConfiguration().AddressBits) {
        return false;
    }
    target.Limit = limit;

    Capacity_.SetTarget(target);
    Memory_.SetHardLimit(limit);
    Capacity_.StartTransition();
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DrainHotResize() noexcept {
    if (auto hotResize = HotResize_.TryLock()) {
        auto cacheOp = BeginOperation();
        return cacheOp.Eviction().DrainHotResize();
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimCold() noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.Eviction().ReclaimCold();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EnforceCurrentLimit() noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.Eviction().EnforceCurrentLimit(0);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::RunMaintenance() noexcept {
    Y_DEBUG_ABORT_UNLESS(CurrentLimit() <= HardLimit());
    if (Eviction_.NeedsKeepColdTrim())
    {
        auto cacheOp = BeginOperation();
        if (cacheOp.Eviction().TrimKeepCold()) {
            return true;
        }
    }
    if (Eviction_.SweepKeepCold()) {
        return true;
    }
    return Capacity_.RunMaintenance();
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseReplacing(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(cacheItem.Matches(expected) && expected.IsPageKind() && expected.Refs() > 0);
        if (expected.IsReplaced()) {
            cacheOp.ReleaseReplaced(cacheItem);
            return;
        }
        // The relocation owner remains held until Replaced is published, so Replacing cannot lose its last ref.
        Y_ABORT_UNLESS(expected.IsReplacing() && expected.Refs() > 1);
        const THandleState desired = expected.DecrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseReplaced(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(
            cacheItem.Matches(expected) && expected.IsPageKind() && expected.IsReplaced() && expected.Refs() > 0);
        if (expected.Refs() > 1) {
            const THandleState desired = expected.DecrementRefs();
            if (handle.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
            {
                return; // existing reference defer cleanup
            }
            continue;
        }

        // final cleanup
        const ui32 nextVersion = AdvanceItemVersion(expected.Version());
        const THandleState desired = expected.WithVersion(nextVersion)
                                         .WithState(EHandleState::Free)
                                         .WithFrequency(0)
                                         .WithSticky(EStickyState::None)
                                         .WithRefs(0);
        TCollection* owner = cacheOp.Collections().PageCollection(handle);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            TOperationItemRef replacementRef(
                spaceOp, TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire)).WithoutFrozen());
            Y_DEBUG_ABORT_UNLESS(!replacementRef.CacheItem().IsNull() && replacementRef.CacheItem() != cacheItem);
            Y_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_acquire) == 0);
            DeleteReplacedPage(handle);
            if (owner) {
                cacheOp.Collections().DropOwnerReference(*owner);
            }
            Y_ABORT_UNLESS(spaceOp.ReturnFreeHandle(cacheItem.Index(), nextVersion));
            NotifyResourcesAvailable();
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::Release(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    TCollection* owner = cacheOp.ReleaseItem(cacheItem);
    if (owner) {
        cacheOp.Collections().DropOwnerReference(*owner);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::CompleteColdPageRelease(TOperationItemRef& ownerRef) noexcept {
    TSpaceOperation& spaceOp = ownerRef.SpaceOperation();
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem cacheItem = ownerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
    if (TCollection* owner = cacheOp.Collections().PageCollection(handle);
        owner && owner->CacheMode_.load(std::memory_order_acquire) == ECacheMode::TryKeepInMemory &&
        owner->IsKeepAllowedPage(handle.Metadata.Page.Type)) {
        this->NotifyKeepPageEviction(owner->Id(), owner->CacheItem, owner->KeepGeneration(),
            NTable::NPage::TPageLocation(
                NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
                handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type,
                handle.Metadata.Page.Crc32));
    }
    AdvanceColdMembership(handle);
    cacheOp.FinishErase(cacheItem, LoadSharedCacheKey(handle, EItemKind::Page));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryReleaseColdPageToKeepCold(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, ui64& expectedRaw) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    // The caller has charged RetainedBytes; a failed CAS must roll back every provisional charge.
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState expected = THandleState::FromRaw(expectedRaw);
    const ui64 bytes = TCacheItems<TTraits>::PageBytes(handle);
    TCollection* owner = cacheOp.Collections().PageCollection(handle);
    if (owner) {
        Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
    }
    Memory_.AddKeepColdOwnedBytes(bytes);
    // Preserve this reference until membership and accounting are ready for the unreferenced handoff.
    const THandleState desired = expected.WithState(EHandleState::KeepCold);
    if (handle.State.compare_exchange_weak(
            expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
    {
        TOperationItemRef itemRef(spaceOp, cacheItem);
        InvokeHook(ESharedCacheHookPoint::AfterKeepColdPromotion, cacheItem);
        const ui32 coldVersion = AdvanceColdMembership(handle);
        Memory_.Cold().TransferBytesTo(Memory_.KeepCold(), bytes);
        Memory_.Cold().TransferPageTo(Memory_.KeepCold(), EItemKind::Page);
        Memory_.RemoveColdItem();
        cacheOp.Eviction().ReleaseKeepColdRef(itemRef, coldVersion);
        return true;
    }
    Memory_.ReleaseKeepColdOwnedBytes(bytes);
    Memory_.ReleaseRetainedBytes(bytes);
    if (owner) {
        Collections_.AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
    }
    return false;
}

#undef TSharedCacheCollectionRef
#undef TSharedCacheItemRef
#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

// Typed-reference builders are instantiated with their callers and constructors
template TSharedPageRefImpl<TProdTraits> TCacheItems<TProdTraits>::BuildPageRef(
    const TSpaceOperation&, TSharedCacheItemRefImpl<TProdTraits>&&) const noexcept;
template TSharedCacheCollectionRefImpl<TProdTraits> TCacheItems<TProdTraits>::BuildCollectionRef(
    const TSpaceOperation&, TSharedCacheItemRefImpl<TProdTraits>&&) const noexcept;
template TSharedPageRefImpl<TTestTraits> TCacheItems<TTestTraits>::BuildPageRef(
    const TSpaceOperation&, TSharedCacheItemRefImpl<TTestTraits>&&) const noexcept;
template TSharedCacheCollectionRefImpl<TTestTraits> TCacheItems<TTestTraits>::BuildCollectionRef(
    const TSpaceOperation&, TSharedCacheItemRefImpl<TTestTraits>&&) const noexcept;

template class TSharedPageRefImpl<TProdTraits>;
template class TSharedCacheCollectionRefImpl<TProdTraits>;
template class TSharedCacheImpl<TProdTraits>;
template class TSharedPageRefImpl<TTestTraits>;
template class TSharedCacheCollectionRefImpl<TTestTraits>;
template class TSharedCacheImpl<TTestTraits>;

} // namespace NKikimr::NSharedCache
