#include "shared_cache.h"
#include "shared_cache_events.h"
#include "shared_cache_traits.h"

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/thread_context.h>

#include <util/generic/cast.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/system/spinlock.h>

#include <cmath>
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

ui32 TProdTraits::CurrentWorkerIndex() noexcept {
    return NActors::TlsThreadContext ? NActors::TlsThreadContext->WorkerId() : Max<ui32>();
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
#define TPageFetch TPageFetchImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
TSharedCacheThreadBinding<TTraits>::TSharedCacheThreadBinding(void* cache, TSpaceHazardBinding&& hazard) noexcept
    : Cache_(cache)
    , Hazard_(std::move(hazard))
{
    if constexpr (requires { TTraits::BindCache(cache); }) {
        TTraits::BindCache(cache);
    }
}

SHARED_CACHE_TEMPLATE
TSharedCacheThreadBinding<TTraits>::TSharedCacheThreadBinding(TSharedCacheThreadBinding&& other) noexcept
    : Cache_(std::exchange(other.Cache_, nullptr))
    , Hazard_(std::move(other.Hazard_))
{}

SHARED_CACHE_TEMPLATE
TSharedCacheThreadBinding<TTraits>& TSharedCacheThreadBinding<TTraits>::operator=(
    TSharedCacheThreadBinding&& other) noexcept {
    if (this != &other) {
        Reset();
        Cache_ = std::exchange(other.Cache_, nullptr);
        Hazard_ = std::move(other.Hazard_);
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TSharedCacheThreadBinding<TTraits>::~TSharedCacheThreadBinding() {
    Reset();
}

SHARED_CACHE_TEMPLATE
void TSharedCacheThreadBinding<TTraits>::Reset() noexcept {
    if (Cache_) {
        if constexpr (requires { TTraits::UnbindCache(Cache_); }) {
            TTraits::UnbindCache(Cache_);
        }
        Cache_ = nullptr;
        Hazard_ = {};
    }
}

SHARED_CACHE_TEMPLATE
TPageFetch::TPageFetchImpl(TPageFetchState* fetch) noexcept
    : FetchState_(fetch)
{
    Y_DEBUG_ABORT_UNLESS(FetchState_ && FetchState_->Page());
}

SHARED_CACHE_TEMPLATE
TPageFetch::TPageFetchImpl(TPageFetch&& other) noexcept
    : FetchState_(std::move(other.FetchState_))
{}

SHARED_CACHE_TEMPLATE
TPageFetch& TPageFetch::operator=(TPageFetch&& other) noexcept {
    if (this != &other) {
        FailReady();
        FetchState_ = std::move(other.FetchState_);
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TPageFetch::~TPageFetchImpl() {
    FailReady();
}

SHARED_CACHE_TEMPLATE
TPageCacheItem TPageFetch::CacheItem() const noexcept {
    Y_DEBUG_ABORT_UNLESS(FetchState_);
    return FetchState_->Page();
}

SHARED_CACHE_TEMPLATE
NTable::NPage::TPageLocation TPageFetch::Location() const noexcept {
    Y_DEBUG_ABORT_UNLESS(FetchState_);
    TSharedCache& cache = TSharedCache::Get();
    auto binding = cache.BindCurrentThreadHazard();
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

SHARED_CACHE_TEMPLATE
ui64 TPageFetch::Size() const noexcept {
    return Location().Size;
}

SHARED_CACHE_TEMPLATE
bool TPageFetch::HasActiveWaiters() const noexcept {
    return FetchState_ && FetchState_->HasActiveWaiters();
}

SHARED_CACHE_TEMPLATE
void TPageFetch::ForEachActiveWaiter(TFunctionRef<void(const TPageFetchWaiter&)> visitor) const noexcept {
    if (FetchState_) {
        FetchState_->ForEachActiveWaiter(visitor);
    }
}

SHARED_CACHE_TEMPLATE
bool TPageFetch::TryReserveMemory() noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCache& cache = TSharedCache::Get();
    auto binding = cache.BindCurrentThreadHazard();
    auto cacheOp = cache.BeginOperation();
    return cacheOp.ReserveFetchMemory(FetchState_->Page().CacheItem(), *FetchState_);
}

SHARED_CACHE_TEMPLATE
bool TPageFetch::Dispatch() noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCache& cache = TSharedCache::Get();
    auto binding = cache.BindCurrentThreadHazard();
    auto cacheOp = cache.BeginOperation();
    return cacheOp.DispatchFetch(FetchState_->Page().CacheItem(), *FetchState_);
}

SHARED_CACHE_TEMPLATE
bool TPageFetch::MakeReady(NActors::TSharedData&& data) noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCache& cache = TSharedCache::Get();
    bool ready;
    {
        auto binding = cache.BindCurrentThreadHazard();
        auto spaceOp = cache.BeginOperation();
        // The fetch owns the insertion ref represented by FetchState_, without storing a second item identity.
        TOperationItemRef owner(spaceOp, FetchState_->Page().CacheItem());
        ready = cache.CompleteFetch(owner, *FetchState_, std::move(data));
    }
    FetchState_.Drop();
    return ready;
}

SHARED_CACHE_TEMPLATE
bool TPageFetch::FailReady() noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCache* cache = TSharedCache::TryGet();
    if (!cache) {
        // Missing actor context is terminal shutdown, as for native item references.
        FetchState_->AbandonWaiters();
        FetchState_.Drop();
        return false;
    }
    bool failed;
    {
        auto binding = cache->BindCurrentThreadHazard();
        auto spaceOp = cache->BeginOperation();
        TOperationItemRef owner(spaceOp, FetchState_->Page().CacheItem());
        failed = cache->FailFetch(owner, *FetchState_);
    }
    FetchState_.Drop();
    return failed;
}

SHARED_CACHE_TEMPLATE
TSharedCache::TSharedCacheImpl(THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits) noexcept
    : TTraits(std::move(traits))
    , Space_(std::move(space))
    , Table_(*Space_, *this)
    , Memory_(Space_->CurrentConfiguration().StaticBytes, hardLimit, Space_->ReservedConfiguration().Limit)
    , TargetHandleCount_(Space_->CurrentConfiguration().HandleCount())
    , HardTarget_(Space_->CurrentConfiguration())
{
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
    auto binding = cache.BindCurrentThreadHazard();
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
    auto binding = cache.BindCurrentThreadHazard();
    return cache.AcquireHeldPage(Ref_.CacheItem());
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
    auto binding = cache.BindCurrentThreadHazard();
    auto cacheOp = cache.BeginOperation();
    const THandle& page = cacheOp.Handles()[CacheItem().Index()];
    return cacheOp.PageCollection(page)->Id();
}

SHARED_CACHE_TEMPLATE
bool TSharedPageRefImpl<TTraits>::IsSticky() const noexcept {
    if (!Ref_) {
        return false;
    }
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::Get();
    auto binding = cache.BindCurrentThreadHazard();
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
    auto binding = cache.BindCurrentThreadHazard();
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
    auto binding = cache.BindCurrentThreadHazard();
    return cache.AcquireHeldCollection(Ref_.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheCollectionRef::IsSoleReference() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    auto& cache = TSharedCache::Get();
    auto binding = cache.BindCurrentThreadHazard();
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
            DeletePendingFetch(handle);
        } else if (state.IsReady() || state.IsTombstone()) {
            DeletePayload(handle, state.Kind(), state.State());
        }
    }
    for (ui64 index = 2; index < space.HandleCount; ++index) {
        THandle& handle = space.Handles[index];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        if (state.IsCollectionKind() && (state.IsReady() || state.IsTombstone())) {
            DeletePayload(handle, state.Kind(), state.State());
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DeleteReplacedPage(THandle& handle) noexcept {
    Y_DEBUG_ABORT_UNLESS(handle.Body.PageBuffer);
    handle.Body.PageBuffer.~TBuffer();
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DeletePendingFetch(THandle& handle) noexcept {
    if (TPageFetchState* fetch = std::exchange(handle.Body.Fetch, nullptr)) {
        fetch->UnRef();
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DeletePayload(THandle& handle, EItemKind kind, EHandleState state, TCollection* owner) noexcept {
    Y_DEBUG_ABORT_UNLESS(IsReady(state) || state == EHandleState::Tombstone);
    Y_DEBUG_ABORT_UNLESS(kind == EItemKind::Page || handle.Body.Collection);
    const bool hasPayload = kind != EItemKind::Page || bool(handle.Body.PageBuffer);
    Y_DEBUG_ABORT_UNLESS(state == EHandleState::Tombstone || hasPayload);
    const ui64 bytes = hasPayload ? PayloadBytes(handle, kind) : 0;
    if (kind == EItemKind::Page) {
        if (hasPayload && owner) {
            AddResidentPageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            if (state == EHandleState::Hot || state == EHandleState::Sticky || state == EHandleState::KeepCold) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
        }
        handle.Body.PageBuffer.~TBuffer();
    } else {
        Y_DEBUG_ABORT_UNLESS(hasPayload);
        delete std::exchange(handle.Body.Collection, nullptr);
        const ui64 previous = Collections_.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
        SubtractExactBytes(CollectionBytes_, bytes);
    }
    if (hasPayload) {
        const i64 estimatedBytes = static_cast<i64>(bytes);
        if (state == EHandleState::Hot) {
            Memory_.Hot().RemoveBytes(estimatedBytes);
            Memory_.Hot().RemovePage(kind);
        } else if (state == EHandleState::Sticky) {
            Memory_.Sticky().RemoveBytes(estimatedBytes);
            Memory_.Sticky().RemovePage(kind);
            Memory_.ReleaseStickyOwnedBytes(kind, bytes);
        } else if (state == EHandleState::KeepCold) {
            Memory_.KeepCold().RemoveBytes(estimatedBytes);
            Memory_.KeepCold().RemovePage(kind);
            Memory_.ReleaseKeepColdOwnedBytes(bytes);
        } else {
            Memory_.Cold().RemoveBytes(estimatedBytes);
            Memory_.Cold().RemovePage(kind);
            Memory_.RemoveColdItem();
        }
        Memory_.RemoveResident(bytes);
        if (state == EHandleState::Hot || state == EHandleState::Sticky || state == EHandleState::KeepCold) {
            Memory_.ReleaseRetainedBytes(bytes);
        }
        Memory_.ReleasePayloadCharge(bytes, kind);
        if (kind == EItemKind::Page) {
            NotifyResourcesAvailable();
        }
    }
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
ui64 TSharedCache::PayloadBytes(const THandle& handle, EItemKind kind) noexcept {
    return kind == EItemKind::Page ? PageBytes(handle) : handle.Metadata.PayloadBytes;
}

SHARED_CACHE_TEMPLATE
ui64 TSharedCache::PageBytes(const THandle& handle) noexcept {
    return AccountedPageBytes(handle.Key2OrSize.load(std::memory_order_relaxed));
}

SHARED_CACHE_TEMPLATE
ui64 TSharedCache::AccountedPageBytes(ui64 size) noexcept {
    Y_ABORT_UNLESS(size <= Max<ui64>() - NActors::TSharedData::OverheadSize);
    return size + NActors::TSharedData::OverheadSize;
}

SHARED_CACHE_TEMPLATE
ui64 TSharedCache::FractionCeil(ui64 value, double fraction) noexcept {
    Y_DEBUG_ABORT_UNLESS(fraction >= 0.0 && fraction <= 1.0);
    return fraction == 1.0 ? value : static_cast<ui64>(std::ceil(static_cast<double>(value) * fraction));
}

SHARED_CACHE_TEMPLATE
TCollection* TSharedCache::PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    return PageCollection(
        spaceOp, TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed))));
}

SHARED_CACHE_TEMPLATE
TCollection* TSharedCache::PageCollection(TSpaceOperation& spaceOp, TCollectionCacheItem item) noexcept {
    if (item.Index() >= spaceOp.AccessibleHandleCount()) {
        return nullptr;
    }
    THandle& collection = spaceOp.Handles()[item.Index()];
    const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
    if (!item.Matches(state) || !collection.Body.Collection) {
        return nullptr;
    }
    return collection.Body.Collection;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AddReference(TCollection& owner) noexcept {
    owner.References.fetch_add(1, std::memory_order_relaxed);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DropReference(TCollection& owner) noexcept {
    const ui64 previous = owner.References.fetch_sub(1, std::memory_order_relaxed);
    Y_ABORT_UNLESS(previous != 0);
    return previous == 1;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DropOwnerReference(TSpaceOperation& spaceOp, TCollection& owner) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    // Keep the payload alive after dropping the page's collection ownership.
    TOperationItemRef collectionRef(spaceOp, cacheOp.TryAcquireStructural(owner.CacheItem.CacheItem()));
    Y_ABORT_UNLESS(collectionRef);
    const bool lastOwner = DropReference(owner);
    InvokeHook(ESharedCacheHookPoint::AfterCollectionOwnerReferenceDropped, collectionRef.CacheItem());
    if (lastOwner) {
        const TLogoBlobID id = owner.Id();
        const TCollectionCacheItem item = owner.CacheItem;
        cacheOp.PublishCollectionForReclaim(item.CacheItem());
        this->NotifyCollectionReleased(id, item);
    } else if (owner.Registry_.load(std::memory_order_acquire) &&
               owner.References.load(std::memory_order_acquire) == 1) {
        // The registry is the only remaining structural owner; actor work may now be cleaned up.
        this->NotifyCollectionReleased(owner.Id(), owner.CacheItem);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::PublishKeepPageBytes(TCollection& owner) noexcept {
    constexpr ui32 Publishing = 1;
    constexpr ui32 Changed = 2;
    if (owner.KeepAccountingState_.fetch_or(Publishing | Changed, std::memory_order_acq_rel) & Publishing) {
        return; // The active publisher will pick up this update; other cache operations do not wait.
    }
    for (;;) {
        owner.KeepAccountingState_.fetch_and(~Changed, std::memory_order_acquire);
        InvokeHook(ESharedCacheHookPoint::BeforeKeepBytesPublished, owner.CacheItem.CacheItem());
        const bool keep = owner.GetCacheMode() == ECacheMode::TryKeepInMemory;
        const ui64 resident = keep ? owner.KeepResidentPageBytes() : 0;
        const ui64 active = keep ? owner.KeepActivePageBytes() : 0;
        const auto update = [&](std::atomic<ui64>& total, ui64 previous, ui64 current) {
            if (current > previous) {
                total.fetch_add(current - previous, std::memory_order_relaxed);
            } else if (current < previous) {
                SubtractExactBytes(total, previous - current);
            }
        };
        update(KeepResidentPageBytes_, owner.ReportedKeepResidentBytes_, resident);
        update(KeepActivePageBytes_, owner.ReportedKeepActiveBytes_, active);
        owner.ReportedKeepResidentBytes_ = resident;
        owner.ReportedKeepActiveBytes_ = active;
        ui32 expected = Publishing;
        if (owner.KeepAccountingState_.compare_exchange_strong(
                expected, 0, std::memory_order_release, std::memory_order_relaxed)) {
            return;
        }
        // An update raced with this snapshot. Clearing Changed and resampling prevents a lost delta.
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AddResidentPageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept {
    const auto add = [&](std::atomic<ui64>& counter) {
        if (bytes >= 0) {
            counter.fetch_add(static_cast<ui64>(bytes), std::memory_order_relaxed);
        } else {
            SubtractExactBytes(counter, static_cast<ui64>(-bytes));
        }
    };
    add(owner.ResidentPageBytes_);
    if (type != NTable::NPage::EPage::Skip) {
        add(owner.KeepResidentPageBytes_);
    }
    if (type == NTable::NPage::EPage::BTreeIndex) {
        add(owner.BTreeIndexV1ResidentBytes_);
    }
    PublishKeepPageBytes(owner);
    if (!owner.BytesNotificationPending_.exchange(true, std::memory_order_acq_rel)) {
        this->NotifyCollectionBytesChanged(owner.Id(), owner.CacheItem);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AddActivePageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept {
    owner.ActivePageBytes_.fetch_add(bytes, std::memory_order_relaxed);
    if (type != NTable::NPage::EPage::Skip) {
        owner.KeepActivePageBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }
    if (type == NTable::NPage::EPage::BTreeIndex) {
        owner.BTreeIndexV1ActiveBytes_.fetch_add(bytes, std::memory_order_relaxed);
    }
    PublishKeepPageBytes(owner);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DropPageItemRef(TSpaceOperation& spaceOp, THandle& page) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_relaxed));
    if (state.Kind() != EItemKind::Page) {
        return;
    }
    if (TCollection* owner = cacheOp.PageCollection(page)) {
        cacheOp.DropOwnerReference(*owner);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept {
    const ui64 previous = counter.fetch_sub(bytes, std::memory_order_relaxed);
    Y_ABORT_UNLESS(previous >= bytes);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AddStats(const TStats& stats) noexcept {
    RequestedPages_.fetch_add(stats.RequestedPages, std::memory_order_relaxed);
    RequestedBytes_.fetch_add(stats.RequestedBytes, std::memory_order_relaxed);
    HitPages_.fetch_add(stats.HitPages, std::memory_order_relaxed);
    HitBytes_.fetch_add(stats.HitBytes, std::memory_order_relaxed);
    MissPages_.fetch_add(stats.MissPages, std::memory_order_relaxed);
    MissBytes_.fetch_add(stats.MissBytes, std::memory_order_relaxed);
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
bool TSharedCache::WantsStickyPage(const TCollection& collection) noexcept {
    return collection.CacheMode_.load(std::memory_order_acquire) == ECacheMode::Sticky;
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::TryAllocateHandle(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem spare = TCacheItem::FromRaw(spaceOp.Hazard().SpareItem.exchange(0, std::memory_order_relaxed));
    if (spare.IsNull()) {
        if (const ui32 index = spaceOp.TryAllocateHandle()) {
            return index;
        }
        // Byte headroom does not guarantee a free handle when pages are much smaller than the expected size.
        // Reclaim Cold, or shrink Hot to feed Cold, before refusing the admission.
        for (ui32 attempt = 0; attempt < spaceOp.HandleCount(); ++attempt) {
            if (!cacheOp.ReclaimCold()) {
                const ui32 hotSlots = static_cast<ui32>(Space_->EffectiveHotSlots());
                cacheOp.EnforceCurrentLimit(0, true);
                // The caller's view may predate growth; drain with a fresh operation under the resize lock.
                DrainHotResize();
                if (hotSlots == Space_->EffectiveHotSlots() && !cacheOp.ReclaimCold()) {
                    break;
                }
            }
            if (const ui32 index = spaceOp.TryAllocateHandle()) {
                return index;
            }
        }
        return 0;
    }

    Y_ABORT_UNLESS(!spare.IsFrozen() && spare.Index() >= 2 && spare.Index() < spaceOp.AccessibleHandleCount());
    const THandle& handle = spaceOp.Handles()[spare.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_ABORT_UNLESS(spare.Matches(state) && state.IsBegin() && state.Refs() == 0);
    return spare.Index();
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RefillSpareItem(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (spaceOp.Hazard().SpareItem.load(std::memory_order_relaxed) != 0) {
        return;
    }
    const ui32 index = spaceOp.TryAllocateHandle();
    if (!index) {
        return;
    }
    THandle& handle = spaceOp.Handles()[index];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    // A recycled free handle may retain a destroyed buffer's union bits. Begin also represents
    // these uninitialized spares, so pending-page cleanup must see a blank fetch payload.
    handle.Key0.store(0, std::memory_order_relaxed);
    handle.Body.Fetch = nullptr;
    const TCacheItem cacheItem = TCacheItem::Make(state.Version(), index);
    if (!spaceOp.TryStoreSpareItem(cacheItem)) {
        cacheOp.DiscardCandidate(cacheItem, state.Kind());
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RecycleCandidate(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (state.IsPageKind() && state.IsPending() && state.IsStickyField()) {
        cacheOp.UnlinkPendingStickyPage(cacheItem);
        state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    }
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && !state.IsFree() && !state.IsTombstone() && state.Refs() == 0);
    const bool pending = state.IsPending();
    const bool reserved = pending && !state.IsWaitingForMemory();
    const ui64 reservedBytes = reserved ? PayloadBytes(handle, state.Kind()) : 0;
    if (!pending) {
        Y_DEBUG_ABORT_UNLESS(state.IsReady());
        Y_DEBUG_ABORT_UNLESS(!state.IsPageKind() || !state.IsSticky());
        Y_DEBUG_ABORT_UNLESS(!state.IsCollectionKind() ||
                             handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) == 0);
        Y_DEBUG_ABORT_UNLESS(
            !state.IsCollectionKind() || handle.Body.Collection->Registry_.load(std::memory_order_acquire) == nullptr);
        DeletePayload(
            handle, state.Kind(), state.State(), state.IsPageKind() ? cacheOp.PageCollection(handle) : nullptr);
        cacheOp.DropPageItemRef(handle);
        if (state.IsPageKind()) {
            handle.Key0.store(0, std::memory_order_relaxed);
            handle.Body.Fetch = nullptr;
        }
        state = state.WithState(EHandleState::Begin).WithFrequency(0).WithSticky(EStickyState::None);
        handle.State.store(state.Raw(), std::memory_order_relaxed);
    }
    if (pending) {
        const bool page = state.IsPageKind();
        if (page) {
            cacheOp.DropPageItemRef(handle);
            handle.Key0.store(0, std::memory_order_relaxed);
            if (state.IsStickyField()) {
                Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, PageBytes(handle));
            }
            DeletePendingFetch(handle);
        }
        if (reserved) {
            ReleaseReservation(reservedBytes, page ? 1 : 0);
        }
        state = state.WithState(EHandleState::Begin).WithFrequency(0).WithSticky(EStickyState::None);
        handle.State.store(state.Raw(), std::memory_order_release);
    }
    if (!spaceOp.TryStoreSpareItem(cacheItem)) {
        cacheOp.DiscardCandidate(cacheItem, state.Kind());
    }
}

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCache::Allocate(TSpaceOperation& spaceOp) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const ui32 index = cacheOp.TryAllocateHandle();
    if (!index) {
        return {};
    }
    const THandleState state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(state.IsBegin() && state.Refs() == 0);
    spaceOp.Handles()[index].Key0.store(0, std::memory_order_relaxed);
    spaceOp.Handles()[index].Body.Fetch = nullptr;
    return TCacheItem::Make(state.Version(), index);
}

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCache::TryAllocateReservedItem(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (!cacheOp.TryReserve(bytes, pages)) {
        return {};
    }
    const TCacheItem cacheItem = cacheOp.Allocate();
    if (cacheItem.IsNull()) {
        ReleaseReservation(bytes, pages);
    }
    return cacheItem;
}

SHARED_CACHE_TEMPLATE
TPageCacheItem TSharedCache::AllocatePage(TCollectionCacheItem collection, ui64 offset, ui64 size,
    NTable::NPage::EPage type, ui32 crc32, EStickyState sticky) noexcept {
    Y_DEBUG_ABORT_UNLESS(type != NTable::NPage::EPage::Undef);
    Y_DEBUG_ABORT_UNLESS(sticky == EStickyState::None || sticky == EStickyState::Sticky);
    const ui64 bytes = AccountedPageBytes(size);
    auto cacheOp = BeginOperation();
    const TCacheItem cacheItem = cacheOp.TryAllocateReservedItem(bytes, 1);
    if (!cacheItem.IsNull()) {
        TCollection* owner = cacheOp.PageCollection(collection);
        const ECacheMode admissionMode =
            owner ? owner->CacheMode_.load(std::memory_order_acquire) : ECacheMode::Regular;
        const EStickyState pageSticky = admissionMode == ECacheMode::TryKeepInMemory ? EStickyState::None : sticky;
        cacheOp.InitializePage(cacheItem, collection, offset, size, type, crc32, pageSticky);
        cacheOp.RefillSpareItem();
    }
    return TPageCacheItem::FromValidated(cacheItem);
}

SHARED_CACHE_TEMPLATE
TIntrusivePtr<TPageFetchState> TSharedCache::TryAcquirePageFetch(
    TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef& owner) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (cacheOp.TryAcquire(page.CacheItem(), true, owner.Get()) != EAcquireStatus::Pending) {
        return {};
    }
    TIntrusivePtr<TPageFetchState> fetch = spaceOp.Handles()[page.Index()].Body.Fetch;
    Y_DEBUG_ABORT_UNLESS(fetch);
    return fetch;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(TPageCacheItem page, NActors::TSharedData&& data) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef owner(cacheOp);
    TIntrusivePtr<TPageFetchState> fetch = cacheOp.TryAcquirePageFetch(page, owner);
    if (!fetch) {
        return false;
    }
    return CompleteFetch(owner, *fetch, std::move(data));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FailReady(TPageCacheItem page) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef owner(cacheOp);
    TIntrusivePtr<TPageFetchState> fetch = cacheOp.TryAcquirePageFetch(page, owner);
    if (!fetch) {
        return false;
    }
    return FailFetch(owner, *fetch);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
    return MakeReady(*CollectionRegistry_, collection, std::move(value));
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
void TSharedCache::PublishCollectionForReclaim(TSpaceOperation& spaceOp, TCacheItem collectionItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind() || !expected.IsSticky() ||
            !expected.IsStickyNoneField() || handle.Body.Collection->Registry_.load(std::memory_order_acquire) ||
            handle.Body.Collection->References.load(std::memory_order_acquire) != 0)
        {
            return;
        }
        const THandleState desired = expected.WithState(EHandleState::Hot).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            Memory_.Sticky().TransferBytesTo(Memory_.Hot(), PayloadBytes(handle, EItemKind::Collection));
            InvokeHook(ESharedCacheHookPoint::BeforeCollectionHotPublished, collectionItem);
            cacheOp.RouteHot(EHotLevel::L1, collectionItem);
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::FinishCollectionRetain(TSpaceOperation& spaceOp, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    if (handle.Body.Collection->Registry_.load(std::memory_order_acquire)) {
        return;
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() && expected.IsSticky());
        if (expected.IsStickyField()) {
            return;
        }
        const THandleState desired = expected.WithSticky(EStickyState::Sticky).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DetachCollection(TCollectionCacheItem collection) noexcept {
    return DetachCollection(*CollectionRegistry_, collection);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DetachCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef collectionRef(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    return cacheOp.DetachCollection(registry, collection.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UnlinkCollectionRegistry(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.UnlinkCollectionRegistry(registry, collection.CacheItem());
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
    TCollection* value = cacheOp.PageCollection(collection);
    if (!value || !cacheOp.SetCollectionPagesCacheMode(*value, ECacheMode::Regular)) {
        return false;
    }
    return cacheOp.DeleteCollection(registry, collection.CacheItem());
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
    return cacheOp.SetCollectionPagesCacheMode(*handle.Body.Collection, mode);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::SetCollectionSkipBTreeIndexV1Shadow(TCollectionCacheItem collection, bool skip) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef ref(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (!ref) {
        return false;
    }
    TCollection* owner = cacheOp.PageCollection(collection);
    if (!owner) {
        return false;
    }
    SetCollectionSkipBTreeIndexV1Shadow(*owner, skip);
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::SetCollectionSkipBTreeIndexV1Shadow(TCollection& owner, bool skip) noexcept {
    owner.PageCollection()->SetSkipBTreeIndexV1Shadow(skip);
    PublishKeepPageBytes(owner);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AcknowledgeCollectionBytes(TCollectionCacheItem collection) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef ref(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    if (auto* owner = ref ? cacheOp.PageCollection(collection) : nullptr) {
        // Clear before the actor samples bytes, so a racing update can schedule the next notification.
        owner->BytesNotificationPending_.store(false, std::memory_order_release);
        return true;
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MatchesPendingFetch(
    const THandle& handle, THandleState state, TCacheItem cacheItem, const TPageFetchState& fetch) noexcept {
    return cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() && handle.Body.Fetch == &fetch &&
           fetch.Page().CacheItem() == cacheItem;
}

SHARED_CACHE_TEMPLATE
THandle* TSharedCache::TryGetPendingFetchHandle(TOperationItemRef& owner, TPageFetchState& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(owner);
    TSpaceOperation& spaceOp = owner.SpaceOperation();
    const TCacheItem cacheItem = owner.CacheItem();
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AccessibleHandleCount());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!MatchesPendingFetch(handle, state, cacheItem, fetch) || fetch.Completion() != EPageFetchCompletion::Pending)
    {
        return nullptr;
    }
    return &handle;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::CompleteFetch(
    TOperationItemRef& owner, TPageFetchState& fetch, NActors::TSharedData&& data) noexcept {
    Y_DEBUG_ABORT_UNLESS(data);
    THandle* handle = TryGetPendingFetchHandle(owner, fetch);
    if (!handle || THandleState::FromRaw(handle->State.load(std::memory_order_acquire)).IsWaitingForMemory()) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(handle->Key2OrSize.load(std::memory_order_relaxed) == data.size());
    fetch.SetReady(std::move(data));
    // Drop the I/O-owner ref; the last pending releaser performs MakeReadyState.
    ReleaseFetchRef(owner, fetch);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReserveFetchMemory(TSpaceOperation& spaceOp, TCacheItem item, TPageFetchState& fetch) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[item.Index()];
    ui64 raw = handle.State.load(std::memory_order_acquire);
    THandleState state = THandleState::FromRaw(raw);
    if (!MatchesPendingFetch(handle, state, item, fetch)) {
        return false;
    }
    if (!state.IsWaitingForMemory()) {
        return true;
    }
    const ui64 bytes = PageBytes(handle);
    if (!cacheOp.TryReserve(bytes, 1)) {
        return false;
    }
    // Only the move-only fetch owner reserves memory. Subscribers may change refs/Sticky concurrently.
    for (;;) {
        state = THandleState::FromRaw(raw);
        if (!MatchesPendingFetch(handle, state, item, fetch) || !state.IsWaitingForMemory()) {
            ReleaseReservation(bytes, 1);
            return false;
        }
        if (handle.State.compare_exchange_weak(raw, state.WithState(EHandleState::Begin).Raw(),
                std::memory_order_release, std::memory_order_relaxed)) {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryReserveFetchBatch(TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept {
    auto cacheOp = BeginOperation();
    TVector<TPageFetchState*> waiting;
    ui64 bytes = 0;
    for (auto& page : pages) {
        TPageFetchState& fetch = *page.FetchState_;
        const TCacheItem item = fetch.Page().CacheItem();
        THandle& handle = cacheOp.Handles()[item.Index()];
        const auto state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_ABORT_UNLESS(MatchesPendingFetch(handle, state, item, fetch));
        if (state.IsWaitingForMemory()) {
            bytes += PageBytes(handle);
            waiting.push_back(&fetch);
        }
    }
    // Reserve the missing batch as a whole; two non-transactional requests must not each
    // hold a partial batch while waiting for the other one's bytes.
    if (!cacheOp.TryReserve(bytes, waiting.size())) {
        return false;
    }
    for (auto* fetch : waiting) {
        const TCacheItem item = fetch->Page().CacheItem();
        THandle& handle = cacheOp.Handles()[item.Index()];
        ui64 raw = handle.State.load(std::memory_order_relaxed);
        for (;;) {
            const auto state = THandleState::FromRaw(raw);
            Y_ABORT_UNLESS(MatchesPendingFetch(handle, state, item, *fetch) && state.IsWaitingForMemory());
            if (handle.State.compare_exchange_weak(raw, state.WithState(EHandleState::Begin).Raw(),
                    std::memory_order_release, std::memory_order_relaxed)) {
                break;
            }
        }
    }
    return true;
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
            const ui64 charge = AccountedPageBytes(location.Size);
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
bool TSharedCache::DispatchFetch(TSpaceOperation& spaceOp, TCacheItem cacheItem, TPageFetchState& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!MatchesPendingFetch(handle, expected, cacheItem, fetch)) {
            return false;
        }
        if (expected.IsRequested() || expected.IsQueuedRequested()) {
            return true;
        }
        if (!expected.IsBegin() && !expected.IsQueued()) {
            return false;
        }
        const EHandleState requested = expected.IsQueued() ? EHandleState::QueuedRequested : EHandleState::Requested;
        const THandleState desired = expected.WithState(requested);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FailFetch(TOperationItemRef& owner, TPageFetchState& fetch) noexcept {
    if (!TryGetPendingFetchHandle(owner, fetch)) {
        return false;
    }
    fetch.SetFailed();
    ReleaseFetchRef(owner, fetch);
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::FinishFailedFetch(
    TOperationItemRef& finalizerRef, TPageFetchState& fetch, TPageFetchWaiter* waiters, bool reserved) noexcept {
    TSpaceOperation& spaceOp = finalizerRef.SpaceOperation();
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem cacheItem = finalizerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    THandleState completing = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(completing) && completing.IsCompleting() && completing.Refs() == 1 &&
                         handle.Body.Fetch == &fetch);
    if (completing.IsStickyField()) {
        cacheOp.UnlinkPendingStickyPage(cacheItem);
        completing = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    }
    DeletePendingFetch(handle);
    new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer();
    if (reserved) {
        ReleaseReservation(PageBytes(handle), 1);
    }
    const bool begun = cacheOp.TryBeginErase(finalizerRef.Get(), completing);
    Y_DEBUG_ABORT_UNLESS(begun);
    InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
    const TSharedCacheKey key = LoadSharedCacheKey(handle, EItemKind::Page);
    cacheOp.FinishErase(cacheItem, key);
    TPageFetchState::DrainWaiters(waiters, [cacheItem](TIntrusivePtr<TPageFetchWaiter> waiter) {
        waiter->Complete(TPageCacheItem::FromValidated(cacheItem), EPageFetchCompletion::Failed);
    });
}

SHARED_CACHE_TEMPLATE
void TSharedCache::FinishFetch(TOperationItemRef& finalizerRef, TPageFetchState& fetch, bool reserved) noexcept {
    TSpaceOperation& spaceOp = finalizerRef.SpaceOperation();
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem page = finalizerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[page.Index()];
    const THandleState completing = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(
        page.Matches(completing) && completing.IsCompleting() && completing.Refs() == 1 && handle.Body.Fetch == &fetch);
    TPageFetchWaiter* waiters = fetch.DetachWaiters();
    Y_DEBUG_ABORT_UNLESS(waiters != TPageFetchState::Sealed);
    if (fetch.Completion() == EPageFetchCompletion::Failed) {
        FinishFailedFetch(finalizerRef, fetch, waiters, reserved);
        return;
    }

    Y_DEBUG_ABORT_UNLESS(reserved && fetch.Completion() == EPageFetchCompletion::Ready);
    {
        InvokeHook(ESharedCacheHookPoint::BeforePageReadyState, page);
        NActors::TSharedData data = fetch.TakeData();
        DeletePendingFetch(handle);
        new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer(std::move(data).Extract());
        const ui64 bytes = PageBytes(handle);
        EHandleState readyState = EHandleState::Hot;
        TCollection* owner = cacheOp.PageCollection(handle);
        const bool regular = owner && owner->CacheMode_.load(std::memory_order_acquire) == ECacheMode::Regular &&
                             !THandleState::FromRaw(handle.State.load(std::memory_order_acquire)).IsStickyField();
        if (regular) {
            if (!Memory_.TryAddRetainedBytes(bytes)) {
                readyState = EHandleState::Cold;
            }
        } else {
            Memory_.AddRetainedBytes(bytes);
        }
        if (owner) {
            AddResidentPageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
        }
        Memory_.CompleteReservedPage();
        cacheOp.MakeReadyState(page, readyState, owner);
    }
    TPageFetchState::DrainWaiters(waiters, [page](TIntrusivePtr<TPageFetchWaiter> waiter) {
        waiter->Complete(TPageCacheItem::FromValidated(page), EPageFetchCompletion::Ready);
    });
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseFetchRef(TOperationItemRef& owner, TPageFetchState& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(owner);
    TSpaceOperation& spaceOp = owner.SpaceOperation();
    const TCacheItem cacheItem = owner.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(MatchesPendingFetch(handle, expected, cacheItem, fetch) && expected.Refs() > 0);
        const bool finish = expected.Refs() == 1 && fetch.Completion() != EPageFetchCompletion::Pending;
        const THandleState desired =
            finish ? expected.WithState(EHandleState::Completing).WithFrequency(0) : expected.DecrementRefs();
        const std::memory_order successOrder = finish ? std::memory_order_acq_rel : std::memory_order_release;
        if (!handle.State.compare_exchange_weak(expectedRaw, desired.Raw(), successOrder, std::memory_order_relaxed)) {
            continue;
        }
        if (finish) {
            FinishFetch(owner, fetch, !expected.IsWaitingForMemory());
        } else {
            // The CAS above already decremented this ref.
            owner.Disarm();
        }
        return;
    }
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
    CollectionBytes_.fetch_add(bytes, std::memory_order_relaxed);
    Collections_.fetch_add(1, std::memory_order_relaxed);
    handle.Body.Collection = value.Get();
    value->CacheItem = TCollectionCacheItem::FromValidated(cacheItem);
    AddReference(*value);
    if (registry) {
        Y_ABORT_UNLESS(cacheOp.LinkCollection(*registry, cacheItem));
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

    const ui64 bytes = PayloadBytes(handle, initial.Kind());
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
        AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
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
                        AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
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
                    AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
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
                cacheOp.RouteHot(EHotLevel::L1, TCacheItem::Make(expected.Version(), index));
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
        cacheOp.InitializeCollection(cacheItem, id, bytes);
        cacheOp.RefillSpareItem();
    }
    return TCollectionCacheItem::FromValidated(cacheItem);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::InitializePage(TSpaceOperation& spaceOp, TCacheItem cacheItem, TCollectionCacheItem collection,
    ui64 offset, ui64 size, NTable::NPage::EPage type, ui32 crc32, EStickyState sticky, bool reserved,
    bool collectionOwnerHeld) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AccessibleHandleCount());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState claimed = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(claimed) && claimed.IsBegin() && claimed.Refs() == 0);
    handle.Key0.store(collection.Raw(), std::memory_order_relaxed);
    handle.Key1.store(offset, std::memory_order_relaxed);
    handle.Key2OrSize.store(size, std::memory_order_relaxed);
    handle.Metadata.Page.Type = type;
    handle.Metadata.Page.Reserved = 0;
    handle.Metadata.Page.Crc32 = crc32;
    Y_DEBUG_ABORT_UNLESS(!handle.Body.Fetch);
    if (TCollection* owner = cacheOp.PageCollection(handle)) {
        const THandle& collection = spaceOp.Handles()[owner->CacheItem.CacheItem().Index()];
        const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
        Y_ABORT_UNLESS(state.IsSticky() && (state.IsStickyField() || collectionOwnerHeld));
        InvokeHook(ESharedCacheHookPoint::AfterPageCollectionStateChecked, owner->CacheItem.CacheItem());
        const ui64 previous = owner->References.fetch_add(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
    }
    handle.Body.Fetch = new TPageFetchState(TPageCacheItem::FromValidated(cacheItem));
    handle.Body.Fetch->Ref();
    handle.Next.store(TCacheItem::Make(claimed.Version(), 0).Raw(), std::memory_order_relaxed);
    handle.NextInOwner.store(0, std::memory_order_relaxed);
    if (sticky == EStickyState::Sticky) {
        Memory_.AddStickyOwnedBytes(AccountedPageBytes(size));
    }
    const THandleState initialized = claimed.WithState(reserved ? EHandleState::Begin : EHandleState::WaitingForMemory)
                                         .WithKind(EItemKind::Page)
                                         .WithFrequency(0)
                                         .WithSticky(sticky);
    handle.State.store(initialized.Raw(), std::memory_order_relaxed);
    if (sticky == EStickyState::Sticky) {
        TOperationItemRef collectionRef(spaceOp);
        cacheOp.LinkStickyPage(cacheItem, collectionRef.Get());
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::InitializeCollection(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept {
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AccessibleHandleCount());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState claimed = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(claimed) && claimed.IsBegin() && claimed.Refs() == 0);
    const ui64* words = id.GetRaw();
    handle.Key0.store(words[0], std::memory_order_relaxed);
    handle.Key1.store(words[1], std::memory_order_relaxed);
    handle.Key2OrSize.store(words[2], std::memory_order_relaxed);
    new (&handle.Body.Collection) TCollection*(nullptr);
    handle.Metadata.PayloadBytes = bytes;
    handle.Next.store(TCacheItem::Make(claimed.Version(), 0).Raw(), std::memory_order_relaxed);
    handle.NextInOwner.store(0, std::memory_order_relaxed);
    const THandleState initialized =
        claimed.WithKind(EItemKind::Collection).WithFrequency(0).WithSticky(EStickyState::Sticky);
    handle.State.store(initialized.Raw(), std::memory_order_relaxed);
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
    const ui64 reservedBytes = state.IsPending() && !state.IsWaitingForMemory() ? PayloadBytes(handle, kind) : 0;
    cacheOp.DiscardCandidate(cacheItem, kind, reservedBytes);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DiscardCandidate(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const ui32 index = cacheItem.Index();
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AccessibleHandleCount());

    THandle& handle = spaceOp.Handles()[index];
    THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (expected.IsPageKind() && expected.IsPending() && expected.IsStickyField()) {
        cacheOp.UnlinkPendingStickyPage(cacheItem);
        expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    }
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) && expected.Kind() == kind && !expected.IsFree() &&
                         !expected.IsTombstone() && expected.Refs() == 0);

    Y_DEBUG_ABORT_UNLESS(!expected.IsPageKind() || !expected.IsSticky());
    Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() || !expected.IsReady() ||
                         handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) == 0);
    Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() || !expected.IsReady() ||
                         handle.Body.Collection->Registry_.load(std::memory_order_acquire) == nullptr);

    const THandleState desired = expected.WithState(EHandleState::Free).WithFrequency(0).WithSticky(EStickyState::None);
    TCollection* owner = nullptr;
    if (expected.IsPageKind() && handle.Body.Collection) {
        owner = cacheOp.PageCollection(handle);
    }
    UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired);
    handle.State.store(desired.Raw(), std::memory_order_release);
    if (expected.IsPageKind() && expected.IsStickyField() && !expected.IsReady()) {
        Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, PageBytes(handle));
    }
    if (expected.IsReady()) {
        DeletePayload(handle, expected.Kind(), expected.State(), owner);
    } else if (expected.IsPageKind() && expected.IsPending()) {
        DeletePendingFetch(handle);
    }
    if (owner) {
        cacheOp.DropOwnerReference(*owner);
    }
    if (reservedBytes != 0) {
        ReleaseReservation(reservedBytes, expected.IsPageKind() ? 1 : 0);
    }
    Y_ABORT_UNLESS(spaceOp.ReturnFreeHandle(index, desired.Version()));
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
        UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired);
        if (expected.IsKeepCold()) {
            AdvanceColdMembership(handle);
            const ui64 bytes = PageBytes(handle);
            Memory_.KeepCold().TransferBytesTo(Memory_.Hot(), bytes);
            Memory_.KeepCold().TransferPageTo(Memory_.Hot(), EItemKind::Page);
            Memory_.ReleaseKeepColdOwnedBytes(bytes);
            cacheOp.RouteHot(EHotLevel::L1, cacheItem);
        } else if (expected.IsCold() && expected.IsPageKind()) {
            if (cacheOp.PromoteCold(cacheItem)) {
                cacheOp.RouteHot(EHotLevel::L1, cacheItem);
            } else {
                AdvanceColdMembership(handle);
            }
        }
        ref = TSharedCacheItemRef(cacheItem);
        return pending ? EAcquireStatus::Pending : EAcquireStatus::Ready;
    }
}

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef TSharedCache::TryAcquireStructural(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen())
    {
        return {};
    }

    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || expected.IsFree() || expected.IsReplacing() || expected.IsReplaced() ||
            expected.IsTombstone())
        {
            return {};
        }

        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs();
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
        {
            UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired);
            return TSharedCacheItemRef(cacheItem);
        }
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
TSharedCacheItemRef TSharedCache::AcquireStickyCollection(
    TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem, TCollection*& collectionValue) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    collectionValue = nullptr;
    TOperationItemRef collectionRef(spaceOp, cacheOp.TryAcquireStructural(collectionItem.CacheItem()));
    if (!collectionRef) {
        return {};
    }

    THandle& collection = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
    if (!collectionItem.Matches(state) || !state.IsSticky() || !state.IsStickyField() || !collection.Body.Collection)
    {
        return {};
    }
    collectionValue = collection.Body.Collection;
    return collectionRef.Take();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::SetCollectionPagesCacheMode(TSpaceOperation& spaceOp, TCollection& value, ECacheMode mode) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TCollectionCacheItem collection = value.CacheItem;
    THandle& handle = spaceOp.Handles()[collection.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collection.Matches(state) || handle.Body.Collection != &value) {
        return false;
    }
    const bool attached = state.IsSticky() && state.IsStickyField();
    const bool detached = (state.IsHot() || state.IsCold()) && state.IsStickyNoneField() &&
                          !value.Registry_.load(std::memory_order_acquire);
    if (!attached && !detached) {
        return false;
    }
    if (mode == ECacheMode::Sticky && !attached) {
        return false;
    }
    const ECacheMode previousMode = value.CacheMode_.load(std::memory_order_acquire);
    if (previousMode == mode) {
        return true;
    }
    const bool wasKeep = previousMode == ECacheMode::TryKeepInMemory;
    const bool keep = mode == ECacheMode::TryKeepInMemory;
    if (wasKeep != keep) {
        value.KeepGeneration_.fetch_add(1, std::memory_order_acq_rel);
    }
    value.CacheMode_.store(mode, std::memory_order_release);
    PublishKeepPageBytes(value);
    if (wasKeep && !keep) {
        KeepModeSweepRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
    }
    if (keep || (previousMode == ECacheMode::Sticky && mode == ECacheMode::Regular)) {
        // Publish the destination mode before withdrawing the previous Sticky ownership.
        cacheOp.DrainStickyPages(value);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RestoreStickyPageList(
    TSpaceOperation& spaceOp, TCollection& collection, ui32 detachedHead) noexcept {
    ui32 tail = detachedHead;
    while (const ui32 next = spaceOp.Handles()[tail].NextInOwner.load(std::memory_order_acquire)) {
        tail = next;
    }
    ui32 concurrentHead = collection.StickyPageListHead.load(std::memory_order_relaxed);
    do {
        spaceOp.Handles()[tail].NextInOwner.store(concurrentHead, std::memory_order_release);
    } while (!collection.StickyPageListHead.compare_exchange_weak(
        concurrentHead, detachedHead, std::memory_order_release, std::memory_order_relaxed));
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DrainStickyPages(TSpaceOperation& spaceOp, TCollection& value) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TCollectionCacheItem collectionItem = value.CacheItem;
    if (value.CacheMode_.load(std::memory_order_acquire) == ECacheMode::Sticky) {
        return;
    }
    ui32 current = value.StickyPageListHead.exchange(0, std::memory_order_acq_rel);
    if (current == 0) {
        return;
    }
    // A page linked after this exchange checks the mode and drains its own list.

    InvokeHook(ESharedCacheHookPoint::AfterStickyPageListDetached, collectionItem.CacheItem());
    if (value.CacheMode_.load(std::memory_order_acquire) == ECacheMode::Sticky) {
        cacheOp.RestoreStickyPageList(value, current);
        return;
    }

    while (current != 0) {
        Y_DEBUG_ABORT_UNLESS(current < spaceOp.AccessibleHandleCount());
        THandle& page = spaceOp.Handles()[current];
        const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
        page.NextInOwner.store(0, std::memory_order_release);

        ui64 expectedRaw = page.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState expected = THandleState::FromRaw(expectedRaw);
            const bool pending = expected.IsPending() || expected.IsCompleting();
            const bool resident = expected.IsSticky();
            const bool reserved = expected.IsHot() && expected.IsStickyField();
            Y_DEBUG_ABORT_UNLESS(expected.IsPageKind() &&
                                 (((pending || resident) && expected.IsStickyField()) || reserved) &&
                                 page.Key0.load(std::memory_order_relaxed) == collectionItem.Raw());

            const ui64 bytes = PageBytes(page);
            THandleState desired = expected.WithSticky(EStickyState::None);
            if (!pending) {
                desired = desired.WithFrequency(0);
                if (resident) {
                    desired = desired.WithState(EHandleState::Hot);
                }
            }
            if (!page.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                continue;
            }

            if (!pending) {
                Memory_.Sticky().TransferBytesTo(Memory_.Hot(), bytes);
                Memory_.Sticky().TransferPageTo(Memory_.Hot(), EItemKind::Page);
            }
            Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
            if (!pending) {
                cacheOp.RouteHot(EHotLevel::L1, TCacheItem::Make(expected.Version(), current));
            }
            break;
        }
        current = next;
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::UnlinkPendingStickyPage(TSpaceOperation& spaceOp, TCacheItem pageItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& target = spaceOp.Handles()[pageItem.Index()];
    TCollection* collection = cacheOp.PageCollection(target);
    Y_ABORT_UNLESS(collection);
    const ui64 bytes = PageBytes(target);
    for (;;) {
        const THandleState state = THandleState::FromRaw(target.State.load(std::memory_order_acquire));
        if (!state.IsStickyField()) {
            return;
        }
        Y_DEBUG_ABORT_UNLESS(pageItem.Matches(state) && (state.IsPending() || state.IsCompleting()));
        const ECacheMode mode = collection->CacheMode_.load(std::memory_order_acquire);
        ui32 current = collection->StickyPageListHead.exchange(0, std::memory_order_acq_rel);
        ui32 retainedHead = 0;
        ui32 retainedTail = 0;
        bool found = false;
        while (current != 0) {
            THandle& page = spaceOp.Handles()[current];
            const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
            if (current == pageItem.Index()) {
                page.NextInOwner.store(0, std::memory_order_release);
                found = true;
            } else {
                if (retainedTail == 0) {
                    retainedTail = current;
                }
                page.NextInOwner.store(retainedHead, std::memory_order_relaxed);
                retainedHead = current;
            }
            current = next;
        }
        if (retainedTail != 0) {
            cacheOp.MergeRetainedStickyPages(*collection, retainedHead, retainedTail);
            const THandleState owner = THandleState::FromRaw(
                spaceOp.Handles()[collection->CacheItem.Index()].State.load(std::memory_order_acquire));
            if (!owner.IsStickyField() || collection->CacheMode_.load(std::memory_order_acquire) != mode) {
                cacheOp.DrainStickyPages(*collection);
            }
        }
        if (found) {
            ui64 expectedRaw = target.State.load(std::memory_order_relaxed);
            for (;;) {
                const THandleState expected = THandleState::FromRaw(expectedRaw);
                if (!expected.IsStickyField()) {
                    return;
                }
                if (target.State.compare_exchange_weak(expectedRaw, expected.WithSticky(EStickyState::None).Raw(),
                        std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
                    return;
                }
            }
        }
        // Another list traversal temporarily owns this node. It will withdraw it or restore it to the head.
        SpinLockPause();
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::LinkStickyPage(
    TSpaceOperation& spaceOp, TCacheItem pageItem, TSharedCacheItemRef& collectionRef) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(pageItem.Matches(state) && state.IsPageKind() &&
                         ((state.IsStickyField() && (state.IsPending() || state.IsCompleting() || state.IsHot())) ||
                             (state.IsSticky() && state.IsStickyField())));
    Y_DEBUG_ABORT_UNLESS(page.NextInOwner.load(std::memory_order_relaxed) == 0);

    const TCollectionCacheItem collectionItem =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed)));
    TCollection* collectionValue = nullptr;
    collectionRef = cacheOp.AcquireStickyCollection(collectionItem, collectionValue);
    const ECacheMode modeAtAdmission =
        collectionValue ? collectionValue->CacheMode_.load(std::memory_order_acquire) : ECacheMode::TryKeepInMemory;
    const auto allowsSticky = [&] {
        if (!collectionRef) {
            return false;
        }
        const ECacheMode mode = collectionValue->CacheMode_.load(std::memory_order_acquire);
        const THandleState collectionState =
            THandleState::FromRaw(spaceOp.Handles()[collectionItem.Index()].State.load(std::memory_order_acquire));
        return collectionState.IsSticky() && collectionState.IsStickyField() && mode != ECacheMode::TryKeepInMemory;
    };
    if (!allowsSticky())
    {
        if (state.IsSticky()) {
            // A replacement borrows the old page's sticky charge until it is published.
            return false;
        }
        ui64 expectedRaw = page.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState expected = THandleState::FromRaw(expectedRaw);
            Y_DEBUG_ABORT_UNLESS(
                pageItem.Matches(expected) && expected.IsPageKind() &&
                ((expected.IsPending() || expected.IsCompleting() || expected.IsHot()) && expected.IsStickyField()));
            const THandleState desired = expected.WithSticky(EStickyState::None);
            if (page.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
            {
                if (expected.IsHot()) {
                    Memory_.Sticky().TransferBytesTo(Memory_.Hot(), PageBytes(page));
                    Memory_.Sticky().TransferPageTo(Memory_.Hot(), EItemKind::Page);
                    cacheOp.RouteHot(EHotLevel::L1, pageItem);
                }
                Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, PageBytes(page));
                return true;
            }
        }
    }

    InvokeHook(ESharedCacheHookPoint::BeforeStickyPageListLink, pageItem);
    ui32 head = collectionValue->StickyPageListHead.load(std::memory_order_relaxed);
    for (;;) {
        page.NextInOwner.store(head, std::memory_order_release);
        if (collectionValue->StickyPageListHead.compare_exchange_weak(
                head, pageItem.Index(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            break;
        }
    }
    InvokeHook(ESharedCacheHookPoint::AfterStickyPageLinked, pageItem);
    if (!allowsSticky() || collectionValue->CacheMode_.load(std::memory_order_acquire) != modeAtAdmission)
    {
        cacheOp.DrainStickyPages(*collectionValue);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakePageSticky(TSpaceOperation& spaceOp, TCacheItem pageItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    TCollection* collection = cacheOp.PageCollection(page);
    if (!collection) {
        return false;
    }
    const ECacheMode admissionMode = collection->CacheMode_.load(std::memory_order_acquire);
    if (admissionMode == ECacheMode::TryKeepInMemory) {
        return false;
    }
    const ui64 bytes = PageBytes(page);
    if (THandleState::FromRaw(page.State.load(std::memory_order_acquire)).IsCold()) {
        cacheOp.PromoteCold(pageItem, true);
    }
    ui64 expectedRaw = page.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!pageItem.Matches(expected) || !expected.IsPageKind()) {
            return false;
        }
        if (expected.IsSticky() && expected.IsStickyField()) {
            return true;
        }
        const bool pending = expected.IsPending();
        if (pending && expected.IsStickyField()) {
            return true;
        }
        if ((!expected.IsHot() && !pending) || !expected.IsStickyNoneField()) {
            return false;
        }
        Memory_.AddStickyOwnedBytes(bytes);
        const THandleState claimed = expected.WithSticky(EStickyState::Sticky);
        if (page.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            if (pending) {
                TOperationItemRef collectionRef(spaceOp);
                cacheOp.LinkStickyPage(pageItem, collectionRef.Get());
                return THandleState::FromRaw(page.State.load(std::memory_order_acquire)).IsStickyField();
            }
            break;
        }
        Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
    }

    Memory_.Hot().TransferBytesTo(Memory_.Sticky(), bytes);
    Memory_.Hot().TransferPageTo(Memory_.Sticky(), EItemKind::Page);
    TOperationItemRef collectionRef(spaceOp);
    if (!cacheOp.LinkStickyPage(pageItem, collectionRef.Get())) {
        ui64 claimedRaw = page.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState claimed = THandleState::FromRaw(claimedRaw);
            Y_DEBUG_ABORT_UNLESS(pageItem.Matches(claimed) && claimed.IsHot() && claimed.IsStickyField());
            if (page.State.compare_exchange_weak(claimedRaw, claimed.WithSticky(EStickyState::None).Raw(),
                    std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                Memory_.Sticky().TransferBytesTo(Memory_.Hot(), bytes);
                Memory_.Sticky().TransferPageTo(Memory_.Hot(), EItemKind::Page);
                Memory_.ReleaseStickyOwnedBytes(EItemKind::Page, bytes);
                cacheOp.RouteHot(EHotLevel::L1, pageItem);
                return false;
            }
        }
    }

    ui64 claimedRaw = page.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState claimed = THandleState::FromRaw(claimedRaw);
        if (!pageItem.Matches(claimed) || !claimed.IsHot() || !claimed.IsStickyField()) {
            return claimed.IsSticky() && claimed.IsStickyField();
        }
        const THandleState desired = claimed.WithState(EHandleState::Sticky).WithSticky(EStickyState::Sticky);
        if (page.State.compare_exchange_weak(
                claimedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::PublishPageUnsticky(THandle& page, THandleState state) noexcept {
    ui64 expectedRaw = state.Raw();
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(expected.Version() == state.Version() && expected.IsPageKind() && expected.IsSticky() &&
                             expected.IsStickyField());
        const THandleState desired = expected.WithSticky(EStickyState::Transition);
        if (page.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::MergeRetainedStickyPages(
    TSpaceOperation& spaceOp, TCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept {
    Y_DEBUG_ABORT_UNLESS(retainedHead != 0 && retainedTail != 0);
    ui32 concurrentHead = collection.StickyPageListHead.load(std::memory_order_relaxed);
    do {
        spaceOp.Handles()[retainedTail].NextInOwner.store(concurrentHead, std::memory_order_release);
    } while (!collection.StickyPageListHead.compare_exchange_weak(
        concurrentHead, retainedHead, std::memory_order_release, std::memory_order_relaxed));
    InvokeHook(ESharedCacheHookPoint::AfterStickyPageListHeadPublished, collection.CacheItem.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UnstickyCutPages(TSpaceOperation& spaceOp, TCacheItem pageItem, ui64 allocationLimit) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& trigger = spaceOp.Handles()[pageItem.Index()];
    const TCollectionCacheItem collectionItem =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(trigger.Key0.load(std::memory_order_relaxed)));
    TCollection* collectionValue = nullptr;
    TOperationItemRef collectionRef(spaceOp, cacheOp.AcquireStickyCollection(collectionItem, collectionValue));
    if (!collectionRef) {
        return false;
    }

    const ui32 detachedHead = collectionValue->StickyPageListHead.exchange(0, std::memory_order_acq_rel);
    InvokeHook(ESharedCacheHookPoint::AfterStickyPageListDetached, collectionItem.CacheItem());

    ui32 current = detachedHead;
    ui32 retainedHead = 0;
    ui32 retainedTail = 0;
    while (current != 0) {
        Y_DEBUG_ABORT_UNLESS(current < spaceOp.AccessibleHandleCount());
        THandle& page = spaceOp.Handles()[current];
        const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
        const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(state.IsPageKind() && (state.IsPending() || state.IsCompleting() || state.IsSticky()) &&
                             state.IsStickyField() &&
                             page.Key0.load(std::memory_order_relaxed) == collectionItem.Raw());

        if (current < allocationLimit || state.IsPending() || state.IsCompleting()) {
            if (retainedTail == 0) {
                retainedTail = current;
            }
            page.NextInOwner.store(retainedHead, std::memory_order_relaxed);
            retainedHead = current;
        } else {
            page.NextInOwner.store(0, std::memory_order_release);
            PublishPageUnsticky(page, state);
        }
        current = next;
    }
    if (retainedTail != 0) {
        cacheOp.MergeRetainedStickyPages(*collectionValue, retainedHead, retainedTail);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::LinkCollection(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(collectionItem.Matches(state) && state.IsCollectionKind());
    Y_DEBUG_ABORT_UNLESS((state.IsBegin() && state.IsStickyField()) ||
                         ((state.IsHot() || state.IsCold()) && state.IsTransitionField()) ||
                         (state.IsSticky() && (state.IsStickyField() || state.IsTransitionField())));
    Y_DEBUG_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_relaxed) == 0);
    Y_DEBUG_ABORT_UNLESS(handle.Body.Collection);

    TCollectionRegistry* expectedRegistry = nullptr;
    if (!handle.Body.Collection->Registry_.compare_exchange_strong(
            expectedRegistry, &registry, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        return false;
    }

    ui32 head = registry.CollectionListHead.load(std::memory_order_relaxed);
    for (;;) {
        handle.NextInOwner.store(head, std::memory_order_release);
        if (registry.CollectionListHead.compare_exchange_weak(
                head, collectionItem.Index(), std::memory_order_release, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryBeginCollectionAttach(TSpaceOperation& spaceOp, TCollectionRegistry& registry,
    TCacheItem collectionItem, THandleState& attachState) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind()) {
            return false;
        }
        if (expected.IsSticky() && expected.IsStickyField()) {
            TCollectionRegistry* owner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
            if (owner == &registry) {
                attachState = expected;
                return true;
            }
            if (!owner && cacheOp.LinkCollection(registry, collectionItem)) {
                attachState = expected;
                return true;
            }
            return false;
        }
        if ((!expected.IsSticky() && !expected.IsHot() && !expected.IsCold()) || !expected.IsStickyNoneField() ||
            handle.Body.Collection->Registry_.load(std::memory_order_acquire)) {
            return false;
        }

        const THandleState claimed = expected.WithSticky(EStickyState::Transition);
        InvokeHook(ESharedCacheHookPoint::BeforeCollectionAttachClaim, collectionItem);
        if (handle.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, claimed);
            attachState = claimed;
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::FinishCollectionAttach(
    TSpaceOperation& spaceOp, TCacheItem collectionItem, EHandleState detachedState) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const ui64 bytes = PayloadBytes(handle, EItemKind::Collection);
    if (detachedState == EHandleState::Cold) {
        AdvanceColdMembership(handle);
        Memory_.AddRetainedBytes(bytes);
        Memory_.Cold().TransferBytesTo(Memory_.Sticky(), bytes);
        Memory_.RemoveColdItem();
    } else if (detachedState == EHandleState::Hot) {
        Memory_.Hot().TransferBytesTo(Memory_.Sticky(), bytes);
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() &&
                       expected.State() == detachedState && expected.IsTransitionField());
        const THandleState desired =
            expected.WithState(EHandleState::Sticky).WithSticky(EStickyState::Sticky).WithFrequency(0);
        InvokeHook(ESharedCacheHookPoint::BeforeCollectionAttachPublished, collectionItem);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AttachCollection(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandleState attachState;
    if (!cacheOp.TryBeginCollectionAttach(registry, collectionItem, attachState)) {
        return false;
    }
    if (attachState.IsStickyField()) {
        return true;
    }

    const EHandleState detachedState = attachState.State();
    Y_ABORT_UNLESS(cacheOp.LinkCollection(registry, collectionItem));
    AddReference(*spaceOp.Handles()[collectionItem.Index()].Body.Collection);
    cacheOp.FinishCollectionAttach(collectionItem, detachedState);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UnlinkCollectionRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& registry,
    TCacheItem collectionItem, EHandleState expectedState) noexcept {
    for (;;) {
        std::atomic<ui32>* link = &registry.CollectionListHead;
        ui32 current = link->load(std::memory_order_acquire);
        while (current != 0) {
            Y_DEBUG_ABORT_UNLESS(current < spaceOp.AccessibleHandleCount());
            THandle& handle = spaceOp.Handles()[current];
            const ui32 next = handle.NextInOwner.load(std::memory_order_acquire);
            const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            Y_DEBUG_ABORT_UNLESS(state.IsCollectionKind() && handle.Body.Collection &&
                                 handle.Body.Collection->Registry_.load(std::memory_order_acquire) == &registry);

            if (current == collectionItem.Index()) {
                if (!collectionItem.Matches(state) || state.State() != expectedState) {
                    return false;
                }
                if (!link->compare_exchange_weak(current, next, std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    break;
                }
                handle.NextInOwner.store(0, std::memory_order_release);
                TCollectionRegistry* expectedRegistry = &registry;
                Y_ABORT_UNLESS(handle.Body.Collection->Registry_.compare_exchange_strong(
                    expectedRegistry, nullptr, std::memory_order_release, std::memory_order_relaxed));
                return true;
            }

            link = &handle.NextInOwner;
            current = next;
        }
        if (current == 0) {
            return false;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::UnlinkCollectionRegistry(TCollectionRegistry& registry, TOperationItemRef& collectionRef) noexcept {
    const bool unlinked = UnlinkCollectionRegistry(
        collectionRef.SpaceOperation(), registry, collectionRef.CacheItem(), EHandleState::Tombstone);
    Y_DEBUG_ABORT_UNLESS(unlinked);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UnlinkCollectionRegistry(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    TOperationItemRef collectionRef(spaceOp, cacheOp.TryAcquireStructural(collectionItem));
    if (!collectionRef) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !state.IsSticky() || !state.IsStickyField() ||
        !handle.Body.Collection || handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &registry)
    {
        return false;
    }

    return cacheOp.UnlinkCollectionRegistry(registry, collectionItem, EHandleState::Sticky);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryBeginCollectionDetach(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    if (!spaceOp.Contains(collectionItem) || collectionItem.IsFrozen()) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind() || !expected.IsSticky() ||
            !expected.IsStickyField() || !handle.Body.Collection ||
            handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &registry)
        {
            return false;
        }
        const THandleState claimed = expected.WithSticky(EStickyState::Transition);
        if (handle.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DetachCollection(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (!cacheOp.TryBeginCollectionDetach(registry, collectionItem)) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    TCollection& value = *handle.Body.Collection;
    const ECacheMode previousMode = value.CacheMode_.exchange(ECacheMode::Regular, std::memory_order_acq_rel);
    PublishKeepPageBytes(value);
    if (previousMode == ECacheMode::TryKeepInMemory) {
        KeepModeSweepRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
    }
    cacheOp.DrainStickyPages(value);
    Y_ABORT_UNLESS(cacheOp.UnlinkCollectionRegistry(registry, collectionItem, EHandleState::Sticky));

    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() && expected.IsSticky() &&
                       expected.IsTransitionField());
        const THandleState desired = expected.WithSticky(EStickyState::None).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            break;
        }
    }
    if (DropReference(*handle.Body.Collection)) {
        cacheOp.PublishCollectionForReclaim(collectionItem);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryClaimCollectionDeletion(TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry,
    TCacheItem collectionItem, THandleState& sourceState, TSharedCacheItemRef& owner) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (!spaceOp.Contains(collectionItem) || collectionItem.IsFrozen()) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    for (;;) {
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind() || !handle.Body.Collection ||
            handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) != 0)
        {
            return false;
        }
        TCollectionRegistry* registry = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
        /* the record's own reference is carried by the sticky field: attached means one
         * reference for the record itself, detached means none, and every other
         * reference belongs to a live page item */
        const ui64 ownReference = expected.IsStickyField() ? 1 : 0;
        if (handle.Body.Collection->References.load(std::memory_order_acquire) != ownReference) {
            return false;
        }
        const bool stickySource = expected.IsSticky() && expected.IsStickyField() && registry == expectedRegistry;
        const bool detachedSource = expected.IsStickyNoneField() && !registry && !expectedRegistry;
        if (!stickySource && !detachedSource) {
            return false;
        }
        if (cacheOp.TryBeginErase(collectionItem, expected, owner)) {
            const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
            UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired);
            sourceState = expected;
            InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, collectionItem);
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DeleteCollection(
    TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry, TCacheItem collectionItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    TOperationItemRef deletionOwner(spaceOp);
    THandleState sourceState;
    if (!cacheOp.TryClaimCollectionDeletion(expectedRegistry, collectionItem, sourceState, deletionOwner.Get())) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    if (sourceState.IsCold()) {
        AdvanceColdMembership(handle);
    }

    TCollectionRegistry* registryOwner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
    if (registryOwner) {
        UnlinkCollectionRegistry(*registryOwner, deletionOwner);
    } else {
        Y_DEBUG_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_relaxed) == 0);
    }

    const ui64 bytes = PayloadBytes(handle, EItemKind::Collection);
    if (sourceState.IsSticky()) {
        Memory_.ReleaseRetainedBytes(bytes);
        Memory_.Sticky().TransferBytesTo(Memory_.Cold(), bytes);
        Memory_.AddColdItem();
    } else if (sourceState.IsHot()) {
        Memory_.ReleaseRetainedBytes(bytes);
        Memory_.Hot().TransferBytesTo(Memory_.Cold(), bytes);
        Memory_.AddColdItem();
    }

    const TSharedCacheKey key = LoadSharedCacheKey(handle, EItemKind::Collection);
    cacheOp.FinishErase(deletionOwner.CacheItem(), key);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MoveCollectionRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& source,
    TCollectionRegistry& destination, TCacheItem collectionItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    TOperationItemRef collectionRef(spaceOp, cacheOp.TryAcquireStructural(collectionItem));
    if (!collectionRef) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !state.IsSticky() || !state.IsStickyField() ||
        !handle.Body.Collection || handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &source)
    {
        return false;
    }
    if (&source == &destination) {
        return true;
    }

    if (!cacheOp.UnlinkCollectionRegistry(source, collectionItem, EHandleState::Sticky)) {
        return false;
    }
    Y_ABORT_UNLESS(cacheOp.LinkCollection(destination, collectionItem));
    return true;
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
TSharedPageRefImpl<TTraits> TSharedCache::BuildPageRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRef&& ref) const noexcept {
    Y_DEBUG_ABORT_UNLESS(&Get() == this && ref);
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && state.IsPageKind() &&
                         (state.IsReady() || state.IsReplacing() || state.IsReplaced() || state.IsTombstone()) &&
                         state.Refs() > 0 && handle.Body.PageBuffer.data());
    return TSharedPageRefImpl<TTraits>(std::move(ref));
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
TSharedPageRefImpl<TTraits> TSharedCache::AcquireHeldPage(TCacheItem item) noexcept {
    auto cacheOp = BeginOperation();
    THandle& handle = cacheOp.Handles()[item.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        // The source owner protects this physical item, including a withdrawn or relocated predecessor.
        Y_ABORT_UNLESS(item.Matches(expected) && expected.IsPageKind() && expected.Refs() != 0);
        Y_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        if (handle.State.compare_exchange_weak(
                expectedRaw, expected.IncrementRefs().Raw(), std::memory_order_relaxed, std::memory_order_relaxed)) {
            return cacheOp.BuildPageRef(TSharedCacheItemRef(item));
        }
    }
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef TSharedCache::AcquireHeldCollection(TCacheItem item) noexcept {
    auto cacheOp = BeginOperation();
    THandle& handle = cacheOp.Handles()[item.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(
            item.Matches(expected) && expected.IsCollectionKind() && expected.Refs() != 0 && handle.Body.Collection);
        Y_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        if (handle.State.compare_exchange_weak(
                expectedRaw, expected.IncrementRefs().Raw(), std::memory_order_relaxed, std::memory_order_relaxed)) {
            return cacheOp.BuildCollectionRef(TSharedCacheItemRef(item));
        }
    }
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef TSharedCache::BuildCollectionRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRef&& ref) const noexcept {
    Y_DEBUG_ABORT_UNLESS(&Get() == this);
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    Y_DEBUG_ABORT_UNLESS([&] {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        return cacheItem.Matches(state) && state.IsCollectionKind() &&
               (state.IsSticky() || state.IsHot() || state.IsCold() || state.IsTombstone()) && state.Refs() > 0 &&
               handle.Body.Collection;
    }());
    return TSharedCacheCollectionRef(std::move(ref));
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
    ForEachCollection(*CollectionRegistry_, callback);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ForEachCollection(
    TCollectionRegistry& registry, TFunctionRef<void(TCollection&)> callback) noexcept {
    auto cacheOp = BeginOperation();
    ui32 current = registry.CollectionListHead.load(std::memory_order_acquire);
    while (current) {
        THandle& handle = cacheOp.Handles()[current];
        const ui32 next = handle.NextInOwner.load(std::memory_order_acquire);
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(state.IsCollectionKind() && handle.Body.Collection &&
                             handle.Body.Collection->Registry_.load(std::memory_order_acquire) == &registry);
        TOperationItemRef ref(cacheOp, cacheOp.TryAcquireStructural(TCacheItem::Make(state.Version(), current)));
        Y_ABORT_UNLESS(ref);
        callback(*handle.Body.Collection);
        current = next;
    }
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
    const ui64 reservedBytes = AccountedPageBytes(candidate.Size);
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
    TCollection* owner = cacheOp.PageCollection(collection);
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
    cacheOp.InitializeCollection(
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
    AddStats(stats);
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
                TCollection* collection = cacheOp.PageCollection(handle);
                if (candidate.Sticky == EStickyState::Sticky || (collection && WantsStickyPage(*collection))) {
                    cacheOp.MakePageSticky(cacheItem);
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
            const ui64 pageBytes = AccountedPageBytes(candidate.Size);
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
        initializedBytes += AccountedPageBytes(candidate.Size);
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
        cacheOp.DropOwnerReference(owner);
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
            if (request.Sticky || WantsStickyPage(owner)) {
                cacheOp.MakePageSticky(page.CacheItem().CacheItem());
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
        AddStats(stats);
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
    return FindOrInsert(*CollectionRegistry_, collection, inserted, hit);
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
            if (owner && owner != registry && !cacheOp.MoveCollectionRegistry(*owner, *registry, ref.CacheItem()))
            {
                return ESharedCacheResultStatus::Pending;
            }
            if (!cacheOp.AttachCollection(*registry, ref.CacheItem())) {
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
bool TSharedCache::EraseCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen())
    {
        return false;
    }

    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
        return false;
    }
    TCollection* pageCollection = nullptr;

    TOperationItemRef ownerRef(spaceOp);
    THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    for (;;) {
        if ((!expected.IsCold() && !expected.IsKeepCold()) || (expected.IsKeepCold() && expected.Refs() != 0) ||
            !expected.IsStickyNoneField() ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.ColdVersion()) {
            return false;
        }
        const TCacheItem cacheItem = TCacheItem::Make(expected.Version(), coldItem.Index());
        if (cacheOp.TryBeginErase(cacheItem, expected, ownerRef.Get())) {
            // The tombstone owner now pins the page and its collection during accounting and notification.
            pageCollection = expected.IsPageKind() ? cacheOp.PageCollection(handle) : nullptr;
            const ui64 bytes = PayloadBytes(handle, expected.Kind());
            const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
            UpdateColdReclaimable(bytes, expected, desired);
            if (expected.IsKeepCold()) {
                Memory_.ReleaseRetainedBytes(bytes);
                Memory_.KeepCold().TransferBytesTo(Memory_.Cold(), bytes);
                Memory_.KeepCold().TransferPageTo(Memory_.Cold(), expected.Kind());
                Memory_.AddColdItem();
                Memory_.ReleaseKeepColdOwnedBytes(bytes);
                if (pageCollection) {
                    AddActivePageBytes(*pageCollection, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
                }
            }
            InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, coldItem);
            break;
        }
    }

    const THandleState coldState = expected;
    // Stale ring entries may name reattached collections; check only after claiming an eviction owner.
    if (coldState.IsCollectionKind()) {
        Y_DEBUG_ABORT_UNLESS(
            !handle.Body.Collection || (!handle.Body.Collection->Registry_.load(std::memory_order_acquire) &&
                                           handle.Body.Collection->References.load(std::memory_order_acquire) == 0));
    }
    const TCacheItem cacheItem = ownerRef.CacheItem();
    const TSharedCacheKey key = LoadSharedCacheKey(handle, coldState.Kind());
    if (coldState.IsPageKind() && pageCollection &&
        pageCollection->CacheMode_.load(std::memory_order_acquire) == ECacheMode::TryKeepInMemory &&
        pageCollection->IsKeepAllowedPage(handle.Metadata.Page.Type))
    {
        this->NotifyKeepPageEviction(pageCollection->Id(), pageCollection->CacheItem, pageCollection->KeepGeneration(),
            NTable::NPage::TPageLocation(NTable::NPage::TPageOffset::FromRaw(key.Word(1)),
                handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type,
                handle.Metadata.Page.Crc32));
    }
    AdvanceColdMembership(handle);
    cacheOp.FinishErase(cacheItem, key);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AdvanceBucketResize() noexcept {
    const TSpaceState spaceState = Space_->CurrentSpaceState();
    if (!spaceState.Resizing()) {
        return false;
    }

    const TBucketResizeState resize = Space_->BucketResizeState();
    auto cacheOp = BeginOperation();
    switch (resize.Phase()) {
        case EBucketResizePhase::Prepared:
            return cacheOp.PrepareBucketSplit() && cacheOp.ActivateBucketSplitOrMerge();

        case EBucketResizePhase::Activated:
            return cacheOp.PublishBucketSplit();

        case EBucketResizePhase::CursorPublished:
            if (resize.Direction() == EBucketResize::Shrinking) {
                return cacheOp.CloseBucketSplit();
            }
            break;

        case EBucketResizePhase::Closed:
            break;

        case EBucketResizePhase::Stable:
            return false;
    }

    if (!cacheOp.RemoveBucketSplit()) {
        return false;
    }
    return resize.Cursor() != resize.PairCount() || Space_->FinishBucketResize();
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
        RefreshEvictableByteBudget();
    }
    if (RetainedBytes() >= softLimit) {
        SoftLimitPressure_.store(true, std::memory_order_relaxed);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RefreshEvictableByteBudget() noexcept {
    const ui64 budget = EvictableByteBudget();
    if (ColdByteBudget_ == budget) {
        return;
    }
    ColdByteBudget_ = budget;
    ColdMinBytes_.store(FractionCeil(budget, Policy_.ColdMin), std::memory_order_relaxed);
    ColdTargetBytes_.store(FractionCeil(budget, (Policy_.ColdMin + Policy_.Grow) / 2), std::memory_order_relaxed);
    ColdGrowBytes_.store(FractionCeil(budget, Policy_.Grow), std::memory_order_relaxed);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateKeepColdLimit(ui64 limit) noexcept {
    if (limit > HardLimit()) {
        return false;
    }
    const ui64 previousMaximum = KeepColdMaxBytes_.exchange(limit, std::memory_order_relaxed);
    if (previousMaximum == limit) {
        return true;
    }
    ui64 current = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 growth = limit > previousMaximum ? limit - previousMaximum : 0;
        const ui64 target = Min(current, limit) + Min(growth, limit - Min(current, limit));
        if (KeepColdLimit_.compare_exchange_weak(current, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::NotifyResourcesAvailable() noexcept {
    ResourceGeneration_.fetch_add(1, std::memory_order_release);
    if (ResourceWaitArmed_.exchange(false, std::memory_order_acq_rel)) {
        TTraits::NotifyResourcesAvailable();
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ArmResourceWait(ui64 generation) noexcept {
    // Acquire the notifier's last exchange even when it has already cleared the flag.
    // This makes its preceding generation update visible and closes the store/load lost-wake race.
    ResourceWaitArmed_.exchange(true, std::memory_order_acq_rel);
    if (ResourceGeneration() != generation && ResourceWaitArmed_.exchange(false, std::memory_order_acq_rel)) {
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
            SoftLimitPressure_.store(true, std::memory_order_relaxed);
            // Aging/resizing can make progress without freeing bytes. Bound cooperative work so
            // concurrent activity cannot keep a requester here instead of putting it on the wait path.
            if (reclamationAttempts++ < SharedCacheTransitionWorkBatch && cacheOp.Reclaim(bytes)) {
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

    HardTarget_ = target;
    TargetHandleCount_.store(target.HandleCount(), std::memory_order_relaxed);
    Memory_.SetHardLimit(limit);
    StartHardTransition();
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EnforceCurrentLimit() noexcept {
    auto cacheOp = BeginOperation();
    return cacheOp.EnforceCurrentLimit(0);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes, bool forceShrinkHot) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const ui64 softLimit = SoftLimit();
    const bool hadCurrentPressure = ExceedsLimit(PayloadUsage(), CurrentLimit(), bytes);
    const ui32 resizeStep = HotResizeStep(static_cast<ui32>(spaceOp.EffectiveHotSlots()));
    // Retained cache bytes drive soft pressure; total page commitment independently drives current pressure.
    const bool enforceSoftLimit =
        SoftLimitPressure_.exchange(false, std::memory_order_relaxed) || RetainedBytes() > softLimit;

    bool progress = false;
    if (hadCurrentPressure) {
        for (ui32 attempt = 0; attempt < resizeStep && ExceedsLimit(PayloadUsage(), CurrentLimit(), bytes); ++attempt) {
            if (!cacheOp.ReclaimCold()) {
                break;
            }
            progress = true;
        }
    }
    const bool enforceCurrentLimit = ExceedsLimit(PayloadUsage(), CurrentLimit(), bytes);
    if (auto hotResize = HotResize_.TryLock()) {
        // Publication uses the same lock, so Hot maintenance can route newly added handles safely.
        auto hotCacheOp = BeginOperation();
        const ui32 effectiveHotSlots = static_cast<ui32>(hotCacheOp.EffectiveHotSlots());
        const ui32 hotResizeStep = HotResizeStep(effectiveHotSlots);
        RefreshEvictableByteBudget();
        if (HotResize_.Phase() == EHotResizePhase::Cut) {
            progress = hotCacheOp.DrainHotResize() || progress;
        } else {
            const ui64 pageUsage = RetainedBytes();
            const ui64 physicalUsage = PayloadUsage();
            const ui64 currentLimit = CurrentLimit();
            const ui64 freeBytes = currentLimit - Min(currentLimit, physicalUsage);
            const ui64 coldBytes = ColdReclaimableBytes();
            // Forecast the room remaining after admitting the caller's payload.
            const ui64 freeAfterAdmission = freeBytes - Min(freeBytes, bytes);
            const ui64 coldFreeBytes = coldBytes + freeAfterAdmission;
            const ui64 coldFreeHandles = ColdReclaimableItems() + hotCacheOp.View().Free().Count();
            const ui64 payloadBudget = EvictableByteBudget();

            const ui64 coldTargetBytes = ColdTargetBytes_.load(std::memory_order_relaxed);
            const ui64 evictableHandles =
                hotCacheOp.AllocationLimit() - 2 - Min(hotCacheOp.AllocationLimit() - 2, StickyPages());
            const ui64 coldMinHandles = (evictableHandles + 4) / 5;
            const ui64 coldGrowHandles = (evictableHandles * 3 + 9) / 10;
            const ui64 coldTargetHandles = (coldMinHandles + coldGrowHandles) / 2;
            const bool belowColdMinimum =
                coldFreeBytes < ColdMinBytes_.load(std::memory_order_relaxed) || coldFreeHandles < coldMinHandles;
            if (coldFreeBytes >= coldTargetBytes && coldFreeHandles >= coldTargetHandles) {
                KeepColdPressure_.store(false, std::memory_order_relaxed);
            }
            const bool refillKeepCold =
                belowColdMinimum || (KeepColdPressure_.load(std::memory_order_relaxed) &&
                                        (coldFreeBytes < coldTargetBytes || coldFreeHandles < coldTargetHandles));
            ui32 targetHotSlots = 0;
            // The caller's own requirement counts as already resident, so it lowers the cold plus free room.
            // Referenced Cold and in-flight loads cannot be reclaimed yet. Under current-limit
            // pressure, let Hot shrink further to compensate for their physical commitment.
            const bool hotUnderPressure =
                enforceCurrentLimit && (ReservedBytes() != 0 || Memory_.Cold().Bytes() > coldBytes);
            const ui32 minimumHotSlots = hotCacheOp.MinimumHotSlots(hotUnderPressure);
            if (effectiveHotSlots < minimumHotSlots) {
                targetHotSlots = minimumHotSlots;
            } else if (enforceCurrentLimit || forceShrinkHot || refillKeepCold ||
                       (pageUsage > softLimit && Memory_.Hot().Bytes() > payloadBudget))
            {
                targetHotSlots = hotCacheOp.ShrinkHotTarget(effectiveHotSlots, hotResizeStep, hotUnderPressure);
                if (targetHotSlots == 0) {
                    const bool needColdHandles =
                        coldFreeHandles < coldMinHandles ||
                        (KeepColdPressure_.load(std::memory_order_relaxed) && coldFreeHandles < coldTargetHandles);
                    if (needColdHandles && KeepColdOwnedBytes() != 0) {
                        KeepColdPressure_.store(true, std::memory_order_relaxed);
                        progress = hotCacheOp.ShrinkKeepCold() || progress;
                    }
                    if (enforceCurrentLimit || forceShrinkHot || pageUsage > softLimit) {
                        progress = hotCacheOp.ProcessHotAtMinimum() || progress;
                    }
                }
            } else if (!hadCurrentPressure && pageUsage <= softLimit &&
                       coldFreeBytes > ColdGrowBytes_.load(std::memory_order_relaxed) &&
                       coldFreeHandles > coldGrowHandles)
            {
                targetHotSlots = hotCacheOp.GrowHotTarget(effectiveHotSlots, hotResizeStep);
                progress = GrowKeepCold() || progress;
            }

            if (targetHotSlots != 0 && Space_->BeginHotResize(targetHotSlots, HotResize_)) {
                if (HotResize_.Phase() == EHotResizePhase::Cut) {
                    progress = hotCacheOp.DrainHotResize() || progress;
                }
            }
        }
    }

    if (enforceCurrentLimit || enforceSoftLimit || (softLimit == 0 && ColdReclaimableItems() != 0)) {
        progress = cacheOp.ReclaimToLimits(softLimit, resizeStep, bytes) || progress;
        if (RetainedBytes() > softLimit) {
            SoftLimitPressure_.store(true, std::memory_order_relaxed);
        }
    }
    return progress;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimToLimits(ui64 softLimit, ui32 step, TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    bool progress = false;
    for (ui32 attempt = 0; attempt < step; ++attempt) {
        if (ExceedsLimit(PayloadUsage(), CurrentLimit(), bytes)) {
            if (!cacheOp.ReclaimCold() && !cacheOp.ReclaimKeepCold()) {
                break;
            }
        } else if (softLimit == 0 && ColdReclaimableItems() != 0) {
            if (!cacheOp.ReclaimCold()) {
                break;
            }
        } else if (RetainedBytes() > softLimit) {
            if (!cacheOp.DemoteKeepColdForSoftLimit()) {
                break;
            }
        } else {
            break;
        }
        progress = true;
    }
    return progress;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::RunMaintenance() noexcept {
    Y_DEBUG_ABORT_UNLESS(CurrentLimit() <= HardLimit());
    if (KeepColdOwnedBytes() > KeepColdLimit() || KeepColdTrimActive_.load(std::memory_order_relaxed))
    {
        auto cacheOp = BeginOperation();
        if (cacheOp.TrimKeepCold()) {
            return true;
        }
    }
    ui64 remaining = KeepModeSweepRemaining_.load(std::memory_order_acquire);
    while (remaining != 0) {
        const ui64 batch = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
        if (KeepModeSweepRemaining_.compare_exchange_weak(
                remaining, remaining - batch, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            auto cacheOp = BeginOperation();
            for (ui64 index = 0; index < batch; ++index) {
                const ui64 raw = cacheOp.KeepCold().Pop();
                if (raw != 0) {
                    cacheOp.RouteKeepCold(TColdCacheItem::FromRaw(raw));
                }
            }
            return true;
        }
    }
    switch (HardTransition_.Phase()) {
        case ETransitionPhase::Idle:
            return StartHardTransition();

        case ETransitionPhase::Prepare:
        case ETransitionPhase::InitializeGrowth:
            return PublishMigrationView(HardTransition_);

        case ETransitionPhase::MigrationDrain:
            return TryDrainTransition(HardTransition_);

        case ETransitionPhase::Migrate: {
            const TSpaceState state = Space_->CurrentSpaceState();
            if (state.Resizing()) {
                return AdvanceBucketResize();
            }
            const ui8 targetAddressBits = HardTransition_.TargetConfiguration().AddressBits;
            if (state.AddressBits() != targetAddressBits) {
                return Space_->BeginBucketResize(targetAddressBits);
            }
            if (HardTransition_.OldConfiguration().AddressBits > targetAddressBits) {
                switch (ReclaimShrinkCut()) {
                    case EShrinkCutStatus::Blocked:
                        return false;
                    case EShrinkCutStatus::Progress:
                        return true;
                    case EShrinkCutStatus::Complete:
                        break;
                }
            }
            return PublishFinalView(HardTransition_);
        }

        case ETransitionPhase::AppendGrowth:
            return AppendFreeHandles(HardTransition_);

        case ETransitionPhase::FinalDrain:
            return TryDrainTransition(HardTransition_) && FinalDrain(HardTransition_);

        case ETransitionPhase::Release:
        case ETransitionPhase::ReleaseRetry:
            return TryReleaseTransition(HardTransition_);

        case ETransitionPhase::Commit:
            return CommitTransition(HardTransition_);

        case ETransitionPhase::PublishMigration:
        case ETransitionPhase::AbandonedForShutdown:
            return false;
    }
    Y_UNREACHABLE();
}

SHARED_CACHE_TEMPLATE
TCollection* TSharedCache::ReleaseTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(cacheItem.Matches(expected) && expected.IsTombstone() && expected.Refs() > 0);
        if (expected.Refs() > 1) {
            const THandleState desired = expected.DecrementRefs();
            if (handle.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
            {
                return nullptr;
            }
            continue;
        }

        if (cacheItem.IsResizeMarker()) {
            const ui32 nextVersion = AdvanceItemVersion(expected.Version());
            const THandleState desired = expected.WithVersion(nextVersion)
                                             .WithState(EHandleState::Free)
                                             .WithFrequency(0)
                                             .WithSticky(EStickyState::None)
                                             .WithRefs(0);
            if (handle.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                return nullptr;
            }
            continue;
        }

        TCollection* owner = expected.IsPageKind() ? cacheOp.PageCollection(handle) : nullptr;
        const TSharedCacheKey key = LoadSharedCacheKey(handle, expected.Kind());
        cacheOp.FinishErase(cacheItem, key);
        Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() ||
                             handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) == 0);
        const ui32 nextVersion = AdvanceItemVersion(expected.Version());
        const THandleState desired = expected.WithVersion(nextVersion)
                                         .WithState(EHandleState::Free)
                                         .WithFrequency(0)
                                         .WithSticky(EStickyState::None)
                                         .WithRefs(0);
        InvokeHook(ESharedCacheHookPoint::BeforeTombstoneFinalCas, cacheItem);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            Y_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_acquire) == 0);
            DeletePayload(handle, expected.Kind(), expected.State(), owner);
            Y_ABORT_UNLESS(spaceOp.ReturnFreeHandle(cacheItem.Index(), nextVersion));
            NotifyResourcesAvailable();
            if (expected.IsCollectionKind()) {
                this->NotifyCollectionReleased(
                    TLogoBlobID(key.Word(0), key.Word(1), key.Word(2)), TCollectionCacheItem::FromValidated(cacheItem));
            }
            return owner;
        }
    }
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
        TCollection* owner = cacheOp.PageCollection(handle);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            TOperationItemRef replacementRef(
                spaceOp, TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire)).WithoutFrozen());
            Y_DEBUG_ABORT_UNLESS(!replacementRef.CacheItem().IsNull() && replacementRef.CacheItem() != cacheItem);
            Y_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_acquire) == 0);
            DeleteReplacedPage(handle);
            if (owner) {
                cacheOp.DropOwnerReference(*owner);
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
        cacheOp.DropOwnerReference(*owner);
    }
}

SHARED_CACHE_TEMPLATE
TCollection* TSharedCache::ReleaseItem(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowKeepCold) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (cacheItem.Matches(expected) && expected.IsTombstone()) {
            return cacheOp.ReleaseTombstone(cacheItem);
        }
        if (cacheItem.Matches(expected) && expected.IsReplacing()) {
            cacheOp.ReleaseReplacing(cacheItem);
            return nullptr;
        }
        if (cacheItem.Matches(expected) && expected.IsReplaced()) {
            cacheOp.ReleaseReplaced(cacheItem);
            return nullptr;
        }
        if (cacheItem.Matches(expected) && expected.IsPageKind() && expected.IsPending())
        {
            TPageFetchState* fetch = handle.Body.Fetch;
            Y_DEBUG_ABORT_UNLESS(fetch && fetch->Page().CacheItem() == cacheItem);
            TOperationItemRef ownerRef(spaceOp, cacheItem);
            ReleaseFetchRef(ownerRef, *fetch);
            return nullptr;
        }
        if (cacheItem.Matches(expected) && expected.IsCold() && expected.IsPageKind() && expected.Refs() == 1 &&
            SoftLimit() == 0) {
            TOperationItemRef ownerRef(spaceOp, cacheItem);
            THandleState checked = expected;
            if (!cacheOp.TryBeginErase(ownerRef.Get(), checked)) {
                ownerRef.Disarm();
                expectedRaw = checked.Raw();
                continue;
            }
            CompleteColdPageRelease(ownerRef);
            return nullptr;
        }
        Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) && expected.Refs() > 0);
        if (allowKeepCold && expected.IsCold() && expected.IsPageKind() && expected.Refs() == 1 &&
            cacheOp.WantsKeepCold(handle) && Memory_.TryAddRetainedBytes(PageBytes(handle)))
        {
            if (cacheOp.TryReleaseColdPageToKeepCold(cacheItem, expectedRaw)) {
                return nullptr;
            }
            continue;
        }
        if (cacheOp.TryReleaseItemRef(cacheItem, expectedRaw)) {
            return nullptr;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::CompleteColdPageRelease(TOperationItemRef& ownerRef) noexcept {
    TSpaceOperation& spaceOp = ownerRef.SpaceOperation();
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem cacheItem = ownerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
    if (TCollection* owner = cacheOp.PageCollection(handle);
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
    const ui64 bytes = PageBytes(handle);
    TCollection* owner = cacheOp.PageCollection(handle);
    if (owner) {
        AddActivePageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
    }
    Memory_.AddKeepColdOwnedBytes(bytes);
    // Preserve this reference until membership and accounting are ready for the unreferenced handoff.
    const THandleState desired = expected.WithState(EHandleState::KeepCold);
    if (handle.State.compare_exchange_weak(
            expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
    {
        TOperationItemRef pin(spaceOp, cacheItem);
        InvokeHook(ESharedCacheHookPoint::AfterKeepColdPromotion, cacheItem);
        const ui32 coldVersion = AdvanceColdMembership(handle);
        Memory_.Cold().TransferBytesTo(Memory_.KeepCold(), bytes);
        Memory_.Cold().TransferPageTo(Memory_.KeepCold(), EItemKind::Page);
        Memory_.RemoveColdItem();
        cacheOp.ReleaseKeepColdPin(pin, coldVersion);
        return true;
    }
    Memory_.ReleaseKeepColdOwnedBytes(bytes);
    Memory_.ReleaseRetainedBytes(bytes);
    if (owner) {
        AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryReleaseItemRef(TSpaceOperation& spaceOp, TCacheItem cacheItem, ui64& expectedRaw) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState expected = THandleState::FromRaw(expectedRaw);
    const bool publishCold = expected.IsCold() && expected.Refs() == 1;
    const bool lastKeepColdRef = expected.IsKeepCold() && expected.Refs() == 1;
    const bool wantsKeepCold = lastKeepColdRef && cacheOp.WantsKeepCold(handle);
    const ui64 coldBytes = publishCold ? PayloadBytes(handle, expected.Kind()) : 0;
    // Capture while the native ref protects the payload; detach/reclaim may follow the final CAS.
    const bool notifyCollectionRelease =
        expected.IsCollectionKind() && expected.IsSticky() && expected.Refs() == 1 && expected.IsStickyField() &&
        handle.Body.Collection->GetActorState() &&
        handle.Body.Collection->StickyPageListHead.load(std::memory_order_acquire) != 0;
    const TLogoBlobID collectionId = notifyCollectionRelease ? handle.Body.Collection->Id() : TLogoBlobID{};
    const THandleState desired = expected.DecrementRefs();
    if (handle.State.compare_exchange_weak(
            expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
    {
        if (notifyCollectionRelease) {
            // No native metadata owner remains to hand these writer-held Sticky pages to an executor.
            this->NotifyCollectionReleased(collectionId, TCollectionCacheItem::FromValidated(cacheItem));
        }
        UpdateColdReclaimable(coldBytes, expected, desired);
        if (publishCold) {
            const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire);
            cacheOp.RouteCold(TColdCacheItem::Make(coldVersion & MaxItemVersion, cacheItem.Index()));
        } else if (lastKeepColdRef) {
            InvokeHook(ESharedCacheHookPoint::AfterKeepColdRefReleased, cacheItem);
            // Check after releasing the final ref: a concurrent trim may have withdrawn this page's ring entry
            // while the ref was being dropped. Policy was read while the original page was still pinned.
            if (!IsKeepColdQueued(handle) || !wantsKeepCold) {
                const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire);
                cacheOp.DemoteKeepCold(TColdCacheItem::Make(coldVersion & MaxItemVersion, cacheItem.Index()),
                    TPageCacheItem::FromValidated(cacheItem));
            }
        }
        if (expected.IsReady() && desired.Refs() == 0 && expected.IsStickyNoneField()) {
            NotifyResourcesAvailable();
        }
        return true;
    }
    return false;
}

#undef TSharedCacheCollectionRef
#undef TSharedCacheItemRef
#undef TPageFetch
#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

template class TSharedCacheThreadBinding<TProdTraits>;
template class TSharedPageRefImpl<TProdTraits>;
template class TSharedCacheCollectionRefImpl<TProdTraits>;
template class TPageFetchImpl<TProdTraits>;
template class TSharedCacheImpl<TProdTraits>;
template class TSharedCacheThreadBinding<TTestTraits>;
template class TSharedPageRefImpl<TTestTraits>;
template class TSharedCacheCollectionRefImpl<TTestTraits>;
template class TPageFetchImpl<TTestTraits>;
template class TSharedCacheImpl<TTestTraits>;

} // namespace NKikimr::NSharedCache
