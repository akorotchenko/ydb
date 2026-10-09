#include "shared_cache.h"
#include "shared_cache_events.h"
#include "shared_cache_pages.h"
#include "shared_cache_traits.h"

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/thread_context.h>

#include <util/generic/cast.h>
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

void* TProdTraits::TrySharedCachePages() noexcept {
    if (HasAppData()) {
        const TIntrusivePtr<TSharedCachePages>& owner = AppData()->SharedCachePages;
        if (owner && owner->Cache) {
            return owner->Cache.Get();
        }
        return nullptr;
    }
    return nullptr;
}

TSharedCachePages& TSharedCachePages::Get() {
    Y_ENSURE(HasAppData(), "Page readers must have an application context");
    const auto& owner = AppData()->SharedCachePages;
    Y_ENSURE(owner && owner->Cache, "Shared cache must be initialized before reading pages");
    return *owner;
}

bool TProdTraits::InstallSharedCachePages(void* cache) noexcept {
    if (!HasAppData()) {
        return false;
    }
    const TIntrusivePtr<TSharedCachePages>& owner = AppData()->SharedCachePages;
    if (!owner || (owner->Cache && owner->Cache.Get() != cache)) {
        return false;
    }
    owner->Cache = TIntrusivePtr<TThrRefBase>(static_cast<TSharedCacheImpl<TProdTraits>*>(cache));
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
    if constexpr (requires { TTraits::BindSharedCachePages(cache); }) {
        TTraits::BindSharedCachePages(cache);
    }
}

SHARED_CACHE_TEMPLATE
TSharedCacheThreadBinding<TTraits>::TSharedCacheThreadBinding(TSharedCacheThreadBinding&& other) noexcept
    : Cache_(std::exchange(other.Cache_, nullptr))
    , Hazard_(std::move(other.Hazard_))
{
}

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
        if constexpr (requires { TTraits::UnbindSharedCachePages(Cache_); }) {
            TTraits::UnbindSharedCachePages(Cache_);
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
{
}

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
    TSharedCache& cache = TSharedCache::SharedCachePages();
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
    TSharedCache& cache = TSharedCache::SharedCachePages();
    auto binding = cache.BindCurrentThreadHazard();
    auto spaceOp = cache.BeginOperation();
    return cache.ReserveFetchMemory(spaceOp, FetchState_->Page().CacheItem(), *FetchState_);
}

SHARED_CACHE_TEMPLATE
bool TPageFetch::Dispatch() noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCache& cache = TSharedCache::SharedCachePages();
    auto binding = cache.BindCurrentThreadHazard();
    auto spaceOp = cache.BeginOperation();
    return cache.DispatchFetch(spaceOp, FetchState_->Page().CacheItem(), *FetchState_);
}

SHARED_CACHE_TEMPLATE
bool TPageFetch::MakeReady(NActors::TSharedData&& data) noexcept {
    if (!FetchState_) {
        return false;
    }
    TSharedCache& cache = TSharedCache::SharedCachePages();
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
    TSharedCache& cache = TSharedCache::SharedCachePages();
    bool failed;
    {
        auto binding = cache.BindCurrentThreadHazard();
        auto spaceOp = cache.BeginOperation();
        TOperationItemRef owner(spaceOp, FetchState_->Page().CacheItem());
        failed = cache.FailFetch(owner, *FetchState_);
    }
    FetchState_.Drop();
    return failed;
}

SHARED_CACHE_TEMPLATE
TSharedCache::TSharedCacheImpl(THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits) noexcept
    : TTraits(std::move(traits))
    , Space_(std::move(space))
    , Table_(*Space_, *this)
    , StaticBytes_(Space_->CurrentConfiguration().StaticBytes)
    , OverallUsage_(Space_->CurrentConfiguration().StaticBytes)
    , CurrentLimit_(hardLimit)
    , HardLimit_(hardLimit)
    , TargetHandleCount_(Space_->CurrentConfiguration().HandleCount())
    , ReservationLimit_(Space_->ReservedConfiguration().Limit)
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
    if (!TTraits::InstallSharedCachePages(cache.Get())) {
        return {};
    }
    return cache;
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits>::TSharedCachePageRefImpl(TSharedCacheItemRef&& ref) noexcept
    : Ref_(std::move(ref))
{
    Y_DEBUG_ABORT_UNLESS(Ref_);
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits>::TSharedCachePageRefImpl(TSharedCachePageRefImpl&& other) noexcept
    : Ref_(std::move(other.Ref_))
{
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits>& TSharedCachePageRefImpl<TTraits>::operator=(
    TSharedCachePageRefImpl&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TPageCacheItem TSharedCachePageRefImpl<TTraits>::CacheItem() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    return TPageCacheItem::FromValidated(Ref_.CacheItem_);
}

SHARED_CACHE_TEMPLATE
template <class TCallback>
decltype(auto) TSharedCachePageRefImpl<TTraits>::WithHandle(TCallback&& callback) const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    TSharedCache& cache = TSharedCache::SharedCachePages();
    auto binding = cache.BindCurrentThreadHazard();
    auto spaceOp = cache.BeginOperation();
    const TCacheItem item = Ref_.CacheItem();
    const THandle& handle = spaceOp.Handles()[item.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(item.Matches(state) && state.IsPageKind() && state.Refs() != 0);
    return std::forward<TCallback>(callback)(handle);
}

SHARED_CACHE_TEMPLATE
const char* TSharedCachePageRefImpl<TTraits>::data() const noexcept {
    return WithHandle([](const THandle& handle) {
        return handle.Body.PageBuffer.data();
    });
}

SHARED_CACHE_TEMPLATE
size_t TSharedCachePageRefImpl<TTraits>::size() const noexcept {
    if (!Ref_) {
        return 0;
    }
    return WithHandle([](const THandle& handle) {
        return handle.Key2OrSize.load(std::memory_order_relaxed);
    });
}

SHARED_CACHE_TEMPLATE
NActors::TSharedData TSharedCachePageRefImpl<TTraits>::BuildSharedData() const noexcept {
    return WithHandle([](const THandle& handle) {
        return handle.Body.PageBuffer.Share(handle.Key2OrSize.load(std::memory_order_relaxed));
    });
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits> TSharedCachePageRefImpl<TTraits>::Acquire() const& noexcept {
    if (!Ref_) {
        return {};
    }
    TSharedCache& cache = TSharedCache::SharedCachePages();
    auto binding = cache.BindCurrentThreadHazard();
    return cache.AcquireHeldPage(Ref_.CacheItem());
}

SHARED_CACHE_TEMPLATE
NTable::NPage::TPageOffset TSharedCachePageRefImpl<TTraits>::GetOffset() const noexcept {
    return WithHandle([](const THandle& handle) {
        return NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed));
    });
}

SHARED_CACHE_TEMPLATE
NTable::NPage::TPageLocation TSharedCachePageRefImpl<TTraits>::GetLocation() const noexcept {
    return WithHandle([](const THandle& handle) {
        return NTable::NPage::TPageLocation(
            NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
            handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type, handle.Metadata.Page.Crc32);
    });
}

SHARED_CACHE_TEMPLATE
NTable::NPage::EPage TSharedCachePageRefImpl<TTraits>::GetType() const noexcept {
    return WithHandle([](const THandle& handle) {
        return handle.Metadata.Page.Type;
    });
}

SHARED_CACHE_TEMPLATE
bool TSharedCachePageRefImpl<TTraits>::IsSticky() const noexcept {
    if (!Ref_) {
        return false;
    }
    TSharedCacheImpl<TTraits>& cache = TSharedCacheImpl<TTraits>::SharedCachePages();
    auto binding = cache.BindCurrentThreadHazard();
    return cache.IsPageSticky(CacheItem());
}

SHARED_CACHE_TEMPLATE
void TSharedCachePageRefImpl<TTraits>::Drop() noexcept {
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
{
}

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
TCacheCollection& TSharedCacheCollectionRef::GetCollection() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    TSharedCache& cache = TSharedCache::SharedCachePages();
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
    TSharedCache& cache = TSharedCache::SharedCachePages();
    auto binding = cache.BindCurrentThreadHazard();
    return cache.AcquireHeldCollection(Ref_.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCacheCollectionRef::IsSoleReference() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    auto& cache = TSharedCache::SharedCachePages();
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
void TSharedCache::DeletePayload(
    THandle& handle, EItemKind kind, EHandleState state, TCacheCollection* owner) noexcept {
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
            HotBytes_.fetch_sub(estimatedBytes, std::memory_order_relaxed);
            SubtractEstimatedPages(HotPages_, kind);
        } else if (state == EHandleState::Sticky) {
            StickyBytes_.fetch_sub(estimatedBytes, std::memory_order_relaxed);
            SubtractEstimatedPages(StickyPages_, kind);
            SubStickyOwnedBytes(kind, bytes);
        } else if (state == EHandleState::KeepCold) {
            KeepColdBytes_.fetch_sub(estimatedBytes, std::memory_order_relaxed);
            SubtractEstimatedPages(KeepColdPages_, kind);
            SubtractExactBytes(KeepColdOwnedBytes_, bytes);
        } else {
            ColdBytes_.fetch_sub(estimatedBytes, std::memory_order_relaxed);
            SubtractEstimatedPages(ColdPages_, kind);
            ColdItems_.fetch_sub(1, std::memory_order_relaxed);
        }
        SubtractExactBytes(ResidentBytes_, bytes);
        if (state == EHandleState::Hot || state == EHandleState::Sticky || state == EHandleState::KeepCold) {
            SubtractExactBytes(RetainedBytes_, bytes);
        }
        SubtractExactBytes(OverallUsage_, bytes);
        if (kind == EItemKind::Page) {
            SubtractExactBytes(PageUsage_, bytes);
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
ui64 TSharedCache::LoadEstimatedBytes(const std::atomic<i64>& counter) noexcept {
    return static_cast<ui64>(Max<i64>(counter.load(std::memory_order_relaxed), 0));
}

SHARED_CACHE_TEMPLATE
ui64 TSharedCache::FractionCeil(ui64 value, double fraction) noexcept {
    Y_DEBUG_ABORT_UNLESS(fraction >= 0.0 && fraction <= 1.0);
    return fraction == 1.0 ? value : static_cast<ui64>(std::ceil(static_cast<double>(value) * fraction));
}

SHARED_CACHE_TEMPLATE
void TSharedCache::TransferEstimatedBytes(
    std::atomic<i64>& source, std::atomic<i64>& destination, ui64 bytes) noexcept {
    const i64 estimatedBytes = static_cast<i64>(bytes);
    destination.fetch_add(estimatedBytes, std::memory_order_relaxed);
    source.fetch_sub(estimatedBytes, std::memory_order_relaxed);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::TransferEstimatedPages(
    std::atomic<i64>& source, std::atomic<i64>& destination, EItemKind kind) noexcept {
    if (kind != EItemKind::Page) {
        return;
    }
    destination.fetch_add(1, std::memory_order_relaxed);
    source.fetch_sub(1, std::memory_order_relaxed);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AddEstimatedPages(std::atomic<i64>& counter, EItemKind kind) noexcept {
    if (kind == EItemKind::Page) {
        counter.fetch_add(1, std::memory_order_relaxed);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::SubtractEstimatedPages(std::atomic<i64>& counter, EItemKind kind) noexcept {
    if (kind == EItemKind::Page) {
        counter.fetch_sub(1, std::memory_order_relaxed);
    }
}

SHARED_CACHE_TEMPLATE
TCacheCollection* TSharedCache::PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    return PageCollection(
        spaceOp, TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed))));
}

SHARED_CACHE_TEMPLATE
TCacheCollection* TSharedCache::PageCollection(TSpaceOperation& spaceOp, TCollectionCacheItem item) noexcept {
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
void TSharedCache::AddReference(TCacheCollection& owner) noexcept {
    owner.References.fetch_add(1, std::memory_order_relaxed);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DropReference(TCacheCollection& owner) noexcept {
    const ui64 previous = owner.References.fetch_sub(1, std::memory_order_relaxed);
    Y_ABORT_UNLESS(previous != 0);
    return previous == 1;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DropOwnerReference(TSpaceOperation& spaceOp, TCacheCollection& owner) noexcept {
    // Keep the payload alive after dropping the page's collection ownership.
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, owner.CacheItem.CacheItem()));
    Y_ABORT_UNLESS(collectionRef);
    const bool lastOwner = DropReference(owner);
    InvokeHook(ESharedCacheHookPoint::AfterCollectionOwnerReferenceDropped, collectionRef.CacheItem());
    if (lastOwner) {
        const TLogoBlobID id = owner.Id();
        const TCollectionCacheItem item = owner.CacheItem;
        PublishCollectionForReclaim(spaceOp, item.CacheItem());
        this->NotifyCollectionReleased(id, item);
    } else if (owner.Registry_.load(std::memory_order_acquire) &&
               owner.References.load(std::memory_order_acquire) == 1) {
        // The registry is the only remaining structural owner; actor work may now be cleaned up.
        this->NotifyCollectionReleased(owner.Id(), owner.CacheItem);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::PublishKeepPageBytes(TCacheCollection& owner) noexcept {
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
void TSharedCache::AddResidentPageBytes(TCacheCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept {
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
void TSharedCache::AddActivePageBytes(TCacheCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept {
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
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_relaxed));
    if (state.Kind() != EItemKind::Page) {
        return;
    }
    if (TCacheCollection* owner = PageCollection(spaceOp, page)) {
        DropOwnerReference(spaceOp, *owner);
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
    const auto reclaimable = [](THandleState state) {
        return state.IsCold() && state.IsStickyNoneField() && state.Refs() == 0;
    };
    const bool wasReclaimable = reclaimable(before);
    const bool isReclaimable = reclaimable(after);
    if (wasReclaimable == isReclaimable) {
        return;
    }
    const i64 bytes = static_cast<i64>(payloadBytes);
    ColdReclaimableBytes_.fetch_add(isReclaimable ? bytes : -bytes, std::memory_order_relaxed);
    ColdReclaimableItems_.fetch_add(isReclaimable ? 1 : -1, std::memory_order_relaxed);
    if (isReclaimable) {
        NotifyResourcesAvailable();
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AdvanceColdMembership(THandle& handle) noexcept {
    ui32 expected = handle.ColdVersion.load(std::memory_order_relaxed);
    for (;;) {
        const ui32 desired = AdvanceColdVersion(expected & MaxItemVersion);
        if (handle.ColdVersion.compare_exchange_weak(
                expected, desired, std::memory_order_release, std::memory_order_relaxed))
        {
            return;
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
void TSharedCache::RouteCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return;
    }

    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.Version()) {
        return;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!state.IsCold() || !state.IsStickyNoneField() || state.Refs() != 0) {
        return;
    }

    ColdRingEntries_.fetch_add(1, std::memory_order_relaxed);
    InvokeHook(ESharedCacheHookPoint::BeforeColdExchange, coldItem);
    const TCacheItem displaced = spaceOp.PutCold(coldItem);
    if (!displaced.IsNull()) {
        const ui64 previous = ColdRingEntries_.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
        EraseCold(spaceOp, displaced);
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    if (TCacheCollection* collection = PageCollection(spaceOp, page)) {
        return collection->CacheMode_.load(std::memory_order_acquire) == ECacheMode::TryKeepInMemory &&
               collection->IsKeepAllowedPage(page.Metadata.Page.Type);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::WantsStickyPage(const TCacheCollection& collection) noexcept {
    return collection.CacheMode_.load(std::memory_order_acquire) == ECacheMode::Sticky;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DemoteKeepCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.Version()) {
        return false;
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!expected.IsKeepCold() || expected.Refs() != 0 ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.Version())
        {
            return false;
        }
        const ui64 bytes = PageBytes(handle);
        const THandleState desired = expected.WithState(EHandleState::Cold);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            UpdateColdReclaimable(bytes, expected, desired);
            AdvanceColdMembership(handle);
            SubtractExactBytes(RetainedBytes_, bytes);
            TransferEstimatedBytes(KeepColdBytes_, ColdBytes_, bytes);
            TransferEstimatedPages(KeepColdPages_, ColdPages_, EItemKind::Page);
            ColdItems_.fetch_add(1, std::memory_order_relaxed);
            SubtractExactBytes(KeepColdOwnedBytes_, bytes);
            if (TCacheCollection* owner = PageCollection(spaceOp, handle)) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
            const ui32 version = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
            RouteCold(spaceOp, TCacheItem::Make(version, coldItem.Index()));
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept {
    if (DemoteKeepCold(spaceOp, coldItem)) {
        return;
    }
    if (!MatchesKeepColdEntry(spaceOp, coldItem)) {
        return;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    ui32 version;
    if (!AdvanceColdMembership(handle, coldItem.Version(), false, version)) {
        return;
    }
    DemoteKeepCold(spaceOp, TCacheItem::Make(version, coldItem.Index()));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MatchesKeepColdEntry(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return false;
    }
    const THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.Version()) {
        return false;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    return state.IsKeepCold();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::IsKeepColdQueued(const THandle& handle) noexcept {
    return (handle.ColdVersion.load(std::memory_order_acquire) & KeepColdQueuedMask) != 0;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RouteKeepCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return;
    }
    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.Version()) {
        return;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!state.IsKeepCold()) {
        return;
    }
    if (state.Refs() != 0) {
        // The caller took this page's ring entry and the page cannot be demoted yet, so it is no longer queued:
        // advancing the membership makes the word stale and lets the last release demote the page.
        WithdrawKeepColdEntry(spaceOp, coldItem);
        return;
    }
    if (!WantsKeepCold(spaceOp, handle)) {
        DemoteKeepCold(spaceOp, coldItem);
        return;
    }
    ui32 queuedVersion;
    if (!AdvanceColdMembership(handle, coldItem.Version(), true, queuedVersion)) {
        return;
    }
    const TCacheItem queuedItem = TCacheItem::Make(queuedVersion, coldItem.Index());
    const ui64 displaced = spaceOp.KeepCold().PutAndPop(queuedItem.Raw());
    if (displaced != 0) {
        const TCacheItem word = TCacheItem::FromRaw(displaced);
        if (!DemoteKeepCold(spaceOp, word)) {
            // The displaced page stays KeepCold without a ring entry: demote it on its last release.
            WithdrawKeepColdEntry(spaceOp, word);
        }
    }
    if (!WantsKeepCold(spaceOp, handle)) {
        DemoteKeepCold(spaceOp, queuedItem);
    }
    TrimKeepCold(spaceOp);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AddKeepColdOwnedBytes(ui64 bytes) noexcept {
    ui64 owned = KeepColdOwnedBytes_.load(std::memory_order_relaxed);
    for (;;) {
        Y_ABORT_UNLESS(owned <= Max<ui64>() - bytes);
        if (KeepColdOwnedBytes_.compare_exchange_weak(
                owned, owned + bytes, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TrimKeepCold(TSpaceOperation& spaceOp) noexcept {
    const ui64 limit = KeepColdLimit();
    if (KeepColdOwnedBytes_.load(std::memory_order_relaxed) <= limit &&
        !KeepColdTrimActive_.load(std::memory_order_relaxed))
    {
        return false;
    }
    if (!KeepColdTrimActive_.exchange(true, std::memory_order_acq_rel)) {
        KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
    }
    const ui64 lowWatermark = limit - (limit / 20 + (limit % 20 != 0));
    ui64 remaining = KeepColdTrimScanRemaining_.load(std::memory_order_acquire);
    ui64 attempts;
    for (;;) {
        if (remaining == 0) {
            KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
            remaining = spaceOp.KeepCold().Capacity;
        }
        attempts = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
        if (KeepColdTrimScanRemaining_.compare_exchange_weak(
                remaining, remaining - attempts, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            break;
        }
    }
    bool progress = false;
    for (ui64 index = 0; index < attempts && KeepColdOwnedBytes_.load(std::memory_order_relaxed) > lowWatermark;
         ++index)
    {
        const ui64 raw = spaceOp.KeepCold().Pop();
        if (raw == 0) {
            continue;
        }
        const TCacheItem word = TCacheItem::FromRaw(raw);
        if (DemoteKeepCold(spaceOp, word)) {
            progress = true;
        } else {
            const ui64 keepColdBytes = KeepColdOwnedBytes_.load(std::memory_order_relaxed);
            WithdrawKeepColdEntry(spaceOp, word);
            // A referenced page cannot be demoted yet, but its entry is gone now: advance the membership so
            // that the page leaves KeepCold on its last release. The withdrawal helper also demotes it if the
            // last release raced with the membership advance.
            progress = progress || KeepColdOwnedBytes_.load(std::memory_order_relaxed) < keepColdBytes;
        }
    }
    if (KeepColdOwnedBytes_.load(std::memory_order_relaxed) <= lowWatermark) {
        KeepColdTrimActive_.store(false, std::memory_order_relaxed);
        KeepColdTrimScanRemaining_.store(0, std::memory_order_relaxed);
    } else if (progress) {
        KeepColdTrimScanRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_relaxed);
    }
    return progress || remaining > attempts;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept {
    // Finish the previous FIFO trim before reducing the target again: sparse slots or held pages may delay it.
    if (KeepColdTrimActive_.load(std::memory_order_relaxed)) {
        return TrimKeepCold(spaceOp);
    }
    ui64 limit = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 occupied = Min(KeepColdLimit(), KeepColdOwnedBytes());
        const ui64 step = Max<ui64>(1, FractionCeil(occupied, Policy_.ResizeStep));
        const ui64 target = occupied - Min(occupied, step);
        if (limit == target) {
            return TrimKeepCold(spaceOp);
        }
        if (KeepColdLimit_.compare_exchange_weak(limit, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            TrimKeepCold(spaceOp);
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::GrowKeepCold() noexcept {
    ui64 limit = KeepColdLimit_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 budget = EvictableByteBudget();
        const ui64 maximum =
            Min(KeepColdMaxBytes_.load(std::memory_order_relaxed), budget - FractionCeil(budget, Policy_.HotMin));
        if (limit >= maximum) {
            return false;
        }
        const ui64 step = Max<ui64>(1, FractionCeil(maximum, Policy_.ResizeStep));
        const ui64 target = limit + Min(maximum - limit, step);
        if (KeepColdLimit_.compare_exchange_weak(limit, target, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCache::PutHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    const TRingExchangeResult exchange =
        spaceOp.Hot(ToUnderlying(level)).PutAndPopWithHook(cacheItem.Raw(), [&]() noexcept {
            InvokeHook(ESharedCacheHookPoint::BeforeHotExchange, cacheItem);
        });
    if (!spaceOp.IsAllowedHotSlot(exchange.Slot)) {
        ui64 expected = cacheItem.Raw();
        if (exchange.Slot->compare_exchange_strong(expected, 0, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            EvictFromHot(spaceOp, cacheItem);
        }
    }
    return TCacheItem::FromRaw(exchange.Previous);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    const TCacheItem displaced = PutHot(spaceOp, level, cacheItem);
    if (!displaced.IsNull()) {
        ProcessHot(spaceOp, level, displaced);
    }
}

SHARED_CACHE_TEMPLATE
Y_FORCE_INLINE bool TSharedCache::TakeHotFrequency(
    THandle& handle, TCacheItem cacheItem, THandleState& state, ui8& frequency) noexcept {
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsHot() || !expected.IsStickyNoneField()) {
            return false;
        }
        const THandleState desired = expected.WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            state = desired;
            frequency = expected.Frequency();
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
Y_FORCE_INLINE void TSharedCache::CompleteHotEviction(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, THandle& handle, THandleState coldState, ui64 bytes) noexcept {
    InvokeHook(ESharedCacheHookPoint::AfterHotEvictionPublished, cacheItem);
    UpdateColdReclaimable(bytes, coldState.WithState(EHandleState::Hot), coldState);
    if (coldState.IsKeepCold()) {
        TransferEstimatedBytes(HotBytes_, KeepColdBytes_, bytes);
        TransferEstimatedPages(HotPages_, KeepColdPages_, coldState.Kind());
    } else {
        SubtractExactBytes(RetainedBytes_, bytes);
        TransferEstimatedBytes(HotBytes_, ColdBytes_, bytes);
        TransferEstimatedPages(HotPages_, ColdPages_, coldState.Kind());
        ColdItems_.fetch_add(1, std::memory_order_relaxed);
        if (coldState.IsPageKind()) {
            if (TCacheCollection* owner = PageCollection(spaceOp, handle)) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
        }
    }
    if (coldState.Refs() == 0) {
        const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire);
        const TCacheItem word = TCacheItem::Make(coldVersion & MaxItemVersion, cacheItem.Index());
        if (coldState.IsKeepCold()) {
            RouteKeepCold(spaceOp, word);
        } else {
            RouteCold(spaceOp, word);
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ProcessHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept {
    for (;;) {
        if (!spaceOp.Contains(cacheItem) || cacheItem.IsFrozen())
        {
            return;
        }

        THandle& handle = spaceOp.Handles()[cacheItem.Index()];
        THandleState state;
        ui8 frequency;
        if (!TakeHotFrequency(handle, cacheItem, state, frequency)) {
            return;
        }

        if (frequency == 0) {
            if (level != EHotLevel::L0) {
                level = EHotLevel::L0;
            } else {
                ui64 hotRaw = state.Raw();
                const ui64 bytes = PayloadBytes(handle, state.Kind());
                const bool keep = state.IsPageKind() && state.Refs() == 0 && WantsKeepCold(spaceOp, handle);
                if (keep) {
                    AddKeepColdOwnedBytes(bytes);
                }
                const THandleState cold = state.WithState(keep ? EHandleState::KeepCold : EHandleState::Cold);
                if (handle.State.compare_exchange_strong(
                        hotRaw, cold.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
                {
                    CompleteHotEviction(spaceOp, cacheItem, handle, cold, bytes);
                    return;
                }

                if (keep) {
                    SubtractExactBytes(KeepColdOwnedBytes_, bytes);
                }

                level = EHotLevel::L0;
            }
        } else {
            level = level >= EHotLevel::L2 && frequency > 1 ? EHotLevel::L3 : EHotLevel::L2;
        }

        const TCacheItem displaced = PutHot(spaceOp, level, cacheItem);
        if (displaced.IsNull()) {
            return;
        }
        cacheItem = displaced;
    }
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::TryAllocateHandle(TSpaceOperation& spaceOp) noexcept {
    const TCacheItem spare = TCacheItem::FromRaw(spaceOp.Hazard().SpareItem.exchange(0, std::memory_order_relaxed));
    if (spare.IsNull()) {
        if (const ui32 index = spaceOp.TryAllocateHandle()) {
            return index;
        }
        // Byte headroom does not guarantee a free handle when pages are much smaller than the expected size.
        // Reclaim Cold, or shrink Hot to feed Cold, before refusing the admission.
        for (ui32 attempt = 0; attempt < spaceOp.HandleCount(); ++attempt) {
            if (!ReclaimCold(spaceOp)) {
                const ui32 hotSlots = static_cast<ui32>(Space_->EffectiveHotSlots());
                EnforceCurrentLimit(spaceOp, 0, true);
                // The caller's view may predate growth; drain with a fresh operation under the resize lock.
                DrainHotResize();
                if (hotSlots == Space_->EffectiveHotSlots() && !ReclaimCold(spaceOp)) {
                    break;
                }
            }
            if (const ui32 index = spaceOp.TryAllocateHandle()) {
                return index;
            }
        }
        return 0;
    }

    Y_ABORT_UNLESS(!spare.IsFrozen() && spare.Index() >= 2 && spare.Index() < spaceOp.AllocationLimit());
    const THandle& handle = spaceOp.Handles()[spare.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_ABORT_UNLESS(spare.Matches(state) && state.IsBegin() && state.Refs() == 0);
    return spare.Index();
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RefillSpareItem(TSpaceOperation& spaceOp) noexcept {
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
        DiscardCandidate(spaceOp, cacheItem, state.Kind());
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RecycleCandidate(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (state.IsPageKind() && state.IsPending() && state.IsStickyField()) {
        UnlinkPendingStickyPage(spaceOp, cacheItem);
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
            handle, state.Kind(), state.State(), state.IsPageKind() ? PageCollection(spaceOp, handle) : nullptr);
        DropPageItemRef(spaceOp, handle);
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
            DropPageItemRef(spaceOp, handle);
            handle.Key0.store(0, std::memory_order_relaxed);
            if (state.IsStickyField()) {
                SubStickyOwnedBytes(EItemKind::Page, PageBytes(handle));
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
        DiscardCandidate(spaceOp, cacheItem, state.Kind());
    }
}

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCache::Allocate(TSpaceOperation& spaceOp) noexcept {
    const ui32 index = TryAllocateHandle(spaceOp);
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
    if (!TryReserve(spaceOp, bytes, pages)) {
        return {};
    }
    const TCacheItem cacheItem = Allocate(spaceOp);
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
    auto spaceOp = BeginOperation();
    const TCacheItem cacheItem = TryAllocateReservedItem(spaceOp, bytes, 1);
    if (!cacheItem.IsNull()) {
        TCacheCollection* owner = PageCollection(spaceOp, collection);
        const ECacheMode admissionMode =
            owner ? owner->CacheMode_.load(std::memory_order_acquire) : ECacheMode::Regular;
        const EStickyState pageSticky = admissionMode == ECacheMode::TryKeepInMemory ? EStickyState::None : sticky;
        InitializePage(spaceOp, cacheItem, collection, offset, size, type, crc32, pageSticky);
        RefillSpareItem(spaceOp);
    }
    return TPageCacheItem::FromValidated(cacheItem);
}

SHARED_CACHE_TEMPLATE
TIntrusivePtr<TPageFetchState> TSharedCache::TryAcquirePageFetch(
    TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef& owner) noexcept {
    if (TryAcquire(spaceOp, page.CacheItem(), true, owner.Get()) != EAcquireStatus::Pending) {
        return {};
    }
    TIntrusivePtr<TPageFetchState> fetch = spaceOp.Handles()[page.Index()].Body.Fetch;
    Y_DEBUG_ABORT_UNLESS(fetch);
    return fetch;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(TPageCacheItem page, NActors::TSharedData&& data) noexcept {
    auto spaceOp = BeginOperation();
    TOperationItemRef owner(spaceOp);
    TIntrusivePtr<TPageFetchState> fetch = TryAcquirePageFetch(spaceOp, page, owner);
    if (!fetch) {
        return false;
    }
    return CompleteFetch(owner, *fetch, std::move(data));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FailReady(TPageCacheItem page) noexcept {
    auto spaceOp = BeginOperation();
    TOperationItemRef owner(spaceOp);
    TIntrusivePtr<TPageFetchState> fetch = TryAcquirePageFetch(spaceOp, page, owner);
    if (!fetch) {
        return false;
    }
    return FailFetch(owner, *fetch);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(
    TCollectionRegistry& registry, TCollectionCacheItem collection, THolder<TCacheCollection>&& value) noexcept {
    return MakeReady(&registry, collection, std::move(value));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(
    TCollectionRegistry* registry, TCollectionCacheItem collection, THolder<TCacheCollection>&& value) noexcept {
    auto spaceOp = BeginOperation();
    return MakeReady(spaceOp, registry, collection.CacheItem(), std::move(value));
}

SHARED_CACHE_TEMPLATE
void TSharedCache::PublishCollectionForReclaim(TSpaceOperation& spaceOp, TCacheItem collectionItem) noexcept {
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
            TransferEstimatedBytes(StickyBytes_, HotBytes_, PayloadBytes(handle, EItemKind::Collection));
            InvokeHook(ESharedCacheHookPoint::BeforeCollectionHotPublished, collectionItem);
            RouteHot(spaceOp, EHotLevel::L1, collectionItem);
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
bool TSharedCache::DetachCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
    auto spaceOp = BeginOperation();
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    return DetachCollection(spaceOp, registry, collection.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UnlinkCollectionRegistry(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
    auto spaceOp = BeginOperation();
    return UnlinkCollectionRegistry(spaceOp, registry, collection.CacheItem());
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
    auto spaceOp = BeginOperation();
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    TCacheCollection* value = PageCollection(spaceOp, collection);
    if (!value || !SetCollectionPagesCacheMode(spaceOp, *value, ECacheMode::Regular)) {
        return false;
    }
    return DeleteCollection(spaceOp, registry, collection.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MoveCollectionRegistry(
    TCollectionRegistry& source, TCollectionRegistry& destination, TCollectionCacheItem collection) noexcept {
    auto spaceOp = BeginOperation();
    return MoveCollectionRegistry(spaceOp, source, destination, collection.CacheItem());
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::SetCollectionPagesCacheMode(TCollectionCacheItem collection, ECacheMode mode) noexcept {
    auto spaceOp = BeginOperation();
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[collection.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collection.Matches(state) || !handle.Body.Collection) {
        return false;
    }
    return SetCollectionPagesCacheMode(spaceOp, *handle.Body.Collection, mode);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::SetCollectionSkipBTreeIndexV1Shadow(TCollectionCacheItem collection, bool skip) noexcept {
    auto spaceOp = BeginOperation();
    TOperationItemRef ref(spaceOp, TryAcquireStructural(spaceOp, collection.CacheItem()));
    if (!ref) {
        return false;
    }
    TCacheCollection* owner = PageCollection(spaceOp, collection);
    if (!owner) {
        return false;
    }
    SetCollectionSkipBTreeIndexV1Shadow(*owner, skip);
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::SetCollectionSkipBTreeIndexV1Shadow(TCacheCollection& owner, bool skip) noexcept {
    owner.PageCollection()->SetSkipBTreeIndexV1Shadow(skip);
    PublishKeepPageBytes(owner);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AcknowledgeCollectionBytes(TCollectionCacheItem collection) noexcept {
    auto spaceOp = BeginOperation();
    TOperationItemRef ref(spaceOp, TryAcquireStructural(spaceOp, collection.CacheItem()));
    if (auto* owner = ref ? PageCollection(spaceOp, collection) : nullptr) {
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
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AllocationLimit());
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
    if (!TryReserve(spaceOp, bytes, 1)) {
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
    auto spaceOp = BeginOperation();
    TVector<TPageFetchState*> waiting;
    ui64 bytes = 0;
    for (auto& page : pages) {
        TPageFetchState& fetch = *page.FetchState_;
        const TCacheItem item = fetch.Page().CacheItem();
        THandle& handle = spaceOp.Handles()[item.Index()];
        const auto state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_ABORT_UNLESS(MatchesPendingFetch(handle, state, item, fetch));
        if (state.IsWaitingForMemory()) {
            bytes += PageBytes(handle);
            waiting.push_back(&fetch);
        }
    }
    // Reserve the missing batch as a whole; two non-transactional requests must not each
    // hold a partial batch while waiting for the other one's bytes.
    if (!TryReserve(spaceOp, bytes, waiting.size())) {
        return false;
    }
    for (auto* fetch : waiting) {
        const TCacheItem item = fetch->Page().CacheItem();
        THandle& handle = spaceOp.Handles()[item.Index()];
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
    auto spaceOp = BeginOperation();
    if (!TryReserve(spaceOp, bytes)) {
        return false;
    }
    // A readiness check, not an allocation owned by the transaction. Its subsequent admissions
    // still use TryReserve and may wait again if another requester consumes the released room.
    ReleaseReservation(bytes);
    return true;
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
    const TCacheItem cacheItem = finalizerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    THandleState completing = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(completing) && completing.IsCompleting() && completing.Refs() == 1 &&
                         handle.Body.Fetch == &fetch);
    if (completing.IsStickyField()) {
        UnlinkPendingStickyPage(spaceOp, cacheItem);
        completing = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    }
    DeletePendingFetch(handle);
    new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer();
    if (reserved) {
        ReleaseReservation(PageBytes(handle), 1);
    }
    handle.State.store(
        completing.WithState(EHandleState::Tombstone).WithSticky(EStickyState::None).Raw(), std::memory_order_release);
    InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
    const TSharedCacheKey key = LoadSharedCacheKey(handle, EItemKind::Page);
    Table_.CompleteTombstone(cacheItem, key, spaceOp);
    TPageFetchState::DrainWaiters(waiters, [cacheItem](TIntrusivePtr<TPageFetchWaiter> waiter) {
        waiter->Complete(TPageCacheItem::FromValidated(cacheItem), EPageFetchCompletion::Failed);
    });
}

SHARED_CACHE_TEMPLATE
void TSharedCache::FinishFetch(TOperationItemRef& finalizerRef, TPageFetchState& fetch, bool reserved) noexcept {
    TSpaceOperation& spaceOp = finalizerRef.SpaceOperation();
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
        TCacheCollection* owner = PageCollection(spaceOp, handle);
        const bool regular = owner && owner->CacheMode_.load(std::memory_order_acquire) == ECacheMode::Regular &&
                             !THandleState::FromRaw(handle.State.load(std::memory_order_acquire)).IsStickyField();
        if (regular) {
            if (!TryAddRetainedBytes(bytes)) {
                readyState = EHandleState::Cold;
            }
        } else {
            RetainedBytes_.fetch_add(bytes, std::memory_order_relaxed);
        }
        if (owner) {
            AddResidentPageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
        }
        SubtractExactBytes(ReservedPages_, 1);
        MakeReadyState(spaceOp, page, readyState, owner);
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
    THolder<TCacheCollection>&& value) noexcept {
    Y_DEBUG_ABORT_UNLESS(value);
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AllocationLimit());
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
        Y_ABORT_UNLESS(LinkCollection(spaceOp, *registry, cacheItem));
    } else {
        Y_DEBUG_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_relaxed) == 0 &&
                             !handle.Body.Collection->Registry_.load(std::memory_order_relaxed));
    }
    MakeReadyState(spaceOp, cacheItem);
    Y_UNUSED(value.Release());
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::MakeReadyState(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, EHandleState readyState, TCacheCollection* pageOwner) noexcept {
    const ui32 index = cacheItem.Index();
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AllocationLimit());

    THandle& handle = spaceOp.Handles()[index];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    const THandleState initial = THandleState::FromRaw(expectedRaw);
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(initial) && (initial.IsPending() || initial.IsCompleting()));

    const ui64 bytes = PayloadBytes(handle, initial.Kind());
    const i64 estimatedBytes = static_cast<i64>(bytes);
    EStickyState accountedSticky = initial.Sticky();
    Y_DEBUG_ABORT_UNLESS(accountedSticky == EStickyState::None || accountedSticky == EStickyState::Sticky);
    if (initial.IsStickyField() && readyState == EHandleState::Cold) {
        RetainedBytes_.fetch_add(bytes, std::memory_order_relaxed);
        readyState = EHandleState::Hot;
    }
    // Withdrawal may have happened after the caller charged retention but before this state was read.
    if (!initial.IsStickyField() && readyState != EHandleState::Cold && pageOwner &&
        pageOwner->CacheMode_.load(std::memory_order_acquire) == ECacheMode::Regular && RetainedBytes() > SoftLimit()) {
        SubtractExactBytes(RetainedBytes_, bytes);
        readyState = EHandleState::Cold;
    }
    ResidentBytes_.fetch_add(bytes, std::memory_order_relaxed);
    if (readyState == EHandleState::Cold) {
        ColdBytes_.fetch_add(estimatedBytes, std::memory_order_relaxed);
        AddEstimatedPages(ColdPages_, initial.Kind());
        ColdItems_.fetch_add(1, std::memory_order_relaxed);
    } else if (accountedSticky == EStickyState::Sticky) {
        StickyBytes_.fetch_add(estimatedBytes, std::memory_order_relaxed);
        AddEstimatedPages(StickyPages_, initial.Kind());
    } else {
        HotBytes_.fetch_add(estimatedBytes, std::memory_order_relaxed);
        AddEstimatedPages(HotPages_, initial.Kind());
    }
    if (pageOwner && readyState != EHandleState::Cold) {
        AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
    }
    SubtractExactBytes(ReservedBytes_, bytes);
    InvokeHook(ESharedCacheHookPoint::BeforeReadyStatePublication, cacheItem);

    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) && (expected.IsPending() || expected.IsCompleting()));
        Y_DEBUG_ABORT_UNLESS(expected.IsStickyNoneField() || expected.IsStickyField());
        if (expected.Sticky() != accountedSticky) {
            if (expected.IsStickyField()) {
                if (readyState == EHandleState::Cold) {
                    RetainedBytes_.fetch_add(bytes, std::memory_order_relaxed);
                    TransferEstimatedBytes(ColdBytes_, HotBytes_, bytes);
                    TransferEstimatedPages(ColdPages_, HotPages_, initial.Kind());
                    ColdItems_.fetch_sub(1, std::memory_order_relaxed);
                    if (pageOwner) {
                        AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
                    }
                    readyState = EHandleState::Hot;
                }
                TransferEstimatedBytes(HotBytes_, StickyBytes_, bytes);
                TransferEstimatedPages(HotPages_, StickyPages_, expected.Kind());
            } else if (pageOwner && pageOwner->CacheMode_.load(std::memory_order_acquire) == ECacheMode::Regular &&
                       RetainedBytes() > SoftLimit()) {
                SubtractExactBytes(RetainedBytes_, bytes);
                TransferEstimatedBytes(StickyBytes_, ColdBytes_, bytes);
                TransferEstimatedPages(StickyPages_, ColdPages_, expected.Kind());
                ColdItems_.fetch_add(1, std::memory_order_relaxed);
                if (pageOwner) {
                    AddActivePageBytes(*pageOwner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
                }
                readyState = EHandleState::Cold;
            } else {
                TransferEstimatedBytes(StickyBytes_, HotBytes_, bytes);
                TransferEstimatedPages(StickyPages_, HotPages_, expected.Kind());
            }
            accountedSticky = expected.Sticky();
        }

        const EHandleState destination = expected.IsStickyField() ? EHandleState::Sticky : readyState;
        const THandleState desired = expected.WithState(destination).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            if (desired.IsHot()) {
                RouteHot(spaceOp, EHotLevel::L1, TCacheItem::Make(expected.Version(), index));
            }
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept {
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.HandleCount());

    const THandleState state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_relaxed));
    if (!state.IsHot()) {
        return false;
    }
    return EvictFromHot(spaceOp, TCacheItem::Make(state.Version(), index));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EvictFromHot(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem) && !cacheItem.IsFrozen() && cacheItem.Index() >= 2);

    const ui32 index = cacheItem.Index();
    THandle& handle = spaceOp.Handles()[index];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsHot() || !expected.IsStickyNoneField()) {
            return false;
        }
        const ui64 bytes = PayloadBytes(handle, expected.Kind());
        const bool keep = expected.IsPageKind() && expected.Refs() == 0 && WantsKeepCold(spaceOp, handle);
        if (keep) {
            AddKeepColdOwnedBytes(bytes);
        }
        const THandleState desired =
            expected.WithState(keep ? EHandleState::KeepCold : EHandleState::Cold).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
        {
            CompleteHotEviction(spaceOp, cacheItem, handle, desired, bytes);
            return true;
        }
        if (keep) {
            SubtractExactBytes(KeepColdOwnedBytes_, bytes);
        }
    }
}

SHARED_CACHE_TEMPLATE
TCollectionCacheItem TSharedCache::AllocateCollection(const TLogoBlobID& id, ui64 bytes) noexcept {
    Y_DEBUG_ABORT_UNLESS(bytes == 0);
    auto spaceOp = BeginOperation();
    const TCacheItem cacheItem = TryAllocateReservedItem(spaceOp, bytes);
    if (!cacheItem.IsNull()) {
        InitializeCollection(spaceOp, cacheItem, id, bytes);
        RefillSpareItem(spaceOp);
    }
    return TCollectionCacheItem::FromValidated(cacheItem);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::InitializePage(TSpaceOperation& spaceOp, TCacheItem cacheItem, TCollectionCacheItem collection,
    ui64 offset, ui64 size, NTable::NPage::EPage type, ui32 crc32, EStickyState sticky, bool reserved) noexcept {
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AllocationLimit());
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
    if (TCacheCollection* owner = PageCollection(spaceOp, handle)) {
        const THandle& collection = spaceOp.Handles()[owner->CacheItem.CacheItem().Index()];
        const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
        Y_ABORT_UNLESS(state.IsSticky() && state.IsStickyField());
        const ui64 previous = owner->References.fetch_add(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
    }
    handle.Body.Fetch = new TPageFetchState(TPageCacheItem::FromValidated(cacheItem));
    handle.Body.Fetch->Ref();
    handle.Next.store(TCacheItem::Make(claimed.Version(), 0).Raw(), std::memory_order_relaxed);
    handle.NextInOwner.store(0, std::memory_order_relaxed);
    // A page enters Sticky only while it fits the sticky budget; otherwise it is admitted with StickyNone and follows
    // the ordinary policy. The check and the exact sticky counter are one step, so concurrent admissions cannot overshoot.
    if (sticky == EStickyState::Sticky && !TryAddStickyOwnedBytes(AccountedPageBytes(size))) {
        sticky = EStickyState::None;
    }
    const THandleState initialized = claimed.WithState(reserved ? EHandleState::Begin : EHandleState::WaitingForMemory)
                                         .WithKind(EItemKind::Page)
                                         .WithFrequency(0)
                                         .WithSticky(sticky);
    handle.State.store(initialized.Raw(), std::memory_order_relaxed);
    if (sticky == EStickyState::Sticky) {
        TOperationItemRef collectionRef(spaceOp);
        LinkStickyPage(spaceOp, cacheItem, collectionRef.Get());
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::InitializeCollection(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept {
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AllocationLimit());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState claimed = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(claimed) && claimed.IsBegin() && claimed.Refs() == 0);
    const ui64* words = id.GetRaw();
    handle.Key0.store(words[0], std::memory_order_relaxed);
    handle.Key1.store(words[1], std::memory_order_relaxed);
    handle.Key2OrSize.store(words[2], std::memory_order_relaxed);
    new (&handle.Body.Collection) TCacheCollection*(nullptr);
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
    auto spaceOp = BeginOperation();
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    const ui64 reservedBytes = state.IsPending() && !state.IsWaitingForMemory() ? PayloadBytes(handle, kind) : 0;
    DiscardCandidate(spaceOp, cacheItem, kind, reservedBytes);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DiscardCandidate(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes) noexcept {
    const ui32 index = cacheItem.Index();
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AllocationLimit());

    THandle& handle = spaceOp.Handles()[index];
    THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (expected.IsPageKind() && expected.IsPending() && expected.IsStickyField()) {
        UnlinkPendingStickyPage(spaceOp, cacheItem);
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
    TCacheCollection* owner = nullptr;
    if (expected.IsPageKind() && handle.Body.Collection) {
        owner = PageCollection(spaceOp, handle);
    }
    UpdateColdReclaimable(PayloadBytes(handle, expected.Kind()), expected, desired);
    handle.State.store(desired.Raw(), std::memory_order_release);
    if (expected.IsPageKind() && expected.IsStickyField() && !expected.IsReady()) {
        SubStickyOwnedBytes(EItemKind::Page, PageBytes(handle));
    }
    if (expected.IsReady()) {
        DeletePayload(handle, expected.Kind(), expected.State(), owner);
    } else if (expected.IsPageKind() && expected.IsPending()) {
        DeletePendingFetch(handle);
    }
    if (owner) {
        DropOwnerReference(spaceOp, *owner);
    }
    if (reservedBytes != 0) {
        ReleaseReservation(reservedBytes, expected.IsPageKind() ? 1 : 0);
    }
    Y_ABORT_UNLESS(spaceOp.ReturnFreeHandle(index, desired.Version()));
}

SHARED_CACHE_TEMPLATE
typename TSharedCache::EAcquireStatus TSharedCache::TryAcquire(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowPending, TSharedCacheItemRef& ref) noexcept {
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
            TransferEstimatedBytes(KeepColdBytes_, HotBytes_, bytes);
            TransferEstimatedPages(KeepColdPages_, HotPages_, EItemKind::Page);
            SubtractExactBytes(KeepColdOwnedBytes_, bytes);
            RouteHot(spaceOp, EHotLevel::L1, cacheItem);
        } else if (expected.IsCold() && expected.IsPageKind()) {
            if (PromoteCold(spaceOp, cacheItem)) {
                RouteHot(spaceOp, EHotLevel::L1, cacheItem);
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
    TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem, TCacheCollection*& collectionValue) noexcept {
    collectionValue = nullptr;
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collectionItem.CacheItem()));
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
bool TSharedCache::SetCollectionPagesCacheMode(
    TSpaceOperation& spaceOp, TCacheCollection& value, ECacheMode mode) noexcept {
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
        DrainStickyPages(spaceOp, value);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RestoreStickyPageList(
    TSpaceOperation& spaceOp, TCacheCollection& collection, ui32 detachedHead) noexcept {
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
void TSharedCache::DrainStickyPages(TSpaceOperation& spaceOp, TCacheCollection& value) noexcept {
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
        RestoreStickyPageList(spaceOp, value, current);
        return;
    }

    while (current != 0) {
        Y_DEBUG_ABORT_UNLESS(current < spaceOp.HandleCount());
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
                TransferEstimatedBytes(StickyBytes_, HotBytes_, bytes);
                TransferEstimatedPages(StickyPages_, HotPages_, EItemKind::Page);
            }
            SubStickyOwnedBytes(EItemKind::Page, bytes);
            if (!pending) {
                RouteHot(spaceOp, EHotLevel::L1, TCacheItem::Make(expected.Version(), current));
            }
            break;
        }
        current = next;
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::UnlinkPendingStickyPage(TSpaceOperation& spaceOp, TCacheItem pageItem) noexcept {
    THandle& target = spaceOp.Handles()[pageItem.Index()];
    TCacheCollection* collection = PageCollection(spaceOp, target);
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
            MergeRetainedStickyPages(spaceOp, *collection, retainedHead, retainedTail);
            const THandleState owner = THandleState::FromRaw(
                spaceOp.Handles()[collection->CacheItem.Index()].State.load(std::memory_order_acquire));
            if (!owner.IsStickyField() || collection->CacheMode_.load(std::memory_order_acquire) != mode) {
                DrainStickyPages(spaceOp, *collection);
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
                    SubStickyOwnedBytes(EItemKind::Page, bytes);
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
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(pageItem.Matches(state) && state.IsPageKind() &&
                         ((state.IsStickyField() && (state.IsPending() || state.IsCompleting() || state.IsHot())) ||
                             (state.IsSticky() && state.IsStickyField())));
    Y_DEBUG_ABORT_UNLESS(page.NextInOwner.load(std::memory_order_relaxed) == 0);

    const TCollectionCacheItem collectionItem =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed)));
    TCacheCollection* collectionValue = nullptr;
    collectionRef = AcquireStickyCollection(spaceOp, collectionItem, collectionValue);
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
                    TransferEstimatedBytes(StickyBytes_, HotBytes_, PageBytes(page));
                    TransferEstimatedPages(StickyPages_, HotPages_, EItemKind::Page);
                    RouteHot(spaceOp, EHotLevel::L1, pageItem);
                }
                SubStickyOwnedBytes(EItemKind::Page, PageBytes(page));
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
        DrainStickyPages(spaceOp, *collectionValue);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakePageSticky(TSpaceOperation& spaceOp, TCacheItem pageItem) noexcept {
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    TCacheCollection* collection = PageCollection(spaceOp, page);
    if (!collection) {
        return false;
    }
    const ECacheMode admissionMode = collection->CacheMode_.load(std::memory_order_acquire);
    if (admissionMode == ECacheMode::TryKeepInMemory) {
        return false;
    }
    const ui64 bytes = PageBytes(page);
    bool stickyBudgetClaimed = false;
    if (THandleState::FromRaw(page.State.load(std::memory_order_acquire)).IsCold()) {
        if (!TryAddStickyOwnedBytes(bytes)) {
            return false;
        }
        stickyBudgetClaimed = true;
        PromoteCold(spaceOp, pageItem, true);
    }
    ui64 expectedRaw = page.State.load(std::memory_order_acquire);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!pageItem.Matches(expected) || !expected.IsPageKind()) {
            if (stickyBudgetClaimed) {
                SubStickyOwnedBytes(EItemKind::Page, bytes);
            }
            return false;
        }
        if (expected.IsSticky() && expected.IsStickyField()) {
            if (stickyBudgetClaimed) {
                SubStickyOwnedBytes(EItemKind::Page, bytes);
            }
            return true;
        }
        const bool pending = expected.IsPending();
        if (pending && expected.IsStickyField()) {
            return true;
        }
        if ((!expected.IsHot() && !pending) || !expected.IsStickyNoneField()) {
            if (stickyBudgetClaimed) {
                SubStickyOwnedBytes(EItemKind::Page, bytes);
            }
            return false;
        }
        if (!stickyBudgetClaimed && !TryAddStickyOwnedBytes(bytes)) {
            return false;
        }
        const THandleState claimed = expected.WithSticky(EStickyState::Sticky);
        if (page.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            if (pending) {
                TOperationItemRef collectionRef(spaceOp);
                LinkStickyPage(spaceOp, pageItem, collectionRef.Get());
                return THandleState::FromRaw(page.State.load(std::memory_order_acquire)).IsStickyField();
            }
            break;
        }
        SubStickyOwnedBytes(EItemKind::Page, bytes);
        stickyBudgetClaimed = false;
    }

    TransferEstimatedBytes(HotBytes_, StickyBytes_, bytes);
    TransferEstimatedPages(HotPages_, StickyPages_, EItemKind::Page);
    TOperationItemRef collectionRef(spaceOp);
    if (!LinkStickyPage(spaceOp, pageItem, collectionRef.Get())) {
        ui64 claimedRaw = page.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState claimed = THandleState::FromRaw(claimedRaw);
            Y_DEBUG_ABORT_UNLESS(pageItem.Matches(claimed) && claimed.IsHot() && claimed.IsStickyField());
            if (page.State.compare_exchange_weak(claimedRaw, claimed.WithSticky(EStickyState::None).Raw(),
                    std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                TransferEstimatedBytes(StickyBytes_, HotBytes_, bytes);
                TransferEstimatedPages(StickyPages_, HotPages_, EItemKind::Page);
                SubStickyOwnedBytes(EItemKind::Page, bytes);
                RouteHot(spaceOp, EHotLevel::L1, pageItem);
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
    TSpaceOperation& spaceOp, TCacheCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept {
    Y_DEBUG_ABORT_UNLESS(retainedHead != 0 && retainedTail != 0);
    const ui32 concurrentHead = collection.StickyPageListHead.exchange(retainedHead, std::memory_order_acq_rel);
    InvokeHook(ESharedCacheHookPoint::AfterStickyPageListHeadExchanged, collection.CacheItem.CacheItem());
    // Only the cache actor traverses; publishers only prepend, so connecting the tail after exchange is safe.
    spaceOp.Handles()[retainedTail].NextInOwner.store(concurrentHead, std::memory_order_release);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UnstickyCutPages(TSpaceOperation& spaceOp, TCacheItem pageItem, ui64 allocationLimit) noexcept {
    THandle& trigger = spaceOp.Handles()[pageItem.Index()];
    const TCollectionCacheItem collectionItem =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(trigger.Key0.load(std::memory_order_relaxed)));
    TCacheCollection* collectionValue = nullptr;
    TOperationItemRef collectionRef(spaceOp, AcquireStickyCollection(spaceOp, collectionItem, collectionValue));
    if (!collectionRef) {
        return false;
    }

    const ui32 detachedHead = collectionValue->StickyPageListHead.exchange(0, std::memory_order_acq_rel);
    InvokeHook(ESharedCacheHookPoint::AfterStickyPageListDetached, collectionItem.CacheItem());

    ui32 current = detachedHead;
    ui32 retainedHead = 0;
    ui32 retainedTail = 0;
    while (current != 0) {
        Y_DEBUG_ABORT_UNLESS(current < spaceOp.HandleCount());
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
        MergeRetainedStickyPages(spaceOp, *collectionValue, retainedHead, retainedTail);
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
            if (!owner && LinkCollection(spaceOp, registry, collectionItem)) {
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
        RetainedBytes_.fetch_add(bytes, std::memory_order_relaxed);
        TransferEstimatedBytes(ColdBytes_, StickyBytes_, bytes);
        ColdItems_.fetch_sub(1, std::memory_order_relaxed);
    } else if (detachedState == EHandleState::Hot) {
        TransferEstimatedBytes(HotBytes_, StickyBytes_, bytes);
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
    THandleState attachState;
    if (!TryBeginCollectionAttach(spaceOp, registry, collectionItem, attachState)) {
        return false;
    }
    if (attachState.IsStickyField()) {
        return true;
    }

    const EHandleState detachedState = attachState.State();
    Y_ABORT_UNLESS(LinkCollection(spaceOp, registry, collectionItem));
    AddReference(*spaceOp.Handles()[collectionItem.Index()].Body.Collection);
    FinishCollectionAttach(spaceOp, collectionItem, detachedState);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UnlinkCollectionRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& registry,
    TCacheItem collectionItem, EHandleState expectedState) noexcept {
    for (;;) {
        std::atomic<ui32>* link = &registry.CollectionListHead;
        ui32 current = link->load(std::memory_order_acquire);
        while (current != 0) {
            Y_DEBUG_ABORT_UNLESS(current < spaceOp.HandleCount());
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
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collectionItem));
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

    return UnlinkCollectionRegistry(spaceOp, registry, collectionItem, EHandleState::Sticky);
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
    if (!TryBeginCollectionDetach(spaceOp, registry, collectionItem)) {
        return false;
    }
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    TCacheCollection& value = *handle.Body.Collection;
    const ECacheMode previousMode = value.CacheMode_.exchange(ECacheMode::Regular, std::memory_order_acq_rel);
    PublishKeepPageBytes(value);
    if (previousMode == ECacheMode::TryKeepInMemory) {
        KeepModeSweepRemaining_.store(spaceOp.KeepCold().Capacity, std::memory_order_release);
    }
    DrainStickyPages(spaceOp, value);
    Y_ABORT_UNLESS(UnlinkCollectionRegistry(spaceOp, registry, collectionItem, EHandleState::Sticky));

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
        PublishCollectionForReclaim(spaceOp, collectionItem);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryClaimCollectionDeletion(TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry,
    TCacheItem collectionItem, THandleState& sourceState) noexcept {
    if (!spaceOp.Contains(collectionItem) || collectionItem.IsFrozen()) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
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
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
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
    THandleState sourceState;
    if (!TryClaimCollectionDeletion(spaceOp, expectedRegistry, collectionItem, sourceState)) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    if (sourceState.IsCold()) {
        AdvanceColdMembership(handle);
    }

    TOperationItemRef deletionOwner(spaceOp, collectionItem);
    TCollectionRegistry* registryOwner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
    if (registryOwner) {
        UnlinkCollectionRegistry(*registryOwner, deletionOwner);
    } else {
        Y_DEBUG_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_relaxed) == 0);
    }

    const ui64 bytes = PayloadBytes(handle, EItemKind::Collection);
    if (sourceState.IsSticky()) {
        SubtractExactBytes(RetainedBytes_, bytes);
        TransferEstimatedBytes(StickyBytes_, ColdBytes_, bytes);
        ColdItems_.fetch_add(1, std::memory_order_relaxed);
    } else if (sourceState.IsHot()) {
        SubtractExactBytes(RetainedBytes_, bytes);
        TransferEstimatedBytes(HotBytes_, ColdBytes_, bytes);
        ColdItems_.fetch_add(1, std::memory_order_relaxed);
    }

    const TSharedCacheKey key = LoadSharedCacheKey(handle, EItemKind::Collection);
    Table_.CompleteTombstone(deletionOwner.CacheItem(), key, spaceOp);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MoveCollectionRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& source,
    TCollectionRegistry& destination, TCacheItem collectionItem) noexcept {
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collectionItem));
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

    if (!UnlinkCollectionRegistry(spaceOp, source, collectionItem, EHandleState::Sticky)) {
        return false;
    }
    Y_ABORT_UNLESS(LinkCollection(spaceOp, destination, collectionItem));
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::IsPageSticky(TPageCacheItem page) const noexcept {
    auto spaceOp = BeginOperation();
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
            if (!next.IsFrozen() || next.WithoutFrozen().IsNull()) {
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
TSharedCachePageRefImpl<TTraits> TSharedCache::BuildPageRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRef&& ref) const noexcept {
    Y_DEBUG_ABORT_UNLESS(&SharedCachePages() == this && ref);
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && state.IsPageKind() &&
                         (state.IsReady() || state.IsReplacing() || state.IsReplaced() || state.IsTombstone()) &&
                         state.Refs() > 0 && handle.Body.PageBuffer.data());
    return TSharedCachePageRefImpl<TTraits>(std::move(ref));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AcquirePage(TPageCacheItem page, TSharedCachePageRefImpl<TTraits>& result) noexcept {
    result.Drop();
    TCacheItem cacheItem = page.CacheItem();
    auto spaceOp = BeginOperation();
    for (;;) {
        if (!spaceOp.Contains(cacheItem)) {
            return false;
        }

        THandle& handle = spaceOp.Handles()[cacheItem.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (!cacheItem.Matches(state) || !state.IsPageKind()) {
            return false;
        }
        if (state.IsReplacing() || state.IsReplaced()) {
            const TCacheItem replacement = TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire));
            if (!replacement.IsFrozen() || replacement.WithoutFrozen().IsNull()) {
                return false;
            }
            cacheItem = replacement.WithoutFrozen();
            continue;
        }
        if (!state.IsReady()) {
            return false;
        }

        // The completion finalizer or replacement hold keeps this generation alive until the new ref is acquired.
        TOperationItemRef ref(spaceOp, TryAcquireStructural(spaceOp, cacheItem));
        if (!ref) {
            continue;
        }
        result = BuildPageRef(spaceOp, ref.Take());
        return true;
    }
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits> TSharedCache::AcquireHeldPage(TCacheItem item) noexcept {
    auto spaceOp = BeginOperation();
    THandle& handle = spaceOp.Handles()[item.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        // The source owner protects this physical item, including a withdrawn or relocated predecessor.
        Y_ABORT_UNLESS(item.Matches(expected) && expected.IsPageKind() && expected.Refs() != 0);
        Y_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        if (handle.State.compare_exchange_weak(
                expectedRaw, expected.IncrementRefs().Raw(), std::memory_order_relaxed, std::memory_order_relaxed)) {
            return BuildPageRef(spaceOp, TSharedCacheItemRef(item));
        }
    }
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef TSharedCache::AcquireHeldCollection(TCacheItem item) noexcept {
    auto spaceOp = BeginOperation();
    THandle& handle = spaceOp.Handles()[item.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(
            item.Matches(expected) && expected.IsCollectionKind() && expected.Refs() != 0 && handle.Body.Collection);
        Y_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        if (handle.State.compare_exchange_weak(
                expectedRaw, expected.IncrementRefs().Raw(), std::memory_order_relaxed, std::memory_order_relaxed)) {
            return BuildCollectionRef(spaceOp, TSharedCacheItemRef(item));
        }
    }
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef TSharedCache::BuildCollectionRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRef&& ref) const noexcept {
    Y_DEBUG_ABORT_UNLESS(&SharedCachePages() == this);
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
    TCollectionCacheItem collection, ui64 offset, TSharedCachePageRefImpl<TTraits>& page) noexcept {
    page.Drop();
    auto spaceOp = BeginOperation();
    TOperationItemRef ref(spaceOp);
    const ESharedCacheResultStatus status = Find(spaceOp, TSharedCacheKey::Page(collection, offset), ref.Get());
    if (status == ESharedCacheResultStatus::Hit) {
        page = BuildPageRef(spaceOp, ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::Find(const TLogoBlobID& id, TSharedCacheCollectionRef& collection) noexcept {
    collection.Drop();
    auto spaceOp = BeginOperation();
    TOperationItemRef ref(spaceOp);
    const ESharedCacheResultStatus status = Find(spaceOp, TSharedCacheKey::Collection(id), ref.Get());
    if (status == ESharedCacheResultStatus::Hit) {
        collection = BuildCollectionRef(spaceOp, ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ForEachCollection(
    TCollectionRegistry& registry, TFunctionRef<void(TCacheCollection&)> callback) noexcept {
    auto spaceOp = BeginOperation();
    ui32 current = registry.CollectionListHead.load(std::memory_order_acquire);
    while (current) {
        THandle& handle = spaceOp.Handles()[current];
        const ui32 next = handle.NextInOwner.load(std::memory_order_acquire);
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(state.IsCollectionKind() && handle.Body.Collection &&
                             handle.Body.Collection->Registry_.load(std::memory_order_acquire) == &registry);
        TOperationItemRef ref(spaceOp, TryAcquireStructural(spaceOp, TCacheItem::Make(state.Version(), current)));
        Y_ABORT_UNLESS(ref);
        callback(*handle.Body.Collection);
        current = next;
    }
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::Find(
    TSpaceOperation& spaceOp, const TSharedCacheKey& key, TSharedCacheItemRef& ref) noexcept {
    for (;;) {
        TCacheItem cacheItem;
        const ESharedCacheResultStatus status = Table_.Find(spaceOp, key, cacheItem);
        if (status == ESharedCacheResultStatus::Miss) {
            return ESharedCacheResultStatus::Miss;
        }
        Y_DEBUG_ABORT_UNLESS(status == ESharedCacheResultStatus::Hit);
        TOperationItemRef acquired(spaceOp);
        switch (TryAcquire(spaceOp, cacheItem, false, acquired.Get())) {
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
    const ui64 reservedBytes = AccountedPageBytes(candidate.Size);
    bool reserved = true;
    TCacheItem cacheItem;
    if (candidate.WaitForMemory) {
        reserved = TryReserve(spaceOp, reservedBytes, 1);
        cacheItem = Allocate(spaceOp);
        if (cacheItem.IsNull() && reserved) {
            ReleaseReservation(reservedBytes, 1);
        }
    } else {
        cacheItem = TryAllocateReservedItem(spaceOp, reservedBytes, 1);
    }
    if (cacheItem.IsNull()) {
        return false;
    }
    const TCollectionCacheItem collection =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(candidate.Key.Word(0)));
    TCacheCollection* owner = PageCollection(spaceOp, collection);
    const ECacheMode admissionMode = owner ? owner->CacheMode_.load(std::memory_order_acquire) : ECacheMode::Regular;
    const EStickyState sticky = candidate.WaitForMemory && admissionMode == ECacheMode::Sticky ? EStickyState::Sticky
                                : admissionMode == ECacheMode::TryKeepInMemory                 ? EStickyState::None
                                                                                               : candidate.Sticky;
    InitializePage(spaceOp, cacheItem, collection, candidate.Key.Word(1), candidate.Size, candidate.Type,
        candidate.Crc32, sticky, reserved);
    candidate.StickyBudgetDenied =
        sticky == EStickyState::Sticky &&
        !THandleState::FromRaw(spaceOp.Handles()[cacheItem.Index()].State.load(std::memory_order_relaxed))
             .IsStickyField();
    candidate.CacheItem = cacheItem;
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PrepareCandidate(TSpaceOperation& spaceOp, TCollectionInsertCandidate& candidate) noexcept {
    Y_DEBUG_ABORT_UNLESS(candidate.Bytes == 0);
    const TCacheItem cacheItem = TryAllocateReservedItem(spaceOp, candidate.Bytes);
    if (cacheItem.IsNull()) {
        return false;
    }
    InitializeCollection(spaceOp, cacheItem,
        TLogoBlobID(candidate.Key.Word(0), candidate.Key.Word(1), candidate.Key.Word(2)), candidate.Bytes);
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
    TSharedCachePageRefImpl<TTraits>& hit) noexcept {
    Y_DEBUG_ABORT_UNLESS(page && page.Type != NTable::NPage::EPage::Undef);
    Y_DEBUG_ABORT_UNLESS(sticky == EStickyState::None || sticky == EStickyState::Sticky);
    inserted = {};
    hit.Drop();
    auto spaceOp = BeginOperation();
    TPageInsertCandidate candidate = BuildPageInsertCandidate(collection, page, sticky);
    TOperationItemRef ref(spaceOp);
    const ESharedCacheResultStatus status = FindOrInsert(spaceOp, candidate, ref.Get());
    switch (status) {
        case ESharedCacheResultStatus::Miss:
        case ESharedCacheResultStatus::Pending:
            return status;
        case ESharedCacheResultStatus::Inserted:
            inserted = TPageCacheItem::FromValidated(ref.CacheItem());
            return status;
        case ESharedCacheResultStatus::Hit:
            hit = BuildPageRef(spaceOp, ref.Take());
            return status;
    }
    Y_UNREACHABLE();
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(TCollectionCacheItem collection,
    const NTable::NPage::TPageLocation& page, EStickyState sticky, TIntrusivePtr<TPageFetchWaiter> waiter,
    TSharedCachePageRefImpl<TTraits>& hit, TPageFetchImpl<TTraits>& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(page && page.Type != NTable::NPage::EPage::Undef && waiter);
    Y_DEBUG_ABORT_UNLESS(sticky == EStickyState::None || sticky == EStickyState::Sticky);
    auto spaceOp = BeginOperation();
    TPageInsertCandidate candidate = BuildPageInsertCandidate(collection, page, sticky);
    const ESharedCacheResultStatus status = FindOrInsertPage(spaceOp, candidate, std::move(waiter), hit, fetch);
    TStats stats;
    stats.AddAdmission(status, page.Size);
    AddStats(stats);
    return status;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsertPage(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate,
    TIntrusivePtr<TPageFetchWaiter> waiter, TSharedCachePageRefImpl<TTraits>& hit,
    TPageFetchImpl<TTraits>& fetch) noexcept {
    hit.Drop();
    fetch = {};
    TOperationItemRef ref(spaceOp);
    const ESharedCacheResultStatus status = FindOrInsert(spaceOp, candidate, ref.Get());
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
                TCacheCollection* collection = PageCollection(spaceOp, handle);
                if (candidate.Sticky == EStickyState::Sticky || (collection && WantsStickyPage(*collection))) {
                    MakePageSticky(spaceOp, cacheItem);
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
            hit = BuildPageRef(spaceOp, ref.Take());
            return status;
    }
    Y_UNREACHABLE();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PreparePageBatch(TSpaceOperation& spaceOp, TCacheCollection& collection,
    TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests, TVector<TPageInsertCandidate>& candidates,
    ui64& reservedBytes) noexcept {
    if (requests.size() > Max<ui32>()) {
        return false;
    }
    reservedBytes = 0;
    candidates.reserve(requests.size());
    for (const TSharedCachePageRequestImpl<TTraits>& request : requests) {
        Y_DEBUG_ABORT_UNLESS(
            request.Location && request.Location.Type != NTable::NPage::EPage::Undef && request.Waiter);
        Y_DEBUG_ABORT_UNLESS(request.Location.Size <= Max<ui64>() - NActors::TSharedData::OverheadSize);

        TPageInsertCandidate candidate = BuildPageInsertCandidate(
            collection.CacheItem, request.Location, request.Sticky ? EStickyState::Sticky : EStickyState::None);
        // Only pages that are absent from the table need admission budget: a page that is already resident is
        // a hit or a pending subscription and must be admitted even when the cache is at its limit.
        TCacheItem resident;
        candidate.Prepare = Table_.Find(spaceOp, candidate.Key, resident) == ESharedCacheResultStatus::Miss;
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
bool TSharedCache::AllocatePageBatch(TSpaceOperation& spaceOp, TCacheCollection& collection,
    TVector<TPageInsertCandidate>& candidates, ui64 reservedBytes, bool waitForMemory) noexcept {
    ui64 preparedPages = 0;
    for (const TPageInsertCandidate& candidate : candidates) {
        preparedPages += candidate.Prepare ? 1 : 0;
    }
    const bool reserved = TryReserve(spaceOp, reservedBytes, preparedPages);
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
        const TCacheItem cacheItem = Allocate(spaceOp);
        if (cacheItem.IsNull()) {
            for (TPageInsertCandidate& initialized : candidates) {
                if (!initialized.CacheItem.IsNull()) {
                    RecycleCandidate(spaceOp, initialized.CacheItem);
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
        InitializePage(spaceOp, cacheItem, collection.CacheItem, candidate.Key.Word(1), candidate.Size, candidate.Type,
            candidate.Crc32, sticky, reserved);
        candidate.StickyBudgetDenied =
            sticky == EStickyState::Sticky &&
            !THandleState::FromRaw(spaceOp.Handles()[cacheItem.Index()].State.load(std::memory_order_relaxed))
                 .IsStickyField();
        candidate.CacheItem = cacheItem;
        initializedBytes += AccountedPageBytes(candidate.Size);
        ++initializedPages;
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FindOrInsertBatch(TCollectionCacheItem collection,
    TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests, bool recordStats, bool waitForMemory) noexcept {
    if (requests.empty()) {
        return true;
    }

    TVector<TPageInsertCandidate> candidates;
    ui64 reservedBytes;

    auto spaceOp = BeginOperation();
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collection.CacheItem()));
    if (!collectionRef) {
        return false;
    }
    THandle& collectionHandle = spaceOp.Handles()[collection.Index()];
    const THandleState collectionState = THandleState::FromRaw(collectionHandle.State.load(std::memory_order_acquire));
    if (!collection.Matches(collectionState) || !collectionState.IsReady() || !collectionHandle.Body.Collection) {
        return false;
    }
    TCacheCollection& owner = *collectionHandle.Body.Collection;
    if (!PreparePageBatch(spaceOp, owner, requests, candidates, reservedBytes)) {
        return false;
    }
    InvokeHook(ESharedCacheHookPoint::AfterPageBatchPrepared, collection.CacheItem());
    if (!AllocatePageBatch(spaceOp, owner, candidates, reservedBytes, waitForMemory)) {
        return false;
    }

    // A rejected batch returns before this point, so every requested page reports exactly one admission outcome.
    TStats stats;
    for (ui32 index = 0; index < requests.size(); ++index) {
        TSharedCachePageRequestImpl<TTraits>& request = requests[index];
        request.Reset();
        TSharedCachePageRefImpl<TTraits> page;
        TPageFetchImpl<TTraits> fetch;
        const ESharedCacheResultStatus status =
            FindOrInsertPage(spaceOp, candidates[index], std::move(request.Waiter), page, fetch);
        if (status == ESharedCacheResultStatus::Hit) {
            if (request.Sticky || WantsStickyPage(owner)) {
                MakePageSticky(spaceOp, page.CacheItem().CacheItem());
            }
            request.SetHit(std::move(page));
        } else if (status == ESharedCacheResultStatus::Inserted) {
            request.SetInserted(std::move(fetch));
        } else if (status == ESharedCacheResultStatus::Pending) {
            request.SetPending();
        }
        stats.AddAdmission(status, request.Location.Size);
    }
    if (recordStats) {
        AddStats(stats);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(
    const TCollectionLocation& collection, TCollectionCacheItem& inserted, TSharedCacheCollectionRef& hit) noexcept {
    inserted = {};
    hit.Drop();
    auto spaceOp = BeginOperation();
    return FindOrInsertCollection(spaceOp, nullptr, collection, inserted, hit);
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(TCollectionRegistry& registry,
    const TCollectionLocation& collection, TCollectionCacheItem& inserted, TSharedCacheCollectionRef& hit) noexcept {
    inserted = {};
    hit.Drop();
    auto spaceOp = BeginOperation();
    return FindOrInsertCollection(spaceOp, &registry, collection, inserted, hit);
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsertCollection(TSpaceOperation& spaceOp, TCollectionRegistry* registry,
    const TCollectionLocation& collection, TCollectionCacheItem& inserted, TSharedCacheCollectionRef& hit) noexcept {
    TCollectionInsertCandidate candidate{
        .Key = TSharedCacheKey::Collection(collection.Id),
        .Bytes = collection.AccountedBytes(),
    };
    TOperationItemRef ref(spaceOp);
    const ESharedCacheResultStatus status = FindOrInsert(spaceOp, candidate, ref.Get());
    if (status == ESharedCacheResultStatus::Inserted) {
        inserted = TCollectionCacheItem::FromValidated(ref.CacheItem());
    } else if (status == ESharedCacheResultStatus::Hit) {
        if (registry) {
            THandle& handle = spaceOp.Handles()[ref.CacheItem().Index()];
            TCollectionRegistry* owner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
            if (owner && owner != registry && !MoveCollectionRegistry(spaceOp, *owner, *registry, ref.CacheItem()))
            {
                return ESharedCacheResultStatus::Pending;
            }
            if (!AttachCollection(spaceOp, *registry, ref.CacheItem())) {
                return ESharedCacheResultStatus::Pending;
            }
        }
        hit = BuildCollectionRef(spaceOp, ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(
    TSpaceOperation& spaceOp, TCacheItem candidate, TSharedCacheItemRef& ref) noexcept {
    const THandle& handle = spaceOp.Handles()[candidate.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    TCacheKeyedItem keyedCandidate{
        .Key = LoadSharedCacheKey(handle, state.Kind()),
        .CacheItem = candidate,
    };
    return FindOrInsert(spaceOp, keyedCandidate, ref);
}

SHARED_CACHE_TEMPLATE
template <class TKeyedCandidate>
ESharedCacheResultStatus TSharedCache::FindOrInsert(
    TSpaceOperation& spaceOp, TKeyedCandidate& candidate, TSharedCacheItemRef& ref) noexcept {
    for (;;) {
        typename TSharedCacheTableImpl<TTraits>::TInsertPosition insertPosition;
        TCacheItem cacheItem;
        const ESharedCacheResultStatus status = Table_.FindForInsert(spaceOp, candidate.Key, cacheItem, insertPosition);
        bool inserted = false;
        if (status == ESharedCacheResultStatus::Miss) {
            if (candidate.CacheItem.IsNull()) {
                if (!PrepareCandidate(spaceOp, candidate)) {
                    insertPosition.OwnerRef.Drop(spaceOp);
                    return ESharedCacheResultStatus::Miss;
                }
                insertPosition.OwnerRef.Drop(spaceOp);
                continue;
            }
            if (!Table_.InsertAt(spaceOp, insertPosition, candidate.CacheItem)) {
                continue;
            }
            cacheItem = candidate.CacheItem;
            inserted = true;
        } else {
            Y_DEBUG_ABORT_UNLESS(status == ESharedCacheResultStatus::Hit);
        }

        TOperationItemRef acquired(spaceOp);
        const EAcquireStatus acquireStatus = TryAcquire(spaceOp, cacheItem, true, acquired.Get());
        if (acquireStatus == EAcquireStatus::Stale) {
            Y_DEBUG_ABORT_UNLESS(!inserted);
            continue;
        }
        Y_DEBUG_ABORT_UNLESS(acquireStatus == EAcquireStatus::Pending || acquireStatus == EAcquireStatus::Ready);

        if (inserted) {
            if constexpr (std::is_same_v<TKeyedCandidate, TPageInsertCandidate>) {
                if (candidate.StickyBudgetDenied) {
                    StickyAdmissionFailures_.fetch_add(1, std::memory_order_relaxed);
                }
            }
            RefillSpareItem(spaceOp);
            ref = acquired.Take();
            return ESharedCacheResultStatus::Inserted;
        }

        Y_DEBUG_ABORT_UNLESS(MatchesCandidateMetadata(spaceOp, candidate, cacheItem));
        if (!candidate.CacheItem.IsNull()) {
            RecycleCandidate(spaceOp, candidate.CacheItem);
            candidate.CacheItem = {};
        }
        ref = acquired.Take();
        return acquireStatus == EAcquireStatus::Pending ? ESharedCacheResultStatus::Pending
                                                        : ESharedCacheResultStatus::Hit;
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EraseCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen())
    {
        return false;
    }

    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.Version()) {
        return false;
    }
    TCacheCollection* pageCollection = nullptr;

    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if ((!expected.IsCold() && !expected.IsKeepCold()) || (expected.IsKeepCold() && expected.Refs() != 0) ||
            !expected.IsStickyNoneField() ||
            (handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.Version()) {
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        THandleState desired = expected.IncrementRefs();
        desired = desired.WithState(EHandleState::Tombstone).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            // The tombstone owner now pins the page and its collection during accounting and notification.
            pageCollection = expected.IsPageKind() ? PageCollection(spaceOp, handle) : nullptr;
            const ui64 bytes = PayloadBytes(handle, expected.Kind());
            UpdateColdReclaimable(bytes, expected, desired);
            if (expected.IsKeepCold()) {
                SubtractExactBytes(RetainedBytes_, bytes);
                TransferEstimatedBytes(KeepColdBytes_, ColdBytes_, bytes);
                TransferEstimatedPages(KeepColdPages_, ColdPages_, expected.Kind());
                ColdItems_.fetch_add(1, std::memory_order_relaxed);
                SubtractExactBytes(KeepColdOwnedBytes_, bytes);
                if (pageCollection) {
                    AddActivePageBytes(*pageCollection, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
                }
            }
            InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, coldItem);
            break;
        }
    }

    const THandleState coldState = THandleState::FromRaw(expectedRaw);
    TOperationItemRef ownerRef(spaceOp, TCacheItem::Make(coldState.Version(), coldItem.Index()));
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
    Table_.CompleteTombstone(cacheItem, key, spaceOp);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimCold() noexcept {
    auto spaceOp = BeginOperation();
    return ReclaimCold(spaceOp);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept {
    const ui64 currentStaticBytes = Space_->CurrentConfiguration().StaticBytes;
    const ui64 staticDeltaBytes = target.StaticBytes > currentStaticBytes ? target.StaticBytes - currentStaticBytes : 0;
    if (staticDeltaBytes != 0) {
        Y_DEBUG_ABORT_UNLESS(StaticDeltaBytes_.load(std::memory_order_relaxed) == 0);
        StaticDeltaBytes_.fetch_add(staticDeltaBytes, std::memory_order_relaxed);
        OverallUsage_.fetch_add(staticDeltaBytes, std::memory_order_relaxed);
    }
    if (Space_->PrepareTransition(target, transition)) {
        return true;
    }
    if (staticDeltaBytes != 0) {
        SubtractExactBytes(StaticDeltaBytes_, staticDeltaBytes);
        SubtractExactBytes(OverallUsage_, staticDeltaBytes);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PublishMigrationView(TTransition& transition) noexcept {
    if (auto hotResize = HotResize_.TryLock()) {
        return Space_->PublishMigrationView(transition);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryDrainTransition(TTransition& transition) noexcept {
    return Space_->TryDrainTransition(transition);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PublishFinalView(TTransition& transition) noexcept {
    if (auto hotResize = HotResize_.TryLock()) {
        if (HotResize_.Phase() == EHotResizePhase::Cut) {
            auto spaceOp = BeginOperation();
            DrainHotResize(spaceOp);
        }
        if (HotResize_.Phase() != EHotResizePhase::Idle) {
            return false;
        }
        return Space_->PublishFinalView(transition);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AppendFreeHandles(TTransition& transition) noexcept {
    return Space_->AppendFreeHandles(transition);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AdvanceBucketResize() noexcept {
    const TSpaceState spaceState = Space_->CurrentSpaceState();
    if (!spaceState.Resizing()) {
        return false;
    }

    const TBucketResizeState resize = Space_->BucketResizeState();
    auto spaceOp = BeginOperation();
    switch (resize.Phase()) {
        case EBucketResizePhase::Prepared:
            return Table_.PrepareBucketSplit(spaceOp) && Table_.ActivateBucketSplit(spaceOp);

        case EBucketResizePhase::Activated:
            return Table_.PublishBucketSplit(spaceOp);

        case EBucketResizePhase::CursorPublished:
            if (resize.Direction() == EBucketResize::Shrinking) {
                return Table_.CloseBucketSplit(spaceOp);
            }
            break;

        case EBucketResizePhase::Closed:
            break;

        case EBucketResizePhase::Stable:
            return false;
    }

    if (!Table_.RemoveBucketSplit(spaceOp)) {
        return false;
    }
    return resize.Cursor() != resize.PairCount() || Space_->FinishBucketResize();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateCurrentLimit(ui64 limit) noexcept {
    if (limit == 0 || limit > HardLimit_.load(std::memory_order_relaxed)) {
        return false;
    }

    CurrentLimit_.store(limit, std::memory_order_relaxed);
    RefreshSoftLimit();
    NotifyResourcesAvailable();
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateSoftLimit(ui64 limit) noexcept {
    SoftLimitOverride_.store(limit, std::memory_order_release);
    RefreshSoftLimit();
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RefreshSoftLimit() noexcept {
    const ui64 currentLimit = CurrentLimit_.load(std::memory_order_relaxed);
    const ui64 configured = SoftLimitOverride_.load(std::memory_order_acquire);
    const bool hasOverride = configured != Max<ui64>();
    const ui64 softLimit = hasOverride ? Min(configured, currentLimit) : CalculateSoftLimit(currentLimit);
    SoftLimit_.store(softLimit, std::memory_order_relaxed);
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
bool TSharedCache::UpdateStickyLimit(ui64 limit) noexcept {
    if (limit > HardLimit_.load(std::memory_order_relaxed)) {
        return false;
    }
    StickyLimit_.store(limit, std::memory_order_relaxed);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateKeepColdLimit(ui64 limit) noexcept {
    if (limit > HardLimit_.load(std::memory_order_relaxed)) {
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
bool TSharedCache::TryAddRetainedBytes(ui64 bytes) noexcept {
    ui64 retained = RetainedBytes_.load(std::memory_order_relaxed);
    for (;;) {
        if (ExceedsLimit(retained, SoftLimit(), bytes)) {
            return false;
        }
        if (RetainedBytes_.compare_exchange_weak(
                retained, retained + bytes, std::memory_order_relaxed, std::memory_order_relaxed)) {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PromoteCold(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool force) noexcept {
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsCold()) {
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(expected.IsPageKind() && expected.Refs() != 0);
        const ui64 bytes = PageBytes(handle);
        TCacheCollection* owner = PageCollection(spaceOp, handle);
        const bool protectedPage =
            force || !owner || owner->CacheMode_.load(std::memory_order_acquire) != ECacheMode::Regular;
        if (protectedPage) {
            RetainedBytes_.fetch_add(bytes, std::memory_order_relaxed);
        } else if (!TryAddRetainedBytes(bytes)) {
            return false;
        }
        HotBytes_.fetch_add(bytes, std::memory_order_relaxed);
        HotPages_.fetch_add(1, std::memory_order_relaxed);
        const THandleState desired = expected.WithState(EHandleState::Hot).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
            AdvanceColdMembership(handle);
            ColdBytes_.fetch_sub(bytes, std::memory_order_relaxed);
            SubtractEstimatedPages(ColdPages_, EItemKind::Page);
            ColdItems_.fetch_sub(1, std::memory_order_relaxed);
            if (owner) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
            }
            return true;
        }
        HotBytes_.fetch_sub(bytes, std::memory_order_relaxed);
        SubtractEstimatedPages(HotPages_, EItemKind::Page);
        SubtractExactBytes(RetainedBytes_, bytes);
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryAddStickyOwnedBytes(ui64 bytes) noexcept {
    ui64 owned = StickyOwnedBytes_.load(std::memory_order_relaxed);
    for (;;) {
        const ui64 limit = StickyLimit_.load(std::memory_order_relaxed);
        if (bytes > limit || owned > limit - bytes) {
            return false;
        }
        if (StickyOwnedBytes_.compare_exchange_weak(
                owned, owned + bytes, std::memory_order_relaxed, std::memory_order_relaxed))
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
    if (bytes == 0 && pages == 0) {
        return true;
    }

    ui64 usage = PageUsage_.load(std::memory_order_relaxed);
    ui32 reclamationAttempts = 0;
    for (;;) {
        const ui64 currentLimit = CurrentLimit_.load(std::memory_order_relaxed);
        if (ExceedsLimit(usage, currentLimit, bytes)) {
            SoftLimitPressure_.store(true, std::memory_order_relaxed);
            // Aging/resizing can make progress without freeing bytes. Bound cooperative work so
            // concurrent activity cannot keep a requester here instead of putting it on the wait path.
            if (reclamationAttempts++ < SharedCacheTransitionWorkBatch && Reclaim(spaceOp, bytes)) {
                usage = PageUsage_.load(std::memory_order_relaxed);
                continue;
            }
            return false;
        }
        if (PageUsage_.compare_exchange_weak(
                usage, usage + bytes, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            OverallUsage_.fetch_add(bytes, std::memory_order_relaxed);
            ReservedBytes_.fetch_add(bytes, std::memory_order_relaxed);
            ReservedPages_.fetch_add(pages, std::memory_order_relaxed);
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseReservation(ui64 bytes, ui64 pages) noexcept {
    SubtractExactBytes(ReservedBytes_, bytes);
    SubtractExactBytes(ReservedPages_, pages);
    SubtractExactBytes(OverallUsage_, bytes);
    SubtractExactBytes(PageUsage_, bytes);
    NotifyResourcesAvailable();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateHardLimit(ui64 limit) noexcept {
    if (limit == 0 || limit < CurrentLimit_.load(std::memory_order_relaxed) || limit > ReservationLimit_) {
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
    HardLimit_.store(limit, std::memory_order_relaxed);
    StartHardTransition();
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EnforceCurrentLimit() noexcept {
    auto spaceOp = BeginOperation();
    return EnforceCurrentLimit(spaceOp, 0);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes, bool forceShrinkHot) noexcept {
    const ui64 softLimit = SoftLimit_.load(std::memory_order_relaxed);
    const bool hadCurrentPressure = ExceedsLimit(PageUsage(), CurrentLimit(), bytes);
    const ui32 resizeStep = HotResizeStep(static_cast<ui32>(spaceOp.EffectiveHotSlots()));
    // Retained cache bytes drive soft pressure; total page commitment independently drives current pressure.
    const bool enforceSoftLimit =
        SoftLimitPressure_.exchange(false, std::memory_order_relaxed) || RetainedBytes() > softLimit;

    bool progress = false;
    if (hadCurrentPressure) {
        for (ui32 attempt = 0; attempt < resizeStep && ExceedsLimit(PageUsage(), CurrentLimit(), bytes); ++attempt) {
            if (!ReclaimCold(spaceOp)) {
                break;
            }
            progress = true;
        }
    }
    const bool enforceCurrentLimit = ExceedsLimit(PageUsage(), CurrentLimit(), bytes);
    if (auto hotResize = HotResize_.TryLock()) {
        // Publication uses the same lock, so Hot maintenance can route newly added handles safely.
        auto hotSpaceOp = BeginOperation();
        const ui32 effectiveHotSlots = static_cast<ui32>(hotSpaceOp.EffectiveHotSlots());
        const ui32 hotResizeStep = HotResizeStep(effectiveHotSlots);
        RefreshEvictableByteBudget();
        if (HotResize_.Phase() == EHotResizePhase::Cut) {
            progress = DrainHotResize(hotSpaceOp) || progress;
        } else {
            const ui64 pageUsage = RetainedBytes();
            const ui64 physicalUsage = PageUsage();
            const ui64 currentLimit = CurrentLimit();
            const ui64 freeBytes = currentLimit - Min(currentLimit, physicalUsage);
            const ui64 coldBytes = LoadEstimatedBytes(ColdReclaimableBytes_);
            // Forecast the room remaining after admitting the caller's payload.
            const ui64 freeAfterAdmission = freeBytes - Min(freeBytes, bytes);
            const ui64 coldFreeBytes = coldBytes + freeAfterAdmission;
            const ui64 coldFreeHandles = ColdReclaimableItems() + hotSpaceOp.View().Free().Count();
            const ui64 payloadBudget = EvictableByteBudget();

            const ui64 coldTargetBytes = ColdTargetBytes_.load(std::memory_order_relaxed);
            const ui64 evictableHandles =
                hotSpaceOp.AllocationLimit() - 2 - Min(hotSpaceOp.AllocationLimit() - 2, StickyPages());
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
                enforceCurrentLimit && (ReservedBytes() != 0 || LoadEstimatedBytes(ColdBytes_) > coldBytes);
            const ui32 minimumHotSlots = MinimumHotSlots(hotSpaceOp, hotUnderPressure);
            if (effectiveHotSlots < minimumHotSlots) {
                targetHotSlots = minimumHotSlots;
            } else if (enforceCurrentLimit || forceShrinkHot || refillKeepCold ||
                       (pageUsage > softLimit && LoadEstimatedBytes(HotBytes_) > payloadBudget))
            {
                targetHotSlots = ShrinkHotTarget(hotSpaceOp, effectiveHotSlots, hotResizeStep, hotUnderPressure);
                if (targetHotSlots == 0) {
                    const bool needColdHandles =
                        coldFreeHandles < coldMinHandles ||
                        (KeepColdPressure_.load(std::memory_order_relaxed) && coldFreeHandles < coldTargetHandles);
                    if (needColdHandles && KeepColdOwnedBytes() != 0) {
                        KeepColdPressure_.store(true, std::memory_order_relaxed);
                        progress = ShrinkKeepCold(hotSpaceOp) || progress;
                    }
                    if (enforceCurrentLimit || forceShrinkHot || pageUsage > softLimit) {
                        progress = ProcessHotAtMinimum(hotSpaceOp) || progress;
                    }
                }
            } else if (!hadCurrentPressure && pageUsage <= softLimit &&
                       coldFreeBytes > ColdGrowBytes_.load(std::memory_order_relaxed) &&
                       coldFreeHandles > coldGrowHandles)
            {
                targetHotSlots = GrowHotTarget(hotSpaceOp, effectiveHotSlots, hotResizeStep);
                progress = GrowKeepCold() || progress;
            }

            if (targetHotSlots != 0 && Space_->BeginHotResize(targetHotSlots, HotResize_)) {
                if (HotResize_.Phase() == EHotResizePhase::Cut) {
                    progress = DrainHotResize(hotSpaceOp) || progress;
                }
            }
        }
    }

    if (enforceCurrentLimit || enforceSoftLimit || (softLimit == 0 && ColdReclaimableItems() != 0)) {
        progress = ReclaimToLimits(softLimit, resizeStep, spaceOp, bytes) || progress;
        if (RetainedBytes() > softLimit) {
            SoftLimitPressure_.store(true, std::memory_order_relaxed);
        }
    }
    return progress;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ProcessHotAtMinimum(TSpaceOperation& spaceOp) noexcept {
    bool progress = false;
    // Give every level a share of the bounded work even when the lower rings are large or empty.
    for (ui32 level = 0; level < 4; ++level) {
        const TRingView hot = spaceOp.Hot(level);
        const ui64 attempts = Min<ui64>(hot.Capacity, SharedCacheTransitionWorkBatch / 4);
        for (ui64 attempt = 0; attempt < attempts; ++attempt) {
            const ui64 raw = hot.Pop();
            if (raw != 0) {
                ProcessHot(spaceOp, static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
                progress = true;
            }
        }
    }
    return progress;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DrainHotResize() noexcept {
    if (auto hotResize = HotResize_.TryLock()) {
        auto spaceOp = BeginOperation();
        return DrainHotResize(spaceOp);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DrainHotResize(TSpaceOperation& spaceOp) noexcept {
    bool progress = false;
    while (HotResize_.Phase() == EHotResizePhase::Cut) {
        ui32 level;
        ui64 raw;
        if (!Space_->DrainHotResize(HotResize_, level, raw)) {
            break;
        }
        progress = true;
        if (raw != 0) {
            ProcessHot(spaceOp, static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
        }
    }
    return progress;
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::MinimumHotSlots(const TSpaceOperation& spaceOp, bool underPressure) const noexcept {
    const ui64 hotPages = HotPages();
    const ui64 hotBytes = LoadEstimatedBytes(HotBytes_);
    const ui64 meanPageBytes = hotPages ? Max<ui64>(1, hotBytes / hotPages)
                                        : AccountedPageBytes(Space_->ReservedConfiguration().ExpectedPageSize);
    const ui64 hotMinBytes =
        FractionCeil(EvictableByteBudget(), underPressure ? Policy_.HotMinUnderPressure : Policy_.HotMin);
    const ui64 byteMinimumSlots = hotMinBytes / meanPageBytes + (hotMinBytes % meanPageBytes != 0);
    return static_cast<ui32>(
        Min<ui64>(spaceOp.View().HotSlotCount, Max<ui64>(Policy_.MinHotSlotsUnderPressure, byteMinimumSlots)));
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::ShrinkHotTarget(
    const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step, bool underPressure) const noexcept {
    const ui32 minimum = MinimumHotSlots(spaceOp, underPressure);
    if (effectiveHotSlots <= minimum) {
        return 0;
    }
    return effectiveHotSlots - minimum > step ? effectiveHotSlots - step : minimum;
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::GrowHotTarget(const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step) const noexcept {
    const ui32 maximum = static_cast<ui32>(spaceOp.View().HotSlotCount);
    const ui32 growthStep = Max<ui32>(1, step / 2);
    const ui64 grownHotSlots = ui64(effectiveHotSlots) + growthStep;
    if (grownHotSlots < maximum) {
        return static_cast<ui32>(grownHotSlots);
    }
    return effectiveHotSlots < maximum ? maximum : 0;
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::HotResizeStep(ui32 effectiveHotSlots) const noexcept {
    return Max(Policy_.MinResizeStep, static_cast<ui32>(std::ceil(effectiveHotSlots * Policy_.ResizeStep)));
}

SHARED_CACHE_TEMPLATE
ui64 TSharedCache::CalculateSoftLimit(ui64 currentLimit) const noexcept {
    const ui64 gap =
        Min(currentLimit, Max(Policy_.MinCurrentLimitGap, FractionCeil(currentLimit, Policy_.CurrentLimitGap)));
    return currentLimit - gap;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimToLimits(ui64 softLimit, ui32 step, TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    bool progress = false;
    for (ui32 attempt = 0; attempt < step; ++attempt) {
        if (ExceedsLimit(PageUsage(), CurrentLimit(), bytes)) {
            if (!ReclaimCold(spaceOp) && !ReclaimKeepCold(spaceOp)) {
                break;
            }
        } else if (softLimit == 0 && ColdReclaimableItems() != 0) {
            if (!ReclaimCold(spaceOp)) {
                break;
            }
        } else if (RetainedBytes() > softLimit) {
            if (!DemoteKeepColdForSoftLimit(spaceOp)) {
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
bool TSharedCache::StartHardTransition() noexcept {
    if (HardTransition_.Phase() != ETransitionPhase::Idle) {
        return false;
    }

    const TSharedCacheCapacity& current = Space_->CurrentConfiguration();
    if (current.AddressBits == HardTarget_.AddressBits) {
        return false;
    }

    const ui8 nextAddressBits =
        current.AddressBits < HardTarget_.AddressBits ? current.AddressBits + 1 : current.AddressBits - 1;
    TSharedCacheCapacity next;
    if (!TryCalculateSharedCacheFootprint(
            nextAddressBits, current.ExpectedPageSize, current.FixedBytes, Space_->HazardCount(), next))
    {
        return false;
    }
    next.Limit = HardLimit_.load(std::memory_order_relaxed);
    return PrepareTransition(next, HardTransition_);
}

SHARED_CACHE_TEMPLATE
TSharedCacheItemRef TSharedCache::TryAcquirePageForRelocation(TSpaceOperation& spaceOp, ui32 index) noexcept {
    THandle& source = spaceOp.Handles()[index];
    ui64 expectedRaw = source.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        const bool ordinary =
            (expected.IsHot() || expected.IsCold()) && expected.IsStickyNoneField() && expected.Refs() != 0;
        const bool unsticky = expected.IsSticky() && expected.IsTransitionField();
        if (!expected.IsPageKind() || (!ordinary && !unsticky))
        {
            return {};
        }
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs();
        if (source.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            return TSharedCacheItemRef(TCacheItem::Make(expected.Version(), index));
        }
    }
}

SHARED_CACHE_TEMPLATE
TCacheItem TSharedCache::PreparePageReplacement(TSpaceOperation& spaceOp, TCacheItem sourceItem) noexcept {
    const TCacheItem replacementItem = Allocate(spaceOp);
    if (replacementItem.IsNull()) {
        return {};
    }

    THandle& source = spaceOp.Handles()[sourceItem.Index()];
    const TCollectionCacheItem collection =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(source.Key0.load(std::memory_order_relaxed)));
    const ui64 offset = source.Key1.load(std::memory_order_relaxed);
    const ui64 size = source.Key2OrSize.load(std::memory_order_relaxed);
    const NTable::NPage::EPage type = source.Metadata.Page.Type;
    const ui32 crc32 = source.Metadata.Page.Crc32;
    const THandleState sourceState = THandleState::FromRaw(source.State.load(std::memory_order_relaxed));
    const bool restoreSticky = sourceState.IsSticky() && sourceState.IsTransitionField();
    NActors::TSharedData data = source.Body.PageBuffer.Share(size);

    // Relocation transfers the source's sticky charge; claiming a second budget slot would double count it.
    InitializePage(spaceOp, replacementItem, collection, offset, size, type, crc32, EStickyState::None);
    THandle& replacement = spaceOp.Handles()[replacementItem.Index()];
    DeletePendingFetch(replacement);
    new (&replacement.Body.PageBuffer) NActors::TSharedData::TBuffer(std::move(data).Extract());
    const THandleState initialized = THandleState::FromRaw(replacement.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(replacementItem.Matches(initialized) && initialized.IsBegin() && initialized.Refs() == 0);
    const EHandleState replacementState = restoreSticky ? EHandleState::Sticky : sourceState.State();
    replacement.State.store(initialized.WithState(replacementState)
                                .WithSticky(restoreSticky ? EStickyState::Sticky : EStickyState::None)
                                .WithRefs(1)
                                .Raw(), std::memory_order_release);
    if (restoreSticky) {
        Y_DEBUG_ABORT_UNLESS(source.NextInOwner.load(std::memory_order_acquire) == 0);
        TOperationItemRef collectionRef(spaceOp);
        if (!LinkStickyPage(spaceOp, replacementItem, collectionRef.Get())) {
            DiscardPageReplacement(spaceOp, replacementItem);
            return {};
        }
    }
    return replacementItem;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DiscardPageReplacement(TSpaceOperation& spaceOp, TCacheItem replacementItem) noexcept {
    THandle& replacement = spaceOp.Handles()[replacementItem.Index()];
    const THandleState state = THandleState::FromRaw(replacement.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(replacementItem.Matches(state) && state.IsPageKind() && state.IsSticky() &&
                         state.Refs() == 1 && replacement.NextInOwner.load(std::memory_order_relaxed) == 0);
    replacement.Body.PageBuffer.~TBuffer();
    replacement.Body.Fetch = nullptr;
    replacement.State.store(state.WithState(EHandleState::Begin).WithSticky(EStickyState::None).WithRefs(0).Raw(),
        std::memory_order_relaxed);
    DiscardCandidate(spaceOp, replacementItem, EItemKind::Page);
}

SHARED_CACHE_TEMPLATE
EHandleState TSharedCache::PublishPageReplacement(
    TSpaceOperation& spaceOp, TCacheItem sourceItem, TCacheItem replacementItem) noexcept {
    THandle& source = spaceOp.Handles()[sourceItem.Index()];
    THandle& replacement = spaceOp.Handles()[replacementItem.Index()];
    const TCacheItem successor = Table_.FreezeNext(spaceOp, sourceItem).WithoutFrozen();
    replacement.Next.store(successor.Raw(), std::memory_order_relaxed);
    source.Next.store(replacementItem.WithFrozen().Raw(), std::memory_order_release);

    ui64 expectedRaw = source.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(sourceItem.Matches(expected) && expected.IsPageKind() &&
                             (((expected.IsHot() || expected.IsCold()) && expected.IsStickyNoneField()) ||
                                 (expected.IsSticky() && expected.IsTransitionField())) &&
                             expected.Refs() > 0);
        const THandleState desired = expected.WithState(EHandleState::Replacing).WithFrequency(0);
        if (source.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            // A held Cold page may have been promoted while the replacement was prepared.
            // Match the source class before the replacement becomes discoverable.
            ui64 replacementRaw = replacement.State.load(std::memory_order_relaxed);
            for (;;) {
                const THandleState prepared = THandleState::FromRaw(replacementRaw);
                const EHandleState destination = expected.State();
                if (destination == prepared.State() ||
                    replacement.State.compare_exchange_weak(replacementRaw, prepared.WithState(destination).Raw(),
                        std::memory_order_release, std::memory_order_relaxed)) {
                    break;
                }
            }
            InvokeHook(ESharedCacheHookPoint::AfterReplacingPublished, sourceItem);
            const TSharedCacheKey key = LoadSharedCacheKey(source, EItemKind::Page);
            Table_.PublishPageReplacement(sourceItem, replacementItem, key, spaceOp);
            return expected.State();
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::CompletePageReplacement(TSpaceOperation& spaceOp, TCacheItem sourceItem) noexcept {
    THandle& source = spaceOp.Handles()[sourceItem.Index()];
    ui64 expectedRaw = source.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(
            sourceItem.Matches(expected) && expected.IsPageKind() && expected.IsReplacing() && expected.Refs() > 0);
        const THandleState desired = expected.WithState(EHandleState::Replaced);
        if (source.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            InvokeHook(ESharedCacheHookPoint::AfterReplacedPublished, sourceItem);
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::RelocatePage(TSpaceOperation& spaceOp, ui32 index) noexcept {
    TOperationItemRef sourceRef(spaceOp, TryAcquirePageForRelocation(spaceOp, index));
    if (!sourceRef) {
        return false;
    }
    const TCacheItem sourceItem = sourceRef.CacheItem();
    const TCacheItem replacementItem = PreparePageReplacement(spaceOp, sourceItem);
    if (replacementItem.IsNull()) {
        return false;
    }

    THandle& source = spaceOp.Handles()[sourceItem.Index()];
    const EHandleState oldState = PublishPageReplacement(spaceOp, sourceItem, replacementItem);
    if (oldState == EHandleState::Cold) {
        AdvanceColdMembership(source);
    }
    if (oldState == EHandleState::Hot) {
        RouteHot(spaceOp, EHotLevel::L1, replacementItem);
    }
    CompletePageReplacement(spaceOp, sourceItem);
    return true;
}

SHARED_CACHE_TEMPLATE
typename TSharedCache::EShrinkCutStatus TSharedCache::ReclaimShrinkHandle(
    TSpaceOperation& spaceOp, ui32 index) noexcept {
    const THandleState state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_acquire));
    if (state.IsFree()) {
        Y_DEBUG_ABORT_UNLESS(state.Refs() == 0);
        return EShrinkCutStatus::Complete;
    }
    if (state.IsHot()) {
        if (state.Refs() != 0 && RelocatePage(spaceOp, index)) {
            return EShrinkCutStatus::Progress;
        }
        if (EvictFromHot(spaceOp, index)) {
            return EShrinkCutStatus::Progress;
        }
    }
    if (state.IsCold()) {
        if (state.Refs() != 0 && RelocatePage(spaceOp, index)) {
            return EShrinkCutStatus::Progress;
        }
        if (state.Refs() == 0) {
            const ui32 coldVersion = spaceOp.Handles()[index].ColdVersion.load(std::memory_order_acquire);
            if (EraseCold(spaceOp, TCacheItem::Make(coldVersion & MaxItemVersion, index))) {
                return EShrinkCutStatus::Progress;
            }
        }
    }
    if (state.IsKeepCold()) {
        const ui32 coldVersion = spaceOp.Handles()[index].ColdVersion.load(std::memory_order_acquire);
        if (DemoteKeepCold(spaceOp, TCacheItem::Make(coldVersion & MaxItemVersion, index))) {
            return EShrinkCutStatus::Progress;
        }
    }
    if (state.IsPageKind() && state.IsSticky()) {
        if (state.IsStickyField()) {
            return UnstickyCutPages(spaceOp, TCacheItem::Make(state.Version(), index), spaceOp.AllocationLimit())
                       ? EShrinkCutStatus::Progress
                       : EShrinkCutStatus::Blocked;
        }
        if (state.IsTransitionField() && RelocatePage(spaceOp, index)) {
            return EShrinkCutStatus::Progress;
        }
    }
    return EShrinkCutStatus::Blocked;
}

SHARED_CACHE_TEMPLATE
typename TSharedCache::EShrinkCutStatus TSharedCache::ReclaimShrinkCut() noexcept {
    TTransition& transition = HardTransition_;
    const ui64 allocationLimit = transition.TargetConfiguration().HandleCount();
    const TCacheItem spare = Space_->TakeCutSpare(allocationLimit);
    if (!spare.IsNull()) {
        auto spaceOp = BeginOperation();
        Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(spare) && spare.Index() >= allocationLimit);
        THandle& handle = spaceOp.Handles()[spare.Index()];
        const THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(spare.Matches(expected) && expected.IsBegin() && expected.Refs() == 0);
        handle.State.store(expected.WithState(EHandleState::Free).Raw(), std::memory_order_release);
        return EShrinkCutStatus::Progress;
    }

    auto spaceOp = BeginOperation();
    const ui64 handleCount = spaceOp.HandleCount();
    ui64& index = transition.NextWorkIndex_;
    if (index < allocationLimit || index > handleCount) {
        index = allocationLimit;
        transition.CutBlocked_ = false;
    }
    const ui64 end = Min(handleCount, index + SharedCacheTransitionWorkBatch);
    while (index < end) {
        switch (ReclaimShrinkHandle(spaceOp, static_cast<ui32>(index))) {
            case EShrinkCutStatus::Progress:
                return EShrinkCutStatus::Progress;
            case EShrinkCutStatus::Blocked:
                transition.CutBlocked_ = true;
                break;
            case EShrinkCutStatus::Complete:
                break;
        }
        ++index;
    }
    if (index < handleCount) {
        return EShrinkCutStatus::Progress;
    }
    const bool blocked = transition.CutBlocked_;
    index = 0;
    transition.CutBlocked_ = false;
    return blocked ? EShrinkCutStatus::Blocked : EShrinkCutStatus::Complete;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::RunMaintenance() noexcept {
    Y_DEBUG_ABORT_UNLESS(CurrentLimit() <= HardLimit());
    if (KeepColdOwnedBytes() > KeepColdLimit() || KeepColdTrimActive_.load(std::memory_order_relaxed))
    {
        auto spaceOp = BeginOperation();
        if (TrimKeepCold(spaceOp)) {
            return true;
        }
    }
    ui64 remaining = KeepModeSweepRemaining_.load(std::memory_order_acquire);
    while (remaining != 0) {
        const ui64 batch = Min<ui64>(remaining, SharedCacheTransitionWorkBatch);
        if (KeepModeSweepRemaining_.compare_exchange_weak(
                remaining, remaining - batch, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            auto spaceOp = BeginOperation();
            for (ui64 index = 0; index < batch; ++index) {
                const ui64 raw = spaceOp.KeepCold().Pop();
                if (raw != 0) {
                    RouteKeepCold(spaceOp, TCacheItem::FromRaw(raw));
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
bool TSharedCache::FinalDrain(TTransition& transition) noexcept {
    auto spaceOp = BeginOperation();
    return Space_->FinalDrain(
        transition,
        [&](ui32 level, ui64 raw) noexcept {
            RouteHot(spaceOp, static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
        },
        [&](ui64 raw) noexcept {
            const ui64 previous = ColdRingEntries_.fetch_sub(1, std::memory_order_relaxed);
            Y_ABORT_UNLESS(previous != 0);
            RouteCold(spaceOp, TCacheItem::FromRaw(raw));
        },
        [&](ui64 raw) noexcept {
            RouteKeepCold(spaceOp, TCacheItem::FromRaw(raw));
        });
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryReleaseTransition(TTransition& transition) noexcept {
    return Space_->TryReleaseTransition(transition);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::CommitTransition(TTransition& transition) noexcept {
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    const ui64 targetLimit = transition.TargetConfiguration().Limit;
    const bool commitTargetLimit = &transition != &HardTransition_;
    if (!Space_->CommitTransition(transition)) {
        return false;
    }

    if (targetStaticBytes > oldStaticBytes) {
        const ui64 delta = targetStaticBytes - oldStaticBytes;
        StaticBytes_.fetch_add(delta, std::memory_order_relaxed);
        SubtractExactBytes(StaticDeltaBytes_, delta);
    } else {
        const ui64 delta = oldStaticBytes - targetStaticBytes;
        SubtractExactBytes(StaticBytes_, delta);
        SubtractExactBytes(OverallUsage_, delta);
    }
    if (commitTargetLimit) {
        TargetHandleCount_.store(transition.TargetConfiguration().HandleCount(), std::memory_order_relaxed);
        HardLimit_.store(targetLimit, std::memory_order_relaxed);
    }
    RefreshSoftLimit();
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AbandonTransition(TTransition& transition) noexcept {
    const ETransitionPhase phase = transition.Phase();
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    if (!Space_->AbandonTransition(transition)) {
        return false;
    }
    if ((phase == ETransitionPhase::Prepare || phase == ETransitionPhase::InitializeGrowth) &&
        targetStaticBytes > oldStaticBytes)
    {
        const ui64 delta = targetStaticBytes - oldStaticBytes;
        SubtractExactBytes(StaticDeltaBytes_, delta);
        SubtractExactBytes(OverallUsage_, delta);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimCold(TSpaceOperation& spaceOp) noexcept {
    const TRingView cold = spaceOp.Cold();
    for (ui64 attempt = 0; attempt < cold.Capacity; ++attempt) {
        if (ColdRingEntries_.load(std::memory_order_relaxed) == 0) {
            return false;
        }
        InvokeHook(ESharedCacheHookPoint::BeforeColdRingPop, {});
        const ui64 rawItem = cold.Pop();
        if (rawItem != 0) {
            const ui64 previous = ColdRingEntries_.fetch_sub(1, std::memory_order_relaxed);
            Y_ABORT_UNLESS(previous != 0);
            if (EraseCold(spaceOp, TCacheItem::FromRaw(rawItem))) {
                return true;
            }
        }
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DemoteKeepColdForSoftLimit(TSpaceOperation& spaceOp) noexcept {
    const TRingView keep = spaceOp.KeepCold();
    for (ui64 attempt = 0; attempt < keep.Capacity && KeepColdOwnedBytes() != 0; ++attempt) {
        const ui64 raw = keep.Pop();
        if (raw == 0) {
            continue;
        }
        const TCacheItem item = TCacheItem::FromRaw(raw);
        if (DemoteKeepCold(spaceOp, item)) {
            return true;
        }
        // The FIFO entry is gone even when a structural ref delays demotion.
        WithdrawKeepColdEntry(spaceOp, item);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimKeepCold(TSpaceOperation& spaceOp) noexcept {
    const TRingView keep = spaceOp.KeepCold();
    for (ui64 attempt = 0; attempt < keep.Capacity && KeepColdOwnedBytes() != 0; ++attempt) {
        const ui64 raw = keep.Pop();
        if (raw == 0) {
            continue;
        }
        const TCacheItem item = TCacheItem::FromRaw(raw);
        if (!MatchesKeepColdEntry(spaceOp, item)) {
            continue;
        }
        if (EraseCold(spaceOp, item)) {
            return true;
        }
        WithdrawKeepColdEntry(spaceOp, item);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::Reclaim(TSpaceOperation& spaceOp, ui64 bytes) noexcept {
    if (ReclaimCold(spaceOp) || ReclaimKeepCold(spaceOp)) {
        return true;
    }
    // Cold is empty, so only moving the hot/cold boundary can make room, and that is EnforceCurrentLimit's job.
    return EnforceCurrentLimit(spaceOp, bytes);
}

SHARED_CACHE_TEMPLATE
TCacheCollection* TSharedCache::ReleaseTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
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

        TCacheCollection* owner = expected.IsPageKind() ? PageCollection(spaceOp, handle) : nullptr;
        const TSharedCacheKey key = LoadSharedCacheKey(handle, expected.Kind());
        Table_.FreezeNext(spaceOp, cacheItem);
        Table_.BridgeTombstone(cacheItem, key, spaceOp);
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
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(cacheItem.Matches(expected) && expected.IsPageKind() && expected.Refs() > 0);
        if (expected.IsReplaced()) {
            ReleaseReplaced(spaceOp, cacheItem);
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
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            TOperationItemRef replacementRef(
                spaceOp, TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire)).WithoutFrozen());
            Y_DEBUG_ABORT_UNLESS(!replacementRef.CacheItem().IsNull() && replacementRef.CacheItem() != cacheItem);
            Y_ABORT_UNLESS(handle.NextInOwner.load(std::memory_order_acquire) == 0);
            DeleteReplacedPage(handle);
            Y_ABORT_UNLESS(spaceOp.ReturnFreeHandle(cacheItem.Index(), nextVersion));
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::Release(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    TCacheCollection* owner = ReleaseItem(spaceOp, cacheItem);
    if (owner) {
        DropOwnerReference(spaceOp, *owner);
    }
}

SHARED_CACHE_TEMPLATE
TCacheCollection* TSharedCache::ReleaseItem(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (cacheItem.Matches(expected) && expected.IsTombstone()) {
            return ReleaseTombstone(spaceOp, cacheItem);
        }
        if (cacheItem.Matches(expected) && expected.IsReplacing()) {
            ReleaseReplacing(spaceOp, cacheItem);
            return nullptr;
        }
        if (cacheItem.Matches(expected) && expected.IsReplaced()) {
            ReleaseReplaced(spaceOp, cacheItem);
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
            const THandleState desired = expected.WithState(EHandleState::Tombstone).WithFrequency(0);
            if (!handle.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed)) {
                continue;
            }
            TOperationItemRef ownerRef(spaceOp, cacheItem);
            InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
            if (TCacheCollection* owner = PageCollection(spaceOp, handle);
                owner && owner->CacheMode_.load(std::memory_order_acquire) == ECacheMode::TryKeepInMemory &&
                owner->IsKeepAllowedPage(handle.Metadata.Page.Type)) {
                this->NotifyKeepPageEviction(owner->Id(), owner->CacheItem, owner->KeepGeneration(),
                    NTable::NPage::TPageLocation(
                        NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
                        handle.Key2OrSize.load(std::memory_order_relaxed), handle.Metadata.Page.Type,
                        handle.Metadata.Page.Crc32));
            }
            AdvanceColdMembership(handle);
            Table_.CompleteTombstone(cacheItem, LoadSharedCacheKey(handle, EItemKind::Page), spaceOp);
            return nullptr;
        }
        Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) && expected.Refs() > 0);
        if (expected.IsCold() && expected.IsPageKind() && expected.Refs() == 1 && WantsKeepCold(spaceOp, handle) &&
            TryAddRetainedBytes(PageBytes(handle)))
        {
            const ui64 bytes = PageBytes(handle);
            TCacheCollection* owner = PageCollection(spaceOp, handle);
            if (owner) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
            }
            AddKeepColdOwnedBytes(bytes);
            const THandleState desired = expected.DecrementRefs().WithState(EHandleState::KeepCold);
            if (handle.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                AdvanceColdMembership(handle);
                TransferEstimatedBytes(ColdBytes_, KeepColdBytes_, bytes);
                TransferEstimatedPages(ColdPages_, KeepColdPages_, EItemKind::Page);
                ColdItems_.fetch_sub(1, std::memory_order_relaxed);
                const ui32 version = handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
                RouteKeepCold(spaceOp, TCacheItem::Make(version, cacheItem.Index()));
                NotifyResourcesAvailable();
                return nullptr;
            }
            SubtractExactBytes(KeepColdOwnedBytes_, bytes);
            SubtractExactBytes(RetainedBytes_, bytes);
            if (owner) {
                AddActivePageBytes(*owner, handle.Metadata.Page.Type, -static_cast<i64>(bytes));
            }
            continue;
        }
        const bool publishCold = expected.IsCold() && expected.Refs() == 1;
        const bool lastKeepColdRef = expected.IsKeepCold() && expected.Refs() == 1;
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
                RouteCold(spaceOp, TCacheItem::Make(coldVersion & MaxItemVersion, cacheItem.Index()));
            } else if (lastKeepColdRef && (!IsKeepColdQueued(handle) || !WantsKeepCold(spaceOp, handle)))
            {
                // Check after releasing the final ref: a concurrent trim may have withdrawn this page's ring entry
                // while the ref was being dropped.
                const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire);
                DemoteKeepCold(spaceOp, TCacheItem::Make(coldVersion & MaxItemVersion, cacheItem.Index()));
            }
            if (expected.IsReady() && desired.Refs() == 0 && expected.IsStickyNoneField()) {
                NotifyResourcesAvailable();
            }
            return nullptr;
        }
    }
}

#undef TSharedCacheCollectionRef
#undef TSharedCacheItemRef
#undef TPageFetch
#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

template class TSharedCacheThreadBinding<TProdTraits>;
template class TSharedCachePageRefImpl<TProdTraits>;
template class TSharedCacheCollectionRefImpl<TProdTraits>;
template class TPageFetchImpl<TProdTraits>;
template class TSharedCacheImpl<TProdTraits>;
template class TSharedCacheThreadBinding<TTestTraits>;
template class TSharedCachePageRefImpl<TTestTraits>;
template class TSharedCacheCollectionRefImpl<TTestTraits>;
template class TPageFetchImpl<TTestTraits>;
template class TSharedCacheImpl<TTestTraits>;

} // namespace NKikimr::NSharedCache
