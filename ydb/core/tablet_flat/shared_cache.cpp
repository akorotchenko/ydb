#include "shared_cache.h"
#include "shared_cache_traits.h"

#include <util/generic/cast.h>
#include <util/system/spinlock.h>

#include <cmath>
#include <type_traits>
#include <utility>

namespace NKikimr::NSharedCache {
namespace {

    // usage + bytes exceeds limit
    [[maybe_unused]] inline bool ExceedsLimit(ui64 usage, ui64 limit, ui64 bytes) noexcept {
        return usage + bytes > limit;
    }

} // anonymous namespace

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
{}

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
    return collection.CacheMode_.load(std::memory_order_acquire) == StickyCacheMode;
}

#undef TSharedCacheCollectionRef
#undef TSharedCacheItemRef
#undef TPageFetch
#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

} // namespace NKikimr::NSharedCache
