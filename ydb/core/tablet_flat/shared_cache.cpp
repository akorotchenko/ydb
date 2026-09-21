#include "shared_cache.h"
#include "shared_cache_pages.h"
#include "shared_cache_traits.h"

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/library/actors/core/thread_context.h>

#include <util/generic/cast.h>

#include <cmath>
#include <utility>

namespace NKikimr::NSharedCache {
namespace {

    thread_local void* BoundProdSharedCachePages = nullptr;

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
    }
    return BoundProdSharedCachePages;
}

bool TProdTraits::InstallSharedCachePages(void* cache) noexcept {
    if (!HasAppData()) {
        return true;
    }
    const TIntrusivePtr<TSharedCachePages>& owner = AppData()->SharedCachePages;
    if (!owner || (owner->Cache && owner->Cache.Get() != cache)) {
        return false;
    }
    owner->Cache = TIntrusivePtr<TThrRefBase>(static_cast<TSharedCacheImpl<TProdTraits>*>(cache));
    return true;
}

void TProdTraits::BindSharedCachePages(void* cache) noexcept {
    Y_DEBUG_ABORT_UNLESS(!BoundProdSharedCachePages);
    BoundProdSharedCachePages = cache;
}

void TProdTraits::UnbindSharedCachePages(void* cache) noexcept {
    Y_DEBUG_ABORT_UNLESS(BoundProdSharedCachePages == cache);
    BoundProdSharedCachePages = nullptr;
}

ui32 TProdTraits::CurrentWorkerIndex() noexcept {
    return NActors::TlsThreadContext ? NActors::TlsThreadContext->WorkerId() : 0;
}

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TSharedCacheItemRef TSharedCacheItemRefImpl<TTraits>
#define TSharedCacheCollectionRef TSharedCacheCollectionRefImpl<TTraits>
#define TPageFetchToken TPageFetchTokenImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
TSharedCacheThreadBinding<TTraits>::TSharedCacheThreadBinding(void* cache, TSpaceHazardBinding&& hazard) noexcept
    : Cache_(cache)
    , Hazard_(std::move(hazard))
{
    TTraits::BindSharedCachePages(cache);
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
        TTraits::UnbindSharedCachePages(Cache_);
        Cache_ = nullptr;
        Hazard_ = {};
    }
}

SHARED_CACHE_TEMPLATE
TPageFetchToken::TPageFetchTokenImpl(TSharedCache* cache, TSharedCacheItemRef&& ref, TPageFetch* fetch) noexcept
    : Cache_(cache)
    , Fetch_(fetch)
    , Ref_(std::move(ref))
{
    Y_DEBUG_ABORT_UNLESS(Cache_ && Fetch_ && Ref_ && Fetch_->Page().CacheItem() == Ref_.CacheItem());
}

SHARED_CACHE_TEMPLATE
TPageFetchToken::TPageFetchTokenImpl(TPageFetchToken&& other) noexcept
    : Cache_(std::move(other.Cache_))
    , Fetch_(std::move(other.Fetch_))
    , Ref_(std::move(other.Ref_))
{
}

SHARED_CACHE_TEMPLATE
TPageFetchToken& TPageFetchToken::operator=(TPageFetchToken&& other) noexcept {
    if (this != &other) {
        FailReady();
        Cache_ = std::move(other.Cache_);
        Fetch_ = std::move(other.Fetch_);
        Ref_ = std::move(other.Ref_);
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TPageFetchToken::~TPageFetchTokenImpl() {
    FailReady();
}

SHARED_CACHE_TEMPLATE
TPageCacheItem TPageFetchToken::CacheItem() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    return TPageCacheItem::FromValidated(Ref_.CacheItem());
}

SHARED_CACHE_TEMPLATE
NTable::NPage::TPageLocation TPageFetchToken::Location() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_ && Cache_ && Fetch_);
    auto binding = Cache_->BindCurrentThreadHazard();
    auto spaceOp = Cache_->BeginOperation();
    const TCacheItem cacheItem = Ref_.CacheItem();
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(
        cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() && handle.Body.Fetch == Fetch_.Get());
    return {
        NTable::NPage::TPageOffset::FromRaw(handle.Key1.load(std::memory_order_relaxed)),
        handle.Key2OrSize.load(std::memory_order_relaxed),
        handle.Metadata.Page.Type,
        handle.Metadata.Page.Crc32,
    };
}

SHARED_CACHE_TEMPLATE
ui64 TPageFetchToken::Size() const noexcept {
    return Location().Size;
}

SHARED_CACHE_TEMPLATE
bool TPageFetchToken::Dispatch() noexcept {
    if (!Ref_) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(Cache_ && Fetch_);
    auto binding = Cache_->BindCurrentThreadHazard();
    auto spaceOp = Cache_->BeginOperation();
    return Cache_->DispatchFetch(spaceOp, Ref_.CacheItem(), *Fetch_);
}

SHARED_CACHE_TEMPLATE
bool TPageFetchToken::MakeReady(NActors::TSharedData&& data) noexcept {
    if (!Ref_) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(Cache_ && Fetch_);
    bool ready;
    {
        auto binding = Cache_->BindCurrentThreadHazard();
        auto spaceOp = Cache_->BeginOperation();
        TOperationItemRef owner(spaceOp, std::move(Ref_));
        ready = Cache_->CompleteFetch(owner, *Fetch_, std::move(data));
    }
    Fetch_.Drop();
    Cache_.Drop();
    return ready;
}

SHARED_CACHE_TEMPLATE
bool TPageFetchToken::FailReady() noexcept {
    if (!Ref_) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(Cache_ && Fetch_);
    bool failed;
    {
        auto binding = Cache_->BindCurrentThreadHazard();
        auto spaceOp = Cache_->BeginOperation();
        TOperationItemRef owner(spaceOp, std::move(Ref_));
        failed = Cache_->FailFetch(owner, *Fetch_);
    }
    Fetch_.Drop();
    Cache_.Drop();
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
    , SoftLimit_(CalculateSoftLimit(hardLimit))
    , HardLimit_(hardLimit)
    , ReservationLimit_(Space_->ReservedConfiguration().Limit)
    , HardTarget_(Space_->CurrentConfiguration())
{
}

SHARED_CACHE_TEMPLATE
TIntrusivePtr<TSharedCache> TSharedCache::Create(THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits)
{
    if (!space || hardLimit < space->CurrentConfiguration().TotalBytes ||
        hardLimit > space->ReservedConfiguration().Limit)
    {
        return {};
    }
    Y_DEBUG_ABORT_UNLESS(Policy_.CurrentLimitGap >= 0.0 && Policy_.CurrentLimitGap < 1.0 && Policy_.ColdMin >= 0.0 &&
                         Policy_.ColdMin < Policy_.Grow && Policy_.Grow <= 1.0 && Policy_.ResizeStep > 0.0 &&
                         Policy_.ResizeStep <= 1.0 && Policy_.MinHotSlots > 0 && Policy_.MinResizeStep > 0 &&
                         Policy_.MinHotSlots <= space->CurrentConfiguration().HotSlotCount() &&
                         SharedCacheHotLayoutIsValid(Policy_.MinHotSlots));
    TSharedCacheCapacity capacity;
    if (!TryCalculateSharedCacheCapacity(hardLimit, space->CurrentConfiguration().ExpectedPageSize,
            space->CurrentConfiguration().FixedBytes, space->HazardCount(), capacity) ||
        capacity.AddressBits != space->CurrentConfiguration().AddressBits)
    {
        return {};
    }
    TIntrusivePtr<TSharedCache> cache(new TSharedCache(std::move(space), hardLimit, std::move(traits)));
    if (!TTraits::InstallSharedCachePages(cache.Get())) {
        return {};
    }
    return cache;
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits>::TSharedCachePageRefImpl(
    TSharedCacheItemRef&& ref, NActors::TSharedData&& data, NTable::NPage::EPage type) noexcept
    : Ref_(std::move(ref))
    , Data_(std::move(data))
    , Type_(type)
{
    Y_DEBUG_ABORT_UNLESS(Ref_ && Data_ && Type_ != NTable::NPage::EPage::Undef);
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits>::TSharedCachePageRefImpl(TSharedCachePageRefImpl&& other) noexcept
    : Ref_(std::move(other.Ref_))
    , Data_(std::move(other.Data_))
    , Type_(std::exchange(other.Type_, NTable::NPage::EPage::Undef))
{
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits>& TSharedCachePageRefImpl<TTraits>::operator=(
    TSharedCachePageRefImpl&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
        Data_ = std::move(other.Data_);
        Type_ = std::exchange(other.Type_, NTable::NPage::EPage::Undef);
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TPageCacheItem TSharedCachePageRefImpl<TTraits>::CacheItem() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    return TPageCacheItem::FromValidated(Ref_.CacheItem_);
}

SHARED_CACHE_TEMPLATE
const char* TSharedCachePageRefImpl<TTraits>::data() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_ && Data_);
    return Data_.data();
}

SHARED_CACHE_TEMPLATE
size_t TSharedCachePageRefImpl<TTraits>::size() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_ && Data_);
    return Data_.size();
}

SHARED_CACHE_TEMPLATE
NActors::TSharedData TSharedCachePageRefImpl<TTraits>::ShareData() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_ && Data_);
    return Data_;
}

SHARED_CACHE_TEMPLATE
NTable::NPage::EPage TSharedCachePageRefImpl<TTraits>::GetType() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_ && Type_ != NTable::NPage::EPage::Undef);
    return Type_;
}

SHARED_CACHE_TEMPLATE
void TSharedCachePageRefImpl<TTraits>::Drop() noexcept {
    Data_ = {};
    Type_ = NTable::NPage::EPage::Undef;
    Ref_.Drop();
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef::TSharedCacheCollectionRefImpl(TSharedCacheItemRef&& ref, TCollection* collection) noexcept
    : Ref_(std::move(ref))
    , Collection_(collection)
{
    Y_DEBUG_ABORT_UNLESS(Ref_ && Collection_);
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef::TSharedCacheCollectionRefImpl(TSharedCacheCollectionRef&& other) noexcept
    : Ref_(std::move(other.Ref_))
    , Collection_(std::exchange(other.Collection_, nullptr))
{
}

SHARED_CACHE_TEMPLATE
TSharedCacheCollectionRef& TSharedCacheCollectionRef::operator=(TSharedCacheCollectionRef&& other) noexcept {
    if (this != &other) {
        Drop();
        Ref_ = std::move(other.Ref_);
        Collection_ = std::exchange(other.Collection_, nullptr);
    }
    return *this;
}

SHARED_CACHE_TEMPLATE
TCollectionCacheItem TSharedCacheCollectionRef::CacheItem() const noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_);
    return TCollectionCacheItem::FromValidated(Ref_.CacheItem_);
}

SHARED_CACHE_TEMPLATE
TCollection& TSharedCacheCollectionRef::GetCollection() noexcept {
    Y_DEBUG_ABORT_UNLESS(Ref_ && Collection_);
    return *Collection_;
}

SHARED_CACHE_TEMPLATE
void TSharedCacheCollectionRef::Drop() noexcept {
    Collection_ = nullptr;
    Ref_.Drop();
}

SHARED_CACHE_TEMPLATE
TSharedCache::~TSharedCacheImpl() {
    const TSpaceView& space = Space_->SpaceView(Space_->CurrentSpaceState());
    for (ui64 index = 2; index < space.HandleCount; ++index) {
        THandle& handle = space.Handles[index];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        if (state.IsReplacing() || state.IsReplaced()) {
            DeleteReplacedPage(handle);
        } else if (state.IsPageKind() && (state.IsPending() || state.IsCompleting()))
        {
            DeletePendingFetch(handle);
        } else if (state.IsReady() || state.IsTombstone()) {
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
    if (TPageFetch* fetch = std::exchange(handle.Body.Fetch, nullptr)) {
        fetch->UnRef();
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DeletePayload(THandle& handle, EItemKind kind, EHandleState state) noexcept {
    Y_DEBUG_ABORT_UNLESS(IsReady(state) || state == EHandleState::Tombstone);
    Y_DEBUG_ABORT_UNLESS(kind == EItemKind::Page || handle.Body.Collection);
    const bool hasPayload = kind != EItemKind::Page || bool(handle.Body.PageBuffer);
    Y_DEBUG_ABORT_UNLESS(state == EHandleState::Tombstone || hasPayload);
    const ui64 bytes = hasPayload ? PayloadBytes(handle, kind) : 0;
    if (kind == EItemKind::Page) {
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
        } else if (state == EHandleState::Keep) {
            KeepBytes_.fetch_sub(estimatedBytes, std::memory_order_relaxed);
            SubtractEstimatedPages(KeepPages_, kind);
            SubKeepOwnedBytes(kind, bytes);
        } else {
            ColdBytes_.fetch_sub(estimatedBytes, std::memory_order_relaxed);
            SubtractEstimatedPages(ColdPages_, kind);
        }
        SubtractExactBytes(ResidentBytes_, bytes);
        SubtractExactBytes(OverallUsage_, bytes);
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
    std::atomic<ui64>& source, std::atomic<ui64>& destination, EItemKind kind) noexcept {
    if (kind != EItemKind::Page) {
        return;
    }
    destination.fetch_add(1, std::memory_order_relaxed);
    const ui64 previous = source.fetch_sub(1, std::memory_order_relaxed);
    Y_ABORT_UNLESS(previous != 0);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::AddEstimatedPages(std::atomic<ui64>& counter, EItemKind kind) noexcept {
    if (kind == EItemKind::Page) {
        counter.fetch_add(1, std::memory_order_relaxed);
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::SubtractEstimatedPages(std::atomic<ui64>& counter, EItemKind kind) noexcept {
    if (kind == EItemKind::Page) {
        const ui64 previous = counter.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
    }
}

SHARED_CACHE_TEMPLATE
TCollection* TSharedCache::PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept {
    const TCacheItem key0 = TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed));
    if (key0.Index() >= spaceOp.HandleCount()) {
        return nullptr;
    }
    THandle& collection = spaceOp.Handles()[key0.Index()];
    const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
    if (!key0.Matches(state) || !state.IsCollectionKind() || !collection.Body.Collection) {
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
    if (!DropReference(owner)) {
        return;
    }
    PublishCollectionForReclaim(spaceOp, owner.CacheItem.CacheItem());
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DropPageItemRef(TSpaceOperation& spaceOp, THandle& page) noexcept {
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_relaxed));
    if (state.Kind() != EItemKind::Page) {
        return;
    }
    if (TCollection* owner = PageCollection(spaceOp, page)) {
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
    MissInMemoryPages_.fetch_add(stats.MissInMemoryPages, std::memory_order_relaxed);
    MissInMemoryBytes_.fetch_add(stats.MissInMemoryBytes, std::memory_order_relaxed);
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
void TSharedCache::RouteCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept {
    if (!spaceOp.Contains(coldItem) || coldItem.IsFrozen()) {
        return;
    }

    THandle& handle = spaceOp.Handles()[coldItem.Index()];
    if ((handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion) != coldItem.Version()) {
        return;
    }
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!state.IsCold() || !state.IsKeepNoneField() || state.Refs() != 0) {
        return;
    }

    const TCacheItem displaced = spaceOp.PutCold(coldItem);
    if (!displaced.IsNull()) {
        EraseCold(spaceOp, displaced);
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
        if (!cacheItem.Matches(expected) || !expected.IsHot() || !expected.IsKeepNoneField()) {
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
    TSpaceOperation& spaceOp, TCacheItem cacheItem, THandle& handle, THandleState coldState) noexcept {
    const ui64 bytes = PayloadBytes(handle, coldState.Kind());
    TransferEstimatedBytes(HotBytes_, ColdBytes_, bytes);
    TransferEstimatedPages(HotPages_, ColdPages_, coldState.Kind());
    if (coldState.Refs() == 0) {
        const ui32 coldVersion = handle.ColdVersion.load(std::memory_order_acquire);
        RouteCold(spaceOp, TCacheItem::Make(coldVersion & MaxItemVersion, cacheItem.Index()));
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
                const THandleState cold = state.WithState(EHandleState::Cold);
                if (handle.State.compare_exchange_strong(
                        hotRaw, cold.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
                {
                    CompleteHotEviction(spaceOp, cacheItem, handle, cold);
                    return;
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
        return spaceOp.TryAllocateHandle();
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
    const THandleState state = THandleState::FromRaw(spaceOp.Handles()[index].State.load(std::memory_order_relaxed));
    const TCacheItem cacheItem = TCacheItem::Make(state.Version(), index);
    if (!spaceOp.TryStoreSpareItem(cacheItem)) {
        DiscardCandidate(spaceOp, cacheItem, state.Kind());
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RecycleCandidate(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && !state.IsFree() && !state.IsTombstone() && state.Refs() == 0);
    const bool reserved = state.IsPending();
    const ui64 reservedBytes = reserved ? PayloadBytes(handle, state.Kind()) : 0;
    if (!state.IsBegin()) {
        Y_DEBUG_ABORT_UNLESS(state.IsReady());
        Y_DEBUG_ABORT_UNLESS(!state.IsPageKind() || !state.IsKeep());
        Y_DEBUG_ABORT_UNLESS(
            !state.IsCollectionKind() || handle.Body.Collection->KeepPageListHead.load(std::memory_order_acquire) == 0);
        Y_DEBUG_ABORT_UNLESS(
            !state.IsCollectionKind() || handle.Body.Collection->Registry_.load(std::memory_order_acquire) == nullptr);
        DeletePayload(handle, state.Kind(), state.State());
        DropPageItemRef(spaceOp, handle);
        state = state.WithState(EHandleState::Begin).WithFrequency(0).WithKeep(EKeepState::None);
        handle.State.store(state.Raw(), std::memory_order_relaxed);
    }
    if (spaceOp.TryStoreSpareItem(cacheItem)) {
        if (reserved) {
            if (state.IsPageKind()) {
                DeletePendingFetch(handle);
            }
            ReleaseReservation(reservedBytes, state.IsPageKind() ? 1 : 0);
        }
    } else if (reserved) {
        DiscardCandidate(spaceOp, cacheItem, state.Kind(), reservedBytes);
    } else {
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
    NTable::NPage::EPage type, ui32 crc32, EKeepState keep) noexcept {
    Y_DEBUG_ABORT_UNLESS(type != NTable::NPage::EPage::Undef);
    Y_DEBUG_ABORT_UNLESS(keep == EKeepState::None || keep == EKeepState::Keep);
    const ui64 bytes = AccountedPageBytes(size);
    auto spaceOp = BeginOperation();
    const TCacheItem cacheItem = TryAllocateReservedItem(spaceOp, bytes, 1);
    if (!cacheItem.IsNull()) {
        InitializePage(spaceOp, cacheItem, collection, offset, size, type, crc32, keep);
        RefillSpareItem(spaceOp);
    }
    return TPageCacheItem::FromValidated(cacheItem);
}

SHARED_CACHE_TEMPLATE
TIntrusivePtr<TPageFetch> TSharedCache::TryAcquirePageFetch(
    TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef& owner) noexcept {
    if (TryAcquire(spaceOp, page.CacheItem(), true, owner.Get()) != EAcquireStatus::Pending) {
        return {};
    }
    TIntrusivePtr<TPageFetch> fetch = spaceOp.Handles()[page.Index()].Body.Fetch;
    Y_DEBUG_ABORT_UNLESS(fetch);
    return fetch;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(TPageCacheItem page, NActors::TSharedData&& data) noexcept {
    auto spaceOp = BeginOperation();
    TOperationItemRef owner(spaceOp);
    TIntrusivePtr<TPageFetch> fetch = TryAcquirePageFetch(spaceOp, page, owner);
    if (!fetch) {
        return false;
    }
    return CompleteFetch(owner, *fetch, std::move(data));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FailReady(TPageCacheItem page) noexcept {
    auto spaceOp = BeginOperation();
    TOperationItemRef owner(spaceOp);
    TIntrusivePtr<TPageFetch> fetch = TryAcquirePageFetch(spaceOp, page, owner);
    if (!fetch) {
        return false;
    }
    return FailFetch(owner, *fetch);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(
    TCollectionRegistry& registry, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
    return MakeReady(&registry, collection, std::move(value));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
    return MakeReady(nullptr, collection, std::move(value));
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MakeReady(
    TCollectionRegistry* registry, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
    auto spaceOp = BeginOperation();
    return MakeReady(spaceOp, registry, collection.CacheItem(), std::move(value));
}

SHARED_CACHE_TEMPLATE
void TSharedCache::PublishCollectionForReclaim(TSpaceOperation& spaceOp, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind() || !expected.IsKeep() ||
            !expected.IsKeepNoneField() || handle.Body.Collection->Registry_.load(std::memory_order_acquire) ||
            handle.Body.Collection->References.load(std::memory_order_acquire) != 0)
        {
            return;
        }
        const THandleState desired = expected.WithState(EHandleState::Hot).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            TransferEstimatedBytes(KeepBytes_, HotBytes_, PayloadBytes(handle, EItemKind::Collection));
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
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() && expected.IsKeep());
        if (expected.IsKeepField()) {
            return;
        }
        const THandleState desired = expected.WithKeep(EKeepState::Keep).WithFrequency(0);
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
    if (!SetCollectionKeepPages(spaceOp, collection.CacheItem(), false)) {
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
    if (!SetCollectionKeepPages(spaceOp, collection.CacheItem(), false)) {
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
bool TSharedCache::SetCollectionKeepPages(TCollectionCacheItem collection, bool keep) noexcept {
    auto spaceOp = BeginOperation();
    return SetCollectionKeepPages(spaceOp, collection.CacheItem(), keep);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::MatchesPendingFetch(
    const THandle& handle, THandleState state, TCacheItem cacheItem, const TPageFetch& fetch) noexcept {
    return cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() && handle.Body.Fetch == &fetch &&
           fetch.Page().CacheItem() == cacheItem;
}

SHARED_CACHE_TEMPLATE
THandle* TSharedCache::TryGetPendingFetchHandle(TOperationItemRef& owner, TPageFetch& fetch) noexcept {
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
bool TSharedCache::CompleteFetch(TOperationItemRef& owner, TPageFetch& fetch, NActors::TSharedData&& data) noexcept {
    Y_DEBUG_ABORT_UNLESS(data);
    THandle* handle = TryGetPendingFetchHandle(owner, fetch);
    if (!handle) {
        return false;
    }
    Y_DEBUG_ABORT_UNLESS(handle->Key2OrSize.load(std::memory_order_relaxed) == data.size());
    fetch.SetReady(std::move(data));
    // Drop the I/O-owner ref; the last pending releaser performs MakeReadyState.
    ReleaseFetchRef(owner, fetch);
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::DispatchFetch(TSpaceOperation& spaceOp, TCacheItem cacheItem, TPageFetch& fetch) noexcept {
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
bool TSharedCache::FailFetch(TOperationItemRef& owner, TPageFetch& fetch) noexcept {
    if (!TryGetPendingFetchHandle(owner, fetch)) {
        return false;
    }
    fetch.SetFailed();
    ReleaseFetchRef(owner, fetch);
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::FinishFailedFetch(
    TOperationItemRef& finalizerRef, TPageFetch& fetch, TPageFetchWaiter* waiters) noexcept {
    TSpaceOperation& spaceOp = finalizerRef.SpaceOperation();
    const TCacheItem cacheItem = finalizerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState completing = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(completing) && completing.IsCompleting() && completing.Refs() == 1 &&
                         handle.Body.Fetch == &fetch);
    DeletePendingFetch(handle);
    new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer();
    ReleaseReservation(PageBytes(handle), 1);
    handle.State.store(completing.WithState(EHandleState::Tombstone).Raw(), std::memory_order_release);
    InvokeHook(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, cacheItem);
    const TSharedCacheKey key = LoadSharedCacheKey(handle, EItemKind::Page);
    Table_.CompleteTombstone(cacheItem, key, spaceOp);
    TPageFetch::DrainWaiters(waiters, [cacheItem](TIntrusivePtr<TPageFetchWaiter> waiter) {
        waiter->Complete(TPageCacheItem::FromValidated(cacheItem), EPageFetchCompletion::Failed);
    });
}

SHARED_CACHE_TEMPLATE
void TSharedCache::FinishFetch(TOperationItemRef& finalizerRef, TPageFetch& fetch) noexcept {
    TSpaceOperation& spaceOp = finalizerRef.SpaceOperation();
    const TCacheItem page = finalizerRef.CacheItem();
    THandle& handle = spaceOp.Handles()[page.Index()];
    const THandleState completing = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
    Y_DEBUG_ABORT_UNLESS(
        page.Matches(completing) && completing.IsCompleting() && completing.Refs() == 1 && handle.Body.Fetch == &fetch);
    TPageFetchWaiter* waiters = fetch.DetachWaiters();
    Y_DEBUG_ABORT_UNLESS(waiters != TPageFetch::Sealed);
    if (fetch.Completion() == EPageFetchCompletion::Failed) {
        FinishFailedFetch(finalizerRef, fetch, waiters);
        return;
    }

    Y_DEBUG_ABORT_UNLESS(fetch.Completion() == EPageFetchCompletion::Ready);
    {
        TOperationItemRef keepCollectionRef(spaceOp);
        if (completing.IsKeepField() && !LinkKeepPage(spaceOp, page, keepCollectionRef.Get())) {
            FinishFailedFetch(finalizerRef, fetch, waiters);
            return;
        }

        NActors::TSharedData data = fetch.TakeData();
        DeletePendingFetch(handle);
        new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer(std::move(data).Extract());
        MakeReadyState(spaceOp, page);
    }
    TPageFetch::DrainWaiters(waiters, [page](TIntrusivePtr<TPageFetchWaiter> waiter) {
        waiter->Complete(TPageCacheItem::FromValidated(page), EPageFetchCompletion::Ready);
    });
}

SHARED_CACHE_TEMPLATE
void TSharedCache::ReleaseFetchRef(TOperationItemRef& owner, TPageFetch& fetch) noexcept {
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
            FinishFetch(owner, fetch);
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
    Y_DEBUG_ABORT_UNLESS(value);
    Y_DEBUG_ABORT_UNLESS(cacheItem.Index() >= 2 && cacheItem.Index() < spaceOp.AllocationLimit());
    THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && state.IsCollectionKind() && state.IsBegin());
    Y_DEBUG_ABORT_UNLESS(LoadSharedCacheKey(handle, EItemKind::Collection) == TSharedCacheKey::Collection(value->Id()));
    Y_DEBUG_ABORT_UNLESS(handle.Metadata.PayloadBytes == value->AccountedBytes());

    const ui64 bytes = handle.Metadata.PayloadBytes;
    CollectionBytes_.fetch_add(bytes, std::memory_order_relaxed);
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
void TSharedCache::MakeReadyState(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
    const ui32 index = cacheItem.Index();
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AllocationLimit());

    THandle& handle = spaceOp.Handles()[index];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    const THandleState initial = THandleState::FromRaw(expectedRaw);
    Y_DEBUG_ABORT_UNLESS(
        cacheItem.Matches(initial) && (initial.IsPending() || (initial.IsPageKind() && initial.IsCompleting())));
    Y_DEBUG_ABORT_UNLESS(!initial.IsCollectionKind() || initial.IsBegin());

    const ui64 bytes = PayloadBytes(handle, initial.Kind());
    const i64 estimatedBytes = static_cast<i64>(bytes);
    EKeepState accountedKeep = initial.Keep();
    Y_DEBUG_ABORT_UNLESS(accountedKeep == EKeepState::None || accountedKeep == EKeepState::Keep);
    ResidentBytes_.fetch_add(bytes, std::memory_order_relaxed);
    if (initial.Kind() == EItemKind::Collection) {
        Collections_.fetch_add(1, std::memory_order_relaxed);
    }
    if (accountedKeep == EKeepState::Keep) {
        KeepBytes_.fetch_add(estimatedBytes, std::memory_order_relaxed);
        AddEstimatedPages(KeepPages_, initial.Kind());
    } else {
        HotBytes_.fetch_add(estimatedBytes, std::memory_order_relaxed);
        AddEstimatedPages(HotPages_, initial.Kind());
    }
    SubtractExactBytes(ReservedBytes_, bytes);
    SubtractExactBytes(ReservedPages_, initial.Kind() == EItemKind::Page ? 1 : 0);

    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) &&
                             (expected.IsPending() || (expected.IsPageKind() && expected.IsCompleting())));
        Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() || expected.IsBegin());
        Y_DEBUG_ABORT_UNLESS(expected.IsKeepNoneField() || expected.IsKeepField());
        if (expected.Keep() != accountedKeep) {
            if (expected.IsKeepField()) {
                TransferEstimatedBytes(HotBytes_, KeepBytes_, bytes);
                TransferEstimatedPages(HotPages_, KeepPages_, expected.Kind());
                AddKeepOwnedBytes(expected.Kind(), bytes);
            } else {
                TransferEstimatedBytes(KeepBytes_, HotBytes_, bytes);
                TransferEstimatedPages(KeepPages_, HotPages_, expected.Kind());
                SubKeepOwnedBytes(expected.Kind(), bytes);
            }
            accountedKeep = expected.Keep();
        }

        const EHandleState readyState = expected.IsKeepField() ? EHandleState::Keep : EHandleState::Hot;
        const THandleState desired = expected.WithState(readyState).WithFrequency(0);
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
        if (!cacheItem.Matches(expected) || !expected.IsHot() || !expected.IsKeepNoneField()) {
            return false;
        }
        const THandleState desired = expected.WithState(EHandleState::Cold).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_relaxed, std::memory_order_relaxed))
        {
            CompleteHotEviction(spaceOp, cacheItem, handle, desired);
            return true;
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
    ui64 offset, ui64 size, NTable::NPage::EPage type, ui32 crc32, EKeepState keep) noexcept {
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
    if (TCollection* owner = PageCollection(spaceOp, handle)) {
        const THandle& collection = spaceOp.Handles()[owner->CacheItem.CacheItem().Index()];
        const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
        Y_ABORT_UNLESS(state.IsKeep() && state.IsKeepField());
        const ui64 previous = owner->References.fetch_add(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous != 0);
    }
    handle.Body.Fetch = new TPageFetch(TPageCacheItem::FromValidated(cacheItem));
    handle.Body.Fetch->Ref();
    handle.Next.store(TCacheItem::Make(claimed.Version(), 0).Raw(), std::memory_order_relaxed);
    handle.NextInOwner.store(0, std::memory_order_relaxed);
    // A page enters Keep only while it fits the keep budget; otherwise it is admitted with KeepNone and follows the
    // ordinary policy. The check and the exact keep counter are one step, so concurrent admissions cannot overshoot.
    if (keep == EKeepState::Keep && !KeepOwnedFits(AccountedPageBytes(size))) {
        keep = EKeepState::None;
    }
    if (keep == EKeepState::Keep) {
        AddKeepOwnedBytes(EItemKind::Page, AccountedPageBytes(size));
    }
    const THandleState initialized = claimed.WithKind(EItemKind::Page).WithFrequency(0).WithKeep(keep);
    handle.State.store(initialized.Raw(), std::memory_order_relaxed);
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
    new (&handle.Body.Collection) TCollection*(nullptr);
    handle.Metadata.PayloadBytes = bytes;
    handle.Next.store(TCacheItem::Make(claimed.Version(), 0).Raw(), std::memory_order_relaxed);
    handle.NextInOwner.store(0, std::memory_order_relaxed);
    const THandleState initialized =
        claimed.WithKind(EItemKind::Collection).WithFrequency(0).WithKeep(EKeepState::Keep);
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
    const ui64 reservedBytes = state.IsPending() ? PayloadBytes(handle, kind) : 0;
    DiscardCandidate(spaceOp, cacheItem, kind, reservedBytes);
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DiscardCandidate(
    TSpaceOperation& spaceOp, TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes) noexcept {
    const ui32 index = cacheItem.Index();
    Y_DEBUG_ABORT_UNLESS(index >= 2 && index < spaceOp.AllocationLimit());

    THandle& handle = spaceOp.Handles()[index];
    const THandleState expected = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(expected) && expected.Kind() == kind && !expected.IsFree() &&
                         !expected.IsTombstone() && expected.Refs() == 0);

    Y_DEBUG_ABORT_UNLESS(!expected.IsPageKind() || !expected.IsKeep());
    Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() || !expected.IsReady() ||
                         handle.Body.Collection->KeepPageListHead.load(std::memory_order_acquire) == 0);
    Y_DEBUG_ABORT_UNLESS(!expected.IsCollectionKind() || !expected.IsReady() ||
                         handle.Body.Collection->Registry_.load(std::memory_order_acquire) == nullptr);

    const THandleState desired = expected.WithState(EHandleState::Free).WithFrequency(0).WithKeep(EKeepState::None);
    TCollection* owner = nullptr;
    if (expected.IsPageKind() && handle.Body.Collection) {
        owner = PageCollection(spaceOp, handle);
    }
    handle.State.store(desired.Raw(), std::memory_order_release);
    if (owner) {
        DropOwnerReference(spaceOp, *owner);
    }
    if (expected.IsReady()) {
        DeletePayload(handle, expected.Kind(), expected.State());
    } else if (expected.IsPageKind() && reservedBytes != 0) {
        DeletePendingFetch(handle);
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

        if (expected.IsCold()) {
            if (expected.IsPageKind()) {
                desired = desired.WithState(EHandleState::Hot).WithFrequency(0);
            }
        }
        if (!handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            continue;
        }
        if (expected.IsCold() && expected.IsPageKind()) {
            AdvanceColdMembership(handle);
            const ui64 bytes = PageBytes(handle);
            TransferEstimatedBytes(ColdBytes_, HotBytes_, bytes);
            TransferEstimatedPages(ColdPages_, HotPages_, EItemKind::Page);
            RouteHot(spaceOp, EHotLevel::L1, cacheItem);
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
            kind == EItemKind::Page ? expected.IsReady() : expected.IsKeep() || expected.IsHot() || expected.IsCold();
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
TSharedCacheItemRef TSharedCache::AcquireKeepCollection(
    TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem, TCollection*& collectionValue) noexcept {
    collectionValue = nullptr;
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collectionItem.CacheItem()));
    if (!collectionRef) {
        return {};
    }

    THandle& collection = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(collection.State.load(std::memory_order_acquire));
    if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !state.IsKeep() || !state.IsKeepField() ||
        !collection.Body.Collection)
    {
        return {};
    }
    collectionValue = collection.Body.Collection;
    return collectionRef.Take();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::SetCollectionKeepPages(TSpaceOperation& spaceOp, TCacheItem collectionItem, bool keep) noexcept {
    TOperationItemRef collectionRef(spaceOp, TryAcquireStructural(spaceOp, collectionItem));
    if (!collectionRef) {
        return false;
    }

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !handle.Body.Collection)
    {
        return false;
    }
    if (!state.IsKeep()) {
        return !keep && (state.IsHot() || state.IsCold()) && state.IsKeepNoneField() &&
               handle.Body.Collection->Registry_.load(std::memory_order_acquire) == nullptr &&
               handle.Body.Collection->KeepPageListHead.load(std::memory_order_acquire) == 0;
    }
    if (!state.IsKeepField()) {
        return false;
    }

    TCollection& value = *handle.Body.Collection;
    value.KeepPages_.store(keep, std::memory_order_release);
    if (!keep) {
        DrainKeepPages(spaceOp, TCollectionCacheItem::FromValidated(collectionItem), value);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::RestoreKeepPageList(TSpaceOperation& spaceOp, TCollection& collection, ui32 detachedHead) noexcept {
    ui32 tail = detachedHead;
    while (const ui32 next = spaceOp.Handles()[tail].NextInOwner.load(std::memory_order_acquire)) {
        tail = next;
    }
    ui32 concurrentHead = collection.KeepPageListHead.load(std::memory_order_relaxed);
    do {
        spaceOp.Handles()[tail].NextInOwner.store(concurrentHead, std::memory_order_release);
    } while (!collection.KeepPageListHead.compare_exchange_weak(
        concurrentHead, detachedHead, std::memory_order_release, std::memory_order_relaxed));
}

SHARED_CACHE_TEMPLATE
ui32 TSharedCache::UnkeepPage(TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem, ui32 pageIndex) noexcept {
    Y_DEBUG_ABORT_UNLESS(pageIndex < spaceOp.HandleCount());
    THandle& page = spaceOp.Handles()[pageIndex];
    const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
    page.NextInOwner.store(0, std::memory_order_release);

    ui64 expectedRaw = page.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(expected.IsPageKind() && expected.IsKeepField() &&
                             page.Key0.load(std::memory_order_relaxed) == collectionItem.Raw());
        if (expected.IsPending() || expected.IsCompleting()) {
            const THandleState desired = expected.WithKeep(EKeepState::None);
            if (page.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
            {
                return next;
            }
            continue;
        }
        Y_DEBUG_ABORT_UNLESS(expected.IsKeep());
        const THandleState desired = expected.WithState(EHandleState::Hot).WithKeep(EKeepState::None).WithFrequency(0);
        if (page.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            TransferEstimatedBytes(KeepBytes_, HotBytes_, PageBytes(page));
            TransferEstimatedPages(KeepPages_, HotPages_, EItemKind::Page);
            SubKeepOwnedBytes(EItemKind::Page, PageBytes(page));
            RouteHot(spaceOp, EHotLevel::L1, TCacheItem::Make(expected.Version(), pageIndex));
            return next;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::DrainKeepPages(
    TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem, TCollection& value) noexcept {
    for (;;) {
        if (value.KeepPages_.load(std::memory_order_acquire)) {
            return;
        }
        ui32 current = value.KeepPageListHead.exchange(0, std::memory_order_acq_rel);
        if (current == 0) {
            return;
        }

        InvokeHook(ESharedCacheHookPoint::AfterKeepPageListDetached, collectionItem.CacheItem());
        if (value.KeepPages_.load(std::memory_order_acquire)) {
            RestoreKeepPageList(spaceOp, value, current);
            return;
        }
        while (current != 0) {
            current = UnkeepPage(spaceOp, collectionItem, current);
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::LinkKeepPage(
    TSpaceOperation& spaceOp, TCacheItem pageItem, TSharedCacheItemRef& collectionRef) noexcept {
    THandle& page = spaceOp.Handles()[pageItem.Index()];
    const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(pageItem.Matches(state) && state.IsPageKind() && state.IsKeepField() &&
                         (state.IsPending() || state.IsCompleting() || state.IsKeep()));
    Y_DEBUG_ABORT_UNLESS(page.NextInOwner.load(std::memory_order_relaxed) == 0);

    const TCollectionCacheItem collectionItem =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(page.Key0.load(std::memory_order_relaxed)));
    TCollection* collectionValue = nullptr;
    collectionRef = AcquireKeepCollection(spaceOp, collectionItem, collectionValue);
    if (!collectionRef) {
        return false;
    }

    if (!collectionValue->KeepPages_.load(std::memory_order_acquire)) {
        ui64 expectedRaw = page.State.load(std::memory_order_relaxed);
        for (;;) {
            const THandleState expected = THandleState::FromRaw(expectedRaw);
            Y_DEBUG_ABORT_UNLESS(pageItem.Matches(expected) && expected.IsPageKind() &&
                                 (expected.IsPending() || expected.IsCompleting()) && expected.IsKeepField());
            const THandleState desired = expected.WithKeep(EKeepState::None);
            if (page.State.compare_exchange_weak(
                    expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
            {
                return true;
            }
        }
    }

    ui32 head = collectionValue->KeepPageListHead.load(std::memory_order_relaxed);
    for (;;) {
        page.NextInOwner.store(head, std::memory_order_release);
        if (collectionValue->KeepPageListHead.compare_exchange_weak(
                head, pageItem.Index(), std::memory_order_release, std::memory_order_relaxed))
        {
            break;
        }
    }
    InvokeHook(ESharedCacheHookPoint::AfterKeepPageLinked, pageItem);
    if (!collectionValue->KeepPages_.load(std::memory_order_acquire)) {
        DrainKeepPages(spaceOp, collectionItem, *collectionValue);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::PublishPageUnkeep(THandle& page, THandleState state) noexcept {
    ui64 expectedRaw = state.Raw();
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_DEBUG_ABORT_UNLESS(expected.Version() == state.Version() && expected.IsPageKind() && expected.IsKeep() &&
                             expected.IsKeepField());
        const THandleState desired = expected.WithKeep(EKeepState::Unkeep);
        if (page.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

SHARED_CACHE_TEMPLATE
void TSharedCache::MergeRetainedKeepPages(TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem,
    TCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept {
    Y_DEBUG_ABORT_UNLESS(retainedHead != 0 && retainedTail != 0);
    const ui32 concurrentHead = collection.KeepPageListHead.exchange(retainedHead, std::memory_order_acq_rel);
    InvokeHook(ESharedCacheHookPoint::AfterKeepPageListHeadExchanged, collectionItem.CacheItem());
    // Only the cache actor traverses; publishers only prepend, so connecting the tail after exchange is safe.
    spaceOp.Handles()[retainedTail].NextInOwner.store(concurrentHead, std::memory_order_release);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UnkeepCutPages(TSpaceOperation& spaceOp, TCacheItem pageItem, ui64 allocationLimit) noexcept {
    THandle& trigger = spaceOp.Handles()[pageItem.Index()];
    const TCollectionCacheItem collectionItem =
        TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(trigger.Key0.load(std::memory_order_relaxed)));
    TCollection* collectionValue = nullptr;
    TOperationItemRef collectionRef(spaceOp, AcquireKeepCollection(spaceOp, collectionItem, collectionValue));
    if (!collectionRef) {
        return false;
    }

    const ui32 detachedHead = collectionValue->KeepPageListHead.exchange(0, std::memory_order_acq_rel);
    InvokeHook(ESharedCacheHookPoint::AfterKeepPageListDetached, collectionItem.CacheItem());

    ui32 current = detachedHead;
    ui32 retainedHead = 0;
    ui32 retainedTail = 0;
    while (current != 0) {
        Y_DEBUG_ABORT_UNLESS(current < spaceOp.HandleCount());
        THandle& page = spaceOp.Handles()[current];
        const ui32 next = page.NextInOwner.load(std::memory_order_acquire);
        const THandleState state = THandleState::FromRaw(page.State.load(std::memory_order_acquire));
        Y_DEBUG_ABORT_UNLESS(state.IsPageKind() && (state.IsPending() || state.IsCompleting() || state.IsKeep()) &&
                             state.IsKeepField() && page.Key0.load(std::memory_order_relaxed) == collectionItem.Raw());

        if (current < allocationLimit || state.IsPending() || state.IsCompleting()) {
            if (retainedTail == 0) {
                retainedTail = current;
            }
            page.NextInOwner.store(retainedHead, std::memory_order_relaxed);
            retainedHead = current;
        } else {
            page.NextInOwner.store(0, std::memory_order_release);
            PublishPageUnkeep(page, state);
        }
        current = next;
    }
    if (retainedTail != 0) {
        MergeRetainedKeepPages(spaceOp, collectionItem, *collectionValue, retainedHead, retainedTail);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::LinkCollection(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collectionItem) noexcept {
    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
    Y_DEBUG_ABORT_UNLESS(collectionItem.Matches(state) && state.IsCollectionKind());
    Y_DEBUG_ABORT_UNLESS((state.IsBegin() && state.IsKeepField()) ||
                         ((state.IsHot() || state.IsCold()) && state.IsKeepReservedField()) ||
                         (state.IsKeep() && state.IsKeepField()));
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
        if (expected.IsKeep() && expected.IsKeepField()) {
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
        if (expected.IsKeep() && expected.IsKeepNoneField()) {
            TCollectionRegistry* owner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
            if (!owner && LinkCollection(spaceOp, registry, collectionItem)) {
                const THandleState claimed = expected.WithKeep(EKeepState::Keep);
                if (handle.State.compare_exchange_weak(
                        expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
                {
                    attachState = claimed;
                    return true;
                }
            }
            return false;
        }
        if ((!expected.IsHot() && !expected.IsCold()) || !expected.IsKeepNoneField()) {
            return false;
        }

        const THandleState claimed = expected.WithKeep(EKeepState::Reserved);
        if (handle.State.compare_exchange_weak(
                expectedRaw, claimed.Raw(), std::memory_order_acq_rel, std::memory_order_relaxed))
        {
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
        TransferEstimatedBytes(ColdBytes_, KeepBytes_, bytes);
    } else {
        TransferEstimatedBytes(HotBytes_, KeepBytes_, bytes);
    }
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() &&
                       expected.State() == detachedState && expected.IsKeepReservedField());
        const THandleState desired = expected.WithState(EHandleState::Keep).WithKeep(EKeepState::Keep).WithFrequency(0);
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
    if (attachState.IsKeep()) {
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
    if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !state.IsKeep() || !state.IsKeepField() ||
        !handle.Body.Collection || handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &registry)
    {
        return false;
    }

    return UnlinkCollectionRegistry(spaceOp, registry, collectionItem, EHandleState::Keep);
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
        if (!collectionItem.Matches(expected) || !expected.IsCollectionKind() || !expected.IsKeep() ||
            !expected.IsKeepField() || !handle.Body.Collection ||
            handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &registry ||
            handle.Body.Collection->KeepPageListHead.load(std::memory_order_acquire) != 0)
        {
            return false;
        }
        const THandleState claimed = expected.WithKeep(EKeepState::Reserved);
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
    Y_ABORT_UNLESS(UnlinkCollectionRegistry(spaceOp, registry, collectionItem, EHandleState::Keep));

    THandle& handle = spaceOp.Handles()[collectionItem.Index()];
    ui64 expectedRaw = handle.State.load(std::memory_order_relaxed);
    for (;;) {
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        Y_ABORT_UNLESS(collectionItem.Matches(expected) && expected.IsCollectionKind() && expected.IsKeep() &&
                       expected.IsKeepReservedField());
        const THandleState desired = expected.WithKeep(EKeepState::None).WithFrequency(0);
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
            handle.Body.Collection->KeepPageListHead.load(std::memory_order_acquire) != 0)
        {
            return false;
        }
        TCollectionRegistry* registry = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
        /* the record's own reference is carried by the keep field: attached means one
         * reference for the record itself, detached means none, and every other
         * reference belongs to a live page item */
        const ui64 ownReference = expected.IsKeepField() ? 1 : 0;
        if (handle.Body.Collection->References.load(std::memory_order_acquire) != ownReference) {
            return false;
        }
        const bool keepSource = expected.IsKeep() && expected.IsKeepField() && registry == expectedRegistry;
        const bool detachedSource = expected.IsKeepNoneField() && !registry && !expectedRegistry;
        if (!keepSource && !detachedSource) {
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(expected.Refs() < MaxHandleRefs);
        const THandleState desired = expected.IncrementRefs().WithState(EHandleState::Tombstone).WithFrequency(0);
        if (handle.State.compare_exchange_weak(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
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
    if (sourceState.IsKeep()) {
        TransferEstimatedBytes(KeepBytes_, ColdBytes_, bytes);
    } else if (sourceState.IsHot()) {
        TransferEstimatedBytes(HotBytes_, ColdBytes_, bytes);
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
    if (!collectionItem.Matches(state) || !state.IsCollectionKind() || !state.IsKeep() || !state.IsKeepField() ||
        !handle.Body.Collection || handle.Body.Collection->Registry_.load(std::memory_order_acquire) != &source)
    {
        return false;
    }
    if (&source == &destination) {
        return true;
    }

    if (!UnlinkCollectionRegistry(spaceOp, source, collectionItem, EHandleState::Keep)) {
        return false;
    }
    Y_ABORT_UNLESS(LinkCollection(spaceOp, destination, collectionItem));
    return true;
}

SHARED_CACHE_TEMPLATE
TSharedCachePageRefImpl<TTraits> TSharedCache::BuildPageRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRef&& ref) const noexcept {
    Y_DEBUG_ABORT_UNLESS(&SharedCachePages() == this && ref);
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    Y_DEBUG_ABORT_UNLESS([&] {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        return cacheItem.Matches(state) && state.IsPageKind() &&
               (state.IsReady() || state.IsReplacing() || state.IsReplaced() || state.IsTombstone()) &&
               state.Refs() > 0 && handle.Body.PageBuffer.data();
    }());
    NActors::TSharedData data = handle.Body.PageBuffer.Share(handle.Key2OrSize.load(std::memory_order_relaxed));
    return TSharedCachePageRefImpl<TTraits>(std::move(ref), std::move(data), handle.Metadata.Page.Type);
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
TSharedCacheCollectionRef TSharedCache::BuildCollectionRef(
    const TSpaceOperation& spaceOp, TSharedCacheItemRef&& ref) const noexcept {
    Y_DEBUG_ABORT_UNLESS(&SharedCachePages() == this);
    const TCacheItem cacheItem = ref.CacheItem_;
    Y_DEBUG_ABORT_UNLESS(spaceOp.Contains(cacheItem));
    const THandle& handle = spaceOp.Handles()[cacheItem.Index()];
    Y_DEBUG_ABORT_UNLESS([&] {
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        return cacheItem.Matches(state) && state.IsCollectionKind() &&
               (state.IsKeep() || state.IsHot() || state.IsCold() || state.IsTombstone()) && state.Refs() > 0 &&
               handle.Body.Collection;
    }());
    return TSharedCacheCollectionRef(std::move(ref), handle.Body.Collection);
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
    const TCacheItem cacheItem = TryAllocateReservedItem(spaceOp, reservedBytes, 1);
    if (cacheItem.IsNull()) {
        return false;
    }
    InitializePage(spaceOp, cacheItem, TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(candidate.Key.Word(0))),
        candidate.Key.Word(1), candidate.Size, candidate.Type, candidate.Crc32, candidate.Keep);
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
    TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page, EKeepState keep) noexcept {
    return {
        .Key = TSharedCacheKey::Page(collection, static_cast<ui64>(page.Offset)),
        .Size = page.Size,
        .Type = page.Type,
        .Crc32 = page.Crc32,
        .Keep = keep,
    };
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsert(TCollectionCacheItem collection,
    const NTable::NPage::TPageLocation& page, EKeepState keep, TPageCacheItem& inserted,
    TSharedCachePageRefImpl<TTraits>& hit) noexcept {
    Y_DEBUG_ABORT_UNLESS(page && page.Type != NTable::NPage::EPage::Undef);
    Y_DEBUG_ABORT_UNLESS(keep == EKeepState::None || keep == EKeepState::Keep);
    inserted = {};
    hit.Drop();
    auto spaceOp = BeginOperation();
    TPageInsertCandidate candidate = BuildPageInsertCandidate(collection, page, keep);
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
    const NTable::NPage::TPageLocation& page, EKeepState keep, TIntrusivePtr<TPageFetchWaiter> waiter,
    TSharedCachePageRefImpl<TTraits>& hit, TPageFetchTokenImpl<TTraits>& fetch) noexcept {
    Y_DEBUG_ABORT_UNLESS(page && page.Type != NTable::NPage::EPage::Undef && waiter);
    Y_DEBUG_ABORT_UNLESS(keep == EKeepState::None || keep == EKeepState::Keep);
    auto spaceOp = BeginOperation();
    TPageInsertCandidate candidate = BuildPageInsertCandidate(collection, page, keep);
    const ESharedCacheResultStatus status = FindOrInsertPage(spaceOp, candidate, std::move(waiter), hit, fetch);
    TStats stats;
    stats.AddAdmission(status, page.Size, keep);
    AddStats(stats);
    return status;
}

SHARED_CACHE_TEMPLATE
ESharedCacheResultStatus TSharedCache::FindOrInsertPage(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate,
    TIntrusivePtr<TPageFetchWaiter> waiter, TSharedCachePageRefImpl<TTraits>& hit,
    TPageFetchTokenImpl<TTraits>& fetch) noexcept {
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
            TPageFetch* pageFetch = handle.Body.Fetch;
            // The acquired pending ref prevents waiter-list closure until this operation releases it.
            Y_ABORT_UNLESS(pageFetch->Subscribe(waiter));
            InvokeHook(ESharedCacheHookPoint::AfterFetchWaiterSubscribed, cacheItem);
            if (status == ESharedCacheResultStatus::Inserted) {
                fetch = TPageFetchTokenImpl<TTraits>(this, ref.Take(), pageFetch);
            }
            return status;
        }
        case ESharedCacheResultStatus::Hit:
            hit = BuildPageRef(spaceOp, ref.Take());
            return status;
    }
    Y_UNREACHABLE();
}

