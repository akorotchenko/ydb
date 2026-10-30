#include "cache.h"
#include "shared_cache_events.h"
#include "cache_traits.h"

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

void TProdTraits::NotifyCollectionReady(
    const TLogoBlobID& collectionId, TCollectionCacheItem cacheItem) const noexcept {
    if (KeepEvictionActorSystem && KeepEvictionActor) {
        KeepEvictionActorSystem->Send(KeepEvictionActor, new TEvCollectionReady(collectionId, cacheItem), 0, 0);
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
#define TCacheItemRef TCacheItemRefImpl<TTraits>
#define TCacheCollectionRef TCacheCollectionRefImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

SHARED_CACHE_TEMPLATE
TSharedCache::TSharedCacheImpl(THolder<TCacheSpace> space, ui64 hardLimit, TTraits traits) noexcept
    : TTraits(std::move(traits))
    , Space_(std::move(space))
    , Table_(*this)
    , Memory_(Space_->CurrentConfiguration().StaticBytes, hardLimit, Space_->ReservedConfiguration().Limit)
    , Items_(*this)
    , Collections_(*this)
    , Eviction_(*this)
    , Capacity_(*this)
    , Fetches_(*this)
    , HotResize_({
          .Context = this,
          .Function =
              [](void* context) noexcept {
                  static_cast<TSharedCache*>(context)->NotifyResourcesAvailable();
              },
      })
{
    Space_->HazardDrainNotify_ = {
        .Context = this,
        .Function = [](void* context) noexcept {
            static_cast<TSharedCache*>(context)->NotifyResourcesAvailable();
        },
    };
    if constexpr (std::is_same_v<TTraits, TTestTraits>) {
        for (TSpaceHazard* hazard : Space_->Hazards_) {
            hazard->TestCache = this;
        }
    }
    RefreshSoftLimit();
}

SHARED_CACHE_TEMPLATE
TIntrusivePtr<TSharedCache> TSharedCache::Create(THolder<TCacheSpace> space, ui64 hardLimit, TTraits traits)
{
    if (!space || hardLimit == 0 || hardLimit > space->ReservedConfiguration().Limit) {
        return {};
    }
    for (const auto& tier : space->CurrentConfiguration().Tiers) {
        if (!THotRingLayout{ tier.HotSlots }.IsValid() || tier.ColdSlots == 0) {
            return {};
        }
    }
    Y_DEBUG_ABORT_UNLESS(Policy_.CurrentLimitGap >= 0.0 && Policy_.CurrentLimitGap < 1.0 && Policy_.ColdMin >= 0.0 &&
                         Policy_.ColdMin < Policy_.Grow && Policy_.Grow <= 1.0 && Policy_.KeepMaxShare > 0.0 &&
                         Policy_.KeepMaxShare < 1.0 && Policy_.ResizeStep > 0.0 && Policy_.ResizeStep <= 1.0 &&
                         Policy_.MinHotSlots > 0 && Policy_.MinResizeStep > 0 &&
                         Policy_.MinHotSlots <= space->CurrentConfiguration().HotSlotCount() &&
                         SharedCacheHotLayoutIsValid(Policy_.MinHotSlots));
    TIntrusivePtr<TSharedCache> cache(new TSharedCache(std::move(space), hardLimit, std::move(traits)));
    if (!TTraits::InstallCache(cache.Get())) {
        return {};
    }
    return cache;
}

SHARED_CACHE_TEMPLATE
TSharedCache::~TSharedCacheImpl() {
    // The actor system may already be destroyed when terminal sweeping releases payloads
    Memory_.DisarmResourceWait();
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
bool TSharedCache::CanAllocateHandles(TArrayRef<const TCacheHandleRequirement> requirements) noexcept {
    auto cacheOp = BeginOperation();
    ui64 missing = 0;
    THashMap<TLogoBlobID, THashSet<ui64>> seen;
    for (const auto& requirement : requirements) {
        TCacheItem item;
        TCollectionCacheItem collection;
        if (cacheOp.Find(TCacheKey::Collection(requirement.CollectionId), item) != ECacheResultStatus::Miss) {
            const THandleState state =
                THandleState::FromRaw(cacheOp.Handles()[item.Index()].State.load(std::memory_order_acquire));
            if (!item.Matches(state) || !state.IsCollectionKind() || !state.IsReady()) {
                return false;
            }
            collection = TCollectionCacheItem::FromValidated(item);
        }
        auto [it, first] = seen.try_emplace(requirement.CollectionId);
        missing += first && !collection;
        for (const auto& location : requirement.Pages) {
            const ui64 offset = static_cast<ui64>(location.Offset);
            if (!it->second.insert(offset).second) {
                continue;
            }
            TCacheItem present;
            missing +=
                !collection || cacheOp.Find(TCacheKey::Page(collection, offset), present) == ECacheResultStatus::Miss;
        }
    }
    return missing <= cacheOp.SpaceOperation().AvailableHandleCount();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::WorkingSetExceedsCapacity(TArrayRef<const TCollectionCacheItem> retainedCollections,
    TArrayRef<const TCacheHandleRequirement> requirements) noexcept {
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
            if (cacheOp.Find(TCacheKey::Collection(requirement.CollectionId), current) == ECacheResultStatus::Miss ||
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
bool TSharedCache::MakeReady(
    TCollectionCacheItem collection, THolder<TCollection>&& value, TCacheCollectionRef& ready) noexcept {
    auto cacheOp = BeginOperation();
    TOperationItemRef publisher(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
    Y_DEBUG_ABORT_UNLESS(publisher);
    if (!cacheOp.MakeReady(&Collections_.Registry(), collection.CacheItem(), std::move(value))) {
        return false;
    }
    ready = cacheOp.BuildCollectionRef(publisher.Take());
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryDetachReleasedCollection(const TCacheCollectionRef& collection) noexcept {
    if (!collection) {
        return false;
    }
    auto cacheOp = BeginOperation();
    return cacheOp.Collections().DetachReleased(Collections_.Registry(), collection.CacheItem().CacheItem());
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
    Y_DEBUG_ABORT_UNLESS(LoadSharedCacheKey(handle, EItemKind::Collection) == TCacheKey::Collection(value->Id()));
    Y_DEBUG_ABORT_UNLESS(handle.Metadata.PayloadBytes == value->AccountedBytes());

    const TLogoBlobID id = value->Id();
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
    TTraits::NotifyCollectionReady(id, TCollectionCacheItem::FromValidated(cacheItem));
    NotifyResourcesAvailable();
    return true;
}

SHARED_CACHE_TEMPLATE
void TSharedCache::MakeReadyState(TSpaceOperation& spaceOp, TCacheItem item, EHandleState readyState,
    TCollection* pageOwner, TKeepReservation* keepReservation) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    THandle& handle = spaceOp.Handles()[item.Index()];
    ui64 raw = handle.State.load(std::memory_order_relaxed);
    const auto initial = THandleState::FromRaw(raw);
    Y_DEBUG_ABORT_UNLESS(item.Matches(initial) && (initial.IsPending() || initial.IsCompleting()));
    const ui64 bytes = TCacheItems<TTraits>::PayloadBytes(handle, initial.Kind());
    const bool page = initial.IsPageKind();
    TKeepReservation localKeep;
    TKeepReservation& keep = keepReservation ? *keepReservation : localKeep;
    bool resident = false;
    bool retained = false;
    bool retentionGuaranteed = false;
    EHandleState accounted = EHandleState::Cold;
    for (;;) {
        const auto before = THandleState::FromRaw(raw);
        Y_DEBUG_ABORT_UNLESS(item.Matches(before) && (before.IsPending() || before.IsCompleting()));
        const bool sticky = before.IsStickyField();
        const bool wantsKeep = page && pageOwner && pageOwner->GetCacheMode() == ECacheMode::TryKeepInMemory &&
                               pageOwner->IsKeepAllowedPage(handle.Metadata.Page.Type);
        if (!sticky && wantsKeep && !keep) {
            keep = Memory_.TryReserveKeep(bytes, Eviction_.KeepLimit());
        }
        const bool useKeep = !sticky && wantsKeep && keep && !Eviction_.NeedsKeepTrim();
        const ECacheTier tier = useKeep ? ECacheTier::Keep : ECacheTier::Regular;
        bool hot = readyState != EHandleState::Cold && readyState != EHandleState::KeepCold;
        if (wantsKeep && !useKeep && !sticky) {
            hot = false;
        }
        const bool guaranteed = sticky || !page || !pageOwner || pageOwner->GetCacheMode() == ECacheMode::Sticky;
        if (retained && retentionGuaranteed && !guaranteed) {
            // Withdrawing Sticky protection requires ordinary admission before publishing Hot
            Memory_.ReleaseRetainedBytes(bytes);
            retained = false;
        }
        if ((sticky || hot) && !retained) {
            if (guaranteed) {
                Memory_.AddRetainedBytes(bytes);
                retained = true;
            } else {
                retained = Memory_.TryAddRetainedBytes(bytes);
            }
            retentionGuaranteed = guaranteed;
            if (!retained) {
                hot = false;
            }
        }
        if (!sticky && !hot && retained) {
            Memory_.ReleaseRetainedBytes(bytes);
            retained = false;
        }
        const EHandleState destination =
            sticky ? EHandleState::Sticky : (hot ? THandleState::HotState(tier) : THandleState::ColdState(tier));
        if (!resident) {
            Memory_.AddResident(bytes);
            auto& usage = Memory_.ResidentCategory(destination);
            usage.AddBytes(static_cast<i64>(bytes));
            usage.AddPage(initial.Kind());
            if (pageOwner) {
                Collections_.AddResidentPageBytes(*pageOwner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
                Collections_.AddReadyPageBytes(*pageOwner, handle.Metadata.Page.Type, static_cast<i64>(bytes));
            }
            Memory_.CompleteReservedBytes(bytes);
            accounted = destination;
            resident = true;
            InvokeHook(ECacheHookPoint::BeforeReadyStatePublication, item);
        } else if (accounted != destination) {
            auto& source = Memory_.ResidentCategory(accounted);
            auto& target = Memory_.ResidentCategory(destination);
            source.TransferBytesTo(target, bytes);
            source.TransferPageTo(target, initial.Kind());
            accounted = destination;
        }
        const auto after = before.WithState(destination).WithFrequency(0);
        if (!handle.State.compare_exchange_weak(
                raw, after.Raw(), std::memory_order_release, std::memory_order_relaxed)) {
            continue;
        }
        if (useKeep) {
            keep.Commit();
            Capacity_.RequestTierRebalance();
        } else {
            keep.Reset();
        }
        if (after.IsHot()) {
            cacheOp.Eviction().RouteHot(after.Tier(), EHotLevel::L1, item);
        }
        return;
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
                InvokeHook(ECacheHookPoint::BeforePageReplacementRetry, item);
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
bool TSharedCache::AcquirePage(TPageCacheItem page, TCachePageRefImpl<TTraits>& result) noexcept {
    result.Drop();
    auto cacheOp = BeginOperation();
    auto ref = AcquirePageItem(cacheOp.SpaceOperation(), page);
    if (!ref) {
        return false;
    }
    result = cacheOp.BuildPageRef(std::move(ref));
    return true;
}

SHARED_CACHE_TEMPLATE
TCacheItemRef TSharedCache::AcquirePageItem(TSpaceOperation& spaceOp, TPageCacheItem page) noexcept {
    TCacheItem cacheItem = page.CacheItem();
    auto cacheOp = BindOperation(spaceOp);
    for (;;) {
        if (!spaceOp.Contains(cacheItem)) {
            return {};
        }

        THandle& handle = spaceOp.Handles()[cacheItem.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        if (!cacheItem.Matches(state) || !state.IsPageKind()) {
            return {};
        }
        if (state.IsReplacing() || state.IsReplaced()) {
            const TCacheItem replacement = TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire));
            if (!replacement.IsFrozen()) {
                InvokeHook(ECacheHookPoint::BeforePageReplacementRetry, cacheItem);
                SpinLockPause();
                continue;
            }
            if (replacement.WithoutFrozen().IsNull()) {
                return {};
            }
            cacheItem = replacement.WithoutFrozen();
            continue;
        }
        if (!state.IsReady() && !state.IsTombstone()) {
            return {};
        }

        // The completion finalizer or replacement hold keeps this generation alive until the new ref is acquired.
        TOperationItemRef ref(spaceOp, state.IsTombstone() ? cacheOp.Items().TryAcquireRetiredItemRef(cacheItem)
                                                           : cacheOp.TryAcquireStructural(cacheItem));
        if (!ref) {
            continue;
        }
        // Failed fetch tombstones own an empty buffer; a Ready completion owns actual data
        if (!handle.Body.PageBuffer) {
            return {};
        }
        return ref.Take();
    }
}

SHARED_CACHE_TEMPLATE
ECacheResultStatus TSharedCache::Find(
    TCollectionCacheItem collection, ui64 offset, TCachePageRefImpl<TTraits>& page) noexcept {
    page.Drop();
    auto cacheOp = BeginOperation();
    TOperationItemRef ref(cacheOp);
    const ECacheResultStatus status = cacheOp.Find(TCacheKey::Page(collection, offset), ref.Get());
    if (status == ECacheResultStatus::Hit) {
        page = cacheOp.BuildPageRef(ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
ECacheResultStatus TSharedCache::Find(const TLogoBlobID& id, TCacheCollectionRef& collection) noexcept {
    collection.Drop();
    auto cacheOp = BeginOperation();
    TOperationItemRef ref(cacheOp);
    const ECacheResultStatus status = cacheOp.Find(TCacheKey::Collection(id), ref.Get());
    if (status == ECacheResultStatus::Hit) {
        collection = cacheOp.BuildCollectionRef(ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
ECacheCollectionLookupStatus TSharedCache::FindCollection(
    TCollectionRegistry& registry, const TLogoBlobID& id, TCacheCollectionRef& collection) noexcept {
    collection.Drop();
    auto cacheOp = BeginOperation();
    for (;;) {
        TCacheItem item;
        if (cacheOp.Find(TCacheKey::Collection(id), item) == ECacheResultStatus::Miss) {
            return ECacheCollectionLookupStatus::Miss;
        }
        InvokeHook(ECacheHookPoint::BeforeCollectionRefAcquired, item);
        TOperationItemRef ref(cacheOp);
        switch (cacheOp.TryAcquire(item, false, ref.Get())) {
            case EAcquireStatus::Stale:
                continue;
            case EAcquireStatus::Pending:
                return ECacheCollectionLookupStatus::Pending;
            case EAcquireStatus::Ready:
                if (!cacheOp.AttachCollection(registry, ref.Get())) {
                    const auto state = THandleState::FromRaw(
                        cacheOp.SpaceOperation().Handles()[item.Index()].State.load(std::memory_order_acquire));
                    if (item.Matches(state) && state.IsCollectionKind() && state.IsReady() &&
                        state.IsTransitionField()) {
                        return ECacheCollectionLookupStatus::Pending;
                    }
                    continue;
                }
                collection = cacheOp.BuildCollectionRef(ref.Take());
                return ECacheCollectionLookupStatus::Hit;
        }
        Y_UNREACHABLE();
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AttachCollection(
    TSpaceOperation& spaceOp, TCollectionRegistry& registry, const TCacheItemRef& ref) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    const TCacheItem item = ref.CacheItem();
    THandle& handle = spaceOp.Handles()[item.Index()];
    TCollectionRegistry* owner = handle.Body.Collection->Registry_.load(std::memory_order_acquire);
    if (owner && owner != &registry && !cacheOp.Collections().MoveRegistry(*owner, registry, item)) {
        return false;
    }
    return cacheOp.Collections().Attach(registry, item);
}

SHARED_CACHE_TEMPLATE
ECacheResultStatus TSharedCache::Find(TOperationView& cacheOp, const TCacheKey& key, TCacheItemRef& ref) noexcept {
    for (;;) {
        TCacheItem cacheItem;
        const ECacheResultStatus status = cacheOp.Find(key, cacheItem);
        if (status == ECacheResultStatus::Miss) {
            return ECacheResultStatus::Miss;
        }
        Y_DEBUG_ABORT_UNLESS(status == ECacheResultStatus::Hit);
        TOperationItemRef acquired(cacheOp.SpaceOperation());
        switch (cacheOp.TryAcquire(cacheItem, false, acquired.Get())) {
            case EAcquireStatus::Stale:
                continue;
            case EAcquireStatus::Pending:
                return ECacheResultStatus::Pending;
            case EAcquireStatus::Ready:
                ref = acquired.Take();
                return ECacheResultStatus::Hit;
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
    } else if (candidate.Admission == EPageAdmission::Preload) {
        if (!cacheOp.TryReserve(reservedBytes, 1)) {
            return false;
        }
        cacheItem = cacheOp.Items().AllocateForPreload();
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
    THandle& initialized = spaceOp.Handles()[cacheItem.Index()];
    const bool wantsKeep = !THandleState::FromRaw(initialized.State.load(std::memory_order_relaxed)).IsStickyField() &&
                           owner && owner->GetCacheMode() == ECacheMode::TryKeepInMemory &&
                           owner->IsKeepAllowedPage(candidate.Type);
    if (wantsKeep && !candidate.KeepReservation) {
        candidate.KeepReservation = candidate.Admission == EPageAdmission::Preload
                                        ? Memory_.TryReserveKeep(reservedBytes, Eviction_.KeepLimit())
                                        : cacheOp.Eviction().ReserveKeepForDemand(reservedBytes);
        if (!candidate.KeepReservation && candidate.Admission == EPageAdmission::Preload) {
            cacheOp.Items().DiscardCandidate(cacheItem, EItemKind::Page, reserved ? reservedBytes : 0);
            return false;
        }
    }
    initialized.Body.Fetch->KeepReservation_ = std::move(candidate.KeepReservation);
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
        .Key = TCacheKey::Page(collection, static_cast<ui64>(page.Offset)),
        .Size = page.Size,
        .Type = page.Type,
        .Crc32 = page.Crc32,
        .Sticky = sticky,
    };
}

SHARED_CACHE_TEMPLATE
ECacheResultStatus TSharedCache::FindOrInsert(TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page,
    EStickyState sticky, TPageCacheItem& inserted, TCachePageRefImpl<TTraits>& hit) noexcept {
    Y_DEBUG_ABORT_UNLESS(page && page.Type != NTable::NPage::EPage::Undef);
    Y_DEBUG_ABORT_UNLESS(sticky == EStickyState::None || sticky == EStickyState::Sticky);
    inserted = {};
    hit.Drop();
    auto cacheOp = BeginOperation();
    TPageInsertCandidate candidate = BuildPageInsertCandidate(collection, page, sticky);
    TOperationItemRef ref(cacheOp);
    const ECacheResultStatus status = cacheOp.FindOrInsert(candidate, ref.Get());
    switch (status) {
        case ECacheResultStatus::Miss:
        case ECacheResultStatus::Pending:
            return status;
        case ECacheResultStatus::Inserted:
            inserted = TPageCacheItem::FromValidated(ref.CacheItem());
            return status;
        case ECacheResultStatus::Hit:
            hit = cacheOp.BuildPageRef(ref.Take());
            return status;
    }
    Y_UNREACHABLE();
}

SHARED_CACHE_TEMPLATE
ECacheResultStatus TSharedCache::FindOrInsertPage(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate,
    TIntrusivePtr<TPageFetchWaiter> waiter, TCachePageRefImpl<TTraits>& hit, TPageFetchImpl<TTraits>& fetch) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    hit.Drop();
    fetch = {};
    TOperationItemRef ref(spaceOp);
    const ECacheResultStatus status = cacheOp.FindOrInsert(candidate, ref.Get());
    switch (status) {
        case ECacheResultStatus::Miss:
            return status;
        case ECacheResultStatus::Inserted:
        case ECacheResultStatus::Pending: {
            const TCacheItem cacheItem = ref.CacheItem();
            THandle& handle = spaceOp.Handles()[cacheItem.Index()];
            const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            Y_DEBUG_ABORT_UNLESS(cacheItem.Matches(state) && state.IsPageKind() && state.IsPending() &&
                                 handle.Body.Fetch && handle.Body.Fetch->Page().CacheItem() == cacheItem);
            TPageFetchState* pageFetch = handle.Body.Fetch;
            if (status == ECacheResultStatus::Pending) {
                TCollection* collection = cacheOp.Collections().PageCollection(handle);
                if (candidate.Sticky == EStickyState::Sticky ||
                    (collection && Collections_.WantsStickyPage(*collection))) {
                    cacheOp.Collections().MakePageSticky(cacheItem);
                }
            }
            // The acquired pending ref prevents waiter-list closure until this operation releases it.
            Y_ABORT_UNLESS(pageFetch->Subscribe(waiter));
            InvokeHook(ECacheHookPoint::AfterFetchWaiterSubscribed, cacheItem);
            if (status == ECacheResultStatus::Inserted) {
                if (!state.IsWaitingForMemory()) {
                    pageFetch->KeepReservation_.MarkPending();
                }
                fetch = TPageFetchImpl<TTraits>(pageFetch);
                // The fetch owns this structural count; FetchState_->Page() supplies its exact identity.
                ref.Disarm();
            }
            return status;
        }
        case ECacheResultStatus::Hit:
            hit = cacheOp.BuildPageRef(ref.Take());
            return status;
    }
    Y_UNREACHABLE();
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PreparePageBatch(TSpaceOperation& spaceOp, TCollection& collection,
    TArrayRef<TCachePageRequestImpl<TTraits>> requests, TVector<TPageInsertCandidate>& candidates,
    ui64& reservedBytes) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    if (requests.size() > Max<ui32>()) {
        return false;
    }
    reservedBytes = 0;
    candidates.reserve(requests.size());
    THashSet<ui64> preparedOffsets;
    for (const TCachePageRequestImpl<TTraits>& request : requests) {
        Y_DEBUG_ABORT_UNLESS(
            request.Location && request.Location.Type != NTable::NPage::EPage::Undef && request.Waiter);
        Y_DEBUG_ABORT_UNLESS(request.Location.Size <= Max<ui64>() - NActors::TSharedData::OverheadSize);

        TPageInsertCandidate candidate = BuildPageInsertCandidate(
            collection.CacheItem, request.Location, request.Sticky ? EStickyState::Sticky : EStickyState::None);
        candidate.CollectionOwnerHeld = true;
        // Only pages that are absent from the table need admission budget: a page that is already resident is
        // a hit or a pending subscription and must be admitted even when the cache is at its limit.
        TCacheItem resident;
        candidate.Prepare = cacheOp.Find(candidate.Key, resident) == ECacheResultStatus::Miss &&
                            preparedOffsets.insert(static_cast<ui64>(request.Location.Offset)).second;
        if (candidate.Prepare) {
            const ui64 pageBytes = TCacheItems<TTraits>::AccountedPageBytes(candidate.Size);
            if (reservedBytes > Max<ui64>() - pageBytes) {
                return false;
            }
            reservedBytes += pageBytes;
        }
        candidates.push_back(std::move(candidate));
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
    const bool wantsKeep = collection.GetCacheMode() == ECacheMode::TryKeepInMemory;
    if (wantsKeep) {
        for (auto& candidate : candidates) {
            if (candidate.Prepare && collection.IsKeepAllowedPage(candidate.Type)) {
                const ui64 bytes = TCacheItems<TTraits>::AccountedPageBytes(candidate.Size);
                candidate.KeepReservation = admission == EPageAdmission::Preload
                                                ? Memory_.TryReserveKeep(bytes, Eviction_.KeepLimit())
                                                : cacheOp.Eviction().ReserveKeepForDemand(bytes);
                if (!candidate.KeepReservation && admission == EPageAdmission::Preload) {
                    return false;
                }
            }
        }
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
        const TCacheItem cacheItem =
            admission == EPageAdmission::Preload ? cacheOp.Items().AllocateForPreload() : cacheOp.Allocate();
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
        spaceOp.Handles()[cacheItem.Index()].Body.Fetch->KeepReservation_ = std::move(candidate.KeepReservation);
        candidate.CacheItem = cacheItem;
        initializedBytes += TCacheItems<TTraits>::AccountedPageBytes(candidate.Size);
        ++initializedPages;
    }
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FindOrInsertBatch(TCollectionCacheItem collection,
    TArrayRef<TCachePageRequestImpl<TTraits>> requests, bool recordStats, bool waitForMemory,
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
    InvokeHook(ECacheHookPoint::AfterPageBatchPrepared, collection.CacheItem());
    if (!cacheOp.AllocatePageBatch(owner, candidates, reservedBytes, waitForMemory, admission)) {
        return false;
    }

    // A rejected batch returns before this point, so every requested page reports exactly one admission outcome.
    TStats stats;
    for (ui32 index = 0; index < requests.size(); ++index) {
        TCachePageRequestImpl<TTraits>& request = requests[index];
        request.Reset();
        TCachePageRefImpl<TTraits> page;
        TPageFetchImpl<TTraits> fetch;
        const ECacheResultStatus status =
            cacheOp.FindOrInsertPage(candidates[index], std::move(request.Waiter), page, fetch);
        if (status == ECacheResultStatus::Hit) {
            if (request.Sticky || Collections_.WantsStickyPage(owner)) {
                cacheOp.Collections().MakePageSticky(page.CacheItem().CacheItem());
            }
            request.SetHit(std::move(page));
        } else if (status == ECacheResultStatus::Inserted) {
            request.SetInserted(std::move(fetch));
        } else if (status == ECacheResultStatus::Pending) {
            request.SetPending();
        }
        if (status != ECacheResultStatus::Miss || !waitForMemory) {
            stats.AddAdmission(status, request.Location.Size);
        }
    }
    if (recordStats) {
        Memory_.AddStats(stats);
    }
    return true;
}

SHARED_CACHE_TEMPLATE
ECacheResultStatus TSharedCache::FindOrInsertCollection(TSpaceOperation& spaceOp, TCollectionRegistry* registry,
    const TCollectionLocation& collection, TCollectionCacheItem& inserted, TCacheCollectionRef& hit) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    TCollectionInsertCandidate candidate{
        .Key = TCacheKey::Collection(collection.Id),
        .Bytes = collection.AccountedBytes(),
    };
    TOperationItemRef ref(spaceOp);
    const ECacheResultStatus status = cacheOp.FindOrInsert(candidate, ref.Get());
    if (status == ECacheResultStatus::Inserted) {
        inserted = TCollectionCacheItem::FromValidated(ref.CacheItem());
    } else if (status == ECacheResultStatus::Hit) {
        if (registry && !cacheOp.AttachCollection(*registry, ref.Get())) {
            return ECacheResultStatus::Pending;
        }
        hit = cacheOp.BuildCollectionRef(ref.Take());
    }
    return status;
}

SHARED_CACHE_TEMPLATE
template <class TKeyedCandidate>
ECacheResultStatus TSharedCache::FindOrInsert(
    TSpaceOperation& spaceOp, TKeyedCandidate& candidate, TCacheItemRef& ref) noexcept {
    auto cacheOp = BindOperation(spaceOp);
    for (;;) {
        typename TOperation::TInsertPosition insertPosition;
        TCacheItem cacheItem;
        const ECacheResultStatus status = cacheOp.FindForInsert(candidate.Key, cacheItem, insertPosition);
        bool inserted = false;
        if (status == ECacheResultStatus::Miss) {
            if (candidate.CacheItem.IsNull()) {
                if (!cacheOp.PrepareCandidate(candidate)) {
                    cacheOp.DropInsertOwner(insertPosition);
                    return ECacheResultStatus::Miss;
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
            Y_DEBUG_ABORT_UNLESS(status == ECacheResultStatus::Hit);
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
            return ECacheResultStatus::Inserted;
        }

        Y_DEBUG_ABORT_UNLESS(cacheOp.MatchesCandidateMetadata(candidate, cacheItem));
        if (!candidate.CacheItem.IsNull()) {
            cacheOp.RecycleCandidate(candidate.CacheItem);
            candidate.CacheItem = {};
        }
        ref = acquired.Take();
        return acquireStatus == EAcquireStatus::Pending ? ECacheResultStatus::Pending : ECacheResultStatus::Hit;
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
            if (reclamationAttempts == SharedCacheTransitionWorkBatch) {
                NotifyResourcesAvailable();
                return false;
            }
            if (!cacheOp.Eviction().Reclaim(bytes)) {
                return false;
            }
            ++reclamationAttempts;
            InvokeHook(ECacheHookPoint::AfterAdmissionReclaimStep, TCacheItem{});
            usage = Memory_.PayloadUsage();
            continue;
        }
        if (Memory_.TryReserve(usage, bytes, pages)) {
            return true;
        }
    }
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::UpdateHardLimit(ui64 limit) noexcept {
    if (limit == 0 || limit < CurrentLimit() || limit > ReservationLimit()) {
        return false;
    }

    const TCacheCapacityConfig& current = Space_->CurrentConfiguration();
    TCacheCapacityConfig target;
    ui8 minimumAddressBits = MinSharedCacheAddressBits;
    while (TCacheCapacityConfig{ .AddressBits = minimumAddressBits }.HotSlotCount() < Policy_.MinHotSlots) {
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
bool TSharedCache::ReclaimSpareHandle() noexcept {
    auto cacheOp = BeginOperation();
    if (!cacheOp.SpaceOperation().ReclaimSpareHandle()) {
        return false;
    }
    NotifyResourcesAvailable();
    return true;
}

SHARED_CACHE_TEMPLATE
typename TSharedCache::TKeepLoadRoom TSharedCache::KeepLoadRoom() noexcept {
    auto cacheOp = BeginOperation();
    const auto& keep = cacheOp.SpaceOperation().View().Tiers[ECacheTier::Keep];
    const THotRingLayout layout{ keep.EffectiveHotSlotCount() };
    // Fresh preload pages initially use L1/L0; empty upper levels are not admission room
    const ui64 slots = keep.ColdStorage.SlotCount + layout.Ring(EHotLevel::L0).Size + layout.Ring(EHotLevel::L1).Size;
    const ui64 pageLimit = Min(slots, Memory_.KeepLimitPages());
    const ui64 byteLimit = Eviction_.KeepLimit();
    return { byteLimit - Min(byteLimit, Memory_.KeepOwnedBytes()),
        pageLimit - Min(pageLimit, Memory_.KeepOwnedPages()) };
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::ReclaimForAdmission(ui64 bytes) noexcept {
    auto cacheOp = BeginOperation();
    bool progress = cacheOp.Eviction().ReclaimCold() || cacheOp.Eviction().ReclaimCold(ECacheTier::Keep);
    if (!progress) {
        progress = cacheOp.Eviction().EnforceCurrentLimit(bytes, true);
        progress = DrainHotResize() || progress;
        progress = cacheOp.Eviction().ReclaimCold() || cacheOp.Eviction().ReclaimCold(ECacheTier::Keep) || progress;
    }
    return progress;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::RunMaintenance() noexcept {
    Y_DEBUG_ABORT_UNLESS(CurrentLimit() <= HardLimit());
    if (Eviction_.NeedsKeepTrim())
    {
        if (Eviction_.SweepModes()) {
            return true;
        }
        auto cacheOp = BeginOperation();
        if (cacheOp.Eviction().TrimKeep()) {
            return true;
        }
    }
    // Give a growing tier its assigned storage before migrating its existing resident pages
    if (Capacity_.NeedsMaintenance() && Capacity_.RunMaintenance()) {
        return true;
    }
    return Eviction_.SweepModes();
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

#undef TCacheCollectionRef
#undef TCacheItemRef
#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

template class TSharedCacheImpl<TProdTraits>;
template class TSharedCacheImpl<TTestTraits>;

} // namespace NKikimr::NSharedCache
