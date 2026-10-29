#pragma once

#include "cache_collection.h"
#include "cache_collections.h"
#include "cache_eviction.h"
#include "cache_fetches.h"
#include "cache_items.h"
#include "cache_memory.h"
#include "cache_ref.h"
#include "cache_capacity.h"
#include "cache_table.h"

#include <util/generic/function_ref.h>
#include <util/generic/size_literals.h>

#include <new>
#include <type_traits>

namespace NKikimr::NSharedCache {

struct TCacheHandleRequirement {
    TLogoBlobID CollectionId;
    TArrayRef<const NTable::NPage::TPageLocation> Pages;
};

template <class TTraits = TProdTraits>
struct TCachePageRequestImpl {
    NTable::NPage::TPageLocation Location;
    TIntrusivePtr<TPageFetchWaiter> Waiter;
    bool Sticky = false;

    TCachePageRequestImpl(
        NTable::NPage::TPageLocation location, TIntrusivePtr<TPageFetchWaiter> waiter, bool sticky = false) noexcept
        : Location(std::move(location))
        , Waiter(std::move(waiter))
        , Sticky(sticky)
    {
        new (&Storage_.Page) TCachePageRefImpl<TTraits>();
    }

    TCachePageRequestImpl(TCachePageRequestImpl&& other) noexcept
        : TCachePageRequestImpl(std::move(other.Location), std::move(other.Waiter), other.Sticky)
    {
        MoveResult(other);
    }

    TCachePageRequestImpl& operator=(TCachePageRequestImpl&& other) noexcept;

    ~TCachePageRequestImpl() {
        Destroy();
    }

    ECacheResultStatus Status() const noexcept {
        return Status_;
    }

    TCachePageRefImpl<TTraits>& Page() noexcept {
        Y_ABORT_UNLESS(Status_ == ECacheResultStatus::Hit);
        return Storage_.Page;
    }

    const TCachePageRefImpl<TTraits>& Page() const noexcept {
        Y_ABORT_UNLESS(Status_ == ECacheResultStatus::Hit);
        return Storage_.Page;
    }

    TPageFetchImpl<TTraits>& Fetch() noexcept {
        Y_ABORT_UNLESS(Status_ == ECacheResultStatus::Inserted);
        return Storage_.Fetch;
    }

    void SetHit(TCachePageRefImpl<TTraits>&& page) noexcept {
        Destroy();
        new (&Storage_.Page) TCachePageRefImpl<TTraits>(std::move(page));
        Status_ = ECacheResultStatus::Hit;
    }

    void SetInserted(TPageFetchImpl<TTraits>&& fetch) noexcept {
        Destroy();
        new (&Storage_.Fetch) TPageFetchImpl<TTraits>(std::move(fetch));
        Status_ = ECacheResultStatus::Inserted;
    }

    void SetPending() noexcept {
        Reset();
        Status_ = ECacheResultStatus::Pending;
    }

    void Reset() noexcept {
        Destroy();
        new (&Storage_.Page) TCachePageRefImpl<TTraits>();
        Status_ = ECacheResultStatus::Miss;
    }

private:
    void Destroy() noexcept;

    void MoveResult(TCachePageRequestImpl& other) noexcept;

    // Fetch is active only for Inserted; Page is active for every other status.
    ECacheResultStatus Status_ = ECacheResultStatus::Miss;

    union TStorage {
        TStorage() noexcept {}

        ~TStorage() {}

        TCachePageRefImpl<TTraits> Page;
        TPageFetchImpl<TTraits> Fetch;
    } Storage_;
};

class TCacheTestAccess;
class TRequestPageWaiter;
class TCompactedPageWaiter;

template <class TTraits = TProdTraits>
class TSharedCacheImpl : public TThrRefBase, private TTraits {
public:
    static constexpr ui64 ExpectedPageSize = 7_KB;
    // Null handle, resize marker and collection metadata
    static constexpr ui64 RequestHandleOverhead = 3;

    static TIntrusivePtr<TSharedCacheImpl> Create(THolder<TCacheSpace> space, ui64 hardLimit, TTraits traits = {});

    ~TSharedCacheImpl() override;

    TSharedCacheImpl(const TSharedCacheImpl&) = delete;
    TSharedCacheImpl& operator=(const TSharedCacheImpl&) = delete;

    using TOperation = TCacheOperation<TTraits>;

    Y_FORCE_INLINE TOperation BeginOperation() noexcept;

    static TSharedCacheImpl& Get() noexcept {
        TSharedCacheImpl* cache = TryGet();
        Y_DEBUG_ABORT_UNLESS(cache);
        return *cache;
    }

    TCacheCollectionRefImpl<TTraits> AdmitCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection)
        requires std::is_same_v<TTraits, TProdTraits>;

    TCachePageRefImpl<TTraits> AdmitPage(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        NTable::NPage::TPageLocation location, NActors::TSharedData&& data, bool sticky = false)
        requires std::is_same_v<TTraits, TProdTraits>;

    static TSharedCacheImpl* TryGet() noexcept {
        return static_cast<TSharedCacheImpl*>(TTraits::TryGetCache());
    }

    Y_FORCE_INLINE bool MakeReady(TPageCacheItem page, NActors::TSharedData&& data) noexcept {
        auto cacheOp = BeginOperation();
        TOperationItemRef<TTraits> owner(cacheOp);
        TIntrusivePtr<TPageFetchState> fetch = cacheOp.Fetches().TryAcquire(page, owner);
        if (!fetch) {
            return false;
        }
        return cacheOp.Fetches().Complete(owner, *fetch, std::move(data));
    }

    Y_FORCE_INLINE bool FailReady(TPageCacheItem page) noexcept {
        auto cacheOp = BeginOperation();
        TOperationItemRef<TTraits> owner(cacheOp);
        TIntrusivePtr<TPageFetchState> fetch = cacheOp.Fetches().TryAcquire(page, owner);
        if (!fetch) {
            return false;
        }
        return cacheOp.Fetches().Fail(owner, *fetch);
    }

    Y_FORCE_INLINE bool MakeReady(TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
        return MakeReady(Collections_.Registry(), collection, std::move(value));
    }

    Y_FORCE_INLINE bool DetachCollection(TCollectionCacheItem collection) noexcept {
        return DetachCollection(Collections_.Registry(), collection);
    }

    Y_FORCE_INLINE bool EraseCollection(TCollectionCacheItem collection) noexcept {
        return EraseCollection(nullptr, collection);
    }

    bool SetCollectionPagesCacheMode(TCollectionCacheItem collection, ECacheMode mode) noexcept;
    bool SetCollectionSkipBTreeIndexV1Shadow(TCollectionCacheItem collection, bool skip) noexcept;

    Y_FORCE_INLINE bool AcknowledgeCollectionBytes(TCollectionCacheItem collection) noexcept {
        auto cacheOp = BeginOperation();
        TOperationItemRef<TTraits> ref(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
        if (auto* owner = ref ? cacheOp.Collections().PageCollection(collection) : nullptr) {
            Collections_.AcknowledgeBytes(*owner);
            return true;
        }
        return false;
    }

    ECacheResultStatus Find(TCollectionCacheItem collection, ui64 offset, TCachePageRefImpl<TTraits>& page) noexcept;

    ECacheResultStatus Find(const TLogoBlobID& id, TCacheCollectionRefImpl<TTraits>& collection) noexcept;

    // Finds and attaches a Ready collection without inserting on a miss
    Y_FORCE_INLINE ECacheCollectionLookupStatus FindCollection(
        const TLogoBlobID& id, TCacheCollectionRefImpl<TTraits>& collection) noexcept {
        return FindCollection(Collections_.Registry(), id, collection);
    }

    // The callback may detach the current collection. Registry mutations remain serialized by its owner.
    Y_FORCE_INLINE void ForEachCollection(TFunctionRef<void(TCollection&)> callback) noexcept {
        auto cacheOp = BeginOperation();
        cacheOp.Collections().ForEach(callback);
    }

    Y_FORCE_INLINE bool FindOrInsertBatch(TCollectionCacheItem collection,
        TArrayRef<TCachePageRequestImpl<TTraits>> requests, bool recordStats = true,
        bool waitForMemory = false) noexcept {
        return FindOrInsertBatch(collection, requests, recordStats, waitForMemory, EPageAdmission::Bounded);
    }

    // Optional preloads may reclaim Regular pages for handles, preserving resident Keep pages
    Y_FORCE_INLINE bool FindOrInsertPreloadBatch(
        TCollectionCacheItem collection, TArrayRef<TCachePageRequestImpl<TTraits>> requests) noexcept {
        return FindOrInsertBatch(collection, requests, false, false, EPageAdmission::Preload);
    }

    Y_FORCE_INLINE ui64 AvailableHandleCount() noexcept {
        return BeginOperation().SpaceOperation().AvailableHandleCount();
    }

    Y_FORCE_INLINE bool ReclaimForPreload() noexcept {
        auto cacheOp = BeginOperation();
        return cacheOp.Eviction().ReclaimRegularPage();
    }

    Y_FORCE_INLINE ECacheResultStatus FindOrInsert(const TCollectionLocation& collection,
        TCollectionCacheItem& inserted, TCacheCollectionRefImpl<TTraits>& hit) noexcept {
        return FindOrInsert(Collections_.Registry(), collection, inserted, hit);
    }

    Y_FORCE_INLINE bool ReclaimCold() noexcept {
        auto cacheOp = BeginOperation();
        return cacheOp.Eviction().ReclaimCold() || cacheOp.Eviction().ReclaimCold(ECacheTier::Keep);
    }

    bool ReclaimSpareHandle() noexcept;
    bool ReclaimForAdmission(ui64 bytes) noexcept;

    Y_FORCE_INLINE bool UpdateCurrentLimit(ui64 limit) noexcept {
        if (limit == 0 || limit > HardLimit()) {
            return false;
        }

        Memory_.SetCurrentLimit(limit);
        RefreshSoftLimit();
        NotifyResourcesAvailable();
        return true;
    }

    // Max<ui64>() restores the calculated default; zero is an explicit target.
    Y_FORCE_INLINE bool UpdateSoftLimitOverride(ui64 limit) noexcept {
        Memory_.SetSoftLimitOverride(limit);
        RefreshSoftLimit();
        return true;
    }

    bool UpdateHardLimit(ui64 limit) noexcept;

    ui64 AdmissionGeneration() const noexcept {
        return Memory_.AdmissionGeneration();
    }

    Y_FORCE_INLINE void ArmResourceWait(ui64 generation) noexcept {
        if (Memory_.ArmResourceWait(generation)) {
            TTraits::NotifyResourcesAvailable();
        }
    }

    Y_FORCE_INLINE bool TryReserveFetchBatch(TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept {
        auto cacheOp = BeginOperation();
        return cacheOp.Fetches().TryReserveBatch(pages);
    }

    // Advisory read-only snapshots; actual admission must still acquire its resources
    Y_FORCE_INLINE bool CanReserve(ui64 bytes) const noexcept {
        const ui64 limit = CurrentLimit();
        return bytes == 0 || (bytes <= limit && Memory_.PayloadUsage() <= limit - bytes);
    }

    bool CanAllocateHandles(TArrayRef<const TCacheHandleRequirement> requirements) noexcept;
    // Pressure-only check of request-held collection handles and distinct requested pages.
    bool WorkingSetExceedsCapacity(TArrayRef<const TCollectionCacheItem> retainedCollections,
        TArrayRef<const TCacheHandleRequirement> requirements) noexcept;

    void DisarmResourceWait() noexcept {
        Memory_.DisarmResourceWait();
    }

    Y_FORCE_INLINE bool EnforceCurrentLimit() noexcept {
        auto cacheOp = BeginOperation();
        return cacheOp.Eviction().EnforceCurrentLimit(0);
    }

    bool RunMaintenance() noexcept;

    ui64 CurrentLimit() const noexcept {
        return Memory_.CurrentLimit();
    }

    ui64 SoftLimit() const noexcept {
        return Memory_.SoftLimit();
    }

    ui64 HardLimit() const noexcept {
        return Memory_.HardLimit();
    }

    // Serialized controller access, like UpdateHardLimit and RunMaintenance.
    bool NeedsCapacityMaintenance() const noexcept {
        return Capacity_.NeedsMaintenance();
    }

    ui64 TargetHandleCount() const noexcept {
        return Capacity_.TargetHandleCount();
    }

    // Serialized controller access; workers use their active space view.
    ui64 PhysicalHandleCount() const noexcept {
        return Space_->CurrentConfiguration().HandleCount();
    }

    ui64 ReservationLimit() const noexcept {
        return Memory_.ReservationLimit();
    }

    ui64 ResidentBytes() const noexcept {
        return Memory_.ResidentBytes();
    }

    ui64 HotBytes() const noexcept {
        return Memory_.Hot(ECacheTier::Regular).Bytes() + Memory_.Hot(ECacheTier::Keep).Bytes();
    }

    ui64 HotPages() const noexcept {
        return Memory_.Hot(ECacheTier::Regular).Pages() + Memory_.Hot(ECacheTier::Keep).Pages();
    }

    ui64 ColdPages() const noexcept {
        return Memory_.Cold(ECacheTier::Regular).Pages() + Memory_.Cold(ECacheTier::Keep).Pages() +
               Memory_.Retired().Pages();
    }

    ui64 ColdRingEntries() const noexcept {
        return Space_->ColdRingEntries(ECacheTier::Regular);
    }

    ui64 StickyPages() const noexcept {
        return Memory_.Sticky().Pages();
    }

    ui64 KeepColdPages() const noexcept {
        return Memory_.Cold(ECacheTier::Keep).Pages();
    }

    ui64 Collections() const noexcept {
        return Memory_.Collections();
    }

    ui64 ColdBytes() const noexcept {
        return Memory_.Cold(ECacheTier::Regular).Bytes() + Memory_.Cold(ECacheTier::Keep).Bytes() +
               Memory_.Retired().Bytes();
    }

    ui64 ColdReclaimableBytes() const noexcept {
        return Memory_.ColdReclaimableBytes();
    }

    ui64 ColdReclaimableItems() const noexcept {
        return Memory_.ColdReclaimableItems();
    }

    ui64 StickyBytes() const noexcept {
        return Memory_.Sticky().Bytes();
    }

    ui64 KeepColdBytes() const noexcept {
        return Memory_.Cold(ECacheTier::Keep).Bytes();
    }

    ui64 KeepResidentPageBytes() const noexcept {
        return Memory_.KeepResidentPageBytes();
    }

    ui64 KeepReadyPageBytes() const noexcept {
        return Memory_.KeepReadyPageBytes();
    }

    ui64 CollectionBytes() const noexcept {
        return Memory_.CollectionBytes();
    }

    ui64 OverallUsage() const noexcept {
        return Memory_.OverallUsage();
    }

    ui64 PayloadUsage() const noexcept {
        return Memory_.PayloadUsage();
    }

    ui64 RetainedBytes() const noexcept {
        return Memory_.RetainedBytes();
    }

    ui64 ReservedBytes() const noexcept {
        return Memory_.ReservedBytes();
    }

    // Admitted pages whose payload has not arrived yet, i.e. the pages currently being loaded. The actor publishes
    // them as the LoadInFlyPages/LoadInFlyBytes counters.
    ui64 ReservedPages() const noexcept {
        return Memory_.ReservedPages();
    }

    Y_FORCE_INLINE bool UpdateKeepLimit(ui64 limit) noexcept {
        return Eviction_.UpdateKeepLimit(limit);
    }

    ui64 EvictableByteBudget() const noexcept {
        return Eviction_.EvictableByteBudget();
    }

    ui64 KeepLimit() const noexcept {
        return Eviction_.KeepLimit();
    }

    ui64 KeepOwnedBytes() const noexcept {
        return Memory_.KeepOwnedBytes();
    }

    struct TKeepLoadRoom {
        ui64 Bytes = 0;
        ui64 Pages = 0;
    };

    TKeepLoadRoom KeepLoadRoom() noexcept;

    ui64 KeepOwnedPages() const noexcept {
        return Memory_.KeepOwnedPages();
    }

    ui64 RetiredBytes() const noexcept {
        return Memory_.Retired().Bytes();
    }

    ui64 KeepPendingBytes() const noexcept {
        return Memory_.Tier(ECacheTier::Keep).Pending.Bytes();
    }

    ui64 StickyOwnedBytes() const noexcept {
        return Memory_.StickyOwnedBytes();
    }

    ui64 StaticBytes() const noexcept {
        return Memory_.StaticBytes();
    }

    ui64 StaticDeltaBytes() const noexcept {
        return Memory_.StaticDeltaBytes();
    }

    using TStats = TCacheMemory::TStats;

    TStats Stats() const noexcept {
        return Memory_.Stats();
    }

private:
    using TOperationView = TCacheOperationView<TTraits>;

    Y_FORCE_INLINE TOperationView BindOperation(TSpaceOperation& spaceOp) noexcept;

    friend class TCacheTestAccess;
    friend class TCacheThreadTestAccess;
    friend class TCollection;
    friend class TCacheItemRefImpl<TTraits>;
    friend class TCacheTableImpl<TTraits>;
    friend class TCacheItems<TTraits>;
    friend class TCachePageRefImpl<TTraits>;
    friend class TCacheCollectionRefImpl<TTraits>;
    friend class TCacheFetches<TTraits>;
    friend class TCacheCollections<TTraits>;
    friend class TCacheEviction<TTraits>;
    friend class TCacheHotPolicy<TTraits>;
    friend class TCacheColdPolicy<TTraits>;
    friend class TCacheCapacity<TTraits>;
    friend class TCacheOperation<TTraits>;
    friend class TCacheOperationView<TTraits>;
    friend class TCacheItemsOperation<TTraits>;
    friend class TCacheCollectionsOperation<TTraits>;
    friend class TCacheEvictionOperation<TTraits>;
    friend class TCacheFetchesOperation<TTraits>;
    friend class TRequestPageWaiter;
    friend class TCompactedPageWaiter;
    friend struct TEvSaveCompactedPages;

    TCacheItemRefImpl<TTraits> AcquirePageItem(TSpaceOperation& spaceOp, TPageCacheItem page) noexcept;

    Y_FORCE_INLINE void InvokeHook(ECacheHookPoint point, TCacheItem cacheItem) const noexcept {
        TTraits::Invoke(point, cacheItem);
    }

    Y_FORCE_INLINE void InvokeHook(ECacheHookPoint point, TColdCacheItem coldItem) const noexcept {
        // Scheduling hooks transport raw token bits; the production APIs keep their distinct types.
        TTraits::Invoke(point, TCacheItem::FromRaw(coldItem.Raw()));
    }

    void SetHooks(TTraits hooks) noexcept {
        static_cast<TTraits&>(*this) = std::move(hooks);
    }

    explicit TSharedCacheImpl(THolder<TCacheSpace> space, ui64 hardLimit, TTraits traits) noexcept;

    using EAcquireStatus = typename TCacheItems<TTraits>::EAcquireStatus;

    enum class EPageAdmission {
        Bounded,
        Preload,
        Written,
    };

    struct TPageInsertCandidate {
        TKeepReservation KeepReservation;
        TCacheKey Key;
        ui64 Size = 0;
        NTable::NPage::EPage Type = NTable::NPage::EPage::Undef;
        ui32 Crc32 = 0;
        EStickyState Sticky = EStickyState::None;
        TCacheItem CacheItem;
        bool CollectionOwnerHeld = false; // a batch owns the collection through candidate retries
        bool Prepare = false; // the page is absent from the table and needs an allocated pending item
        bool WaitForMemory = false;
        EPageAdmission Admission = EPageAdmission::Bounded;
    };

    struct TCollectionInsertCandidate {
        TCacheKey Key;
        ui64 Bytes = 0;
        TCacheItem CacheItem;
    };

    struct TCacheKeyedItem {
        TCacheKey Key;
        TCacheItem CacheItem;
    };

    TPageCacheItem AllocatePage(TCollectionCacheItem collection, ui64 offset, ui64 size, NTable::NPage::EPage type,
        ui32 crc32, EStickyState sticky) noexcept;

    TCollectionCacheItem AllocateCollection(const TLogoBlobID& id, ui64 bytes) noexcept;

    Y_FORCE_INLINE bool MakeReady(
        TCollectionRegistry& registry, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
        return MakeReady(&registry, collection, std::move(value));
    }

    Y_FORCE_INLINE bool MakeReady(
        TCollectionRegistry* registry, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
        auto cacheOp = BeginOperation();
        return cacheOp.MakeReady(registry, collection.CacheItem(), std::move(value));
    }

    bool MakeReady(TSpaceOperation& spaceOp, TCollectionRegistry* registry, TCacheItem cacheItem,
        THolder<TCollection>&& value) noexcept;

    void MakeReadyState(TSpaceOperation& spaceOp, TCacheItem cacheItem, EHandleState readyState = EHandleState::Hot,
        TCollection* pageOwner = nullptr, TKeepReservation* keepReservation = nullptr) noexcept;

    ECacheResultStatus Find(TOperationView& cacheOp, const TCacheKey& key, TCacheItemRefImpl<TTraits>& ref) noexcept;

    ECacheCollectionLookupStatus FindCollection(
        TCollectionRegistry& registry, const TLogoBlobID& id, TCacheCollectionRefImpl<TTraits>& collection) noexcept;
    bool AttachCollection(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, const TCacheItemRefImpl<TTraits>& ref) noexcept;

    Y_FORCE_INLINE ECacheResultStatus FindOrInsert(
        TSpaceOperation& spaceOp, TCacheItem candidate, TCacheItemRefImpl<TTraits>& ref) noexcept {
        auto cacheOp = BindOperation(spaceOp);
        const THandle& handle = spaceOp.Handles()[candidate.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        TCacheKeyedItem keyedCandidate{
            .Key = LoadSharedCacheKey(handle, state.Kind()),
            .CacheItem = candidate,
        };
        return cacheOp.FindOrInsert(keyedCandidate, ref);
    }

    static TPageInsertCandidate BuildPageInsertCandidate(
        TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page, EStickyState sticky) noexcept;

    ECacheResultStatus FindOrInsert(TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page,
        EStickyState sticky, TPageCacheItem& inserted, TCachePageRefImpl<TTraits>& hit) noexcept;

    Y_FORCE_INLINE ECacheResultStatus FindOrInsert(TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EStickyState sticky, TIntrusivePtr<TPageFetchWaiter> waiter,
        TCachePageRefImpl<TTraits>& hit, TPageFetchImpl<TTraits>& fetch) noexcept {
        Y_DEBUG_ABORT_UNLESS(page && page.Type != NTable::NPage::EPage::Undef && waiter);
        Y_DEBUG_ABORT_UNLESS(sticky == EStickyState::None || sticky == EStickyState::Sticky);
        auto cacheOp = BeginOperation();
        TPageInsertCandidate candidate = BuildPageInsertCandidate(collection, page, sticky);
        const ECacheResultStatus status = cacheOp.FindOrInsertPage(candidate, std::move(waiter), hit, fetch);
        TStats stats;
        stats.AddAdmission(status, page.Size);
        Memory_.AddStats(stats);
        return status;
    }

    Y_FORCE_INLINE ECacheResultStatus FindOrInsert(TCollectionRegistry& registry, const TCollectionLocation& collection,
        TCollectionCacheItem& inserted, TCacheCollectionRefImpl<TTraits>& hit) noexcept {
        inserted = {};
        hit.Drop();
        auto cacheOp = BeginOperation();
        return cacheOp.FindOrInsertCollection(&registry, collection, inserted, hit);
    }

    Y_FORCE_INLINE ECacheResultStatus FindOrInsertUnregistered(const TCollectionLocation& collection,
        TCollectionCacheItem& inserted, TCacheCollectionRefImpl<TTraits>& hit) noexcept {
        inserted = {};
        hit.Drop();
        auto cacheOp = BeginOperation();
        return cacheOp.FindOrInsertCollection(nullptr, collection, inserted, hit);
    }

    ECacheResultStatus FindOrInsertPage(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate,
        TIntrusivePtr<TPageFetchWaiter> waiter, TCachePageRefImpl<TTraits>& hit,
        TPageFetchImpl<TTraits>& fetch) noexcept;
    bool PreparePageBatch(TSpaceOperation& spaceOp, TCollection& collection,
        TArrayRef<TCachePageRequestImpl<TTraits>> requests, TVector<TPageInsertCandidate>& candidates,
        ui64& reservedBytes) noexcept;
    bool AllocatePageBatch(TSpaceOperation& spaceOp, TCollection& collection, TVector<TPageInsertCandidate>& candidates,
        ui64 reservedBytes, bool waitForMemory, EPageAdmission admission) noexcept;

    Y_FORCE_INLINE bool FindOrInsertWrittenBatch(
        TCollectionCacheItem collection, TArrayRef<TCachePageRequestImpl<TTraits>> requests) noexcept {
        return FindOrInsertBatch(collection, requests, false, false, EPageAdmission::Written);
    }

    bool FindOrInsertBatch(TCollectionCacheItem collection, TArrayRef<TCachePageRequestImpl<TTraits>> requests,
        bool recordStats, bool waitForMemory, EPageAdmission admission) noexcept;
    ECacheResultStatus FindOrInsertCollection(TSpaceOperation& spaceOp, TCollectionRegistry* registry,
        const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TCacheCollectionRefImpl<TTraits>& hit) noexcept;

    template <class TKeyedCandidate>
    ECacheResultStatus FindOrInsert(
        TSpaceOperation& spaceOp, TKeyedCandidate& candidate, TCacheItemRefImpl<TTraits>& ref) noexcept;

    bool PrepareCandidate(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate) noexcept;
    bool PrepareCandidate(TSpaceOperation& spaceOp, TCollectionInsertCandidate& candidate) noexcept;

    bool PrepareCandidate(TSpaceOperation&, TCacheKeyedItem& candidate) noexcept {
        return !candidate.CacheItem.IsNull();
    }

    bool TryReserve(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages = 0) noexcept;

    Y_FORCE_INLINE void NotifyResourcesAvailable() noexcept {
        if (Memory_.NotifyResourcesAvailable()) {
            TTraits::NotifyResourcesAvailable();
        }
    }

    Y_FORCE_INLINE void ReleaseReservation(ui64 bytes, ui64 pages = 0) noexcept {
        Memory_.ReleaseReservation(bytes, pages);
        NotifyResourcesAvailable();
    }

    Y_FORCE_INLINE void DiscardCandidate(TPageCacheItem page) noexcept {
        DiscardCandidate(page.CacheItem(), EItemKind::Page);
    }

    Y_FORCE_INLINE void DiscardCandidate(TCollectionCacheItem collection) noexcept {
        DiscardCandidate(collection.CacheItem(), EItemKind::Collection);
    }

    Y_FORCE_INLINE void DiscardCandidate(TCacheItem cacheItem, EItemKind kind) noexcept {
        auto cacheOp = BeginOperation();
        const THandle& handle = cacheOp.Handles()[cacheItem.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        const ui64 reservedBytes =
            state.IsPending() && !state.IsWaitingForMemory() ? TCacheItems<TTraits>::PayloadBytes(handle, kind) : 0;
        cacheOp.DiscardCandidate(cacheItem, kind, reservedBytes);
    }

    Y_FORCE_INLINE bool DetachCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
        auto cacheOp = BeginOperation();
        TOperationItemRef<TTraits> collectionRef(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
        if (!collectionRef) {
            return false;
        }
        return cacheOp.Collections().Detach(registry, collection.CacheItem());
    }

    Y_FORCE_INLINE bool EraseCollection(TCollectionRegistry* registry, TCollectionCacheItem collection) noexcept {
        auto cacheOp = BeginOperation();
        TOperationItemRef<TTraits> collectionRef(cacheOp, cacheOp.TryAcquireStructural(collection.CacheItem()));
        if (!collectionRef) {
            return false;
        }
        TCollection* value = cacheOp.Collections().PageCollection(collection);
        if (!value || !cacheOp.Collections().SetPagesCacheMode(*value, ECacheMode::Regular)) {
            return false;
        }
        return cacheOp.Collections().Erase(registry, collection.CacheItem());
    }

    bool IsPageSticky(TPageCacheItem page) const noexcept;

    Y_FORCE_INLINE TCachePageRefImpl<TTraits> AcquireMorePageRef(TCacheItem item) noexcept {
        auto cacheOp = BeginOperation();
        return cacheOp.BuildPageRef(cacheOp.Items().AcquireMorePageRef(item));
    }

    Y_FORCE_INLINE TCacheCollectionRefImpl<TTraits> AcquireMoreCollectionRef(TCacheItem item) noexcept {
        auto cacheOp = BeginOperation();
        return cacheOp.BuildCollectionRef(cacheOp.Items().AcquireMoreCollectionRef(item));
    }

    bool AcquirePage(TPageCacheItem page, TCachePageRefImpl<TTraits>& result) noexcept;

    // Pending-item cleanup may reenter this coordinator
    void Release(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept {
        auto cacheOp = BindOperation(spaceOp);
        TCollection* owner = cacheOp.ReleaseItem(cacheItem);
        if (owner) {
            cacheOp.Collections().DropOwnerReference(*owner);
        }
    }

    void ReleaseReplacing(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void ReleaseReplaced(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    static Y_FORCE_INLINE void DeleteReplacedPage(THandle& handle) noexcept {
        Y_DEBUG_ABORT_UNLESS(handle.Body.PageBuffer);
        handle.Body.PageBuffer.~TBuffer();
    }

    bool MatchesCandidateMetadata(
        const TSpaceOperation& spaceOp, TCacheItem candidate, TCacheItem existing) const noexcept;

    Y_FORCE_INLINE bool MatchesCandidateMetadata(
        const TSpaceOperation& spaceOp, const TPageInsertCandidate& candidate, TCacheItem existing) const noexcept {
        const THandle& handle = spaceOp.Handles()[existing.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        Y_DEBUG_ABORT_UNLESS(existing.Matches(state) && state.IsPageKind());
        return handle.Key2OrSize.load(std::memory_order_relaxed) == candidate.Size &&
               handle.Metadata.Page.Type == candidate.Type && handle.Metadata.Page.Crc32 == candidate.Crc32;
    }

    Y_FORCE_INLINE bool MatchesCandidateMetadata(const TSpaceOperation& spaceOp,
        const TCollectionInsertCandidate& candidate, TCacheItem existing) const noexcept {
        const THandle& handle = spaceOp.Handles()[existing.Index()];
        const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
        Y_DEBUG_ABORT_UNLESS(existing.Matches(state) && state.IsCollectionKind());
        return handle.Metadata.PayloadBytes == candidate.Bytes;
    }

    bool MatchesCandidateMetadata(
        const TSpaceOperation& spaceOp, const TCacheKeyedItem& candidate, TCacheItem existing) const noexcept {
        return MatchesCandidateMetadata(spaceOp, candidate.CacheItem, existing);
    }

    Y_FORCE_INLINE void UpdateColdReclaimable(ui64 payloadBytes, THandleState before, THandleState after) noexcept {
        if (Memory_.UpdateColdReclaimable(payloadBytes, before, after)) {
            NotifyResourcesAvailable();
        }
    }

    static ui32 AdvanceColdMembership(THandle& handle) noexcept;

    bool DrainHotResize() noexcept;

    Y_FORCE_INLINE void RefreshSoftLimit() noexcept {
        const ui64 softLimit = Memory_.RefreshSoftLimit(Policy_.CurrentLimitGap, Policy_.MinCurrentLimitGap);
        Eviction_.RefreshKeepLimits();
        if (RetainedBytes() >= softLimit) {
            Eviction_.MarkSoftLimitPressure();
        }
    }

    TCacheItemRefImpl<TTraits> TryAcquirePageForRelocation(TSpaceOperation& spaceOp, ui32 index) noexcept;
    TCacheItem PreparePageReplacement(TSpaceOperation& spaceOp, TCacheItem source) noexcept;
    void DiscardPageReplacement(TSpaceOperation& spaceOp, TCacheItem replacement) noexcept;
    bool PublishPageReplacement(
        TSpaceOperation& spaceOp, TCacheItem source, TCacheItem replacement, EHandleState& oldState) noexcept;
    void CompletePageReplacement(TSpaceOperation& spaceOp, TCacheItem source) noexcept;
    bool RelocatePage(TSpaceOperation& spaceOp, ui32 index) noexcept;
    bool DemoteUnlinkedStickyPage(TSpaceOperation& spaceOp, ui32 index) noexcept;
    EShrinkCutStatus ReclaimShrinkHandle(TSpaceOperation& spaceOp, ui32 index) noexcept;

private:
    THolder<TCacheSpace> Space_;
    TCacheTableImpl<TTraits> Table_;
    TCacheMemory Memory_;
    TCacheItems<TTraits> Items_;
    TCacheCollections<TTraits> Collections_;
    TCacheEviction<TTraits> Eviction_;
    TCacheCapacity<TTraits> Capacity_;
    TCacheFetches<TTraits> Fetches_;
    THotResize HotResize_;

    inline static constexpr TCachePolicy Policy_ = SharedCachePolicyFor<TTraits>;
};

template <class TTraits>
class TCacheOperationView {
    using EPageAdmission = typename TSharedCacheImpl<TTraits>::EPageAdmission;
    using TPageInsertCandidate = typename TSharedCacheImpl<TTraits>::TPageInsertCandidate;
    using TCollectionInsertCandidate = typename TSharedCacheImpl<TTraits>::TCollectionInsertCandidate;
    using TCacheKeyedItem = typename TSharedCacheImpl<TTraits>::TCacheKeyedItem;

public: // Cache table operations
    using TInsertPosition = typename TCacheTableImpl<TTraits>::TInsertPosition;
    using EResizeStep = typename TCacheTableImpl<TTraits>::EResizeStep;
    using EAcquireStatus = typename TSharedCacheImpl<TTraits>::EAcquireStatus;

    Y_FORCE_INLINE TSpaceOperation& SpaceOperation() const noexcept {
        return SpaceOp_;
    }

    Y_FORCE_INLINE ECacheResultStatus Find(const TCacheKey& key, TCacheItem& item) noexcept {
        return Cache_.Table_.Find(SpaceOp_, key, item);
    }

    Y_FORCE_INLINE ECacheResultStatus Find(const TCacheKey& key, TCacheItemRefImpl<TTraits>& ref) noexcept {
        return Cache_.Find(*this, key, ref);
    }

    Y_FORCE_INLINE ECacheResultStatus FindForInsert(
        const TCacheKey& key, TCacheItem& item, TInsertPosition& position) noexcept {
        return Cache_.Table_.FindForInsert(SpaceOp_, key, item, position);
    }

    Y_FORCE_INLINE bool InsertAt(TInsertPosition& position, TCacheItem candidate) noexcept {
        return Cache_.Table_.InsertAt(SpaceOp_, position, candidate);
    }

    Y_FORCE_INLINE void DropInsertOwner(TInsertPosition& position) noexcept {
        position.DropOwner(SpaceOp_);
    }

    Y_FORCE_INLINE bool TryBeginErase(
        TCacheItem item, THandleState& expected, TCacheItemRefImpl<TTraits>& owner) noexcept {
        return Cache_.Table_.TryBeginErase(SpaceOp_, item, expected, owner);
    }

    Y_FORCE_INLINE bool TryBeginErase(TCacheItemRefImpl<TTraits>& owner, THandleState& expected) noexcept {
        return Cache_.Table_.TryBeginErase(SpaceOp_, owner, expected);
    }

    Y_FORCE_INLINE void FinishErase(TCacheItem item, const TCacheKey& key) noexcept {
        Cache_.Table_.FinishErase(SpaceOp_, item, key);
    }

public: // Cache helpers
    Y_FORCE_INLINE EAcquireStatus TryAcquire(
        TCacheItem item, bool allowPending, TCacheItemRefImpl<TTraits>& ref) noexcept {
        return Items().TryAcquire(item, allowPending, ref);
    }

    Y_FORCE_INLINE TCacheItemRefImpl<TTraits> TryAcquireStructural(TCacheItem item) noexcept {
        return Items().TryAcquireStructural(item);
    }

    Y_FORCE_INLINE void Release(TCacheItem item) noexcept {
        Cache_.Release(SpaceOp_, item);
    }

    Y_FORCE_INLINE typename TCacheFetches<TTraits>::TOperation Fetches() noexcept {
        return { Cache_, SpaceOp_ };
    }

    Y_FORCE_INLINE TCacheItem Allocate() noexcept {
        return Items().Allocate();
    }

    Y_FORCE_INLINE TCacheItem TryAllocateReservedItem(ui64 bytes, ui64 pages = 0) noexcept {
        return Items().TryAllocateReservedItem(bytes, pages);
    }

    Y_FORCE_INLINE ui32 TryAllocateHandle() noexcept {
        return Items().TryAllocateHandle();
    }

    Y_FORCE_INLINE void RefillSpareItem() noexcept {
        Items().RefillSpareItem();
    }

    Y_FORCE_INLINE void RecycleCandidate(TCacheItem cacheItem) noexcept {
        Items().RecycleCandidate(cacheItem);
    }

    Y_FORCE_INLINE bool MakeReady(
        TCollectionRegistry* registry, TCacheItem cacheItem, THolder<TCollection>&& value) noexcept {
        return Cache_.MakeReady(SpaceOp_, registry, cacheItem, std::move(value));
    }

    Y_FORCE_INLINE void MakeReadyState(
        TCacheItem cacheItem, EHandleState readyState = EHandleState::Hot, TCollection* pageOwner = nullptr) noexcept {
        Cache_.MakeReadyState(SpaceOp_, cacheItem, readyState, pageOwner);
    }

    Y_FORCE_INLINE ECacheResultStatus FindOrInsert(TCacheItem candidate, TCacheItemRefImpl<TTraits>& ref) noexcept {
        return Cache_.FindOrInsert(SpaceOp_, candidate, ref);
    }

    Y_FORCE_INLINE ECacheResultStatus FindOrInsertPage(TPageInsertCandidate& candidate,
        TIntrusivePtr<TPageFetchWaiter> waiter, TCachePageRefImpl<TTraits>& hit,
        TPageFetchImpl<TTraits>& fetch) noexcept {
        return Cache_.FindOrInsertPage(SpaceOp_, candidate, std::move(waiter), hit, fetch);
    }

    Y_FORCE_INLINE bool PreparePageBatch(TCollection& collection, TArrayRef<TCachePageRequestImpl<TTraits>> requests,
        TVector<TPageInsertCandidate>& candidates, ui64& reservedBytes) noexcept {
        return Cache_.PreparePageBatch(SpaceOp_, collection, requests, candidates, reservedBytes);
    }

    Y_FORCE_INLINE bool AllocatePageBatch(TCollection& collection, TVector<TPageInsertCandidate>& candidates,
        ui64 reservedBytes, bool waitForMemory, EPageAdmission admission) noexcept {
        return Cache_.AllocatePageBatch(SpaceOp_, collection, candidates, reservedBytes, waitForMemory, admission);
    }

    Y_FORCE_INLINE ECacheResultStatus FindOrInsertCollection(TCollectionRegistry* registry,
        const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TCacheCollectionRefImpl<TTraits>& hit) noexcept {
        return Cache_.FindOrInsertCollection(SpaceOp_, registry, collection, inserted, hit);
    }

    Y_FORCE_INLINE bool AttachCollection(
        TCollectionRegistry& registry, const TCacheItemRefImpl<TTraits>& ref) noexcept {
        return Cache_.AttachCollection(SpaceOp_, registry, ref);
    }

    template <class TKeyedCandidate>
    Y_FORCE_INLINE ECacheResultStatus FindOrInsert(
        TKeyedCandidate& candidate, TCacheItemRefImpl<TTraits>& ref) noexcept {
        return Cache_.FindOrInsert(SpaceOp_, candidate, ref);
    }

    Y_FORCE_INLINE bool PrepareCandidate(TPageInsertCandidate& candidate) noexcept {
        return Cache_.PrepareCandidate(SpaceOp_, candidate);
    }

    Y_FORCE_INLINE bool PrepareCandidate(TCollectionInsertCandidate& candidate) noexcept {
        return Cache_.PrepareCandidate(SpaceOp_, candidate);
    }

    Y_FORCE_INLINE bool PrepareCandidate(TCacheKeyedItem& candidate) noexcept {
        return Cache_.PrepareCandidate(SpaceOp_, candidate);
    }

    Y_FORCE_INLINE bool TryReserve(ui64 bytes, ui64 pages = 0) noexcept {
        return Cache_.TryReserve(SpaceOp_, bytes, pages);
    }

    Y_FORCE_INLINE void DiscardCandidate(TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes = 0) noexcept {
        Items().DiscardCandidate(cacheItem, kind, reservedBytes);
    }

    Y_FORCE_INLINE void InitializePage(TCacheItem cacheItem, TCollectionCacheItem collection, ui64 offset, ui64 size,
        NTable::NPage::EPage type, ui32 crc32, EStickyState sticky, bool reserved = true,
        bool collectionOwnerHeld = false) noexcept {
        Items().InitializePage(cacheItem, collection, offset, size, type, crc32, sticky, reserved, collectionOwnerHeld);
    }

    Y_FORCE_INLINE TCachePageRefImpl<TTraits> BuildPageRef(TCacheItemRefImpl<TTraits>&& ref) const noexcept {
        Y_DEBUG_ABORT_UNLESS(&TSharedCacheImpl<TTraits>::Get() == &Cache_);
        return Items().BuildPageRef(std::move(ref));
    }

    Y_FORCE_INLINE TCacheCollectionRefImpl<TTraits> BuildCollectionRef(
        TCacheItemRefImpl<TTraits>&& ref) const noexcept {
        Y_DEBUG_ABORT_UNLESS(&TSharedCacheImpl<TTraits>::Get() == &Cache_);
        return Items().BuildCollectionRef(std::move(ref));
    }

    Y_FORCE_INLINE TCollection* ReleaseItem(TCacheItem cacheItem) noexcept {
        return Items().ReleaseItem(cacheItem);
    }

    Y_FORCE_INLINE void ReleaseReplacing(TCacheItem cacheItem) noexcept {
        Cache_.ReleaseReplacing(SpaceOp_, cacheItem);
    }

    Y_FORCE_INLINE void ReleaseReplaced(TCacheItem cacheItem) noexcept {
        Cache_.ReleaseReplaced(SpaceOp_, cacheItem);
    }

    Y_FORCE_INLINE bool MatchesCandidateMetadata(TCacheItem candidate, TCacheItem existing) const noexcept {
        return Cache_.MatchesCandidateMetadata(SpaceOp_, candidate, existing);
    }

    Y_FORCE_INLINE bool MatchesCandidateMetadata(
        const TPageInsertCandidate& candidate, TCacheItem existing) const noexcept {
        return Cache_.MatchesCandidateMetadata(SpaceOp_, candidate, existing);
    }

    Y_FORCE_INLINE bool MatchesCandidateMetadata(
        const TCollectionInsertCandidate& candidate, TCacheItem existing) const noexcept {
        return Cache_.MatchesCandidateMetadata(SpaceOp_, candidate, existing);
    }

    Y_FORCE_INLINE bool MatchesCandidateMetadata(const TCacheKeyedItem& candidate, TCacheItem existing) const noexcept {
        return Cache_.MatchesCandidateMetadata(SpaceOp_, candidate, existing);
    }

    Y_FORCE_INLINE typename TCacheCollections<TTraits>::TOperation Collections() noexcept {
        return { Cache_, SpaceOp_ };
    }

    Y_FORCE_INLINE typename TCacheEviction<TTraits>::TOperation Eviction() noexcept {
        return { Cache_, SpaceOp_ };
    }

    Y_FORCE_INLINE typename TCacheItems<TTraits>::TOperation Items() const noexcept {
        return { Cache_, SpaceOp_ };
    }

    Y_FORCE_INLINE TCacheItemRefImpl<TTraits> TryAcquirePageForRelocation(ui32 index) noexcept {
        return Cache_.TryAcquirePageForRelocation(SpaceOp_, index);
    }

    Y_FORCE_INLINE TCacheItem PreparePageReplacement(TCacheItem source) noexcept {
        return Cache_.PreparePageReplacement(SpaceOp_, source);
    }

    Y_FORCE_INLINE void DiscardPageReplacement(TCacheItem replacement) noexcept {
        Cache_.DiscardPageReplacement(SpaceOp_, replacement);
    }

    Y_FORCE_INLINE bool PublishPageReplacement(
        TCacheItem source, TCacheItem replacement, EHandleState& oldState) noexcept {
        return Cache_.PublishPageReplacement(SpaceOp_, source, replacement, oldState);
    }

    Y_FORCE_INLINE void CompletePageReplacement(TCacheItem source) noexcept {
        Cache_.CompletePageReplacement(SpaceOp_, source);
    }

    Y_FORCE_INLINE bool RelocatePage(ui32 index) noexcept {
        return Cache_.RelocatePage(SpaceOp_, index);
    }

    Y_FORCE_INLINE bool DemoteUnlinkedStickyPage(ui32 index) noexcept {
        return Cache_.DemoteUnlinkedStickyPage(SpaceOp_, index);
    }

    Y_FORCE_INLINE EShrinkCutStatus ReclaimShrinkHandle(ui32 index) noexcept {
        return Cache_.ReclaimShrinkHandle(SpaceOp_, index);
    }

public: // Resizing
    Y_FORCE_INLINE EResizeStep PrepareBucketSplit() noexcept {
        return Cache_.Table_.PrepareBucketSplit(SpaceOp_);
    }

    Y_FORCE_INLINE EResizeStep ActivateBucketSplitOrMerge() noexcept {
        return Cache_.Table_.ActivateBucketSplitOrMerge(SpaceOp_);
    }

    Y_FORCE_INLINE EResizeStep PublishBucketSplit() noexcept {
        return Cache_.Table_.PublishBucketSplit(SpaceOp_);
    }

    Y_FORCE_INLINE EResizeStep CloseBucketSplit() noexcept {
        return Cache_.Table_.CloseBucketSplit(SpaceOp_);
    }

    Y_FORCE_INLINE EResizeStep RemoveBucketSplit() noexcept {
        return Cache_.Table_.RemoveBucketSplit(SpaceOp_);
    }

    Y_FORCE_INLINE void PublishPageReplacement(
        TCacheItem source, TCacheItem replacement, const TCacheKey& key) noexcept {
        Cache_.Table_.PublishPageReplacement(source, replacement, key, SpaceOp_);
    }

private:
    friend class TSharedCacheImpl<TTraits>;
    friend class TCacheOperation<TTraits>;

    Y_FORCE_INLINE TCacheOperationView(TSharedCacheImpl<TTraits>& cache, TSpaceOperation& spaceOp) noexcept
        : Cache_(cache)
        , SpaceOp_(spaceOp)
    {}

protected: // Data members
    TSharedCacheImpl<TTraits>& Cache_;
    TSpaceOperation& SpaceOp_;
};

// Leave operations implicitly instantiated so their constructors can inline
template <class TTraits>
class TCacheOperation final : public TSpaceOperation, public TCacheOperationView<TTraits> {
    using TOperationView = TCacheOperationView<TTraits>;

public:
    using TOperationView::TryAllocateHandle;

    TCacheOperation(const TCacheOperation&) = delete;
    TCacheOperation& operator=(const TCacheOperation&) = delete;
    TCacheOperation& operator=(TCacheOperation&&) = delete;

    Y_FORCE_INLINE TCacheOperation(TCacheOperation&& other) noexcept
        : TSpaceOperation(std::move(other))
        , TOperationView(other.Cache_, *this)
    {}

private:
    friend class TSharedCacheImpl<TTraits>;

    Y_FORCE_INLINE TCacheOperation(TSharedCacheImpl<TTraits>& cache, TSpaceOperation&& spaceOp) noexcept
        : TSpaceOperation(std::move(spaceOp))
        , TOperationView(cache, *this)
    {}
};

template <class TTraits>
Y_FORCE_INLINE typename TSharedCacheImpl<TTraits>::TOperationView TSharedCacheImpl<TTraits>::BindOperation(
    TSpaceOperation& spaceOp) noexcept {
    return TOperationView(*this, spaceOp);
}

template <class TTraits>
Y_FORCE_INLINE typename TSharedCacheImpl<TTraits>::TOperation TSharedCacheImpl<TTraits>::BeginOperation() noexcept {
    return TOperation(*this, Space_->BeginOperation());
}

using TPageFetch = TPageFetchImpl<>;
using TCachePageRequest = TCachePageRequestImpl<>;
using TSharedCache = TSharedCacheImpl<>;

// Inline implementations

template <class TTraits>
inline TCachePageRequestImpl<TTraits>& TCachePageRequestImpl<TTraits>::operator=(
    TCachePageRequestImpl&& other) noexcept {
    if (this != &other) {
        Location = std::move(other.Location);
        Waiter = std::move(other.Waiter);
        Sticky = other.Sticky;
        MoveResult(other);
    }
    return *this;
}

template <class TTraits>
inline void TCachePageRequestImpl<TTraits>::Destroy() noexcept {
    if (Status_ == ECacheResultStatus::Inserted) {
        Storage_.Fetch.~TPageFetchImpl<TTraits>();
    } else {
        Storage_.Page.~TCachePageRefImpl<TTraits>();
    }
}

template <class TTraits>
inline void TCachePageRequestImpl<TTraits>::MoveResult(TCachePageRequestImpl& other) noexcept {
    Destroy();
    if (other.Status_ == ECacheResultStatus::Inserted) {
        new (&Storage_.Fetch) TPageFetchImpl<TTraits>(std::move(other.Storage_.Fetch));
    } else {
        new (&Storage_.Page) TCachePageRefImpl<TTraits>(std::move(other.Storage_.Page));
    }
    Status_ = other.Status_;
}

} // namespace NKikimr::NSharedCache
