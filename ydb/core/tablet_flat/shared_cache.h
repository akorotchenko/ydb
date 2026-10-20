#pragma once

#include "shared_cache_collection.h"
#include "shared_cache_fetches.h"
#include "shared_cache_memory.h"
#include "shared_cache_table.h"

#include <util/generic/function_ref.h>
#include <util/generic/noncopyable.h>

#include <new>
#include <type_traits>

namespace NKikimr::NSharedCache {

struct TSharedCacheHandleRequirement {
    TLogoBlobID CollectionId;
    TArrayRef<const NTable::NPage::TPageLocation> Pages;
};

template <class TTraits>
class TSharedCacheThreadBinding {
public:
    TSharedCacheThreadBinding() noexcept = default;
    TSharedCacheThreadBinding(const TSharedCacheThreadBinding&) = delete;
    TSharedCacheThreadBinding& operator=(const TSharedCacheThreadBinding&) = delete;
    TSharedCacheThreadBinding(TSharedCacheThreadBinding&& other) noexcept;
    TSharedCacheThreadBinding& operator=(TSharedCacheThreadBinding&& other) noexcept;
    ~TSharedCacheThreadBinding();

private:
    template <class>
    friend class TSharedCacheImpl;

    TSharedCacheThreadBinding(void* cache, TSpaceHazardBinding&& hazard) noexcept;

    void Reset() noexcept;

private:
    void* Cache_ = nullptr;
    TSpaceHazardBinding Hazard_;
};

template <class TTraits = TProdTraits>
class TSharedPageRefImpl : private TMoveOnly {
public:
    TSharedPageRefImpl() noexcept = default;
    TSharedPageRefImpl(TSharedPageRefImpl&& other) noexcept;
    TSharedPageRefImpl& operator=(TSharedPageRefImpl&& other) noexcept;

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    TPageCacheItem CacheItem() const noexcept;
    const char* data() const noexcept;
    size_t size() const noexcept;
    NActors::TSharedData BuildSharedData() const noexcept;
    // Construct an independent owner only when moving or borrowing cannot preserve the required lifetime.
    TSharedPageRefImpl Acquire() const& noexcept;
    NTable::NPage::TPageOffset GetOffset() const noexcept;
    NTable::NPage::TPageLocation GetLocation() const noexcept;
    TLogoBlobID GetCollectionId() const noexcept;
    NTable::NPage::EPage GetType() const noexcept;
    bool IsSticky() const noexcept;
    void Drop() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;

    explicit TSharedPageRefImpl(TSharedCacheItemRefImpl<TTraits>&& ref) noexcept;

    template <class TCallback>
    decltype(auto) WithHandle(TCallback&& callback) const noexcept;

private:
    TSharedCacheItemRefImpl<TTraits> Ref_;
};

template <class TTraits = TProdTraits>
class TSharedCacheCollectionRefImpl : private TMoveOnly {
public:
    TSharedCacheCollectionRefImpl() noexcept = default;
    TSharedCacheCollectionRefImpl(TSharedCacheCollectionRefImpl&& other) noexcept;
    TSharedCacheCollectionRefImpl& operator=(TSharedCacheCollectionRefImpl&& other) noexcept;

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    TCollectionCacheItem CacheItem() const noexcept;
    TCollection& GetCollection() const noexcept;
    TSharedCacheCollectionRefImpl Acquire() const& noexcept;
    bool IsSoleReference() const noexcept;

    TCollection* Get() const noexcept {
        return Ref_ ? &GetCollection() : nullptr;
    }

    TCollection* operator->() const noexcept {
        return &GetCollection();
    }

    TCollection& operator*() const noexcept {
        return GetCollection();
    }

    void Drop() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;

    explicit TSharedCacheCollectionRefImpl(TSharedCacheItemRefImpl<TTraits>&& ref) noexcept;

private:
    TSharedCacheItemRefImpl<TTraits> Ref_;
};

template <class TTraits = TProdTraits>
struct TSharedCachePageRequestImpl {
    NTable::NPage::TPageLocation Location;
    TIntrusivePtr<TPageFetchWaiter> Waiter;
    bool Sticky = false;

    TSharedCachePageRequestImpl(
        NTable::NPage::TPageLocation location, TIntrusivePtr<TPageFetchWaiter> waiter, bool sticky = false) noexcept
        : Location(std::move(location))
        , Waiter(std::move(waiter))
        , Sticky(sticky)
    {
        new (&Storage_.Page) TSharedPageRefImpl<TTraits>();
    }

    TSharedCachePageRequestImpl(TSharedCachePageRequestImpl&& other) noexcept
        : TSharedCachePageRequestImpl(std::move(other.Location), std::move(other.Waiter), other.Sticky)
    {
        MoveResult(other);
    }

    TSharedCachePageRequestImpl& operator=(TSharedCachePageRequestImpl&& other) noexcept {
        if (this != &other) {
            Location = std::move(other.Location);
            Waiter = std::move(other.Waiter);
            Sticky = other.Sticky;
            MoveResult(other);
        }
        return *this;
    }

    ~TSharedCachePageRequestImpl() {
        Destroy();
    }

    ESharedCacheResultStatus Status() const noexcept {
        return Status_;
    }

    TSharedPageRefImpl<TTraits>& Page() noexcept {
        Y_ABORT_UNLESS(Status_ == ESharedCacheResultStatus::Hit);
        return Storage_.Page;
    }

    const TSharedPageRefImpl<TTraits>& Page() const noexcept {
        Y_ABORT_UNLESS(Status_ == ESharedCacheResultStatus::Hit);
        return Storage_.Page;
    }

    TPageFetchImpl<TTraits>& Fetch() noexcept {
        Y_ABORT_UNLESS(Status_ == ESharedCacheResultStatus::Inserted);
        return Storage_.Fetch;
    }

    void SetHit(TSharedPageRefImpl<TTraits>&& page) noexcept {
        Destroy();
        new (&Storage_.Page) TSharedPageRefImpl<TTraits>(std::move(page));
        Status_ = ESharedCacheResultStatus::Hit;
    }

    void SetInserted(TPageFetchImpl<TTraits>&& fetch) noexcept {
        Destroy();
        new (&Storage_.Fetch) TPageFetchImpl<TTraits>(std::move(fetch));
        Status_ = ESharedCacheResultStatus::Inserted;
    }

    void SetPending() noexcept {
        Reset();
        Status_ = ESharedCacheResultStatus::Pending;
    }

    void Reset() noexcept {
        Destroy();
        new (&Storage_.Page) TSharedPageRefImpl<TTraits>();
        Status_ = ESharedCacheResultStatus::Miss;
    }

private:
    void Destroy() noexcept {
        if (Status_ == ESharedCacheResultStatus::Inserted) {
            Storage_.Fetch.~TPageFetchImpl<TTraits>();
        } else {
            Storage_.Page.~TSharedPageRefImpl<TTraits>();
        }
    }

    void MoveResult(TSharedCachePageRequestImpl& other) noexcept {
        Destroy();
        if (other.Status_ == ESharedCacheResultStatus::Inserted) {
            new (&Storage_.Fetch) TPageFetchImpl<TTraits>(std::move(other.Storage_.Fetch));
        } else {
            new (&Storage_.Page) TSharedPageRefImpl<TTraits>(std::move(other.Storage_.Page));
        }
        Status_ = other.Status_;
    }

    // Fetch is active only for Inserted; Page is active for every other status.
    ESharedCacheResultStatus Status_ = ESharedCacheResultStatus::Miss;

    union TStorage {
        TStorage() noexcept {}

        ~TStorage() {}

        TSharedPageRefImpl<TTraits> Page;
        TPageFetchImpl<TTraits> Fetch;
    } Storage_;
};

class TSharedCacheTestAccess;
class TRequestPageWaiter;
class TCompactedPageWaiter;

template <class TTraits = TProdTraits>
class TSharedCacheImpl : public TThrRefBase, private TTraits {
public:
    static constexpr ui64 ExpectedPageSize = 7 * 1024;

    static TIntrusivePtr<TSharedCacheImpl> Create(
        THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits = {});

    ~TSharedCacheImpl() override;

    TSharedCacheImpl(const TSharedCacheImpl&) = delete;
    TSharedCacheImpl& operator=(const TSharedCacheImpl&) = delete;

    class TOperation;

    TOperation BeginOperation() noexcept;

    static TSharedCacheImpl& Get() noexcept {
        TSharedCacheImpl* cache = TryGet();
        Y_DEBUG_ABORT_UNLESS(cache);
        return *cache;
    }

    TSharedCacheCollectionRefImpl<TTraits> AdmitCollection(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection)
        requires std::is_same_v<TTraits, TProdTraits>;

    TSharedPageRefImpl<TTraits> AdmitPage(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        NTable::NPage::TPageLocation location, NActors::TSharedData&& data, bool sticky = false)
        requires std::is_same_v<TTraits, TProdTraits>;

    static TSharedCacheImpl* TryGet() noexcept {
        return static_cast<TSharedCacheImpl*>(TTraits::TryGetCache());
    }

    TSharedCacheThreadBinding<TTraits> BindThreadHazard(ui32 index) const noexcept {
        return { const_cast<TSharedCacheImpl*>(this), Space_->BindThreadHazard(index) };
    }

    TSharedCacheThreadBinding<TTraits> BindCurrentThreadHazard() const noexcept {
        if (Space_->ThreadHazardBound()) {
            return {};
        }
        const ui32 worker = TTraits::CurrentWorkerIndex();
        if (worker != Max<ui32>()) {
            return BindThreadHazard(worker);
        }
        return { const_cast<TSharedCacheImpl*>(this), Space_->BindAnyThreadHazard() };
    }

    bool MakeReady(TPageCacheItem page, NActors::TSharedData&& data) noexcept;

    bool FailReady(TPageCacheItem page) noexcept;

    bool MakeReady(TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept;

    bool DetachCollection(TCollectionCacheItem collection) noexcept;

    bool DeleteCollection(TCollectionCacheItem collection) noexcept;

    bool SetCollectionPagesCacheMode(TCollectionCacheItem collection, ECacheMode mode) noexcept;
    bool SetCollectionSkipBTreeIndexV1Shadow(TCollectionCacheItem collection, bool skip) noexcept;
    bool AcknowledgeCollectionBytes(TCollectionCacheItem collection) noexcept;

    ESharedCacheResultStatus Find(
        TCollectionCacheItem collection, ui64 offset, TSharedPageRefImpl<TTraits>& page) noexcept;

    ESharedCacheResultStatus Find(const TLogoBlobID& id, TSharedCacheCollectionRefImpl<TTraits>& collection) noexcept;

    // The callback may detach the current collection. Registry mutations remain serialized by its owner.
    void ForEachCollection(TFunctionRef<void(TCollection&)> callback) noexcept;

    bool FindOrInsertBatch(TCollectionCacheItem collection, TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests,
        bool recordStats = true, bool waitForMemory = false) noexcept;

    ESharedCacheResultStatus FindOrInsert(const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TSharedCacheCollectionRefImpl<TTraits>& hit) noexcept;

    bool ReclaimCold() noexcept;

    bool UpdateCurrentLimit(ui64 limit) noexcept;
    // Max<ui64>() restores the calculated default; zero is an explicit target.
    bool UpdateSoftLimit(ui64 limit) noexcept;
    bool UpdateHardLimit(ui64 limit) noexcept;

    ui64 ResourceGeneration() const noexcept {
        return Memory_.ResourceGeneration();
    }

    void ArmResourceWait(ui64 generation) noexcept;
    bool TryReserveFetchBatch(TArrayRef<TPageFetchImpl<TTraits>> pages) noexcept;
    bool CanAdmitWorkingSet(ui64 bytes) noexcept;
    bool CanAdmitPageHandles(TArrayRef<const TSharedCacheHandleRequirement> requirements) noexcept;
    // Pressure-only check of request-held collection handles and distinct requested pages.
    bool WorkingSetExceedsCapacity(TArrayRef<const TCollectionCacheItem> retainedCollections,
        TArrayRef<const TSharedCacheHandleRequirement> requirements) noexcept;

    void DisarmResourceWait() noexcept {
        Memory_.DisarmResourceWait();
    }

    bool EnforceCurrentLimit() noexcept;
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
        return HardTransition_.Phase() != ETransitionPhase::Idle || PhysicalHandleCount() != TargetHandleCount();
    }

    ui64 TargetHandleCount() const noexcept {
        return TargetHandleCount_.load(std::memory_order_relaxed);
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
        return Memory_.Hot().Bytes();
    }

    ui64 HotPages() const noexcept {
        return Memory_.Hot().Pages();
    }

    ui64 ColdPages() const noexcept {
        return Memory_.Cold().Pages();
    }

    ui64 ColdRingEntries() const noexcept {
        return Memory_.ColdRingEntries();
    }

    ui64 StickyPages() const noexcept {
        return Memory_.Sticky().Pages();
    }

    ui64 KeepColdPages() const noexcept {
        return Memory_.KeepCold().Pages();
    }

    ui64 Collections() const noexcept {
        return Memory_.Collections();
    }

    ui64 ColdBytes() const noexcept {
        return Memory_.Cold().Bytes();
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
        return Memory_.KeepCold().Bytes();
    }

    ui64 KeepResidentPageBytes() const noexcept {
        return Memory_.KeepResidentPageBytes();
    }

    ui64 KeepActivePageBytes() const noexcept {
        return Memory_.KeepActivePageBytes();
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

    bool UpdateKeepColdLimit(ui64 limit) noexcept;

    ui64 EvictableByteBudget() const noexcept {
        const ui64 softLimit = SoftLimit();
        return softLimit - Min(softLimit, StickyBytes());
    }

    ui64 KeepColdLimit() const noexcept {
        const ui64 budget = EvictableByteBudget();
        // KeepCold uses the remainder after the normal Hot minimum (40% with HotMin = 60%).
        const ui64 maximum = budget - FractionCeil(budget, Policy_.HotMin);
        return Min(Min(KeepColdLimit_.load(std::memory_order_relaxed),
                       KeepColdMaxBytes_.load(std::memory_order_relaxed)), maximum);
    }

    ui64 KeepColdOwnedBytes() const noexcept {
        return Memory_.KeepColdOwnedBytes();
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
    class TOperationView;

    TOperationView BindOperation(TSpaceOperation& spaceOp) noexcept;

    friend class TSharedCacheTestAccess;
    friend class TCollection;
    friend class TSharedCacheItemRefImpl<TTraits>;
    friend class TSharedCacheTableImpl<TTraits>;
    friend class TSharedPageRefImpl<TTraits>;
    friend class TSharedCacheCollectionRefImpl<TTraits>;
    friend class TCacheFetchServices<TTraits>;
    friend class TRequestPageWaiter;
    friend class TCompactedPageWaiter;
    friend struct TEvSaveCompactedPages;

    Y_FORCE_INLINE void InvokeHook(ESharedCacheHookPoint point, TCacheItem cacheItem) const noexcept {
        TTraits::Invoke(point, cacheItem);
    }

    Y_FORCE_INLINE void InvokeHook(ESharedCacheHookPoint point, TColdCacheItem coldItem) const noexcept {
        // Scheduling hooks transport raw token bits; the production APIs keep their distinct types.
        TTraits::Invoke(point, TCacheItem::FromRaw(coldItem.Raw()));
    }

    void SetHooks(TTraits hooks) noexcept {
        static_cast<TTraits&>(*this) = std::move(hooks);
    }

    explicit TSharedCacheImpl(THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits) noexcept;

    enum class EAcquireStatus {
        Stale,
        Pending,
        Ready,
    };

    enum class EHotLevel : ui32 {
        L0,
        L1,
        L2,
        L3,
    };

    enum class EShrinkCutStatus {
        Blocked,
        Progress,
        Complete,
    };

    enum class EPageAdmission {
        Bounded,
        Written,
    };

    struct TPageInsertCandidate {
        TSharedCacheKey Key;
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
        TSharedCacheKey Key;
        ui64 Bytes = 0;
        TCacheItem CacheItem;
    };

    struct TCacheKeyedItem {
        TSharedCacheKey Key;
        TCacheItem CacheItem;
    };

    TPageCacheItem AllocatePage(TCollectionCacheItem collection, ui64 offset, ui64 size, NTable::NPage::EPage type,
        ui32 crc32, EStickyState sticky) noexcept;

    TCollectionCacheItem AllocateCollection(const TLogoBlobID& id, ui64 bytes) noexcept;

    TCacheItem Allocate(TSpaceOperation& spaceOp) noexcept;
    TCacheItem TryAllocateReservedItem(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages = 0) noexcept;

    ui32 TryAllocateHandle(TSpaceOperation& spaceOp) noexcept;

    void RefillSpareItem(TSpaceOperation& spaceOp) noexcept;

    void RecycleCandidate(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    bool MakeReady(
        TCollectionRegistry& registry, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept;

    bool MakeReady(
        TCollectionRegistry* registry, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept;

    bool MakeReady(TSpaceOperation& spaceOp, TCollectionRegistry* registry, TCacheItem cacheItem,
        THolder<TCollection>&& value) noexcept;

    void MakeReadyState(TSpaceOperation& spaceOp, TCacheItem cacheItem, EHandleState readyState = EHandleState::Hot,
        TCollection* pageOwner = nullptr) noexcept;

    ESharedCacheResultStatus Find(
        TOperationView& cacheOp, const TSharedCacheKey& key, TSharedCacheItemRefImpl<TTraits>& ref) noexcept;

    ESharedCacheResultStatus FindOrInsert(
        TSpaceOperation& spaceOp, TCacheItem candidate, TSharedCacheItemRefImpl<TTraits>& ref) noexcept;

    static TPageInsertCandidate BuildPageInsertCandidate(
        TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page, EStickyState sticky) noexcept;

    ESharedCacheResultStatus FindOrInsert(TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page,
        EStickyState sticky, TPageCacheItem& inserted, TSharedPageRefImpl<TTraits>& hit) noexcept;
    ESharedCacheResultStatus FindOrInsert(TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page,
        EStickyState sticky, TIntrusivePtr<TPageFetchWaiter> waiter, TSharedPageRefImpl<TTraits>& hit,
        TPageFetchImpl<TTraits>& fetch) noexcept;
    ESharedCacheResultStatus FindOrInsert(TCollectionRegistry& registry, const TCollectionLocation& collection,
        TCollectionCacheItem& inserted, TSharedCacheCollectionRefImpl<TTraits>& hit) noexcept;
    ESharedCacheResultStatus FindOrInsertUnregistered(const TCollectionLocation& collection,
        TCollectionCacheItem& inserted, TSharedCacheCollectionRefImpl<TTraits>& hit) noexcept;

    ESharedCacheResultStatus FindOrInsertPage(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate,
        TIntrusivePtr<TPageFetchWaiter> waiter, TSharedPageRefImpl<TTraits>& hit,
        TPageFetchImpl<TTraits>& fetch) noexcept;
    bool PreparePageBatch(TSpaceOperation& spaceOp, TCollection& collection,
        TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests, TVector<TPageInsertCandidate>& candidates,
        ui64& reservedBytes) noexcept;
    bool AllocatePageBatch(TSpaceOperation& spaceOp, TCollection& collection, TVector<TPageInsertCandidate>& candidates,
        ui64 reservedBytes, bool waitForMemory, EPageAdmission admission) noexcept;
    bool FindOrInsertWrittenBatch(
        TCollectionCacheItem collection, TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests) noexcept;
    bool FindOrInsertBatch(TCollectionCacheItem collection, TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests,
        bool recordStats, bool waitForMemory, EPageAdmission admission) noexcept;
    ESharedCacheResultStatus FindOrInsertCollection(TSpaceOperation& spaceOp, TCollectionRegistry* registry,
        const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TSharedCacheCollectionRefImpl<TTraits>& hit) noexcept;

    template <class TKeyedCandidate>
    ESharedCacheResultStatus FindOrInsert(
        TSpaceOperation& spaceOp, TKeyedCandidate& candidate, TSharedCacheItemRefImpl<TTraits>& ref) noexcept;

    bool PrepareCandidate(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate) noexcept;
    bool PrepareCandidate(TSpaceOperation& spaceOp, TCollectionInsertCandidate& candidate) noexcept;

    bool PrepareCandidate(TSpaceOperation&, TCacheKeyedItem& candidate) noexcept {
        return !candidate.CacheItem.IsNull();
    }

    bool EraseCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept;

    bool ReclaimCold(TSpaceOperation& spaceOp) noexcept;
    bool DemoteKeepColdForSoftLimit(TSpaceOperation& spaceOp) noexcept;
    // Cooperative reclamation: takes a cold entry and, when cold is empty, lets EnforceCurrentLimit move the
    // hot/cold boundary so that the coldest hot entries become reclaimable.
    bool Reclaim(TSpaceOperation& spaceOp, ui64 bytes = 0) noexcept;

    bool AdvanceBucketResize() noexcept;

    bool TryReserve(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages = 0) noexcept;
    void NotifyResourcesAvailable() noexcept;
    void ReleaseReservation(ui64 bytes, ui64 pages = 0) noexcept;

    bool PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept;
    bool PublishMigrationView(TTransition& transition) noexcept;
    bool TryDrainTransition(TTransition& transition) noexcept;
    bool PublishFinalView(TTransition& transition) noexcept;
    bool AppendFreeHandles(TTransition& transition) noexcept;
    bool FinalDrain(TTransition& transition) noexcept;
    bool TryReleaseTransition(TTransition& transition) noexcept;
    bool CommitTransition(TTransition& transition) noexcept;
    bool AbandonTransition(TTransition& transition) noexcept;

    void DiscardCandidate(TPageCacheItem page) noexcept;

    void DiscardCandidate(TCollectionCacheItem collection) noexcept;

    void DiscardCandidate(TCacheItem cacheItem, EItemKind kind) noexcept;

    void DiscardCandidate(
        TSpaceOperation& spaceOp, TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes = 0) noexcept;

    void InitializePage(TSpaceOperation& spaceOp, TCacheItem cacheItem, TCollectionCacheItem collection, ui64 offset,
        ui64 size, NTable::NPage::EPage type, ui32 crc32, EStickyState sticky, bool reserved = true,
        bool collectionOwnerHeld = false) noexcept;

    void InitializeCollection(
        TSpaceOperation& spaceOp, TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept;

    EAcquireStatus TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowPending,
        TSharedCacheItemRefImpl<TTraits>& ref) noexcept;

    TSharedCacheItemRefImpl<TTraits> TryAcquireStructural(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    TSharedCacheItemRefImpl<TTraits> BuildExternalRef(EItemKind kind, TCacheItem cacheItem) noexcept;

    TSharedCacheItemRefImpl<TTraits> AcquireStickyCollection(
        TSpaceOperation& spaceOp, TCollectionCacheItem collection, TCollection*& value) noexcept;

    bool LinkStickyPage(
        TSpaceOperation& spaceOp, TCacheItem page, TSharedCacheItemRefImpl<TTraits>& collectionRef) noexcept;
    void UnlinkPendingStickyPage(TSpaceOperation& spaceOp, TCacheItem page) noexcept;
    void RestoreStickyPageList(TSpaceOperation& spaceOp, TCollection& collection, ui32 detachedHead) noexcept;
    void DrainStickyPages(TSpaceOperation& spaceOp, TCollection& collection) noexcept;
    static void PublishPageUnsticky(THandle& page, THandleState state) noexcept;
    void MergeRetainedStickyPages(
        TSpaceOperation& spaceOp, TCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept;
    bool SetCollectionPagesCacheMode(TSpaceOperation& spaceOp, TCollection& collection, ECacheMode mode) noexcept;
    bool MakePageSticky(TSpaceOperation& spaceOp, TCacheItem page) noexcept;
    bool UnstickyCutPages(TSpaceOperation& spaceOp, TCacheItem page, ui64 allocationLimit) noexcept;

    bool LinkCollection(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection) noexcept;
    bool TryBeginCollectionAttach(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection,
        THandleState& attachState) noexcept;
    void FinishCollectionAttach(TSpaceOperation& spaceOp, TCacheItem collection, EHandleState detachedState) noexcept;
    bool AttachCollection(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection) noexcept;
    bool TryBeginCollectionDetach(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection) noexcept;
    void PublishCollectionForReclaim(TSpaceOperation& spaceOp, TCacheItem collection) noexcept;
    void FinishCollectionRetain(TSpaceOperation& spaceOp, TCacheItem collection) noexcept;
    bool DetachCollection(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection) noexcept;
    bool DetachCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept;
    bool UnlinkCollectionRegistry(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept;
    bool DeleteCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept;
    void ForEachCollection(TCollectionRegistry& registry, TFunctionRef<void(TCollection&)> callback) noexcept;
    bool UnlinkCollectionRegistry(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection) noexcept;
    bool TryClaimCollectionDeletion(TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry,
        TCacheItem collection, THandleState& sourceState, TSharedCacheItemRefImpl<TTraits>& owner) noexcept;
    bool DeleteCollection(TCollectionRegistry* registry, TCollectionCacheItem collection) noexcept;
    bool DeleteCollection(
        TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry, TCacheItem collection) noexcept;
    bool MoveCollectionRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& source, TCollectionRegistry& destination,
        TCacheItem collection) noexcept;
    bool UnlinkCollectionRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection,
        EHandleState expectedState) noexcept;
    void UnlinkCollectionRegistry(TCollectionRegistry& registry, TOperationItemRef<TTraits>& collection) noexcept;

    bool IsPageSticky(TPageCacheItem page) const noexcept;
    TSharedPageRefImpl<TTraits> BuildPageRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;
    TSharedPageRefImpl<TTraits> AcquireHeldPage(TCacheItem item) noexcept;
    TSharedCacheCollectionRefImpl<TTraits> AcquireHeldCollection(TCacheItem item) noexcept;

    bool AcquirePage(TPageCacheItem page, TSharedPageRefImpl<TTraits>& result) noexcept;

    TSharedCacheCollectionRefImpl<TTraits> BuildCollectionRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;

    void Release(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    TCollection* ReleaseTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;
    TCollection* ReleaseItem(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowKeepCold = true) noexcept;
    void CompleteColdPageRelease(TOperationItemRef<TTraits>& ownerRef) noexcept;
    bool TryReleaseColdPageToKeepCold(TSpaceOperation& spaceOp, TCacheItem cacheItem, ui64& expectedRaw) noexcept;
    bool TryReleaseItemRef(TSpaceOperation& spaceOp, TCacheItem cacheItem, ui64& expectedRaw) noexcept;

    void ReleaseReplacing(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void ReleaseReplaced(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void DeletePayload(THandle& handle, EItemKind kind, EHandleState state, TCollection* owner = nullptr) noexcept;

    static void DeleteReplacedPage(THandle& handle) noexcept;

    bool MatchesCandidateMetadata(
        const TSpaceOperation& spaceOp, TCacheItem candidate, TCacheItem existing) const noexcept;
    bool MatchesCandidateMetadata(
        const TSpaceOperation& spaceOp, const TPageInsertCandidate& candidate, TCacheItem existing) const noexcept;

    bool MatchesCandidateMetadata(const TSpaceOperation& spaceOp, const TCollectionInsertCandidate& candidate,
        TCacheItem existing) const noexcept;

    bool MatchesCandidateMetadata(
        const TSpaceOperation& spaceOp, const TCacheKeyedItem& candidate, TCacheItem existing) const noexcept {
        return MatchesCandidateMetadata(spaceOp, candidate.CacheItem, existing);
    }

    static TCollection* PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    static TCollection* PageCollection(TSpaceOperation& spaceOp, TCollectionCacheItem collection) noexcept;
    static void AddReference(TCollection& owner) noexcept;
    void DropPageItemRef(TSpaceOperation& spaceOp, THandle& page) noexcept;
    static bool DropReference(TCollection& owner) noexcept;
    void AddResidentPageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept;
    void AddActivePageBytes(TCollection& owner, NTable::NPage::EPage type, i64 bytes) noexcept;
    void PublishKeepPageBytes(TCollection& owner) noexcept;
    void SetCollectionSkipBTreeIndexV1Shadow(TCollection& owner, bool skip) noexcept;
    void DropOwnerReference(TSpaceOperation& spaceOp, TCollection& owner) noexcept;
    static ui64 PayloadBytes(const THandle& handle, EItemKind kind) noexcept;
    static ui64 AccountedPageBytes(ui64 size) noexcept;
    static ui64 PageBytes(const THandle& handle) noexcept;
    static ui64 FractionCeil(ui64 value, double fraction) noexcept;
    static void SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept;

    bool PromoteCold(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool force = false) noexcept;
    bool ReclaimKeepCold(TSpaceOperation& spaceOp) noexcept;

    bool TrimKeepCold(TSpaceOperation& spaceOp) noexcept;
    bool ShrinkKeepCold(TSpaceOperation& spaceOp) noexcept;
    bool GrowKeepCold() noexcept;

    bool WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    static bool WantsStickyPage(const TCollection& collection) noexcept;

    void RouteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept;

    bool DemoteKeepCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem, TPageCacheItem expectedPage = {}) noexcept;
    void ReleaseColdDemotionPin(TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& pin) noexcept;
    void ReleaseKeepColdPin(
        TSpaceOperation& spaceOp, TOperationItemRef<TTraits>& pin, ui32 initialColdVersion) noexcept;

    void WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept;

    static bool IsKeepColdQueued(const THandle& handle) noexcept;

    bool MatchesKeepColdEntry(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept;

    bool EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept;

    bool EvictFromHot(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void UpdateColdReclaimable(ui64 payloadBytes, THandleState before, THandleState after) noexcept;

    static ui32 AdvanceColdMembership(THandle& handle) noexcept;

    static bool AdvanceColdMembership(
        THandle& handle, ui32 expectedVersion, bool keepColdQueued, ui32& newVersion) noexcept;

    void RouteCold(TSpaceOperation& spaceOp, TColdCacheItem coldItem) noexcept;

    TCacheItem PutHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept;

    void RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept;

    TSharedCacheItemRefImpl<TTraits> TryAcquireHotEvictionPin(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;
    static Y_FORCE_INLINE bool TakeHotFrequency(
        THandle& handle, TCacheItem cacheItem, THandleState& state, ui8& frequency) noexcept;
    Y_FORCE_INLINE void CompleteHotEviction(TSpaceOperation& spaceOp, THandle& handle, THandleState coldState,
        ui64 bytes, TOperationItemRef<TTraits>& pin, ui32 initialColdVersion) noexcept;

    void ProcessHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept;

    bool ProcessHotAtMinimum(TSpaceOperation& spaceOp) noexcept;

    bool DrainHotResize() noexcept;
    bool DrainHotResize(TSpaceOperation& spaceOp) noexcept;
    ui32 MinimumHotSlots(const TSpaceOperation& spaceOp, bool underPressure = false) const noexcept;
    ui32 ShrinkHotTarget(
        const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step, bool underPressure) const noexcept;
    ui32 GrowHotTarget(const TSpaceOperation& spaceOp, ui32 effectiveHotSlots, ui32 step) const noexcept;
    ui32 HotResizeStep(ui32 effectiveHotSlots) const noexcept;
    void RefreshSoftLimit() noexcept;
    void RefreshEvictableByteBudget() noexcept;
    bool EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes = 0, bool forceShrinkHot = false) noexcept;
    bool ReclaimToLimits(ui64 softLimit, ui32 step, TSpaceOperation& spaceOp, ui64 bytes = 0) noexcept;

    bool StartHardTransition() noexcept;
    TSharedCacheItemRefImpl<TTraits> TryAcquirePageForRelocation(TSpaceOperation& spaceOp, ui32 index) noexcept;
    TCacheItem PreparePageReplacement(TSpaceOperation& spaceOp, TCacheItem source) noexcept;
    void DiscardPageReplacement(TSpaceOperation& spaceOp, TCacheItem replacement) noexcept;
    bool PublishPageReplacement(
        TSpaceOperation& spaceOp, TCacheItem source, TCacheItem replacement, EHandleState& oldState) noexcept;
    void CompletePageReplacement(TSpaceOperation& spaceOp, TCacheItem source) noexcept;
    bool RelocatePage(TSpaceOperation& spaceOp, ui32 index) noexcept;
    bool DemoteUnlinkedStickyPage(TSpaceOperation& spaceOp, ui32 index) noexcept;
    EShrinkCutStatus ReclaimShrinkHandle(TSpaceOperation& spaceOp, ui32 index) noexcept;
    EShrinkCutStatus ReclaimShrinkCut() noexcept;

private:
    THolder<TSharedCacheSpace> Space_;
    TSharedCacheTableImpl<TTraits> Table_;
    TIntrusivePtr<TCollectionRegistry> CollectionRegistry_ = new TCollectionRegistry;
    TCacheMemory Memory_;
    TCacheFetches<TTraits> Fetches_;
    ui64 ColdByteBudget_ = Max<ui64>(); // Protected by HotResize_; watermark calculation input.
    std::atomic<ui64> KeepColdMaxBytes_{ 0 }; // Configured ceiling; repeated updates preserve pressure shrinkage.
    std::atomic<ui64> KeepColdLimit_{ 0 }; // Effective budget, resized according to Cold+Free headroom.
    std::atomic<ui64> KeepColdTrimScanRemaining_{ 0 };
    std::atomic<ui64> KeepModeSweepRemaining_{ 0 };
    std::atomic<ui64> ColdMinBytes_{ 0 };
    std::atomic<ui64> ColdTargetBytes_{ 0 };
    std::atomic<ui64> ColdGrowBytes_{ 0 };
    std::atomic<ui64> TargetHandleCount_{ 0 };
    std::atomic<bool> KeepColdPressure_{ false };
    std::atomic<bool> KeepColdTrimActive_{ false };
    std::atomic<bool> SoftLimitPressure_{ false };
    THotResize HotResize_;
    TSharedCacheCapacity HardTarget_;
    TTransition HardTransition_;

    inline static constexpr TSharedCachePolicy Policy_ = SharedCachePolicyFor<TTraits>;
};

template <class TTraits>
class TSharedCacheImpl<TTraits>::TOperationView {
public: // Cache table operations
    using TInsertPosition = typename TSharedCacheTableImpl<TTraits>::TInsertPosition;
    using EAcquireStatus = typename TSharedCacheImpl<TTraits>::EAcquireStatus;

    Y_FORCE_INLINE TSpaceOperation& SpaceOperation() const noexcept {
        return SpaceOp_;
    }

    Y_FORCE_INLINE ESharedCacheResultStatus Find(const TSharedCacheKey& key, TCacheItem& item) noexcept {
        return Cache_.Table_.Find(SpaceOp_, key, item);
    }

    Y_FORCE_INLINE ESharedCacheResultStatus Find(
        const TSharedCacheKey& key, TSharedCacheItemRefImpl<TTraits>& ref) noexcept {
        return Cache_.Find(*this, key, ref);
    }

    Y_FORCE_INLINE ESharedCacheResultStatus FindForInsert(
        const TSharedCacheKey& key, TCacheItem& item, TInsertPosition& position) noexcept {
        return Cache_.Table_.FindForInsert(SpaceOp_, key, item, position);
    }

    Y_FORCE_INLINE bool InsertAt(TInsertPosition& position, TCacheItem candidate) noexcept {
        return Cache_.Table_.InsertAt(SpaceOp_, position, candidate);
    }

    Y_FORCE_INLINE void DropInsertOwner(TInsertPosition& position) noexcept {
        position.DropOwner(SpaceOp_);
    }

    Y_FORCE_INLINE bool TryBeginErase(
        TCacheItem item, THandleState& expected, TSharedCacheItemRefImpl<TTraits>& owner) noexcept {
        return Cache_.Table_.TryBeginErase(SpaceOp_, item, expected, owner);
    }

    Y_FORCE_INLINE bool TryBeginErase(TSharedCacheItemRefImpl<TTraits>& owner, THandleState& expected) noexcept {
        return Cache_.Table_.TryBeginErase(SpaceOp_, owner, expected);
    }

    Y_FORCE_INLINE void FinishErase(TCacheItem item, const TSharedCacheKey& key) noexcept {
        Cache_.Table_.FinishErase(SpaceOp_, item, key);
    }

public: // Cache helpers
    Y_FORCE_INLINE EAcquireStatus TryAcquire(
        TCacheItem item, bool allowPending, TSharedCacheItemRefImpl<TTraits>& ref) noexcept {
        return Cache_.TryAcquire(SpaceOp_, item, allowPending, ref);
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> TryAcquireStructural(TCacheItem item) noexcept {
        return Cache_.TryAcquireStructural(SpaceOp_, item);
    }

    Y_FORCE_INLINE void Release(TCacheItem item) noexcept {
        Cache_.Release(SpaceOp_, item);
    }

    Y_FORCE_INLINE typename TCacheFetches<TTraits>::TOperation Fetches() noexcept {
        return Cache_.Fetches_.BindOperation(SpaceOp_);
    }

    Y_FORCE_INLINE void DrainStickyPages(TCollection& collection) noexcept {
        Cache_.DrainStickyPages(SpaceOp_, collection);
    }

    Y_FORCE_INLINE bool EnforceCurrentLimit(ui64 bytes = 0, bool forceShrinkHot = false) noexcept {
        return Cache_.EnforceCurrentLimit(SpaceOp_, bytes, forceShrinkHot);
    }

    Y_FORCE_INLINE TCacheItem Allocate() noexcept {
        return Cache_.Allocate(SpaceOp_);
    }

    Y_FORCE_INLINE TCacheItem TryAllocateReservedItem(ui64 bytes, ui64 pages = 0) noexcept {
        return Cache_.TryAllocateReservedItem(SpaceOp_, bytes, pages);
    }

    Y_FORCE_INLINE ui32 TryAllocateHandle() noexcept {
        return Cache_.TryAllocateHandle(SpaceOp_);
    }

    Y_FORCE_INLINE void RefillSpareItem() noexcept {
        Cache_.RefillSpareItem(SpaceOp_);
    }

    Y_FORCE_INLINE void RecycleCandidate(TCacheItem cacheItem) noexcept {
        Cache_.RecycleCandidate(SpaceOp_, cacheItem);
    }

    Y_FORCE_INLINE bool MakeReady(
        TCollectionRegistry* registry, TCacheItem cacheItem, THolder<TCollection>&& value) noexcept {
        return Cache_.MakeReady(SpaceOp_, registry, cacheItem, std::move(value));
    }

    Y_FORCE_INLINE void MakeReadyState(
        TCacheItem cacheItem, EHandleState readyState = EHandleState::Hot, TCollection* pageOwner = nullptr) noexcept {
        Cache_.MakeReadyState(SpaceOp_, cacheItem, readyState, pageOwner);
    }

    Y_FORCE_INLINE ESharedCacheResultStatus FindOrInsert(
        TCacheItem candidate, TSharedCacheItemRefImpl<TTraits>& ref) noexcept {
        return Cache_.FindOrInsert(SpaceOp_, candidate, ref);
    }

    Y_FORCE_INLINE ESharedCacheResultStatus FindOrInsertPage(TPageInsertCandidate& candidate,
        TIntrusivePtr<TPageFetchWaiter> waiter, TSharedPageRefImpl<TTraits>& hit,
        TPageFetchImpl<TTraits>& fetch) noexcept {
        return Cache_.FindOrInsertPage(SpaceOp_, candidate, std::move(waiter), hit, fetch);
    }

    Y_FORCE_INLINE bool PreparePageBatch(TCollection& collection,
        TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests, TVector<TPageInsertCandidate>& candidates,
        ui64& reservedBytes) noexcept {
        return Cache_.PreparePageBatch(SpaceOp_, collection, requests, candidates, reservedBytes);
    }

    Y_FORCE_INLINE bool AllocatePageBatch(TCollection& collection, TVector<TPageInsertCandidate>& candidates,
        ui64 reservedBytes, bool waitForMemory, EPageAdmission admission) noexcept {
        return Cache_.AllocatePageBatch(SpaceOp_, collection, candidates, reservedBytes, waitForMemory, admission);
    }

    Y_FORCE_INLINE ESharedCacheResultStatus FindOrInsertCollection(TCollectionRegistry* registry,
        const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TSharedCacheCollectionRefImpl<TTraits>& hit) noexcept {
        return Cache_.FindOrInsertCollection(SpaceOp_, registry, collection, inserted, hit);
    }

    template <class TKeyedCandidate>
    Y_FORCE_INLINE ESharedCacheResultStatus FindOrInsert(
        TKeyedCandidate& candidate, TSharedCacheItemRefImpl<TTraits>& ref) noexcept {
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

    Y_FORCE_INLINE bool EraseCold(TColdCacheItem coldItem) noexcept {
        return Cache_.EraseCold(SpaceOp_, coldItem);
    }

    Y_FORCE_INLINE bool ReclaimCold() noexcept {
        return Cache_.ReclaimCold(SpaceOp_);
    }

    Y_FORCE_INLINE bool DemoteKeepColdForSoftLimit() noexcept {
        return Cache_.DemoteKeepColdForSoftLimit(SpaceOp_);
    }

    Y_FORCE_INLINE bool Reclaim(ui64 bytes = 0) noexcept {
        return Cache_.Reclaim(SpaceOp_, bytes);
    }

    Y_FORCE_INLINE bool TryReserve(ui64 bytes, ui64 pages = 0) noexcept {
        return Cache_.TryReserve(SpaceOp_, bytes, pages);
    }

    Y_FORCE_INLINE void DiscardCandidate(TCacheItem cacheItem, EItemKind kind, ui64 reservedBytes = 0) noexcept {
        Cache_.DiscardCandidate(SpaceOp_, cacheItem, kind, reservedBytes);
    }

    Y_FORCE_INLINE void InitializePage(TCacheItem cacheItem, TCollectionCacheItem collection, ui64 offset, ui64 size,
        NTable::NPage::EPage type, ui32 crc32, EStickyState sticky, bool reserved = true,
        bool collectionOwnerHeld = false) noexcept {
        Cache_.InitializePage(
            SpaceOp_, cacheItem, collection, offset, size, type, crc32, sticky, reserved, collectionOwnerHeld);
    }

    Y_FORCE_INLINE void InitializeCollection(TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept {
        Cache_.InitializeCollection(SpaceOp_, cacheItem, id, bytes);
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> AcquireStickyCollection(
        TCollectionCacheItem collection, TCollection*& value) noexcept {
        return Cache_.AcquireStickyCollection(SpaceOp_, collection, value);
    }

    Y_FORCE_INLINE bool LinkStickyPage(TCacheItem page, TSharedCacheItemRefImpl<TTraits>& collectionRef) noexcept {
        return Cache_.LinkStickyPage(SpaceOp_, page, collectionRef);
    }

    Y_FORCE_INLINE void UnlinkPendingStickyPage(TCacheItem page) noexcept {
        Cache_.UnlinkPendingStickyPage(SpaceOp_, page);
    }

    Y_FORCE_INLINE void RestoreStickyPageList(TCollection& collection, ui32 detachedHead) noexcept {
        Cache_.RestoreStickyPageList(SpaceOp_, collection, detachedHead);
    }

    Y_FORCE_INLINE void MergeRetainedStickyPages(
        TCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept {
        Cache_.MergeRetainedStickyPages(SpaceOp_, collection, retainedHead, retainedTail);
    }

    Y_FORCE_INLINE bool SetCollectionPagesCacheMode(TCollection& collection, ECacheMode mode) noexcept {
        return Cache_.SetCollectionPagesCacheMode(SpaceOp_, collection, mode);
    }

    Y_FORCE_INLINE bool MakePageSticky(TCacheItem page) noexcept {
        return Cache_.MakePageSticky(SpaceOp_, page);
    }

    Y_FORCE_INLINE bool UnstickyCutPages(TCacheItem page, ui64 allocationLimit) noexcept {
        return Cache_.UnstickyCutPages(SpaceOp_, page, allocationLimit);
    }

    Y_FORCE_INLINE bool LinkCollection(TCollectionRegistry& registry, TCacheItem collection) noexcept {
        return Cache_.LinkCollection(SpaceOp_, registry, collection);
    }

    Y_FORCE_INLINE bool TryBeginCollectionAttach(
        TCollectionRegistry& registry, TCacheItem collection, THandleState& attachState) noexcept {
        return Cache_.TryBeginCollectionAttach(SpaceOp_, registry, collection, attachState);
    }

    Y_FORCE_INLINE void FinishCollectionAttach(TCacheItem collection, EHandleState detachedState) noexcept {
        Cache_.FinishCollectionAttach(SpaceOp_, collection, detachedState);
    }

    Y_FORCE_INLINE bool AttachCollection(TCollectionRegistry& registry, TCacheItem collection) noexcept {
        return Cache_.AttachCollection(SpaceOp_, registry, collection);
    }

    Y_FORCE_INLINE bool TryBeginCollectionDetach(TCollectionRegistry& registry, TCacheItem collection) noexcept {
        return Cache_.TryBeginCollectionDetach(SpaceOp_, registry, collection);
    }

    Y_FORCE_INLINE void PublishCollectionForReclaim(TCacheItem collection) noexcept {
        Cache_.PublishCollectionForReclaim(SpaceOp_, collection);
    }

    Y_FORCE_INLINE bool DetachCollection(TCollectionRegistry& registry, TCacheItem collection) noexcept {
        return Cache_.DetachCollection(SpaceOp_, registry, collection);
    }

    Y_FORCE_INLINE bool UnlinkCollectionRegistry(TCollectionRegistry& registry, TCacheItem collection) noexcept {
        return Cache_.UnlinkCollectionRegistry(SpaceOp_, registry, collection);
    }

    Y_FORCE_INLINE bool TryClaimCollectionDeletion(TCollectionRegistry* expectedRegistry, TCacheItem collection,
        THandleState& sourceState, TSharedCacheItemRefImpl<TTraits>& owner) noexcept {
        return Cache_.TryClaimCollectionDeletion(SpaceOp_, expectedRegistry, collection, sourceState, owner);
    }

    Y_FORCE_INLINE bool DeleteCollection(TCollectionRegistry* expectedRegistry, TCacheItem collection) noexcept {
        return Cache_.DeleteCollection(SpaceOp_, expectedRegistry, collection);
    }

    Y_FORCE_INLINE bool MoveCollectionRegistry(
        TCollectionRegistry& source, TCollectionRegistry& destination, TCacheItem collection) noexcept {
        return Cache_.MoveCollectionRegistry(SpaceOp_, source, destination, collection);
    }

    Y_FORCE_INLINE bool UnlinkCollectionRegistry(
        TCollectionRegistry& registry, TCacheItem collection, EHandleState expectedState) noexcept {
        return Cache_.UnlinkCollectionRegistry(SpaceOp_, registry, collection, expectedState);
    }

    Y_FORCE_INLINE TSharedPageRefImpl<TTraits> BuildPageRef(TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
        return Cache_.BuildPageRef(SpaceOp_, std::move(ref));
    }

    Y_FORCE_INLINE TSharedCacheCollectionRefImpl<TTraits> BuildCollectionRef(
        TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept {
        return Cache_.BuildCollectionRef(SpaceOp_, std::move(ref));
    }

    Y_FORCE_INLINE TCollection* ReleaseTombstone(TCacheItem cacheItem) noexcept {
        return Cache_.ReleaseTombstone(SpaceOp_, cacheItem);
    }

    Y_FORCE_INLINE TCollection* ReleaseItem(TCacheItem cacheItem, bool allowKeepCold = true) noexcept {
        return Cache_.ReleaseItem(SpaceOp_, cacheItem, allowKeepCold);
    }

    Y_FORCE_INLINE bool TryReleaseColdPageToKeepCold(TCacheItem cacheItem, ui64& expectedRaw) noexcept {
        return Cache_.TryReleaseColdPageToKeepCold(SpaceOp_, cacheItem, expectedRaw);
    }

    Y_FORCE_INLINE bool TryReleaseItemRef(TCacheItem cacheItem, ui64& expectedRaw) noexcept {
        return Cache_.TryReleaseItemRef(SpaceOp_, cacheItem, expectedRaw);
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

    Y_FORCE_INLINE TCollection* PageCollection(const THandle& page) noexcept {
        return Cache_.PageCollection(SpaceOp_, page);
    }

    Y_FORCE_INLINE TCollection* PageCollection(TCollectionCacheItem collection) noexcept {
        return Cache_.PageCollection(SpaceOp_, collection);
    }

    Y_FORCE_INLINE void DropPageItemRef(THandle& page) noexcept {
        Cache_.DropPageItemRef(SpaceOp_, page);
    }

    Y_FORCE_INLINE void DropOwnerReference(TCollection& owner) noexcept {
        Cache_.DropOwnerReference(SpaceOp_, owner);
    }

    Y_FORCE_INLINE bool PromoteCold(TCacheItem cacheItem, bool force = false) noexcept {
        return Cache_.PromoteCold(SpaceOp_, cacheItem, force);
    }

    Y_FORCE_INLINE bool ReclaimKeepCold() noexcept {
        return Cache_.ReclaimKeepCold(SpaceOp_);
    }

    Y_FORCE_INLINE bool TrimKeepCold() noexcept {
        return Cache_.TrimKeepCold(SpaceOp_);
    }

    Y_FORCE_INLINE bool ShrinkKeepCold() noexcept {
        return Cache_.ShrinkKeepCold(SpaceOp_);
    }

    Y_FORCE_INLINE bool WantsKeepCold(const THandle& page) noexcept {
        return Cache_.WantsKeepCold(SpaceOp_, page);
    }

    Y_FORCE_INLINE void RouteKeepCold(TColdCacheItem coldItem) noexcept {
        Cache_.RouteKeepCold(SpaceOp_, coldItem);
    }

    Y_FORCE_INLINE bool DemoteKeepCold(TColdCacheItem coldItem, TPageCacheItem expectedPage = {}) noexcept {
        return Cache_.DemoteKeepCold(SpaceOp_, coldItem, expectedPage);
    }

    Y_FORCE_INLINE void ReleaseColdDemotionPin(TOperationItemRef<TTraits>& pin) noexcept {
        Cache_.ReleaseColdDemotionPin(SpaceOp_, pin);
    }

    Y_FORCE_INLINE void ReleaseKeepColdPin(TOperationItemRef<TTraits>& pin, ui32 initialColdVersion) noexcept {
        Cache_.ReleaseKeepColdPin(SpaceOp_, pin, initialColdVersion);
    }

    Y_FORCE_INLINE void WithdrawKeepColdEntry(TColdCacheItem coldItem) noexcept {
        Cache_.WithdrawKeepColdEntry(SpaceOp_, coldItem);
    }

    Y_FORCE_INLINE bool MatchesKeepColdEntry(TColdCacheItem coldItem) noexcept {
        return Cache_.MatchesKeepColdEntry(SpaceOp_, coldItem);
    }

    Y_FORCE_INLINE bool EvictFromHot(ui32 index) noexcept {
        return Cache_.EvictFromHot(SpaceOp_, index);
    }

    Y_FORCE_INLINE bool EvictFromHot(TCacheItem cacheItem) noexcept {
        return Cache_.EvictFromHot(SpaceOp_, cacheItem);
    }

    Y_FORCE_INLINE void RouteCold(TColdCacheItem coldItem) noexcept {
        Cache_.RouteCold(SpaceOp_, coldItem);
    }

    Y_FORCE_INLINE TCacheItem PutHot(EHotLevel level, TCacheItem cacheItem) noexcept {
        return Cache_.PutHot(SpaceOp_, level, cacheItem);
    }

    Y_FORCE_INLINE void RouteHot(EHotLevel level, TCacheItem cacheItem) noexcept {
        Cache_.RouteHot(SpaceOp_, level, cacheItem);
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> TryAcquireHotEvictionPin(TCacheItem cacheItem) noexcept {
        return Cache_.TryAcquireHotEvictionPin(SpaceOp_, cacheItem);
    }

    void CompleteHotEviction(THandle& handle, THandleState coldState, ui64 bytes, TOperationItemRef<TTraits>& pin,
        ui32 initialColdVersion) noexcept;

    Y_FORCE_INLINE void ProcessHot(EHotLevel level, TCacheItem cacheItem) noexcept {
        Cache_.ProcessHot(SpaceOp_, level, cacheItem);
    }

    Y_FORCE_INLINE bool ProcessHotAtMinimum() noexcept {
        return Cache_.ProcessHotAtMinimum(SpaceOp_);
    }

    Y_FORCE_INLINE bool DrainHotResize() noexcept {
        return Cache_.DrainHotResize(SpaceOp_);
    }

    Y_FORCE_INLINE ui32 MinimumHotSlots(bool underPressure = false) const noexcept {
        return Cache_.MinimumHotSlots(SpaceOp_, underPressure);
    }

    Y_FORCE_INLINE ui32 ShrinkHotTarget(ui32 effectiveHotSlots, ui32 step, bool underPressure) const noexcept {
        return Cache_.ShrinkHotTarget(SpaceOp_, effectiveHotSlots, step, underPressure);
    }

    Y_FORCE_INLINE ui32 GrowHotTarget(ui32 effectiveHotSlots, ui32 step) const noexcept {
        return Cache_.GrowHotTarget(SpaceOp_, effectiveHotSlots, step);
    }

    Y_FORCE_INLINE bool ReclaimToLimits(ui64 softLimit, ui32 step, ui64 bytes = 0) noexcept {
        return Cache_.ReclaimToLimits(softLimit, step, SpaceOp_, bytes);
    }

    Y_FORCE_INLINE TSharedCacheItemRefImpl<TTraits> TryAcquirePageForRelocation(ui32 index) noexcept {
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
    Y_FORCE_INLINE bool PrepareBucketSplit() noexcept {
        return Cache_.Table_.PrepareBucketSplit(SpaceOp_);
    }

    Y_FORCE_INLINE bool ActivateBucketSplitOrMerge() noexcept {
        return Cache_.Table_.ActivateBucketSplitOrMerge(SpaceOp_);
    }

    Y_FORCE_INLINE bool PublishBucketSplit() noexcept {
        return Cache_.Table_.PublishBucketSplit(SpaceOp_);
    }

    Y_FORCE_INLINE bool CloseBucketSplit() noexcept {
        return Cache_.Table_.CloseBucketSplit(SpaceOp_);
    }

    Y_FORCE_INLINE bool RemoveBucketSplit() noexcept {
        return Cache_.Table_.RemoveBucketSplit(SpaceOp_);
    }

    Y_FORCE_INLINE void PublishPageReplacement(
        TCacheItem source, TCacheItem replacement, const TSharedCacheKey& key) noexcept {
        Cache_.Table_.PublishPageReplacement(source, replacement, key, SpaceOp_);
    }

protected:
    friend class TSharedCacheImpl<TTraits>;

    TOperationView(TSharedCacheImpl<TTraits>& cache, TSpaceOperation& spaceOp) noexcept
        : Cache_(cache)
        , SpaceOp_(spaceOp)
    {}

protected: // Data members
    TSharedCacheImpl<TTraits>& Cache_;
    TSpaceOperation& SpaceOp_;
};

template <class TTraits>
class TSharedCacheImpl<TTraits>::TOperation final : public TSpaceOperation, public TOperationView {
public:
    using TOperationView::TryAllocateHandle;

    TOperation(const TOperation&) = delete;
    TOperation& operator=(const TOperation&) = delete;
    TOperation& operator=(TOperation&&) = delete;

    TOperation(TOperation&& other) noexcept
        : TSpaceOperation(std::move(other))
        , TOperationView(other.Cache_, *this)
    {}

private:
    friend class TSharedCacheImpl<TTraits>;

    TOperation(TSharedCacheImpl<TTraits>& cache, TSpaceOperation&& spaceOp) noexcept
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
using TSharedCachePageRequest = TSharedCachePageRequestImpl<>;
using TSharedCache = TSharedCacheImpl<>;

} // namespace NKikimr::NSharedCache
