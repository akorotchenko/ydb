#pragma once

#include "shared_cache_collection.h"
#include "shared_cache_table.h"

#include <util/generic/noncopyable.h>

namespace NKikimr::NSharedCache {

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
class TSharedCachePageRefImpl : private TMoveOnly {
public:
    TSharedCachePageRefImpl() noexcept = default;
    TSharedCachePageRefImpl(TSharedCachePageRefImpl&& other) noexcept;
    TSharedCachePageRefImpl& operator=(TSharedCachePageRefImpl&& other) noexcept;

    explicit operator bool() const noexcept {
        return bool(Ref_);
    }

    TPageCacheItem CacheItem() const noexcept;
    const char* data() const noexcept;
    size_t size() const noexcept;
    NActors::TSharedData ShareData() const noexcept;
    NTable::NPage::EPage GetType() const noexcept;
    void Drop() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;

    TSharedCachePageRefImpl(
        TSharedCacheItemRefImpl<TTraits>&& ref, NActors::TSharedData&& data, NTable::NPage::EPage type) noexcept;

private:
    TSharedCacheItemRefImpl<TTraits> Ref_;
    NActors::TSharedData Data_;
    NTable::NPage::EPage Type_ = NTable::NPage::EPage::Undef;
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
    TCacheCollection& GetCollection() noexcept;
    void Drop() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;

    TSharedCacheCollectionRefImpl(TSharedCacheItemRefImpl<TTraits>&& ref, TCacheCollection* collection) noexcept;

private:
    TSharedCacheItemRefImpl<TTraits> Ref_;
    TCacheCollection* Collection_ = nullptr;
};

template <class TTraits = TProdTraits>
class TPageFetchImpl : private TMoveOnly {
public:
    TPageFetchImpl() noexcept = default;
    TPageFetchImpl(TPageFetchImpl&& other) noexcept;
    TPageFetchImpl& operator=(TPageFetchImpl&& other) noexcept;
    ~TPageFetchImpl();

    explicit operator bool() const noexcept {
        return bool(FetchState_);
    }

    TPageCacheItem CacheItem() const noexcept;
    NTable::NPage::TPageLocation Location() const noexcept;
    ui64 Size() const noexcept;
    bool Dispatch() noexcept;
    bool MakeReady(NActors::TSharedData&& data) noexcept;
    bool FailReady() noexcept;

private:
    friend class TSharedCacheImpl<TTraits>;

    explicit TPageFetchImpl(TPageFetchState* fetch) noexcept;

private:
    TIntrusivePtr<TPageFetchState> FetchState_;
};

template <class TTraits = TProdTraits>
struct TSharedCachePageRequestImpl {
    NTable::NPage::TPageLocation Location;
    TIntrusivePtr<TPageFetchWaiter> Waiter;
    ESharedCacheResultStatus Status = ESharedCacheResultStatus::Miss;
    TSharedCachePageRefImpl<TTraits> Page;
    TPageFetchImpl<TTraits> Fetch;
};

class TSharedCacheTestAccess;
class TRequestPageWaiter;

template <class TTraits = TProdTraits>
class TSharedCacheImpl : public TThrRefBase, private TTraits {
public:
    static constexpr ui64 ExpectedPageSize = 7 * 1024;

    static TIntrusivePtr<TSharedCacheImpl> Create(
        THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits = {});

    ~TSharedCacheImpl() override;

    TSharedCacheImpl(const TSharedCacheImpl&) = delete;
    TSharedCacheImpl& operator=(const TSharedCacheImpl&) = delete;

    static TSharedCacheImpl& SharedCachePages() noexcept {
        TSharedCacheImpl* cache = TrySharedCachePages();
        Y_DEBUG_ABORT_UNLESS(cache);
        return *cache;
    }

    static TSharedCacheImpl* TrySharedCachePages() noexcept {
        return static_cast<TSharedCacheImpl*>(TTraits::TrySharedCachePages());
    }

    TSharedCacheThreadBinding<TTraits> BindThreadHazard(ui32 index) const noexcept {
        return { const_cast<TSharedCacheImpl*>(this), Space_->BindThreadHazard(index) };
    }

    TSharedCacheThreadBinding<TTraits> BindCurrentThreadHazard() const noexcept {
        if (Space_->ThreadHazardBound()) {
            return {};
        }
        return BindThreadHazard(TTraits::CurrentWorkerIndex());
    }

    bool MakeReady(TPageCacheItem page, NActors::TSharedData&& data) noexcept;

    bool FailReady(TPageCacheItem page) noexcept;

    bool MakeReady(
        TCollectionRegistry& registry, TCollectionCacheItem collection, THolder<TCacheCollection>&& value) noexcept;

    bool MakeReady(TCollectionCacheItem collection, THolder<TCacheCollection>&& value) noexcept;

    bool DetachCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept;

    bool UnlinkCollectionRegistry(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept;

    bool DeleteCollection(TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept;

    bool DeleteCollection(TCollectionCacheItem collection) noexcept;

    bool MoveCollectionRegistry(
        TCollectionRegistry& source, TCollectionRegistry& destination, TCollectionCacheItem collection) noexcept;

    bool SetCollectionStickyPages(TCollectionCacheItem collection, bool sticky) noexcept;

    bool SetCollectionKeepPages(TCollectionCacheItem collection, bool enabled) noexcept;

    ESharedCacheResultStatus Find(
        TCollectionCacheItem collection, ui64 offset, TSharedCachePageRefImpl<TTraits>& page) noexcept;

    ESharedCacheResultStatus Find(const TLogoBlobID& id, TSharedCacheCollectionRefImpl<TTraits>& collection) noexcept;

    ESharedCacheResultStatus FindOrInsert(TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page,
        EStickyState sticky, TIntrusivePtr<TPageFetchWaiter> waiter, TSharedCachePageRefImpl<TTraits>& hit,
        TPageFetchImpl<TTraits>& fetch) noexcept;

    bool FindOrInsertBatch(TCollectionCacheItem collection, TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests,
        bool recordStats = true) noexcept;

    ESharedCacheResultStatus FindOrInsert(TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page,
        EStickyState sticky, TPageCacheItem& inserted, TSharedCachePageRefImpl<TTraits>& hit) noexcept;

    ESharedCacheResultStatus FindOrInsert(TCollectionRegistry& registry, const TCollectionLocation& collection,
        TCollectionCacheItem& inserted, TSharedCacheCollectionRefImpl<TTraits>& hit) noexcept;

    ESharedCacheResultStatus FindOrInsert(const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TSharedCacheCollectionRefImpl<TTraits>& hit) noexcept;

    bool ReclaimCold() noexcept;

    bool UpdateCurrentLimit(ui64 limit) noexcept;
    bool UpdateHardLimit(ui64 limit) noexcept;
    bool EnforceCurrentLimit() noexcept;
    bool EnforceCurrentLimit(TSpaceOperation& spaceOp, ui64 bytes = 0) noexcept;
    bool RunMaintenance() noexcept;

    ui64 CurrentLimit() const noexcept {
        return CurrentLimit_.load(std::memory_order_relaxed);
    }

    ui64 SoftLimit() const noexcept {
        return SoftLimit_.load(std::memory_order_relaxed);
    }

    ui64 HardLimit() const noexcept {
        return HardLimit_.load(std::memory_order_relaxed);
    }

    ui64 ReservationLimit() const noexcept {
        return ReservationLimit_;
    }

    ui64 ResidentBytes() const noexcept {
        return ResidentBytes_.load(std::memory_order_relaxed);
    }

    ui64 HotBytes() const noexcept {
        return LoadEstimatedBytes(HotBytes_);
    }

    ui64 HotPages() const noexcept {
        return HotPages_.load(std::memory_order_relaxed);
    }

    ui64 ColdPages() const noexcept {
        return ColdPages_.load(std::memory_order_relaxed);
    }

    ui64 StickyPages() const noexcept {
        return StickyPages_.load(std::memory_order_relaxed);
    }

    ui64 KeepColdPages() const noexcept {
        return KeepColdPages_.load(std::memory_order_relaxed);
    }

    ui64 Collections() const noexcept {
        return Collections_.load(std::memory_order_relaxed);
    }

    ui64 ColdBytes() const noexcept {
        return LoadEstimatedBytes(ColdBytes_);
    }

    ui64 StickyBytes() const noexcept {
        return LoadEstimatedBytes(StickyBytes_);
    }

    ui64 KeepColdBytes() const noexcept {
        return LoadEstimatedBytes(KeepColdBytes_);
    }

    ui64 CollectionBytes() const noexcept {
        return CollectionBytes_.load(std::memory_order_relaxed);
    }

    ui64 OverallUsage() const noexcept {
        return OverallUsage_.load(std::memory_order_relaxed);
    }

    ui64 ReservedBytes() const noexcept {
        return ReservedBytes_.load(std::memory_order_relaxed);
    }

    // Admitted pages whose payload has not arrived yet, i.e. the pages currently being loaded. The actor publishes
    // them as the InFlightPages/Bytes counters.
    ui64 ReservedPages() const noexcept {
        return ReservedPages_.load(std::memory_order_relaxed);
    }

    // Pages may be kept only while they fit this budget; a page that does not fit is admitted with StickyNone and
    // follows ordinary policy. StickyOwnedBytes() is the exact counter the limit is applied to.
    ui64 StickyLimit() const noexcept {
        return StickyLimit_.load(std::memory_order_relaxed);
    }

    bool UpdateStickyLimit(ui64 limit) noexcept;

    bool UpdateKeepColdLimit(ui64 limit) noexcept;

    ui64 KeepColdOwnedBytes() const noexcept {
        return KeepColdOwnedBytes_.load(std::memory_order_relaxed);
    }

    ui64 StickyOwnedBytes() const noexcept {
        return StickyOwnedBytes_.load(std::memory_order_relaxed);
    }

    ui64 StaticBytes() const noexcept {
        return StaticBytes_.load(std::memory_order_relaxed);
    }

    ui64 StaticDeltaBytes() const noexcept {
        return StaticDeltaBytes_.load(std::memory_order_relaxed);
    }

    struct TStats {
        ui64 RequestedPages = 0;
        ui64 RequestedBytes = 0;
        ui64 HitPages = 0;
        ui64 HitBytes = 0;
        ui64 MissPages = 0;
        ui64 MissBytes = 0;
        ui64 StickyAdmissionFailures = 0; // page insertions that requested Sticky but fell back to ordinary on budget

        void AddAdmission(ESharedCacheResultStatus status, ui64 bytes) noexcept {
            ++RequestedPages;
            RequestedBytes += bytes;
            if (status == ESharedCacheResultStatus::Hit) {
                ++HitPages;
                HitBytes += bytes;
                return;
            }
            ++MissPages;
            MissBytes += bytes;
        }
    };

    TStats Stats() const noexcept {
        return {
            .RequestedPages = RequestedPages_.load(std::memory_order_relaxed),
            .RequestedBytes = RequestedBytes_.load(std::memory_order_relaxed),
            .HitPages = HitPages_.load(std::memory_order_relaxed),
            .HitBytes = HitBytes_.load(std::memory_order_relaxed),
            .MissPages = MissPages_.load(std::memory_order_relaxed),
            .MissBytes = MissBytes_.load(std::memory_order_relaxed),
            .StickyAdmissionFailures = StickyAdmissionFailures_.load(std::memory_order_relaxed),
        };
    }

private:
    friend class TSharedCacheTestAccess;
    friend class TSharedCacheItemRefImpl<TTraits>;
    friend class TSharedCacheTableImpl<TTraits>;
    friend class TSharedCachePageRefImpl<TTraits>;
    friend class TSharedCacheCollectionRefImpl<TTraits>;
    friend class TPageFetchImpl<TTraits>;
    friend class TRequestPageWaiter;

    Y_FORCE_INLINE void InvokeHook(ESharedCacheHookPoint point, TCacheItem cacheItem) noexcept {
        TTraits::Invoke(point, cacheItem);
    }

    void SetHooks(TTraits hooks) noexcept {
        static_cast<TTraits&>(*this) = std::move(hooks);
    }

    explicit TSharedCacheImpl(THolder<TSharedCacheSpace> space, ui64 hardLimit, TTraits traits) noexcept;

    TSpaceOperation BeginOperation() const noexcept {
        return Space_->BeginOperation();
    }

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

    struct TPageInsertCandidate {
        TSharedCacheKey Key;
        ui64 Size = 0;
        NTable::NPage::EPage Type = NTable::NPage::EPage::Undef;
        ui32 Crc32 = 0;
        EStickyState Sticky = EStickyState::None;
        TCacheItem CacheItem;
        bool Prepare = false; // the page is absent from the table and needs an allocated pending item
        bool StickyBudgetDenied = false; // count only if this prepared page wins table insertion
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

    TIntrusivePtr<TPageFetchState> TryAcquirePageFetch(
        TSpaceOperation& spaceOp, TPageCacheItem page, TOperationItemRef<TTraits>& owner) noexcept;
    static bool MatchesPendingFetch(
        const THandle& handle, THandleState state, TCacheItem cacheItem, const TPageFetchState& fetch) noexcept;
    THandle* TryGetPendingFetchHandle(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept;

    bool CompleteFetch(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch, NActors::TSharedData&& data) noexcept;

    bool DispatchFetch(TSpaceOperation& spaceOp, TCacheItem cacheItem, TPageFetchState& fetch) noexcept;

    bool FailFetch(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept;

    bool MakeReady(
        TCollectionRegistry* registry, TCollectionCacheItem collection, THolder<TCacheCollection>&& value) noexcept;

    bool MakeReady(TSpaceOperation& spaceOp, TCollectionRegistry* registry, TCacheItem cacheItem,
        THolder<TCacheCollection>&& value) noexcept;

    void MakeReadyState(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void FinishFetch(TOperationItemRef<TTraits>& finalizerRef, TPageFetchState& fetch) noexcept;
    void FinishFailedFetch(
        TOperationItemRef<TTraits>& finalizerRef, TPageFetchState& fetch, TPageFetchWaiter* waiters) noexcept;
    void ReleaseFetchRef(TOperationItemRef<TTraits>& owner, TPageFetchState& fetch) noexcept;
    static void DeletePendingFetch(THandle& handle) noexcept;

    ESharedCacheResultStatus Find(
        TSpaceOperation& spaceOp, const TSharedCacheKey& key, TSharedCacheItemRefImpl<TTraits>& ref) noexcept;

    ESharedCacheResultStatus FindOrInsert(
        TSpaceOperation& spaceOp, TCacheItem candidate, TSharedCacheItemRefImpl<TTraits>& ref) noexcept;

    static TPageInsertCandidate BuildPageInsertCandidate(
        TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page, EStickyState sticky) noexcept;

    ESharedCacheResultStatus FindOrInsertPage(TSpaceOperation& spaceOp, TPageInsertCandidate& candidate,
        TIntrusivePtr<TPageFetchWaiter> waiter, TSharedCachePageRefImpl<TTraits>& hit,
        TPageFetchImpl<TTraits>& fetch) noexcept;
    bool PreparePageBatch(TSpaceOperation& spaceOp, TCacheCollection& collection,
        TArrayRef<TSharedCachePageRequestImpl<TTraits>> requests, TVector<TPageInsertCandidate>& candidates,
        ui64& reservedBytes) noexcept;
    bool AllocatePageBatch(TSpaceOperation& spaceOp, TCacheCollection& collection,
        TVector<TPageInsertCandidate>& candidates, ui64 reservedBytes) noexcept;
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

    bool EraseCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept;

    bool ReclaimCold(TSpaceOperation& spaceOp) noexcept;
    // Cooperative reclamation: takes a cold entry and, when cold is empty, lets EnforceCurrentLimit move the
    // hot/cold boundary so that the coldest hot entries become reclaimable.
    bool Reclaim(TSpaceOperation& spaceOp, ui64 bytes = 0) noexcept;

    bool AdvanceBucketResize() noexcept;

    bool TryReserve(TSpaceOperation& spaceOp, ui64 bytes, ui64 pages = 0) noexcept;
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
        ui64 size, NTable::NPage::EPage type, ui32 crc32, EStickyState sticky) noexcept;

    void InitializeCollection(
        TSpaceOperation& spaceOp, TCacheItem cacheItem, const TLogoBlobID& id, ui64 bytes) noexcept;

    EAcquireStatus TryAcquire(TSpaceOperation& spaceOp, TCacheItem cacheItem, bool allowPending,
        TSharedCacheItemRefImpl<TTraits>& ref) noexcept;

    TSharedCacheItemRefImpl<TTraits> TryAcquireStructural(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    TSharedCacheItemRefImpl<TTraits> BuildExternalRef(EItemKind kind, TCacheItem cacheItem) noexcept;

    TSharedCacheItemRefImpl<TTraits> AcquireStickyCollection(
        TSpaceOperation& spaceOp, TCollectionCacheItem collection, TCacheCollection*& value) noexcept;

    bool LinkStickyPage(
        TSpaceOperation& spaceOp, TCacheItem page, TSharedCacheItemRefImpl<TTraits>& collectionRef) noexcept;
    void RestoreStickyPageList(TSpaceOperation& spaceOp, TCacheCollection& collection, ui32 detachedHead) noexcept;
    void DrainStickyPages(TSpaceOperation& spaceOp, TCollectionCacheItem collection, TCacheCollection& value) noexcept;
    static void PublishPageUnsticky(THandle& page, THandleState state) noexcept;
    void MergeRetainedStickyPages(TSpaceOperation& spaceOp, TCollectionCacheItem collectionItem,
        TCacheCollection& collection, ui32 retainedHead, ui32 retainedTail) noexcept;
    bool SetCollectionStickyPages(TSpaceOperation& spaceOp, TCacheItem collection, bool sticky) noexcept;
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
    bool UnlinkCollectionRegistry(
        TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection) noexcept;
    bool TryClaimCollectionDeletion(TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry,
        TCacheItem collection, THandleState& sourceState) noexcept;
    bool DeleteCollection(TCollectionRegistry* registry, TCollectionCacheItem collection) noexcept;
    bool DeleteCollection(
        TSpaceOperation& spaceOp, TCollectionRegistry* expectedRegistry, TCacheItem collection) noexcept;
    bool MoveCollectionRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& source, TCollectionRegistry& destination,
        TCacheItem collection) noexcept;
    bool UnlinkCollectionRegistry(TSpaceOperation& spaceOp, TCollectionRegistry& registry, TCacheItem collection,
        EHandleState expectedState) noexcept;
    void UnlinkCollectionRegistry(TCollectionRegistry& registry, TOperationItemRef<TTraits>& collection) noexcept;

    TSharedCachePageRefImpl<TTraits> BuildPageRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;

    bool AcquirePage(TPageCacheItem page, TSharedCachePageRefImpl<TTraits>& result) noexcept;

    TSharedCacheCollectionRefImpl<TTraits> BuildCollectionRef(
        const TSpaceOperation& spaceOp, TSharedCacheItemRefImpl<TTraits>&& ref) const noexcept;

    void Release(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    TCacheCollection* ReleaseTombstone(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;
    TCacheCollection* ReleaseItem(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void ReleaseReplacing(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void ReleaseReplaced(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    void DeletePayload(THandle& handle, EItemKind kind, EHandleState state) noexcept;

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

    static TCacheCollection* PageCollection(TSpaceOperation& spaceOp, const THandle& page) noexcept;
    static void AddReference(TCacheCollection& owner) noexcept;
    void DropPageItemRef(TSpaceOperation& spaceOp, THandle& page) noexcept;
    static bool DropReference(TCacheCollection& owner) noexcept;
    void DropOwnerReference(TSpaceOperation& spaceOp, TCacheCollection& owner) noexcept;
    static ui64 PayloadBytes(const THandle& handle, EItemKind kind) noexcept;
    static ui64 AccountedPageBytes(ui64 size) noexcept;
    static ui64 PageBytes(const THandle& handle) noexcept;
    static ui64 LoadEstimatedBytes(const std::atomic<i64>& counter) noexcept;
    static ui64 FractionCeil(ui64 value, double fraction) noexcept;
    static void TransferEstimatedBytes(std::atomic<i64>& source, std::atomic<i64>& destination, ui64 bytes) noexcept;
    static void TransferEstimatedPages(
        std::atomic<ui64>& source, std::atomic<ui64>& destination, EItemKind kind) noexcept;
    static void AddEstimatedPages(std::atomic<ui64>& counter, EItemKind kind) noexcept;
    static void SubtractEstimatedPages(std::atomic<ui64>& counter, EItemKind kind) noexcept;
    static void SubtractExactBytes(std::atomic<ui64>& counter, ui64 bytes) noexcept;

    // The sticky budget covers pages only: collection records move through the sticky estimate but never consume it.
    void AddStickyOwnedBytes(EItemKind kind, ui64 bytes) noexcept {
        if (kind == EItemKind::Page) {
            StickyOwnedBytes_.fetch_add(bytes, std::memory_order_relaxed);
        }
    }

    void SubStickyOwnedBytes(EItemKind kind, ui64 bytes) noexcept {
        if (kind == EItemKind::Page) {
            SubtractExactBytes(StickyOwnedBytes_, bytes);
        }
    }

    bool TryAddStickyOwnedBytes(ui64 bytes) noexcept;

    void AddKeepColdOwnedBytes(ui64 bytes) noexcept;

    bool TrimKeepCold(TSpaceOperation& spaceOp) noexcept;

    bool WantsKeepCold(TSpaceOperation& spaceOp, const THandle& page) noexcept;

    void RouteKeepCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept;

    bool DemoteKeepCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept;

    void WithdrawKeepColdEntry(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept;

    static bool IsKeepColdQueued(const THandle& handle) noexcept;

    bool MatchesKeepColdEntry(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept;

    void AddStats(const TStats& stats) noexcept;

    bool EvictFromHot(TSpaceOperation& spaceOp, ui32 index) noexcept;

    bool EvictFromHot(TSpaceOperation& spaceOp, TCacheItem cacheItem) noexcept;

    static void AdvanceColdMembership(THandle& handle) noexcept;

    static bool AdvanceColdMembership(
        THandle& handle, ui32 expectedVersion, bool keepColdQueued, ui32& newVersion) noexcept;

    void RouteCold(TSpaceOperation& spaceOp, TCacheItem coldItem) noexcept;

    TCacheItem PutHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept;

    void RouteHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept;

    static Y_FORCE_INLINE bool TakeHotFrequency(
        THandle& handle, TCacheItem cacheItem, THandleState& state, ui8& frequency) noexcept;
    Y_FORCE_INLINE void CompleteHotEviction(
        TSpaceOperation& spaceOp, TCacheItem cacheItem, THandle& handle, THandleState coldState) noexcept;

    void ProcessHot(TSpaceOperation& spaceOp, EHotLevel level, TCacheItem cacheItem) noexcept;

    bool DrainHotResize() noexcept;
    bool DrainHotResize(TSpaceOperation& spaceOp) noexcept;
    ui32 ShrinkHotTarget(ui32 effectiveHotSlots, ui32 step) const noexcept;
    ui32 GrowHotTarget(ui32 effectiveHotSlots, ui32 step) const noexcept;
    ui32 HotResizeStep(ui32 effectiveHotSlots) const noexcept;
    ui64 CalculateSoftLimit(ui64 currentLimit) const noexcept;
    bool ReclaimColdToLimit(ui64 softLimit, ui32 step, TSpaceOperation& spaceOp, ui64 bytes = 0) noexcept;

    bool StartHardTransition() noexcept;
    TSharedCacheItemRefImpl<TTraits> TryAcquirePageForRelocation(TSpaceOperation& spaceOp, ui32 index) noexcept;
    TCacheItem PreparePageReplacement(TSpaceOperation& spaceOp, TCacheItem source) noexcept;
    void DiscardPageReplacement(TSpaceOperation& spaceOp, TCacheItem replacement) noexcept;
    EHandleState PublishPageReplacement(TSpaceOperation& spaceOp, TCacheItem source, TCacheItem replacement) noexcept;
    void CompletePageReplacement(TSpaceOperation& spaceOp, TCacheItem source) noexcept;
    bool RelocatePage(TSpaceOperation& spaceOp, ui32 index) noexcept;
    EShrinkCutStatus ReclaimShrinkHandle(TSpaceOperation& spaceOp, ui32 index) noexcept;
    EShrinkCutStatus ReclaimShrinkCut() noexcept;

private:
    THolder<TSharedCacheSpace> Space_;
    TSharedCacheTableImpl<TTraits> Table_;
    std::atomic<ui64> ResidentBytes_{ 0 };
    std::atomic<i64> HotBytes_{ 0 };
    std::atomic<i64> ColdBytes_{ 0 };
    std::atomic<i64> KeepColdBytes_{ 0 };
    std::atomic<i64> StickyBytes_{ 0 };
    std::atomic<ui64> HotPages_{ 0 };
    std::atomic<ui64> ColdPages_{ 0 };
    std::atomic<ui64> KeepColdPages_{ 0 };
    std::atomic<ui64> StickyPages_{ 0 };
    std::atomic<ui64> Collections_{ 0 };
    std::atomic<ui64> CollectionBytes_{ 0 };
    std::atomic<ui64> StaticBytes_{ 0 };
    std::atomic<ui64> StaticDeltaBytes_{ 0 };
    std::atomic<ui64> OverallUsage_{ 0 };
    std::atomic<ui64> ReservedBytes_{ 0 };
    std::atomic<ui64> ReservedPages_{ 0 };
    std::atomic<ui64> StickyLimit_{ Max<ui64>() };
    std::atomic<ui64> StickyOwnedBytes_{ 0 };
    std::atomic<ui64> KeepColdLimit_{ 0 };
    std::atomic<ui64> KeepColdOwnedBytes_{ 0 };
    std::atomic<bool> KeepColdTrimActive_{ false };
    std::atomic<ui64> KeepColdTrimScanRemaining_{ 0 };
    std::atomic<ui64> KeepModeSweepRemaining_{ 0 };
    std::atomic<ui64> CurrentLimit_{ 0 };
    std::atomic<ui64> SoftLimit_{ 0 };
    std::atomic<bool> SoftLimitPressure_{ false };
    std::atomic<ui64> HardLimit_{ 0 };
    std::atomic<ui64> RequestedPages_{ 0 };
    std::atomic<ui64> RequestedBytes_{ 0 };
    std::atomic<ui64> HitPages_{ 0 };
    std::atomic<ui64> HitBytes_{ 0 };
    std::atomic<ui64> MissPages_{ 0 };
    std::atomic<ui64> MissBytes_{ 0 };
    std::atomic<ui64> StickyAdmissionFailures_{ 0 };
    ui64 ReservationLimit_ = 0;
    THotResize HotResize_;
    TSharedCacheCapacity HardTarget_;
    TTransition HardTransition_;

    inline static constexpr TSharedCachePolicy Policy_ = SharedCachePolicyFor<TTraits>;
};

using TSharedCacheCollectionRef = TSharedCacheCollectionRefImpl<>;
using TPageFetch = TPageFetchImpl<>;
using TSharedCachePageRequest = TSharedCachePageRequestImpl<>;
using TSharedCache = TSharedCacheImpl<>;

} // namespace NKikimr::NSharedCache
