#include <ydb/core/tablet_flat/cache_thread_test.h>
#include "cache.h"
#include "cache_mapping_test.h"
#include "cache_traits.h"

#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>

#include <array>
#include <atomic>
#include <thread>
#include <type_traits>
#include <utility>

namespace NKikimr::NSharedCache {

using TTestSharedCache = TSharedCacheImpl<TTestTraits>;
using EResizeStep = TCacheTableImpl<TTestTraits>::EResizeStep;
using TTestCacheItemRef = TCacheItemRefImpl<TTestTraits>;
using TTestCachePageRef = TCachePageRefImpl<TTestTraits>;
using TTestCacheCollectionRef = TCacheCollectionRefImpl<TTestTraits>;
using TTestPageFetch = TPageFetchImpl<TTestTraits>;
using TTestCachePageRequest = TCachePageRequestImpl<TTestTraits>;

static_assert(sizeof(TTestCacheItemRef) == sizeof(TCacheItem));
static_assert(sizeof(TTestCachePageRef) == 8);
static_assert(sizeof(TTestCacheCollectionRef) == sizeof(TCacheItem));
static_assert(!std::is_move_constructible_v<TTransition>);
static_assert(!std::is_move_assignable_v<TTransition>);

struct TSeparatePageRequest {
    NTable::NPage::TPageLocation Location;
    TIntrusivePtr<TPageFetchWaiter> Waiter;
    ECacheResultStatus Status;
    TTestCachePageRef Page;
    TTestPageFetch Fetch;
};

static_assert(sizeof(TTestCachePageRequest) < sizeof(TSeparatePageRequest));

template <class TRef>
struct TTestResult {
    ECacheResultStatus Status = ECacheResultStatus::Miss;
    TRef Ref;
};

using TPageResult = TTestResult<TTestCachePageRef>;
using TCollectionResult = TTestResult<TTestCacheCollectionRef>;
using TItemResult = TTestResult<TTestCacheItemRef>;

class TCacheTestAccess {
public:
    static bool FindOrInsertWrittenBatch(
        TTestSharedCache& cache, TCollectionCacheItem collection, TArrayRef<TTestCachePageRequest> requests) noexcept {
        return cache.FindOrInsertWrittenBatch(collection, requests);
    }

    static bool MakeReady(TTestSharedCache& cache, TCollectionRegistry& registry, TCollectionCacheItem collection,
        THolder<TCollection>&& value) noexcept {
        return cache.MakeReady(registry, collection, std::move(value));
    }

    static bool DetachCollection(
        TTestSharedCache& cache, TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
        return cache.DetachCollection(registry, collection);
    }

    static bool UnlinkCollectionRegistry(
        TTestSharedCache& cache, TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
        auto cacheOp = cache.BeginOperation();
        return cacheOp.Collections().Unlink(registry, collection.CacheItem());
    }

    static bool EraseCollection(
        TTestSharedCache& cache, TCollectionRegistry& registry, TCollectionCacheItem collection) noexcept {
        return cache.EraseCollection(&registry, collection);
    }

    static ECacheResultStatus FindOrInsert(TTestSharedCache& cache, TCollectionRegistry& registry,
        const TCollectionLocation& collection, TCollectionCacheItem& inserted, TTestCacheCollectionRef& hit) noexcept {
        return cache.FindOrInsert(registry, collection, inserted, hit);
    }

    static ECacheCollectionLookupStatus FindCollection(TTestSharedCache& cache, TCollectionRegistry& registry,
        const TLogoBlobID& id, TTestCacheCollectionRef& collection) noexcept {
        return cache.FindCollection(registry, id, collection);
    }

    static bool MakeReadyUnregistered(
        TTestSharedCache& cache, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
        return cache.MakeReady(nullptr, collection, std::move(value));
    }

    static TTestCacheItemRef AcquireStructural(TTestSharedCache& cache, TCacheItem item) noexcept {
        auto operation = cache.BeginOperation();
        return operation.TryAcquireStructural(item);
    }

    static bool MakePendingPageSticky(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        auto spaceOp = cache.BeginOperation();
        TOperationItemRef<TTestTraits> ref(spaceOp, spaceOp.TryAcquireStructural(page.CacheItem()));
        return ref && spaceOp.Collections().MakePageSticky(page.CacheItem());
    }

    static bool MakePageSticky(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        TTestCachePageRef held;
        if (!cache.AcquirePage(page, held)) {
            return false;
        }
        auto spaceOp = cache.BeginOperation();
        return spaceOp.Collections().MakePageSticky(page.CacheItem());
    }

    static TPageCacheItem AllocatePage(TTestSharedCache& cache, TCollectionCacheItem collection, ui64 offset, ui64 size,
        NTable::NPage::EPage type, ui32 crc32, EStickyState sticky) noexcept {
        return cache.AllocatePage(collection, offset, size, type, crc32, sticky);
    }

    static TCollectionCacheItem AllocateCollection(
        TTestSharedCache& cache, const TLogoBlobID& id, ui64 bytes) noexcept {
        return cache.AllocateCollection(id, bytes);
    }

    template <class TTraits>
    static ECacheResultStatus FindOrInsert(TSharedCacheImpl<TTraits>& cache, TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EStickyState sticky, TPageCacheItem& inserted,
        TCachePageRefImpl<TTraits>& hit) noexcept {
        return cache.FindOrInsert(collection, page, sticky, inserted, hit);
    }

    template <class TTraits>
    static ECacheResultStatus FindOrInsert(TSharedCacheImpl<TTraits>& cache, TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EStickyState sticky, TIntrusivePtr<TPageFetchWaiter> waiter,
        TCachePageRefImpl<TTraits>& hit, TPageFetchImpl<TTraits>& fetch) noexcept {
        return cache.FindOrInsert(collection, page, sticky, std::move(waiter), hit, fetch);
    }

    static ECacheResultStatus FindOrInsertUnregistered(TTestSharedCache& cache, const TCollectionLocation& collection,
        TCollectionCacheItem& inserted, TTestCacheCollectionRef& hit) noexcept {
        return cache.FindOrInsertUnregistered(collection, inserted, hit);
    }

    static bool AcquirePage(TTestSharedCache& cache, TPageCacheItem page, TTestCachePageRef& result) noexcept {
        return cache.AcquirePage(page, result);
    }

    static void DiscardCandidate(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        cache.DiscardCandidate(page);
    }

    static void DiscardCandidate(TTestSharedCache& cache, TCollectionCacheItem collection) noexcept {
        cache.DiscardCandidate(collection);
    }

    static bool AdvanceBucketResize(TTestSharedCache& cache) noexcept {
        return cache.Capacity_.AdvanceBucketResize();
    }

    static bool TryReserve(TTestSharedCache& cache, ui64 bytes, ui64 pages = 0) noexcept {
        auto spaceOp = cache.BeginOperation();
        return cache.TryReserve(spaceOp, bytes, pages);
    }

    static TKeepReservation ReserveKeepCandidate(TTestSharedCache& cache, ui64 bytes) noexcept {
        return cache.Memory_.TryReserveKeep(bytes, cache.KeepLimit());
    }

    static void ReleaseReservation(TTestSharedCache& cache, ui64 bytes, ui64 pages = 0) noexcept {
        cache.ReleaseReservation(bytes, pages);
    }

    static TCacheMapping& BucketsMapping(TTestSharedCache& cache) noexcept {
        return cache.Space_->BucketsMapping_;
    }

    static void SetCapacityTarget(TTestSharedCache& cache, const TCacheCapacityConfig& target) noexcept {
        cache.Capacity_.SetTarget(target);
    }

    static TTransition& CapacityTransition(TTestSharedCache& cache) noexcept {
        return cache.Capacity_.Transition_;
    }

    static EPrepareResult PrepareTransition(
        TTestSharedCache& cache, const TCacheCapacityConfig& target, TTransition& transition) noexcept {
        return cache.Capacity_.PrepareTransition(target, transition);
    }

    static bool PrepareReceiver(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.Capacity_.PrepareReceiver(transition);
    }

    template <class TCallback>
    static bool PrepareReceiver(TTestSharedCache& cache, TTransition& transition, TCallback& callback) noexcept {
        auto guard = cache.HotResize_.TryLock();
        if (!guard || !cache.HotResize_.OwnsCapacityTransition(transition)) {
            return false;
        }
        return cache.Space_->PrepareReceiverWithHook(
            transition, {
                            .Context = &callback,
                            .Function = [](void* context, ESpaceMap mapping) noexcept {
                                return (*static_cast<TCallback*>(context))(mapping);
                            },
                        });
    }

    static TCacheMapping& RingMapping(TTestSharedCache& cache, ESpaceMap mapping) noexcept {
        return cache.Space_->RingMapping(mapping);
    }

    static bool PublishMigrationView(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.Capacity_.PublishMigrationView(transition);
    }

    static bool TryDrainTransition(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.Capacity_.TryDrainTransition(transition);
    }

    static bool PublishFinalView(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.Capacity_.PublishFinalView(transition);
    }

    static bool AppendFreeHandles(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.Capacity_.AppendFreeHandles(transition);
    }

    static bool FinalDrain(TTestSharedCache& cache, TTransition& transition) noexcept {
        do {
            if (!cache.Capacity_.FinalDrain(transition)) {
                return false;
            }
        } while (transition.Phase() == ETransitionPhase::FinalDrain);
        return true;
    }

    static bool TryReleaseTransition(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.Capacity_.TryReleaseTransition(transition);
    }

    static bool CommitTransition(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.Capacity_.CommitTransition(transition);
    }

    static bool AbandonTransition(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.Capacity_.AbandonTransition(transition);
    }

    template <class TCallback>
    static decltype(auto) WithSpace(TTestSharedCache& cache, TCallback&& callback) {
        auto spaceOp = cache.BeginOperation();
        return std::forward<TCallback>(callback)(spaceOp);
    }

    static auto BindOperation(TTestSharedCache& cache, TSpaceOperation& spaceOp) noexcept {
        return cache.BindOperation(spaceOp);
    }

    template <class TCallback>
    static decltype(auto) WithHandle(TTestSharedCache& cache, ui32 index, TCallback&& callback) {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) -> decltype(auto) {
            return std::forward<TCallback>(callback)(spaceOp.Handles()[index]);
        });
    }

    static THandleState HandleState(
        TTestSharedCache& cache, ui32 index, std::memory_order order = std::memory_order_acquire) noexcept {
        return WithHandle(cache, index, [&](THandle& handle) {
            return THandleState::FromRaw(handle.State.load(order));
        });
    }

    static ui32 ColdVersion(TTestSharedCache& cache, ui32 index) noexcept {
        return WithHandle(cache, index, [](THandle& handle) {
            return handle.ColdVersion.load(std::memory_order_acquire) & MaxItemVersion;
        });
    }

    static ui64 HandleNext(TTestSharedCache& cache, ui32 index) noexcept {
        return WithHandle(cache, index, [](THandle& handle) {
            return handle.Next.load(std::memory_order_acquire);
        });
    }

    static ui32 NextInOwner(TTestSharedCache& cache, ui32 index) noexcept {
        return WithHandle(cache, index, [](THandle& handle) {
            return handle.NextInOwner.load(std::memory_order_acquire);
        });
    }

    static NTable::NPage::EPage PageType(TTestSharedCache& cache, ui32 index) noexcept {
        return WithHandle(cache, index, [](THandle& handle) {
            return handle.Metadata.Page.Type;
        });
    }

    static ui32 PageCrc32(TTestSharedCache& cache, ui32 index) noexcept {
        return WithHandle(cache, index, [](THandle& handle) {
            return handle.Metadata.Page.Crc32;
        });
    }

    static void HoldPageInKeepColdForTest(TTestSharedCache& cache, TPageCacheItem page, ui32 coldVersion) noexcept {
        WithHandle(cache, page.Index(), [&](THandle& handle) {
            const auto before = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
            Y_ABORT_UNLESS(before.IsHot() && before.Refs() == 1);
            const ui64 bytes = TCacheItems<TTestTraits>::PageBytes(handle);
            TKeepReservation incoming;
            if (before.Tier() != ECacheTier::Keep) {
                incoming = cache.Memory_.TryReserveKeep(bytes, cache.KeepLimit());
                Y_ABORT_UNLESS(incoming);
            }
            handle.State.store(before.WithState(EHandleState::KeepCold).Raw(), std::memory_order_relaxed);
            handle.ColdVersion.store(coldVersion, std::memory_order_relaxed);
            cache.Memory_.Hot(before.Tier()).TransferBytesTo(cache.Memory_.Cold(ECacheTier::Keep), bytes);
            cache.Memory_.Hot(before.Tier()).TransferPageTo(cache.Memory_.Cold(ECacheTier::Keep), EItemKind::Page);
            cache.Memory_.ReleaseRetainedBytes(bytes);
            incoming.Commit();
        });
    }

    static ui32 QueueKeepColdPageForTest(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            THandle& handle = spaceOp.Handles()[page.Index()];
            const ui32 version = cache.AdvanceColdMembership(handle);
            handle.ColdVersion.fetch_or(ColdTicketOwnedMask, std::memory_order_release);
            auto& tier = spaceOp.View().Tiers[ECacheTier::Keep];
            tier.Rings->AddColdEntry();
            const auto displaced =
                TColdCacheItem::FromRaw(tier.Cold().PutAndPop(TColdCacheItem::Make(version, page.Index()).Raw()));
            if (!displaced.IsNull()) {
                tier.Rings->RemoveColdEntry();
            }
            return version;
        });
    }

    static TCollection* CollectionValue(TTestSharedCache& cache, ui32 index) noexcept {
        return WithHandle(cache, index, [](THandle& handle) {
            return handle.Body.Collection;
        });
    }

    static ui32 StickyPageListHead(TTestSharedCache& cache, TCollectionCacheItem collection) noexcept {
        TCollection* value = CollectionValue(cache, collection.Index());
        Y_ABORT_UNLESS(value);
        return value->StickyPageListHead.load(std::memory_order_acquire);
    }

    static ui32 CollectionListHead(const TCollectionRegistry& registry) noexcept {
        return registry.CollectionListHead.load(std::memory_order_acquire);
    }

    static TCacheItem BucketHead(TTestSharedCache& cache, ui32 bucket) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            return TCacheItem::FromRaw(spaceOp.Buckets()[bucket].load(std::memory_order_acquire));
        });
    }

    static ui64 HandleCount(TTestSharedCache& cache) noexcept {
        return WithSpace(cache, [](TSpaceOperation& spaceOp) {
            return spaceOp.HandleCount();
        });
    }

    static ui32 MinimumHotSlots(TTestSharedCache& cache) noexcept {
        return static_cast<ui32>(Min<ui64>(cache.Eviction_.MinimumHotSlots(ECacheTier::Regular),
            cache.Space_->CurrentConfiguration().Tiers[ECacheTier::Regular].HotSlots));
    }

    static ui64 ColdEntries(TTestSharedCache& cache, ECacheTier tier) noexcept {
        return cache.Space_->ColdRingEntries(tier);
    }

    static bool SweepModes(TTestSharedCache& cache) noexcept {
        return cache.Eviction_.SweepModes();
    }

    static std::array<ui64, 4> HotPolicy(TTestSharedCache& cache, const TSpaceOperation& op) noexcept {
        const ui32 effective = static_cast<ui32>(op.EffectiveHotSlots());
        const ui32 minimum = MinimumHotSlots(cache);
        return { effective, minimum, cache.Eviction_.ShrinkHotTarget(effective, 1, minimum),
            cache.Eviction_.GrowHotTarget(op, ECacheTier::Regular, effective, 2) };
    }

    static bool EnforceWithOperation(
        TTestSharedCache& cache, TSpaceOperation& spaceOp, ui64 bytes = 0, bool forceShrinkHot = true) noexcept {
        return cache.BindOperation(spaceOp).Eviction().EnforceCurrentLimit(bytes, forceShrinkHot);
    }

    static THotResize::TGuard LockHotResize(TTestSharedCache& cache) noexcept {
        return cache.HotResize_.TryLock();
    }

    static bool CanChangeTiers(TTestSharedCache& cache) noexcept {
        auto guard = cache.HotResize_.TryLock();
        return guard && cache.HotResize_.CanChangeTiers();
    }

    static bool OwnsCapacityTransition(TTestSharedCache& cache, const TTransition& transition) noexcept {
        auto guard = cache.HotResize_.TryLock();
        return guard && cache.HotResize_.OwnsCapacityTransition(transition);
    }

    template <class TCallback>
    static bool TryReleaseTransitionWithHook(
        TTestSharedCache& cache, TTransition& transition, TCallback& callback) noexcept {
        return cache.Space_->TryReleaseTransitionWithHook(
            transition, {
                            .Context = &callback,
                            .Function = [](void* context, ESpaceMap mapping, const void* begin, size_t size) noexcept {
                                return (*static_cast<TCallback*>(context))(mapping, begin, size);
                            },
                        });
    }

    static std::pair<ui64, ui64> ColdByteWatermarks(const TTestSharedCache& cache) noexcept {
        const ui64 budget = cache.EvictableByteBudget();
        return { cache.Eviction_.FractionCeil(budget, cache.Policy_.ColdMin),
            cache.Eviction_.FractionCeil(budget, cache.Policy_.Grow) };
    }

    static void ShrinkHotToMinimum(TTestSharedCache& cache) noexcept {
        for (;;) {
            const ui64 before = EffectiveHotSlots(cache);
            WithSpace(cache, [&](TSpaceOperation& spaceOp) {
                cache.BindOperation(spaceOp).Eviction().EnforceCurrentLimit(0, true);
            });
            if (EffectiveHotSlots(cache) == before) {
                return;
            }
        }
    }

    static bool ShrinkKeepCold(TTestSharedCache& cache) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            return cache.BindOperation(spaceOp).Eviction().TrimKeep();
        });
    }

    static bool ProcessHotAtMinimum(TTestSharedCache& cache) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            return cache.BindOperation(spaceOp).Eviction().ProcessHotAtMinimum();
        });
    }

    static void ProcessHotToken(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            cache.BindOperation(spaceOp).Eviction().ProcessHot(EHotLevel::L0, page.CacheItem());
        });
    }

    static ui64 EffectiveHotSlots(TTestSharedCache& cache) noexcept {
        return WithSpace(cache, [](TSpaceOperation& spaceOp) {
            return spaceOp.EffectiveHotSlots();
        });
    }

    static ui64 FreeCount(const TTestSharedCache& cache) noexcept {
        return cache.Space_->FreeRingCursor_->Count();
    }

    static const TCacheSpace& Space(const TTestSharedCache& cache) noexcept {
        return *cache.Space_;
    }

    static ui32 HazardCount(const TTestSharedCache& cache) noexcept {
        return cache.Space_->HazardCount_;
    }

    static ui64 SpareItem(const TTestSharedCache& cache, ui32 index) noexcept {
        return cache.Space_->Hazards_[index]->SpareItem.load(std::memory_order_relaxed);
    }

    static TSpaceState CurrentSpaceState(const TCacheSpace& space) noexcept {
        return space.CurrentSpaceState();
    }

    static TBucketResizeState BucketResizeState(const TCacheSpace& space) noexcept {
        return space.BucketResizeState();
    }

    static bool EvictFromHot(TTestSharedCache& cache, TPageCacheItem page) {
        auto operation = cache.BeginOperation();
        return operation.Eviction().EvictFromHot(page.Index());
    }

    static bool EvictFromHot(TTestSharedCache& cache, TCollectionCacheItem collection) {
        auto operation = cache.BeginOperation();
        return operation.Eviction().EvictFromHot(collection.Index());
    }

    static void TransferColdToHot(TTestSharedCache& cache, ui64 bytes) {
        cache.Memory_.Cold().TransferBytesTo(cache.Memory_.Hot(), bytes);
    }

    static void TransferHotToCold(TTestSharedCache& cache, ui64 bytes) {
        cache.Memory_.Hot().TransferBytesTo(cache.Memory_.Cold(), bytes);
    }

    static i64 ColdBytesEstimate(const TTestSharedCache& cache) {
        return cache.Memory_.Cold().BytesEstimate();
    }

    static i64 ColdPagesEstimate(const TTestSharedCache& cache) {
        return cache.Memory_.Cold().PagesEstimate();
    }

    static i64 KeepColdPagesEstimate(const TTestSharedCache& cache) {
        return cache.Memory_.Cold(ECacheTier::Keep).PagesEstimate();
    }

    static ETransitionPhase HardTransitionPhase(const TTestSharedCache& cache) noexcept {
        return cache.Capacity_.Transition_.Phase();
    }

    static ui64 HardTransitionWorkIndex(const TTestSharedCache& cache) noexcept {
        return cache.Capacity_.Transition_.NextWorkIndex_;
    }

    static EHotResizePhase HotResizePhase(const TTestSharedCache& cache) noexcept {
        return cache.HotResize_.Phase();
    }

    static bool BeginHotResize(TTestSharedCache& cache, ui32 target) noexcept {
        return cache.Space_->BeginHotResize(ECacheTier::Regular, target, cache.HotResize_);
    }

    static bool DrainHotResize(TTestSharedCache& cache) noexcept {
        return cache.DrainHotResize();
    }

    static ui64 HotSlot(TTestSharedCache& cache, ui64 slot) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            Y_ABORT_UNLESS(slot < spaceOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount);
            return spaceOp.View().Tiers[ECacheTier::Regular].HotStorage.Slots[slot].load(std::memory_order_acquire);
        });
    }

    static const TCacheMemory::TTierUsage& TierUsage(const TTestSharedCache& cache, ECacheTier tier) noexcept {
        return cache.Memory_.Tier(tier);
    }

    static ui64 CollectionReferences(TTestSharedCache& cache, TCollectionCacheItem collection) {
        auto operation = cache.BeginOperation();
        const auto& handle = operation.Handles()[collection.Index()];
        const auto state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        // Count page owners plus the implicit registry/foundation owner
        return handle.Body.Collection->PageReferences.load(std::memory_order_relaxed) +
               (state.IsStickyField() || state.IsTransitionField());
    }

    static ui64 PageReferences(TTestSharedCache& cache, TCollectionCacheItem collection) {
        auto operation = cache.BeginOperation();
        return operation.Handles()[collection.Index()].Body.Collection->PageReferences.load(std::memory_order_acquire);
    }

    static void AddPageReference(TTestSharedCache& cache, TCollection& collection) {
        auto operation = cache.BeginOperation();
        operation.Collections().AddPageReference(collection);
    }

    static void DropPageReference(TTestSharedCache& cache, TCollection& collection) {
        auto operation = cache.BeginOperation();
        operation.Collections().DropPageReference(collection);
    }

    static bool EraseCold(TTestSharedCache& cache, TColdCacheItem cacheItem) {
        auto operation = cache.BeginOperation();
        const bool keep =
            operation.SpaceOperation().Contains(cacheItem) &&
            THandleState::FromRaw(operation.Handles()[cacheItem.Index()].State.load(std::memory_order_acquire))
                .IsKeepCold();
        return operation.Eviction().EraseCold(cacheItem, keep ? ECacheTier::Keep : ECacheTier::Regular);
    }

    static bool ReclaimCold(TTestSharedCache& cache, TSpaceOperation& operation) noexcept {
        return cache.BindOperation(operation).Eviction().ReclaimCold();
    }

    static bool ReclaimKeepCold(TTestSharedCache& cache, TSpaceOperation& operation) noexcept {
        return cache.BindOperation(operation).Eviction().ReclaimCold(ECacheTier::Keep);
    }

    static bool RetierColdToRegular(TTestSharedCache& cache, TColdCacheItem hint) noexcept {
        auto op = cache.BeginOperation();
        THandle& handle = op.Handles()[hint.Index()];
        const auto state = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
        TOperationItemRef<TTestTraits> ref(
            op.SpaceOperation(), op.TryAcquireStructural(TCacheItem::Make(state.Version(), hint.Index())));
        if (!ref) {
            return false;
        }
        if (TCollection* owner = op.Collections().PageCollection(handle)) {
            if (!cache.SetCollectionPagesCacheMode(owner->CacheItem, ECacheMode::Regular)) {
                return false;
            }
        }
        return op.Eviction().ReconcilePage(ref.Get());
    }

    static bool TrimKeep(TTestSharedCache& cache, TSpaceOperation& operation) noexcept {
        return cache.BindOperation(operation).Eviction().TrimKeep();
    }

    static bool RetirePageResult(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        auto op = cache.BeginOperation();
        TOperationItemRef<TTestTraits> ref(op.SpaceOperation(), op.TryAcquireStructural(page.CacheItem()));
        if (!ref) {
            return false;
        }
        auto state = THandleState::FromRaw(op.Handles()[page.Index()].State.load(std::memory_order_acquire));
        if (!op.TryBeginErase(ref.Get(), state)) {
            return false;
        }
        op.Eviction().FinishPageErase(page.CacheItem());
        return true;
    }

    static void RouteOwnedPage(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        auto op = cache.BeginOperation();
        TOperationItemRef<TTestTraits> ref(op.SpaceOperation(), op.TryAcquireStructural(page.CacheItem()));
        if (ref) {
            op.Eviction().RouteOwnedHot(ref.Get(), EHotLevel::L1);
        }
    }

    static bool UnstickyCutPages(TTestSharedCache& cache, TPageCacheItem page, ui64 allocationLimit) {
        auto operation = cache.BeginOperation();
        return operation.Collections().UnstickyCutPages(page.CacheItem(), allocationLimit);
    }

    static bool RelocatePage(TTestSharedCache& cache, TPageCacheItem page) {
        auto operation = cache.BeginOperation();
        return cache.RelocatePage(operation, page.Index());
    }

    static bool BeginBucketResize(TTestSharedCache& cache, ui8 newAddressBits) {
        return cache.Space_->BeginBucketResize(newAddressBits);
    }

    static EResizeStep PrepareBucketSplitStep(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.PrepareBucketSplit(spaceOp);
    }

    static bool PrepareBucketSplit(TTestSharedCache& cache) {
        return PrepareBucketSplitStep(cache) == EResizeStep::Complete;
    }

    static EResizeStep ActivateBucketSplitStep(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.ActivateBucketSplitOrMerge(spaceOp);
    }

    static bool ActivateBucketSplitOrMerge(TTestSharedCache& cache) {
        return ActivateBucketSplitStep(cache) == EResizeStep::Complete;
    }

    static bool PublishBucketSplit(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.PublishBucketSplit(spaceOp) == EResizeStep::Complete;
    }

    static bool CloseBucketSplit(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.CloseBucketSplit(spaceOp) == EResizeStep::Complete;
    }

    static EResizeStep RemoveBucketSplitStep(TTestSharedCache& cache) {
        auto operation = cache.BeginOperation();
        return cache.Table_.RemoveBucketSplit(operation);
    }

    static bool RemoveBucketSplit(TTestSharedCache& cache) {
        return RemoveBucketSplitStep(cache) == EResizeStep::Complete;
    }

    static bool FinishBucketResize(TTestSharedCache& cache) {
        return cache.Space_->FinishBucketResize();
    }

    static void FinishErase(TTestSharedCache& cache, TCacheItem cacheItem, const TCacheKey& key) {
        auto operation = cache.BeginOperation();
        TOperationItemRef<TTestTraits> ownerRef(operation, cacheItem);
        cache.Table_.FinishErase(operation, ownerRef.CacheItem(), key);
    }

    static void InstallHooks(TTestSharedCache& cache, TTestTraits* hooks) noexcept {
        cache.SetHooks(hooks ? *hooks : TTestTraits{});
    }

    static bool ValidateTerminalOwner(TTestSharedCache& cache, TCacheItem owner, TCacheItem terminal) noexcept {
        auto spaceOp = cache.BeginOperation();
        TCacheTableImpl<TTestTraits>::TOrderedLink orderedLink{
            .Expected = terminal,
            .Owner = owner,
        };
        return cache.Table_.ValidateTerminalOwner(spaceOp, orderedLink);
    }

    static bool AcquireAfterGenerationChange(
        TTestSharedCache& cache, const TCacheKey& key, bool expectPending) noexcept {
        TTestCacheItemRef ref;
        {
            auto operation = cache.BeginOperation();
            TCacheItem cacheItem;
            Y_ABORT_UNLESS(cache.Table_.Find(operation, key, cacheItem) == ECacheResultStatus::Hit);
            UNIT_ASSERT(cache.Space_->AdvanceSpaceGeneration());
            const auto status = operation.TryAcquire(cacheItem, false, ref);
            Y_ABORT_UNLESS(status == (expectPending ? TTestSharedCache::EAcquireStatus::Pending
                                                    : TTestSharedCache::EAcquireStatus::Ready));
        }
        ref.Drop();
        return true;
    }

    static TTestCacheItemRef AcquirePending(TTestSharedCache& cache, const TCacheKey& key) noexcept {
        auto operation = cache.BeginOperation();
        TCacheItem cacheItem;
        Y_ABORT_UNLESS(cache.Table_.Find(operation, key, cacheItem) == ECacheResultStatus::Hit);
        TTestCacheItemRef ref;
        Y_ABORT_UNLESS(operation.TryAcquire(cacheItem, true, ref) == TTestSharedCache::EAcquireStatus::Pending);
        return ref;
    }

    static TTestCacheItemRef AcquireTombstoneHelper(TTestSharedCache& cache, TCacheItem cacheItem) noexcept {
        auto operation = cache.BeginOperation();
        return cache.Table_.TryAcquireTombstone(operation, cacheItem);
    }

    static TItemResult FindOrInsert(TTestSharedCache& cache, const TCacheKey& key, TPageCacheItem candidate) noexcept {
        auto spaceOp = cache.BeginOperation();
        Y_ABORT_UNLESS(LoadSharedCacheKey(spaceOp.Handles()[candidate.Index()], EItemKind::Page) == key);
        TTestCacheItemRef ref;
        const ECacheResultStatus status = cache.FindOrInsert(spaceOp, candidate.CacheItem(), ref);
        if (status == ECacheResultStatus::Pending) {
            ref.Drop();
        }
        return { .Status = status, .Ref = std::move(ref) };
    }

    static TItemResult FindOrInsert(
        TTestSharedCache& cache, const TCacheKey& key, TCollectionCacheItem candidate) noexcept {
        auto spaceOp = cache.BeginOperation();
        Y_ABORT_UNLESS(LoadSharedCacheKey(spaceOp.Handles()[candidate.Index()], EItemKind::Collection) == key);
        TTestCacheItemRef ref;
        const ECacheResultStatus status = cache.FindOrInsert(spaceOp, candidate.CacheItem(), ref);
        if (status == ECacheResultStatus::Pending) {
            ref.Drop();
        }
        return { .Status = status, .Ref = std::move(ref) };
    }
};

ui64 CurrentBucketCount(const TCacheSpace& space) noexcept {
    const TSpaceState state = TCacheTestAccess::CurrentSpaceState(space);
    return state.Resizing() ? SharedCacheBucketCount(TCacheTestAccess::BucketResizeState(space).OldAddressBits())
                            : state.BucketCount();
}

ui64 ExpectedOverallUsage(const TTestSharedCache& cache, ui64 payloadBytes) noexcept {
    return cache.StaticBytes() + cache.StaticDeltaBytes() + payloadBytes;
}

namespace {

    static_assert(!std::is_copy_constructible_v<TTestCachePageRef>);
    static_assert(!std::is_copy_assignable_v<TTestCachePageRef>);
    static_assert(std::is_move_constructible_v<TTestCachePageRef>);
    static_assert(std::is_move_assignable_v<TTestCachePageRef>);
    static_assert(!std::is_copy_constructible_v<TTestCacheCollectionRef>);
    static_assert(std::is_move_constructible_v<TTestCacheCollectionRef>);
    static_assert(std::is_move_assignable_v<TTestCacheCollectionRef>);
    static_assert(!std::is_copy_constructible_v<TTestCacheItemRef>);
    static_assert(!std::is_convertible_v<TTestCachePageRef, TTestCacheCollectionRef>);
    static_assert(!std::is_convertible_v<TTestCacheCollectionRef, TTestCachePageRef>);

    class TTestPageFetchWaiter final : public TPageFetchWaiter {
    public:
        TTestPageFetchWaiter(
            std::atomic<ui32>& completed, std::atomic<ui32>& ready, std::atomic<ui64>& completedItem) noexcept
            : Completed_(completed)
            , Ready_(ready)
            , CompletedItem_(completedItem)
        {}

        void Complete(TPageCacheItem page, EPageFetchCompletion completion) noexcept override {
            CompletedItem_.store(page.CacheItem().Raw(), std::memory_order_relaxed);
            Ready_.fetch_add(completion == EPageFetchCompletion::Ready, std::memory_order_relaxed);
            Completed_.fetch_add(1, std::memory_order_release);
        }

    private:
        std::atomic<ui32>& Completed_;
        std::atomic<ui32>& Ready_;
        std::atomic<ui64>& CompletedItem_;
    };

    class TAcquiringPageFetchWaiter final : public TPageFetchWaiter {
    public:
        TAcquiringPageFetchWaiter(TTestSharedCache& cache, TTestCachePageRef& page) noexcept
            : Cache_(cache)
            , Page_(page)
        {}

        void Complete(TPageCacheItem page, EPageFetchCompletion completion) noexcept override {
            Ready_ = completion == EPageFetchCompletion::Ready && TCacheTestAccess::AcquirePage(Cache_, page, Page_);
        }

        bool Ready() const noexcept {
            return Ready_;
        }

    private:
        TTestSharedCache& Cache_;
        TTestCachePageRef& Page_;
        bool Ready_ = false;
    };

    class TTestPageCollection final : public NPageCollection::IPageCollection {
    public:
        TTestPageCollection(TLogoBlobID id, size_t backingSize) noexcept
            : Id_(id)
            , BackingSize_(backingSize)
        {}

        TTestPageCollection(TLogoBlobID id, NActors::TSharedData page, bool& pagesReleasedBeforeCollection) noexcept
            : TTestPageCollection(id, 0)
        {
            DestructionProbe_ = std::move(page);
            PagesReleasedBeforeCollection_ = &pagesReleasedBeforeCollection;
        }

        ~TTestPageCollection() override {
            if (PagesReleasedBeforeCollection_) {
                *PagesReleasedBeforeCollection_ = DestructionProbe_.IsPrivate();
            }
        }

        const TLogoBlobID& Label() const noexcept override {
            return Id_;
        }

        ui32 Total() const noexcept override {
            return 0;
        }

        NPageCollection::TInfo Page(ui32) const override {
            return {};
        }

        NPageCollection::TBorder Bounds(ui32) const override {
            return {};
        }

        NPageCollection::TBorder Bounds(const NTable::NPage::TPageLocation&) const override {
            return {};
        }

        NPageCollection::TGlobId Glob(ui32) const override {
            return {};
        }

        bool Verify(ui32, TArrayRef<const char>) const override {
            return false;
        }

        bool Verify(const NTable::NPage::TPageLocation&, TArrayRef<const char>) const override {
            return false;
        }

        size_t BackingSize() const noexcept override {
            return BackingSize_;
        }

        NTable::NPage::TPageLocation GetLocation(ui32) const override {
            return {};
        }

        bool SkipBTreeIndexV1Shadow() const noexcept override {
            return SkipShadow_.load(std::memory_order_relaxed);
        }

        void SetSkipBTreeIndexV1Shadow(bool skip) const noexcept override {
            SkipShadow_.store(skip, std::memory_order_relaxed);
        }

    private:
        const TLogoBlobID Id_;
        const size_t BackingSize_;
        mutable std::atomic<bool> SkipShadow_{ false };
        NActors::TSharedData DestructionProbe_;
        bool* PagesReleasedBeforeCollection_ = nullptr;
    };

    TCollectionCacheItem MakeCollectionCacheItem(ui32 version, ui32 index) {
        return TCollectionCacheItem::FromValidated(TCacheItem::Make(version, index));
    }

    NActors::TSharedData MakePageData(ui32 index, size_t size = 4096) {
        auto data = NActors::TSharedData::Uninitialized(size);
        data.mutable_data()[0] = static_cast<char>(index);
        return data;
    }

    NTable::NPage::TPageLocation MakePageLocation(
        ui64 offset, ui64 size = 4096, NTable::NPage::EPage type = NTable::NPage::EPage::DataPage, ui32 crc32 = 0) {
        return NTable::NPage::TPageLocation::FromByteOffset(offset, size, type, crc32);
    }

    THolder<TCollection> MakeCollection(const TLogoBlobID& id, size_t backingSize = 0) {
        return MakeHolder<TCollection>(
            TIntrusiveConstPtr<NPageCollection::IPageCollection>(new TTestPageCollection(id, backingSize)));
    }

    TColdCacheItem MakeColdItem(TTestSharedCache& cache, ui32 index) {
        return TColdCacheItem::Make(TCacheTestAccess::ColdVersion(cache, index) & MaxItemVersion, index);
    }

    ui64 AvailableFreeHandles(const TTestSharedCache& cache) {
        ui64 result = TCacheTestAccess::FreeCount(cache);
        for (ui32 index = 0; index < TCacheTestAccess::HazardCount(cache); ++index) {
            result += TCacheTestAccess::SpareItem(cache, index) != 0;
        }
        return result;
    }

    TCollectionCacheItem PageCollection(const TCacheKey& key) {
        Y_ABORT_UNLESS(key.IsPageKind());
        return TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(key.Word(0)));
    }

    struct TSharedCacheGate {
        struct TSlot {
            std::atomic<ECacheHookPoint> Wanted{ ECacheHookPoint::BeforeTableLinkCas };
            std::atomic<ui64> Target{ 0 };
            std::atomic<bool> Arrived = false;
            std::atomic<bool> Continue = true;
            std::atomic<bool> Triggered = false;

            void Arm(ECacheHookPoint point, TCacheItem target = {}) noexcept {
                Target.store(target.Raw(), std::memory_order_relaxed);
                Wanted.store(point, std::memory_order_release);
                Arrived.store(false, std::memory_order_relaxed);
                Continue.store(false, std::memory_order_relaxed);
                Triggered.store(false, std::memory_order_relaxed);
            }

            void Arm(ECacheHookPoint point, TColdCacheItem target) noexcept {
                Arm(point, TCacheItem::FromRaw(target.Raw()));
            }

            void Wait() noexcept {
                const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
                while (!Arrived.load(std::memory_order_acquire)) {
                    if (TInstant::Now() >= deadline) {
                        Y_ABORT("Shared-cache test hook was not reached");
                    }
                    std::this_thread::yield();
                }
            }

            void Release() noexcept {
                Continue.store(true, std::memory_order_release);
            }

            void TryInvoke(ECacheHookPoint point, TCacheItem cacheItem) noexcept {
                if (point != Wanted.load(std::memory_order_acquire)) {
                    return;
                }
                // Wanted is read first so its acquire publishes Arm's relaxed Target store.
                const TCacheItem target = TCacheItem::FromRaw(Target.load(std::memory_order_acquire));
                if (!target.IsNull() && target != cacheItem) {
                    return;
                }
                Block();
            }

            void Block() noexcept {
                if (Triggered.exchange(true, std::memory_order_acq_rel)) {
                    return;
                }
                Arrived.store(true, std::memory_order_release);
                const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
                while (!Continue.load(std::memory_order_acquire)) {
                    if (TInstant::Now() >= deadline) {
                        Y_ABORT("Shared-cache test hook was not released");
                    }
                    std::this_thread::yield();
                }
            }
        } Slots[2];

        TTestTraits Hooks;

        TSharedCacheGate()
            : Hooks{ this, &Invoke }
        {}

        static void Invoke(void* context, ECacheHookPoint point, TCacheItem cacheItem) noexcept {
            auto& gate = *static_cast<TSharedCacheGate*>(context);
            for (auto& slot : gate.Slots) {
                slot.TryInvoke(point, cacheItem);
            }
        }
    };

    struct TSharedCacheHookGuard {
        TTestSharedCache& Cache;

        TSharedCacheHookGuard(TTestSharedCache& cache, TTestTraits& hooks) noexcept
            : Cache(cache)
        {
            TCacheTestAccess::InstallHooks(Cache, &hooks);
        }

        ~TSharedCacheHookGuard() {
            TCacheTestAccess::InstallHooks(Cache, nullptr);
        }

        TSharedCacheHookGuard(const TSharedCacheHookGuard&) = delete;
        TSharedCacheHookGuard& operator=(const TSharedCacheHookGuard&) = delete;
    };

    struct TGateThread {
        TSharedCacheGate& Gate;
        std::thread Thread;

        template <class TFunc>
        TGateThread(TSharedCacheGate& gate, TFunc&& func)
            : Gate(gate)
            , Thread(std::forward<TFunc>(func))
        {}

        ~TGateThread() {
            if (Thread.joinable()) {
                for (auto& slot : Gate.Slots) {
                    slot.Release();
                }
                Thread.join();
            }
        }

        void Join() {
            if (Thread.joinable()) {
                Thread.join();
            }
        }

        TGateThread(const TGateThread&) = delete;
        TGateThread& operator=(const TGateThread&) = delete;
    };

    TLogoBlobID CollectionId(const TCacheKey& key) {
        Y_ABORT_UNLESS(key.IsCollectionKind());
        return TLogoBlobID(key.Word(0), key.Word(1), key.Word(2));
    }

    // The fixture inserts Target first and no other key in this bucket before Successor.
    std::array<TCacheKey, 2> MakeAdjacentKeys(const TCacheSpace& space, TCollectionCacheItem token, ui64 startOffset) {
        TVector<TCacheKey> keys;
        const ui32 bucket =
            TCacheKey::Page(token, startOffset).Hash() & TCacheTestAccess::CurrentSpaceState(space).BucketMask();
        for (ui64 offset = startOffset; keys.size() < 2; ++offset) {
            Y_ABORT_UNLESS(offset - startOffset < (1u << 20));
            const auto key = TCacheKey::Page(token, offset);
            if ((key.Hash() & TCacheTestAccess::CurrentSpaceState(space).BucketMask()) == bucket) {
                keys.push_back(key);
            }
        }
        Sort(keys.begin(), keys.end(), [](const auto& left, const auto& right) {
            return CompareSharedCacheKeys(left, right) < 0;
        });
        Y_ABORT_UNLESS(CompareSharedCacheKeys(keys[0], keys[1]) < 0);
        return { keys[0], keys[1] };
    }

    std::array<TCacheKey, 3> MakeResizeKeys(const TCacheSpace& space, TCollectionCacheItem token, ui64 startOffset) {
        const ui64 oldBucketCount = CurrentBucketCount(space);
        Y_ABORT_UNLESS(oldBucketCount <= (ui64{ 1 } << 31));
        TMaybe<TCacheKey> low;
        TVector<TCacheKey> high;
        for (ui64 offset = startOffset; !low || high.size() < 2; ++offset) {
            Y_ABORT_UNLESS(offset - startOffset < (1u << 20));
            const auto key = TCacheKey::Page(token, offset);
            const ui32 hash = key.Hash();
            if ((hash & (oldBucketCount - 1)) != 0) {
                continue;
            }
            if (hash & oldBucketCount) {
                high.push_back(key);
            } else {
                low = key;
            }
        }
        Sort(high.begin(), high.end(), [](const auto& left, const auto& right) {
            return CompareSharedCacheKeys(left, right) < 0;
        });
        return { *low, high[0], high[1] };
    }

    TPageCacheItem AllocatePage(TTestSharedCache& cache, const TCacheKey& key, ui64 size,
        NTable::NPage::EPage type = NTable::NPage::EPage::DataPage, ui32 crc32 = 0,
        EStickyState sticky = EStickyState::None) {
        return TCacheTestAccess::AllocatePage(cache, PageCollection(key), key.Word(1), size, type, crc32, sticky);
    }

    TPageResult FindPage(TTestSharedCache& cache, const TCacheKey& key) {
        TPageResult result;
        result.Status = cache.Find(PageCollection(key), key.Word(1), result.Ref);
        return result;
    }

    TItemResult FindOrInsertPage(TTestSharedCache& cache, const TCacheKey& key, TPageCacheItem candidate) {
        return TCacheTestAccess::FindOrInsert(cache, key, candidate);
    }

    template <class TTraits>
    ECacheResultStatus FindOrInsertPage(TSharedCacheImpl<TTraits>& cache, TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EStickyState sticky, TPageCacheItem& inserted,
        TCachePageRefImpl<TTraits>& hit) {
        return TCacheTestAccess::FindOrInsert(cache, collection, page, sticky, inserted, hit);
    }

    TCollectionCacheItem AllocateCollection(TTestSharedCache& cache, const TCacheKey& key, size_t backingSize = 0) {
        Y_UNUSED(backingSize);
        return TCacheTestAccess::AllocateCollection(cache, CollectionId(key), 0);
    }

    TCollectionResult FindCollection(TTestSharedCache& cache, const TCacheKey& key) {
        TCollectionResult result;
        result.Status = cache.Find(CollectionId(key), result.Ref);
        return result;
    }

    TItemResult FindOrInsertCollection(TTestSharedCache& cache, const TCacheKey& key, TCollectionCacheItem candidate) {
        return TCacheTestAccess::FindOrInsert(cache, key, candidate);
    }

    TPageCacheItem InsertReadyPage(TTestSharedCache& cache, const TCacheKey& key) {
        TPageCacheItem page;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(cache, PageCollection(key), MakePageLocation(key.Word(1)), EStickyState::None,
                        page, hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(cache.MakeReady(page, MakePageData(page.Index())));
        return page;
    }

    bool ConfigureKeep(TTestSharedCache& cache, ui64 bytes) {
        if (!cache.UpdateKeepLimit(bytes)) {
            return false;
        }
        for (ui64 step = 0; step < cache.TargetHandleCount() * 4 + 4096; ++step) {
            const bool work = cache.RunMaintenance();
            if (!work && !cache.NeedsCapacityMaintenance()) {
                return true;
            }
        }
        return false;
    }

    TPageCacheItem InsertOwnedPage(
        TTestSharedCache& cache, TCollectionCacheItem collection, ui64 offset, TTestCachePageRef& result) {
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(cache, result);
        TTestCachePageRequest request(MakePageLocation(offset), waiter);
        UNIT_ASSERT(cache.FindOrInsertBatch(collection, { &request, 1 }));
        if (request.Status() == ECacheResultStatus::Hit) {
            result = std::move(request.Page());
            return result.CacheItem();
        }
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        const auto item = request.Fetch().CacheItem();
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(item.Index())));
        UNIT_ASSERT(waiter->Ready());
        return result.CacheItem();
    }

    TCollectionCacheItem CreateModeCollection(
        TTestSharedCache& cache, TCollectionRegistry& registry, const TLogoBlobID& id, ECacheMode mode) {
        const auto item = AllocateCollection(cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(cache, registry, item, MakeCollection(id)));
        UNIT_ASSERT(cache.SetCollectionPagesCacheMode(item, mode));
        return item;
    }

    TPageCacheItem InsertOwnedSizedPage(TTestSharedCache& cache, TCollectionCacheItem collection, ui64 offset,
        ui32 size, char value, TTestCachePageRef& result) {
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(cache, result);
        TTestCachePageRequest request(MakePageLocation(offset, size), waiter);
        UNIT_ASSERT(cache.FindOrInsertBatch(collection, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(static_cast<ui32>(value), size)));
        UNIT_ASSERT(waiter->Ready());
        return result.CacheItem();
    }

    void AssertPageHit(TTestSharedCache& cache, const TCacheKey& key, TPageCacheItem expected) {
        auto found = FindPage(cache, key);
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(found.Ref.CacheItem() == expected);
        found.Ref.Drop();
    }

    void AssertPageUnreferenced(TTestSharedCache& cache, TPageCacheItem page) {
        const THandleState state = TCacheTestAccess::HandleState(cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), page.Version());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
    }

    void AssertResizeMarker(TTestSharedCache& cache, EHandleState expectedState) {
        const THandleState state = TCacheTestAccess::HandleState(cache, ResizeMarkerIndex);
        UNIT_ASSERT(state.State() == expectedState);
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
    }

    void CompleteBucketResize(TTestSharedCache& cache, TCacheSpace& space) {
        const ui64 maxSteps = ui64(TCacheTestAccess::BucketResizeState(space).PairCount()) * 5 + 1;
        for (ui64 step = 0; step < maxSteps; ++step) {
            if (!TCacheTestAccess::CurrentSpaceState(space).Resizing()) {
                return;
            }
            UNIT_ASSERT(TCacheTestAccess::AdvanceBucketResize(cache));
        }
        UNIT_ASSERT_C(!TCacheTestAccess::CurrentSpaceState(space).Resizing(),
            "Bucket resize did not finish within its step bound");
    }

    void CommitTransition(TTestSharedCache& cache, TTransition& transition) {
        UNIT_ASSERT(TCacheTestAccess::PublishFinalView(cache, transition));
        while (transition.Phase() == ETransitionPhase::AppendGrowth) {
            UNIT_ASSERT(TCacheTestAccess::AppendFreeHandles(cache, transition));
        }
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(cache, transition));
        while (transition.Phase() == ETransitionPhase::FinalDrain) {
            UNIT_ASSERT(TCacheTestAccess::FinalDrain(cache, transition));
        }
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(cache, transition));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(cache, transition));
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSharedCacheTableKeyTest) {
    Y_UNIT_TEST(PageKeyRoundTripIgnoresSizeWord) {
        THandle handle;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(5, 17), 1234);
        StoreSharedCacheKey(handle, key);
        handle.Key2OrSize.store(98765);

        UNIT_ASSERT(LoadSharedCacheKey(handle, EItemKind::Page) == key);
    }
    Y_UNIT_TEST(CollectionKeyRoundTripUsesAllWords) {
        THandle handle;
        const auto key = TCacheKey::Collection(TLogoBlobID(11, 22, 33));
        StoreSharedCacheKey(handle, key);

        UNIT_ASSERT(LoadSharedCacheKey(handle, EItemKind::Collection) == key);
        UNIT_ASSERT_VALUES_EQUAL(handle.Key2OrSize.load(), 33);
    }
    Y_UNIT_TEST(HashUsesEveryTypedKeyWord) {
        const auto token = MakeCollectionCacheItem(1, 2);
        UNIT_ASSERT_VALUES_UNEQUAL(HashPageKey(token, 10), HashPageKey(token, 11));
        UNIT_ASSERT_VALUES_UNEQUAL(HashPageKey(token, 10), HashPageKey(MakeCollectionCacheItem(2, 2), 10));

        const auto base = HashCollectionKey(TLogoBlobID(1, 2, 3));
        UNIT_ASSERT_VALUES_UNEQUAL(base, HashCollectionKey(TLogoBlobID(2, 2, 3)));
        UNIT_ASSERT_VALUES_UNEQUAL(base, HashCollectionKey(TLogoBlobID(1, 3, 3)));
        UNIT_ASSERT_VALUES_UNEQUAL(base, HashCollectionKey(TLogoBlobID(1, 2, 4)));
    }
    Y_UNIT_TEST(CrossKindHashTieUsesKind) {
        const auto page = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 3);
        const auto collection = TCacheKey::Collection(TLogoBlobID(1, 2, 3));

        UNIT_ASSERT(CompareSharedCacheKeys(page, 42, collection, 42) < 0);
        UNIT_ASSERT(CompareSharedCacheKeys(collection, 42, page, 42) > 0);
    }
    Y_UNIT_TEST(TypedWordOrderBreaksHashTie) {
        const auto first = TCacheKey::Collection(TLogoBlobID(1, 2, 3));
        const auto second = TCacheKey::Collection(TLogoBlobID(1, 2, 4));

        UNIT_ASSERT(CompareSharedCacheKeys(first, 42, second, 42) < 0);
        UNIT_ASSERT(CompareSharedCacheKeys(second, 42, first, 42) > 0);
        UNIT_ASSERT_VALUES_EQUAL(CompareSharedCacheKeys(first, 42, first, 42), 0);
    }
    Y_UNIT_TEST(ComparatorDefinesTotalOrder) {
        TVector<TCacheKey> keys = {
            TCacheKey::Page(MakeCollectionCacheItem(1, 7), 11),
            TCacheKey::Collection(TLogoBlobID(3, 2, 1)),
            TCacheKey::Page(MakeCollectionCacheItem(1, 7), 10),
            TCacheKey::Collection(TLogoBlobID(1, 2, 3)),
            TCacheKey::Page(MakeCollectionCacheItem(2, 7), 10),
        };

        Sort(keys.begin(), keys.end(), [](const auto& left, const auto& right) {
            return CompareSharedCacheKeys(left, right) < 0;
        });

        for (size_t index = 1; index < keys.size(); ++index) {
            UNIT_ASSERT(CompareSharedCacheKeys(keys[index - 1], keys[index]) < 0);
        }
    }
}

Y_UNIT_TEST_SUITE(TSharedCacheTableTest) {
    Y_UNIT_TEST(PageFetchWaitersCloseExactlyOnce) {
        TIntrusivePtr<TPageFetchState> fetch = new TPageFetchState;
        for (ui32 index = 0; index < 8; ++index) {
            UNIT_ASSERT(fetch->Subscribe(new TPageFetchWaiter));
        }

        ui32 drained = 0;
        UNIT_ASSERT(fetch->CloseWaiters([&](TIntrusivePtr<TPageFetchWaiter> waiter) {
            UNIT_ASSERT(waiter);
            ++drained;
        }));
        UNIT_ASSERT_VALUES_EQUAL(drained, 8);
        UNIT_ASSERT(fetch->Closed());
        UNIT_ASSERT(!fetch->Subscribe(new TPageFetchWaiter));
        UNIT_ASSERT(!fetch->CloseWaiters([](TIntrusivePtr<TPageFetchWaiter>) {
        }));
    }
    Y_UNIT_TEST(PageFetchSubscribeRacesClose) {
        TIntrusivePtr<TPageFetchState> fetch = new TPageFetchState;
        std::atomic<bool> start = false;
        std::atomic<ui32> subscribed = 0;
        std::array<std::thread, 8> threads;
        for (std::thread& thread : threads) {
            thread = std::thread([&] {
                while (!start.load(std::memory_order_acquire)) {
                }
                if (fetch->Subscribe(new TPageFetchWaiter)) {
                    subscribed.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        start.store(true, std::memory_order_release);
        ui32 drained = 0;
        fetch->CloseWaiters([&](TIntrusivePtr<TPageFetchWaiter>) {
            ++drained;
        });
        for (std::thread& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(drained, subscribed.load(std::memory_order_relaxed));
    }

    struct TFixture {
        TFixture(ui8 addressBits = 6, ui32 hazards = 8, ui8 reservedAddressBits = 0, ui64 hardLimit = 0) {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(addressBits, 4096, 0, hazards, Capacity));
            if (reservedAddressBits == 0) {
                reservedAddressBits = addressBits;
            }
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(reservedAddressBits, 4096, 0, hazards, ReservedCapacity));
            Threads = MakeHolder<TCacheThreadSubSystem>(hazards);
            auto space = TCacheSpace::Create(Capacity, ReservedCapacity, *Threads);
            UNIT_ASSERT(space);
            Space = space.Get();
            Cache = TTestSharedCache::Create(std::move(space), hardLimit ? hardLimit : Capacity.Limit);
            UNIT_ASSERT(Cache);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*Cache, hazards - 1);
        }

        ~TFixture() {
            Y_DEBUG_ABORT_UNLESS(!TlsCacheHazard || TlsCacheHazard->State.load(std::memory_order_relaxed) == 0);
            TlsCacheHazard = nullptr;
        }

        THolder<TCacheThreadSubSystem> Threads;
        TCacheCapacityConfig Capacity;
        TCacheCapacityConfig ReservedCapacity;
        TCacheSpace* Space = nullptr;
        TCollectionRegistry Registry;
        TIntrusivePtr<TTestSharedCache> Cache;
    };

    Y_UNIT_TEST(CollectionPublicationKeepsFoundationStickyDefault) {
        TFixture fixture;
        const TLogoBlobID id(111, 112, 150);
        TCollectionCacheItem collection;
        TTestCacheCollectionRef published;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(TCollectionLocation{ .Id = id }, collection, published) ==
                    ECacheResultStatus::Inserted);
        auto value = MakeCollection(id);
        UNIT_ASSERT(value->GetCacheMode() == ECacheMode::Sticky);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, std::move(value)));
        UNIT_ASSERT(fixture.Cache->Find(id, published) == ECacheResultStatus::Hit);
        UNIT_ASSERT(published->GetCacheMode() == ECacheMode::Sticky);
    }

    Y_UNIT_TEST(ColdTicketCannotReviveAfterPhysicalShrinkAndRegrowth) {
        TFixture fixture(10, 2);
        auto& cache = *fixture.Cache;
        TCacheCapacityConfig small;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(9, 4096, 0, 2, small));
        UNIT_ASSERT(cache.UpdateSoftLimitOverride(4 * (64 + TSharedData::OverheadSize)));
        const auto collection =
            CreateModeCollection(cache, fixture.Registry, TLogoBlobID(111, 112, 152), ECacheMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(collection.Index(), 2);
        TVector<TCollectionCacheItem> candidates;
        Y_DEFER {
            for (auto item : candidates) {
                TCacheTestAccess::DiscardCandidate(cache, item);
            }
        };
        for (ui64 index = 3; index < small.HandleCount(); ++index) {
            const auto item = AllocateCollection(cache, TCacheKey::Collection(TLogoBlobID(111, 113, index)));
            UNIT_ASSERT_VALUES_EQUAL(item.Index(), index);
            candidates.push_back(item);
        }
        TTestCachePageRef oldOwner;
        const auto old = InsertOwnedSizedPage(cache, collection, 1, 64, 'a', oldOwner);
        UNIT_ASSERT_VALUES_EQUAL(old.Index(), small.HandleCount());
        oldOwner.Drop();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, old));
        UNIT_ASSERT(TCacheTestAccess::HandleState(cache, old.Index()).IsCold());
        const auto oldTicket = MakeColdItem(cache, old.Index());
        const auto firstColdWord = [&] {
            auto operation = cache.BeginOperation();
            return operation.SpaceOperation().View().Tiers[ECacheTier::Regular].ColdStorage.Slots[0].load();
        };
        UNIT_ASSERT_VALUES_EQUAL(firstColdWord(), oldTicket.Raw());
        const auto resize = [&](const TCacheCapacityConfig& target) {
            if (cache.CurrentLimit() > target.Limit) {
                UNIT_ASSERT(cache.UpdateCurrentLimit(target.Limit));
            }
            UNIT_ASSERT(cache.UpdateHardLimit(target.Limit));
            for (ui32 step = 0; step < 10000 && cache.NeedsCapacityMaintenance(); ++step) {
                cache.RunMaintenance();
            }
            UNIT_ASSERT(!cache.NeedsCapacityMaintenance());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(cache), target.HandleCount());
        };
        resize(small);
        UNIT_ASSERT_VALUES_EQUAL(cache.StaticBytes(), small.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(firstColdWord(), 0);
        TCacheTestAccess::DiscardCandidate(cache, candidates.front());
        candidates.erase(candidates.begin());
        TTestCachePageRef olderOwner;
        const auto older = InsertOwnedSizedPage(cache, collection, 2, 64, 'b', olderOwner);
        UNIT_ASSERT(older.Index() < small.HandleCount());
        olderOwner.Drop();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, older));
        resize(fixture.Capacity);
        UNIT_ASSERT_VALUES_EQUAL(cache.StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(firstColdWord(), 0);
        TTestCachePageRef newOwner;
        const auto newer = InsertOwnedSizedPage(cache, collection, 3, 64, 'c', newOwner);
        UNIT_ASSERT_VALUES_EQUAL(newer.Index(), old.Index());
        newOwner.Drop();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, newer));
        UNIT_ASSERT_VALUES_EQUAL(MakeColdItem(cache, newer.Index()).ColdVersion(), oldTicket.ColdVersion());
        UNIT_ASSERT(cache.ReclaimCold());
        UNIT_ASSERT(TCacheTestAccess::HandleState(cache, older.Index()).IsFree());
        auto found = FindPage(cache, TCacheKey::Page(collection, 3));
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(found.Ref.data()[0], 'c');
        UNIT_ASSERT_VALUES_EQUAL(newer.Version(), old.Version());
    }

    struct TPageAttachmentFixture : TFixture {
        TPageAttachmentFixture()
            : TFixture(6, 3)
        {
            const TLogoBlobID id(111, 112, 153);
            UNIT_ASSERT(Cache->FindOrInsert({ .Id = id }, Item, Owner) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(Cache->MakeReady(Item, MakeCollection(id), Owner));
            UNIT_ASSERT(Cache->SetCollectionPagesCacheMode(Item, ECacheMode::Regular));
        }

        TCollectionCacheItem Item;
        TTestCacheCollectionRef Owner;
    };

    Y_UNIT_TEST(PageSetOwnsOneNativeCollectionAttachment) {
        for (bool keepOwner : { false, true }) {
            TPageAttachmentFixture fixture;
            auto& cache = *fixture.Cache;
            const auto check = [&](ui64 pages, bool attachment, ui32 refs) {
                const auto state = TCacheTestAccess::HandleState(cache, fixture.Item.Index());
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::PageReferences(cache, fixture.Item), pages);
                UNIT_ASSERT_VALUES_EQUAL(state.HasPageAttachment(), attachment);
                UNIT_ASSERT_VALUES_EQUAL(state.Refs(), refs);
            };
            check(0, false, 1);
            std::array<TPageCacheItem, 2> items;
            for (ui32 index = 0; index < items.size(); ++index) {
                TTestCachePageRef page;
                items[index] = InsertOwnedSizedPage(cache, fixture.Item, index, 64, 'p', page);
                page.Drop();
                check(index + 1, true, 1);
            }
            UNIT_ASSERT(cache.DetachCollection(fixture.Item));
            UNIT_ASSERT(!cache.SetCollectionPagesCacheMode(fixture.Item, ECacheMode::Sticky));
            UNIT_ASSERT(!cache.SetCollectionPagesCacheMode(fixture.Item, ECacheMode::TryKeepInMemory));
            if (!keepOwner) {
                fixture.Owner.Drop();
            }
            for (ui32 index = 0; index < items.size(); ++index) {
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, items[index]));
                UNIT_ASSERT(TCacheTestAccess::EraseCold(cache, MakeColdItem(cache, items[index].Index())));
                const ui64 remaining = items.size() - index - 1;
                check(remaining, remaining != 0, keepOwner);
            }
            if (keepOwner) {
                UNIT_ASSERT(TCacheTestAccess::HandleState(cache, fixture.Item.Index()).IsSticky());
            }
            fixture.Owner.Drop();
            check(0, false, 0);
            UNIT_ASSERT(TCacheTestAccess::HandleState(cache, fixture.Item.Index()).IsHot());
        }
    }

    Y_UNIT_TEST(RetiredPageRetainsItsVirtualCollectionAttachment) {
        TPageAttachmentFixture fixture;
        auto& cache = *fixture.Cache;
        TTestCachePageRef owner;
        const auto page = InsertOwnedSizedPage(cache, fixture.Item, 0, 64, 'p', owner);
        UNIT_ASSERT(cache.DetachCollection(fixture.Item));
        fixture.Owner.Drop();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, page));
        UNIT_ASSERT(TCacheTestAccess::EraseCold(cache, MakeColdItem(cache, page.Index())));
        UNIT_ASSERT(TCacheTestAccess::HandleState(cache, page.Index()).IsTombstone());
        const auto state = TCacheTestAccess::HandleState(cache, fixture.Item.Index());
        UNIT_ASSERT(state.IsSticky() && state.HasPageAttachment());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::PageReferences(cache, fixture.Item), 1);
        UNIT_ASSERT_VALUES_EQUAL(owner.data()[0], 'p');
        UNIT_ASSERT_VALUES_EQUAL(owner.size(), 64);
        owner.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::PageReferences(cache, fixture.Item), 0);
        UNIT_ASSERT(TCacheTestAccess::HandleState(cache, fixture.Item.Index()).IsHot());
    }

    Y_UNIT_TEST(NewPageRestoresAttachmentDuringLastPageDestruction) {
        TPageAttachmentFixture fixture;
        auto& cache = *fixture.Cache;
        TTestCachePageRef oldOwner;
        const auto oldPage = InsertOwnedSizedPage(cache, fixture.Item, 0, 64, 'o', oldOwner);
        oldOwner.Drop();

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Item;
            TTestCachePageRef NewOwner{};
            bool Inserted = false;
        } context{ cache, fixture.Item };

        TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint point, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (point == ECacheHookPoint::AfterPageAttachmentWithdrawn &&
                                  item == ctx.Item.CacheItem() && !ctx.Inserted) {
                                  ctx.Inserted = true;
                                  InsertOwnedSizedPage(ctx.Cache, ctx.Item, 1, 64, 'n', ctx.NewOwner);
                              }
                          } };
        TSharedCacheHookGuard guard(cache, hooks);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, oldPage));
        UNIT_ASSERT(TCacheTestAccess::EraseCold(cache, MakeColdItem(cache, oldPage.Index())));
        UNIT_ASSERT(context.Inserted);
        const auto state = TCacheTestAccess::HandleState(cache, fixture.Item.Index());
        UNIT_ASSERT(state.HasPageAttachment());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::PageReferences(cache, fixture.Item), 1);
        UNIT_ASSERT_VALUES_EQUAL(context.NewOwner.data()[0], 'n');
    }

    Y_UNIT_TEST(PageOnlyCollectionShrinkWakeIsOneShot) {
        for (bool releaseBeforeArm : { false, true }) {
            TPageAttachmentFixture fixture;
            auto& cache = *fixture.Cache;
            TTestCachePageRef page;
            InsertOwnedSizedPage(cache, fixture.Item, 0, 64, 'p', page);

            struct TContext {
                TTestCacheCollectionRef& Owner;
                bool ReleaseBeforeArm;
                ui32 Notifications = 0;
            } context{ fixture.Owner, releaseBeforeArm };

            TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint point, TCacheItem) noexcept {
                                  auto& ctx = *static_cast<TContext*>(ptr);
                                  if (point == ECacheHookPoint::BeforeCollectionShrinkWait && ctx.ReleaseBeforeArm) {
                                      ctx.Owner.Drop();
                                  } else if (point == ECacheHookPoint::ResourcesAvailable) {
                                      ++ctx.Notifications;
                                  }
                              } };
            TSharedCacheHookGuard guard(cache, hooks);
            cache.ArmResourceWait(cache.AdmissionGeneration());
            Y_DEFER {
                cache.DisarmResourceWait();
            };
            {
                auto op = cache.BeginOperation();
                UNIT_ASSERT(op.ReclaimShrinkHandle(fixture.Item.Index()) ==
                            (releaseBeforeArm ? EShrinkCutStatus::Progress : EShrinkCutStatus::Blocked));
            }
            if (!releaseBeforeArm) {
                UNIT_ASSERT(TCacheTestAccess::HandleState(cache, fixture.Item.Index()).HasShrinkWaiter());
                fixture.Owner.Drop();
            }
            UNIT_ASSERT_VALUES_EQUAL(context.Notifications, releaseBeforeArm ? 0 : 1);
            UNIT_ASSERT(!TCacheTestAccess::HandleState(cache, fixture.Item.Index()).HasShrinkWaiter());
            const auto generation = cache.AdmissionGeneration();
            cache.ArmResourceWait(generation);
            for (ui32 probe = 0; probe < 3; ++probe) {
                auto op = cache.BeginOperation();
                TOperationItemRef<TTestTraits> ref(op, op.TryAcquireStructural(fixture.Item.CacheItem()));
                UNIT_ASSERT(ref);
            }
            UNIT_ASSERT_VALUES_EQUAL(cache.AdmissionGeneration(), generation);
            UNIT_ASSERT_VALUES_EQUAL(context.Notifications, releaseBeforeArm ? 0 : 1);
        }
    }

    Y_UNIT_TEST(InFlightPagesRetainMetadataAfterCollectionErase) {
        TPageAttachmentFixture fixture;
        auto& cache = *fixture.Cache;
        UNIT_ASSERT(cache.DetachCollection(fixture.Item));
        auto* collection = fixture.Owner.Get();
        const auto id = collection->Id();
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeCollectionEraseClaim, fixture.Item.CacheItem());
        gate.Slots[1].Arm(ECacheHookPoint::BeforePageAttachmentPublished, fixture.Item.CacheItem());
        TSharedCacheHookGuard guard(cache, gate.Hooks);
        bool erased = false;
        TGateThread eraser(gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            erased = cache.EraseCollection(fixture.Item);
        });
        gate.Slots[0].Wait();
        TGateThread adder(gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(cache, 1);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            // The existing owner was acquired before erasure; this is an in-flight admission
            TCacheTestAccess::AddPageReference(cache, *collection);
        });
        gate.Slots[1].Wait();
        // A nonfirst admission can finish while the first is paused before installing the hold
        TCacheTestAccess::AddPageReference(cache, *collection);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::PageReferences(cache, fixture.Item), 2);
        gate.Slots[0].Release();
        eraser.Join();
        UNIT_ASSERT(erased);
        UNIT_ASSERT(TCacheTestAccess::HandleState(cache, fixture.Item.Index()).IsTombstone());
        TTestCacheCollectionRef fresh;
        UNIT_ASSERT(cache.Find(id, fresh) == ECacheResultStatus::Miss);
        gate.Slots[1].Release();
        adder.Join();
        fixture.Owner.Drop();
        const auto state = TCacheTestAccess::HandleState(cache, fixture.Item.Index());
        UNIT_ASSERT(state.IsTombstone() && state.HasPageAttachment());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(collection->Id(), id);
        TCacheTestAccess::DropPageReference(cache, *collection);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::PageReferences(cache, fixture.Item), 1);
        TCacheTestAccess::DropPageReference(cache, *collection);
        UNIT_ASSERT(TCacheTestAccess::HandleState(cache, fixture.Item.Index()).IsFree());
    }

    Y_UNIT_TEST(CollectionLastReleaseTracksDetachEpoch) {
        for (bool detachAndReattach : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(111, 112, 151);
            TCollectionCacheItem item;
            TTestCacheCollectionRef owner;
            UNIT_ASSERT(fixture.Cache->FindOrInsert({ .Id = id }, item, owner) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id), owner));
            owner->EnsureActorState();

            struct TObservation {
                TTestSharedCache* Cache;
                TCollection* Collection;
                TLogoBlobID Id;
                TCollectionCacheItem Item;
                bool DetachAndReattach;
                bool Changed = false;
                bool RestoredState = false;
                ui32 Notifications = 0;
            } observation{ fixture.Cache.Get(), owner.Get(), id, item, detachAndReattach };

            TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem item) noexcept {
                                  auto& state = *static_cast<TObservation*>(context);
                                  if (item != state.Item.CacheItem()) {
                                      return;
                                  }
                                  if (point == ECacheHookPoint::CollectionReleased) {
                                      ++state.Notifications;
                                  } else if (point == ECacheHookPoint::BeforeLastCollectionRefRelease &&
                                             state.DetachAndReattach && !state.Changed) {
                                      state.Changed = true;
                                      const auto before = TCacheTestAccess::HandleState(*state.Cache, item.Index());
                                      state.Collection->ResetActorState();
                                      Y_ABORT_UNLESS(state.Cache->DetachCollection(state.Item));
                                      TTestCacheCollectionRef joined;
                                      Y_ABORT_UNLESS(state.Cache->FindCollection(state.Id, joined) ==
                                                     ECacheCollectionLookupStatus::Hit);
                                      joined.Drop();
                                      state.RestoredState =
                                          TCacheTestAccess::HandleState(*state.Cache, item.Index()).Raw() ==
                                          before.Raw();
                                  }
                              } };
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            owner.Drop();
            UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, detachAndReattach ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(observation.Changed, detachAndReattach);
            UNIT_ASSERT_VALUES_EQUAL(observation.RestoredState, detachAndReattach);

            TTestCacheCollectionRef cleanup;
            UNIT_ASSERT(fixture.Cache->Find(id, cleanup) == ECacheResultStatus::Hit);
            cleanup->ResetActorState();
            UNIT_ASSERT(fixture.Cache->TryDetachReleasedCollection(cleanup));
            cleanup.Drop();
            UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, detachAndReattach ? 1 : 0);
        }
    }

    TCacheKey ReviewKeyForBucket(
        TCollectionCacheItem collection, ui32 mask, ui32 bucket, ui64 startOffset, const TCacheKey* after = nullptr) {
        for (ui64 offset = startOffset; offset - startOffset < (1u << 20); ++offset) {
            const auto key = TCacheKey::Page(collection, offset);
            if ((key.Hash() & mask) == bucket && (!after || CompareSharedCacheKeys(*after, key) < 0)) {
                return key;
            }
        }
        UNIT_FAIL("Could not construct the review test's bucket key");
        Y_UNREACHABLE();
    }

    void InsertRetriesCompletingFetch(bool ready) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(83, 84);
        const ui32 bucketMask = static_cast<ui32>(CurrentBucketCount(*fixture.Space) - 1);
        const auto sourceKey = ReviewKeyForBucket(collection, bucketMask, 0, 1000);
        const auto successorKey = ReviewKeyForBucket(collection, bucketMask, 0, sourceKey.Word(1) + 1, &sourceKey);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> readyCount = 0;
        std::atomic<ui64> completedItem = 0;
        TIntrusivePtr<TPageFetchWaiter> waiter = new TTestPageFetchWaiter(completed, readyCount, completedItem);
        TTestCachePageRef sourceHit;
        TTestPageFetch fetch;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(sourceKey.Word(1)),
                        EStickyState::None, waiter, sourceHit, fetch) == ECacheResultStatus::Inserted);
        const TPageCacheItem source = fetch.CacheItem();

        struct TCompletionGate {
            TSharedCacheGate Gate;
            TCacheItem Source;
            std::atomic<ui32> Attempts = 0;
            std::atomic<bool> Retried = false;

            static void Invoke(void* context, ECacheHookPoint point, TCacheItem item) noexcept {
                auto& self = *static_cast<TCompletionGate*>(context);
                if (item != self.Source) {
                    return;
                }
                if (point == ECacheHookPoint::AfterFetchCompletingClaim) {
                    self.Gate.Slots[0].TryInvoke(point, item);
                } else if (point == ECacheHookPoint::BeforeInsertOwnerAcquire) {
                    if (self.Attempts.fetch_add(1, std::memory_order_relaxed) == 1) {
                        self.Retried.store(true, std::memory_order_relaxed);
                        self.Gate.Slots[1].Block();
                    }
                } else if (point == ECacheHookPoint::AfterInsertPositionFound) {
                    self.Gate.Slots[1].Block();
                }
            }
        } completionGate;

        completionGate.Source = source.CacheItem();
        auto& gate = completionGate.Gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterFetchCompletingClaim, source.CacheItem());
        gate.Slots[1].Arm(ECacheHookPoint::AfterInsertPositionFound, source.CacheItem());
        TTestTraits hooks{ &completionGate, &TCompletionGate::Invoke };
        TSharedCacheHookGuard hookGuard(*fixture.Cache, hooks);
        auto cache = fixture.Cache;
        bool finished = false;
        TGateThread finisher(gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            finished = ready ? fetch.MakeReady(MakePageData(source.Index())) : fetch.FailReady();
        });
        gate.Slots[0].Wait();

        ECacheResultStatus insertStatus = ECacheResultStatus::Miss;
        TPageCacheItem successor;
        TTestCachePageRef successorHit;
        TGateThread inserter(gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*cache, 1);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            insertStatus = FindOrInsertPage(*cache, collection, MakePageLocation(successorKey.Word(1)),
                EStickyState::None, successor, successorHit);
        });
        gate.Slots[1].Wait();

        // A completing predecessor must be retried without taking another structural reference
        UNIT_ASSERT(completionGate.Retried.load(std::memory_order_relaxed));
        const THandleState completing = TCacheTestAccess::HandleState(*cache, source.Index());
        UNIT_ASSERT(completing.IsCompleting());
        UNIT_ASSERT_VALUES_EQUAL(completing.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 0);

        gate.Slots[0].Release();
        finisher.Join();
        UNIT_ASSERT(finished);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 1);
        UNIT_ASSERT_VALUES_EQUAL(readyCount.load(std::memory_order_relaxed), ready ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), source.CacheItem().Raw());

        gate.Slots[1].Release();
        inserter.Join();
        UNIT_ASSERT(insertStatus == ECacheResultStatus::Inserted);
        UNIT_ASSERT(successor);
        UNIT_ASSERT(!successorHit);
        UNIT_ASSERT(cache->MakeReady(successor, MakePageData(successor.Index())));
        AssertPageHit(*cache, successorKey, successor);
        if (ready) {
            AssertPageHit(*cache, sourceKey, source);
        } else {
            UNIT_ASSERT(FindPage(*cache, sourceKey).Status == ECacheResultStatus::Miss);
        }
        UNIT_ASSERT_VALUES_EQUAL(cache->ReservedBytes(), 0);
    }

    TVector<TTestCachePageRef> HoldReviewFillers(TFixture & fixture, ui32 excludedMask, ui32 excludedBucket) {
        TVector<TTestCachePageRef> held;
        const auto collection = MakeCollectionCacheItem(100, 50000);
        for (ui64 offset = 0; AvailableFreeHandles(*fixture.Cache) != 0; ++offset) {
            UNIT_ASSERT(offset < (1u << 20));
            const auto key = TCacheKey::Page(collection, offset);
            if ((key.Hash() & excludedMask) == excludedBucket) {
                continue;
            }
            InsertReadyPage(*fixture.Cache, key);
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
            held.push_back(std::move(found.Ref));
        }
        return held;
    }

    void AssertReviewTerminal(TTestSharedCache & cache, TCacheItem owner, const char* finding) {
        const auto terminal = TCacheItem::FromRaw(TCacheTestAccess::HandleNext(cache, owner.Index()));
        UNIT_ASSERT(terminal.IsNull());
        // Check before Find: the broken terminal would make a miss lookup retry forever.
        UNIT_ASSERT_C(TCacheTestAccess::ValidateTerminalOwner(cache, owner, terminal),
            TStringBuilder() << finding << ": terminal version " << terminal.Version() << " differs from owner version "
                             << owner.Version());
    }

    void ReviewEmptyMarkerAfterReuse(bool grow) {
        TFixture fixture(6, 8, grow ? 7 : 6);
        TTransition growth;
        if (grow) {
            UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, grow ? 7 : 5));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        if (!grow) {
            UNIT_ASSERT(TCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        }
        UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
        const auto reused = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        UNIT_ASSERT(reused.IsFree());
        UNIT_ASSERT(reused.Version() != 0);
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        const auto markerState = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        const auto marker = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);
        AssertReviewTerminal(*fixture.Cache, marker, "R1");
        const auto resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
        const auto missing = ReviewKeyForBucket(MakeCollectionCacheItem(101, 50001), (resize.SplitBit() << 1) - 1,
            resize.Cursor() | resize.SplitBit(), 10000);
        UNIT_ASSERT(FindPage(*fixture.Cache, missing).Status == ECacheResultStatus::Miss);
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        if (grow) {
            CommitTransition(*fixture.Cache, growth);
        }
    }

    Y_UNIT_TEST(ReviewR1GrowthEmptyMarkerTerminal) {
        ReviewEmptyMarkerAfterReuse(true);
    }

    Y_UNIT_TEST(ReviewR1ShrinkEmptyMarkerTerminal) {
        ReviewEmptyMarkerAfterReuse(false);
    }

    Y_UNIT_TEST(ReviewR2MergePreservesRecycledTailTerminal) {
        TFixture fixture;
        const auto token = MakeCollectionCacheItem(102, 50002);
        const auto key = ReviewKeyForBucket(token, 63, 0, 20000);
        const auto original = InsertReadyPage(*fixture.Cache, key);
        auto originalHold = FindPage(*fixture.Cache, key);
        auto fillers = HoldReviewFillers(fixture, 31, 0);
        originalHold.Ref.Drop();
        if (TCacheTestAccess::HandleState(*fixture.Cache, original.Index()).IsHot()) {
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, original));
        }
        UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, original.Index())));
        const auto tail = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT_VALUES_EQUAL(tail.Index(), original.Index());
        UNIT_ASSERT(tail.Version() != original.Version());
        UNIT_ASSERT(tail.Version() != 0);
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 5));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
        AssertReviewTerminal(*fixture.Cache, tail.CacheItem(), "R2");
        const auto missing = ReviewKeyForBucket(token, 31, 0, 30000, &key);
        UNIT_ASSERT(FindPage(*fixture.Cache, missing).Status == ECacheResultStatus::Miss);
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
    }

    Y_UNIT_TEST(ReviewR3RelocatedTailUsesReplacementVersion) {
        TFixture fixture;
        const auto token = MakeCollectionCacheItem(103, 50003);
        const auto sourceKey = ReviewKeyForBucket(token, 63, 0, 40000);
        const auto spareKey = ReviewKeyForBucket(token, 63, 1, 50000);
        const auto spare = InsertReadyPage(*fixture.Cache, spareKey);
        auto spareHold = FindPage(*fixture.Cache, spareKey);
        const auto source = InsertReadyPage(*fixture.Cache, sourceKey);
        auto sourceHold = FindPage(*fixture.Cache, sourceKey);
        auto fillers = HoldReviewFillers(fixture, 63, 0);
        spareHold.Ref.Drop();
        if (TCacheTestAccess::HandleState(*fixture.Cache, spare.Index()).IsHot()) {
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, spare));
        }
        UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, spare.Index())));
        UNIT_ASSERT(TCacheTestAccess::RelocatePage(*fixture.Cache, source));
        const auto replacement =
            TCacheItem::FromRaw(TCacheTestAccess::HandleNext(*fixture.Cache, source.Index())).WithoutFrozen();
        UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), spare.Index());
        UNIT_ASSERT(replacement.Version() != source.Version());
        AssertReviewTerminal(*fixture.Cache, replacement, "R3");
        const auto missing = ReviewKeyForBucket(token, 63, 0, 60000, &sourceKey);
        UNIT_ASSERT(FindPage(*fixture.Cache, missing).Status == ECacheResultStatus::Miss);
    }

    void ReviewRelocationLosesToDeletion(bool reclaimDuringAllocation) {
        TFixture fixture;
        const TLogoBlobID id(151, 152, 153);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto sourceKey = TCacheKey::Page(collection, 1);
        const auto source = InsertReadyPage(*fixture.Cache, sourceKey);
        auto sourceHold = FindPage(*fixture.Cache, sourceKey);
        TPageCacheItem spare;
        TPageResult spareHold;
        TVector<TTestCachePageRef> fillers;
        if (reclaimDuringAllocation) {
            const auto spareKey = TCacheKey::Page(collection, 2);
            spare = InsertReadyPage(*fixture.Cache, spareKey);
            spareHold = FindPage(*fixture.Cache, spareKey);
            fillers = HoldReviewFillers(fixture, 63, sourceKey.Hash() & 63);
        }
        sourceHold.Ref.Drop();
        if (TCacheTestAccess::HandleState(*fixture.Cache, source.Index()).IsHot()) {
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, source));
        }
        const auto coldItem = MakeColdItem(*fixture.Cache, source.Index());
        auto structural = TCacheTestAccess::AcquireStructural(*fixture.Cache, source.CacheItem());
        UNIT_ASSERT(structural);
        if (reclaimDuringAllocation) {
            spareHold.Ref.Drop();
            if (TCacheTestAccess::HandleState(*fixture.Cache, spare.Index()).IsHot()) {
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, spare));
            }
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        }
        const ui64 freeBefore = AvailableFreeHandles(*fixture.Cache);
        const ui64 residentBefore = fixture.Cache->ResidentBytes();
        const ui32 ownersBefore = TCacheTestAccess::CollectionReferences(*fixture.Cache, collection);

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TColdCacheItem ColdItem;
            bool ReclaimDuringAllocation;
            bool Visited = false;
            bool Deleted = false;
        } context{ *fixture.Cache, collection, coldItem, reclaimDuringAllocation };

        TTestTraits hooks{ &context,
            [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (ctx.ReclaimDuringAllocation) {
                    if (at == ECacheHookPoint::AfterTombstoneOwnerClaim && item.Index() == ctx.ColdItem.Index()) {
                        ctx.Deleted = true;
                    }
                } else if (at == ECacheHookPoint::AfterPageCollectionStateChecked &&
                           item == ctx.Collection.CacheItem() && !ctx.Visited) {
                    ctx.Visited = true;
                    ctx.Deleted = TCacheTestAccess::EraseCold(ctx.Cache, ctx.ColdItem);
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(!TCacheTestAccess::RelocatePage(*fixture.Cache, source));
        UNIT_ASSERT(context.Deleted);
        const auto state = TCacheTestAccess::HandleState(*fixture.Cache, source.Index());
        UNIT_ASSERT(state.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
        UNIT_ASSERT(FindPage(*fixture.Cache, sourceKey).Status == ECacheResultStatus::Miss);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->ResidentBytes(), residentBefore - (reclaimDuringAllocation ? bytes : 0));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection),
            ownersBefore - (reclaimDuringAllocation ? 1 : 0));
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freeBefore + (reclaimDuringAllocation ? 1 : 0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        structural.Drop();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, source.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->ResidentBytes(), residentBefore - (reclaimDuringAllocation ? 2 * bytes : bytes));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection),
            ownersBefore - (reclaimDuringAllocation ? 2 : 1));
    }

    Y_UNIT_TEST(ReviewR10DeletionDuringReplacementPreparation) {
        ReviewRelocationLosesToDeletion(false);
    }

    Y_UNIT_TEST(ReviewR10AllocationReclaimsRelocationSource) {
        ReviewRelocationLosesToDeletion(true);
    }

    Y_UNIT_TEST(ReviewR10RelocationPreservesConcurrentSuccessorInsertion) {
        TFixture fixture;
        const auto collection = MakeCollectionCacheItem(151, 50051);
        const auto keys = MakeAdjacentKeys(*fixture.Space, collection, 100000);
        const auto source = InsertReadyPage(*fixture.Cache, keys[0]);
        auto sourceHold = FindPage(*fixture.Cache, keys[0]);
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeTableLinkCas, source.CacheItem());
        gate.Slots[1].Arm(ECacheHookPoint::BeforePageReplacementLinkCas, source.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        TPageCacheItem inserted;
        TGateThread insertion(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            inserted = InsertReadyPage(*fixture.Cache, keys[1]);
        });
        gate.Slots[0].Wait();
        bool relocated = false;
        TGateThread relocation(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            relocated = TCacheTestAccess::RelocatePage(*fixture.Cache, source);
        });
        gate.Slots[1].Wait();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, source.Index()).IsReplacing());
        UNIT_ASSERT(!TCacheItem::FromRaw(TCacheTestAccess::HandleNext(*fixture.Cache, source.Index())).IsFrozen());
        gate.Slots[0].Release();
        insertion.Join();
        UNIT_ASSERT(
            TCacheItem::FromRaw(TCacheTestAccess::HandleNext(*fixture.Cache, source.Index())) == inserted.CacheItem());
        gate.Slots[1].Release();
        relocation.Join();
        UNIT_ASSERT(relocated);
        auto replacement = FindPage(*fixture.Cache, keys[0]);
        UNIT_ASSERT(replacement.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(replacement.Ref.CacheItem() != source);
        UNIT_ASSERT_VALUES_EQUAL(replacement.Ref.data(), sourceHold.Ref.data());
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertReviewTerminal(*fixture.Cache, inserted.CacheItem(), "R10");
    }

    void ReviewHeldPageWaitsForForwarder(bool querySticky) {
        TFixture fixture;
        const TLogoBlobID id(151, 152, 154);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto key = TCacheKey::Page(collection, 1);
        const auto source = InsertReadyPage(*fixture.Cache, key);
        auto sourceHold = FindPage(*fixture.Cache, key);
        if (querySticky) {
            UNIT_ASSERT(TCacheTestAccess::MakePageSticky(*fixture.Cache, source));
            UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*fixture.Cache, source, source.Index()));
        }
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforePageReplacementLinkCas, source.CacheItem());
        gate.Slots[1].Arm(ECacheHookPoint::BeforePageReplacementRetry, source.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        bool relocated = false;
        TGateThread relocation(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            relocated = TCacheTestAccess::RelocatePage(*fixture.Cache, source);
        });
        gate.Slots[0].Wait();
        bool acquired = false;
        TTestCachePageRef page;
        TGateThread reader(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            acquired =
                querySticky ? sourceHold.Ref.IsSticky() : TCacheTestAccess::AcquirePage(*fixture.Cache, source, page);
        });
        gate.Slots[1].Wait();
        gate.Slots[0].Release();
        relocation.Join();
        gate.Slots[1].Release();
        reader.Join();
        UNIT_ASSERT(relocated);
        UNIT_ASSERT(acquired);
        if (!querySticky) {
            UNIT_ASSERT(page.CacheItem() != source);
            UNIT_ASSERT_VALUES_EQUAL(page.data(), sourceHold.Ref.data());
        }
    }

    Y_UNIT_TEST(ReviewR10AcquirePageWaitsForForwarder) {
        ReviewHeldPageWaitsForForwarder(false);
    }

    Y_UNIT_TEST(ReviewR10StickyQueryWaitsForForwarder) {
        ReviewHeldPageWaitsForForwarder(true);
    }

    void ReviewBatchDetach(ECacheHookPoint point) {
        TFixture fixture;
        const TLogoBlobID id(111, 112, 113);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        struct TContext {
            TFixture& Fixture;
            TCollectionCacheItem Collection;
            ECacheHookPoint Point;
            bool Detached = false;
        } context{ fixture, collection, point };

        TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (at == ctx.Point && item == ctx.Collection.CacheItem() && !ctx.Detached) {
                                  Y_ABORT_UNLESS(TCacheTestAccess::CollectionReferences(
                                                     *ctx.Fixture.Cache, ctx.Collection) == 2);
                                  ctx.Detached = true;
                                  Y_ABORT_UNLESS(TCacheTestAccess::DetachCollection(
                                      *ctx.Fixture.Cache, ctx.Fixture.Registry, ctx.Collection));
                              }
                          } };
        TSharedCacheHookGuard hookGuard(*fixture.Cache, hooks);
        TTestCachePageRequest request(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        const bool admitted = fixture.Cache->FindOrInsertBatch(collection, { &request, 1 });
        UNIT_ASSERT(context.Detached);
        UNIT_ASSERT(admitted);
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(request.Fetch().CacheItem().Index())));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR4BatchSurvivesDetachAfterPreparation) {
        ReviewBatchDetach(ECacheHookPoint::AfterPageBatchPrepared);
    }

    Y_UNIT_TEST(ReviewR4BatchSurvivesDetachBeforeOwnerIncrement) {
        ReviewBatchDetach(ECacheHookPoint::AfterPageCollectionStateChecked);
    }

    Y_UNIT_TEST(ReviewR4BatchRejectsDetachedCollection) {
        TFixture fixture;
        const TLogoBlobID id(111, 112, 114);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        TTestCachePageRequest request(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR4RejectedBatchReleasesCollectionOwner) {
        TFixture fixture;
        const TLogoBlobID id(111, 112, 115);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        TTestCachePageRequest request(
            MakePageLocation(1, fixture.Cache->HardLimit()), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR5CapTrimErasesOrdinaryKeepCold) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        ConfigureKeep(*fixture.Cache, 2 * bytes);
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(121, 122, 125), ECacheMode::TryKeepInMemory);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(0));
        for (ui32 step = 0; step < 512 && fixture.Cache->KeepOwnedBytes(); ++step) {
            fixture.Cache->RunMaintenance();
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 0);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ReadyPageBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR5DemotionKeepsOriginalPageUntilAccounting) {
        TFixture fixture;
        const TLogoBlobID aId(121, 122, 123);
        const TLogoBlobID bId(124, 125, 126);
        const auto a = AllocateCollection(*fixture.Cache, TCacheKey::Collection(aId));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, a, MakeCollection(aId)));
        const auto b = AllocateCollection(*fixture.Cache, TCacheKey::Collection(bId));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, b, MakeCollection(bId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(a, ECacheMode::TryKeepInMemory));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(2 * bytes));
        const auto sourceKey = TCacheKey::Page(a, 1);
        const auto source = InsertReadyPage(*fixture.Cache, sourceKey);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, source));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, source.Index()).IsKeepCold());
        auto fillers = HoldReviewFillers(fixture, 63, sourceKey.Hash() & 63);
        const auto token = MakeColdItem(*fixture.Cache, source.Index());
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeColdEraseClaim, token);
        gate.Slots[1].Arm(ECacheHookPoint::AfterPageRetierClaim, source.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool reclaimed = false;
        TGateThread consumer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            reclaimed = TCacheTestAccess::WithSpace(*cache, [&](TSpaceOperation& op) {
                return TCacheTestAccess::ReclaimKeepCold(*cache, op);
            });
        });
        gate.Slots[0].Wait();
        bool demoted = false;
        TGateThread demoter(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            demoted = TCacheTestAccess::RetierColdToRegular(*cache, token);
        });
        gate.Slots[1].Wait();
        gate.Slots[0].Release();
        consumer.Join();
        UNIT_ASSERT(!reclaimed);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, source.Index()).IsCold());
        UNIT_ASSERT(TCacheTestAccess::WithSpace(*cache, [&](TSpaceOperation& op) {
            return TCacheTestAccess::BindOperation(*cache, op).Eviction().EraseCold(token, ECacheTier::Regular);
        }));
        const auto state = TCacheTestAccess::HandleState(*cache, source.Index());
        const bool reusedEarly = state.IsFree();
        TPageCacheItem replacement;
        TTestCachePageRef replacementHold;
        ui32 replacementMembership = 0;
        const auto replacementKey = TCacheKey::Page(b, 1);
        if (reusedEarly) {
            replacement = InsertReadyPage(*cache, replacementKey);
            UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), source.Index());
            UNIT_ASSERT(replacement.Version() != source.Version());
            auto found = FindPage(*cache, replacementKey);
            replacementHold = std::move(found.Ref);
            replacementMembership = TCacheTestAccess::ColdVersion(*cache, replacement.Index());
        } else {
            UNIT_ASSERT(state.IsTombstone() && state.Refs() != 0);
        }
        gate.Slots[1].Release();
        demoter.Join();
        UNIT_ASSERT(demoted);
        if (!reusedEarly) {
            replacement = InsertReadyPage(*cache, replacementKey);
            UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), source.Index());
            auto found = FindPage(*cache, replacementKey);
            replacementHold = std::move(found.Ref);
            replacementMembership = TCacheTestAccess::ColdVersion(*cache, replacement.Index());
        }
        UNIT_ASSERT_C(TCacheTestAccess::ColdVersion(*cache, replacement.Index()) == replacementMembership,
            "R5: demotion changed the replacement page's membership generation");
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionValue(*cache, a.Index())->ReadyPageBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR7LastColdPublicationDoesNotTouchReusedHandle) {
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            TFixture fixture;
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            ConfigureKeep(*fixture.Cache, 4 * bytes);
            const auto a = CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(121, 122, 134),
                tier == ECacheTier::Keep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular);
            const auto b = CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(121, 122, 135),
                tier == ECacheTier::Keep ? ECacheMode::Regular : ECacheMode::TryKeepInMemory);
            const auto sourceKey = TCacheKey::Page(a, 1);
            const auto source = InsertReadyPage(*fixture.Cache, sourceKey);
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, source));
            auto held = TCacheTestAccess::AcquireStructural(*fixture.Cache, source.CacheItem());
            UNIT_ASSERT(held);
            auto fillers = HoldReviewFillers(fixture, 63, sourceKey.Hash() & 63);

            struct TContext {
                TTestSharedCache& Cache;
                TPageCacheItem Source;
                TCollectionCacheItem ReplacementCollection;
                ECacheTier Tier;
                bool Visited = false;
                TPageCacheItem Replacement = {};
                ui32 ReplacementMembership = 0;
            } context{ *fixture.Cache, source, b, tier };

            TTestTraits hooks{ &context,
                [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                    auto& ctx = *static_cast<TContext*>(ptr);
                    if (at == ECacheHookPoint::AfterLastColdRefReleased && item == ctx.Source.CacheItem() &&
                        !ctx.Visited) {
                        ctx.Visited = true;
                        Y_ABORT_UNLESS(TCacheTestAccess::WithSpace(ctx.Cache, [&](TSpaceOperation& op) {
                            return TCacheTestAccess::BindOperation(ctx.Cache, op)
                                .Eviction()
                                .EraseCold(MakeColdItem(ctx.Cache, ctx.Source.Index()), ctx.Tier);
                        }));
                        ctx.Replacement = InsertReadyPage(ctx.Cache, TCacheKey::Page(ctx.ReplacementCollection, 1));
                        Y_ABORT_UNLESS(ctx.Replacement.Index() == ctx.Source.Index());
                        Y_ABORT_UNLESS(TCacheTestAccess::EvictFromHot(ctx.Cache, ctx.Replacement));
                        ctx.ReplacementMembership = TCacheTestAccess::ColdVersion(ctx.Cache, ctx.Replacement.Index());
                    }
                } };
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            held.Drop();
            UNIT_ASSERT(context.Visited);
            UNIT_ASSERT(context.Replacement.Version() != source.Version());
            const auto state = TCacheTestAccess::HandleState(*fixture.Cache, context.Replacement.Index());
            UNIT_ASSERT(state.IsColdInTier(tier == ECacheTier::Keep ? ECacheTier::Regular : ECacheTier::Keep));
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdVersion(*fixture.Cache, context.Replacement.Index()),
                context.ReplacementMembership);
            UNIT_ASSERT_VALUES_EQUAL(
                TCacheTestAccess::CollectionValue(*fixture.Cache, b.Index())->ReadyPageBytes(), bytes);
        }
    }

    void ReviewColdPromotion(bool reheat) {
        TFixture fixture;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(fixture.Cache->HardLimit()));
        const TLogoBlobID id(141, 142, 143);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(2 * bytes));
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 1);
        const auto coldItem = MakeColdItem(*fixture.Cache, page.Index());
        auto held = TCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem());
        UNIT_ASSERT(held);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()), coldItem.ColdVersion());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Page;
            bool Reheat;
            bool Visited = false;
            bool Reclaimed = false;
            ui32 RefsDuringPromotion = 0;
            TTestCachePageRef Reader = {};
        } context{ *fixture.Cache, collection, page, reheat };

        TTestTraits hooks{ &context,
            [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (at == ECacheHookPoint::AfterPageRetierClaim && item == ctx.Page.CacheItem() && !ctx.Visited) {
                    ctx.Visited = true;
                    ctx.RefsDuringPromotion = TCacheTestAccess::HandleState(ctx.Cache, item.Index()).Refs();
                    if (ctx.Reheat) {
                        Y_ABORT_UNLESS(ctx.Cache.Find(ctx.Collection, 1, ctx.Reader) == ECacheResultStatus::Hit);
                    }
                    ctx.Reclaimed = TCacheTestAccess::WithSpace(ctx.Cache, [&](TSpaceOperation& op) {
                        return TCacheTestAccess::ReclaimCold(ctx.Cache, op);
                    });
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        held.Drop();
        UNIT_ASSERT(context.Visited);
        UNIT_ASSERT_VALUES_EQUAL(context.RefsDuringPromotion, 1);
        UNIT_ASSERT(!context.Reclaimed);
        const auto state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), page.Version());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), reheat ? 1 : 0);
        UNIT_ASSERT(reheat ? state.IsHot() : state.IsKeepCold());
        const ui64 keepBytes = reheat ? 0 : bytes;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdBytes(), keepBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdPages(), reheat ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), reheat ? bytes : 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), reheat ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), reheat ? bytes : 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), keepBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), reheat ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ReadyPageBytes(), bytes);
        if (!reheat) {
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        }
    }

    Y_UNIT_TEST(ReviewR8PromotionPinsPageAgainstOldColdToken) {
        ReviewColdPromotion(false);
    }

    Y_UNIT_TEST(ReviewR8ReheatOvertakesPromotionAccounting) {
        ReviewColdPromotion(true);
    }

    Y_UNIT_TEST(ShutdownReleasesPageBuffersBeforeCollectionMetadata) {
        bool pagesReleasedBeforeCollection = false;
        {
            TFixture fixture;
            const TLogoBlobID id(37, 38, 39);
            const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(collection);
            auto data = MakePageData(1);
            auto metadata = TIntrusiveConstPtr<NPageCollection::IPageCollection>(
                new TTestPageCollection(id, data, pagesReleasedBeforeCollection));
            UNIT_ASSERT(TCacheTestAccess::MakeReady(
                *fixture.Cache, fixture.Registry, collection, MakeHolder<TCollection>(std::move(metadata))));
            const auto page = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 1), data.size());
            UNIT_ASSERT(page);
            // Handle-order teardown would destroy this metadata before releasing its page buffer.
            UNIT_ASSERT(collection.Index() < page.Index());
            UNIT_ASSERT(fixture.Cache->MakeReady(page, std::move(data)));
        }
        UNIT_ASSERT(pagesReleasedBeforeCollection);
    }

    void CheckHotResizeAdmissionNotification(bool armBeforeUnlock) {
        ui32 notifications = 0;
        TFixture fixture;
        TTestTraits hooks{ &notifications, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        auto owner = TCacheTestAccess::LockHotResize(*fixture.Cache);
        UNIT_ASSERT(owner);
        UNIT_ASSERT(!TCacheTestAccess::LockHotResize(*fixture.Cache));
        UNIT_ASSERT(!TCacheTestAccess::LockHotResize(*fixture.Cache));
        if (armBeforeUnlock) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        owner = {};
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(notifications, armBeforeUnlock ? 1 : 0);
        if (!armBeforeUnlock) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);

        fixture.Cache->ArmResourceWait(fixture.Cache->AdmissionGeneration());
        {
            auto uncontended = TCacheTestAccess::LockHotResize(*fixture.Cache);
            UNIT_ASSERT(uncontended);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        fixture.Cache->DisarmResourceWait();
    }

    Y_UNIT_TEST(HotResizeUnlockNotifiesArmedAdmissionWaiter) {
        CheckHotResizeAdmissionNotification(true);
    }

    Y_UNIT_TEST(HotResizeUnlockBeforeAdmissionArmDoesNotLoseWakeup) {
        CheckHotResizeAdmissionNotification(false);
    }

    enum class ECollectionPublication {
        Ready,
        Attach,
        Detach,
    };

    void CheckCollectionAdmissionNotification(ECollectionPublication publication, bool armBeforePublication) {
        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            TCollectionCacheItem Item;
            TLogoBlobID Id;
            bool Attached = true;
            ui32 Notifications = 0;
            bool Published = true;
        } observation;

        TFixture fixture;
        const TLogoBlobID id(205, 206, 1);
        const TCollectionLocation location{ .Id = id, .BackingSize = 0 };
        TCollectionCacheItem item;
        TTestCacheCollectionRef held;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(location, item, held) == ECacheResultStatus::Inserted);
        if (publication != ECollectionPublication::Ready) {
            UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->Find(id, held) == ECacheResultStatus::Hit);
            if (publication == ECollectionPublication::Attach) {
                UNIT_ASSERT(fixture.Cache->DetachCollection(item));
            }
        } else {
            TCollectionCacheItem pending;
            UNIT_ASSERT(fixture.Cache->FindOrInsert(location, pending, held) == ECacheResultStatus::Pending);
        }
        observation.Cache = fixture.Cache.Get();
        observation.Item = item;
        observation.Id = id;
        observation.Attached = publication != ECollectionPublication::Detach;
        TTestTraits hooks{ &observation,
            [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                if (point != ECacheHookPoint::ResourcesAvailable) {
                    return;
                }
                auto& observation = *static_cast<TObservation*>(context);
                ++observation.Notifications;
                const auto state = TCacheTestAccess::HandleState(*observation.Cache, observation.Item.Index());
                const bool stable =
                    state.IsReady() && (observation.Attached ? state.IsStickyField() : state.IsStickyNoneField());
                TTestCacheCollectionRef found;
                observation.Published &= stable &&
                                         observation.Cache->Find(observation.Id, found) == ECacheResultStatus::Hit &&
                                         found.CacheItem() == observation.Item;
            } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        switch (publication) {
            case ECollectionPublication::Ready:
                UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id)));
                break;
            case ECollectionPublication::Attach: {
                TCollectionCacheItem inserted;
                TTestCacheCollectionRef hit;
                UNIT_ASSERT(fixture.Cache->FindOrInsert(location, inserted, hit) == ECacheResultStatus::Hit);
                break;
            }
            case ECollectionPublication::Detach:
                UNIT_ASSERT(fixture.Cache->DetachCollection(item));
                break;
        }
        UNIT_ASSERT(fixture.Cache->AdmissionGeneration() > generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforePublication ? 1 : 0);
        if (!armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published);

        const ui64 stableGeneration = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(stableGeneration);
        if (publication == ECollectionPublication::Detach) {
            UNIT_ASSERT(!fixture.Cache->DetachCollection(item));
        } else {
            TCollectionCacheItem inserted;
            TTestCacheCollectionRef hit;
            UNIT_ASSERT(fixture.Cache->FindOrInsert(location, inserted, hit) == ECacheResultStatus::Hit);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), stableGeneration);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        fixture.Cache->DisarmResourceWait();
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
    }

    Y_UNIT_TEST(EmptyReservationRefundDoesNotWakeAdmissionWaiter) {
        TFixture fixture;
        ui32 notifications = 0;
        TTestTraits hooks{ &notifications, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        ui32 expected = 0;
        for (auto [bytes, pages] : { std::pair<ui64, ui64>{ 0, 0 }, { 1, 0 }, { 0, 1 } }) {
            const ui64 generation = fixture.Cache->AdmissionGeneration();
            fixture.Cache->ArmResourceWait(generation);
            UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes, pages));
            TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes, pages);
            expected += bytes != 0 || pages != 0;
            UNIT_ASSERT_VALUES_EQUAL(notifications, expected);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + (bytes != 0 || pages != 0));
        }
    }

    Y_UNIT_TEST(CollectionReadinessNotifiesAdmissionWaiter) {
        for (bool armed : { false, true }) {
            CheckCollectionAdmissionNotification(ECollectionPublication::Ready, armed);
        }
    }

    Y_UNIT_TEST(CollectionAttachmentNotifiesAdmissionWaiter) {
        for (bool armed : { false, true }) {
            CheckCollectionAdmissionNotification(ECollectionPublication::Attach, armed);
        }
    }

    Y_UNIT_TEST(CollectionDetachmentNotifiesAdmissionWaiter) {
        for (bool armed : { false, true }) {
            CheckCollectionAdmissionNotification(ECollectionPublication::Detach, armed);
        }
    }

    void CheckShutdownResourceNotification(bool sticky) {
        ui32 notifications = 0;
        TFixture fixture;
        const TLogoBlobID id(37, 38, 40);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(
            fixture.Cache->SetCollectionPagesCacheMode(collection, sticky ? ECacheMode::Sticky : ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));

        TTestCachePageRef held;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
        TTestCachePageRef hit;
        TTestPageFetch fetch;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1),
                        sticky ? EStickyState::Sticky : EStickyState::None, waiter, hit,
                        fetch) == ECacheResultStatus::Inserted);
        const TPageCacheItem page = fetch.CacheItem();
        auto data = MakePageData(page.Index());
        auto payload = data;
        UNIT_ASSERT(fetch.MakeReady(std::move(data)));
        UNIT_ASSERT(held);
        const THandleState resident = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(sticky ? resident.IsSticky() : resident.IsCold());
        if (sticky) {
            held.Drop();
        }

        TTestTraits hooks{ &notifications, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        constexpr ui64 reservedBytes = 1;
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, reservedBytes));
        fixture.Cache->ArmResourceWait(fixture.Cache->AdmissionGeneration());
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, reservedBytes);
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT(!payload.IsPrivate());

        fixture.Cache->ArmResourceWait(fixture.Cache->AdmissionGeneration());
        TlsCacheHazard = nullptr;
        held.Drop();
        fixture.Cache.Drop();
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT(payload.IsPrivate());
    }

    Y_UNIT_TEST(ShutdownDoesNotNotifyResourcesForStickyPages) {
        CheckShutdownResourceNotification(true);
    }
    Y_UNIT_TEST(ShutdownDoesNotNotifyResourcesForHeldPages) {
        CheckShutdownResourceNotification(false);
    }

    Y_UNIT_TEST(ReviewR12UnboundFetchDestructionAbandonsWaiters) {
        for (bool dispatched : { false, true }) {
            std::atomic<ui32> completed = 0;
            std::atomic<ui32> ready = 0;
            std::atomic<ui64> completedItem = 0;
            auto first = MakeIntrusive<TTestPageFetchWaiter>(completed, ready, completedItem);
            auto second = MakeIntrusive<TTestPageFetchWaiter>(completed, ready, completedItem);
            TFixture fixture;
            const auto collection = MakeCollectionCacheItem(63, 64);
            const auto location = MakePageLocation(9);
            {
                TTestCachePageRef hit;
                TTestPageFetch fetch;
                TTestPageFetch duplicate;
                UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None,
                                first, hit, fetch) == ECacheResultStatus::Inserted);
                UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None,
                                second, hit, duplicate) == ECacheResultStatus::Pending);
                if (dispatched) {
                    UNIT_ASSERT(fetch.Dispatch());
                }
                TlsCacheHazard = nullptr;
                UNIT_ASSERT(!TTestSharedCache::TryGet());
            }
            UNIT_ASSERT_VALUES_EQUAL(completed.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(first->RefCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(second->RefCount(), 1);
            fixture.Cache.Drop();
            UNIT_ASSERT_VALUES_EQUAL(completed.load(), 0);
        }
    }

    Y_UNIT_TEST(ReviewR12UnboundFetchMoveAssignmentAbandonsPreviousWaiters) {
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        auto firstWaiter = MakeIntrusive<TTestPageFetchWaiter>(completed, ready, completedItem);
        auto secondWaiter = MakeIntrusive<TTestPageFetchWaiter>(completed, ready, completedItem);
        TFixture fixture;
        const auto collection = MakeCollectionCacheItem(64, 65);
        TTestCachePageRef hit;
        TTestPageFetch first;
        TTestPageFetch second;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(10), EStickyState::None,
                        firstWaiter, hit, first) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(11), EStickyState::None,
                        secondWaiter, hit, second) == ECacheResultStatus::Inserted);
        TlsCacheHazard = nullptr;
        first = std::move(second);
        UNIT_ASSERT(first);
        UNIT_ASSERT(!second);
        UNIT_ASSERT_VALUES_EQUAL(firstWaiter->RefCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(secondWaiter->RefCount(), 2);
        first = {};
        UNIT_ASSERT_VALUES_EQUAL(secondWaiter->RefCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 0);
        fixture.Cache.Drop();
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 0);
    }

    Y_UNIT_TEST(ReviewR12FetchOutlivesCacheWithoutCallbacks) {
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        auto waiter = MakeIntrusive<TTestPageFetchWaiter>(completed, ready, completedItem);
        TTestPageFetch fetch;
        {
            TFixture fixture;
            TTestCachePageRef hit;
            UNIT_ASSERT(
                TCacheTestAccess::FindOrInsert(*fixture.Cache, MakeCollectionCacheItem(65, 66), MakePageLocation(12),
                    EStickyState::None, waiter, hit, fetch) == ECacheResultStatus::Inserted);
            TlsCacheHazard = nullptr;
            fixture.Cache.Drop();
            UNIT_ASSERT_VALUES_EQUAL(waiter->RefCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(completed.load(), 0);
        }
        UNIT_ASSERT(fetch);
        fetch = {};
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 0);
    }

    Y_UNIT_TEST(ReviewR12FinalSweepAbandonsDeferredCompletions) {
        for (bool successful : { false, true }) {
            std::atomic<ui32> completed = 0;
            std::atomic<ui32> ready = 0;
            std::atomic<ui64> completedItem = 0;
            auto waiter = MakeIntrusive<TTestPageFetchWaiter>(completed, ready, completedItem);
            NActors::TSharedData payload;
            TFixture fixture;
            const auto collection = MakeCollectionCacheItem(66, 67);
            const auto location = MakePageLocation(13);
            TTestCachePageRef hit;
            TTestPageFetch fetch;
            UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None, waiter,
                            hit, fetch) == ECacheResultStatus::Inserted);
            auto structural = TCacheTestAccess::AcquirePending(
                *fixture.Cache, TCacheKey::Page(collection, static_cast<ui64>(location.Offset)));
            UNIT_ASSERT(structural);
            if (successful) {
                auto data = MakePageData(fetch.CacheItem().Index());
                payload = data;
                UNIT_ASSERT(fetch.MakeReady(std::move(data)));
            } else {
                UNIT_ASSERT(fetch.FailReady());
            }
            UNIT_ASSERT(!fetch);
            UNIT_ASSERT_VALUES_EQUAL(completed.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(waiter->RefCount(), 2);
            TlsCacheHazard = nullptr;
            structural.Drop();
            fixture.Cache.Drop();
            UNIT_ASSERT_VALUES_EQUAL(completed.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(waiter->RefCount(), 1);
            if (successful) {
                UNIT_ASSERT(payload.IsPrivate());
            }
        }
    }

    Y_UNIT_TEST(OwnedCollectionRegistryLifecycle) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(12, 34, 56) };
        TCollectionCacheItem collection;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(location, collection, hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(location.Id)));
        UNIT_ASSERT(!fixture.Cache->EraseCollection(collection));

        bool visited = false;
        fixture.Cache->ForEachCollection([&](TCollection& value) {
            UNIT_ASSERT(!visited);
            visited = true;
            UNIT_ASSERT_VALUES_EQUAL(value.Id(), location.Id);
            UNIT_ASSERT(fixture.Cache->DetachCollection(value.CacheItem));
        });
        UNIT_ASSERT(visited);
        UNIT_ASSERT(fixture.Cache->EraseCollection(collection));
        fixture.Cache->ForEachCollection([](TCollection&) {
            UNIT_FAIL("Detached collection remained registered");
        });
    }

    Y_UNIT_TEST(CacheOperationMovesGuardAndSharesHelperScope) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        TSpaceHazard* hazard = nullptr;
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            hazard = &cacheOp.Hazard();
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
            TCacheItem item;
            UNIT_ASSERT(cacheOp.Find(key, item) == ECacheResultStatus::Hit);
            UNIT_ASSERT(item == page.CacheItem());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
            {
                auto movedOp = std::move(cacheOp);
                UNIT_ASSERT(&movedOp.SpaceOperation() == static_cast<TSpaceOperation*>(&movedOp));
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
                TOperationItemRef<TTestTraits> acquired(movedOp);
                UNIT_ASSERT(movedOp.TryAcquire(item, false, acquired.Get()) ==
                            TTestSharedCache::TOperation::EAcquireStatus::Ready);
                UNIT_ASSERT(acquired.CacheItem() == item);
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(hazard->State.load(std::memory_order_acquire), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(hazard->State.load(std::memory_order_acquire), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
    }

    Y_UNIT_TEST(BorrowedCacheOperationPreservesExistingGuard) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        TSpaceHazard* hazard = nullptr;
        {
            auto spaceOp = fixture.Space->BeginOperation();
            hazard = &spaceOp.Hazard();
            {
                auto cacheOp = TCacheTestAccess::BindOperation(*fixture.Cache, spaceOp);
                UNIT_ASSERT(&cacheOp.SpaceOperation() == &spaceOp);
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
                TCacheItem item;
                UNIT_ASSERT(cacheOp.Find(key, item) == ECacheResultStatus::Hit);
                UNIT_ASSERT(item == page.CacheItem());
                {
                    TOperationItemRef<TTestTraits> acquired(cacheOp.SpaceOperation());
                    UNIT_ASSERT(cacheOp.TryAcquire(item, false, acquired.Get()) ==
                                TTestSharedCache::TOperation::EAcquireStatus::Ready);
                    UNIT_ASSERT(acquired.CacheItem() == item);
                    UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
                }
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(hazard->State.load(std::memory_order_acquire), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
    }

    Y_UNIT_TEST(BeginEraseAcquiresOwnerAndFinishErasePreservesHeldItem) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));

        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& cacheOp) {
            TOperationItemRef<TTestTraits> held(cacheOp, cacheOp.TryAcquireStructural(page.CacheItem()));
            UNIT_ASSERT(held);
            THandle& handle = cacheOp.Handles()[page.Index()];
            const THandleState before = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            UNIT_ASSERT(before.IsCold());
            UNIT_ASSERT_VALUES_EQUAL(before.Refs(), 1);
            {
                TOperationItemRef<TTestTraits> owner(cacheOp);
                THandleState expected = before.IncrementRefs();
                UNIT_ASSERT(!cacheOp.TryBeginErase(page.CacheItem(), expected, owner.Get()));
                UNIT_ASSERT(!owner);
                UNIT_ASSERT_VALUES_EQUAL(expected.Raw(), before.Raw());
                UNIT_ASSERT_VALUES_EQUAL(handle.State.load(std::memory_order_acquire), before.Raw());

                UNIT_ASSERT(cacheOp.TryBeginErase(page.CacheItem(), expected, owner.Get()));
                UNIT_ASSERT(owner.CacheItem() == page.CacheItem());
                const THandleState deleted = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
                UNIT_ASSERT(deleted.IsTombstone());
                UNIT_ASSERT_VALUES_EQUAL(deleted.Refs(), 2);
                TCacheItem found;
                UNIT_ASSERT(cacheOp.Find(key, found) == ECacheResultStatus::Miss);

                cacheOp.FinishErase(owner.CacheItem(), key);
                cacheOp.FinishErase(owner.CacheItem(), key);
                UNIT_ASSERT_VALUES_EQUAL(handle.State.load(std::memory_order_acquire), deleted.Raw());
            }
            const THandleState retained = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            UNIT_ASSERT(retained.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(retained.Refs(), 1);
        });
        const THandleState released = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(released.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(released.Version(), AdvanceItemVersion(page.Version()));
    }

    Y_UNIT_TEST(BeginEraseReusesOwnerWithoutReferenceIncrement) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));

        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& cacheOp) {
            TOperationItemRef<TTestTraits> owner(cacheOp, cacheOp.TryAcquireStructural(page.CacheItem()));
            UNIT_ASSERT(owner);
            THandle& handle = cacheOp.Handles()[page.Index()];
            const THandleState before = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            UNIT_ASSERT(before.IsCold());
            UNIT_ASSERT_VALUES_EQUAL(before.Refs(), 1);

            THandleState expected = before.IncrementRefs();
            UNIT_ASSERT(!cacheOp.TryBeginErase(owner.Get(), expected));
            UNIT_ASSERT(owner.CacheItem() == page.CacheItem());
            UNIT_ASSERT_VALUES_EQUAL(expected.Raw(), before.Raw());
            UNIT_ASSERT_VALUES_EQUAL(handle.State.load(std::memory_order_acquire), before.Raw());

            UNIT_ASSERT(cacheOp.TryBeginErase(owner.Get(), expected));
            const THandleState deleted = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            UNIT_ASSERT(deleted.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(deleted.Refs(), 1);
            cacheOp.FinishErase(owner.CacheItem(), key);
            cacheOp.FinishErase(owner.CacheItem(), key);
            UNIT_ASSERT(owner.CacheItem() == page.CacheItem());
            UNIT_ASSERT_VALUES_EQUAL(handle.State.load(std::memory_order_acquire), deleted.Raw());
        });
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
    }

    Y_UNIT_TEST(InsertFindAndRollback) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto candidate = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(candidate);

        auto inserted = FindOrInsertPage(*fixture.Cache, key, candidate);
        UNIT_ASSERT(inserted.Status == ECacheResultStatus::Inserted);
        inserted.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(found.Ref);
        UNIT_ASSERT(found.Ref.CacheItem() == candidate);
        found.Ref.Drop();

        const auto duplicate = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(duplicate);
        auto existing = FindOrInsertPage(*fixture.Cache, key, duplicate);
        UNIT_ASSERT(existing.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(existing.Ref);
        existing.Ref.Drop();
    }
    Y_UNIT_TEST(PageFindOrInsertAllocatesOnlyAfterMiss) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 104);
        const ui64 freeBefore = TCacheTestAccess::FreeCount(*fixture.Cache);

        TPageCacheItem inserted;
        TTestCachePageRef page;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key),
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EStickyState::None,
                        inserted, page) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!page);
        const ui64 spare =
            TCacheTestAccess::SpareItem(*fixture.Cache, TCacheTestAccess::HazardCount(*fixture.Cache) - 1);
        UNIT_ASSERT(spare != 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);

        TPageCacheItem pendingItem;
        TTestCachePageRef pendingPage;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key),
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EStickyState::None,
                        pendingItem, pendingPage) == ECacheResultStatus::Pending);
        UNIT_ASSERT(!pendingItem);
        UNIT_ASSERT(!pendingPage);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::SpareItem(*fixture.Cache, TCacheTestAccess::HazardCount(*fixture.Cache) - 1), spare);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);

        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        TPageCacheItem duplicate;
        TTestCachePageRef hitPage;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key),
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EStickyState::None,
                        duplicate, hitPage) == ECacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hitPage.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);
        hitPage.Drop();
    }
    Y_UNIT_TEST(PageFindOrInsertSubscribesAndCompletesFetch) {
        TFixture fixture;
        const TLogoBlobID id(61, 62, 63);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = NTable::NPage::TPageLocation::FromPageIndex(7, 4096, NTable::NPage::EPage::DataPage, 123);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        TTestCachePageRef hit;
        TTestPageFetch fetch;
        TIntrusivePtr<TPageFetchWaiter> first = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::Sticky, first,
                        hit, fetch) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT(!hit);
        UNIT_ASSERT(fetch.Location() == location);
        const TPageCacheItem page = fetch.CacheItem();
        UNIT_ASSERT(fetch.Dispatch());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsRequested());

        TTestPageFetch duplicateFetch;
        TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::Sticky, second,
                        hit, duplicateFetch) == ECacheResultStatus::Pending);
        UNIT_ASSERT(!duplicateFetch);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);

        UNIT_ASSERT(fetch.MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(!fetch);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 2);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), page.CacheItem().Raw());

        UNIT_ASSERT(
            fixture.Cache->Find(collection, static_cast<ui64>(location.Offset), hit) == ECacheResultStatus::Hit);
        UNIT_ASSERT(hit.CacheItem() == page);
        hit.Drop();
    }
    Y_UNIT_TEST(ReviewR13WrittenBatchCoalescesAndAccountsOnce) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 121);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        const ui64 overallBefore = fixture.Cache->OverallUsage();
        auto body = MakePageData(1);
        const char* pointer = body.data();
        auto duplicateBody = MakePageData(1);
        std::atomic<ui32> completed = 0, ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(MakePageLocation(1), new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        UNIT_ASSERT(TCacheTestAccess::FindOrInsertWrittenBatch(*fixture.Cache, collection, requests));
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ECacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overallBefore + bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 1);
        UNIT_ASSERT(requests[0].Fetch().MakeReady(std::move(body)));
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
        UNIT_ASSERT(duplicateBody);
        TTestCachePageRequest hit(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(TCacheTestAccess::FindOrInsertWrittenBatch(*fixture.Cache, collection, { &hit, 1 }));
        UNIT_ASSERT(hit.Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(hit.Page().data(), pointer);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
    }

    Y_UNIT_TEST(ReviewR13WrittenAdmissionSurvivesEvictedPresenceSnapshot) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 122);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const auto source = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, source));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        auto body = MakePageData(1);

        struct TContext {
            TFixture& Fixture;
            TCollectionCacheItem Collection;
            TPageCacheItem Source;
            ui64 Bytes;
            bool Evicted = false;
        } context{ fixture, collection, source, bytes };

        TTestTraits hooks{ &context,
            [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (at == ECacheHookPoint::AfterPageBatchPrepared && item == ctx.Collection.CacheItem() &&
                    !ctx.Evicted) {
                    ctx.Evicted = true;
                    Y_ABORT_UNLESS(TCacheTestAccess::EraseCold(
                        *ctx.Fixture.Cache, MakeColdItem(*ctx.Fixture.Cache, ctx.Source.Index())));
                    Y_ABORT_UNLESS(TCacheTestAccess::TryReserve(*ctx.Fixture.Cache, ctx.Bytes));
                    Y_ABORT_UNLESS(ctx.Fixture.Cache->SetCollectionPagesCacheMode(ctx.Collection, ECacheMode::Sticky));
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        TTestCachePageRequest request(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(TCacheTestAccess::FindOrInsertWrittenBatch(*fixture.Cache, collection, { &request, 1 }));
        UNIT_ASSERT(context.Evicted);
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        const auto page = request.Fetch().CacheItem();
        UNIT_ASSERT(request.Fetch().MakeReady(std::move(body)));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
    }

    Y_UNIT_TEST(ReviewR13WrittenBatchRollsBackHandlePressure) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 123);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        TVector<TTestCachePageRef> held;
        for (ui64 offset = 100; AvailableFreeHandles(*fixture.Cache) > 1; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            TTestCachePageRef page;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, page) == ECacheResultStatus::Hit);
            held.push_back(std::move(page));
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(1));
        const ui64 usageBefore = fixture.Cache->PayloadUsage();
        const ui64 overallBefore = fixture.Cache->OverallUsage();
        const std::array bodies{ MakePageData(1), MakePageData(2) };
        TVector<TTestCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), MakeIntrusive<TPageFetchWaiter>(), true);
        }
        UNIT_ASSERT(!TCacheTestAccess::FindOrInsertWrittenBatch(*fixture.Cache, collection, requests));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), usageBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overallBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT(bodies[0] && bodies[1]);
    }

    TTestCacheCollectionRef MakeReviewRetainedCollection(TFixture & fixture, const TLogoBlobID& id) {
        TCollectionCacheItem item;
        TTestCacheCollectionRef ref;
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert(TCollectionLocation{ .Id = id }, item, ref) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->Find(id, ref) == ECacheResultStatus::Hit);
        return ref;
    }

    void AddReviewRetainedPage(TFixture & fixture, TCollectionCacheItem collection, ui64 offset) {
        TPageCacheItem item;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset, 16), EStickyState::Sticky,
                        item, hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(item, MakePageData(item.Index(), 16)));
    }

    Y_UNIT_TEST(ReviewR20RetainedPseudoCollectionConsumesAHandle) {
        TFixture fixture(5);
        auto main = MakeReviewRetainedCollection(fixture, TLogoBlobID(120, 50020, 1));
        auto pseudo = MakeReviewRetainedCollection(fixture, TLogoBlobID(120, 50020, 2));
        TVector<NTable::NPage::TPageLocation> pages;
        for (ui64 offset = 100;
             pages.size() < fixture.Cache->TargetHandleCount() - TTestSharedCache::RequestHandleOverhead; ++offset) {
            pages.push_back(MakePageLocation(offset, 16));
        }
        const TCacheHandleRequirement requirement{ main->Id(), pages };
        const std::array retained{ main.CacheItem(), pseudo.CacheItem(), main.CacheItem() };
        UNIT_ASSERT_GT(fixture.Cache->CurrentLimit(), (pages.size() + 1) * (16 + NActors::TSharedData::OverheadSize));
        UNIT_ASSERT(!fixture.Cache->WorkingSetExceedsCapacity({ &retained[0], 1 }, { &requirement, 1 }));
        UNIT_ASSERT(fixture.Cache->WorkingSetExceedsCapacity(retained, { &requirement, 1 }));
    }

    Y_UNIT_TEST(ReviewR20IndependentStickyPagesRemainGlobalPressure) {
        TFixture fixture(5);
        auto main = MakeReviewRetainedCollection(fixture, TLogoBlobID(120, 50020, 3));
        auto foreign = MakeReviewRetainedCollection(fixture, TLogoBlobID(120, 50020, 4));
        AddReviewRetainedPage(fixture, main.CacheItem(), 1);
        AddReviewRetainedPage(fixture, foreign.CacheItem(), 1);
        const ui64 pageBytes = 16 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(2 * pageBytes));
        const std::array pages{ MakePageLocation(1, 16), MakePageLocation(2, 16) };
        const TCacheHandleRequirement requirement{ main->Id(), pages };
        const std::array retained{ main.CacheItem(), foreign.CacheItem() };
        const ui64 before = fixture.Cache->PayloadUsage();
        UNIT_ASSERT(!fixture.Cache->WorkingSetExceedsCapacity({ &retained[0], 1 }, { &requirement, 1 }));
        UNIT_ASSERT(!fixture.Cache->CanReserve(pageBytes));
        UNIT_ASSERT(!fixture.Cache->WorkingSetExceedsCapacity(retained, { &requirement, 1 }));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), before);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR20RetiredAndCurrentCollectionGenerationsAreDistinct) {
        TFixture fixture(5);
        const TLogoBlobID id(120, 50020, 5);
        auto retired = MakeReviewRetainedCollection(fixture, id);
        UNIT_ASSERT(fixture.Cache->DetachCollection(retired.CacheItem()));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, retired.CacheItem().Index()).IsSticky());
        UNIT_ASSERT(fixture.Cache->EraseCollection(retired.CacheItem()));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, retired.CacheItem().Index()).IsTombstone());
        auto current = MakeReviewRetainedCollection(fixture, id);
        UNIT_ASSERT(retired.CacheItem() != current.CacheItem());
        TVector<NTable::NPage::TPageLocation> pages;
        for (ui64 offset = 100;
             pages.size() < fixture.Cache->TargetHandleCount() - TTestSharedCache::RequestHandleOverhead; ++offset) {
            pages.push_back(MakePageLocation(offset, 16));
        }
        const TCacheHandleRequirement requirement{ id, pages };
        const std::array retained{ retired.CacheItem(), current.CacheItem() };
        UNIT_ASSERT(fixture.Cache->WorkingSetExceedsCapacity({ &retained[0], 1 }, { &requirement, 1 }));
        UNIT_ASSERT(fixture.Cache->WorkingSetExceedsCapacity(retained, { &requirement, 1 }));
        UNIT_ASSERT(!fixture.Cache->WorkingSetExceedsCapacity({ &retained[1], 1 }, { &requirement, 1 }));
        retired.Drop();
        UNIT_ASSERT(!fixture.Cache->WorkingSetExceedsCapacity(retained, { &requirement, 1 }));
    }

    void CheckHandleReturnAdmissionNotification(bool reserved, bool armBeforeReturn) {
        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            TCacheHandleRequirement Requirement{ TLogoBlobID(207, 208, 1), {} };
            ui32 Notifications = 0;
            bool Published = true;
        } observation;

        TFixture fixture(5);
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        TPageCacheItem page;
        TCollectionCacheItem collection;
        TVector<TCollectionCacheItem> others;
        Y_DEFER {
            TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            if (page) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
            }
            if (collection) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, collection);
            }
            for (const auto item : others) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        if (reserved) {
            page = AllocatePage(*fixture.Cache, TCacheKey::Page(MakeCollectionCacheItem(579, 580), 0), 16);
            UNIT_ASSERT(page);
        } else {
            collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(209, 210, 1)));
            UNIT_ASSERT(collection);
        }
        for (ui32 index = 0; index < fixture.Capacity.HandleCount() - 3; ++index) {
            const auto item =
                AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(211, 212, index + 1)));
            UNIT_ASSERT(item);
            others.push_back(item);
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        observation.Cache = fixture.Cache.Get();
        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  auto& observation = *static_cast<TObservation*>(context);
                                  ++observation.Notifications;
                                  observation.Published &=
                                      observation.Cache->ReservedBytes() == 0 &&
                                      observation.Cache->CanAllocateHandles({ &observation.Requirement, 1 });
                                  const auto candidate = AllocateCollection(
                                      *observation.Cache, TCacheKey::Collection(observation.Requirement.CollectionId));
                                  observation.Published &= static_cast<bool>(candidate);
                                  if (candidate) {
                                      TCacheTestAccess::DiscardCandidate(*observation.Cache, candidate);
                                  }
                              }
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforeReturn) {
            fixture.Cache->ArmResourceWait(generation);
        }
        if (reserved) {
            TCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
            page = {};
        } else {
            TCacheTestAccess::DiscardCandidate(*fixture.Cache, collection);
            collection = {};
        }
        UNIT_ASSERT(fixture.Cache->AdmissionGeneration() > generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforeReturn ? 1 : 0);
        if (!armBeforeReturn) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(ZeroByteHandleReturnNotifiesAfterPublication) {
        for (bool armed : { false, true }) {
            CheckHandleReturnAdmissionNotification(false, armed);
        }
    }

    Y_UNIT_TEST(ReservedHandleReturnNotifiesAfterPublication) {
        for (bool armed : { false, true }) {
            CheckHandleReturnAdmissionNotification(true, armed);
        }
    }

    Y_UNIT_TEST(HandleReadinessPeekPreservesLocalSpare) {
        ui32 notifications = 0;
        TFixture fixture(5, 1);
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        TVector<TCollectionCacheItem> held;
        Y_DEFER {
            TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            for (const auto item : held) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        for (ui32 index = 0; index < fixture.Capacity.HandleCount() - 2; ++index) {
            const auto item =
                AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(213, 214, index + 1)));
            UNIT_ASSERT(item);
            held.push_back(item);
        }
        TCacheTestAccess::DiscardCandidate(*fixture.Cache, held.back());
        held.pop_back();
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            cacheOp.RefillSpareItem();
        }
        const ui64 spare = TCacheTestAccess::SpareItem(*fixture.Cache, 0);
        UNIT_ASSERT(spare);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), 0);
        TTestTraits hooks{ &notifications, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        const std::array locations{ MakePageLocation(0, 16) };
        const TLogoBlobID id(215, 216, 1);
        const TCacheHandleRequirement one{ id, {} };
        const TCacheHandleRequirement two{ id, locations };
        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            UNIT_ASSERT(fixture.Cache->CanAllocateHandles({ &one, 1 }));
            UNIT_ASSERT(!fixture.Cache->CanAllocateHandles({ &two, 1 }));
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), 0);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::SpareItem(*fixture.Cache, 0), spare);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
            UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        }
    }

    Y_UNIT_TEST(WorkingSetReadinessPeekDoesNotReserveOrReclaim) {
        ui32 notifications = 0;
        TFixture fixture(5, 1);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(bytes));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(2 * bytes));
        const auto collection = MakeCollectionCacheItem(581, 582);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui64 state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Raw();
        const ui64 free = AvailableFreeHandles(*fixture.Cache);
        const ui64 overall = fixture.Cache->OverallUsage();
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        TTestTraits hooks{ &notifications, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        fixture.Cache->ArmResourceWait(generation);
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            UNIT_ASSERT(fixture.Cache->CanReserve(0));
            UNIT_ASSERT(fixture.Cache->CanReserve(bytes));
            UNIT_ASSERT(!fixture.Cache->CanReserve(bytes + 1));
            UNIT_ASSERT(!fixture.Cache->CanReserve(Max<ui64>()));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overall);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Raw(), state);
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), free);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
            UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        }
        UNIT_ASSERT(fixture.Cache->ReclaimForAdmission(bytes + 1));
        UNIT_ASSERT(fixture.Cache->CanReserve(bytes + 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
    }

    Y_UNIT_TEST(ExplicitRetryReclaimsHandlesAfterReadOnlyPeek) {
        TFixture fixture(5, 1);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(bytes));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const auto collection = MakeCollectionCacheItem(583, 584);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        for (ui32 touch = 0; touch < 2; ++touch) {
            TTestCachePageRef held;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ECacheResultStatus::Hit);
        }
        for (ui32 index = 0; index < fixture.Capacity.HandleCount() - 3; ++index) {
            const TLogoBlobID id(217, 218, index + 1);
            const auto item = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(item);
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, item, MakeCollection(id)));
        }
        const TCacheHandleRequirement requirement{ TLogoBlobID(219, 220, 1), {} };
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT(!fixture.Cache->CanAllocateHandles({ &requirement, 1 }));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Frequency(), 2);
        for (ui32 step = 0; !fixture.Cache->CanAllocateHandles({ &requirement, 1 }); ++step) {
            UNIT_ASSERT(step < SharedCacheTransitionWorkBatch);
            UNIT_ASSERT(fixture.Cache->ReclaimForAdmission(0));
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    void CheckGrowthHandleNotification(bool laterBatch, bool armBeforePublication) {
        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            TCollectionCacheItem Claimed;
            ui64 MinimumIndex = 0;
            ui32 Notifications = 0;
            bool Published = false;
            bool Unlocked = false;
        } observation;

        const ui8 addressBits = laterBatch ? 9 : 5;
        TFixture fixture(addressBits, 1, addressBits + 1);
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        TTransition growth;
        TVector<TCollectionCacheItem> held;
        Y_DEFER {
            TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            if (observation.Claimed) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, observation.Claimed);
            }
            for (const auto item : held) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        const auto fillHandles = [&] {
            while (AvailableFreeHandles(*fixture.Cache) != 0) {
                const auto item =
                    AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(221, 222, held.size() + 1)));
                UNIT_ASSERT(item);
                held.push_back(item);
            }
        };
        fillHandles();
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        do {
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        } while (growth.Phase() == ETransitionPhase::InitializeGrowth);
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, addressBits + 1));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        observation.MinimumIndex = fixture.Capacity.HandleCount();
        if (laterBatch) {
            UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            UNIT_ASSERT(growth.Phase() == ETransitionPhase::AppendGrowth);
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), SharedCacheTransitionWorkBatch);
            fillHandles();
            observation.MinimumIndex += SharedCacheTransitionWorkBatch;
        }
        observation.Cache = fixture.Cache.Get();
        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point != ECacheHookPoint::ResourcesAvailable) {
                                  return;
                              }
                              auto& observation = *static_cast<TObservation*>(context);
                              ++observation.Notifications;
                              {
                                  auto lock = TCacheTestAccess::LockHotResize(*observation.Cache);
                                  observation.Unlocked = static_cast<bool>(lock);
                              }
                              observation.Claimed = AllocateCollection(
                                  *observation.Cache, TCacheKey::Collection(TLogoBlobID(223, 224, 1)));
                              observation.Published =
                                  observation.Claimed && observation.Claimed.Index() >= observation.MinimumIndex;
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        TTransition idle;
        UNIT_ASSERT(!TCacheTestAccess::PublishFinalView(*fixture.Cache, idle));
        UNIT_ASSERT(!TCacheTestAccess::AppendFreeHandles(*fixture.Cache, idle));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 0);
        if (laterBatch) {
            UNIT_ASSERT(TCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
        } else {
            UNIT_ASSERT(!TCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
        }
        UNIT_ASSERT(fixture.Cache->AdmissionGeneration() > generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforePublication ? 1 : 0);
        if (!armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published);
        UNIT_ASSERT(observation.Unlocked);
        UNIT_ASSERT(growth.Phase() == ETransitionPhase::FinalDrain);
        const ui64 afterPublication = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(afterPublication);
        UNIT_ASSERT(!TCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
        UNIT_ASSERT(!TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), afterPublication);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, growth));
    }

    Y_UNIT_TEST(FirstGrowthBatchNotifiesAfterHandlePublication) {
        for (bool armed : { false, true }) {
            CheckGrowthHandleNotification(false, armed);
        }
    }

    Y_UNIT_TEST(LaterGrowthBatchNotifiesAfterHandlePublication) {
        for (bool armed : { false, true }) {
            CheckGrowthHandleNotification(true, armed);
        }
    }

    void RefillWorkerSpare(TTestSharedCache & cache, ui32 index) {
        std::thread worker([&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(cache, index);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            auto cacheOp = cache.BeginOperation();
            cacheOp.RefillSpareItem();
        });
        worker.join();
    }

    TVector<TCollectionCacheItem> AllocateAvailableHandleCandidates(TTestSharedCache & cache) {
        TVector<TCollectionCacheItem> held;
        const TCacheHandleRequirement requirement{ TLogoBlobID(225, 226, 1), {} };
        while (cache.CanAllocateHandles({ &requirement, 1 })) {
            const auto item = AllocateCollection(cache, TCacheKey::Collection(TLogoBlobID(227, 228, held.size() + 1)));
            UNIT_ASSERT(item);
            held.push_back(item);
        }
        return held;
    }

    void CheckForeignSpareRecovery(bool controller) {
        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            ui32 Index = 0;
            ui32 Notifications = 0;
            bool Published = false;
        } observation;

        TFixture fixture(5, 2);
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        RefillWorkerSpare(*fixture.Cache, 0);
        const auto spare = TCacheItem::FromRaw(TCacheTestAccess::SpareItem(*fixture.Cache, 0));
        UNIT_ASSERT(!spare.IsNull());
        auto held = AllocateAvailableHandleCandidates(*fixture.Cache);
        TCollectionCacheItem claimed;
        Y_DEFER {
            TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            if (claimed) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, claimed);
            }
            for (const auto item : held) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        const TCacheHandleRequirement requirement{ TLogoBlobID(229, 230, 1), {} };
        UNIT_ASSERT(!fixture.Cache->CanAllocateHandles({ &requirement, 1 }));
        observation.Cache = fixture.Cache.Get();
        observation.Index = spare.Index();
        TTestTraits hooks{ &observation,
            [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                if (point == ECacheHookPoint::ResourcesAvailable) {
                    auto& observation = *static_cast<TObservation*>(context);
                    ++observation.Notifications;
                    observation.Published =
                        TCacheTestAccess::FreeCount(*observation.Cache) != 0 &&
                        TCacheTestAccess::HandleState(*observation.Cache, observation.Index).IsFree();
                }
            } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        fixture.Cache->ArmResourceWait(fixture.Cache->AdmissionGeneration());
        if (controller) {
            UNIT_ASSERT(fixture.Cache->ReclaimSpareHandle());
            UNIT_ASSERT(fixture.Cache->CanAllocateHandles({ &requirement, 1 }));
        }
        claimed = AllocateCollection(*fixture.Cache, TCacheKey::Collection(requirement.CollectionId));
        UNIT_ASSERT(claimed);
        UNIT_ASSERT_VALUES_EQUAL(claimed.Index(), spare.Index());
        UNIT_ASSERT_VALUES_EQUAL(claimed.Version(), spare.Version());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::SpareItem(*fixture.Cache, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        UNIT_ASSERT(!fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
    }

    Y_UNIT_TEST(NativeHandlePressureReclaimsForeignSpare) {
        CheckForeignSpareRecovery(false);
    }

    Y_UNIT_TEST(ControllerHandlePressureReclaimsForeignSpare) {
        CheckForeignSpareRecovery(true);
    }

    Y_UNIT_TEST(NativeAllocationPreservesForeignSpareWithoutPressure) {
        TFixture fixture(5, 2);
        RefillWorkerSpare(*fixture.Cache, 0);
        const ui64 spare = TCacheTestAccess::SpareItem(*fixture.Cache, 0);
        UNIT_ASSERT(spare);
        const auto first = AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(231, 232, 1)));
        const auto second = AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(231, 232, 2)));
        UNIT_ASSERT(first && second);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::SpareItem(*fixture.Cache, 0), spare);
        TCacheTestAccess::DiscardCandidate(*fixture.Cache, first);
        TCacheTestAccess::DiscardCandidate(*fixture.Cache, second);
    }

    Y_UNIT_TEST(OwnerAndPressureRecoveryClaimSpareOnce) {
        for (ui32 attempt = 0; attempt < 32; ++attempt) {
            TFixture fixture(5, 2);
            TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
            RefillWorkerSpare(*fixture.Cache, 0);
            const auto spare = TCacheItem::FromRaw(TCacheTestAccess::SpareItem(*fixture.Cache, 0));
            UNIT_ASSERT(!spare.IsNull());
            auto held = AllocateAvailableHandleCandidates(*fixture.Cache);
            TCollectionCacheItem owner;
            TCollectionCacheItem pressure;
            Y_DEFER {
                if (owner) {
                    TCacheTestAccess::DiscardCandidate(*fixture.Cache, owner);
                }
                if (pressure) {
                    TCacheTestAccess::DiscardCandidate(*fixture.Cache, pressure);
                }
                for (const auto item : held) {
                    TCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
                }
            };
            std::atomic<bool> start{ false };
            std::thread worker([&] {
                Y_ABORT_UNLESS(!TlsCacheHazard);
                TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
                Y_DEFER {
                    TlsCacheHazard = nullptr;
                };
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                owner = AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(233, 234, 1)));
            });
            start.store(true, std::memory_order_release);
            pressure = AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(233, 234, 2)));
            worker.join();
            UNIT_ASSERT(static_cast<bool>(owner) != static_cast<bool>(pressure));
            UNIT_ASSERT_VALUES_EQUAL((owner ? owner : pressure).Index(), spare.Index());
            UNIT_ASSERT_VALUES_EQUAL((owner ? owner : pressure).Version(), spare.Version());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::SpareItem(*fixture.Cache, 0), 0);
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        }
    }

    void CheckSparePublication(bool recycle, bool armBeforePublication, EItemKind kind = EItemKind::Collection) {
        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            ui32 Notifications = 0;
            bool RearmedBeforeStore = false;
            bool Published = false;
        } observation;

        TFixture fixture(5, 1);
        TCacheItem candidate;
        Y_DEFER {
            TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            if (!candidate.IsNull()) {
                if (kind == EItemKind::Page) {
                    TCacheTestAccess::DiscardCandidate(*fixture.Cache, TPageCacheItem::FromValidated(candidate));
                } else {
                    TCacheTestAccess::DiscardCandidate(*fixture.Cache, TCollectionCacheItem::FromValidated(candidate));
                }
            }
        };
        if (recycle) {
            auto cacheOp = fixture.Cache->BeginOperation();
            if (kind == EItemKind::Page) {
                constexpr ui64 size = 64;
                candidate = cacheOp.TryAllocateReservedItem(size + TSharedData::OverheadSize, 1);
                UNIT_ASSERT(!candidate.IsNull());
                cacheOp.Items().InitializePage(
                    candidate, {}, 1, size, NTable::NPage::EPage::DataPage, 0, EStickyState::None, true, false);
            } else {
                candidate = cacheOp.Allocate();
                UNIT_ASSERT(!candidate.IsNull());
                cacheOp.Items().InitializeCollection(candidate, TLogoBlobID(235, 236, 1), 0);
            }
        }
        observation.Cache = fixture.Cache.Get();
        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point != ECacheHookPoint::ResourcesAvailable) {
                                  return;
                              }
                              auto& observation = *static_cast<TObservation*>(context);
                              ++observation.Notifications;
                              if (TCacheTestAccess::SpareItem(*observation.Cache, 0) == 0) {
                                  observation.RearmedBeforeStore = true;
                                  observation.Cache->ArmResourceWait(observation.Cache->AdmissionGeneration());
                              } else {
                                  observation.Published = true;
                              }
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            if (recycle) {
                cacheOp.RecycleCandidate(candidate);
                candidate = {};
            } else {
                cacheOp.RefillSpareItem();
            }
        }
        const bool refundsReservation = recycle && kind == EItemKind::Page;
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforePublication ? (refundsReservation ? 2 : 1) : 0);
        if (!armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT(observation.Published);
        UNIT_ASSERT_VALUES_EQUAL(observation.RearmedBeforeStore, refundsReservation && armBeforePublication);
        const ui32 notifications = observation.Notifications;
        const ui64 afterPublication = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(afterPublication);
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            cacheOp.RefillSpareItem();
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, notifications);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), afterPublication);
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), fixture.Capacity.HandleCount() - 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
    }

    Y_UNIT_TEST(SpareRefillNotifiesAfterPublication) {
        for (bool armed : { false, true }) {
            CheckSparePublication(false, armed);
        }
    }

    Y_UNIT_TEST(SpareRecycleNotifiesAfterPublication) {
        for (auto kind : { EItemKind::Collection, EItemKind::Page }) {
            for (bool armed : { false, true }) {
                CheckSparePublication(true, armed, kind);
            }
        }
    }

    Y_UNIT_TEST(SpareRecoveryQuarantinesShrinkCutHandles) {
        TFixture fixture(6, 2);
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        TCacheCapacityConfig target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 2, target));
        TVector<TCollectionCacheItem> held;
        Y_DEFER {
            for (const auto item : held) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        for (ui32 index = 0; index < target.HandleCount() - 2; ++index) {
            const auto item =
                AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(237, 238, index + 1)));
            UNIT_ASSERT(item);
            UNIT_ASSERT(item.Index() < target.HandleCount());
            held.push_back(item);
        }
        RefillWorkerSpare(*fixture.Cache, 0);
        const std::array spares{ TCacheItem::FromRaw(TCacheTestAccess::SpareItem(*fixture.Cache, 0)),
            TCacheItem::FromRaw(TCacheTestAccess::SpareItem(*fixture.Cache, 1)) };
        for (const auto spare : spares) {
            UNIT_ASSERT(!spare.IsNull() && spare.Index() >= target.HandleCount());
        }
        TTransition shrink;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, target, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, target.AddressBits));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(cacheOp.SpaceOperation().TryAllocateHandle(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), 0);
        UNIT_ASSERT(fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT(fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT(!fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), 0);
        for (const auto spare : spares) {
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, spare.Index()).IsFree());
        }
        UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, shrink));
    }

    void CheckGuardDrainNotification(bool finalDrain, bool armBeforeDrain, bool nested) {
        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            TSpaceHazard* Hazard = nullptr;
            ui64 PublishedGeneration = 0;
            ui32 Notifications = 0;
            bool Cleared = true;
            bool CurrentView = true;
        } observation;

        TFixture fixture(5, 1, 6);
        TTransition growth;
        Y_DEFER {
            TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
        };
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        if (finalDrain) {
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, fixture.ReservedCapacity.AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);
        }
        observation.Cache = fixture.Cache.Get();
        observation.Hazard = TlsCacheHazard;
        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point != ECacheHookPoint::ResourcesAvailable) {
                                  return;
                              }
                              auto& observation = *static_cast<TObservation*>(context);
                              ++observation.Notifications;
                              observation.Cleared &= observation.Hazard->State.load(std::memory_order_acquire) == 0;
                              auto cacheOp = observation.Cache->BeginOperation();
                              observation.CurrentView &=
                                  cacheOp.SpaceOperation().EntryGeneration() == observation.PublishedGeneration;
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        ui64 generation = 0;
        {
            auto original = fixture.Cache->BeginOperation();
            auto moved = std::move(original);
            if (finalDrain) {
                UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            } else {
                UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            }
            observation.PublishedGeneration = TCacheTestAccess::CurrentSpaceState(*fixture.Space).Generation();
            UNIT_ASSERT(moved.SpaceOperation().EntryGeneration() != observation.PublishedGeneration);
            generation = fixture.Cache->AdmissionGeneration();
            if (armBeforeDrain) {
                fixture.Cache->ArmResourceWait(generation);
            }
            if (nested) {
                auto inner = fixture.Cache->BeginOperation();
                UNIT_ASSERT(!TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(observation.Hazard->State.load()), 2);
            } else {
                UNIT_ASSERT(!TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            }
            const ui64 state = observation.Hazard->State.load(std::memory_order_acquire);
            UNIT_ASSERT(state & SpaceHazardDrainWaiter);
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(state), 1);
            UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Hazard->State.load(std::memory_order_acquire), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforeDrain ? 1 : 0);
        if (!armBeforeDrain) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Cleared && observation.CurrentView);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        if (!finalDrain) {
            UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, fixture.ReservedCapacity.AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);
            UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, growth));
    }

    Y_UNIT_TEST(MigrationGuardDrainNotifiesAdmissionWaiter) {
        for (bool armed : { false, true }) {
            CheckGuardDrainNotification(false, armed, false);
        }
    }

    Y_UNIT_TEST(FinalGuardDrainNotifiesAdmissionWaiter) {
        for (bool armed : { false, true }) {
            CheckGuardDrainNotification(true, armed, false);
        }
    }

    Y_UNIT_TEST(NestedMovedGuardDrainNotifiesOnlyAfterLastExit) {
        for (bool finalDrain : { false, true }) {
            CheckGuardDrainNotification(finalDrain, true, true);
        }
    }

    void CheckQuietGuardExit(bool currentGeneration) {
        ui32 notifications = 0;
        TFixture fixture(5, 1, 6);
        TTransition growth;
        Y_DEFER {
            TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
        };
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        TTestTraits hooks{ &notifications, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        if (currentGeneration) {
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            auto guard = fixture.Cache->BeginOperation();
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            UNIT_ASSERT(!(TlsCacheHazard->State.load() & SpaceHazardDrainWaiter));
        } else {
            {
                auto guard = fixture.Cache->BeginOperation();
                UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            }
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, fixture.ReservedCapacity.AddressBits));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
    }

    Y_UNIT_TEST(UnregisteredGuardExitDoesNotNotify) {
        CheckQuietGuardExit(false);
    }

    Y_UNIT_TEST(CurrentGenerationGuardExitDoesNotNotify) {
        CheckQuietGuardExit(true);
    }

    Y_UNIT_TEST(ReviewR15DuplicateBatchNeedsOneHandle) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 124);
        TCollectionCacheItem collection;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(TCollectionLocation{ .Id = id }, collection, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        TVector<TTestCachePageRef> held;
        for (ui64 offset = 100; AvailableFreeHandles(*fixture.Cache) > 1; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            TTestCachePageRef page;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, page) == ECacheResultStatus::Hit);
            held.push_back(std::move(page));
        }
        const std::array locations{ MakePageLocation(1), MakePageLocation(2) };
        const TCacheHandleRequirement requirement{ id, locations };
        const ui64 bytesBefore = fixture.Cache->PayloadUsage();
        UNIT_ASSERT(!fixture.Cache->CanAllocateHandles({ &requirement, 1 }));
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytesBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);

        std::atomic<ui32> completed = 0, ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(locations[0], new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ECacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        const TCacheHandleRequirement pending{ id, { &locations[0], 1 } };
        UNIT_ASSERT(fixture.Cache->CanAllocateHandles({ &pending, 1 }));
        UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(1)));
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
    }

    Y_UNIT_TEST(ReviewR15PresenceRaceReportsOnlyCompletedAdmissions) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 125);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        const auto source = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 2));
        TVector<TTestCachePageRef> held;
        TTestCachePageRef first;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, first) == ECacheResultStatus::Hit);
        TTestCachePageRef second;
        UNIT_ASSERT(fixture.Cache->Find(collection, 2, second) == ECacheResultStatus::Hit);
        for (ui64 offset = 100; AvailableFreeHandles(*fixture.Cache) != 0; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            TTestCachePageRef page;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, page) == ECacheResultStatus::Hit);
            held.push_back(std::move(page));
        }

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Source;
            TTestCachePageRef& SourceHold;
            TTestCachePageRef Replacement;
            bool Evicted = false;
        } context{ *fixture.Cache, collection, source, first, {}, false };

        TTestTraits hooks{ &context, [](void* opaque, ECacheHookPoint point, TCacheItem) noexcept {
                              auto& ctx = *static_cast<TContext*>(opaque);
                              if (point == ECacheHookPoint::AfterPageBatchPrepared && !ctx.Evicted) {
                                  ctx.Evicted = true;
                                  ctx.SourceHold.Drop();
                                  if (TCacheTestAccess::HandleState(ctx.Cache, ctx.Source.Index()).IsHot()) {
                                      Y_ABORT_UNLESS(TCacheTestAccess::EvictFromHot(ctx.Cache, ctx.Source));
                                  }
                                  Y_ABORT_UNLESS(TCacheTestAccess::EraseCold(
                                      ctx.Cache, MakeColdItem(ctx.Cache, ctx.Source.Index())));
                                  InsertReadyPage(ctx.Cache, TCacheKey::Page(ctx.Collection, 9000));
                                  Y_ABORT_UNLESS(ctx.Cache.Find(ctx.Collection, 9000, ctx.Replacement) ==
                                                 ECacheResultStatus::Hit);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        const auto before = fixture.Cache->Stats();
        TVector<TTestCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), MakeIntrusive<TPageFetchWaiter>());
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(context.Evicted);
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Miss);
        UNIT_ASSERT(requests[1].Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().RequestedPages, before.RequestedPages + 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().HitPages, before.HitPages + 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().MissPages, before.MissPages);

        context.Replacement.Drop();
        TTestCachePageRequest retry(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, { &retry, 1 }, true, true));
        UNIT_ASSERT(retry.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(retry.Fetch().MakeReady(MakePageData(1)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().RequestedPages, before.RequestedPages + 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().MissPages, before.MissPages + 1);
    }

    void CheckPreloadHandlePressure(ui32 freeHandles) {
        TFixture fixture;
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * pageBytes));
        const TLogoBlobID id(83, 84, 126);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const auto resident = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 100));
        TVector<TCollectionCacheItem> metadata;
        for (ui64 value = 1000; AvailableFreeHandles(*fixture.Cache) > freeHandles; ++value) {
            const TLogoBlobID fillerId(83, 84, value);
            const auto filler = AllocateCollection(*fixture.Cache, TCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
            metadata.push_back(filler);
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freeHandles);
        if (freeHandles == 0) {
            const ui64 generation = fixture.Cache->AdmissionGeneration();
            UNIT_ASSERT(!fixture.Cache->ReclaimForPreload());
            UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        }
        const ui64 bytes = fixture.Cache->PayloadUsage();
        const ui64 reservations = fixture.Cache->ReservedBytes();
        TVector<TTestCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), MakeIntrusive<TPageFetchWaiter>());
        }
        UNIT_ASSERT(fixture.Cache->CanReserve(2 * pageBytes));
        UNIT_ASSERT(!fixture.Cache->FindOrInsertPreloadBatch(collection, requests));
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freeHandles);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), reservations);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepPendingBytes(), 0);
        AssertPageHit(*fixture.Cache, TCacheKey::Page(collection, 100), resident);
        for (auto filler : metadata) {
            const auto state = TCacheTestAccess::HandleState(*fixture.Cache, filler.Index());
            UNIT_ASSERT(filler.Matches(state) && state.IsSticky());
        }
    }

    Y_UNIT_TEST(PreloadDoesNotEvictToAllocateHandles) {
        CheckPreloadHandlePressure(0);
    }

    Y_UNIT_TEST(PreloadRollsBackPartialHandleAllocationWithoutEviction) {
        CheckPreloadHandlePressure(1);
    }

    void CheckPreloadReclaimsRegularPage(bool cold) {
        TFixture fixture;
        const TLogoBlobID keepId(83, 84, 128), regularId(83, 84, 129), stickyId(83, 84, 130);
        const auto keep = AllocateCollection(*fixture.Cache, TCacheKey::Collection(keepId));
        const auto regular = AllocateCollection(*fixture.Cache, TCacheKey::Collection(regularId));
        const auto sticky = AllocateCollection(*fixture.Cache, TCacheKey::Collection(stickyId));
        UNIT_ASSERT(fixture.Cache->MakeReady(keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(sticky, MakeCollection(stickyId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 16 * 4096));
        const auto coldKeep = InsertReadyPage(*fixture.Cache, TCacheKey::Page(keep, 3));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, coldKeep));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        const auto hotKeep = InsertReadyPage(*fixture.Cache, TCacheKey::Page(keep, 1));
        const auto keepCold = InsertReadyPage(*fixture.Cache, TCacheKey::Page(keep, 2));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, keepCold));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, keepCold.Index()).IsKeepCold());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, coldKeep.Index()).IsColdInTier(ECacheTier::Regular));
        const auto victim = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 1));
        InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 2));
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->Find(regular, 2, held) == ECacheResultStatus::Hit);
        if (cold) {
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, victim));
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, victim.Index()).IsCold());
        }
        TTestCachePageRequest stickyRequest(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>(), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(sticky, { &stickyRequest, 1 }));
        const auto stickyPage = stickyRequest.Fetch().CacheItem();
        UNIT_ASSERT(stickyRequest.Fetch().MakeReady(MakePageData(1)));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, stickyPage.Index()).IsSticky());
        for (ui64 id = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++id) {
            const TLogoBlobID fillerId(83, 84, id);
            const auto filler = AllocateCollection(*fixture.Cache, TCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }
        const auto hotState = TCacheTestAccess::HandleState(*fixture.Cache, hotKeep.Index());
        const auto keepColdState = TCacheTestAccess::HandleState(*fixture.Cache, keepCold.Index());
        const auto coldState = TCacheTestAccess::HandleState(*fixture.Cache, coldKeep.Index());
        TTestCachePageRequest request(MakePageLocation(100), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(100)));
        TTestCachePageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(regular, 1, missing) == ECacheResultStatus::Miss);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, hotKeep.Index()).Raw(), hotState.Raw());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::HandleState(*fixture.Cache, keepCold.Index()).Raw(), keepColdState.Raw());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::HandleState(*fixture.Cache, coldKeep.Index()).Raw(), coldState.Raw());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, stickyPage.Index()).IsSticky());
        UNIT_ASSERT(held && held.data()[0] == static_cast<char>(held.CacheItem().Index()));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
    }

    Y_UNIT_TEST(PreloadReclaimsRegularColdPageWithoutRoutingKeepPages) {
        CheckPreloadReclaimsRegularPage(true);
    }

    Y_UNIT_TEST(PreloadReclaimsRegularHotPageWithoutRoutingKeepPages) {
        CheckPreloadReclaimsRegularPage(false);
    }

    Y_UNIT_TEST(PreloadDoesNotErasePageAcquiredAfterVictimSelection) {
        TFixture fixture;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * (4096 + NActors::TSharedData::OverheadSize)));
        const TLogoBlobID keepId(83, 84, 131), regularId(83, 84, 132);
        const auto keep = AllocateCollection(*fixture.Cache, TCacheKey::Collection(keepId));
        const auto regular = AllocateCollection(*fixture.Cache, TCacheKey::Collection(regularId));
        UNIT_ASSERT(fixture.Cache->MakeReady(keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        const auto victim = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 1));
        for (ui64 id = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++id) {
            const TLogoBlobID fillerId(83, 84, id);
            const auto filler = AllocateCollection(*fixture.Cache, TCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Victim;
            TTestCachePageRef Held;
            bool Acquired = false;
        } context{ *fixture.Cache, regular, victim, {} };

        TTestTraits hooks{ &context, [](void* opaque, ECacheHookPoint point, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(opaque);
                              if (point == ECacheHookPoint::BeforePreloadPageErase && item == ctx.Victim.CacheItem() &&
                                  !ctx.Acquired) {
                                  ctx.Acquired = true;
                                  Y_ABORT_UNLESS(
                                      ctx.Cache.Find(ctx.Collection, 1, ctx.Held) == ECacheResultStatus::Hit);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        TTestCachePageRequest request(MakePageLocation(100), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(!fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(context.Acquired && context.Held);
        UNIT_ASSERT_VALUES_EQUAL(context.Held.data()[0], static_cast<char>(victim.Index()));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
        context.Held.Drop();
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(100)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
    }

    void MovePreloadTestPageToLevel(TTestSharedCache & cache, TPageCacheItem page, EHotLevel level) {
        auto cacheOp = cache.BeginOperation();
        auto& spaceOp = cacheOp.SpaceOperation();
        const auto source = spaceOp.Hot(EHotLevel::L1);
        for (ui64 slot = 0; slot < source.Capacity; ++slot) {
            ui64 raw = page.CacheItem().Raw();
            if (source.Slots[slot].compare_exchange_strong(raw, 0)) {
                UNIT_ASSERT_VALUES_EQUAL(spaceOp.Hot(level).PutAndPop(page.CacheItem().Raw()), 0);
                return;
            }
        }
        UNIT_FAIL("Expected the test page in L1");
    }

    void CheckPreloadVictimOrder(bool cold, bool protectedPromotion = false) {
        TFixture fixture;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * (4096 + NActors::TSharedData::OverheadSize)));
        const TLogoBlobID keepId(83, 84, 133), regularId(83, 84, 134);
        const auto keep = AllocateCollection(*fixture.Cache, TCacheKey::Collection(keepId));
        const auto regular = AllocateCollection(*fixture.Cache, TCacheKey::Collection(regularId));
        UNIT_ASSERT(fixture.Cache->MakeReady(keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        const auto first = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 1));
        const auto second = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 2));
        TVector<TTestCacheItemRef> blockers;
        if (protectedPromotion) {
            const ui64 slots = fixture.Cache->BeginOperation().SpaceOperation().Hot(EHotLevel::L2).Capacity;
            for (ui64 offset = 1000; offset < 1000 + slots; ++offset) {
                const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, offset));
                blockers.push_back(TCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem()));
                UNIT_ASSERT(blockers.back());
                MovePreloadTestPageToLevel(*fixture.Cache, page, EHotLevel::L2);
            }
        }
        if (cold) {
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, second));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, first));
        } else {
            for (ui32 hit = 0; hit < 2; ++hit) {
                TTestCachePageRef page;
                UNIT_ASSERT(fixture.Cache->Find(regular, 1, page) == ECacheResultStatus::Hit);
            }
        }
        for (ui64 id = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++id) {
            const TLogoBlobID fillerId(83, 84, id);
            const auto filler = AllocateCollection(*fixture.Cache, TCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }
        TTestCachePageRequest request(MakePageLocation(100), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(100)));
        TTestCachePageRef survivor, victim;
        UNIT_ASSERT(fixture.Cache->Find(regular, 2, victim) == ECacheResultStatus::Miss);
        UNIT_ASSERT(fixture.Cache->Find(regular, 1, survivor) == ECacheResultStatus::Hit);
        UNIT_ASSERT(survivor.CacheItem() == first);
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
    }

    Y_UNIT_TEST(PreloadUsesColdFifoRatherThanHandleOrder) {
        CheckPreloadVictimOrder(true);
    }

    Y_UNIT_TEST(PreloadUsesHotFrequencyAndLevelRouting) {
        CheckPreloadVictimOrder(false);
    }

    Y_UNIT_TEST(PreloadPreservesSecondChanceWhenHeldPagesBlockPromotion) {
        CheckPreloadVictimOrder(false, true);
    }

    Y_UNIT_TEST(PreloadKeepsDisplacedFrequentPageInItsSourceTier) {
        TFixture fixture;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * (4096 + NActors::TSharedData::OverheadSize)));
        const TLogoBlobID keepId(83, 84, 136), regularId(83, 84, 137);
        const auto keep = AllocateCollection(*fixture.Cache, TCacheKey::Collection(keepId));
        const auto regular = AllocateCollection(*fixture.Cache, TCacheKey::Collection(regularId));
        UNIT_ASSERT(fixture.Cache->MakeReady(keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        const auto first = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 1));
        const auto second = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 2));
        MovePreloadTestPageToLevel(*fixture.Cache, first, EHotLevel::L0);
        MovePreloadTestPageToLevel(*fixture.Cache, second, EHotLevel::L2);
        TVector<TTestCacheItemRef> blockers;
        for (ui32 level : { 2, 3 }) {
            const ui64 slots =
                fixture.Cache->BeginOperation().SpaceOperation().Hot(static_cast<EHotLevel>(level)).Capacity;
            for (ui64 slot = level == 2 ? 1 : 0; slot < slots; ++slot) {
                const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, level * 1000 + slot));
                blockers.push_back(TCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem()));
                UNIT_ASSERT(blockers.back());
                MovePreloadTestPageToLevel(*fixture.Cache, page, static_cast<EHotLevel>(level));
            }
        }
        for (ui32 hit = 0; hit < 3; ++hit) {
            TTestCachePageRef page;
            UNIT_ASSERT(fixture.Cache->Find(regular, hit == 0 ? 1 : 2, page) == ECacheResultStatus::Hit);
        }
        for (ui64 id = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++id) {
            const TLogoBlobID fillerId(83, 84, id);
            const auto filler = AllocateCollection(*fixture.Cache, TCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }
        TTestCachePageRequest request(MakePageLocation(100), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(100)));
        TTestCachePageRef victim, survivor;
        UNIT_ASSERT(fixture.Cache->Find(regular, 1, victim) == ECacheResultStatus::Miss);
        UNIT_ASSERT(fixture.Cache->Find(regular, 2, survivor) == ECacheResultStatus::Hit);
        UNIT_ASSERT(survivor.CacheItem() == second);
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
    }

    Y_UNIT_TEST(PreloadPreservesSameVersionKeepTokenWhenAnotherThreadReheatsIt) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 135);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 16 * 4096));
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        MovePreloadTestPageToLevel(*fixture.Cache, page, EHotLevel::L0);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsColdInTier(ECacheTier::Regular));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Page;
            bool Reheated = false;
        } context{ *fixture.Cache, collection, page };

        TTestTraits hooks{ &context,
            [](void* opaque, ECacheHookPoint point, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(opaque);
                if (point == ECacheHookPoint::AfterPreloadHotTokenChecked && item == ctx.Page.CacheItem() &&
                    !ctx.Reheated) {
                    ctx.Reheated = true;
                    std::thread reader([&ctx] {
                        TlsCacheHazard = &TCacheThreadTestAccess::Hazard(ctx.Cache, 0);
                        {
                            TTestCachePageRef result;
                            Y_ABORT_UNLESS(ctx.Cache.Find(ctx.Collection, 1, result) == ECacheResultStatus::Hit);
                        }
                        TlsCacheHazard = nullptr;
                    });
                    reader.join();
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(!fixture.Cache->ReclaimForPreload());
        UNIT_ASSERT(context.Reheated);
        AssertPageHit(*fixture.Cache, TCacheKey::Page(collection, 1), page);
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
    }

    Y_UNIT_TEST(PreloadPresenceRetryDoesNotEvictToAllocateHandle) {
        TFixture fixture;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * (4096 + NActors::TSharedData::OverheadSize)));
        const TLogoBlobID id(83, 84, 127);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const auto source = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        const auto other = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 2));
        TTestCachePageRef sourceHold;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, sourceHold) == ECacheResultStatus::Hit);
        for (ui64 value = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++value) {
            const TLogoBlobID fillerId(83, 84, value);
            const auto filler = AllocateCollection(*fixture.Cache, TCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Source;
            TTestCachePageRef& SourceHold;
            TTestCachePageRef Replacement;
            bool Evicted = false;
        } context{ *fixture.Cache, collection, source, sourceHold, {} };

        TTestTraits hooks{ &context, [](void* opaque, ECacheHookPoint point, TCacheItem) noexcept {
                              auto& ctx = *static_cast<TContext*>(opaque);
                              if (point == ECacheHookPoint::AfterPageBatchPrepared && !ctx.Evicted) {
                                  ctx.Evicted = true;
                                  ctx.SourceHold.Drop();
                                  if (TCacheTestAccess::HandleState(ctx.Cache, ctx.Source.Index()).IsHot()) {
                                      Y_ABORT_UNLESS(TCacheTestAccess::EvictFromHot(ctx.Cache, ctx.Source));
                                  }
                                  Y_ABORT_UNLESS(TCacheTestAccess::EraseCold(
                                      ctx.Cache, MakeColdItem(ctx.Cache, ctx.Source.Index())));
                                  InsertReadyPage(ctx.Cache, TCacheKey::Page(ctx.Collection, 9000));
                                  Y_ABORT_UNLESS(ctx.Cache.Find(ctx.Collection, 9000, ctx.Replacement) ==
                                                 ECacheResultStatus::Hit);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        TVector<TTestCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), MakeIntrusive<TPageFetchWaiter>());
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(collection, requests));
        UNIT_ASSERT(context.Evicted);
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Miss);
        UNIT_ASSERT(requests[1].Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT(requests[1].Page().CacheItem() == other);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
    }

    Y_UNIT_TEST(PageBatchRejectsReservationAboveCurrentLimit) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 85);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        const ui64 freeBefore = TCacheTestAccess::FreeCount(*fixture.Cache);
        const ui64 pageBytes = requests[0].Location.Size + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(pageBytes));
        const ui64 overallBefore = fixture.Cache->OverallUsage();
        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overallBefore);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), freeBefore);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);
    }
    Y_UNIT_TEST(ReviewR13WaitingStickyPagesPreserveRetention) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 95);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = MakePageLocation(1);
        const ui64 bytes = location.Size + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ECacheResultStatus::Pending);
        auto& fetch = requests[0].Fetch();
        const auto page = fetch.CacheItem();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, fetch.CacheItem().Index()).IsWaitingForMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, fetch.CacheItem().Index()).IsStickyField());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT(!fetch.Dispatch());
        UNIT_ASSERT(!fetch.TryReserveMemory());
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT(fixture.Cache->AdmissionGeneration() > generation);
        UNIT_ASSERT(fetch.TryReserveMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fetch.TryReserveMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fetch.Dispatch());
        UNIT_ASSERT(fetch.MakeReady(MakePageData(fetch.CacheItem().Index())));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
    }

    Y_UNIT_TEST(CancellingMemoryWaitingPageDoesNotRefundUnownedBytes) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 96);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = MakePageLocation(1);
        const ui64 bytes = location.Size + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(requests[0].Fetch().FailReady());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }

    Y_UNIT_TEST(MemoryWaitingBatchReservesAllPagesBeforeDispatch) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 97);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = MakePageLocation(1);
        const ui64 bytes = location.Size + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(2 * bytes));
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        TVector<TTestPageFetch> fetches;
        for (auto& request : requests) {
            fetches.push_back(std::move(request.Fetch()));
        }
        UNIT_ASSERT(!fixture.Cache->TryReserveFetchBatch(fetches));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        for (auto& fetch : fetches) {
            UNIT_ASSERT(!fetch.Dispatch());
        }
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT(fixture.Cache->TryReserveFetchBatch(fetches));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 2 * bytes);
        for (auto& fetch : fetches) {
            UNIT_ASSERT(fetch.Dispatch());
            UNIT_ASSERT(fetch.MakeReady(MakePageData(fetch.CacheItem().Index())));
        }
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(MemoryWaitingAdmissionSurvivesEvictionAfterBatchPresenceCheck) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 98);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, held) == ECacheResultStatus::Hit);
        const ui64 bytes = held.size() + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));

        struct TContext {
            TTestSharedCache& Cache;
            TPageCacheItem Page;
            bool Evicted = false;
        } context{ *fixture.Cache, page };

        TTestTraits hooks{ &context, [](void* opaque, ECacheHookPoint point, TCacheItem) noexcept {
                              auto& context = *static_cast<TContext*>(opaque);
                              if (point == ECacheHookPoint::AfterPageBatchPrepared && !context.Evicted) {
                                  context.Evicted = true;
                                  Y_ABORT_UNLESS(TCacheTestAccess::EvictFromHot(context.Cache, context.Page));
                                  Y_ABORT_UNLESS(TCacheTestAccess::EraseCold(
                                      context.Cache, MakeColdItem(context.Cache, context.Page.Index())));
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(1), new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(context.Evicted);
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(!requests[0].Fetch().TryReserveMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        held.Drop();
        UNIT_ASSERT(requests[0].Fetch().TryReserveMemory());
        UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(requests[0].Fetch().CacheItem().Index())));
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 1);
    }

    Y_UNIT_TEST(PageBatchWorksAtMinimumTestCapacity) {
        TFixture fixture(MinSharedCacheAddressBits);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Capacity.HotSlotCount(), 24);
        const TLogoBlobID id(83, 84, 86);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        const auto location = MakePageLocation(1);
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ECacheResultStatus::Pending);

        const TPageCacheItem page = requests[0].Fetch().CacheItem();
        UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);

        TVector<TTestCachePageRequest> hit;
        hit.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hit));
        UNIT_ASSERT(hit[0].Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT(hit[0].Page().CacheItem() == page);
    }
    Y_UNIT_TEST(PageBatchUsesCollectionStickyMode) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 87);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(1), new TTestPageFetchWaiter(completed, ready, completedItem));
        requests.emplace_back(MakePageLocation(2), new TTestPageFetchWaiter(completed, ready, completedItem));

        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, false));
        std::array<TPageCacheItem, 2> pageItems;
        for (ui32 index = 0; index < requests.size(); ++index) {
            auto& request = requests[index];
            UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
            pageItems[index] = request.Fetch().CacheItem();
            UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(pageItems[index].Index())));
        }
        const auto stickyState = TCacheTestAccess::HandleState(*fixture.Cache, pageItems[0].Index());
        const auto secondState = TCacheTestAccess::HandleState(*fixture.Cache, pageItems[1].Index());
        UNIT_ASSERT(stickyState.IsSticky());
        UNIT_ASSERT(secondState.IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->StickyOwnedBytes(), 2 * (requests[0].Location.Size + NActors::TSharedData::OverheadSize));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().RequestedPages, 0);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TVector<TTestCachePageRequest> regular;
        regular.emplace_back(MakePageLocation(3), new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, regular));
        UNIT_ASSERT(regular[0].Status() == ECacheResultStatus::Inserted);
        const TPageCacheItem ordinary = regular[0].Fetch().CacheItem();
        UNIT_ASSERT(regular[0].Fetch().MakeReady(MakePageData(ordinary.Index())));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsHot());

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));
        TVector<TTestCachePageRequest> hits;
        hits.emplace_back(requests[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem));
        hits.emplace_back(regular[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
        UNIT_ASSERT(hits[0].Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT(hits[1].Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT(hits[0].Page().IsSticky());
        UNIT_ASSERT(hits[1].Page().IsSticky());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, pageItems[0].Index()).IsSticky());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsSticky());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, pageItems[1].Index()).IsHot());
    }
    Y_UNIT_TEST(RegularPageBatchUsesExplicitStickyHints) {
        TFixture fixture;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * (4096 + NActors::TSharedData::OverheadSize)));
        const TLogoBlobID id(85, 86, 89);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        for (ui32 index = 0; index < 4; ++index) {
            requests.emplace_back(MakePageLocation(index, 4096,
                                      index % 2 ? NTable::NPage::EPage::FlatIndex : NTable::NPage::EPage::DataPage),
                new TTestPageFetchWaiter(completed, ready, completedItem), index >= 2);
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        for (ui32 index = 0; index < requests.size(); ++index) {
            const auto page = requests[index].Fetch().CacheItem();
            UNIT_ASSERT(requests[index].Fetch().MakeReady(MakePageData(page.Index())));
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT_VALUES_EQUAL(state.IsSticky(), index >= 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 2);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 2);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        for (ui32 index = 2; index < requests.size(); ++index) {
            const auto& location = requests[index].Location;
            TVector<TTestCachePageRequest> hits;
            hits.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem), true);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
            UNIT_ASSERT(hits[0].Status() == ECacheResultStatus::Hit);
            UNIT_ASSERT(hits[0].Page().IsSticky());
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 2);
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(ExplicitStickyHintPromotesAnOrdinaryPendingFetch) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 92);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> ordinary;
        ordinary.emplace_back(MakePageLocation(0, 4096, NTable::NPage::EPage::FlatIndex),
            new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, ordinary));
        const auto page = ordinary[0].Fetch().CacheItem();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsStickyNoneField());
        TVector<TTestCachePageRequest> sticky;
        sticky.emplace_back(ordinary[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, sticky));
        UNIT_ASSERT(sticky[0].Status() == ECacheResultStatus::Pending);
        UNIT_ASSERT(ordinary[0].Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 1);
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(FailedStickyFetchUnlinksOnlyItsPage) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 93);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(MakePageLocation(index), new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto failed = requests[0].Fetch().CacheItem();
        const auto kept = requests[1].Fetch().CacheItem();
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 2 * bytes);
        UNIT_ASSERT(requests[0].Fetch().FailReady());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), kept.Index());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, failed.Index()).IsFree());
        UNIT_ASSERT(requests[1].Fetch().MakeReady(MakePageData(kept.Index())));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, kept.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 1);
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(StickyBatchCompletionHonorsModeWithdrawal) {
        for (bool explicitSticky : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(85, 86, 90);
            const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            std::atomic<ui32> completed = 0;
            std::atomic<ui32> ready = 0;
            std::atomic<ui64> completedItem = 0;
            TVector<TTestCachePageRequest> requests;
            requests.emplace_back(
                MakePageLocation(0), new TTestPageFetchWaiter(completed, ready, completedItem), explicitSticky);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
            const auto page = requests[0].Fetch().CacheItem();
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsPending());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), page.Index());
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsStickyNoneField());
            UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(page.Index())));
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        }
    }
    Y_UNIT_TEST(RegularStickyPageLinkedAfterDetachIsReleased) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 91);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), new TTestPageFetchWaiter(completed, ready, completedItem), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto page = requests[0].Fetch().CacheItem();
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforePageReadyState, page.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        bool succeeded = false;
        auto fetch = std::move(requests[0].Fetch());
        TGateThread completer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            succeeded = fetch.MakeReady(MakePageData(page.Index()));
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(succeeded);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
    }
    Y_UNIT_TEST(RegularStickyPageLinkedAfterKeepTransitionIsReleased) {
        TFixture fixture;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * (4096 + NActors::TSharedData::OverheadSize)));
        const TLogoBlobID id(85, 86, 91);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), new TTestPageFetchWaiter(completed, ready, completedItem), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto page = requests[0].Fetch().CacheItem();
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforePageReadyState, page.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        bool succeeded = false;
        auto fetch = std::move(requests[0].Fetch());
        TGateThread completer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            succeeded = fetch.MakeReady(MakePageData(page.Index()));
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(succeeded);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
    }
    Y_UNIT_TEST(PageBatchResultMovesActiveMember) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 88);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        const auto location = MakePageLocation(1);
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));

        TTestCachePageRequest fetchResult(
            MakePageLocation(2), new TTestPageFetchWaiter(completed, ready, completedItem));
        fetchResult = std::move(requests[0]);
        UNIT_ASSERT(fetchResult.Status() == ECacheResultStatus::Inserted);
        const TPageCacheItem page = fetchResult.Fetch().CacheItem();
        UNIT_ASSERT(fetchResult.Fetch().MakeReady(MakePageData(page.Index())));

        TVector<TTestCachePageRequest> hits;
        hits.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
        TTestCachePageRequest hitResult(std::move(hits[0]));
        UNIT_ASSERT(hitResult.Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT(hitResult.Page().CacheItem() == page);
    }
    Y_UNIT_TEST(PageBatchOverflowLeavesOutputsUntouched) {
        TFixture fixture;
        const TLogoBlobID id(89, 90, 91);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(
            MakePageLocation(1, Max<ui64>() / 2), new TTestPageFetchWaiter(completed, ready, completedItem));
        requests.emplace_back(
            MakePageLocation(2, Max<ui64>() / 2), new TTestPageFetchWaiter(completed, ready, completedItem));
        requests[0].SetHit({});
        requests[1].SetPending();

        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT(requests[1].Status() == ECacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(PageBatchHandleClaimRollsBack) {
        TFixture fixture;
        const TLogoBlobID id(87, 88, 89);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        const ui32 requestCount = static_cast<ui32>(fixture.Capacity.HandleCount());
        for (ui32 index = 0; index < requestCount; ++index) {
            requests.emplace_back(
                MakePageLocation(index + 1, 1), new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        const ui64 availableBefore = AvailableFreeHandles(*fixture.Cache);

        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), availableBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);
        for (const TTestCachePageRequest& request : requests) {
            TTestCachePageRef missing;
            UNIT_ASSERT(fixture.Cache->Find(collection, static_cast<ui64>(request.Location.Offset), missing) ==
                        ECacheResultStatus::Miss);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT(TCacheTestAccess::EraseCollection(*fixture.Cache, fixture.Registry, collection));
    }
    Y_UNIT_TEST(PageBatchCoalescesDuplicateCandidates) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 87);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = MakePageLocation(3);
        const ui64 pageBytes = location.Size + NActors::TSharedData::OverheadSize;
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        }

        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        ui32 inserted = 0;
        ui32 owner = 0;
        for (ui32 index = 0; index < requests.size(); ++index) {
            if (requests[index].Status() == ECacheResultStatus::Inserted) {
                ++inserted;
                owner = index;
                UNIT_ASSERT(requests[index].Fetch());
            } else {
                UNIT_ASSERT(requests[index].Status() == ECacheResultStatus::Pending);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(inserted, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        const TPageCacheItem page = requests[owner].Fetch().CacheItem();
        UNIT_ASSERT(requests[owner].Fetch().Dispatch());
        UNIT_ASSERT(requests[owner].Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(PageBatchTelemetryCountsAdmissionOutcomes) {
        TFixture fixture;
        const TLogoBlobID id(93, 94, 95);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto first = MakePageLocation(1, 4096);
        const auto second = MakePageLocation(2, 8192);
        const ui64 requestedBytes = first.Size + second.Size;
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        const auto addRequest = [&](TVector<TTestCachePageRequest>& requests,
                                    const NTable::NPage::TPageLocation& location) {
            requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        };

        const TTestSharedCache::TStats empty = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(empty.RequestedPages, 0);
        UNIT_ASSERT_VALUES_EQUAL(empty.RequestedBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(empty.MissBytes, 0);

        TVector<TTestCachePageRequest> misses;
        addRequest(misses, first);
        addRequest(misses, second);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, misses));
        for (TTestCachePageRequest& request : misses) {
            UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
            UNIT_ASSERT(request.Fetch());
            const TPageCacheItem page = request.Fetch().CacheItem();
            UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(page.Index(), request.Location.Size)));
        }

        const TTestSharedCache::TStats admitted = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(admitted.RequestedPages, 2);
        UNIT_ASSERT_VALUES_EQUAL(admitted.RequestedBytes, requestedBytes);
        UNIT_ASSERT_VALUES_EQUAL(admitted.HitPages, 0);
        UNIT_ASSERT_VALUES_EQUAL(admitted.HitBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(admitted.MissPages, 2);
        UNIT_ASSERT_VALUES_EQUAL(admitted.MissBytes, requestedBytes);

        TVector<TTestCachePageRequest> resident;
        addRequest(resident, first);
        addRequest(resident, second);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, resident));
        for (const TTestCachePageRequest& request : resident) {
            UNIT_ASSERT(request.Status() == ECacheResultStatus::Hit);
            UNIT_ASSERT(request.Page());
        }

        const TTestSharedCache::TStats served = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(served.RequestedPages, 4);
        UNIT_ASSERT_VALUES_EQUAL(served.HitPages, 2);
        UNIT_ASSERT_VALUES_EQUAL(served.HitBytes, requestedBytes);
        UNIT_ASSERT_VALUES_EQUAL(served.MissPages, 2);
        UNIT_ASSERT_VALUES_EQUAL(served.MissBytes, requestedBytes);

        TVector<TTestCachePageRequest> keepRequest;
        addRequest(keepRequest, MakePageLocation(3, 4096));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, keepRequest));
        UNIT_ASSERT(keepRequest[0].Status() == ECacheResultStatus::Inserted);
        const TPageCacheItem sticky = keepRequest[0].Fetch().CacheItem();
        UNIT_ASSERT(keepRequest[0].Fetch().MakeReady(MakePageData(sticky.Index(), keepRequest[0].Location.Size)));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, sticky.Index()).IsSticky());

        const TTestSharedCache::TStats kept = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(kept.RequestedPages, 5);
        UNIT_ASSERT_VALUES_EQUAL(kept.MissPages, 3);

        TVector<TTestCachePageRequest> anotherSticky;
        addRequest(anotherSticky, MakePageLocation(4, 4096));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, anotherSticky));
        UNIT_ASSERT(anotherSticky[0].Status() == ECacheResultStatus::Inserted);
        const TPageCacheItem anotherPage = anotherSticky[0].Fetch().CacheItem();
        UNIT_ASSERT(
            anotherSticky[0].Fetch().MakeReady(MakePageData(anotherPage.Index(), anotherSticky[0].Location.Size)));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, anotherPage.Index()).IsSticky());

        const TTestSharedCache::TStats moreKept = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(moreKept.RequestedPages, 6);
        UNIT_ASSERT_VALUES_EQUAL(moreKept.MissPages, 4);

        TVector<TTestCachePageRequest> rejected;
        addRequest(rejected, MakePageLocation(5, Max<ui64>() / 2));
        addRequest(rejected, MakePageLocation(6, Max<ui64>() / 2));
        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, rejected));

        const TTestSharedCache::TStats unchanged = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(unchanged.RequestedPages, moreKept.RequestedPages);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.RequestedBytes, moreKept.RequestedBytes);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.HitPages, moreKept.HitPages);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.MissPages, moreKept.MissPages);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.MissBytes, moreKept.MissBytes);
    }
    Y_UNIT_TEST(PageCountersFollowResidency) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(95, 96);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 0);

        TPageCacheItem hotItem;
        TTestCachePageRef hit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1, 4096),
                        EStickyState::None, hotItem, hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(hotItem, MakePageData(hotItem.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 0);

        const TCollectionLocation location{ .Id = TLogoBlobID(95, 96, 97), .BackingSize = 113 };
        TCollectionCacheItem insertedCollection;
        TTestCacheCollectionRef collectionRef;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsertUnregistered(
                        *fixture.Cache, location, insertedCollection, collectionRef) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(
            TCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, insertedCollection, MakeCollection(location.Id)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(insertedCollection, ECacheMode::Sticky));
        TPageCacheItem keepItem;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, insertedCollection, MakePageLocation(2, 4096),
                        EStickyState::Sticky, keepItem, hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(keepItem, MakePageData(keepItem.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 0);
    }
    Y_UNIT_TEST(CollectionPagesCacheModeTransitionsAreExclusive) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 104);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        TCollection* value = TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());
        const ui64 generation = value->KeepGeneration();
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(4 * bytes));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> pending;
        pending.emplace_back(MakePageLocation(0), new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, pending));
        const auto page = pending[0].Fetch().CacheItem();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT_VALUES_EQUAL(value->KeepGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT_VALUES_EQUAL(value->KeepGeneration(), generation + 1);
        UNIT_ASSERT(pending[0].Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));
        UNIT_ASSERT_VALUES_EQUAL(value->KeepGeneration(), generation + 2);
        for (ui32 step = 0; step < 4096 && TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold();
             ++step) {
            UNIT_ASSERT(fixture.Cache->RunMaintenance());
        }
        UNIT_ASSERT(!TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        TVector<TTestCachePageRequest> hits;
        hits.emplace_back(pending[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
        UNIT_ASSERT(hits[0].Status() == ECacheResultStatus::Hit);
        UNIT_ASSERT(hits[0].Page().IsSticky());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(value->KeepGeneration(), generation + 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        TVector<TTestCachePageRequest> explicitSticky;
        explicitSticky.emplace_back(
            pending[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, explicitSticky));
        UNIT_ASSERT(explicitSticky[0].Page().IsSticky());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(AttachmentTransitionRejectsCollectionPagesCacheModeChanges) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 105);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto page = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 0), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        bool detached = false;
        TGateThread detacher(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            detached = TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsTransitionField());
        for (ECacheMode mode : { ECacheMode::Regular, ECacheMode::Sticky, ECacheMode::TryKeepInMemory }) {
            UNIT_ASSERT(!fixture.Cache->SetCollectionPagesCacheMode(collection, mode));
        }
        gate.Slots[0].Release();
        detacher.Join();
        UNIT_ASSERT(detached);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(PairedKeepCreditsBoundUniquePendingAndIgnoreSubscribers) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 1), ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, bytes));
        TTestCachePageRef first;
        TTestCachePageRef second;
        auto firstWaiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, first);
        auto secondWaiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, second);
        TTestCachePageRequest requests[] = { { MakePageLocation(0), firstWaiter },
            { MakePageLocation(0), secondWaiter } };
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ECacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepPendingBytes(), bytes);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        for (ui32 probe = 0; probe < 32; ++probe) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLoadRoom().Bytes, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        const auto page = requests[0].Fetch().CacheItem();
        UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(firstWaiter->Ready() && secondWaiter->Ready());
        UNIT_ASSERT(first.CacheItem() == second.CacheItem());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepPendingBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
    }

    Y_UNIT_TEST(PairedKeepRefusalReturnsOwnedTransientWithoutRegularSpill) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 2), ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 2 * bytes));
        TTestCachePageRef first;
        TTestCachePageRef second;
        TTestCachePageRef overflow;
        const auto a = InsertOwnedPage(*fixture.Cache, collection, 0, first);
        const auto b = InsertOwnedPage(*fixture.Cache, collection, 1, second);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, a));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, b));
        const auto uncached = InsertOwnedPage(*fixture.Cache, collection, 2, overflow);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 2);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, uncached.Index()).IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(overflow.size(), 4096);
        TTestCachePageRef found;
        UNIT_ASSERT(fixture.Cache->Find(collection, 2, found) == ECacheResultStatus::Miss);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 3 * bytes);
        overflow.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
    }

    Y_UNIT_TEST(PairedHeldColdFinalReleaseReconcilesBothModeDirections) {
        for (bool fromKeep : { false, true }) {
            TFixture fixture;
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            const auto collection = CreateModeCollection(*fixture.Cache, fixture.Registry,
                TLogoBlobID(195, 196, 3u + fromKeep), fromKeep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular);
            UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 2 * bytes));
            TTestCachePageRef held;
            const auto page = InsertOwnedPage(*fixture.Cache, collection, 0, held);
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                collection, fromKeep ? ECacheMode::Regular : ECacheMode::TryKeepInMemory));
            for (ui32 step = 0; step < 1024 && fixture.Cache->RunMaintenance(); ++step) {
            }
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index())
                            .IsColdInTier(fromKeep ? ECacheTier::Keep : ECacheTier::Regular));
            held.Drop();
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index())
                            .IsColdInTier(fromKeep ? ECacheTier::Regular : ECacheTier::Keep));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), fromKeep ? 0 : bytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        }
    }

    Y_UNIT_TEST(PairedKeepModeOffPreservesColdPageWithZeroKeepBudget) {
        for (bool heldDuringModeChange : { false, true }) {
            TFixture fixture;
            constexpr ui32 size = 128;
            const ui64 bytes = size + NActors::TSharedData::OverheadSize;
            UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
            const auto collection = CreateModeCollection(*fixture.Cache, fixture.Registry,
                TLogoBlobID(195, 196, 55u + heldDuringModeChange), ECacheMode::TryKeepInMemory);
            TTestCachePageRef held;
            const auto page = InsertOwnedSizedPage(*fixture.Cache, collection, 0, size, 'r', held);
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            if (!heldDuringModeChange) {
                held.Drop();
            }
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(0));
            if (heldDuringModeChange) {
                held.Drop();
            }
            UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 0));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
            TTestCachePageRef found;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, found) == ECacheResultStatus::Hit);
            UNIT_ASSERT(found.CacheItem() == page);
            UNIT_ASSERT_VALUES_EQUAL(found.data()[0], 'r');
        }
    }

    Y_UNIT_TEST(PairedStructuralColdReleasePreservesSingleSlotResident) {
        TFixture fixture;
        constexpr ui32 size = 128;
        const ui64 bytes = size + NActors::TSharedData::OverheadSize;
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 50), ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            UNIT_ASSERT_VALUES_EQUAL(spaceOp.View().Tiers[ECacheTier::Keep].Cold().Capacity, 1);
        });
        TTestCachePageRef held;
        const auto page = InsertOwnedSizedPage(*fixture.Cache, collection, 0, size, 'a', held);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        held.Drop();
        const ui32 membership = TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
        auto structural = TCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem());
        UNIT_ASSERT(structural);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()), membership);
        structural.Drop();
        const auto state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(page.CacheItem().Matches(state) && state.IsKeepCold() && state.Refs() == 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(PairedClaimedColdPublicationSurvivesStructuralAcquire) {
        TFixture fixture;
        constexpr ui32 size = 128;
        const ui64 bytes = size + NActors::TSharedData::OverheadSize;
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 52), ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
        TTestCachePageRef held;
        const auto page = InsertOwnedSizedPage(*fixture.Cache, collection, 0, size, 'a', held);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));

        struct TContext {
            TTestSharedCache& Cache;
            TPageCacheItem Page;
            TTestCachePageRef Acquired;
            bool Visited = false;
        } context{ *fixture.Cache, page, {} };

        TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (at == ECacheHookPoint::AfterColdTicketClaim &&
                                  item.Index() == ctx.Page.Index() && !ctx.Visited) {
                                  ctx.Visited = true;
                                  Y_ABORT_UNLESS(
                                      TCacheTestAccess::AcquirePage(ctx.Cache, ctx.Page, ctx.Acquired));
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        held.Drop();
        UNIT_ASSERT(context.Visited && context.Acquired);
        UNIT_ASSERT_VALUES_EQUAL(context.Acquired.data()[0], 'a');
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        context.Acquired.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(PairedPreloadColdCarrySurvivesHeldVictimAndOccupiedVacancy) {
        for (bool occupyVacancy : { false, true }) {
            TFixture fixture;
            constexpr ui32 size = 128;
            const ui64 bytes = size + NActors::TSharedData::OverheadSize;
            UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
            const auto regular =
                CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 53), ECacheMode::Regular);
            const auto keep = CreateModeCollection(
                *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 54), ECacheMode::TryKeepInMemory);
            TTestCachePageRef regularRef;
            TTestCachePageRef keepRef;
            const auto victim = InsertOwnedSizedPage(*fixture.Cache, regular, 0, size, 'r', regularRef);
            const auto protectedPage = InsertOwnedSizedPage(*fixture.Cache, keep, 0, size, 'k', keepRef);
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, victim));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, protectedPage));
            regularRef.Drop();
            keepRef.Drop();
            const auto victimTicket = MakeColdItem(*fixture.Cache, victim.Index());
            const auto protectedTicket = MakeColdItem(*fixture.Cache, protectedPage.Index());
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
                for (const auto& [tier, ticket] : { std::pair{ ECacheTier::Regular, victimTicket },
                         std::pair{ ECacheTier::Keep, protectedTicket } }) {
                    const auto& source = spaceOp.View().Tiers[tier];
                    bool found = false;
                    for (ui64 attempt = 0; attempt < source.Cold().Capacity; ++attempt) {
                        const ui64 raw = source.Cold().Pop();
                        if (raw != 0) {
                            source.Rings->RemoveColdEntry();
                            UNIT_ASSERT_VALUES_EQUAL(raw, ticket.Raw());
                            found = true;
                            break;
                        }
                    }
                    UNIT_ASSERT(found);
                }
            });
            std::array<std::atomic<ui64>, 1> slots{};
            TCacheTierRings rings;
            rings.AddColdEntry();
            slots[0].store(victimTicket.Raw(), std::memory_order_release);
            const TCacheTierView tier{ &rings, {}, { slots.data(), sizeof(slots), slots.size() } };

            struct TContext {
                TTestSharedCache& Cache;
                TPageCacheItem Victim;
                TColdCacheItem Protected;
                TCacheTierRings& Rings;
                std::atomic<ui64>& Slot;
                TTestCachePageRef Held;
                bool Occupy;
                bool Visited = false;
            } context{ *fixture.Cache, victim, protectedTicket, rings, slots[0], {}, occupyVacancy };

            TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                                  auto& ctx = *static_cast<TContext*>(ptr);
                                  if (at == ECacheHookPoint::BeforePreloadPageErase && item == ctx.Victim.CacheItem() &&
                                      !ctx.Visited) {
                                      ctx.Visited = true;
                                      Y_ABORT_UNLESS(TCacheTestAccess::AcquirePage(ctx.Cache, ctx.Victim, ctx.Held));
                                      if (ctx.Occupy) {
                                          ctx.Rings.AddColdEntry();
                                          ctx.Slot.store(ctx.Protected.Raw(), std::memory_order_release);
                                      }
                                  }
                              } };
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
                UNIT_ASSERT_VALUES_EQUAL(policy.ReclaimForPreload(spaceOp, tier), occupyVacancy);
            });
            UNIT_ASSERT(context.Visited && context.Held);
            UNIT_ASSERT_VALUES_EQUAL(context.Held.data()[0], 'r');
            UNIT_ASSERT_VALUES_EQUAL(rings.ColdEntries(), 1);
            UNIT_ASSERT_VALUES_EQUAL(slots[0].load(), occupyVacancy ? protectedTicket.Raw() : victimTicket.Raw());
            const auto state = TCacheTestAccess::HandleState(*fixture.Cache, victim.Index());
            UNIT_ASSERT(occupyVacancy ? state.IsTombstone() : state.IsCold());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), occupyVacancy ? bytes : 0);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, protectedPage.Index()).IsKeepCold());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
            context.Held.Drop();
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
                if (!occupyVacancy) {
                    UNIT_ASSERT(policy.ReclaimForPreload(spaceOp, tier));
                } else {
                    slots[0].store(0);
                    rings.RemoveColdEntry();
                }
                policy.RouteCold(spaceOp, spaceOp.View().Tiers[ECacheTier::Keep], protectedTicket);
            });
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
            UNIT_ASSERT(fixture.Cache->ReclaimCold());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        }
    }

    Y_UNIT_TEST(PairedDuplicateColdPublicationPreservesSingleSlotResident) {
        TFixture fixture;
        constexpr ui32 size = 128;
        const ui64 bytes = size + NActors::TSharedData::OverheadSize;
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 51), ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
        TTestCachePageRef held;
        const auto page = InsertOwnedSizedPage(*fixture.Cache, collection, 0, size, 'a', held);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        held.Drop();
        TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            const auto& keep = spaceOp.View().Tiers[ECacheTier::Keep];
            UNIT_ASSERT_VALUES_EQUAL(keep.Cold().Capacity, 1);
            policy.PublishCold(spaceOp, keep, page.CacheItem());
            UNIT_ASSERT_VALUES_EQUAL(keep.Rings->ColdEntries(), 1);
        });
        const auto state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(page.CacheItem().Matches(state) && state.IsKeepCold() && state.Refs() == 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(PairedKeepColdOverflowRetiresHeldVictimWithoutPhysicalRefund) {
        TFixture fixture;
        constexpr ui32 size = 128;
        const ui64 bytes = size + NActors::TSharedData::OverheadSize;
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 5), ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
        TTestCachePageRef first;
        const auto a = InsertOwnedSizedPage(*fixture.Cache, collection, 0, size, 'a', first);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, a));
        first.Drop();
        UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, a, first));
        TTestCachePageRef second;
        const auto b = InsertOwnedSizedPage(*fixture.Cache, collection, 1, size, 'b', second);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, b));
        second.Drop();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, a.Index()).IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(first.data()[0], 'a');
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        first.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
    }

    Y_UNIT_TEST(PairedStickyGrowthImmediatelyLowersForeignKeepAdmission) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(100 * bytes));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
        const auto keep = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 6), ECacheMode::TryKeepInMemory);
        const auto sticky =
            CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 7), ECacheMode::Sticky);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 10 * bytes));
        TTestCachePageRef a;
        TTestCachePageRef b;
        const auto ap = InsertOwnedPage(*fixture.Cache, keep, 0, a);
        const auto bp = InsertOwnedPage(*fixture.Cache, keep, 1, b);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, ap));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, bp));
        for (ui32 offset = 0; offset < 6; ++offset) {
            TTestCachePageRef data;
            InsertOwnedPage(*fixture.Cache, sticky, offset, data);
            UNIT_ASSERT(data.IsSticky());
        }
        UNIT_ASSERT(fixture.Cache->KeepLimit() < 2 * bytes);
        TTestCachePageRef refused;
        const auto page = InsertOwnedPage(*fixture.Cache, keep, 2, refused);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 2 * bytes);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(refused.size(), 4096);
    }

    Y_UNIT_TEST(PairedReadyCallbacksReadRetiredAndRelocatedResults) {
        for (bool relocate : { false, true }) {
            TFixture fixture;
            const auto collection = CreateModeCollection(
                *fixture.Cache, fixture.Registry, TLogoBlobID(195, 197, 1u + relocate), ECacheMode::Regular);
            TTestCachePageRef first;
            TTestCachePageRef second;
            auto a = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, first);
            auto b = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, second);
            TTestCachePageRequest requests[] = { { MakePageLocation(0), a }, { MakePageLocation(0), b } };
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
            const auto original = requests[0].Fetch().CacheItem();

            struct TContext {
                TTestSharedCache& Cache;
                TPageCacheItem Original;
                bool Relocate;
                bool Visited = false;
                bool Moved = false;
                bool Erased = false;
            } context{ *fixture.Cache, original, relocate };

            TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                                  auto& ctx = *static_cast<TContext*>(ptr);
                                  if (at == ECacheHookPoint::BeforeReadyWaiters &&
                                      item == ctx.Original.CacheItem() && !ctx.Visited) {
                                      ctx.Visited = true;
                                      if (ctx.Relocate) {
                                          ctx.Moved = TCacheTestAccess::RelocatePage(ctx.Cache, ctx.Original);
                                      }
                                      TTestCachePageRef current;
                                      if (TCacheTestAccess::AcquirePage(ctx.Cache, ctx.Original, current)) {
                                          ctx.Erased =
                                              TCacheTestAccess::RetirePageResult(ctx.Cache, current.CacheItem());
                                      }
                                  }
                              } };
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(original.Index())));
            UNIT_ASSERT(context.Visited && context.Erased && (!relocate || context.Moved));
            UNIT_ASSERT(a->Ready() && b->Ready());
            UNIT_ASSERT_VALUES_EQUAL(first.size(), 4096);
            UNIT_ASSERT_VALUES_EQUAL(second.size(), 4096);
            UNIT_ASSERT_VALUES_EQUAL(first.data(), second.data());
            UNIT_ASSERT_VALUES_EQUAL(first.data()[0], second.data()[0]);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 4096 + NActors::TSharedData::OverheadSize);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), fixture.Cache->PayloadUsage());
            TTestCachePageRef missing;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, missing) == ECacheResultStatus::Miss);
            first.Drop();
            second.Drop();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), 0);
            UNIT_ASSERT(!TCacheTestAccess::AcquirePage(*fixture.Cache, original, missing));
        }
    }

    Y_UNIT_TEST(PairedPostDeliveryReconciliationPreservesKeepAndStickyOwners) {
        for (bool sticky : { false, true }) {
            TFixture fixture;
            const auto collection = CreateModeCollection(
                *fixture.Cache, fixture.Registry, TLogoBlobID(195, 197, 3u + sticky), ECacheMode::Regular);
            TTestCachePageRef result;
            auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, result);
            TTestCachePageRequest request(MakePageLocation(0), waiter);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
            const auto page = request.Fetch().CacheItem();

            struct TContext {
                TTestSharedCache& Cache;
                TCollectionCacheItem Collection;
                TPageCacheItem Page;
                bool Sticky;
                bool Visited = false;
                bool Changed = false;
            } context{ *fixture.Cache, collection, page, sticky };

            TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                                  auto& ctx = *static_cast<TContext*>(ptr);
                                  if (at == ECacheHookPoint::BeforeReadyWaiters && item == ctx.Page.CacheItem() &&
                                      !ctx.Visited) {
                                      ctx.Visited = true;
                                      if (ctx.Sticky) {
                                          ctx.Changed = TCacheTestAccess::MakePageSticky(ctx.Cache, ctx.Page);
                                      } else {
                                          ctx.Changed =
                                              ctx.Cache.SetCollectionPagesCacheMode(
                                                  ctx.Collection, ECacheMode::TryKeepInMemory) &&
                                              ctx.Cache.UpdateKeepLimit(4096 + NActors::TSharedData::OverheadSize);
                                          TCacheTestAccess::RouteOwnedPage(ctx.Cache, ctx.Page);
                                      }
                                  }
                              } };
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(page.Index())));
            UNIT_ASSERT(context.Visited && context.Changed && waiter->Ready());
            const auto state = TCacheTestAccess::HandleState(*fixture.Cache, result.CacheItem().Index());
            UNIT_ASSERT(sticky ? state.IsSticky() : state.IsHotInTier(ECacheTier::Keep));
            UNIT_ASSERT_VALUES_EQUAL(
                fixture.Cache->KeepOwnedBytes(), sticky ? 0 : 4096 + NActors::TSharedData::OverheadSize);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), 0);
            result.Drop();
            TTestCachePageRef cached;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, cached) == ECacheResultStatus::Hit);
        }
    }

    Y_UNIT_TEST(PairedReconciliationFollowsRelocationAfterOwnerAcquisition) {
        TFixture fixture;
        const auto collection =
            CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(195, 197, 9), ECacheMode::Regular);
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(0));
        TTestCachePageRef result;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, result);
        TTestCachePageRequest request(MakePageLocation(0), waiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        const auto page = request.Fetch().CacheItem();

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Page;
            bool Visited = false;
            bool Moved = false;
        } context{ *fixture.Cache, collection, page };

        TTestTraits hooks{ &context,
            [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (at == ECacheHookPoint::BeforeReadyReconcile && item == ctx.Page.CacheItem() && !ctx.Visited) {
                    ctx.Visited = true;
                    ctx.Moved = TCacheTestAccess::RelocatePage(ctx.Cache, ctx.Page);
                    Y_ABORT_UNLESS(ctx.Cache.SetCollectionPagesCacheMode(ctx.Collection, ECacheMode::TryKeepInMemory));
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(context.Visited && context.Moved && waiter->Ready());
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 4096);
        TTestCachePageRef current;
        UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, page, current));
        UNIT_ASSERT(current.CacheItem().Index() != page.Index());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, current.CacheItem().Index()).IsTombstone());
        TTestCachePageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, missing) == ECacheResultStatus::Miss);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetiredBytes(), 4096 + NActors::TSharedData::OverheadSize);
        current.Drop();
        result.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(PairedLateColdPublicationRestartsCompletedModeSweep) {
        for (bool fromKeep : { false, true }) {
            TFixture fixture;
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            ConfigureKeep(*fixture.Cache, 2 * bytes);
            const auto collection = CreateModeCollection(*fixture.Cache, fixture.Registry,
                TLogoBlobID(195, 197, 10u + fromKeep), fromKeep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular);
            TTestCachePageRef held;
            const auto page = InsertOwnedPage(*fixture.Cache, collection, 0, held);
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));

            struct TContext {
                TTestSharedCache& Cache;
                TCollectionCacheItem Collection;
                TPageCacheItem Page;
                bool FromKeep;
                bool Visited = false;
                bool SweepFinished = false;
            } context{ *fixture.Cache, collection, page, fromKeep };

            TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                                  auto& ctx = *static_cast<TContext*>(ptr);
                                  if (at == ECacheHookPoint::AfterLastColdRefReleased && item == ctx.Page.CacheItem() &&
                                      !ctx.Visited) {
                                      ctx.Visited = true;
                                      Y_ABORT_UNLESS(ctx.Cache.SetCollectionPagesCacheMode(ctx.Collection,
                                          ctx.FromKeep ? ECacheMode::Regular : ECacheMode::TryKeepInMemory));
                                      for (ui32 step = 0; step < 1024; ++step) {
                                          if (!TCacheTestAccess::SweepModes(ctx.Cache)) {
                                              ctx.SweepFinished = true;
                                              break;
                                          }
                                      }
                                  }
                              } };
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            held.Drop();
            UNIT_ASSERT(context.Visited && context.SweepFinished);
            for (ui32 step = 0; step < 2048 && fixture.Cache->RunMaintenance(); ++step) {
            }
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index())
                            .IsColdInTier(fromKeep ? ECacheTier::Regular : ECacheTier::Keep));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), fromKeep ? 0 : bytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(
                TCacheTestAccess::ColdEntries(*fixture.Cache, fromKeep ? ECacheTier::Regular : ECacheTier::Keep), 1);
        }
    }

    Y_UNIT_TEST(PairedModeSweepProgressesWhileCapacityIsBlocked) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 2 * bytes));
        const auto collection =
            CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(195, 197, 12), ECacheMode::Regular);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(3 * bytes));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        auto lock = TCacheTestAccess::LockHotResize(*fixture.Cache);
        UNIT_ASSERT(lock);
        UNIT_ASSERT(fixture.Cache->NeedsCapacityMaintenance());
        for (ui32 step = 0;
             step < 1024 && !TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsColdInTier(ECacheTier::Keep);
             ++step) {
            fixture.Cache->RunMaintenance();
        }
        UNIT_ASSERT(fixture.Cache->NeedsCapacityMaintenance());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsColdInTier(ECacheTier::Keep));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
    }

    Y_UNIT_TEST(PairedColdObserverRoutesConcurrentHotRetier) {
        for (bool releasing : { false, true }) {
            for (bool fromKeep : { false, true }) {
                TFixture fixture;
                const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
                UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
                const auto collection = CreateModeCollection(*fixture.Cache, fixture.Registry,
                    TLogoBlobID(195, 198, 1u + releasing * 2 + fromKeep),
                    fromKeep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular);
                TTestCachePageRef owned;
                const auto page = InsertOwnedPage(*fixture.Cache, collection, 0, owned);
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
                if (!releasing) {
                    owned.Drop();
                }
                TSharedCacheGate gate;
                gate.Slots[0].Arm(releasing ? ECacheHookPoint::BeforeLastColdModeCheck
                                            : ECacheHookPoint::AfterColdRefAcquired, page.CacheItem());
                TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
                TTestCachePageRef first;
                auto cache = fixture.Cache;
                TGateThread observer(gate, [&] {
                    Y_ABORT_UNLESS(!TlsCacheHazard);
                    TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*cache, 0);
                    Y_DEFER {
                        TlsCacheHazard = nullptr;
                    };
                    if (releasing) {
                        owned.Drop();
                    } else {
                        Y_ABORT_UNLESS(cache->Find(collection, 0, first) == ECacheResultStatus::Hit);
                    }
                });
                gate.Slots[0].Wait();
                TTestCachePageRef second;
                UNIT_ASSERT(cache->Find(collection, 0, second) == ECacheResultStatus::Hit);
                UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, page.Index())
                                .IsHotInTier(fromKeep ? ECacheTier::Keep : ECacheTier::Regular));
                second.Drop();
                UNIT_ASSERT(cache->SetCollectionPagesCacheMode(
                    collection, fromKeep ? ECacheMode::Regular : ECacheMode::TryKeepInMemory));
                gate.Slots[0].Release();
                observer.Join();
                first.Drop();
                const ECacheTier destination = fromKeep ? ECacheTier::Regular : ECacheTier::Keep;
                UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, page.Index()).IsHotInTier(destination));
                const bool queued = TCacheTestAccess::WithSpace(*cache, [&](auto& op) {
                    const auto& storage = op.View().Tiers[destination].HotStorage;
                    for (ui64 slot = 0; slot < storage.SlotCount; ++slot) {
                        if (storage.Slots[slot].load(std::memory_order_acquire) == page.CacheItem().Raw()) {
                            return true;
                        }
                    }
                    return false;
                });
                UNIT_ASSERT_C(queued, "A successful Hot retier must publish a token in its destination tier");
                UNIT_ASSERT_VALUES_EQUAL(cache->KeepOwnedBytes(), fromKeep ? 0 : bytes);
                UNIT_ASSERT(cache->UpdateCurrentLimit(1));
                UNIT_ASSERT(cache->UpdateSoftLimitOverride(0));
                for (ui32 step = 0; step < 4096 && cache->PayloadUsage(); ++step) {
                    cache->EnforceCurrentLimit();
                    cache->RunMaintenance();
                }
                UNIT_ASSERT_VALUES_EQUAL(cache->PayloadUsage(), 0);
                UNIT_ASSERT_VALUES_EQUAL(cache->KeepOwnedBytes(), 0);
            }
        }
    }

    Y_UNIT_TEST(PairedGlobalColdReclaimPrioritizesRegularThenKeep) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 3 * bytes));
        const auto regular =
            CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(195, 198, 5), ECacheMode::Regular);
        const auto keep = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 198, 6), ECacheMode::TryKeepInMemory);
        const auto r = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 0));
        const auto k = InsertReadyPage(*fixture.Cache, TCacheKey::Page(keep, 0));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, r));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, k));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 2 * bytes);
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, r.Index()).IsFree());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, k.Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, k.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT(!fixture.Cache->ReclaimCold());
    }

    Y_UNIT_TEST(PairedModeSweepDoesNotAgeUnchangedHotTier) {
        TFixture fixture(8);
        const ui64 largePageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(32 * largePageBytes));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * largePageBytes));
        const auto keep = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 198, 7), ECacheMode::TryKeepInMemory);
        const auto other =
            CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(195, 198, 8), ECacheMode::Regular);
        for (ui32 step = 0; step < 4096 && fixture.Cache->RunMaintenance(); ++step) {
        }
        const ui64 count = TCacheTestAccess::WithSpace(*fixture.Cache, [](auto& op) {
            return op.View().Tiers[ECacheTier::Keep].Hot(EHotLevel::L0).Capacity;
        });
        UNIT_ASSERT(count > 1);
        TVector<TPageCacheItem> pages;
        for (ui32 offset = 0; offset < count; ++offset) {
            TTestCachePageRef owned;
            const auto page = InsertOwnedSizedPage(*fixture.Cache, keep, offset, 1, 'x', owned);
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& op) {
                const auto& tier = op.View().Tiers[ECacheTier::Keep];
                const auto l1 = tier.Hot(EHotLevel::L1);
                bool moved = false;
                for (ui64 slot = 0; slot < l1.Capacity; ++slot) {
                    ui64 expected = page.CacheItem().Raw();
                    if (l1.Slots[slot].compare_exchange_strong(expected, 0)) {
                        UNIT_ASSERT_VALUES_EQUAL(tier.Hot(EHotLevel::L0).PutAndPop(page.CacheItem().Raw()), 0);
                        moved = true;
                        break;
                    }
                }
                UNIT_ASSERT(moved);
            });
            pages.push_back(page);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), count);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdPages(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(other, ECacheMode::TryKeepInMemory));
        for (ui32 step = 0; step < 4096 && fixture.Cache->RunMaintenance(); ++step) {
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), count);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), count);
        for (auto page : pages) {
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHotInTier(ECacheTier::Keep));
        }
    }

    Y_UNIT_TEST(PairedKeepLimitRefreshDoesNotInspectResidentModes) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 3 * bytes));
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 198, 9), ECacheMode::TryKeepInMemory);
        for (ui32 step = 0; step < 4096 && fixture.Cache->RunMaintenance(); ++step) {
        }
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui32 membership = TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
        const ui64 generation = fixture.Cache->BeginOperation().Eviction().ModeGeneration();
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->BeginOperation().Eviction().ModeGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()), membership);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT(!TCacheTestAccess::SweepModes(*fixture.Cache));
    }

    Y_UNIT_TEST(PairedPendingModeWalkRestartsAfterLogicalHotCut) {
        constexpr ui32 BeforeCut = 546;
        constexpr ui32 AfterCut = 512;
        constexpr ui64 PageSlot = THotRingLayout{ BeforeCut }.Ring(EHotLevel::L1).End() - 1;
        static_assert(THotRingLayout{ BeforeCut }.Ring(EHotLevel::L0).Size == SharedCacheTransitionWorkBatch);
        static_assert(PageSlot >= THotRingLayout{ AfterCut }.Ring(EHotLevel::L0).Offset);
        static_assert(PageSlot < THotRingLayout{ AfterCut }.Ring(EHotLevel::L0).End());
        TFixture fixture(10);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(32 * bytes));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
        const auto collection =
            CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(195, 198, 10), ECacheMode::Regular);
        for (ui32 step = 0; step < 4096 && fixture.Cache->RunMaintenance(); ++step) {
        }
        UNIT_ASSERT(TCacheTestAccess::BeginHotResize(*fixture.Cache, BeforeCut));
        UNIT_ASSERT(TCacheTestAccess::DrainHotResize(*fixture.Cache));
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& op) {
            const auto& tier = op.View().Tiers[ECacheTier::Regular];
            const auto l1 = tier.Hot(EHotLevel::L1);
            bool moved = false;
            for (ui64 slot = 0; slot < l1.Capacity; ++slot) {
                ui64 expected = page.CacheItem().Raw();
                if (l1.Slots[slot].compare_exchange_strong(expected, 0)) {
                    UNIT_ASSERT_VALUES_EQUAL(tier.HotStorage.Slots[PageSlot].exchange(page.CacheItem().Raw()), 0);
                    moved = true;
                    break;
                }
            }
            UNIT_ASSERT(moved);
            UNIT_ASSERT_VALUES_EQUAL(tier.Hot(EHotLevel::L0).Capacity, SharedCacheTransitionWorkBatch);
        });
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(TCacheTestAccess::SweepModes(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHotInTier(ECacheTier::Regular));
        UNIT_ASSERT(TCacheTestAccess::BeginHotResize(*fixture.Cache, AfterCut));
        UNIT_ASSERT(TCacheTestAccess::DrainHotResize(*fixture.Cache));
        for (ui32 step = 0; step < 4096 && TCacheTestAccess::SweepModes(*fixture.Cache); ++step) {
        }
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHotInTier(ECacheTier::Keep));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
    }

    Y_UNIT_TEST(PairedShadowSelectionSchedulesTierReconciliation) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(32 * bytes));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 198, 11), ECacheMode::TryKeepInMemory);
        TTestCachePageRef held;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
        TTestCachePageRequest request(MakePageLocation(0, 4096, NTable::NPage::EPage::BTreeIndex), waiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        const auto page = request.Fetch().CacheItem();
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(waiter->Ready());
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, true));
        for (ui32 step = 0; step < 4096 && fixture.Cache->RunMaintenance(); ++step) {
        }
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHotInTier(ECacheTier::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 0);
        const ui64 generation = fixture.Cache->BeginOperation().Eviction().ModeGeneration();
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, true));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->BeginOperation().Eviction().ModeGeneration(), generation);
        UNIT_ASSERT(!fixture.Cache->RunMaintenance());
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, false));
        for (ui32 step = 0; step < 4096 && fixture.Cache->RunMaintenance(); ++step) {
        }
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHotInTier(ECacheTier::Keep));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(held.size(), 4096);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
    }

    void DrainModeSweepInWorker(TTestSharedCache & cache) {
        std::thread worker([&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            for (ui32 step = 0; step < 4096; ++step) {
                if (!TCacheTestAccess::SweepModes(cache)) {
                    return;
                }
            }
            Y_ABORT("Mode inspection did not converge");
        });
        worker.join();
    }

    Y_UNIT_TEST(PairedLateOldViewPublicationRestartsWholeCurrentWalk) {
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(9, 4096, 0, 8, reserved));
        TFixture fixture(8, 8, 9, reserved.Limit);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(32 * bytes));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 198, 12), ECacheMode::TryKeepInMemory);
        for (ui32 step = 0; step < 4096 && fixture.Cache->RunMaintenance(); ++step) {
        }
        TTestCachePageRef held;
        const auto page = InsertOwnedPage(*fixture.Cache, collection, 0, held);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        auto heldItem = TCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem());
        UNIT_ASSERT(heldItem);
        held.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 0);
        auto target = fixture.ReservedCapacity;
        const auto current = fixture.Space->CurrentConfiguration();
        for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
            target.Tiers[tier] = { 2 * current.Tiers[tier].HotSlots, 2 * current.Tiers[tier].ColdSlots };
        }
        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, target, growth) == EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, target.AddressBits));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        {
            auto oldOperation = fixture.Cache->BeginOperation();
            const ui32 oldGeneration = oldOperation.SpaceOperation().View().Generation;
            UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            while (growth.Phase() == ETransitionPhase::AppendGrowth) {
                UNIT_ASSERT(TCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
            }

            struct TContext {
                TTestSharedCache& Cache;
                TCollectionCacheItem Collection;
                TPageCacheItem Page;
                bool Visited = false;
            } context{ *fixture.Cache, collection, page };

            TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                                  auto& ctx = *static_cast<TContext*>(ptr);
                                  if (at == ECacheHookPoint::AfterLastColdRefReleased && item == ctx.Page.CacheItem() &&
                                      !ctx.Visited) {
                                      ctx.Visited = true;
                                      Y_ABORT_UNLESS(
                                          ctx.Cache.SetCollectionPagesCacheMode(ctx.Collection, ECacheMode::Regular));
                                      DrainModeSweepInWorker(ctx.Cache);
                                  }
                              } };
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            const auto beforeDrop = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(beforeDrop.IsKeepCold() && beforeDrop.Refs() == 1);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 0);
            TOperationItemRef<TTestTraits> oldReference(oldOperation.SpaceOperation(), std::move(heldItem));
            oldReference.Drop();
            UNIT_ASSERT(context.Visited);
            UNIT_ASSERT_VALUES_EQUAL(oldOperation.SpaceOperation().View().Generation, oldGeneration);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
            DrainModeSweepInWorker(*fixture.Cache);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsColdInTier(ECacheTier::Regular));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 0);
        }
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, growth));
    }

    Y_UNIT_TEST(PairedEmptyFailedFetchTombstoneDoesNotProduceAResult) {
        TFixture fixture;
        const auto collection =
            CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(195, 197, 5), ECacheMode::Regular);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TTestCachePageRequest request(
            MakePageLocation(0), MakeIntrusive<TTestPageFetchWaiter>(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        const auto page = request.Fetch().CacheItem();

        struct TContext {
            TTestSharedCache& Cache;
            TPageCacheItem Page;
            bool Visited = false;
            bool Acquired = false;
        } context{ *fixture.Cache, page };

        TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint at, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (at == ECacheHookPoint::AfterTombstoneOwnerClaim &&
                                  item == ctx.Page.CacheItem() && !ctx.Visited) {
                                  ctx.Visited = true;
                                  TTestCachePageRef result;
                                  ctx.Acquired = TCacheTestAccess::AcquirePage(ctx.Cache, ctx.Page, result);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(request.Fetch().FailReady());
        UNIT_ASSERT(context.Visited && !context.Acquired);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 0);
        TTestCachePageRef result;
        UNIT_ASSERT(!TCacheTestAccess::AcquirePage(*fixture.Cache, page, result));
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, result) == ECacheResultStatus::Miss);
    }

    Y_UNIT_TEST(PairedCapReductionDrainsHeldAndPendingCredits) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 8), ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 2 * bytes));
        TTestCachePageRef held;
        const auto page = InsertOwnedPage(*fixture.Cache, collection, 0, held);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        TTestCachePageRef arriving;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, arriving);
        TTestCachePageRequest request(MakePageLocation(1), waiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 2 * bytes);
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(0));
        const auto pending = request.Fetch().CacheItem();
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(pending.Index())));
        UNIT_ASSERT(waiter->Ready());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, arriving.CacheItem().Index()).IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        held.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 0);
        arriving.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(PageFetchWaiterAcquiresReadyPageBeforeFinalizerDrops) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(81, 82);
        const auto location = MakePageLocation(17);
        TTestCachePageRef completedPage;
        TIntrusivePtr<TAcquiringPageFetchWaiter> waiter = new TAcquiringPageFetchWaiter(*fixture.Cache, completedPage);
        TTestCachePageRef hit;
        TTestPageFetch fetch;

        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None, waiter,
                        hit, fetch) == ECacheResultStatus::Inserted);
        const TPageCacheItem page = fetch.CacheItem();
        UNIT_ASSERT(fetch.MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(waiter->Ready());
        UNIT_ASSERT(completedPage);
        UNIT_ASSERT(completedPage.CacheItem() == page);
        const THandleState referenced = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(referenced.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(referenced.Frequency(), 0);

        completedPage.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
    }
    Y_UNIT_TEST(PageFetchCompletionIsInheritedByLastSubscriber) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(65, 66);
        const auto location = MakePageLocation(11);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        TTestCachePageRef hit;
        TTestPageFetch fetch;
        TIntrusivePtr<TPageFetchWaiter> first = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None, first, hit,
                        fetch) == ECacheResultStatus::Inserted);
        const TPageCacheItem page = fetch.CacheItem();

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterFetchWaiterSubscribed, page.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        ECacheResultStatus subscribeStatus = ECacheResultStatus::Miss;
        TGateThread subscriber(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            TTestCachePageRef subscriberHit;
            TTestPageFetch subscriberFetch;
            TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
            subscribeStatus = TCacheTestAccess::FindOrInsert(
                *cache, collection, location, EStickyState::None, second, subscriberHit, subscriberFetch);
        });

        gate.Slots[0].Wait();
        UNIT_ASSERT(fetch.MakeReady(MakePageData(page.Index())));
        const THandleState pending = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(pending.IsBegin());
        UNIT_ASSERT_VALUES_EQUAL(pending.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);

        gate.Slots[0].Release();
        subscriber.Join();
        UNIT_ASSERT(subscribeStatus == ECacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 2);
        const THandleState resident = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(resident.IsHot());
        UNIT_ASSERT_VALUES_EQUAL(resident.Refs(), 0);
    }
    Y_UNIT_TEST(InsertRetriesSuccessfullyCompletingFetch) {
        InsertRetriesCompletingFetch(true);
    }
    Y_UNIT_TEST(InsertRetriesFailedCompletingFetch) {
        InsertRetriesCompletingFetch(false);
    }
    void FailedStickyFetchWithdrawsBeforeCompletion(bool externalDrain) {
        TFixture fixture(5);
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(83, 84, 85));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

        const ui32 bucketMask = static_cast<ui32>(CurrentBucketCount(*fixture.Space) - 1);
        const ui32 bucket = (collectionKey.Hash() + 1) & bucketMask;
        const auto sourceKey = ReviewKeyForBucket(collection, bucketMask, bucket, 1000);
        const auto victimKey = ReviewKeyForBucket(collection, bucketMask, bucket, sourceKey.Word(1) + 1, &sourceKey);
        const auto restoredKey = ReviewKeyForBucket(collection, bucketMask, (bucket + 1) & bucketMask, 2000);
        const TPageCacheItem victim = InsertReadyPage(*fixture.Cache, victimKey);
        const ui64 hotPages = TCacheTestAccess::WithSpace(*fixture.Cache, [](TSpaceOperation& spaceOp) {
            return spaceOp.Hot(EHotLevel::L0).Capacity + spaceOp.Hot(EHotLevel::L1).Capacity;
        });
        ui64 nextOffset = restoredKey.Word(1) + 1;
        for (ui64 index = 1; index < hotPages; ++index) {
            const auto fillerKey = ReviewKeyForBucket(collection, bucketMask, (bucket + 1) & bucketMask, nextOffset);
            InsertReadyPage(*fixture.Cache, fillerKey);
            nextOffset = fillerKey.Word(1) + 1;
        }
        // The oldest zero-frequency page will be evicted when a restored sibling enters the full L1/L0 rings
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, victim.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, victim.Index()).Frequency(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TIntrusivePtr<TPageFetchWaiter> waiter = new TTestPageFetchWaiter(completed, ready, completedItem);
        TTestCachePageRef hit;
        TTestPageFetch fetch;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(sourceKey.Word(1)),
                        EStickyState::Sticky, waiter, hit, fetch) == ECacheResultStatus::Inserted);
        const TPageCacheItem source = fetch.CacheItem();
        const THandleState pending = TCacheTestAccess::HandleState(*fixture.Cache, source.Index());
        UNIT_ASSERT(pending.IsPending() && pending.IsStickyField());
        TPageCacheItem restored;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(restoredKey.Word(1)),
                        EStickyState::Sticky, restored, hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(restored, MakePageData(restored.Index())));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, restored.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::HandleNext(*fixture.Cache, source.Index()), victim.CacheItem().Raw());
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        if (externalDrain) {
            gate.Slots[1].Arm(ECacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        }

        struct TDrainHook {
            TSharedCacheGate& Gate;
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Source;
            TPageCacheItem Victim;
            TSpaceHazard* WaitingFinisher;
            bool Evicted = false;

            static void Invoke(void* context, ECacheHookPoint point, TCacheItem item) noexcept {
                auto& self = *static_cast<TDrainHook*>(context);
                if (point == ECacheHookPoint::AfterStickyPageListDetached && item == self.Collection.CacheItem()) {
                    auto& slot = self.Gate.Slots[TlsCacheHazard == self.WaitingFinisher ? 1 : 0];
                    slot.TryInvoke(point, item);
                }
                if (point == ECacheHookPoint::AfterTombstoneOwnerClaim && item == self.Victim.CacheItem()) {
                    // Fail before an early drain waits on its own Completing predecessor
                    Y_ABORT_UNLESS(TCacheTestAccess::HandleState(self.Cache, self.Source.Index()).IsPending(),
                        "Failed fetch must remain pending while a Sticky list owner can still need its predecessor");
                    self.Evicted = true;
                }
            }
        } drainHook{ gate, *fixture.Cache, collection, source, victim,
            externalDrain ? &TCacheThreadTestAccess::Hazard(*fixture.Cache, 1) : nullptr };

        TTestTraits hooks{ &drainHook, &TDrainHook::Invoke };
        TSharedCacheHookGuard hookGuard(*fixture.Cache, hooks);
        auto cache = fixture.Cache;
        bool failed = false;
        const auto failFetch = [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*cache, externalDrain ? 1 : 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            failed = fetch.FailReady();
        };
        bool modeChanged = false;
        if (externalDrain) {
            TGateThread drainer(gate, [&] {
                Y_ABORT_UNLESS(!TlsCacheHazard);
                TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    TlsCacheHazard = nullptr;
                };
                modeChanged = cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular);
            });
            gate.Slots[0].Wait();
            TGateThread finisher(gate, failFetch);
            gate.Slots[1].Wait();
            const THandleState waiting = TCacheTestAccess::HandleState(*cache, source.Index());
            UNIT_ASSERT(waiting.IsPending() && waiting.IsStickyField());
            UNIT_ASSERT_VALUES_EQUAL(waiting.Refs(), 1);
            UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 0);
            gate.Slots[1].Release();
            gate.Slots[0].Release();
            drainer.Join();
            finisher.Join();
        } else {
            TGateThread finisher(gate, failFetch);
            gate.Slots[0].Wait();
            UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, source.Index()).IsPending());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*cache, collection), 0);
            modeChanged = cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular);
            gate.Slots[0].Release();
            finisher.Join();
        }

        UNIT_ASSERT(modeChanged);
        UNIT_ASSERT(failed);
        UNIT_ASSERT(drainHook.Evicted);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, source.Index()).IsFree());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, victim.Index()).IsFree());
        const THandleState restoredState = TCacheTestAccess::HandleState(*cache, restored.Index());
        UNIT_ASSERT(restoredState.IsHot() && restoredState.IsStickyNoneField());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 0);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), source.CacheItem().Raw());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*cache, collection), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(FailedStickyFetchDrainsBeforeExclusiveCompletion) {
        FailedStickyFetchWithdrawsBeforeCompletion(false);
    }
    Y_UNIT_TEST(FailedStickyFetchWaitsForDetachedListBeforeExclusiveCompletion) {
        FailedStickyFetchWithdrawsBeforeCompletion(true);
    }
    Y_UNIT_TEST(ConcurrentPageFetchFindOrInsertCoalescesWaiters) {
        constexpr ui32 ThreadCount = 8;
        TFixture fixture(6, ThreadCount + 1);
        const TCollectionCacheItem collection = MakeCollectionCacheItem(67, 68);
        const auto location = MakePageLocation(13);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        std::atomic<bool> start = false;
        std::array<ECacheResultStatus, ThreadCount> statuses;
        std::array<TTestPageFetch, ThreadCount> fetches;
        TVector<std::thread> threads;

        for (ui32 thread = 0; thread < ThreadCount; ++thread) {
            threads.emplace_back([&, thread] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, thread);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                TTestCachePageRef hit;
                TIntrusivePtr<TPageFetchWaiter> waiter = new TTestPageFetchWaiter(completed, ready, completedItem);
                statuses[thread] = TCacheTestAccess::FindOrInsert(
                    *fixture.Cache, collection, location, EStickyState::None, waiter, hit, fetches[thread]);
            });
        }
        start.store(true, std::memory_order_release);
        for (std::thread& thread : threads) {
            thread.join();
        }

        ui32 inserted = 0;
        ui32 winner = 0;
        for (ui32 thread = 0; thread < ThreadCount; ++thread) {
            if (statuses[thread] == ECacheResultStatus::Inserted) {
                ++inserted;
                winner = thread;
                UNIT_ASSERT(fetches[thread]);
            } else {
                UNIT_ASSERT(statuses[thread] == ECacheResultStatus::Pending);
                UNIT_ASSERT(!fetches[thread]);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(inserted, 1);
        const TPageCacheItem page = fetches[winner].CacheItem();
        UNIT_ASSERT(fetches[winner].MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), ThreadCount);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), ThreadCount);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), page.CacheItem().Raw());
    }
    Y_UNIT_TEST(PageFetchAbandonmentFailsAllWaiters) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(63, 64);
        const auto location = MakePageLocation(9);
        const ui64 pageBytes = location.Size + NActors::TSharedData::OverheadSize;
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        TPageCacheItem page;
        {
            TTestCachePageRef hit;
            TTestPageFetch fetch;
            TIntrusivePtr<TPageFetchWaiter> first = new TTestPageFetchWaiter(completed, ready, completedItem);
            UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None, first,
                            hit, fetch) == ECacheResultStatus::Inserted);
            page = fetch.CacheItem();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);

            TTestPageFetch duplicateFetch;
            TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
            UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None, second,
                            hit, duplicateFetch) == ECacheResultStatus::Pending);
        }

        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 0);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), page.CacheItem().Raw());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
        TTestCachePageRef missing;
        UNIT_ASSERT(
            fixture.Cache->Find(collection, static_cast<ui64>(location.Offset), missing) == ECacheResultStatus::Miss);
    }
    Y_UNIT_TEST(PageFetchMoveAssignmentReleasesPreviousFetch) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(64, 65);
        const auto firstLocation = MakePageLocation(10);
        const auto secondLocation = MakePageLocation(11);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TTestCachePageRef hit;
        TTestPageFetch first;
        TTestPageFetch second;
        TIntrusivePtr<TPageFetchWaiter> firstWaiter = new TTestPageFetchWaiter(completed, ready, completedItem);
        TIntrusivePtr<TPageFetchWaiter> secondWaiter = new TTestPageFetchWaiter(completed, ready, completedItem);

        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, firstLocation, EStickyState::None,
                        firstWaiter, hit, first) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, secondLocation, EStickyState::None,
                        secondWaiter, hit, second) == ECacheResultStatus::Inserted);
        const TPageCacheItem secondPage = second.CacheItem();

        first = std::move(second);
        UNIT_ASSERT(first);
        UNIT_ASSERT(!second);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 0);
        UNIT_ASSERT(first.MakeReady(MakePageData(secondPage.Index())));
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(PublicCollectionFindOrInsertAllocatesOnlyAfterMiss) {
        TFixture fixture;
        const TLogoBlobID id(4, 5, 7);
        const ui64 freeBefore = TCacheTestAccess::FreeCount(*fixture.Cache);
        const TCollectionLocation location{ .Id = id, .BackingSize = 113 };

        TCollectionCacheItem inserted;
        TTestCacheCollectionRef collection;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, inserted, collection) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!collection);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), location.AccountedBytes());
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, inserted, MakeCollection(id, location.BackingSize)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), location.AccountedBytes());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), inserted.Index());

        TCollectionCacheItem duplicate;
        TTestCacheCollectionRef hitCollection;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, duplicate,
                        hitCollection) == ECacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hitCollection.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);
        hitCollection.Drop();
    }
    Y_UNIT_TEST(FindCollectionMissAndPendingDoNotAllocate) {
        TFixture fixture;
        const TLogoBlobID id(207, 208, 1);
        TTestCacheCollectionRef found;
        const ui64 freeBefore = AvailableFreeHandles(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Miss);
        UNIT_ASSERT(!found);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freeBefore);

        TCollectionCacheItem item;
        UNIT_ASSERT(fixture.Cache->FindOrInsert({ .Id = id }, item, found) == ECacheResultStatus::Inserted);
        const ui64 freePending = AvailableFreeHandles(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Pending);
        UNIT_ASSERT(!found);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freePending);
        UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Hit);
        UNIT_ASSERT(found.CacheItem() == item);
        UNIT_ASSERT(
            fixture.Cache->FindCollection(TLogoBlobID(207, 208, 2), found) == ECacheCollectionLookupStatus::Miss);
        UNIT_ASSERT(!found);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freePending);
    }

    Y_UNIT_TEST(FindCollectionAttachesUnregisteredAndDetachedReady) {
        for (bool unregistered : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(207, 208, 3);
            TCollectionCacheItem item;
            TTestCacheCollectionRef held;
            UNIT_ASSERT(TCacheTestAccess::FindOrInsertUnregistered(*fixture.Cache, { .Id = id }, item, held) ==
                        ECacheResultStatus::Inserted);
            auto value = MakeCollection(id);
            auto* canonical = value.Get();
            value->PageCollection()->SetSkipBTreeIndexV1Shadow(true);
            UNIT_ASSERT(TCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, item, std::move(value)));
            UNIT_ASSERT(fixture.Cache->Find(id, held) == ECacheResultStatus::Hit);
            if (!unregistered) {
                TTestCacheCollectionRef attached;
                UNIT_ASSERT(fixture.Cache->FindCollection(id, attached) == ECacheCollectionLookupStatus::Hit);
                UNIT_ASSERT(fixture.Cache->DetachCollection(item));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                TCacheTestAccess::CollectionReferences(*fixture.Cache, item), unregistered ? 1 : 0);
            const ui64 freeBefore = AvailableFreeHandles(*fixture.Cache);
            TTestCacheCollectionRef found;
            UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Hit);
            UNIT_ASSERT(found.Get() == canonical && found.CacheItem() == item);
            UNIT_ASSERT(found->PageCollection()->SkipBTreeIndexV1Shadow());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, item), 1);
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freeBefore);
            ui32 attached = 0;
            fixture.Cache->ForEachCollection([&](TCollection& collection) {
                UNIT_ASSERT(&collection == canonical);
                ++attached;
            });
            UNIT_ASSERT_VALUES_EQUAL(attached, 1);
            TTestCacheCollectionRef joined;
            UNIT_ASSERT(fixture.Cache->FindCollection(id, joined) == ECacheCollectionLookupStatus::Hit);
            UNIT_ASSERT(joined.Get() == canonical);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, item), 1);
        }
    }

    Y_UNIT_TEST(FindCollectionMovesRegistryAndPreservesStickyPages) {
        TFixture fixture;
        TCollectionRegistry destination;
        const TLogoBlobID id(207, 208, 4);
        TCollectionCacheItem item;
        TTestCacheCollectionRef found;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = id }, item, found) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, item, MakeCollection(id)));
        const auto page = AllocatePage(
            *fixture.Cache, TCacheKey::Page(item, 1), 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(page && fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        const ui64 references = TCacheTestAccess::CollectionReferences(*fixture.Cache, item);
        UNIT_ASSERT(TCacheTestAccess::FindCollection(*fixture.Cache, destination, id, found) ==
                    ECacheCollectionLookupStatus::Hit);
        UNIT_ASSERT(found.CacheItem() == item);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(destination), item.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, item), references);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, item), page.Index());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, destination, item));
    }

    void CheckFindCollectionTransitionWait(bool attaching, bool armBeforeCompletion) {
        TFixture fixture;
        const TLogoBlobID id(207, 208, 5);
        TCollectionCacheItem item;
        TTestCacheCollectionRef held;
        UNIT_ASSERT(fixture.Cache->FindOrInsert({ .Id = id }, item, held) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->Find(id, held) == ECacheResultStatus::Hit);
        if (attaching) {
            UNIT_ASSERT(fixture.Cache->DetachCollection(item));
        } else {
            const auto page = AllocatePage(*fixture.Cache, TCacheKey::Page(item, 1), 4096,
                NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
            UNIT_ASSERT(page && fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        }
        TSharedCacheGate gate;
        gate.Slots[0].Arm(attaching ? ECacheHookPoint::BeforeCollectionAttachPublished
                                    : ECacheHookPoint::AfterStickyPageListDetached, item.CacheItem());

        struct TObservation {
            TSharedCacheGate& Gate;
            std::atomic<ui32> Notifications = 0;
        } observation{ gate };

        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem cacheItem) noexcept {
                              auto& observation = *static_cast<TObservation*>(context);
                              observation.Gate.Hooks.Invoke(point, cacheItem);
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  observation.Notifications.fetch_add(1, std::memory_order_relaxed);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        bool completed = false;
        TTestCacheCollectionRef attached;
        TGateThread worker(gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            completed = attaching ? fixture.Cache->FindCollection(id, attached) == ECacheCollectionLookupStatus::Hit
                                  : fixture.Cache->DetachCollection(item);
        });
        gate.Slots[0].Wait();
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        TTestCacheCollectionRef found;
        UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Pending);
        UNIT_ASSERT(!found);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        if (armBeforeCompletion) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), 0);
        gate.Slots[0].Release();
        worker.Join();
        UNIT_ASSERT(completed);
        UNIT_ASSERT(fixture.Cache->AdmissionGeneration() > generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), armBeforeCompletion ? 1 : 0);
        if (!armBeforeCompletion) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), 1);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Hit);
        UNIT_ASSERT(found.Get() == held.Get());
    }

    Y_UNIT_TEST(FindCollectionWaitsDuringAttachment) {
        CheckFindCollectionTransitionWait(true, true);
        CheckFindCollectionTransitionWait(true, false);
    }

    Y_UNIT_TEST(FindCollectionWaitsDuringDetachment) {
        CheckFindCollectionTransitionWait(false, true);
        CheckFindCollectionTransitionWait(false, false);
    }

    Y_UNIT_TEST(FindCollectionFollowsConcurrentReplacement) {
        TFixture fixture;
        const TLogoBlobID id(207, 208, 6);
        TCollectionCacheItem original;
        TTestCacheCollectionRef found;
        UNIT_ASSERT(fixture.Cache->FindOrInsert({ .Id = id }, original, found) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(original, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->DetachCollection(original));

        struct TObservation {
            TTestSharedCache* Cache;
            TLogoBlobID Id;
            TCollectionCacheItem Original;
            TCollectionCacheItem Replacement = {};
            bool Replaced = false;
        } observation{ fixture.Cache.Get(), id, original };

        TTestTraits hooks{ &observation,
            [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                auto& observation = *static_cast<TObservation*>(context);
                if (point == ECacheHookPoint::BeforeCollectionRefAcquired && !observation.Replaced) {
                    observation.Replaced = true;
                    Y_ABORT_UNLESS(observation.Cache->EraseCollection(observation.Original));
                    TTestCacheCollectionRef duplicate;
                    Y_ABORT_UNLESS(observation.Cache->FindOrInsert({ .Id = observation.Id }, observation.Replacement,
                                       duplicate) == ECacheResultStatus::Inserted);
                    Y_ABORT_UNLESS(
                        observation.Cache->MakeReady(observation.Replacement, MakeCollection(observation.Id)));
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Hit);
        UNIT_ASSERT(observation.Replaced && observation.Replacement != original);
        UNIT_ASSERT(found.CacheItem() == observation.Replacement);
    }

    Y_UNIT_TEST(FindCollectionReturnsMissAfterConcurrentErase) {
        TFixture fixture;
        const TLogoBlobID id(207, 208, 8);
        TCollectionCacheItem item;
        TTestCacheCollectionRef found;
        UNIT_ASSERT(fixture.Cache->FindOrInsert({ .Id = id }, item, found) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->DetachCollection(item));

        struct TObservation {
            TTestSharedCache* Cache;
            TCollectionCacheItem Item;
            bool Erased = false;
        } observation{ fixture.Cache.Get(), item };

        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              auto& observation = *static_cast<TObservation*>(context);
                              if (point == ECacheHookPoint::BeforeCollectionRefAcquired && !observation.Erased) {
                                  observation.Erased = true;
                                  Y_ABORT_UNLESS(observation.Cache->EraseCollection(observation.Item));
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Miss);
        UNIT_ASSERT(observation.Erased && !found);
    }

    Y_UNIT_TEST(CollectionReadyHintFollowsOwnershipTransfer) {
        TFixture fixture;
        const TLogoBlobID id(207, 208, 7);
        TCollectionCacheItem item;
        TTestCacheCollectionRef found;
        UNIT_ASSERT(fixture.Cache->FindOrInsert({ .Id = id }, item, found) == ECacheResultStatus::Inserted);
        auto value = MakeCollection(id);

        struct TObservation {
            TTestSharedCache* Cache;
            THolder<TCollection>* ConstructorOwner;
            TLogoBlobID Id;
            TCollectionCacheItem Item;
            ui32 Notifications = 0;
            bool Released = false;
            bool Ready = false;
        } observation{ fixture.Cache.Get(), &value, id, item };

        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem item) noexcept {
                              if (point == ECacheHookPoint::CollectionReady) {
                                  auto& observation = *static_cast<TObservation*>(context);
                                  ++observation.Notifications;
                                  observation.Released = !*observation.ConstructorOwner;
                                  TTestCacheCollectionRef found;
                                  observation.Ready = item == observation.Item.CacheItem() &&
                                                      observation.Cache->FindCollection(observation.Id, found) ==
                                                          ECacheCollectionLookupStatus::Hit &&
                                                      found.CacheItem() == observation.Item;
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(fixture.Cache->MakeReady(item, std::move(value)));
        UNIT_ASSERT(observation.Released && observation.Ready);
        UNIT_ASSERT(fixture.Cache->FindCollection(id, found) == ECacheCollectionLookupStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
    }

    Y_UNIT_TEST(UnattachedCollectionAttachesWithoutTemporaryRegistry) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(5, 6, 8) };

        TCollectionCacheItem inserted;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsertUnregistered(*fixture.Cache, location, inserted, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, inserted, MakeCollection(location.Id)));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, inserted.Index()), 0);

        TCollectionCacheItem duplicate;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, duplicate, hit) ==
                    ECacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hit.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), inserted.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, inserted.Index()), 0);
        hit.Drop();

        UNIT_ASSERT(TCacheTestAccess::UnlinkCollectionRegistry(*fixture.Cache, fixture.Registry, inserted));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        const THandleState retained = TCacheTestAccess::HandleState(*fixture.Cache, inserted.Index());
        UNIT_ASSERT(retained.IsSticky());
        UNIT_ASSERT(retained.IsStickyField());
        UNIT_ASSERT(fixture.Cache->EraseCollection(inserted));
    }
    Y_UNIT_TEST(UnattachedCollectionCanBeDeleted) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(6, 7, 9) };

        TCollectionCacheItem inserted;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsertUnregistered(*fixture.Cache, location, inserted, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, inserted, MakeCollection(location.Id)));
        UNIT_ASSERT(fixture.Cache->EraseCollection(inserted));

        UNIT_ASSERT(fixture.Cache->Find(location.Id, hit) == ECacheResultStatus::Miss);
    }
    Y_UNIT_TEST(CollectionRegistryDetachesAndReattaches) {
        TFixture fixture;
        const TLogoBlobID firstId(4, 6, 8);
        const TLogoBlobID secondId(5, 7, 9);
        const TCollectionLocation firstLocation{ .Id = firstId };
        const TCollectionLocation secondLocation{ .Id = secondId };

        TCollectionCacheItem first;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, firstLocation, first, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, first, MakeCollection(firstId)));

        TCollectionCacheItem second;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, secondLocation, second, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, second, MakeCollection(secondId)));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), second.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);

        /* a detached, unreferenced record goes to the eviction control, so it can be re-attached
         * while it is still there */
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, first));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), second.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), 0);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, first.Index()).IsHot());
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, first));
        const auto staleColdEntry = MakeColdItem(*fixture.Cache, first.Index());

        TCollectionCacheItem duplicate;
        TTestCacheCollectionRef reattached;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, firstLocation, duplicate,
                        reattached) == ECacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(reattached.CacheItem() == first);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, first.Index()).IsSticky());
        UNIT_ASSERT(!TCacheTestAccess::EraseCold(*fixture.Cache, staleColdEntry));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), second.Index());

        reattached.Drop();
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, first));
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, second));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), 0);
    }
    Y_UNIT_TEST(CollectionReattachRestoresOwnReferenceWithResidentPage) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 17, 19) };
        TCollectionCacheItem collection;
        TTestCacheCollectionRef collectionHit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection,
                        collectionHit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(
            TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(location.Id)));

        TPageCacheItem page;
        TTestCachePageRef pageHit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(1), EStickyState::None, page,
                        pageHit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        TCollectionCacheItem duplicate;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, duplicate,
                        collectionHit) == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 2);
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 0);
    }
    Y_UNIT_TEST(CollectionAttachRetriesNativeReferenceChanges) {
        for (EHandleState sourceState : { EHandleState::Sticky, EHandleState::Hot, EHandleState::Cold }) {
            TFixture fixture;
            const TCollectionLocation location{ .Id = TLogoBlobID(15, 17, 20) };
            TCollectionCacheItem collection;
            TTestCacheCollectionRef collectionHit;
            UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection,
                            collectionHit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(collection);
            UNIT_ASSERT(
                TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(location.Id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            TPageCacheItem page;
            if (sourceState == EHandleState::Sticky) {
                page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
            }
            UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
            if (sourceState == EHandleState::Cold) {
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, collection));
            }
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).State() == sourceState);
            const ui64 pageOwners = page ? 1 : 0;
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners);

            TSharedCacheGate gate;
            gate.Slots[0].Arm(ECacheHookPoint::BeforeCollectionAttachClaim, collection.CacheItem());
            gate.Slots[1].Arm(ECacheHookPoint::BeforeCollectionAttachPublished, collection.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            TTestCacheCollectionRef attached;
            ECacheResultStatus status = ECacheResultStatus::Miss;
            TGateThread attacher(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                TCollectionCacheItem duplicate;
                status = TCacheTestAccess::FindOrInsert(*cache, fixture.Registry, location, duplicate, attached);
            });
            gate.Slots[0].Wait();
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), 0);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, collection.Index()).IsStickyNoneField());
            TTestCacheCollectionRef claimReader;
            UNIT_ASSERT(cache->Find(location.Id, claimReader) == ECacheResultStatus::Hit);
            gate.Slots[0].Release();

            gate.Slots[1].Wait();
            UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, collection.Index()).IsTransitionField());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), collection.Index());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*cache, collection.Index()), 0);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*cache, collection), pageOwners + 1);
            TTestCacheCollectionRef publishReader;
            UNIT_ASSERT(cache->Find(location.Id, publishReader) == ECacheResultStatus::Hit);
            gate.Slots[1].Release();
            attacher.Join();

            UNIT_ASSERT(status == ECacheResultStatus::Hit);
            UNIT_ASSERT(attached.CacheItem() == collection);
            const THandleState ready = TCacheTestAccess::HandleState(*cache, collection.Index());
            UNIT_ASSERT(ready.IsSticky() && ready.IsStickyField());
            UNIT_ASSERT_VALUES_EQUAL(ready.Refs(), 3);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), collection.Index());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*cache, collection.Index()), 0);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*cache, collection), pageOwners + 1);
            claimReader.Drop();
            publishReader.Drop();
            attached.Drop();
            UNIT_ASSERT(TCacheTestAccess::DetachCollection(*cache, fixture.Registry, collection));
            if (page) {
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*cache, page));
                UNIT_ASSERT(TCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, page.Index())));
            }
            UNIT_ASSERT(cache->EraseCollection(collection));
            UNIT_ASSERT_VALUES_EQUAL(cache->Collections(), 0);
        }
    }
    Y_UNIT_TEST(KeepAccountingFollowsShadowSelectionAcrossReclaim) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 17, 19) };
        TIntrusiveConstPtr<NPageCollection::IPageCollection> source = new TTestPageCollection(location.Id, 0);
        TCollectionCacheItem collection;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection, hit) ==
                    ECacheResultStatus::Inserted);
        auto value = MakeHolder<TCollection>(source, collection);
        auto* owner = value.Get();
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, std::move(value)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TPageCacheItem shadow;
        TTestCachePageRef pageHit;
        UNIT_ASSERT(
            FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(1, 4096, NTable::NPage::EPage::BTreeIndex),
                EStickyState::None, shadow, pageHit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(shadow, MakePageData(shadow.Index())));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepReadyPageBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepResidentPageBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepReadyPageBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, true));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepReadyPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepReadyPageBytes(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, false));
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepResidentPageBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepReadyPageBytes(), 0);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, shadow));
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepReadyPageBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepReadyPageBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, true));
        UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, shadow.Index())));
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, false));
        UNIT_ASSERT_VALUES_EQUAL(owner->ResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepReadyPageBytes(), 0);
    }

    Y_UNIT_TEST(KeepAggregatesIncludeModeChangeDuringPagePublication) {
        for (bool initiallyKeep : { false, true }) {
            TFixture fixture;
            UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * (4096 + NActors::TSharedData::OverheadSize)));
            const TLogoBlobID id(16, 18, 20);
            const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(collection);
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                collection, initiallyKeep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular));
            TTestCachePageRef hit;
            TTestPageFetch fetch;
            UNIT_ASSERT(
                TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1), EStickyState::None,
                    MakeIntrusive<TPageFetchWaiter>(), hit, fetch) == ECacheResultStatus::Inserted);
            const auto page = fetch.CacheItem();
            TSharedCacheGate gate;
            gate.Slots[0].Arm(ECacheHookPoint::BeforeKeepBytesPublished, collection.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            bool ready = false;
            TGateThread publisher(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                ready = fetch.MakeReady(MakePageData(page.Index()));
            });
            gate.Slots[0].Wait();
            // The mode update must complete while another publisher is stopped at the hook.
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                collection, initiallyKeep ? ECacheMode::Regular : ECacheMode::TryKeepInMemory));
            gate.Slots[0].Release();
            publisher.Join();
            UNIT_ASSERT(ready);
            const ui64 bytes = fixture.Cache->PayloadUsage();
            UNIT_ASSERT_GT(bytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), initiallyKeep ? 0 : bytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepReadyPageBytes(), initiallyKeep ? 0 : bytes);
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepReadyPageBytes(), 0);
        }
    }

    Y_UNIT_TEST(CollectionLogicalDeleteDefersPhysicalDestruction) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 16, 17) };

        TCollectionCacheItem inserted;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, inserted, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(
            TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, inserted, MakeCollection(location.Id)));

        TTestCacheCollectionRef held;
        UNIT_ASSERT(fixture.Cache->Find(location.Id, held) == ECacheResultStatus::Hit);
        const TCollection* value = &held.GetCollection();
        UNIT_ASSERT(TCacheTestAccess::EraseCollection(*fixture.Cache, fixture.Registry, inserted));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), 0);

        TTestCacheCollectionRef missing;
        UNIT_ASSERT(fixture.Cache->Find(location.Id, missing) == ECacheResultStatus::Miss);
        const THandleState tombstone = TCacheTestAccess::HandleState(*fixture.Cache, inserted.Index());
        UNIT_ASSERT(tombstone.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(tombstone.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(&held.GetCollection(), value);

        auto acquired = held.Acquire();
        UNIT_ASSERT(acquired.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, inserted.Index()).Refs(), 2);
        held.Drop();
        UNIT_ASSERT_VALUES_EQUAL(&acquired.GetCollection(), value);
        UNIT_ASSERT_VALUES_EQUAL(acquired->Id(), location.Id);
        acquired.Drop();
        const THandleState free = TCacheTestAccess::HandleState(*fixture.Cache, inserted.Index());
        UNIT_ASSERT(free.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(free.Version(), AdvanceItemVersion(inserted.Version()));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionValue(*fixture.Cache, inserted.Index()), nullptr);
    }
    Y_UNIT_TEST(CollectionOwnerDropPinsMetadataDuringReclaim) {
        for (bool lastOwner : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(15, 16, 18);
            TCollectionCacheItem collection;
            TTestCacheCollectionRef collectionHit;
            UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = id }, collection,
                            collectionHit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(collection);
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

            const ui32 pageCount = lastOwner ? 1 : 2;
            TVector<TPageCacheItem> pages;
            for (ui32 offset = 1; offset <= pageCount; ++offset) {
                pages.push_back(InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset)));
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, pages.back()));
            }
            UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageCount);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).Refs(), 0);
            const TCollection* metadata = TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());

            TSharedCacheGate gate;
            gate.Slots[0].Arm(lastOwner
                                  ? ECacheHookPoint::BeforeCollectionHotPublished
                                  : ECacheHookPoint::AfterCollectionOwnerReferenceDropped, collection.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            bool erased = false;
            TGateThread releaser(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                erased = TCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, pages[0].Index()));
            });
            gate.Slots[0].Wait();

            if (lastOwner) {
                UNIT_ASSERT(cache->EraseCollection(collection));
                // Final release already ended ownership; the delayed weak Hot publication must not pin metadata
                UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, collection.Index()).IsFree());
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionValue(*cache, collection.Index()), nullptr);
                UNIT_ASSERT_VALUES_EQUAL(cache->Collections(), 0);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*cache, collection), 1);
                UNIT_ASSERT(TCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, pages[1].Index())));
                UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, collection.Index()).IsSticky());
                UNIT_ASSERT(cache->EraseCollection(collection));

                const THandleState retired = TCacheTestAccess::HandleState(*cache, collection.Index());
                UNIT_ASSERT(retired.IsTombstone());
                UNIT_ASSERT_VALUES_EQUAL(retired.Refs(), 1);
                UNIT_ASSERT_VALUES_EQUAL(cache->Collections(), 1);
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionValue(*cache, collection.Index()), metadata);
                UNIT_ASSERT_VALUES_EQUAL(metadata->Id(), id);
            }
            TTestCacheCollectionRef missing;
            UNIT_ASSERT(cache->Find(id, missing) == ECacheResultStatus::Miss);

            gate.Slots[0].Release();
            releaser.Join();
            UNIT_ASSERT(erased);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, collection.Index()).IsFree());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionValue(*cache, collection.Index()), nullptr);
            UNIT_ASSERT_VALUES_EQUAL(cache->Collections(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->ResidentBytes(), 0);
        }
    }
    Y_UNIT_TEST(CollectionLogicalDeleteUnstickysPagesAndRetainsRecord) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 16, 17) };
        TCollectionCacheItem collection;
        TTestCacheCollectionRef collectionHit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection,
                        collectionHit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(
            TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(location.Id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));

        std::array<TPageCacheItem, 2> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestCachePageRef pageHit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(index + 1), EStickyState::Sticky,
                            pages[index], pageHit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index())));
        }
        UNIT_ASSERT_VALUES_UNEQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        UNIT_ASSERT(!fixture.Cache->EraseCollection(collection));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsSticky());
        for (ui32 index = 0; index < pages.size(); ++index) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, pages[index].Index());
            UNIT_ASSERT(state.IsHot());
            UNIT_ASSERT(state.IsStickyNoneField());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, pages[index].Index()), 0);
            TTestCachePageRef found;
            UNIT_ASSERT(fixture.Cache->Find(collection, index + 1, found) == ECacheResultStatus::Hit);
        }

        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 2);
        for (ui32 index = 0; index < pages.size(); ++index) {
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
            UNIT_ASSERT(
                TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, pages[index].Index())));
            if (index + 1 < pages.size()) {
                /* the record is still referenced by the remaining page items */
                UNIT_ASSERT_VALUES_EQUAL(
                    TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pages.size() - index - 1);
            }
        }

        /* the last page item's release handed the unreferenced record to the eviction control,
         * which reclaims it like any other item */
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsHot());
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, collection));
        UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, collection.Index())));
        TTestCacheCollectionRef missing;
        UNIT_ASSERT(fixture.Cache->Find(location.Id, missing) == ECacheResultStatus::Miss);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsFree());
    }
    Y_UNIT_TEST(CollectionRegistryMovePreservesStickyPages) {
        TFixture fixture;
        TCollectionRegistry destination;
        const TCollectionLocation location{ .Id = TLogoBlobID(18, 19, 20) };

        TCollectionCacheItem collection;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(
            TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(location.Id)));

        const TPageCacheItem page = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(page);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        TCollectionCacheItem duplicate;
        TTestCacheCollectionRef moved;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, destination, location, duplicate, moved) ==
                    ECacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(moved.CacheItem() == collection);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionListHead(destination), collection.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), page.Index());
        const THandleState pageState = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(pageState.IsSticky());
        UNIT_ASSERT(pageState.IsStickyField());
        moved.Drop();
    }
    Y_UNIT_TEST(ConcurrentCollectionReadyPreservesRegistryList) {
        TFixture fixture;
        const std::array<TCollectionLocation, 2> locations{
            TCollectionLocation{ .Id = TLogoBlobID(8, 10, 12) },
            TCollectionLocation{ .Id = TLogoBlobID(9, 11, 13) },
        };
        std::array<THolder<TCollection>, 2> values;
        std::array<TCollectionCacheItem, 2> collections;
        TTestCacheCollectionRef hit;
        for (ui32 index = 0; index < collections.size(); ++index) {
            UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, locations[index],
                            collections[index], hit) == ECacheResultStatus::Inserted);
            values[index] = MakeCollection(locations[index].Id);
        }

        std::atomic<bool> start = false;
        std::array<bool, 2> ready{};
        std::array<std::thread, 2> workers;
        for (ui32 index = 0; index < workers.size(); ++index) {
            workers[index] = std::thread([&, index] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, index);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                ready[index] = TCacheTestAccess::MakeReady(
                    *fixture.Cache, fixture.Registry, collections[index], std::move(values[index]));
            });
        }
        start.store(true, std::memory_order_release);
        for (std::thread& worker : workers) {
            worker.join();
        }
        UNIT_ASSERT(ready[0] && ready[1]);

        const ui32 head = TCacheTestAccess::CollectionListHead(fixture.Registry);
        UNIT_ASSERT(head == collections[0].Index() || head == collections[1].Index());
        const ui32 tail = TCacheTestAccess::NextInOwner(*fixture.Cache, head);
        UNIT_ASSERT(tail == collections[0].Index() || tail == collections[1].Index());
        UNIT_ASSERT_VALUES_UNEQUAL(head, tail);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, tail), 0);
    }
    Y_UNIT_TEST(PublicFindOrInsertHitNeedsNoAdmissionHeadroom) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(45, 46);
        const TCacheKey key = TCacheKey::Page(collection, 0);
        const TPageCacheItem inserted = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->OverallUsage()));

        TPageCacheItem duplicate;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(key.Word(1)), EStickyState::None,
                        duplicate, hit) == ECacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hit.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(PublicFindOrInsertHitDoesNotFillEmptyWorkerSpare) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(47, 48);
        const TCacheKey key = TCacheKey::Page(collection, 0);
        const TPageCacheItem inserted = InsertReadyPage(*fixture.Cache, key);
        const ui64 freeBefore = TCacheTestAccess::FreeCount(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::SpareItem(*fixture.Cache, 0), 0);

        ECacheResultStatus status = ECacheResultStatus::Miss;
        TPageCacheItem duplicate;
        TTestCachePageRef hit;
        std::thread worker([&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            status = FindOrInsertPage(
                *fixture.Cache, collection, MakePageLocation(key.Word(1)), EStickyState::None, duplicate, hit);
            UNIT_ASSERT(hit.CacheItem() == inserted);
        });
        worker.join();

        UNIT_ASSERT(status == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::SpareItem(*fixture.Cache, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::FreeCount(*fixture.Cache), freeBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(InsertPositionRetriesAfterBucketRouteChange) {
        TFixture fixture(6, 8, 7);
        const TCollectionCacheItem collection = MakeCollectionCacheItem(49, 50);
        const ui32 splitBit = static_cast<ui32>(SharedCacheBucketCount(5));
        const TCacheKey key = [&] {
            for (ui64 offset = 1000; offset < 1000 + (1 << 20); ++offset) {
                const TCacheKey candidate = TCacheKey::Page(collection, offset);
                if (candidate.Hash() & splitBit) {
                    return candidate;
                }
            }
            Y_UNREACHABLE();
        }();

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterInsertPositionFound);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        ECacheResultStatus status = ECacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestCachePageRef hit;
        TGateThread writer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            status = FindOrInsertPage(
                *cache, PageCollection(key), MakePageLocation(key.Word(1)), EStickyState::None, inserted, hit);
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 5));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT_VALUES_EQUAL(CurrentBucketCount(*fixture.Space), SharedCacheBucketCount(5));

        gate.Slots[0].Release();
        writer.Join();
        UNIT_ASSERT(status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!hit);
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        AssertPageHit(*fixture.Cache, key, inserted);
    }
    Y_UNIT_TEST(IncrementalBucketGrowthAndShrink) {
        TFixture fixture(6, 8, 7);
        const auto keys = MakeResizeKeys(*fixture.Space, MakeCollectionCacheItem(7, 11), 500);

        const auto low = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(low);
        auto lowInsert = FindOrInsertPage(*fixture.Cache, keys[0], low);
        UNIT_ASSERT(lowInsert.Status == ECacheResultStatus::Inserted);
        lowInsert.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->MakeReady(low, MakePageData(low.Index())));

        const auto high = AllocatePage(*fixture.Cache, keys[1], 4096);
        UNIT_ASSERT(high);
        auto highInsert = FindOrInsertPage(*fixture.Cache, keys[1], high);
        UNIT_ASSERT(highInsert.Status == ECacheResultStatus::Inserted);
        highInsert.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->MakeReady(high, MakePageData(high.Index())));

        const auto assertHit = [&](const TCacheKey& key) {
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
            found.Ref.Drop();
        };

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        auto resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
        UNIT_ASSERT(resize.Direction() == EBucketResize::Growing);
        UNIT_ASSERT(resize.Phase() == EBucketResizePhase::Prepared);
        UNIT_ASSERT_VALUES_EQUAL(resize.Cursor(), 0);
        do {
            resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
            UNIT_ASSERT_C(resize.Phase() == EBucketResizePhase::Prepared,
                TStringBuilder() << "growth phase=" << static_cast<ui32>(resize.Phase())
                                 << " cursor=" << resize.Cursor());
            const bool firstPair = resize.Cursor() == 0;
            UNIT_ASSERT_C(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache),
                TStringBuilder() << "growth cursor=" << resize.Cursor());
            if (firstPair) {
                assertHit(keys[0]);
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
            if (firstPair) {
                assertHit(keys[0]);
                assertHit(keys[1]);
                const auto redirected = AllocatePage(*fixture.Cache, keys[2], 4096);
                UNIT_ASSERT(redirected);
                auto redirectedInsert = FindOrInsertPage(*fixture.Cache, keys[2], redirected);
                UNIT_ASSERT(redirectedInsert.Status == ECacheResultStatus::Inserted);
                redirectedInsert.Ref.Drop();
                UNIT_ASSERT(fixture.Cache->MakeReady(redirected, MakePageData(redirected.Index())));
                assertHit(keys[2]);
            }
            UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));
            if (firstPair) {
                assertHit(keys[0]);
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
            resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
        } while (resize.Cursor() < resize.PairCount());
        UNIT_ASSERT(TCacheTestAccess::FinishBucketResize(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(CurrentBucketCount(*fixture.Space), 128);
        UNIT_ASSERT(!TCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing());

        assertHit(keys[0]);
        assertHit(keys[1]);
        assertHit(keys[2]);

        UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, growth));

        TTransition shrink;
        UNIT_ASSERT(
            TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
        UNIT_ASSERT(resize.Direction() == EBucketResize::Shrinking);
        do {
            resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
            UNIT_ASSERT_C(resize.Phase() == EBucketResizePhase::Prepared,
                TStringBuilder() << "shrink phase=" << static_cast<ui32>(resize.Phase())
                                 << " cursor=" << resize.Cursor());
            const bool firstPair = resize.Cursor() == 0;
            UNIT_ASSERT_C(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache),
                TStringBuilder() << "shrink cursor=" << resize.Cursor());
            if (firstPair) {
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
            if (firstPair) {
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));
            UNIT_ASSERT(TCacheTestAccess::CloseBucketSplit(*fixture.Cache));
            if (firstPair) {
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
            resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
        } while (resize.Cursor() < resize.PairCount());
        UNIT_ASSERT(TCacheTestAccess::FinishBucketResize(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(CurrentBucketCount(*fixture.Space), 64);

        assertHit(keys[0]);
        assertHit(keys[1]);
        assertHit(keys[2]);

        UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, shrink));
    }
    Y_UNIT_TEST(HotPolicyUsesProtectedViewAcrossCapacityCommit) {
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        for (const bool growing : { true, false }) {
            TFixture fixture(6, 8, 7, reserved.Limit);
            if (!growing) {
                TTransition growth;
                UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                            EPrepareResult::Prepared);
                UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
                UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
                UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
                CompleteBucketResize(*fixture.Cache, *fixture.Space);
                CommitTransition(*fixture.Cache, growth);
            }
            UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(fixture.ReservedCapacity.Limit));
            const auto& target = growing ? fixture.ReservedCapacity : fixture.Capacity;
            TTransition transition;
            UNIT_ASSERT(
                TCacheTestAccess::PrepareTransition(*fixture.Cache, target, transition) == EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, target.AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);

            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& oldOp) {
                const auto oldPolicy = TCacheTestAccess::HotPolicy(*fixture.Cache, oldOp);
                UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, transition));
                TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& newOp) {
                    UNIT_ASSERT_VALUES_EQUAL(newOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount,
                        target.Tiers[ECacheTier::Regular].HotSlots);
                    const auto policy = TCacheTestAccess::HotPolicy(*fixture.Cache, newOp);
                    for (ui64 bound : policy) {
                        UNIT_ASSERT(bound <= target.Tiers[ECacheTier::Regular].HotSlots);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(policy[1], 15);
                    UNIT_ASSERT_VALUES_EQUAL(policy[1], oldPolicy[1]);
                    if (growing) {
                        UNIT_ASSERT(policy[3] > oldPolicy[3]);
                        UNIT_ASSERT_VALUES_EQUAL(
                            oldOp.EffectiveHotSlots(), oldOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount);
                    }
                    for (ui64 bound : TCacheTestAccess::HotPolicy(*fixture.Cache, oldOp)) {
                        UNIT_ASSERT(bound <= oldOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount);
                    }
                });
            });

            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
            UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
                const auto before = TCacheTestAccess::HotPolicy(*fixture.Cache, spaceOp);
                UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
                UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, transition));
                UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, transition));
                UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, transition));
                UNIT_ASSERT(before == TCacheTestAccess::HotPolicy(*fixture.Cache, spaceOp));
            });
        }
    }

    Y_UNIT_TEST(HotMaintenanceRefreshesOperationAfterGrowth) {
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));
        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        {
            auto lock = TCacheTestAccess::LockHotResize(*fixture.Cache);
            UNIT_ASSERT(lock);
            UNIT_ASSERT(!TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        TVector<TTestCachePageRef> held;
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& oldOp) {
            {
                auto lock = TCacheTestAccess::LockHotResize(*fixture.Cache);
                UNIT_ASSERT(lock);
                UNIT_ASSERT(!TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            }
            UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::BeginHotResize(
                *fixture.Cache, fixture.ReservedCapacity.Tiers[ECacheTier::Regular].HotSlots));
            const auto collection = MakeCollectionCacheItem(701, 702);
            TPageCacheItem highPage;
            for (ui64 offset = 0; offset < fixture.Capacity.HandleCount() - 1; ++offset) {
                const auto key = TCacheKey::Page(collection, offset);
                highPage = InsertReadyPage(*fixture.Cache, key);
                auto found = FindPage(*fixture.Cache, key);
                UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
                held.push_back(std::move(found.Ref));
            }
            UNIT_ASSERT(highPage.Index() >= oldOp.HandleCount());
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& newOp) {
                auto& handle = newOp.Handles()[highPage.Index()];
                const auto state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
                UNIT_ASSERT(state.IsHot());
                handle.State.store(state.WithFrequency(0).Raw(), std::memory_order_relaxed);
                bool moved = false;
                for (ui64 slot = 0; slot < newOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount; ++slot) {
                    auto& word = newOp.View().Tiers[ECacheTier::Regular].HotStorage.Slots[slot];
                    if (word.load(std::memory_order_relaxed) == highPage.CacheItem().Raw()) {
                        const ui64 displaced =
                            newOp.View()
                                .Tiers[ECacheTier::Regular]
                                .HotStorage.Slots[newOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount - 1]
                                .exchange(highPage.CacheItem().Raw(), std::memory_order_relaxed);
                        word.store(displaced, std::memory_order_relaxed);
                        moved = true;
                        break;
                    }
                }
                UNIT_ASSERT(moved);
            });
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
            TCacheTestAccess::EnforceWithOperation(*fixture.Cache, oldOp);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, highPage.Index()).IsCold());
        });
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, growth));
    }

    Y_UNIT_TEST(HotPolicyRunsWhileCapacityCommits) {
        for (const bool growing : { true, false }) {
            TFixture fixture(6, 8, 7);
            if (!growing) {
                TTransition growth;
                UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                            EPrepareResult::Prepared);
                UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
                UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
                UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
                CompleteBucketResize(*fixture.Cache, *fixture.Space);
                CommitTransition(*fixture.Cache, growth);
            }
            const auto& target = growing ? fixture.ReservedCapacity : fixture.Capacity;
            TTransition transition;
            UNIT_ASSERT(
                TCacheTestAccess::PrepareTransition(*fixture.Cache, target, transition) == EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, target.AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);
            UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, transition));

            auto commitLock = TCacheTestAccess::LockHotResize(*fixture.Cache);
            UNIT_ASSERT(commitLock);
            std::atomic<bool> ready = false;
            std::atomic<bool> done = false;
            std::atomic<bool> policyValid = true;
            std::thread worker([&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
                    ready.store(true, std::memory_order_release);
                    do {
                        const auto policy = TCacheTestAccess::HotPolicy(*fixture.Cache, spaceOp);
                        for (ui64 bound : policy) {
                            if (bound > target.HotSlotCount()) {
                                policyValid.store(false, std::memory_order_relaxed);
                            }
                        }
                        fixture.Cache->EnforceCurrentLimit();
                    } while (!done.load(std::memory_order_acquire));
                });
            });
            Y_DEFER {
                done.store(true, std::memory_order_release);
                if (worker.joinable()) {
                    worker.join();
                }
            };
            while (!ready.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, transition));
            UNIT_ASSERT(!TCacheTestAccess::CommitTransition(*fixture.Cache, transition));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::Commit);
            commitLock = {};
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
            while (!TCacheTestAccess::CommitTransition(*fixture.Cache, transition)) {
                UNIT_ASSERT_C(TInstant::Now() < deadline, "Capacity commit did not finish while Hot policy ran");
                std::this_thread::yield();
            }
            done.store(true, std::memory_order_release);
            worker.join();
            UNIT_ASSERT(policyValid.load(std::memory_order_relaxed));
        }
    }

    Y_UNIT_TEST(OldViewReclaimsHighColdAndKeepColdPages) {
        enum class ECase {
            Cold,
            KeepCold,
            RetierCold
        };
        for (ECase scenario : { ECase::Cold, ECase::KeepCold, ECase::RetierCold }) {
            const bool keep = scenario != ECase::Cold;
            TCacheCapacityConfig reserved;
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
            TFixture fixture(6, 8, 7, reserved.Limit);
            const TLogoBlobID baseId(901, 902, 903);
            TCollectionCacheItem base;
            TTestCacheCollectionRef collectionHit;
            UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = baseId }, base,
                            collectionHit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, base, MakeCollection(baseId)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(base, ECacheMode::Regular));

            TVector<TTestCachePageRef> held;
            for (ui64 offset = 0; offset < fixture.Capacity.HandleCount() - 3; ++offset) {
                const TCacheKey key = TCacheKey::Page(base, offset);
                const TPageCacheItem page = InsertReadyPage(*fixture.Cache, key);
                auto found = FindPage(*fixture.Cache, key);
                UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
                UNIT_ASSERT(found.Ref.CacheItem() == page);
                held.push_back(std::move(found.Ref));
            }
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);

            TTransition growth;
            UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);

            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& oldOp) {
                UNIT_ASSERT_VALUES_EQUAL(oldOp.HandleCount(), fixture.Capacity.HandleCount());
                UNIT_ASSERT_VALUES_EQUAL(oldOp.AccessibleHandleCount(), fixture.ReservedCapacity.HandleCount());
                UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
                while (growth.Phase() == ETransitionPhase::AppendGrowth) {
                    UNIT_ASSERT(TCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
                }

                const TLogoBlobID id(901, 902, keep ? 905 : 904);
                TCollectionCacheItem collection;
                TTestCacheCollectionRef hit;
                UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = id }, collection,
                                hit) == ECacheResultStatus::Inserted);
                UNIT_ASSERT(collection.Index() >= oldOp.HandleCount());
                UNIT_ASSERT(
                    TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
                UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                    collection, keep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular));
                const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
                if (keep) {
                    UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(4 * bytes));
                }

                const TCacheKey key = TCacheKey::Page(collection, 1);
                const TPageCacheItem page = InsertReadyPage(*fixture.Cache, key);
                UNIT_ASSERT(page.Index() >= oldOp.HandleCount());
                auto found = FindPage(*fixture.Cache, key);
                UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
                TCollection* value = TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 2);
                UNIT_ASSERT_VALUES_EQUAL(value->ResidentPageBytes(), bytes);

                if (keep) {
                    found.Ref.Drop();
                    UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
                    UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
                    if (scenario == ECase::KeepCold) {
                        UNIT_ASSERT(TCacheTestAccess::ReclaimKeepCold(*fixture.Cache, oldOp));
                    } else {
                        UNIT_ASSERT(TCacheTestAccess::RetierColdToRegular(
                            *fixture.Cache, MakeColdItem(*fixture.Cache, page.Index())));
                        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index())
                                        .IsColdInTier(ECacheTier::Regular));
                        UNIT_ASSERT(TCacheTestAccess::ReclaimCold(*fixture.Cache, oldOp));
                    }
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 0);
                } else {
                    UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
                    found.Ref.Drop();
                    UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 1);
                    UNIT_ASSERT(TCacheTestAccess::ReclaimCold(*fixture.Cache, oldOp));
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
                }
                UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
                UNIT_ASSERT_VALUES_EQUAL(value->ResidentPageBytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(value->ReadyPageBytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
                UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ECacheResultStatus::Miss);
            });

            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, growth));
        }
    }

    Y_UNIT_TEST(PageBatchAdmitsHighHandleThroughMigrationOperation) {
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));

        const TLogoBlobID id(901, 902, 906);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

        TVector<TTestCachePageRef> held;
        for (ui64 offset = 0; offset < fixture.Capacity.HandleCount() - 3; ++offset) {
            const auto key = TCacheKey::Page(collection, offset);
            InsertReadyPage(*fixture.Cache, key);
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
            held.push_back(std::move(found.Ref));
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::SpareItem(*fixture.Cache, 7), 0);

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);

        struct TContext {
            TTestSharedCache& Cache;
            TTransition& Growth;
            bool Published = false;
        } context{ *fixture.Cache, growth };

        TTestTraits hooks{ &context,
            [](void* opaque, ECacheHookPoint point, TCacheItem) noexcept {
                auto& context = *static_cast<TContext*>(opaque);
                if (point == ECacheHookPoint::AfterPageBatchPrepared) {
                    Y_ABORT_UNLESS(!context.Published);
                    Y_ABORT_UNLESS(TCacheTestAccess::PublishFinalView(context.Cache, context.Growth));
                    while (context.Growth.Phase() == ETransitionPhase::AppendGrowth) {
                        Y_ABORT_UNLESS(TCacheTestAccess::AppendFreeHandles(context.Cache, context.Growth));
                    }
                    context.Published = true;
                }
            } };
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(1000), new TTestPageFetchWaiter(completed, ready, completedItem));
        {
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        }
        UNIT_ASSERT(context.Published);
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Inserted);
        const TPageCacheItem page = requests[0].Fetch().CacheItem();
        UNIT_ASSERT(page.Index() >= fixture.Capacity.HandleCount());
        UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(page.Index())));
        AssertPageHit(*fixture.Cache, TCacheKey::Page(collection, 1000), page);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 1);

        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, growth));
    }

    Y_UNIT_TEST(FinalDrainHot) {
        TFixture fixture(6, 8, 7);

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);

        const TCollectionCacheItem collection = MakeCollectionCacheItem(20, 21);
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < 32; ++offset) {
            pages.push_back(InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset)));
            UNIT_ASSERT(pages.back().Index() < fixture.Capacity.HandleCount());
        }

        const auto countHotWords = [&] {
            TVector<ui32> counts(fixture.ReservedCapacity.HandleCount(), 0);
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
                for (ui32 level = 0; level < 4; ++level) {
                    const TRingView& ring = spaceOp.Hot(static_cast<EHotLevel>(level));
                    for (ui64 slot = 0; slot < ring.Capacity; ++slot) {
                        const TCacheItem cacheItem =
                            TCacheItem::FromRaw(ring.Slots[slot].load(std::memory_order_relaxed));
                        if (!cacheItem.IsNull() && cacheItem.Index() < counts.size()) {
                            ++counts[cacheItem.Index()];
                        }
                    }
                }
            });
            return counts;
        };

        auto counts = countHotWords();
        for (TPageCacheItem page : pages) {
            UNIT_ASSERT_VALUES_EQUAL(counts[page.Index()], 1);
        }

        TTransition shrink;
        UNIT_ASSERT(
            TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, shrink));

        counts = countHotWords();
        for (TPageCacheItem page : pages) {
            UNIT_ASSERT_VALUES_EQUAL(counts[page.Index()], 1);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        }
    }
    Y_UNIT_TEST(FinalDrainCold) {
        TFixture fixture(6, 8, 7);

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));

        const TCollectionCacheItem collection = MakeCollectionCacheItem(22, 23);
        TVector<TCacheKey> keys;
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < fixture.Capacity.HandleCount() - 2; ++offset) {
            keys.push_back(TCacheKey::Page(collection, offset));
            pages.push_back(InsertReadyPage(*fixture.Cache, keys.back()));
            UNIT_ASSERT(pages.back().Index() < fixture.Capacity.HandleCount());
        }
        for (TPageCacheItem page : pages) {
            const EHandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).State();
            if (state == EHandleState::Hot) {
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            } else {
                UNIT_ASSERT(state == EHandleState::Cold);
            }
        }
        for (ui32 index = 0; index < 8; ++index) {
            auto found = FindPage(*fixture.Cache, keys[index]);
            UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
            found.Ref.Drop();
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
        }
        const ui32 target = 2;
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, pages[target].Index()).IsCold());

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Capacity.Limit));
        TTransition shrink;
        UNIT_ASSERT(
            TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, shrink));

        while (fixture.Cache->ReclaimCold()) {
        }
        UNIT_ASSERT(FindPage(*fixture.Cache, keys[target]).Status == ECacheResultStatus::Miss);
    }
    Y_UNIT_TEST(TransitionStaticAccounting) {
        TFixture fixture(6, 8, 7);
        const ui64 growthDelta = fixture.ReservedCapacity.StaticBytes - fixture.Capacity.StaticBytes;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);

        TTransition canceled;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, canceled) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), growthDelta);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT(TCacheTestAccess::AbandonTransition(*fixture.Cache, canceled));
        UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
        UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->TargetHandleCount(), fixture.ReservedCapacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);

        TTransition shrink;
        UNIT_ASSERT(
            TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, shrink);
        UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->TargetHandleCount(), fixture.Capacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);
    }

    Y_UNIT_TEST(CapacityCleanupRetainsOwnerAndReservationUntilAcknowledged) {
        using EStep = TCacheMappingTestAccess::EStep;
        TFixture fixture(6, 8, 7);
        auto& mapping = TCacheTestAccess::BucketsMapping(*fixture.Cache);
        bool failCleanup = true;
        ui32 maps = 0;
        auto hook = [&](EStep step) noexcept {
            if (step == EStep::Map) {
                ++maps;
            }
            if (step == EStep::AfterMap) {
                return false;
            }
            return !failCleanup || (step != EStep::Unmap && step != EStep::ShrinkBacking && step != EStep::DiscardTail);
        };
        TCacheMappingTestAccess::SetHook(mapping, hook);
        TTransition transition;
        // Register cleanup before any assertion can unwind the retained owner
        Y_DEFER {
            TCacheMappingTestAccess::ClearHook(mapping);
            if (transition.Phase() != ETransitionPhase::Idle) {
                TCacheTestAccess::AbandonTransition(*fixture.Cache, transition);
            }
        };
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        const auto result = TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, transition);
        if (maps == 0) {
            return;
        }
        UNIT_ASSERT(result == EPrepareResult::CleanupPending);
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::CancelRetry);
        const ui64 pending = fixture.ReservedCapacity.StaticBytes - fixture.Capacity.StaticBytes;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), pending);
        UNIT_ASSERT(TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
        UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(!TCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        TTransition contender;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, contender) ==
                    EPrepareResult::Rejected);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        failCleanup = false;
        UNIT_ASSERT(TCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT(!TCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
    }

    Y_UNIT_TEST(FailedCapacityPreparationAndCleanupYieldToPeriodicRetry) {
        using EStep = TCacheMappingTestAccess::EStep;
        for (bool delayed : { false, true }) {
            TFixture fixture(6, 8, 7);
            auto& mapping = TCacheTestAccess::BucketsMapping(*fixture.Cache);
            bool failCleanup = delayed;
            ui32 prepares = 0;
            auto hook = [&](EStep step) noexcept {
                if (step == EStep::Map) {
                    ++prepares;
                }
                return step != EStep::AfterMap &&
                       (!failCleanup ||
                           (step != EStep::Unmap && step != EStep::ShrinkBacking && step != EStep::DiscardTail));
            };
            TCacheMappingTestAccess::SetHook(mapping, hook);
            auto& transition = TCacheTestAccess::CapacityTransition(*fixture.Cache);
            Y_DEFER {
                TCacheMappingTestAccess::ClearHook(mapping);
                if (transition.Phase() != ETransitionPhase::Idle) {
                    TCacheTestAccess::AbandonTransition(*fixture.Cache, transition);
                }
            };
            TCacheTestAccess::SetCapacityTarget(*fixture.Cache, fixture.ReservedCapacity);
            const ui64 generation = fixture.Cache->AdmissionGeneration();
            const bool moreWork = fixture.Cache->RunMaintenance();
            if (prepares == 0) {
                return;
            }
            UNIT_ASSERT(!moreWork);
            if (!delayed) {
                UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
                continue;
            }
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::CancelRetry);
            for (ui32 attempt = 0; attempt < 3; ++attempt) {
                UNIT_ASSERT(!fixture.Cache->RunMaintenance());
            }
            UNIT_ASSERT_VALUES_EQUAL(prepares, 1);
            failCleanup = false;
            UNIT_ASSERT(!fixture.Cache->RunMaintenance());
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(prepares, 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        }
    }

    Y_UNIT_TEST(CapacityTierTransferRetainsOwnerThroughPostDonorCleanup) {
        using EStep = TCacheMappingTestAccess::EStep;
        for (bool abandon : { false, true }) {
            TFixture fixture(16, 2);
            auto target = fixture.Capacity;
            target.Tiers[ECacheTier::Regular].ColdSlots = target.HandleCount() / 4;
            target.Tiers[ECacheTier::Keep].ColdSlots = target.HandleCount() * 3 / 4;
            UNIT_ASSERT(target.HasValidTierCapacity());
            TTransition transition;
            UNIT_ASSERT(
                TCacheTestAccess::PrepareTransition(*fixture.Cache, target, transition) == EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, transition));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::PrepareReceiver);
            const ui64 generation = fixture.Cache->AdmissionGeneration();
            UNIT_ASSERT(TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
            UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
            auto& mapping = TCacheTestAccess::RingMapping(*fixture.Cache, ESpaceMap::KeepCold);
            bool fail = true;
            auto cleanup = [&](EStep step) noexcept {
                return !fail || (step != EStep::Unmap && step != EStep::ShrinkBacking && step != EStep::DiscardTail);
            };
            TCacheMappingTestAccess::SetHook(mapping, cleanup);
            Y_DEFER {
                TCacheMappingTestAccess::ClearHook(mapping);
                if (transition.Phase() != ETransitionPhase::Idle &&
                    transition.Phase() != ETransitionPhase::AbandonedForShutdown) {
                    TCacheTestAccess::AbandonTransition(*fixture.Cache, transition);
                }
            };
            auto prepare = [](ESpaceMap mapping) noexcept {
                return mapping != ESpaceMap::Free;
            };
            UNIT_ASSERT(!TCacheTestAccess::PrepareReceiver(*fixture.Cache, transition, prepare));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::CancelReceiver);
            UNIT_ASSERT(TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
            UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
            if (abandon) {
                UNIT_ASSERT(TCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
                UNIT_ASSERT(transition.Phase() == ETransitionPhase::AbandonedForShutdown);
                UNIT_ASSERT(!TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
                UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
                TTransition next;
                UNIT_ASSERT(
                    TCacheTestAccess::PrepareTransition(*fixture.Cache, target, next) == EPrepareResult::Rejected);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
                continue;
            }
            fail = false;
            UNIT_ASSERT(!TCacheTestAccess::PrepareReceiver(*fixture.Cache, transition));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::PrepareReceiver);
            UNIT_ASSERT(TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
            TCacheMappingTestAccess::ClearHook(mapping);
            UNIT_ASSERT(TCacheTestAccess::PrepareReceiver(*fixture.Cache, transition));
            do {
                UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            } while (transition.Phase() == ETransitionPhase::InitializeGrowth);
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
            UNIT_ASSERT(fixture.Space->CurrentConfiguration().Tiers == target.Tiers);
        }
    }

    Y_UNIT_TEST(CapacityPreparationExcludesOtherResizeOwners) {
        TFixture fixture(6, 8, 7);
        TTransition first;
        TTransition second;
        {
            auto guard = TCacheTestAccess::LockHotResize(*fixture.Cache);
            UNIT_ASSERT(guard);
            UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, first) ==
                        EPrepareResult::Rejected);
        }
        UNIT_ASSERT(first.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        auto invalid = fixture.ReservedCapacity;
        ++invalid.ExpectedPageSize;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, invalid, first) == EPrepareResult::Rejected);
        UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, first) ==
                    EPrepareResult::Prepared);
        const ui64 pendingBytes = fixture.Cache->StaticDeltaBytes();
        UNIT_ASSERT(TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, first));
        UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, second) ==
                    EPrepareResult::Rejected);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), pendingBytes);

        auto [foreignThreads, foreignSpace] =
            TCacheThreadTestAccess::MakeSpace(fixture.Capacity, fixture.ReservedCapacity, 1);
        UNIT_ASSERT(foreignSpace);
        UNIT_ASSERT(foreignSpace->PrepareTransition(fixture.ReservedCapacity, second) == EPrepareResult::Prepared);
        UNIT_ASSERT(!TCacheTestAccess::PublishMigrationView(*fixture.Cache, second));
        UNIT_ASSERT(!TCacheTestAccess::PublishFinalView(*fixture.Cache, second));
        UNIT_ASSERT(!TCacheTestAccess::CommitTransition(*fixture.Cache, second));
        UNIT_ASSERT(!TCacheTestAccess::AbandonTransition(*fixture.Cache, second));
        UNIT_ASSERT(second.Phase() == ETransitionPhase::Prepare);
        UNIT_ASSERT(foreignSpace->AbandonTransition(second));
        UNIT_ASSERT(TCacheTestAccess::AbandonTransition(*fixture.Cache, first));
        UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
    }

    Y_UNIT_TEST(CapacityOwnershipAllowsLegacyHotShrink) {
        TFixture fixture(6, 8, 7);
        TTransition transition;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, transition) ==
                    EPrepareResult::Prepared);
        {
            auto guard = TCacheTestAccess::LockHotResize(*fixture.Cache);
            UNIT_ASSERT(guard);
            const ui32 target = static_cast<ui32>(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) - 1);
            UNIT_ASSERT(TCacheTestAccess::BeginHotResize(*fixture.Cache, target));
        }
        while (TCacheTestAccess::HotResizePhase(*fixture.Cache) == EHotResizePhase::Cut) {
            UNIT_ASSERT(TCacheTestAccess::DrainHotResize(*fixture.Cache));
        }
        UNIT_ASSERT(TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
        UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
    }

    Y_UNIT_TEST(PublishedCapacityAbandonmentKeepsSpaceClosed) {
        TFixture fixture(6, 8, 7);
        TTransition transition;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, transition) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
        UNIT_ASSERT(TCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::AbandonedForShutdown);
        UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(!TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
        TTransition next;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, next) ==
                    EPrepareResult::Rejected);
        UNIT_ASSERT(!TCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
        UNIT_ASSERT(!TCacheTestAccess::CommitTransition(*fixture.Cache, transition));
    }

    Y_UNIT_TEST(CapacityOwnershipSurvivesReleaseRetry) {
        TFixture fixture(11, 8, 11);
        TCacheCapacityConfig target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(
            10, fixture.Capacity.ExpectedPageSize, fixture.Capacity.FixedBytes, fixture.Space->HazardCount(), target));
        TTransition shrink;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, target, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 10));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(TCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        while (shrink.Phase() == ETransitionPhase::FinalDrain) {
            UNIT_ASSERT(TCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        }
        auto failRelease = [](ESpaceMap, const void*, size_t) noexcept {
            return false;
        };
        UNIT_ASSERT(!TCacheTestAccess::TryReleaseTransitionWithHook(*fixture.Cache, shrink, failRelease));
        UNIT_ASSERT(shrink.Phase() == ETransitionPhase::ReleaseRetry);
        UNIT_ASSERT(TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(!TCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::CommitTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::CanChangeTiers(*fixture.Cache));
    }
    Y_UNIT_TEST(ColdReclaimSkipsEmptyRingAndFindsSparseEntry) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(505, 506);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        ui64 probes = 0;
        TTestTraits hooks{ &probes, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::BeforeColdRingPop) {
                                  ++*static_cast<ui64*>(context);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(!fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(probes, 0);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(probes, fixture.Capacity.ColdSlotCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        UNIT_ASSERT(!fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(probes, fixture.Capacity.ColdSlotCount());
    }

    Y_UNIT_TEST(ColdReclaimPreservesInProgressPublication) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(507, 508);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeColdExchange, TColdCacheItem{});
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        TGateThread publisher(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*cache, page));
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 1);
        UNIT_ASSERT(!cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 1);
        gate.Slots[0].Release();
        publisher.Join();
        UNIT_ASSERT(cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdReclaimableBytes(), 0);
    }

    Y_UNIT_TEST(ColdBytesSeparateHeldPayloadFromReclaimablePayload) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(501, 502);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ECacheResultStatus::Hit);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        held.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 1);
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        held.Drop();
        UNIT_ASSERT(!fixture.Cache->ReclaimCold()); // Removes the stale membership without charging it again.
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 1);
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
    }

    Y_UNIT_TEST(HeldColdBytesDoNotSuppressHotShrinkUnderPressure) {
        TFixture fixture(7);
        const TCollectionCacheItem collection = MakeCollectionCacheItem(503, 504);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        TVector<TTestCachePageRef> held;
        for (ui64 offset = 0; offset < 8; ++offset) {
            const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            held.emplace_back();
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, held.back()) == ECacheResultStatus::Hit);
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }
        for (ui64 offset = 8; offset < 16; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 8 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), 0);
        const ui64 softLimit = 12 * bytes;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(softLimit));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(softLimit));
        const ui64 hotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        for (ui32 step = 0; step < 1000 && fixture.Cache->PayloadUsage() > softLimit; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_LE(fixture.Cache->PayloadUsage(), softLimit);
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
        for (const auto& page : held) {
            UNIT_ASSERT_VALUES_EQUAL(page.size(), 4096);
        }
    }

    Y_UNIT_TEST(PhysicalCapacityMayExceedExactHardLimit) {
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(6, 4096, 0, 8, capacity));
        const ui64 hardLimit = capacity.StaticBytes + 8 * (4096 + NActors::TSharedData::OverheadSize);
        UNIT_ASSERT(hardLimit < capacity.TotalBytes);
        TFixture fixture(6, 8, 6, hardLimit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), hardLimit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), hardLimit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PhysicalHandleCount(), capacity.HandleCount());
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(hardLimit - 4096));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PhysicalHandleCount(), capacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), hardLimit);
    }

    Y_UNIT_TEST(DynamicCurrentAndHardLimits) {
        TFixture fixture(6, 8, 7);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), fixture.Capacity.Limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.Capacity.Limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservationLimit(), fixture.ReservedCapacity.Limit);

        const ui64 policyLimit = fixture.Capacity.Limit - 1;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(policyLimit));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), policyLimit);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), fixture.Capacity.HandleCount());
        UNIT_ASSERT(!fixture.Cache->UpdateCurrentLimit(fixture.Capacity.Limit + 1));
        UNIT_ASSERT(!fixture.Cache->UpdateHardLimit(policyLimit - 1));
        UNIT_ASSERT(!fixture.Cache->UpdateHardLimit(fixture.ReservedCapacity.Limit + 1));
        UNIT_ASSERT(!fixture.Cache->UpdateCurrentLimit(0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), policyLimit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), 1);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(policyLimit));

        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(fixture.ReservedCapacity.Limit));
        for (ui32 step = 0;
             step < 10000 && (TCacheTestAccess::HandleCount(*fixture.Cache) != fixture.ReservedCapacity.HandleCount() ||
                                 TCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "growth stalled at step " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), fixture.ReservedCapacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.ReservedCapacity.Limit);

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Capacity.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(fixture.Capacity.Limit));
        for (ui32 step = 0;
             step < 10000 && (TCacheTestAccess::HandleCount(*fixture.Cache) != fixture.Capacity.HandleCount() ||
                                 TCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink stalled at step " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), fixture.Capacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), fixture.Capacity.Limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.Capacity.Limit);
    }
    Y_UNIT_TEST(HardLimitRetargetSurvivesIntermediateCommit) {
        constexpr ui32 HazardCount = 7;
        TFixture fixture(6, HazardCount, 8);
        TCacheCapacityConfig middle;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, HazardCount, middle));

        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(middle.Limit));
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Prepare);
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(fixture.ReservedCapacity.Limit));
        UNIT_ASSERT(middle.Limit + 1 < fixture.ReservedCapacity.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(middle.Limit + 1));

        for (ui32 step = 0;
             step < 10000 && (TCacheTestAccess::HandleCount(*fixture.Cache) != middle.HandleCount() ||
                                 TCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "first growth step stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), middle.HandleCount());
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.ReservedCapacity.Limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), middle.Limit + 1);

        for (ui32 step = 0;
             step < 10000 && (TCacheTestAccess::HandleCount(*fixture.Cache) != fixture.ReservedCapacity.HandleCount() ||
                                 TCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(
                fixture.Cache->RunMaintenance(), TStringBuilder() << "second growth step stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), fixture.ReservedCapacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.ReservedCapacity.Limit);
    }
    Y_UNIT_TEST(HardShrinkCutScanIsBatched) {
        constexpr ui32 HazardCount = 1;
        TFixture fixture(11, HazardCount, 11);
        TCacheCapacityConfig target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(10, 4096, 0, HazardCount, target));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(target.Limit));

        for (ui32 step = 0; step < 10000; ++step) {
            const TSpaceState state = TCacheTestAccess::CurrentSpaceState(*fixture.Space);
            if (TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate &&
                !state.Resizing() && state.AddressBits() == target.AddressBits)
            {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
        }

        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HardTransitionWorkIndex(*fixture.Cache), 0);
        UNIT_ASSERT(fixture.Cache->RunMaintenance());
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HardTransitionWorkIndex(*fixture.Cache),
            target.HandleCount() + SharedCacheTransitionWorkBatch);

        for (ui32 step = 0;
             step < 10000 && (TCacheTestAccess::HandleCount(*fixture.Cache) != target.HandleCount() ||
                                 TCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink completion stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Idle);
    }

    struct TShrinkDependencyFixture : TFixture {
        explicit TShrinkDependencyFixture(ui8 addressBits = 6, ECacheMode mode = ECacheMode::Regular)
            : TFixture(addressBits, 2)
        {
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(addressBits - 1, 4096, 0, 2, Target));
            if (mode == ECacheMode::TryKeepInMemory) {
                UNIT_ASSERT(ConfigureKeep(*Cache, 4096));
            }
            // Exhaust unpublished candidates to place metadata high and recycle all other handles low-first
            TVector<TCollectionCacheItem> candidates;
            Y_DEFER {
                for (auto item : candidates) {
                    TCacheTestAccess::DiscardCandidate(*Cache, item);
                }
            };
            for (ui64 index = 2; index < Capacity.HandleCount(); ++index) {
                TCollectionCacheItem item;
                if (index == Target.HandleCount()) {
                    const TLogoBlobID id(53, 54, 156);
                    UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*Cache, Registry, { .Id = id }, item, CollectionOwner) ==
                                ECacheResultStatus::Inserted);
                    UNIT_ASSERT(TCacheTestAccess::MakeReady(*Cache, Registry, item, MakeCollection(id)));
                    UNIT_ASSERT(Cache->SetCollectionPagesCacheMode(item, mode));
                    Collection = item;
                    UNIT_ASSERT(Cache->Find(id, CollectionOwner) == ECacheResultStatus::Hit);
                } else {
                    item = AllocateCollection(*Cache, TCacheKey::Collection(TLogoBlobID(53, 55, index)));
                    candidates.push_back(item);
                }
                UNIT_ASSERT_VALUES_EQUAL(item.Index(), index);
            }
        }

        TPageCacheItem AddPage(ui64 offset, TTestCachePageRef& owner) {
            const auto page = InsertOwnedSizedPage(*Cache, Collection, offset, 64, 'x', owner);
            UNIT_ASSERT(page.Index() < Target.HandleCount());
            return page;
        }

        void Detach(bool retainOwner = false) {
            UNIT_ASSERT(TCacheTestAccess::DetachCollection(*Cache, Registry, Collection));
            if (!retainOwner) {
                CollectionOwner.Drop();
            }
            const auto state = TCacheTestAccess::HandleState(*Cache, Collection.Index());
            UNIT_ASSERT(state.IsCollectionKind() && state.IsSticky() && state.IsStickyNoneField());
            UNIT_ASSERT_VALUES_EQUAL(state.Refs(), retainOwner ? 1 : 0);
        }

        void BeginShrink() {
            UNIT_ASSERT(Cache->PayloadUsage() < Cache->SoftLimit());
            UNIT_ASSERT(AvailableFreeHandles(*Cache) > Target.HandleCount() / 2);
            UNIT_ASSERT(Cache->UpdateCurrentLimit(Target.Limit));
            UNIT_ASSERT(Cache->UpdateHardLimit(Target.Limit));
            for (ui32 step = 0; step < 10000; ++step) {
                const auto state = TCacheTestAccess::CurrentSpaceState(*Space);
                if (TCacheTestAccess::HardTransitionPhase(*Cache) == ETransitionPhase::Migrate && !state.Resizing() &&
                    state.AddressBits() == Target.AddressBits) {
                    return;
                }
                UNIT_ASSERT_C(Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
            }
            UNIT_FAIL("shrink did not reach its handle cut");
        }

        void FinishShrink() {
            for (ui32 step = 0; step < 10000 && Cache->NeedsCapacityMaintenance(); ++step) {
                const bool progress = Cache->RunMaintenance();
                UNIT_ASSERT_C(progress || !Cache->NeedsCapacityMaintenance(),
                    TStringBuilder() << "shrink completion stalled at " << step);
            }
            UNIT_ASSERT(!Cache->NeedsCapacityMaintenance());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*Cache), Target.HandleCount());
            UNIT_ASSERT_VALUES_EQUAL(Cache->StaticBytes(), Target.StaticBytes);
        }

        TCacheCapacityConfig Target;
        TCollectionCacheItem Collection;
        TTestCacheCollectionRef CollectionOwner;
    };

    Y_UNIT_TEST(DetachedHighCollectionReclaimsLowColdPagesBeforeShrink) {
        TShrinkDependencyFixture fixture;
        std::array<TPageCacheItem, 2> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestCachePageRef owner;
            pages[index] = fixture.AddPage(index, owner);
            owner.Drop();
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, pages[index].Index()).IsCold());
        }
        fixture.Detach();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, fixture.Collection), 2);
        fixture.BeginShrink();
        fixture.FinishShrink();
        for (auto page : pages) {
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(RegisteredPageOnlyCollectionReclaimsLowDependenciesBeforeShrink) {
        TShrinkDependencyFixture fixture;
        auto& cache = *fixture.Cache;
        const auto id = fixture.CollectionOwner->Id();
        TTestCachePageRef owner;
        const auto page = fixture.AddPage(0, owner);
        owner.Drop();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, page));
        fixture.CollectionOwner.Drop();
        const auto state = TCacheTestAccess::HandleState(cache, fixture.Collection.Index());
        UNIT_ASSERT(state.IsStickyField() && state.HasPageAttachment());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionRegistry& Registry;
            TCollectionCacheItem Item;
            TLogoBlobID Id;
            ui32 Detached = 0;
        } context{ cache, fixture.Registry, fixture.Collection, id };

        TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint point, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (point != ECacheHookPoint::CollectionReleased || item != ctx.Item.CacheItem() ||
                                  !TCacheTestAccess::HandleState(ctx.Cache, item.Index()).IsStickyField()) {
                                  return;
                              }
                              // Deliver the normal deferred actorless cleanup hint after the page set disappears
                              TTestCacheCollectionRef ref;
                              if (TCacheTestAccess::FindCollection(ctx.Cache, ctx.Registry, ctx.Id, ref) ==
                                  ECacheCollectionLookupStatus::Hit) {
                                  auto op = ctx.Cache.BeginOperation();
                                  if (op.Collections().DetachReleased(ctx.Registry, ctx.Item.CacheItem())) {
                                      ++ctx.Detached;
                                  }
                              }
                          } };
        TSharedCacheHookGuard guard(cache, hooks);
        fixture.BeginShrink();
        fixture.FinishShrink();
        UNIT_ASSERT_VALUES_EQUAL(context.Detached, 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.PayloadUsage(), 0);
    }

    Y_UNIT_TEST(DetachedHighCollectionReclaimsSparseLowHotPagesBeforeShrink) {
        for (bool pendingCut : { false, true }) {
            TShrinkDependencyFixture fixture(10);
            TTestCachePageRef owner;
            const auto page = fixture.AddPage(0, owner);
            owner.Drop();
            fixture.Detach();
            fixture.BeginShrink();
            while (TCacheTestAccess::SweepModes(*fixture.Cache)) {
            }
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
                const auto& tier = op.View().Tiers[ECacheTier::Regular];
                ui32 removed = 0;
                for (ui64 slot = 0; slot < tier.HotStorage.SlotCount; ++slot) {
                    if (tier.HotStorage.Slots[slot].load() == page.CacheItem().Raw()) {
                        tier.HotStorage.Slots[slot].store(0);
                        ++removed;
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(removed, 1);
                const auto ring = tier.Hot(EHotLevel::L0);
                constexpr ui64 batch = SharedCacheTransitionWorkBatch / HotLevelCount;
                UNIT_ASSERT(ring.Capacity > batch + 1);
                const ui64 next = ring.SelectPopSlot() - ring.Slots + 1;
                const ui64 slot = pendingCut ? ring.Capacity - 1 : (next + batch) % ring.Capacity;
                ring.Slots[slot].store(page.CacheItem().Raw());
            });
            if (pendingCut) {
                UNIT_ASSERT(TCacheTestAccess::BeginHotResize(*fixture.Cache, 15));
                UNIT_ASSERT(TCacheTestAccess::HotResizePhase(*fixture.Cache) == EHotResizePhase::Cut);
            }
            {
                auto op = fixture.Cache->BeginOperation();
                UNIT_ASSERT(op.ReclaimShrinkHandle(fixture.Collection.Index()) == EShrinkCutStatus::Progress);
            }
            if (pendingCut) {
                UNIT_ASSERT(TCacheTestAccess::HotResizePhase(*fixture.Cache) == EHotResizePhase::Idle);
            } else {
                // The first empty batch continued instead of parking before the sparse victim
                UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
            }
            fixture.FinishShrink();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        }
    }

    Y_UNIT_TEST(DetachedHighKeepCollectionFinishesItsModeSweepBeforeShrink) {
        TShrinkDependencyFixture fixture(6, ECacheMode::TryKeepInMemory);
        TTestCachePageRef owner;
        const auto page = fixture.AddPage(0, owner);
        owner.Drop();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHotInTier(ECacheTier::Keep));
        const auto keep = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(53, 54, 157), ECacheMode::TryKeepInMemory);
        TTestCachePageRef keepOwner;
        const auto keepPage = InsertOwnedSizedPage(*fixture.Cache, keep, 0, 64, 'k', keepOwner);
        keepOwner.Drop();
        UNIT_ASSERT(keep.Index() < fixture.Target.HandleCount());
        UNIT_ASSERT(keepPage.Index() < fixture.Target.HandleCount());
        fixture.Detach();
        fixture.BeginShrink();
        fixture.FinishShrink();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, keepPage.Index()).IsHotInTier(ECacheTier::Keep));
        TTestCachePageRef found;
        UNIT_ASSERT(fixture.Cache->Find(keep, 0, found) == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(found.data()[0], 'k');
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 1);
    }

    Y_UNIT_TEST(DetachedHighCollectionParksForRealOwnersBeforeShrink) {
        for (bool metadataOwner : { false, true }) {
            TShrinkDependencyFixture fixture;
            UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4096));
            TTestCachePageRef pageOwner;
            const auto page = fixture.AddPage(0, pageOwner);
            if (metadataOwner) {
                pageOwner.Drop();
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            }
            const auto keep = CreateModeCollection(
                *fixture.Cache, fixture.Registry, TLogoBlobID(53, 54, 158), ECacheMode::TryKeepInMemory);
            TTestCachePageRef keepOwner;
            const auto keepPage = InsertOwnedSizedPage(*fixture.Cache, keep, 0, 64, 'k', keepOwner);
            keepOwner.Drop();
            fixture.Detach(metadataOwner);
            fixture.BeginShrink();
            ui32 step = 0;
            while (step < 10000 && fixture.Cache->RunMaintenance()) {
                ++step;
            }
            UNIT_ASSERT(step < 10000);
            UNIT_ASSERT(fixture.Cache->NeedsCapacityMaintenance());
            UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
            const auto generation = fixture.Cache->AdmissionGeneration();
            for (ui32 retry = 0; retry < 4; ++retry) {
                UNIT_ASSERT(!fixture.Cache->RunMaintenance());
                UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
                UNIT_ASSERT(
                    TCacheTestAccess::HandleState(*fixture.Cache, keepPage.Index()).IsHotInTier(ECacheTier::Keep));
            }
            if (metadataOwner) {
                UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(pageOwner.data()[0], 'x');
                UNIT_ASSERT_VALUES_EQUAL(pageOwner.size(), 64);
            }
            fixture.Cache->ArmResourceWait(generation);
            Y_DEFER {
                fixture.Cache->DisarmResourceWait();
            };
            fixture.CollectionOwner.Drop();
            pageOwner.Drop();
            UNIT_ASSERT(fixture.Cache->AdmissionGeneration() != generation);
            fixture.FinishShrink();
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, keepPage.Index()).IsHotInTier(ECacheTier::Keep));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 1);
        }
    }

    Y_UNIT_TEST(DetachedCollectionPagesRelocateBeforeShrink) {
        constexpr ui32 HazardCount = 8;
        TFixture fixture(6, HazardCount, 6);
        TCacheCapacityConfig target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, HazardCount, target));

        const TCollectionLocation collectionLocation{ .Id = TLogoBlobID(53, 54, 55) };
        TCollectionCacheItem collection;
        TTestCacheCollectionRef collectionRef;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, collectionLocation, collection,
                        collectionRef) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(collectionLocation.Id)));
        TVector<TCacheKey> keys;
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < target.HandleCount() - 1; ++offset) {
            keys.push_back(TCacheKey::Page(collection, offset));
            pages.push_back(InsertReadyPage(*fixture.Cache, keys.back()));
        }
        const TPageCacheItem hotSource = pages[pages.size() - 2];
        const TPageCacheItem coldSource = pages.back();
        UNIT_ASSERT_VALUES_EQUAL(hotSource.Index(), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(coldSource.Index(), target.HandleCount() + 1);

        for (ui32 index = 0; index < 2; ++index) {
            const TPageCacheItem page = pages[index];
            if (TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot()) {
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            }
            const ui32 coldVersion = TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
            UNIT_ASSERT(TCacheTestAccess::EraseCold(
                *fixture.Cache, TColdCacheItem::Make(coldVersion & MaxItemVersion, page.Index())));
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        }
        const ui64 pageOwners = target.HandleCount() - 3;
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners + 1);
        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        collectionRef.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners);

        auto oldHot = FindPage(*fixture.Cache, keys[keys.size() - 2]);
        auto oldCold = FindPage(*fixture.Cache, keys.back());
        UNIT_ASSERT(oldHot.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(oldCold.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(oldHot.Ref.CacheItem() == hotSource);
        UNIT_ASSERT(oldCold.Ref.CacheItem() == coldSource);
        const char* oldHotData = oldHot.Ref.data();
        const char* oldColdData = oldCold.Ref.data();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, coldSource));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index()).IsCold());
        const ui64 residentBytes = fixture.Cache->ResidentBytes();

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(target.Limit));
        for (ui32 step = 0; step < 10000; ++step) {
            const TSpaceState state = TCacheTestAccess::CurrentSpaceState(*fixture.Space);
            if (TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate &&
                !state.Resizing() && state.AddressBits() == target.AddressBits)
            {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
        }
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // quarantine the high worker spare

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterReplacingPublished, hotSource.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool relocated = false;
        TGateThread relocator(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            relocated = cache->RunMaintenance();
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners + 1);

        const THandleState forwarding = TCacheTestAccess::HandleState(*fixture.Cache, hotSource.Index());
        UNIT_ASSERT(forwarding.IsReplacing());
        const TCacheItem hotReplacement =
            TCacheItem::FromRaw(TCacheTestAccess::HandleNext(*fixture.Cache, hotSource.Index())).WithoutFrozen();
        UNIT_ASSERT(hotReplacement.Index() >= 2 && hotReplacement.Index() < target.HandleCount());

        TTestCachePageRef acquiredHot;
        UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, hotSource, acquiredHot));
        UNIT_ASSERT(acquiredHot.CacheItem().CacheItem() == hotReplacement);
        acquiredHot.Drop();

        auto foundHot = FindPage(*fixture.Cache, keys[keys.size() - 2]);
        UNIT_ASSERT(foundHot.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(foundHot.Ref.CacheItem().CacheItem() == hotReplacement);
        UNIT_ASSERT_VALUES_EQUAL(foundHot.Ref.data(), oldHotData);
        UNIT_ASSERT_VALUES_EQUAL(oldHot.Ref.data(), oldHotData);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        foundHot.Ref.Drop();

        TPageCacheItem duplicate;
        TTestCachePageRef existing;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(keys[keys.size() - 2].Word(1)),
                        EStickyState::None, duplicate, existing) == ECacheResultStatus::Hit);
        UNIT_ASSERT(existing.CacheItem().CacheItem() == hotReplacement);
        existing.Drop();
        oldHot.Ref.Drop();
        const THandleState relocationOwner = TCacheTestAccess::HandleState(*fixture.Cache, hotSource.Index());
        UNIT_ASSERT(relocationOwner.IsReplacing());
        UNIT_ASSERT_VALUES_EQUAL(relocationOwner.Refs(), 1);

        gate.Slots[0].Release();
        relocator.Join();
        UNIT_ASSERT(relocated);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, hotSource.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners);

        gate.Slots[0].Arm(ECacheHookPoint::AfterReplacedPublished, coldSource.CacheItem());
        bool coldRelocated = false;
        TGateThread coldRelocator(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            coldRelocated = cache->RunMaintenance();
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners + 1);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index()).IsReplaced());
        const TCacheItem coldReplacement =
            TCacheItem::FromRaw(TCacheTestAccess::HandleNext(*fixture.Cache, coldSource.Index())).WithoutFrozen();
        UNIT_ASSERT(coldReplacement.Index() >= 2 && coldReplacement.Index() < target.HandleCount());
        TTestCachePageRef acquiredCold;
        UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, coldSource, acquiredCold));
        UNIT_ASSERT(acquiredCold.CacheItem().CacheItem() == coldReplacement);
        acquiredCold.Drop();
        auto foundCold = FindPage(*fixture.Cache, keys.back());
        UNIT_ASSERT(foundCold.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(foundCold.Ref.CacheItem().CacheItem() == coldReplacement);
        UNIT_ASSERT_VALUES_EQUAL(foundCold.Ref.data(), oldColdData);
        UNIT_ASSERT_VALUES_EQUAL(oldCold.Ref.data(), oldColdData);
        foundCold.Ref.Drop();
        gate.Slots[0].Release();
        coldRelocator.Join();
        UNIT_ASSERT(coldRelocated);
        const THandleState externalOwner = TCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index());
        UNIT_ASSERT(externalOwner.IsReplaced());
        UNIT_ASSERT_VALUES_EQUAL(externalOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(oldCold.Ref.data(), oldColdData);
        oldCold.Ref.Drop();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners);

        for (ui32 step = 0;
             step < 10000 && (TCacheTestAccess::HandleCount(*fixture.Cache) != target.HandleCount() ||
                                 TCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink completion stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        auto finalHot = FindPage(*fixture.Cache, keys[keys.size() - 2]);
        auto finalCold = FindPage(*fixture.Cache, keys.back());
        UNIT_ASSERT(finalHot.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(finalCold.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(finalHot.Ref.CacheItem().CacheItem() == hotReplacement);
        UNIT_ASSERT(finalCold.Ref.CacheItem().CacheItem() == coldReplacement);
    }
    Y_UNIT_TEST(StickyPagesUnlinkOnceThenRelocateBeforeShrink) {
        constexpr ui32 HazardCount = 8;
        TFixture fixture(6, HazardCount, 6);
        TCacheCapacityConfig target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, HazardCount, target));

        const TCollectionLocation collectionLocation{ .Id = TLogoBlobID(70, 71, 72) };
        TCollectionCacheItem collection;
        TTestCacheCollectionRef collectionRef;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, collectionLocation, collection,
                        collectionRef) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(collectionLocation.Id)));

        TVector<TCacheKey> regularKeys;
        TVector<TPageCacheItem> regularPages;
        for (ui64 offset = 0; offset < target.HandleCount() - 3; ++offset) {
            regularKeys.push_back(TCacheKey::Page(collection, offset));
            regularPages.push_back(InsertReadyPage(*fixture.Cache, regularKeys.back()));
        }
        UNIT_ASSERT_VALUES_EQUAL(regularPages.back().Index(), target.HandleCount() - 1);

        auto insertSticky = [&](ui64 offset) {
            TPageCacheItem page;
            TTestCachePageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset), EStickyState::Sticky,
                            page, hit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            return page;
        };

        const ui64 firstStickyOffset = target.HandleCount();
        const ui64 secondStickyOffset = firstStickyOffset + 1;
        const TPageCacheItem firstSticky = insertSticky(firstStickyOffset);
        const TPageCacheItem secondSticky = insertSticky(secondStickyOffset);
        TTestCachePageRef heldFirst;
        TTestCachePageRef heldSecond;
        UNIT_ASSERT(fixture.Cache->Find(collection, firstStickyOffset, heldFirst) == ECacheResultStatus::Hit);
        UNIT_ASSERT(fixture.Cache->Find(collection, secondStickyOffset, heldSecond) == ECacheResultStatus::Hit);
        UNIT_ASSERT(heldFirst.IsSticky() && heldSecond.IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(firstSticky.Index(), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(secondSticky.Index(), target.HandleCount() + 1);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), secondSticky.Index());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::NextInOwner(*fixture.Cache, secondSticky.Index()), firstSticky.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, firstSticky.Index()), 0);

        for (ui32 index = 0; index < 2; ++index) {
            const TPageCacheItem page = regularPages[index];
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            const ui32 coldVersion = TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
            UNIT_ASSERT(TCacheTestAccess::EraseCold(
                *fixture.Cache, TColdCacheItem::Make(coldVersion & MaxItemVersion, page.Index())));
        }

        const ui64 residentBytes = fixture.Cache->ResidentBytes();
        const ui64 keepBytes = fixture.Cache->StickyBytes();
        const ui64 stickyOwnedBytes = fixture.Cache->StickyOwnedBytes();
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(target.Limit));
        for (ui32 step = 0; step < 10000; ++step) {
            const TSpaceState state = TCacheTestAccess::CurrentSpaceState(*fixture.Space);
            if (TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate &&
                !state.Resizing() && state.AddressBits() == target.AddressBits)
            {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
        }
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // quarantine the high worker spare

        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // detach the Sticky list and Unsticky every cut page
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { firstSticky, secondSticky }) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsTransitionField());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }

        UNIT_ASSERT(heldFirst.IsSticky() && heldSecond.IsSticky());
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // relocate and re-Sticky the first cut page
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, firstSticky.Index()).IsReplaced());
        auto firstFound = FindPage(*fixture.Cache, TCacheKey::Page(collection, firstStickyOffset));
        UNIT_ASSERT(firstFound.Status == ECacheResultStatus::Hit);
        const TPageCacheItem firstReplacement = firstFound.Ref.CacheItem();
        firstFound.Ref.Drop();
        UNIT_ASSERT(firstReplacement.Index() < target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), firstReplacement.Index());
        const THandleState firstReplacementState =
            TCacheTestAccess::HandleState(*fixture.Cache, firstReplacement.Index());
        UNIT_ASSERT(firstReplacementState.IsSticky());
        UNIT_ASSERT(firstReplacementState.IsStickyField());

        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // relocate and re-Sticky the second cut page
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, secondSticky.Index()).IsReplaced());
        auto secondFound = FindPage(*fixture.Cache, TCacheKey::Page(collection, secondStickyOffset));
        UNIT_ASSERT(secondFound.Status == ECacheResultStatus::Hit);
        const TPageCacheItem secondReplacement = secondFound.Ref.CacheItem();
        secondFound.Ref.Drop();
        UNIT_ASSERT(secondReplacement.Index() < target.HandleCount());
        const THandleState secondReplacementState =
            TCacheTestAccess::HandleState(*fixture.Cache, secondReplacement.Index());
        UNIT_ASSERT(secondReplacementState.IsSticky());
        UNIT_ASSERT(secondReplacementState.IsStickyField());
        UNIT_ASSERT(heldFirst.IsSticky() && heldSecond.IsSticky());
        heldFirst.Drop();
        heldSecond.Drop();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, firstSticky.Index()).IsFree());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, secondSticky.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), secondReplacement.Index());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::NextInOwner(*fixture.Cache, secondReplacement.Index()), firstReplacement.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, firstReplacement.Index()), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), keepBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), stickyOwnedBytes);

        for (ui32 step = 0;
             step < 10000 && (TCacheTestAccess::HandleCount(*fixture.Cache) != target.HandleCount() ||
                                 TCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            fixture.Cache->RunMaintenance();
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), keepBytes);
    }
    Y_UNIT_TEST(DetachedStickyCutPagesFinishShrink) {
        constexpr ui32 HazardCount = 8;
        TFixture fixture(6, HazardCount, 6);
        TCacheCapacityConfig target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, HazardCount, target));

        const TCollectionLocation collectionLocation{ .Id = TLogoBlobID(73, 74, 75) };
        TCollectionCacheItem collection;
        TTestCacheCollectionRef collectionRef;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, collectionLocation, collection,
                        collectionRef) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(collectionLocation.Id)));

        TVector<TPageCacheItem> regularPages;
        for (ui64 offset = 0; offset < target.HandleCount() - 3; ++offset) {
            regularPages.push_back(InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset)));
        }
        const auto insertSticky = [&](ui64 offset) {
            TPageCacheItem page;
            TTestCachePageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset), EStickyState::Sticky,
                            page, hit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            return page;
        };
        const ui64 heldOffset = target.HandleCount();
        const TPageCacheItem heldPage = insertSticky(heldOffset);
        const TPageCacheItem unheldPage = insertSticky(heldOffset + 1);
        UNIT_ASSERT_VALUES_EQUAL(heldPage.Index(), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(unheldPage.Index(), target.HandleCount() + 1);
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, heldOffset, held) == ECacheResultStatus::Hit);
        const char* heldData = held.data();

        for (ui32 index = 0; index < 2; ++index) {
            const TPageCacheItem page = regularPages[index];
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, page.Index())));
        }

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(target.Limit));
        for (ui32 step = 0; step < 10000; ++step) {
            const TSpaceState state = TCacheTestAccess::CurrentSpaceState(*fixture.Space);
            if (TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate &&
                !state.Resizing() && state.AddressBits() == target.AddressBits) {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
        }
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // quarantine the high worker spare
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // unstick both cut pages
        for (TPageCacheItem page : { heldPage, unheldPage }) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky() && state.IsTransitionField());
        }

        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        collectionRef.Drop();
        for (ui32 step = 0; step < 10000; ++step) {
            if (TCacheTestAccess::HandleState(*fixture.Cache, heldPage.Index()).IsReplaced() &&
                TCacheTestAccess::HandleState(*fixture.Cache, unheldPage.Index()).IsFree()) {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "detached cut stalled at " << step);
        }
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, heldPage.Index()).IsReplaced());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, unheldPage.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(held.data(), heldData);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);

        held.Drop();
        for (ui32 step = 0;
             step < 10000 && (TCacheTestAccess::HandleCount(*fixture.Cache) != target.HandleCount() ||
                                 TCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step) {
            fixture.Cache->RunMaintenance();
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT(TCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Idle);
        auto found = FindPage(*fixture.Cache, TCacheKey::Page(collection, heldOffset));
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(found.Ref.data(), heldData);
    }
    Y_UNIT_TEST(PageBudgetExcludesStaticMetadataButConsumptionIncludesIt) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->StaticBytes() > bytes);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(bytes));
        const TCollectionCacheItem collection = MakeCollectionCacheItem(519, 520);
        InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes() + bytes);
        fixture.Cache->EnforceCurrentLimit();
        TTestCachePageRef ref;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, ref) == ECacheResultStatus::Hit);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        ref.Drop();
        for (ui32 step = 0; step < 100 && fixture.Cache->PayloadUsage() != 0; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(AdmissionReclaimBudgetNotifiesWaiterArmedAtYield) {
        struct TProgress {
            TTestSharedCache* Cache = nullptr;
            ui32 Steps = 0;
            ui32 Notifications = 0;
        } progress;

        TFixture fixture(10);
        const auto collection = MakeCollectionCacheItem(571, 572);
        const ui32 bucketMask = static_cast<ui32>(CurrentBucketCount(*fixture.Space) - 1);
        for (ui32 bucket = 0; bucket <= SharedCacheTransitionWorkBatch; ++bucket) {
            const auto key = ReviewKeyForBucket(collection, bucketMask, bucket, 10000);
            const auto page = InsertReadyPage(*fixture.Cache, key);
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        const ui64 bytes = (SharedCacheTransitionWorkBatch + 1) * pageBytes;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), SharedCacheTransitionWorkBatch + 1);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        progress.Cache = fixture.Cache.Get();
        TTestTraits hooks{ &progress, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              auto& progress = *static_cast<TProgress*>(context);
                              if (point == ECacheHookPoint::AfterAdmissionReclaimStep) {
                                  if (++progress.Steps == SharedCacheTransitionWorkBatch) {
                                      progress.Cache->ArmResourceWait(progress.Cache->AdmissionGeneration());
                                  }
                              } else if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++progress.Notifications;
                              }
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        UNIT_ASSERT(!TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(progress.Steps, SharedCacheTransitionWorkBatch);
        UNIT_ASSERT_VALUES_EQUAL(progress.Notifications, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }

    Y_UNIT_TEST(AdmissionWithoutReclaimProgressRemainsQuiet) {
        ui32 notifications = 0;
        TFixture fixture(5);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(1));
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, 1));
        TTestTraits hooks{ &notifications, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        UNIT_ASSERT(!TCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        fixture.Cache->DisarmResourceWait();
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, 1);
    }

    Y_UNIT_TEST(HandleAdmissionContinuesFrequencyAgingAtFixedHotSize) {
        TFixture fixture(5, 1);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(bytes));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 hotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        const auto collection = MakeCollectionCacheItem(573, 574);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        for (ui32 touch = 0; touch < 2; ++touch) {
            TTestCachePageRef held;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ECacheResultStatus::Hit);
        }
        for (ui32 index = 0; index < fixture.Capacity.HandleCount() - 3; ++index) {
            const TLogoBlobID id(201, 202, index + 1);
            const auto item = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(item);
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, item, MakeCollection(id)));
        }
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Frequency(), 2);
        const auto admitted = AllocateCollection(*fixture.Cache, TCacheKey::Collection(TLogoBlobID(203, 204, 1)));
        UNIT_ASSERT(admitted);
        UNIT_ASSERT_VALUES_EQUAL(admitted.Index(), page.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), hotSlots);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        TCacheTestAccess::DiscardCandidate(*fixture.Cache, admitted);
    }

    Y_UNIT_TEST(HotLayoutGrowthReportsMaintenanceProgress) {
        TFixture fixture(8);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 before = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::MinimumHotSlots(*fixture.Cache), before);
        UNIT_ASSERT(fixture.Cache->EnforceCurrentLimit());
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) > before);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(CurrentLimitReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const ui64 limit = 2 * bytes;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(limit));

        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, 0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), limit);

        // In-flight commitment has no reclaimable buffer; rejected reservations change no counters.
        const ui64 overall = fixture.Cache->OverallUsage();
        UNIT_ASSERT(!TCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overall);

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(limit - bytes));
        UNIT_ASSERT(!TCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), limit);
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }
    Y_UNIT_TEST(CurrentLimitRejectsReservationUntilHeldBufferIsReleased) {
        TFixture fixture;
        const auto collection = MakeCollectionCacheItem(541, 542);
        InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ECacheResultStatus::Hit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(!TCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(held.size(), 4096);
        held.Drop();
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }

    Y_UNIT_TEST(ReservedPageMakeReadyTransfersReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));

        const TCollectionCacheItem collection = MakeCollectionCacheItem(36, 37);
        TPageCacheItem cacheItem;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::None, cacheItem,
                        hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->MakeReady(cacheItem, MakePageData(cacheItem.Index())));

        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);

        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, cacheItem));
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }
    Y_UNIT_TEST(ReadyPageRejectsCompletionWithoutLeakingReference) {
        TFixture fixture;
        const auto collection = MakeCollectionCacheItem(36, 37);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsReady());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);

        UNIT_ASSERT(!fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
        UNIT_ASSERT(!fixture.Cache->FailReady(page));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);

        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
    }
    Y_UNIT_TEST(ReservationReclaimsCold) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(47, 48);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        const ui64 bytes = fixture.Cache->PayloadUsage();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->PayloadUsage()));

        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), fixture.Cache->CurrentLimit());

        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }
    Y_UNIT_TEST(CollectionAdmissionIsTemporarilyUnaccounted) {
        TFixture fixture;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->StaticBytes()));
        const TCollectionLocation location{ .Id = TLogoBlobID(38, 39, 40) };

        TCollectionCacheItem inserted;
        TTestCacheCollectionRef hit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, inserted, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!hit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT(
            TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, inserted, MakeCollection(location.Id)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(FailedPageReadyRefundsReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(41, 42);

        TPageCacheItem cacheItem;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::None, cacheItem,
                        hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->FailReady(cacheItem));

        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
        const THandleState free = TCacheTestAccess::HandleState(*fixture.Cache, cacheItem.Index());
        UNIT_ASSERT(free.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(free.Version(), AdvanceItemVersion(cacheItem.Version()));
        UNIT_ASSERT(!fixture.Cache->FailReady(cacheItem));

        TTestCachePageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, missing) == ECacheResultStatus::Miss);
    }
    Y_UNIT_TEST(FailedStickyPageReadyRefundsOwnership) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TLogoBlobID id(42, 43, 44);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        TPageCacheItem cacheItem;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::Sticky, cacheItem,
                        hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->FailReady(cacheItem));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(CurrentLimitUsesPolicyPath) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(30, 31);
        for (ui64 offset = 0; offset < 30; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
        }

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        const ui64 currentLimit = 15 * pageBytes;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(currentLimit));
        UNIT_ASSERT(!fixture.Cache->RunMaintenance());

        ui32 steps = 0;
        while (steps < 10000 && fixture.Cache->PayloadUsage() > currentLimit) {
            fixture.Cache->EnforceCurrentLimit();
            ++steps;
        }
        UNIT_ASSERT_C(steps < 10000, "policy did not converge");
        UNIT_ASSERT_C(fixture.Cache->PayloadUsage() <= currentLimit,
            TStringBuilder() << "usage=" << fixture.Cache->PayloadUsage() << " limit=" << currentLimit
                             << " steps=" << steps
                             << " phase=" << static_cast<ui32>(TCacheTestAccess::HotResizePhase(*fixture.Cache))
                             << " effectiveHot=" << TCacheTestAccess::EffectiveHotSlots(*fixture.Cache)
                             << " coldBytes=" << fixture.Cache->ColdBytes());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.Capacity.Limit);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), fixture.Capacity.HandleCount());
    }
    Y_UNIT_TEST(CurrentLimitReclaimsToSoftLimit) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(45, 46);
        for (ui64 offset = 0; offset < 30; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
        }

        const ui64 currentLimit = fixture.Cache->PayloadUsage();
        const ui64 payloadLimit = currentLimit;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(currentLimit));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), currentLimit - (payloadLimit * 3 + 19) / 20);

        ui32 steps = 0;
        while (steps < 10000 && fixture.Cache->RetainedBytes() > fixture.Cache->SoftLimit()) {
            fixture.Cache->EnforceCurrentLimit();
            ++steps;
        }
        UNIT_ASSERT_C(steps < 10000, "policy did not reach the soft limit");
        // Soft pressure limits active/protected bytes; passive Cold may remain within CurrentLimit.
        UNIT_ASSERT(fixture.Cache->RetainedBytes() <= fixture.Cache->SoftLimit());

        const ui64 usage = fixture.Cache->PayloadUsage();
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), usage);
    }
    Y_UNIT_TEST(CalculatedAndExplicitSoftLimitsUseSameRetentionWatermarks) {
        TFixture fixture;
        const auto checkWatermarks = [&] {
            const ui64 softLimit = fixture.Cache->SoftLimit();
            const auto [minimum, growth] = TCacheTestAccess::ColdByteWatermarks(*fixture.Cache);
            UNIT_ASSERT_VALUES_EQUAL(minimum, (softLimit + 4) / 5);
            UNIT_ASSERT_VALUES_EQUAL(growth, (softLimit * 3 + 9) / 10);
        };
        const ui64 calculated = fixture.Cache->SoftLimit();
        UNIT_ASSERT(calculated < fixture.Cache->CurrentLimit());
        checkWatermarks();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(calculated));
        checkWatermarks();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(Max<ui64>()));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), calculated);
        checkWatermarks();
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->CurrentLimit() - 4096));
        UNIT_ASSERT(fixture.Cache->SoftLimit() < calculated);
        checkWatermarks();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        checkWatermarks();
    }
    Y_UNIT_TEST(HotPolicyRejectsAnotherTiersPage) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(83, 84), 0);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        const auto before = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(before.IsHot());
        TCacheHotPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 64> slots{};
        TCacheTierRings rings;
        const TCacheTierView wrong{
            .Rings = &rings,
            .HotStorage = { slots.data(), sizeof(slots), slots.size() },
            .Id = ECacheTier::Keep,
        };
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.ProcessHot(spaceOp, wrong, EHotLevel::L0, page.CacheItem());
        });
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Raw(), before.Raw());
        for (const auto& slot : slots) {
            UNIT_ASSERT_VALUES_EQUAL(slot.load(), 0);
        }
    }

    Y_UNIT_TEST(HotCutDoesNotEvictAnotherTiersPage) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(83, 85), 0);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        const auto before = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(before.IsHot());
        TCacheHotPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 64> slots{};
        TCacheTierRings rings;
        std::atomic<ui64> limit = slots.size();
        const TCacheTierView wrong{
            .Rings = &rings,
            .HotStorage = { slots.data(), sizeof(slots), slots.size() },
            .EffectiveHotSlots = &limit,
            .Id = ECacheTier::Keep,
        };

        struct TContext {
            std::atomic<ui64>& Limit;
            TCacheItem Page;
            bool Visited = false;
        } context{ limit, page.CacheItem() };

        TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint point, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (point == ECacheHookPoint::BeforeHotExchange && item == ctx.Page) {
                                  ctx.Visited = true;
                                  ctx.Limit.store(HotLevelCount);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            UNIT_ASSERT(policy.PutHot(spaceOp, wrong, EHotLevel::L0, page.CacheItem()).IsNull());
        });
        UNIT_ASSERT(context.Visited);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Raw(), before.Raw());
        for (const auto& slot : slots) {
            UNIT_ASSERT_VALUES_EQUAL(slot.load(), 0);
        }
    }

    Y_UNIT_TEST(HotPolicyUsesTheSuppliedTierView) {
        TFixture fixture;
        TCacheHotPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 64> firstSlots{};
        std::array<std::atomic<ui64>, 32> secondSlots{};
        TCacheTierRings firstRings;
        TCacheTierRings secondRings;
        const TCacheTierView first{ &firstRings, { firstSlots.data(), sizeof(firstSlots), firstSlots.size() }, {} };
        const TCacheTierView second{ &secondRings, { secondSlots.data(), sizeof(secondSlots), secondSlots.size() },
            {} };
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            ui32 index = 2;
            for (auto level : { EHotLevel::L0, EHotLevel::L1, EHotLevel::L2, EHotLevel::L3 }) {
                const auto a = TCacheItem::Make(1, index++);
                const auto b = TCacheItem::Make(1, index++);
                UNIT_ASSERT(policy.PutHot(spaceOp, first, level, a).IsNull());
                UNIT_ASSERT(policy.PutHot(spaceOp, second, level, b).IsNull());
                UNIT_ASSERT_VALUES_EQUAL(first.Hot(level).Slots[0].load(), a.Raw());
                UNIT_ASSERT_VALUES_EQUAL(second.Hot(level).Slots[0].load(), b.Raw());
            }
        });
    }

    Y_UNIT_TEST(ProductionHotMinimumFitsOneBoundedPass) {
        constexpr ui32 hotSlots = SharedCachePolicyFor<TProdTraits>.MinHotSlots;
        UNIT_ASSERT(hotSlots >= 64 && hotSlots <= 1024);
        std::array<std::atomic<ui64>, hotSlots> slots{};
        TS5FifoRings rings;
        TSpaceView view;
        view.Tiers[ECacheTier::Regular] = {
            .Rings = &rings.Tier(ECacheTier::Regular),
            .HotStorage = { slots.data(), sizeof(slots), hotSlots },
        };
        for (ui32 level = 0; level < 4; ++level) {
            const auto hot = view.Hot(static_cast<EHotLevel>(level));
            UNIT_ASSERT(hot.Capacity > 0);
            UNIT_ASSERT(hot.Capacity <= SharedCacheTransitionWorkBatch / 4);
            hot.Slots[hot.Capacity - 1].store(level + 1, std::memory_order_relaxed);
            ui32 found = 0;
            const ui64 attempts = Min<ui64>(hot.Capacity, SharedCacheTransitionWorkBatch / 4);
            for (ui64 attempt = 0; attempt < attempts; ++attempt) {
                if (const ui64 item = hot.Pop()) {
                    UNIT_ASSERT_VALUES_EQUAL(item, level + 1);
                    ++found;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(found, 1);
        }
    }

    Y_UNIT_TEST(PairedColdTargetCountsReadyAndAdmittedPendingPages) {
        TFixture fixture(8);
        constexpr ui32 size = 128;
        constexpr ui64 bytes = size + NActors::TSharedData::OverheadSize;
        constexpr ui64 limit = 16 * bytes;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, limit));
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 58), ECacheMode::TryKeepInMemory);
        TVector<TTestCachePageRequest> requests;
        for (ui32 offset = 0; offset < 4; ++offset) {
            requests.emplace_back(MakePageLocation(offset, size), MakeIntrusive<TPageFetchWaiter>());
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        TVector<TKeepReservation> speculative;
        for (ui32 index = 0; index < 3; ++index) {
            auto candidate = TCacheTestAccess::ReserveKeepCandidate(*fixture.Cache, bytes);
            UNIT_ASSERT(candidate);
            speculative.push_back(std::move(candidate));
        }
        const auto first = requests.front().Fetch().CacheItem();
        UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(first.Index(), size)));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, limit));
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            const auto& tiers = op.View().Tiers;
            UNIT_ASSERT_VALUES_EQUAL(tiers[ECacheTier::Keep].ColdStorage.SlotCount, 4);
            UNIT_ASSERT_VALUES_EQUAL(tiers[ECacheTier::Regular].ColdStorage.SlotCount +
                                         tiers[ECacheTier::Keep].ColdStorage.SlotCount, fixture.Capacity.HandleCount());
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 7);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepPendingBytes(), 3 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), limit);
        speculative.clear();
        for (ui32 index = 1; index < requests.size(); ++index) {
            const auto page = requests[index].Fetch().CacheItem();
            UNIT_ASSERT(requests[index].Fetch().MakeReady(MakePageData(page.Index(), size)));
        }
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, limit));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 4);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepPendingBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->TargetHandleCount(), fixture.Capacity.HandleCount());
        for (ui32 offset = 0; offset < requests.size(); ++offset) {
            TTestCachePageRef page;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, page) == ECacheResultStatus::Hit);
            UNIT_ASSERT_VALUES_EQUAL(page.size(), size);
        }
    }

    Y_UNIT_TEST(PairedActiveHotMinimaShareTheExistingBudget) {
        constexpr auto production = SharedCachePolicyFor<TProdTraits>.HotMinimums(true);
        static_assert(production[ECacheTier::Regular] == 64 && production[ECacheTier::Keep] == 64);
        TFixture fixture(8);
        const ui64 handles = fixture.Cache->TargetHandleCount();
        const ui64 slots = fixture.Capacity.HotSlotCount();
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 1));
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            const auto& tiers = op.View().Tiers;
            UNIT_ASSERT_VALUES_EQUAL(
                tiers[ECacheTier::Regular].HotStorage.SlotCount + tiers[ECacheTier::Keep].HotStorage.SlotCount, slots);
            UNIT_ASSERT(tiers[ECacheTier::Keep].HotStorage.SlotCount >= 7);
        });
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), 8);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            UNIT_ASSERT_VALUES_EQUAL(op.View().Tiers[ECacheTier::Keep].EffectiveHotSlotCount(), 7);
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->TargetHandleCount(), handles);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);

        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 0));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), 15);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            UNIT_ASSERT_VALUES_EQUAL(op.View().Tiers[ECacheTier::Keep].HotStorage.SlotCount, HotLevelCount);
        });
    }

    Y_UNIT_TEST(PairedPendingKeepCreditRetainsTheActiveHotMinimum) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 2 * bytes));
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(195, 196, 57), ECacheMode::TryKeepInMemory);
        TTestCachePageRequest request(MakePageLocation(0), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ECacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 1);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 0));
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            UNIT_ASSERT(op.View().Tiers[ECacheTier::Keep].HotStorage.SlotCount >= 7);
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepPendingBytes(), bytes);
        UNIT_ASSERT(request.Fetch().FailReady());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedPages(), 0);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 0));
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            UNIT_ASSERT_VALUES_EQUAL(op.View().Tiers[ECacheTier::Keep].HotStorage.SlotCount, HotLevelCount);
        });
    }

    Y_UNIT_TEST(PairedActivationDuringCapacityShrinkKeepsTheIntermediateMonotone) {
        TFixture fixture(9);
        TCacheCapacityConfig smaller;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(8, 4096, 0, 8, smaller));
        const auto before = fixture.Space->CurrentConfiguration().Tiers;
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 0));
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(1));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(smaller.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(smaller.Limit));
        const auto& transition = TCacheTestAccess::CapacityTransition(*fixture.Cache);
        UNIT_ASSERT(transition.Phase() != ETransitionPhase::Idle);
        const auto& intermediate = transition.TargetConfiguration().Tiers;
        UNIT_ASSERT(intermediate[ECacheTier::Regular].HotSlots <= before[ECacheTier::Regular].HotSlots);
        UNIT_ASSERT(intermediate[ECacheTier::Keep].HotSlots <= before[ECacheTier::Keep].HotSlots);
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleCount(*fixture.Cache), smaller.HandleCount());
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            UNIT_ASSERT(op.View().Tiers[ECacheTier::Keep].HotStorage.SlotCount >= 7);
        });
    }

    Y_UNIT_TEST(HotMinimumIsAbsoluteAcrossBudgetsAndStickyUsage) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const auto ordinary = MakeCollectionCacheItem(531, 532);
        InsertReadyPage(*fixture.Cache, TCacheKey::Page(ordinary, 0));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(100 * bytes));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::MinimumHotSlots(*fixture.Cache), 15);
        const TLogoBlobID id(95, 96, 121);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        for (ui32 offset = 0; offset < 20; ++offset) {
            const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            UNIT_ASSERT(TCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        }
        for (ui64 limit : { ui64{ 0 }, bytes, 100 * bytes, fixture.Cache->CurrentLimit() }) {
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::MinimumHotSlots(*fixture.Cache), 15);
        }
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), 15);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            for (ui32 level = 0; level < 4; ++level) {
                UNIT_ASSERT(spaceOp.Hot(static_cast<EHotLevel>(level)).Capacity <= SharedCacheTransitionWorkBatch / 4);
            }
        });
    }
    Y_UNIT_TEST(InFlightPressureKeepsAbsoluteHotMinimumAndRecoveryCanGrow) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        InsertReadyPage(*fixture.Cache, TCacheKey::Page(MakeCollectionCacheItem(537, 538), 0));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(100 * bytes));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), 15);

        const ui64 reservation = fixture.Cache->CurrentLimit() - fixture.Cache->PayloadUsage();
        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, reservation));
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            for (ui32 pass = 0; pass < 200; ++pass) {
                TCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, bytes + 1, false);
            }
        });
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), 15);
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, reservation);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) > 15);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::MinimumHotSlots(*fixture.Cache), 15);
    }
    Y_UNIT_TEST(BoundedHotAgingVisitsEveryLevelOfLargeRings) {
        TFixture fixture(10);
        const auto collection = MakeCollectionCacheItem(535, 536);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        for (ui32 touch = 0; touch < 3; ++touch) {
            TTestCachePageRef ref;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, ref) == ECacheResultStatus::Hit);
        }
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            UNIT_ASSERT(spaceOp.Hot(EHotLevel::L0).Capacity > SharedCacheTransitionWorkBatch);
        });
        for (ui32 pass = 0; pass < 200 && fixture.Cache->HotPages() != 0; ++pass) {
            TCacheTestAccess::ProcessHotAtMinimum(*fixture.Cache);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 0);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
    }
    Y_UNIT_TEST(StickyBytesAreExcludedFromHotColdAndKeepBudgets) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(20 * bytes));
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(20 * bytes));
        const TLogoBlobID id(95, 96, 120);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        for (ui32 offset = 0; offset < 4; ++offset) {
            const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            UNIT_ASSERT(TCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), 4 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->EvictableByteBudget(), 16 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), (16 * bytes * 2) / 5);
        fixture.Cache->EnforceCurrentLimit();
        const auto [minimum, growth] = TCacheTestAccess::ColdByteWatermarks(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(minimum, (16 * bytes + 4) / 5);
        UNIT_ASSERT_VALUES_EQUAL(growth, (16 * bytes * 3 + 9) / 10);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(3 * bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->EvictableByteBudget(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), 0);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(20 * bytes));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->EvictableByteBudget(), 20 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), 8 * bytes);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->EvictableByteBudget(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), 0);
    }
    Y_UNIT_TEST(SoftLimitOverrideReclaimsAndSurvivesCurrentLimitChange) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(46, 47);
        for (ui64 offset = 0; offset < 30; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
        }

        const ui64 currentLimit = fixture.Cache->CurrentLimit();
        const ui64 defaultSoftLimit = fixture.Cache->SoftLimit();
        const ui64 softLimit = 15 * (4096 + NActors::TSharedData::OverheadSize);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), currentLimit);
        fixture.Cache->UpdateSoftLimitOverride(Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), defaultSoftLimit);
        fixture.Cache->UpdateSoftLimitOverride(softLimit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), currentLimit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), softLimit);

        for (ui32 step = 0; step < 10000 && fixture.Cache->RetainedBytes() > softLimit; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_LE(fixture.Cache->RetainedBytes(), softLimit);

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(softLimit - 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), softLimit - 1);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(currentLimit));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), softLimit);
        fixture.Cache->UpdateSoftLimitOverride(Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), defaultSoftLimit);
        fixture.Cache->UpdateSoftLimitOverride(Max<ui64>() - 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), currentLimit);
        fixture.Cache->UpdateSoftLimitOverride(Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), defaultSoftLimit);
    }
    Y_UNIT_TEST(CurrentPressureWithInFlightBytesReclaimsHotBelowSoftLimit) {
        for (bool atMinimum : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(95, 96, 119);
            const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            if (atMinimum) {
                TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
            }
            const ui64 hotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
            // A frequent page at minimum Hot must age normally before reclamation.
            if (atMinimum) {
                for (ui32 touch = 0; touch < 2; ++touch) {
                    TTestCachePageRef ref;
                    UNIT_ASSERT(fixture.Cache->Find(collection, 0, ref) == ECacheResultStatus::Hit);
                }
            }
            const ui64 current = fixture.Cache->CurrentLimit();
            UNIT_ASSERT(fixture.Cache->SoftLimit() > 2 * bytes);
            UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, current - bytes));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), current);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
            UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
            const ui64 usage = fixture.Cache->PayloadUsage();
            TCacheTestAccess::ReleaseReservation(*fixture.Cache, fixture.Cache->ReservedBytes());
            UNIT_ASSERT_LE(usage, current);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
            if (atMinimum) {
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache),
                    TCacheTestAccess::MinimumHotSlots(*fixture.Cache));
            } else {
                UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
            }
        }
    }
    Y_UNIT_TEST(StickyWithdrawalDuringCompletionUsesLiveRegularAdmission) {
        for (auto point : { ECacheHookPoint::BeforePageReadyState, ECacheHookPoint::BeforeReadyStatePublication }) {
            TFixture fixture;
            const TLogoBlobID id(95, 96, 120);
            const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
            TTestCachePageRef held;
            auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
            TVector<TTestCachePageRequest> requests;
            requests.emplace_back(MakePageLocation(0), waiter);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
            const auto page = requests.front().Fetch().CacheItem();
            TSharedCacheGate gate;
            gate.Slots[0].Arm(point, page.CacheItem());
            TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            bool ready = false;
            TGateThread writer(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                ready = requests.front().Fetch().MakeReady(MakePageData(page.Index()));
            });
            gate.Slots[0].Wait();
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            gate.Slots[0].Release();
            writer.Join();
            UNIT_ASSERT(ready && waiter->Ready());
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
            UNIT_ASSERT(!held.IsSticky());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
            held.Drop();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        }
    }
    Y_UNIT_TEST(PageReferenceStickyQueryTracksWithdrawalAndPromotion) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 121);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ECacheResultStatus::Hit);
        // Private single-page helpers admit explicitly ordinary pages; promote the held page first.
        UNIT_ASSERT(TCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        UNIT_ASSERT(held.IsSticky());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(!held.IsSticky());
        UNIT_ASSERT(TCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        UNIT_ASSERT(held.IsSticky());
    }

    struct TColdPoolFixture : TFixture {
        static constexpr ui64 PageBytes = 4096 + NActors::TSharedData::OverheadSize;
        TCollectionCacheItem Regular;
        TVector<TPageCacheItem> Fillers;

        TColdPoolFixture()
            : TFixture(8)
        {
            UNIT_ASSERT(Cache->UpdateCurrentLimit(100 * PageBytes));
            UNIT_ASSERT(Cache->UpdateSoftLimitOverride(20 * PageBytes));
            const TLogoBlobID keepId(95, 96, 123);
            const auto keep = AllocateCollection(*Cache, TCacheKey::Collection(keepId));
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*Cache, Registry, keep, MakeCollection(keepId)));
            UNIT_ASSERT(Cache->SetCollectionPagesCacheMode(keep, ECacheMode::Regular));
            for (ui32 offset = 0; offset < 20; ++offset) {
                Fillers.push_back(InsertReadyPage(*Cache, TCacheKey::Page(keep, offset)));
            }
            const TLogoBlobID regularId(95, 96, 124);
            Regular = AllocateCollection(*Cache, TCacheKey::Collection(regularId));
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*Cache, Registry, Regular, MakeCollection(regularId)));
            UNIT_ASSERT(Cache->SetCollectionPagesCacheMode(Regular, ECacheMode::Regular));
            UNIT_ASSERT_VALUES_EQUAL(Cache->RetainedBytes(), 20 * PageBytes);
        }

        TTestCachePageRef Fetch(ui32 offset) {
            TTestCachePageRef held;
            auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*Cache, held);
            TVector<TTestCachePageRequest> requests;
            requests.emplace_back(MakePageLocation(offset), waiter);
            UNIT_ASSERT(Cache->FindOrInsertBatch(Regular, requests));
            UNIT_ASSERT(requests.front().Status() == ECacheResultStatus::Inserted);
            const auto page = requests.front().Fetch().CacheItem();
            UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(page.Index())));
            UNIT_ASSERT(waiter->Ready());
            UNIT_ASSERT(TCacheTestAccess::HandleState(*Cache, page.Index()).IsCold());
            return held;
        }
    };

    Y_UNIT_TEST(ColdBuffersRemainOutsideSoftAfterFinalRelease) {
        TColdPoolFixture fixture;
        auto page = fixture.Fetch(0);
        const auto item = page.CacheItem();
        page.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 20 * fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 21 * fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 1);
        TTestCachePageRef hit;
        UNIT_ASSERT(fixture.Cache->Find(fixture.Regular, 0, hit) == ECacheResultStatus::Hit);
        UNIT_ASSERT(hit.CacheItem() == item);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, item.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 20 * fixture.PageBytes);
    }

    Y_UNIT_TEST(ColdPolicyRejectsAnotherTiersPage) {
        TColdPoolFixture fixture;
        auto held = fixture.Fetch(0);
        const auto page = held.CacheItem();
        held.Drop();
        const auto ticket = MakeColdItem(*fixture.Cache, page.Index());
        TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 4> slots{};
        TCacheTierRings rings;
        const TCacheTierView wrong{
            .Rings = &rings,
            .ColdStorage = { slots.data(), sizeof(slots), slots.size() },
            .Id = ECacheTier::Keep,
        };
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.PublishCold(spaceOp, wrong, page.CacheItem());
            policy.RouteCold(spaceOp, wrong, ticket);
            UNIT_ASSERT_VALUES_EQUAL(rings.ColdEntries(), 0);
            UNIT_ASSERT(!TCacheTestAccess::BindOperation(*fixture.Cache, spaceOp).Eviction().EraseCold(ticket,
                ECacheTier::Keep));
            for (bool preload : { false, true }) {
                rings.AddColdEntry();
                slots[0].store(ticket.Raw());
                UNIT_ASSERT(!(preload ? policy.ReclaimForPreload(spaceOp, wrong) : policy.Reclaim(spaceOp, wrong)));
                UNIT_ASSERT_VALUES_EQUAL(rings.ColdEntries(), 0);
                UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
            }
        });
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
    }

    Y_UNIT_TEST(ColdPolicyUsesIndependentQueueOwnersAndDrainsStaleWords) {
        TColdPoolFixture fixture;
        auto held = fixture.Fetch(0);
        const auto page = held.CacheItem();
        held.Drop();
        const auto ticket =
            TColdCacheItem::Make(TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()), page.Index());
        TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 4> firstSlots{};
        std::array<std::atomic<ui64>, 8> secondSlots{};
        TCacheTierRings firstRings;
        TCacheTierRings secondRings;
        const TCacheTierView first{ &firstRings, {}, { firstSlots.data(), sizeof(firstSlots), firstSlots.size() } };
        const TCacheTierView second{ &secondRings, {},
            { secondSlots.data(), sizeof(secondSlots), secondSlots.size() } };
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.RouteCold(spaceOp, first, ticket);
            UNIT_ASSERT_VALUES_EQUAL(firstRings.ColdEntries(), 1);
            UNIT_ASSERT_VALUES_EQUAL(secondRings.ColdEntries(), 0);
            policy.RouteCold(spaceOp, second, ticket);
            UNIT_ASSERT_VALUES_EQUAL(secondRings.ColdEntries(), 1);
            UNIT_ASSERT(policy.Reclaim(spaceOp, first));
            UNIT_ASSERT_VALUES_EQUAL(firstRings.ColdEntries(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
            UNIT_ASSERT(!policy.Reclaim(spaceOp, second));
            UNIT_ASSERT_VALUES_EQUAL(secondRings.ColdEntries(), 0);
        });
        UNIT_ASSERT(!fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 20 * fixture.PageBytes);
    }

    Y_UNIT_TEST(ColdRoutingDoesNotRefreshAStaleMembership) {
        TColdPoolFixture fixture;
        auto held = fixture.Fetch(0);
        const auto page = held.CacheItem();
        const auto oldTicket =
            TColdCacheItem::Make(TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()), page.Index());
        held.Drop();
        TTestCachePageRef reader;
        UNIT_ASSERT(fixture.Cache->Find(fixture.Regular, 0, reader) == ECacheResultStatus::Hit);
        reader.Drop();
        UNIT_ASSERT(TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()) != oldTicket.ColdVersion());
        TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 4> slots{};
        TCacheTierRings rings;
        const TCacheTierView tier{ &rings, {}, { slots.data(), sizeof(slots), slots.size() } };
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.RouteCold(spaceOp, tier, oldTicket);
            UNIT_ASSERT_VALUES_EQUAL(rings.ColdEntries(), 0);
            UNIT_ASSERT(!policy.Reclaim(spaceOp, tier));
        });
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
    }

    Y_UNIT_TEST(ColdEraseRetryRejectsAReusedNativeIdentity) {
        TFixture fixture(5, 2);
        auto& cache = *fixture.Cache;
        const auto collection = MakeCollectionCacheItem(541, 542);
        const auto oldPage = InsertReadyPage(cache, TCacheKey::Page(collection, 0));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, oldPage));
        const auto oldTicket = MakeColdItem(cache, oldPage.Index());
        TVector<TCollectionCacheItem> candidates;
        Y_DEFER {
            for (auto candidate : candidates) {
                TCacheTestAccess::DiscardCandidate(cache, candidate);
            }
        };
        while (AvailableFreeHandles(cache)) {
            const auto candidate =
                AllocateCollection(cache, TCacheKey::Collection(TLogoBlobID(243, 244, candidates.size() + 1)));
            UNIT_ASSERT(candidate);
            candidates.push_back(candidate);
        }

        struct TContext {
            TTestSharedCache& Cache;
            TColdCacheItem Ticket;
            TCacheKey NewKey;
            TPageCacheItem Replacement{};
            bool Reused = false;
        } context{ cache, oldTicket, TCacheKey::Page(collection, 1) };

        TTestTraits hooks{ &context, [](void* ptr, ECacheHookPoint point, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (point != ECacheHookPoint::BeforeColdEraseClaim ||
                                  item.Index() != ctx.Ticket.Index() || ctx.Reused) {
                                  return;
                              }
                              ctx.Reused = true;
                              Y_ABORT_UNLESS(TCacheTestAccess::EraseCold(ctx.Cache, ctx.Ticket));
                              ctx.Replacement = InsertReadyPage(ctx.Cache, ctx.NewKey);
                              Y_ABORT_UNLESS(ctx.Replacement.Index() == ctx.Ticket.Index());
                              Y_ABORT_UNLESS(TCacheTestAccess::EvictFromHot(ctx.Cache, ctx.Replacement));
                          } };
        TSharedCacheHookGuard guard(cache, hooks);
        UNIT_ASSERT(!TCacheTestAccess::EraseCold(cache, oldTicket));
        UNIT_ASSERT(context.Reused);
        UNIT_ASSERT_VALUES_EQUAL(context.Replacement.Index(), oldPage.Index());
        UNIT_ASSERT(context.Replacement.Version() != oldPage.Version());
        UNIT_ASSERT(MakeColdItem(cache, context.Replacement.Index()).ColdVersion() != oldTicket.ColdVersion());
        UNIT_ASSERT(TCacheTestAccess::HandleState(cache, context.Replacement.Index()).IsCold());
        auto found = FindPage(cache, context.NewKey);
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(found.Ref.CacheItem() == context.Replacement);
    }

    Y_UNIT_TEST(ColdPublicationRejectsAReusedNativeIdentity) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 126);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const auto key = TCacheKey::Page(collection, 0);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        auto held = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(held.Ref);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        auto fillers = HoldReviewFillers(fixture, 63, key.Hash() & 63);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(1));
        held.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        const auto replacement = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), page.Index());
        UNIT_ASSERT(replacement.Version() != page.Version());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, replacement.Index()).IsCold());
        const ui64 entries = fixture.Cache->ColdRingEntries();
        const ui32 membership = TCacheTestAccess::ColdVersion(*fixture.Cache, replacement.Index());
        TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.PublishCold(spaceOp, spaceOp.View().Tiers[ECacheTier::Regular], page.CacheItem());
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), entries);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdVersion(*fixture.Cache, replacement.Index()), membership);
    }

    Y_UNIT_TEST(KeepColdPhysicalOccupancyIncludesStaleWords) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 127);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(2 * TColdPoolFixture::PageBytes));
        const auto key = TCacheKey::Page(collection, 0);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        auto held = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(held.Ref && TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), TColdPoolFixture::PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            TCacheTestAccess::ReclaimKeepCold(*fixture.Cache, op);
        });
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 0);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
    }

    Y_UNIT_TEST(LastColdReleasePublishesAfterRefcountABA) {
        TColdPoolFixture fixture;
        auto held = fixture.Fetch(0);
        const auto page = held.CacheItem();
        const auto before = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        const ui32 coldVersion = TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
        UNIT_ASSERT(before.IsCold() && before.Refs() == 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeLastColdRefRelease, page.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        TGateThread releaser(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            held.Drop();
        });
        gate.Slots[0].Wait();
        TTestCachePageRef other;
        UNIT_ASSERT(cache->Find(fixture.Regular, 0, other) == ECacheResultStatus::Hit);
        const auto acquired = TCacheTestAccess::HandleState(*cache, page.Index());
        UNIT_ASSERT(acquired.IsCold() && acquired.Refs() == 2);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdVersion(*cache, page.Index()), AdvanceColdVersion(coldVersion));
        other.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*cache, page.Index()).Raw(), before.Raw());
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 0);
        gate.Slots[0].Release();
        releaser.Join();
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdReclaimableItems(), 1);
        UNIT_ASSERT(cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->RetainedBytes(), 20 * fixture.PageBytes);
        UNIT_ASSERT(cache->Find(fixture.Regular, 0, other) == ECacheResultStatus::Miss);
    }

    Y_UNIT_TEST(CurrentPressureReclaimsOrdinaryColdBeforeProtectedKeep) {
        TColdPoolFixture fixture;
        auto first = fixture.Fetch(0);
        const auto firstItem = first.CacheItem();
        first.Drop();
        auto second = fixture.Fetch(1);
        second.Drop();
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(21 * fixture.PageBytes));
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, firstItem.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 21 * fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 20 * fixture.PageBytes);
        for (auto keep : fixture.Fillers) {
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, keep.Index()).IsReady());
        }
    }

    Y_UNIT_TEST(ColdPromotionClaimsSoftBudgetWhenRoomAppears) {
        TColdPoolFixture fixture;
        auto page = fixture.Fetch(0);
        const auto item = page.CacheItem();
        page.Drop();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(21 * fixture.PageBytes));
        TTestCachePageRef hit;
        UNIT_ASSERT(fixture.Cache->Find(fixture.Regular, 0, hit) == ECacheResultStatus::Hit);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, item.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 21 * fixture.PageBytes);
    }

    Y_UNIT_TEST(SoftPressureDemotesHotWithoutFreeingItsBuffer) {
        TColdPoolFixture fixture;
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, fixture.Fillers.front()));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(19 * fixture.PageBytes));
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT_LE(fixture.Cache->RetainedBytes(), 19 * fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 20 * fixture.PageBytes);
        for (auto keep : fixture.Fillers) {
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, keep.Index()).IsReady());
        }
    }

    Y_UNIT_TEST(SoftPressurePreservesHeldColdUntilRelease) {
        TColdPoolFixture fixture;
        const auto page = fixture.Fillers.front();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        auto held = TCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem());
        UNIT_ASSERT(held);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(19 * fixture.PageBytes));
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        held.Drop();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 19 * fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 20 * fixture.PageBytes);
    }

    Y_UNIT_TEST(ColdFreeByteBalanceSubtractsInFlightCommitment) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(200 * bytes));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(100 * bytes));
        const auto collection = MakeCollectionCacheItem(539, 540);
        InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ECacheResultStatus::Hit);
        UNIT_ASSERT(TCacheTestAccess::TryReserve(
            *fixture.Cache, fixture.Cache->CurrentLimit() - fixture.Cache->PayloadUsage()));
        UNIT_ASSERT(fixture.Cache->RetainedBytes() < fixture.Cache->SoftLimit());
        const ui64 before = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < before);
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, fixture.Cache->ReservedBytes());
    }

    Y_UNIT_TEST(ColdOutsideSoftRegularPagesShareFetchAndLiveUntilLastReference) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TLogoBlobID id(95, 96, 113);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        TTestCachePageRef first;
        TTestCachePageRef second;
        auto firstWaiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, first);
        auto secondWaiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, second);
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), firstWaiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto page = requests.front().Fetch().CacheItem();
        TTestCachePageRef hit;
        TVector<TTestCachePageRequest> duplicate;
        duplicate.emplace_back(MakePageLocation(0), secondWaiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, duplicate));
        UNIT_ASSERT(duplicate.front().Status() == ECacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(firstWaiter->Ready() && secondWaiter->Ready());
        UNIT_ASSERT(first.CacheItem() == second.CacheItem());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, hit) == ECacheResultStatus::Hit);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        first.Drop();
        second.Drop();
        UNIT_ASSERT_VALUES_EQUAL(hit.size(), 4096);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        hit.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, hit) == ECacheResultStatus::Miss);
    }
    Y_UNIT_TEST(ColdOutsideSoftLastReleaseCannotResurrectTheOldPage) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 117);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        TTestCachePageRef held;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), waiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto oldPage = requests.front().Fetch().CacheItem();
        UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(oldPage.Index())));
        UNIT_ASSERT(waiter->Ready());
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterTombstoneOwnerClaim, oldPage.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        TGateThread releaser(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            held.Drop();
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, oldPage.Index()).IsTombstone());
        TTestCachePageRef hit;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, hit) == ECacheResultStatus::Miss);
        TTestCachePageRef replacement;
        auto nextWaiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, replacement);
        TVector<TTestCachePageRequest> next;
        next.emplace_back(MakePageLocation(0), nextWaiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, next));
        UNIT_ASSERT(next.front().Status() == ECacheResultStatus::Inserted);
        const auto nextPage = next.front().Fetch().CacheItem();
        UNIT_ASSERT(nextPage != oldPage);
        gate.Slots[0].Release();
        releaser.Join();
        UNIT_ASSERT(next.front().Fetch().MakeReady(MakePageData(nextPage.Index())));
        UNIT_ASSERT(nextWaiter->Ready());
        replacement.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
    }
    Y_UNIT_TEST(ColdOutsideSoftRegularDataDoesNotCreateSoftPressureOnKeep) {
        TFixture fixture;
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TLogoBlobID keepId(95, 96, 114);
        const auto keep = AllocateCollection(*fixture.Cache, TCacheKey::Collection(keepId));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 2 * bytes));
        const auto kept = InsertReadyPage(*fixture.Cache, TCacheKey::Page(keep, 0));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, kept));
        const TLogoBlobID regularId(95, 96, 115);
        const auto regular = AllocateCollection(*fixture.Cache, TCacheKey::Collection(regularId));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        for (ui32 offset = 1; offset <= 10; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, offset));
        }
        TTestCachePageRef ref;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, ref);
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), waiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(regular, requests));
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, kept.Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 10 * bytes);
        UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(requests.front().Fetch().CacheItem().Index())));
        UNIT_ASSERT(waiter->Ready());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 10 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 12 * bytes);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, kept.Index()).IsKeepCold());
        ref.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 12 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 10 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), 2 * bytes);
    }
    Y_UNIT_TEST(ColdOutsideSoftPagePromotesAfterCollectionBecomesKeepOrSticky) {
        for (ECacheMode mode : { ECacheMode::TryKeepInMemory, ECacheMode::Sticky }) {
            TFixture fixture;
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            const TLogoBlobID id(95, 96, 116);
            const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
            TTestCachePageRef held;
            auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
            TVector<TTestCachePageRequest> requests;
            requests.emplace_back(MakePageLocation(0), waiter);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
            const auto page = requests.front().Fetch().CacheItem();
            UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(page.Index())));
            UNIT_ASSERT(waiter->Ready());
            if (mode == ECacheMode::TryKeepInMemory) {
                UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
                UNIT_ASSERT(ConfigureKeep(*fixture.Cache, bytes));
            }
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, mode));
            TVector<TTestCachePageRequest> hits;
            hits.emplace_back(MakePageLocation(0), waiter);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
            UNIT_ASSERT(hits.front().Status() == ECacheResultStatus::Hit);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
            if (mode == ECacheMode::Sticky) {
                UNIT_ASSERT(hits.front().Page().IsSticky());
            } else {
                UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
            }
        }
    }
    Y_UNIT_TEST(HotAtMinimumRetainsFrequencyAging) {
        TFixture fixture(5);
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 minimum = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        const TCollectionCacheItem collection = MakeCollectionCacheItem(515, 516);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        for (ui32 hit = 0; hit < 2; ++hit) {
            TTestCachePageRef ref;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, ref) == ECacheResultStatus::Hit);
        }
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Frequency(), 2);
        UNIT_ASSERT(TCacheTestAccess::ProcessHotAtMinimum(*fixture.Cache));
        const auto aged = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(aged.IsHot());
        UNIT_ASSERT_VALUES_EQUAL(aged.Frequency(), 0);
        UNIT_ASSERT(TCacheTestAccess::ProcessHotAtMinimum(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), minimum);
    }
    Y_UNIT_TEST(PressureEvictsHotPagesAtMinimumWithoutScanningHandles) {
        for (bool zeroPayload : { false, true }) {
            TFixture fixture(5);
            TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
            const TCollectionCacheItem collection = MakeCollectionCacheItem(517, 518);
            for (ui64 offset = 0; offset < 4; ++offset) {
                InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            }
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            const ui64 limit = zeroPayload ? 0 : bytes;
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
            const ui64 minimum = TCacheTestAccess::MinimumHotSlots(*fixture.Cache);
            const ui64 target = zeroPayload ? 0 : bytes;
            for (ui32 step = 0;
                 step < 100 && (zeroPayload ? fixture.Cache->PayloadUsage() : fixture.Cache->RetainedBytes()) > target;
                 ++step) {
                fixture.Cache->EnforceCurrentLimit();
            }
            UNIT_ASSERT_LE((zeroPayload ? fixture.Cache->PayloadUsage() : fixture.Cache->RetainedBytes()), target);
            UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) >= minimum);
        }
    }
    Y_UNIT_TEST(PressureAtMinimumPreservesStickyAndHeldData) {
        TFixture fixture(5);
        const TLogoBlobID id(95, 96, 107);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const auto heldItem = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ECacheResultStatus::Hit);
        InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        const auto sticky = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(fixture.Cache->MakeReady(sticky, MakePageData(sticky.Index())));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        for (ui32 step = 0; step < 30; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_VALUES_EQUAL(held.size(), 4096);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, heldItem.Index()).IsCold());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, sticky.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 4096 + NActors::TSharedData::OverheadSize);
        TTestCachePageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, missing) == ECacheResultStatus::Miss);
    }
    Y_UNIT_TEST(KeepColdRemainsProtectedWhenPhysicalHeadroomAllowsHotGrowth) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 minimum = TCacheTestAccess::MinimumHotSlots(*fixture.Cache);
        const TLogoBlobID id(95, 96, 108);
        const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 5 * bytes));
        TVector<TPageCacheItem> keep;
        for (ui32 offset = 0; offset < 4; ++offset) {
            keep.push_back(InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset)));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, keep.back()));
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(MakeCollectionCacheItem(521, 522), offset));
        }
        for (ui32 step = 0; step < 10; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 4);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), 4 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 4 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 4 * bytes);
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) >= minimum);
        for (const auto page : keep) {
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        }
    }
    Y_UNIT_TEST(KeepCapReductionConvergesAcrossBoundedFifoBatches) {
        TFixture fixture(10);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
        ConfigureKeep(*fixture.Cache, 3 * bytes);
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(95, 96, 112), ECacheMode::TryKeepInMemory);
        TVector<TPageCacheItem> pages;
        for (ui32 offset = 0; offset < 3; ++offset) {
            pages.push_back(InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset)));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, pages.back()));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 3 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(bytes));
        for (ui32 step = 0; step < 8192 && fixture.Cache->KeepOwnedBytes() > bytes; ++step) {
            fixture.Cache->RunMaintenance();
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), bytes);
        UNIT_ASSERT_LE(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, pages.front().Index()).IsFree());
        UNIT_ASSERT_LE(fixture.Cache->PayloadUsage(), bytes);
    }

    Y_UNIT_TEST(OrdinaryColdRemainsCachedDespiteKeepPressureWhileCurrentLimitFits) {
        for (bool pressure : { false, true }) {
            for (bool held : { false, true }) {
                TFixture fixture;
                TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
                const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
                const TLogoBlobID id(95, 96, 109);
                const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
                UNIT_ASSERT(
                    TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
                UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
                UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
                UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 4 * bytes));
                TPageCacheItem ordinary;
                const auto regular = MakeCollectionCacheItem(523, 524);
                for (ui32 offset = 0; offset < 4; ++offset) {
                    const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
                    UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
                    const auto regularPage = InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, offset));
                    if (offset == 0) {
                        ordinary = regularPage;
                    }
                }
                InsertReadyPage(*fixture.Cache, TCacheKey::Page(regular, 4));
                UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 4 * bytes);
                if (pressure) {
                    UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(3 * bytes));
                    UNIT_ASSERT(fixture.Cache->KeepOwnedBytes() > fixture.Cache->KeepLimit());
                }
                UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, 1));
                const ui64 entries = fixture.Cache->ColdRingEntries();
                TTestCachePageRef ref;
                if (held) {
                    UNIT_ASSERT(fixture.Cache->Find(regular, 0, ref) == ECacheResultStatus::Hit);
                }
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, ordinary));
                if (held) {
                    UNIT_ASSERT_VALUES_EQUAL(ref.size(), 4096);
                    UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsCold());
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), entries);
                    ref.Drop();
                }
                UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), entries + 1);
                UNIT_ASSERT(fixture.Cache->Find(regular, 0, ref) == ECacheResultStatus::Hit);
                TCacheTestAccess::ReleaseReservation(*fixture.Cache, 1);
            }
        }
    }
    Y_UNIT_TEST(KeepColdProvidesHandlesUnderSlotPressureWithoutChangingItsCap) {
        TFixture fixture(6, 8, 8, ui64(1) << 20);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(92 * bytes));
        const auto collection = CreateModeCollection(
            *fixture.Cache, fixture.Registry, TLogoBlobID(95, 96, 110), ECacheMode::TryKeepInMemory);
        const auto stickyCollection =
            CreateModeCollection(*fixture.Cache, fixture.Registry, TLogoBlobID(95, 96, 111), ECacheMode::Sticky);
        TVector<TPageCacheItem> sticky;
        for (ui32 offset = 0; offset < 30; ++offset) {
            const auto page = AllocatePage(*fixture.Cache, TCacheKey::Page(stickyCollection, offset), 4096,
                NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
            auto inserted = FindOrInsertPage(*fixture.Cache, TCacheKey::Page(stickyCollection, offset), page);
            UNIT_ASSERT(inserted.Status == ECacheResultStatus::Inserted);
            inserted.Ref.Drop();
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            sticky.push_back(page);
        }
        UNIT_ASSERT(ConfigureKeep(*fixture.Cache, 24 * bytes));
        TVector<TPageCacheItem> keep;
        for (ui32 offset = 0; offset < 24; ++offset) {
            keep.push_back(InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset)));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, keep.back()));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), 24 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 24);
        const ui64 free = AvailableFreeHandles(*fixture.Cache);
        UNIT_ASSERT(free < 8);
        const auto regular = MakeCollectionCacheItem(525, 526);
        TVector<TTestCachePageRef> heldRegular;
        for (ui32 offset = 0; offset <= free; ++offset) {
            const auto key = TCacheKey::Page(regular, offset);
            InsertReadyPage(*fixture.Cache, key);
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
            heldRegular.push_back(std::move(found.Ref));
        }
        bool reclaimed = false;
        for (auto page : keep) {
            const auto state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            reclaimed |= !page.Matches(state) || !state.IsReady();
        }
        UNIT_ASSERT(reclaimed);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepLimit(), 24 * bytes);
        for (auto page : sticky) {
            const auto state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(page.Matches(state) && state.IsSticky());
        }
    }
    Y_UNIT_TEST(CurrentLimitShrinksHotAndReclaimsCold) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(34, 35);
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < 30; ++offset) {
            pages.push_back(InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset)));
        }
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, pages.front()));

        const ui64 usage = fixture.Cache->PayloadUsage();
        const ui64 coldBytes = fixture.Cache->ColdBytes();
        const ui64 effectiveHotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(usage - 1));
        UNIT_ASSERT(coldBytes < (usage - 1) / 5);

        UNIT_ASSERT(fixture.Cache->EnforceCurrentLimit());
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < effectiveHotSlots);
        UNIT_ASSERT(fixture.Cache->PayloadUsage() < usage);
    }
    Y_UNIT_TEST(IncomingPayloadShrinksHotUsingRemainingByteHeadroom) {
        for (bool exceedsFree : { false, true }) {
            TFixture fixture;
            const TCollectionCacheItem collection = MakeCollectionCacheItem(505, 506);
            for (ui64 offset = 0; offset < 8; ++offset) {
                InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            }
            const ui64 limit = fixture.Cache->CurrentLimit();
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
            const ui64 freeBytes = limit - fixture.Cache->PayloadUsage();
            UNIT_ASSERT(freeBytes > limit / 10);
            const ui64 incomingBytes = exceedsFree ? Max<ui64>() : freeBytes - limit / 10;
            const ui64 hotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
            TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
                TCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, incomingBytes, false);
            });
            UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
        }
    }
    Y_UNIT_TEST(IncomingPayloadPreventsHotGrowthWithoutCrossingShrinkWatermark) {
        TFixture fixture;
        const ui64 limit = fixture.Cache->CurrentLimit();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
        const ui64 freeBytes = limit - fixture.Cache->PayloadUsage();
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            TCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, freeBytes, false);
        });
        const ui64 hotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT(hotSlots < fixture.Capacity.HotSlotCount());
        const ui64 incomingBytes = freeBytes - limit / 4;
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            TCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, incomingBytes, false);
        });
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), hotSlots);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) > hotSlots);
    }
    Y_UNIT_TEST(IncomingPayloadPreventsHotGrowthAbovePolicyLimitWithColdHeadroom) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(509, 510);
        for (ui64 offset = 0; offset < 8; ++offset) {
            TPageCacheItem page;
            TTestCachePageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset, 16384),
                            EStickyState::None, page, hit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index(), 16384)));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }
        const ui64 limit = fixture.Cache->CurrentLimit();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
        UNIT_ASSERT(fixture.Cache->ColdReclaimableBytes() > limit * 3 / 10 + 1);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            UNIT_ASSERT(
                fixture.Cache->ColdReclaimableItems() + spaceOp.View().Free().Count() > spaceOp.View().ColdGrowHandles);
            TCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, 0, true);
        });
        const ui64 hotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT(hotSlots < fixture.Capacity.HotSlotCount());
        const ui64 incomingBytes = limit - fixture.Cache->PayloadUsage() + 1;
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            TCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, incomingBytes, false);
        });
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache), hotSlots);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) > hotSlots);
    }
    Y_UNIT_TEST(StaleColdEntriesDoNotSuppressHotShrinkForHandlePressure) {
        TFixture fixture;
        // Tiny pages need a matching byte budget so the 60% Hot floor leaves room to shrink.
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(64 * (1 + NActors::TSharedData::OverheadSize)));
        const TCollectionCacheItem collection = MakeCollectionCacheItem(511, 512);
        TVector<TTestCachePageRef> held;
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < 60; ++offset) {
            TPageCacheItem page;
            TTestCachePageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset, 1), EStickyState::None,
                            page, hit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index(), 1)));
            held.emplace_back();
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, held.back()) == ECacheResultStatus::Hit);
            pages.push_back(page);
        }
        for (ui64 offset = 0; offset < 20; ++offset) {
            TTestCachePageRef hit;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, hit) == ECacheResultStatus::Hit);
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, pages[offset]));
            hit.Drop();
            held[offset].Drop();
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, held[offset]) == ECacheResultStatus::Hit);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        UNIT_ASSERT(fixture.Cache->ColdRingEntries() >= 20);
        TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            const ui64 free = spaceOp.View().Free().Count();
            UNIT_ASSERT(free < spaceOp.View().ColdMinHandles);
            UNIT_ASSERT(fixture.Cache->ColdRingEntries() + free > spaceOp.View().ColdGrowHandles);
        });
        const ui64 hotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
        for (const auto& page : held) {
            UNIT_ASSERT_VALUES_EQUAL(page.size(), 1);
        }
    }
    Y_UNIT_TEST(HandleHeadroomShrinksHotBeforeBytePressure) {
        TFixture fixture;
        // Tiny pages need a matching byte budget so the 60% Hot floor leaves room to shrink.
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(64 * (1 + NActors::TSharedData::OverheadSize)));
        const TLogoBlobID id(93, 94, 96);
        TCollectionCacheItem collection;
        TTestCacheCollectionRef collectionHit;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = id }, collection,
                        collectionHit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        for (ui64 offset = 1; offset <= 60; ++offset) {
            TPageCacheItem page;
            TTestCachePageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset, 1),
                            offset <= 20 ? EStickyState::Sticky : EStickyState::None, page,
                            hit) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index(), 1)));
        }

        const ui64 freeHandles = AvailableFreeHandles(*fixture.Cache);
        UNIT_ASSERT(freeHandles < fixture.Capacity.HandleCount() / 5);
        UNIT_ASSERT(fixture.Cache->PayloadUsage() + fixture.Capacity.Limit / 5 < fixture.Cache->CurrentLimit());
        const ui64 hotSlots = TCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        for (ui32 step = 0; step < 1000 && TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) == hotSlots; ++step)
        {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT(TCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
    }
    void CheckHotRequeueAdmissionWake(bool releaseForeignRef) {
        TFixture fixture(6, 2);
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(601, 602, 1));
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(1));

        TVector<TTestCachePageRef> held;
        TPageCacheItem cold;
        for (ui64 offset = 0; offset < 2; ++offset) {
            const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
            held.emplace_back();
            UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, page, held.back()));
            if (offset == 0) {
                cold = page;
            }
        }

        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(fixture.Cache->CurrentLimit()));
        const ui64 l1Slots = TCacheTestAccess::WithSpace(*fixture.Cache, [](auto& spaceOp) {
            return spaceOp.Hot(EHotLevel::L1).Capacity;
        });
        TPageCacheItem hot;
        for (ui64 offset = 2; offset < l1Slots + 2; ++offset) {
            const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
            if (offset == 2) {
                hot = page;
            } else {
                held.emplace_back();
                UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, page, held.back()));
            }
        }
        TTestCachePageRef foreign;
        UNIT_ASSERT(fixture.Cache->Find(collection, 2, foreign) == ECacheResultStatus::Hit);
        if (!releaseForeignRef) {
            foreign.Drop();
        }
        const ui64 bytes = held.front().size() + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->PayloadUsage()));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(fixture.Cache->CurrentLimit() - 1));
        UNIT_ASSERT(!fixture.Cache->CanReserve(bytes));

        struct TObservation {
            TSharedCacheGate Gate;
            std::atomic<ui32> Notifications = 0;
        } observation;

        observation.Gate.Slots[0].Arm(ECacheHookPoint::BeforeHotExchange, hot.CacheItem());
        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem item) noexcept {
                              auto& observation = *static_cast<TObservation*>(context);
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  observation.Notifications.fetch_add(1, std::memory_order_relaxed);
                              }
                              TSharedCacheGate::Invoke(&observation.Gate, point, item);
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        fixture.Cache->ArmResourceWait(fixture.Cache->AdmissionGeneration());
        TTestCachePageRef found;
        TGateThread lookup(observation.Gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            Y_ABORT_UNLESS(fixture.Cache->Find(collection, 0, found) == ECacheResultStatus::Hit);
        });
        observation.Gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), releaseForeignRef ? 0 : 1);
        foreign.Drop();
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, hot.Index()).Refs(), 0);
        const auto hotIsQueued = [&] {
            return TCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
                for (ui64 slot = 0; slot < spaceOp.EffectiveHotSlots(); ++slot) {
                    if (spaceOp.View().Tiers[ECacheTier::Regular].HotStorage.Slots[slot].load(
                            std::memory_order_acquire) == hot.CacheItem().Raw()) {
                        return true;
                    }
                }
                return false;
            });
        };
        UNIT_ASSERT(!hotIsQueued());

        // Consume the early wake and exhaust reclamation before the actor would re-arm
        for (ui32 step = 0;; ++step) {
            UNIT_ASSERT(step < 1000);
            if (!fixture.Cache->ReclaimForAdmission(bytes)) {
                break;
            }
        }
        UNIT_ASSERT(!fixture.Cache->CanReserve(bytes));
        UNIT_ASSERT(!fixture.Cache->EnforceCurrentLimit());
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        UNIT_ASSERT(!TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), 1);

        observation.Gate.Slots[0].Release();
        lookup.Join();
        UNIT_ASSERT(found && found.CacheItem() == cold);
        UNIT_ASSERT(hotIsQueued());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), 2);
        UNIT_ASSERT(!fixture.Cache->CanReserve(bytes));

        UNIT_ASSERT(TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        TCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT_VALUES_EQUAL(found.size(), 4096);
        UNIT_ASSERT_VALUES_EQUAL(found.data()[0], static_cast<char>(cold.Index()));
    }

    Y_UNIT_TEST(HotRequeueNotifiesAfterEarlyLastRefWake) {
        CheckHotRequeueAdmissionWake(false);
    }

    Y_UNIT_TEST(HotRequeueNotifiesAfterForeignLastRefWake) {
        CheckHotRequeueAdmissionWake(true);
    }

    Y_UNIT_TEST(HeldColdBytePressureDoesNotWakeWithoutProgress) {
        TFixture fixture;
        TCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(603, 604), 1);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        TTestCachePageRef held;
        UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, page, held));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui64 bytes = held.size() + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(!TCacheTestAccess::TryReserve(*fixture.Cache, bytes));

        ui32 notifications = 0;
        TTestTraits hooks{ &notifications, [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            UNIT_ASSERT(!TCacheTestAccess::TryReserve(*fixture.Cache, bytes));
            UNIT_ASSERT(!fixture.Cache->ReclaimForAdmission(bytes));
            UNIT_ASSERT(!fixture.Cache->EnforceCurrentLimit());
            UNIT_ASSERT(!fixture.Cache->RunMaintenance());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
            UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        }
    }

    Y_UNIT_TEST(LateHotExchangeAfterLogicalShrinkIsRecovered) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(43, 44);
        for (ui64 offset = 0; offset < 6; ++offset) {
            InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, offset));
        }

        TPageCacheItem cacheItem;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(6), EStickyState::None, cacheItem,
                        hit) == ECacheResultStatus::Inserted);

        TSharedCacheGate gate;
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        gate.Slots[0].Arm(ECacheHookPoint::BeforeHotExchange, cacheItem.CacheItem());

        bool ready = false;
        TGateThread readyThread(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            ready = fixture.Cache->MakeReady(cacheItem, MakePageData(cacheItem.Index()));
        });
        gate.Slots[0].Wait();

        const ui64 lateSlot = TCacheTestAccess::WithSpace(*fixture.Cache, [](auto& op) {
            const auto& tier = op.View().Tiers[ECacheTier::Regular];
            const auto l1 = tier.Hot(EHotLevel::L1);
            return static_cast<ui64>(l1.Slots - tier.HotStorage.Slots) + 6 % l1.Capacity;
        });
        const ui32 targetHotSlots = static_cast<ui32>(lateSlot);
        UNIT_ASSERT(TCacheTestAccess::BeginHotResize(*fixture.Cache, targetHotSlots));
        UNIT_ASSERT(TCacheTestAccess::DrainHotResize(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::HotResizePhase(*fixture.Cache) == EHotResizePhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HotSlot(*fixture.Cache, lateSlot), 0);

        gate.Slots[0].Release();
        readyThread.Join();

        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HotSlot(*fixture.Cache, lateSlot), 0);
        const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, cacheItem.Index());
        UNIT_ASSERT(state.IsCold());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
    }
    Y_UNIT_TEST(CurrentLimitExposesReferencedOverage) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(32, 33);
        TPageCacheItem insertedItem;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::None, insertedItem,
                        hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(insertedItem, MakePageData(insertedItem.Index())));
        TTestCachePageRef page;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, page) == ECacheResultStatus::Hit);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page.CacheItem()));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(1));

        for (ui32 step = 0; step < 1000 && fixture.Cache->EnforceCurrentLimit(); ++step) {
        }
        UNIT_ASSERT(fixture.Cache->PayloadUsage() > fixture.Cache->CurrentLimit());
        const THandleState referenced = TCacheTestAccess::HandleState(*fixture.Cache, page.CacheItem().Index());
        UNIT_ASSERT(referenced.IsCold());
        UNIT_ASSERT_VALUES_EQUAL(referenced.Refs(), 1);

        page.Drop();
        for (ui32 step = 0; step < 1000 && fixture.Cache->PayloadUsage() > fixture.Cache->CurrentLimit(); ++step) {
            UNIT_ASSERT_C(fixture.Cache->EnforceCurrentLimit(), TStringBuilder() << "policy stalled at step " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }
    Y_UNIT_TEST(GrowthPinsSplitPredecessorUntilLinkCas) {
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));
        const auto keys = MakeResizeKeys(*fixture.Space, MakeCollectionCacheItem(8, 15), 800);
        const TPageCacheItem high = InsertReadyPage(*fixture.Cache, keys[2]);
        TVector<TTestCachePageRef> held;
        {
            auto found = FindPage(*fixture.Cache, keys[2]);
            UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
            held.push_back(std::move(found.Ref));
        }
        const TCollectionCacheItem fillerCollection = MakeCollectionCacheItem(81, 82);
        for (ui64 offset = 0; held.size() < fixture.Capacity.HandleCount() - 3; ++offset) {
            const TCacheKey key = TCacheKey::Page(fillerCollection, offset);
            if ((key.Hash() & (fixture.Capacity.BucketCount() - 1)) == 0) {
                continue;
            }
            InsertReadyPage(*fixture.Cache, key);
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
            held.push_back(std::move(found.Ref));
        }
        const TPageCacheItem low = InsertReadyPage(*fixture.Cache, keys[0]);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, low));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, low.Index()).IsCold());

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterGrowthBucketPublished);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool firstActivation = true;
        TGateThread firstActivator(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            firstActivation = TCacheTestAccess::ActivateBucketSplitOrMerge(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, low.Index())));
        const THandleState pinned = TCacheTestAccess::HandleState(*fixture.Cache, low.Index());
        UNIT_ASSERT(pinned.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(pinned.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::WithSpace(*fixture.Cache, [](TSpaceOperation& spaceOp) {
            return spaceOp.TryAllocateHandle();
        }), 0);
        gate.Slots[0].Release();
        firstActivator.Join();
        UNIT_ASSERT(!firstActivation);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, low.Index()).IsFree());

        const TPageCacheItem reused = AllocatePage(*fixture.Cache, keys[1], 4096);
        UNIT_ASSERT(reused);
        UNIT_ASSERT_VALUES_EQUAL(reused.Index(), low.Index());
        UNIT_ASSERT_VALUES_UNEQUAL(reused.Version(), low.Version());
        auto inserted = FindOrInsertPage(*fixture.Cache, keys[1], reused);
        UNIT_ASSERT(inserted.Status == ECacheResultStatus::Inserted);
        inserted.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->MakeReady(reused, MakePageData(reused.Index())));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        AssertPageHit(*fixture.Cache, keys[1], reused);
        AssertPageHit(*fixture.Cache, keys[2], high);
        UNIT_ASSERT(FindPage(*fixture.Cache, keys[0]).Status == ECacheResultStatus::Miss);
        CommitTransition(*fixture.Cache, growth);
    }

    void ReviewMarkerRemovalPredecessor(bool grow, bool reuseBeforeClaim) {
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(reserved.Limit));
        const ui32 splitBit = static_cast<ui32>(SharedCacheBucketCount(grow ? 6 : 5));
        const auto collection = MakeCollectionCacheItem(117, 50017);
        const auto key = ReviewKeyForBucket(collection, (splitBit << 1) - 1, 0, 100000);
        const auto replacementKey = ReviewKeyForBucket(collection, (splitBit << 1) - 1, 0, 200000, &key);
        const auto predecessor = InsertReadyPage(*fixture.Cache, key);
        auto predecessorHold = FindPage(*fixture.Cache, key);
        auto fillers = HoldReviewFillers(fixture, splitBit - 1, 0);
        predecessorHold.Ref.Drop();
        if (TCacheTestAccess::HandleState(*fixture.Cache, predecessor.Index()).IsHot()) {
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, predecessor));
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);

        TTransition growth;
        if (grow) {
            UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, grow ? 7 : 5));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        if (!grow) {
            UNIT_ASSERT(TCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        }

        TSharedCacheGate gate;
        gate.Slots[0].Arm(reuseBeforeClaim ? ECacheHookPoint::BeforeBucketSplitLinkClaim
                                           : ECacheHookPoint::BeforeTableLinkCas, predecessor.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool removed = true;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TCacheTestAccess::RemoveBucketSplit(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, predecessor.Index())));
        TPageCacheItem replacement;
        if (reuseBeforeClaim) {
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, predecessor.Index()).IsFree());
            replacement = InsertReadyPage(*fixture.Cache, replacementKey);
        } else {
            const auto pinned = TCacheTestAccess::HandleState(*fixture.Cache, predecessor.Index());
            UNIT_ASSERT(pinned.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(pinned.Refs(), 1);
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        }
        gate.Slots[0].Release();
        remover.Join();
        UNIT_ASSERT(!removed);
        if (!reuseBeforeClaim) {
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, predecessor.Index()).IsFree());
            replacement = InsertReadyPage(*fixture.Cache, replacementKey);
        }
        UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), predecessor.Index());
        UNIT_ASSERT_VALUES_UNEQUAL(replacement.Version(), predecessor.Version());
        UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
        AssertReviewTerminal(*fixture.Cache, replacement.CacheItem(), "R17");
        AssertPageHit(*fixture.Cache, replacementKey, replacement);
        const auto missing = ReviewKeyForBucket(collection, splitBit - 1, 0, 300000, &replacementKey);
        UNIT_ASSERT(FindPage(*fixture.Cache, missing).Status == ECacheResultStatus::Miss);
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        AssertPageHit(*fixture.Cache, replacementKey, replacement);
        if (grow) {
            CommitTransition(*fixture.Cache, growth);
        }
    }

    Y_UNIT_TEST(ReviewR17GrowthRemovalRejectsReusedPredecessor) {
        ReviewMarkerRemovalPredecessor(true, true);
    }

    Y_UNIT_TEST(ReviewR17ShrinkRemovalRejectsReusedPredecessor) {
        ReviewMarkerRemovalPredecessor(false, true);
    }

    Y_UNIT_TEST(ReviewR17GrowthRemovalPinsPredecessorThroughCas) {
        ReviewMarkerRemovalPredecessor(true, false);
    }

    Y_UNIT_TEST(ReviewR17ShrinkRemovalPinsPredecessorThroughCas) {
        ReviewMarkerRemovalPredecessor(false, false);
    }

    Y_UNIT_TEST(ReviewR17GrowthRemovalRetiresMarkerBeforeReleasingPredecessor) {
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(reserved.Limit));
        const auto keys = MakeResizeKeys(*fixture.Space, MakeCollectionCacheItem(118, 50018), 400000);
        const auto low = InsertReadyPage(*fixture.Cache, keys[0]);
        const auto high = InsertReadyPage(*fixture.Cache, keys[2]);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, low));

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeLastColdRefRelease, low.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool removed = false;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TCacheTestAccess::RemoveBucketSplit(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex).IsFree());
        const ui32 highBucket = TCacheTestAccess::BucketResizeState(*fixture.Space).SplitBit();
        UNIT_ASSERT(TCacheTestAccess::BucketHead(*fixture.Cache, highBucket) == high.CacheItem());
        AssertReviewTerminal(*fixture.Cache, low.CacheItem(), "R17");
        gate.Slots[0].Release();
        remover.Join();
        UNIT_ASSERT(removed);
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[2], high);
        CommitTransition(*fixture.Cache, growth);
    }

    void CheckResizeInterference(bool remove) {
        TFixture fixture(6, 8, 7);
        const auto collection = MakeCollectionCacheItem(259, 50059);
        const ui32 splitBit = static_cast<ui32>(CurrentBucketCount(*fixture.Space));
        const ui32 mask = (splitBit << 1) - 1;
        const auto firstKey = ReviewKeyForBucket(collection, mask, 0, 900000);
        const auto nextKey = ReviewKeyForBucket(collection, mask, 0, firstKey.Word(1) + 1, &firstKey);
        const auto first = InsertReadyPage(*fixture.Cache, firstKey);
        TTestCachePageRef held;
        UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, first, held));
        auto candidates = AllocateAvailableHandleCandidates(*fixture.Cache);
        Y_DEFER {
            for (const auto item : candidates) {
                TCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        UNIT_ASSERT(!candidates.empty());
        TCacheTestAccess::DiscardCandidate(*fixture.Cache, candidates.back());
        candidates.pop_back();
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        if (remove) {
            UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
            UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        }
        const auto before = TCacheTestAccess::BucketResizeState(*fixture.Space);
        TSharedCacheGate gate;
        gate.Slots[0].Arm(remove ? ECacheHookPoint::BeforeTableLinkCas : ECacheHookPoint::AfterGrowthBucketPublished,
            remove ? first.CacheItem() : TCacheItem{});
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        bool moreWork = false;
        TGateThread resizer(gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            moreWork = TCacheTestAccess::AdvanceBucketResize(*fixture.Cache);
        });
        gate.Slots[0].Wait();
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        TPageCacheItem inserted;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(nextKey.Word(1)), EStickyState::None,
                        inserted, hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, inserted.Index()).IsPending());
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        gate.Slots[0].Release();
        resizer.Join();
        UNIT_ASSERT(moreWork);
        const auto after = TCacheTestAccess::BucketResizeState(*fixture.Space);
        UNIT_ASSERT(after.Phase() == before.Phase());
        UNIT_ASSERT_VALUES_EQUAL(after.Cursor(), before.Cursor());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
    }

    Y_UNIT_TEST(GrowthSplitInterferenceKeepsMaintenanceRunnable) {
        CheckResizeInterference(false);
    }

    Y_UNIT_TEST(MarkerRemovalInterferenceKeepsMaintenanceRunnable) {
        CheckResizeInterference(true);
    }

    Y_UNIT_TEST(WrongPhaseResizeStepsRemainBlocked) {
        TFixture fixture;
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        UNIT_ASSERT(!TCacheTestAccess::AdvanceBucketResize(*fixture.Cache));
    }

    void CheckUnlinkNotification(bool chain, bool armBeforeUnlink) {
        TFixture fixture(6, 8, 7);
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        const auto collection = MakeCollectionCacheItem(257, 50057);
        const ui32 splitBit = static_cast<ui32>(CurrentBucketCount(*fixture.Space));
        const ui32 mask = (splitBit << 1) - 1;
        TVector<TCacheKey> keys{ ReviewKeyForBucket(collection, mask, 0, 800000) };
        if (chain) {
            keys.push_back(ReviewKeyForBucket(collection, mask, 0, keys.front().Word(1) + 1, &keys.front()));
        }
        TVector<TPageCacheItem> pages;
        TVector<TTestCachePageRef> held;
        for (const auto& key : keys) {
            const auto page = InsertReadyPage(*fixture.Cache, key);
            pages.push_back(page);
            held.emplace_back();
            UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, page, held.back()));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }
        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));

        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            TArrayRef<const TPageCacheItem> Pages;
            ui64 Bytes = 0;
            ui64 FreeCount = 0;
            ui32 Notifications = 0;
            bool Published = false;
            bool Activated = false;
        } observation;

        observation.Cache = fixture.Cache.Get();
        observation.Pages = TArrayRef<const TPageCacheItem>(pages.data(), pages.size());
        observation.Bytes = fixture.Cache->PayloadUsage();
        observation.FreeCount = TCacheTestAccess::FreeCount(*fixture.Cache);
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            TOperationItemRef<TTestTraits> firstOwner(cacheOp);
            TOperationItemRef<TTestTraits> secondOwner(cacheOp);
            auto expected = TCacheTestAccess::HandleState(*fixture.Cache, pages.front().Index());
            UNIT_ASSERT(cacheOp.TryBeginErase(pages.front().CacheItem(), expected, firstOwner.Get()));
            if (chain) {
                expected = TCacheTestAccess::HandleState(*fixture.Cache, pages.back().Index());
                UNIT_ASSERT(cacheOp.TryBeginErase(pages.back().CacheItem(), expected, secondOwner.Get()));
            }
            UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
            TTestTraits hooks{ &observation,
                [](void* context, ECacheHookPoint point, TCacheItem) noexcept {
                    if (point != ECacheHookPoint::ResourcesAvailable) {
                        return;
                    }
                    auto& observation = *static_cast<TObservation*>(context);
                    ++observation.Notifications;
                    observation.Published = observation.Cache->PayloadUsage() == observation.Bytes &&
                                            TCacheTestAccess::FreeCount(*observation.Cache) == observation.FreeCount;
                    for (const auto page : observation.Pages) {
                        const auto state = TCacheTestAccess::HandleState(*observation.Cache, page.Index());
                        observation.Published &=
                            state.IsTombstone() && state.Refs() == 2 && page.CacheItem().Matches(state);
                    }
                    observation.Activated = TCacheTestAccess::ActivateBucketSplitOrMerge(*observation.Cache);
                } };
            TSharedCacheHookGuard hookGuard(*fixture.Cache, hooks);
            const ui64 generation = fixture.Cache->AdmissionGeneration();
            if (armBeforeUnlink) {
                fixture.Cache->ArmResourceWait(generation);
            }
            cacheOp.FinishErase(pages.back().CacheItem(), keys.back());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
            UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforeUnlink ? 1 : 0);
            if (!armBeforeUnlink) {
                fixture.Cache->ArmResourceWait(generation);
            }
            UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
            UNIT_ASSERT(observation.Published && observation.Activated);
            const ui64 unlinkedGeneration = fixture.Cache->AdmissionGeneration();
            fixture.Cache->ArmResourceWait(unlinkedGeneration);
            for (ui32 index = 0; index < pages.size(); ++index) {
                cacheOp.FinishErase(pages[index].CacheItem(), keys[index]);
            }
            firstOwner.Drop();
            secondOwner.Drop();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), unlinkedGeneration);
            UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
            for (ui32 index = 0; index < pages.size(); ++index) {
                UNIT_ASSERT(held[index]);
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, pages[index].Index()).Refs(), 1);
            }
            fixture.Cache->DisarmResourceWait();
        }
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
    }

    Y_UNIT_TEST(LogicalUnlinkNotifiesWithHeldPayload) {
        for (bool armed : { false, true }) {
            CheckUnlinkNotification(false, armed);
        }
    }

    Y_UNIT_TEST(TombstoneChainUnlinkNotifiesWithHeldPayloads) {
        for (bool armed : { false, true }) {
            CheckUnlinkNotification(true, armed);
        }
    }

    void CheckReadyPublicationNotification(bool armBeforeReady) {
        TFixture fixture(6, 8, 7);
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        const auto collection = MakeCollectionCacheItem(251, 50051);
        const ui32 splitBit = static_cast<ui32>(CurrentBucketCount(*fixture.Space));
        const auto key = ReviewKeyForBucket(collection, (splitBit << 1) - 1, 0, 700000);
        TTestCachePageRef held;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
        TTestCachePageRef hit;
        TTestPageFetch fetch;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(key.Word(1)),
                        EStickyState::None, waiter, hit, fetch) == ECacheResultStatus::Inserted);
        const auto page = fetch.CacheItem();
        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));

        struct TObservation {
            TSharedCacheGate Gate;
            TTestSharedCache* Cache = nullptr;
            TTestCachePageRef* Held = nullptr;
            TPageCacheItem Page;
            ui32 Notifications = 0;
            bool Published = false;
            bool Activated = false;
        } observation;

        observation.Cache = fixture.Cache.Get();
        observation.Held = &held;
        observation.Page = page;
        observation.Gate.Slots[0].Arm(ECacheHookPoint::BeforeReadyStatePublication, page.CacheItem());
        TTestTraits hooks{ &observation, [](void* context, ECacheHookPoint point, TCacheItem item) noexcept {
                              auto& observation = *static_cast<TObservation*>(context);
                              TSharedCacheGate::Invoke(&observation.Gate, point, item);
                              if (point == ECacheHookPoint::ResourcesAvailable) {
                                  ++observation.Notifications;
                                  const auto state =
                                      TCacheTestAccess::HandleState(*observation.Cache, observation.Page.Index());
                                  observation.Published = state.IsReady() && state.Refs() > 0 && *observation.Held &&
                                                          observation.Held->CacheItem() == observation.Page;
                                  observation.Activated =
                                      TCacheTestAccess::ActivateBucketSplitOrMerge(*observation.Cache);
                              }
                          } };
        TSharedCacheHookGuard hookGuard(*fixture.Cache, hooks);
        bool ready = false;
        TGateThread completer(observation.Gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            ready = fetch.MakeReady(MakePageData(page.Index()));
        });
        observation.Gate.Slots[0].Wait();
        const auto blocked = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(blocked.IsCompleting());
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforeReady) {
            fixture.Cache->ArmResourceWait(generation);
        }
        observation.Gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(ready && waiter->Ready() && held);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforeReady ? 1 : 0);
        if (!armBeforeReady) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published && observation.Activated);
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
    }

    Y_UNIT_TEST(ReadyPublicationNotifiesBlockedSplitWithHeldResult) {
        for (bool armed : { false, true }) {
            CheckReadyPublicationNotification(armed);
        }
    }

    void CheckStickyPublicationNotification(bool restore, bool armBeforePublication) {
        TFixture fixture;
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(253, 254, 255));
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        auto metadata = TCacheTestAccess::AcquireStructural(*fixture.Cache, collection.CacheItem());
        UNIT_ASSERT(metadata);
        const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        TTestCachePageRef held;
        UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, page, held));
        const auto ordinary = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(ordinary.IsHot() && ordinary.IsStickyNoneField());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));

        struct TObservation {
            TSharedCacheGate Gate;
            TTestSharedCache* Cache = nullptr;
            TPageCacheItem Page;
            TCollectionCacheItem Collection;
            bool Restore = false;
            ui32 Notifications = 0;
            bool Published = false;
        } observation;

        observation.Cache = fixture.Cache.Get();
        observation.Page = page;
        observation.Collection = collection;
        observation.Restore = restore;
        observation.Gate.Slots[0].Arm(restore ? ECacheHookPoint::BeforeStickyPageListLink
                                              : ECacheHookPoint::AfterStickyPageLinked, page.CacheItem());
        TTestTraits hooks{ &observation,
            [](void* context, ECacheHookPoint point, TCacheItem item) noexcept {
                auto& observation = *static_cast<TObservation*>(context);
                TSharedCacheGate::Invoke(&observation.Gate, point, item);
                if (point == ECacheHookPoint::ResourcesAvailable) {
                    ++observation.Notifications;
                    const auto state = TCacheTestAccess::HandleState(*observation.Cache, observation.Page.Index());
                    observation.Published =
                        state.Refs() > 0 &&
                        TCacheTestAccess::HandleState(*observation.Cache, observation.Collection.Index()).Refs() > 0;
                    if (observation.Restore) {
                        observation.Published &=
                            state.IsHot() && state.IsStickyNoneField() && observation.Cache->StickyOwnedBytes() == 0 &&
                            TCacheTestAccess::NextInOwner(*observation.Cache, observation.Page.Index()) == 0;
                    } else {
                        observation.Published &= state.IsSticky() && state.IsStickyField() &&
                                                 TCacheTestAccess::StickyPageListHead(*observation.Cache,
                                                     observation.Collection) == observation.Page.Index();
                    }
                }
            } };
        TSharedCacheHookGuard hookGuard(*fixture.Cache, hooks);
        bool promoted = false;
        TGateThread promoter(observation.Gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            promoted = TCacheTestAccess::MakePageSticky(*fixture.Cache, page);
        });
        observation.Gate.Slots[0].Wait();
        if (restore) {
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        }
        const auto blocked = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(blocked.IsHot() && blocked.IsStickyField());
        UNIT_ASSERT(!TCacheTestAccess::RelocatePage(*fixture.Cache, page));
        UNIT_ASSERT(!TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        observation.Gate.Slots[0].Release();
        promoter.Join();
        UNIT_ASSERT_VALUES_EQUAL(promoted, !restore);
        UNIT_ASSERT(held && metadata);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforePublication ? 1 : 0);
        if (!armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published);
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        if (!restore) {
            UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*fixture.Cache, page, 0));
        }
        UNIT_ASSERT(TCacheTestAccess::RelocatePage(*fixture.Cache, page));
    }

    Y_UNIT_TEST(StickyPromotionPublicationNotifiesWithHeldPageAndMetadata) {
        for (bool armed : { false, true }) {
            CheckStickyPublicationNotification(false, armed);
        }
    }

    Y_UNIT_TEST(StickyPromotionRestorationNotifiesWithHeldPageAndMetadata) {
        for (bool armed : { false, true }) {
            CheckStickyPublicationNotification(true, armed);
        }
    }

    void CheckStickyNoopNotification(bool pending) {
        TFixture fixture;
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(253, 254, 256));
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        auto metadata = TCacheTestAccess::AcquireStructural(*fixture.Cache, collection.CacheItem());
        UNIT_ASSERT(metadata);
        const auto key = TCacheKey::Page(collection, 1);
        const auto page = pending ? AllocatePage(*fixture.Cache, key, 4096) : InsertReadyPage(*fixture.Cache, key);
        TTestCachePageRef held;
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));
        if (!pending) {
            UNIT_ASSERT(TCacheTestAccess::AcquirePage(*fixture.Cache, page, held));
            UNIT_ASSERT(TCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        }
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            UNIT_ASSERT(pending ? TCacheTestAccess::MakePendingPageSticky(*fixture.Cache, page)
                                : TCacheTestAccess::MakePageSticky(*fixture.Cache, page));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        }
        fixture.Cache->DisarmResourceWait();
        if (pending) {
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        }
    }

    Y_UNIT_TEST(AlreadyStickyHitDoesNotNotify) {
        CheckStickyNoopNotification(false);
    }

    Y_UNIT_TEST(PendingStickyHitDoesNotNotify) {
        CheckStickyNoopNotification(true);
    }

    void CheckMarkerReleaseNotification(bool grow, bool armBeforeRelease, bool finalRelease) {
        TFixture fixture(6, 8, grow ? 7 : 6);
        TTestCacheItemRef firstHelper;
        TTestCacheItemRef secondHelper;

        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            TTestCacheItemRef* LateHelper = nullptr;
            TCacheItem Marker;
            ui32 Notifications = 0;
            bool FinalRelease = false;
            bool CaptureLateHelper = false;
            bool Published = true;
            bool Reused = false;
        } observation;

        Y_DEFER {
            TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
        };
        TTransition growth;
        if (grow) {
            UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, grow ? 7 : 5));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        if (!grow) {
            UNIT_ASSERT(TCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        }
        const auto markerState = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        observation.Cache = fixture.Cache.Get();
        observation.LateHelper = &firstHelper;
        observation.Marker = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);
        observation.FinalRelease = finalRelease;
        observation.CaptureLateHelper = finalRelease;
        TTestTraits hooks{ &observation,
            [](void* context, ECacheHookPoint point, TCacheItem item) noexcept {
                auto& observation = *static_cast<TObservation*>(context);
                if (point == ECacheHookPoint::AfterBucketSplitOwnerCheck && observation.CaptureLateHelper) {
                    observation.CaptureLateHelper = false;
                    *observation.LateHelper = TCacheTestAccess::AcquireTombstoneHelper(*observation.Cache, item);
                } else if (point == ECacheHookPoint::ResourcesAvailable) {
                    ++observation.Notifications;
                    const auto state = TCacheTestAccess::HandleState(*observation.Cache, ResizeMarkerIndex);
                    if (observation.FinalRelease) {
                        observation.Published &= state.IsFree() && state.Refs() == 0 &&
                                                 state.Version() == AdvanceItemVersion(observation.Marker.Version());
                        observation.Reused = TCacheTestAccess::PrepareBucketSplit(*observation.Cache);
                    } else {
                        observation.Published &=
                            state.IsTombstone() && state.Refs() == 1 && observation.Marker.Matches(state);
                    }
                }
            } };
        TCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        if (finalRelease) {
            UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
            UNIT_ASSERT(firstHelper);
            AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
            UNIT_ASSERT(TCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        } else {
            firstHelper = TCacheTestAccess::AcquireStructural(*fixture.Cache, observation.Marker);
            secondHelper = TCacheTestAccess::AcquireStructural(*fixture.Cache, observation.Marker);
            UNIT_ASSERT(firstHelper && secondHelper);
            UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
            const auto blocked = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
            UNIT_ASSERT(blocked.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(blocked.Refs(), 3);
        }
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforeRelease) {
            fixture.Cache->ArmResourceWait(generation);
        }
        firstHelper.Drop();
        if (!finalRelease) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
            UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 0);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex).Refs(), 2);
            secondHelper.Drop();
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforeRelease ? 1 : 0);
        if (!armBeforeRelease) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published);
        if (finalRelease) {
            UNIT_ASSERT(observation.Reused);
            const auto reused = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
            UNIT_ASSERT(reused.IsBucketSplit());
            UNIT_ASSERT_VALUES_EQUAL(reused.Refs(), 1);
            UNIT_ASSERT_VALUES_EQUAL(reused.Version(), AdvanceItemVersion(observation.Marker.Version()));
        }
        TCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        if (!finalRelease) {
            UNIT_ASSERT(TCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
        }
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        if (grow) {
            CommitTransition(*fixture.Cache, growth);
        }
    }

    Y_UNIT_TEST(GrowthMarkerSoleOwnerReleaseNotifiesAdmissionWaiter) {
        for (bool armed : { false, true }) {
            CheckMarkerReleaseNotification(true, armed, false);
        }
    }

    Y_UNIT_TEST(ShrinkMarkerSoleOwnerReleaseNotifiesAdmissionWaiter) {
        for (bool armed : { false, true }) {
            CheckMarkerReleaseNotification(false, armed, false);
        }
    }

    Y_UNIT_TEST(GrowthRetiredMarkerReleaseNotifiesBeforeReuse) {
        for (bool armed : { false, true }) {
            CheckMarkerReleaseNotification(true, armed, true);
        }
    }

    Y_UNIT_TEST(ShrinkRetiredMarkerReleaseNotifiesBeforeReuse) {
        for (bool armed : { false, true }) {
            CheckMarkerReleaseNotification(false, armed, true);
        }
    }

    Y_UNIT_TEST(BucketSplitHelperReleaseDoesNotNotify) {
        TFixture fixture;
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 5));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        const auto state = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        auto helper =
            TCacheTestAccess::AcquireStructural(*fixture.Cache, TCacheItem::Make(state.Version(), ResizeMarkerIndex));
        UNIT_ASSERT(helper);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        helper.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        fixture.Cache->DisarmResourceWait();
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
    }

    Y_UNIT_TEST(OrdinaryTombstoneSoleOwnerReleaseDoesNotNotify) {
        TFixture fixture;
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(241, 242), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        auto cacheOp = fixture.Cache->BeginOperation();
        TOperationItemRef<TTestTraits> held(cacheOp, cacheOp.TryAcquireStructural(page.CacheItem()));
        UNIT_ASSERT(held);
        TOperationItemRef<TTestTraits> owner(cacheOp);
        auto expected = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(cacheOp.TryBeginErase(page.CacheItem(), expected, owner.Get()));
        cacheOp.FinishErase(owner.CacheItem(), key);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 2);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        owner.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
        fixture.Cache->DisarmResourceWait();
        held.Drop();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
    }

    void ReviewMarkerRetirementWaitsForHelper(bool grow, bool helperFirst) {
        TCacheCapacityConfig reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(reserved.Limit));
        const ui32 splitBit = static_cast<ui32>(SharedCacheBucketCount(grow ? 6 : 5));
        const auto key = ReviewKeyForBucket(MakeCollectionCacheItem(119, 50019), (splitBit << 1) - 1, splitBit, 500000);
        const auto survivingKey = ReviewKeyForBucket(PageCollection(key), (splitBit << 1) - 1, splitBit, 600000, &key);
        const auto successor = InsertReadyPage(*fixture.Cache, key);
        const auto survivor = InsertReadyPage(*fixture.Cache, survivingKey);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, successor));

        TTransition growth;
        if (grow) {
            UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, grow ? 7 : 5));
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        if (!grow) {
            UNIT_ASSERT(TCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        }
        const auto markerState = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        const auto marker = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterBucketSplitOwnerCheck, marker);
        gate.Slots[1].Arm(ECacheHookPoint::AfterTombstoneHelperClaim, marker);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool removed = false;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TCacheTestAccess::RemoveBucketSplit(*cache);
        });
        gate.Slots[0].Wait();
        bool erased = false;
        TGateThread eraser(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            erased = TCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, successor.Index()));
        });
        gate.Slots[1].Wait();
        const auto heldMarker = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        UNIT_ASSERT(heldMarker.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(heldMarker.Refs(), 2);
        if (helperFirst) {
            gate.Slots[1].Release();
            eraser.Join();
            UNIT_ASSERT(erased);
            AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, successor.Index()).IsFree());
            gate.Slots[0].Release();
            remover.Join();
            UNIT_ASSERT(removed);
        } else {
            gate.Slots[0].Release();
            remover.Join();
            UNIT_ASSERT(removed);
            AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
            UNIT_ASSERT(TCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::Prepared);
            UNIT_ASSERT(!TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
            AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
            gate.Slots[1].Release();
            eraser.Join();
            UNIT_ASSERT(erased);
        }
        if (grow) {
            UNIT_ASSERT(TCacheTestAccess::BucketHead(*fixture.Cache, 0).IsNull());
            UNIT_ASSERT(TCacheTestAccess::BucketHead(*fixture.Cache, splitBit) == survivor.CacheItem());
        } else {
            UNIT_ASSERT(TCacheTestAccess::BucketHead(*fixture.Cache, 0) == survivor.CacheItem());
        }
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex).IsFree());
        UNIT_ASSERT(TCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ECacheResultStatus::Miss);
        AssertPageHit(*fixture.Cache, survivingKey, survivor);
        if (grow) {
            CommitTransition(*fixture.Cache, growth);
        }
    }

    Y_UNIT_TEST(ReviewR18GrowthRetirementWaitsForLateMarkerHelper) {
        ReviewMarkerRetirementWaitsForHelper(true, false);
    }

    Y_UNIT_TEST(ReviewR18ShrinkRetirementWaitsForLateMarkerHelper) {
        ReviewMarkerRetirementWaitsForHelper(false, false);
    }

    Y_UNIT_TEST(ReviewR18GrowthRetirementFinishesAfterMarkerHelper) {
        ReviewMarkerRetirementWaitsForHelper(true, true);
    }

    Y_UNIT_TEST(ReviewR18ShrinkRetirementFinishesAfterMarkerHelper) {
        ReviewMarkerRetirementWaitsForHelper(false, true);
    }

    Y_UNIT_TEST(ConcurrentGrowthPublicationPoints) {
        TFixture fixture(6, 8, 7);
        const auto keys = MakeResizeKeys(*fixture.Space, MakeCollectionCacheItem(8, 12), 600);
        const TPageCacheItem low = InsertReadyPage(*fixture.Cache, keys[0]);
        const TPageCacheItem high = InsertReadyPage(*fixture.Cache, keys[2]);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterGrowthBucketPublished);
        gate.Slots[1].Arm(ECacheHookPoint::AfterBucketSplitActivated);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;

        bool firstActivation = true;
        TGateThread firstActivator(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            firstActivation = TCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[0].Wait();
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[2], high);

        const TPageCacheItem inserted = InsertReadyPage(*fixture.Cache, keys[1]);
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        gate.Slots[0].Release();
        firstActivator.Join();
        UNIT_ASSERT(firstActivation);
        UNIT_ASSERT(TCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::Prepared);

        bool activated = false;
        TGateThread activator(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            activated = TCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[1].Wait();
        UNIT_ASSERT(TCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::Prepared);
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertPageHit(*fixture.Cache, keys[2], high);
        gate.Slots[1].Release();
        activator.Join();
        UNIT_ASSERT(activated);

        const ui32 splitBit = TCacheTestAccess::BucketResizeState(*fixture.Space).SplitBit();
        TMaybe<TCacheKey> futureKey;
        for (ui64 offset = 30000; !futureKey && offset < 30000 + (1 << 20); ++offset) {
            const TCacheKey key = TCacheKey::Page(PageCollection(keys[0]), offset);
            const ui32 hash = key.Hash();
            if ((hash & (splitBit - 1)) == 1 && (hash & splitBit) != 0) {
                futureKey = key;
            }
        }
        UNIT_ASSERT(futureKey);
        const TPageCacheItem future = InsertReadyPage(*fixture.Cache, *futureKey);
        UNIT_ASSERT(TCacheTestAccess::BucketHead(*fixture.Cache, 1 | splitBit).IsNull());
        AssertPageHit(*fixture.Cache, *futureKey, future);

        gate.Slots[0].Arm(ECacheHookPoint::AfterBucketResizeCursorPublished);
        bool published = false;
        TGateThread publisher(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            published = TCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::CursorPublished);
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertPageHit(*fixture.Cache, keys[2], high);
        AssertPageHit(*fixture.Cache, *futureKey, future);
        gate.Slots[0].Release();
        publisher.Join();
        UNIT_ASSERT(published);

        gate.Slots[0].Arm(ECacheHookPoint::AfterBucketSplitRemoved);
        bool removed = false;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[0].Wait();
        AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertPageHit(*fixture.Cache, keys[2], high);
        AssertPageHit(*fixture.Cache, *futureKey, future);
        gate.Slots[0].Release();
        remover.Join();
        UNIT_ASSERT(removed);

        const THandleState markerState = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        UNIT_ASSERT(markerState.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(markerState.Refs(), 0);
        AssertPageUnreferenced(*fixture.Cache, low);
        AssertPageUnreferenced(*fixture.Cache, inserted);
        AssertPageUnreferenced(*fixture.Cache, high);
        AssertPageUnreferenced(*fixture.Cache, future);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 4 * pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 4 * pageBytes));

        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(!TCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing());
        AssertPageHit(*fixture.Cache, *futureKey, future);
        CommitTransition(*fixture.Cache, growth);
    }
    Y_UNIT_TEST(ConcurrentShrinkPublicationPoints) {
        TFixture fixture(6, 8, 7);
        const auto keys = MakeResizeKeys(*fixture.Space, MakeCollectionCacheItem(9, 13), 700);
        const TPageCacheItem low = InsertReadyPage(*fixture.Cache, keys[0]);
        const TPageCacheItem high = InsertReadyPage(*fixture.Cache, keys[2]);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;

        TTransition growth;
        UNIT_ASSERT(TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);

        TTransition shrink;
        UNIT_ASSERT(
            TCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        UNIT_ASSERT(TCacheTestAccess::AdvanceBucketResize(*fixture.Cache));
        UNIT_ASSERT(TCacheTestAccess::AdvanceBucketResize(*fixture.Cache));

        const TBucketResizeState resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
        const ui32 highBucket = resize.SplitBit();
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterShrinkBucketClosed);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;

        bool closed = false;
        TGateThread closer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            closed = TCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TCacheTestAccess::BucketHead(*fixture.Cache, highBucket).IsClosedBucketHead());
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[2], high);

        const TPageCacheItem inserted = InsertReadyPage(*fixture.Cache, keys[1]);
        TPageCacheItem duplicate;
        TTestCachePageRef existing;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(keys[1]), MakePageLocation(keys[1].Word(1)),
                        EStickyState::None, duplicate, existing) == ECacheResultStatus::Hit);
        UNIT_ASSERT(existing.CacheItem() == inserted);
        existing.Drop();
        UNIT_ASSERT(TCacheTestAccess::BucketHead(*fixture.Cache, highBucket).IsClosedBucketHead());
        gate.Slots[0].Release();
        closer.Join();
        UNIT_ASSERT(closed);

        gate.Slots[0].Arm(ECacheHookPoint::AfterBucketSplitRemoved);
        bool removed = false;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[0].Wait();
        AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertPageHit(*fixture.Cache, keys[2], high);
        gate.Slots[0].Release();
        remover.Join();
        UNIT_ASSERT(removed);

        AssertPageUnreferenced(*fixture.Cache, low);
        AssertPageUnreferenced(*fixture.Cache, inserted);
        AssertPageUnreferenced(*fixture.Cache, high);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 3 * pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 3 * pageBytes));

        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(!TCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing());
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertPageHit(*fixture.Cache, keys[2], high);
        CommitTransition(*fixture.Cache, shrink);
    }
    Y_UNIT_TEST(RandomizedConcurrentBucketResize) {
        constexpr ui32 ReaderCount = 2;
        constexpr ui32 WriterCount = 2;
        constexpr ui32 StableKeyCount = 8;
        constexpr ui32 WriterKeyCount = 12;
        constexpr ui32 WriterLookupCount = 2000;
        constexpr ui32 ResizeCycleCount = 4;

        TFixture fixture(7, 8, 7);
        const TCollectionCacheItem collection = MakeCollectionCacheItem(10, 14);
        TVector<TCacheKey> stableKeys;
        TVector<TPageCacheItem> stablePages;
        stableKeys.reserve(StableKeyCount);
        stablePages.reserve(StableKeyCount);
        for (ui32 index = 0; index < StableKeyCount; ++index) {
            stableKeys.push_back(TCacheKey::Page(collection, 10000 + index));
            stablePages.push_back(InsertReadyPage(*fixture.Cache, stableKeys.back()));
        }

        TVector<TCacheKey> writerKeys;
        writerKeys.reserve(WriterCount * WriterKeyCount);
        for (ui32 index = 0; index < WriterCount * WriterKeyCount; ++index) {
            writerKeys.push_back(TCacheKey::Page(collection, 20000 + index));
        }
        TVector<TPageCacheItem> writerPages(WriterCount * WriterKeyCount);

        std::atomic<ui32> ready = 0;
        std::atomic<ui32> paused = 0;
        std::atomic<ui32> writersDone = 0;
        std::atomic<bool> start = false;
        std::atomic<bool> pause = false;
        std::atomic<bool> stop = false;
        std::atomic<bool> failed = false;
        std::atomic<ui32> failureCode = 0;
        std::array<std::thread, ReaderCount + WriterCount> threads;
        auto cache = fixture.Cache;
        const auto fail = [&](ui32 code) {
            ui32 expected = 0;
            failureCode.compare_exchange_strong(expected, code, std::memory_order_relaxed);
            failed.store(true, std::memory_order_release);
        };

        for (ui32 reader = 0; reader < ReaderCount; ++reader) {
            threads[reader] = std::thread([&, reader] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, reader);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                ui64 random = 0x9E3779B97F4A7C15ULL + reader;
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                while (!stop.load(std::memory_order_acquire)) {
                    if (pause.load(std::memory_order_acquire)) {
                        paused.fetch_add(1, std::memory_order_acq_rel);
                        while (pause.load(std::memory_order_acquire) && !stop.load(std::memory_order_acquire)) {
                            std::this_thread::yield();
                        }
                        paused.fetch_sub(1, std::memory_order_acq_rel);
                        continue;
                    }
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    const ui32 index = static_cast<ui32>(random % stableKeys.size());
                    TTestCachePageRef ref;
                    const ECacheResultStatus status =
                        cache->Find(PageCollection(stableKeys[index]), stableKeys[index].Word(1), ref);
                    if (status != ECacheResultStatus::Hit || !ref || ref.CacheItem() != stablePages[index]) {
                        fail(100 + static_cast<ui32>(status));
                    }
                    ref.Drop();
                }
            });
        }

        for (ui32 writer = 0; writer < WriterCount; ++writer) {
            threads[ReaderCount + writer] = std::thread([&, writer] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard =
                    &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, ReaderCount + writer);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                ui64 random = 0xD1B54A32D192ED03ULL + writer;
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                const auto waitForResume = [&] {
                    if (!pause.load(std::memory_order_acquire)) {
                        return !stop.load(std::memory_order_acquire);
                    }
                    paused.fetch_add(1, std::memory_order_acq_rel);
                    while (pause.load(std::memory_order_acquire) && !stop.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    paused.fetch_sub(1, std::memory_order_acq_rel);
                    return !stop.load(std::memory_order_acquire);
                };

                const ui32 begin = writer * WriterKeyCount;
                const ui32 end = begin + WriterKeyCount;
                for (ui32 index = begin; index < end && !failed.load(std::memory_order_acquire) && waitForResume();
                     ++index)
                {
                    const TCacheKey& key = writerKeys[index];
                    TPageCacheItem page;
                    TTestCachePageRef hit;
                    const ECacheResultStatus status = FindOrInsertPage(
                        *cache, PageCollection(key), MakePageLocation(key.Word(1)), EStickyState::None, page, hit);
                    if (status != ECacheResultStatus::Inserted || !page) {
                        fail(200 + static_cast<ui32>(status));
                        break;
                    }
                    if (!cache->MakeReady(page, MakePageData(page.Index()))) {
                        fail(300);
                        break;
                    }
                    writerPages[index] = page;
                }

                for (ui32 attempt = 0;
                     attempt < WriterLookupCount && !failed.load(std::memory_order_acquire) && waitForResume();
                     ++attempt)
                {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    const ui32 index = begin + static_cast<ui32>(random % WriterKeyCount);
                    const TCacheKey& key = writerKeys[index];
                    TPageCacheItem duplicate;
                    TTestCachePageRef hit;
                    const ECacheResultStatus status = FindOrInsertPage(
                        *cache, PageCollection(key), MakePageLocation(key.Word(1)), EStickyState::None, duplicate, hit);
                    if (status != ECacheResultStatus::Hit || !hit || hit.CacheItem() != writerPages[index])
                    {
                        fail(400 + static_cast<ui32>(status));
                    }
                    hit.Drop();
                }
                writersDone.fetch_add(1, std::memory_order_release);
                while (!stop.load(std::memory_order_acquire)) {
                    waitForResume();
                    std::this_thread::yield();
                }
            });
        }

        bool ownerFailed = false;
        TString ownerError;
        const TInstant readyDeadline = TInstant::Now() + TDuration::Seconds(10);
        while (ready.load(std::memory_order_acquire) != threads.size()) {
            if (TInstant::Now() >= readyDeadline) {
                ownerFailed = true;
                ownerError = "workers did not start";
                break;
            }
            std::this_thread::yield();
        }
        start.store(true, std::memory_order_release);

        for (ui32 cycle = 0; cycle < ResizeCycleCount && !ownerFailed && !failed.load(std::memory_order_acquire);
             ++cycle)
        {
            if (cycle != 0) {
                pause.store(true, std::memory_order_release);
                const TInstant pauseDeadline = TInstant::Now() + TDuration::Seconds(10);
                while (paused.load(std::memory_order_acquire) != threads.size()) {
                    if (TInstant::Now() >= pauseDeadline) {
                        ownerFailed = true;
                        ownerError = TStringBuilder() << "workers did not quiesce before cycle " << cycle;
                        break;
                    }
                    std::this_thread::yield();
                }
                if (ownerFailed) {
                    break;
                }
            }
            const ui8 targetAddressBits = cycle % 2 == 0 ? 6 : 7;
            if (!TCacheTestAccess::BeginBucketResize(*fixture.Cache, targetAddressBits)) {
                ownerFailed = true;
                ownerError = TStringBuilder()
                             << "cannot begin cycle " << cycle << " target " << static_cast<ui32>(targetAddressBits);
                break;
            }
            pause.store(false, std::memory_order_release);
            constexpr ui32 MaxResizeSteps = 1 << 16;
            ui32 step = 0;
            while (TCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing() && step++ < MaxResizeSteps) {
                if (!TCacheTestAccess::AdvanceBucketResize(*fixture.Cache)) {
                    std::this_thread::yield();
                }
            }
            if (TCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing() ||
                CurrentBucketCount(*fixture.Space) != SharedCacheBucketCount(targetAddressBits))
            {
                ownerFailed = true;
                if (TCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing()) {
                    const TBucketResizeState resize = TCacheTestAccess::BucketResizeState(*fixture.Space);
                    const THandleState markerState = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
                    const ui64 markerNext = TCacheTestAccess::HandleNext(*fixture.Cache, ResizeMarkerIndex);
                    const ui32 bucket = resize.Cursor();
                    const ui32 peer = bucket | resize.SplitBit();
                    ownerError = TStringBuilder()
                                 << "step bound at cycle " << cycle << " direction "
                                 << static_cast<i32>(resize.Direction()) << " phase "
                                 << static_cast<ui32>(resize.Phase()) << " cursor " << resize.Cursor()
                                 << " marker-state " << static_cast<ui32>(markerState.State()) << " marker-refs "
                                 << markerState.Refs() << " marker-next " << markerNext << " bucket-head "
                                 << TCacheTestAccess::BucketHead(*fixture.Cache, bucket).Raw() << " peer-head "
                                 << TCacheTestAccess::BucketHead(*fixture.Cache, peer).Raw();
                } else {
                    ownerError = TStringBuilder() << "wrong bucket count after cycle " << cycle;
                }
            }
        }
        pause.store(false, std::memory_order_release);

        const TInstant writersDeadline = TInstant::Now() + TDuration::Seconds(10);
        while (!ownerFailed && !failed.load(std::memory_order_acquire) &&
               writersDone.load(std::memory_order_acquire) != WriterCount)
        {
            if (TInstant::Now() >= writersDeadline) {
                ownerFailed = true;
                ownerError = "writers did not finish";
                break;
            }
            std::this_thread::yield();
        }
        stop.store(true, std::memory_order_release);
        pause.store(false, std::memory_order_release);
        for (std::thread& thread : threads) {
            thread.join();
        }

        for (ui32 step = 0; TCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing() && step < (1 << 16);
             ++step) {
            TCacheTestAccess::AdvanceBucketResize(*fixture.Cache);
        }
        UNIT_ASSERT_C(!ownerFailed, ownerError);
        UNIT_ASSERT_C(
            !failed.load(std::memory_order_acquire), TStringBuilder() << "failure code " << failureCode.load());
        UNIT_ASSERT(!TCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing());
        UNIT_ASSERT_VALUES_EQUAL(CurrentBucketCount(*fixture.Space), SharedCacheBucketCount(7));

        for (ui32 index = 0; index < StableKeyCount; ++index) {
            AssertPageHit(*fixture.Cache, stableKeys[index], stablePages[index]);
            AssertPageUnreferenced(*fixture.Cache, stablePages[index]);
        }
        for (ui32 index = 0; index < writerKeys.size(); ++index) {
            AssertPageHit(*fixture.Cache, writerKeys[index], writerPages[index]);
            AssertPageUnreferenced(*fixture.Cache, writerPages[index]);
        }

        const ui64 itemCount = StableKeyCount + writerKeys.size();
        ui64 hotCount = 0;
        for (ui32 index = 2; index < TCacheTestAccess::HandleCount(*fixture.Cache); ++index) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, index);
            if (state.IsHot()) {
                ++hotCount;
                UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(hotCount, itemCount);
        UNIT_ASSERT_VALUES_EQUAL(
            AvailableFreeHandles(*fixture.Cache), TCacheTestAccess::HandleCount(*fixture.Cache) - 2 - itemCount);

        const ui64 expectedBytes = itemCount * (4096 + NActors::TSharedData::OverheadSize);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), expectedBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), expectedBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, expectedBytes));

        const THandleState markerState = TCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        UNIT_ASSERT(markerState.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(markerState.Refs(), 0);
        for (ui32 bucket = 0; bucket < CurrentBucketCount(*fixture.Space); ++bucket) {
            UNIT_ASSERT(!TCacheTestAccess::BucketHead(*fixture.Cache, bucket).IsClosedBucketHead());
        }
    }
    Y_UNIT_TEST(DiscardCandidateDeletesTypedPayload) {
        TFixture fixture;
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;

        const auto pageKey = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 101);
        const auto page =
            AllocatePage(*fixture.Cache, pageKey, 4096, NTable::NPage::EPage::DataPage, 123, EStickyState::None);
        UNIT_ASSERT(page);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::PageType(*fixture.Cache, page.Index()), NTable::NPage::EPage::DataPage);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::PageCrc32(*fixture.Cache, page.Index()), 123);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        TCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index(), std::memory_order_relaxed).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 0));

        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(4, 5, 6));
        constexpr size_t BackingSize = 317;
        const ui64 collectionBytes = 0;
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey, BackingSize);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReadyUnregistered(
            *fixture.Cache, collection, MakeCollection(CollectionId(collectionKey), BackingSize)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), collectionBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), collectionBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), collectionBytes);

        TCacheTestAccess::DiscardCandidate(*fixture.Cache, collection);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index()), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 0));
    }
    Y_UNIT_TEST(StickyPageListContainsOnlyStickyPages) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(7, 8, 9));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto regularKey = TCacheKey::Page(collection, 1);
        const TPageCacheItem regular = AllocatePage(*fixture.Cache, regularKey, 4096);
        UNIT_ASSERT(regular);
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakePageData(regular.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, regular.Index()), 0);

        const auto firstKey = TCacheKey::Page(collection, 2);
        const TPageCacheItem first =
            AllocatePage(*fixture.Cache, firstKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(first);
        UNIT_ASSERT(fixture.Cache->MakeReady(first, MakePageData(first.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);

        const auto secondKey = TCacheKey::Page(collection, 3);
        const TPageCacheItem second =
            AllocatePage(*fixture.Cache, secondKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(second);
        UNIT_ASSERT(fixture.Cache->MakeReady(second, MakePageData(second.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), second.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), first.Index());

        UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*fixture.Cache, second, second.Index()));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);
        const THandleState secondState = TCacheTestAccess::HandleState(*fixture.Cache, second.Index());
        UNIT_ASSERT(secondState.IsSticky());
        UNIT_ASSERT(secondState.IsTransitionField());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), 0);

        UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*fixture.Cache, first, first.Index()));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        const THandleState firstState = TCacheTestAccess::HandleState(*fixture.Cache, first.Index());
        UNIT_ASSERT(firstState.IsSticky());
        UNIT_ASSERT(firstState.IsTransitionField());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);
    }
    Y_UNIT_TEST(DetachedCollectionRejectsStickyReplacementWithoutLeakingOwner) {
        TFixture fixture;
        const TCollectionLocation collectionLocation{ .Id = TLogoBlobID(7, 8, 10) };
        TCollectionCacheItem collection;
        TTestCacheCollectionRef collectionRef;
        UNIT_ASSERT(TCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, collectionLocation, collection,
                        collectionRef) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(collectionLocation.Id)));

        TPageCacheItem page;
        TTestCachePageRef pageHit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(1), EStickyState::Sticky, page,
                        pageHit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*fixture.Cache, page, page.Index()));
        const THandleState sourceState = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(sourceState.IsSticky() && sourceState.IsTransitionField());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(TCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        collectionRef.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        const ui64 residentBytes = fixture.Cache->ResidentBytes();
        UNIT_ASSERT(!TCacheTestAccess::RelocatePage(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
    }
    Y_UNIT_TEST(ReadyPageCanBecomeStickyAndDetach) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(16, 17, 18));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto key = TCacheKey::Page(collection, 1);
        const TPageCacheItem page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);

        UNIT_ASSERT(TCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), page.Index());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT(TCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pageBytes);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
    }
    Y_UNIT_TEST(StickyPromotionLosesToCollectionDisable) {
        constexpr ui32 size = 64;
        constexpr ui64 bytes = size + TSharedData::OverheadSize;
        for (auto sourceTier : { ECacheTier::Regular, ECacheTier::Keep }) {
            for (auto mode : { ECacheMode::Regular, ECacheMode::TryKeepInMemory }) {
                TFixture fixture;
                auto& cache = *fixture.Cache;
                UNIT_ASSERT(ConfigureKeep(cache, 4 * bytes));
                const auto collection = CreateModeCollection(cache, fixture.Registry, TLogoBlobID(19, 20, 21),
                    sourceTier == ECacheTier::Keep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular);
                TTestCachePageRef initialOwner;
                const auto page = InsertOwnedSizedPage(cache, collection, 1, size, 's', initialOwner);
                initialOwner.Drop();
                UNIT_ASSERT(TCacheTestAccess::HandleState(cache, page.Index()).IsHotInTier(sourceTier));
                if (sourceTier == ECacheTier::Keep) {
                    UNIT_ASSERT(cache.SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
                }
                // Leave mode sweeps queued so the promotion starts from the original Hot tier
                UNIT_ASSERT(cache.SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));

                TSharedCacheGate gate;
                gate.Slots[0].Arm(ECacheHookPoint::AfterStickyPageLinked, page.CacheItem());
                TSharedCacheHookGuard hookGuard(cache, gate.Hooks);
                bool promoted = true;
                TGateThread promoter(gate, [&] {
                    Y_ABORT_UNLESS(!TlsCacheHazard);
                    TlsCacheHazard = &TCacheThreadTestAccess::Hazard(cache, 0);
                    Y_DEFER {
                        TlsCacheHazard = nullptr;
                    };
                    promoted = TCacheTestAccess::MakePageSticky(cache, page);
                });
                gate.Slots[0].Wait();
                UNIT_ASSERT_VALUES_EQUAL(cache.StickyOwnedBytes(), bytes);
                UNIT_ASSERT(cache.SetCollectionPagesCacheMode(collection, mode));
                UNIT_ASSERT_VALUES_EQUAL(cache.StickyOwnedBytes(), 0);
                gate.Slots[0].Release();
                promoter.Join();
                UNIT_ASSERT(!promoted);
                UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(cache, collection), 0);
                const auto finalTier = mode == ECacheMode::TryKeepInMemory ? ECacheTier::Keep : ECacheTier::Regular;
                UNIT_ASSERT(TCacheTestAccess::HandleState(cache, page.Index()).IsHotInTier(finalTier));
                for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                    const auto& usage = TCacheTestAccess::TierUsage(cache, tier);
                    UNIT_ASSERT_VALUES_EQUAL(usage.Hot.PagesEstimate(), tier == finalTier ? 1 : 0);
                    UNIT_ASSERT_VALUES_EQUAL(usage.Hot.BytesEstimate(), tier == finalTier ? bytes : 0);
                }
                UNIT_ASSERT_VALUES_EQUAL(cache.StickyBytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(cache.KeepOwnedPages(), finalTier == ECacheTier::Keep ? 1 : 0);
                UNIT_ASSERT_VALUES_EQUAL(cache.KeepOwnedBytes(), finalTier == ECacheTier::Keep ? bytes : 0);

                UNIT_ASSERT(cache.SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
                ui32 sweeps = 0;
                while (TCacheTestAccess::SweepModes(cache)) {
                    UNIT_ASSERT(++sweeps < 1024);
                }
                UNIT_ASSERT(TCacheTestAccess::HandleState(cache, page.Index()).IsHotInTier(ECacheTier::Regular));
                UNIT_ASSERT_VALUES_EQUAL(cache.KeepOwnedBytes(), 0);
                UNIT_ASSERT(TCacheTestAccess::EvictFromHot(cache, page));
                UNIT_ASSERT(TCacheTestAccess::EraseCold(cache, MakeColdItem(cache, page.Index())));
                for (auto tier : { ECacheTier::Regular, ECacheTier::Keep }) {
                    const auto& usage = TCacheTestAccess::TierUsage(cache, tier);
                    UNIT_ASSERT_VALUES_EQUAL(usage.Hot.PagesEstimate(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(usage.Hot.BytesEstimate(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(usage.Cold.PagesEstimate(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(usage.Cold.BytesEstimate(), 0);
                }
                UNIT_ASSERT_VALUES_EQUAL(cache.KeepOwnedPages(), 0);
                UNIT_ASSERT_VALUES_EQUAL(cache.StickyOwnedBytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(cache.PayloadUsage(), 0);
            }
        }
    }
    Y_UNIT_TEST(ConcurrentStickyPageInsertionPreservesBothPages) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(10, 11, 12));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        std::array<TPageCacheItem, 2> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            const auto key = TCacheKey::Page(collection, index);
            pages[index] =
                AllocatePage(*fixture.Cache, key, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::None);
            UNIT_ASSERT(pages[index]);
        }

        std::atomic<bool> start = false;
        std::array<bool, 2> ready{};
        std::array<std::thread, 2> workers;
        for (ui32 index = 0; index < workers.size(); ++index) {
            workers[index] = std::thread([&, index] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, index);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                UNIT_ASSERT(TCacheTestAccess::MakePendingPageSticky(*fixture.Cache, pages[index]));
                ready[index] = fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index()));
            });
        }
        start.store(true, std::memory_order_release);
        for (std::thread& worker : workers) {
            worker.join();
        }
        UNIT_ASSERT(ready[0] && ready[1]);

        const ui32 head = TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection);
        UNIT_ASSERT(head == pages[0].Index() || head == pages[1].Index());
        const ui32 tail = TCacheTestAccess::NextInOwner(*fixture.Cache, head);
        UNIT_ASSERT(tail == pages[0].Index() || tail == pages[1].Index());
        UNIT_ASSERT_VALUES_UNEQUAL(head, tail);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, tail), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).Refs(), 0);
    }
    Y_UNIT_TEST(ReviewR13ConcurrentStickyAdmissionTracksAllCharges) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(13, 14, 15));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        std::array<TPageCacheItem, 2> pages;
        std::atomic<bool> start = false;
        std::array<std::thread, 2> workers;
        for (ui32 index = 0; index < workers.size(); ++index) {
            workers[index] = std::thread([&, index] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, index);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                pages[index] = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, index), 4096,
                    NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
            });
        }
        start.store(true, std::memory_order_release);
        for (std::thread& worker : workers) {
            worker.join();
        }

        ui32 stickyCount = 0;
        for (TPageCacheItem page : pages) {
            UNIT_ASSERT(page);
            stickyCount += TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsStickyField();
        }
        UNIT_ASSERT_VALUES_EQUAL(stickyCount, pages.size());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pages.size() * pageBytes);
        for (TPageCacheItem page : pages) {
            TCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(StickyWithdrawalAfterReadyAccountingRefundsBudgetOnce) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 106);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto page = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 0), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeReadyStatePublication, page.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        bool ready = false;
        TGateThread completer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            ready = fixture.Cache->MakeReady(page, MakePageData(page.Index()));
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(ready);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), bytes);
    }
    Y_UNIT_TEST(CollectionStickyDisableCoversReadyAndCompletingPages) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(12, 13, 14));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const TPageCacheItem ready = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        const TPageCacheItem completing = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(ready && completing);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 2 * pageBytes);
        UNIT_ASSERT(fixture.Cache->MakeReady(ready, MakePageData(ready.Index())));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforePageReadyState, completing.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool completed = false;
        TGateThread completer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            completed = cache->MakeReady(completing, MakePageData(completing.Index()));
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { ready, completing }) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsStickyNoneField());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, ready.Index()).IsHot());
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, completing.Index()).IsCompleting());

        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(completed);
        const THandleState completedState = TCacheTestAccess::HandleState(*fixture.Cache, completing.Index());
        UNIT_ASSERT(completedState.IsHot());
        UNIT_ASSERT(completedState.IsStickyNoneField());

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));
        const TPageCacheItem kept = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 3), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(kept);
        UNIT_ASSERT(fixture.Cache->MakeReady(kept, MakePageData(kept.Index())));
        const THandleState keptState = TCacheTestAccess::HandleState(*fixture.Cache, kept.Index());
        UNIT_ASSERT(keptState.IsSticky());
        UNIT_ASSERT(keptState.IsStickyField());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), kept.Index());
    }
    Y_UNIT_TEST(StickyPageLinkedAfterSinglePassDrain) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(31, 32, 33));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const TPageCacheItem existing = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        const TPageCacheItem pending = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(existing && pending);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeStickyPageListLink, pending.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool ready = false;
        TGateThread completer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            TCacheTestAccess::MakePendingPageSticky(*cache, pending);
            ready = cache->MakeReady(pending, MakePageData(pending.Index()));
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        for (TPageCacheItem page : { existing, pending }) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsHot());
            UNIT_ASSERT(state.IsStickyNoneField());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
    }
    Y_UNIT_TEST(StickyListDetachRetainsPendingInsertion) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(13, 14, 15));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto existingKey = TCacheKey::Page(collection, 1);
        const TPageCacheItem existing =
            AllocatePage(*fixture.Cache, existingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        const auto pendingKey = TCacheKey::Page(collection, 2);
        const TPageCacheItem pending =
            AllocatePage(*fixture.Cache, pendingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(pending);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterStickyPageLinked, pending.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool ready = false;
        TGateThread completer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            TCacheTestAccess::MakePendingPageSticky(*cache, pending);
            ready = cache->MakeReady(pending, MakePageData(pending.Index()));
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), pending.Index());
        const THandleState pendingState = TCacheTestAccess::HandleState(*fixture.Cache, pending.Index());
        UNIT_ASSERT(pendingState.IsPending());
        UNIT_ASSERT(pendingState.IsStickyField());
        const THandleState existingState = TCacheTestAccess::HandleState(*fixture.Cache, existing.Index());
        UNIT_ASSERT(existingState.IsSticky());
        UNIT_ASSERT(existingState.IsTransitionField());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, existing.Index()), 0);

        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(ready);
        UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { existing, pending }) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsTransitionField());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
    }
    Y_UNIT_TEST(StickyListDetachRetainsInFlightPromotion) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(16, 17, 19));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

        const auto existingKey = TCacheKey::Page(collection, 1);
        const TPageCacheItem existing =
            AllocatePage(*fixture.Cache, existingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        const auto promotingKey = TCacheKey::Page(collection, 2);
        const TPageCacheItem promoting = AllocatePage(*fixture.Cache, promotingKey, 4096);
        UNIT_ASSERT(promoting);
        UNIT_ASSERT(fixture.Cache->MakeReady(promoting, MakePageData(promoting.Index())));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, promoting.Index()).IsHot());

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterStickyPageLinked, promoting.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool promoted = false;
        TGateThread promoter(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            promoted = TCacheTestAccess::MakePageSticky(*cache, promoting);
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), promoting.Index());
        const THandleState promotingState = TCacheTestAccess::HandleState(*fixture.Cache, promoting.Index());
        UNIT_ASSERT(promotingState.IsHot());
        UNIT_ASSERT(promotingState.IsStickyField());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, promoting.Index()), 0);
        const THandleState existingState = TCacheTestAccess::HandleState(*fixture.Cache, existing.Index());
        UNIT_ASSERT(existingState.IsSticky());
        UNIT_ASSERT(existingState.IsTransitionField());

        gate.Slots[0].Release();
        promoter.Join();
        UNIT_ASSERT(promoted);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, promoting.Index()).IsSticky());

        UNIT_ASSERT(TCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { existing, promoting }) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsTransitionField());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
    }
    Y_UNIT_TEST(StickyListMergePreservesConcurrentInsertions) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(16, 17, 18));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto existingKey = TCacheKey::Page(collection, 1);
        const TPageCacheItem existing =
            AllocatePage(*fixture.Cache, existingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        const auto beforeExchangeKey = TCacheKey::Page(collection, 2);
        const TPageCacheItem beforeExchange = AllocatePage(
            *fixture.Cache, beforeExchangeKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(beforeExchange);
        const auto afterExchangeKey = TCacheKey::Page(collection, 3);
        const TPageCacheItem afterExchange =
            AllocatePage(*fixture.Cache, afterExchangeKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(afterExchange);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        gate.Slots[1].Arm(ECacheHookPoint::AfterStickyPageListHeadPublished, collection.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool cut = false;
        TGateThread cutter(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            cut = TCacheTestAccess::UnstickyCutPages(*cache, existing, fixture.Capacity.HandleCount());
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(TCacheTestAccess::MakePendingPageSticky(*cache, beforeExchange));
        UNIT_ASSERT(cache->MakeReady(beforeExchange, MakePageData(beforeExchange.Index())));
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), beforeExchange.Index());

        gate.Slots[0].Release();
        gate.Slots[1].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), existing.Index());

        UNIT_ASSERT(TCacheTestAccess::MakePendingPageSticky(*cache, afterExchange));
        UNIT_ASSERT(cache->MakeReady(afterExchange, MakePageData(afterExchange.Index())));
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), afterExchange.Index());

        gate.Slots[1].Release();
        cutter.Join();
        UNIT_ASSERT(cut);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), afterExchange.Index());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::NextInOwner(*fixture.Cache, afterExchange.Index()), existing.Index());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::NextInOwner(*fixture.Cache, existing.Index()), beforeExchange.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*fixture.Cache, beforeExchange.Index()), 0);
        for (TPageCacheItem page : { existing, beforeExchange, afterExchange }) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsStickyField());
        }
    }
    Y_UNIT_TEST(StickyMergeRetainsConcurrentPageWhenFetchFails) {
        TFixture fixture;
        const TLogoBlobID id(16, 17, 19);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(1), new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status() == ECacheResultStatus::Inserted);
        const TPageCacheItem pending = requests[0].Fetch().CacheItem();
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, pending.Index()).IsStickyField());

        const TPageCacheItem cutPage = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(cutPage);
        UNIT_ASSERT(fixture.Cache->MakeReady(cutPage, MakePageData(cutPage.Index())));
        const TPageCacheItem concurrent = AllocatePage(*fixture.Cache, TCacheKey::Page(collection, 3), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(concurrent);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        gate.Slots[1].Arm(ECacheHookPoint::AfterStickyPageListHeadPublished, collection.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool cut = false;
        TGateThread cutter(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            cut = TCacheTestAccess::UnstickyCutPages(*cache, cutPage, cutPage.Index());
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*cache, collection), 0);
        UNIT_ASSERT(TCacheTestAccess::MakePendingPageSticky(*cache, concurrent));
        UNIT_ASSERT(cache->MakeReady(concurrent, MakePageData(concurrent.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*cache, collection), concurrent.Index());

        gate.Slots[0].Release();
        gate.Slots[1].Wait();
        UNIT_ASSERT(requests[0].Fetch().FailReady());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*cache, collection), concurrent.Index());
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::NextInOwner(*cache, concurrent.Index()), 0);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, pending.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 0);

        gate.Slots[1].Release();
        cutter.Join();
        UNIT_ASSERT(cut);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*cache, collection), concurrent.Index());
        UNIT_ASSERT(cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::StickyPageListHead(*cache, collection), 0);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, concurrent.Index()).IsStickyNoneField());
    }

    Y_UNIT_TEST(ReheatOvertakesHotEvictionAccounting) {
        for (ECacheMode mode : { ECacheMode::Regular, ECacheMode::TryKeepInMemory }) {
            TFixture fixture;
            const TLogoBlobID id(37, 39, 43);
            const auto collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(collection);
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, mode));
            const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
            UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(2 * pageBytes));
            const auto page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
            AssertPageUnreferenced(*fixture.Cache, page);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 0);

            TSharedCacheGate gate;
            gate.Slots[0].Arm(ECacheHookPoint::AfterHotEvictionPublished, page.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            bool evicted = false;
            TGateThread evictor(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                evicted = TCacheTestAccess::EvictFromHot(*cache, page);
            });
            gate.Slots[0].Wait();
            const bool keep = mode == ECacheMode::TryKeepInMemory;
            UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, page.Index()).State() ==
                        (keep ? EHandleState::KeepCold : EHandleState::Cold));

            TTestCachePageRef held;
            UNIT_ASSERT(cache->Find(collection, 1, held) == ECacheResultStatus::Hit);
            UNIT_ASSERT(held.CacheItem() == page);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, page.Index()).IsHot());
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdPagesEstimate(*cache), keep ? 0 : -1);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::KeepColdPagesEstimate(*cache), keep ? -1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->HotPages(), 2);
            UNIT_ASSERT_VALUES_EQUAL(cache->ColdPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->KeepColdPages(), 0);

            gate.Slots[0].Release();
            evictor.Join();
            UNIT_ASSERT(evicted);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdPagesEstimate(*cache), 0);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::KeepColdPagesEstimate(*cache), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->HotPages(), 1);
            UNIT_ASSERT_VALUES_EQUAL(cache->ColdPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->KeepColdPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->StickyPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->HotBytes(), pageBytes);
            UNIT_ASSERT_VALUES_EQUAL(cache->ResidentBytes(), pageBytes);
            UNIT_ASSERT_VALUES_EQUAL(held.size(), 4096);
        }
    }
    Y_UNIT_TEST(HotEvictionRetainsOriginalPageThroughAccounting) {
        for (ECacheMode mode : { ECacheMode::Regular, ECacheMode::TryKeepInMemory }) {
            TFixture fixture;
            const TLogoBlobID id(37, 39, mode == ECacheMode::Regular ? 44 : 45);
            const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
            UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, mode));
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(3 * bytes));
            UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(bytes));
            const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
            TTestCachePageRef held;
            UNIT_ASSERT(fixture.Cache->Find(collection, 1, held) == ECacheResultStatus::Hit);
            TCollection* value = TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());
            UNIT_ASSERT_VALUES_EQUAL(value->ReadyPageBytes(), bytes);

            TSharedCacheGate gate;
            gate.Slots[0].Arm(ECacheHookPoint::AfterHotEvictionPublished, page.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            bool evicted = false;
            TGateThread evictor(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                evicted = TCacheTestAccess::EvictFromHot(*cache, page);
            });
            gate.Slots[0].Wait();
            UNIT_ASSERT(cache->UpdateSoftLimitOverride(0));
            held.Drop();
            const THandleState pinned = TCacheTestAccess::HandleState(*cache, page.Index());
            UNIT_ASSERT_VALUES_EQUAL(pinned.Version(), page.Version());
            UNIT_ASSERT(pinned.IsCold());
            UNIT_ASSERT_VALUES_EQUAL(pinned.Refs(), 1);
            UNIT_ASSERT_VALUES_EQUAL(value->ReadyPageBytes(), bytes);

            gate.Slots[0].Release();
            evictor.Join();
            UNIT_ASSERT(evicted);
            UNIT_ASSERT(TCacheTestAccess::HandleState(*cache, page.Index()).IsFree());
            UNIT_ASSERT_VALUES_EQUAL(value->ReadyPageBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->ResidentBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->RetainedBytes(), 0);
        }
    }
    Y_UNIT_TEST(HotEvictionPinQueuesInitialKeepColdEntry) {
        TFixture fixture;
        const TLogoBlobID id(37, 39, 46);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(2 * bytes));
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(state.IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ReadyPageBytes(), bytes);
    }
    Y_UNIT_TEST(StaleHotWordDoesNotReclassifyColdPage) {
        TFixture fixture;
        const TLogoBlobID id(37, 39, 47);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TCacheKey::Collection(id));
        UNIT_ASSERT(TCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(bytes));
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 1));
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        const ui64 retained = fixture.Cache->RetainedBytes();
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(2 * bytes));
        TCacheTestAccess::ProcessHotToken(*fixture.Cache, page);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), retained);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepOwnedBytes(), bytes);
    }
    Y_UNIT_TEST(ReorderedCategoryTransfersConverge) {
        TFixture fixture;
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 103);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        TCacheTestAccess::TransferColdToHot(*fixture.Cache, pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdBytesEstimate(*fixture.Cache), -static_cast<i64>(pageBytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), 2 * pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);

        TCacheTestAccess::TransferHotToCold(*fixture.Cache, pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::ColdBytesEstimate(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);

        TCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
    }
    Y_UNIT_TEST(ApplicationOwnerRetainsCacheAndSpace) {
        TFixture fixture;
        TIntrusivePtr<TTestSharedCache> applicationOwner = fixture.Cache;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 102);
        {
            const auto page = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(page);
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        }

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Ref);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache.RefCount(), 2);

        auto hold = std::move(found.Ref);
        fixture.Cache.Reset();
        hold.Drop();
        TlsCacheHazard = nullptr;
        applicationOwner.Reset();
    }
    Y_UNIT_TEST(UnboundDropAbandonsItemForFinalSweep) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 104);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, key);
        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);

        TlsCacheHazard = nullptr;
        found.Ref.Drop();

        TlsCacheHazard =
            &TCacheThreadTestAccess::Hazard(*fixture.Cache, TCacheTestAccess::HazardCount(*fixture.Cache) - 1);
        const THandleState abandoned = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(abandoned.Refs(), 1);
    }
    Y_UNIT_TEST(FinalTombstoneHoldStartsOuterOperation) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(1, 2), 103);
        TTestCacheItemRef hold;
        ui32 pageIndex = 0;
        ui32 pageVersion = 0;

        {
            const auto page = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(page);
            pageIndex = page.Index();
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);

            hold = TCacheTestAccess::AcquirePending(*fixture.Cache, key);

            pageVersion = TCacheTestAccess::WithHandle(*fixture.Cache, page.Index(), [](THandle& handle) {
                const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
                UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
                TPageFetchState* fetch = std::exchange(handle.Body.Fetch, nullptr);
                UNIT_ASSERT(fetch);
                fetch->UnRef();
                new (&handle.Body.PageBuffer) NActors::TSharedData::TBuffer();
                handle.State.store(
                    state.IncrementRefs().WithState(EHandleState::Tombstone).Raw(), std::memory_order_release);
                return state.Version();
            });
            TCacheTestAccess::FinishErase(*fixture.Cache, TCacheItem::Make(pageVersion, page.Index()), key);
            UNIT_ASSERT_VALUES_EQUAL(
                TCacheTestAccess::HandleState(*fixture.Cache, page.Index(), std::memory_order_relaxed).Refs(), 1);
        }

        hold.Drop();
        const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, pageIndex, std::memory_order_relaxed);
        UNIT_ASSERT(state.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), AdvanceItemVersion(pageVersion));
    }
    Y_UNIT_TEST(PublicWriterStickysSuccessorPublishedBeforeReclaim) {
        TFixture fixture;
        const auto keys = MakeAdjacentKeys(*fixture.Space, MakeCollectionCacheItem(4, 5), 400);
        const auto target = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(target);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[0], target).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(target, MakePageData(target.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, target));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterTableLinkCas);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);

        auto cache = fixture.Cache;
        ECacheResultStatus status = ECacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestCachePageRef hit;
        TGateThread writer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            status = FindOrInsertPage(
                *cache, PageCollection(keys[1]), MakePageLocation(keys[1].Word(1)), EStickyState::None, inserted, hit);
        });

        gate.Slots[0].Wait();
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        const THandleState afterOwner = TCacheTestAccess::HandleState(*fixture.Cache, target.Index());
        UNIT_ASSERT(afterOwner.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(afterOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 2 * pageBytes));
        gate.Slots[0].Release();
        writer.Join();

        UNIT_ASSERT(status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, target.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(FindPage(*fixture.Cache, keys[0]).Status == ECacheResultStatus::Miss);
        auto successor = FindPage(*fixture.Cache, keys[1]);
        UNIT_ASSERT(successor.Status == ECacheResultStatus::Hit);
        successor.Ref.Drop();
    }
    Y_UNIT_TEST(PublicWriterRetriesAfterReclaimWinsBeforeLinkCas) {
        TFixture fixture;
        const auto keys = MakeAdjacentKeys(*fixture.Space, MakeCollectionCacheItem(4, 5), 410);
        const auto target = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(target);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[0], target).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(target, MakePageData(target.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, target));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeTableLinkCas);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);

        auto cache = fixture.Cache;
        ECacheResultStatus status = ECacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestCachePageRef hit;
        TGateThread writer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            status = FindOrInsertPage(
                *cache, PageCollection(keys[1]), MakePageLocation(keys[1].Word(1)), EStickyState::None, inserted, hit);
        });

        gate.Slots[0].Wait();
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        const THandleState afterOwner = TCacheTestAccess::HandleState(*fixture.Cache, target.Index());
        UNIT_ASSERT(afterOwner.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(afterOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 2 * pageBytes));
        gate.Slots[0].Release();
        writer.Join();

        UNIT_ASSERT(status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, target.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        auto successor = FindPage(*fixture.Cache, keys[1]);
        UNIT_ASSERT(successor.Status == ECacheResultStatus::Hit);
        successor.Ref.Drop();
    }
    Y_UNIT_TEST(PublicHelperBecomesFinalRefAfterOwnerRelease) {
        TFixture fixture;
        const auto keys = MakeAdjacentKeys(*fixture.Space, MakeCollectionCacheItem(4, 5), 420);
        const auto target = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(target);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[0], target).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(target, MakePageData(target.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, target));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::AfterTombstoneOwnerClaim);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        std::atomic<bool> reclaimed = false;
        TGateThread owner(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            reclaimed.store(cache->ReclaimCold(), std::memory_order_release);
        });
        gate.Slots[0].Wait();

        gate.Slots[1].Arm(ECacheHookPoint::AfterTombstoneHelperClaim);
        ECacheResultStatus status = ECacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestCachePageRef hit;
        TGateThread writer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            status = FindOrInsertPage(
                *cache, PageCollection(keys[1]), MakePageLocation(keys[1].Word(1)), EStickyState::None, inserted, hit);
        });
        gate.Slots[1].Wait();
        gate.Slots[0].Release();
        owner.Join();
        UNIT_ASSERT(reclaimed.load(std::memory_order_acquire));
        const THandleState afterOwner = TCacheTestAccess::HandleState(*fixture.Cache, target.Index());
        UNIT_ASSERT(afterOwner.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(afterOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        gate.Slots[1].Release();
        writer.Join();

        UNIT_ASSERT(status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, target.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        auto successor = FindPage(*fixture.Cache, keys[1]);
        UNIT_ASSERT(successor.Status == ECacheResultStatus::Hit);
        successor.Ref.Drop();
    }
    Y_UNIT_TEST(PublicHelperWinsBeforeFinalTombstoneCas) {
        TFixture fixture;
        const auto keys = MakeAdjacentKeys(*fixture.Space, MakeCollectionCacheItem(4, 5), 430);
        const auto target = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(target);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[0], target).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(target, MakePageData(target.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, target));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeTombstoneFinalCas, target.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        std::atomic<bool> reclaimed = false;
        TGateThread owner(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            reclaimed.store(cache->ReclaimCold(), std::memory_order_release);
        });
        gate.Slots[0].Wait();

        gate.Slots[1].Arm(ECacheHookPoint::AfterTombstoneHelperClaim, target.CacheItem());
        TGateThread helper(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            auto hold = TCacheTestAccess::AcquireTombstoneHelper(*cache, target.CacheItem());
            hold.Drop();
        });
        gate.Slots[1].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::HandleState(*fixture.Cache, target.Index()).Refs(), 2);
        gate.Slots[0].Release();
        owner.Join();
        UNIT_ASSERT(reclaimed.load(std::memory_order_acquire));
        const THandleState afterOwner = TCacheTestAccess::HandleState(*fixture.Cache, target.Index());
        UNIT_ASSERT(afterOwner.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(afterOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        gate.Slots[1].Release();
        helper.Join();

        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, target.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 0));
        UNIT_ASSERT(FindPage(*fixture.Cache, keys[0]).Status == ECacheResultStatus::Miss);
    }
    Y_UNIT_TEST(PublicReclaimPausesBeforeFinalTombstoneCas) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(4, 5), 440);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ECacheHookPoint::BeforeTombstoneFinalCas);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        std::atomic<bool> reclaimed = false;
        TGateThread owner(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            reclaimed.store(cache->ReclaimCold(), std::memory_order_release);
        });

        gate.Slots[0].Wait();
        const THandleState paused = TCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(paused.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(paused.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ECacheResultStatus::Miss);
        gate.Slots[0].Release();
        owner.Join();

        UNIT_ASSERT(reclaimed.load(std::memory_order_acquire));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 0));
    }
    Y_UNIT_TEST(PendingFindAndEqualInsert) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(2, 3), 200);
        const auto candidate = AllocatePage(*fixture.Cache, key, 8192);
        UNIT_ASSERT(candidate);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, candidate).Status == ECacheResultStatus::Inserted);

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ECacheResultStatus::Pending);
        UNIT_ASSERT(!found.Ref);

        const auto duplicate = AllocatePage(*fixture.Cache, key, 8192);
        UNIT_ASSERT(duplicate);
        auto existing = FindOrInsertPage(*fixture.Cache, key, duplicate);
        UNIT_ASSERT(existing.Status == ECacheResultStatus::Pending);
        UNIT_ASSERT(!existing.Ref);
    }
    Y_UNIT_TEST(TerminalBoundaryReloadsOwnerVersion) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(2, 3), 201);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);

        const auto [state, terminal] =
            TCacheTestAccess::WithHandle(*fixture.Cache, page.Index(), [](THandle& handle) {
                return std::pair{
                    THandleState::FromRaw(handle.State.load(std::memory_order_acquire)),
                    TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire)),
                };
            });
        const TCacheItem owner = TCacheItem::Make(state.Version(), page.Index());
        UNIT_ASSERT(terminal.IsNull());
        UNIT_ASSERT(TCacheTestAccess::ValidateTerminalOwner(*fixture.Cache, owner, terminal));

        TCacheTestAccess::WithHandle(*fixture.Cache, page.Index(), [&](THandle& handle) {
            handle.State.store(state.WithVersion(AdvanceItemVersion(state.Version())).Raw(), std::memory_order_release);
        });
        UNIT_ASSERT(!TCacheTestAccess::ValidateTerminalOwner(*fixture.Cache, owner, terminal));
        TCacheTestAccess::WithHandle(*fixture.Cache, page.Index(), [&](THandle& handle) {
            handle.State.store(state.Raw(), std::memory_order_release);
        });
    }
    Y_UNIT_TEST(PendingAcquireSurvivesGenerationChangeAfterTableFind) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(2, 3), 202);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);

        UNIT_ASSERT(TCacheTestAccess::AcquireAfterGenerationChange(*fixture.Cache, key, true));
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ECacheResultStatus::Pending);
    }
    Y_UNIT_TEST(AcquiredRefSurvivesGenerationChangeAfterTableFind) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(2, 3), 203);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        UNIT_ASSERT(TCacheTestAccess::AcquireAfterGenerationChange(*fixture.Cache, key, false));
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::HandleState(*fixture.Cache, page.Index(), std::memory_order_relaxed).Refs(), 0);
        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
        found.Ref.Drop();
    }
    Y_UNIT_TEST(HotHitSaturatesFrequencyInAcquireCas) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(3, 4), 300);
        const auto candidate = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(candidate);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, candidate).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));

        for (ui8 expectedFrequency : { ui8{ 1 }, ui8{ 2 }, ui8{ 2 } }) {
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
            const auto state =
                TCacheTestAccess::HandleState(*fixture.Cache, candidate.Index(), std::memory_order_relaxed);
            UNIT_ASSERT_VALUES_EQUAL(state.Frequency(), expectedFrequency);
            UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
            found.Ref.Drop();
        }
    }
    Y_UNIT_TEST(ColdHitReheatsAndInvalidatesMembership) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(4, 5), 400);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        ui32 pageIndex = 0;
        ui32 coldVersion = 0;
        {
            const auto page = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(page);
            pageIndex = page.Index();
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            coldVersion = TCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), pageBytes);
        }

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
        const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, pageIndex, std::memory_order_relaxed);
        UNIT_ASSERT(state.IsHot());
        UNIT_ASSERT_VALUES_EQUAL(
            TCacheTestAccess::ColdVersion(*fixture.Cache, pageIndex), AdvanceColdVersion(coldVersion));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        found.Ref.Drop();

        UNIT_ASSERT(!fixture.Cache->ReclaimCold());
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ECacheResultStatus::Hit);
    }
    Y_UNIT_TEST(LastColdReleasePublishesMembership) {
        TFixture fixture;
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(5, 6), 500);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        ui32 pageIndex = 0;
        TPageCacheItem pageItem;
        {
            const auto page = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(page);
            pageIndex = page.Index();
            pageItem = page;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        }

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Ref);
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, pageItem));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, pageIndex, std::memory_order_relaxed).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), pageBytes);

        found.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ECacheResultStatus::Miss);
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, pageIndex, std::memory_order_relaxed).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
    }
    Y_UNIT_TEST(S5FifoRoutesColdCandidates) {
        TFixture fixture(5);
        const auto token = MakeCollectionCacheItem(6, 7);
        TVector<ui32> pages;
        {
            for (ui64 offset = 0; offset <= TCacheTestAccess::EffectiveHotSlots(*fixture.Cache); ++offset) {
                const auto key = TCacheKey::Page(token, offset);
                const auto page = AllocatePage(*fixture.Cache, key, 4096);
                UNIT_ASSERT(page);
                pages.push_back(page.Index());
                UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ECacheResultStatus::Inserted);
                UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            }
        }

        ui32 coldPages = 0;
        for (ui32 index : pages) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, index, std::memory_order_relaxed);
            coldPages += state.IsCold();
        }
        UNIT_ASSERT(coldPages > 0);

        UNIT_ASSERT(fixture.Cache->ReclaimCold());
    }

    Y_UNIT_TEST(MixedSizeAndModeChurnPreservesDataAndAccounting) {
        TFixture fixture(7);
        std::array<TCollectionCacheItem, 2> collections;
        std::array<TTestCacheCollectionRef, 2> owners;
        for (ui32 index = 0; index < collections.size(); ++index) {
            const TLogoBlobID id(96, 108, index + 1);
            UNIT_ASSERT(TCacheTestAccess::FindOrInsertUnregistered(*fixture.Cache, { .Id = id }, collections[index],
                            owners[index]) == ECacheResultStatus::Inserted);
            UNIT_ASSERT(
                TCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, collections[index], MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->Find(id, owners[index]) == ECacheResultStatus::Hit);
        }

        const ui64 initialLimit = fixture.Cache->CurrentLimit();
        UNIT_ASSERT(fixture.Cache->UpdateKeepLimit(initialLimit / 4));
        std::array<bool, 256> seen{};
        ui64 random = 0x4c464341434845;
        ui32 hits = 0;
        ui32 reloads = 0;
        for (ui32 step = 0; step < 10000; ++step) {
            if (step % 256 == 0) {
                const ui32 phase = step / 256;
                UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(phase % 2 ? initialLimit / 2 : initialLimit));
                for (ui32 index = 0; index < collections.size(); ++index) {
                    UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                        collections[index], (phase + index) % 2 ? ECacheMode::TryKeepInMemory : ECacheMode::Regular));
                }
            }

            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            const ui32 pageId = random % seen.size();
            const ui64 size = 1_KB << ((pageId >> 1) & 3);
            TPageCacheItem page;
            TTestCachePageRef ref;
            ECacheResultStatus status = ECacheResultStatus::Miss;
            for (ui32 attempt = 0; attempt < 256; ++attempt) {
                status = FindOrInsertPage(*fixture.Cache, collections[pageId % collections.size()],
                    MakePageLocation(pageId, size), EStickyState::None, page, ref);
                if (status != ECacheResultStatus::Miss) {
                    break;
                }
                fixture.Cache->EnforceCurrentLimit();
            }
            if (status == ECacheResultStatus::Hit) {
                ++hits;
                UNIT_ASSERT_VALUES_EQUAL(ref.size(), size);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(ref.data()[0]), static_cast<ui8>(pageId));
            } else {
                UNIT_ASSERT(status == ECacheResultStatus::Inserted);
                reloads += seen[pageId];
                UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(pageId, size)));
                seen[pageId] = true;
            }
            ref.Drop();

            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), fixture.Cache->ResidentBytes());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(),
                fixture.Cache->HotBytes() + fixture.Cache->ColdBytes() + fixture.Cache->StickyBytes());
        }
        UNIT_ASSERT(hits > 0);
        UNIT_ASSERT(reloads > 0);

        for (auto collection : collections) {
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        }
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(1));
        for (ui32 attempt = 0; attempt < 4096 && fixture.Cache->ResidentBytes() != 0; ++attempt) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
    }

    Y_UNIT_TEST(PageAndCollectionShareTable) {
        TFixture fixture;
        const auto collectionKey = TCacheKey::Collection(TLogoBlobID(1, 2, 3));
        const auto pageKey = TCacheKey::Page(MakeCollectionCacheItem(7, 9), 42);
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        const auto page = AllocatePage(*fixture.Cache, pageKey, 4096);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(page);
        UNIT_ASSERT(TCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(
            FindOrInsertCollection(*fixture.Cache, collectionKey, collection).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, pageKey, page).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        auto foundCollection = FindCollection(*fixture.Cache, collectionKey);
        auto foundPage = FindPage(*fixture.Cache, pageKey);
        UNIT_ASSERT(foundCollection.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(foundPage.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(foundCollection.Ref.CacheItem() == collection);
        UNIT_ASSERT(foundCollection.Ref.CacheItem() == collection);
        UNIT_ASSERT_VALUES_EQUAL(foundCollection.Ref.GetCollection().Id(), CollectionId(collectionKey));
        UNIT_ASSERT_VALUES_EQUAL(&foundCollection.Ref.GetCollection(),
            TCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index()));
        UNIT_ASSERT(foundPage.Ref.CacheItem() == page);
        UNIT_ASSERT_VALUES_EQUAL(foundPage.Ref.size(), 4096);
        UNIT_ASSERT_VALUES_EQUAL(foundPage.Ref.data()[0], static_cast<char>(page.Index()));
        UNIT_ASSERT(foundPage.Ref.GetType() == NTable::NPage::EPage::DataPage);
        auto sharedData = foundPage.Ref.BuildSharedData();
        UNIT_ASSERT_VALUES_EQUAL(sharedData.data(), foundPage.Ref.data());
        UNIT_ASSERT_VALUES_EQUAL(sharedData.size(), foundPage.Ref.size());

        const char* pageData = foundPage.Ref.data();
        const TCollection* collectionValue = &foundCollection.Ref.GetCollection();
        auto movedPage = std::move(foundPage.Ref);
        auto movedCollection = std::move(foundCollection.Ref);
        UNIT_ASSERT(!foundPage.Ref);
        UNIT_ASSERT(!foundCollection.Ref);
        UNIT_ASSERT(movedPage.CacheItem() == page);
        UNIT_ASSERT_VALUES_EQUAL(movedPage.data(), pageData);
        UNIT_ASSERT(movedCollection.CacheItem() == collection);
        UNIT_ASSERT_VALUES_EQUAL(&movedCollection.GetCollection(), collectionValue);
        movedPage.Drop();
        movedCollection.Drop();
    }
    Y_UNIT_TEST(PageRefSnapshotSurvivesAliasRelease) {
        TFixture fixture(6, 8, 7);
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(51, 52), 0);
        TPageCacheItem insertedItem;
        TTestCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key), MakePageLocation(key.Word(1)),
                        EStickyState::None, insertedItem, hit) == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(insertedItem, MakePageData(insertedItem.Index())));
        TTestCachePageRef page;
        UNIT_ASSERT(fixture.Cache->Find(PageCollection(key), key.Word(1), page) == ECacheResultStatus::Hit);
        const char* data = page.data();
        auto moved = std::move(page);
        UNIT_ASSERT(!page);
        const auto* handles = TCacheTestAccess::WithSpace(*fixture.Cache, [](const auto& operation) {
            return operation.Handles();
        });

        for (const auto* target : { &fixture.ReservedCapacity, &fixture.Capacity, &fixture.ReservedCapacity }) {
            TTransition transition;
            UNIT_ASSERT(
                TCacheTestAccess::PrepareTransition(*fixture.Cache, *target, transition) == EPrepareResult::Prepared);
            UNIT_ASSERT(TCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TCacheTestAccess::BeginBucketResize(*fixture.Cache, target->AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);
            CommitTransition(*fixture.Cache, transition);
            UNIT_ASSERT_VALUES_EQUAL(TCacheTestAccess::WithSpace(*fixture.Cache, [](const auto& operation) {
                return operation.Handles();
            }), handles);
            UNIT_ASSERT_VALUES_EQUAL(moved.data(), data);
            UNIT_ASSERT_VALUES_EQUAL(moved.size(), 4096);
        }

        UNIT_ASSERT_VALUES_EQUAL(moved.data(), data);
        UNIT_ASSERT_VALUES_EQUAL(moved.size(), 4096);
        UNIT_ASSERT(moved.GetType() == NTable::NPage::EPage::DataPage);
        auto sharedData = moved.BuildSharedData();
        UNIT_ASSERT_VALUES_EQUAL(sharedData.data(), data);
        UNIT_ASSERT_VALUES_EQUAL(sharedData.size(), 4096);
    }
    Y_UNIT_TEST(PageRefAcquireRetainsWithdrawnPhysicalItem) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(61, 62);
        const TPageCacheItem item = InsertReadyPage(*fixture.Cache, TCacheKey::Page(collection, 0));
        TTestCachePageRef source;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, source) == ECacheResultStatus::Hit);
        const char* data = source.data();
        UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, item));
        UNIT_ASSERT(TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, item.Index())));
        TTestCachePageRef absent;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, absent) == ECacheResultStatus::Miss);

        auto owner = source.Acquire();
        UNIT_ASSERT(source);
        UNIT_ASSERT(owner.CacheItem() == source.CacheItem());
        source.Drop();
        UNIT_ASSERT_VALUES_EQUAL(owner.data(), data);
        UNIT_ASSERT_VALUES_EQUAL(owner.size(), 4096);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 4096 + NActors::TSharedData::OverheadSize);
        owner.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(PageRefMoveRetainsCachePage) {
        TActorSystemStub context;
        TCacheCapacityConfig capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(11, 4096, 0, 1, capacity));
        auto [spaceThreads, space] = TCacheThreadTestAccess::MakeSpace(capacity, capacity, 1);
        UNIT_ASSERT(space);
        auto cache = TSharedCache::Create(std::move(space), capacity.Limit);
        UNIT_ASSERT(cache);
        Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
        NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache);
        Y_DEFER {
            NSharedCache::TlsCacheHazard = nullptr;
        };

        const TCollectionCacheItem collection = MakeCollectionCacheItem(61, 62);
        TPageCacheItem inserted;
        TCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*cache, collection, MakePageLocation(0), EStickyState::None, inserted, hit) ==
                    ECacheResultStatus::Inserted);
        UNIT_ASSERT(cache->MakeReady(inserted, MakePageData(inserted.Index())));

        TCachePageRef page;
        UNIT_ASSERT(cache->Find(collection, 0, page) == ECacheResultStatus::Hit);
        TCachePageRef shared = std::move(page);
        UNIT_ASSERT(!page);
        UNIT_ASSERT(shared);
        UNIT_ASSERT_VALUES_EQUAL(shared.GetType(), NTable::NPage::EPage::DataPage);
        TCachePageRef pinned = std::move(shared);
        {
            auto* hazard = std::exchange(NSharedCache::TlsCacheHazard, nullptr);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = hazard;
            };
            UNIT_ASSERT_VALUES_EQUAL(pinned.data()[0], static_cast<char>(inserted.Index()));
            UNIT_ASSERT_VALUES_EQUAL(pinned.size(), 4096);
            UNIT_ASSERT(pinned.GetType() == NTable::NPage::EPage::DataPage);
        }
        UNIT_ASSERT_VALUES_EQUAL(pinned.BuildSharedData().size(), 4096);
        UNIT_ASSERT_VALUES_EQUAL(pinned.BuildSharedData().data()[0], static_cast<char>(inserted.Index()));
    }
    Y_UNIT_TEST(ConcurrentEqualInsertionSelectsOneWinner) {
        constexpr ui32 ThreadCount = 8;
        TFixture fixture(6, ThreadCount + 1);
        const auto key = TCacheKey::Page(MakeCollectionCacheItem(5, 6), 700);
        std::array<TPageCacheItem, ThreadCount> candidates;

        std::atomic<bool> start = false;
        std::array<ECacheResultStatus, ThreadCount> statuses;
        TVector<std::thread> threads;
        for (ui32 thread = 0; thread < ThreadCount; ++thread) {
            threads.emplace_back([&, thread] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, thread);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                const auto candidate = AllocatePage(*fixture.Cache, key, 4096);
                Y_ABORT_UNLESS(candidate);
                candidates[thread] = candidate;
                auto result = FindOrInsertPage(*fixture.Cache, key, candidate);
                statuses[thread] = result.Status;
            });
        }
        start.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }

        ui32 winners = 0;
        TPageCacheItem winner;
        for (ui32 thread = 0; thread < ThreadCount; ++thread) {
            if (statuses[thread] == ECacheResultStatus::Inserted) {
                ++winners;
                winner = candidates[thread];
            } else {
                UNIT_ASSERT(statuses[thread] == ECacheResultStatus::Pending);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(winners, 1);
        UNIT_ASSERT(fixture.Cache->MakeReady(winner, MakePageData(winner.Index())));

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT(found.Ref.CacheItem() == winner);
    }
    Y_UNIT_TEST(EraseColdHeadInteriorAndTail) {
        TFixture fixture;
        const auto token = MakeCollectionCacheItem(9, 10);
        TVector<TCacheKey> keys;
        ui32 bucket = Max<ui32>();
        for (ui64 offset = 1; keys.size() < 3; ++offset) {
            const auto key = TCacheKey::Page(token, offset);
            const ui32 candidateBucket = key.Hash() & TCacheTestAccess::CurrentSpaceState(*fixture.Space).BucketMask();
            if (keys.empty()) {
                bucket = candidateBucket;
            }
            if (candidateBucket == bucket) {
                keys.push_back(key);
            }
        }
        Sort(keys.begin(), keys.end(), [](const auto& left, const auto& right) {
            return CompareSharedCacheKeys(left, right) < 0;
        });

        std::array<ui32, 3> indices;
        for (ui32 index = 0; index < keys.size(); ++index) {
            const auto& key = keys[index];
            const auto candidate = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(candidate);
            indices[index] = candidate.Index();
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, candidate).Status == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, candidate));
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), fixture.Capacity.HandleCount() - 5);

        auto erase = [&](ui32 index) {
            return TCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, index));
        };

        UNIT_ASSERT(erase(indices[1]));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, indices[1], std::memory_order_relaxed).IsFree());
        UNIT_ASSERT(erase(indices[0]));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, indices[0], std::memory_order_relaxed).IsFree());
        UNIT_ASSERT(erase(indices[2]));
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, indices[2], std::memory_order_relaxed).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), fixture.Capacity.HandleCount() - 2);

        for (ui32 index = 0; index < keys.size(); ++index) {
            const auto& key = keys[index];
            UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ECacheResultStatus::Miss);
            UNIT_ASSERT(!erase(indices[index]));
        }
    }
    Y_UNIT_TEST(BridgesConsecutiveTombstonesIteratively) {
        constexpr ui32 TombstoneCount = 8;
        TFixture fixture;
        const auto token = MakeCollectionCacheItem(11, 12);
        TVector<TCacheKey> keys;
        ui32 bucket = Max<ui32>();
        for (ui64 offset = 1; keys.size() < TombstoneCount + 1; ++offset) {
            const auto key = TCacheKey::Page(token, offset);
            const ui32 candidateBucket = key.Hash() & TCacheTestAccess::CurrentSpaceState(*fixture.Space).BucketMask();
            if (keys.empty()) {
                bucket = candidateBucket;
            }
            if (candidateBucket == bucket) {
                keys.push_back(key);
            }
        }
        Sort(keys.begin(), keys.end(), [](const auto& left, const auto& right) {
            return CompareSharedCacheKeys(left, right) < 0;
        });

        std::array<ui32, TombstoneCount + 1> indices;
        std::array<TCacheItem, TombstoneCount> tombstones;
        for (ui32 index = 0; index < keys.size(); ++index) {
            const auto page = AllocatePage(*fixture.Cache, keys[index], 4096);
            UNIT_ASSERT(page);
            indices[index] = page.Index();
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[index], page).Status == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }

        for (ui32 index = 0; index < TombstoneCount; ++index) {
            tombstones[index] =
                TCacheTestAccess::WithHandle(*fixture.Cache, indices[index], [&](THandle& handle) {
                    const THandleState cold = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
                    UNIT_ASSERT(cold.IsCold());
                    handle.State.store(
                        cold.WithState(EHandleState::Tombstone).WithRefs(1).Raw(), std::memory_order_release);
                    return TCacheItem::Make(cold.Version(), indices[index]);
                });
        }

        TCacheTestAccess::FinishErase(*fixture.Cache, tombstones[TombstoneCount - 1], keys[TombstoneCount - 1]);

        for (ui32 index = 0; index + 1 < TombstoneCount; ++index) {
            const THandleState state = TCacheTestAccess::HandleState(*fixture.Cache, indices[index]);
            UNIT_ASSERT(state.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
        }
        UNIT_ASSERT(TCacheTestAccess::HandleState(*fixture.Cache, indices[TombstoneCount - 1]).IsFree());

        for (ui32 index = 0; index < TombstoneCount; ++index) {
            UNIT_ASSERT(FindPage(*fixture.Cache, keys[index]).Status == ECacheResultStatus::Miss);
        }
        auto successor = FindPage(*fixture.Cache, keys[TombstoneCount]);
        UNIT_ASSERT(successor.Status == ECacheResultStatus::Hit);
        successor.Ref.Drop();

        for (ui32 index = 0; index + 1 < TombstoneCount; ++index) {
            TCacheTestAccess::FinishErase(*fixture.Cache, tombstones[index], keys[index]);
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), fixture.Capacity.HandleCount() - 3);
    }
    Y_UNIT_TEST(ReaderTraversesAndWriterHelpsPausedTombstone) {
        TFixture fixture;
        const auto token = MakeCollectionCacheItem(12, 13);
        TVector<TCacheKey> keys;
        ui32 bucket = Max<ui32>();
        for (ui64 offset = 1; keys.size() < 4; ++offset) {
            const auto key = TCacheKey::Page(token, offset);
            const ui32 candidateBucket = key.Hash() & TCacheTestAccess::CurrentSpaceState(*fixture.Space).BucketMask();
            if (keys.empty()) {
                bucket = candidateBucket;
            }
            if (candidateBucket == bucket) {
                keys.push_back(key);
            }
        }
        Sort(keys.begin(), keys.end(), [](const auto& left, const auto& right) {
            return CompareSharedCacheKeys(left, right) < 0;
        });

        std::array<ui32, 3> indices;
        for (ui32 index = 0; index < indices.size(); ++index) {
            const auto candidate = AllocatePage(*fixture.Cache, keys[index], 4096);
            UNIT_ASSERT(candidate);
            indices[index] = candidate.Index();
            UNIT_ASSERT(
                FindOrInsertPage(*fixture.Cache, keys[index], candidate).Status == ECacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));
            UNIT_ASSERT(TCacheTestAccess::EvictFromHot(*fixture.Cache, candidate));
        }

        const TCacheItem tombstoneItem =
            TCacheTestAccess::WithHandle(*fixture.Cache, indices[1], [&](THandle& tombstone) {
                const THandleState cold = THandleState::FromRaw(tombstone.State.load(std::memory_order_relaxed));
                UNIT_ASSERT(cold.IsCold());
                tombstone.State.store(cold.WithState(EHandleState::Tombstone).WithRefs(1).Raw(),
                    std::memory_order_release); // paused deletion owner
                return TCacheItem::Make(cold.Version(), indices[1]);
            });

        UNIT_ASSERT(FindPage(*fixture.Cache, keys[1]).Status == ECacheResultStatus::Miss);
        auto afterTombstone = FindPage(*fixture.Cache, keys[2]);
        UNIT_ASSERT(afterTombstone.Status == ECacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(afterTombstone.Ref.CacheItem().Index(), indices[2]);
        afterTombstone.Ref.Drop();

        const auto candidate = AllocatePage(*fixture.Cache, keys[3], 4096);
        UNIT_ASSERT(candidate);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[3], candidate).Status == ECacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));

        auto state = TCacheTestAccess::HandleState(*fixture.Cache, indices[1], std::memory_order_relaxed);
        UNIT_ASSERT(state.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1); // only the paused deletion owner remains
        UNIT_ASSERT(TCacheItem::FromRaw(TCacheTestAccess::HandleNext(*fixture.Cache, indices[1])).IsFrozen());

        TCacheTestAccess::FinishErase(*fixture.Cache, tombstoneItem, keys[1]);
        state = TCacheTestAccess::HandleState(*fixture.Cache, indices[1], std::memory_order_relaxed);
        UNIT_ASSERT(state.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), AdvanceItemVersion(tombstoneItem.Version()));
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
    }
}

} // namespace NKikimr::NSharedCache
