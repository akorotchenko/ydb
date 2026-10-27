#include <ydb/core/tablet_flat/shared_cache_thread_test.h>
#include "shared_cache.h"
#include "shared_cache_mapping_test.h"
#include "shared_cache_traits.h"

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
using EResizeStep = TSharedCacheTableImpl<TTestTraits>::EResizeStep;
using TTestSharedCacheItemRef = TSharedCacheItemRefImpl<TTestTraits>;
using TTestSharedPageRef = TSharedPageRefImpl<TTestTraits>;
using TTestSharedCacheCollectionRef = TSharedCacheCollectionRefImpl<TTestTraits>;
using TTestPageFetch = TPageFetchImpl<TTestTraits>;
using TTestSharedCachePageRequest = TSharedCachePageRequestImpl<TTestTraits>;

static_assert(sizeof(TTestSharedCacheItemRef) == sizeof(TCacheItem));
static_assert(sizeof(TTestSharedPageRef) == 8);
static_assert(sizeof(TTestSharedCacheCollectionRef) == sizeof(TCacheItem));
static_assert(!std::is_move_constructible_v<TTransition>);
static_assert(!std::is_move_assignable_v<TTransition>);

struct TSeparatePageRequest {
    NTable::NPage::TPageLocation Location;
    TIntrusivePtr<TPageFetchWaiter> Waiter;
    ESharedCacheResultStatus Status;
    TTestSharedPageRef Page;
    TTestPageFetch Fetch;
};

static_assert(sizeof(TTestSharedCachePageRequest) < sizeof(TSeparatePageRequest));

template <class TRef>
struct TTestResult {
    ESharedCacheResultStatus Status = ESharedCacheResultStatus::Miss;
    TRef Ref;
};

using TPageResult = TTestResult<TTestSharedPageRef>;
using TCollectionResult = TTestResult<TTestSharedCacheCollectionRef>;
using TItemResult = TTestResult<TTestSharedCacheItemRef>;

class TSharedCacheTestAccess {
public:
    static bool FindOrInsertWrittenBatch(TTestSharedCache& cache, TCollectionCacheItem collection,
        TArrayRef<TTestSharedCachePageRequest> requests) noexcept {
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

    static ESharedCacheResultStatus FindOrInsert(TTestSharedCache& cache, TCollectionRegistry& registry,
        const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TTestSharedCacheCollectionRef& hit) noexcept {
        return cache.FindOrInsert(registry, collection, inserted, hit);
    }

    static bool MakeReadyUnregistered(
        TTestSharedCache& cache, TCollectionCacheItem collection, THolder<TCollection>&& value) noexcept {
        return cache.MakeReady(nullptr, collection, std::move(value));
    }

    static TTestSharedCacheItemRef AcquireStructural(TTestSharedCache& cache, TCacheItem item) noexcept {
        auto operation = cache.BeginOperation();
        return operation.TryAcquireStructural(item);
    }

    static bool MakePendingPageSticky(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        auto spaceOp = cache.BeginOperation();
        TOperationItemRef<TTestTraits> ref(spaceOp, spaceOp.TryAcquireStructural(page.CacheItem()));
        return ref && spaceOp.Collections().MakePageSticky(page.CacheItem());
    }

    static bool MakePageSticky(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        TTestSharedPageRef held;
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
    static ESharedCacheResultStatus FindOrInsert(TSharedCacheImpl<TTraits>& cache, TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EStickyState sticky, TPageCacheItem& inserted,
        TSharedPageRefImpl<TTraits>& hit) noexcept {
        return cache.FindOrInsert(collection, page, sticky, inserted, hit);
    }

    template <class TTraits>
    static ESharedCacheResultStatus FindOrInsert(TSharedCacheImpl<TTraits>& cache, TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EStickyState sticky, TIntrusivePtr<TPageFetchWaiter> waiter,
        TSharedPageRefImpl<TTraits>& hit, TPageFetchImpl<TTraits>& fetch) noexcept {
        return cache.FindOrInsert(collection, page, sticky, std::move(waiter), hit, fetch);
    }

    static ESharedCacheResultStatus FindOrInsertUnregistered(TTestSharedCache& cache,
        const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TTestSharedCacheCollectionRef& hit) noexcept {
        return cache.FindOrInsertUnregistered(collection, inserted, hit);
    }

    static bool AcquirePage(TTestSharedCache& cache, TPageCacheItem page, TTestSharedPageRef& result) noexcept {
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

    static bool TryReserve(TTestSharedCache& cache, ui64 bytes) noexcept {
        auto spaceOp = cache.BeginOperation();
        return cache.TryReserve(spaceOp, bytes);
    }

    static void ReleaseReservation(TTestSharedCache& cache, ui64 bytes) noexcept {
        cache.ReleaseReservation(bytes);
    }

    static TSharedCacheMapping& BucketsMapping(TTestSharedCache& cache) noexcept {
        return cache.Space_->BucketsMapping_;
    }

    static void SetCapacityTarget(TTestSharedCache& cache, const TSharedCacheCapacity& target) noexcept {
        cache.Capacity_.SetTarget(target);
    }

    static TTransition& CapacityTransition(TTestSharedCache& cache) noexcept {
        return cache.Capacity_.Transition_;
    }

    static EPrepareResult PrepareTransition(
        TTestSharedCache& cache, const TSharedCacheCapacity& target, TTransition& transition) noexcept {
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

    static TSharedCacheMapping& RingMapping(TTestSharedCache& cache, ESpaceMap mapping) noexcept {
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
            return handle.ColdVersion.load(std::memory_order_acquire);
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
            const THandleState state = THandleState::FromRaw(handle.State.load(std::memory_order_relaxed));
            Y_ABORT_UNLESS(state.IsHot() && state.Refs() == 1);
            const ui64 bytes = TCacheItems<TTestTraits>::PageBytes(handle);
            handle.State.store(state.WithState(EHandleState::KeepCold).Raw(), std::memory_order_relaxed);
            handle.ColdVersion.store(coldVersion, std::memory_order_relaxed);
            cache.Memory_.Hot().RemoveBytes(bytes);
            cache.Memory_.KeepCold().AddBytes(bytes);
            cache.Memory_.Hot().RemovePage(EItemKind::Page);
            cache.Memory_.KeepCold().AddPage(EItemKind::Page);
            cache.Memory_.AddKeepColdOwnedBytes(bytes);
        });
    }

    static ui32 QueueKeepColdPageForTest(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            THandle& handle = spaceOp.Handles()[page.Index()];
            const ui32 coldVersion =
                AdvanceColdVersion(handle.ColdVersion.load(std::memory_order_relaxed) & MaxItemVersion) |
                KeepColdQueuedMask;
            handle.ColdVersion.store(coldVersion, std::memory_order_relaxed);
            auto& rings = *spaceOp.View().Tiers[ECacheTier::Keep].Rings;
            rings.AddColdEntry();
            if (spaceOp.KeepCold().PutAndPop(TColdCacheItem::Make(coldVersion & MaxItemVersion, page.Index()).Raw())) {
                rings.RemoveColdEntry();
            }
            return coldVersion;
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
        return cache.Eviction_.MinimumHotSlots();
    }

    static ui64 ColdEntries(TTestSharedCache& cache, ECacheTier tier) noexcept {
        return cache.Space_->ColdRingEntries(tier);
    }

    static bool SweepKeepCold(TTestSharedCache& cache) noexcept {
        WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            cache.Eviction_.ScheduleKeepColdSweep(spaceOp.KeepCold().Capacity);
        });
        return cache.Eviction_.SweepKeepCold();
    }

    static std::array<ui64, 4> HotPolicy(TTestSharedCache& cache, const TSpaceOperation& spaceOp) noexcept {
        const ui32 effective = static_cast<ui32>(spaceOp.EffectiveHotSlots());
        return { effective, cache.Eviction_.MinimumHotSlots(), cache.Eviction_.ShrinkHotTarget(effective, 1),
            cache.Eviction_.GrowHotTarget(spaceOp, effective, 2) };
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
        return { cache.Eviction_.ColdMinBytes_.load(std::memory_order_relaxed),
            cache.Eviction_.ColdGrowBytes_.load(std::memory_order_relaxed) };
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
            return cache.BindOperation(spaceOp).Eviction().ShrinkKeepCold();
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

    static const TSharedCacheSpace& Space(const TTestSharedCache& cache) noexcept {
        return *cache.Space_;
    }

    static ui32 HazardCount(const TTestSharedCache& cache) noexcept {
        return cache.Space_->HazardCount_;
    }

    static ui64 SpareItem(const TTestSharedCache& cache, ui32 index) noexcept {
        return cache.Space_->Hazards_[index]->SpareItem.load(std::memory_order_relaxed);
    }

    static TSpaceState CurrentSpaceState(const TSharedCacheSpace& space) noexcept {
        return space.CurrentSpaceState();
    }

    static TBucketResizeState BucketResizeState(const TSharedCacheSpace& space) noexcept {
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
        return cache.Memory_.KeepCold().PagesEstimate();
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
        return cache.Space_->BeginHotResize(target, cache.HotResize_);
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

    static ui64 CollectionReferences(TTestSharedCache& cache, TCollectionCacheItem collection) {
        auto operation = cache.BeginOperation();
        return operation.Handles()[collection.Index()].Body.Collection->References.load(std::memory_order_relaxed);
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
        return cache.BindOperation(operation).Eviction().ReclaimKeepCold();
    }

    static bool DemoteKeepCold(TTestSharedCache& cache, TColdCacheItem item) noexcept {
        auto operation = cache.BeginOperation();
        return operation.Eviction().DemoteKeepCold(item);
    }

    static bool TrimKeepCold(TTestSharedCache& cache, TSpaceOperation& operation) noexcept {
        return cache.BindOperation(operation).Eviction().TrimKeepCold();
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

    static void FinishErase(TTestSharedCache& cache, TCacheItem cacheItem, const TSharedCacheKey& key) {
        auto operation = cache.BeginOperation();
        TOperationItemRef<TTestTraits> ownerRef(operation, cacheItem);
        cache.Table_.FinishErase(operation, ownerRef.CacheItem(), key);
    }

    static void InstallHooks(TTestSharedCache& cache, TTestTraits* hooks) noexcept {
        cache.SetHooks(hooks ? *hooks : TTestTraits{});
    }

    static bool ValidateTerminalOwner(TTestSharedCache& cache, TCacheItem owner, TCacheItem terminal) noexcept {
        auto spaceOp = cache.BeginOperation();
        TSharedCacheTableImpl<TTestTraits>::TOrderedLink orderedLink{
            .Expected = terminal,
            .Owner = owner,
        };
        return cache.Table_.ValidateTerminalOwner(spaceOp, orderedLink);
    }

    static bool AcquireAfterGenerationChange(
        TTestSharedCache& cache, const TSharedCacheKey& key, bool expectPending) noexcept {
        TTestSharedCacheItemRef ref;
        {
            auto operation = cache.BeginOperation();
            TCacheItem cacheItem;
            Y_ABORT_UNLESS(cache.Table_.Find(operation, key, cacheItem) == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(cache.Space_->AdvanceSpaceGeneration());
            const auto status = operation.TryAcquire(cacheItem, false, ref);
            Y_ABORT_UNLESS(status == (expectPending ? TTestSharedCache::EAcquireStatus::Pending
                                                    : TTestSharedCache::EAcquireStatus::Ready));
        }
        ref.Drop();
        return true;
    }

    static TTestSharedCacheItemRef AcquirePending(TTestSharedCache& cache, const TSharedCacheKey& key) noexcept {
        auto operation = cache.BeginOperation();
        TCacheItem cacheItem;
        Y_ABORT_UNLESS(cache.Table_.Find(operation, key, cacheItem) == ESharedCacheResultStatus::Hit);
        TTestSharedCacheItemRef ref;
        Y_ABORT_UNLESS(operation.TryAcquire(cacheItem, true, ref) == TTestSharedCache::EAcquireStatus::Pending);
        return ref;
    }

    static TTestSharedCacheItemRef AcquireTombstoneHelper(TTestSharedCache& cache, TCacheItem cacheItem) noexcept {
        auto operation = cache.BeginOperation();
        return cache.Table_.TryAcquireTombstone(operation, cacheItem);
    }

    static TItemResult FindOrInsert(
        TTestSharedCache& cache, const TSharedCacheKey& key, TPageCacheItem candidate) noexcept {
        auto spaceOp = cache.BeginOperation();
        Y_ABORT_UNLESS(LoadSharedCacheKey(spaceOp.Handles()[candidate.Index()], EItemKind::Page) == key);
        TTestSharedCacheItemRef ref;
        const ESharedCacheResultStatus status = cache.FindOrInsert(spaceOp, candidate.CacheItem(), ref);
        if (status == ESharedCacheResultStatus::Pending) {
            ref.Drop();
        }
        return { .Status = status, .Ref = std::move(ref) };
    }

    static TItemResult FindOrInsert(
        TTestSharedCache& cache, const TSharedCacheKey& key, TCollectionCacheItem candidate) noexcept {
        auto spaceOp = cache.BeginOperation();
        Y_ABORT_UNLESS(LoadSharedCacheKey(spaceOp.Handles()[candidate.Index()], EItemKind::Collection) == key);
        TTestSharedCacheItemRef ref;
        const ESharedCacheResultStatus status = cache.FindOrInsert(spaceOp, candidate.CacheItem(), ref);
        if (status == ESharedCacheResultStatus::Pending) {
            ref.Drop();
        }
        return { .Status = status, .Ref = std::move(ref) };
    }
};

ui64 CurrentBucketCount(const TSharedCacheSpace& space) noexcept {
    const TSpaceState state = TSharedCacheTestAccess::CurrentSpaceState(space);
    return state.Resizing() ? SharedCacheBucketCount(TSharedCacheTestAccess::BucketResizeState(space).OldAddressBits())
                            : state.BucketCount();
}

ui64 ExpectedOverallUsage(const TTestSharedCache& cache, ui64 payloadBytes) noexcept {
    return cache.StaticBytes() + cache.StaticDeltaBytes() + payloadBytes;
}

namespace {

    static_assert(!std::is_copy_constructible_v<TTestSharedPageRef>);
    static_assert(!std::is_copy_assignable_v<TTestSharedPageRef>);
    static_assert(std::is_move_constructible_v<TTestSharedPageRef>);
    static_assert(std::is_move_assignable_v<TTestSharedPageRef>);
    static_assert(!std::is_copy_constructible_v<TTestSharedCacheCollectionRef>);
    static_assert(std::is_move_constructible_v<TTestSharedCacheCollectionRef>);
    static_assert(std::is_move_assignable_v<TTestSharedCacheCollectionRef>);
    static_assert(!std::is_copy_constructible_v<TTestSharedCacheItemRef>);
    static_assert(!std::is_convertible_v<TTestSharedPageRef, TTestSharedCacheCollectionRef>);
    static_assert(!std::is_convertible_v<TTestSharedCacheCollectionRef, TTestSharedPageRef>);

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
        TAcquiringPageFetchWaiter(TTestSharedCache& cache, TTestSharedPageRef& page) noexcept
            : Cache_(cache)
            , Page_(page)
        {}

        void Complete(TPageCacheItem page, EPageFetchCompletion completion) noexcept override {
            Ready_ =
                completion == EPageFetchCompletion::Ready && TSharedCacheTestAccess::AcquirePage(Cache_, page, Page_);
        }

        bool Ready() const noexcept {
            return Ready_;
        }

    private:
        TTestSharedCache& Cache_;
        TTestSharedPageRef& Page_;
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
        return TColdCacheItem::Make(TSharedCacheTestAccess::ColdVersion(cache, index) & MaxItemVersion, index);
    }

    ui64 AvailableFreeHandles(const TTestSharedCache& cache) {
        ui64 result = TSharedCacheTestAccess::FreeCount(cache);
        for (ui32 index = 0; index < TSharedCacheTestAccess::HazardCount(cache); ++index) {
            result += TSharedCacheTestAccess::SpareItem(cache, index) != 0;
        }
        return result;
    }

    TCollectionCacheItem PageCollection(const TSharedCacheKey& key) {
        Y_ABORT_UNLESS(key.IsPageKind());
        return TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(key.Word(0)));
    }

    struct TSharedCacheGate {
        struct TSlot {
            std::atomic<ESharedCacheHookPoint> Wanted{ ESharedCacheHookPoint::BeforeTableLinkCas };
            std::atomic<ui64> Target{ 0 };
            std::atomic<bool> Arrived = false;
            std::atomic<bool> Continue = true;
            std::atomic<bool> Triggered = false;

            void Arm(ESharedCacheHookPoint point, TCacheItem target = {}) noexcept {
                Target.store(target.Raw(), std::memory_order_relaxed);
                Wanted.store(point, std::memory_order_release);
                Arrived.store(false, std::memory_order_relaxed);
                Continue.store(false, std::memory_order_relaxed);
                Triggered.store(false, std::memory_order_relaxed);
            }

            void Arm(ESharedCacheHookPoint point, TColdCacheItem target) noexcept {
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

            void TryInvoke(ESharedCacheHookPoint point, TCacheItem cacheItem) noexcept {
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

        static void Invoke(void* context, ESharedCacheHookPoint point, TCacheItem cacheItem) noexcept {
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
            TSharedCacheTestAccess::InstallHooks(Cache, &hooks);
        }

        ~TSharedCacheHookGuard() {
            TSharedCacheTestAccess::InstallHooks(Cache, nullptr);
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

    TLogoBlobID CollectionId(const TSharedCacheKey& key) {
        Y_ABORT_UNLESS(key.IsCollectionKind());
        return TLogoBlobID(key.Word(0), key.Word(1), key.Word(2));
    }

    // The fixture inserts Target first and no other key in this bucket before Successor.
    std::array<TSharedCacheKey, 2> MakeAdjacentKeys(
        const TSharedCacheSpace& space, TCollectionCacheItem token, ui64 startOffset) {
        TVector<TSharedCacheKey> keys;
        const ui32 bucket = TSharedCacheKey::Page(token, startOffset).Hash() &
                            TSharedCacheTestAccess::CurrentSpaceState(space).BucketMask();
        for (ui64 offset = startOffset; keys.size() < 2; ++offset) {
            Y_ABORT_UNLESS(offset - startOffset < (1u << 20));
            const auto key = TSharedCacheKey::Page(token, offset);
            if ((key.Hash() & TSharedCacheTestAccess::CurrentSpaceState(space).BucketMask()) == bucket) {
                keys.push_back(key);
            }
        }
        Sort(keys.begin(), keys.end(), [](const auto& left, const auto& right) {
            return CompareSharedCacheKeys(left, right) < 0;
        });
        Y_ABORT_UNLESS(CompareSharedCacheKeys(keys[0], keys[1]) < 0);
        return { keys[0], keys[1] };
    }

    std::array<TSharedCacheKey, 3> MakeResizeKeys(
        const TSharedCacheSpace& space, TCollectionCacheItem token, ui64 startOffset) {
        const ui64 oldBucketCount = CurrentBucketCount(space);
        Y_ABORT_UNLESS(oldBucketCount <= (ui64{ 1 } << 31));
        TMaybe<TSharedCacheKey> low;
        TVector<TSharedCacheKey> high;
        for (ui64 offset = startOffset; !low || high.size() < 2; ++offset) {
            Y_ABORT_UNLESS(offset - startOffset < (1u << 20));
            const auto key = TSharedCacheKey::Page(token, offset);
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

    TPageCacheItem AllocatePage(TTestSharedCache& cache, const TSharedCacheKey& key, ui64 size,
        NTable::NPage::EPage type = NTable::NPage::EPage::DataPage, ui32 crc32 = 0,
        EStickyState sticky = EStickyState::None) {
        return TSharedCacheTestAccess::AllocatePage(cache, PageCollection(key), key.Word(1), size, type, crc32, sticky);
    }

    TPageResult FindPage(TTestSharedCache& cache, const TSharedCacheKey& key) {
        TPageResult result;
        result.Status = cache.Find(PageCollection(key), key.Word(1), result.Ref);
        return result;
    }

    TItemResult FindOrInsertPage(TTestSharedCache& cache, const TSharedCacheKey& key, TPageCacheItem candidate) {
        return TSharedCacheTestAccess::FindOrInsert(cache, key, candidate);
    }

    template <class TTraits>
    ESharedCacheResultStatus FindOrInsertPage(TSharedCacheImpl<TTraits>& cache, TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EStickyState sticky, TPageCacheItem& inserted,
        TSharedPageRefImpl<TTraits>& hit) {
        return TSharedCacheTestAccess::FindOrInsert(cache, collection, page, sticky, inserted, hit);
    }

    TCollectionCacheItem AllocateCollection(
        TTestSharedCache& cache, const TSharedCacheKey& key, size_t backingSize = 0) {
        Y_UNUSED(backingSize);
        return TSharedCacheTestAccess::AllocateCollection(cache, CollectionId(key), 0);
    }

    TCollectionResult FindCollection(TTestSharedCache& cache, const TSharedCacheKey& key) {
        TCollectionResult result;
        result.Status = cache.Find(CollectionId(key), result.Ref);
        return result;
    }

    TItemResult FindOrInsertCollection(
        TTestSharedCache& cache, const TSharedCacheKey& key, TCollectionCacheItem candidate) {
        return TSharedCacheTestAccess::FindOrInsert(cache, key, candidate);
    }

    TPageCacheItem InsertReadyPage(TTestSharedCache& cache, const TSharedCacheKey& key) {
        TPageCacheItem page;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(cache, PageCollection(key), MakePageLocation(key.Word(1)), EStickyState::None,
                        page, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(cache.MakeReady(page, MakePageData(page.Index())));
        return page;
    }

    void AssertPageHit(TTestSharedCache& cache, const TSharedCacheKey& key, TPageCacheItem expected) {
        auto found = FindPage(cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(found.Ref.CacheItem() == expected);
        found.Ref.Drop();
    }

    void AssertPageUnreferenced(TTestSharedCache& cache, TPageCacheItem page) {
        const THandleState state = TSharedCacheTestAccess::HandleState(cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), page.Version());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
    }

    void AssertResizeMarker(TTestSharedCache& cache, EHandleState expectedState) {
        const THandleState state = TSharedCacheTestAccess::HandleState(cache, ResizeMarkerIndex);
        UNIT_ASSERT(state.State() == expectedState);
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
    }

    void CompleteBucketResize(TTestSharedCache& cache, TSharedCacheSpace& space) {
        const ui64 maxSteps = ui64(TSharedCacheTestAccess::BucketResizeState(space).PairCount()) * 5 + 1;
        for (ui64 step = 0; step < maxSteps; ++step) {
            if (!TSharedCacheTestAccess::CurrentSpaceState(space).Resizing()) {
                return;
            }
            UNIT_ASSERT(TSharedCacheTestAccess::AdvanceBucketResize(cache));
        }
        UNIT_ASSERT_C(!TSharedCacheTestAccess::CurrentSpaceState(space).Resizing(),
            "Bucket resize did not finish within its step bound");
    }

    void CommitTransition(TTestSharedCache& cache, TTransition& transition) {
        UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(cache, transition));
        while (transition.Phase() == ETransitionPhase::AppendGrowth) {
            UNIT_ASSERT(TSharedCacheTestAccess::AppendFreeHandles(cache, transition));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(cache, transition));
        while (transition.Phase() == ETransitionPhase::FinalDrain) {
            UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(cache, transition));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(cache, transition));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(cache, transition));
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSharedCacheTableKeyTest) {
    Y_UNIT_TEST(PageKeyRoundTripIgnoresSizeWord) {
        THandle handle;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(5, 17), 1234);
        StoreSharedCacheKey(handle, key);
        handle.Key2OrSize.store(98765);

        UNIT_ASSERT(LoadSharedCacheKey(handle, EItemKind::Page) == key);
    }
    Y_UNIT_TEST(CollectionKeyRoundTripUsesAllWords) {
        THandle handle;
        const auto key = TSharedCacheKey::Collection(TLogoBlobID(11, 22, 33));
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
        const auto page = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 3);
        const auto collection = TSharedCacheKey::Collection(TLogoBlobID(1, 2, 3));

        UNIT_ASSERT(CompareSharedCacheKeys(page, 42, collection, 42) < 0);
        UNIT_ASSERT(CompareSharedCacheKeys(collection, 42, page, 42) > 0);
    }
    Y_UNIT_TEST(TypedWordOrderBreaksHashTie) {
        const auto first = TSharedCacheKey::Collection(TLogoBlobID(1, 2, 3));
        const auto second = TSharedCacheKey::Collection(TLogoBlobID(1, 2, 4));

        UNIT_ASSERT(CompareSharedCacheKeys(first, 42, second, 42) < 0);
        UNIT_ASSERT(CompareSharedCacheKeys(second, 42, first, 42) > 0);
        UNIT_ASSERT_VALUES_EQUAL(CompareSharedCacheKeys(first, 42, first, 42), 0);
    }
    Y_UNIT_TEST(ComparatorDefinesTotalOrder) {
        TVector<TSharedCacheKey> keys = {
            TSharedCacheKey::Page(MakeCollectionCacheItem(1, 7), 11),
            TSharedCacheKey::Collection(TLogoBlobID(3, 2, 1)),
            TSharedCacheKey::Page(MakeCollectionCacheItem(1, 7), 10),
            TSharedCacheKey::Collection(TLogoBlobID(1, 2, 3)),
            TSharedCacheKey::Page(MakeCollectionCacheItem(2, 7), 10),
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
            auto space = TSharedCacheSpace::Create(Capacity, ReservedCapacity, *Threads);
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
        TSharedCacheCapacity Capacity;
        TSharedCacheCapacity ReservedCapacity;
        TSharedCacheSpace* Space = nullptr;
        TCollectionRegistry Registry;
        TIntrusivePtr<TTestSharedCache> Cache;
    };

    Y_UNIT_TEST(CollectionPublicationKeepsFoundationStickyDefault) {
        TFixture fixture;
        const TLogoBlobID id(111, 112, 150);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef published;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(TCollectionLocation{ .Id = id }, collection, published) ==
                    ESharedCacheResultStatus::Inserted);
        auto value = MakeCollection(id);
        UNIT_ASSERT(value->GetCacheMode() == ECacheMode::Sticky);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, std::move(value)));
        UNIT_ASSERT(fixture.Cache->Find(id, published) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(published->GetCacheMode() == ECacheMode::Sticky);
    }

    TSharedCacheKey ReviewKeyForBucket(TCollectionCacheItem collection, ui32 mask, ui32 bucket, ui64 startOffset,
        const TSharedCacheKey* after = nullptr) {
        for (ui64 offset = startOffset; offset - startOffset < (1u << 20); ++offset) {
            const auto key = TSharedCacheKey::Page(collection, offset);
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
        TTestSharedPageRef sourceHit;
        TTestPageFetch fetch;
        UNIT_ASSERT(
            TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(sourceKey.Word(1)),
                EStickyState::None, waiter, sourceHit, fetch) == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem source = fetch.CacheItem();

        struct TCompletionGate {
            TSharedCacheGate Gate;
            TCacheItem Source;
            std::atomic<ui32> Attempts = 0;
            std::atomic<bool> Retried = false;

            static void Invoke(void* context, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                auto& self = *static_cast<TCompletionGate*>(context);
                if (item != self.Source) {
                    return;
                }
                if (point == ESharedCacheHookPoint::AfterFetchCompletingClaim) {
                    self.Gate.Slots[0].TryInvoke(point, item);
                } else if (point == ESharedCacheHookPoint::BeforeInsertOwnerAcquire) {
                    if (self.Attempts.fetch_add(1, std::memory_order_relaxed) == 1) {
                        self.Retried.store(true, std::memory_order_relaxed);
                        self.Gate.Slots[1].Block();
                    }
                } else if (point == ESharedCacheHookPoint::AfterInsertPositionFound) {
                    self.Gate.Slots[1].Block();
                }
            }
        } completionGate;

        completionGate.Source = source.CacheItem();
        auto& gate = completionGate.Gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterFetchCompletingClaim, source.CacheItem());
        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterInsertPositionFound, source.CacheItem());
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

        ESharedCacheResultStatus insertStatus = ESharedCacheResultStatus::Miss;
        TPageCacheItem successor;
        TTestSharedPageRef successorHit;
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
        const THandleState completing = TSharedCacheTestAccess::HandleState(*cache, source.Index());
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
        UNIT_ASSERT(insertStatus == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(successor);
        UNIT_ASSERT(!successorHit);
        UNIT_ASSERT(cache->MakeReady(successor, MakePageData(successor.Index())));
        AssertPageHit(*cache, successorKey, successor);
        if (ready) {
            AssertPageHit(*cache, sourceKey, source);
        } else {
            UNIT_ASSERT(FindPage(*cache, sourceKey).Status == ESharedCacheResultStatus::Miss);
        }
        UNIT_ASSERT_VALUES_EQUAL(cache->ReservedBytes(), 0);
    }

    TVector<TTestSharedPageRef> HoldReviewFillers(TFixture & fixture, ui32 excludedMask, ui32 excludedBucket) {
        TVector<TTestSharedPageRef> held;
        const auto collection = MakeCollectionCacheItem(100, 50000);
        for (ui64 offset = 0; AvailableFreeHandles(*fixture.Cache) != 0; ++offset) {
            UNIT_ASSERT(offset < (1u << 20));
            const auto key = TSharedCacheKey::Page(collection, offset);
            if ((key.Hash() & excludedMask) == excludedBucket) {
                continue;
            }
            InsertReadyPage(*fixture.Cache, key);
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
            held.push_back(std::move(found.Ref));
        }
        return held;
    }

    void AssertReviewTerminal(TTestSharedCache & cache, TCacheItem owner, const char* finding) {
        const auto terminal = TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(cache, owner.Index()));
        UNIT_ASSERT(terminal.IsNull());
        // Check before Find: the broken terminal would make a miss lookup retry forever.
        UNIT_ASSERT_C(TSharedCacheTestAccess::ValidateTerminalOwner(cache, owner, terminal),
            TStringBuilder() << finding << ": terminal version " << terminal.Version() << " differs from owner version "
                             << owner.Version());
    }

    void ReviewEmptyMarkerAfterReuse(bool grow) {
        TFixture fixture(6, 8, grow ? 7 : 6);
        TTransition growth;
        if (grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, grow ? 7 : 5));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        if (!grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
        const auto reused = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        UNIT_ASSERT(reused.IsFree());
        UNIT_ASSERT(reused.Version() != 0);
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        const auto markerState = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        const auto marker = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);
        AssertReviewTerminal(*fixture.Cache, marker, "R1");
        const auto resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
        const auto missing = ReviewKeyForBucket(MakeCollectionCacheItem(101, 50001), (resize.SplitBit() << 1) - 1,
            resize.Cursor() | resize.SplitBit(), 10000);
        UNIT_ASSERT(FindPage(*fixture.Cache, missing).Status == ESharedCacheResultStatus::Miss);
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
        if (TSharedCacheTestAccess::HandleState(*fixture.Cache, original.Index()).IsHot()) {
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, original));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, original.Index())));
        const auto tail = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT_VALUES_EQUAL(tail.Index(), original.Index());
        UNIT_ASSERT(tail.Version() != original.Version());
        UNIT_ASSERT(tail.Version() != 0);
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 5));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
        AssertReviewTerminal(*fixture.Cache, tail.CacheItem(), "R2");
        const auto missing = ReviewKeyForBucket(token, 31, 0, 30000, &key);
        UNIT_ASSERT(FindPage(*fixture.Cache, missing).Status == ESharedCacheResultStatus::Miss);
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
        if (TSharedCacheTestAccess::HandleState(*fixture.Cache, spare.Index()).IsHot()) {
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, spare));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, spare.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::RelocatePage(*fixture.Cache, source));
        const auto replacement =
            TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(*fixture.Cache, source.Index())).WithoutFrozen();
        UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), spare.Index());
        UNIT_ASSERT(replacement.Version() != source.Version());
        AssertReviewTerminal(*fixture.Cache, replacement, "R3");
        const auto missing = ReviewKeyForBucket(token, 63, 0, 60000, &sourceKey);
        UNIT_ASSERT(FindPage(*fixture.Cache, missing).Status == ESharedCacheResultStatus::Miss);
    }

    void ReviewRelocationLosesToDeletion(bool reclaimDuringAllocation) {
        TFixture fixture;
        const TLogoBlobID id(151, 152, 153);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto sourceKey = TSharedCacheKey::Page(collection, 1);
        const auto source = InsertReadyPage(*fixture.Cache, sourceKey);
        auto sourceHold = FindPage(*fixture.Cache, sourceKey);
        TPageCacheItem spare;
        TPageResult spareHold;
        TVector<TTestSharedPageRef> fillers;
        if (reclaimDuringAllocation) {
            const auto spareKey = TSharedCacheKey::Page(collection, 2);
            spare = InsertReadyPage(*fixture.Cache, spareKey);
            spareHold = FindPage(*fixture.Cache, spareKey);
            fillers = HoldReviewFillers(fixture, 63, sourceKey.Hash() & 63);
        }
        sourceHold.Ref.Drop();
        if (TSharedCacheTestAccess::HandleState(*fixture.Cache, source.Index()).IsHot()) {
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, source));
        }
        const auto coldItem = MakeColdItem(*fixture.Cache, source.Index());
        auto structural = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, source.CacheItem());
        UNIT_ASSERT(structural);
        if (reclaimDuringAllocation) {
            spareHold.Ref.Drop();
            if (TSharedCacheTestAccess::HandleState(*fixture.Cache, spare.Index()).IsHot()) {
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, spare));
            }
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        }
        const ui64 freeBefore = AvailableFreeHandles(*fixture.Cache);
        const ui64 residentBefore = fixture.Cache->ResidentBytes();
        const ui32 ownersBefore = TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection);

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TColdCacheItem ColdItem;
            bool ReclaimDuringAllocation;
            bool Visited = false;
            bool Deleted = false;
        } context{ *fixture.Cache, collection, coldItem, reclaimDuringAllocation };

        TTestTraits hooks{ &context,
            [](void* ptr, ESharedCacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (ctx.ReclaimDuringAllocation) {
                    if (at == ESharedCacheHookPoint::AfterTombstoneOwnerClaim && item.Index() == ctx.ColdItem.Index()) {
                        ctx.Deleted = true;
                    }
                } else if (at == ESharedCacheHookPoint::AfterPageCollectionStateChecked &&
                           item == ctx.Collection.CacheItem() && !ctx.Visited) {
                    ctx.Visited = true;
                    ctx.Deleted = TSharedCacheTestAccess::EraseCold(ctx.Cache, ctx.ColdItem);
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(!TSharedCacheTestAccess::RelocatePage(*fixture.Cache, source));
        UNIT_ASSERT(context.Deleted);
        const auto state = TSharedCacheTestAccess::HandleState(*fixture.Cache, source.Index());
        UNIT_ASSERT(state.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
        UNIT_ASSERT(FindPage(*fixture.Cache, sourceKey).Status == ESharedCacheResultStatus::Miss);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->ResidentBytes(), residentBefore - (reclaimDuringAllocation ? bytes : 0));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection),
            ownersBefore - (reclaimDuringAllocation ? 1 : 0));
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freeBefore + (reclaimDuringAllocation ? 1 : 0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        structural.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, source.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->ResidentBytes(), residentBefore - (reclaimDuringAllocation ? 2 * bytes : bytes));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection),
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
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeTableLinkCas, source.CacheItem());
        gate.Slots[1].Arm(ESharedCacheHookPoint::BeforePageReplacementLinkCas, source.CacheItem());
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
            relocated = TSharedCacheTestAccess::RelocatePage(*fixture.Cache, source);
        });
        gate.Slots[1].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, source.Index()).IsReplacing());
        UNIT_ASSERT(
            !TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(*fixture.Cache, source.Index())).IsFrozen());
        gate.Slots[0].Release();
        insertion.Join();
        UNIT_ASSERT(TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(*fixture.Cache, source.Index())) ==
                    inserted.CacheItem());
        gate.Slots[1].Release();
        relocation.Join();
        UNIT_ASSERT(relocated);
        auto replacement = FindPage(*fixture.Cache, keys[0]);
        UNIT_ASSERT(replacement.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(replacement.Ref.CacheItem() != source);
        UNIT_ASSERT_VALUES_EQUAL(replacement.Ref.data(), sourceHold.Ref.data());
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertReviewTerminal(*fixture.Cache, inserted.CacheItem(), "R10");
    }

    void ReviewHeldPageWaitsForForwarder(bool querySticky) {
        TFixture fixture;
        const TLogoBlobID id(151, 152, 154);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto key = TSharedCacheKey::Page(collection, 1);
        const auto source = InsertReadyPage(*fixture.Cache, key);
        auto sourceHold = FindPage(*fixture.Cache, key);
        if (querySticky) {
            UNIT_ASSERT(TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, source));
            UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*fixture.Cache, source, source.Index()));
        }
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforePageReplacementLinkCas, source.CacheItem());
        gate.Slots[1].Arm(ESharedCacheHookPoint::BeforePageReplacementRetry, source.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        bool relocated = false;
        TGateThread relocation(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            relocated = TSharedCacheTestAccess::RelocatePage(*fixture.Cache, source);
        });
        gate.Slots[0].Wait();
        bool acquired = false;
        TTestSharedPageRef page;
        TGateThread reader(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            acquired = querySticky ? sourceHold.Ref.IsSticky()
                                   : TSharedCacheTestAccess::AcquirePage(*fixture.Cache, source, page);
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

    void ReviewBatchDetach(ESharedCacheHookPoint point) {
        TFixture fixture;
        const TLogoBlobID id(111, 112, 113);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        struct TContext {
            TFixture& Fixture;
            TCollectionCacheItem Collection;
            ESharedCacheHookPoint Point;
            bool Detached = false;
        } context{ fixture, collection, point };

        TTestTraits hooks{ &context, [](void* ptr, ESharedCacheHookPoint at, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (at == ctx.Point && item == ctx.Collection.CacheItem() && !ctx.Detached) {
                                  Y_ABORT_UNLESS(TSharedCacheTestAccess::CollectionReferences(
                                                     *ctx.Fixture.Cache, ctx.Collection) == 2);
                                  ctx.Detached = true;
                                  Y_ABORT_UNLESS(TSharedCacheTestAccess::DetachCollection(
                                      *ctx.Fixture.Cache, ctx.Fixture.Registry, ctx.Collection));
                              }
                          } };
        TSharedCacheHookGuard hookGuard(*fixture.Cache, hooks);
        TTestSharedCachePageRequest request(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        const bool admitted = fixture.Cache->FindOrInsertBatch(collection, { &request, 1 });
        UNIT_ASSERT(context.Detached);
        UNIT_ASSERT(admitted);
        UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(request.Fetch().CacheItem().Index())));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR4BatchSurvivesDetachAfterPreparation) {
        ReviewBatchDetach(ESharedCacheHookPoint::AfterPageBatchPrepared);
    }

    Y_UNIT_TEST(ReviewR4BatchSurvivesDetachBeforeOwnerIncrement) {
        ReviewBatchDetach(ESharedCacheHookPoint::AfterPageCollectionStateChecked);
    }

    Y_UNIT_TEST(ReviewR4BatchRejectsDetachedCollection) {
        TFixture fixture;
        const TLogoBlobID id(111, 112, 114);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        TTestSharedCachePageRequest request(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR4RejectedBatchReleasesCollectionOwner) {
        TFixture fixture;
        const TLogoBlobID id(111, 112, 115);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        TTestSharedCachePageRequest request(
            MakePageLocation(1, fixture.Cache->HardLimit()), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, { &request, 1 }));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR5TrimStaysColdAndPublishesReclaimablePage) {
        TFixture fixture;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(fixture.Cache->HardLimit()));
        UNIT_ASSERT(fixture.Cache->SoftLimit() > 0);
        const TLogoBlobID id(121, 122, 125);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * bytes));
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(0));
        UNIT_ASSERT(TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            return TSharedCacheTestAccess::TrimKeepCold(*fixture.Cache, op);
        }));
        const auto state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(state.IsCold());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ActivePageBytes(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& op) {
            return TSharedCacheTestAccess::ReclaimCold(*fixture.Cache, op);
        }));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(ReviewR5DemotionKeepsOriginalPageUntilAccounting) {
        TFixture fixture;
        const TLogoBlobID aId(121, 122, 123);
        const TLogoBlobID bId(124, 125, 126);
        const auto a = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(aId));
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, a, MakeCollection(aId)));
        const auto b = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(bId));
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, b, MakeCollection(bId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(a, ECacheMode::TryKeepInMemory));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * bytes));
        const auto sourceKey = TSharedCacheKey::Page(a, 1);
        const auto source = InsertReadyPage(*fixture.Cache, sourceKey);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, source));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, source.Index()).IsKeepCold());
        auto fillers = HoldReviewFillers(fixture, 63, sourceKey.Hash() & 63);
        const auto token = MakeColdItem(*fixture.Cache, source.Index());
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterKeepColdEntryMatched, token);
        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterKeepColdDemoted, token);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool reclaimed = false;
        TGateThread consumer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            reclaimed = TSharedCacheTestAccess::WithSpace(*cache, [&](TSpaceOperation& op) {
                return TSharedCacheTestAccess::ReclaimKeepCold(*cache, op);
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
            demoted = TSharedCacheTestAccess::DemoteKeepCold(*cache, token);
        });
        gate.Slots[1].Wait();
        gate.Slots[0].Release();
        consumer.Join();
        UNIT_ASSERT(!reclaimed);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, source.Index()).IsCold());
        UNIT_ASSERT(TSharedCacheTestAccess::WithSpace(*cache, [&](TSpaceOperation& op) {
            return TSharedCacheTestAccess::BindOperation(*cache, op).Eviction().EraseCold(token, ECacheTier::Regular);
        }));
        const auto state = TSharedCacheTestAccess::HandleState(*cache, source.Index());
        const bool reusedEarly = state.IsFree();
        TPageCacheItem replacement;
        TTestSharedPageRef replacementHold;
        ui32 replacementMembership = 0;
        const auto replacementKey = TSharedCacheKey::Page(b, 1);
        if (reusedEarly) {
            replacement = InsertReadyPage(*cache, replacementKey);
            UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), source.Index());
            UNIT_ASSERT(replacement.Version() != source.Version());
            auto found = FindPage(*cache, replacementKey);
            replacementHold = std::move(found.Ref);
            replacementMembership = TSharedCacheTestAccess::ColdVersion(*cache, replacement.Index());
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
            replacementMembership = TSharedCacheTestAccess::ColdVersion(*cache, replacement.Index());
        }
        UNIT_ASSERT_C(TSharedCacheTestAccess::ColdVersion(*cache, replacement.Index()) == replacementMembership,
            "R5: demotion changed the replacement page's membership generation");
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionValue(*cache, a.Index())->ActivePageBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR7KeepColdRoutingPinsPublishedPage) {
        TFixture fixture;
        const TLogoBlobID id(131, 132, 133);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * bytes));
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));

        struct TContext {
            TTestSharedCache& Cache;
            TPageCacheItem Page;
            bool Visited = false;
            bool Reclaimed = false;
            ui32 RefsAtPublication = 0;
        } context{ *fixture.Cache, page };

        TTestTraits hooks{ &context,
            [](void* ptr, ESharedCacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (at == ESharedCacheHookPoint::AfterKeepColdQueued && item.Index() == ctx.Page.Index() &&
                    !ctx.Visited) {
                    ctx.Visited = true;
                    ctx.RefsAtPublication = TSharedCacheTestAccess::HandleState(ctx.Cache, item.Index()).Refs();
                    ctx.Reclaimed = TSharedCacheTestAccess::WithSpace(ctx.Cache, [&](TSpaceOperation& op) {
                        return TSharedCacheTestAccess::ReclaimKeepCold(ctx.Cache, op);
                    });
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(context.Visited);
        UNIT_ASSERT_VALUES_EQUAL(context.RefsAtPublication, 1);
        UNIT_ASSERT(!context.Reclaimed);
        const auto state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), page.Version());
        UNIT_ASSERT(state.IsCold()); // The consumer withdrew the token while the routing pin was held.
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ActivePageBytes(), 0);
    }

    Y_UNIT_TEST(ReviewR7LastKeepColdReleaseDoesNotDemoteReusedHandle) {
        TFixture fixture;
        const TLogoBlobID aId(131, 132, 134);
        const TLogoBlobID bId(131, 132, 135);
        const auto a = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(aId));
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, a, MakeCollection(aId)));
        const auto b = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(bId));
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, b, MakeCollection(bId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(a, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(b, ECacheMode::TryKeepInMemory));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * bytes));
        const auto sourceKey = TSharedCacheKey::Page(a, 1);
        const auto source = InsertReadyPage(*fixture.Cache, sourceKey);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, source));
        auto held = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, source.CacheItem());
        UNIT_ASSERT(held);
        auto fillers = HoldReviewFillers(fixture, 63, sourceKey.Hash() & 63);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(a, ECacheMode::Regular));

        struct TContext {
            TTestSharedCache& Cache;
            TPageCacheItem Source;
            TCollectionCacheItem ReplacementCollection;
            bool Visited = false;
            bool Reclaimed = false;
            TPageCacheItem Replacement = {};
            ui32 ReplacementMembership = 0;
        } context{ *fixture.Cache, source, b };

        TTestTraits hooks{ &context,
            [](void* ptr, ESharedCacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (at == ESharedCacheHookPoint::AfterKeepColdRefReleased && item == ctx.Source.CacheItem() &&
                    !ctx.Visited) {
                    ctx.Visited = true;
                    ctx.Reclaimed = TSharedCacheTestAccess::WithSpace(ctx.Cache, [&](TSpaceOperation& op) {
                        return TSharedCacheTestAccess::ReclaimKeepCold(ctx.Cache, op);
                    });
                    Y_ABORT_UNLESS(ctx.Reclaimed);
                    ctx.Replacement = InsertReadyPage(ctx.Cache, TSharedCacheKey::Page(ctx.ReplacementCollection, 1));
                    Y_ABORT_UNLESS(ctx.Replacement.Index() == ctx.Source.Index());
                    Y_ABORT_UNLESS(TSharedCacheTestAccess::EvictFromHot(ctx.Cache, ctx.Replacement));
                    Y_ABORT_UNLESS(
                        ctx.Cache.SetCollectionPagesCacheMode(ctx.ReplacementCollection, ECacheMode::Regular));
                    ctx.ReplacementMembership = TSharedCacheTestAccess::ColdVersion(ctx.Cache, ctx.Replacement.Index());
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        held.Drop();
        UNIT_ASSERT(context.Visited && context.Reclaimed);
        UNIT_ASSERT(context.Replacement.Version() != source.Version());
        const auto state = TSharedCacheTestAccess::HandleState(*fixture.Cache, context.Replacement.Index());
        UNIT_ASSERT(state.IsKeepCold()); // The old release must not apply its policy to this new page.
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, context.Replacement.Index()),
            context.ReplacementMembership);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, b.Index())->ActivePageBytes(), bytes);
    }

    void ReviewColdPromotion(bool reheat) {
        TFixture fixture;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(fixture.Cache->HardLimit()));
        const TLogoBlobID id(141, 142, 143);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * bytes));
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 1);
        const auto coldItem = MakeColdItem(*fixture.Cache, page.Index());
        auto held = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem());
        UNIT_ASSERT(held);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()), coldItem.ColdVersion());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Page;
            bool Reheat;
            bool Visited = false;
            bool Reclaimed = false;
            ui32 RefsDuringPromotion = 0;
            TTestSharedPageRef Reader = {};
        } context{ *fixture.Cache, collection, page, reheat };

        TTestTraits hooks{ &context,
            [](void* ptr, ESharedCacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (at == ESharedCacheHookPoint::AfterKeepColdPromotion && item == ctx.Page.CacheItem() &&
                    !ctx.Visited) {
                    ctx.Visited = true;
                    ctx.RefsDuringPromotion = TSharedCacheTestAccess::HandleState(ctx.Cache, item.Index()).Refs();
                    if (ctx.Reheat) {
                        Y_ABORT_UNLESS(ctx.Cache.Find(ctx.Collection, 1, ctx.Reader) == ESharedCacheResultStatus::Hit);
                    }
                    ctx.Reclaimed = TSharedCacheTestAccess::WithSpace(ctx.Cache, [&](TSpaceOperation& op) {
                        return TSharedCacheTestAccess::ReclaimCold(ctx.Cache, op);
                    });
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        held.Drop();
        UNIT_ASSERT(context.Visited);
        UNIT_ASSERT_VALUES_EQUAL(context.RefsDuringPromotion, 1);
        UNIT_ASSERT(!context.Reclaimed);
        const auto state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), page.Version());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), reheat ? 1 : 0);
        UNIT_ASSERT(reheat ? state.IsHot() : state.IsKeepCold());
        const ui64 keepBytes = reheat ? 0 : bytes;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), keepBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdBytes(), keepBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdPages(), reheat ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), reheat ? bytes : 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), reheat ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ActivePageBytes(), bytes);
        if (!reheat) {
            UNIT_ASSERT(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()) & KeepColdQueuedMask);
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
            const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(collection);
            auto data = MakePageData(1);
            auto metadata = TIntrusiveConstPtr<NPageCollection::IPageCollection>(
                new TTestPageCollection(id, data, pagesReleasedBeforeCollection));
            UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
                *fixture.Cache, fixture.Registry, collection, MakeHolder<TCollection>(std::move(metadata))));
            const auto page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 1), data.size());
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
        TTestTraits hooks{ &notifications, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        auto owner = TSharedCacheTestAccess::LockHotResize(*fixture.Cache);
        UNIT_ASSERT(owner);
        UNIT_ASSERT(!TSharedCacheTestAccess::LockHotResize(*fixture.Cache));
        UNIT_ASSERT(!TSharedCacheTestAccess::LockHotResize(*fixture.Cache));
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
            auto uncontended = TSharedCacheTestAccess::LockHotResize(*fixture.Cache);
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
        TTestSharedCacheCollectionRef held;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(location, item, held) == ESharedCacheResultStatus::Inserted);
        if (publication != ECollectionPublication::Ready) {
            UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->Find(id, held) == ESharedCacheResultStatus::Hit);
            if (publication == ECollectionPublication::Attach) {
                UNIT_ASSERT(fixture.Cache->DetachCollection(item));
            }
        } else {
            TCollectionCacheItem pending;
            UNIT_ASSERT(fixture.Cache->FindOrInsert(location, pending, held) == ESharedCacheResultStatus::Pending);
        }
        observation.Cache = fixture.Cache.Get();
        observation.Item = item;
        observation.Id = id;
        observation.Attached = publication != ECollectionPublication::Detach;
        TTestTraits hooks{ &observation,
            [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                if (point != ESharedCacheHookPoint::ResourcesAvailable) {
                    return;
                }
                auto& observation = *static_cast<TObservation*>(context);
                ++observation.Notifications;
                const auto state = TSharedCacheTestAccess::HandleState(*observation.Cache, observation.Item.Index());
                const bool stable =
                    state.IsReady() && (observation.Attached ? state.IsStickyField() : state.IsStickyNoneField());
                TTestSharedCacheCollectionRef found;
                observation.Published &=
                    stable && observation.Cache->Find(observation.Id, found) == ESharedCacheResultStatus::Hit &&
                    found.CacheItem() == observation.Item;
            } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
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
                TTestSharedCacheCollectionRef hit;
                UNIT_ASSERT(fixture.Cache->FindOrInsert(location, inserted, hit) == ESharedCacheResultStatus::Hit);
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
            TTestSharedCacheCollectionRef hit;
            UNIT_ASSERT(fixture.Cache->FindOrInsert(location, inserted, hit) == ESharedCacheResultStatus::Hit);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), stableGeneration);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        fixture.Cache->DisarmResourceWait();
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
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
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(
            fixture.Cache->SetCollectionPagesCacheMode(collection, sticky ? ECacheMode::Sticky : ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));

        TTestSharedPageRef held;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
        TTestSharedPageRef hit;
        TTestPageFetch fetch;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1),
                        sticky ? EStickyState::Sticky : EStickyState::None, waiter, hit,
                        fetch) == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem page = fetch.CacheItem();
        auto data = MakePageData(page.Index());
        auto payload = data;
        UNIT_ASSERT(fetch.MakeReady(std::move(data)));
        UNIT_ASSERT(held);
        const THandleState resident = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(sticky ? resident.IsSticky() : resident.IsCold());
        if (sticky) {
            held.Drop();
        }

        TTestTraits hooks{ &notifications, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        constexpr ui64 reservedBytes = 1;
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, reservedBytes));
        fixture.Cache->ArmResourceWait(fixture.Cache->AdmissionGeneration());
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, reservedBytes);
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
                TTestSharedPageRef hit;
                TTestPageFetch fetch;
                TTestPageFetch duplicate;
                UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location,
                                EStickyState::None, first, hit, fetch) == ESharedCacheResultStatus::Inserted);
                UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location,
                                EStickyState::None, second, hit, duplicate) == ESharedCacheResultStatus::Pending);
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
        TTestSharedPageRef hit;
        TTestPageFetch first;
        TTestPageFetch second;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(10),
                        EStickyState::None, firstWaiter, hit, first) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(11),
                        EStickyState::None, secondWaiter, hit, second) == ESharedCacheResultStatus::Inserted);
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
            TTestSharedPageRef hit;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, MakeCollectionCacheItem(65, 66),
                            MakePageLocation(12), EStickyState::None, waiter, hit,
                            fetch) == ESharedCacheResultStatus::Inserted);
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
            TTestSharedPageRef hit;
            TTestPageFetch fetch;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None,
                            waiter, hit, fetch) == ESharedCacheResultStatus::Inserted);
            auto structural = TSharedCacheTestAccess::AcquirePending(
                *fixture.Cache, TSharedCacheKey::Page(collection, static_cast<ui64>(location.Offset)));
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
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(location, collection, hit) == ESharedCacheResultStatus::Inserted);
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
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        TSpaceHazard* hazard = nullptr;
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            hazard = &cacheOp.Hazard();
            UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
            TCacheItem item;
            UNIT_ASSERT(cacheOp.Find(key, item) == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(item == page.CacheItem());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
            {
                auto movedOp = std::move(cacheOp);
                UNIT_ASSERT(&movedOp.SpaceOperation() == static_cast<TSpaceOperation*>(&movedOp));
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
                TOperationItemRef<TTestTraits> acquired(movedOp);
                UNIT_ASSERT(movedOp.TryAcquire(item, false, acquired.Get()) ==
                            TTestSharedCache::TOperation::EAcquireStatus::Ready);
                UNIT_ASSERT(acquired.CacheItem() == item);
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
                UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(hazard->State.load(std::memory_order_acquire), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(hazard->State.load(std::memory_order_acquire), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
    }

    Y_UNIT_TEST(BorrowedCacheOperationPreservesExistingGuard) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        TSpaceHazard* hazard = nullptr;
        {
            auto spaceOp = fixture.Space->BeginOperation();
            hazard = &spaceOp.Hazard();
            {
                auto cacheOp = TSharedCacheTestAccess::BindOperation(*fixture.Cache, spaceOp);
                UNIT_ASSERT(&cacheOp.SpaceOperation() == &spaceOp);
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(hazard->State.load(std::memory_order_acquire)), 1);
                TCacheItem item;
                UNIT_ASSERT(cacheOp.Find(key, item) == ESharedCacheResultStatus::Hit);
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
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
    }

    Y_UNIT_TEST(BeginEraseAcquiresOwnerAndFinishErasePreservesHeldItem) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));

        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& cacheOp) {
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
                UNIT_ASSERT(cacheOp.Find(key, found) == ESharedCacheResultStatus::Miss);

                cacheOp.FinishErase(owner.CacheItem(), key);
                cacheOp.FinishErase(owner.CacheItem(), key);
                UNIT_ASSERT_VALUES_EQUAL(handle.State.load(std::memory_order_acquire), deleted.Raw());
            }
            const THandleState retained = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
            UNIT_ASSERT(retained.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(retained.Refs(), 1);
        });
        const THandleState released = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(released.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(released.Version(), AdvanceItemVersion(page.Version()));
    }

    Y_UNIT_TEST(BeginEraseReusesOwnerWithoutReferenceIncrement) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));

        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& cacheOp) {
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
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
    }

    Y_UNIT_TEST(InsertFindAndRollback) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 100);
        const auto candidate = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(candidate);

        auto inserted = FindOrInsertPage(*fixture.Cache, key, candidate);
        UNIT_ASSERT(inserted.Status == ESharedCacheResultStatus::Inserted);
        inserted.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(found.Ref);
        UNIT_ASSERT(found.Ref.CacheItem() == candidate);
        found.Ref.Drop();

        const auto duplicate = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(duplicate);
        auto existing = FindOrInsertPage(*fixture.Cache, key, duplicate);
        UNIT_ASSERT(existing.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(existing.Ref);
        existing.Ref.Drop();
    }
    Y_UNIT_TEST(PageFindOrInsertAllocatesOnlyAfterMiss) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 104);
        const ui64 freeBefore = TSharedCacheTestAccess::FreeCount(*fixture.Cache);

        TPageCacheItem inserted;
        TTestSharedPageRef page;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key),
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EStickyState::None,
                        inserted, page) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!page);
        const ui64 spare =
            TSharedCacheTestAccess::SpareItem(*fixture.Cache, TSharedCacheTestAccess::HazardCount(*fixture.Cache) - 1);
        UNIT_ASSERT(spare != 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);

        TPageCacheItem pendingItem;
        TTestSharedPageRef pendingPage;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key),
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EStickyState::None,
                        pendingItem, pendingPage) == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT(!pendingItem);
        UNIT_ASSERT(!pendingPage);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::SpareItem(
                                     *fixture.Cache, TSharedCacheTestAccess::HazardCount(*fixture.Cache) - 1), spare);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);

        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        TPageCacheItem duplicate;
        TTestSharedPageRef hitPage;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key),
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EStickyState::None,
                        duplicate, hitPage) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hitPage.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);
        hitPage.Drop();
    }
    Y_UNIT_TEST(PageFindOrInsertSubscribesAndCompletesFetch) {
        TFixture fixture;
        const TLogoBlobID id(61, 62, 63);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = NTable::NPage::TPageLocation::FromPageIndex(7, 4096, NTable::NPage::EPage::DataPage, 123);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        TTestSharedPageRef hit;
        TTestPageFetch fetch;
        TIntrusivePtr<TPageFetchWaiter> first = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::Sticky,
                        first, hit, fetch) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT(!hit);
        UNIT_ASSERT(fetch.Location() == location);
        const TPageCacheItem page = fetch.CacheItem();
        UNIT_ASSERT(fetch.Dispatch());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsRequested());

        TTestPageFetch duplicateFetch;
        TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::Sticky,
                        second, hit, duplicateFetch) == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT(!duplicateFetch);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);

        UNIT_ASSERT(fetch.MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(!fetch);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 2);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), page.CacheItem().Raw());

        UNIT_ASSERT(
            fixture.Cache->Find(collection, static_cast<ui64>(location.Offset), hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hit.CacheItem() == page);
        hit.Drop();
    }
    Y_UNIT_TEST(ReviewR13WrittenBatchCoalescesAndAccountsOnce) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 121);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        const ui64 overallBefore = fixture.Cache->OverallUsage();
        auto body = MakePageData(1);
        const char* pointer = body.data();
        auto duplicateBody = MakePageData(1);
        std::atomic<ui32> completed = 0, ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(MakePageLocation(1), new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertWrittenBatch(*fixture.Cache, collection, requests));
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overallBefore + bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 1);
        UNIT_ASSERT(requests[0].Fetch().MakeReady(std::move(body)));
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
        UNIT_ASSERT(duplicateBody);
        TTestSharedCachePageRequest hit(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertWrittenBatch(*fixture.Cache, collection, { &hit, 1 }));
        UNIT_ASSERT(hit.Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(hit.Page().data(), pointer);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
    }

    Y_UNIT_TEST(ReviewR13WrittenAdmissionSurvivesEvictedPresenceSnapshot) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 122);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const auto source = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, source));
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
            [](void* ptr, ESharedCacheHookPoint at, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(ptr);
                if (at == ESharedCacheHookPoint::AfterPageBatchPrepared && item == ctx.Collection.CacheItem() &&
                    !ctx.Evicted) {
                    ctx.Evicted = true;
                    Y_ABORT_UNLESS(TSharedCacheTestAccess::EraseCold(
                        *ctx.Fixture.Cache, MakeColdItem(*ctx.Fixture.Cache, ctx.Source.Index())));
                    Y_ABORT_UNLESS(TSharedCacheTestAccess::TryReserve(*ctx.Fixture.Cache, ctx.Bytes));
                    Y_ABORT_UNLESS(ctx.Fixture.Cache->SetCollectionPagesCacheMode(ctx.Collection, ECacheMode::Sticky));
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        TTestSharedCachePageRequest request(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertWrittenBatch(*fixture.Cache, collection, { &request, 1 }));
        UNIT_ASSERT(context.Evicted);
        UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Inserted);
        const auto page = request.Fetch().CacheItem();
        UNIT_ASSERT(request.Fetch().MakeReady(std::move(body)));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
    }

    Y_UNIT_TEST(ReviewR13WrittenBatchRollsBackHandlePressure) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 123);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        TVector<TTestSharedPageRef> held;
        for (ui64 offset = 100; AvailableFreeHandles(*fixture.Cache) > 1; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            TTestSharedPageRef page;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, page) == ESharedCacheResultStatus::Hit);
            held.push_back(std::move(page));
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(1));
        const ui64 usageBefore = fixture.Cache->PayloadUsage();
        const ui64 overallBefore = fixture.Cache->OverallUsage();
        const std::array bodies{ MakePageData(1), MakePageData(2) };
        TVector<TTestSharedCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), MakeIntrusive<TPageFetchWaiter>(), true);
        }
        UNIT_ASSERT(!TSharedCacheTestAccess::FindOrInsertWrittenBatch(*fixture.Cache, collection, requests));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), usageBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overallBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT(bodies[0] && bodies[1]);
    }

    TTestSharedCacheCollectionRef MakeReviewRetainedCollection(TFixture & fixture, const TLogoBlobID& id) {
        TCollectionCacheItem item;
        TTestSharedCacheCollectionRef ref;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(TCollectionLocation{ .Id = id }, item, ref) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(item, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->Find(id, ref) == ESharedCacheResultStatus::Hit);
        return ref;
    }

    void AddReviewRetainedPage(TFixture & fixture, TCollectionCacheItem collection, ui64 offset) {
        TPageCacheItem item;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset, 16), EStickyState::Sticky,
                        item, hit) == ESharedCacheResultStatus::Inserted);
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
        const TSharedCacheHandleRequirement requirement{ main->Id(), pages };
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
        const TSharedCacheHandleRequirement requirement{ main->Id(), pages };
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
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, retired.CacheItem()));
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(
            *fixture.Cache, MakeColdItem(*fixture.Cache, retired.CacheItem().Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, retired.CacheItem().Index()).IsTombstone());
        auto current = MakeReviewRetainedCollection(fixture, id);
        UNIT_ASSERT(retired.CacheItem() != current.CacheItem());
        TVector<NTable::NPage::TPageLocation> pages;
        for (ui64 offset = 100;
             pages.size() < fixture.Cache->TargetHandleCount() - TTestSharedCache::RequestHandleOverhead; ++offset) {
            pages.push_back(MakePageLocation(offset, 16));
        }
        const TSharedCacheHandleRequirement requirement{ id, pages };
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
            TSharedCacheHandleRequirement Requirement{ TLogoBlobID(207, 208, 1), {} };
            ui32 Notifications = 0;
            bool Published = true;
        } observation;

        TFixture fixture(5);
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        TPageCacheItem page;
        TCollectionCacheItem collection;
        TVector<TCollectionCacheItem> others;
        Y_DEFER {
            TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            if (page) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
            }
            if (collection) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, collection);
            }
            for (const auto item : others) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        if (reserved) {
            page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(MakeCollectionCacheItem(579, 580), 0), 16);
            UNIT_ASSERT(page);
        } else {
            collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(209, 210, 1)));
            UNIT_ASSERT(collection);
        }
        for (ui32 index = 0; index < fixture.Capacity.HandleCount() - 3; ++index) {
            const auto item =
                AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(211, 212, index + 1)));
            UNIT_ASSERT(item);
            others.push_back(item);
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        observation.Cache = fixture.Cache.Get();
        TTestTraits hooks{ &observation, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  auto& observation = *static_cast<TObservation*>(context);
                                  ++observation.Notifications;
                                  observation.Published &=
                                      observation.Cache->ReservedBytes() == 0 &&
                                      observation.Cache->CanAllocateHandles({ &observation.Requirement, 1 });
                                  const auto candidate = AllocateCollection(*observation.Cache,
                                      TSharedCacheKey::Collection(observation.Requirement.CollectionId));
                                  observation.Published &= static_cast<bool>(candidate);
                                  if (candidate) {
                                      TSharedCacheTestAccess::DiscardCandidate(*observation.Cache, candidate);
                                  }
                              }
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforeReturn) {
            fixture.Cache->ArmResourceWait(generation);
        }
        if (reserved) {
            TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
            page = {};
        } else {
            TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, collection);
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
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        TVector<TCollectionCacheItem> held;
        Y_DEFER {
            TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            for (const auto item : held) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        for (ui32 index = 0; index < fixture.Capacity.HandleCount() - 2; ++index) {
            const auto item =
                AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(213, 214, index + 1)));
            UNIT_ASSERT(item);
            held.push_back(item);
        }
        TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, held.back());
        held.pop_back();
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            cacheOp.RefillSpareItem();
        }
        const ui64 spare = TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0);
        UNIT_ASSERT(spare);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), 0);
        TTestTraits hooks{ &notifications, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        const std::array locations{ MakePageLocation(0, 16) };
        const TLogoBlobID id(215, 216, 1);
        const TSharedCacheHandleRequirement one{ id, {} };
        const TSharedCacheHandleRequirement two{ id, locations };
        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            UNIT_ASSERT(fixture.Cache->CanAllocateHandles({ &one, 1 }));
            UNIT_ASSERT(!fixture.Cache->CanAllocateHandles({ &two, 1 }));
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), 0);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0), spare);
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
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(2 * bytes));
        const auto collection = MakeCollectionCacheItem(581, 582);
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui64 state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Raw();
        const ui64 free = AvailableFreeHandles(*fixture.Cache);
        const ui64 overall = fixture.Cache->OverallUsage();
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        TTestTraits hooks{ &notifications, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
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
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Raw(), state);
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
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const auto collection = MakeCollectionCacheItem(583, 584);
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        for (ui32 touch = 0; touch < 2; ++touch) {
            TTestSharedPageRef held;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ESharedCacheResultStatus::Hit);
        }
        for (ui32 index = 0; index < fixture.Capacity.HandleCount() - 3; ++index) {
            const TLogoBlobID id(217, 218, index + 1);
            const auto item = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(item);
            UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, item, MakeCollection(id)));
        }
        const TSharedCacheHandleRequirement requirement{ TLogoBlobID(219, 220, 1), {} };
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT(!fixture.Cache->CanAllocateHandles({ &requirement, 1 }));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Frequency(), 2);
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
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        TTransition growth;
        TVector<TCollectionCacheItem> held;
        Y_DEFER {
            TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            if (observation.Claimed) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, observation.Claimed);
            }
            for (const auto item : held) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        const auto fillHandles = [&] {
            while (AvailableFreeHandles(*fixture.Cache) != 0) {
                const auto item = AllocateCollection(
                    *fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(221, 222, held.size() + 1)));
                UNIT_ASSERT(item);
                held.push_back(item);
            }
        };
        fillHandles();
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        do {
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        } while (growth.Phase() == ETransitionPhase::InitializeGrowth);
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, addressBits + 1));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        observation.MinimumIndex = fixture.Capacity.HandleCount();
        if (laterBatch) {
            UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            UNIT_ASSERT(growth.Phase() == ETransitionPhase::AppendGrowth);
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), SharedCacheTransitionWorkBatch);
            fillHandles();
            observation.MinimumIndex += SharedCacheTransitionWorkBatch;
        }
        observation.Cache = fixture.Cache.Get();
        TTestTraits hooks{ &observation, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point != ESharedCacheHookPoint::ResourcesAvailable) {
                                  return;
                              }
                              auto& observation = *static_cast<TObservation*>(context);
                              ++observation.Notifications;
                              {
                                  auto lock = TSharedCacheTestAccess::LockHotResize(*observation.Cache);
                                  observation.Unlocked = static_cast<bool>(lock);
                              }
                              observation.Claimed = AllocateCollection(
                                  *observation.Cache, TSharedCacheKey::Collection(TLogoBlobID(223, 224, 1)));
                              observation.Published =
                                  observation.Claimed && observation.Claimed.Index() >= observation.MinimumIndex;
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        TTransition idle;
        UNIT_ASSERT(!TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, idle));
        UNIT_ASSERT(!TSharedCacheTestAccess::AppendFreeHandles(*fixture.Cache, idle));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 0);
        if (laterBatch) {
            UNIT_ASSERT(TSharedCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
        } else {
            UNIT_ASSERT(!TSharedCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
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
        UNIT_ASSERT(!TSharedCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
        UNIT_ASSERT(!TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), afterPublication);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, growth));
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
        const TSharedCacheHandleRequirement requirement{ TLogoBlobID(225, 226, 1), {} };
        while (cache.CanAllocateHandles({ &requirement, 1 })) {
            const auto item =
                AllocateCollection(cache, TSharedCacheKey::Collection(TLogoBlobID(227, 228, held.size() + 1)));
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
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        RefillWorkerSpare(*fixture.Cache, 0);
        const auto spare = TCacheItem::FromRaw(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0));
        UNIT_ASSERT(!spare.IsNull());
        auto held = AllocateAvailableHandleCandidates(*fixture.Cache);
        TCollectionCacheItem claimed;
        Y_DEFER {
            TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            if (claimed) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, claimed);
            }
            for (const auto item : held) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        const TSharedCacheHandleRequirement requirement{ TLogoBlobID(229, 230, 1), {} };
        UNIT_ASSERT(!fixture.Cache->CanAllocateHandles({ &requirement, 1 }));
        observation.Cache = fixture.Cache.Get();
        observation.Index = spare.Index();
        TTestTraits hooks{ &observation,
            [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                    auto& observation = *static_cast<TObservation*>(context);
                    ++observation.Notifications;
                    observation.Published =
                        TSharedCacheTestAccess::FreeCount(*observation.Cache) != 0 &&
                        TSharedCacheTestAccess::HandleState(*observation.Cache, observation.Index).IsFree();
                }
            } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        fixture.Cache->ArmResourceWait(fixture.Cache->AdmissionGeneration());
        if (controller) {
            UNIT_ASSERT(fixture.Cache->ReclaimSpareHandle());
            UNIT_ASSERT(fixture.Cache->CanAllocateHandles({ &requirement, 1 }));
        }
        claimed = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(requirement.CollectionId));
        UNIT_ASSERT(claimed);
        UNIT_ASSERT_VALUES_EQUAL(claimed.Index(), spare.Index());
        UNIT_ASSERT_VALUES_EQUAL(claimed.Version(), spare.Version());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0), 0);
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
        const ui64 spare = TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0);
        UNIT_ASSERT(spare);
        const auto first = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(231, 232, 1)));
        const auto second = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(231, 232, 2)));
        UNIT_ASSERT(first && second);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0), spare);
        TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, first);
        TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, second);
    }

    Y_UNIT_TEST(OwnerAndPressureRecoveryClaimSpareOnce) {
        for (ui32 attempt = 0; attempt < 32; ++attempt) {
            TFixture fixture(5, 2);
            TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
            RefillWorkerSpare(*fixture.Cache, 0);
            const auto spare = TCacheItem::FromRaw(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0));
            UNIT_ASSERT(!spare.IsNull());
            auto held = AllocateAvailableHandleCandidates(*fixture.Cache);
            TCollectionCacheItem owner;
            TCollectionCacheItem pressure;
            Y_DEFER {
                if (owner) {
                    TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, owner);
                }
                if (pressure) {
                    TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, pressure);
                }
                for (const auto item : held) {
                    TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
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
                owner = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(233, 234, 1)));
            });
            start.store(true, std::memory_order_release);
            pressure = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(233, 234, 2)));
            worker.join();
            UNIT_ASSERT(static_cast<bool>(owner) != static_cast<bool>(pressure));
            UNIT_ASSERT_VALUES_EQUAL((owner ? owner : pressure).Index(), spare.Index());
            UNIT_ASSERT_VALUES_EQUAL((owner ? owner : pressure).Version(), spare.Version());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0), 0);
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        }
    }

    void CheckSparePublication(bool recycle, bool armBeforePublication) {
        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            ui32 Notifications = 0;
            bool RearmedBeforeStore = false;
            bool Published = false;
        } observation;

        TFixture fixture(5, 1);
        TCollectionCacheItem candidate;
        Y_DEFER {
            TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
            if (candidate) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, candidate);
            }
        };
        if (recycle) {
            auto cacheOp = fixture.Cache->BeginOperation();
            const auto item = cacheOp.Allocate();
            UNIT_ASSERT(!item.IsNull());
            cacheOp.Items().InitializeCollection(item, TLogoBlobID(235, 236, 1), 0);
            candidate = TCollectionCacheItem::FromValidated(item);
        }
        observation.Cache = fixture.Cache.Get();
        TTestTraits hooks{ &observation, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point != ESharedCacheHookPoint::ResourcesAvailable) {
                                  return;
                              }
                              auto& observation = *static_cast<TObservation*>(context);
                              ++observation.Notifications;
                              if (TSharedCacheTestAccess::SpareItem(*observation.Cache, 0) == 0) {
                                  observation.RearmedBeforeStore = true;
                                  observation.Cache->ArmResourceWait(observation.Cache->AdmissionGeneration());
                              } else {
                                  observation.Published = true;
                              }
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            if (recycle) {
                cacheOp.RecycleCandidate(candidate.CacheItem());
                candidate = {};
            } else {
                cacheOp.RefillSpareItem();
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforePublication ? (recycle ? 2 : 1) : 0);
        if (!armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT(observation.Published);
        UNIT_ASSERT_VALUES_EQUAL(observation.RearmedBeforeStore, recycle && armBeforePublication);
        const ui32 notifications = observation.Notifications;
        const ui64 afterPublication = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(afterPublication);
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            cacheOp.RefillSpareItem();
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, notifications);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), afterPublication);
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), fixture.Capacity.HandleCount() - 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }

    Y_UNIT_TEST(SpareRefillNotifiesAfterPublication) {
        for (bool armed : { false, true }) {
            CheckSparePublication(false, armed);
        }
    }

    Y_UNIT_TEST(SpareRecycleNotifiesAfterPublication) {
        for (bool armed : { false, true }) {
            CheckSparePublication(true, armed);
        }
    }

    Y_UNIT_TEST(SpareRecoveryQuarantinesShrinkCutHandles) {
        TFixture fixture(6, 2);
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        TSharedCacheCapacity target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, 2, target));
        TVector<TCollectionCacheItem> held;
        Y_DEFER {
            for (const auto item : held) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        for (ui32 index = 0; index < target.HandleCount() - 2; ++index) {
            const auto item =
                AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(237, 238, index + 1)));
            UNIT_ASSERT(item);
            UNIT_ASSERT(item.Index() < target.HandleCount());
            held.push_back(item);
        }
        RefillWorkerSpare(*fixture.Cache, 0);
        const std::array spares{ TCacheItem::FromRaw(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0)),
            TCacheItem::FromRaw(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 1)) };
        for (const auto spare : spares) {
            UNIT_ASSERT(!spare.IsNull() && spare.Index() >= target.HandleCount());
        }
        TTransition shrink;
        UNIT_ASSERT(
            TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, target, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, target.AddressBits));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            UNIT_ASSERT_VALUES_EQUAL(cacheOp.SpaceOperation().TryAllocateHandle(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), 0);
        UNIT_ASSERT(fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT(fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT(!fixture.Cache->ReclaimSpareHandle());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), 0);
        for (const auto spare : spares) {
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, spare.Index()).IsFree());
        }
        UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, shrink));
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
            TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
        };
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        if (finalDrain) {
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            UNIT_ASSERT(
                TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, fixture.ReservedCapacity.AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);
        }
        observation.Cache = fixture.Cache.Get();
        observation.Hazard = TlsCacheHazard;
        TTestTraits hooks{ &observation, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point != ESharedCacheHookPoint::ResourcesAvailable) {
                                  return;
                              }
                              auto& observation = *static_cast<TObservation*>(context);
                              ++observation.Notifications;
                              observation.Cleared &= observation.Hazard->State.load(std::memory_order_acquire) == 0;
                              auto cacheOp = observation.Cache->BeginOperation();
                              observation.CurrentView &=
                                  cacheOp.SpaceOperation().EntryGeneration() == observation.PublishedGeneration;
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        ui64 generation = 0;
        {
            auto original = fixture.Cache->BeginOperation();
            auto moved = std::move(original);
            if (finalDrain) {
                UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            } else {
                UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            }
            observation.PublishedGeneration = TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Generation();
            UNIT_ASSERT(moved.SpaceOperation().EntryGeneration() != observation.PublishedGeneration);
            generation = fixture.Cache->AdmissionGeneration();
            if (armBeforeDrain) {
                fixture.Cache->ArmResourceWait(generation);
            }
            if (nested) {
                auto inner = fixture.Cache->BeginOperation();
                UNIT_ASSERT(!TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
                UNIT_ASSERT_VALUES_EQUAL(SpaceHazardActiveCount(observation.Hazard->State.load()), 2);
            } else {
                UNIT_ASSERT(!TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
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
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        if (!finalDrain) {
            UNIT_ASSERT(
                TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, fixture.ReservedCapacity.AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, growth));
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
            TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
        };
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        TTestTraits hooks{ &notifications, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        if (currentGeneration) {
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            auto guard = fixture.Cache->BeginOperation();
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            UNIT_ASSERT(!(TlsCacheHazard->State.load() & SpaceHazardDrainWaiter));
        } else {
            {
                auto guard = fixture.Cache->BeginOperation();
                UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            }
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, fixture.ReservedCapacity.AddressBits));
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
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(TCollectionLocation{ .Id = id }, collection, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        TVector<TTestSharedPageRef> held;
        for (ui64 offset = 100; AvailableFreeHandles(*fixture.Cache) > 1; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            TTestSharedPageRef page;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, page) == ESharedCacheResultStatus::Hit);
            held.push_back(std::move(page));
        }
        const std::array locations{ MakePageLocation(1), MakePageLocation(2) };
        const TSharedCacheHandleRequirement requirement{ id, locations };
        const ui64 bytesBefore = fixture.Cache->PayloadUsage();
        UNIT_ASSERT(!fixture.Cache->CanAllocateHandles({ &requirement, 1 }));
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytesBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);

        std::atomic<ui32> completed = 0, ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(locations[0], new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        const TSharedCacheHandleRequirement pending{ id, { &locations[0], 1 } };
        UNIT_ASSERT(fixture.Cache->CanAllocateHandles({ &pending, 1 }));
        UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(1)));
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
    }

    Y_UNIT_TEST(ReviewR15PresenceRaceReportsOnlyCompletedAdmissions) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 125);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        const auto source = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 2));
        TVector<TTestSharedPageRef> held;
        TTestSharedPageRef first;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, first) == ESharedCacheResultStatus::Hit);
        TTestSharedPageRef second;
        UNIT_ASSERT(fixture.Cache->Find(collection, 2, second) == ESharedCacheResultStatus::Hit);
        for (ui64 offset = 100; AvailableFreeHandles(*fixture.Cache) != 0; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            TTestSharedPageRef page;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, page) == ESharedCacheResultStatus::Hit);
            held.push_back(std::move(page));
        }

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Source;
            TTestSharedPageRef& SourceHold;
            TTestSharedPageRef Replacement;
            bool Evicted = false;
        } context{ *fixture.Cache, collection, source, first, {}, false };

        TTestTraits hooks{ &context, [](void* opaque, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              auto& ctx = *static_cast<TContext*>(opaque);
                              if (point == ESharedCacheHookPoint::AfterPageBatchPrepared && !ctx.Evicted) {
                                  ctx.Evicted = true;
                                  ctx.SourceHold.Drop();
                                  if (TSharedCacheTestAccess::HandleState(ctx.Cache, ctx.Source.Index()).IsHot()) {
                                      Y_ABORT_UNLESS(TSharedCacheTestAccess::EvictFromHot(ctx.Cache, ctx.Source));
                                  }
                                  Y_ABORT_UNLESS(TSharedCacheTestAccess::EraseCold(
                                      ctx.Cache, MakeColdItem(ctx.Cache, ctx.Source.Index())));
                                  InsertReadyPage(ctx.Cache, TSharedCacheKey::Page(ctx.Collection, 9000));
                                  Y_ABORT_UNLESS(ctx.Cache.Find(ctx.Collection, 9000, ctx.Replacement) ==
                                                 ESharedCacheResultStatus::Hit);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        const auto before = fixture.Cache->Stats();
        TVector<TTestSharedCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), MakeIntrusive<TPageFetchWaiter>());
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(context.Evicted);
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Miss);
        UNIT_ASSERT(requests[1].Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().RequestedPages, before.RequestedPages + 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().HitPages, before.HitPages + 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().MissPages, before.MissPages);

        context.Replacement.Drop();
        TTestSharedCachePageRequest retry(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, { &retry, 1 }, true, true));
        UNIT_ASSERT(retry.Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(retry.Fetch().MakeReady(MakePageData(1)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().RequestedPages, before.RequestedPages + 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().MissPages, before.MissPages + 1);
    }

    void CheckPreloadHandlePressure(ui32 freeHandles) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 126);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        TVector<TPageCacheItem> resident;
        for (ui64 offset = 100; AvailableFreeHandles(*fixture.Cache) > freeHandles; ++offset) {
            resident.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
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
        TVector<TTestSharedCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), MakeIntrusive<TPageFetchWaiter>());
        }
        UNIT_ASSERT(fixture.Cache->CanReserve(2 * (4096 + NActors::TSharedData::OverheadSize)));
        UNIT_ASSERT(!fixture.Cache->FindOrInsertPreloadBatch(collection, requests));
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), freeHandles);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), reservations);
        for (ui32 index = 0; index < resident.size(); ++index) {
            AssertPageHit(*fixture.Cache, TSharedCacheKey::Page(collection, index + 100), resident[index]);
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
        const auto keep = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(keepId));
        const auto regular = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(regularId));
        const auto sticky = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(stickyId));
        UNIT_ASSERT(fixture.Cache->MakeReady(keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(sticky, MakeCollection(stickyId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(16 * 4096));
        const auto hotKeep = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(keep, 1));
        const auto keepCold = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(keep, 2));
        const auto coldKeep = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(keep, 3));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, keepCold));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, coldKeep));
        UNIT_ASSERT(
            TSharedCacheTestAccess::DemoteKeepCold(*fixture.Cache, MakeColdItem(*fixture.Cache, coldKeep.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, keepCold.Index()).IsKeepCold());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, coldKeep.Index()).IsCold());
        const auto victim = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, 1));
        InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, 2));
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->Find(regular, 2, held) == ESharedCacheResultStatus::Hit);
        if (cold) {
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, victim));
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, victim.Index()).IsCold());
        }
        TTestSharedCachePageRequest stickyRequest(MakePageLocation(1), MakeIntrusive<TPageFetchWaiter>(), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(sticky, { &stickyRequest, 1 }));
        const auto stickyPage = stickyRequest.Fetch().CacheItem();
        UNIT_ASSERT(stickyRequest.Fetch().MakeReady(MakePageData(1)));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, stickyPage.Index()).IsSticky());
        for (ui64 id = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++id) {
            const TLogoBlobID fillerId(83, 84, id);
            const auto filler = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }
        const auto hotState = TSharedCacheTestAccess::HandleState(*fixture.Cache, hotKeep.Index());
        const auto keepColdState = TSharedCacheTestAccess::HandleState(*fixture.Cache, keepCold.Index());
        const auto coldState = TSharedCacheTestAccess::HandleState(*fixture.Cache, coldKeep.Index());
        TTestSharedCachePageRequest request(MakePageLocation(100), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
        UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(100)));
        TTestSharedPageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(regular, 1, missing) == ESharedCacheResultStatus::Miss);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, hotKeep.Index()).Raw(), hotState.Raw());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, keepCold.Index()).Raw(), keepColdState.Raw());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, coldKeep.Index()).Raw(), coldState.Raw());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, stickyPage.Index()).IsSticky());
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
        const TLogoBlobID keepId(83, 84, 131), regularId(83, 84, 132);
        const auto keep = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(keepId));
        const auto regular = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(regularId));
        UNIT_ASSERT(fixture.Cache->MakeReady(keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        const auto victim = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, 1));
        for (ui64 id = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++id) {
            const TLogoBlobID fillerId(83, 84, id);
            const auto filler = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Victim;
            TTestSharedPageRef Held;
            bool Acquired = false;
        } context{ *fixture.Cache, regular, victim, {} };

        TTestTraits hooks{ &context, [](void* opaque, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(opaque);
                              if (point == ESharedCacheHookPoint::BeforePreloadPageErase &&
                                  item == ctx.Victim.CacheItem() && !ctx.Acquired) {
                                  ctx.Acquired = true;
                                  Y_ABORT_UNLESS(
                                      ctx.Cache.Find(ctx.Collection, 1, ctx.Held) == ESharedCacheResultStatus::Hit);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        TTestSharedCachePageRequest request(MakePageLocation(100), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(!fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(context.Acquired && context.Held);
        UNIT_ASSERT_VALUES_EQUAL(context.Held.data()[0], static_cast<char>(victim.Index()));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
        context.Held.Drop();
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Inserted);
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
        const TLogoBlobID keepId(83, 84, 133), regularId(83, 84, 134);
        const auto keep = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(keepId));
        const auto regular = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(regularId));
        UNIT_ASSERT(fixture.Cache->MakeReady(keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        const auto first = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, 1));
        const auto second = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, 2));
        if (protectedPromotion) {
            const ui64 slots = fixture.Cache->BeginOperation().SpaceOperation().Hot(EHotLevel::L2).Capacity;
            for (ui64 offset = 1000; offset < 1000 + slots; ++offset) {
                const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(keep, offset));
                MovePreloadTestPageToLevel(*fixture.Cache, page, EHotLevel::L2);
            }
        }
        if (cold) {
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, second));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, first));
        } else {
            for (ui32 hit = 0; hit < 2; ++hit) {
                TTestSharedPageRef page;
                UNIT_ASSERT(fixture.Cache->Find(regular, 1, page) == ESharedCacheResultStatus::Hit);
            }
        }
        for (ui64 id = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++id) {
            const TLogoBlobID fillerId(83, 84, id);
            const auto filler = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }
        TTestSharedCachePageRequest request(MakePageLocation(100), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(100)));
        TTestSharedPageRef survivor, victim;
        UNIT_ASSERT(fixture.Cache->Find(regular, 2, victim) == ESharedCacheResultStatus::Miss);
        UNIT_ASSERT(fixture.Cache->Find(regular, 1, survivor) == ESharedCacheResultStatus::Hit);
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

    Y_UNIT_TEST(PreloadPreservesSecondChanceWhenKeepPagesBlockPromotion) {
        CheckPreloadVictimOrder(false, true);
    }

    Y_UNIT_TEST(PreloadKeepsDisplacedFrequentPageInItsSourceTier) {
        TFixture fixture;
        const TLogoBlobID keepId(83, 84, 136), regularId(83, 84, 137);
        const auto keep = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(keepId));
        const auto regular = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(regularId));
        UNIT_ASSERT(fixture.Cache->MakeReady(keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        const auto first = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, 1));
        const auto second = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, 2));
        MovePreloadTestPageToLevel(*fixture.Cache, first, EHotLevel::L0);
        MovePreloadTestPageToLevel(*fixture.Cache, second, EHotLevel::L2);
        for (ui32 level : { 2, 3 }) {
            const ui64 slots =
                fixture.Cache->BeginOperation().SpaceOperation().Hot(static_cast<EHotLevel>(level)).Capacity;
            for (ui64 slot = level == 2 ? 1 : 0; slot < slots; ++slot) {
                const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(keep, level * 1000 + slot));
                MovePreloadTestPageToLevel(*fixture.Cache, page, static_cast<EHotLevel>(level));
            }
        }
        for (ui32 hit = 0; hit < 3; ++hit) {
            TTestSharedPageRef page;
            UNIT_ASSERT(fixture.Cache->Find(regular, hit == 0 ? 1 : 2, page) == ESharedCacheResultStatus::Hit);
        }
        for (ui64 id = 1000; AvailableFreeHandles(*fixture.Cache) != 0; ++id) {
            const TLogoBlobID fillerId(83, 84, id);
            const auto filler = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(fillerId));
            UNIT_ASSERT(fixture.Cache->MakeReady(filler, MakeCollection(fillerId)));
        }
        TTestSharedCachePageRequest request(MakePageLocation(100), MakeIntrusive<TPageFetchWaiter>());
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(keep, { &request, 1 }));
        UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(100)));
        TTestSharedPageRef victim, survivor;
        UNIT_ASSERT(fixture.Cache->Find(regular, 1, victim) == ESharedCacheResultStatus::Miss);
        UNIT_ASSERT(fixture.Cache->Find(regular, 2, survivor) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(survivor.CacheItem() == second);
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
    }

    Y_UNIT_TEST(PreloadPreservesSameVersionKeepTokenWhenAnotherThreadReheatsIt) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 135);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(16 * 4096));
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        MovePreloadTestPageToLevel(*fixture.Cache, page, EHotLevel::L0);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TSharedCacheTestAccess::DemoteKeepCold(*fixture.Cache, MakeColdItem(*fixture.Cache, page.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Page;
            bool Reheated = false;
        } context{ *fixture.Cache, collection, page };

        TTestTraits hooks{ &context,
            [](void* opaque, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                auto& ctx = *static_cast<TContext*>(opaque);
                if (point == ESharedCacheHookPoint::AfterPreloadHotTokenChecked && item == ctx.Page.CacheItem() &&
                    !ctx.Reheated) {
                    ctx.Reheated = true;
                    std::thread reader([&ctx] {
                        TlsCacheHazard = &TCacheThreadTestAccess::Hazard(ctx.Cache, 0);
                        {
                            TTestSharedPageRef result;
                            Y_ABORT_UNLESS(ctx.Cache.Find(ctx.Collection, 1, result) == ESharedCacheResultStatus::Hit);
                        }
                        TlsCacheHazard = nullptr;
                    });
                    reader.join();
                }
            } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(!fixture.Cache->ReclaimForPreload());
        UNIT_ASSERT(context.Reheated);
        AssertPageHit(*fixture.Cache, TSharedCacheKey::Page(collection, 1), page);
        UNIT_ASSERT(!TlsCacheHazard->PreloadReclaim);
    }

    Y_UNIT_TEST(PreloadPresenceRetryDoesNotEvictToAllocateHandle) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 127);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const auto source = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        const auto other = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 2));
        TTestSharedPageRef sourceHold;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, sourceHold) == ESharedCacheResultStatus::Hit);
        TVector<TTestSharedPageRef> held;
        for (ui64 offset = 100; AvailableFreeHandles(*fixture.Cache) != 0; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            TTestSharedPageRef page;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, page) == ESharedCacheResultStatus::Hit);
            held.push_back(std::move(page));
        }

        struct TContext {
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Source;
            TTestSharedPageRef& SourceHold;
            TTestSharedPageRef Replacement;
            bool Evicted = false;
        } context{ *fixture.Cache, collection, source, sourceHold, {} };

        TTestTraits hooks{ &context, [](void* opaque, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              auto& ctx = *static_cast<TContext*>(opaque);
                              if (point == ESharedCacheHookPoint::AfterPageBatchPrepared && !ctx.Evicted) {
                                  ctx.Evicted = true;
                                  ctx.SourceHold.Drop();
                                  if (TSharedCacheTestAccess::HandleState(ctx.Cache, ctx.Source.Index()).IsHot()) {
                                      Y_ABORT_UNLESS(TSharedCacheTestAccess::EvictFromHot(ctx.Cache, ctx.Source));
                                  }
                                  Y_ABORT_UNLESS(TSharedCacheTestAccess::EraseCold(
                                      ctx.Cache, MakeColdItem(ctx.Cache, ctx.Source.Index())));
                                  InsertReadyPage(ctx.Cache, TSharedCacheKey::Page(ctx.Collection, 9000));
                                  Y_ABORT_UNLESS(ctx.Cache.Find(ctx.Collection, 9000, ctx.Replacement) ==
                                                 ESharedCacheResultStatus::Hit);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        TVector<TTestSharedCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), MakeIntrusive<TPageFetchWaiter>());
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertPreloadBatch(collection, requests));
        UNIT_ASSERT(context.Evicted);
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Miss);
        UNIT_ASSERT(requests[1].Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(requests[1].Page().CacheItem() == other);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
    }

    Y_UNIT_TEST(PageBatchRejectsReservationAboveCurrentLimit) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 85);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.emplace_back(MakePageLocation(offset), new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        const ui64 freeBefore = TSharedCacheTestAccess::FreeCount(*fixture.Cache);
        const ui64 pageBytes = requests[0].Location.Size + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(pageBytes));
        const ui64 overallBefore = fixture.Cache->OverallUsage();
        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overallBefore);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);
    }
    Y_UNIT_TEST(ReviewR13WaitingStickyPagesPreserveRetention) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 95);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = MakePageLocation(1);
        const ui64 bytes = location.Size + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ESharedCacheResultStatus::Pending);
        auto& fetch = requests[0].Fetch();
        const auto page = fetch.CacheItem();
        UNIT_ASSERT(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, fetch.CacheItem().Index()).IsWaitingForMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, fetch.CacheItem().Index()).IsStickyField());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT(!fetch.Dispatch());
        UNIT_ASSERT(!fetch.TryReserveMemory());
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT(fixture.Cache->AdmissionGeneration() > generation);
        UNIT_ASSERT(fetch.TryReserveMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fetch.TryReserveMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fetch.Dispatch());
        UNIT_ASSERT(fetch.MakeReady(MakePageData(fetch.CacheItem().Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
    }

    Y_UNIT_TEST(CancellingMemoryWaitingPageDoesNotRefundUnownedBytes) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 96);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = MakePageLocation(1);
        const ui64 bytes = location.Size + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(requests[0].Fetch().FailReady());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }

    Y_UNIT_TEST(MemoryWaitingBatchReservesAllPagesBeforeDispatch) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 97);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = MakePageLocation(1);
        const ui64 bytes = location.Size + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(2 * bytes));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
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
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
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
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, held) == ESharedCacheResultStatus::Hit);
        const ui64 bytes = held.size() + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));

        struct TContext {
            TTestSharedCache& Cache;
            TPageCacheItem Page;
            bool Evicted = false;
        } context{ *fixture.Cache, page };

        TTestTraits hooks{ &context, [](void* opaque, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              auto& context = *static_cast<TContext*>(opaque);
                              if (point == ESharedCacheHookPoint::AfterPageBatchPrepared && !context.Evicted) {
                                  context.Evicted = true;
                                  Y_ABORT_UNLESS(TSharedCacheTestAccess::EvictFromHot(context.Cache, context.Page));
                                  Y_ABORT_UNLESS(TSharedCacheTestAccess::EraseCold(
                                      context.Cache, MakeColdItem(context.Cache, context.Page.Index())));
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(1), new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        UNIT_ASSERT(context.Evicted);
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Inserted);
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
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        const auto location = MakePageLocation(1);
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status() == ESharedCacheResultStatus::Pending);

        const TPageCacheItem page = requests[0].Fetch().CacheItem();
        UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);

        TVector<TTestSharedCachePageRequest> hit;
        hit.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hit));
        UNIT_ASSERT(hit[0].Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hit[0].Page().CacheItem() == page);
    }
    Y_UNIT_TEST(PageBatchUsesCollectionStickyMode) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 87);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(1), new TTestPageFetchWaiter(completed, ready, completedItem));
        requests.emplace_back(MakePageLocation(2), new TTestPageFetchWaiter(completed, ready, completedItem));

        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, false));
        std::array<TPageCacheItem, 2> pageItems;
        for (ui32 index = 0; index < requests.size(); ++index) {
            auto& request = requests[index];
            UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Inserted);
            pageItems[index] = request.Fetch().CacheItem();
            UNIT_ASSERT(request.Fetch().MakeReady(MakePageData(pageItems[index].Index())));
        }
        const auto stickyState = TSharedCacheTestAccess::HandleState(*fixture.Cache, pageItems[0].Index());
        const auto secondState = TSharedCacheTestAccess::HandleState(*fixture.Cache, pageItems[1].Index());
        UNIT_ASSERT(stickyState.IsSticky());
        UNIT_ASSERT(secondState.IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->StickyOwnedBytes(), 2 * (requests[0].Location.Size + NActors::TSharedData::OverheadSize));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().RequestedPages, 0);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TVector<TTestSharedCachePageRequest> regular;
        regular.emplace_back(MakePageLocation(3), new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, regular));
        UNIT_ASSERT(regular[0].Status() == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem ordinary = regular[0].Fetch().CacheItem();
        UNIT_ASSERT(regular[0].Fetch().MakeReady(MakePageData(ordinary.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsHot());

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));
        TVector<TTestSharedCachePageRequest> hits;
        hits.emplace_back(requests[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem));
        hits.emplace_back(regular[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
        UNIT_ASSERT(hits[0].Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hits[1].Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hits[0].Page().IsSticky());
        UNIT_ASSERT(hits[1].Page().IsSticky());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pageItems[0].Index()).IsSticky());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsSticky());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pageItems[1].Index()).IsHot());
    }
    Y_UNIT_TEST(RegularPageBatchUsesExplicitStickyHints) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 89);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        for (ui32 index = 0; index < 4; ++index) {
            requests.emplace_back(MakePageLocation(index, 4096,
                                      index % 2 ? NTable::NPage::EPage::FlatIndex : NTable::NPage::EPage::DataPage),
                new TTestPageFetchWaiter(completed, ready, completedItem), index >= 2);
        }
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        for (ui32 index = 0; index < requests.size(); ++index) {
            const auto page = requests[index].Fetch().CacheItem();
            UNIT_ASSERT(requests[index].Fetch().MakeReady(MakePageData(page.Index())));
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
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
            TVector<TTestSharedCachePageRequest> hits;
            hits.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem), true);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
            UNIT_ASSERT(hits[0].Status() == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(hits[0].Page().IsSticky());
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 2);
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(ExplicitStickyHintPromotesAnOrdinaryPendingFetch) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 92);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> ordinary;
        ordinary.emplace_back(MakePageLocation(0, 4096, NTable::NPage::EPage::FlatIndex),
            new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, ordinary));
        const auto page = ordinary[0].Fetch().CacheItem();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsStickyNoneField());
        TVector<TTestSharedCachePageRequest> sticky;
        sticky.emplace_back(ordinary[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, sticky));
        UNIT_ASSERT(sticky[0].Status() == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT(ordinary[0].Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 1);
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(FailedStickyFetchUnlinksOnlyItsPage) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 93);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
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
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), kept.Index());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, failed.Index()).IsFree());
        UNIT_ASSERT(requests[1].Fetch().MakeReady(MakePageData(kept.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, kept.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 1);
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(StickyBatchCompletionHonorsModeWithdrawal) {
        for (bool explicitSticky : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(85, 86, 90);
            const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            std::atomic<ui32> completed = 0;
            std::atomic<ui32> ready = 0;
            std::atomic<ui64> completedItem = 0;
            TVector<TTestSharedCachePageRequest> requests;
            requests.emplace_back(
                MakePageLocation(0), new TTestPageFetchWaiter(completed, ready, completedItem), explicitSticky);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
            const auto page = requests[0].Fetch().CacheItem();
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsPending());
            UNIT_ASSERT_VALUES_EQUAL(
                TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), page.Index());
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsStickyNoneField());
            UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(page.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        }
    }
    Y_UNIT_TEST(RegularStickyPageLinkedAfterDetachIsReleased) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 91);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), new TTestPageFetchWaiter(completed, ready, completedItem), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto page = requests[0].Fetch().CacheItem();
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforePageReadyState, page.CacheItem());
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
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(succeeded);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
    }
    Y_UNIT_TEST(RegularStickyPageLinkedAfterKeepTransitionIsReleased) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 91);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), new TTestPageFetchWaiter(completed, ready, completedItem), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto page = requests[0].Fetch().CacheItem();
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforePageReadyState, page.CacheItem());
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
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
    }
    Y_UNIT_TEST(PageBatchResultMovesActiveMember) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 88);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        const auto location = MakePageLocation(1);
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));

        TTestSharedCachePageRequest fetchResult(
            MakePageLocation(2), new TTestPageFetchWaiter(completed, ready, completedItem));
        fetchResult = std::move(requests[0]);
        UNIT_ASSERT(fetchResult.Status() == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem page = fetchResult.Fetch().CacheItem();
        UNIT_ASSERT(fetchResult.Fetch().MakeReady(MakePageData(page.Index())));

        TVector<TTestSharedCachePageRequest> hits;
        hits.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
        TTestSharedCachePageRequest hitResult(std::move(hits[0]));
        UNIT_ASSERT(hitResult.Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hitResult.Page().CacheItem() == page);
    }
    Y_UNIT_TEST(PageBatchOverflowLeavesOutputsUntouched) {
        TFixture fixture;
        const TLogoBlobID id(89, 90, 91);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(
            MakePageLocation(1, Max<ui64>() / 2), new TTestPageFetchWaiter(completed, ready, completedItem));
        requests.emplace_back(
            MakePageLocation(2, Max<ui64>() / 2), new TTestPageFetchWaiter(completed, ready, completedItem));
        requests[0].SetHit({});
        requests[1].SetPending();

        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(requests[1].Status() == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(PageBatchHandleClaimRollsBack) {
        TFixture fixture;
        const TLogoBlobID id(87, 88, 89);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
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
        for (const TTestSharedCachePageRequest& request : requests) {
            TTestSharedPageRef missing;
            UNIT_ASSERT(fixture.Cache->Find(collection, static_cast<ui64>(request.Location.Offset), missing) ==
                        ESharedCacheResultStatus::Miss);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCollection(*fixture.Cache, fixture.Registry, collection));
    }
    Y_UNIT_TEST(PageBatchCoalescesDuplicateCandidates) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 87);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto location = MakePageLocation(3);
        const ui64 pageBytes = location.Size + NActors::TSharedData::OverheadSize;
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        }

        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        ui32 inserted = 0;
        ui32 owner = 0;
        for (ui32 index = 0; index < requests.size(); ++index) {
            if (requests[index].Status() == ESharedCacheResultStatus::Inserted) {
                ++inserted;
                owner = index;
                UNIT_ASSERT(requests[index].Fetch());
            } else {
                UNIT_ASSERT(requests[index].Status() == ESharedCacheResultStatus::Pending);
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
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto first = MakePageLocation(1, 4096);
        const auto second = MakePageLocation(2, 8192);
        const ui64 requestedBytes = first.Size + second.Size;
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        const auto addRequest = [&](TVector<TTestSharedCachePageRequest>& requests,
                                    const NTable::NPage::TPageLocation& location) {
            requests.emplace_back(location, new TTestPageFetchWaiter(completed, ready, completedItem));
        };

        const TTestSharedCache::TStats empty = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(empty.RequestedPages, 0);
        UNIT_ASSERT_VALUES_EQUAL(empty.RequestedBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(empty.MissBytes, 0);

        TVector<TTestSharedCachePageRequest> misses;
        addRequest(misses, first);
        addRequest(misses, second);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, misses));
        for (TTestSharedCachePageRequest& request : misses) {
            UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Inserted);
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

        TVector<TTestSharedCachePageRequest> resident;
        addRequest(resident, first);
        addRequest(resident, second);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, resident));
        for (const TTestSharedCachePageRequest& request : resident) {
            UNIT_ASSERT(request.Status() == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(request.Page());
        }

        const TTestSharedCache::TStats served = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(served.RequestedPages, 4);
        UNIT_ASSERT_VALUES_EQUAL(served.HitPages, 2);
        UNIT_ASSERT_VALUES_EQUAL(served.HitBytes, requestedBytes);
        UNIT_ASSERT_VALUES_EQUAL(served.MissPages, 2);
        UNIT_ASSERT_VALUES_EQUAL(served.MissBytes, requestedBytes);

        TVector<TTestSharedCachePageRequest> keepRequest;
        addRequest(keepRequest, MakePageLocation(3, 4096));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, keepRequest));
        UNIT_ASSERT(keepRequest[0].Status() == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem sticky = keepRequest[0].Fetch().CacheItem();
        UNIT_ASSERT(keepRequest[0].Fetch().MakeReady(MakePageData(sticky.Index(), keepRequest[0].Location.Size)));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, sticky.Index()).IsSticky());

        const TTestSharedCache::TStats kept = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(kept.RequestedPages, 5);
        UNIT_ASSERT_VALUES_EQUAL(kept.MissPages, 3);

        TVector<TTestSharedCachePageRequest> anotherSticky;
        addRequest(anotherSticky, MakePageLocation(4, 4096));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, anotherSticky));
        UNIT_ASSERT(anotherSticky[0].Status() == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem anotherPage = anotherSticky[0].Fetch().CacheItem();
        UNIT_ASSERT(
            anotherSticky[0].Fetch().MakeReady(MakePageData(anotherPage.Index(), anotherSticky[0].Location.Size)));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, anotherPage.Index()).IsSticky());

        const TTestSharedCache::TStats moreKept = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(moreKept.RequestedPages, 6);
        UNIT_ASSERT_VALUES_EQUAL(moreKept.MissPages, 4);

        TVector<TTestSharedCachePageRequest> rejected;
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
        TTestSharedPageRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1, 4096),
                        EStickyState::None, hotItem, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(hotItem, MakePageData(hotItem.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 0);

        const TCollectionLocation location{ .Id = TLogoBlobID(95, 96, 97), .BackingSize = 113 };
        TCollectionCacheItem insertedCollection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(*fixture.Cache, location, insertedCollection,
                        collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReadyUnregistered(
            *fixture.Cache, insertedCollection, MakeCollection(location.Id)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(insertedCollection, ECacheMode::Sticky));
        TPageCacheItem keepItem;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, insertedCollection, MakePageLocation(2, 4096),
                        EStickyState::Sticky, keepItem, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(keepItem, MakePageData(keepItem.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 0);
    }
    Y_UNIT_TEST(CollectionPagesCacheModeTransitionsAreExclusive) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 104);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        TCollection* value = TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());
        const ui64 generation = value->KeepGeneration();
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(4 * bytes));
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> pending;
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
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));
        UNIT_ASSERT_VALUES_EQUAL(value->KeepGeneration(), generation + 2);
        for (ui32 step = 0; step < 10 && TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold();
             ++step) {
            UNIT_ASSERT(fixture.Cache->RunMaintenance());
        }
        UNIT_ASSERT(!TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        TVector<TTestSharedCachePageRequest> hits;
        hits.emplace_back(pending[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
        UNIT_ASSERT(hits[0].Status() == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hits[0].Page().IsSticky());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(value->KeepGeneration(), generation + 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        TVector<TTestSharedCachePageRequest> explicitSticky;
        explicitSticky.emplace_back(
            pending[0].Location, new TTestPageFetchWaiter(completed, ready, completedItem), true);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, explicitSticky));
        UNIT_ASSERT(explicitSticky[0].Page().IsSticky());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(AttachmentTransitionRejectsCollectionPagesCacheModeChanges) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 105);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 0), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        bool detached = false;
        TGateThread detacher(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            detached = TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsTransitionField());
        for (ECacheMode mode : { ECacheMode::Regular, ECacheMode::Sticky, ECacheMode::TryKeepInMemory }) {
            UNIT_ASSERT(!fixture.Cache->SetCollectionPagesCacheMode(collection, mode));
        }
        gate.Slots[0].Release();
        detacher.Join();
        UNIT_ASSERT(detached);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(KeepColdSpillsToOrdinaryColdAndReheats) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 98);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(
                        *fixture.Cache, { .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * pageBytes));
        std::array<TPageCacheItem, 3> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestSharedPageRef hit;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(index + 1),
                            EStickyState::None, pages[index], hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[0].Index()).IsCold());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[1].Index()).IsCold());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[2].Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ActivePageBytes(), pageBytes);

        TTestSharedPageRef hit;
        UNIT_ASSERT(fixture.Cache->Find(collection, 3, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[2].Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ActivePageBytes(), pageBytes);
        hit.Drop();

        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[2]));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[2].Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);

        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(0));
        UNIT_ASSERT(fixture.Cache->RunMaintenance());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[2].Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ActivePageBytes(), 0);
    }
    Y_UNIT_TEST(KeepColdPublishesOnLastReleaseAndWithdrawsOnModeChange) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 99);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(
                        *fixture.Cache, { .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, collection, MakeCollection(id)));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * pageBytes));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));

        TPageCacheItem page;
        TTestSharedPageRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1),
                        EStickyState::None, page, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        hit.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->RunMaintenance());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
    }
    Y_UNIT_TEST(KeepColdLastReleaseDemotesWithdrawnEntriesOnly) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 105);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(
                        *fixture.Cache, { .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * pageBytes));
        std::array<TPageCacheItem, 2> pages;
        std::array<ui32, 2> queuedVersions;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestSharedPageRef unused;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(index + 1),
                            EStickyState::None, pages[index], unused) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
            queuedVersions[index] = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, pages[index].Index());
            UNIT_ASSERT(queuedVersions[index] & KeepColdQueuedMask);
        }

        std::array<TTestSharedPageRef, 2> refs;
        for (ui32 index = 0; index < pages.size(); ++index) {
            UNIT_ASSERT(fixture.Cache->Find(collection, index + 1, refs[index]) == ESharedCacheResultStatus::Hit);
            const ui32 coldVersion = index == 0
                                         ? TSharedCacheTestAccess::ColdVersion(*fixture.Cache, pages[index].Index())
                                         : queuedVersions[index];
            TSharedCacheTestAccess::HoldPageInKeepColdForTest(*fixture.Cache, pages[index], coldVersion);
        }
        UNIT_ASSERT(TSharedCacheTestAccess::QueueKeepColdPageForTest(*fixture.Cache, pages[1]) & KeepColdQueuedMask);

        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(0));
        fixture.Cache->RunMaintenance();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 2 * pageBytes);
        UNIT_ASSERT(!(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, pages[1].Index()) & KeepColdQueuedMask));

        refs[0].Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[0].Index()).IsCold());
        refs[1].Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[1].Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
    }
    Y_UNIT_TEST(KeepColdLastReleaseKeepsCurrentRingEntry) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 106);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(
                        *fixture.Cache, { .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(pageBytes));
        TPageCacheItem page;
        TTestSharedPageRef unused;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1),
                        EStickyState::None, page, unused) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui32 queuedVersion = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());

        TTestSharedPageRef ref;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, ref) == ESharedCacheResultStatus::Hit);
        TSharedCacheTestAccess::HoldPageInKeepColdForTest(*fixture.Cache, page, queuedVersion);
        ref.Drop();

        const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(state.IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()) & KeepColdQueuedMask);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);

        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(0));
        fixture.Cache->RunMaintenance();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
    }
    Y_UNIT_TEST(KeepColdLimitScanContinuesAcrossBatches) {
        TFixture fixture(10);
        const TLogoBlobID id(95, 96, 100);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(
                        *fixture.Cache, { .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(pageBytes));
        TPageCacheItem page;
        TTestSharedPageRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1),
                        EStickyState::None, page, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);

        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(0));
        UNIT_ASSERT(fixture.Cache->RunMaintenance());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);
        UNIT_ASSERT(fixture.Cache->RunMaintenance());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
    }
    Y_UNIT_TEST(KeepColdSlotOverflowDemotesOldestPage) {
        TFixture fixture(6, 8, 8, ui64(1) << 20);
        const TLogoBlobID id(95, 96, 101);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(
                        *fixture.Cache, { .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(fixture.Cache->HardLimit()));

        TPageCacheItem oldest;
        for (ui32 index = 0; index <= fixture.Capacity.KeepColdSlotCount(); ++index) {
            TPageCacheItem page;
            TTestSharedPageRef hit;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(index + 1),
                            EStickyState::None, page, hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            if (index == 0) {
                oldest = page;
            }
        }
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, oldest.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), fixture.Capacity.KeepColdSlotCount() * pageBytes);
    }
    Y_UNIT_TEST(PageFetchWaiterAcquiresReadyPageBeforeFinalizerDrops) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(81, 82);
        const auto location = MakePageLocation(17);
        TTestSharedPageRef completedPage;
        TIntrusivePtr<TAcquiringPageFetchWaiter> waiter = new TAcquiringPageFetchWaiter(*fixture.Cache, completedPage);
        TTestSharedPageRef hit;
        TTestPageFetch fetch;

        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None,
                        waiter, hit, fetch) == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem page = fetch.CacheItem();
        UNIT_ASSERT(fetch.MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(waiter->Ready());
        UNIT_ASSERT(completedPage);
        UNIT_ASSERT(completedPage.CacheItem() == page);
        const THandleState referenced = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(referenced.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(referenced.Frequency(), 0);

        completedPage.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
    }
    Y_UNIT_TEST(PageFetchCompletionIsInheritedByLastSubscriber) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(65, 66);
        const auto location = MakePageLocation(11);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        TTestSharedPageRef hit;
        TTestPageFetch fetch;
        TIntrusivePtr<TPageFetchWaiter> first = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None,
                        first, hit, fetch) == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem page = fetch.CacheItem();

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterFetchWaiterSubscribed, page.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        ESharedCacheResultStatus subscribeStatus = ESharedCacheResultStatus::Miss;
        TGateThread subscriber(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            TTestSharedPageRef subscriberHit;
            TTestPageFetch subscriberFetch;
            TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
            subscribeStatus = TSharedCacheTestAccess::FindOrInsert(
                *cache, collection, location, EStickyState::None, second, subscriberHit, subscriberFetch);
        });

        gate.Slots[0].Wait();
        UNIT_ASSERT(fetch.MakeReady(MakePageData(page.Index())));
        const THandleState pending = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(pending.IsBegin());
        UNIT_ASSERT_VALUES_EQUAL(pending.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);

        gate.Slots[0].Release();
        subscriber.Join();
        UNIT_ASSERT(subscribeStatus == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 2);
        const THandleState resident = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
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
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(83, 84, 85));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

        const ui32 bucketMask = static_cast<ui32>(CurrentBucketCount(*fixture.Space) - 1);
        const ui32 bucket = (collectionKey.Hash() + 1) & bucketMask;
        const auto sourceKey = ReviewKeyForBucket(collection, bucketMask, bucket, 1000);
        const auto victimKey = ReviewKeyForBucket(collection, bucketMask, bucket, sourceKey.Word(1) + 1, &sourceKey);
        const auto restoredKey = ReviewKeyForBucket(collection, bucketMask, (bucket + 1) & bucketMask, 2000);
        const TPageCacheItem victim = InsertReadyPage(*fixture.Cache, victimKey);
        const ui64 hotPages = TSharedCacheTestAccess::WithSpace(*fixture.Cache, [](TSpaceOperation& spaceOp) {
            return spaceOp.Hot(EHotLevel::L0).Capacity + spaceOp.Hot(EHotLevel::L1).Capacity;
        });
        ui64 nextOffset = restoredKey.Word(1) + 1;
        for (ui64 index = 1; index < hotPages; ++index) {
            const auto fillerKey = ReviewKeyForBucket(collection, bucketMask, (bucket + 1) & bucketMask, nextOffset);
            InsertReadyPage(*fixture.Cache, fillerKey);
            nextOffset = fillerKey.Word(1) + 1;
        }
        // The oldest zero-frequency page will be evicted when a restored sibling enters the full L1/L0 rings
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, victim.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, victim.Index()).Frequency(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TIntrusivePtr<TPageFetchWaiter> waiter = new TTestPageFetchWaiter(completed, ready, completedItem);
        TTestSharedPageRef hit;
        TTestPageFetch fetch;
        UNIT_ASSERT(
            TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(sourceKey.Word(1)),
                EStickyState::Sticky, waiter, hit, fetch) == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem source = fetch.CacheItem();
        const THandleState pending = TSharedCacheTestAccess::HandleState(*fixture.Cache, source.Index());
        UNIT_ASSERT(pending.IsPending() && pending.IsStickyField());
        TPageCacheItem restored;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(restoredKey.Word(1)),
                        EStickyState::Sticky, restored, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(restored, MakePageData(restored.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, restored.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::HandleNext(*fixture.Cache, source.Index()), victim.CacheItem().Raw());
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        if (externalDrain) {
            gate.Slots[1].Arm(ESharedCacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        }

        struct TDrainHook {
            TSharedCacheGate& Gate;
            TTestSharedCache& Cache;
            TCollectionCacheItem Collection;
            TPageCacheItem Source;
            TPageCacheItem Victim;
            TSpaceHazard* WaitingFinisher;
            bool Evicted = false;

            static void Invoke(void* context, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                auto& self = *static_cast<TDrainHook*>(context);
                if (point == ESharedCacheHookPoint::AfterStickyPageListDetached &&
                    item == self.Collection.CacheItem()) {
                    auto& slot = self.Gate.Slots[TlsCacheHazard == self.WaitingFinisher ? 1 : 0];
                    slot.TryInvoke(point, item);
                }
                if (point == ESharedCacheHookPoint::AfterTombstoneOwnerClaim && item == self.Victim.CacheItem()) {
                    // Fail before an early drain waits on its own Completing predecessor
                    Y_ABORT_UNLESS(TSharedCacheTestAccess::HandleState(self.Cache, self.Source.Index()).IsPending(),
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
            const THandleState waiting = TSharedCacheTestAccess::HandleState(*cache, source.Index());
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
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, source.Index()).IsPending());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*cache, collection), 0);
            modeChanged = cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular);
            gate.Slots[0].Release();
            finisher.Join();
        }

        UNIT_ASSERT(modeChanged);
        UNIT_ASSERT(failed);
        UNIT_ASSERT(drainHook.Evicted);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, source.Index()).IsFree());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, victim.Index()).IsFree());
        const THandleState restoredState = TSharedCacheTestAccess::HandleState(*cache, restored.Index());
        UNIT_ASSERT(restoredState.IsHot() && restoredState.IsStickyNoneField());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 0);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), source.CacheItem().Raw());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*cache, collection), 0);
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
        std::array<ESharedCacheResultStatus, ThreadCount> statuses;
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
                TTestSharedPageRef hit;
                TIntrusivePtr<TPageFetchWaiter> waiter = new TTestPageFetchWaiter(completed, ready, completedItem);
                statuses[thread] = TSharedCacheTestAccess::FindOrInsert(
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
            if (statuses[thread] == ESharedCacheResultStatus::Inserted) {
                ++inserted;
                winner = thread;
                UNIT_ASSERT(fetches[thread]);
            } else {
                UNIT_ASSERT(statuses[thread] == ESharedCacheResultStatus::Pending);
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
            TTestSharedPageRef hit;
            TTestPageFetch fetch;
            TIntrusivePtr<TPageFetchWaiter> first = new TTestPageFetchWaiter(completed, ready, completedItem);
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None,
                            first, hit, fetch) == ESharedCacheResultStatus::Inserted);
            page = fetch.CacheItem();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);

            TTestPageFetch duplicateFetch;
            TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, location, EStickyState::None,
                            second, hit, duplicateFetch) == ESharedCacheResultStatus::Pending);
        }

        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 0);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), page.CacheItem().Raw());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
        TTestSharedPageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(collection, static_cast<ui64>(location.Offset), missing) ==
                    ESharedCacheResultStatus::Miss);
    }
    Y_UNIT_TEST(PageFetchMoveAssignmentReleasesPreviousFetch) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(64, 65);
        const auto firstLocation = MakePageLocation(10);
        const auto secondLocation = MakePageLocation(11);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TTestSharedPageRef hit;
        TTestPageFetch first;
        TTestPageFetch second;
        TIntrusivePtr<TPageFetchWaiter> firstWaiter = new TTestPageFetchWaiter(completed, ready, completedItem);
        TIntrusivePtr<TPageFetchWaiter> secondWaiter = new TTestPageFetchWaiter(completed, ready, completedItem);

        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, firstLocation, EStickyState::None,
                        firstWaiter, hit, first) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, secondLocation, EStickyState::None,
                        secondWaiter, hit, second) == ESharedCacheResultStatus::Inserted);
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
        const ui64 freeBefore = TSharedCacheTestAccess::FreeCount(*fixture.Cache);
        const TCollectionLocation location{ .Id = id, .BackingSize = 113 };

        TCollectionCacheItem inserted;
        TTestSharedCacheCollectionRef collection;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, inserted,
                        collection) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!collection);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), location.AccountedBytes());
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, inserted, MakeCollection(id, location.BackingSize)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), location.AccountedBytes());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), inserted.Index());

        TCollectionCacheItem duplicate;
        TTestSharedCacheCollectionRef hitCollection;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, duplicate,
                        hitCollection) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hitCollection.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);
        hitCollection.Drop();
    }
    Y_UNIT_TEST(UnattachedCollectionAttachesWithoutTemporaryRegistry) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(5, 6, 8) };

        TCollectionCacheItem inserted;
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(*fixture.Cache, location, inserted, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, inserted, MakeCollection(location.Id)));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, inserted.Index()), 0);

        TCollectionCacheItem duplicate;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, duplicate, hit) ==
                    ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hit.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), inserted.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, inserted.Index()), 0);
        hit.Drop();

        UNIT_ASSERT(TSharedCacheTestAccess::UnlinkCollectionRegistry(*fixture.Cache, fixture.Registry, inserted));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        const THandleState retained = TSharedCacheTestAccess::HandleState(*fixture.Cache, inserted.Index());
        UNIT_ASSERT(retained.IsSticky());
        UNIT_ASSERT(retained.IsStickyField());
        UNIT_ASSERT(fixture.Cache->EraseCollection(inserted));
    }
    Y_UNIT_TEST(UnattachedCollectionCanBeDeleted) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(6, 7, 9) };

        TCollectionCacheItem inserted;
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(*fixture.Cache, location, inserted, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, inserted, MakeCollection(location.Id)));
        UNIT_ASSERT(fixture.Cache->EraseCollection(inserted));

        UNIT_ASSERT(fixture.Cache->Find(location.Id, hit) == ESharedCacheResultStatus::Miss);
    }
    Y_UNIT_TEST(CollectionRegistryDetachesAndReattaches) {
        TFixture fixture;
        const TLogoBlobID firstId(4, 6, 8);
        const TLogoBlobID secondId(5, 7, 9);
        const TCollectionLocation firstLocation{ .Id = firstId };
        const TCollectionLocation secondLocation{ .Id = secondId };

        TCollectionCacheItem first;
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, firstLocation, first, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, first, MakeCollection(firstId)));

        TCollectionCacheItem second;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, secondLocation, second,
                        hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, second, MakeCollection(secondId)));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), second.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);

        /* a detached, unreferenced record goes to the eviction control, so it can be re-attached
         * while it is still there */
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, first));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), second.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, first.Index()).IsHot());
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, first));
        const auto staleColdEntry = MakeColdItem(*fixture.Cache, first.Index());

        TCollectionCacheItem duplicate;
        TTestSharedCacheCollectionRef reattached;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, firstLocation, duplicate,
                        reattached) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(reattached.CacheItem() == first);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, first.Index()).IsSticky());
        UNIT_ASSERT(!TSharedCacheTestAccess::EraseCold(*fixture.Cache, staleColdEntry));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), second.Index());

        reattached.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, first));
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, second));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
    }
    Y_UNIT_TEST(CollectionReattachRestoresOwnReferenceWithResidentPage) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 17, 19) };
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionHit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection,
                        collectionHit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(location.Id)));

        TPageCacheItem page;
        TTestSharedPageRef pageHit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(1), EStickyState::None, page,
                        pageHit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        TCollectionCacheItem duplicate;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, duplicate,
                        collectionHit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 2);
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 0);
    }
    Y_UNIT_TEST(CollectionAttachRetriesNativeReferenceChanges) {
        for (EHandleState sourceState : { EHandleState::Sticky, EHandleState::Hot, EHandleState::Cold }) {
            TFixture fixture;
            const TCollectionLocation location{ .Id = TLogoBlobID(15, 17, 20) };
            TCollectionCacheItem collection;
            TTestSharedCacheCollectionRef collectionHit;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection,
                            collectionHit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(collection);
            UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
                *fixture.Cache, fixture.Registry, collection, MakeCollection(location.Id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            TPageCacheItem page;
            if (sourceState == EHandleState::Sticky) {
                page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
            }
            UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
            if (sourceState == EHandleState::Cold) {
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, collection));
            }
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).State() == sourceState);
            const ui64 pageOwners = page ? 1 : 0;
            UNIT_ASSERT_VALUES_EQUAL(
                TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners);

            TSharedCacheGate gate;
            gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeCollectionAttachClaim, collection.CacheItem());
            gate.Slots[1].Arm(ESharedCacheHookPoint::BeforeCollectionAttachPublished, collection.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            TTestSharedCacheCollectionRef attached;
            ESharedCacheResultStatus status = ESharedCacheResultStatus::Miss;
            TGateThread attacher(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                TCollectionCacheItem duplicate;
                status = TSharedCacheTestAccess::FindOrInsert(*cache, fixture.Registry, location, duplicate, attached);
            });
            gate.Slots[0].Wait();
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, collection.Index()).IsStickyNoneField());
            TTestSharedCacheCollectionRef claimReader;
            UNIT_ASSERT(cache->Find(location.Id, claimReader) == ESharedCacheResultStatus::Hit);
            gate.Slots[0].Release();

            gate.Slots[1].Wait();
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, collection.Index()).IsTransitionField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), collection.Index());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*cache, collection.Index()), 0);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*cache, collection), pageOwners + 1);
            TTestSharedCacheCollectionRef publishReader;
            UNIT_ASSERT(cache->Find(location.Id, publishReader) == ESharedCacheResultStatus::Hit);
            gate.Slots[1].Release();
            attacher.Join();

            UNIT_ASSERT(status == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(attached.CacheItem() == collection);
            const THandleState ready = TSharedCacheTestAccess::HandleState(*cache, collection.Index());
            UNIT_ASSERT(ready.IsSticky() && ready.IsStickyField());
            UNIT_ASSERT_VALUES_EQUAL(ready.Refs(), 3);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), collection.Index());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*cache, collection.Index()), 0);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*cache, collection), pageOwners + 1);
            claimReader.Drop();
            publishReader.Drop();
            attached.Drop();
            UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*cache, fixture.Registry, collection));
            if (page) {
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*cache, page));
                UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, page.Index())));
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
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection, hit) ==
                    ESharedCacheResultStatus::Inserted);
        auto value = MakeHolder<TCollection>(source, collection);
        auto* owner = value.Get();
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, std::move(value)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TPageCacheItem shadow;
        TTestSharedPageRef pageHit;
        UNIT_ASSERT(
            FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(1, 4096, NTable::NPage::EPage::BTreeIndex),
                EStickyState::None, shadow, pageHit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(shadow, MakePageData(shadow.Index())));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepActivePageBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepResidentPageBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepActivePageBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, true));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepActivePageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepActivePageBytes(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, false));
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepResidentPageBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepActivePageBytes(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, shadow));
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepActivePageBytes(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepActivePageBytes(), 0);
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, true));
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, shadow.Index())));
        UNIT_ASSERT(fixture.Cache->SetCollectionSkipBTreeIndexV1Shadow(collection, false));
        UNIT_ASSERT_VALUES_EQUAL(owner->ResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepResidentPageBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(owner->KeepActivePageBytes(), 0);
    }

    Y_UNIT_TEST(KeepAggregatesIncludeModeChangeDuringPagePublication) {
        for (bool initiallyKeep : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(16, 18, 20);
            const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(collection);
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                collection, initiallyKeep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular));
            TTestSharedPageRef hit;
            TTestPageFetch fetch;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1),
                            EStickyState::None, MakeIntrusive<TPageFetchWaiter>(), hit,
                            fetch) == ESharedCacheResultStatus::Inserted);
            const auto page = fetch.CacheItem();
            TSharedCacheGate gate;
            gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeKeepBytesPublished, collection.CacheItem());
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
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepActivePageBytes(), initiallyKeep ? 0 : bytes);
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepResidentPageBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepActivePageBytes(), 0);
        }
    }

    Y_UNIT_TEST(CollectionLogicalDeleteDefersPhysicalDestruction) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 16, 17) };

        TCollectionCacheItem inserted;
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, inserted, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, inserted, MakeCollection(location.Id)));

        TTestSharedCacheCollectionRef held;
        UNIT_ASSERT(fixture.Cache->Find(location.Id, held) == ESharedCacheResultStatus::Hit);
        const TCollection* value = &held.GetCollection();
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCollection(*fixture.Cache, fixture.Registry, inserted));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);

        TTestSharedCacheCollectionRef missing;
        UNIT_ASSERT(fixture.Cache->Find(location.Id, missing) == ESharedCacheResultStatus::Miss);
        const THandleState tombstone = TSharedCacheTestAccess::HandleState(*fixture.Cache, inserted.Index());
        UNIT_ASSERT(tombstone.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(tombstone.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(&held.GetCollection(), value);

        auto acquired = held.Acquire();
        UNIT_ASSERT(acquired.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, inserted.Index()).Refs(), 2);
        held.Drop();
        UNIT_ASSERT_VALUES_EQUAL(&acquired.GetCollection(), value);
        UNIT_ASSERT_VALUES_EQUAL(acquired->Id(), location.Id);
        acquired.Drop();
        const THandleState free = TSharedCacheTestAccess::HandleState(*fixture.Cache, inserted.Index());
        UNIT_ASSERT(free.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(free.Version(), AdvanceItemVersion(inserted.Version()));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionValue(*fixture.Cache, inserted.Index()), nullptr);
    }
    Y_UNIT_TEST(CollectionOwnerDropPinsMetadataDuringReclaim) {
        for (bool lastOwner : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(15, 16, 18);
            TCollectionCacheItem collection;
            TTestSharedCacheCollectionRef collectionHit;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = id }, collection,
                            collectionHit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(collection);
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

            const ui32 pageCount = lastOwner ? 1 : 2;
            TVector<TPageCacheItem> pages;
            for (ui32 offset = 1; offset <= pageCount; ++offset) {
                pages.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages.back()));
            }
            UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
            UNIT_ASSERT_VALUES_EQUAL(
                TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageCount);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).Refs(), 0);
            const TCollection* metadata = TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());

            TSharedCacheGate gate;
            gate.Slots[0].Arm(
                lastOwner ? ESharedCacheHookPoint::BeforeCollectionHotPublished
                          : ESharedCacheHookPoint::AfterCollectionOwnerReferenceDropped, collection.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            bool erased = false;
            TGateThread releaser(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                erased = TSharedCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, pages[0].Index()));
            });
            gate.Slots[0].Wait();

            if (lastOwner) {
                UNIT_ASSERT(cache->EraseCollection(collection));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*cache, collection), 1);
                UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, pages[1].Index())));
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*cache, collection));
                UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, collection.Index())));
            }

            const THandleState retired = TSharedCacheTestAccess::HandleState(*cache, collection.Index());
            UNIT_ASSERT(retired.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(retired.Refs(), 1);
            UNIT_ASSERT_VALUES_EQUAL(cache->Collections(), 1);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionValue(*cache, collection.Index()), metadata);
            UNIT_ASSERT_VALUES_EQUAL(metadata->Id(), id);
            TTestSharedCacheCollectionRef missing;
            UNIT_ASSERT(cache->Find(id, missing) == ESharedCacheResultStatus::Miss);

            gate.Slots[0].Release();
            releaser.Join();
            UNIT_ASSERT(erased);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, collection.Index()).IsFree());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionValue(*cache, collection.Index()), nullptr);
            UNIT_ASSERT_VALUES_EQUAL(cache->Collections(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->ResidentBytes(), 0);
        }
    }
    Y_UNIT_TEST(CollectionLogicalDeleteUnstickysPagesAndRetainsRecord) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 16, 17) };
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionHit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection,
                        collectionHit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(location.Id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));

        std::array<TPageCacheItem, 2> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestSharedPageRef pageHit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(index + 1), EStickyState::Sticky,
                            pages[index], pageHit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index())));
        }
        UNIT_ASSERT_VALUES_UNEQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        UNIT_ASSERT(!fixture.Cache->EraseCollection(collection));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsSticky());
        for (ui32 index = 0; index < pages.size(); ++index) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[index].Index());
            UNIT_ASSERT(state.IsHot());
            UNIT_ASSERT(state.IsStickyNoneField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, pages[index].Index()), 0);
            TTestSharedPageRef found;
            UNIT_ASSERT(fixture.Cache->Find(collection, index + 1, found) == ESharedCacheResultStatus::Hit);
        }

        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 2);
        for (ui32 index = 0; index < pages.size(); ++index) {
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
            UNIT_ASSERT(
                TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, pages[index].Index())));
            if (index + 1 < pages.size()) {
                /* the record is still referenced by the remaining page items */
                UNIT_ASSERT_VALUES_EQUAL(
                    TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pages.size() - index - 1);
            }
        }

        /* the last page item's release handed the unreferenced record to the eviction control,
         * which reclaims it like any other item */
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsHot());
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, collection));
        UNIT_ASSERT(
            TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, collection.Index())));
        TTestSharedCacheCollectionRef missing;
        UNIT_ASSERT(fixture.Cache->Find(location.Id, missing) == ESharedCacheResultStatus::Miss);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsFree());
    }
    Y_UNIT_TEST(CollectionRegistryMovePreservesStickyPages) {
        TFixture fixture;
        TCollectionRegistry destination;
        const TCollectionLocation location{ .Id = TLogoBlobID(18, 19, 20) };

        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, collection, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(location.Id)));

        const TPageCacheItem page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(page);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        TCollectionCacheItem duplicate;
        TTestSharedCacheCollectionRef moved;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, destination, location, duplicate, moved) ==
                    ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(moved.CacheItem() == collection);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(destination), collection.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), page.Index());
        const THandleState pageState = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
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
        TTestSharedCacheCollectionRef hit;
        for (ui32 index = 0; index < collections.size(); ++index) {
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, locations[index],
                            collections[index], hit) == ESharedCacheResultStatus::Inserted);
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
                ready[index] = TSharedCacheTestAccess::MakeReady(
                    *fixture.Cache, fixture.Registry, collections[index], std::move(values[index]));
            });
        }
        start.store(true, std::memory_order_release);
        for (std::thread& worker : workers) {
            worker.join();
        }
        UNIT_ASSERT(ready[0] && ready[1]);

        const ui32 head = TSharedCacheTestAccess::CollectionListHead(fixture.Registry);
        UNIT_ASSERT(head == collections[0].Index() || head == collections[1].Index());
        const ui32 tail = TSharedCacheTestAccess::NextInOwner(*fixture.Cache, head);
        UNIT_ASSERT(tail == collections[0].Index() || tail == collections[1].Index());
        UNIT_ASSERT_VALUES_UNEQUAL(head, tail);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, tail), 0);
    }
    Y_UNIT_TEST(PublicFindOrInsertHitNeedsNoAdmissionHeadroom) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(45, 46);
        const TSharedCacheKey key = TSharedCacheKey::Page(collection, 0);
        const TPageCacheItem inserted = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->OverallUsage()));

        TPageCacheItem duplicate;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(key.Word(1)), EStickyState::None,
                        duplicate, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hit.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(PublicFindOrInsertHitDoesNotFillEmptyWorkerSpare) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(47, 48);
        const TSharedCacheKey key = TSharedCacheKey::Page(collection, 0);
        const TPageCacheItem inserted = InsertReadyPage(*fixture.Cache, key);
        const ui64 freeBefore = TSharedCacheTestAccess::FreeCount(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0), 0);

        ESharedCacheResultStatus status = ESharedCacheResultStatus::Miss;
        TPageCacheItem duplicate;
        TTestSharedPageRef hit;
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

        UNIT_ASSERT(status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(InsertPositionRetriesAfterBucketRouteChange) {
        TFixture fixture(6, 8, 7);
        const TCollectionCacheItem collection = MakeCollectionCacheItem(49, 50);
        const ui32 splitBit = static_cast<ui32>(SharedCacheBucketCount(5));
        const TSharedCacheKey key = [&] {
            for (ui64 offset = 1000; offset < 1000 + (1 << 20); ++offset) {
                const TSharedCacheKey candidate = TSharedCacheKey::Page(collection, offset);
                if (candidate.Hash() & splitBit) {
                    return candidate;
                }
            }
            Y_UNREACHABLE();
        }();

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterInsertPositionFound);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        ESharedCacheResultStatus status = ESharedCacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestSharedPageRef hit;
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

        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 5));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT_VALUES_EQUAL(CurrentBucketCount(*fixture.Space), SharedCacheBucketCount(5));

        gate.Slots[0].Release();
        writer.Join();
        UNIT_ASSERT(status == ESharedCacheResultStatus::Inserted);
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
        UNIT_ASSERT(lowInsert.Status == ESharedCacheResultStatus::Inserted);
        lowInsert.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->MakeReady(low, MakePageData(low.Index())));

        const auto high = AllocatePage(*fixture.Cache, keys[1], 4096);
        UNIT_ASSERT(high);
        auto highInsert = FindOrInsertPage(*fixture.Cache, keys[1], high);
        UNIT_ASSERT(highInsert.Status == ESharedCacheResultStatus::Inserted);
        highInsert.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->MakeReady(high, MakePageData(high.Index())));

        const auto assertHit = [&](const TSharedCacheKey& key) {
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
            found.Ref.Drop();
        };

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        auto resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
        UNIT_ASSERT(resize.Direction() == EBucketResize::Growing);
        UNIT_ASSERT(resize.Phase() == EBucketResizePhase::Prepared);
        UNIT_ASSERT_VALUES_EQUAL(resize.Cursor(), 0);
        do {
            resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
            UNIT_ASSERT_C(resize.Phase() == EBucketResizePhase::Prepared,
                TStringBuilder() << "growth phase=" << static_cast<ui32>(resize.Phase())
                                 << " cursor=" << resize.Cursor());
            const bool firstPair = resize.Cursor() == 0;
            UNIT_ASSERT_C(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache),
                TStringBuilder() << "growth cursor=" << resize.Cursor());
            if (firstPair) {
                assertHit(keys[0]);
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
            if (firstPair) {
                assertHit(keys[0]);
                assertHit(keys[1]);
                const auto redirected = AllocatePage(*fixture.Cache, keys[2], 4096);
                UNIT_ASSERT(redirected);
                auto redirectedInsert = FindOrInsertPage(*fixture.Cache, keys[2], redirected);
                UNIT_ASSERT(redirectedInsert.Status == ESharedCacheResultStatus::Inserted);
                redirectedInsert.Ref.Drop();
                UNIT_ASSERT(fixture.Cache->MakeReady(redirected, MakePageData(redirected.Index())));
                assertHit(keys[2]);
            }
            UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));
            if (firstPair) {
                assertHit(keys[0]);
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
            resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
        } while (resize.Cursor() < resize.PairCount());
        UNIT_ASSERT(TSharedCacheTestAccess::FinishBucketResize(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(CurrentBucketCount(*fixture.Space), 128);
        UNIT_ASSERT(!TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing());

        assertHit(keys[0]);
        assertHit(keys[1]);
        assertHit(keys[2]);

        UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, growth));

        TTransition shrink;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
        UNIT_ASSERT(resize.Direction() == EBucketResize::Shrinking);
        do {
            resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
            UNIT_ASSERT_C(resize.Phase() == EBucketResizePhase::Prepared,
                TStringBuilder() << "shrink phase=" << static_cast<ui32>(resize.Phase())
                                 << " cursor=" << resize.Cursor());
            const bool firstPair = resize.Cursor() == 0;
            UNIT_ASSERT_C(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache),
                TStringBuilder() << "shrink cursor=" << resize.Cursor());
            if (firstPair) {
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
            if (firstPair) {
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));
            UNIT_ASSERT(TSharedCacheTestAccess::CloseBucketSplit(*fixture.Cache));
            if (firstPair) {
                assertHit(keys[1]);
            }
            UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
            resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
        } while (resize.Cursor() < resize.PairCount());
        UNIT_ASSERT(TSharedCacheTestAccess::FinishBucketResize(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(CurrentBucketCount(*fixture.Space), 64);

        assertHit(keys[0]);
        assertHit(keys[1]);
        assertHit(keys[2]);

        UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, shrink));
    }
    Y_UNIT_TEST(HotPolicyUsesProtectedViewAcrossCapacityCommit) {
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        for (const bool growing : { true, false }) {
            TFixture fixture(6, 8, 7, reserved.Limit);
            if (!growing) {
                TTransition growth;
                UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(
                                *fixture.Cache, fixture.ReservedCapacity, growth) == EPrepareResult::Prepared);
                UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
                UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
                UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
                CompleteBucketResize(*fixture.Cache, *fixture.Space);
                CommitTransition(*fixture.Cache, growth);
            }
            UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(fixture.ReservedCapacity.Limit));
            const auto& target = growing ? fixture.ReservedCapacity : fixture.Capacity;
            TTransition transition;
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, target, transition) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, target.AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);

            TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& oldOp) {
                const auto oldPolicy = TSharedCacheTestAccess::HotPolicy(*fixture.Cache, oldOp);
                UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, transition));
                TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& newOp) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        newOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount, target.HotSlotCount());
                    const auto policy = TSharedCacheTestAccess::HotPolicy(*fixture.Cache, newOp);
                    for (ui64 bound : policy) {
                        UNIT_ASSERT(bound <= target.HotSlotCount());
                    }
                    UNIT_ASSERT_VALUES_EQUAL(policy[1], 15);
                    UNIT_ASSERT_VALUES_EQUAL(policy[1], oldPolicy[1]);
                    if (growing) {
                        UNIT_ASSERT(policy[3] > oldPolicy[3]);
                        UNIT_ASSERT_VALUES_EQUAL(
                            oldOp.EffectiveHotSlots(), oldOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount);
                    }
                    for (ui64 bound : TSharedCacheTestAccess::HotPolicy(*fixture.Cache, oldOp)) {
                        UNIT_ASSERT(bound <= oldOp.View().Tiers[ECacheTier::Regular].HotStorage.SlotCount);
                    }
                });
            });

            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
            UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
            TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
                const auto before = TSharedCacheTestAccess::HotPolicy(*fixture.Cache, spaceOp);
                UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
                UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, transition));
                UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, transition));
                UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, transition));
                UNIT_ASSERT(before == TSharedCacheTestAccess::HotPolicy(*fixture.Cache, spaceOp));
            });
        }
    }

    Y_UNIT_TEST(HotMaintenanceRefreshesOperationAfterGrowth) {
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));
        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        {
            auto lock = TSharedCacheTestAccess::LockHotResize(*fixture.Cache);
            UNIT_ASSERT(lock);
            UNIT_ASSERT(!TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        TVector<TTestSharedPageRef> held;
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& oldOp) {
            {
                auto lock = TSharedCacheTestAccess::LockHotResize(*fixture.Cache);
                UNIT_ASSERT(lock);
                UNIT_ASSERT(!TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            }
            UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
            UNIT_ASSERT(
                TSharedCacheTestAccess::BeginHotResize(*fixture.Cache, fixture.ReservedCapacity.HotSlotCount()));
            const auto collection = MakeCollectionCacheItem(701, 702);
            TPageCacheItem highPage;
            for (ui64 offset = 0; offset < fixture.Capacity.HandleCount() - 1; ++offset) {
                const auto key = TSharedCacheKey::Page(collection, offset);
                highPage = InsertReadyPage(*fixture.Cache, key);
                auto found = FindPage(*fixture.Cache, key);
                UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
                held.push_back(std::move(found.Ref));
            }
            UNIT_ASSERT(highPage.Index() >= oldOp.HandleCount());
            TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& newOp) {
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
            TSharedCacheTestAccess::EnforceWithOperation(*fixture.Cache, oldOp);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, highPage.Index()).IsCold());
        });
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, growth));
    }

    Y_UNIT_TEST(HotPolicyRunsWhileCapacityCommits) {
        for (const bool growing : { true, false }) {
            TFixture fixture(6, 8, 7);
            if (!growing) {
                TTransition growth;
                UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(
                                *fixture.Cache, fixture.ReservedCapacity, growth) == EPrepareResult::Prepared);
                UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
                UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
                UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
                CompleteBucketResize(*fixture.Cache, *fixture.Space);
                CommitTransition(*fixture.Cache, growth);
            }
            const auto& target = growing ? fixture.ReservedCapacity : fixture.Capacity;
            TTransition transition;
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, target, transition) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, target.AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, transition));

            auto commitLock = TSharedCacheTestAccess::LockHotResize(*fixture.Cache);
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
                TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
                    ready.store(true, std::memory_order_release);
                    do {
                        const auto policy = TSharedCacheTestAccess::HotPolicy(*fixture.Cache, spaceOp);
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
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, transition));
            UNIT_ASSERT(!TSharedCacheTestAccess::CommitTransition(*fixture.Cache, transition));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::Commit);
            commitLock = {};
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
            while (!TSharedCacheTestAccess::CommitTransition(*fixture.Cache, transition)) {
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
            KeepColdTrim,
            KeepColdWithdraw
        };
        for (ECase scenario : { ECase::Cold, ECase::KeepCold, ECase::KeepColdTrim, ECase::KeepColdWithdraw }) {
            const bool keep = scenario != ECase::Cold;
            TSharedCacheCapacity reserved;
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
            TFixture fixture(6, 8, 7, reserved.Limit);
            const TLogoBlobID baseId(901, 902, 903);
            TCollectionCacheItem base;
            TTestSharedCacheCollectionRef collectionHit;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = baseId }, base,
                            collectionHit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, base, MakeCollection(baseId)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(base, ECacheMode::Regular));

            TVector<TTestSharedPageRef> held;
            for (ui64 offset = 0; offset < fixture.Capacity.HandleCount() - 3; ++offset) {
                const TSharedCacheKey key = TSharedCacheKey::Page(base, offset);
                const TPageCacheItem page = InsertReadyPage(*fixture.Cache, key);
                auto found = FindPage(*fixture.Cache, key);
                UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
                UNIT_ASSERT(found.Ref.CacheItem() == page);
                held.push_back(std::move(found.Ref));
            }
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);

            TTransition growth;
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);

            TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& oldOp) {
                UNIT_ASSERT_VALUES_EQUAL(oldOp.HandleCount(), fixture.Capacity.HandleCount());
                UNIT_ASSERT_VALUES_EQUAL(oldOp.AccessibleHandleCount(), fixture.ReservedCapacity.HandleCount());
                UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, growth));
                while (growth.Phase() == ETransitionPhase::AppendGrowth) {
                    UNIT_ASSERT(TSharedCacheTestAccess::AppendFreeHandles(*fixture.Cache, growth));
                }

                const TLogoBlobID id(901, 902, keep ? 905 : 904);
                TCollectionCacheItem collection;
                TTestSharedCacheCollectionRef hit;
                UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = id },
                                collection, hit) == ESharedCacheResultStatus::Inserted);
                UNIT_ASSERT(collection.Index() >= oldOp.HandleCount());
                UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
                    *fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
                UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                    collection, keep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular));
                const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
                if (keep) {
                    UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(4 * bytes));
                }

                const TSharedCacheKey key = TSharedCacheKey::Page(collection, 1);
                const TPageCacheItem page = InsertReadyPage(*fixture.Cache, key);
                UNIT_ASSERT(page.Index() >= oldOp.HandleCount());
                auto found = FindPage(*fixture.Cache, key);
                UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
                TCollection* value = TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());
                UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 2);
                UNIT_ASSERT_VALUES_EQUAL(value->ResidentPageBytes(), bytes);

                if (keep) {
                    found.Ref.Drop();
                    UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
                    UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), bytes);
                    UNIT_ASSERT(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()) & KeepColdQueuedMask);
                    if (scenario == ECase::KeepCold) {
                        UNIT_ASSERT(TSharedCacheTestAccess::ReclaimKeepCold(*fixture.Cache, oldOp));
                    } else {
                        TTestSharedCacheItemRef heldPage;
                        if (scenario == ECase::KeepColdWithdraw) {
                            heldPage = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem());
                            UNIT_ASSERT(heldPage);
                        }
                        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(0));
                        TSharedCacheTestAccess::TrimKeepCold(*fixture.Cache, oldOp);
                        if (heldPage) {
                            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
                            UNIT_ASSERT(!(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()) &
                                          KeepColdQueuedMask));
                            heldPage.Drop();
                        }
                        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
                        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 1);
                        UNIT_ASSERT(TSharedCacheTestAccess::ReclaimCold(*fixture.Cache, oldOp));
                        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
                } else {
                    UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
                    found.Ref.Drop();
                    UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 1);
                    UNIT_ASSERT(TSharedCacheTestAccess::ReclaimCold(*fixture.Cache, oldOp));
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
                }
                UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
                UNIT_ASSERT_VALUES_EQUAL(value->ResidentPageBytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(value->ActivePageBytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
                UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ESharedCacheResultStatus::Miss);
            });

            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, growth));
        }
    }

    Y_UNIT_TEST(PageBatchAdmitsHighHandleThroughMigrationOperation) {
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));

        const TLogoBlobID id(901, 902, 906);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

        TVector<TTestSharedPageRef> held;
        for (ui64 offset = 0; offset < fixture.Capacity.HandleCount() - 3; ++offset) {
            const auto key = TSharedCacheKey::Page(collection, offset);
            InsertReadyPage(*fixture.Cache, key);
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
            held.push_back(std::move(found.Ref));
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::SpareItem(*fixture.Cache, 7), 0);

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);

        struct TContext {
            TTestSharedCache& Cache;
            TTransition& Growth;
            bool Published = false;
        } context{ *fixture.Cache, growth };

        TTestTraits hooks{ &context,
            [](void* opaque, ESharedCacheHookPoint point, TCacheItem) noexcept {
                auto& context = *static_cast<TContext*>(opaque);
                if (point == ESharedCacheHookPoint::AfterPageBatchPrepared) {
                    Y_ABORT_UNLESS(!context.Published);
                    Y_ABORT_UNLESS(TSharedCacheTestAccess::PublishFinalView(context.Cache, context.Growth));
                    while (context.Growth.Phase() == ETransitionPhase::AppendGrowth) {
                        Y_ABORT_UNLESS(TSharedCacheTestAccess::AppendFreeHandles(context.Cache, context.Growth));
                    }
                    context.Published = true;
                }
            } };
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(1000), new TTestPageFetchWaiter(completed, ready, completedItem));
        {
            TSharedCacheHookGuard guard(*fixture.Cache, hooks);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, true, true));
        }
        UNIT_ASSERT(context.Published);
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem page = requests[0].Fetch().CacheItem();
        UNIT_ASSERT(page.Index() >= fixture.Capacity.HandleCount());
        UNIT_ASSERT(requests[0].Fetch().MakeReady(MakePageData(page.Index())));
        AssertPageHit(*fixture.Cache, TSharedCacheKey::Page(collection, 1000), page);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 1);

        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, growth));
    }

    Y_UNIT_TEST(FinalDrainHot) {
        TFixture fixture(6, 8, 7);

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);

        const TCollectionCacheItem collection = MakeCollectionCacheItem(20, 21);
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < 32; ++offset) {
            pages.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
            UNIT_ASSERT(pages.back().Index() < fixture.Capacity.HandleCount());
        }

        const auto countHotWords = [&] {
            TVector<ui32> counts(fixture.ReservedCapacity.HandleCount(), 0);
            TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, shrink));

        counts = countHotWords();
        for (TPageCacheItem page : pages) {
            UNIT_ASSERT_VALUES_EQUAL(counts[page.Index()], 1);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        }
    }
    Y_UNIT_TEST(FinalDrainCold) {
        TFixture fixture(6, 8, 7);

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));

        const TCollectionCacheItem collection = MakeCollectionCacheItem(22, 23);
        TVector<TSharedCacheKey> keys;
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < fixture.Capacity.HandleCount() - 2; ++offset) {
            keys.push_back(TSharedCacheKey::Page(collection, offset));
            pages.push_back(InsertReadyPage(*fixture.Cache, keys.back()));
            UNIT_ASSERT(pages.back().Index() < fixture.Capacity.HandleCount());
        }
        for (TPageCacheItem page : pages) {
            const EHandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).State();
            if (state == EHandleState::Hot) {
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            } else {
                UNIT_ASSERT(state == EHandleState::Cold);
            }
        }
        for (ui32 index = 0; index < 8; ++index) {
            auto found = FindPage(*fixture.Cache, keys[index]);
            UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
            found.Ref.Drop();
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
        }
        const ui32 target = 2;
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[target].Index()).IsCold());

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Capacity.Limit));
        TTransition shrink;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, shrink));

        while (fixture.Cache->ReclaimCold()) {
        }
        UNIT_ASSERT(FindPage(*fixture.Cache, keys[target]).Status == ESharedCacheResultStatus::Miss);
    }
    Y_UNIT_TEST(TransitionStaticAccounting) {
        TFixture fixture(6, 8, 7);
        const ui64 growthDelta = fixture.ReservedCapacity.StaticBytes - fixture.Capacity.StaticBytes;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);

        TTransition canceled;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, canceled) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), growthDelta);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT(TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, canceled));
        UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
        UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->TargetHandleCount(), fixture.ReservedCapacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);

        TTransition shrink;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, shrink);
        UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->TargetHandleCount(), fixture.Capacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);
    }

    Y_UNIT_TEST(CapacityCleanupRetainsOwnerAndReservationUntilAcknowledged) {
        using EStep = TSharedCacheMappingTestAccess::EStep;
        TFixture fixture(6, 8, 7);
        auto& mapping = TSharedCacheTestAccess::BucketsMapping(*fixture.Cache);
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
        TSharedCacheMappingTestAccess::SetHook(mapping, hook);
        TTransition transition;
        // Register cleanup before any assertion can unwind the retained owner
        Y_DEFER {
            TSharedCacheMappingTestAccess::ClearHook(mapping);
            if (transition.Phase() != ETransitionPhase::Idle) {
                TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition);
            }
        };
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        const auto result =
            TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, transition);
        if (maps == 0) {
            return;
        }
        UNIT_ASSERT(result == EPrepareResult::CleanupPending);
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::CancelRetry);
        const ui64 pending = fixture.ReservedCapacity.StaticBytes - fixture.Capacity.StaticBytes;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), pending);
        UNIT_ASSERT(TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
        UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(!TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        TTransition contender;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, contender) ==
                    EPrepareResult::Rejected);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        failCleanup = false;
        UNIT_ASSERT(TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT(!TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
    }

    Y_UNIT_TEST(FailedCapacityPreparationAndCleanupYieldToPeriodicRetry) {
        using EStep = TSharedCacheMappingTestAccess::EStep;
        for (bool delayed : { false, true }) {
            TFixture fixture(6, 8, 7);
            auto& mapping = TSharedCacheTestAccess::BucketsMapping(*fixture.Cache);
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
            TSharedCacheMappingTestAccess::SetHook(mapping, hook);
            auto& transition = TSharedCacheTestAccess::CapacityTransition(*fixture.Cache);
            Y_DEFER {
                TSharedCacheMappingTestAccess::ClearHook(mapping);
                if (transition.Phase() != ETransitionPhase::Idle) {
                    TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition);
                }
            };
            TSharedCacheTestAccess::SetCapacityTarget(*fixture.Cache, fixture.ReservedCapacity);
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
        using EStep = TSharedCacheMappingTestAccess::EStep;
        for (bool abandon : { false, true }) {
            TFixture fixture(16, 2);
            auto target = fixture.Capacity;
            target.Tiers[ECacheTier::Regular].ColdSlots = target.HandleCount() / 4;
            target.Tiers[ECacheTier::Keep].ColdSlots = target.HandleCount() * 3 / 4;
            UNIT_ASSERT(target.HasValidTierCapacity());
            TTransition transition;
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, target, transition) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, transition));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::PrepareReceiver);
            const ui64 generation = fixture.Cache->AdmissionGeneration();
            UNIT_ASSERT(TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
            UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
            auto& mapping = TSharedCacheTestAccess::RingMapping(*fixture.Cache, ESpaceMap::KeepCold);
            bool fail = true;
            auto cleanup = [&](EStep step) noexcept {
                return !fail || (step != EStep::Unmap && step != EStep::ShrinkBacking && step != EStep::DiscardTail);
            };
            TSharedCacheMappingTestAccess::SetHook(mapping, cleanup);
            Y_DEFER {
                TSharedCacheMappingTestAccess::ClearHook(mapping);
                if (transition.Phase() != ETransitionPhase::Idle &&
                    transition.Phase() != ETransitionPhase::AbandonedForShutdown) {
                    TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition);
                }
            };
            auto prepare = [](ESpaceMap mapping) noexcept {
                return mapping != ESpaceMap::Free;
            };
            UNIT_ASSERT(!TSharedCacheTestAccess::PrepareReceiver(*fixture.Cache, transition, prepare));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::CancelReceiver);
            UNIT_ASSERT(TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
            UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
            if (abandon) {
                UNIT_ASSERT(TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
                UNIT_ASSERT(transition.Phase() == ETransitionPhase::AbandonedForShutdown);
                UNIT_ASSERT(!TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
                UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
                TTransition next;
                UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, target, next) ==
                            EPrepareResult::Rejected);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
                continue;
            }
            fail = false;
            UNIT_ASSERT(!TSharedCacheTestAccess::PrepareReceiver(*fixture.Cache, transition));
            UNIT_ASSERT(transition.Phase() == ETransitionPhase::PrepareReceiver);
            UNIT_ASSERT(TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
            TSharedCacheMappingTestAccess::ClearHook(mapping);
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareReceiver(*fixture.Cache, transition));
            do {
                UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            } while (transition.Phase() == ETransitionPhase::InitializeGrowth);
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
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
            auto guard = TSharedCacheTestAccess::LockHotResize(*fixture.Cache);
            UNIT_ASSERT(guard);
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, first) ==
                        EPrepareResult::Rejected);
        }
        UNIT_ASSERT(first.Phase() == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        auto invalid = fixture.ReservedCapacity;
        ++invalid.ExpectedPageSize;
        UNIT_ASSERT(
            TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, invalid, first) == EPrepareResult::Rejected);
        UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, first) ==
                    EPrepareResult::Prepared);
        const ui64 pendingBytes = fixture.Cache->StaticDeltaBytes();
        UNIT_ASSERT(TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, first));
        UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, second) ==
                    EPrepareResult::Rejected);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), pendingBytes);

        auto [foreignThreads, foreignSpace] =
            TCacheThreadTestAccess::MakeSpace(fixture.Capacity, fixture.ReservedCapacity, 1);
        UNIT_ASSERT(foreignSpace);
        UNIT_ASSERT(foreignSpace->PrepareTransition(fixture.ReservedCapacity, second) == EPrepareResult::Prepared);
        UNIT_ASSERT(!TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, second));
        UNIT_ASSERT(!TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, second));
        UNIT_ASSERT(!TSharedCacheTestAccess::CommitTransition(*fixture.Cache, second));
        UNIT_ASSERT(!TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, second));
        UNIT_ASSERT(second.Phase() == ETransitionPhase::Prepare);
        UNIT_ASSERT(foreignSpace->AbandonTransition(second));
        UNIT_ASSERT(TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, first));
        UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
    }

    Y_UNIT_TEST(CapacityOwnershipAllowsLegacyHotShrink) {
        TFixture fixture(6, 8, 7);
        TTransition transition;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, transition) ==
                    EPrepareResult::Prepared);
        {
            auto guard = TSharedCacheTestAccess::LockHotResize(*fixture.Cache);
            UNIT_ASSERT(guard);
            const ui32 target = static_cast<ui32>(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) - 1);
            UNIT_ASSERT(TSharedCacheTestAccess::BeginHotResize(*fixture.Cache, target));
        }
        while (TSharedCacheTestAccess::HotResizePhase(*fixture.Cache) == EHotResizePhase::Cut) {
            UNIT_ASSERT(TSharedCacheTestAccess::DrainHotResize(*fixture.Cache));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
        UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
    }

    Y_UNIT_TEST(PublishedCapacityAbandonmentKeepsSpaceClosed) {
        TFixture fixture(6, 8, 7);
        TTransition transition;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, transition) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
        UNIT_ASSERT(TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, transition));
        UNIT_ASSERT(transition.Phase() == ETransitionPhase::AbandonedForShutdown);
        UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(!TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, transition));
        TTransition next;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, next) ==
                    EPrepareResult::Rejected);
        UNIT_ASSERT(!TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
        UNIT_ASSERT(!TSharedCacheTestAccess::CommitTransition(*fixture.Cache, transition));
    }

    Y_UNIT_TEST(CapacityOwnershipSurvivesReleaseRetry) {
        TFixture fixture(11, 8, 11);
        TSharedCacheCapacity target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(
            10, fixture.Capacity.ExpectedPageSize, fixture.Capacity.FixedBytes, fixture.Space->HazardCount(), target));
        TTransition shrink;
        UNIT_ASSERT(
            TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, target, shrink) == EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 10));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishFinalView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        while (shrink.Phase() == ETransitionPhase::FinalDrain) {
            UNIT_ASSERT(TSharedCacheTestAccess::FinalDrain(*fixture.Cache, shrink));
        }
        auto failRelease = [](ESpaceMap, const void*, size_t) noexcept {
            return false;
        };
        UNIT_ASSERT(!TSharedCacheTestAccess::TryReleaseTransitionWithHook(*fixture.Cache, shrink, failRelease));
        UNIT_ASSERT(shrink.Phase() == ETransitionPhase::ReleaseRetry);
        UNIT_ASSERT(TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(!TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReleaseTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::OwnsCapacityTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::CommitTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::CanChangeTiers(*fixture.Cache));
    }
    Y_UNIT_TEST(ColdReclaimSkipsEmptyRingAndFindsSparseEntry) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(505, 506);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        ui64 probes = 0;
        TTestTraits hooks{ &probes, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::BeforeColdRingPop) {
                                  ++*static_cast<ui64*>(context);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        UNIT_ASSERT(!fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(probes, 0);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(probes, fixture.Capacity.ColdSlotCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        UNIT_ASSERT(!fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(probes, fixture.Capacity.ColdSlotCount());
    }

    Y_UNIT_TEST(ColdReclaimPreservesInProgressPublication) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(507, 508);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeColdExchange, TColdCacheItem{});
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        TGateThread publisher(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*cache, page));
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
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        held.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 1);
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        held.Drop();
        UNIT_ASSERT(!fixture.Cache->ReclaimCold()); // Removes the stale membership without charging it again.
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
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
        TVector<TTestSharedPageRef> held;
        for (ui64 offset = 0; offset < 8; ++offset) {
            const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            held.emplace_back();
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, held.back()) == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }
        for (ui64 offset = 8; offset < 16; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 8 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), 0);
        const ui64 softLimit = 12 * bytes;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(softLimit));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(softLimit));
        const ui64 hotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        for (ui32 step = 0; step < 1000 && fixture.Cache->PayloadUsage() > softLimit; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_LE(fixture.Cache->PayloadUsage(), softLimit);
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
        for (const auto& page : held) {
            UNIT_ASSERT_VALUES_EQUAL(page.size(), 4096);
        }
    }

    Y_UNIT_TEST(PhysicalCapacityMayExceedExactHardLimit) {
        TSharedCacheCapacity capacity;
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
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), fixture.Capacity.HandleCount());
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
             step < 10000 &&
             (TSharedCacheTestAccess::HandleCount(*fixture.Cache) != fixture.ReservedCapacity.HandleCount() ||
                 TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "growth stalled at step " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::HandleCount(*fixture.Cache), fixture.ReservedCapacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.ReservedCapacity.Limit);

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Capacity.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(fixture.Capacity.Limit));
        for (ui32 step = 0;
             step < 10000 && (TSharedCacheTestAccess::HandleCount(*fixture.Cache) != fixture.Capacity.HandleCount() ||
                                 TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink stalled at step " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), fixture.Capacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), fixture.Capacity.Limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.Capacity.Limit);
    }
    Y_UNIT_TEST(HardLimitRetargetSurvivesIntermediateCommit) {
        constexpr ui32 HazardCount = 7;
        TFixture fixture(6, HazardCount, 8);
        TSharedCacheCapacity middle;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, HazardCount, middle));

        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(middle.Limit));
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Prepare);
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(fixture.ReservedCapacity.Limit));
        UNIT_ASSERT(middle.Limit + 1 < fixture.ReservedCapacity.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(middle.Limit + 1));

        for (ui32 step = 0;
             step < 10000 && (TSharedCacheTestAccess::HandleCount(*fixture.Cache) != middle.HandleCount() ||
                                 TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "first growth step stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), middle.HandleCount());
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.ReservedCapacity.Limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), middle.Limit + 1);

        for (ui32 step = 0;
             step < 10000 &&
             (TSharedCacheTestAccess::HandleCount(*fixture.Cache) != fixture.ReservedCapacity.HandleCount() ||
                 TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(
                fixture.Cache->RunMaintenance(), TStringBuilder() << "second growth step stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::HandleCount(*fixture.Cache), fixture.ReservedCapacity.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.ReservedCapacity.Limit);
    }
    Y_UNIT_TEST(HardShrinkCutScanIsBatched) {
        constexpr ui32 HazardCount = 1;
        TFixture fixture(11, HazardCount, 11);
        TSharedCacheCapacity target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(10, 4096, 0, HazardCount, target));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(target.Limit));

        for (ui32 step = 0; step < 10000; ++step) {
            const TSpaceState state = TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space);
            if (TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate &&
                !state.Resizing() && state.AddressBits() == target.AddressBits)
            {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
        }

        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HardTransitionWorkIndex(*fixture.Cache), 0);
        UNIT_ASSERT(fixture.Cache->RunMaintenance());
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HardTransitionWorkIndex(*fixture.Cache),
            target.HandleCount() + SharedCacheTransitionWorkBatch);

        for (ui32 step = 0;
             step < 10000 && (TSharedCacheTestAccess::HandleCount(*fixture.Cache) != target.HandleCount() ||
                                 TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink completion stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Idle);
    }
    Y_UNIT_TEST(DetachedCollectionPagesRelocateBeforeShrink) {
        constexpr ui32 HazardCount = 8;
        TFixture fixture(6, HazardCount, 6);
        TSharedCacheCapacity target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, HazardCount, target));

        const TCollectionLocation collectionLocation{ .Id = TLogoBlobID(53, 54, 55) };
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, collectionLocation,
                        collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(collectionLocation.Id)));
        TVector<TSharedCacheKey> keys;
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < target.HandleCount() - 1; ++offset) {
            keys.push_back(TSharedCacheKey::Page(collection, offset));
            pages.push_back(InsertReadyPage(*fixture.Cache, keys.back()));
        }
        const TPageCacheItem hotSource = pages[pages.size() - 2];
        const TPageCacheItem coldSource = pages.back();
        UNIT_ASSERT_VALUES_EQUAL(hotSource.Index(), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(coldSource.Index(), target.HandleCount() + 1);

        for (ui32 index = 0; index < 2; ++index) {
            const TPageCacheItem page = pages[index];
            if (TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot()) {
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            }
            const ui32 coldVersion = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
            UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(
                *fixture.Cache, TColdCacheItem::Make(coldVersion & MaxItemVersion, page.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        }
        const ui64 pageOwners = target.HandleCount() - 3;
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners + 1);
        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        collectionRef.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners);

        auto oldHot = FindPage(*fixture.Cache, keys[keys.size() - 2]);
        auto oldCold = FindPage(*fixture.Cache, keys.back());
        UNIT_ASSERT(oldHot.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(oldCold.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(oldHot.Ref.CacheItem() == hotSource);
        UNIT_ASSERT(oldCold.Ref.CacheItem() == coldSource);
        const char* oldHotData = oldHot.Ref.data();
        const char* oldColdData = oldCold.Ref.data();
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, coldSource));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index()).IsCold());
        const ui64 residentBytes = fixture.Cache->ResidentBytes();

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(target.Limit));
        for (ui32 step = 0; step < 10000; ++step) {
            const TSpaceState state = TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space);
            if (TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate &&
                !state.Resizing() && state.AddressBits() == target.AddressBits)
            {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // quarantine the high worker spare

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterReplacingPublished, hotSource.CacheItem());
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
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners + 1);

        const THandleState forwarding = TSharedCacheTestAccess::HandleState(*fixture.Cache, hotSource.Index());
        UNIT_ASSERT(forwarding.IsReplacing());
        const TCacheItem hotReplacement =
            TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(*fixture.Cache, hotSource.Index())).WithoutFrozen();
        UNIT_ASSERT(hotReplacement.Index() >= 2 && hotReplacement.Index() < target.HandleCount());

        TTestSharedPageRef acquiredHot;
        UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, hotSource, acquiredHot));
        UNIT_ASSERT(acquiredHot.CacheItem().CacheItem() == hotReplacement);
        acquiredHot.Drop();

        auto foundHot = FindPage(*fixture.Cache, keys[keys.size() - 2]);
        UNIT_ASSERT(foundHot.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(foundHot.Ref.CacheItem().CacheItem() == hotReplacement);
        UNIT_ASSERT_VALUES_EQUAL(foundHot.Ref.data(), oldHotData);
        UNIT_ASSERT_VALUES_EQUAL(oldHot.Ref.data(), oldHotData);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        foundHot.Ref.Drop();

        TPageCacheItem duplicate;
        TTestSharedPageRef existing;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(keys[keys.size() - 2].Word(1)),
                        EStickyState::None, duplicate, existing) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(existing.CacheItem().CacheItem() == hotReplacement);
        existing.Drop();
        oldHot.Ref.Drop();
        const THandleState relocationOwner = TSharedCacheTestAccess::HandleState(*fixture.Cache, hotSource.Index());
        UNIT_ASSERT(relocationOwner.IsReplacing());
        UNIT_ASSERT_VALUES_EQUAL(relocationOwner.Refs(), 1);

        gate.Slots[0].Release();
        relocator.Join();
        UNIT_ASSERT(relocated);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, hotSource.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners);

        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterReplacedPublished, coldSource.CacheItem());
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
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners + 1);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index()).IsReplaced());
        const TCacheItem coldReplacement =
            TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(*fixture.Cache, coldSource.Index())).WithoutFrozen();
        UNIT_ASSERT(coldReplacement.Index() >= 2 && coldReplacement.Index() < target.HandleCount());
        TTestSharedPageRef acquiredCold;
        UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, coldSource, acquiredCold));
        UNIT_ASSERT(acquiredCold.CacheItem().CacheItem() == coldReplacement);
        acquiredCold.Drop();
        auto foundCold = FindPage(*fixture.Cache, keys.back());
        UNIT_ASSERT(foundCold.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(foundCold.Ref.CacheItem().CacheItem() == coldReplacement);
        UNIT_ASSERT_VALUES_EQUAL(foundCold.Ref.data(), oldColdData);
        UNIT_ASSERT_VALUES_EQUAL(oldCold.Ref.data(), oldColdData);
        foundCold.Ref.Drop();
        gate.Slots[0].Release();
        coldRelocator.Join();
        UNIT_ASSERT(coldRelocated);
        const THandleState externalOwner = TSharedCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index());
        UNIT_ASSERT(externalOwner.IsReplaced());
        UNIT_ASSERT_VALUES_EQUAL(externalOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(oldCold.Ref.data(), oldColdData);
        oldCold.Ref.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageOwners);

        for (ui32 step = 0;
             step < 10000 && (TSharedCacheTestAccess::HandleCount(*fixture.Cache) != target.HandleCount() ||
                                 TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink completion stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        auto finalHot = FindPage(*fixture.Cache, keys[keys.size() - 2]);
        auto finalCold = FindPage(*fixture.Cache, keys.back());
        UNIT_ASSERT(finalHot.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(finalCold.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(finalHot.Ref.CacheItem().CacheItem() == hotReplacement);
        UNIT_ASSERT(finalCold.Ref.CacheItem().CacheItem() == coldReplacement);
    }
    Y_UNIT_TEST(StickyPagesUnlinkOnceThenRelocateBeforeShrink) {
        constexpr ui32 HazardCount = 8;
        TFixture fixture(6, HazardCount, 6);
        TSharedCacheCapacity target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, HazardCount, target));

        const TCollectionLocation collectionLocation{ .Id = TLogoBlobID(70, 71, 72) };
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, collectionLocation,
                        collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(collectionLocation.Id)));

        TVector<TSharedCacheKey> regularKeys;
        TVector<TPageCacheItem> regularPages;
        for (ui64 offset = 0; offset < target.HandleCount() - 3; ++offset) {
            regularKeys.push_back(TSharedCacheKey::Page(collection, offset));
            regularPages.push_back(InsertReadyPage(*fixture.Cache, regularKeys.back()));
        }
        UNIT_ASSERT_VALUES_EQUAL(regularPages.back().Index(), target.HandleCount() - 1);

        auto insertSticky = [&](ui64 offset) {
            TPageCacheItem page;
            TTestSharedPageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset), EStickyState::Sticky,
                            page, hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            return page;
        };

        const ui64 firstStickyOffset = target.HandleCount();
        const ui64 secondStickyOffset = firstStickyOffset + 1;
        const TPageCacheItem firstSticky = insertSticky(firstStickyOffset);
        const TPageCacheItem secondSticky = insertSticky(secondStickyOffset);
        TTestSharedPageRef heldFirst;
        TTestSharedPageRef heldSecond;
        UNIT_ASSERT(fixture.Cache->Find(collection, firstStickyOffset, heldFirst) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(fixture.Cache->Find(collection, secondStickyOffset, heldSecond) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(heldFirst.IsSticky() && heldSecond.IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(firstSticky.Index(), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(secondSticky.Index(), target.HandleCount() + 1);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), secondSticky.Index());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::NextInOwner(*fixture.Cache, secondSticky.Index()), firstSticky.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, firstSticky.Index()), 0);

        for (ui32 index = 0; index < 2; ++index) {
            const TPageCacheItem page = regularPages[index];
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            const ui32 coldVersion = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
            UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(
                *fixture.Cache, TColdCacheItem::Make(coldVersion & MaxItemVersion, page.Index())));
        }

        const ui64 residentBytes = fixture.Cache->ResidentBytes();
        const ui64 keepBytes = fixture.Cache->StickyBytes();
        const ui64 stickyOwnedBytes = fixture.Cache->StickyOwnedBytes();
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(target.Limit));
        for (ui32 step = 0; step < 10000; ++step) {
            const TSpaceState state = TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space);
            if (TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate &&
                !state.Resizing() && state.AddressBits() == target.AddressBits)
            {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // quarantine the high worker spare

        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // detach the Sticky list and Unsticky every cut page
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { firstSticky, secondSticky }) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsTransitionField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }

        UNIT_ASSERT(heldFirst.IsSticky() && heldSecond.IsSticky());
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // relocate and re-Sticky the first cut page
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, firstSticky.Index()).IsReplaced());
        auto firstFound = FindPage(*fixture.Cache, TSharedCacheKey::Page(collection, firstStickyOffset));
        UNIT_ASSERT(firstFound.Status == ESharedCacheResultStatus::Hit);
        const TPageCacheItem firstReplacement = firstFound.Ref.CacheItem();
        firstFound.Ref.Drop();
        UNIT_ASSERT(firstReplacement.Index() < target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), firstReplacement.Index());
        const THandleState firstReplacementState =
            TSharedCacheTestAccess::HandleState(*fixture.Cache, firstReplacement.Index());
        UNIT_ASSERT(firstReplacementState.IsSticky());
        UNIT_ASSERT(firstReplacementState.IsStickyField());

        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // relocate and re-Sticky the second cut page
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, secondSticky.Index()).IsReplaced());
        auto secondFound = FindPage(*fixture.Cache, TSharedCacheKey::Page(collection, secondStickyOffset));
        UNIT_ASSERT(secondFound.Status == ESharedCacheResultStatus::Hit);
        const TPageCacheItem secondReplacement = secondFound.Ref.CacheItem();
        secondFound.Ref.Drop();
        UNIT_ASSERT(secondReplacement.Index() < target.HandleCount());
        const THandleState secondReplacementState =
            TSharedCacheTestAccess::HandleState(*fixture.Cache, secondReplacement.Index());
        UNIT_ASSERT(secondReplacementState.IsSticky());
        UNIT_ASSERT(secondReplacementState.IsStickyField());
        UNIT_ASSERT(heldFirst.IsSticky() && heldSecond.IsSticky());
        heldFirst.Drop();
        heldSecond.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, firstSticky.Index()).IsFree());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, secondSticky.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), secondReplacement.Index());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::NextInOwner(*fixture.Cache, secondReplacement.Index()), firstReplacement.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, firstReplacement.Index()), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), keepBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), stickyOwnedBytes);

        for (ui32 step = 0;
             step < 10000 && (TSharedCacheTestAccess::HandleCount(*fixture.Cache) != target.HandleCount() ||
                                 TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step)
        {
            fixture.Cache->RunMaintenance();
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), keepBytes);
    }
    Y_UNIT_TEST(DetachedStickyCutPagesFinishShrink) {
        constexpr ui32 HazardCount = 8;
        TFixture fixture(6, HazardCount, 6);
        TSharedCacheCapacity target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, HazardCount, target));

        const TCollectionLocation collectionLocation{ .Id = TLogoBlobID(73, 74, 75) };
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, collectionLocation,
                        collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(collectionLocation.Id)));

        TVector<TPageCacheItem> regularPages;
        for (ui64 offset = 0; offset < target.HandleCount() - 3; ++offset) {
            regularPages.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
        }
        const auto insertSticky = [&](ui64 offset) {
            TPageCacheItem page;
            TTestSharedPageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset), EStickyState::Sticky,
                            page, hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            return page;
        };
        const ui64 heldOffset = target.HandleCount();
        const TPageCacheItem heldPage = insertSticky(heldOffset);
        const TPageCacheItem unheldPage = insertSticky(heldOffset + 1);
        UNIT_ASSERT_VALUES_EQUAL(heldPage.Index(), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(unheldPage.Index(), target.HandleCount() + 1);
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, heldOffset, held) == ESharedCacheResultStatus::Hit);
        const char* heldData = held.data();

        for (ui32 index = 0; index < 2; ++index) {
            const TPageCacheItem page = regularPages[index];
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, page.Index())));
        }

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(target.Limit));
        UNIT_ASSERT(fixture.Cache->UpdateHardLimit(target.Limit));
        for (ui32 step = 0; step < 10000; ++step) {
            const TSpaceState state = TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space);
            if (TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate &&
                !state.Resizing() && state.AddressBits() == target.AddressBits) {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink routing stalled at " << step);
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Migrate);
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // quarantine the high worker spare
        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // unstick both cut pages
        for (TPageCacheItem page : { heldPage, unheldPage }) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky() && state.IsTransitionField());
        }

        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        collectionRef.Drop();
        for (ui32 step = 0; step < 10000; ++step) {
            if (TSharedCacheTestAccess::HandleState(*fixture.Cache, heldPage.Index()).IsReplaced() &&
                TSharedCacheTestAccess::HandleState(*fixture.Cache, unheldPage.Index()).IsFree()) {
                break;
            }
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "detached cut stalled at " << step);
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, heldPage.Index()).IsReplaced());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, unheldPage.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(held.data(), heldData);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);

        held.Drop();
        for (ui32 step = 0;
             step < 10000 && (TSharedCacheTestAccess::HandleCount(*fixture.Cache) != target.HandleCount() ||
                                 TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) != ETransitionPhase::Idle);
             ++step) {
            fixture.Cache->RunMaintenance();
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT(TSharedCacheTestAccess::HardTransitionPhase(*fixture.Cache) == ETransitionPhase::Idle);
        auto found = FindPage(*fixture.Cache, TSharedCacheKey::Page(collection, heldOffset));
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(found.Ref.data(), heldData);
    }
    Y_UNIT_TEST(PageBudgetExcludesStaticMetadataButConsumptionIncludesIt) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->StaticBytes() > bytes);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(bytes));
        const TCollectionCacheItem collection = MakeCollectionCacheItem(519, 520);
        InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes() + bytes);
        fixture.Cache->EnforceCurrentLimit();
        TTestSharedPageRef ref;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, ref) == ESharedCacheResultStatus::Hit);
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
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        const ui64 bytes = (SharedCacheTransitionWorkBatch + 1) * pageBytes;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), SharedCacheTransitionWorkBatch + 1);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        progress.Cache = fixture.Cache.Get();
        TTestTraits hooks{ &progress, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              auto& progress = *static_cast<TProgress*>(context);
                              if (point == ESharedCacheHookPoint::AfterAdmissionReclaimStep) {
                                  if (++progress.Steps == SharedCacheTransitionWorkBatch) {
                                      progress.Cache->ArmResourceWait(progress.Cache->AdmissionGeneration());
                                  }
                              } else if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  ++progress.Notifications;
                              }
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        UNIT_ASSERT(!TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(progress.Steps, SharedCacheTransitionWorkBatch);
        UNIT_ASSERT_VALUES_EQUAL(progress.Notifications, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }

    Y_UNIT_TEST(AdmissionWithoutReclaimProgressRemainsQuiet) {
        ui32 notifications = 0;
        TFixture fixture(5);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(1));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, 1));
        TTestTraits hooks{ &notifications, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  ++*static_cast<ui32*>(context);
                              }
                          } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        UNIT_ASSERT(!TSharedCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        fixture.Cache->DisarmResourceWait();
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, 1);
    }

    Y_UNIT_TEST(HandleAdmissionContinuesFrequencyAgingAtFixedHotSize) {
        TFixture fixture(5, 1);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(bytes));
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 hotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        const auto collection = MakeCollectionCacheItem(573, 574);
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        for (ui32 touch = 0; touch < 2; ++touch) {
            TTestSharedPageRef held;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ESharedCacheResultStatus::Hit);
        }
        for (ui32 index = 0; index < fixture.Capacity.HandleCount() - 3; ++index) {
            const TLogoBlobID id(201, 202, index + 1);
            const auto item = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(item);
            UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, item, MakeCollection(id)));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Frequency(), 2);
        const auto admitted = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(TLogoBlobID(203, 204, 1)));
        UNIT_ASSERT(admitted);
        UNIT_ASSERT_VALUES_EQUAL(admitted.Index(), page.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache), hotSlots);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, admitted);
    }

    Y_UNIT_TEST(HotLayoutGrowthReportsMaintenanceProgress) {
        TFixture fixture(8);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 before = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::MinimumHotSlots(*fixture.Cache), before);
        UNIT_ASSERT(fixture.Cache->EnforceCurrentLimit());
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) > before);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }

    Y_UNIT_TEST(CurrentLimitReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const ui64 limit = 2 * bytes;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(limit));

        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, 0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), limit);

        // In-flight commitment has no reclaimable buffer; rejected reservations change no counters.
        const ui64 overall = fixture.Cache->OverallUsage();
        UNIT_ASSERT(!TSharedCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overall);

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(limit - bytes));
        UNIT_ASSERT(!TSharedCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), limit);
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, limit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }
    Y_UNIT_TEST(CurrentLimitRejectsReservationUntilHeldBufferIsReleased) {
        TFixture fixture;
        const auto collection = MakeCollectionCacheItem(541, 542);
        InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(!TSharedCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(held.size(), 4096);
        held.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }

    Y_UNIT_TEST(ReservedPageMakeReadyTransfersReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));

        const TCollectionCacheItem collection = MakeCollectionCacheItem(36, 37);
        TPageCacheItem cacheItem;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::None, cacheItem,
                        hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->MakeReady(cacheItem, MakePageData(cacheItem.Index())));

        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);

        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, cacheItem));
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }
    Y_UNIT_TEST(ReadyPageRejectsCompletionWithoutLeakingReference) {
        TFixture fixture;
        const auto collection = MakeCollectionCacheItem(36, 37);
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsReady());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);

        UNIT_ASSERT(!fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);
        UNIT_ASSERT(!fixture.Cache->FailReady(page));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 0);

        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
    }
    Y_UNIT_TEST(ReservationReclaimsCold) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(47, 48);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        const ui64 bytes = fixture.Cache->PayloadUsage();
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->PayloadUsage()));

        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), fixture.Cache->CurrentLimit());

        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }
    Y_UNIT_TEST(CollectionAdmissionIsTemporarilyUnaccounted) {
        TFixture fixture;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->StaticBytes()));
        const TCollectionLocation location{ .Id = TLogoBlobID(38, 39, 40) };

        TCollectionCacheItem inserted;
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, location, inserted, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!hit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, inserted, MakeCollection(location.Id)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(FailedPageReadyRefundsReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(41, 42);

        TPageCacheItem cacheItem;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::None, cacheItem,
                        hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->FailReady(cacheItem));

        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
        const THandleState free = TSharedCacheTestAccess::HandleState(*fixture.Cache, cacheItem.Index());
        UNIT_ASSERT(free.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(free.Version(), AdvanceItemVersion(cacheItem.Version()));
        UNIT_ASSERT(!fixture.Cache->FailReady(cacheItem));

        TTestSharedPageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, missing) == ESharedCacheResultStatus::Miss);
    }
    Y_UNIT_TEST(FailedStickyPageReadyRefundsOwnership) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TLogoBlobID id(42, 43, 44);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        TPageCacheItem cacheItem;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::Sticky, cacheItem,
                        hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->FailReady(cacheItem));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
    }
    Y_UNIT_TEST(CurrentLimitUsesPolicyPath) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(30, 31);
        for (ui64 offset = 0; offset < 30; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
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
                             << " phase=" << static_cast<ui32>(TSharedCacheTestAccess::HotResizePhase(*fixture.Cache))
                             << " effectiveHot=" << TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache)
                             << " coldBytes=" << fixture.Cache->ColdBytes());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HardLimit(), fixture.Capacity.Limit);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), fixture.Capacity.HandleCount());
    }
    Y_UNIT_TEST(CurrentLimitReclaimsToSoftLimit) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(45, 46);
        for (ui64 offset = 0; offset < 30; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
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
            const auto [minimum, growth] = TSharedCacheTestAccess::ColdByteWatermarks(*fixture.Cache);
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
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(83, 84), 0);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        const auto before = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(before.IsHot());
        TCacheHotPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 64> slots{};
        TCacheTierRings rings;
        const TCacheTierView wrong{
            .Rings = &rings,
            .HotStorage = { slots.data(), sizeof(slots), slots.size() },
            .Id = ECacheTier::Keep,
        };
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.ProcessHot(spaceOp, wrong, EHotLevel::L0, page.CacheItem());
        });
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Raw(), before.Raw());
        for (const auto& slot : slots) {
            UNIT_ASSERT_VALUES_EQUAL(slot.load(), 0);
        }
    }

    Y_UNIT_TEST(HotCutDoesNotEvictAnotherTiersPage) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(83, 85), 0);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        const auto before = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
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

        TTestTraits hooks{ &context, [](void* ptr, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                              auto& ctx = *static_cast<TContext*>(ptr);
                              if (point == ESharedCacheHookPoint::BeforeHotExchange && item == ctx.Page) {
                                  ctx.Visited = true;
                                  ctx.Limit.store(HotLevelCount);
                              }
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            UNIT_ASSERT(policy.PutHot(spaceOp, wrong, EHotLevel::L0, page.CacheItem()).IsNull());
        });
        UNIT_ASSERT(context.Visited);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Raw(), before.Raw());
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
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
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

    Y_UNIT_TEST(HotMinimumIsAbsoluteAcrossBudgetsAndStickyUsage) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const auto ordinary = MakeCollectionCacheItem(531, 532);
        InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(ordinary, 0));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(100 * bytes));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::MinimumHotSlots(*fixture.Cache), 15);
        const TLogoBlobID id(95, 96, 121);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        for (ui32 offset = 0; offset < 20; ++offset) {
            const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            UNIT_ASSERT(TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        }
        for (ui64 limit : { ui64{ 0 }, bytes, 100 * bytes, fixture.Cache->CurrentLimit() }) {
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::MinimumHotSlots(*fixture.Cache), 15);
        }
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache), 15);
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            for (ui32 level = 0; level < 4; ++level) {
                UNIT_ASSERT(spaceOp.Hot(static_cast<EHotLevel>(level)).Capacity <= SharedCacheTransitionWorkBatch / 4);
            }
        });
    }
    Y_UNIT_TEST(InFlightPressureKeepsAbsoluteHotMinimumAndRecoveryCanGrow) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(MakeCollectionCacheItem(537, 538), 0));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(100 * bytes));
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache), 15);

        const ui64 reservation = fixture.Cache->CurrentLimit() - fixture.Cache->PayloadUsage();
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, reservation));
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            for (ui32 pass = 0; pass < 200; ++pass) {
                TSharedCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, bytes + 1, false);
            }
        });
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache), 15);
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, reservation);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) > 15);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::MinimumHotSlots(*fixture.Cache), 15);
    }
    Y_UNIT_TEST(BoundedHotAgingVisitsEveryLevelOfLargeRings) {
        TFixture fixture(10);
        const auto collection = MakeCollectionCacheItem(535, 536);
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        for (ui32 touch = 0; touch < 3; ++touch) {
            TTestSharedPageRef ref;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, ref) == ESharedCacheResultStatus::Hit);
        }
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            UNIT_ASSERT(spaceOp.Hot(EHotLevel::L0).Capacity > SharedCacheTransitionWorkBatch);
        });
        for (ui32 pass = 0; pass < 200 && fixture.Cache->HotPages() != 0; ++pass) {
            TSharedCacheTestAccess::ProcessHotAtMinimum(*fixture.Cache);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
    }
    Y_UNIT_TEST(StickyBytesAreExcludedFromHotColdAndKeepBudgets) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(20 * bytes));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(20 * bytes));
        const TLogoBlobID id(95, 96, 120);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        for (ui32 offset = 0; offset < 4; ++offset) {
            const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            UNIT_ASSERT(TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), 4 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->EvictableByteBudget(), 16 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdLimit(), (16 * bytes * 2) / 5);
        fixture.Cache->EnforceCurrentLimit();
        const auto [minimum, growth] = TSharedCacheTestAccess::ColdByteWatermarks(*fixture.Cache);
        UNIT_ASSERT_VALUES_EQUAL(minimum, (16 * bytes + 4) / 5);
        UNIT_ASSERT_VALUES_EQUAL(growth, (16 * bytes * 3 + 9) / 10);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(3 * bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->EvictableByteBudget(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdLimit(), 0);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(20 * bytes));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->EvictableByteBudget(), 20 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdLimit(), 8 * bytes);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->EvictableByteBudget(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdLimit(), 0);
    }
    Y_UNIT_TEST(SoftLimitOverrideReclaimsAndSurvivesCurrentLimitChange) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(46, 47);
        for (ui64 offset = 0; offset < 30; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
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
            const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            if (atMinimum) {
                TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
            }
            const ui64 hotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
            // A frequent page at minimum Hot must age normally before reclamation.
            if (atMinimum) {
                for (ui32 touch = 0; touch < 2; ++touch) {
                    TTestSharedPageRef ref;
                    UNIT_ASSERT(fixture.Cache->Find(collection, 0, ref) == ESharedCacheResultStatus::Hit);
                }
            }
            const ui64 current = fixture.Cache->CurrentLimit();
            UNIT_ASSERT(fixture.Cache->SoftLimit() > 2 * bytes);
            UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, current - bytes));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), current);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
            UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
            const ui64 usage = fixture.Cache->PayloadUsage();
            TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, fixture.Cache->ReservedBytes());
            UNIT_ASSERT_LE(usage, current);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
            if (atMinimum) {
                UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache),
                    TSharedCacheTestAccess::MinimumHotSlots(*fixture.Cache));
            } else {
                UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
            }
        }
    }
    Y_UNIT_TEST(StickyWithdrawalDuringCompletionUsesLiveRegularAdmission) {
        for (auto point :
            { ESharedCacheHookPoint::BeforePageReadyState, ESharedCacheHookPoint::BeforeReadyStatePublication }) {
            TFixture fixture;
            const TLogoBlobID id(95, 96, 120);
            const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
            TTestSharedPageRef held;
            auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
            TVector<TTestSharedCachePageRequest> requests;
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
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
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
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ESharedCacheResultStatus::Hit);
        // Private single-page helpers admit explicitly ordinary pages; promote the held page first.
        UNIT_ASSERT(TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        UNIT_ASSERT(held.IsSticky());
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(!held.IsSticky());
        UNIT_ASSERT(TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        UNIT_ASSERT(held.IsSticky());
    }

    struct TColdPoolFixture : TFixture {
        static constexpr ui64 PageBytes = 4096 + NActors::TSharedData::OverheadSize;
        TCollectionCacheItem Regular;
        TVector<TPageCacheItem> Keep;

        TColdPoolFixture()
            : TFixture(8)
        {
            UNIT_ASSERT(Cache->UpdateCurrentLimit(100 * PageBytes));
            UNIT_ASSERT(Cache->UpdateSoftLimitOverride(20 * PageBytes));
            UNIT_ASSERT(Cache->UpdateKeepColdLimit(10 * PageBytes));
            const TLogoBlobID keepId(95, 96, 123);
            const auto keep = AllocateCollection(*Cache, TSharedCacheKey::Collection(keepId));
            UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*Cache, Registry, keep, MakeCollection(keepId)));
            UNIT_ASSERT(Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
            for (ui32 offset = 0; offset < 20; ++offset) {
                Keep.push_back(InsertReadyPage(*Cache, TSharedCacheKey::Page(keep, offset)));
            }
            const TLogoBlobID regularId(95, 96, 124);
            Regular = AllocateCollection(*Cache, TSharedCacheKey::Collection(regularId));
            UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*Cache, Registry, Regular, MakeCollection(regularId)));
            UNIT_ASSERT(Cache->SetCollectionPagesCacheMode(Regular, ECacheMode::Regular));
            UNIT_ASSERT_VALUES_EQUAL(Cache->RetainedBytes(), 20 * PageBytes);
        }

        TTestSharedPageRef Fetch(ui32 offset) {
            TTestSharedPageRef held;
            auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*Cache, held);
            TVector<TTestSharedCachePageRequest> requests;
            requests.emplace_back(MakePageLocation(offset), waiter);
            UNIT_ASSERT(Cache->FindOrInsertBatch(Regular, requests));
            UNIT_ASSERT(requests.front().Status() == ESharedCacheResultStatus::Inserted);
            const auto page = requests.front().Fetch().CacheItem();
            UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(page.Index())));
            UNIT_ASSERT(waiter->Ready());
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*Cache, page.Index()).IsCold());
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
        TTestSharedPageRef hit;
        UNIT_ASSERT(fixture.Cache->Find(fixture.Regular, 0, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hit.CacheItem() == item);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, item.Index()).IsCold());
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
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.PublishCold(spaceOp, wrong, page.CacheItem());
            policy.RouteCold(spaceOp, wrong, ticket);
            UNIT_ASSERT_VALUES_EQUAL(rings.ColdEntries(), 0);
            UNIT_ASSERT(!TSharedCacheTestAccess::BindOperation(*fixture.Cache, spaceOp).Eviction().EraseCold(ticket,
                ECacheTier::Keep));
            for (bool preload : { false, true }) {
                rings.AddColdEntry();
                slots[0].store(ticket.Raw());
                UNIT_ASSERT(!(preload ? policy.ReclaimForPreload(spaceOp, wrong) : policy.Reclaim(spaceOp, wrong)));
                UNIT_ASSERT_VALUES_EQUAL(rings.ColdEntries(), 0);
                UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
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
            TColdCacheItem::Make(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()), page.Index());
        TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 4> firstSlots{};
        std::array<std::atomic<ui64>, 8> secondSlots{};
        TCacheTierRings firstRings;
        TCacheTierRings secondRings;
        const TCacheTierView first{ &firstRings, {}, { firstSlots.data(), sizeof(firstSlots), firstSlots.size() } };
        const TCacheTierView second{ &secondRings, {},
            { secondSlots.data(), sizeof(secondSlots), secondSlots.size() } };
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
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
            TColdCacheItem::Make(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()), page.Index());
        held.Drop();
        TTestSharedPageRef reader;
        UNIT_ASSERT(fixture.Cache->Find(fixture.Regular, 0, reader) == ESharedCacheResultStatus::Hit);
        reader.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()) != oldTicket.ColdVersion());
        TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
        std::array<std::atomic<ui64>, 4> slots{};
        TCacheTierRings rings;
        const TCacheTierView tier{ &rings, {}, { slots.data(), sizeof(slots), slots.size() } };
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.RouteCold(spaceOp, tier, oldTicket);
            UNIT_ASSERT_VALUES_EQUAL(rings.ColdEntries(), 0);
            UNIT_ASSERT(!policy.Reclaim(spaceOp, tier));
        });
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
    }

    Y_UNIT_TEST(ColdPublicationRejectsAReusedNativeIdentity) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 126);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const auto key = TSharedCacheKey::Page(collection, 0);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        auto held = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(held.Ref);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        auto fillers = HoldReviewFillers(fixture, 63, key.Hash() & 63);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(1));
        held.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        const auto replacement = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), page.Index());
        UNIT_ASSERT(replacement.Version() != page.Version());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, replacement.Index()).IsCold());
        const ui64 entries = fixture.Cache->ColdRingEntries();
        const ui32 membership = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, replacement.Index());
        TCacheColdPolicy<TTestTraits> policy(*fixture.Cache);
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            policy.PublishCold(spaceOp, spaceOp.View().Tiers[ECacheTier::Regular], page.CacheItem());
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), entries);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, replacement.Index()), membership);
    }

    Y_UNIT_TEST(KeepColdPhysicalOccupancyIncludesStaleWords) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 127);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * TColdPoolFixture::PageBytes));
        const auto key = TSharedCacheKey::Page(collection, 0);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        auto held = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(held.Ref && TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 1);
        UNIT_ASSERT(TSharedCacheTestAccess::SweepKeepCold(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::ColdEntries(*fixture.Cache, ECacheTier::Keep), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
    }

    Y_UNIT_TEST(LastColdReleasePublishesAfterRefcountABA) {
        TColdPoolFixture fixture;
        auto held = fixture.Fetch(0);
        const auto page = held.CacheItem();
        const auto before = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        const ui32 coldVersion = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
        UNIT_ASSERT(before.IsCold() && before.Refs() == 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeLastColdRefRelease, page.CacheItem());
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
        TTestSharedPageRef other;
        UNIT_ASSERT(cache->Find(fixture.Regular, 0, other) == ESharedCacheResultStatus::Hit);
        const auto acquired = TSharedCacheTestAccess::HandleState(*cache, page.Index());
        UNIT_ASSERT(acquired.IsCold() && acquired.Refs() == 2);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::ColdVersion(*cache, page.Index()), AdvanceColdVersion(coldVersion));
        other.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*cache, page.Index()).Raw(), before.Raw());
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 0);
        gate.Slots[0].Release();
        releaser.Join();
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdReclaimableItems(), 1);
        UNIT_ASSERT(cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdRingEntries(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->RetainedBytes(), 20 * fixture.PageBytes);
        UNIT_ASSERT(cache->Find(fixture.Regular, 0, other) == ESharedCacheResultStatus::Miss);
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
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, firstItem.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 21 * fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 20 * fixture.PageBytes);
        for (auto keep : fixture.Keep) {
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, keep.Index()).IsReady());
        }
    }

    Y_UNIT_TEST(ColdPromotionClaimsSoftBudgetWhenRoomAppears) {
        TColdPoolFixture fixture;
        auto page = fixture.Fetch(0);
        const auto item = page.CacheItem();
        page.Drop();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(21 * fixture.PageBytes));
        TTestSharedPageRef hit;
        UNIT_ASSERT(fixture.Cache->Find(fixture.Regular, 0, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, item.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 21 * fixture.PageBytes);
    }

    Y_UNIT_TEST(SoftPressureDemotesKeepColdWithoutFreeingItsBuffer) {
        TColdPoolFixture fixture;
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, fixture.Keep.front()));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(19 * fixture.PageBytes));
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT_LE(fixture.Cache->RetainedBytes(), 19 * fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 20 * fixture.PageBytes);
        for (auto keep : fixture.Keep) {
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, keep.Index()).IsReady());
        }
    }

    Y_UNIT_TEST(SoftDemotionWithdrawsHeldKeepColdMembership) {
        TColdPoolFixture fixture;
        const auto page = fixture.Keep.front();
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        auto held = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, page.CacheItem());
        UNIT_ASSERT(held);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(19 * fixture.PageBytes));
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        held.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 19 * fixture.PageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 20 * fixture.PageBytes);
    }

    Y_UNIT_TEST(ColdFreeByteBalanceSubtractsInFlightCommitment) {
        TFixture fixture(8);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(200 * bytes));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(100 * bytes));
        const auto collection = MakeCollectionCacheItem(539, 540);
        InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(
            *fixture.Cache, fixture.Cache->CurrentLimit() - fixture.Cache->PayloadUsage()));
        UNIT_ASSERT(fixture.Cache->RetainedBytes() < fixture.Cache->SoftLimit());
        const ui64 before = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < before);
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, fixture.Cache->ReservedBytes());
    }

    Y_UNIT_TEST(ColdOutsideSoftRegularPagesShareFetchAndLiveUntilLastReference) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TLogoBlobID id(95, 96, 113);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        TTestSharedPageRef first;
        TTestSharedPageRef second;
        auto firstWaiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, first);
        auto secondWaiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, second);
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), firstWaiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto page = requests.front().Fetch().CacheItem();
        TTestSharedPageRef hit;
        TVector<TTestSharedCachePageRequest> duplicate;
        duplicate.emplace_back(MakePageLocation(0), secondWaiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, duplicate));
        UNIT_ASSERT(duplicate.front().Status() == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), bytes);
        UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(firstWaiter->Ready() && secondWaiter->Ready());
        UNIT_ASSERT(first.CacheItem() == second.CacheItem());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), 0);
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        first.Drop();
        second.Drop();
        UNIT_ASSERT_VALUES_EQUAL(hit.size(), 4096);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        hit.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, hit) == ESharedCacheResultStatus::Miss);
    }
    Y_UNIT_TEST(ColdOutsideSoftLastReleaseCannotResurrectTheOldPage) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 117);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        TTestSharedPageRef held;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), waiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        const auto oldPage = requests.front().Fetch().CacheItem();
        UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(oldPage.Index())));
        UNIT_ASSERT(waiter->Ready());
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterTombstoneOwnerClaim, oldPage.CacheItem());
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
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, oldPage.Index()).IsTombstone());
        TTestSharedPageRef hit;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, hit) == ESharedCacheResultStatus::Miss);
        TTestSharedPageRef replacement;
        auto nextWaiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, replacement);
        TVector<TTestSharedCachePageRequest> next;
        next.emplace_back(MakePageLocation(0), nextWaiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, next));
        UNIT_ASSERT(next.front().Status() == ESharedCacheResultStatus::Inserted);
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
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TLogoBlobID keepId(95, 96, 114);
        const auto keep = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(keepId));
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, keep, MakeCollection(keepId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(keep, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(bytes));
        const auto kept = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(keep, 0));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, kept));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(bytes));
        const TLogoBlobID regularId(95, 96, 115);
        const auto regular = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(regularId));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, regular, MakeCollection(regularId)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(regular, ECacheMode::Regular));
        TTestSharedPageRef ref;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, ref);
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(0), waiter);
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(regular, requests));
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, kept.Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
        UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(requests.front().Fetch().CacheItem().Index())));
        UNIT_ASSERT(waiter->Ready());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, kept.Index()).IsKeepCold());
        ref.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableBytes(), bytes);
    }
    Y_UNIT_TEST(ColdOutsideSoftPagePromotesAfterCollectionBecomesKeepOrSticky) {
        for (ECacheMode mode : { ECacheMode::TryKeepInMemory, ECacheMode::Sticky }) {
            TFixture fixture;
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            const TLogoBlobID id(95, 96, 116);
            const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
            TTestSharedPageRef held;
            auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
            TVector<TTestSharedCachePageRequest> requests;
            requests.emplace_back(MakePageLocation(0), waiter);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
            const auto page = requests.front().Fetch().CacheItem();
            UNIT_ASSERT(requests.front().Fetch().MakeReady(MakePageData(page.Index())));
            UNIT_ASSERT(waiter->Ready());
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, mode));
            TVector<TTestSharedCachePageRequest> hits;
            hits.emplace_back(MakePageLocation(0), waiter);
            UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
            UNIT_ASSERT(hits.front().Status() == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
            if (mode == ECacheMode::Sticky) {
                UNIT_ASSERT(hits.front().Page().IsSticky());
            } else {
                UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
            }
        }
    }
    Y_UNIT_TEST(HotAtMinimumRetainsFrequencyAging) {
        TFixture fixture(5);
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 minimum = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        const TCollectionCacheItem collection = MakeCollectionCacheItem(515, 516);
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        for (ui32 hit = 0; hit < 2; ++hit) {
            TTestSharedPageRef ref;
            UNIT_ASSERT(fixture.Cache->Find(collection, 0, ref) == ESharedCacheResultStatus::Hit);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Frequency(), 2);
        UNIT_ASSERT(TSharedCacheTestAccess::ProcessHotAtMinimum(*fixture.Cache));
        const auto aged = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(aged.IsHot());
        UNIT_ASSERT_VALUES_EQUAL(aged.Frequency(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::ProcessHotAtMinimum(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache), minimum);
    }
    Y_UNIT_TEST(PressureEvictsHotPagesAtMinimumWithoutScanningHandles) {
        for (bool zeroPayload : { false, true }) {
            TFixture fixture(5);
            TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
            const TCollectionCacheItem collection = MakeCollectionCacheItem(517, 518);
            for (ui64 offset = 0; offset < 4; ++offset) {
                InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            }
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            const ui64 limit = zeroPayload ? 0 : bytes;
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
            const ui64 minimum = TSharedCacheTestAccess::MinimumHotSlots(*fixture.Cache);
            const ui64 target = zeroPayload ? 0 : bytes;
            for (ui32 step = 0;
                 step < 100 && (zeroPayload ? fixture.Cache->PayloadUsage() : fixture.Cache->RetainedBytes()) > target;
                 ++step) {
                fixture.Cache->EnforceCurrentLimit();
            }
            UNIT_ASSERT_LE((zeroPayload ? fixture.Cache->PayloadUsage() : fixture.Cache->RetainedBytes()), target);
            UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) >= minimum);
        }
    }
    Y_UNIT_TEST(PressureAtMinimumPreservesStickyAndHeldData) {
        TFixture fixture(5);
        const TLogoBlobID id(95, 96, 107);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const auto heldItem = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, held) == ESharedCacheResultStatus::Hit);
        InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        const auto sticky = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(fixture.Cache->MakeReady(sticky, MakePageData(sticky.Index())));
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(0));
        for (ui32 step = 0; step < 30; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_VALUES_EQUAL(held.size(), 4096);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, heldItem.Index()).IsCold());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, sticky.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 4096 + NActors::TSharedData::OverheadSize);
        TTestSharedPageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, missing) == ESharedCacheResultStatus::Miss);
    }
    Y_UNIT_TEST(KeepColdRemainsProtectedWhenPhysicalHeadroomAllowsHotGrowth) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(10 * bytes));
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 minimum = TSharedCacheTestAccess::MinimumHotSlots(*fixture.Cache);
        const TLogoBlobID id(95, 96, 108);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(5 * bytes));
        TVector<TPageCacheItem> keep;
        for (ui32 offset = 0; offset < 4; ++offset) {
            keep.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, keep.back()));
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(MakeCollectionCacheItem(521, 522), offset));
        }
        for (ui32 step = 0; step < 10; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdLimit(), 4 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 4 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), 8 * bytes);
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) >= minimum);
        for (const auto page : keep) {
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        }
    }
    Y_UNIT_TEST(KeepColdPressureFinishesPendingTrimBeforeShrinkingAgain) {
        // The three words sit at the start of a 512-slot ring. Its head starts after them,
        // so FIFO demotion needs more than one bounded scan to reach the oldest page.
        TFixture fixture(10);
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TLogoBlobID id(95, 96, 112);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(3 * bytes));
        TVector<TPageCacheItem> keep;
        for (ui32 offset = 0; offset < 3; ++offset) {
            keep.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, keep.back()));
        }
        for (ui32 offset = 0; offset < 5; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(MakeCollectionCacheItem(525, 526), offset));
        }
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(8 * bytes));
        UNIT_ASSERT(TSharedCacheTestAccess::ShrinkKeepCold(*fixture.Cache));
        const ui64 target = fixture.Cache->KeepColdLimit();
        UNIT_ASSERT(target < 3 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 3 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::ShrinkKeepCold(*fixture.Cache));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdLimit(), target);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 1);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, keep.front().Index()).IsCold());
    }
    Y_UNIT_TEST(OrdinaryColdRemainsCachedDespiteKeepPressureWhileCurrentLimitFits) {
        for (bool pressure : { false, true }) {
            for (bool held : { false, true }) {
                TFixture fixture;
                TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
                const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
                const TLogoBlobID id(95, 96, 109);
                const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
                UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
                    *fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
                UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
                UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(4 * bytes));
                TPageCacheItem ordinary;
                const auto regular = MakeCollectionCacheItem(523, 524);
                for (ui32 offset = 0; offset < 4; ++offset) {
                    const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
                    UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
                    const auto regularPage = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, offset));
                    if (offset == 0) {
                        ordinary = regularPage;
                    }
                }
                InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(regular, 4));
                UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(9 * bytes - 1));
                if (pressure) {
                    UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(3 * bytes));
                    UNIT_ASSERT(fixture.Cache->KeepColdOwnedBytes() > fixture.Cache->KeepColdLimit());
                }
                UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, 1));
                const ui64 entries = fixture.Cache->ColdRingEntries();
                TTestSharedPageRef ref;
                if (held) {
                    UNIT_ASSERT(fixture.Cache->Find(regular, 0, ref) == ESharedCacheResultStatus::Hit);
                }
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, ordinary));
                if (held) {
                    UNIT_ASSERT_VALUES_EQUAL(ref.size(), 4096);
                    UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsCold());
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), entries);
                    ref.Drop();
                }
                UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdRingEntries(), entries + 1);
                UNIT_ASSERT(fixture.Cache->Find(regular, 0, ref) == ESharedCacheResultStatus::Hit);
                TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, 1);
            }
        }
    }
    Y_UNIT_TEST(KeepColdRefillRespondsToHandlePressureWithoutBytePressure) {
        TFixture fixture(6, 8, 8, ui64(1) << 20);
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TLogoBlobID id(95, 96, 110);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const TLogoBlobID stickyId(95, 96, 111);
        const auto stickyCollection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(stickyId));
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, stickyCollection, MakeCollection(stickyId)));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(26 * bytes));
        for (ui32 offset = 0; offset < 54; ++offset) {
            if (offset < 26) {
                const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            } else {
                const auto page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(stickyCollection, offset), 4096,
                    NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
                auto inserted = FindOrInsertPage(*fixture.Cache, TSharedCacheKey::Page(stickyCollection, offset), page);
                UNIT_ASSERT(inserted.Status == ESharedCacheResultStatus::Inserted);
                inserted.Ref.Drop();
                UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
                UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 26 * bytes);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(100 * bytes));
        const ui64 free = TSharedCacheTestAccess::FreeCount(*fixture.Cache);
        ui64 minimumHandles;
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](TSpaceOperation& spaceOp) {
            const ui64 evictableHandles = spaceOp.AllocationLimit() - 2 - fixture.Cache->StickyPages();
            minimumHandles = (evictableHandles + 4) / 5;
        });
        UNIT_ASSERT(free < minimumHandles);
        UNIT_ASSERT(100 * bytes - fixture.Cache->PayloadUsage() > 30 * bytes);
        for (ui32 step = 0; step < 20 && fixture.Cache->ColdReclaimableItems() + free < minimumHandles; ++step) {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT(fixture.Cache->ColdReclaimableItems() + free >= minimumHandles);
        UNIT_ASSERT(fixture.Cache->KeepColdLimit() < 26 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache),
            TSharedCacheTestAccess::MinimumHotSlots(*fixture.Cache));
    }
    Y_UNIT_TEST(CurrentLimitShrinksHotAndReclaimsCold) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(34, 35);
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < 30; ++offset) {
            pages.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages.front()));

        const ui64 usage = fixture.Cache->PayloadUsage();
        const ui64 coldBytes = fixture.Cache->ColdBytes();
        const ui64 effectiveHotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(usage - 1));
        UNIT_ASSERT(coldBytes < (usage - 1) / 5);

        UNIT_ASSERT(fixture.Cache->EnforceCurrentLimit());
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < effectiveHotSlots);
        UNIT_ASSERT(fixture.Cache->PayloadUsage() < usage);
    }
    Y_UNIT_TEST(IncomingPayloadShrinksHotUsingRemainingByteHeadroom) {
        for (bool exceedsFree : { false, true }) {
            TFixture fixture;
            const TCollectionCacheItem collection = MakeCollectionCacheItem(505, 506);
            for (ui64 offset = 0; offset < 8; ++offset) {
                InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            }
            const ui64 limit = fixture.Cache->CurrentLimit();
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
            const ui64 freeBytes = limit - fixture.Cache->PayloadUsage();
            UNIT_ASSERT(freeBytes > limit / 10);
            const ui64 incomingBytes = exceedsFree ? Max<ui64>() : freeBytes - limit / 10;
            const ui64 hotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
            TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
                TSharedCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, incomingBytes, false);
            });
            UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
        }
    }
    Y_UNIT_TEST(IncomingPayloadPreventsHotGrowthWithoutCrossingShrinkWatermark) {
        TFixture fixture;
        const ui64 limit = fixture.Cache->CurrentLimit();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
        const ui64 freeBytes = limit - fixture.Cache->PayloadUsage();
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            TSharedCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, freeBytes, false);
        });
        const ui64 hotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT(hotSlots < fixture.Capacity.HotSlotCount());
        const ui64 incomingBytes = freeBytes - limit / 4;
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            TSharedCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, incomingBytes, false);
        });
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache), hotSlots);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) > hotSlots);
    }
    Y_UNIT_TEST(IncomingPayloadPreventsHotGrowthAbovePolicyLimitWithColdHeadroom) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(509, 510);
        for (ui64 offset = 0; offset < 8; ++offset) {
            TPageCacheItem page;
            TTestSharedPageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset, 16384),
                            EStickyState::None, page, hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index(), 16384)));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }
        const ui64 limit = fixture.Cache->CurrentLimit();
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(limit));
        UNIT_ASSERT(fixture.Cache->ColdReclaimableBytes() > limit * 3 / 10 + 1);
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            UNIT_ASSERT(
                fixture.Cache->ColdReclaimableItems() + spaceOp.View().Free().Count() > spaceOp.View().ColdGrowHandles);
            TSharedCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, 0, true);
        });
        const ui64 hotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT(hotSlots < fixture.Capacity.HotSlotCount());
        const ui64 incomingBytes = limit - fixture.Cache->PayloadUsage() + 1;
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            TSharedCacheTestAccess::EnforceWithOperation(*fixture.Cache, spaceOp, incomingBytes, false);
        });
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache), hotSlots);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) > hotSlots);
    }
    Y_UNIT_TEST(StaleColdEntriesDoNotSuppressHotShrinkForHandlePressure) {
        TFixture fixture;
        // Tiny pages need a matching byte budget so the 60% Hot floor leaves room to shrink.
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(64 * (1 + NActors::TSharedData::OverheadSize)));
        const TCollectionCacheItem collection = MakeCollectionCacheItem(511, 512);
        TVector<TTestSharedPageRef> held;
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < 60; ++offset) {
            TPageCacheItem page;
            TTestSharedPageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset, 1), EStickyState::None,
                            page, hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index(), 1)));
            held.emplace_back();
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, held.back()) == ESharedCacheResultStatus::Hit);
            pages.push_back(page);
        }
        for (ui64 offset = 0; offset < 20; ++offset) {
            TTestSharedPageRef hit;
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, hit) == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[offset]));
            hit.Drop();
            held[offset].Drop();
            UNIT_ASSERT(fixture.Cache->Find(collection, offset, held[offset]) == ESharedCacheResultStatus::Hit);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdReclaimableItems(), 0);
        UNIT_ASSERT(fixture.Cache->ColdRingEntries() >= 20);
        TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
            const ui64 free = spaceOp.View().Free().Count();
            UNIT_ASSERT(free < spaceOp.View().ColdMinHandles);
            UNIT_ASSERT(fixture.Cache->ColdRingEntries() + free > spaceOp.View().ColdGrowHandles);
        });
        const ui64 hotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
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
        TTestSharedCacheCollectionRef collectionHit;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, { .Id = id }, collection,
                        collectionHit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        for (ui64 offset = 1; offset <= 60; ++offset) {
            TPageCacheItem page;
            TTestSharedPageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset, 1),
                            offset <= 20 ? EStickyState::Sticky : EStickyState::None, page,
                            hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index(), 1)));
        }

        const ui64 freeHandles = AvailableFreeHandles(*fixture.Cache);
        UNIT_ASSERT(freeHandles < fixture.Capacity.HandleCount() / 5);
        UNIT_ASSERT(fixture.Cache->PayloadUsage() + fixture.Capacity.Limit / 5 < fixture.Cache->CurrentLimit());
        const ui64 hotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        for (ui32 step = 0; step < 1000 && TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) == hotSlots;
             ++step)
        {
            fixture.Cache->EnforceCurrentLimit();
        }
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < hotSlots);
    }
    void CheckHotRequeueAdmissionWake(bool releaseForeignRef) {
        TFixture fixture(6, 2);
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(601, 602, 1));
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(1));

        TVector<TTestSharedPageRef> held;
        TPageCacheItem cold;
        for (ui64 offset = 0; offset < 2; ++offset) {
            const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
            held.emplace_back();
            UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, page, held.back()));
            if (offset == 0) {
                cold = page;
            }
        }

        UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(fixture.Cache->CurrentLimit()));
        const ui64 l1Slots = TSharedCacheTestAccess::WithSpace(*fixture.Cache, [](auto& spaceOp) {
            return spaceOp.Hot(EHotLevel::L1).Capacity;
        });
        TPageCacheItem hot;
        for (ui64 offset = 2; offset < l1Slots + 2; ++offset) {
            const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
            if (offset == 2) {
                hot = page;
            } else {
                held.emplace_back();
                UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, page, held.back()));
            }
        }
        TTestSharedPageRef foreign;
        UNIT_ASSERT(fixture.Cache->Find(collection, 2, foreign) == ESharedCacheResultStatus::Hit);
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

        observation.Gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeHotExchange, hot.CacheItem());
        TTestTraits hooks{ &observation, [](void* context, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                              auto& observation = *static_cast<TObservation*>(context);
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  observation.Notifications.fetch_add(1, std::memory_order_relaxed);
                              }
                              TSharedCacheGate::Invoke(&observation.Gate, point, item);
                          } };
        TSharedCacheHookGuard guard(*fixture.Cache, hooks);
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        fixture.Cache->ArmResourceWait(fixture.Cache->AdmissionGeneration());
        TTestSharedPageRef found;
        TGateThread lookup(observation.Gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            Y_ABORT_UNLESS(fixture.Cache->Find(collection, 0, found) == ESharedCacheResultStatus::Hit);
        });
        observation.Gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), releaseForeignRef ? 0 : 1);
        foreign.Drop();
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, hot.Index()).Refs(), 0);
        const auto hotIsQueued = [&] {
            return TSharedCacheTestAccess::WithSpace(*fixture.Cache, [&](auto& spaceOp) {
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
        UNIT_ASSERT(!TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), 1);

        observation.Gate.Slots[0].Release();
        lookup.Join();
        UNIT_ASSERT(found && found.CacheItem() == cold);
        UNIT_ASSERT(hotIsQueued());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications.load(), 2);
        UNIT_ASSERT(!fixture.Cache->CanReserve(bytes));
        UNIT_ASSERT(!fixture.Cache->EnforceCurrentLimit());
        UNIT_ASSERT(!fixture.Cache->RunMaintenance());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);

        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
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
        TSharedCacheTestAccess::ShrinkHotToMinimum(*fixture.Cache);
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(603, 604), 1);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        TTestSharedPageRef held;
        UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, page, held));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui64 bytes = held.size() + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(bytes));
        UNIT_ASSERT(!TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));

        ui32 notifications = 0;
        TTestTraits hooks{ &notifications, [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
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
            UNIT_ASSERT(!TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
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
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
        }

        TPageCacheItem cacheItem;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(6), EStickyState::None, cacheItem,
                        hit) == ESharedCacheResultStatus::Inserted);

        TSharedCacheGate gate;
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeHotExchange, cacheItem.CacheItem());

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

        constexpr ui32 TargetHotSlots = 15;
        constexpr ui64 LateSlot = 15;
        UNIT_ASSERT(TSharedCacheTestAccess::BeginHotResize(*fixture.Cache, TargetHotSlots));
        UNIT_ASSERT(TSharedCacheTestAccess::DrainHotResize(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::HotResizePhase(*fixture.Cache) == EHotResizePhase::Idle);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HotSlot(*fixture.Cache, LateSlot), 0);

        gate.Slots[0].Release();
        readyThread.Join();

        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HotSlot(*fixture.Cache, LateSlot), 0);
        const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, cacheItem.Index());
        UNIT_ASSERT(state.IsCold());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
    }
    Y_UNIT_TEST(CurrentLimitExposesReferencedOverage) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(32, 33);
        TPageCacheItem insertedItem;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::None, insertedItem,
                        hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(insertedItem, MakePageData(insertedItem.Index())));
        TTestSharedPageRef page;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, page) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page.CacheItem()));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(1));

        for (ui32 step = 0; step < 1000 && fixture.Cache->EnforceCurrentLimit(); ++step) {
        }
        UNIT_ASSERT(fixture.Cache->PayloadUsage() > fixture.Cache->CurrentLimit());
        const THandleState referenced = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.CacheItem().Index());
        UNIT_ASSERT(referenced.IsCold());
        UNIT_ASSERT_VALUES_EQUAL(referenced.Refs(), 1);

        page.Drop();
        for (ui32 step = 0; step < 1000 && fixture.Cache->PayloadUsage() > fixture.Cache->CurrentLimit(); ++step) {
            UNIT_ASSERT_C(fixture.Cache->EnforceCurrentLimit(), TStringBuilder() << "policy stalled at step " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), 0);
    }
    Y_UNIT_TEST(GrowthPinsSplitPredecessorUntilLinkCas) {
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.ReservedCapacity.Limit));
        const auto keys = MakeResizeKeys(*fixture.Space, MakeCollectionCacheItem(8, 15), 800);
        const TPageCacheItem high = InsertReadyPage(*fixture.Cache, keys[2]);
        TVector<TTestSharedPageRef> held;
        {
            auto found = FindPage(*fixture.Cache, keys[2]);
            UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
            held.push_back(std::move(found.Ref));
        }
        const TCollectionCacheItem fillerCollection = MakeCollectionCacheItem(81, 82);
        for (ui64 offset = 0; held.size() < fixture.Capacity.HandleCount() - 3; ++offset) {
            const TSharedCacheKey key = TSharedCacheKey::Page(fillerCollection, offset);
            if ((key.Hash() & (fixture.Capacity.BucketCount() - 1)) == 0) {
                continue;
            }
            InsertReadyPage(*fixture.Cache, key);
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
            held.push_back(std::move(found.Ref));
        }
        const TPageCacheItem low = InsertReadyPage(*fixture.Cache, keys[0]);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, low));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, low.Index()).IsCold());

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterGrowthBucketPublished);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool firstActivation = true;
        TGateThread firstActivator(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            firstActivation = TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, low.Index())));
        const THandleState pinned = TSharedCacheTestAccess::HandleState(*fixture.Cache, low.Index());
        UNIT_ASSERT(pinned.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(pinned.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::WithSpace(*fixture.Cache, [](TSpaceOperation& spaceOp) {
            return spaceOp.TryAllocateHandle();
        }), 0);
        gate.Slots[0].Release();
        firstActivator.Join();
        UNIT_ASSERT(!firstActivation);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, low.Index()).IsFree());

        const TPageCacheItem reused = AllocatePage(*fixture.Cache, keys[1], 4096);
        UNIT_ASSERT(reused);
        UNIT_ASSERT_VALUES_EQUAL(reused.Index(), low.Index());
        UNIT_ASSERT_VALUES_UNEQUAL(reused.Version(), low.Version());
        auto inserted = FindOrInsertPage(*fixture.Cache, keys[1], reused);
        UNIT_ASSERT(inserted.Status == ESharedCacheResultStatus::Inserted);
        inserted.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->MakeReady(reused, MakePageData(reused.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        AssertPageHit(*fixture.Cache, keys[1], reused);
        AssertPageHit(*fixture.Cache, keys[2], high);
        UNIT_ASSERT(FindPage(*fixture.Cache, keys[0]).Status == ESharedCacheResultStatus::Miss);
        CommitTransition(*fixture.Cache, growth);
    }

    void ReviewMarkerRemovalPredecessor(bool grow, bool reuseBeforeClaim) {
        TSharedCacheCapacity reserved;
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
        if (TSharedCacheTestAccess::HandleState(*fixture.Cache, predecessor.Index()).IsHot()) {
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, predecessor));
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);

        TTransition growth;
        if (grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, grow ? 7 : 5));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        if (!grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        }

        TSharedCacheGate gate;
        gate.Slots[0].Arm(reuseBeforeClaim ? ESharedCacheHookPoint::BeforeBucketSplitLinkClaim
                                           : ESharedCacheHookPoint::BeforeTableLinkCas, predecessor.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool removed = true;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TSharedCacheTestAccess::RemoveBucketSplit(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(
            TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, predecessor.Index())));
        TPageCacheItem replacement;
        if (reuseBeforeClaim) {
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, predecessor.Index()).IsFree());
            replacement = InsertReadyPage(*fixture.Cache, replacementKey);
        } else {
            const auto pinned = TSharedCacheTestAccess::HandleState(*fixture.Cache, predecessor.Index());
            UNIT_ASSERT(pinned.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(pinned.Refs(), 1);
            UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        }
        gate.Slots[0].Release();
        remover.Join();
        UNIT_ASSERT(!removed);
        if (!reuseBeforeClaim) {
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, predecessor.Index()).IsFree());
            replacement = InsertReadyPage(*fixture.Cache, replacementKey);
        }
        UNIT_ASSERT_VALUES_EQUAL(replacement.Index(), predecessor.Index());
        UNIT_ASSERT_VALUES_UNEQUAL(replacement.Version(), predecessor.Version());
        UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
        AssertReviewTerminal(*fixture.Cache, replacement.CacheItem(), "R17");
        AssertPageHit(*fixture.Cache, replacementKey, replacement);
        const auto missing = ReviewKeyForBucket(collection, splitBit - 1, 0, 300000, &replacementKey);
        UNIT_ASSERT(FindPage(*fixture.Cache, missing).Status == ESharedCacheResultStatus::Miss);
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

    Y_UNIT_TEST(ReviewR17GrowthRemovalRetiresMarkerBeforeRoutingPredecessor) {
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(reserved.Limit));
        const auto keys = MakeResizeKeys(*fixture.Space, MakeCollectionCacheItem(118, 50018), 400000);
        const auto low = InsertReadyPage(*fixture.Cache, keys[0]);
        const auto high = InsertReadyPage(*fixture.Cache, keys[2]);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, low));

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeColdExchange, MakeColdItem(*fixture.Cache, low.Index()));
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool removed = false;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TSharedCacheTestAccess::RemoveBucketSplit(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex).IsFree());
        const ui32 highBucket = TSharedCacheTestAccess::BucketResizeState(*fixture.Space).SplitBit();
        UNIT_ASSERT(TSharedCacheTestAccess::BucketHead(*fixture.Cache, highBucket) == high.CacheItem());
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
        TTestSharedPageRef held;
        UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, first, held));
        auto candidates = AllocateAvailableHandleCandidates(*fixture.Cache);
        Y_DEFER {
            for (const auto item : candidates) {
                TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, item);
            }
        };
        UNIT_ASSERT(!candidates.empty());
        TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, candidates.back());
        candidates.pop_back();
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 1);
        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        if (remove) {
            UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
            UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        }
        const auto before = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
        TSharedCacheGate gate;
        gate.Slots[0].Arm(
            remove ? ESharedCacheHookPoint::BeforeTableLinkCas : ESharedCacheHookPoint::AfterGrowthBucketPublished,
            remove ? first.CacheItem() : TCacheItem{});
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        bool moreWork = false;
        TGateThread resizer(gate, [&] {
            Y_ABORT_UNLESS(!TlsCacheHazard);
            TlsCacheHazard = &TCacheThreadTestAccess::Hazard(*fixture.Cache, 0);
            Y_DEFER {
                TlsCacheHazard = nullptr;
            };
            moreWork = TSharedCacheTestAccess::AdvanceBucketResize(*fixture.Cache);
        });
        gate.Slots[0].Wait();
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        TPageCacheItem inserted;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(nextKey.Word(1)), EStickyState::None,
                        inserted, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, inserted.Index()).IsPending());
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        gate.Slots[0].Release();
        resizer.Join();
        UNIT_ASSERT(moreWork);
        const auto after = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
        UNIT_ASSERT(after.Phase() == before.Phase());
        UNIT_ASSERT_VALUES_EQUAL(after.Cursor(), before.Cursor());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        UNIT_ASSERT(!TSharedCacheTestAccess::AdvanceBucketResize(*fixture.Cache));
    }

    void CheckUnlinkNotification(bool chain, bool armBeforeUnlink) {
        TFixture fixture(6, 8, 7);
        Y_DEFER {
            fixture.Cache->DisarmResourceWait();
        };
        const auto collection = MakeCollectionCacheItem(257, 50057);
        const ui32 splitBit = static_cast<ui32>(CurrentBucketCount(*fixture.Space));
        const ui32 mask = (splitBit << 1) - 1;
        TVector<TSharedCacheKey> keys{ ReviewKeyForBucket(collection, mask, 0, 800000) };
        if (chain) {
            keys.push_back(ReviewKeyForBucket(collection, mask, 0, keys.front().Word(1) + 1, &keys.front()));
        }
        TVector<TPageCacheItem> pages;
        TVector<TTestSharedPageRef> held;
        for (const auto& key : keys) {
            const auto page = InsertReadyPage(*fixture.Cache, key);
            pages.push_back(page);
            held.emplace_back();
            UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, page, held.back()));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }
        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));

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
        observation.FreeCount = TSharedCacheTestAccess::FreeCount(*fixture.Cache);
        {
            auto cacheOp = fixture.Cache->BeginOperation();
            TOperationItemRef<TTestTraits> firstOwner(cacheOp);
            TOperationItemRef<TTestTraits> secondOwner(cacheOp);
            auto expected = TSharedCacheTestAccess::HandleState(*fixture.Cache, pages.front().Index());
            UNIT_ASSERT(cacheOp.TryBeginErase(pages.front().CacheItem(), expected, firstOwner.Get()));
            if (chain) {
                expected = TSharedCacheTestAccess::HandleState(*fixture.Cache, pages.back().Index());
                UNIT_ASSERT(cacheOp.TryBeginErase(pages.back().CacheItem(), expected, secondOwner.Get()));
            }
            UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
            TTestTraits hooks{ &observation,
                [](void* context, ESharedCacheHookPoint point, TCacheItem) noexcept {
                    if (point != ESharedCacheHookPoint::ResourcesAvailable) {
                        return;
                    }
                    auto& observation = *static_cast<TObservation*>(context);
                    ++observation.Notifications;
                    observation.Published =
                        observation.Cache->PayloadUsage() == observation.Bytes &&
                        TSharedCacheTestAccess::FreeCount(*observation.Cache) == observation.FreeCount;
                    for (const auto page : observation.Pages) {
                        const auto state = TSharedCacheTestAccess::HandleState(*observation.Cache, page.Index());
                        observation.Published &=
                            state.IsTombstone() && state.Refs() == 2 && page.CacheItem().Matches(state);
                    }
                    observation.Activated = TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*observation.Cache);
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
                UNIT_ASSERT_VALUES_EQUAL(
                    TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[index].Index()).Refs(), 1);
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
        TTestSharedPageRef held;
        auto waiter = MakeIntrusive<TAcquiringPageFetchWaiter>(*fixture.Cache, held);
        TTestSharedPageRef hit;
        TTestPageFetch fetch;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(key.Word(1)),
                        EStickyState::None, waiter, hit, fetch) == ESharedCacheResultStatus::Inserted);
        const auto page = fetch.CacheItem();
        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));

        struct TObservation {
            TSharedCacheGate Gate;
            TTestSharedCache* Cache = nullptr;
            TTestSharedPageRef* Held = nullptr;
            TPageCacheItem Page;
            ui32 Notifications = 0;
            bool Published = false;
            bool Activated = false;
        } observation;

        observation.Cache = fixture.Cache.Get();
        observation.Held = &held;
        observation.Page = page;
        observation.Gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeReadyStatePublication, page.CacheItem());
        TTestTraits hooks{ &observation, [](void* context, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                              auto& observation = *static_cast<TObservation*>(context);
                              TSharedCacheGate::Invoke(&observation.Gate, point, item);
                              if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                                  ++observation.Notifications;
                                  const auto state =
                                      TSharedCacheTestAccess::HandleState(*observation.Cache, observation.Page.Index());
                                  observation.Published = state.IsReady() && state.Refs() > 0 && *observation.Held &&
                                                          observation.Held->CacheItem() == observation.Page;
                                  observation.Activated =
                                      TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*observation.Cache);
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
        const auto blocked = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(blocked.IsCompleting());
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforeReady) {
            fixture.Cache->ArmResourceWait(generation);
        }
        observation.Gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(ready && waiter->Ready() && held);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforeReady ? 1 : 0);
        if (!armBeforeReady) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published && observation.Activated);
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
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
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(253, 254, 255));
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        auto metadata = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, collection.CacheItem());
        UNIT_ASSERT(metadata);
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        TTestSharedPageRef held;
        UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, page, held));
        const auto ordinary = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
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
        observation.Gate.Slots[0].Arm(restore ? ESharedCacheHookPoint::BeforeStickyPageListLink
                                              : ESharedCacheHookPoint::AfterStickyPageLinked, page.CacheItem());
        TTestTraits hooks{ &observation,
            [](void* context, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                auto& observation = *static_cast<TObservation*>(context);
                TSharedCacheGate::Invoke(&observation.Gate, point, item);
                if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                    ++observation.Notifications;
                    const auto state =
                        TSharedCacheTestAccess::HandleState(*observation.Cache, observation.Page.Index());
                    observation.Published =
                        state.Refs() > 0 &&
                        TSharedCacheTestAccess::HandleState(*observation.Cache, observation.Collection.Index()).Refs() >
                            0;
                    if (observation.Restore) {
                        observation.Published &=
                            state.IsHot() && state.IsStickyNoneField() && observation.Cache->StickyOwnedBytes() == 0 &&
                            TSharedCacheTestAccess::NextInOwner(*observation.Cache, observation.Page.Index()) == 0;
                    } else {
                        observation.Published &= state.IsSticky() && state.IsStickyField() &&
                                                 TSharedCacheTestAccess::StickyPageListHead(*observation.Cache,
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
            promoted = TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page);
        });
        observation.Gate.Slots[0].Wait();
        if (restore) {
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        }
        const auto blocked = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(blocked.IsHot() && blocked.IsStickyField());
        UNIT_ASSERT(!TSharedCacheTestAccess::RelocatePage(*fixture.Cache, page));
        UNIT_ASSERT(!TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        if (armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        observation.Gate.Slots[0].Release();
        promoter.Join();
        UNIT_ASSERT_VALUES_EQUAL(promoted, !restore);
        UNIT_ASSERT(held && metadata);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, armBeforePublication ? 1 : 0);
        if (!armBeforePublication) {
            fixture.Cache->ArmResourceWait(generation);
        }
        UNIT_ASSERT_VALUES_EQUAL(observation.Notifications, 1);
        UNIT_ASSERT(observation.Published);
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        if (!restore) {
            UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*fixture.Cache, page, 0));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::RelocatePage(*fixture.Cache, page));
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
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(253, 254, 256));
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        auto metadata = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, collection.CacheItem());
        UNIT_ASSERT(metadata);
        const auto key = TSharedCacheKey::Page(collection, 1);
        const auto page = pending ? AllocatePage(*fixture.Cache, key, 4096) : InsertReadyPage(*fixture.Cache, key);
        TTestSharedPageRef held;
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));
        if (!pending) {
            UNIT_ASSERT(TSharedCacheTestAccess::AcquirePage(*fixture.Cache, page, held));
            UNIT_ASSERT(TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        }
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            UNIT_ASSERT(pending ? TSharedCacheTestAccess::MakePendingPageSticky(*fixture.Cache, page)
                                : TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page));
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
        TTestSharedCacheItemRef firstHelper;
        TTestSharedCacheItemRef secondHelper;

        struct TObservation {
            TTestSharedCache* Cache = nullptr;
            TTestSharedCacheItemRef* LateHelper = nullptr;
            TCacheItem Marker;
            ui32 Notifications = 0;
            bool FinalRelease = false;
            bool CaptureLateHelper = false;
            bool Published = true;
            bool Reused = false;
        } observation;

        Y_DEFER {
            TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
            fixture.Cache->DisarmResourceWait();
        };
        TTransition growth;
        if (grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, grow ? 7 : 5));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        if (!grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        }
        const auto markerState = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        observation.Cache = fixture.Cache.Get();
        observation.LateHelper = &firstHelper;
        observation.Marker = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);
        observation.FinalRelease = finalRelease;
        observation.CaptureLateHelper = finalRelease;
        TTestTraits hooks{ &observation,
            [](void* context, ESharedCacheHookPoint point, TCacheItem item) noexcept {
                auto& observation = *static_cast<TObservation*>(context);
                if (point == ESharedCacheHookPoint::AfterBucketSplitOwnerCheck && observation.CaptureLateHelper) {
                    observation.CaptureLateHelper = false;
                    *observation.LateHelper = TSharedCacheTestAccess::AcquireTombstoneHelper(*observation.Cache, item);
                } else if (point == ESharedCacheHookPoint::ResourcesAvailable) {
                    ++observation.Notifications;
                    const auto state = TSharedCacheTestAccess::HandleState(*observation.Cache, ResizeMarkerIndex);
                    if (observation.FinalRelease) {
                        observation.Published &= state.IsFree() && state.Refs() == 0 &&
                                                 state.Version() == AdvanceItemVersion(observation.Marker.Version());
                        observation.Reused = TSharedCacheTestAccess::PrepareBucketSplit(*observation.Cache);
                    } else {
                        observation.Published &=
                            state.IsTombstone() && state.Refs() == 1 && observation.Marker.Matches(state);
                    }
                }
            } };
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, &hooks);
        if (finalRelease) {
            UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
            UNIT_ASSERT(firstHelper);
            AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
            UNIT_ASSERT(
                TSharedCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
        } else {
            firstHelper = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, observation.Marker);
            secondHelper = TSharedCacheTestAccess::AcquireStructural(*fixture.Cache, observation.Marker);
            UNIT_ASSERT(firstHelper && secondHelper);
            UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplitStep(*fixture.Cache) == EResizeStep::Blocked);
            const auto blocked = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
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
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex).Refs(), 2);
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
            const auto reused = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
            UNIT_ASSERT(reused.IsBucketSplit());
            UNIT_ASSERT_VALUES_EQUAL(reused.Refs(), 1);
            UNIT_ASSERT_VALUES_EQUAL(reused.Version(), AdvanceItemVersion(observation.Marker.Version()));
        }
        TSharedCacheTestAccess::InstallHooks(*fixture.Cache, nullptr);
        fixture.Cache->DisarmResourceWait();
        if (!finalRelease) {
            UNIT_ASSERT(TSharedCacheTestAccess::RemoveBucketSplit(*fixture.Cache));
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
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 5));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        const auto state = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        auto helper = TSharedCacheTestAccess::AcquireStructural(
            *fixture.Cache, TCacheItem::Make(state.Version(), ResizeMarkerIndex));
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
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(241, 242), 100);
        const auto page = InsertReadyPage(*fixture.Cache, key);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        auto cacheOp = fixture.Cache->BeginOperation();
        TOperationItemRef<TTestTraits> held(cacheOp, cacheOp.TryAcquireStructural(page.CacheItem()));
        UNIT_ASSERT(held);
        TOperationItemRef<TTestTraits> owner(cacheOp);
        auto expected = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(cacheOp.TryBeginErase(page.CacheItem(), expected, owner.Get()));
        cacheOp.FinishErase(owner.CacheItem(), key);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 2);
        const ui64 generation = fixture.Cache->AdmissionGeneration();
        fixture.Cache->ArmResourceWait(generation);
        owner.Drop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->AdmissionGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).Refs(), 1);
        fixture.Cache->DisarmResourceWait();
        held.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
    }

    void ReviewMarkerRetirementWaitsForHelper(bool grow, bool helperFirst) {
        TSharedCacheCapacity reserved;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(7, 4096, 0, 8, reserved));
        TFixture fixture(6, 8, 7, reserved.Limit);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(reserved.Limit));
        const ui32 splitBit = static_cast<ui32>(SharedCacheBucketCount(grow ? 6 : 5));
        const auto key = ReviewKeyForBucket(MakeCollectionCacheItem(119, 50019), (splitBit << 1) - 1, splitBit, 500000);
        const auto survivingKey = ReviewKeyForBucket(PageCollection(key), (splitBit << 1) - 1, splitBit, 600000, &key);
        const auto successor = InsertReadyPage(*fixture.Cache, key);
        const auto survivor = InsertReadyPage(*fixture.Cache, survivingKey);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, successor));

        TTransition growth;
        if (grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, grow ? 7 : 5));
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishBucketSplit(*fixture.Cache));
        if (!grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::CloseBucketSplit(*fixture.Cache));
        }
        const auto markerState = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        const auto marker = TCacheItem::Make(markerState.Version(), ResizeMarkerIndex);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterBucketSplitOwnerCheck, marker);
        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterTombstoneHelperClaim, marker);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool removed = false;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TSharedCacheTestAccess::RemoveBucketSplit(*cache);
        });
        gate.Slots[0].Wait();
        bool erased = false;
        TGateThread eraser(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            erased = TSharedCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, successor.Index()));
        });
        gate.Slots[1].Wait();
        const auto heldMarker = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        UNIT_ASSERT(heldMarker.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(heldMarker.Refs(), 2);
        if (helperFirst) {
            gate.Slots[1].Release();
            eraser.Join();
            UNIT_ASSERT(erased);
            AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, successor.Index()).IsFree());
            gate.Slots[0].Release();
            remover.Join();
            UNIT_ASSERT(removed);
        } else {
            gate.Slots[0].Release();
            remover.Join();
            UNIT_ASSERT(removed);
            AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
            UNIT_ASSERT(
                TSharedCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::Prepared);
            UNIT_ASSERT(!TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
            AssertResizeMarker(*fixture.Cache, EHandleState::Tombstone);
            gate.Slots[1].Release();
            eraser.Join();
            UNIT_ASSERT(erased);
        }
        if (grow) {
            UNIT_ASSERT(TSharedCacheTestAccess::BucketHead(*fixture.Cache, 0).IsNull());
            UNIT_ASSERT(TSharedCacheTestAccess::BucketHead(*fixture.Cache, splitBit) == survivor.CacheItem());
        } else {
            UNIT_ASSERT(TSharedCacheTestAccess::BucketHead(*fixture.Cache, 0) == survivor.CacheItem());
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex).IsFree());
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareBucketSplit(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplitOrMerge(*fixture.Cache));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ESharedCacheResultStatus::Miss);
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterGrowthBucketPublished);
        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterBucketSplitActivated);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;

        bool firstActivation = true;
        TGateThread firstActivator(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            firstActivation = TSharedCacheTestAccess::AdvanceBucketResize(*cache);
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
        UNIT_ASSERT(TSharedCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::Prepared);

        bool activated = false;
        TGateThread activator(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            activated = TSharedCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[1].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::Prepared);
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertPageHit(*fixture.Cache, keys[2], high);
        gate.Slots[1].Release();
        activator.Join();
        UNIT_ASSERT(activated);

        const ui32 splitBit = TSharedCacheTestAccess::BucketResizeState(*fixture.Space).SplitBit();
        TMaybe<TSharedCacheKey> futureKey;
        for (ui64 offset = 30000; !futureKey && offset < 30000 + (1 << 20); ++offset) {
            const TSharedCacheKey key = TSharedCacheKey::Page(PageCollection(keys[0]), offset);
            const ui32 hash = key.Hash();
            if ((hash & (splitBit - 1)) == 1 && (hash & splitBit) != 0) {
                futureKey = key;
            }
        }
        UNIT_ASSERT(futureKey);
        const TPageCacheItem future = InsertReadyPage(*fixture.Cache, *futureKey);
        UNIT_ASSERT(TSharedCacheTestAccess::BucketHead(*fixture.Cache, 1 | splitBit).IsNull());
        AssertPageHit(*fixture.Cache, *futureKey, future);

        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterBucketResizeCursorPublished);
        bool published = false;
        TGateThread publisher(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            published = TSharedCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(
            TSharedCacheTestAccess::BucketResizeState(*fixture.Space).Phase() == EBucketResizePhase::CursorPublished);
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        AssertPageHit(*fixture.Cache, keys[1], inserted);
        AssertPageHit(*fixture.Cache, keys[2], high);
        AssertPageHit(*fixture.Cache, *futureKey, future);
        gate.Slots[0].Release();
        publisher.Join();
        UNIT_ASSERT(published);

        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterBucketSplitRemoved);
        bool removed = false;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TSharedCacheTestAccess::AdvanceBucketResize(*cache);
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

        const THandleState markerState = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        UNIT_ASSERT(markerState.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(markerState.Refs(), 0);
        AssertPageUnreferenced(*fixture.Cache, low);
        AssertPageUnreferenced(*fixture.Cache, inserted);
        AssertPageUnreferenced(*fixture.Cache, high);
        AssertPageUnreferenced(*fixture.Cache, future);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 4 * pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 4 * pageBytes));

        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        UNIT_ASSERT(!TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing());
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);

        TTransition shrink;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink) ==
                    EPrepareResult::Prepared);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        UNIT_ASSERT(TSharedCacheTestAccess::AdvanceBucketResize(*fixture.Cache));
        UNIT_ASSERT(TSharedCacheTestAccess::AdvanceBucketResize(*fixture.Cache));

        const TBucketResizeState resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
        const ui32 highBucket = resize.SplitBit();
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterShrinkBucketClosed);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;

        bool closed = false;
        TGateThread closer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            closed = TSharedCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::BucketHead(*fixture.Cache, highBucket).IsClosedBucketHead());
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[2], high);

        const TPageCacheItem inserted = InsertReadyPage(*fixture.Cache, keys[1]);
        TPageCacheItem duplicate;
        TTestSharedPageRef existing;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(keys[1]), MakePageLocation(keys[1].Word(1)),
                        EStickyState::None, duplicate, existing) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(existing.CacheItem() == inserted);
        existing.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::BucketHead(*fixture.Cache, highBucket).IsClosedBucketHead());
        gate.Slots[0].Release();
        closer.Join();
        UNIT_ASSERT(closed);

        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterBucketSplitRemoved);
        bool removed = false;
        TGateThread remover(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            removed = TSharedCacheTestAccess::AdvanceBucketResize(*cache);
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
        UNIT_ASSERT(!TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing());
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
        TVector<TSharedCacheKey> stableKeys;
        TVector<TPageCacheItem> stablePages;
        stableKeys.reserve(StableKeyCount);
        stablePages.reserve(StableKeyCount);
        for (ui32 index = 0; index < StableKeyCount; ++index) {
            stableKeys.push_back(TSharedCacheKey::Page(collection, 10000 + index));
            stablePages.push_back(InsertReadyPage(*fixture.Cache, stableKeys.back()));
        }

        TVector<TSharedCacheKey> writerKeys;
        writerKeys.reserve(WriterCount * WriterKeyCount);
        for (ui32 index = 0; index < WriterCount * WriterKeyCount; ++index) {
            writerKeys.push_back(TSharedCacheKey::Page(collection, 20000 + index));
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
                    TTestSharedPageRef ref;
                    const ESharedCacheResultStatus status =
                        cache->Find(PageCollection(stableKeys[index]), stableKeys[index].Word(1), ref);
                    if (status != ESharedCacheResultStatus::Hit || !ref || ref.CacheItem() != stablePages[index]) {
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
                    const TSharedCacheKey& key = writerKeys[index];
                    TPageCacheItem page;
                    TTestSharedPageRef hit;
                    const ESharedCacheResultStatus status = FindOrInsertPage(
                        *cache, PageCollection(key), MakePageLocation(key.Word(1)), EStickyState::None, page, hit);
                    if (status != ESharedCacheResultStatus::Inserted || !page) {
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
                    const TSharedCacheKey& key = writerKeys[index];
                    TPageCacheItem duplicate;
                    TTestSharedPageRef hit;
                    const ESharedCacheResultStatus status = FindOrInsertPage(
                        *cache, PageCollection(key), MakePageLocation(key.Word(1)), EStickyState::None, duplicate, hit);
                    if (status != ESharedCacheResultStatus::Hit || !hit || hit.CacheItem() != writerPages[index])
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
            if (!TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, targetAddressBits)) {
                ownerFailed = true;
                ownerError = TStringBuilder()
                             << "cannot begin cycle " << cycle << " target " << static_cast<ui32>(targetAddressBits);
                break;
            }
            pause.store(false, std::memory_order_release);
            constexpr ui32 MaxResizeSteps = 1 << 16;
            ui32 step = 0;
            while (TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing() && step++ < MaxResizeSteps) {
                if (!TSharedCacheTestAccess::AdvanceBucketResize(*fixture.Cache)) {
                    std::this_thread::yield();
                }
            }
            if (TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing() ||
                CurrentBucketCount(*fixture.Space) != SharedCacheBucketCount(targetAddressBits))
            {
                ownerFailed = true;
                if (TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing()) {
                    const TBucketResizeState resize = TSharedCacheTestAccess::BucketResizeState(*fixture.Space);
                    const THandleState markerState =
                        TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
                    const ui64 markerNext = TSharedCacheTestAccess::HandleNext(*fixture.Cache, ResizeMarkerIndex);
                    const ui32 bucket = resize.Cursor();
                    const ui32 peer = bucket | resize.SplitBit();
                    ownerError = TStringBuilder()
                                 << "step bound at cycle " << cycle << " direction "
                                 << static_cast<i32>(resize.Direction()) << " phase "
                                 << static_cast<ui32>(resize.Phase()) << " cursor " << resize.Cursor()
                                 << " marker-state " << static_cast<ui32>(markerState.State()) << " marker-refs "
                                 << markerState.Refs() << " marker-next " << markerNext << " bucket-head "
                                 << TSharedCacheTestAccess::BucketHead(*fixture.Cache, bucket).Raw() << " peer-head "
                                 << TSharedCacheTestAccess::BucketHead(*fixture.Cache, peer).Raw();
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

        for (ui32 step = 0; TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing() && step < (1 << 16);
             ++step) {
            TSharedCacheTestAccess::AdvanceBucketResize(*fixture.Cache);
        }
        UNIT_ASSERT_C(!ownerFailed, ownerError);
        UNIT_ASSERT_C(
            !failed.load(std::memory_order_acquire), TStringBuilder() << "failure code " << failureCode.load());
        UNIT_ASSERT(!TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).Resizing());
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
        for (ui32 index = 2; index < TSharedCacheTestAccess::HandleCount(*fixture.Cache); ++index) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, index);
            if (state.IsHot()) {
                ++hotCount;
                UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(hotCount, itemCount);
        UNIT_ASSERT_VALUES_EQUAL(
            AvailableFreeHandles(*fixture.Cache), TSharedCacheTestAccess::HandleCount(*fixture.Cache) - 2 - itemCount);

        const ui64 expectedBytes = itemCount * (4096 + NActors::TSharedData::OverheadSize);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), expectedBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), expectedBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, expectedBytes));

        const THandleState markerState = TSharedCacheTestAccess::HandleState(*fixture.Cache, ResizeMarkerIndex);
        UNIT_ASSERT(markerState.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(markerState.Refs(), 0);
        for (ui32 bucket = 0; bucket < CurrentBucketCount(*fixture.Space); ++bucket) {
            UNIT_ASSERT(!TSharedCacheTestAccess::BucketHead(*fixture.Cache, bucket).IsClosedBucketHead());
        }
    }
    Y_UNIT_TEST(DiscardCandidateDeletesTypedPayload) {
        TFixture fixture;
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;

        const auto pageKey = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 101);
        const auto page =
            AllocatePage(*fixture.Cache, pageKey, 4096, NTable::NPage::EPage::DataPage, 123, EStickyState::None);
        UNIT_ASSERT(page);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::PageType(*fixture.Cache, page.Index()), NTable::NPage::EPage::DataPage);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::PageCrc32(*fixture.Cache, page.Index()), 123);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
        UNIT_ASSERT(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index(), std::memory_order_relaxed).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 0));

        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(4, 5, 6));
        constexpr size_t BackingSize = 317;
        const ui64 collectionBytes = 0;
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey, BackingSize);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReadyUnregistered(
            *fixture.Cache, collection, MakeCollection(CollectionId(collectionKey), BackingSize)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), collectionBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), collectionBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), collectionBytes);

        TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, collection);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index()), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 0));
    }
    Y_UNIT_TEST(StickyPageListContainsOnlyStickyPages) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(7, 8, 9));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto regularKey = TSharedCacheKey::Page(collection, 1);
        const TPageCacheItem regular = AllocatePage(*fixture.Cache, regularKey, 4096);
        UNIT_ASSERT(regular);
        UNIT_ASSERT(fixture.Cache->MakeReady(regular, MakePageData(regular.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, regular.Index()), 0);

        const auto firstKey = TSharedCacheKey::Page(collection, 2);
        const TPageCacheItem first =
            AllocatePage(*fixture.Cache, firstKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(first);
        UNIT_ASSERT(fixture.Cache->MakeReady(first, MakePageData(first.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);

        const auto secondKey = TSharedCacheKey::Page(collection, 3);
        const TPageCacheItem second =
            AllocatePage(*fixture.Cache, secondKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(second);
        UNIT_ASSERT(fixture.Cache->MakeReady(second, MakePageData(second.Index())));
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), second.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), first.Index());

        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*fixture.Cache, second, second.Index()));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);
        const THandleState secondState = TSharedCacheTestAccess::HandleState(*fixture.Cache, second.Index());
        UNIT_ASSERT(secondState.IsSticky());
        UNIT_ASSERT(secondState.IsTransitionField());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), 0);

        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*fixture.Cache, first, first.Index()));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        const THandleState firstState = TSharedCacheTestAccess::HandleState(*fixture.Cache, first.Index());
        UNIT_ASSERT(firstState.IsSticky());
        UNIT_ASSERT(firstState.IsTransitionField());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);
    }
    Y_UNIT_TEST(DetachedCollectionRejectsStickyReplacementWithoutLeakingOwner) {
        TFixture fixture;
        const TCollectionLocation collectionLocation{ .Id = TLogoBlobID(7, 8, 10) };
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, fixture.Registry, collectionLocation,
                        collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(collectionLocation.Id)));

        TPageCacheItem page;
        TTestSharedPageRef pageHit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(1), EStickyState::Sticky, page,
                        pageHit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*fixture.Cache, page, page.Index()));
        const THandleState sourceState = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(sourceState.IsSticky() && sourceState.IsTransitionField());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(TSharedCacheTestAccess::DetachCollection(*fixture.Cache, fixture.Registry, collection));
        collectionRef.Drop();
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        const ui64 residentBytes = fixture.Cache->ResidentBytes();
        UNIT_ASSERT(!TSharedCacheTestAccess::RelocatePage(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
    }
    Y_UNIT_TEST(ReadyPageCanBecomeStickyAndDetach) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(16, 17, 18));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto key = TSharedCacheKey::Page(collection, 1);
        const TPageCacheItem page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);

        UNIT_ASSERT(TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), page.Index());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsSticky());
        UNIT_ASSERT(TSharedCacheTestAccess::MakePageSticky(*fixture.Cache, page));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pageBytes);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
    }
    Y_UNIT_TEST(StickyPromotionLosesToCollectionDisable) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(19, 20, 21));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto key = TSharedCacheKey::Page(collection, 1);
        const TPageCacheItem page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageLinked, page.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool promoted = true;
        TGateThread promoter(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            promoted = TSharedCacheTestAccess::MakePageSticky(*cache, page);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        gate.Slots[0].Release();
        promoter.Join();
        UNIT_ASSERT(!promoted);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
    }
    Y_UNIT_TEST(ConcurrentStickyPageInsertionPreservesBothPages) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(10, 11, 12));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        std::array<TPageCacheItem, 2> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            const auto key = TSharedCacheKey::Page(collection, index);
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
                UNIT_ASSERT(TSharedCacheTestAccess::MakePendingPageSticky(*fixture.Cache, pages[index]));
                ready[index] = fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index()));
            });
        }
        start.store(true, std::memory_order_release);
        for (std::thread& worker : workers) {
            worker.join();
        }
        UNIT_ASSERT(ready[0] && ready[1]);

        const ui32 head = TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection);
        UNIT_ASSERT(head == pages[0].Index() || head == pages[1].Index());
        const ui32 tail = TSharedCacheTestAccess::NextInOwner(*fixture.Cache, head);
        UNIT_ASSERT(tail == pages[0].Index() || tail == pages[1].Index());
        UNIT_ASSERT_VALUES_UNEQUAL(head, tail);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, tail), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).Refs(), 0);
    }
    Y_UNIT_TEST(ReviewR13ConcurrentStickyAdmissionTracksAllCharges) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(13, 14, 15));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
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
                pages[index] = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, index), 4096,
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
            stickyCount += TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsStickyField();
        }
        UNIT_ASSERT_VALUES_EQUAL(stickyCount, pages.size());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pages.size() * pageBytes);
        for (TPageCacheItem page : pages) {
            TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(StickyWithdrawalAfterReadyAccountingRefundsBudgetOnce) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 106);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        const auto page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 0), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeReadyStatePublication, page.CacheItem());
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
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), bytes);
    }
    Y_UNIT_TEST(CollectionStickyDisableCoversReadyAndCompletingPages) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(12, 13, 14));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const TPageCacheItem ready = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        const TPageCacheItem completing = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(ready && completing);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 2 * pageBytes);
        UNIT_ASSERT(fixture.Cache->MakeReady(ready, MakePageData(ready.Index())));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforePageReadyState, completing.CacheItem());
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
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { ready, completing }) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsStickyNoneField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, ready.Index()).IsHot());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, completing.Index()).IsCompleting());

        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(completed);
        const THandleState completedState = TSharedCacheTestAccess::HandleState(*fixture.Cache, completing.Index());
        UNIT_ASSERT(completedState.IsHot());
        UNIT_ASSERT(completedState.IsStickyNoneField());

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Sticky));
        const TPageCacheItem kept = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 3), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(kept);
        UNIT_ASSERT(fixture.Cache->MakeReady(kept, MakePageData(kept.Index())));
        const THandleState keptState = TSharedCacheTestAccess::HandleState(*fixture.Cache, kept.Index());
        UNIT_ASSERT(keptState.IsSticky());
        UNIT_ASSERT(keptState.IsStickyField());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), kept.Index());
    }
    Y_UNIT_TEST(StickyPageLinkedAfterSinglePassDrain) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(31, 32, 33));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const TPageCacheItem existing = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        const TPageCacheItem pending = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(existing && pending);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeStickyPageListLink, pending.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool ready = false;
        TGateThread completer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            TSharedCacheTestAccess::MakePendingPageSticky(*cache, pending);
            ready = cache->MakeReady(pending, MakePageData(pending.Index()));
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        for (TPageCacheItem page : { existing, pending }) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsHot());
            UNIT_ASSERT(state.IsStickyNoneField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
    }
    Y_UNIT_TEST(StickyListDetachRetainsPendingInsertion) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(13, 14, 15));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto existingKey = TSharedCacheKey::Page(collection, 1);
        const TPageCacheItem existing =
            AllocatePage(*fixture.Cache, existingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        const auto pendingKey = TSharedCacheKey::Page(collection, 2);
        const TPageCacheItem pending =
            AllocatePage(*fixture.Cache, pendingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(pending);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageLinked, pending.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool ready = false;
        TGateThread completer(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            TSharedCacheTestAccess::MakePendingPageSticky(*cache, pending);
            ready = cache->MakeReady(pending, MakePageData(pending.Index()));
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), pending.Index());
        const THandleState pendingState = TSharedCacheTestAccess::HandleState(*fixture.Cache, pending.Index());
        UNIT_ASSERT(pendingState.IsPending());
        UNIT_ASSERT(pendingState.IsStickyField());
        const THandleState existingState = TSharedCacheTestAccess::HandleState(*fixture.Cache, existing.Index());
        UNIT_ASSERT(existingState.IsSticky());
        UNIT_ASSERT(existingState.IsTransitionField());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, existing.Index()), 0);

        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(ready);
        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { existing, pending }) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsTransitionField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
    }
    Y_UNIT_TEST(StickyListDetachRetainsInFlightPromotion) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(16, 17, 19));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

        const auto existingKey = TSharedCacheKey::Page(collection, 1);
        const TPageCacheItem existing =
            AllocatePage(*fixture.Cache, existingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        const auto promotingKey = TSharedCacheKey::Page(collection, 2);
        const TPageCacheItem promoting = AllocatePage(*fixture.Cache, promotingKey, 4096);
        UNIT_ASSERT(promoting);
        UNIT_ASSERT(fixture.Cache->MakeReady(promoting, MakePageData(promoting.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, promoting.Index()).IsHot());

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageLinked, promoting.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool promoted = false;
        TGateThread promoter(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            promoted = TSharedCacheTestAccess::MakePageSticky(*cache, promoting);
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), promoting.Index());
        const THandleState promotingState = TSharedCacheTestAccess::HandleState(*fixture.Cache, promoting.Index());
        UNIT_ASSERT(promotingState.IsHot());
        UNIT_ASSERT(promotingState.IsStickyField());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, promoting.Index()), 0);
        const THandleState existingState = TSharedCacheTestAccess::HandleState(*fixture.Cache, existing.Index());
        UNIT_ASSERT(existingState.IsSticky());
        UNIT_ASSERT(existingState.IsTransitionField());

        gate.Slots[0].Release();
        promoter.Join();
        UNIT_ASSERT(promoted);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, promoting.Index()).IsSticky());

        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { existing, promoting }) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsTransitionField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
    }
    Y_UNIT_TEST(StickyListMergePreservesConcurrentInsertions) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(16, 17, 18));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto existingKey = TSharedCacheKey::Page(collection, 1);
        const TPageCacheItem existing =
            AllocatePage(*fixture.Cache, existingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        const auto beforeExchangeKey = TSharedCacheKey::Page(collection, 2);
        const TPageCacheItem beforeExchange = AllocatePage(
            *fixture.Cache, beforeExchangeKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(beforeExchange);
        const auto afterExchangeKey = TSharedCacheKey::Page(collection, 3);
        const TPageCacheItem afterExchange =
            AllocatePage(*fixture.Cache, afterExchangeKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(afterExchange);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterStickyPageListHeadPublished, collection.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool cut = false;
        TGateThread cutter(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            cut = TSharedCacheTestAccess::UnstickyCutPages(*cache, existing, fixture.Capacity.HandleCount());
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(TSharedCacheTestAccess::MakePendingPageSticky(*cache, beforeExchange));
        UNIT_ASSERT(cache->MakeReady(beforeExchange, MakePageData(beforeExchange.Index())));
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), beforeExchange.Index());

        gate.Slots[0].Release();
        gate.Slots[1].Wait();
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), existing.Index());

        UNIT_ASSERT(TSharedCacheTestAccess::MakePendingPageSticky(*cache, afterExchange));
        UNIT_ASSERT(cache->MakeReady(afterExchange, MakePageData(afterExchange.Index())));
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), afterExchange.Index());

        gate.Slots[1].Release();
        cutter.Join();
        UNIT_ASSERT(cut);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), afterExchange.Index());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::NextInOwner(*fixture.Cache, afterExchange.Index()), existing.Index());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::NextInOwner(*fixture.Cache, existing.Index()), beforeExchange.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, beforeExchange.Index()), 0);
        for (TPageCacheItem page : { existing, beforeExchange, afterExchange }) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsStickyField());
        }
    }
    Y_UNIT_TEST(StickyMergeRetainsConcurrentPageWhenFetchFails) {
        TFixture fixture;
        const TLogoBlobID id(16, 17, 19);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));

        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.emplace_back(MakePageLocation(1), new TTestPageFetchWaiter(completed, ready, completedItem));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status() == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem pending = requests[0].Fetch().CacheItem();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pending.Index()).IsStickyField());

        const TPageCacheItem cutPage = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(cutPage);
        UNIT_ASSERT(fixture.Cache->MakeReady(cutPage, MakePageData(cutPage.Index())));
        const TPageCacheItem concurrent = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 3), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::None);
        UNIT_ASSERT(concurrent);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterStickyPageListHeadPublished, collection.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool cut = false;
        TGateThread cutter(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            cut = TSharedCacheTestAccess::UnstickyCutPages(*cache, cutPage, cutPage.Index());
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*cache, collection), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::MakePendingPageSticky(*cache, concurrent));
        UNIT_ASSERT(cache->MakeReady(concurrent, MakePageData(concurrent.Index())));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*cache, collection), concurrent.Index());

        gate.Slots[0].Release();
        gate.Slots[1].Wait();
        UNIT_ASSERT(requests[0].Fetch().FailReady());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*cache, collection), concurrent.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*cache, concurrent.Index()), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, pending.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 0);

        gate.Slots[1].Release();
        cutter.Join();
        UNIT_ASSERT(cut);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*cache, collection), concurrent.Index());
        UNIT_ASSERT(cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*cache, collection), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, concurrent.Index()).IsStickyNoneField());
    }

    Y_UNIT_TEST(ReheatOvertakesHotEvictionAccounting) {
        for (ECacheMode mode : { ECacheMode::Regular, ECacheMode::TryKeepInMemory }) {
            TFixture fixture;
            const TLogoBlobID id(37, 39, 43);
            const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(collection);
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, mode));
            const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
            UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * pageBytes));
            const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
            AssertPageUnreferenced(*fixture.Cache, page);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 0);

            TSharedCacheGate gate;
            gate.Slots[0].Arm(ESharedCacheHookPoint::AfterHotEvictionPublished, page.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            bool evicted = false;
            TGateThread evictor(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                evicted = TSharedCacheTestAccess::EvictFromHot(*cache, page);
            });
            gate.Slots[0].Wait();
            const bool keep = mode == ECacheMode::TryKeepInMemory;
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, page.Index()).State() ==
                        (keep ? EHandleState::KeepCold : EHandleState::Cold));

            TTestSharedPageRef held;
            UNIT_ASSERT(cache->Find(collection, 1, held) == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(held.CacheItem() == page);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, page.Index()).IsHot());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::ColdPagesEstimate(*cache), keep ? 0 : -1);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::KeepColdPagesEstimate(*cache), keep ? -1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->HotPages(), 2);
            UNIT_ASSERT_VALUES_EQUAL(cache->ColdPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->KeepColdPages(), 0);

            gate.Slots[0].Release();
            evictor.Join();
            UNIT_ASSERT(evicted);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::ColdPagesEstimate(*cache), 0);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::KeepColdPagesEstimate(*cache), 0);
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
            const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, mode));
            const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
            UNIT_ASSERT(fixture.Cache->UpdateSoftLimitOverride(2 * bytes));
            const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
            TTestSharedPageRef held;
            UNIT_ASSERT(fixture.Cache->Find(collection, 1, held) == ESharedCacheResultStatus::Hit);
            TCollection* value = TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());
            UNIT_ASSERT_VALUES_EQUAL(value->ActivePageBytes(), bytes);

            TSharedCacheGate gate;
            gate.Slots[0].Arm(ESharedCacheHookPoint::AfterHotEvictionPublished, page.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            bool evicted = false;
            TGateThread evictor(gate, [&] {
                Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
                NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 0);
                Y_DEFER {
                    NSharedCache::TlsCacheHazard = nullptr;
                };
                evicted = TSharedCacheTestAccess::EvictFromHot(*cache, page);
            });
            gate.Slots[0].Wait();
            UNIT_ASSERT(cache->UpdateSoftLimitOverride(0));
            held.Drop();
            const THandleState pinned = TSharedCacheTestAccess::HandleState(*cache, page.Index());
            UNIT_ASSERT_VALUES_EQUAL(pinned.Version(), page.Version());
            UNIT_ASSERT(pinned.IsCold());
            UNIT_ASSERT_VALUES_EQUAL(pinned.Refs(), 1);
            UNIT_ASSERT_VALUES_EQUAL(value->ActivePageBytes(), bytes);

            gate.Slots[0].Release();
            evictor.Join();
            UNIT_ASSERT(evicted);
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*cache, page.Index()).IsFree());
            UNIT_ASSERT_VALUES_EQUAL(value->ActivePageBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->ResidentBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache->RetainedBytes(), 0);
        }
    }
    Y_UNIT_TEST(HotEvictionPinQueuesInitialKeepColdEntry) {
        TFixture fixture;
        const TLogoBlobID id(37, 39, 46);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * bytes));
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(state.IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index()) & KeepColdQueuedMask);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index())->ActivePageBytes(), bytes);
    }
    Y_UNIT_TEST(StaleHotWordDoesNotReclassifyColdPage) {
        TFixture fixture;
        const TLogoBlobID id(37, 39, 47);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(
            TSharedCacheTestAccess::MakeReady(*fixture.Cache, fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::TryKeepInMemory));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(0));
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        const ui64 retained = fixture.Cache->RetainedBytes();
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * bytes));
        TSharedCacheTestAccess::ProcessHotToken(*fixture.Cache, page);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->RetainedBytes(), retained);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
    }
    Y_UNIT_TEST(ReorderedCategoryTransfersConverge) {
        TFixture fixture;
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 103);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        TSharedCacheTestAccess::TransferColdToHot(*fixture.Cache, pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::ColdBytesEstimate(*fixture.Cache), -static_cast<i64>(pageBytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), 2 * pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);

        TSharedCacheTestAccess::TransferHotToCold(*fixture.Cache, pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::ColdBytesEstimate(*fixture.Cache), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);

        TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
    }
    Y_UNIT_TEST(ApplicationOwnerRetainsCacheAndSpace) {
        TFixture fixture;
        TIntrusivePtr<TTestSharedCache> applicationOwner = fixture.Cache;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 102);
        {
            const auto page = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(page);
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);
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
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 104);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, key);
        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);

        TlsCacheHazard = nullptr;
        found.Ref.Drop();

        TlsCacheHazard =
            &TCacheThreadTestAccess::Hazard(*fixture.Cache, TSharedCacheTestAccess::HazardCount(*fixture.Cache) - 1);
        const THandleState abandoned = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(abandoned.Refs(), 1);
    }
    Y_UNIT_TEST(FinalTombstoneHoldStartsOuterOperation) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 103);
        TTestSharedCacheItemRef hold;
        ui32 pageIndex = 0;
        ui32 pageVersion = 0;

        {
            const auto page = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(page);
            pageIndex = page.Index();
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);

            hold = TSharedCacheTestAccess::AcquirePending(*fixture.Cache, key);

            pageVersion = TSharedCacheTestAccess::WithHandle(*fixture.Cache, page.Index(), [](THandle& handle) {
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
            TSharedCacheTestAccess::FinishErase(*fixture.Cache, TCacheItem::Make(pageVersion, page.Index()), key);
            UNIT_ASSERT_VALUES_EQUAL(
                TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index(), std::memory_order_relaxed).Refs(), 1);
        }

        hold.Drop();
        const THandleState state =
            TSharedCacheTestAccess::HandleState(*fixture.Cache, pageIndex, std::memory_order_relaxed);
        UNIT_ASSERT(state.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), AdvanceItemVersion(pageVersion));
    }
    Y_UNIT_TEST(PublicWriterStickysSuccessorPublishedBeforeReclaim) {
        TFixture fixture;
        const auto keys = MakeAdjacentKeys(*fixture.Space, MakeCollectionCacheItem(4, 5), 400);
        const auto target = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(target);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[0], target).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(target, MakePageData(target.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, target));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterTableLinkCas);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);

        auto cache = fixture.Cache;
        ESharedCacheResultStatus status = ESharedCacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestSharedPageRef hit;
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
        const THandleState afterOwner = TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index());
        UNIT_ASSERT(afterOwner.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(afterOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 2 * pageBytes));
        gate.Slots[0].Release();
        writer.Join();

        UNIT_ASSERT(status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(FindPage(*fixture.Cache, keys[0]).Status == ESharedCacheResultStatus::Miss);
        auto successor = FindPage(*fixture.Cache, keys[1]);
        UNIT_ASSERT(successor.Status == ESharedCacheResultStatus::Hit);
        successor.Ref.Drop();
    }
    Y_UNIT_TEST(PublicWriterRetriesAfterReclaimWinsBeforeLinkCas) {
        TFixture fixture;
        const auto keys = MakeAdjacentKeys(*fixture.Space, MakeCollectionCacheItem(4, 5), 410);
        const auto target = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(target);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[0], target).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(target, MakePageData(target.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, target));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeTableLinkCas);
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);

        auto cache = fixture.Cache;
        ESharedCacheResultStatus status = ESharedCacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestSharedPageRef hit;
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
        const THandleState afterOwner = TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index());
        UNIT_ASSERT(afterOwner.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(afterOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 2 * pageBytes));
        gate.Slots[0].Release();
        writer.Join();

        UNIT_ASSERT(status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        auto successor = FindPage(*fixture.Cache, keys[1]);
        UNIT_ASSERT(successor.Status == ESharedCacheResultStatus::Hit);
        successor.Ref.Drop();
    }
    Y_UNIT_TEST(PublicHelperBecomesFinalRefAfterOwnerRelease) {
        TFixture fixture;
        const auto keys = MakeAdjacentKeys(*fixture.Space, MakeCollectionCacheItem(4, 5), 420);
        const auto target = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(target);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[0], target).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(target, MakePageData(target.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, target));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterTombstoneOwnerClaim);
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

        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterTombstoneHelperClaim);
        ESharedCacheResultStatus status = ESharedCacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestSharedPageRef hit;
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
        const THandleState afterOwner = TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index());
        UNIT_ASSERT(afterOwner.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(afterOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        gate.Slots[1].Release();
        writer.Join();

        UNIT_ASSERT(status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakePageData(inserted.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        auto successor = FindPage(*fixture.Cache, keys[1]);
        UNIT_ASSERT(successor.Status == ESharedCacheResultStatus::Hit);
        successor.Ref.Drop();
    }
    Y_UNIT_TEST(PublicHelperWinsBeforeFinalTombstoneCas) {
        TFixture fixture;
        const auto keys = MakeAdjacentKeys(*fixture.Space, MakeCollectionCacheItem(4, 5), 430);
        const auto target = AllocatePage(*fixture.Cache, keys[0], 4096);
        UNIT_ASSERT(target);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[0], target).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(target, MakePageData(target.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, target));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeTombstoneFinalCas, target.CacheItem());
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

        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterTombstoneHelperClaim, target.CacheItem());
        TGateThread helper(gate, [&] {
            Y_ABORT_UNLESS(!NSharedCache::TlsCacheHazard);
            NSharedCache::TlsCacheHazard = &NSharedCache::TCacheThreadTestAccess::Hazard(*cache, 1);
            Y_DEFER {
                NSharedCache::TlsCacheHazard = nullptr;
            };
            auto hold = TSharedCacheTestAccess::AcquireTombstoneHelper(*cache, target.CacheItem());
            hold.Drop();
        });
        gate.Slots[1].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index()).Refs(), 2);
        gate.Slots[0].Release();
        owner.Join();
        UNIT_ASSERT(reclaimed.load(std::memory_order_acquire));
        const THandleState afterOwner = TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index());
        UNIT_ASSERT(afterOwner.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(afterOwner.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        gate.Slots[1].Release();
        helper.Join();

        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, target.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 0));
        UNIT_ASSERT(FindPage(*fixture.Cache, keys[0]).Status == ESharedCacheResultStatus::Miss);
    }
    Y_UNIT_TEST(PublicReclaimPausesBeforeFinalTombstoneCas) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(4, 5), 440);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeTombstoneFinalCas);
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
        const THandleState paused = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
        UNIT_ASSERT(paused.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(paused.Refs(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, pageBytes));
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ESharedCacheResultStatus::Miss);
        gate.Slots[0].Release();
        owner.Join();

        UNIT_ASSERT(reclaimed.load(std::memory_order_acquire));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), ExpectedOverallUsage(*fixture.Cache, 0));
    }
    Y_UNIT_TEST(PendingFindAndEqualInsert) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(2, 3), 200);
        const auto candidate = AllocatePage(*fixture.Cache, key, 8192);
        UNIT_ASSERT(candidate);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, candidate).Status == ESharedCacheResultStatus::Inserted);

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT(!found.Ref);

        const auto duplicate = AllocatePage(*fixture.Cache, key, 8192);
        UNIT_ASSERT(duplicate);
        auto existing = FindOrInsertPage(*fixture.Cache, key, duplicate);
        UNIT_ASSERT(existing.Status == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT(!existing.Ref);
    }
    Y_UNIT_TEST(TerminalBoundaryReloadsOwnerVersion) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(2, 3), 201);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);

        const auto [state, terminal] =
            TSharedCacheTestAccess::WithHandle(*fixture.Cache, page.Index(), [](THandle& handle) {
                return std::pair{
                    THandleState::FromRaw(handle.State.load(std::memory_order_acquire)),
                    TCacheItem::FromRaw(handle.Next.load(std::memory_order_acquire)),
                };
            });
        const TCacheItem owner = TCacheItem::Make(state.Version(), page.Index());
        UNIT_ASSERT(terminal.IsNull());
        UNIT_ASSERT(TSharedCacheTestAccess::ValidateTerminalOwner(*fixture.Cache, owner, terminal));

        TSharedCacheTestAccess::WithHandle(*fixture.Cache, page.Index(), [&](THandle& handle) {
            handle.State.store(state.WithVersion(AdvanceItemVersion(state.Version())).Raw(), std::memory_order_release);
        });
        UNIT_ASSERT(!TSharedCacheTestAccess::ValidateTerminalOwner(*fixture.Cache, owner, terminal));
        TSharedCacheTestAccess::WithHandle(*fixture.Cache, page.Index(), [&](THandle& handle) {
            handle.State.store(state.Raw(), std::memory_order_release);
        });
    }
    Y_UNIT_TEST(PendingAcquireSurvivesGenerationChangeAfterTableFind) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(2, 3), 202);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);

        UNIT_ASSERT(TSharedCacheTestAccess::AcquireAfterGenerationChange(*fixture.Cache, key, true));
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ESharedCacheResultStatus::Pending);
    }
    Y_UNIT_TEST(AcquiredRefSurvivesGenerationChangeAfterTableFind) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(2, 3), 203);
        const auto page = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(page);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        UNIT_ASSERT(TSharedCacheTestAccess::AcquireAfterGenerationChange(*fixture.Cache, key, false));
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index(), std::memory_order_relaxed).Refs(), 0);
        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
        found.Ref.Drop();
    }
    Y_UNIT_TEST(HotHitSaturatesFrequencyInAcquireCas) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(3, 4), 300);
        const auto candidate = AllocatePage(*fixture.Cache, key, 4096);
        UNIT_ASSERT(candidate);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, candidate).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));

        for (ui8 expectedFrequency : { ui8{ 1 }, ui8{ 2 }, ui8{ 2 } }) {
            auto found = FindPage(*fixture.Cache, key);
            UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
            const auto state =
                TSharedCacheTestAccess::HandleState(*fixture.Cache, candidate.Index(), std::memory_order_relaxed);
            UNIT_ASSERT_VALUES_EQUAL(state.Frequency(), expectedFrequency);
            UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
            found.Ref.Drop();
        }
    }
    Y_UNIT_TEST(ColdHitReheatsAndInvalidatesMembership) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(4, 5), 400);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        ui32 pageIndex = 0;
        ui32 coldVersion = 0;
        {
            const auto page = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(page);
            pageIndex = page.Index();
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            coldVersion = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), pageBytes);
        }

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
        const THandleState state =
            TSharedCacheTestAccess::HandleState(*fixture.Cache, pageIndex, std::memory_order_relaxed);
        UNIT_ASSERT(state.IsHot());
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::ColdVersion(*fixture.Cache, pageIndex), AdvanceColdVersion(coldVersion));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
        found.Ref.Drop();

        UNIT_ASSERT(!fixture.Cache->ReclaimCold());
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ESharedCacheResultStatus::Hit);
    }
    Y_UNIT_TEST(LastColdReleasePublishesMembership) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(5, 6), 500);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        ui32 pageIndex = 0;
        TPageCacheItem pageItem;
        {
            const auto page = AllocatePage(*fixture.Cache, key, 4096);
            UNIT_ASSERT(page);
            pageIndex = page.Index();
            pageItem = page;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        }

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Ref);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pageItem));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pageIndex, std::memory_order_relaxed).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), pageBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), pageBytes);

        found.Ref.Drop();
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ESharedCacheResultStatus::Miss);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pageIndex, std::memory_order_relaxed).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdBytes(), 0);
    }
    Y_UNIT_TEST(S5FifoRoutesColdCandidates) {
        TFixture fixture(5);
        const auto token = MakeCollectionCacheItem(6, 7);
        TVector<ui32> pages;
        {
            for (ui64 offset = 0; offset <= TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache); ++offset) {
                const auto key = TSharedCacheKey::Page(token, offset);
                const auto page = AllocatePage(*fixture.Cache, key, 4096);
                UNIT_ASSERT(page);
                pages.push_back(page.Index());
                UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, page).Status == ESharedCacheResultStatus::Inserted);
                UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            }
        }

        ui32 coldPages = 0;
        for (ui32 index : pages) {
            const THandleState state =
                TSharedCacheTestAccess::HandleState(*fixture.Cache, index, std::memory_order_relaxed);
            coldPages += state.IsCold();
        }
        UNIT_ASSERT(coldPages > 0);

        UNIT_ASSERT(fixture.Cache->ReclaimCold());
    }

    Y_UNIT_TEST(MixedSizeAndModeChurnPreservesDataAndAccounting) {
        TFixture fixture(7);
        std::array<TCollectionCacheItem, 2> collections;
        std::array<TTestSharedCacheCollectionRef, 2> owners;
        for (ui32 index = 0; index < collections.size(); ++index) {
            const TLogoBlobID id(96, 108, index + 1);
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsertUnregistered(*fixture.Cache, { .Id = id },
                            collections[index], owners[index]) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(
                TSharedCacheTestAccess::MakeReadyUnregistered(*fixture.Cache, collections[index], MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->Find(id, owners[index]) == ESharedCacheResultStatus::Hit);
        }

        const ui64 initialLimit = fixture.Cache->CurrentLimit();
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(initialLimit / 4));
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
            TTestSharedPageRef ref;
            ESharedCacheResultStatus status = ESharedCacheResultStatus::Miss;
            for (ui32 attempt = 0; attempt < 256; ++attempt) {
                status = FindOrInsertPage(*fixture.Cache, collections[pageId % collections.size()],
                    MakePageLocation(pageId, size), EStickyState::None, page, ref);
                if (status != ESharedCacheResultStatus::Miss) {
                    break;
                }
                fixture.Cache->EnforceCurrentLimit();
            }
            if (status == ESharedCacheResultStatus::Hit) {
                ++hits;
                UNIT_ASSERT_VALUES_EQUAL(ref.size(), size);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(ref.data()[0]), static_cast<ui8>(pageId));
            } else {
                UNIT_ASSERT(status == ESharedCacheResultStatus::Inserted);
                reloads += seen[pageId];
                UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(pageId, size)));
                seen[pageId] = true;
            }
            ref.Drop();

            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedPages(), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PayloadUsage(), fixture.Cache->ResidentBytes());
            UNIT_ASSERT_VALUES_EQUAL(
                fixture.Cache->ResidentBytes(), fixture.Cache->HotBytes() + fixture.Cache->ColdBytes() +
                                                    fixture.Cache->KeepColdBytes() + fixture.Cache->StickyBytes());
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
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(1, 2, 3));
        const auto pageKey = TSharedCacheKey::Page(MakeCollectionCacheItem(7, 9), 42);
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        const auto page = AllocatePage(*fixture.Cache, pageKey, 4096);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(page);
        UNIT_ASSERT(TSharedCacheTestAccess::MakeReady(
            *fixture.Cache, fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
        UNIT_ASSERT(FindOrInsertCollection(*fixture.Cache, collectionKey, collection).Status ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, pageKey, page).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        auto foundCollection = FindCollection(*fixture.Cache, collectionKey);
        auto foundPage = FindPage(*fixture.Cache, pageKey);
        UNIT_ASSERT(foundCollection.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(foundPage.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(foundCollection.Ref.CacheItem() == collection);
        UNIT_ASSERT(foundCollection.Ref.CacheItem() == collection);
        UNIT_ASSERT_VALUES_EQUAL(foundCollection.Ref.GetCollection().Id(), CollectionId(collectionKey));
        UNIT_ASSERT_VALUES_EQUAL(&foundCollection.Ref.GetCollection(),
            TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index()));
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
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(51, 52), 0);
        TPageCacheItem insertedItem;
        TTestSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key), MakePageLocation(key.Word(1)),
                        EStickyState::None, insertedItem, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(insertedItem, MakePageData(insertedItem.Index())));
        TTestSharedPageRef page;
        UNIT_ASSERT(fixture.Cache->Find(PageCollection(key), key.Word(1), page) == ESharedCacheResultStatus::Hit);
        const char* data = page.data();
        auto moved = std::move(page);
        UNIT_ASSERT(!page);
        const auto* handles = TSharedCacheTestAccess::WithSpace(*fixture.Cache, [](const auto& operation) {
            return operation.Handles();
        });

        for (const auto* target : { &fixture.ReservedCapacity, &fixture.Capacity, &fixture.ReservedCapacity }) {
            TTransition transition;
            UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, *target, transition) ==
                        EPrepareResult::Prepared);
            UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, transition));
            UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, target->AddressBits));
            CompleteBucketResize(*fixture.Cache, *fixture.Space);
            CommitTransition(*fixture.Cache, transition);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::WithSpace(*fixture.Cache, [](const auto& operation) {
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
        const TPageCacheItem item = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        TTestSharedPageRef source;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, source) == ESharedCacheResultStatus::Hit);
        const char* data = source.data();
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, item));
        UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, item.Index())));
        TTestSharedPageRef absent;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, absent) == ESharedCacheResultStatus::Miss);

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
        TSharedCacheCapacity capacity;
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
        TSharedPageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*cache, collection, MakePageLocation(0), EStickyState::None, inserted, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(cache->MakeReady(inserted, MakePageData(inserted.Index())));

        TSharedPageRef page;
        UNIT_ASSERT(cache->Find(collection, 0, page) == ESharedCacheResultStatus::Hit);
        TSharedPageRef shared = std::move(page);
        UNIT_ASSERT(!page);
        UNIT_ASSERT(shared);
        UNIT_ASSERT_VALUES_EQUAL(shared.GetType(), NTable::NPage::EPage::DataPage);
        TSharedPageRef pinned = std::move(shared);
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
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(5, 6), 700);
        std::array<TPageCacheItem, ThreadCount> candidates;

        std::atomic<bool> start = false;
        std::array<ESharedCacheResultStatus, ThreadCount> statuses;
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
            if (statuses[thread] == ESharedCacheResultStatus::Inserted) {
                ++winners;
                winner = candidates[thread];
            } else {
                UNIT_ASSERT(statuses[thread] == ESharedCacheResultStatus::Pending);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(winners, 1);
        UNIT_ASSERT(fixture.Cache->MakeReady(winner, MakePageData(winner.Index())));

        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(found.Ref.CacheItem() == winner);
    }
    Y_UNIT_TEST(EraseColdHeadInteriorAndTail) {
        TFixture fixture;
        const auto token = MakeCollectionCacheItem(9, 10);
        TVector<TSharedCacheKey> keys;
        ui32 bucket = Max<ui32>();
        for (ui64 offset = 1; keys.size() < 3; ++offset) {
            const auto key = TSharedCacheKey::Page(token, offset);
            const ui32 candidateBucket =
                key.Hash() & TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).BucketMask();
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
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, key, candidate).Status == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, candidate));
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), fixture.Capacity.HandleCount() - 5);

        auto erase = [&](ui32 index) {
            return TSharedCacheTestAccess::EraseCold(*fixture.Cache, MakeColdItem(*fixture.Cache, index));
        };

        UNIT_ASSERT(erase(indices[1]));
        UNIT_ASSERT(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, indices[1], std::memory_order_relaxed).IsFree());
        UNIT_ASSERT(erase(indices[0]));
        UNIT_ASSERT(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, indices[0], std::memory_order_relaxed).IsFree());
        UNIT_ASSERT(erase(indices[2]));
        UNIT_ASSERT(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, indices[2], std::memory_order_relaxed).IsFree());
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), fixture.Capacity.HandleCount() - 2);

        for (ui32 index = 0; index < keys.size(); ++index) {
            const auto& key = keys[index];
            UNIT_ASSERT(FindPage(*fixture.Cache, key).Status == ESharedCacheResultStatus::Miss);
            UNIT_ASSERT(!erase(indices[index]));
        }
    }
    Y_UNIT_TEST(BridgesConsecutiveTombstonesIteratively) {
        constexpr ui32 TombstoneCount = 8;
        TFixture fixture;
        const auto token = MakeCollectionCacheItem(11, 12);
        TVector<TSharedCacheKey> keys;
        ui32 bucket = Max<ui32>();
        for (ui64 offset = 1; keys.size() < TombstoneCount + 1; ++offset) {
            const auto key = TSharedCacheKey::Page(token, offset);
            const ui32 candidateBucket =
                key.Hash() & TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).BucketMask();
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
            UNIT_ASSERT(
                FindOrInsertPage(*fixture.Cache, keys[index], page).Status == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        }

        for (ui32 index = 0; index < TombstoneCount; ++index) {
            tombstones[index] =
                TSharedCacheTestAccess::WithHandle(*fixture.Cache, indices[index], [&](THandle& handle) {
                    const THandleState cold = THandleState::FromRaw(handle.State.load(std::memory_order_acquire));
                    UNIT_ASSERT(cold.IsCold());
                    handle.State.store(
                        cold.WithState(EHandleState::Tombstone).WithRefs(1).Raw(), std::memory_order_release);
                    return TCacheItem::Make(cold.Version(), indices[index]);
                });
        }

        TSharedCacheTestAccess::FinishErase(*fixture.Cache, tombstones[TombstoneCount - 1], keys[TombstoneCount - 1]);

        for (ui32 index = 0; index + 1 < TombstoneCount; ++index) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, indices[index]);
            UNIT_ASSERT(state.IsTombstone());
            UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, indices[TombstoneCount - 1]).IsFree());

        for (ui32 index = 0; index < TombstoneCount; ++index) {
            UNIT_ASSERT(FindPage(*fixture.Cache, keys[index]).Status == ESharedCacheResultStatus::Miss);
        }
        auto successor = FindPage(*fixture.Cache, keys[TombstoneCount]);
        UNIT_ASSERT(successor.Status == ESharedCacheResultStatus::Hit);
        successor.Ref.Drop();

        for (ui32 index = 0; index + 1 < TombstoneCount; ++index) {
            TSharedCacheTestAccess::FinishErase(*fixture.Cache, tombstones[index], keys[index]);
        }
        UNIT_ASSERT_VALUES_EQUAL(AvailableFreeHandles(*fixture.Cache), fixture.Capacity.HandleCount() - 3);
    }
    Y_UNIT_TEST(ReaderTraversesAndWriterHelpsPausedTombstone) {
        TFixture fixture;
        const auto token = MakeCollectionCacheItem(12, 13);
        TVector<TSharedCacheKey> keys;
        ui32 bucket = Max<ui32>();
        for (ui64 offset = 1; keys.size() < 4; ++offset) {
            const auto key = TSharedCacheKey::Page(token, offset);
            const ui32 candidateBucket =
                key.Hash() & TSharedCacheTestAccess::CurrentSpaceState(*fixture.Space).BucketMask();
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
                FindOrInsertPage(*fixture.Cache, keys[index], candidate).Status == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, candidate));
        }

        const TCacheItem tombstoneItem =
            TSharedCacheTestAccess::WithHandle(*fixture.Cache, indices[1], [&](THandle& tombstone) {
                const THandleState cold = THandleState::FromRaw(tombstone.State.load(std::memory_order_relaxed));
                UNIT_ASSERT(cold.IsCold());
                tombstone.State.store(cold.WithState(EHandleState::Tombstone).WithRefs(1).Raw(),
                    std::memory_order_release); // paused deletion owner
                return TCacheItem::Make(cold.Version(), indices[1]);
            });

        UNIT_ASSERT(FindPage(*fixture.Cache, keys[1]).Status == ESharedCacheResultStatus::Miss);
        auto afterTombstone = FindPage(*fixture.Cache, keys[2]);
        UNIT_ASSERT(afterTombstone.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(afterTombstone.Ref.CacheItem().Index(), indices[2]);
        afterTombstone.Ref.Drop();

        const auto candidate = AllocatePage(*fixture.Cache, keys[3], 4096);
        UNIT_ASSERT(candidate);
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, keys[3], candidate).Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(candidate, MakePageData(candidate.Index())));

        auto state = TSharedCacheTestAccess::HandleState(*fixture.Cache, indices[1], std::memory_order_relaxed);
        UNIT_ASSERT(state.IsTombstone());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1); // only the paused deletion owner remains
        UNIT_ASSERT(TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(*fixture.Cache, indices[1])).IsFrozen());

        TSharedCacheTestAccess::FinishErase(*fixture.Cache, tombstoneItem, keys[1]);
        state = TSharedCacheTestAccess::HandleState(*fixture.Cache, indices[1], std::memory_order_relaxed);
        UNIT_ASSERT(state.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), AdvanceItemVersion(tombstoneItem.Version()));
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
    }
}

} // namespace NKikimr::NSharedCache
