#include "shared_cache.h"
#include "shared_cache_traits.h"

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
using TTestSharedCacheItemRef = TSharedCacheItemRefImpl<TTestTraits>;
using TTestSharedCachePageRef = TSharedCachePageRefImpl<TTestTraits>;
using TTestSharedCacheCollectionRef = TSharedCacheCollectionRefImpl<TTestTraits>;
using TTestPageFetch = TPageFetchImpl<TTestTraits>;
using TTestSharedCachePageRequest = TSharedCachePageRequestImpl<TTestTraits>;

static_assert(sizeof(TTestSharedCacheItemRef) == sizeof(TCacheItem));
static_assert(sizeof(TTestSharedCachePageRef) == 8);
static_assert(sizeof(TTestSharedCacheCollectionRef) == sizeof(TCacheItem));

struct TSeparatePageRequest {
    NTable::NPage::TPageLocation Location;
    TIntrusivePtr<TPageFetchWaiter> Waiter;
    ESharedCacheResultStatus Status;
    TTestSharedCachePageRef Page;
    TTestPageFetch Fetch;
};

static_assert(sizeof(TTestSharedCachePageRequest) < sizeof(TSeparatePageRequest));

template <class TRef>
struct TTestResult {
    ESharedCacheResultStatus Status = ESharedCacheResultStatus::Miss;
    TRef Ref;
};

using TPageResult = TTestResult<TTestSharedCachePageRef>;
using TCollectionResult = TTestResult<TTestSharedCacheCollectionRef>;
using TItemResult = TTestResult<TTestSharedCacheItemRef>;

class TSharedCacheTestAccess {
public:
    static bool MakeReadyUnregistered(
        TTestSharedCache& cache, TCollectionCacheItem collection, THolder<TCacheCollection>&& value) noexcept {
        return cache.MakeReady(nullptr, collection, std::move(value));
    }

    static TTestSharedCacheItemRef AcquireStructural(TTestSharedCache& cache, TCacheItem item) noexcept {
        auto operation = cache.BeginOperation();
        return cache.TryAcquireStructural(operation, item);
    }

    static bool MakePendingPageSticky(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        auto spaceOp = cache.BeginOperation();
        TOperationItemRef<TTestTraits> ref(spaceOp, cache.TryAcquireStructural(spaceOp, page.CacheItem()));
        return ref && cache.MakePageSticky(spaceOp, page.CacheItem());
    }

    static bool MakePageSticky(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        TTestSharedCachePageRef held;
        if (!cache.AcquirePage(page, held)) {
            return false;
        }
        auto spaceOp = cache.BeginOperation();
        return cache.MakePageSticky(spaceOp, page.CacheItem());
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
        TSharedCachePageRefImpl<TTraits>& hit) noexcept {
        return cache.FindOrInsert(collection, page, sticky, inserted, hit);
    }

    template <class TTraits>
    static ESharedCacheResultStatus FindOrInsert(TSharedCacheImpl<TTraits>& cache, TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EStickyState sticky, TIntrusivePtr<TPageFetchWaiter> waiter,
        TSharedCachePageRefImpl<TTraits>& hit, TPageFetchImpl<TTraits>& fetch) noexcept {
        return cache.FindOrInsert(collection, page, sticky, std::move(waiter), hit, fetch);
    }

    static ESharedCacheResultStatus FindOrInsertUnregistered(TTestSharedCache& cache,
        const TCollectionLocation& collection, TCollectionCacheItem& inserted,
        TTestSharedCacheCollectionRef& hit) noexcept {
        return cache.FindOrInsert(collection, inserted, hit);
    }

    static bool AcquirePage(TTestSharedCache& cache, TPageCacheItem page, TTestSharedCachePageRef& result) noexcept {
        return cache.AcquirePage(page, result);
    }

    static void DiscardCandidate(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        cache.DiscardCandidate(page);
    }

    static void DiscardCandidate(TTestSharedCache& cache, TCollectionCacheItem collection) noexcept {
        cache.DiscardCandidate(collection);
    }

    static bool AdvanceBucketResize(TTestSharedCache& cache) noexcept {
        return cache.AdvanceBucketResize();
    }

    static bool TryReserve(TTestSharedCache& cache, ui64 bytes) noexcept {
        auto spaceOp = cache.BeginOperation();
        return cache.TryReserve(spaceOp, bytes);
    }

    static void ReleaseReservation(TTestSharedCache& cache, ui64 bytes) noexcept {
        cache.ReleaseReservation(bytes);
    }

    static bool PrepareTransition(
        TTestSharedCache& cache, const TSharedCacheCapacity& target, TTransition& transition) noexcept {
        return cache.PrepareTransition(target, transition);
    }

    static bool PublishMigrationView(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.PublishMigrationView(transition);
    }

    static bool TryDrainTransition(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.TryDrainTransition(transition);
    }

    static bool PublishFinalView(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.PublishFinalView(transition);
    }

    static bool AppendFreeHandles(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.AppendFreeHandles(transition);
    }

    static bool FinalDrain(TTestSharedCache& cache, TTransition& transition) noexcept {
        do {
            if (!cache.FinalDrain(transition)) {
                return false;
            }
        } while (transition.Phase() == ETransitionPhase::FinalDrain);
        return true;
    }

    static bool TryReleaseTransition(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.TryReleaseTransition(transition);
    }

    static bool CommitTransition(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.CommitTransition(transition);
    }

    static bool AbandonTransition(TTestSharedCache& cache, TTransition& transition) noexcept {
        return cache.AbandonTransition(transition);
    }

    template <class TCallback>
    static decltype(auto) WithSpace(TTestSharedCache& cache, TCallback&& callback) {
        auto spaceOp = cache.BeginOperation();
        return std::forward<TCallback>(callback)(spaceOp);
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
            const ui64 bytes = cache.PageBytes(handle);
            handle.State.store(state.WithState(EHandleState::KeepCold).Raw(), std::memory_order_relaxed);
            handle.ColdVersion.store(coldVersion, std::memory_order_relaxed);
            cache.HotBytes_.fetch_sub(bytes, std::memory_order_relaxed);
            cache.KeepColdBytes_.fetch_add(bytes, std::memory_order_relaxed);
            cache.HotPages_.fetch_sub(1, std::memory_order_relaxed);
            cache.KeepColdPages_.fetch_add(1, std::memory_order_relaxed);
            cache.AddKeepColdOwnedBytes(bytes);
        });
    }

    static ui32 QueueKeepColdPageForTest(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            THandle& handle = spaceOp.Handles()[page.Index()];
            const ui32 version =
                AdvanceColdVersion(handle.ColdVersion.load(std::memory_order_relaxed) & MaxItemVersion) |
                KeepColdQueuedMask;
            handle.ColdVersion.store(version, std::memory_order_relaxed);
            spaceOp.KeepCold().PutAndPop(TCacheItem::Make(version & MaxItemVersion, page.Index()).Raw());
            return version;
        });
    }

    static TCacheCollection* CollectionValue(TTestSharedCache& cache, ui32 index) noexcept {
        return WithHandle(cache, index, [](THandle& handle) {
            return handle.Body.Collection;
        });
    }

    static ui32 StickyPageListHead(TTestSharedCache& cache, TCollectionCacheItem collection) noexcept {
        TCacheCollection* value = CollectionValue(cache, collection.Index());
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

    static ui32 MinimumHotSlots(TTestSharedCache& cache, bool underPressure = false) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            return cache.MinimumHotSlots(spaceOp, underPressure);
        });
    }

    static std::array<ui64, 4> HotPolicy(TTestSharedCache& cache, const TSpaceOperation& spaceOp) noexcept {
        const ui32 effective = static_cast<ui32>(spaceOp.EffectiveHotSlots());
        return { effective, cache.MinimumHotSlots(spaceOp), cache.ShrinkHotTarget(spaceOp, effective, 1, false),
            cache.GrowHotTarget(spaceOp, effective, 2) };
    }

    static bool EnforceWithOperation(TTestSharedCache& cache, TSpaceOperation& spaceOp) noexcept {
        return cache.EnforceCurrentLimit(spaceOp, 0, true);
    }

    static THotResize::TGuard LockHotResize(TTestSharedCache& cache) noexcept {
        return cache.HotResize_.TryLock();
    }

    static std::pair<ui64, ui64> ColdByteWatermarks(const TTestSharedCache& cache) noexcept {
        return { cache.ColdMinBytes_.load(std::memory_order_relaxed),
            cache.ColdGrowBytes_.load(std::memory_order_relaxed) };
    }

    static void ShrinkHotToMinimum(TTestSharedCache& cache) noexcept {
        for (;;) {
            const ui64 before = EffectiveHotSlots(cache);
            WithSpace(cache, [&](TSpaceOperation& spaceOp) {
                cache.EnforceCurrentLimit(spaceOp, 0, true);
            });
            if (EffectiveHotSlots(cache) == before) {
                return;
            }
        }
    }

    static bool ShrinkKeepCold(TTestSharedCache& cache) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            return cache.ShrinkKeepCold(spaceOp);
        });
    }

    static bool ProcessHotAtMinimum(TTestSharedCache& cache) noexcept {
        return WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            return cache.ProcessHotAtMinimum(spaceOp);
        });
    }

    static void ProcessHotToken(TTestSharedCache& cache, TPageCacheItem page) noexcept {
        WithSpace(cache, [&](TSpaceOperation& spaceOp) {
            cache.ProcessHot(spaceOp, TTestSharedCache::EHotLevel::L0, page.CacheItem());
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
        return cache.Space_->Hazards_[index].SpareItem.load(std::memory_order_relaxed);
    }

    static TSpaceState CurrentSpaceState(const TSharedCacheSpace& space) noexcept {
        return space.CurrentSpaceState();
    }

    static TBucketResizeState BucketResizeState(const TSharedCacheSpace& space) noexcept {
        return space.BucketResizeState();
    }

    static bool EvictFromHot(TTestSharedCache& cache, TPageCacheItem page) {
        auto operation = cache.BeginOperation();
        return cache.EvictFromHot(operation, page.Index());
    }

    static bool EvictFromHot(TTestSharedCache& cache, TCollectionCacheItem collection) {
        auto operation = cache.BeginOperation();
        return cache.EvictFromHot(operation, collection.Index());
    }

    static void TransferColdToHot(TTestSharedCache& cache, ui64 bytes) {
        cache.TransferEstimatedBytes(cache.ColdBytes_, cache.HotBytes_, bytes);
    }

    static void TransferHotToCold(TTestSharedCache& cache, ui64 bytes) {
        cache.TransferEstimatedBytes(cache.HotBytes_, cache.ColdBytes_, bytes);
    }

    static i64 ColdBytesEstimate(const TTestSharedCache& cache) {
        return cache.ColdBytes_.load(std::memory_order_relaxed);
    }

    static i64 ColdPagesEstimate(const TTestSharedCache& cache) {
        return cache.ColdPages_.load(std::memory_order_relaxed);
    }

    static i64 KeepColdPagesEstimate(const TTestSharedCache& cache) {
        return cache.KeepColdPages_.load(std::memory_order_relaxed);
    }

    static ETransitionPhase HardTransitionPhase(const TTestSharedCache& cache) noexcept {
        return cache.HardTransition_.Phase();
    }

    static ui64 HardTransitionWorkIndex(const TTestSharedCache& cache) noexcept {
        return cache.HardTransition_.NextWorkIndex_;
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
            Y_ABORT_UNLESS(slot < spaceOp.View().HotSlotCount);
            return spaceOp.View().HotSlots[slot].load(std::memory_order_acquire);
        });
    }

    static ui64 CollectionReferences(TTestSharedCache& cache, TCollectionCacheItem collection) {
        auto operation = cache.BeginOperation();
        return operation.Handles()[collection.Index()].Body.Collection->References.load(std::memory_order_relaxed);
    }

    static bool EraseCold(TTestSharedCache& cache, TCacheItem cacheItem) {
        auto operation = cache.BeginOperation();
        return cache.EraseCold(operation, cacheItem);
    }

    static bool ReclaimCold(TTestSharedCache& cache, TSpaceOperation& operation) noexcept {
        return cache.ReclaimCold(operation);
    }

    static bool ReclaimKeepCold(TTestSharedCache& cache, TSpaceOperation& operation) noexcept {
        return cache.ReclaimKeepCold(operation);
    }

    static bool TrimKeepCold(TTestSharedCache& cache, TSpaceOperation& operation) noexcept {
        return cache.TrimKeepCold(operation);
    }

    static bool UnstickyCutPages(TTestSharedCache& cache, TPageCacheItem page, ui64 allocationLimit) {
        auto operation = cache.BeginOperation();
        return cache.UnstickyCutPages(operation, page.CacheItem(), allocationLimit);
    }

    static bool RelocatePage(TTestSharedCache& cache, TPageCacheItem page) {
        auto operation = cache.BeginOperation();
        return cache.RelocatePage(operation, page.Index());
    }

    static bool BeginBucketResize(TTestSharedCache& cache, ui8 newAddressBits) {
        return cache.Space_->BeginBucketResize(newAddressBits);
    }

    static bool PrepareBucketSplit(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.PrepareBucketSplit(spaceOp);
    }

    static bool ActivateBucketSplitOrMerge(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.ActivateBucketSplitOrMerge(spaceOp);
    }

    static bool PublishBucketSplit(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.PublishBucketSplit(spaceOp);
    }

    static bool CloseBucketSplit(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.CloseBucketSplit(spaceOp);
    }

    static bool RemoveBucketSplit(TTestSharedCache& cache) {
        auto operation = cache.BeginOperation();
        return cache.Table_.RemoveBucketSplit(operation);
    }

    static bool FinishBucketResize(TTestSharedCache& cache) {
        return cache.Space_->FinishBucketResize();
    }

    static void CompleteTombstone(TTestSharedCache& cache, TCacheItem cacheItem, const TSharedCacheKey& key) {
        auto operation = cache.BeginOperation();
        TOperationItemRef<TTestTraits> ownerRef(operation, cacheItem);
        cache.Table_.CompleteTombstone(ownerRef.CacheItem(), key, operation);
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
            const auto status = cache.TryAcquire(operation, cacheItem, false, ref);
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
        Y_ABORT_UNLESS(cache.TryAcquire(operation, cacheItem, true, ref) == TTestSharedCache::EAcquireStatus::Pending);
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

    static_assert(!std::is_copy_constructible_v<TTestSharedCachePageRef>);
    static_assert(!std::is_copy_assignable_v<TTestSharedCachePageRef>);
    static_assert(std::is_move_constructible_v<TTestSharedCachePageRef>);
    static_assert(std::is_move_assignable_v<TTestSharedCachePageRef>);
    static_assert(!std::is_copy_constructible_v<TTestSharedCacheCollectionRef>);
    static_assert(std::is_move_constructible_v<TTestSharedCacheCollectionRef>);
    static_assert(std::is_move_assignable_v<TTestSharedCacheCollectionRef>);
    static_assert(!std::is_copy_constructible_v<TTestSharedCacheItemRef>);
    static_assert(!std::is_convertible_v<TTestSharedCachePageRef, TTestSharedCacheCollectionRef>);
    static_assert(!std::is_convertible_v<TTestSharedCacheCollectionRef, TTestSharedCachePageRef>);

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
        TAcquiringPageFetchWaiter(TTestSharedCache& cache, TTestSharedCachePageRef& page) noexcept
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
        TTestSharedCachePageRef& Page_;
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

    [[maybe_unused]] TCollectionCacheItem MakeCollectionCacheItem(ui32 version, ui32 index) {
        return TCollectionCacheItem::FromValidated(TCacheItem::Make(version, index));
    }

    [[maybe_unused]] NActors::TSharedData MakePageData(ui32 index, size_t size = 4096) {
        auto data = NActors::TSharedData::Uninitialized(size);
        data.mutable_data()[0] = static_cast<char>(index);
        return data;
    }

    [[maybe_unused]] NTable::NPage::TPageLocation MakePageLocation(
        ui64 offset, ui64 size = 4096, NTable::NPage::EPage type = NTable::NPage::EPage::DataPage, ui32 crc32 = 0) {
        return NTable::NPage::TPageLocation::FromByteOffset(offset, size, type, crc32);
    }

    [[maybe_unused]] THolder<TCacheCollection> MakeCollection(const TLogoBlobID& id, size_t backingSize = 0) {
        return MakeHolder<TCacheCollection>(
            TIntrusiveConstPtr<NPageCollection::IPageCollection>(new TTestPageCollection(id, backingSize)));
    }

    [[maybe_unused]] TCacheItem MakeColdItem(TTestSharedCache& cache, ui32 index) {
        return TCacheItem::Make(TSharedCacheTestAccess::ColdVersion(cache, index) & MaxItemVersion, index);
    }

    [[maybe_unused]] ui64 AvailableFreeHandles(const TTestSharedCache& cache) {
        ui64 result = TSharedCacheTestAccess::FreeCount(cache);
        for (ui32 index = 0; index < TSharedCacheTestAccess::HazardCount(cache); ++index) {
            result += TSharedCacheTestAccess::SpareItem(cache, index) != 0;
        }
        return result;
    }

    [[maybe_unused]] TCollectionCacheItem PageCollection(const TSharedCacheKey& key) {
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
                if ((!target.IsNull() && target != cacheItem) || Triggered.exchange(true, std::memory_order_acq_rel)) {
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

    [[maybe_unused]] TLogoBlobID CollectionId(const TSharedCacheKey& key) {
        Y_ABORT_UNLESS(key.IsCollectionKind());
        return TLogoBlobID(key.Word(0), key.Word(1), key.Word(2));
    }

    // The fixture inserts Target first and no other key in this bucket before Successor.
    [[maybe_unused]] std::array<TSharedCacheKey, 2> MakeAdjacentKeys(
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

    [[maybe_unused]] std::array<TSharedCacheKey, 3> MakeResizeKeys(
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

    [[maybe_unused]] TPageCacheItem AllocatePage(TTestSharedCache& cache, const TSharedCacheKey& key, ui64 size,
        NTable::NPage::EPage type = NTable::NPage::EPage::DataPage, ui32 crc32 = 0,
        EStickyState sticky = EStickyState::None) {
        return TSharedCacheTestAccess::AllocatePage(cache, PageCollection(key), key.Word(1), size, type, crc32, sticky);
    }

    [[maybe_unused]] TPageResult FindPage(TTestSharedCache& cache, const TSharedCacheKey& key) {
        TPageResult result;
        result.Status = cache.Find(PageCollection(key), key.Word(1), result.Ref);
        return result;
    }

    [[maybe_unused]] TItemResult FindOrInsertPage(
        TTestSharedCache& cache, const TSharedCacheKey& key, TPageCacheItem candidate) {
        return TSharedCacheTestAccess::FindOrInsert(cache, key, candidate);
    }

    template <class TTraits>
    [[maybe_unused]] ESharedCacheResultStatus FindOrInsertPage(TSharedCacheImpl<TTraits>& cache,
        TCollectionCacheItem collection, const NTable::NPage::TPageLocation& page, EStickyState sticky,
        TPageCacheItem& inserted, TSharedCachePageRefImpl<TTraits>& hit) {
        return TSharedCacheTestAccess::FindOrInsert(cache, collection, page, sticky, inserted, hit);
    }

    [[maybe_unused]] TCollectionCacheItem AllocateCollection(
        TTestSharedCache& cache, const TSharedCacheKey& key, size_t backingSize = 0) {
        Y_UNUSED(backingSize);
        return TSharedCacheTestAccess::AllocateCollection(cache, CollectionId(key), 0);
    }

    [[maybe_unused]] TCollectionResult FindCollection(TTestSharedCache& cache, const TSharedCacheKey& key) {
        TCollectionResult result;
        result.Status = cache.Find(CollectionId(key), result.Ref);
        return result;
    }

    [[maybe_unused]] TItemResult FindOrInsertCollection(
        TTestSharedCache& cache, const TSharedCacheKey& key, TCollectionCacheItem candidate) {
        return TSharedCacheTestAccess::FindOrInsert(cache, key, candidate);
    }

    [[maybe_unused]] TPageCacheItem InsertReadyPage(TTestSharedCache& cache, const TSharedCacheKey& key) {
        TPageCacheItem page;
        TTestSharedCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(cache, PageCollection(key), MakePageLocation(key.Word(1)), EStickyState::None,
                        page, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(cache.MakeReady(page, MakePageData(page.Index())));
        return page;
    }

    [[maybe_unused]] void AssertPageHit(TTestSharedCache& cache, const TSharedCacheKey& key, TPageCacheItem expected) {
        auto found = FindPage(cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(found.Ref.CacheItem() == expected);
        found.Ref.Drop();
    }

    [[maybe_unused]] void AssertPageUnreferenced(TTestSharedCache& cache, TPageCacheItem page) {
        const THandleState state = TSharedCacheTestAccess::HandleState(cache, page.Index());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), page.Version());
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
    }

    [[maybe_unused]] void AssertResizeMarker(TTestSharedCache& cache, EHandleState expectedState) {
        const THandleState state = TSharedCacheTestAccess::HandleState(cache, ResizeMarkerIndex);
        UNIT_ASSERT(state.State() == expectedState);
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 1);
    }

    [[maybe_unused]] void CompleteBucketResize(TTestSharedCache& cache, TSharedCacheSpace& space) {
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

    [[maybe_unused]] void CommitTransition(TTestSharedCache& cache, TTransition& transition) {
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
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(addressBits, 4096, 0, hazards, Capacity));
            if (reservedAddressBits == 0) {
                reservedAddressBits = addressBits;
            }
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(reservedAddressBits, 4096, 0, hazards, ReservedCapacity));
            auto space = TSharedCacheSpace::Create(Capacity, ReservedCapacity, hazards);
            UNIT_ASSERT(space);
            Space = space.Get();
            Cache = TTestSharedCache::Create(std::move(space), hardLimit ? hardLimit : Capacity.Limit);
            UNIT_ASSERT(Cache);
            MainHazard = Cache->BindThreadHazard(hazards - 1);
        }

        TSharedCacheCapacity Capacity;
        TSharedCacheCapacity ReservedCapacity;
        TSharedCacheSpace* Space = nullptr;
        TCollectionRegistry Registry;
        TIntrusivePtr<TTestSharedCache> Cache;
        TSharedCacheThreadBinding<TTestTraits> MainHazard;
    };

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
            UNIT_ASSERT(fixture.Cache->MakeReady(
                fixture.Registry, collection, MakeHolder<TCacheCollection>(std::move(metadata))));
            const auto page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 1), data.size());
            UNIT_ASSERT(page);
            // Handle-order teardown would destroy this metadata before releasing its page buffer.
            UNIT_ASSERT(collection.Index() < page.Index());
            UNIT_ASSERT(fixture.Cache->MakeReady(page, std::move(data)));
        }
        UNIT_ASSERT(pagesReleasedBeforeCollection);
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
        TTestSharedCachePageRef page;
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
        TTestSharedCachePageRef pendingPage;
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
        TTestSharedCachePageRef hitPage;
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
        const auto location = NTable::NPage::TPageLocation::FromPageIndex(7, 4096, NTable::NPage::EPage::DataPage, 123);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        TTestSharedCachePageRef hit;
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
    Y_UNIT_TEST(PageBatchRejectsReservationAboveCurrentLimit) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 85);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PageUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), overallBefore);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);
    }
    Y_UNIT_TEST(MemoryWaitingPagesCoalesceAndReserveAfterRelease) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 95);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT(
            TSharedCacheTestAccess::HandleState(*fixture.Cache, fetch.CacheItem().Index()).IsWaitingForMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(!fetch.Dispatch());
        UNIT_ASSERT(!fetch.TryReserveMemory());
        const ui64 generation = fixture.Cache->ResourceGeneration();
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
        UNIT_ASSERT(fixture.Cache->ResourceGeneration() > generation);
        UNIT_ASSERT(fetch.TryReserveMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fetch.TryReserveMemory());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fetch.Dispatch());
        UNIT_ASSERT(fetch.MakeReady(MakePageData(fetch.CacheItem().Index())));
        UNIT_ASSERT_VALUES_EQUAL(completed.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PageUsage(), bytes);
    }

    Y_UNIT_TEST(CancellingMemoryWaitingPageDoesNotRefundUnownedBytes) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 96);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->PageUsage(), bytes);
        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }

    Y_UNIT_TEST(MemoryWaitingBatchReservesAllPagesBeforeDispatch) {
        TFixture fixture;
        const TLogoBlobID id(83, 84, 97);
        const auto collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        const auto page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
        TTestSharedCachePageRef held;
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));

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
}

} // namespace NKikimr::NSharedCache
