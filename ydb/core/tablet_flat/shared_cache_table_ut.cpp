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
    Y_UNIT_TEST(PageBatchUsesCollectionStickyMode) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 87);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));

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

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, StickyCacheMode));
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(ExplicitStickyHintPromotesAnOrdinaryPendingFetch) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 92);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(FailedStickyFetchUnlinksOnlyItsPage) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 93);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(StickyBatchCompletionHonorsModeWithdrawal) {
        for (bool explicitSticky : { false, true }) {
            TFixture fixture;
            const TLogoBlobID id(85, 86, 90);
            const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
            UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
            auto binding = fixture.Cache->BindThreadHazard(0);
            succeeded = fetch.MakeReady(MakePageData(page.Index()));
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
            auto binding = fixture.Cache->BindThreadHazard(0);
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));

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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
            TTestSharedCachePageRef missing;
            UNIT_ASSERT(fixture.Cache->Find(collection, static_cast<ui64>(request.Location.Offset), missing) ==
                        ESharedCacheResultStatus::Miss);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        UNIT_ASSERT(fixture.Cache->DeleteCollection(fixture.Registry, collection));
    }
    Y_UNIT_TEST(PageBatchCoalescesDuplicateCandidates) {
        TFixture fixture;
        const TLogoBlobID id(85, 86, 87);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(collection);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
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
        UNIT_ASSERT_VALUES_EQUAL(admitted.StickyAdmissionFailures, 0);

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

        UNIT_ASSERT(fixture.Cache->UpdateStickyLimit(fixture.Cache->HardLimit()));
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
        UNIT_ASSERT_VALUES_EQUAL(kept.StickyAdmissionFailures, 0);

        UNIT_ASSERT(fixture.Cache->UpdateStickyLimit(0));
        TVector<TTestSharedCachePageRequest> failedSticky;
        addRequest(failedSticky, MakePageLocation(4, 4096));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, failedSticky));
        UNIT_ASSERT(failedSticky[0].Status() == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem ordinary = failedSticky[0].Fetch().CacheItem();
        UNIT_ASSERT(failedSticky[0].Fetch().MakeReady(MakePageData(ordinary.Index(), failedSticky[0].Location.Size)));
        UNIT_ASSERT(!TSharedCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsSticky());

        const TTestSharedCache::TStats fallback = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(fallback.RequestedPages, 6);
        UNIT_ASSERT_VALUES_EQUAL(fallback.MissPages, 4);
        UNIT_ASSERT_VALUES_EQUAL(fallback.StickyAdmissionFailures, 1);

        TVector<TTestSharedCachePageRequest> rejected;
        addRequest(rejected, MakePageLocation(5, Max<ui64>() / 2));
        addRequest(rejected, MakePageLocation(6, Max<ui64>() / 2));
        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, rejected));

        const TTestSharedCache::TStats unchanged = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(unchanged.RequestedPages, fallback.RequestedPages);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.RequestedBytes, fallback.RequestedBytes);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.HitPages, fallback.HitPages);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.MissPages, fallback.MissPages);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.MissBytes, fallback.MissBytes);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.StickyAdmissionFailures, fallback.StickyAdmissionFailures);
    }
    Y_UNIT_TEST(PageCountersFollowResidency) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(95, 96);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 0);

        TPageCacheItem hotItem;
        TTestSharedCachePageRef hit;
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

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(insertedCollection, StickyCacheMode));
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
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
        TCacheCollection* value = TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());
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
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, StickyCacheMode));
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
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(AttachmentTransitionRejectsCollectionPagesCacheModeChanges) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 105);
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, TSharedCacheKey::Collection(id));
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
        const auto page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 0), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
        bool detached = false;
        TGateThread detacher(gate, [&] {
            auto binding = fixture.Cache->BindThreadHazard(0);
            detached = fixture.Cache->DetachCollection(fixture.Registry, collection);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsTransitionField());
        for (ECacheMode mode : { ECacheMode::Regular, StickyCacheMode, ECacheMode::TryKeepInMemory }) {
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
            TTestSharedCachePageRef hit;
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

        TTestSharedCachePageRef hit;
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
        TTestSharedCachePageRef hit;
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
            TTestSharedCachePageRef unused;
            UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(index + 1),
                            EStickyState::None, pages[index], unused) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
            queuedVersions[index] = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, pages[index].Index());
            UNIT_ASSERT(queuedVersions[index] & KeepColdQueuedMask);
        }

        std::array<TTestSharedCachePageRef, 2> refs;
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
        TTestSharedCachePageRef unused;
        UNIT_ASSERT(TSharedCacheTestAccess::FindOrInsert(*fixture.Cache, collection, MakePageLocation(1),
                        EStickyState::None, page, unused) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        const ui32 queuedVersion = TSharedCacheTestAccess::ColdVersion(*fixture.Cache, page.Index());

        TTestSharedCachePageRef ref;
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
        TTestSharedCachePageRef hit;
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
            TTestSharedCachePageRef hit;
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
        TTestSharedCachePageRef completedPage;
        TIntrusivePtr<TAcquiringPageFetchWaiter> waiter = new TAcquiringPageFetchWaiter(*fixture.Cache, completedPage);
        TTestSharedCachePageRef hit;
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

        TTestSharedCachePageRef hit;
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
            auto binding = cache->BindThreadHazard(0);
            TTestSharedCachePageRef subscriberHit;
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
                auto binding = fixture.Cache->BindThreadHazard(thread);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                TTestSharedCachePageRef hit;
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
            TTestSharedCachePageRef hit;
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
        TTestSharedCachePageRef missing;
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
        TTestSharedCachePageRef hit;
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, inserted, collection) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!collection);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), location.AccountedBytes());
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, inserted, MakeCollection(id, location.BackingSize)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), location.AccountedBytes());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::FreeCount(*fixture.Cache), freeBefore - 2);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), inserted.Index());

        TCollectionCacheItem duplicate;
        TTestSharedCacheCollectionRef hitCollection;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, duplicate, hitCollection) ==
                    ESharedCacheResultStatus::Hit);
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
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert(fixture.Registry, location, duplicate, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(hit.CacheItem() == inserted);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), inserted.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, inserted.Index()), 0);
        hit.Drop();

        UNIT_ASSERT(fixture.Cache->UnlinkCollectionRegistry(fixture.Registry, inserted));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        const THandleState retained = TSharedCacheTestAccess::HandleState(*fixture.Cache, inserted.Index());
        UNIT_ASSERT(retained.IsSticky());
        UNIT_ASSERT(retained.IsStickyField());
        UNIT_ASSERT(fixture.Cache->DeleteCollection(inserted));
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
        UNIT_ASSERT(fixture.Cache->DeleteCollection(inserted));

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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, firstLocation, first, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, first, MakeCollection(firstId)));

        TCollectionCacheItem second;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, secondLocation, second, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, second, MakeCollection(secondId)));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), second.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);

        /* a detached, unreferenced record goes to the eviction control, so it can be re-attached
         * while it is still there */
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, first));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), second.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, first.Index()).IsHot());
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, first));
        const auto staleColdEntry = MakeColdItem(*fixture.Cache, first.Index());

        TCollectionCacheItem duplicate;
        TTestSharedCacheCollectionRef reattached;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, firstLocation, duplicate, reattached) ==
                    ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(reattached.CacheItem() == first);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, first.Index()).IsSticky());
        UNIT_ASSERT(!TSharedCacheTestAccess::EraseCold(*fixture.Cache, staleColdEntry));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), second.Index());

        reattached.Drop();
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, first));
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, second));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
    }
    Y_UNIT_TEST(CollectionReattachRestoresOwnReferenceWithResidentPage) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 17, 19) };
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionHit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, collection, collectionHit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(location.Id)));

        TPageCacheItem page;
        TTestSharedCachePageRef pageHit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(1), EStickyState::None, page,
                        pageHit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 1);
        TCollectionCacheItem duplicate;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, duplicate, collectionHit) ==
                    ESharedCacheResultStatus::Hit);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), 2);
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
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
            UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, collection, collectionHit) ==
                        ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(collection);
            UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(location.Id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
            TPageCacheItem page;
            if (sourceState == EHandleState::Sticky) {
                page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 1));
            }
            UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
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
                auto binding = cache->BindThreadHazard(0);
                TCollectionCacheItem duplicate;
                status = cache->FindOrInsert(fixture.Registry, location, duplicate, attached);
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
            UNIT_ASSERT(cache->DetachCollection(fixture.Registry, collection));
            if (page) {
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*cache, page));
                UNIT_ASSERT(TSharedCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, page.Index())));
            }
            UNIT_ASSERT(cache->DeleteCollection(collection));
            UNIT_ASSERT_VALUES_EQUAL(cache->Collections(), 0);
        }
    }
    Y_UNIT_TEST(KeepAccountingFollowsShadowSelectionAcrossReclaim) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 17, 19) };
        TIntrusiveConstPtr<NPageCollection::IPageCollection> source = new TTestPageCollection(location.Id, 0);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, collection, hit) ==
                    ESharedCacheResultStatus::Inserted);
        auto value = MakeHolder<TCacheCollection>(source, collection);
        auto* owner = value.Get();
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, std::move(value)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        TPageCacheItem shadow;
        TTestSharedCachePageRef pageHit;
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
            UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                collection, initiallyKeep ? ECacheMode::TryKeepInMemory : ECacheMode::Regular));
            TTestSharedCachePageRef hit;
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
                auto binding = fixture.Cache->BindThreadHazard(0);
                ready = fetch.MakeReady(MakePageData(page.Index()));
            });
            gate.Slots[0].Wait();
            // The mode update must complete while another publisher is stopped at the hook.
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(
                collection, initiallyKeep ? ECacheMode::Regular : ECacheMode::TryKeepInMemory));
            gate.Slots[0].Release();
            publisher.Join();
            UNIT_ASSERT(ready);
            const ui64 bytes = fixture.Cache->PageUsage();
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, inserted, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, inserted, MakeCollection(location.Id)));

        TTestSharedCacheCollectionRef held;
        UNIT_ASSERT(fixture.Cache->Find(location.Id, held) == ESharedCacheResultStatus::Hit);
        const TCacheCollection* value = &held.GetCollection();
        UNIT_ASSERT(fixture.Cache->DeleteCollection(fixture.Registry, inserted));
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
            UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, { .Id = id }, collection, collectionHit) ==
                        ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(collection);
            UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(id)));
            UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));

            const ui32 pageCount = lastOwner ? 1 : 2;
            TVector<TPageCacheItem> pages;
            for (ui32 offset = 1; offset <= pageCount; ++offset) {
                pages.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
                UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages.back()));
            }
            UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
            UNIT_ASSERT_VALUES_EQUAL(
                TSharedCacheTestAccess::CollectionReferences(*fixture.Cache, collection), pageCount);
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).Refs(), 0);
            const TCacheCollection* metadata =
                TSharedCacheTestAccess::CollectionValue(*fixture.Cache, collection.Index());

            TSharedCacheGate gate;
            gate.Slots[0].Arm(
                lastOwner ? ESharedCacheHookPoint::BeforeCollectionHotPublished
                          : ESharedCacheHookPoint::AfterCollectionOwnerReferenceDropped, collection.CacheItem());
            TSharedCacheHookGuard guard(*fixture.Cache, gate.Hooks);
            auto cache = fixture.Cache;
            bool erased = false;
            TGateThread releaser(gate, [&] {
                auto binding = cache->BindThreadHazard(0);
                erased = TSharedCacheTestAccess::EraseCold(*cache, MakeColdItem(*cache, pages[0].Index()));
            });
            gate.Slots[0].Wait();

            if (lastOwner) {
                UNIT_ASSERT(cache->DeleteCollection(collection));
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, collection, collectionHit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(location.Id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, StickyCacheMode));

        std::array<TPageCacheItem, 2> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestSharedCachePageRef pageHit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(index + 1), EStickyState::Sticky,
                            pages[index], pageHit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index())));
        }
        UNIT_ASSERT_VALUES_UNEQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(fixture.Cache->SetCollectionPagesCacheMode(collection, ECacheMode::Regular));
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, collection));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
        UNIT_ASSERT(!fixture.Cache->DeleteCollection(collection));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, collection.Index()).IsSticky());
        for (ui32 index = 0; index < pages.size(); ++index) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[index].Index());
            UNIT_ASSERT(state.IsHot());
            UNIT_ASSERT(state.IsStickyNoneField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, pages[index].Index()), 0);
            TTestSharedCachePageRef found;
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, collection, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(location.Id)));

        const TPageCacheItem page = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(page);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));

        TCollectionCacheItem duplicate;
        TTestSharedCacheCollectionRef moved;
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert(destination, location, duplicate, moved) == ESharedCacheResultStatus::Hit);
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
        std::array<THolder<TCacheCollection>, 2> values;
        std::array<TCollectionCacheItem, 2> collections;
        TTestSharedCacheCollectionRef hit;
        for (ui32 index = 0; index < collections.size(); ++index) {
            UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, locations[index], collections[index], hit) ==
                        ESharedCacheResultStatus::Inserted);
            values[index] = MakeCollection(locations[index].Id);
        }

        std::atomic<bool> start = false;
        std::array<bool, 2> ready{};
        std::array<std::thread, 2> workers;
        for (ui32 index = 0; index < workers.size(); ++index) {
            workers[index] = std::thread([&, index] {
                auto binding = fixture.Cache->BindThreadHazard(index);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                ready[index] = fixture.Cache->MakeReady(fixture.Registry, collections[index], std::move(values[index]));
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
        TTestSharedCachePageRef hit;
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
        TTestSharedCachePageRef hit;
        std::thread worker([&] {
            auto binding = fixture.Cache->BindThreadHazard(0);
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
        TTestSharedCachePageRef hit;
        TGateThread writer(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth));
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink));
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
}

} // namespace NKikimr::NSharedCache
