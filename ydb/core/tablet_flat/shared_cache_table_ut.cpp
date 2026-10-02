#include "shared_cache.h"
#include "shared_cache_traits.h"
#include "shared_handle.h"

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
static_assert(sizeof(TTestSharedCachePageRef) == 32);
static_assert(sizeof(TTestSharedCacheCollectionRef) == 2 * sizeof(TCacheItem));

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

    static bool UnstickyCutPages(TTestSharedCache& cache, TPageCacheItem page, ui64 allocationLimit) {
        auto operation = cache.BeginOperation();
        return cache.UnstickyCutPages(operation, page.CacheItem(), allocationLimit);
    }

    static bool BeginBucketResize(TTestSharedCache& cache, ui8 newAddressBits) {
        return cache.Space_->BeginBucketResize(newAddressBits);
    }

    static bool PrepareBucketSplit(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.PrepareBucketSplit(spaceOp);
    }

    static bool ActivateBucketSplit(TTestSharedCache& cache) {
        auto spaceOp = cache.BeginOperation();
        return cache.Table_.ActivateBucketSplit(spaceOp);
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
        {
        }

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
        {
        }

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
        {
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

    private:
        const TLogoBlobID Id_;
        const size_t BackingSize_;
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

    THolder<TCacheCollection> MakeCollection(const TLogoBlobID& id, size_t backingSize = 0) {
        return MakeHolder<TCacheCollection>(
            TIntrusiveConstPtr<NPageCollection::IPageCollection>(new TTestPageCollection(id, backingSize)));
    }

    TCacheItem MakeColdItem(TTestSharedCache& cache, ui32 index) {
        return TCacheItem::Make(TSharedCacheTestAccess::ColdVersion(cache, index) & MaxItemVersion, index);
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
        {
        }

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
        {
        }

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
        TSharedCachePageRefImpl<TTraits>& hit) {
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
        TTestSharedCachePageRef hit;
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
        TFixture(ui8 addressBits = 6, ui32 hazards = 8, ui8 reservedAddressBits = 0) {
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(addressBits, 4096, 0, hazards, Capacity));
            if (reservedAddressBits == 0) {
                reservedAddressBits = addressBits;
            }
            UNIT_ASSERT(TryCalculateSharedCacheFootprint(reservedAddressBits, 4096, 0, hazards, ReservedCapacity));
            auto space = TSharedCacheSpace::Create(Capacity, ReservedCapacity, hazards);
            UNIT_ASSERT(space);
            Space = space.Get();
            Cache = TTestSharedCache::Create(std::move(space), Capacity.Limit);
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
    Y_UNIT_TEST(PublicPageFindOrInsertAllocatesOnlyAfterMiss) {
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
    Y_UNIT_TEST(PublicPageFindOrInsertSubscribesAndCompletesFetch) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(61, 62);
        const auto location = NTable::NPage::TPageLocation::FromPageIndex(7, 4096, NTable::NPage::EPage::DataPage, 123);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;

        TTestSharedCachePageRef hit;
        TTestPageFetch fetch;
        TIntrusivePtr<TPageFetchWaiter> first = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, location, EStickyState::None, first, hit, fetch) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT(!hit);
        UNIT_ASSERT(fetch.Location() == location);
        const TPageCacheItem page = fetch.CacheItem();
        UNIT_ASSERT(fetch.Dispatch());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsRequested());

        TTestPageFetch duplicateFetch;
        TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, location, EStickyState::None, second, hit,
                        duplicateFetch) == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT(!duplicateFetch);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);

        UNIT_ASSERT(fetch.MakeReady(MakePageData(page.Index())));
        UNIT_ASSERT(!fetch);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_acquire), 2);
        UNIT_ASSERT_VALUES_EQUAL(ready.load(std::memory_order_relaxed), 2);
        UNIT_ASSERT_VALUES_EQUAL(completedItem.load(std::memory_order_relaxed), page.CacheItem().Raw());

        UNIT_ASSERT(
            fixture.Cache->Find(collection, static_cast<ui64>(location.Offset), hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hit.CacheItem() == page);
        hit.Drop();
    }
    Y_UNIT_TEST(PageBatchOvershootsTheCurrentLimit) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(83, 84);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        for (ui64 offset : { 1, 2 }) {
            requests.push_back({
                .Location = MakePageLocation(offset),
                .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem),
            });
        }
        const ui64 freeBefore = TSharedCacheTestAccess::FreeCount(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->StaticBytes()));
        // The budget never refuses admission: the batch overshoots the current limit and lets maintenance
        // reclaim the excess later. Only the owner of an inserted page keeps its reservation.
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->ReservedBytes(), 2 * (requests[0].Location.Size + NActors::TSharedData::OverheadSize));
        UNIT_ASSERT(TSharedCacheTestAccess::FreeCount(*fixture.Cache) < freeBefore);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);
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
        requests.push_back(
            { .Location = MakePageLocation(1), .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem) });
        requests.push_back(
            { .Location = MakePageLocation(2), .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem) });

        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests, false));
        std::array<TPageCacheItem, 2> pageItems;
        for (ui32 index = 0; index < requests.size(); ++index) {
            auto& request = requests[index];
            UNIT_ASSERT(request.Status == ESharedCacheResultStatus::Inserted);
            pageItems[index] = request.Fetch.CacheItem();
            UNIT_ASSERT(request.Fetch.MakeReady(MakePageData(pageItems[index].Index())));
        }
        const auto stickyState = TSharedCacheTestAccess::HandleState(*fixture.Cache, pageItems[0].Index());
        const auto secondState = TSharedCacheTestAccess::HandleState(*fixture.Cache, pageItems[1].Index());
        UNIT_ASSERT(stickyState.IsSticky());
        UNIT_ASSERT(secondState.IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->StickyOwnedBytes(), 2 * (requests[0].Location.Size + NActors::TSharedData::OverheadSize));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->Stats().RequestedPages, 0);

        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, false));
        TVector<TTestSharedCachePageRequest> regular;
        regular.push_back(
            { .Location = MakePageLocation(3), .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem) });
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, regular));
        UNIT_ASSERT(regular[0].Status == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem ordinary = regular[0].Fetch.CacheItem();
        UNIT_ASSERT(regular[0].Fetch.MakeReady(MakePageData(ordinary.Index())));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsHot());

        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, true));
        TVector<TTestSharedCachePageRequest> hits;
        hits.push_back(
            { .Location = requests[0].Location, .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem) });
        hits.push_back(
            { .Location = regular[0].Location, .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem) });
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, hits));
        UNIT_ASSERT(hits[0].Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(hits[1].Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pageItems[0].Index()).IsSticky());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, ordinary.Index()).IsSticky());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pageItems[1].Index()).IsHot());
    }
    Y_UNIT_TEST(PageBatchOverflowLeavesOutputsUntouched) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(89, 90);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        requests.push_back({
            .Location = MakePageLocation(1, Max<ui64>() / 2),
            .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem),
            .Status = ESharedCacheResultStatus::Hit,
        });
        requests.push_back({
            .Location = MakePageLocation(2, Max<ui64>() / 2),
            .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem),
            .Status = ESharedCacheResultStatus::Pending,
        });

        UNIT_ASSERT(!fixture.Cache->FindOrInsertBatch(collection, requests));
        UNIT_ASSERT(requests[0].Status == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(requests[1].Status == ESharedCacheResultStatus::Pending);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(PageBatchHandleClaimRollsBack) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(87, 88);
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        const ui32 requestCount = static_cast<ui32>(fixture.Capacity.HandleCount());
        for (ui32 index = 0; index < requestCount; ++index) {
            requests.push_back({
                .Location = MakePageLocation(index + 1, 1),
                .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem),
            });
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
    }
    Y_UNIT_TEST(PageBatchCoalescesDuplicateCandidates) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(85, 86);
        const auto location = MakePageLocation(3);
        const ui64 pageBytes = location.Size + NActors::TSharedData::OverheadSize;
        std::atomic<ui32> completed = 0;
        std::atomic<ui32> ready = 0;
        std::atomic<ui64> completedItem = 0;
        TVector<TTestSharedCachePageRequest> requests;
        for (ui32 index = 0; index < 2; ++index) {
            requests.push_back({
                .Location = location,
                .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem),
            });
        }

        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, requests));
        ui32 inserted = 0;
        ui32 owner = 0;
        for (ui32 index = 0; index < requests.size(); ++index) {
            if (requests[index].Status == ESharedCacheResultStatus::Inserted) {
                ++inserted;
                owner = index;
                UNIT_ASSERT(requests[index].Fetch);
            } else {
                UNIT_ASSERT(requests[index].Status == ESharedCacheResultStatus::Pending);
                UNIT_ASSERT(!requests[index].Fetch);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(inserted, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);
        const TPageCacheItem page = requests[owner].Fetch.CacheItem();
        UNIT_ASSERT(requests[owner].Fetch.Dispatch());
        UNIT_ASSERT(requests[owner].Fetch.MakeReady(MakePageData(page.Index())));
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
            requests.push_back({
                .Location = location,
                .Waiter = new TTestPageFetchWaiter(completed, ready, completedItem),
            });
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
            UNIT_ASSERT(request.Status == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(request.Fetch);
            const TPageCacheItem page = request.Fetch.CacheItem();
            UNIT_ASSERT(request.Fetch.MakeReady(MakePageData(page.Index(), request.Location.Size)));
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
            UNIT_ASSERT(request.Status == ESharedCacheResultStatus::Hit);
            UNIT_ASSERT(request.Page);
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
        UNIT_ASSERT(keepRequest[0].Status == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem sticky = keepRequest[0].Fetch.CacheItem();
        UNIT_ASSERT(keepRequest[0].Fetch.MakeReady(MakePageData(sticky.Index(), keepRequest[0].Location.Size)));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, sticky.Index()).IsSticky());

        const TTestSharedCache::TStats kept = fixture.Cache->Stats();
        UNIT_ASSERT_VALUES_EQUAL(kept.RequestedPages, 5);
        UNIT_ASSERT_VALUES_EQUAL(kept.MissPages, 3);
        UNIT_ASSERT_VALUES_EQUAL(kept.StickyAdmissionFailures, 0);

        UNIT_ASSERT(fixture.Cache->UpdateStickyLimit(0));
        TVector<TTestSharedCachePageRequest> failedSticky;
        addRequest(failedSticky, MakePageLocation(4, 4096));
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, failedSticky));
        UNIT_ASSERT(failedSticky[0].Status == ESharedCacheResultStatus::Inserted);
        const TPageCacheItem ordinary = failedSticky[0].Fetch.CacheItem();
        UNIT_ASSERT(failedSticky[0].Fetch.MakeReady(MakePageData(ordinary.Index(), failedSticky[0].Location.Size)));
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, MakePageLocation(1, 4096), EStickyState::None, hotItem,
                        hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(hotItem, MakePageData(hotItem.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 0);

        const TCollectionLocation location{ .Id = TLogoBlobID(95, 96, 97), .BackingSize = 113 };
        TCollectionCacheItem insertedCollection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(location, insertedCollection, collectionRef) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(insertedCollection, MakeCollection(location.Id)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);

        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(insertedCollection, true));
        TPageCacheItem keepItem;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(insertedCollection, MakePageLocation(2, 4096), EStickyState::Sticky,
                        keepItem, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(keepItem, MakePageData(keepItem.Index())));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->HotPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyPages(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ColdPages(), 0);
    }
    Y_UNIT_TEST(KeepColdSpillsToOrdinaryColdAndReheats) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 98);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert({ .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionKeepPages(collection, true));
        UNIT_ASSERT(!fixture.Cache->SetCollectionStickyPages(collection, true));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * pageBytes));
        std::array<TPageCacheItem, 3> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestSharedCachePageRef hit;
            UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, MakePageLocation(index + 1), EStickyState::None,
                            pages[index], hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[index]));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[0].Index()).IsCold());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[1].Index()).IsCold());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[2].Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);

        TTestSharedCachePageRef hit;
        UNIT_ASSERT(fixture.Cache->Find(collection, 3, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[2].Index()).IsHot());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
        hit.Drop();

        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages[2]));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[2].Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);

        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(0));
        UNIT_ASSERT(fixture.Cache->RunMaintenance());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, pages[2].Index()).IsCold());
    }
    Y_UNIT_TEST(KeepColdPublishesOnLastReleaseAndWithdrawsOnModeChange) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 99);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert({ .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * pageBytes));
        UNIT_ASSERT(fixture.Cache->SetCollectionKeepPages(collection, true));

        TPageCacheItem page;
        TTestSharedCachePageRef hit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, MakePageLocation(1), EStickyState::None, page, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
        UNIT_ASSERT(fixture.Cache->Find(collection, 1, hit) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        hit.Drop();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsKeepCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), pageBytes);

        UNIT_ASSERT(fixture.Cache->SetCollectionKeepPages(collection, false));
        UNIT_ASSERT(fixture.Cache->RunMaintenance());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->KeepColdOwnedBytes(), 0);
    }
    Y_UNIT_TEST(KeepColdLastReleaseDemotesWithdrawnEntriesOnly) {
        TFixture fixture;
        const TLogoBlobID id(95, 96, 105);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert({ .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionKeepPages(collection, true));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(2 * pageBytes));
        std::array<TPageCacheItem, 2> pages;
        std::array<ui32, 2> queuedVersions;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestSharedCachePageRef unused;
            UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, MakePageLocation(index + 1), EStickyState::None,
                            pages[index], unused) == ESharedCacheResultStatus::Inserted);
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
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert({ .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionKeepPages(collection, true));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(pageBytes));
        TPageCacheItem page;
        TTestSharedCachePageRef unused;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, MakePageLocation(1), EStickyState::None, page, unused) ==
                    ESharedCacheResultStatus::Inserted);
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
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert({ .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionKeepPages(collection, true));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(pageBytes));
        TPageCacheItem page;
        TTestSharedCachePageRef hit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, MakePageLocation(1), EStickyState::None, page, hit) ==
                    ESharedCacheResultStatus::Inserted);
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
        TFixture fixture;
        const TLogoBlobID id(95, 96, 101);
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionRef;
        UNIT_ASSERT(
            fixture.Cache->FindOrInsert({ .Id = id }, collection, collectionRef) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionKeepPages(collection, true));
        UNIT_ASSERT(fixture.Cache->UpdateKeepColdLimit(fixture.Cache->HardLimit()));

        TPageCacheItem oldest;
        for (ui32 index = 0; index <= fixture.Capacity.KeepColdSlotCount(); ++index) {
            TPageCacheItem page;
            TTestSharedCachePageRef hit;
            UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, MakePageLocation(index + 1), EStickyState::None,
                            page, hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
            if (index == 0) {
                oldest = page;
            }
        }
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, oldest.Index()).IsCold());
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Cache->KeepColdOwnedBytes(), fixture.Capacity.KeepColdSlotCount() * pageBytes);
    }
    Y_UNIT_TEST(PageFetchWaiterAcquiresReadyPageBeforeFinalizerDrops) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(81, 82);
        const auto location = MakePageLocation(17);
        TTestSharedCachePageRef completedPage;
        TIntrusivePtr<TAcquiringPageFetchWaiter> waiter = new TAcquiringPageFetchWaiter(*fixture.Cache, completedPage);
        TTestSharedCachePageRef hit;
        TTestPageFetch fetch;

        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, location, EStickyState::None, waiter, hit, fetch) ==
                    ESharedCacheResultStatus::Inserted);
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, location, EStickyState::None, first, hit, fetch) ==
                    ESharedCacheResultStatus::Inserted);
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
            subscribeStatus =
                cache->FindOrInsert(collection, location, EStickyState::None, second, subscriberHit, subscriberFetch);
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
                statuses[thread] =
                    fixture.Cache->FindOrInsert(collection, location, EStickyState::None, waiter, hit, fetches[thread]);
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
            UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, location, EStickyState::None, first, hit, fetch) ==
                        ESharedCacheResultStatus::Inserted);
            page = fetch.CacheItem();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), pageBytes);

            TTestPageFetch duplicateFetch;
            TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
            UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, location, EStickyState::None, second, hit,
                            duplicateFetch) == ESharedCacheResultStatus::Pending);
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

        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, firstLocation, EStickyState::None, firstWaiter, hit,
                        first) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, secondLocation, EStickyState::None, secondWaiter, hit,
                        second) == ESharedCacheResultStatus::Inserted);
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(location, inserted, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakeCollection(location.Id)));
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(location, inserted, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(inserted, MakeCollection(location.Id)));
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

        TCollectionCacheItem duplicate;
        TTestSharedCacheCollectionRef reattached;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, firstLocation, duplicate, reattached) ==
                    ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(!duplicate);
        UNIT_ASSERT(reattached.CacheItem() == first);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, first.Index()).IsSticky());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), first.Index());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), second.Index());

        reattached.Drop();
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, first));
        UNIT_ASSERT(fixture.Cache->DetachCollection(fixture.Registry, second));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionListHead(fixture.Registry), 0);
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

        held.Drop();
        const THandleState free = TSharedCacheTestAccess::HandleState(*fixture.Cache, inserted.Index());
        UNIT_ASSERT(free.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(free.Version(), AdvanceItemVersion(inserted.Version()));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::CollectionValue(*fixture.Cache, inserted.Index()), nullptr);
    }
    Y_UNIT_TEST(CollectionLogicalDeleteUnstickysPagesAndRetainsRecord) {
        TFixture fixture;
        const TCollectionLocation location{ .Id = TLogoBlobID(15, 16, 17) };
        TCollectionCacheItem collection;
        TTestSharedCacheCollectionRef collectionHit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, collection, collectionHit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(location.Id)));
        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, true));

        std::array<TPageCacheItem, 2> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            TTestSharedCachePageRef pageHit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(index + 1), EStickyState::Sticky,
                            pages[index], pageHit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(pages[index], MakePageData(pages[index].Index())));
        }
        UNIT_ASSERT_VALUES_UNEQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, false));
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
            UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplit(*fixture.Cache));
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
            UNIT_ASSERT(TSharedCacheTestAccess::ActivateBucketSplit(*fixture.Cache));
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
    Y_UNIT_TEST(FinalDrainHot) {
        TFixture fixture(6, 8, 7);

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth));
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
                    const TRingView& ring = spaceOp.Hot(level);
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink));
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth));
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink));
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, canceled));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), growthDelta);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT(TSharedCacheTestAccess::AbandonTransition(*fixture.Cache, canceled));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);

        TTransition shrink;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.ReservedCapacity.StaticBytes);
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, shrink));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 6));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, shrink);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticBytes(), fixture.Capacity.StaticBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StaticDeltaBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Capacity.StaticBytes);
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
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CurrentLimit(), 0);
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
    Y_UNIT_TEST(ReferencedPagesRelocateBeforeShrink) {
        constexpr ui32 HazardCount = 8;
        TFixture fixture(6, HazardCount, 6);
        TSharedCacheCapacity target;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(5, 4096, 0, HazardCount, target));

        const TCollectionCacheItem collection = MakeCollectionCacheItem(53, 54);
        TVector<TSharedCacheKey> keys;
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < target.HandleCount(); ++offset) {
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
                *fixture.Cache, TCacheItem::Make(coldVersion & MaxItemVersion, page.Index())));
            UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsFree());
        }

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
            auto binding = cache->BindThreadHazard(0);
            relocated = cache->RunMaintenance();
        });
        gate.Slots[0].Wait();

        const THandleState forwarding = TSharedCacheTestAccess::HandleState(*fixture.Cache, hotSource.Index());
        UNIT_ASSERT(forwarding.IsReplacing());
        const TCacheItem hotReplacement =
            TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(*fixture.Cache, hotSource.Index())).WithoutFrozen();
        UNIT_ASSERT(hotReplacement.Index() >= 2 && hotReplacement.Index() < target.HandleCount());

        TTestSharedCachePageRef acquiredHot;
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
        TTestSharedCachePageRef existing;
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

        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterReplacedPublished, coldSource.CacheItem());
        bool coldRelocated = false;
        TGateThread coldRelocator(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
            coldRelocated = cache->RunMaintenance();
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, coldSource.Index()).IsReplaced());
        const TCacheItem coldReplacement =
            TCacheItem::FromRaw(TSharedCacheTestAccess::HandleNext(*fixture.Cache, coldSource.Index())).WithoutFrozen();
        UNIT_ASSERT(coldReplacement.Index() >= 2 && coldReplacement.Index() < target.HandleCount());
        TTestSharedCachePageRef acquiredCold;
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
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, collectionLocation, collection, collectionRef) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(collectionLocation.Id)));

        TVector<TSharedCacheKey> regularKeys;
        TVector<TPageCacheItem> regularPages;
        for (ui64 offset = 0; offset < target.HandleCount() - 3; ++offset) {
            regularKeys.push_back(TSharedCacheKey::Page(collection, offset));
            regularPages.push_back(InsertReadyPage(*fixture.Cache, regularKeys.back()));
        }
        UNIT_ASSERT_VALUES_EQUAL(regularPages.back().Index(), target.HandleCount() - 1);

        auto insertSticky = [&](ui64 offset) {
            TPageCacheItem page;
            TTestSharedCachePageRef hit;
            UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(offset), EStickyState::Sticky,
                            page, hit) == ESharedCacheResultStatus::Inserted);
            UNIT_ASSERT(fixture.Cache->MakeReady(page, MakePageData(page.Index())));
            return page;
        };

        const ui64 firstStickyOffset = target.HandleCount();
        const ui64 secondStickyOffset = firstStickyOffset + 1;
        const TPageCacheItem firstSticky = insertSticky(firstStickyOffset);
        const TPageCacheItem secondSticky = insertSticky(secondStickyOffset);
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
                *fixture.Cache, TCacheItem::Make(coldVersion & MaxItemVersion, page.Index())));
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
            UNIT_ASSERT(state.IsUnstickyField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }

        UNIT_ASSERT(fixture.Cache->RunMaintenance()); // relocate and re-Sticky the first cut page
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, firstSticky.Index()).IsFree());
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
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, secondSticky.Index()).IsFree());
        auto secondFound = FindPage(*fixture.Cache, TSharedCacheKey::Page(collection, secondStickyOffset));
        UNIT_ASSERT(secondFound.Status == ESharedCacheResultStatus::Hit);
        const TPageCacheItem secondReplacement = secondFound.Ref.CacheItem();
        secondFound.Ref.Drop();
        UNIT_ASSERT(secondReplacement.Index() < target.HandleCount());
        const THandleState secondReplacementState =
            TSharedCacheTestAccess::HandleState(*fixture.Cache, secondReplacement.Index());
        UNIT_ASSERT(secondReplacementState.IsSticky());
        UNIT_ASSERT(secondReplacementState.IsStickyField());
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
            UNIT_ASSERT_C(fixture.Cache->RunMaintenance(), TStringBuilder() << "shrink completion stalled at " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::HandleCount(*fixture.Cache), target.HandleCount());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), residentBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyBytes(), keepBytes);
    }
    Y_UNIT_TEST(CurrentLimitReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const ui64 limit = fixture.Cache->StaticBytes() + 2 * bytes;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(limit));

        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, 0));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 2 * bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), limit);

        // Nothing is resident, so reclamation cannot make room: admission overshoots the current limit instead
        // of refusing the page the client asked for, and marks the soft limit as violated.
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 2 * bytes + 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), limit + 1);

        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(limit - bytes));
        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, 1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), limit + 2);

        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, 2 * bytes + 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(ReservedPageMakeReadyTransfersReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->StaticBytes() + bytes));

        const TCollectionCacheItem collection = MakeCollectionCacheItem(36, 37);
        TPageCacheItem cacheItem;
        TTestSharedCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::None, cacheItem,
                        hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT(fixture.Cache->MakeReady(cacheItem, MakePageData(cacheItem.Index())));

        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes() + bytes);

        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, cacheItem));
        UNIT_ASSERT(fixture.Cache->ReclaimCold());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(ReservationReclaimsCold) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(47, 48);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, 0));
        const ui64 bytes = fixture.Cache->OverallUsage() - fixture.Cache->StaticBytes();
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->OverallUsage()));

        UNIT_ASSERT(TSharedCacheTestAccess::TryReserve(*fixture.Cache, bytes));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->CurrentLimit());

        TSharedCacheTestAccess::ReleaseReservation(*fixture.Cache, bytes);
    }
    Y_UNIT_TEST(CollectionAdmissionIsTemporarilyUnaccounted) {
        TFixture fixture;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->StaticBytes()));
        const TCollectionLocation location{ .Id = TLogoBlobID(38, 39, 40) };

        TCollectionCacheItem inserted;
        TTestSharedCacheCollectionRef hit;
        UNIT_ASSERT(fixture.Cache->FindOrInsert(fixture.Registry, location, inserted, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(inserted);
        UNIT_ASSERT(!hit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(), 0);
        UNIT_ASSERT(fixture.Cache->MakeReady(fixture.Registry, inserted, MakeCollection(location.Id)));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ResidentBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->CollectionBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->StaticBytes());
    }
    Y_UNIT_TEST(FailedPageReadyRefundsReservation) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(41, 42);

        TPageCacheItem cacheItem;
        TTestSharedCachePageRef hit;
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

        TTestSharedCachePageRef missing;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, missing) == ESharedCacheResultStatus::Miss);
    }
    Y_UNIT_TEST(FailedStickyPageReadyRefundsOwnership) {
        TFixture fixture;
        const ui64 bytes = 4096 + NActors::TSharedData::OverheadSize;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(42, 43);
        TPageCacheItem cacheItem;
        TTestSharedCachePageRef hit;
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
        const ui64 currentLimit = fixture.Cache->StaticBytes() + 15 * pageBytes;
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(currentLimit));
        UNIT_ASSERT(!fixture.Cache->RunMaintenance());

        ui32 steps = 0;
        while (steps < 10000 && fixture.Cache->OverallUsage() > currentLimit) {
            fixture.Cache->EnforceCurrentLimit();
            ++steps;
        }
        UNIT_ASSERT_C(steps < 10000, "policy did not converge");
        UNIT_ASSERT_C(fixture.Cache->OverallUsage() < currentLimit,
            TStringBuilder() << "usage=" << fixture.Cache->OverallUsage() << " limit=" << currentLimit
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

        const ui64 currentLimit = fixture.Cache->OverallUsage();
        const ui64 payloadLimit = currentLimit - fixture.Cache->StaticBytes();
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(currentLimit));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->SoftLimit(), currentLimit - (payloadLimit + 9) / 10);

        ui32 steps = 0;
        while (steps < 10000 && fixture.Cache->OverallUsage() > fixture.Cache->SoftLimit()) {
            fixture.Cache->EnforceCurrentLimit();
            ++steps;
        }
        UNIT_ASSERT_C(steps < 10000, "policy did not reach the soft limit");
        // The reclaim target is "usage fits below the soft limit", so landing exactly on it is enough.
        UNIT_ASSERT(fixture.Cache->OverallUsage() <= fixture.Cache->SoftLimit());

        const ui64 usage = fixture.Cache->OverallUsage();
        fixture.Cache->EnforceCurrentLimit();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), usage);
    }
    Y_UNIT_TEST(CurrentLimitShrinksHotAndReclaimsCold) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(34, 35);
        TVector<TPageCacheItem> pages;
        for (ui64 offset = 0; offset < 30; ++offset) {
            pages.push_back(InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset)));
        }
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, pages.front()));

        const ui64 usage = fixture.Cache->OverallUsage();
        const ui64 coldBytes = fixture.Cache->ColdBytes();
        const ui64 effectiveHotSlots = TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache);
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(usage - 1));
        UNIT_ASSERT(coldBytes < (usage - 1) / 5);

        UNIT_ASSERT(fixture.Cache->EnforceCurrentLimit());
        UNIT_ASSERT(TSharedCacheTestAccess::EffectiveHotSlots(*fixture.Cache) < effectiveHotSlots);
        UNIT_ASSERT(fixture.Cache->OverallUsage() < usage);
    }
    Y_UNIT_TEST(LateHotExchangeAfterLogicalShrinkIsRecovered) {
        TFixture fixture;
        const TCollectionCacheItem collection = MakeCollectionCacheItem(43, 44);
        for (ui64 offset = 0; offset < 6; ++offset) {
            InsertReadyPage(*fixture.Cache, TSharedCacheKey::Page(collection, offset));
        }

        TPageCacheItem cacheItem;
        TTestSharedCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(6), EStickyState::None, cacheItem,
                        hit) == ESharedCacheResultStatus::Inserted);

        TSharedCacheGate gate;
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeHotExchange, cacheItem.CacheItem());

        bool ready = false;
        TGateThread readyThread(gate, [&] {
            auto binding = fixture.Cache->BindThreadHazard(0);
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
        TTestSharedCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, collection, MakePageLocation(0), EStickyState::None, insertedItem,
                        hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(insertedItem, MakePageData(insertedItem.Index())));
        TTestSharedCachePageRef page;
        UNIT_ASSERT(fixture.Cache->Find(collection, 0, page) == ESharedCacheResultStatus::Hit);
        UNIT_ASSERT(TSharedCacheTestAccess::EvictFromHot(*fixture.Cache, page.CacheItem()));
        UNIT_ASSERT(fixture.Cache->UpdateCurrentLimit(fixture.Cache->StaticBytes()));

        for (ui32 step = 0; step < 1000 && fixture.Cache->EnforceCurrentLimit(); ++step) {
        }
        UNIT_ASSERT(fixture.Cache->OverallUsage() > fixture.Cache->CurrentLimit());
        const THandleState referenced = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.CacheItem().Index());
        UNIT_ASSERT(referenced.IsCold());
        UNIT_ASSERT_VALUES_EQUAL(referenced.Refs(), 1);

        page.Drop();
        for (ui32 step = 0; step < 1000 && fixture.Cache->OverallUsage() > fixture.Cache->CurrentLimit(); ++step) {
            UNIT_ASSERT_C(fixture.Cache->EnforceCurrentLimit(), TStringBuilder() << "policy stalled at step " << step);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->OverallUsage(), fixture.Cache->CurrentLimit());
    }
    Y_UNIT_TEST(ConcurrentGrowthPublicationPoints) {
        TFixture fixture(6, 8, 7);
        const auto keys = MakeResizeKeys(*fixture.Space, MakeCollectionCacheItem(8, 12), 600);
        const TPageCacheItem low = InsertReadyPage(*fixture.Cache, keys[0]);
        const TPageCacheItem high = InsertReadyPage(*fixture.Cache, keys[2]);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth));
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
            auto binding = cache->BindThreadHazard(0);
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
        UNIT_ASSERT(!firstActivation);

        bool activated = false;
        TGateThread activator(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
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
            auto binding = cache->BindThreadHazard(0);
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
            auto binding = cache->BindThreadHazard(0);
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
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);

        TTransition shrink;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.Capacity, shrink));
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
            auto binding = cache->BindThreadHazard(0);
            closed = TSharedCacheTestAccess::AdvanceBucketResize(*cache);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(TSharedCacheTestAccess::BucketHead(*fixture.Cache, highBucket).IsClosedBucketHead());
        AssertResizeMarker(*fixture.Cache, EHandleState::BucketSplit);
        AssertPageHit(*fixture.Cache, keys[0], low);
        AssertPageHit(*fixture.Cache, keys[2], high);

        const TPageCacheItem inserted = InsertReadyPage(*fixture.Cache, keys[1]);
        TPageCacheItem duplicate;
        TTestSharedCachePageRef existing;
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
            auto binding = cache->BindThreadHazard(0);
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
                auto binding = cache->BindThreadHazard(reader);
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
                    TTestSharedCachePageRef ref;
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
                auto binding = cache->BindThreadHazard(ReaderCount + writer);
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
                    TTestSharedCachePageRef hit;
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
                    TTestSharedCachePageRef hit;
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
        UNIT_ASSERT(fixture.Cache->MakeReady(collection, MakeCollection(CollectionId(collectionKey), BackingSize)));
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
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

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
        UNIT_ASSERT(secondState.IsUnstickyField());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, second.Index()), 0);

        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*fixture.Cache, first, first.Index()));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        const THandleState firstState = TSharedCacheTestAccess::HandleState(*fixture.Cache, first.Index());
        UNIT_ASSERT(firstState.IsSticky());
        UNIT_ASSERT(firstState.IsUnstickyField());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, first.Index()), 0);
    }
    Y_UNIT_TEST(ReadyPageCanBecomeStickyAndDetach) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(16, 17, 18));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

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

        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, false));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsHot());
    }
    Y_UNIT_TEST(StickyPromotionLosesToCollectionDisable) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(19, 20, 21));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

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
            auto binding = cache->BindThreadHazard(0);
            promoted = TSharedCacheTestAccess::MakePageSticky(*cache, page);
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, false));
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
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        std::array<TPageCacheItem, 2> pages;
        for (ui32 index = 0; index < pages.size(); ++index) {
            const auto key = TSharedCacheKey::Page(collection, index);
            pages[index] =
                AllocatePage(*fixture.Cache, key, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
            UNIT_ASSERT(pages[index]);
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
    Y_UNIT_TEST(ConcurrentStickyAdmissionRespectsLimitAndRollback) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(13, 14, 15));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT(fixture.Cache->UpdateStickyLimit(pageBytes));
        std::array<TPageCacheItem, 2> pages;
        std::atomic<bool> start = false;
        std::array<std::thread, 2> workers;
        for (ui32 index = 0; index < workers.size(); ++index) {
            workers[index] = std::thread([&, index] {
                auto binding = fixture.Cache->BindThreadHazard(index);
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
        UNIT_ASSERT_VALUES_EQUAL(stickyCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), pageBytes);
        for (TPageCacheItem page : pages) {
            TSharedCacheTestAccess::DiscardCandidate(*fixture.Cache, page);
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 0);
    }
    Y_UNIT_TEST(CollectionStickyDisableCoversReadyAndCompletingPages) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(12, 13, 14));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const TPageCacheItem ready = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        const TPageCacheItem completing = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(ready && completing);
        const ui64 pageBytes = 4096 + NActors::TSharedData::OverheadSize;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->StickyOwnedBytes(), 2 * pageBytes);
        UNIT_ASSERT(fixture.Cache->MakeReady(ready, MakePageData(ready.Index())));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageLinked, completing.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool completed = false;
        TGateThread completer(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
            completed = cache->MakeReady(completing, MakePageData(completing.Index()));
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, false));
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

        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, true));
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
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const TPageCacheItem existing = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 1), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        const TPageCacheItem pending = AllocatePage(*fixture.Cache, TSharedCacheKey::Page(collection, 2), 4096,
            NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing && pending);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::BeforeStickyPageListLink, pending.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool ready = false;
        TGateThread completer(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
            ready = cache->MakeReady(pending, MakePageData(pending.Index()));
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(fixture.Cache->SetCollectionStickyPages(collection, false));
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
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto existingKey = TSharedCacheKey::Page(collection, 1);
        const TPageCacheItem existing =
            AllocatePage(*fixture.Cache, existingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        const auto pendingKey = TSharedCacheKey::Page(collection, 2);
        const TPageCacheItem pending =
            AllocatePage(*fixture.Cache, pendingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(pending);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageLinked, pending.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool ready = false;
        TGateThread completer(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
            ready = cache->MakeReady(pending, MakePageData(pending.Index()));
        });
        gate.Slots[0].Wait();

        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), pending.Index());
        const THandleState pendingState = TSharedCacheTestAccess::HandleState(*fixture.Cache, pending.Index());
        UNIT_ASSERT(pendingState.IsCompleting());
        UNIT_ASSERT(pendingState.IsStickyField());
        const THandleState existingState = TSharedCacheTestAccess::HandleState(*fixture.Cache, existing.Index());
        UNIT_ASSERT(existingState.IsSticky());
        UNIT_ASSERT(existingState.IsUnstickyField());
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, existing.Index()), 0);

        gate.Slots[0].Release();
        completer.Join();
        UNIT_ASSERT(ready);
        UNIT_ASSERT(TSharedCacheTestAccess::UnstickyCutPages(*cache, existing, 0));
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);
        for (TPageCacheItem page : { existing, pending }) {
            const THandleState state = TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index());
            UNIT_ASSERT(state.IsSticky());
            UNIT_ASSERT(state.IsUnstickyField());
            UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::NextInOwner(*fixture.Cache, page.Index()), 0);
        }
    }
    Y_UNIT_TEST(StickyListMergePreservesConcurrentInsertions) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(16, 17, 18));
        const TCollectionCacheItem collection = AllocateCollection(*fixture.Cache, collectionKey);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));

        const auto existingKey = TSharedCacheKey::Page(collection, 1);
        const TPageCacheItem existing =
            AllocatePage(*fixture.Cache, existingKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(existing);
        UNIT_ASSERT(fixture.Cache->MakeReady(existing, MakePageData(existing.Index())));

        const auto beforeExchangeKey = TSharedCacheKey::Page(collection, 2);
        const TPageCacheItem beforeExchange = AllocatePage(
            *fixture.Cache, beforeExchangeKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(beforeExchange);
        const auto afterExchangeKey = TSharedCacheKey::Page(collection, 3);
        const TPageCacheItem afterExchange = AllocatePage(
            *fixture.Cache, afterExchangeKey, 4096, NTable::NPage::EPage::DataPage, 0, EStickyState::Sticky);
        UNIT_ASSERT(afterExchange);

        TSharedCacheGate gate;
        gate.Slots[0].Arm(ESharedCacheHookPoint::AfterStickyPageListDetached, collection.CacheItem());
        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterStickyPageListHeadExchanged, collection.CacheItem());
        TSharedCacheHookGuard hookGuard(*fixture.Cache, gate.Hooks);
        auto cache = fixture.Cache;
        bool cut = false;
        TGateThread cutter(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
            cut = TSharedCacheTestAccess::UnstickyCutPages(*cache, existing, fixture.Capacity.HandleCount());
        });
        gate.Slots[0].Wait();
        UNIT_ASSERT_VALUES_EQUAL(TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), 0);

        UNIT_ASSERT(cache->MakeReady(beforeExchange, MakePageData(beforeExchange.Index())));
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), beforeExchange.Index());

        gate.Slots[0].Release();
        gate.Slots[1].Wait();
        UNIT_ASSERT_VALUES_EQUAL(
            TSharedCacheTestAccess::StickyPageListHead(*fixture.Cache, collection), existing.Index());

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
        fixture.MainHazard = {};
        applicationOwner.Reset();
    }
    Y_UNIT_TEST(UnboundDropAbandonsItemForFinalSweep) {
        TFixture fixture;
        const auto key = TSharedCacheKey::Page(MakeCollectionCacheItem(1, 2), 104);
        const TPageCacheItem page = InsertReadyPage(*fixture.Cache, key);
        auto found = FindPage(*fixture.Cache, key);
        UNIT_ASSERT(found.Status == ESharedCacheResultStatus::Hit);

        fixture.MainHazard = {};
        found.Ref.Drop();

        fixture.MainHazard = fixture.Cache->BindThreadHazard(TSharedCacheTestAccess::HazardCount(*fixture.Cache) - 1);
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
            TSharedCacheTestAccess::CompleteTombstone(*fixture.Cache, TCacheItem::Make(pageVersion, page.Index()), key);
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
        TTestSharedCachePageRef hit;
        TGateThread writer(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
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
        TTestSharedCachePageRef hit;
        TGateThread writer(gate, [&] {
            auto binding = cache->BindThreadHazard(0);
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
            auto binding = cache->BindThreadHazard(0);
            reclaimed.store(cache->ReclaimCold(), std::memory_order_release);
        });
        gate.Slots[0].Wait();

        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterTombstoneHelperClaim);
        ESharedCacheResultStatus status = ESharedCacheResultStatus::Miss;
        TPageCacheItem inserted;
        TTestSharedCachePageRef hit;
        TGateThread writer(gate, [&] {
            auto binding = cache->BindThreadHazard(1);
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
            auto binding = cache->BindThreadHazard(0);
            reclaimed.store(cache->ReclaimCold(), std::memory_order_release);
        });
        gate.Slots[0].Wait();

        gate.Slots[1].Arm(ESharedCacheHookPoint::AfterTombstoneHelperClaim, target.CacheItem());
        TGateThread helper(gate, [&] {
            auto binding = cache->BindThreadHazard(1);
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
            auto binding = cache->BindThreadHazard(0);
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
    Y_UNIT_TEST(PageAndCollectionShareTable) {
        TFixture fixture;
        const auto collectionKey = TSharedCacheKey::Collection(TLogoBlobID(1, 2, 3));
        const auto pageKey = TSharedCacheKey::Page(MakeCollectionCacheItem(7, 9), 42);
        const auto collection = AllocateCollection(*fixture.Cache, collectionKey);
        const auto page = AllocatePage(*fixture.Cache, pageKey, 4096);
        UNIT_ASSERT(collection);
        UNIT_ASSERT(page);
        UNIT_ASSERT(
            fixture.Cache->MakeReady(fixture.Registry, collection, MakeCollection(CollectionId(collectionKey))));
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
        auto sharedData = foundPage.Ref.ShareData();
        UNIT_ASSERT_VALUES_EQUAL(sharedData.data(), foundPage.Ref.data());
        UNIT_ASSERT_VALUES_EQUAL(sharedData.size(), foundPage.Ref.size());

        const char* pageData = foundPage.Ref.data();
        const TCacheCollection* collectionValue = &foundCollection.Ref.GetCollection();
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
        TTestSharedCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*fixture.Cache, PageCollection(key), MakePageLocation(key.Word(1)),
                        EStickyState::None, insertedItem, hit) == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fixture.Cache->MakeReady(insertedItem, MakePageData(insertedItem.Index())));
        TTestSharedCachePageRef page;
        UNIT_ASSERT(fixture.Cache->Find(PageCollection(key), key.Word(1), page) == ESharedCacheResultStatus::Hit);
        const char* data = page.data();
        auto moved = std::move(page);
        UNIT_ASSERT(!page);

        TTransition growth;
        UNIT_ASSERT(TSharedCacheTestAccess::PrepareTransition(*fixture.Cache, fixture.ReservedCapacity, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::PublishMigrationView(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::TryDrainTransition(*fixture.Cache, growth));
        UNIT_ASSERT(TSharedCacheTestAccess::BeginBucketResize(*fixture.Cache, 7));
        CompleteBucketResize(*fixture.Cache, *fixture.Space);
        CommitTransition(*fixture.Cache, growth);

        UNIT_ASSERT_VALUES_EQUAL(moved.data(), data);
        UNIT_ASSERT_VALUES_EQUAL(moved.size(), 4096);
        UNIT_ASSERT(moved.GetType() == NTable::NPage::EPage::DataPage);
        auto sharedData = moved.ShareData();
        UNIT_ASSERT_VALUES_EQUAL(sharedData.data(), data);
        UNIT_ASSERT_VALUES_EQUAL(sharedData.size(), 4096);
    }
    Y_UNIT_TEST(SharedPageRefAdoptsCachePage) {
        TSharedCacheCapacity capacity;
        UNIT_ASSERT(TryCalculateSharedCacheFootprint(11, 4096, 0, 1, capacity));
        auto space = TSharedCacheSpace::Create(capacity, capacity, 1);
        UNIT_ASSERT(space);
        auto cache = TSharedCache::Create(std::move(space), capacity.Limit);
        UNIT_ASSERT(cache);
        auto binding = cache->BindCurrentThreadHazard();

        const TCollectionCacheItem collection = MakeCollectionCacheItem(61, 62);
        TPageCacheItem inserted;
        TSharedCachePageRef hit;
        UNIT_ASSERT(FindOrInsertPage(*cache, collection, MakePageLocation(0), EStickyState::None, inserted, hit) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(cache->MakeReady(inserted, MakePageData(inserted.Index())));

        TSharedCachePageRef page;
        UNIT_ASSERT(cache->Find(collection, 0, page) == ESharedCacheResultStatus::Hit);
        TSharedPageRef shared = MakeSharedPageRef(std::move(page));
        UNIT_ASSERT(!page);
        UNIT_ASSERT(shared.IsUsed());
        UNIT_ASSERT_VALUES_EQUAL(shared.GetType(), NTable::NPage::EPage::DataPage);
        TPinnedPageRef pinned(shared);
        UNIT_ASSERT_VALUES_EQUAL(pinned.GetData().size(), 4096);
        UNIT_ASSERT_VALUES_EQUAL(pinned.GetData().data()[0], static_cast<char>(inserted.Index()));
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
                auto hazard = fixture.Cache->BindThreadHazard(thread);
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

        TSharedCacheTestAccess::CompleteTombstone(
            *fixture.Cache, tombstones[TombstoneCount - 1], keys[TombstoneCount - 1]);

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
            TSharedCacheTestAccess::CompleteTombstone(*fixture.Cache, tombstones[index], keys[index]);
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

        TSharedCacheTestAccess::CompleteTombstone(*fixture.Cache, tombstoneItem, keys[1]);
        state = TSharedCacheTestAccess::HandleState(*fixture.Cache, indices[1], std::memory_order_relaxed);
        UNIT_ASSERT(state.IsFree());
        UNIT_ASSERT_VALUES_EQUAL(state.Version(), AdvanceItemVersion(tombstoneItem.Version()));
        UNIT_ASSERT_VALUES_EQUAL(state.Refs(), 0);
    }
}

} // namespace NKikimr::NSharedCache
