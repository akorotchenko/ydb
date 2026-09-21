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
using TTestPageFetchToken = TPageFetchTokenImpl<TTestTraits>;
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
    static TPageCacheItem AllocatePage(TTestSharedCache& cache, TCollectionCacheItem collection, ui64 offset, ui64 size,
        NTable::NPage::EPage type, ui32 crc32, EKeepState keep) noexcept {
        return cache.AllocatePage(collection, offset, size, type, crc32, keep);
    }

    static TCollectionCacheItem AllocateCollection(
        TTestSharedCache& cache, const TLogoBlobID& id, ui64 bytes) noexcept {
        return cache.AllocateCollection(id, bytes);
    }

    template <class TTraits>
    static ESharedCacheResultStatus FindOrInsert(TSharedCacheImpl<TTraits>& cache, TCollectionCacheItem collection,
        const NTable::NPage::TPageLocation& page, EKeepState keep, TPageCacheItem& inserted,
        TSharedCachePageRefImpl<TTraits>& hit) noexcept {
        return cache.FindOrInsert(collection, page, keep, inserted, hit);
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
        return cache.FinalDrain(transition);
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

    static TCollection* CollectionValue(TTestSharedCache& cache, ui32 index) noexcept {
        return WithHandle(cache, index, [](THandle& handle) {
            return handle.Body.Collection;
        });
    }

    static ui32 KeepPageListHead(TTestSharedCache& cache, TCollectionCacheItem collection) noexcept {
        TCollection* value = CollectionValue(cache, collection.Index());
        Y_ABORT_UNLESS(value);
        return value->KeepPageListHead.load(std::memory_order_acquire);
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

    static bool UnkeepCutPages(TTestSharedCache& cache, TPageCacheItem page, ui64 allocationLimit) {
        auto operation = cache.BeginOperation();
        return cache.UnkeepCutPages(operation, page.CacheItem(), allocationLimit);
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

    THolder<TCollection> MakeCollection(const TLogoBlobID& id, size_t backingSize = 0) {
        return MakeHolder<TCollection>(
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
        EKeepState keep = EKeepState::None) {
        return TSharedCacheTestAccess::AllocatePage(cache, PageCollection(key), key.Word(1), size, type, crc32, keep);
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
        const NTable::NPage::TPageLocation& page, EKeepState keep, TPageCacheItem& inserted,
        TSharedCachePageRefImpl<TTraits>& hit) {
        return TSharedCacheTestAccess::FindOrInsert(cache, collection, page, keep, inserted, hit);
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
        UNIT_ASSERT(FindOrInsertPage(cache, PageCollection(key), MakePageLocation(key.Word(1)), EKeepState::None, page,
                        hit) == ESharedCacheResultStatus::Inserted);
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
        TIntrusivePtr<TPageFetch> fetch = new TPageFetch;
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
        TIntrusivePtr<TPageFetch> fetch = new TPageFetch;
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
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EKeepState::None,
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
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EKeepState::None,
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
                        MakePageLocation(key.Word(1), 4096, NTable::NPage::EPage::DataPage, 123), EKeepState::None,
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
        TTestPageFetchToken fetch;
        TIntrusivePtr<TPageFetchWaiter> first = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, location, EKeepState::None, first, hit, fetch) ==
                    ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT(!hit);
        UNIT_ASSERT(fetch.Location() == location);
        const TPageCacheItem page = fetch.CacheItem();
        UNIT_ASSERT(fetch.Dispatch());
        UNIT_ASSERT(TSharedCacheTestAccess::HandleState(*fixture.Cache, page.Index()).IsRequested());

        TTestPageFetchToken duplicateFetch;
        TIntrusivePtr<TPageFetchWaiter> second = new TTestPageFetchWaiter(completed, ready, completedItem);
        UNIT_ASSERT(fixture.Cache->FindOrInsert(collection, location, EKeepState::None, second, hit, duplicateFetch) ==
                    ESharedCacheResultStatus::Pending);
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
        UNIT_ASSERT(fixture.Cache->FindOrInsertBatch(collection, EKeepState::None, requests));
        UNIT_ASSERT(requests[0].Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT(requests[1].Status == ESharedCacheResultStatus::Inserted);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Cache->ReservedBytes(),
            2 * (requests[0].Location.Size + NActors::TSharedData::OverheadSize));
        UNIT_ASSERT(TSharedCacheTestAccess::FreeCount(*fixture.Cache) < freeBefore);
        UNIT_ASSERT_VALUES_EQUAL(completed.load(std::memory_order_relaxed), 0);
    }
