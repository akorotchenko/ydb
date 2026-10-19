#include "shared_cache_space.h"

#include <array>
#include <new>

namespace NKikimr::NSharedCache {
namespace {

    bool TryAdd(ui64 left, ui64 right, ui64& result) noexcept {
        if (left > Max<ui64>() - right) {
            return false;
        }
        result = left + right;
        return true;
    }

    bool TryMultiply(ui64 left, ui64 right, ui64& result) noexcept {
        if (left != 0 && right > Max<ui64>() / left) {
            return false;
        }
        result = left * right;
        return true;
    }

    bool TryAccumulateProduct(ui64 count, ui64 size, ui64& total) noexcept {
        ui64 product;
        return TryMultiply(count, size, product) && TryAdd(total, product, total);
    }

    bool TryMappingSize(ui64 count, ui64 itemSize, size_t& result) noexcept {
        ui64 bytes;
        if (!TryMultiply(count, itemSize, bytes) || bytes > Max<size_t>()) {
            return false;
        }
        result = static_cast<size_t>(bytes);
        return true;
    }

} // anonymous namespace

TSharedCacheSpace::~TSharedCacheSpace() = default;

bool TSharedCacheSpace::PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept {
    const TSpaceState spaceState = CurrentSpaceState();
    const TSpaceView& current = SpaceView(spaceState);
    if (transition.Phase_ != ETransitionPhase::Idle || target.AddressBits < MinSharedCacheAddressBits ||
        target.AddressBits > MaxSharedCacheAddressBits || target.AddressBits > ReservedCapacity_.AddressBits ||
        target.AddressBits == Capacity_.AddressBits || target.ExpectedPageSize != ReservedCapacity_.ExpectedPageSize ||
        target.StaticBytes > ReservedCapacity_.StaticBytes || current.HandleCount != Capacity_.HandleCount() ||
        current.AllocationLimit != Capacity_.HandleCount())
    {
        return false;
    }

    if (spaceState.Resizing() || spaceState.AddressBits() != Capacity_.AddressBits)
    {
        return false;
    }

    THolder<TSpaceView> view = MakeHolder<TSpaceView>();
    if (!TryMappingSize(target.HandleCount(), sizeof(THandle), view->HandlesBytes) ||
        !TryMappingSize(target.BucketCount(), sizeof(std::atomic<ui64>), view->BucketsBytes) ||
        !TryMappingSize(target.HotSlotCount(), sizeof(std::atomic<ui64>), view->HotSlotBytes) ||
        !TryMappingSize(target.ColdSlotCount(), sizeof(std::atomic<ui64>), view->ColdSlotBytes) ||
        !TryMappingSize(target.KeepColdSlotCount(), sizeof(std::atomic<ui64>), view->KeepColdSlotBytes) ||
        !TryMappingSize(target.HandleCount(), sizeof(std::atomic<ui64>), view->FreeSlotBytes) ||
        !HandlesMapping_.Prepare(view->HandlesBytes, view->Handles) ||
        !BucketsMapping_.Prepare(view->BucketsBytes, view->Buckets) ||
        !HotMapping_.Prepare(view->HotSlotBytes, view->HotSlots) ||
        !ColdMapping_.Prepare(view->ColdSlotBytes, view->ColdSlots) ||
        !KeepColdMapping_.Prepare(view->KeepColdSlotBytes, view->KeepColdSlots) ||
        !FreeMapping_.Prepare(view->FreeSlotBytes, view->FreeSlots))
    {
        CancelPreparedView(*view);
        return false;
    }

    transition.Phase_ = ETransitionPhase::Prepare;
    transition.Generation_ = spaceState.Generation();
    transition.OldConfiguration_ = Capacity_;
    transition.TargetConfiguration_ = target;
    transition.ResizeDeltaBits_ = Capacity_.AddressBits < target.AddressBits
                                      ? target.AddressBits - Capacity_.AddressBits
                                      : Capacity_.AddressBits - target.AddressBits;
    transition.ReleaseStage_ = ESpaceMap::Done;
    transition.NextWorkIndex_ = 0;
    transition.FinalDrainStage_ = ESpaceMap::Done;
    transition.CutBlocked_ = false;
    transition.PreparedView_ = std::move(view);
    transition.OldView_.Reset();
    return true;
}

void TSharedCacheSpace::PrepareView(ui32 generation, TSpaceView view) noexcept {
    Y_DEBUG_ABORT_UNLESS(S5FifoRings_ && FreeRingCursor_);
    view.Space = this;
    view.Generation = generation;
    const ui64 usableHandles = view.AllocationLimit - 2;
    view.ColdMinHandles = (usableHandles + 4) / 5;
    view.ColdGrowHandles = (usableHandles * 3 + 9) / 10;
    view.Rings = S5FifoRings_.Get();
    view.FreeCursor = FreeRingCursor_.Get();
    view.EffectiveHotSlots = &EffectiveHotSlots_;
    SpaceViews_[generation & 1] = view;
}

ui32 TSharedCacheSpace::HotLevelForSlot(ui64 slot, ui64 hotSlotCount) noexcept {
    const ui64 l3End = hotSlotCount / 15;
    const ui64 l2End = l3End + (hotSlotCount * 2) / 15;
    const ui64 l1End = l2End + (hotSlotCount * 5) / 15;
    if (slot < l3End) {
        return 3;
    }
    if (slot < l2End) {
        return 2;
    }
    if (slot < l1End) {
        return 1;
    }
    return 0;
}

bool TSharedCacheSpace::IsCurrentFreeWord(const TSpaceView& view, ui64 raw) const noexcept {
    const TCacheItem cacheItem = TCacheItem::FromRaw(raw);
    if (cacheItem.IsNull() || cacheItem.IsFrozen() || cacheItem.Index() < 2 ||
        cacheItem.Index() >= view.AllocationLimit || cacheItem.Index() >= view.HandleCount)
    {
        return false;
    }
    const THandleState state =
        THandleState::FromRaw(view.Handles[cacheItem.Index()].State.load(std::memory_order_acquire));
    return cacheItem.Matches(state) && state.IsFree() && state.Refs() == 0;
}

void TSharedCacheSpace::RouteFreeWord(const TSpaceView& view, ui64 raw) noexcept {
    const TFreeRingView free = view.Free();
    while (raw != 0 && IsCurrentFreeWord(view, raw)) {
        raw = free.Push(raw).Previous;
    }
}

TSpaceHazardBinding::TSpaceHazardBinding(const TSharedCacheSpace* space, TSpaceHazard* hazard) noexcept
    : Space_(space)
    , Hazard_(hazard)
{
    Y_DEBUG_ABORT_UNLESS(!TSharedCacheSpace::TlsSpace_);
    Y_ABORT_UNLESS(!TSharedCacheSpace::TlsHazard_);
    Y_IF_DEBUG(TSharedCacheSpace::TlsSpace_ = space;)
    TSharedCacheSpace::TlsHazard_ = hazard;
}

TSpaceHazardBinding::TSpaceHazardBinding(TSpaceHazardBinding&& other) noexcept
    : Space_(std::exchange(other.Space_, nullptr))
    , Hazard_(std::exchange(other.Hazard_, nullptr))
{}

TSpaceHazardBinding& TSpaceHazardBinding::operator=(TSpaceHazardBinding&& other) noexcept {
    if (this != &other) {
        Reset();
        Space_ = std::exchange(other.Space_, nullptr);
        Hazard_ = std::exchange(other.Hazard_, nullptr);
    }
    return *this;
}

TSpaceHazardBinding::~TSpaceHazardBinding() {
    Reset();
}

void TSpaceHazardBinding::Reset() noexcept {
    if (Space_) {
        Y_DEBUG_ABORT_UNLESS(TSharedCacheSpace::TlsSpace_ == Space_);
        Y_ABORT_UNLESS(TSharedCacheSpace::TlsHazard_ == Hazard_);
        Y_IF_DEBUG(TSharedCacheSpace::TlsSpace_ = nullptr;)
        TSharedCacheSpace::TlsHazard_ = nullptr;
        Y_ABORT_UNLESS(!SpaceHazardIsActive(Hazard_->State.load(std::memory_order_relaxed)));
        Hazard_->Bound.store(false, std::memory_order_release);
        Space_ = nullptr;
        Hazard_ = nullptr;
    }
}

TSpaceHazardBinding TSharedCacheSpace::BindThreadHazard(ui32 index) const noexcept {
    Y_ABORT_UNLESS(index < HazardSlotCount_);
    bool unbound = false;
    Y_ABORT_UNLESS(Hazards_[index].Bound.compare_exchange_strong(unbound, true, std::memory_order_acquire));
    return TSpaceHazardBinding(this, &Hazards_[index]);
}

TSpaceHazardBinding TSharedCacheSpace::BindAnyThreadHazard() const noexcept {
    for (ui32 index = 0; index < HazardSlotCount_; ++index) {
        bool unbound = false;
        if (Hazards_[index].Bound.compare_exchange_strong(unbound, true, std::memory_order_acquire)) {
            return TSpaceHazardBinding(this, &Hazards_[index]);
        }
    }
    Y_ABORT("Shared-cache hazard slots exhausted");
}

TCacheItem TSharedCacheSpace::TakeCutSpare(ui64 allocationLimit) noexcept {
    const auto take = [allocationLimit](TSpaceHazard& hazard) noexcept {
        ui64 raw = hazard.SpareItem.load(std::memory_order_acquire);
        while (raw != 0) {
            const TCacheItem cacheItem = TCacheItem::FromRaw(raw);
            if (cacheItem.Index() < allocationLimit) {
                return TCacheItem{};
            }
            if (hazard.SpareItem.compare_exchange_weak(raw, 0, std::memory_order_acquire, std::memory_order_relaxed))
            {
                return cacheItem;
            }
        }
        return TCacheItem{};
    };

    for (ui32 index = 0; index < HazardSlotCount_; ++index) {
        TCacheItem cacheItem = take(Hazards_[index]);
        if (!cacheItem.IsNull()) {
            return cacheItem;
        }
    }
    return {};
}

bool TryCalculateSharedCacheFootprint(
    ui8 addressBits, ui64 expectedPageSize, ui64 fixedBytes, ui32 hazardCount, TSharedCacheCapacity& result) noexcept {
    if (addressBits < MinSharedCacheAddressBits || addressBits > MaxSharedCacheAddressBits || expectedPageSize == 0) {
        return false;
    }

    const ui64 handleCount = ui64{ 1 } << addressBits;
    const ui64 bucketCount = handleCount;
    const ui64 hotSlotCount = (handleCount * 3) / 4;
    ui64 accountedPageBytes;
    if (!TryAdd(expectedPageSize, NActors::TSharedData::OverheadSize, accountedPageBytes)) {
        return false;
    }

    ui64 staticBytes = fixedBytes;
    if (!TryAccumulateProduct(handleCount, sizeof(THandle), staticBytes) ||
        !TryAccumulateProduct(bucketCount, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(hotSlotCount, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(handleCount / 2, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(handleCount / 2, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(handleCount, sizeof(std::atomic<ui64>), staticBytes) ||
        !TryAccumulateProduct(Max<ui64>(hazardCount, 1), sizeof(TSpaceHazard), staticBytes))
    {
        return false;
    }

    ui64 minimumPayloadBytes;
    if (!TryMultiply(handleCount - 2, accountedPageBytes, minimumPayloadBytes)) {
        return false;
    }

    ui64 totalBytes;
    if (!TryAdd(staticBytes, minimumPayloadBytes, totalBytes)) {
        return false;
    }

    result = {
        .AddressBits = addressBits,
        .Limit = totalBytes,
        .ExpectedPageSize = expectedPageSize,
        .FixedBytes = fixedBytes,
        .StaticBytes = staticBytes,
        .MinimumPayloadBytes = minimumPayloadBytes,
        .TotalBytes = totalBytes,
    };
    return true;
}

bool TryCalculateSharedCacheCapacity(
    ui64 limit, ui64 expectedPageSize, ui64 fixedBytes, ui32 hazardCount, TSharedCacheCapacity& result) noexcept {
    bool found = false;
    TSharedCacheCapacity best;
    for (ui32 addressBits = MinSharedCacheAddressBits; addressBits <= MaxSharedCacheAddressBits; ++addressBits) {
        TSharedCacheCapacity footprint;
        if (!TryCalculateSharedCacheFootprint(
                static_cast<ui8>(addressBits), expectedPageSize, fixedBytes, hazardCount, footprint) ||
            footprint.TotalBytes > limit)
        {
            break;
        }
        best = footprint;
        found = true;
    }
    if (!found) {
        return false;
    }
    best.Limit = limit;
    result = best;
    return true;
}

THolder<TSharedCacheSpace> TSharedCacheSpace::Create(
    const TSharedCacheCapacity& current, const TSharedCacheCapacity& reserved, ui32 hazardCount)
{
    THolder<TSharedCacheSpace> result(new TSharedCacheSpace());
    if (!result->Initialize(current, reserved, hazardCount)) {
        return nullptr;
    }
    return result;
}

bool TSharedCacheSpace::Initialize(
    const TSharedCacheCapacity& current, const TSharedCacheCapacity& reserved, ui32 hazardCount)
{
    if (current.AddressBits < MinSharedCacheAddressBits || current.AddressBits > MaxSharedCacheAddressBits ||
        reserved.AddressBits < current.AddressBits || reserved.AddressBits > MaxSharedCacheAddressBits ||
        current.ExpectedPageSize == 0 || current.ExpectedPageSize != reserved.ExpectedPageSize ||
        current.FixedBytes != reserved.FixedBytes || current.Limit < current.TotalBytes ||
        reserved.Limit < reserved.TotalBytes || reserved.Limit < current.Limit)
    {
        return false;
    }

    THolder<TSpaceView> view = MakeHolder<TSpaceView>();
    size_t reservedHandlesBytes;
    size_t reservedBucketsBytes;
    size_t reservedHotBytes;
    size_t reservedSlotsBytes;
    if (!TryMappingSize(current.HandleCount(), sizeof(THandle), view->HandlesBytes) ||
        !TryMappingSize(current.BucketCount(), sizeof(std::atomic<ui64>), view->BucketsBytes) ||
        !TryMappingSize(current.HotSlotCount(), sizeof(std::atomic<ui64>), view->HotSlotBytes) ||
        !TryMappingSize(current.ColdSlotCount(), sizeof(std::atomic<ui64>), view->ColdSlotBytes) ||
        !TryMappingSize(current.KeepColdSlotCount(), sizeof(std::atomic<ui64>), view->KeepColdSlotBytes) ||
        !TryMappingSize(current.HandleCount(), sizeof(std::atomic<ui64>), view->FreeSlotBytes) ||
        !TryMappingSize(reserved.HandleCount(), sizeof(THandle), reservedHandlesBytes) ||
        !TryMappingSize(reserved.BucketCount(), sizeof(std::atomic<ui64>), reservedBucketsBytes) ||
        !TryMappingSize(reserved.HotSlotCount(), sizeof(std::atomic<ui64>), reservedHotBytes) ||
        !TryMappingSize(reserved.HandleCount(), sizeof(std::atomic<ui64>), reservedSlotsBytes) ||
        !HandlesMapping_.Allocate(view->HandlesBytes, reservedHandlesBytes, view->Handles) ||
        !BucketsMapping_.Allocate(view->BucketsBytes, reservedBucketsBytes, view->Buckets) ||
        !HotMapping_.Allocate(view->HotSlotBytes, reservedHotBytes, view->HotSlots) ||
        !ColdMapping_.Allocate(view->ColdSlotBytes, reservedSlotsBytes / 2, view->ColdSlots) ||
        !KeepColdMapping_.Allocate(view->KeepColdSlotBytes, reservedSlotsBytes / 2, view->KeepColdSlots) ||
        !FreeMapping_.Allocate(view->FreeSlotBytes, reservedSlotsBytes, view->FreeSlots))
    {
        return false;
    }

    for (ui64 index = 0; index < current.HandleCount(); ++index) {
        new (&view->Handles[index]) THandle();
        new (&view->FreeSlots[index]) std::atomic<ui64>(0);
    }
    for (ui64 index = 0; index < current.ColdSlotCount(); ++index) {
        new (&view->ColdSlots[index]) std::atomic<ui64>(0);
    }
    for (ui64 index = 0; index < current.BucketCount(); ++index) {
        new (&view->Buckets[index]) std::atomic<ui64>(0);
    }
    for (ui64 index = 0; index < current.HotSlotCount(); ++index) {
        new (&view->HotSlots[index]) std::atomic<ui64>(0);
    }
    for (ui64 index = 0; index < current.KeepColdSlotCount(); ++index) {
        new (&view->KeepColdSlots[index]) std::atomic<ui64>(0);
    }

    for (ui64 index = 2; index < current.HandleCount(); ++index) {
        view->FreeSlots[index - 2].store(
            TCacheItem::Make(0, static_cast<ui32>(index)).Raw(), std::memory_order_relaxed);
    }

    HazardSlotCount_ = Max<ui32>(hazardCount, 1);
    Hazards_ = TArrayHolder<TSpaceHazard>(new TSpaceHazard[HazardSlotCount_]);
    HazardCount_ = hazardCount;
    Capacity_ = current;
    ReservedCapacity_ = reserved;
    NewestViewOwner_ = std::move(view);
    S5FifoRings_ = MakeHolder<TS5FifoRings>();
    FreeRingCursor_ = MakeHolder<TFreeRingCursor>(0, current.HandleCount() - 2, current.HandleCount() - 2);
    NewestView_.store(NewestViewOwner_.Get(), std::memory_order_release);
    TSpaceView initialView;
    initialView.CopyMappings(*NewestViewOwner_);
    initialView.HandleCount = current.HandleCount();
    initialView.AllocationLimit = current.HandleCount();
    initialView.HotSlotCount = current.HotSlotCount();
    PrepareView(0, initialView);
    SpaceState_.store(TSpaceState::Make(0, current.AddressBits).Raw(), std::memory_order_release);
    EffectiveHotSlots_.store(current.HotSlotCount(), std::memory_order_release);
    return true;
}

ui32 TSpaceOperation::TryAllocateHandle() noexcept {
    return View().Space->TryAllocateHandleImpl<false>(*this, {});
}

template <bool WithHook>
Y_FORCE_INLINE ui32 TSharedCacheSpace::TryAllocateHandleImpl(
    TSpaceOperation& spaceOp, TFreeExchangeHook hook) noexcept {
    for (;;) {
        const TSpaceState attemptState = CurrentSpaceState();
        const TSpaceView& attemptView = SpaceView(attemptState);
        const TFreeRingView free = attemptView.Free();
        const TRingExchangeResult exchange = free.Pop([&] {
            if constexpr (WithHook) {
                hook.Run();
            }
        });
        if (!exchange.Slot) {
            return 0;
        }
        const ui64 rawItem = exchange.Previous;
        if (rawItem == 0) {
            continue;
        }

        const TCacheItem cacheItem = TCacheItem::FromRaw(rawItem);
        if (CurrentSpaceState().Generation() != attemptState.Generation())
        {
            if (!cacheItem.IsNull() && !cacheItem.IsFrozen()) {
                spaceOp.ReturnFreeHandle(cacheItem.Index(), cacheItem.Version());
            }
            continue;
        }
        if (cacheItem.IsNull() || cacheItem.IsFrozen() || cacheItem.Index() >= attemptView.AllocationLimit) {
            continue;
        }

        THandle& handle = attemptView.Handles[cacheItem.Index()];
        ui64 expectedRaw = handle.State.load(std::memory_order_acquire);
        const THandleState expected = THandleState::FromRaw(expectedRaw);
        if (!cacheItem.Matches(expected) || !expected.IsFree() || expected.Refs() != 0)
        {
            continue;
        }

        const THandleState desired =
            expected.WithState(EHandleState::Begin).WithFrequency(0).WithSticky(EStickyState::None);
        if (handle.State.compare_exchange_strong(
                expectedRaw, desired.Raw(), std::memory_order_acquire, std::memory_order_relaxed))
        {
            Y_DEBUG_ABORT_UNLESS(cacheItem.Index() < spaceOp.AccessibleHandleCount());
            return cacheItem.Index();
        }
    }
}

ui32 TSharedCacheSpace::TryAllocateHandleWithHook(TSpaceOperation& spaceOp, TFreeExchangeHook hook) noexcept {
    return TryAllocateHandleImpl<true>(spaceOp, hook);
}

bool TSpaceOperation::ReturnFreeHandle(ui32 index, ui32 version) noexcept {
    return View().Space->ReturnFreeHandleImpl<false>(*this, index, version, {});
}

template <bool WithHook>
Y_FORCE_INLINE bool TSharedCacheSpace::ReturnFreeHandleImpl(
    TSpaceOperation& spaceOp, ui32 index, ui32 version, TFreeExchangeHook hook) noexcept {
    TSpaceState currentState = CurrentSpaceState();
    const TSpaceView* currentView = &SpaceView(currentState);
    const TSpaceView& operationView = spaceOp.View();
    if (index < 2 || index >= spaceOp.AccessibleHandleCount()) {
        return false;
    }

    const THandleState state =
        THandleState::FromRaw(operationView.Handles[index].State.load(std::memory_order_acquire));
    if (state.Version() != version || !state.IsFree() || state.Refs() != 0) {
        return false;
    }
    if (index >= currentView->AllocationLimit) {
        return true;
    }

    std::array<ui64, 2> pending = {
        TCacheItem::Make(version, index).Raw(),
        0,
    };
    ui32 pendingCount = 1;
    while (pendingCount) {
        ui64 carried = pending[--pendingCount];
        for (;;) {
            const TCacheItem cacheItem = TCacheItem::FromRaw(carried);
            currentState = CurrentSpaceState();
            currentView = &SpaceView(currentState);
            if (cacheItem.IsNull() || cacheItem.IsFrozen() || cacheItem.Index() >= currentView->AllocationLimit)
            {
                break;
            }
            const THandleState handleState =
                THandleState::FromRaw(currentView->Handles[cacheItem.Index()].State.load(std::memory_order_acquire));
            if (!cacheItem.Matches(handleState) || !handleState.IsFree() || handleState.Refs() != 0)
            {
                break;
            }

            const TSpaceState attemptState = currentState;
            const TSpaceView* attemptView = currentView;
            const TRingExchangeResult exchange = attemptView->Free().Push(carried, [&] {
                if constexpr (WithHook) {
                    hook.Run();
                }
            });
            const ui64 displaced = exchange.Previous;

            if (CurrentSpaceState().Generation() == attemptState.Generation())
            {
                if (displaced == 0) {
                    break;
                }
                carried = displaced;
                continue;
            }

            const ui64 late = FreeRingCursor_->Clear(exchange.Slot);
            if (late != 0) {
                Y_ABORT_UNLESS(pendingCount < pending.size());
                pending[pendingCount++] = late;
            }
            if (displaced == 0) {
                break;
            }
            carried = displaced;
        }
    }
    return true;
}

bool TSharedCacheSpace::ReturnFreeHandleWithHook(
    TSpaceOperation& spaceOp, ui32 index, ui32 version, TFreeExchangeHook hook) noexcept {
    return ReturnFreeHandleImpl<true>(spaceOp, index, version, hook);
}

} // namespace NKikimr::NSharedCache
