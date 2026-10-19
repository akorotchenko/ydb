#include "shared_cache.h"

namespace NKikimr::NSharedCache {

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>

SHARED_CACHE_TEMPLATE
bool TSharedCache::PrepareTransition(const TSharedCacheCapacity& target, TTransition& transition) noexcept {
    const ui64 currentStaticBytes = Space_->CurrentConfiguration().StaticBytes;
    const ui64 staticDeltaBytes = target.StaticBytes > currentStaticBytes ? target.StaticBytes - currentStaticBytes : 0;
    if (staticDeltaBytes != 0) {
        Y_DEBUG_ABORT_UNLESS(StaticDeltaBytes_.load(std::memory_order_relaxed) == 0);
        StaticDeltaBytes_.fetch_add(staticDeltaBytes, std::memory_order_relaxed);
        OverallUsage_.fetch_add(staticDeltaBytes, std::memory_order_relaxed);
    }
    if (Space_->PrepareTransition(target, transition)) {
        return true;
    }
    if (staticDeltaBytes != 0) {
        SubtractExactBytes(StaticDeltaBytes_, staticDeltaBytes);
        SubtractExactBytes(OverallUsage_, staticDeltaBytes);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PublishMigrationView(TTransition& transition) noexcept {
    if (auto hotResize = HotResize_.TryLock()) {
        return Space_->PublishMigrationView(transition);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryDrainTransition(TTransition& transition) noexcept {
    return Space_->TryDrainTransition(transition);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::PublishFinalView(TTransition& transition) noexcept {
    if (auto hotResize = HotResize_.TryLock()) {
        if (HotResize_.Phase() == EHotResizePhase::Cut) {
            auto spaceOp = BeginOperation();
            DrainHotResize(spaceOp);
        }
        if (HotResize_.Phase() != EHotResizePhase::Idle) {
            return false;
        }
        return Space_->PublishFinalView(transition);
    }
    return false;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AppendFreeHandles(TTransition& transition) noexcept {
    return Space_->AppendFreeHandles(transition);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::StartHardTransition() noexcept {
    if (HardTransition_.Phase() != ETransitionPhase::Idle) {
        return false;
    }

    const TSharedCacheCapacity& current = Space_->CurrentConfiguration();
    if (current.AddressBits == HardTarget_.AddressBits) {
        return false;
    }

    const ui8 nextAddressBits =
        current.AddressBits < HardTarget_.AddressBits ? current.AddressBits + 1 : current.AddressBits - 1;
    TSharedCacheCapacity next;
    if (!TryCalculateSharedCacheFootprint(
            nextAddressBits, current.ExpectedPageSize, current.FixedBytes, Space_->HazardCount(), next))
    {
        return false;
    }
    next.Limit = HardLimit_.load(std::memory_order_relaxed);
    return PrepareTransition(next, HardTransition_);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::FinalDrain(TTransition& transition) noexcept {
    auto spaceOp = BeginOperation();
    return Space_->FinalDrain(
        transition,
        [&](ui32 level, ui64 raw) noexcept {
            RouteHot(spaceOp, static_cast<EHotLevel>(level), TCacheItem::FromRaw(raw));
        },
        [&](ui64 raw) noexcept {
            const ui64 previous = ColdRingEntries_.fetch_sub(1, std::memory_order_relaxed);
            Y_ABORT_UNLESS(previous != 0);
            RouteCold(spaceOp, TColdCacheItem::FromRaw(raw));
        },
        [&](ui64 raw) noexcept {
            RouteKeepCold(spaceOp, TColdCacheItem::FromRaw(raw));
        });
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::TryReleaseTransition(TTransition& transition) noexcept {
    return Space_->TryReleaseTransition(transition);
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::CommitTransition(TTransition& transition) noexcept {
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    const ui64 targetLimit = transition.TargetConfiguration().Limit;
    const ui64 targetHandleCount = transition.TargetConfiguration().HandleCount();
    const bool commitTargetLimit = &transition != &HardTransition_;
    if (!Space_->CommitTransition(transition)) {
        return false;
    }

    if (targetStaticBytes > oldStaticBytes) {
        const ui64 delta = targetStaticBytes - oldStaticBytes;
        StaticBytes_.fetch_add(delta, std::memory_order_relaxed);
        SubtractExactBytes(StaticDeltaBytes_, delta);
    } else {
        const ui64 delta = oldStaticBytes - targetStaticBytes;
        SubtractExactBytes(StaticBytes_, delta);
        SubtractExactBytes(OverallUsage_, delta);
    }
    if (commitTargetLimit) {
        TargetHandleCount_.store(targetHandleCount, std::memory_order_relaxed);
        HardLimit_.store(targetLimit, std::memory_order_relaxed);
    }
    RefreshSoftLimit();
    return true;
}

SHARED_CACHE_TEMPLATE
bool TSharedCache::AbandonTransition(TTransition& transition) noexcept {
    const ETransitionPhase phase = transition.Phase();
    const ui64 oldStaticBytes = transition.OldConfiguration().StaticBytes;
    const ui64 targetStaticBytes = transition.TargetConfiguration().StaticBytes;
    if (!Space_->AbandonTransition(transition)) {
        return false;
    }
    if ((phase == ETransitionPhase::Prepare || phase == ETransitionPhase::InitializeGrowth) &&
        targetStaticBytes > oldStaticBytes)
    {
        const ui64 delta = targetStaticBytes - oldStaticBytes;
        SubtractExactBytes(StaticDeltaBytes_, delta);
        SubtractExactBytes(OverallUsage_, delta);
    }
    return true;
}

#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

// Capacity transition members are instantiated here for both trait variants.
template bool TSharedCacheImpl<TProdTraits>::PrepareTransition(const TSharedCacheCapacity&, TTransition&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::PublishMigrationView(TTransition&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::TryDrainTransition(TTransition&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::PublishFinalView(TTransition&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::AppendFreeHandles(TTransition&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::StartHardTransition() noexcept;
template bool TSharedCacheImpl<TProdTraits>::FinalDrain(TTransition&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::TryReleaseTransition(TTransition&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::CommitTransition(TTransition&) noexcept;
template bool TSharedCacheImpl<TProdTraits>::AbandonTransition(TTransition&) noexcept;

template bool TSharedCacheImpl<TTestTraits>::PrepareTransition(const TSharedCacheCapacity&, TTransition&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::PublishMigrationView(TTransition&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::TryDrainTransition(TTransition&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::PublishFinalView(TTransition&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::AppendFreeHandles(TTransition&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::StartHardTransition() noexcept;
template bool TSharedCacheImpl<TTestTraits>::FinalDrain(TTransition&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::TryReleaseTransition(TTransition&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::CommitTransition(TTransition&) noexcept;
template bool TSharedCacheImpl<TTestTraits>::AbandonTransition(TTransition&) noexcept;

} // namespace NKikimr::NSharedCache
