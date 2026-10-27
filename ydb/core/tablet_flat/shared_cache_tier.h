#pragma once

#include "shared_cache_cyclic_buffer.h"

#include <algorithm>
#include <array>

namespace NKikimr::NSharedCache {

enum EHotLevel : ui32 {
    L0,
    L1,
    L2,
    L3,
};

inline constexpr ui32 HotLevelCount = 4;

struct THotRingRange {
    ui64 Offset = 0;
    ui64 Size = 0;

    constexpr ui64 End() const noexcept {
        return Offset + Size;
    }
};

struct THotRingLayout {
    ui64 SlotCount = 0;

    constexpr bool IsValid() const noexcept {
        return SlotCount >= HotLevelCount;
    }

    constexpr THotRingRange Ring(EHotLevel level) const noexcept;
};

class TCacheTierRings {
public:
    Y_FORCE_INLINE TRingCursor& Hot(EHotLevel level) noexcept {
        Y_DEBUG_ABORT_UNLESS(level < HotLevelCount);
        return HotCursors_[level];
    }

    Y_FORCE_INLINE TRingCursor& Cold() noexcept {
        return ColdCursor_;
    }

private:
    std::array<TRingCursor, HotLevelCount> HotCursors_;
    TRingCursor ColdCursor_;
};

// Borrows one tier's storage and stable cursors; the caller protects their lifetime
struct TCacheTierView {
    TCacheTierRings* Rings = nullptr;
    std::atomic<ui64>* HotSlots = nullptr;
    std::atomic<ui64>* ColdSlots = nullptr;
    ui64 HotSlotCount = 0;
    ui64 ColdSlotCount = 0;

    Y_FORCE_INLINE TRingView Hot(EHotLevel level) const noexcept;

    Y_FORCE_INLINE TRingView Cold() const noexcept {
        Y_DEBUG_ABORT_UNLESS(Rings && ColdSlots && ColdSlotCount != 0);
        return { &Rings->Cold(), ColdSlots, ColdSlotCount };
    }
};

constexpr THotRingRange THotRingLayout::Ring(EHotLevel level) const noexcept {
    Y_DEBUG_ABORT_UNLESS(IsValid() && level < HotLevelCount);
    // Existing 7:5:2:1 weights, with one slot per level for small standalone tiers
    const ui64 l3 = std::max<ui64>(1, SlotCount / 15);
    const ui64 l2 = std::max<ui64>(1, (SlotCount / 15) * 2 + ((SlotCount % 15) * 2) / 15);
    const ui64 l1 = std::max<ui64>(1, SlotCount / 3);
    const ui64 l0 = SlotCount - l3 - l2 - l1;
    const ui64 sizes[]{ l0, l1, l2, l3 };
    const ui64 offsets[]{ l3 + l2 + l1, l3 + l2, l3, 0 };
    return { offsets[level], sizes[level] };
}

Y_FORCE_INLINE TRingView TCacheTierView::Hot(EHotLevel level) const noexcept {
    Y_DEBUG_ABORT_UNLESS(Rings && HotSlots);
    const auto range = THotRingLayout{ HotSlotCount }.Ring(level);
    return { &Rings->Hot(level), HotSlots + range.Offset, range.Size };
}

} // namespace NKikimr::NSharedCache
