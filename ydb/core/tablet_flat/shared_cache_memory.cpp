#include "shared_cache_memory.h"

namespace NKikimr::NSharedCache {

TCacheMemory::TCacheMemory(ui64 staticBytes) noexcept
    : StaticBytes_(staticBytes)
    , OverallUsage_(staticBytes)
{}

void TCacheMemory::PrepareStaticResize(ui64 bytes) noexcept {
    Y_DEBUG_ABORT_UNLESS(StaticDeltaBytes_.load(std::memory_order_relaxed) == 0);
    StaticDeltaBytes_.fetch_add(bytes, std::memory_order_relaxed);
    OverallUsage_.fetch_add(bytes, std::memory_order_relaxed);
}

void TCacheMemory::CancelStaticResize(ui64 bytes) noexcept {
    SubtractExactBytes(StaticDeltaBytes_, bytes);
    SubtractExactBytes(OverallUsage_, bytes);
}

void TCacheMemory::CommitStaticResize(ui64 oldStaticBytes, ui64 targetStaticBytes) noexcept {
    if (targetStaticBytes > oldStaticBytes) {
        const ui64 delta = targetStaticBytes - oldStaticBytes;
        StaticBytes_.fetch_add(delta, std::memory_order_relaxed);
        SubtractExactBytes(StaticDeltaBytes_, delta);
    } else {
        const ui64 delta = oldStaticBytes - targetStaticBytes;
        SubtractExactBytes(StaticBytes_, delta);
        SubtractExactBytes(OverallUsage_, delta);
    }
}

} // namespace NKikimr::NSharedCache
