#include "shared_cache_mapping.h"

#include <util/system/align.h>
#include <util/system/info.h>
#include <util/system/winint.h>

namespace NKikimr::NSharedCache {

class TSharedCacheMapping::TImpl {
public:
    ~TImpl() {
        if (Base_) {
            VirtualFree(Base_, 0, MEM_RELEASE);
        }
    }

    bool Allocate(size_t currentSize, size_t reservedSize, void*& pointer) noexcept {
        if (Base_ || currentSize == 0 || currentSize > reservedSize) {
            return false;
        }
        ReservedSize_ = reservedSize;
        ReservedMappingSize_ = AlignUp(reservedSize, NSystemInfo::GetPageSize());
        Base_ = VirtualAlloc(nullptr, ReservedMappingSize_, MEM_RESERVE, PAGE_NOACCESS);
        if (!Base_ || !Commit(0, currentSize)) {
            return false;
        }
        CurrentSize_ = currentSize;
        pointer = Base_;
        return true;
    }

    bool Prepare(size_t targetSize, void*& pointer) noexcept {
        if (targetSize == 0 || targetSize > ReservedSize_) {
            return false;
        }
        if (targetSize > CurrentSize_ && !Commit(CurrentSize_, targetSize)) {
            return false;
        }
        pointer = Base_;
        return true;
    }

    void Cancel(size_t targetSize) noexcept {
        if (targetSize > CurrentSize_) {
            Decommit(CurrentSize_, targetSize);
        }
    }

    void Publish(size_t size) noexcept {
        CurrentSize_ = size;
    }

    bool NeedsRelease(size_t oldSize, size_t currentSize) const noexcept {
        const size_t pageSize = NSystemInfo::GetPageSize();
        return oldSize > currentSize && AlignUp(oldSize, pageSize) > AlignUp(currentSize, pageSize);
    }

    void ReleaseRange(size_t oldSize, size_t currentSize, const void*& begin, size_t& size) const noexcept {
        const size_t pageSize = NSystemInfo::GetPageSize();
        const size_t beginOffset = AlignUp(currentSize, pageSize);
        const size_t endOffset = AlignUp(oldSize, pageSize);
        begin = static_cast<char*>(Base_) + beginOffset;
        size = endOffset - beginOffset;
    }

    bool Release(size_t oldSize, size_t currentSize) noexcept {
        return !NeedsRelease(oldSize, currentSize) || Decommit(currentSize, oldSize);
    }

private:
    bool Commit(size_t oldSize, size_t newSize) noexcept {
        const size_t pageSize = NSystemInfo::GetPageSize();
        const size_t begin = AlignUp(oldSize, pageSize);
        const size_t end = AlignUp(newSize, pageSize);
        return begin == end ||
               VirtualAlloc(static_cast<char*>(Base_) + begin, end - begin, MEM_COMMIT, PAGE_READWRITE) != nullptr;
    }

    bool Decommit(size_t newSize, size_t oldSize) noexcept {
        const size_t pageSize = NSystemInfo::GetPageSize();
        const size_t begin = AlignUp(newSize, pageSize);
        const size_t end = AlignUp(oldSize, pageSize);
        return begin == end || VirtualFree(static_cast<char*>(Base_) + begin, end - begin, MEM_DECOMMIT) != FALSE;
    }

private:
    void* Base_ = nullptr;
    size_t CurrentSize_ = 0;
    size_t ReservedSize_ = 0;
    size_t ReservedMappingSize_ = 0;
};

TSharedCacheMapping::TSharedCacheMapping()
    : Impl_(MakeHolder<TImpl>())
{
}

TSharedCacheMapping::~TSharedCacheMapping() = default;

bool TSharedCacheMapping::Allocate(size_t currentSize, size_t reservedSize, void*& pointer) noexcept {
    return Impl_->Allocate(currentSize, reservedSize, pointer);
}

bool TSharedCacheMapping::Prepare(size_t targetSize, void*& pointer) noexcept {
    return Impl_->Prepare(targetSize, pointer);
}

void TSharedCacheMapping::Cancel(void* pointer, size_t targetSize) noexcept {
    if (pointer) {
        Impl_->Cancel(targetSize);
    }
}

void TSharedCacheMapping::Publish(void*, size_t size) noexcept {
    Impl_->Publish(size);
}

bool TSharedCacheMapping::NeedsRelease(void*, size_t oldSize, void*, size_t currentSize) const noexcept {
    return Impl_->NeedsRelease(oldSize, currentSize);
}

void TSharedCacheMapping::ReleaseRange(
    void*, size_t oldSize, void*, size_t currentSize, const void*& begin, size_t& size) const noexcept {
    Impl_->ReleaseRange(oldSize, currentSize, begin, size);
}

bool TSharedCacheMapping::Release(void*& oldPointer, size_t oldSize, void*, size_t currentSize) noexcept {
    if (!Impl_->Release(oldSize, currentSize)) {
        return false;
    }
    oldPointer = nullptr;
    return true;
}

} // namespace NKikimr::NSharedCache
