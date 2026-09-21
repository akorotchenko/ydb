#include "shared_cache_mapping.h"

#include <sys/mman.h>
#include <util/generic/vector.h>
#include <util/system/file.h>
#include <util/system/madvise.h>

#include <unistd.h>

#if defined(_linux_)
#include <sys/syscall.h>
#endif

namespace NKikimr::NSharedCache {
namespace {

    int CreateMemoryFile() noexcept {
#if defined(_linux_)
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
#if defined(SYS_memfd_create)
        return static_cast<int>(syscall(SYS_memfd_create, "ydb-shared-cache", MFD_CLOEXEC));
#else
        return -1;
#endif
#else
        return memfd_create("ydb-shared-cache", MFD_CLOEXEC);
#endif
    }

} // anonymous namespace

class TSharedCacheMapping::TImpl {
private:
    struct TView {
        void* Pointer = nullptr;
        size_t Size = 0;
    };

public:
    ~TImpl() {
        for (const TView& view : Views_) {
            munmap(view.Pointer, view.Size);
        }
    }

    bool Allocate(size_t currentSize, size_t reservedSize, void*& pointer) noexcept {
        if (CurrentPointer_ || currentSize == 0 || currentSize > reservedSize) {
            return false;
        }
        const int fd = CreateMemoryFile();
        if (fd < 0) {
            return false;
        }
        Backing_ = TFileHandle(fd);
        if (!ResizeBacking(currentSize) || !Map(currentSize, pointer)) {
            return false;
        }
        CurrentPointer_ = pointer;
        CurrentSize_ = currentSize;
        ReservedSize_ = reservedSize;
        return true;
    }

    bool Prepare(size_t targetSize, void*& pointer) noexcept {
        if (targetSize == 0 || targetSize > ReservedSize_) {
            return false;
        }
        if (targetSize == CurrentSize_) {
            pointer = CurrentPointer_;
            return true;
        }

        const size_t oldBackingSize = BackingSize_;
        if (targetSize > BackingSize_ && !ResizeBacking(targetSize)) {
            return false;
        }
        if (!Map(targetSize, pointer)) {
            if (BackingSize_ != oldBackingSize) {
                ResizeBacking(oldBackingSize);
            }
            return false;
        }
        return true;
    }

    void Cancel(void* pointer, size_t targetSize) noexcept {
        if (!pointer || pointer == CurrentPointer_ && targetSize == CurrentSize_) {
            return;
        }
        Unmap(pointer, targetSize);
        if (BackingSize_ > CurrentSize_) {
            ResizeBacking(CurrentSize_);
        }
    }

    void Publish(void* pointer, size_t size) noexcept {
        Y_DEBUG_ABORT_UNLESS(pointer);
        CurrentPointer_ = pointer;
        CurrentSize_ = size;
    }

    bool NeedsRelease(void* oldPointer, void* currentPointer, size_t currentSize) const noexcept {
        return (oldPointer && oldPointer != currentPointer) || BackingSize_ > currentSize;
    }

    void ReleaseRange(
        void* oldPointer, size_t oldSize, void* currentPointer, const void*& begin, size_t& size) const noexcept {
        if (oldPointer && oldPointer != currentPointer) {
            begin = oldPointer;
            size = oldSize;
        } else {
            begin = currentPointer;
            size = 0;
        }
    }

    bool Release(void*& oldPointer, size_t oldSize, void* currentPointer, size_t currentSize) noexcept {
        if (!NeedsRelease(oldPointer, currentPointer, currentSize)) {
            oldPointer = nullptr;
            return true;
        }
        if (oldPointer && oldPointer != currentPointer) {
            if (!Unmap(oldPointer, oldSize)) {
                return false;
            }
            oldPointer = nullptr;
        }
        return BackingSize_ <= currentSize || ResizeBacking(currentSize);
    }

private:
    bool ResizeBacking(size_t size) noexcept {
        if (size > static_cast<size_t>(Max<i64>()) || !Backing_.Resize(static_cast<i64>(size))) {
            return false;
        }
        BackingSize_ = size;
        return true;
    }

    bool Map(size_t size, void*& pointer) noexcept {
        int flags = MAP_SHARED;
#if defined(_freebsd_)
        flags |= MAP_NOCORE;
#endif
        void* view = mmap(nullptr, size, PROT_READ | PROT_WRITE, flags, Backing_, 0);
        if (view == MAP_FAILED) {
            return false;
        }
#if defined(_linux_)
        try {
            MadviseExcludeFromCoreDump(view, size);
        } catch (...) {
            munmap(view, size);
            return false;
        }
#endif
        try {
            Views_.push_back({ view, size });
        } catch (...) {
            munmap(view, size);
            return false;
        }
        pointer = view;
        return true;
    }

    bool Unmap(void* pointer, size_t size) noexcept {
        if (munmap(pointer, size) != 0) {
            return false;
        }
        for (auto it = Views_.begin(); it != Views_.end(); ++it) {
            if (it->Pointer == pointer) {
                Views_.erase(it);
                break;
            }
        }
        return true;
    }

private:
    TFileHandle Backing_;
    void* CurrentPointer_ = nullptr;
    size_t CurrentSize_ = 0;
    size_t ReservedSize_ = 0;
    size_t BackingSize_ = 0;
    TVector<TView> Views_;
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
    Impl_->Cancel(pointer, targetSize);
}

void TSharedCacheMapping::Publish(void* pointer, size_t size) noexcept {
    Impl_->Publish(pointer, size);
}

bool TSharedCacheMapping::NeedsRelease(
    void* oldPointer, size_t, void* currentPointer, size_t currentSize) const noexcept {
    return Impl_->NeedsRelease(oldPointer, currentPointer, currentSize);
}

void TSharedCacheMapping::ReleaseRange(
    void* oldPointer, size_t oldSize, void* currentPointer, size_t, const void*& begin, size_t& size) const noexcept {
    Impl_->ReleaseRange(oldPointer, oldSize, currentPointer, begin, size);
}

bool TSharedCacheMapping::Release(
    void*& oldPointer, size_t oldSize, void* currentPointer, size_t currentSize) noexcept {
    return Impl_->Release(oldPointer, oldSize, currentPointer, currentSize);
}

} // namespace NKikimr::NSharedCache
