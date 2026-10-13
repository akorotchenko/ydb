#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSharedCache {

class TSharedCacheMapping : TMoveOnly {
public:
    TSharedCacheMapping();
    ~TSharedCacheMapping();

    bool Allocate(size_t currentSize, size_t reservedSize, void*& pointer) noexcept;
    bool Prepare(size_t targetSize, void*& pointer) noexcept;

    template <class T>
    bool Allocate(size_t currentSize, size_t reservedSize, T*& pointer) noexcept {
        void* data;
        if (!Allocate(currentSize, reservedSize, data)) {
            return false;
        }
        pointer = static_cast<T*>(data);
        return true;
    }

    template <class T>
    bool Prepare(size_t targetSize, T*& pointer) noexcept {
        void* data;
        if (!Prepare(targetSize, data)) {
            return false;
        }
        pointer = static_cast<T*>(data);
        return true;
    }

    void Cancel(void* pointer, size_t targetSize) noexcept;
    void Publish(void* pointer, size_t size) noexcept;

    bool NeedsRelease(void* oldPointer, size_t oldSize, void* currentPointer, size_t currentSize) const noexcept;

    void ReleaseRange(void* oldPointer, size_t oldSize, void* currentPointer, size_t currentSize, const void*& begin,
        size_t& size) const noexcept;

    bool Release(void*& oldPointer, size_t oldSize, void* currentPointer, size_t currentSize) noexcept;

private:
    class TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NKikimr::NSharedCache
