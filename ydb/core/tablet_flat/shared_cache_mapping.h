#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSharedCache {

class TSharedCacheMapping : TMoveOnly {
public:
    enum class EAddressMode {
        Default,
        Stable,
    };

    explicit TSharedCacheMapping(EAddressMode addressMode = EAddressMode::Default);
    ~TSharedCacheMapping();

    bool Allocate(size_t currentSize, size_t reservedSize, void*& pointer) noexcept;
    bool Prepare(size_t targetSize, void*& pointer) noexcept;

    template <class T>
    bool Allocate(size_t currentSize, size_t reservedSize, T*& pointer) noexcept;

    template <class T>
    bool Prepare(size_t targetSize, T*& pointer) noexcept;

    // A cleared candidate may still have backing cleanup to retry
    bool Cancel(void*& pointer, size_t targetSize) noexcept;

    template <class T>
    bool Cancel(T*& pointer, size_t targetSize) noexcept;
    void Publish(void* pointer, size_t size) noexcept;

    bool NeedsRelease(void* oldPointer, size_t oldSize, void* currentPointer, size_t currentSize) const noexcept;

    void ReleaseRange(void* oldPointer, size_t oldSize, void* currentPointer, size_t currentSize, const void*& begin,
        size_t& size) const noexcept;

    bool Release(void*& oldPointer, size_t oldSize, void* currentPointer, size_t currentSize) noexcept;

private:
    friend class TSharedCacheMappingTestAccess;

    enum class ETestStep {
        GrowBacking,
        Map,
        AfterMap,
        Unmap,
        ShrinkBacking,
        DiscardTail,
    };

    struct TTestHook {
        void* Context = nullptr;
        bool (*Function)(void*, ETestStep) noexcept = nullptr;

        bool Run(ETestStep step) const noexcept {
            return !Function || Function(Context, step);
        }
    };

    void SetTestHook(TTestHook hook) noexcept;

    class TImpl;
    THolder<TImpl> Impl_;
};

// Inline implementations

template <class T>
inline bool TSharedCacheMapping::Allocate(size_t currentSize, size_t reservedSize, T*& pointer) noexcept {
    void* data;
    if (!Allocate(currentSize, reservedSize, data)) {
        return false;
    }
    pointer = static_cast<T*>(data);
    return true;
}

template <class T>
inline bool TSharedCacheMapping::Prepare(size_t targetSize, T*& pointer) noexcept {
    void* data = nullptr;
    const bool prepared = Prepare(targetSize, data);
    pointer = static_cast<T*>(data);
    return prepared;
}

template <class T>
inline bool TSharedCacheMapping::Cancel(T*& pointer, size_t targetSize) noexcept {
    void* data = pointer;
    const bool cancelled = Cancel(data, targetSize);
    pointer = static_cast<T*>(data);
    return cancelled;
}

} // namespace NKikimr::NSharedCache
