#pragma once

#include <util/generic/noncopyable.h>
#include <util/system/types.h>

#include <utility>

namespace NKikimr::NSharedCache {

class TCacheMemory;

// Owns one page's Keep admission until publication transfers it to the native state
class TKeepReservation : TMoveOnly {
public:
    TKeepReservation() noexcept = default;
    ~TKeepReservation();

    TKeepReservation(TKeepReservation&& other) noexcept
        : Memory_(std::exchange(other.Memory_, nullptr))
        , Bytes_(other.Bytes_)
        , Pending_(other.Pending_)
    {}

    TKeepReservation& operator=(TKeepReservation&& other) noexcept;

    explicit operator bool() const noexcept {
        return Memory_ != nullptr;
    }

    void MarkPending() noexcept;
    void Commit() noexcept;
    void Reset() noexcept;

private:
    friend class TCacheMemory;

    TKeepReservation(TCacheMemory& memory, ui64 bytes) noexcept
        : Memory_(&memory)
        , Bytes_(bytes)
    {}

private:
    TCacheMemory* Memory_ = nullptr;
    ui64 Bytes_ = 0;
    bool Pending_ = false;
};

} // namespace NKikimr::NSharedCache
