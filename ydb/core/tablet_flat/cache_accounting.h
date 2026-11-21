#pragma once

#include "flat_sausage_gut.h"

namespace NKikimr::NSharedCache {

class TCacheAccount;
class TCacheAccountingTestAccess;
class TCacheAccounting;

// Allocated fetch buffers stay prepaid until installation, independently of cache/TLS lifetime
class TCachePendingBuffers {
public:
    TCachePendingBuffers() noexcept;
    ~TCachePendingBuffers();
    TCachePendingBuffers(TCachePendingBuffers&& other) noexcept;
    TCachePendingBuffers& operator=(TCachePendingBuffers&& other) noexcept;
    TCachePendingBuffers(const TCachePendingBuffers&) = delete;
    TCachePendingBuffers& operator=(const TCachePendingBuffers&) = delete;

    // Register each fresh fetch allocation once, while the caller owns its buffer
    void AccountBuffer(const TSharedData& buffer, bool metadata = false);
    TCachePendingBuffers Take(ui64 bytes) noexcept;
    void Reset() noexcept;

private:
    friend class TCacheAccounting;

    explicit TCachePendingBuffers(TIntrusivePtr<TCacheAccount> account, ui64 bytes = 0) noexcept;

    TIntrusivePtr<TCacheAccount> Account_;
    ui64 Bytes_ = 0;
};

// Registrations follow allocation/object lifetime, including references outside the cache
class TCacheAccounting {
public:
    TCacheAccounting();
    ~TCacheAccounting();
    TCacheAccounting(const TCacheAccounting&) = delete;
    TCacheAccounting& operator=(const TCacheAccounting&) = delete;
    TCacheAccounting(TCacheAccounting&& other) noexcept;
    TCacheAccounting& operator=(TCacheAccounting&& other) noexcept;

    TCacheAccounting Share() const noexcept;
    TCachePendingBuffers TrackPendingBuffers() const noexcept;

    // The caller owns the allocation/object throughout registration
    void AccountBuffer(const TSharedData& buffer) const;
    void AccountMetadataBuffer(const TSharedData& buffer) const;
    void AccountMetadata(const NPageCollection::IPageCollection& collection) const;

    // Exact after concurrent registration/destruction settles; hard admission uses its own ledger
    ui64 BufferBytes() const noexcept;
    // Metadata objects, Steps and their Raw/Globs allocations
    ui64 MetadataBytes() const noexcept;
    ui64 GetMemoryUsage() const noexcept;
    ui64 PendingBufferBytes() const noexcept;

private:
    friend class TCacheAccount;
    friend class TCacheAccountingTestAccess;
    friend class TCachePendingBuffers;

    struct TRegistrationHook {
        void* Context = nullptr;
        void (*Function)(void*, bool descriptor) noexcept = nullptr;
    };

    void AccountBuffer(const TSharedData& buffer, bool metadata) const;
    void SetRegistrationHook(TRegistrationHook hook) noexcept;
    explicit TCacheAccounting(TIntrusivePtr<TCacheAccount> account) noexcept;

    TIntrusivePtr<TCacheAccount> Account_;
};

} // namespace NKikimr::NSharedCache
