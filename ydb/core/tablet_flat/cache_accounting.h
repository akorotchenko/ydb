#pragma once

#include "flat_sausage_gut.h"

namespace NKikimr::NSharedCache {

class TCacheAccount;
class TCacheAccountingTestAccess;

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

    // The caller owns the allocation/object throughout registration
    void AccountBuffer(const TSharedData& buffer) const;
    void AccountMetadataBuffer(const TSharedData& buffer) const;
    void AccountMetadata(const NPageCollection::IPageCollection& collection) const;

    // Exact after concurrent registration/destruction settles; hard admission uses its own ledger
    ui64 BufferBytes() const noexcept;
    // Metadata objects, Steps and their Raw/Globs allocations
    ui64 MetadataBytes() const noexcept;
    ui64 GetMemoryUsage() const noexcept;

private:
    friend class TCacheAccount;
    friend class TCacheAccountingTestAccess;

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
