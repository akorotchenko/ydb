#pragma once

#include "flat_sausage_gut.h"

namespace NKikimr::NSharedCache {

class TMetadataAccount;
class TMetadataRecord;
class TCacheMetadataTestAccess;

enum class EMetadataReconcile : ui8 {
    Descriptor,
    Buffer,
};

// Descriptor ownership and accounting travel together as a value
class TMetadataRetainer {
public:
    TMetadataRetainer() noexcept = default;
    TMetadataRetainer(const TMetadataRetainer& other) noexcept;
    TMetadataRetainer(TMetadataRetainer&& other) noexcept;
    TMetadataRetainer& operator=(const TMetadataRetainer& other) noexcept;
    TMetadataRetainer& operator=(TMetadataRetainer&& other) noexcept;
    ~TMetadataRetainer();

    explicit operator bool() const noexcept {
        return Record_ != nullptr;
    }

    void Reset() noexcept;

    const TIntrusiveConstPtr<NPageCollection::IPageCollection>& PageCollection() const noexcept {
        return PageCollection_;
    }

private:
    friend class TCacheMetadata;

    TMetadataRetainer(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, TMetadataRecord& record) noexcept;

    void Swap(TMetadataRetainer& other) noexcept;

    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection_;
    TMetadataRecord* Record_ = nullptr;
};

class TCacheMetadata {
public:
    TCacheMetadata();
    ~TCacheMetadata();
    TCacheMetadata(const TCacheMetadata&) = delete;
    TCacheMetadata& operator=(const TCacheMetadata&) = delete;

    // Footprint and buffer identity must stay fixed for the descriptor's entire lifetime
    TMetadataRetainer Track(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection);

    bool Owns(const TMetadataRetainer& retainer) const noexcept;

    // Allocation may occur here, before entering a no-throw native publication path
    void PreparePageCharge(const TSharedData& buffer);

    void AddPageCharge(const TSharedData& buffer, ui64 bytes) noexcept;
    void AddPageCharge(const TSharedData::TBuffer& buffer, ui64 bytes) noexcept;
    void RemovePageCharge(const TSharedData& buffer, ui64 bytes) noexcept;
    void RemovePageCharge(const TSharedData::TBuffer& buffer, ui64 bytes) noexcept;

    // Descriptor and buffer bytes, excluding tracker bookkeeping; exact after concurrent updates settle
    // No hard-admission decision depends on a partial fold
    ui64 GetMemoryUsage() const noexcept;

private:
    friend class TCacheMetadataTestAccess;
    friend class TMetadataAccount;

    struct TReconcileHook {
        void* Context = nullptr;
        void (*Function)(void*, EMetadataReconcile) noexcept = nullptr;
    };

    void SetReconcileHook(TReconcileHook hook) noexcept;

    TIntrusivePtr<TMetadataAccount> Account_;
};

} // namespace NKikimr::NSharedCache
