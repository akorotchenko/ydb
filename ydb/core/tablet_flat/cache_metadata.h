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

// Copies retain one cache ownership group, without acquiring a native cache item
class TMetadataLease final : public TAtomicRefCount<TMetadataLease> {
public:
    ~TMetadataLease();
    TMetadataLease(const TMetadataLease&) = delete;
    TMetadataLease& operator=(const TMetadataLease&) = delete;
    TMetadataLease(TMetadataLease&&) = delete;
    TMetadataLease& operator=(TMetadataLease&&) = delete;

    const TIntrusiveConstPtr<NPageCollection::IPageCollection>& PageCollection() const noexcept {
        return PageCollection_;
    }

private:
    friend class TCacheMetadata;

    TMetadataLease(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, TMetadataRecord& record) noexcept;

    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection_;
    TMetadataRecord& Record_;
};

class TCacheMetadata {
public:
    TCacheMetadata();
    ~TCacheMetadata();
    TCacheMetadata(const TCacheMetadata&) = delete;
    TCacheMetadata& operator=(const TCacheMetadata&) = delete;

    // Footprint and buffer identity must stay fixed for the descriptor's entire lifetime
    TIntrusivePtr<TMetadataLease> Track(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection);

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
