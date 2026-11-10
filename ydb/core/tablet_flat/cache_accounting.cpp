#include "cache_accounting.h"

#include <util/generic/algorithm.h>

#include <utility>

namespace NKikimr::NSharedCache {

class TCacheAccount final : public TAtomicRefCount<TCacheAccount>, public TSharedData::IOwner {
public:
    class TMetadataOwner final : public TSharedData::IOwner {
    public:
        explicit TMetadataOwner(TCacheAccount& account) noexcept
            : Account_(account)
        {}

        size_t GetAllocationSize(const char* data) const noexcept override {
            return BuiltinAllocationSize(data);
        }

        void* FindAllocationTag(const void* key) const noexcept override {
            if (key == TSharedData::AllocationTagKey<TMetadataOwner>()) {
                return const_cast<TMetadataOwner*>(this);
            }
            return Account_.FindAllocationTag(key);
        }

        void Deallocate(char* data) noexcept override {
            Account_.FreeBuffer(data, true);
        }

    private:
        TCacheAccount& Account_;
    };

    TCacheAccount()
        : MetadataOwner(*this)
    {}

    std::atomic<ui64> Buffers{ 0 };
    std::atomic<ui64> MetadataBuffers{ 0 };
    std::atomic<ui64> Metadata{ 0 };
    std::atomic<ui64> PendingBuffers{ 0 };
    TMetadataOwner MetadataOwner;

    size_t GetAllocationSize(const char* data) const noexcept override {
        return BuiltinAllocationSize(data);
    }

    void* FindAllocationTag(const void* key) const noexcept override {
        if (key == TSharedData::AllocationTagKey<TCacheAccount>()) {
            return const_cast<TCacheAccount*>(this);
        }
        return nullptr;
    }

    void Deallocate(char* data) noexcept override {
        FreeBuffer(data, false);
    }

    void FreeBuffer(char* data, bool metadata) noexcept {
        const ui64 bytes = BuiltinAllocationSize(data);
        DeallocateBuiltin(data);
        if (metadata) {
            MetadataBuffers.fetch_sub(bytes, std::memory_order_relaxed);
        }
        Buffers.fetch_sub(bytes, std::memory_order_relaxed);
        UnRef();
    }

    void BeforeCharge(bool descriptor) const noexcept {
        if (Hook_.Function) {
            Hook_.Function(Hook_.Context, descriptor);
        }
    }

private:
    friend class TCacheAccounting;
    TCacheAccounting::TRegistrationHook Hook_;
};

namespace {

    struct TAllocationCharge {
        TAllocationCharge(TIntrusivePtr<TCacheAccount> account, ui64 bytes) noexcept
            : Account(std::move(account))
            , AllocationBytes(bytes)
        {}

        ~TAllocationCharge() {
            if (Metadata.load(std::memory_order_relaxed)) {
                Account->MetadataBuffers.fetch_sub(AllocationBytes, std::memory_order_relaxed);
            }
            Account->Buffers.fetch_sub(ChargedBytes, std::memory_order_relaxed);
        }

        void ChargeTotal() noexcept {
            Account->Buffers.fetch_add(AllocationBytes, std::memory_order_relaxed);
            ChargedBytes = AllocationBytes;
        }

        void MarkMetadata() noexcept {
            if (!Metadata.exchange(true, std::memory_order_acq_rel)) {
                Account->MetadataBuffers.fetch_add(AllocationBytes, std::memory_order_relaxed);
            }
        }

        const TIntrusivePtr<TCacheAccount> Account;
        const ui64 AllocationBytes;
        TAllocationCharge* Next = nullptr;
        ui64 ChargedBytes = 0;
        std::atomic<bool> Metadata{ false };
    };

    // The existing SharedData owner destroys this tag after the allocation's final release
    class TAllocationTag {
    public:
        ~TAllocationTag() {
            auto* charge = Charges_.load(std::memory_order_relaxed);
            while (charge) {
                auto* next = charge->Next;
                delete charge;
                charge = next;
            }
        }

        void Account(const TIntrusivePtr<TCacheAccount>& account, ui64 bytes, bool total, bool metadata) {
            auto* head = Charges_.load(std::memory_order_acquire);
            THolder<TAllocationCharge> candidate;
            for (;;) {
                for (auto* charge = head; charge; charge = charge->Next) {
                    if (charge->Account == account) {
                        if (metadata) {
                            charge->MarkMetadata();
                        }
                        return;
                    }
                }
                if (!candidate) {
                    candidate = MakeHolder<TAllocationCharge>(account, bytes);
                }
                candidate->Next = head;
                if (Charges_.compare_exchange_weak(
                        head, candidate.Get(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                    // The caller owns the buffer until both charges are established
                    auto* charge = candidate.Release();
                    if (total) {
                        account->BeforeCharge(false);
                        charge->ChargeTotal();
                    }
                    if (metadata) {
                        charge->MarkMetadata();
                    }
                    return;
                }
            }
        }

    private:
        std::atomic<TAllocationCharge*> Charges_{ nullptr };
    };

} // namespace

// Fixed descriptor/Steps storage; Raw is registered through its SharedData allocation tag
class TMetadataRecord {
public:
    explicit TMetadataRecord(TIntrusivePtr<TCacheAccount> account) noexcept
        : Account(std::move(account))
    {}

    ~TMetadataRecord() {
        Account->Metadata.fetch_sub(Bytes, std::memory_order_relaxed);
    }

    void Charge(ui64 bytes) noexcept {
        Account->Metadata.fetch_add(bytes, std::memory_order_relaxed);
        Bytes = bytes;
    }

    const TIntrusivePtr<TCacheAccount> Account;
    TMetadataRecord* Next = nullptr;
    ui64 Bytes = 0;
};

TCacheAccounting::TCacheAccounting()
    : Account_(MakeIntrusive<TCacheAccount>())
{}

TCacheAccounting::TCacheAccounting(TIntrusivePtr<TCacheAccount> account) noexcept
    : Account_(std::move(account))
{}

TCacheAccounting::TCacheAccounting(TCacheAccounting&& other) noexcept = default;
TCacheAccounting& TCacheAccounting::operator=(TCacheAccounting&& other) noexcept = default;
TCacheAccounting::~TCacheAccounting() = default;

TCacheAccounting TCacheAccounting::Share() const noexcept {
    return TCacheAccounting(Account_);
}

void TCacheAccounting::AccountBuffer(const TSharedData& buffer) const {
    AccountBuffer(buffer, false);
}

void TCacheAccounting::AccountMetadataBuffer(const TSharedData& buffer) const {
    AccountBuffer(buffer, true);
}

void TCacheAccounting::AccountBuffer(const TSharedData& buffer, bool metadata) const {
    if (!buffer.data()) {
        return;
    }
    const bool own = buffer.FindAllocationTag<TCacheAccount>() == Account_.Get();
    if (own && (!metadata || buffer.FindAllocationTag<TCacheAccount::TMetadataOwner>() == &Account_->MetadataOwner)) {
        return;
    }
    const ui64 bytes = buffer.GetAllocationSize();
    TSharedData::IOwner* owner = Account_.Get();
    if (metadata) {
        owner = &Account_->MetadataOwner;
    }
    if (!own && buffer.TrySetBuiltinOwner(*owner)) {
        // Input ownership protects both shared views until the lifetime reference and charge exist
        Account_->Ref();
        Account_->BeforeCharge(false);
        Account_->Buffers.fetch_add(bytes, std::memory_order_relaxed);
        if (metadata) {
            Account_->MetadataBuffers.fetch_add(bytes, std::memory_order_relaxed);
        }
        return;
    }
    const bool adopted = buffer.FindAllocationTag<TCacheAccount>() == Account_.Get();
    if (adopted &&
        (!metadata || buffer.FindAllocationTag<TCacheAccount::TMetadataOwner>() == &Account_->MetadataOwner)) {
        return;
    }
    // Our ordinary owner needs only classification; custom/foreign owners also need a total charge
    buffer.GetOrCreateAllocationTag<TAllocationTag>().Account(Account_, bytes, !adopted, metadata);
}

void TCacheAccounting::AccountMetadata(const NPageCollection::IPageCollection& collection) const {
    auto* head = collection.MetadataAccounts_.load(std::memory_order_acquire);
    THolder<TMetadataRecord> candidate;
    ui64 fixedBytes = 0;
    for (;;) {
        for (auto* record = head; record; record = record->Next) {
            if (record->Account == Account_) {
                return;
            }
        }
        if (!candidate) {
            const i64 footprint = collection.GetMetadataMemoryUsage();
            Y_ENSURE(footprint >= 0, "Negative metadata footprint");
            const auto* raw = collection.GetMetadataBuffer();
            const ui64 allocationBytes = raw ? raw->GetAllocationSize() : 0;
            Y_ENSURE(ui64(footprint) >= allocationBytes, "Metadata footprint omits its backing allocation");
            if (raw) {
                AccountMetadataBuffer(*raw);
            }
            fixedBytes = ui64(footprint) - allocationBytes;
            candidate = MakeHolder<TMetadataRecord>(Account_);
        }
        candidate->Next = head;
        if (collection.MetadataAccounts_.compare_exchange_weak(
                head, candidate.Get(), std::memory_order_acq_rel, std::memory_order_acquire)) {
            // The registering caller owns the descriptor until this charge is established
            Account_->BeforeCharge(true);
            candidate.Release()->Charge(fixedBytes);
            return;
        }
    }
}

void TCacheAccounting::SetRegistrationHook(TRegistrationHook hook) noexcept {
    Account_->Hook_ = hook;
}

ui64 TCacheAccounting::BufferBytes() const noexcept {
    const ui64 all = Account_->Buffers.load(std::memory_order_relaxed);
    const ui64 metadata = Account_->MetadataBuffers.load(std::memory_order_relaxed);
    return all - Min(all, metadata);
}

ui64 TCacheAccounting::MetadataBytes() const noexcept {
    return Account_->Metadata.load(std::memory_order_relaxed) +
           Account_->MetadataBuffers.load(std::memory_order_relaxed);
}

ui64 TCacheAccounting::GetMemoryUsage() const noexcept {
    return Account_->Buffers.load(std::memory_order_acquire) + Account_->Metadata.load(std::memory_order_relaxed);
}

ui64 TCacheAccounting::PendingBufferBytes() const noexcept {
    return Account_->PendingBuffers.load(std::memory_order_acquire);
}

TCachePendingBuffers TCacheAccounting::TrackPendingBuffers() const noexcept {
    return TCachePendingBuffers(Account_);
}

TCachePendingBuffers::TCachePendingBuffers() noexcept = default;

TCachePendingBuffers::~TCachePendingBuffers() {
    Reset();
}

TCachePendingBuffers::TCachePendingBuffers(TIntrusivePtr<TCacheAccount> account, ui64 bytes) noexcept
    : Account_(std::move(account))
    , Bytes_(bytes)
{}

TCachePendingBuffers::TCachePendingBuffers(TCachePendingBuffers&& other) noexcept
    : Account_(std::move(other.Account_))
    , Bytes_(std::exchange(other.Bytes_, 0))
{}

TCachePendingBuffers& TCachePendingBuffers::operator=(TCachePendingBuffers&& other) noexcept {
    if (this != &other) {
        Reset();
        Account_ = std::move(other.Account_);
        Bytes_ = std::exchange(other.Bytes_, 0);
    }
    return *this;
}

void TCachePendingBuffers::AccountBuffer(const TSharedData& buffer, bool metadata) {
    Y_DEBUG_ABORT_UNLESS(Account_);
    const ui64 bytes = buffer.GetAllocationSize();
    Account_->PendingBuffers.fetch_add(bytes, std::memory_order_relaxed);
    // Publish classification before the physical charge can be observed by maintenance
    std::atomic_thread_fence(std::memory_order_release);
    try {
        TCacheAccounting(Account_).AccountBuffer(buffer, metadata);
    } catch (...) {
        Account_->PendingBuffers.fetch_sub(bytes, std::memory_order_release);
        throw;
    }
    Bytes_ += bytes;
}

TCachePendingBuffers TCachePendingBuffers::Take(ui64 bytes) noexcept {
    if (!Account_) {
        return {};
    }
    Y_DEBUG_ABORT_UNLESS(bytes <= Bytes_);
    Bytes_ -= bytes;
    return TCachePendingBuffers(Account_, bytes);
}

void TCachePendingBuffers::Reset() noexcept {
    if (Account_) {
        // A successful fetch publishes Resident accounting before ending this classification
        if (Bytes_ != 0) {
            Account_->PendingBuffers.fetch_sub(std::exchange(Bytes_, 0), std::memory_order_release);
        }
        Account_.Drop();
    }
}

} // namespace NKikimr::NSharedCache

namespace NKikimr::NPageCollection {

IPageCollection::~IPageCollection() {
    auto* record = MetadataAccounts_.load(std::memory_order_relaxed);
    while (record) {
        auto* next = record->Next;
        delete record;
        record = next;
    }
}

} // namespace NKikimr::NPageCollection
