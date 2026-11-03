#include "cache_metadata.h"

#include <util/generic/algorithm.h>

#include <utility>

namespace NKikimr::NSharedCache {

class TMetadataAccount final : public TAtomicRefCount<TMetadataAccount> {
public:
    void Update(ui64 previous, ui64 current) noexcept {
        if (current > previous) {
            Bytes_.fetch_add(current - previous, std::memory_order_relaxed);
        } else if (current < previous) {
            Bytes_.fetch_sub(previous - current, std::memory_order_relaxed);
        }
    }

    ui64 Read() const noexcept {
        return Bytes_.load(std::memory_order_relaxed);
    }

    void BeforeReconcile(EMetadataReconcile kind) const noexcept {
        if (Hook_.Function) {
            Hook_.Function(Hook_.Context, kind);
        }
    }

private:
    friend class TCacheMetadata;

    std::atomic<ui64> Bytes_{ 0 };
    TCacheMetadata::TReconcileHook Hook_;
};

namespace {

    constexpr ui32 Publishing = 1;
    constexpr ui32 Changed = 2;

    template <class TFold>
    void Reconcile(std::atomic<ui32>& state, TFold&& fold) noexcept {
        if (state.fetch_or(Publishing | Changed, std::memory_order_acq_rel) & Publishing) {
            return;
        }
        for (;;) {
            state.fetch_and(~Changed, std::memory_order_acquire);
            fold();
            ui32 expected = Publishing;
            if (state.compare_exchange_strong(expected, 0, std::memory_order_release, std::memory_order_relaxed)) {
                return;
            }
        }
    }

    class TBufferCharge {
    public:
        TBufferCharge(TIntrusivePtr<TMetadataAccount> account, ui64 bytes) noexcept
            : Account(std::move(account))
            , AllocationBytes_(bytes)
        {}

        ~TBufferCharge() {
            Y_DEBUG_ABORT_UNLESS(!MetadataReferences_.load(std::memory_order_relaxed));
            Y_DEBUG_ABORT_UNLESS(!PageBytes_.load(std::memory_order_relaxed));
            Y_DEBUG_ABORT_UNLESS(!PublishingState_.load(std::memory_order_relaxed));
            Y_DEBUG_ABORT_UNLESS(!ReportedBytes_);
        }

        void AddMetadataReference() noexcept {
            MetadataReferences_.fetch_add(1, std::memory_order_relaxed);
            Publish();
        }

        void RemoveMetadataReference() noexcept {
            const ui64 previous = MetadataReferences_.fetch_sub(1, std::memory_order_relaxed);
            Y_ABORT_UNLESS(previous);
            Publish();
        }

        void AddPageBytes(ui64 bytes) noexcept {
            PageBytes_.fetch_add(bytes, std::memory_order_relaxed);
            Publish();
        }

        void RemovePageBytes(ui64 bytes) noexcept {
            const ui64 previous = PageBytes_.fetch_sub(bytes, std::memory_order_relaxed);
            Y_ABORT_UNLESS(previous >= bytes);
            Publish();
        }

        // Assigned before list publication; account identity stays alive with the allocation
        const TIntrusivePtr<TMetadataAccount> Account;
        TBufferCharge* Next = nullptr;

    private:
        void Publish() noexcept {
            Reconcile(PublishingState_, [this]() noexcept {
                Account->BeforeReconcile(EMetadataReconcile::Buffer);
                const bool referenced = MetadataReferences_.load(std::memory_order_relaxed) != 0;
                const ui64 pageBytes = PageBytes_.load(std::memory_order_relaxed);
                const ui64 bytes = referenced ? AllocationBytes_ - Min(AllocationBytes_, pageBytes) : 0;
                Account->Update(ReportedBytes_, bytes);
                ReportedBytes_ = bytes;
            });
        }

        const ui64 AllocationBytes_;
        std::atomic<ui64> MetadataReferences_{ 0 };
        std::atomic<ui64> PageBytes_{ 0 };
        std::atomic<ui32> PublishingState_{ 0 };
        ui64 ReportedBytes_ = 0; // Only the elected publisher accesses this contribution
    };

    class TMetadataBufferTag {
    public:
        ~TMetadataBufferTag() {
            auto* charge = Charges_.load(std::memory_order_relaxed);
            while (charge) {
                auto* next = charge->Next;
                delete charge;
                charge = next;
            }
        }

        TBufferCharge* Find(TMetadataAccount* account) const noexcept {
            for (auto* charge = Charges_.load(std::memory_order_acquire); charge; charge = charge->Next) {
                if (charge->Account.Get() == account) {
                    return charge;
                }
            }
            return nullptr;
        }

        TBufferCharge& Get(TIntrusivePtr<TMetadataAccount> account, ui64 bytes) {
            auto* head = Charges_.load(std::memory_order_acquire);
            THolder<TBufferCharge> candidate;
            for (;;) {
                for (auto* charge = head; charge; charge = charge->Next) {
                    if (charge->Account == account) {
                        return *charge;
                    }
                }
                if (!candidate) {
                    candidate = MakeHolder<TBufferCharge>(account, bytes);
                }
                candidate->Next = head;
                if (Charges_.compare_exchange_weak(
                        head, candidate.Get(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                    return *candidate.Release();
                }
            }
        }

    private:
        std::atomic<TBufferCharge*> Charges_{ nullptr };
    };

    TBufferCharge& PreparedCharge(TMetadataBufferTag* tag, TMetadataAccount* account) noexcept {
        Y_ABORT_UNLESS(tag);
        auto* charge = tag->Find(account);
        Y_ABORT_UNLESS(charge);
        return *charge;
    }

} // namespace

// Shared accounting entry for one descriptor and cache account
class TMetadataRecord {
public:
    TMetadataRecord(TIntrusivePtr<TMetadataAccount> account, const NPageCollection::IPageCollection& collection)
        : Account(std::move(account))
    {
        const i64 footprint = collection.GetMetadataMemoryUsage();
        Y_ENSURE(footprint >= 0, "Negative metadata footprint");
        const auto* raw = collection.GetMetadataBuffer();
        const ui64 allocationBytes = raw ? raw->GetAllocationSize() : 0;
        Y_ENSURE(ui64(footprint) >= allocationBytes, "Metadata footprint omits its backing allocation");
        FixedBytes_ = ui64(footprint) - allocationBytes;
        if (allocationBytes) {
            auto& tag = raw->GetOrCreateAllocationTag<TMetadataBufferTag>();
            Raw_ = &tag.Get(Account, allocationBytes);
        }
    }

    ~TMetadataRecord() {
        // Derived Raw storage is already gone here; never dereference Raw_ during record cleanup
        Y_DEBUG_ABORT_UNLESS(!References_.load(std::memory_order_relaxed));
        Y_DEBUG_ABORT_UNLESS(!PublishingState_.load(std::memory_order_relaxed));
        Y_DEBUG_ABORT_UNLESS(!ReportedActive_);
    }

    void AddReference() noexcept {
        References_.fetch_add(1, std::memory_order_relaxed);
        Publish();
    }

    void RemoveReference() noexcept {
        const ui64 previous = References_.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous);
        Publish();
    }

    const TIntrusivePtr<TMetadataAccount> Account;
    TMetadataRecord* Next = nullptr;

private:
    void Publish() noexcept {
        Reconcile(PublishingState_, [this]() noexcept {
            Account->BeforeReconcile(EMetadataReconcile::Descriptor);
            const bool active = References_.load(std::memory_order_relaxed) != 0;
            if (active == ReportedActive_) {
                return;
            }
            if (active) {
                Account->Update(0, FixedBytes_);
                if (Raw_) {
                    Raw_->AddMetadataReference();
                }
            } else {
                if (Raw_) {
                    Raw_->RemoveMetadataReference();
                }
                Account->Update(FixedBytes_, 0);
            }
            ReportedActive_ = active;
        });
    }

    ui64 FixedBytes_ = 0;
    TBufferCharge* Raw_ = nullptr; // Borrowed only while a caller owns the descriptor and its Raw allocation
    std::atomic<ui64> References_{ 0 };
    std::atomic<ui32> PublishingState_{ 0 };
    bool ReportedActive_ = false;
};

bool TCacheMetadata::Owns(const TMetadataRetainer& retainer) const noexcept {
    return retainer.Record_ && retainer.Record_->Account == Account_;
}

TMetadataRetainer::TMetadataRetainer(
    TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, TMetadataRecord& record) noexcept
    : PageCollection_(std::move(collection))
    , Record_(&record)
{
    Record_->AddReference();
}

TMetadataRetainer::TMetadataRetainer(const TMetadataRetainer& other) noexcept
    : PageCollection_(other.PageCollection_)
    , Record_(other.Record_)
{
    if (Record_) {
        Record_->AddReference();
    }
}

TMetadataRetainer::TMetadataRetainer(TMetadataRetainer&& other) noexcept
    : PageCollection_(std::move(other.PageCollection_))
    , Record_(std::exchange(other.Record_, nullptr))
{}

TMetadataRetainer& TMetadataRetainer::operator=(const TMetadataRetainer& other) noexcept {
    if (this != &other) {
        TMetadataRetainer copy(other);
        Swap(copy);
    }
    return *this;
}

TMetadataRetainer& TMetadataRetainer::operator=(TMetadataRetainer&& other) noexcept {
    TMetadataRetainer moved(std::move(other));
    Swap(moved);
    return *this;
}

TMetadataRetainer::~TMetadataRetainer() {
    if (Record_) {
        // Keep the descriptor and its Raw allocation alive throughout the accounting update
        Record_->RemoveReference();
    }
}

void TMetadataRetainer::Reset() noexcept {
    TMetadataRetainer{}.Swap(*this);
}

void TMetadataRetainer::Swap(TMetadataRetainer& other) noexcept {
    PageCollection_.Swap(other.PageCollection_);
    std::swap(Record_, other.Record_);
}

TCacheMetadata::TCacheMetadata()
    : Account_(MakeIntrusive<TMetadataAccount>())
{}

TCacheMetadata::~TCacheMetadata() = default;

TMetadataRetainer TCacheMetadata::Track(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection) {
    Y_ENSURE(collection);
    auto* head = collection->MetadataAccounts_.load(std::memory_order_acquire);
    THolder<TMetadataRecord> candidate;
    for (;;) {
        for (auto* record = head; record; record = record->Next) {
            if (record->Account == Account_) {
                return TMetadataRetainer(std::move(collection), *record);
            }
        }
        if (!candidate) {
            candidate = MakeHolder<TMetadataRecord>(Account_, *collection);
        }
        candidate->Next = head;
        if (collection->MetadataAccounts_.compare_exchange_weak(
                head, candidate.Get(), std::memory_order_acq_rel, std::memory_order_acquire)) {
            auto* record = candidate.Release();
            return TMetadataRetainer(std::move(collection), *record);
        }
    }
}

void TCacheMetadata::PreparePageCharge(const TSharedData& buffer) {
    Y_ENSURE(buffer.data());
    auto& tag = buffer.GetOrCreateAllocationTag<TMetadataBufferTag>();
    tag.Get(Account_, buffer.GetAllocationSize());
}

void TCacheMetadata::AddPageCharge(const TSharedData& buffer, ui64 bytes) noexcept {
    PreparedCharge(buffer.FindAllocationTag<TMetadataBufferTag>(), Account_.Get()).AddPageBytes(bytes);
}

void TCacheMetadata::AddPageCharge(const TSharedData::TBuffer& buffer, ui64 bytes) noexcept {
    PreparedCharge(buffer.FindAllocationTag<TMetadataBufferTag>(), Account_.Get()).AddPageBytes(bytes);
}

void TCacheMetadata::RemovePageCharge(const TSharedData& buffer, ui64 bytes) noexcept {
    PreparedCharge(buffer.FindAllocationTag<TMetadataBufferTag>(), Account_.Get()).RemovePageBytes(bytes);
}

void TCacheMetadata::RemovePageCharge(const TSharedData::TBuffer& buffer, ui64 bytes) noexcept {
    PreparedCharge(buffer.FindAllocationTag<TMetadataBufferTag>(), Account_.Get()).RemovePageBytes(bytes);
}

ui64 TCacheMetadata::GetMemoryUsage() const noexcept {
    return Account_->Read();
}

void TCacheMetadata::SetReconcileHook(TReconcileHook hook) noexcept {
    Account_->Hook_ = hook;
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
