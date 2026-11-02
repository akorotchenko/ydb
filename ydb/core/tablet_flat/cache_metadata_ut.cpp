#include "cache_metadata.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>

#include <thread>
#include <type_traits>

namespace NKikimr::NSharedCache {

class TCacheMetadataTestAccess {
public:
    static void SetHook(
        TCacheMetadata& metadata, void* context, void (*hook)(void*, EMetadataReconcile) noexcept) noexcept {
        metadata.SetReconcileHook({ context, hook });
    }
};

static_assert(!std::is_copy_constructible_v<TMetadataLease>);
static_assert(!std::is_move_constructible_v<TMetadataLease>);

namespace {

    class TDescriptor final : public NPageCollection::IPageCollection {
    public:
        TDescriptor(TSharedData raw, ui64 fixedBytes, ui32 id = 1, std::atomic<ui32>* destroyed = nullptr)
            : Id_(77, 1, id)
            , Raw_(std::move(raw))
            , FixedBytes_(fixedBytes)
            , Destroyed_(destroyed)
        {}

        ~TDescriptor() override {
            if (Destroyed_) {
                Destroyed_->fetch_add(1, std::memory_order_relaxed);
            }
        }

        const TLogoBlobID& Label() const noexcept override {
            return Id_;
        }

        ui32 Total() const noexcept override {
            return 0;
        }

        NPageCollection::TInfo Page(ui32) const override {
            return {};
        }

        NPageCollection::TBorder Bounds(ui32) const override {
            return {};
        }

        NPageCollection::TBorder Bounds(const NPageCollection::TPageLocation&) const override {
            return {};
        }

        NPageCollection::TGlobId Glob(ui32) const override {
            return {};
        }

        bool Verify(ui32, TArrayRef<const char>) const override {
            return false;
        }

        bool Verify(const NPageCollection::TPageLocation&, TArrayRef<const char>) const override {
            return false;
        }

        size_t BackingSize() const noexcept override {
            return 0;
        }

        NPageCollection::TPageLocation GetLocation(ui32) const override {
            return {};
        }

        i64 GetMetadataMemoryUsage() const noexcept override {
            return FixedBytes_ + Raw_.GetAllocationSize();
        }

        const TSharedData* GetMetadataBuffer() const noexcept override {
            return Raw_.data() ? &Raw_ : nullptr;
        }

    private:
        const TLogoBlobID Id_;
        const TSharedData Raw_;
        const ui64 FixedBytes_;
        std::atomic<ui32>* const Destroyed_;
    };

    void WaitFor(const std::atomic<bool>& ready) {
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        while (!ready.load(std::memory_order_acquire)) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Metadata accounting updater did not finish");
            std::this_thread::yield();
        }
    }

    struct TPause {
        explicit TPause(EMetadataReconcile kind)
            : Kind(kind)
        {}

        static void OnReconcile(void* context, EMetadataReconcile kind) noexcept {
            auto& self = *static_cast<TPause*>(context);
            if (kind != self.Kind || self.Calls.fetch_add(1, std::memory_order_relaxed)) {
                return;
            }
            self.Entered.store(true, std::memory_order_release);
            while (!self.Resume.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        }

        const EMetadataReconcile Kind;
        std::atomic<ui32> Calls{ 0 };
        std::atomic<bool> Entered{ false };
        std::atomic<bool> Resume{ false };
    };

} // namespace

Y_UNIT_TEST_SUITE(TSharedCacheMetadataAccounting) {
    Y_UNIT_TEST(DescriptorWithoutRawCountsItsFixedFootprint) {
        TCacheMetadata metadata;
        auto lease = metadata.Track(MakeIntrusiveConst<TDescriptor>(TSharedData{}, 12));
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 12);
        lease.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(DeduplicatesDescriptorGroupsAndLeaseCopies) {
        TCacheMetadata metadata;
        auto raw = TSharedData::Uninitialized(64);
        auto descriptor = MakeIntrusiveConst<TDescriptor>(raw, 12);
        auto first = metadata.Track(descriptor);
        auto copy = first;
        auto second = metadata.Track(descriptor);
        const ui64 expected = 12 + raw.GetAllocationSize();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), expected);
        first.Reset();
        second.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), expected);
        copy.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
        UNIT_ASSERT(descriptor);
    }

    Y_UNIT_TEST(DeduplicatesSharedRawButKeepsBothFixedFootprints) {
        TCacheMetadata metadata;
        auto raw = TSharedData::Uninitialized(64);
        auto first = metadata.Track(MakeIntrusiveConst<TDescriptor>(raw, 12, 1));
        auto second = metadata.Track(MakeIntrusiveConst<TDescriptor>(raw, 20, 2));
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 32 + raw.GetAllocationSize());
        first.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 20 + raw.GetAllocationSize());
        second.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(DistinctAllocationsKeepDistinctCharges) {
        TCacheMetadata metadata;
        auto firstRaw = TSharedData::Uninitialized(64);
        auto secondRaw = TSharedData::Uninitialized(64);
        auto first = metadata.Track(MakeIntrusiveConst<TDescriptor>(firstRaw, 12, 1));
        auto second = metadata.Track(MakeIntrusiveConst<TDescriptor>(secondRaw, 20, 2));
        UNIT_ASSERT_VALUES_EQUAL(
            metadata.GetMemoryUsage(), 32 + firstRaw.GetAllocationSize() + secondRaw.GetAllocationSize());
        first.Reset();
        second.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(PageOverlapBeforeMetadataRestoresRetainedAllocation) {
        TCacheMetadata metadata;
        auto raw = TSharedData::Uninitialized(128);
        const ui64 allocation = raw.GetAllocationSize();
        raw.TrimBack(64);
        const ui64 pageCharge = 64 + TSharedData::OverheadSize;
        metadata.PreparePageCharge(raw);
        metadata.AddPageCharge(raw, pageCharge);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
        auto descriptor = MakeIntrusiveConst<TDescriptor>(raw, 12);
        auto lease = metadata.Track(descriptor);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 12 + allocation - pageCharge);
        auto buffer = std::move(raw).Extract();
        metadata.RemovePageCharge(buffer, pageCharge);
        buffer = {};
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 12 + allocation);
        lease.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(MultiplePageChargesSaturateTheRawCredit) {
        TCacheMetadata metadata;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 allocation = raw.GetAllocationSize();
        metadata.PreparePageCharge(raw);
        auto lease = metadata.Track(MakeIntrusiveConst<TDescriptor>(raw, 12));
        metadata.AddPageCharge(raw, allocation);
        metadata.AddPageCharge(raw, allocation);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 12);
        metadata.RemovePageCharge(raw, allocation);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 12);
        metadata.RemovePageCharge(raw, allocation);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 12 + allocation);
        lease.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(AccountsHaveIndependentIdentityForSharedStorage) {
        TCacheMetadata first;
        TCacheMetadata second;
        auto raw = TSharedData::Uninitialized(64);
        auto descriptor = MakeIntrusiveConst<TDescriptor>(raw, 12);
        auto firstLease = first.Track(descriptor);
        auto secondLease = second.Track(descriptor);
        const ui64 expected = 12 + raw.GetAllocationSize();
        first.PreparePageCharge(raw);
        first.AddPageCharge(raw, raw.GetAllocationSize());
        UNIT_ASSERT_VALUES_EQUAL(first.GetMemoryUsage(), 12);
        UNIT_ASSERT_VALUES_EQUAL(second.GetMemoryUsage(), expected);
        first.RemovePageCharge(raw, raw.GetAllocationSize());
        firstLease.Reset();
        UNIT_ASSERT_VALUES_EQUAL(first.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(second.GetMemoryUsage(), expected);
        secondLease.Reset();
        UNIT_ASSERT_VALUES_EQUAL(second.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(LeaseRetainsDescriptorAfterFacadeDestruction) {
        std::atomic<ui32> destroyed = 0;
        TIntrusivePtr<TMetadataLease> lease;
        {
            TCacheMetadata metadata;
            auto descriptor = MakeIntrusiveConst<TDescriptor>(TSharedData::Uninitialized(64), 12, 1, &destroyed);
            lease = metadata.Track(std::move(descriptor));
            UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 0);
        lease.Reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
    }

    Y_UNIT_TEST(PausedDescriptorPublisherFoldsLastDropAndReacquisition) {
        TCacheMetadata metadata;
        auto raw = TSharedData::Uninitialized(64);
        auto descriptor = MakeIntrusiveConst<TDescriptor>(raw, 12);
        auto first = metadata.Track(descriptor);
        TPause pause(EMetadataReconcile::Descriptor);
        TCacheMetadataTestAccess::SetHook(metadata, &pause, TPause::OnReconcile);
        TIntrusivePtr<TMetadataLease> final;
        std::atomic<bool> updated = false;
        std::thread dropping([lease = std::move(first)]() mutable {
            lease.Reset();
        });
        std::thread updating;
        Y_DEFER {
            pause.Resume.store(true, std::memory_order_release);
            if (updating.joinable()) {
                updating.join();
            }
            if (dropping.joinable()) {
                dropping.join();
            }
            TCacheMetadataTestAccess::SetHook(metadata, nullptr, nullptr);
        };
        WaitFor(pause.Entered);
        updating = std::thread([&] {
            auto temporary = metadata.Track(descriptor);
            temporary.Reset();
            final = metadata.Track(descriptor);
            updated.store(true, std::memory_order_release);
        });
        WaitFor(updated);
        pause.Resume.store(true, std::memory_order_release);
        updating.join();
        dropping.join();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 12 + raw.GetAllocationSize());
        final.Reset();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(PausedRawPublisherSurvivesLastDescriptorDestruction) {
        TCacheMetadata metadata;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        std::atomic<ui32> destroyed = 0;
        auto descriptor = MakeIntrusiveConst<TDescriptor>(raw, 12, 1, &destroyed);
        auto lease = metadata.Track(descriptor);
        descriptor.Reset();
        metadata.PreparePageCharge(raw);
        TPause pause(EMetadataReconcile::Buffer);
        TCacheMetadataTestAccess::SetHook(metadata, &pause, TPause::OnReconcile);
        std::atomic<bool> released = false;
        std::thread publishing([&] {
            metadata.AddPageCharge(raw, bytes);
        });
        std::thread releasing;
        Y_DEFER {
            pause.Resume.store(true, std::memory_order_release);
            if (releasing.joinable()) {
                releasing.join();
            }
            if (publishing.joinable()) {
                publishing.join();
            }
            TCacheMetadataTestAccess::SetHook(metadata, nullptr, nullptr);
        };
        WaitFor(pause.Entered);
        releasing = std::thread([&] {
            lease.Reset();
            released.store(true, std::memory_order_release);
        });
        WaitFor(released);
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
        pause.Resume.store(true, std::memory_order_release);
        releasing.join();
        publishing.join();
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
        metadata.RemovePageCharge(raw, bytes);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(ConcurrentDescriptorAndPageUpdatesSettleExactly) {
        TCacheMetadata metadata;
        auto raw = TSharedData::Uninitialized(64);
        auto first = MakeIntrusiveConst<TDescriptor>(raw, 12, 1);
        auto second = MakeIntrusiveConst<TDescriptor>(raw, 20, 2);
        metadata.PreparePageCharge(raw);
        std::atomic<bool> start = false;
        TVector<std::thread> workers;
        for (ui32 index = 0; index < 4; ++index) {
            workers.emplace_back([&, index] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (ui32 iteration = 0; iteration < 500; ++iteration) {
                    auto lease = metadata.Track(index % 2 ? first : second);
                    metadata.AddPageCharge(raw, 32);
                    metadata.RemovePageCharge(raw, 32);
                }
            });
        }
        start.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetMemoryUsage(), 0);
    }
}

} // namespace NKikimr::NSharedCache
