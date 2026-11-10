#include "cache_accounting.h"

#include <ydb/library/actors/util/shared_data_backtracing_owner.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>

#include <thread>
#include <type_traits>

namespace NKikimr::NSharedCache {

class TCacheAccountingTestAccess {
public:
    static void SetHook(TCacheAccounting& accounting, void* context, void (*hook)(void*, bool) noexcept) noexcept {
        accounting.SetRegistrationHook({ context, hook });
    }
};

static_assert(std::is_nothrow_move_constructible_v<TCacheAccounting>);
static_assert(std::is_nothrow_move_constructible_v<TCachePendingBuffers>);
static_assert(!std::is_copy_constructible_v<TCachePendingBuffers>);

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

} // namespace

Y_UNIT_TEST_SUITE(TCacheAccountingTest) {
    Y_UNIT_TEST(PendingBuffersMoveAndSplitWithoutChangingAllocationLifetime) {
        TCacheAccounting accounting;
        auto first = TSharedData::Uninitialized(100);
        auto second = TSharedData::Uninitialized(200);
        const ui64 firstBytes = first.GetAllocationSize();
        const ui64 secondBytes = second.GetAllocationSize();
        auto pending = accounting.TrackPendingBuffers();
        pending.AccountBuffer(first);
        pending.AccountBuffer(second, true);
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), firstBytes + secondBytes);
        UNIT_ASSERT_VALUES_EQUAL(accounting.PendingBufferBytes(), firstBytes + secondBytes);
        auto slice = pending.Take(firstBytes);
        TCachePendingBuffers reply = std::move(pending);
        pending.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.PendingBufferBytes(), firstBytes + secondBytes);
        slice.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.PendingBufferBytes(), secondBytes);
        reply.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.PendingBufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), firstBytes + secondBytes);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), secondBytes);
        first = {};
        second = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(PendingBuffersSurviveCacheAccountOwnerAndRefundAbandonedReplies) {
        TCacheAccounting observer;
        TCachePendingBuffers reply;
        TSharedData retained;
        {
            TCacheAccounting accounting;
            observer = accounting.Share();
            retained = TSharedData::Uninitialized(100);
            reply = accounting.TrackPendingBuffers();
            reply.AccountBuffer(retained);
        }
        const ui64 bytes = retained.GetAllocationSize();
        UNIT_ASSERT_VALUES_EQUAL(observer.PendingBufferBytes(), bytes);
        reply = {};
        UNIT_ASSERT_VALUES_EQUAL(observer.PendingBufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(observer.GetMemoryUsage(), bytes);
        retained = {};
        UNIT_ASSERT_VALUES_EQUAL(observer.GetMemoryUsage(), 0);
    }

    Y_UNIT_TEST(EmptyBufferHasNoAllocationCharge) {
        TCacheAccounting accounting;
        accounting.AccountBuffer({});
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(BuiltinBuffersShareOneOwnerPerCacheWithoutWrappers) {
        TCacheAccounting accounting;
        auto first = TSharedData::Uninitialized(64);
        auto second = TSharedData::Uninitialized(128);
        const auto owner = [](const TSharedData& buffer) {
            const auto* header = reinterpret_cast<const TSharedData::THeader*>(buffer.data() - TSharedData::HeaderSize);
            return header->Owner.load(std::memory_order_acquire);
        };
        UNIT_ASSERT(owner(first) == nullptr);
        UNIT_ASSERT(owner(second) == nullptr);
        accounting.AccountBuffer(first);
        accounting.AccountBuffer(second);
        UNIT_ASSERT(owner(first));
        UNIT_ASSERT(owner(first) == owner(second));
        const auto* sharedOwner = owner(first);
        accounting.AccountBuffer(first);
        UNIT_ASSERT(owner(first) == sharedOwner);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), first.GetAllocationSize() + second.GetAllocationSize());
        first = {};
        second = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(MetadataBuffersShareAnOwnerAndCountOnlyIntoMetadata) {
        TCacheAccounting accounting;
        auto first = TSharedData::Uninitialized(64);
        auto second = TSharedData::Uninitialized(128);
        accounting.AccountMetadataBuffer(first);
        accounting.AccountMetadataBuffer(second);
        const auto owner = [](const TSharedData& buffer) {
            const auto* header = reinterpret_cast<const TSharedData::THeader*>(buffer.data() - TSharedData::HeaderSize);
            return header->Owner.load(std::memory_order_acquire);
        };
        UNIT_ASSERT(owner(first) == owner(second));
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), first.GetAllocationSize() + second.GetAllocationSize());
        accounting.AccountBuffer(first);
        accounting.AccountMetadataBuffer(first);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        first = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), second.GetAllocationSize());
        second = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(LateMetadataClassificationDoesNotChargeTheAllocationAgain) {
        TCacheAccounting accounting;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        accounting.AccountBuffer(raw);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), bytes);
        accounting.AccountMetadataBuffer(raw);
        accounting.AccountMetadataBuffer(raw);
        accounting.AccountBuffer(raw);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes);
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(BufferKeepsItsSharedOwnerAliveAfterAllFacadesAreGone) {
        for (bool metadata : { false, true }) {
            TSharedData raw;
            ui64 bytes = 0;
            {
                TCacheAccounting accounting;
                raw = TSharedData::Uninitialized(64);
                bytes = raw.GetAllocationSize();
                if (metadata) {
                    accounting.AccountMetadataBuffer(raw);
                } else {
                    accounting.AccountBuffer(raw);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(raw.GetAllocationSize(), bytes);
            raw = {};
        }
    }

    Y_UNIT_TEST(TracingAfterAdoptionStillFindsTheSharedAccount) {
        TCacheAccounting accounting;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        accounting.AccountBuffer(raw);
        TBackTracingOwner::FakeOwner(raw);
        accounting.AccountBuffer(raw);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), bytes);
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(ExistingTracingOwnerUsesTheFallbackWithoutDoubleCharging) {
        TCacheAccounting accounting;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        TBackTracingOwner::FakeOwner(raw);
        accounting.AccountBuffer(raw);
        accounting.AccountBuffer(raw);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), bytes);
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(TracingAndForeignAccountsKeepMetadataClassificationIndependent) {
        for (bool traceFirst : { false, true }) {
            TCacheAccounting first;
            TCacheAccounting second;
            auto raw = TSharedData::Uninitialized(64);
            const ui64 bytes = raw.GetAllocationSize();
            if (traceFirst) {
                TBackTracingOwner::FakeOwner(raw);
            }
            first.AccountMetadataBuffer(raw);
            if (!traceFirst) {
                TBackTracingOwner::FakeOwner(raw);
            }
            first.AccountMetadataBuffer(raw);
            second.AccountBuffer(raw);
            UNIT_ASSERT_VALUES_EQUAL(first.MetadataBytes(), bytes);
            UNIT_ASSERT_VALUES_EQUAL(first.BufferBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(second.BufferBytes(), bytes);
            UNIT_ASSERT_VALUES_EQUAL(second.MetadataBytes(), 0);
            second.AccountMetadataBuffer(raw);
            second.AccountMetadataBuffer(raw);
            UNIT_ASSERT_VALUES_EQUAL(second.MetadataBytes(), bytes);
            UNIT_ASSERT_VALUES_EQUAL(second.BufferBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(first.GetMemoryUsage(), bytes);
            UNIT_ASSERT_VALUES_EQUAL(second.GetMemoryUsage(), bytes);
            raw = {};
            UNIT_ASSERT_VALUES_EQUAL(first.GetMemoryUsage(), 0);
            UNIT_ASSERT_VALUES_EQUAL(first.BufferBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(first.MetadataBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(second.GetMemoryUsage(), 0);
            UNIT_ASSERT_VALUES_EQUAL(second.BufferBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(second.MetadataBytes(), 0);
        }
    }

    Y_UNIT_TEST(ConcurrentOrdinaryAndMetadataAdoptionEndsWithOneMetadataCharge) {
        TCacheAccounting accounting;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        std::atomic<bool> start = false;
        TVector<std::thread> threads;
        for (ui32 index = 0; index < 6; ++index) {
            threads.emplace_back([&, index] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (ui32 iteration = 0; iteration < 200; ++iteration) {
                    if (index % 2) {
                        accounting.AccountMetadataBuffer(raw);
                    } else {
                        accounting.AccountBuffer(raw);
                    }
                }
            });
        }
        start.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(CopiesTrimsAndCompactOwnershipShareOneAllocation) {
        TCacheAccounting accounting;
        auto raw = TSharedData::Uninitialized(128);
        const ui64 bytes = raw.GetAllocationSize();
        accounting.AccountBuffer(raw);
        auto copy = raw;
        copy.TrimBack(16);
        accounting.AccountBuffer(copy);
        auto compact = std::move(raw).Extract();
        copy = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), bytes);
        compact = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(CallerOwnershipRemainsChargedUntilFinalFree) {
        TCacheAccounting accounting;
        TSharedData caller;
        ui64 bytes = 0;
        {
            auto cacheBuffer = TSharedData::Uninitialized(64);
            bytes = cacheBuffer.GetAllocationSize();
            accounting.AccountBuffer(cacheBuffer);
            caller = cacheBuffer;
        }
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), bytes);
        caller = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
    }

    Y_UNIT_TEST(SharedRawAndDescriptorStorageHaveSeparateFinalOwners) {
        TCacheAccounting accounting;
        auto raw = TSharedData::Uninitialized(64);
        const ui64 bytes = raw.GetAllocationSize();
        auto first = MakeIntrusiveConst<TDescriptor>(raw, ui64{ 12 }, ui32{ 1 });
        auto second = MakeIntrusiveConst<TDescriptor>(raw, ui64{ 20 }, ui32{ 2 });
        accounting.AccountBuffer(raw);
        accounting.AccountMetadata(*first);
        accounting.AccountMetadata(*first);
        accounting.AccountMetadata(*second);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), bytes + 32);
        first.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), bytes + 20);
        second.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(DescriptorLifetimeSurvivesRegistrationScope) {
        TCacheAccounting accounting;
        std::atomic<ui32> destroyed = 0;
        TIntrusiveConstPtr<TDescriptor> caller;
        {
            auto descriptor = MakeIntrusiveConst<TDescriptor>(TSharedData{}, ui64{ 12 }, ui32{ 1 }, &destroyed);
            accounting.AccountMetadata(*descriptor);
            caller = descriptor;
        }
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 12);
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 0);
        caller.Reset();
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
    }

    Y_UNIT_TEST(DistinctAllocationsStayDistinct) {
        TCacheAccounting accounting;
        auto first = TSharedData::Uninitialized(64);
        auto second = TSharedData::Uninitialized(64);
        accounting.AccountBuffer(first);
        accounting.AccountBuffer(second);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), first.GetAllocationSize() + second.GetAllocationSize());
        first = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), second.GetAllocationSize());
        second = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(SharedFacadeKeepsTheAccountAlive) {
        TCacheAccounting observer;
        auto raw = TSharedData::Uninitialized(64);
        {
            TCacheAccounting original;
            observer = original.Share();
            original.AccountBuffer(raw);
        }
        UNIT_ASSERT_VALUES_EQUAL(observer.BufferBytes(), raw.GetAllocationSize());
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(observer.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(observer.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(observer.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(AccountsKeepIndependentAttribution) {
        TCacheAccounting first;
        TCacheAccounting second;
        auto raw = TSharedData::Uninitialized(64);
        first.AccountBuffer(raw);
        second.AccountBuffer(raw);
        UNIT_ASSERT_VALUES_EQUAL(first.BufferBytes(), raw.GetAllocationSize());
        UNIT_ASSERT_VALUES_EQUAL(second.BufferBytes(), raw.GetAllocationSize());
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(first.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(first.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(first.MetadataBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(second.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(second.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(second.MetadataBytes(), 0);
    }

    Y_UNIT_TEST(PausedInstallerStillOwnsTheRegisteredObjectUntilCharge) {
        for (bool descriptorKind : { false, true }) {
            TCacheAccounting accounting;
            auto raw = TSharedData::Uninitialized(64);
            auto descriptor = MakeIntrusiveConst<TDescriptor>(raw, ui64{ 12 });

            struct TPause {
                bool Descriptor;
                std::atomic<bool> Entered = false;
                std::atomic<bool> Resume = false;
            } pause{ descriptorKind };

            TCacheAccountingTestAccess::SetHook(accounting, &pause, [](void* context, bool descriptor) noexcept {
                auto& pause = *static_cast<TPause*>(context);
                if (descriptor != pause.Descriptor) {
                    return;
                }
                pause.Entered.store(true, std::memory_order_release);
                while (!pause.Resume.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
            });
            std::thread creator([&, heldBuffer = raw, heldDescriptor = descriptor] {
                if (descriptorKind) {
                    accounting.AccountMetadata(*heldDescriptor);
                } else {
                    accounting.AccountBuffer(heldBuffer);
                }
            });
            Y_DEFER {
                pause.Resume.store(true, std::memory_order_release);
                if (creator.joinable()) {
                    creator.join();
                }
                TCacheAccountingTestAccess::SetHook(accounting, nullptr, nullptr);
            };
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (!pause.Entered.load(std::memory_order_acquire)) {
                UNIT_ASSERT_C(TInstant::Now() < deadline, "Accounting installer did not reach the hook");
                std::this_thread::yield();
            }
            // An already installed registration can be observed before its owner publishes the charge
            if (descriptorKind) {
                accounting.AccountMetadata(*descriptor);
                UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), raw.GetAllocationSize());
            } else {
                accounting.AccountBuffer(raw);
                UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
            }
            descriptor.Reset();
            raw = {};
            pause.Resume.store(true, std::memory_order_release);
            creator.join();
            TCacheAccountingTestAccess::SetHook(accounting, nullptr, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
            UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
        }
    }

    Y_UNIT_TEST(ConcurrentRegistrationChargesEachObjectOnce) {
        TCacheAccounting accounting;
        auto raw = TSharedData::Uninitialized(128);
        auto descriptor = MakeIntrusiveConst<TDescriptor>(raw, ui64{ 12 });
        std::atomic<bool> start = false;
        TVector<std::thread> threads;
        for (ui32 index = 0; index < 6; ++index) {
            threads.emplace_back([&] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (ui32 iteration = 0; iteration < 200; ++iteration) {
                    accounting.AccountBuffer(raw);
                    accounting.AccountMetadata(*descriptor);
                }
            });
        }
        start.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), raw.GetAllocationSize() + 12);
        descriptor.Reset();
        raw = {};
        UNIT_ASSERT_VALUES_EQUAL(accounting.GetMemoryUsage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.BufferBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(accounting.MetadataBytes(), 0);
    }
}

} // namespace NKikimr::NSharedCache
