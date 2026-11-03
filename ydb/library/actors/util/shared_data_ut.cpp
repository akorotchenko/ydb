#include "shared_data.h"
#include "shared_data_backtracing_owner.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/deque.h>
#include <util/generic/yexception.h>
#include <util/system/sys_alloc.h>

#include <array>
#include <thread>

namespace NActors {

    Y_UNIT_TEST_SUITE(TSharedDataTest) {

        Y_UNIT_TEST(BasicBehavior) {
            auto data = TSharedData::Copy("Hello", 5);
            UNIT_ASSERT(data.IsPrivate());
            UNIT_ASSERT(!data.IsShared());
            UNIT_ASSERT_VALUES_EQUAL(data.size(), 5u);
            UNIT_ASSERT_VALUES_EQUAL(data.end() - data.begin(), 5u);
            UNIT_ASSERT_VALUES_EQUAL(data.mutable_end() - data.mutable_begin(), 5u);
            UNIT_ASSERT(data.begin() == data.data());
            UNIT_ASSERT(data.mutable_data() == data.data());
            UNIT_ASSERT(data.mutable_begin() == data.mutable_data());

            UNIT_ASSERT_VALUES_EQUAL(data.ToString(), TString("Hello"));
            UNIT_ASSERT_VALUES_EQUAL(::memcmp(data.data(), "Hello", 5), 0);

            auto link = data;
            UNIT_ASSERT(!link.IsPrivate());
            UNIT_ASSERT(!data.IsPrivate());
            UNIT_ASSERT(link.IsShared());
            UNIT_ASSERT(data.IsShared());
            UNIT_ASSERT(link.data() == data.data());
            UNIT_ASSERT(link.size() == data.size());

            link = { };
            UNIT_ASSERT(link.IsPrivate());
            UNIT_ASSERT(data.IsPrivate());
            UNIT_ASSERT(!link.IsShared());
            UNIT_ASSERT(!data.IsShared());

            UNIT_ASSERT_VALUES_EQUAL(TString(TStringBuf(data)), TString("Hello"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice()), TString("Hello"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1)), TString("ello"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1, 3)), TString("ell"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(1, 100)), TString("ello"));
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice(0, 4)), TString("Hell"));

            link = data;
            UNIT_ASSERT(link.data() == data.data());
            UNIT_ASSERT_VALUES_UNEQUAL(link.Detach(), data.data());
            UNIT_ASSERT_EQUAL(data.size(), link.size());
            UNIT_ASSERT_VALUES_EQUAL(TString(data.Slice()), TString(link.Slice()));
        }

        Y_UNIT_TEST(TrimBehavior) {
            auto data = TSharedData::Uninitialized(42);

            UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
            UNIT_ASSERT(data.data() != nullptr);

            // Trim to non-zero does not change addresses
            const char* ptr1 = data.data();
            data.TrimBack(31);
            const char* ptr2 = data.data();

            UNIT_ASSERT_VALUES_EQUAL(data.size(), 31u);
            UNIT_ASSERT(ptr1 == ptr2);

            // Trim to zero releases underlying data
            data.TrimBack(0);

            UNIT_ASSERT_VALUES_EQUAL(data.size(), 0u);
            UNIT_ASSERT(data.data() == nullptr);
        }

        Y_UNIT_TEST(BufferOwnership) {
            auto data = TSharedData::Copy("Hello", 5);
            const char* ptr = data.data();

            auto buffer = std::move(data).Extract();
            UNIT_ASSERT(!data);
            UNIT_ASSERT(buffer);
            UNIT_ASSERT(buffer.data() == ptr);

            auto shared = buffer.Share(5);
            UNIT_ASSERT(shared.IsShared());
            UNIT_ASSERT(shared.data() == ptr);
            UNIT_ASSERT_VALUES_EQUAL(shared.ToString(), TString("Hello"));

            buffer = {};
            UNIT_ASSERT(shared.IsPrivate());
            UNIT_ASSERT_VALUES_EQUAL(shared.ToString(), TString("Hello"));
        }

        class TCustomOwner : public TSharedData::IOwner {
            using THeader = TSharedData::THeader;

        public:
            TSharedData Allocate(size_t size) {
                char* raw = reinterpret_cast<char*>(y_allocate(sizeof(THeader) + size));
                THeader* header = reinterpret_cast<THeader*>(raw);
                new (header) THeader(this);
                char* data = raw + sizeof(THeader);
                Y_ABORT_UNLESS(Allocated_.emplace(data, sizeof(THeader) + size).second);
                return TSharedData::AttachUnsafe(data, size);
            }

            size_t GetAllocationSize(const char* data) const noexcept override {
                const auto it = Allocated_.find(data);
                Y_ABORT_UNLESS(it != Allocated_.end());
                return it->second;
            }

            void Deallocate(char* data) noexcept override {
                Y_ABORT_UNLESS(Allocated_.erase(data) > 0);
                char* raw = data - sizeof(THeader);
                THeader* header = reinterpret_cast<THeader*>(raw);
                header->~THeader();
                y_deallocate(raw);
                Deallocated_.push_back(data);
            }

            char* NextDeallocated() {
                char* result = nullptr;
                if (Deallocated_) {
                    result = Deallocated_.front();
                    Deallocated_.pop_front();
                }
                return result;
            }

        private:
            THashMap<const void*, size_t> Allocated_;
            TDeque<char*> Deallocated_;
        };

        class TBuiltinOwner final : public TSharedData::IOwner {
        public:
            size_t GetAllocationSize(const char* data) const noexcept override {
                return BuiltinAllocationSize(data);
            }

            void Deallocate(char* data) noexcept override {
                ++Deallocated;
                DeallocateBuiltin(data);
            }

            ui32 Deallocated = 0;
        };

        Y_UNIT_TEST(BuiltinOwnerCanBeSharedByIndependentAllocations) {
            TBuiltinOwner owner;
            auto first = TSharedData::Uninitialized(64);
            auto second = TSharedData::Uninitialized(128);
            const auto firstBytes = first.GetAllocationSize();
            const auto secondBytes = second.GetAllocationSize();
            UNIT_ASSERT(first.TrySetBuiltinOwner(owner));
            UNIT_ASSERT(second.TrySetBuiltinOwner(owner));
            UNIT_ASSERT(!first.TrySetBuiltinOwner(owner));
            first.TrimBack(16);
            UNIT_ASSERT_VALUES_EQUAL(first.GetAllocationSize(), firstBytes);
            UNIT_ASSERT_VALUES_EQUAL(second.GetAllocationSize(), secondBytes);
            auto compact = std::move(first).Extract();
            auto view = compact.Share(16);
            compact = {};
            UNIT_ASSERT_VALUES_EQUAL(owner.Deallocated, 0);
            view = {};
            UNIT_ASSERT_VALUES_EQUAL(owner.Deallocated, 1);
            second = {};
            UNIT_ASSERT_VALUES_EQUAL(owner.Deallocated, 2);
        }

        Y_UNIT_TEST(BuiltinOwnerReplacementPreservesCustomOwners) {
            TCustomOwner custom;
            TBuiltinOwner replacement;
            auto data = custom.Allocate(64);
            const char* pointer = data.data();
            UNIT_ASSERT(!data.TrySetBuiltinOwner(replacement));
            data = {};
            UNIT_ASSERT_VALUES_EQUAL(replacement.Deallocated, 0);
            UNIT_ASSERT(custom.NextDeallocated() == pointer);
        }

        struct TAllocationTag {
            TAllocationTag(std::atomic<size_t>& created, std::atomic<size_t>& destroyed)
                : Destroyed(destroyed)
            {
                ++created;
            }

            ~TAllocationTag() {
                ++Destroyed;
            }

            std::atomic<size_t>& Destroyed;
        };

        Y_UNIT_TEST(AllocationSizeSurvivesTrim) {
            UNIT_ASSERT_VALUES_EQUAL(TSharedData().GetAllocationSize(), 0);
            auto data = TSharedData::Uninitialized(1024);
            const auto copy = data;
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 1024 + TSharedData::OverheadSize);
            data.TrimBack(64);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), copy.GetAllocationSize());
            data.TrimBack(0);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(copy.GetAllocationSize(), 1024 + TSharedData::OverheadSize);
        }

        Y_UNIT_TEST(AllocationTagsSurviveCompactOwnership) {
            for (bool tagBeforeExtract : { false, true }) {
                std::atomic<size_t> created = 0;
                std::atomic<size_t> destroyed = 0;
                auto data = TSharedData::Uninitialized(128);
                TAllocationTag* tag = nullptr;
                if (tagBeforeExtract) {
                    tag = &data.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
                }
                auto buffer = std::move(data).Extract();
                UNIT_ASSERT(!data);
                if (!tagBeforeExtract) {
                    tag = &buffer.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
                }
                UNIT_ASSERT(buffer.FindAllocationTag<TAllocationTag>() == tag);
                UNIT_ASSERT_VALUES_EQUAL(buffer.GetAllocationSize(), 128 + TSharedData::OverheadSize);
                auto shared = buffer.Share(64);
                UNIT_ASSERT(shared.FindAllocationTag<TAllocationTag>() == tag);
                UNIT_ASSERT(&shared.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed) == tag);
                buffer = {};
                UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 0);
                UNIT_ASSERT_VALUES_EQUAL(shared.GetAllocationSize(), 128 + TSharedData::OverheadSize);
                shared = {};
                UNIT_ASSERT_VALUES_EQUAL(created.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
            }
        }

        Y_UNIT_TEST(AllocationTagPreservesCustomOwner) {
            std::atomic<size_t> created = 0;
            std::atomic<size_t> destroyed = 0;
            TCustomOwner owner;
            auto data = owner.Allocate(42);
            const char* pointer = data.data();
            data.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
            data.TrimBack(20);
            auto buffer = std::move(data).Extract();
            UNIT_ASSERT(buffer.FindAllocationTag<TAllocationTag>());
            UNIT_ASSERT_VALUES_EQUAL(buffer.GetAllocationSize(), 42 + TSharedData::HeaderSize);
            buffer = {};
            UNIT_ASSERT(owner.NextDeallocated() == pointer);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            UNIT_ASSERT_VALUES_EQUAL(created.load(), destroyed.load());
        }

        Y_UNIT_TEST(AllocationTagComposesWithTracingOwner) {
            for (bool traceFirst : { false, true }) {
                std::atomic<size_t> created = 0;
                std::atomic<size_t> destroyed = 0;
                auto data = TSharedData::Uninitialized(42);
                if (traceFirst) {
                    TBackTracingOwner::FakeOwner(data);
                }
                auto& tag = data.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
                if (!traceFirst) {
                    TBackTracingOwner::FakeOwner(data);
                }
                UNIT_ASSERT(data.FindAllocationTag<TAllocationTag>() == &tag);
                UNIT_ASSERT(data.FindAllocationTag<TBackTracingOwner>());
                UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42 + TSharedData::OverheadSize);
                data = {};
                UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
            }
        }

        Y_UNIT_TEST(ConcurrentAllocationTagInstallation) {
            std::atomic<size_t> created = 0;
            std::atomic<size_t> destroyed = 0;
            std::atomic<bool> start = false;
            auto data = TSharedData::Uninitialized(42);
            std::array<TAllocationTag*, 8> tags;
            std::array<std::thread, 8> threads;
            for (size_t index = 0; index < threads.size(); ++index) {
                threads[index] = std::thread([&, index, copy = data] {
                    while (!start.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    if (index % 2 == 0) {
                        TBackTracingOwner::FakeOwner(copy);
                    }
                    tags[index] = &copy.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
                });
            }
            start.store(true, std::memory_order_release);
            for (auto& thread : threads) {
                thread.join();
            }
            for (const auto* tag : tags) {
                UNIT_ASSERT(tag == tags.front());
            }
            UNIT_ASSERT_VALUES_EQUAL(created.load() - destroyed.load(), 1);
            UNIT_ASSERT(data.FindAllocationTag<TBackTracingOwner>());
            data = {};
            UNIT_ASSERT_VALUES_EQUAL(created.load(), destroyed.load());
        }

        Y_UNIT_TEST(ThrowingTagPreservesExistingOwnerChain) {
            struct TThrowingTag {
                TThrowingTag() {
                    ythrow yexception() << "Expected tag construction failure";
                }
            };

            std::atomic<size_t> created = 0;
            std::atomic<size_t> destroyed = 0;
            TCustomOwner owner;
            auto data = owner.Allocate(42);
            const char* pointer = data.data();
            auto& existing = data.GetOrCreateAllocationTag<TAllocationTag>(created, destroyed);
            TBackTracingOwner::FakeOwner(data);
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                data.GetOrCreateAllocationTag<TThrowingTag>(), yexception, "tag construction failure");
            UNIT_ASSERT(!data.FindAllocationTag<TThrowingTag>());
            UNIT_ASSERT(data.FindAllocationTag<TAllocationTag>() == &existing);
            UNIT_ASSERT(data.FindAllocationTag<TBackTracingOwner>());
            UNIT_ASSERT(data.IsPrivate());
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42 + TSharedData::HeaderSize);
            data = {};
            UNIT_ASSERT(owner.NextDeallocated() == pointer);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            UNIT_ASSERT_VALUES_EQUAL(created.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
        }

        Y_UNIT_TEST(CustomOwnerAllocationSizeSurvivesTrim) {
            TCustomOwner owner;
            auto data = owner.Allocate(42);
            data.TrimBack(10);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42 + TSharedData::HeaderSize);
        }

        Y_UNIT_TEST(TracingOwnerAllocationSizeSurvivesTrim) {
            auto data = TSharedData::AttachUnsafe(TBackTracingOwner::Allocate(42), 42);
            data.TrimBack(10);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42 + TSharedData::OverheadSize);
        }

        Y_UNIT_TEST(TracingOwnerDelegatesCustomAllocationSize) {
            TCustomOwner owner;
            auto data = owner.Allocate(42);
            const char* pointer = data.data();
            TBackTracingOwner::FakeOwner(data);
            data.TrimBack(10);
            UNIT_ASSERT_VALUES_EQUAL(data.GetAllocationSize(), 42 + TSharedData::HeaderSize);
            data = {};
            UNIT_ASSERT(owner.NextDeallocated() == pointer);
        }

        Y_UNIT_TEST(CustomOwner) {
            TCustomOwner owner;
            const char* ptr;

            // Test destructor releases data
            {
                auto data = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
                ptr = data.data();
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test assignment releases data
            {
                auto data = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
                ptr = data.data();
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
                data = { };
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test copies keep references correctly
            {
                auto data = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
                ptr = data.data();
                auto copy = data;
                UNIT_ASSERT_VALUES_EQUAL(copy.size(), 42u);
                UNIT_ASSERT(copy.data() == ptr);
                data = { };
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 0u);
                UNIT_ASSERT(data.data() == nullptr);
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test assignment releases correct data
            {
                auto data1 = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data1.size(), 42u);
                auto data2 = owner.Allocate(31);
                UNIT_ASSERT_VALUES_EQUAL(data2.size(), 31u);
                ptr = data1.data();
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
                data1 = data2;
                UNIT_ASSERT(owner.NextDeallocated() == ptr);
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
                ptr = data2.data();
                UNIT_ASSERT_VALUES_EQUAL(data1.size(), 31u);
                UNIT_ASSERT(data1.data() == ptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test moves don't produce dangling references
            {
                auto data = owner.Allocate(42);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 42u);
                ptr = data.data();
                auto moved = std::move(data);
                UNIT_ASSERT_VALUES_EQUAL(moved.size(), 42u);
                UNIT_ASSERT(moved.data() == ptr);
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 0u);
                UNIT_ASSERT(data.data() == nullptr);
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test Detach copies correctly and doesn't affect owned data
            {
                auto data = owner.Allocate(42);
                ptr = data.data();
                auto disowned = data;
                disowned.Detach();
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

            // Test buffer ownership preserves a custom owner
            {
                auto data = owner.Allocate(42);
                ptr = data.data();
                auto buffer = std::move(data).Extract();
                auto shared = buffer.Share(42);
                buffer = {};
                UNIT_ASSERT(owner.NextDeallocated() == nullptr);
                UNIT_ASSERT(shared.data() == ptr);
            }

            UNIT_ASSERT(owner.NextDeallocated() == ptr);
            UNIT_ASSERT(owner.NextDeallocated() == nullptr);

        }

    }

}
