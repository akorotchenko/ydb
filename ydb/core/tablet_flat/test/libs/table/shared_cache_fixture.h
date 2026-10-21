#pragma once

#include "shared_cache_context.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable::NTest {

class TTableCacheTest : public NUnitTest::TBaseTestCase {
public:
    void SetUp(NUnitTest::TTestContext&) override {
        Context_ = MakeHolder<TTableCacheContext>();
    }

    void TearDown(NUnitTest::TTestContext&) override {
        Context_.Reset();
    }

protected:
    TTableCacheContext& CacheContext() noexcept {
        return *Context_;
    }

private:
    THolder<TTableCacheContext> Context_;
};

} // namespace NKikimr::NTable::NTest
