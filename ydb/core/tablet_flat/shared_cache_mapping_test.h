#pragma once

#include "shared_cache_mapping.h"

namespace NKikimr::NSharedCache {

class TSharedCacheMappingTestAccess {
public:
    using EStep = TSharedCacheMapping::ETestStep;

    template <class TCallback>
    static void SetHook(TSharedCacheMapping& mapping, TCallback& callback) noexcept {
        mapping.SetTestHook({
            .Context = &callback,
            .Function = [](void* context, EStep step) noexcept {
                return (*static_cast<TCallback*>(context))(step);
            },
        });
    }

    static void ClearHook(TSharedCacheMapping& mapping) noexcept {
        mapping.SetTestHook({});
    }
};

} // namespace NKikimr::NSharedCache
