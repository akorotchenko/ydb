#pragma once

#include "cache_mapping.h"

namespace NKikimr::NSharedCache {

class TCacheMappingTestAccess {
public:
    using EStep = TCacheMapping::ETestStep;

    template <class TCallback>
    static void SetHook(TCacheMapping& mapping, TCallback& callback) noexcept {
        mapping.SetTestHook({
            .Context = &callback,
            .Function = [](void* context, EStep step) noexcept {
                return (*static_cast<TCallback*>(context))(step);
            },
        });
    }

    static void ClearHook(TCacheMapping& mapping) noexcept {
        mapping.SetTestHook({});
    }
};

} // namespace NKikimr::NSharedCache
