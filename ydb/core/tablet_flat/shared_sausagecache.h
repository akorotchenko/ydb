#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/protos/shared_cache.pb.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
struct TAppData;
}

namespace NKikimrConfig {
class TAppConfig;
}

namespace NKikimr::NSharedCache {

using TSharedCacheConfig = NKikimrSharedCache::TSharedCacheConfig;

TSharedCacheConfig ResolveSharedCacheConfig(
    const NKikimrConfig::TAppConfig& config, const TAppData* fallback = nullptr);

// A zero expectedPageSize uses the internal estimate; fixtures may override it in C++
IActor* CreateSharedPageCache(
    const TSharedCacheConfig& config,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    ui64 expectedPageSize = 0
);

inline TActorId MakeSharedPageCacheId(ui64 id = 0) {
    char x[12] = { 's', 'h', 's', 'c' };
    WriteUnaligned<ui64>((ui64*)(x+4), id);
    return TActorId(0, TStringBuf(x, 12));
}

class TSharedCachePages;

namespace NLegacy {

    // Uses the node's legacy GC holder when the service starts
    IActor* CreateSharedPageCache(
        const TSharedCacheConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    IActor* CreateSharedPageCache(const TSharedCacheConfig& config,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, TIntrusivePtr<TSharedCachePages> pages);

} // namespace NLegacy

} // namespace NKikimr::NSharedCache
