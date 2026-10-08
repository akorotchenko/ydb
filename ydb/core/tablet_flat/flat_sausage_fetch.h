#pragma once

#include <atomic>

#include "flat_sausage_solid.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/xrange.h>
#include "shared_cache.h"

namespace NKikimr {
namespace NPageCollection {

    struct TPagesWaitPad : public TThrRefBase {
        ui64 PendingRequests = 0;
        ui64 WorkingSetBytes = 0; // Immutable page budget for retry after releasing this attempt's pins.
        // Set by the executor or fetch completions for non-Sticky pins; stays set for this attempt.
        std::atomic<bool> HasPinnedPages{ false };
        bool PostponedForResources = false; // Cache-actor-owned; closes late requests of the abandoned attempt.
    };

    struct TPageData {
        TPageData() = default;

        TPageData(TPageLocation location, TSharedData data)
            : Location(location)
            , Data(std::move(data))
        {
        }

        explicit operator bool() const noexcept
        {
            return Data && bool(Location);
        }

        TPageLocation Location;
        TSharedData Data;
    };

    // Lightweight: offset+data only; Size/Type/Crc32 come from the fetch location.
    struct TLoadedPageData {
        TLoadedPageData() = default;

        TLoadedPageData(TPageOffset offset, TSharedData data)
            : Offset(offset)
            , Data(std::move(data))
        {

        }

        explicit operator bool() const noexcept
        {
            return Data && bool(Offset);
        }

        TPageOffset Offset;
        TSharedData Data;
    };

} // namespace NPageCollection
} // namespace NKikimr
