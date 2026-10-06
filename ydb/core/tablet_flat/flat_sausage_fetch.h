#pragma once

#include <atomic>

#include "flat_sausage_solid.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NPageCollection {

    struct TPagesWaitPad : public TThrRefBase {
        ui64 PendingRequests = 0;
        ui64 WorkingSetBytes = 0; // Immutable page budget for retry after releasing this attempt's pins.
        // Executor writes this flag for non-Sticky pins; the cache never reads the mutable seat.
        std::atomic<bool> HasPinnedPages{ false };
        bool PostponedForResources = false; // Cache-actor-owned; closes late requests of the abandoned attempt.
    };

    struct TLoadedPage {
        TLoadedPage() = default;

        TLoadedPage(TPageLocation location, TSharedData data)
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

    // Lightweight: offset+data only; Size/Type/Crc32 are authoritative in the cache's PageSet.
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
