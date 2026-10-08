#pragma once

#include "flat_part_iface.h"
#include "flat_sausage_fetch.h"
#include "flat_fwd_misc.h"
#include "shared_cache.h"
#include "util_fmt_abort.h"

namespace NKikimr {
namespace NTable {
namespace NFwd {

    using TPageOffset = NPage::TPageOffset;

    enum class EUsage : ui8 {
        None    = 0,
        Seen    = 1,    /* Page has been used by reference  */
        Keep    = 2,    /* Data has been used at least once */
    };

    enum class EFetch : ui8 {
        Wait    = 0,    /* Page has been queued for load    */
        Drop    = 1,    /* Queued page page won't be used   */
        None    = 2,
        Done    = 3,    /* Page has been settled with data  */
    };

    struct TPage {
        TPage(TPageOffset offset, ui64 size, ui16 tag, TPageId refer, ui32 crc32 = 0)
            : Size(size), Offset(offset), Refer(refer), Crc32(crc32), Tag(tag)
        {
        }

        TPage(NTable::NPage::TPageLocation loc, ui16 tag, TPageId refer)
            : Size(loc.Size), Offset(loc.Offset), Refer(refer), Crc32(loc.Crc32), Tag(tag)
        {
        }

        explicit operator bool() const
        {
            return bool(SharedPageRef) && bool(Offset);
        }

        bool Ready() const noexcept
        {
            return Fetch == EFetch::None || Fetch == EFetch::Done;
        }

        bool operator<(TPageOffset offset) const
        {
            return Offset < offset;
        }

        ui32 Settle(TSharedCachePageRef&& page)
        {
            const auto was = std::exchange(Fetch, EFetch::Done);

            if (Offset != page.GetOffset()) {
                Y_TABLET_ERROR("Settling page with different reference offset");
            } else if (Size != page.size()) {
                Y_TABLET_ERROR("Requested and obtained page sizes are not the same");
            } else if (was == EFetch::Drop) {
                page.Drop();
            } else if (was != EFetch::Wait) {
                Y_TABLET_ERROR("Settling page that is not waiting for any data");
            } else {
                SharedPageRef = std::move(page);
            }

            return SharedPageRef.size();
        }

        const TSharedCachePageRef* Touch(TPageOffset offset, TStat& stat)
        {
            if (Offset != offset || (!SharedPageRef && Fetch == EFetch::Done)) {
                Y_TABLET_ERROR("Touching page that doesn't fit to this action");
            } else {
                auto to = Fetch == EFetch::None ? EUsage::Seen : EUsage::Keep;

                if (std::exchange(Usage, to) != to && to == EUsage::Keep)
                    stat.Usage += Size;
            }

            return SharedPageRef ? &SharedPageRef : nullptr;
        }

        ui64 Release()
        {
            Fetch = Max(Fetch, EFetch::Drop);
            const ui64 bytes = SharedPageRef.size();
            SharedPageRef.Drop();
            return bytes;
        }

        bool Released() const noexcept
        {
            return !SharedPageRef;
        }

        const ui64 Size = 0;
        const TPageOffset Offset;
        const ui32 Refer = 0;
        const ui32 Crc32 = 0;
        const ui16 Tag  = Max<ui16>();
        EUsage Usage    = EUsage::None;
        EFetch Fetch    = EFetch::None;
        TSharedCachePageRef SharedPageRef;
    };

}
}
}
