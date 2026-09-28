#pragma once

#include "defs.h"
#include "flat_bio_events.h"
#include "shared_handle.h"
#include "shared_cache_item.h"
#include "shared_page.h"
#include <ydb/core/protos/shared_cache.pb.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NSharedCache {
using EPriority = NTabletFlatExecutor::NBlockIO::EPriority;
using TPageId = NTable::NPage::TPageId;

    enum class EWakeupTag {
        DoGCScheduled = 1,
        DoGCManual = 2,
        DoLimitDecrease = 3,
        ContinueBTreeWalk = 4,
        DoLimitMaintenance = 5,
    };

    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR),

        EvTouch = EvBegin + 512,
        EvUnregister,
        EvDetach,
        EvAttach,
        EvAttached,
        EvSaveCompactedPages,
        EvRequest,
        EvResult,
        EvUpdated,
        EvInFlightReleased,
        EvRequestAnswered,
        EvStickyCollectionPages,

        EvEnd

        /* +1024 range is reserved for scan events */
    };

    enum class ERequestTypeCookie : ui64 {
        Undefined = 0,
        Transaction = 1,
        StickyPages,
        PendingInit,
        BootLogic,
        TryKeepInMemPages,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_EXECUTOR), "");

    struct TEvUnregister : public TEventLocal<TEvUnregister, EvUnregister> {
        TIntrusivePtr<TCollectionRegistry> Registry;

        explicit TEvUnregister(TIntrusivePtr<TCollectionRegistry> registry = {})
            : Registry(std::move(registry))
        {
        }
    };

    struct TEvDetach : public TEventLocal<TEvDetach, EvDetach> {
        const TLogoBlobID PageCollectionId;
        TIntrusivePtr<TCollectionRegistry> Registry;

        TEvDetach(const TLogoBlobID& pageCollectionId, TIntrusivePtr<TCollectionRegistry> registry = {})
            : PageCollectionId(pageCollectionId)
            , Registry(std::move(registry))
        {
        }
    };

    // notifies Shared Cache about Private Cache owned shared bodies
    // so it can send back dropped pages
    struct TEvSync : public TEventLocal<TEvSync, EvTouch> {
        THashMap<TLogoBlobID, THashSet<TPageOffset>> Pages;

        TEvSync(THashMap<TLogoBlobID, THashSet<TPageOffset>>&& pages)
            : Pages(std::move(pages))
        {
        }
    };

    struct TEvAttach : public TEventLocal<TEvAttach, EvAttach> {
        // One B-tree per group: index nodes always live in the main group's collection
        // (IndexCollectionId), while the data pages they point at live in that group's
        // collection (DataCollectionId) — the same collection for group 0.
        struct TBtreeSeed {
            TLogoBlobID IndexCollectionId;
            TLogoBlobID DataCollectionId;
            NTable::NPage::TPageLocation Root;
            ui32 LevelCount = 0;
            bool QueueDataPages = true;
            bool Sticky = false;
            bool IndexCollectionSticky = false;

            bool operator==(const TBtreeSeed&) const = default;
        };

        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TIntrusivePtr<TCollectionRegistry> Registry;
        ECacheMode CacheMode;
        bool RouteToCore = false;
        // Authoritative for the sender: an empty vector withdraws that owner's walks.
        TVector<TBtreeSeed> BtreeSeeds;
        // Revisit unchanged sticky seeds after the owner's private cache is recreated.
        bool ReplayStickyWalk = false;
        TVector<TPageOffset> StickyOffsets;

        // The cache walks the seeded B-trees itself.
        TEvAttach(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, ECacheMode cacheMode,
            TVector<TBtreeSeed> btreeSeeds, bool routeToCore = false, TVector<TPageOffset> stickyOffsets = {},
            bool replayStickyWalk = false)
            : PageCollection(std::move(pageCollection))
            , CacheMode(cacheMode)
            , RouteToCore(routeToCore)
            , BtreeSeeds(std::move(btreeSeeds))
            , ReplayStickyWalk(replayStickyWalk)
            , StickyOffsets(std::move(stickyOffsets))
        {
        }

        // The registry ties the collection to the owner's shared cache state.
        TEvAttach(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, ECacheMode cacheMode,
            TIntrusivePtr<TCollectionRegistry> registry = {})
            : PageCollection(std::move(pageCollection))
            , Registry(std::move(registry))
            , CacheMode(cacheMode)
        {
        }
    };

    struct TEvAttached : public TEventLocal<TEvAttached, EvAttached> {
        TLogoBlobID PageCollectionId;
        TCollectionCacheItem CacheItem;

        TEvAttached(const TLogoBlobID& pageCollectionId, TCollectionCacheItem cacheItem)
            : PageCollectionId(pageCollectionId)
            , CacheItem(cacheItem)
        {
        }
    };

    // Note: compacted pages do not have an owner yet
    // at first they should be accepted by an executor
    // and it will send TEvAttach itself when it have happened
    struct TEvSaveCompactedPages : public TEventLocal<TEvSaveCompactedPages, EvSaveCompactedPages> {
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TIntrusivePtr<TPage>> Pages;

        TEvSaveCompactedPages(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection)
            : PageCollection(std::move(pageCollection))
        {
        }
    };

    struct TEvRequest : public TEventLocal<TEvRequest, EvRequest> {
        TEvRequest(EPriority priority, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TVector<TPageLocation> pages, ui64 cookie = 0)
            : Priority(priority)
            , PageCollection(std::move(pageCollection))
            , Pages(std::move(pages))
            , Cookie(cookie)
        { }

        const EPriority Priority;
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TPageLocation> Pages;
        TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad;
        NWilson::TTraceId TraceId;
        const ui64 Cookie;
    };

    struct TEvResult : public TEventLocal<TEvResult, EvResult> {
        using EStatus = NKikimrProto::EReplyStatus;

        TEvResult(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, EStatus status, ui64 cookie,
            ui64 executorGeneration = 0, ui64 requestId = 0, bool coreRoute = false)
            : Status(status)
            , PageCollection(std::move(pageCollection))
            , Cookie(cookie)
            , ExecutorGeneration(executorGeneration)
            , RequestId(requestId)
            , CoreRoute(coreRoute)
        {
        }

        void Describe(IOutputStream& out) const {
            out << "TEvResult{" << Pages.size() << " pages"
                << " " << PageCollection->Label() << " " << (Status == NKikimrProto::OK ? "ok" : "fail") << " "
                << NKikimrProto::EReplyStatus_Name(Status) << "}";
        }

        ui64 Bytes() const
        {
            return
                std::accumulate(Pages.begin(), Pages.end(), ui64(0),
                    [](ui64 bytes, const TLoaded& loaded)
                        { return bytes + loaded.Size; });
        }

        struct TLoaded {
            TLoaded(NTable::NPage::TPageOffset offset, size_t size, TSharedPageRef page)
                : Offset(offset)
                , Size(size)
                , Page(std::move(page))
            { }

            NTable::NPage::TPageOffset Offset;
            size_t Size;
            TSharedPageRef Page;
        };

        const EStatus Status;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TLoaded> Pages;
        TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad;
        const ui64 Cookie;
        const ui64 ExecutorGeneration;
        const ui64 RequestId;
        const bool CoreRoute;
    };

    struct TRequestCompletionParams {
        NActors::TActorSystem* ActorSystem = nullptr;
        TActorId ReplyTo;
        ui64 EventCookie = 0;
        ui64 ExecutorGeneration = 0;
        ui64 RequestId = 0;
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TPageLocation> Pages;
        TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad;
        ui64 Cookie = 0;
        NActors::TActorId Notify;
        bool CoreRoute = false;
    };

    class TRequestCompletion final : public TThrRefBase {
    public:
        explicit TRequestCompletion(TRequestCompletionParams&& params) noexcept;

        void Complete(ui32 index, TSharedPageRef page, EPageFetchCompletion completion) noexcept;
        void Cancel() noexcept;

        const TVector<TPageLocation>& Locations() const noexcept {
            return Locations_;
        }

    private:
        void SendResult() noexcept;

    private:
        NActors::TActorSystem* const ActorSystem_;
        const TActorId ReplyTo_;
        const ui64 EventCookie_;
        const ui64 ExecutorGeneration_;
        const ui64 RequestId_;
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection_;
        TVector<TPageLocation> Locations_;
        TVector<TSharedPageRef> Pages_;
        TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad_;
        const ui64 Cookie_;
        const NActors::TActorId Notify_;
        const bool CoreRoute_;
        std::atomic<TEvResult::EStatus> Status_{ NKikimrProto::OK };
        std::atomic<ui32> Remaining_;
    };

    struct TEvRequestAnswered : public TEventLocal<TEvRequestAnswered, EvRequestAnswered> {
        explicit TEvRequestAnswered(ui64 status) noexcept
            : Status(status)
        { }

        const ui64 Status;
    };

    struct TEvInFlightReleased : public TEventLocal<TEvInFlightReleased, EvInFlightReleased> {
        TEvInFlightReleased(ui64 cookie, ui64 bytes, ui32 pages) noexcept
            : Cookie(cookie)
            , Bytes(bytes)
            , Pages(pages)
        { }

        const ui64 Cookie; // queue type
        const ui64 Bytes; // fetch cookie, the requested size
        const ui32 Pages;
    };

    class TRequestPageWaiter final : public TPageFetchWaiter {
    public:
        TRequestPageWaiter(TIntrusivePtr<TRequestCompletion> completion, ui32 index, bool sticky = false) noexcept;

        void Complete(TPageCacheItem page, EPageFetchCompletion completion) noexcept override;

    private:
        TIntrusivePtr<TRequestCompletion> Completion_;
        const ui32 Index_;
        const bool Sticky_;
    };

    struct TEvUpdated : public TEventLocal<TEvUpdated, EvUpdated> {
        THashMap<TLogoBlobID, THashSet<TPageOffset>> DroppedPages;
    };

    // The pages of a sticky collection, for the owner to fetch and keep.
    struct TEvStickyCollectionPages : public TEventLocal<TEvStickyCollectionPages, EvStickyCollectionPages> {
        static constexpr size_t MaxBatchLocations = 1024;

        TEvStickyCollectionPages(TLogoBlobID collectionId, TVector<TPageLocation> locations)
            : CollectionId(std::move(collectionId))
            , Locations(std::move(locations))
        {}

        const TLogoBlobID CollectionId;
        TVector<TPageLocation> Locations;
    };
    } // namespace NKikimr::NSharedCache

template<> inline
void Out<NKikimr::NTable::NPage::TPageLocation>(IOutputStream& o, const NKikimr::NTable::NPage::TPageLocation& val) {
    val.Describe(o);
}

template<> inline
void Out<NKikimr::NTable::NPage::TPageOffset>(IOutputStream& o, const NKikimr::NTable::NPage::TPageOffset& val) {
    val.Describe(o);
}

template<> inline
void Out<TVector<NKikimr::NTable::NPage::TPageLocation>>(IOutputStream& o, const TVector<NKikimr::NTable::NPage::TPageLocation>& vec) {
    o << "[ ";
    for (const auto& x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<THashSet<NKikimr::NTable::NPage::TPageOffset>>(IOutputStream& o, const THashSet<NKikimr::NTable::NPage::TPageOffset>& set) {
    o << "[ ";
    for (const auto& x : set)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<NKikimr::NTabletFlatExecutor::NBlockIO::EPriority>(
        IOutputStream& o,
        NKikimr::NTabletFlatExecutor::NBlockIO::EPriority value)
{
    switch (value) {
    case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Fast:
        o << "Online";
        break;
    case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Bkgr:
        o << "AsyncLoad";
        break;
    case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Bulk:
        o << "Scan";
        break;
    default:
        o << static_cast<ui32>(value);
        break;
    }
}
