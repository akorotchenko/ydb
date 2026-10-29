#pragma once

#include "flat_sausage_solid.h"
#include "shared_cache_events.h"

namespace NKikimr::NSharedCache {

enum class ECollectionLoadKind {
    Unknown,
    Regular,
    Outer,
};

struct TEvCollectionRawReady : public TEventLocal<TEvCollectionRawReady, EvCollectionRawReady> {
    const ui64 LoadId;
    const ui64 Bytes;

    TEvCollectionRawReady(ui64 loadId, ui64 bytes) noexcept
        : LoadId(loadId)
        , Bytes(bytes)
    {}
};

// The owner grants parsing only after the representation and parser slot are available
struct TEvParseCollection : public TEventLocal<TEvParseCollection, EvParseCollection> {
    const ECollectionLoadKind Kind;

    explicit TEvParseCollection(ECollectionLoadKind kind) noexcept
        : Kind(kind)
    {}
};

struct TEvCollectionLoaded : public TEventLocal<TEvCollectionLoaded, EvCollectionLoaded> {
    const ui64 LoadId;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;

    TEvCollectionLoaded(ui64 loadId, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection) noexcept
        : LoadId(loadId)
        , PageCollection(std::move(pageCollection))
    {}
};

struct TEvCollectionLoadFailed : public TEventLocal<TEvCollectionLoadFailed, EvCollectionLoadFailed> {
    const ui64 LoadId;
    const NKikimrProto::EReplyStatus Status;
    const TString Error;

    TEvCollectionLoadFailed(ui64 loadId, NKikimrProto::EReplyStatus status, TString error) noexcept
        : LoadId(loadId)
        , Status(status)
        , Error(std::move(error))
    {}
};

struct TEvCollectionLoadStopped : public TEventLocal<TEvCollectionLoadStopped, EvCollectionLoadStopped> {
    const ui64 LoadId;

    explicit TEvCollectionLoadStopped(ui64 loadId) noexcept
        : LoadId(loadId)
    {}
};

// Worker identity is the attempt identity; outcomes carry the stable load identity
NActors::IActor* CreateCollectionLoadActor(const TActorId& owner, ui64 loadId,
    const NPageCollection::TLargeGlobId& descriptor, EPriority priority = EPriority::Fast);

} // namespace NKikimr::NSharedCache
