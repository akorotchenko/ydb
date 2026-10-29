#pragma once

#include "flat_sausage_solid.h"
#include "shared_cache_events.h"

#include <atomic>

namespace NKikimr::NSharedCache {

enum class ECollectionLoadKind {
    Unknown,
    Regular,
    Outer,
};

// Cancellation and role resolution may precede delivery of the request event
class TCollectionRequest : public TThrRefBase {
public:
    explicit TCollectionRequest(const NPageCollection::TLargeGlobId& descriptor,
        ECollectionLoadKind kind = ECollectionLoadKind::Unknown) noexcept
        : Descriptor(descriptor)
        , Kind_(kind)
    {}

    ECollectionLoadKind Kind() const noexcept {
        return Kind_.load(std::memory_order_acquire);
    }

    // Send TEvCollectionRequestUpdate after Resolve or Cancel to wake a joined load
    bool Resolve(ECollectionLoadKind kind) noexcept;

    void Cancel() noexcept {
        Cancelled_.store(true, std::memory_order_release);
    }

    bool IsActive() const noexcept {
        return !Cancelled_.load(std::memory_order_acquire) && !Completed_.load(std::memory_order_acquire);
    }

    const NPageCollection::TLargeGlobId Descriptor;

private:
    friend class TCollectionLoadController;

    bool TryComplete() noexcept;

    std::atomic<ECollectionLoadKind> Kind_;
    std::atomic<bool> Cancelled_ = false;
    std::atomic<bool> Completed_ = false;
    bool Joined_ = false; // Cache actor only
};

struct TEvCollectionRequest : public TEventLocal<TEvCollectionRequest, EvCollectionRequest> {
    TIntrusivePtr<TCollectionRequest> Request;
    const EPriority Priority;

    explicit TEvCollectionRequest(
        TIntrusivePtr<TCollectionRequest> request, EPriority priority = EPriority::Fast) noexcept
        : Request(std::move(request))
        , Priority(priority)
    {}
};

// A hint to re-read the request's durable cancellation/role state
struct TEvCollectionRequestUpdate : public TEventLocal<TEvCollectionRequestUpdate, EvCollectionRequestUpdate> {
    TIntrusivePtr<TCollectionRequest> Request;

    explicit TEvCollectionRequestUpdate(TIntrusivePtr<TCollectionRequest> request) noexcept
        : Request(std::move(request))
    {}
};

struct TEvCollectionResult : public TEventLocal<TEvCollectionResult, EvCollectionResult> {
    TIntrusivePtr<TCollectionRequest> Request;
    const NKikimrProto::EReplyStatus Status;
    TCacheCollectionRef Collection;
    const TString Error;

    TEvCollectionResult(TIntrusivePtr<TCollectionRequest> request, NKikimrProto::EReplyStatus status,
        TCacheCollectionRef collection = {}, TString error = {}) noexcept
        : Request(std::move(request))
        , Status(status)
        , Collection(std::move(collection))
        , Error(std::move(error))
    {}
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
struct TEvCollectionParse : public TEventLocal<TEvCollectionParse, EvCollectionParse> {
    const ECollectionLoadKind Kind;

    explicit TEvCollectionParse(ECollectionLoadKind kind) noexcept
        : Kind(kind)
    {}
};

struct TEvCollectionData : public TEventLocal<TEvCollectionData, EvCollectionData> {
    using EStatus = NKikimrProto::EReplyStatus;

    const EStatus Status;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    const ui64 LoadId;
    const TString Error;

    TEvCollectionData(EStatus status, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, ui64 loadId,
        TString error = {}) noexcept
        : Status(status)
        , PageCollection(std::move(pageCollection))
        , LoadId(loadId)
        , Error(std::move(error))
    {}
};

// Worker identity is the attempt identity; outcomes carry the stable load identity
NActors::IActor* CreateCollectionLoadActor(const TActorId& owner, ui64 loadId,
    const NPageCollection::TLargeGlobId& descriptor, EPriority priority = EPriority::Fast);

} // namespace NKikimr::NSharedCache
