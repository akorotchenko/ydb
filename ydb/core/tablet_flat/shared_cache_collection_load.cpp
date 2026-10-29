#include "shared_cache_collection_load.h"
#include "flat_load_blob_queue.h"
#include "flat_sausage_packet.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/yexception.h>

namespace NKikimr::NSharedCache {
namespace {

    class TCollectionLoadActor final : public NActors::TActorBootstrapped<TCollectionLoadActor>,
                                       private NTabletFlatExecutor::ILoadBlob {
        using TThis = TCollectionLoadActor;

    public:
        TCollectionLoadActor(
            const TActorId& owner, ui64 loadId, const NPageCollection::TLargeGlobId& descriptor, EPriority priority);

        void Bootstrap();

    private:
        void StateWork(TAutoPtr<NActors::IEventHandle>& ev);
        void Handle(TEvBlobStorage::TEvGetResult::TPtr& ev);
        void Handle(TEvCollectionParse::TPtr& ev);
        void OnBlobLoaded(const TLogoBlobID& id, TString body, uintptr_t cookie) override;
        void TryParse();
        void ClearBuffers();
        void Fail(NKikimrProto::EReplyStatus status, TString error);
        void Stop();

    private:
        const TActorId Owner_;
        const ui64 LoadId_;
        const NPageCollection::TLargeGlobId Descriptor_;
        NTabletFlatExecutor::TLoadBlobQueue Queue_;
        THolder<NPageCollection::TLargeGlobIdRestoreState> Restore_;
        NActors::TSharedData Raw_;
        ECollectionLoadKind Kind_ = ECollectionLoadKind::Unknown;
        bool ParseRequested_ = false;
    };

    TCollectionLoadActor::TCollectionLoadActor(
        const TActorId& owner, ui64 loadId, const NPageCollection::TLargeGlobId& descriptor, EPriority priority)
        : Owner_(owner)
        , LoadId_(loadId)
        , Descriptor_(descriptor)
    {
        Queue_.Config.TabletID = descriptor.Lead.TabletID();
        Queue_.Config.Generation = descriptor.Lead.Generation();
        switch (priority) {
            case EPriority::None:
            case EPriority::Fast:
                Queue_.Config.ReadPrio = NKikimrBlobStorage::FastRead;
                break;
            case EPriority::Bulk:
            case EPriority::Bkgr:
                Queue_.Config.ReadPrio = NKikimrBlobStorage::AsyncRead;
                break;
            case EPriority::Low:
                Queue_.Config.ReadPrio = NKikimrBlobStorage::LowRead;
                break;
        }
    }

    void TCollectionLoadActor::Bootstrap() {
        if (!Descriptor_ || !Descriptor_.Lead || !Descriptor_.Lead.BlobSize() ||
            Descriptor_.Bytes < Descriptor_.Lead.BlobSize() || Descriptor_.Bytes > Max<ui32>() ||
            Descriptor_.Group == NPageCollection::TLargeGlobId::InvalidGroup) {
            Fail(NKikimrProto::ERROR, "Invalid collection metadata descriptor");
            return;
        }
        Restore_ = MakeHolder<NPageCollection::TLargeGlobIdRestoreState>(Descriptor_);
        for (const auto& id : Restore_->GetBlobs()) {
            Queue_.Enqueue(id, Descriptor_.Group, this);
        }
        Become(&TThis::StateWork);
        Queue_.SendRequests(SelfId());
    }

    STFUNC(TCollectionLoadActor::StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            hFunc(TEvCollectionParse, Handle);
            cFunc(NActors::TEvents::TSystem::PoisonPill, Stop);
            case NActors::TEvents::TEvUndelivered::EventType:
                Fail(NKikimrProto::UNKNOWN, "Collection metadata read was not delivered");
                break;
        }
    }

    void TCollectionLoadActor::OnBlobLoaded(const TLogoBlobID& id, TString body, uintptr_t) {
        Restore_->Apply(id, std::move(body));
    }

    void TCollectionLoadActor::Handle(TEvBlobStorage::TEvGetResult::TPtr& ev) {
        if (!Restore_) {
            return;
        }
        auto* msg = ev->Get();
        auto status = msg->Status;
        if (status == NKikimrProto::OK) {
            for (ui32 index = 0; index < msg->ResponseSz; ++index) {
                if (msg->Responses[index].Status != NKikimrProto::OK) {
                    status = msg->Responses[index].Status;
                    break;
                }
            }
        }
        try {
            // ProcessResult owns callback iteration; advance only after it returns
            if (!Queue_.ProcessResult(msg)) {
                Fail(status, "Collection metadata blob read failed");
                return;
            }
            Queue_.SendRequests(SelfId());
            if (!*Restore_) {
                return;
            }
            Raw_ = Restore_->ExtractSharedData();
            Restore_.Destroy();
            Queue_.Clear();
        } catch (const yexception&) {
            Fail(NKikimrProto::CORRUPTED, CurrentExceptionMessage());
            return;
        }
        Send(Owner_, new TEvCollectionRawReady(LoadId_, Raw_.size()));
        TryParse();
    }

    void TCollectionLoadActor::Handle(TEvCollectionParse::TPtr& ev) {
        if (ev->Sender != Owner_) {
            return;
        }
        const auto kind = ev->Get()->Kind;
        if ((kind != ECollectionLoadKind::Regular && kind != ECollectionLoadKind::Outer) ||
            (ParseRequested_ && kind != Kind_)) {
            Fail(NKikimrProto::ERROR, "Invalid collection metadata parse grant");
            return;
        }
        Kind_ = kind;
        ParseRequested_ = true;
        TryParse();
    }

    void TCollectionLoadActor::TryParse() {
        if (!ParseRequested_ || !Raw_) {
            return;
        }
        TIntrusiveConstPtr<NPageCollection::IPageCollection> candidate;
        try {
            switch (Kind_) {
                case ECollectionLoadKind::Regular:
                    candidate = MakeIntrusiveConst<NPageCollection::TPageCollection>(Descriptor_, std::move(Raw_));
                    break;
                case ECollectionLoadKind::Outer:
                    candidate = MakeIntrusiveConst<NPageCollection::TOuterPageCollection>(Descriptor_, std::move(Raw_));
                    break;
                case ECollectionLoadKind::Unknown:
                    Y_UNREACHABLE();
            }
        } catch (const yexception&) {
            Fail(NKikimrProto::CORRUPTED, CurrentExceptionMessage());
            return;
        }
        // The outcome owns the parsed buffer until the coordinator publishes or discards it
        Send(Owner_, new TEvCollectionData(NKikimrProto::OK, std::move(candidate), LoadId_));
        PassAway();
    }

    void TCollectionLoadActor::ClearBuffers() {
        Queue_.Clear();
        Restore_.Destroy();
        Raw_ = {};
    }

    void TCollectionLoadActor::Fail(NKikimrProto::EReplyStatus status, TString error) {
        ClearBuffers();
        Send(Owner_, new TEvCollectionData(status, {}, LoadId_, std::move(error)));
        PassAway();
    }

    void TCollectionLoadActor::Stop() {
        ClearBuffers();
        Send(Owner_, new TEvCollectionData(NKikimrProto::RACE, {}, LoadId_, "Collection metadata worker stopped"));
        PassAway();
    }

} // anonymous namespace

NActors::IActor* CreateCollectionLoadActor(
    const TActorId& owner, ui64 loadId, const NPageCollection::TLargeGlobId& descriptor, EPriority priority) {
    return new TCollectionLoadActor(owner, loadId, descriptor, priority);
}

} // namespace NKikimr::NSharedCache
