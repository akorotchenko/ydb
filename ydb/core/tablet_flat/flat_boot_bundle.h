#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_bio_events.h"
#include "flat_sausage_packet.h"
#include "flat_part_loader.h"
#include "flat_dbase_naked.h"
#include "util_fmt_abort.h"

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TBundleLoadStep final: public NBoot::IStep {
        friend class NTable::TMetadataLoadContext;

    public:
        static constexpr NBoot::EStep StepKind = NBoot::EStep::Bundle;

        TBundleLoadStep() = delete;

        TBundleLoadStep(
            IStep* owner, ui32 table, TSwitch::TBundle& bundle, TIntrusivePtr<IPageMetadataWorkingSet> workingSet)
            : IStep(owner, NBoot::EStep::Bundle)
            , Table(table)
            , WorkingSet(std::move(workingSet))
            , LargeGlobIds(std::move(bundle.LargeGlobIds))
            , Parts(NTable::TPartComponents{
                  .PageCollectionComponents = TVector<NTable::TPageCollectionComponents>(LargeGlobIds.size()),
                  .Legacy = std::move(bundle.Legacy),
                  .Opaque = std::move(bundle.Opaque),
                  .Deltas = std::move(bundle.Deltas),
                  .Epoch = bundle.Epoch,
              })
        {
            for (ui32 slot = 0; slot < LargeGlobIds.size(); ++slot) {
                Parts.PageCollectionComponents[slot].LargeGlobId = LargeGlobIds[slot];
            }
            // Boot queues every bundle step before starting metadata requests
            if (WorkingSet) {
                WorkingSet->AddCollections(Parts.PageCollectionComponents);
            }
        }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() override
        {
            if (Logic->Cancelled) {
                return;
            }
            auto environment = Logic->PageCache.MakeLoaderEnvironment(LargeGlobIds, Back->PageCollections.Get());
            Loader = new NTable::TLoader(std::move(environment), std::move(Parts));
            Loader->BeginMetadata(MetadataContext());
            TryFinalize();
        }

        bool HandleBio(const TPageCacheResult& msg, NActors::IEventBase& event) override {
            Y_ENSURE(Loader, "PageCollections loader got un unexpected pages fetch");

            LeftReads -= 1;

            if (msg.Status == NKikimrProto::OK) {
                Loader->Save(event);

                TryFinalize();

            } else if (auto logl = Env->Logger()->Log(ELnLev::Error)) {
                logl
                    << NFmt::Do(*Back)
                    << " Page collection load failed, " << NFmt::Do(msg);
            }

            return msg.Status == NKikimrProto::OK;
        }

        void HandleStep(TIntrusivePtr<IStep> step) override {
            auto* load = step->FinalCast<TLoadBlobs>();
            const auto result = Loader->SaveMetadata(load->Cookie, load->PlainData());
            if (result == NTable::EMetadataLoadResult::Error) {
                Y_TABLET_ERROR("Got empty page collection metadata");
            }
            if (result != NTable::EMetadataLoadResult::Ignored) {
                TryFinalize();
            }
        }

        NTable::EMetadataLoadResult HandleMetadata(
            ui32 slot, const TPageCacheMetadataResult& metadata, NActors::IEventBase& event) override {
            const auto result = Loader->AcceptMetadata(slot, metadata, event, MetadataContext());
            if (result == NTable::EMetadataLoadResult::Error) {
                if (auto logl = Env->Logger()->Log(ELnLev::Error)) {
                    logl << NFmt::Do(*Back) << " Collection metadata load failed for " << LargeGlobIds[slot].Lead
                         << ": " << metadata.Error;
                }
                return result;
            }
            if (result != NTable::EMetadataLoadResult::Ignored) {
                TryFinalize();
            }
            return result;
        }

        void CancelMetadata() noexcept override {
            Loader->CancelMetadata();
        }

    private:
        NTable::TMetadataLoadContext MetadataContext() {
            return { *this, TActivationContext::ActorSystem(), Logic->SelfId, Logic->BootAttempt,
                NBlockIO::EPriority::Fast, WorkingSet.Get() };
        }

        void LoadRawMetadata(ui32 slot, const NPageCollection::TLargeGlobId& descriptor) {
            Spawn<TLoadBlobs>(descriptor, slot);
        }

        bool SendMetadataRequest(ui32 slot, const void* request, THolder<NActors::IEventBase> event) {
            return Logic->LoadMetadata(this, slot, LargeGlobIds.at(slot), request, std::move(event));
        }

        void SendMetadataUpdate(THolder<NActors::IEventBase> event) {
            Logic->UpdateMetadata(std::move(event));
        }

        void TryFinalize()
        {
            if (Logic->Cancelled || !Loader->MetadataReady()) {
                return;
            }
            if (!LeftReads) {
                if (auto fetch = Loader->Run({.PreloadIndex = true, .PreloadData = false})) {
                    LeftReads += Logic->LoadPages(this, *Loader, std::move(fetch));
                }
            }

            if (!LeftReads) {
                NTable::TPartView partView = Loader->Result();
                const auto* partStore = partView.As<NTable::TPartStore>();

                if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Back) << " table " << Table
                        << " part loaded, page collections [";

                    for (ui32 slot : xrange(partStore->GetPageCollectionCount())) {
                        logl << " " << partStore->GetPageCollection(slot)->Label();
                    }

                    logl << " ]";
                }

                Logic->PageCache.CaptureForRestart(*partStore, Logic->Result().PageCollections);
                Back->DatabaseImpl->Merge(Table, std::move(partView));

                Env->Finish(this); /* return self to owner */
            }
        }

    private:
        const ui32 Table = Max<ui32>();
        TIntrusivePtr<IPageMetadataWorkingSet> WorkingSet;

        TAutoPtr<NTable::TLoader> Loader;
        TVector<NPageCollection::TLargeGlobId> LargeGlobIds;
        NTable::TPartComponents Parts;

        TLeft LeftReads;
    };
}
}
}
