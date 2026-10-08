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
    public:
        static constexpr NBoot::EStep StepKind = NBoot::EStep::Bundle;

        TBundleLoadStep() = delete;

        TBundleLoadStep(IStep *owner, ui32 table, TSwitch::TBundle &bundle)
            : IStep(owner, NBoot::EStep::Bundle)
            , Table(table)
            , LargeGlobIds(std::move(bundle.LargeGlobIds))
            , Legacy(std::move(bundle.Legacy))
            , Opaque(std::move(bundle.Opaque))
            , Deltas(std::move(bundle.Deltas))
            , Epoch(bundle.Epoch)
        {

        }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() override
        {
            Prebuilt.resize(LargeGlobIds.size());
            Components.resize(LargeGlobIds.size());

            auto* cache = TSharedCache::TrySharedCachePages();
            auto binding = cache ? cache->BindCurrentThreadHazard() : TSharedCacheThreadBinding<TProdTraits>{};
            for (auto slot: xrange(LargeGlobIds.size())) {
                if (cache) {
                    cache->Find(LargeGlobIds[slot].Lead, Prebuilt[slot]);
                }
                if (!Prebuilt[slot]) {
                    LeftMetas += Spawn<TLoadBlobs>(LargeGlobIds[slot], slot);
                }
            }

            TryLoad();
        }

        bool HandleBio(NSharedCache::TEvResult &msg) override
        {
            Y_ENSURE(Loader, "PageCollections loader got un unexpected pages fetch");

            LeftReads -= 1;

            if (msg.Status == NKikimrProto::OK) {
                Loader->Save(std::move(msg.Pages));

                TryFinalize();

            } else if (auto logl = Env->Logger()->Log(ELnLev::Error)) {
                logl
                    << NFmt::Do(*Back)
                    << " Page collection load failed, " << NFmt::Do(msg);
            }

            return msg.Status == NKikimrProto::OK;
        }

        void HandleStep(TIntrusivePtr<IStep> step) override
        {
            auto *load = step->ConsumeAs<TLoadBlobs>(LeftMetas);

            if (Loader) {
                Y_TABLET_ERROR("Got an unexpected load blobs result");
            } else if (load->Cookie >= LargeGlobIds.size()) {
                Y_TABLET_ERROR("Got blobs load step with an invalid cookie");
            } else if (Prebuilt[load->Cookie]) {
                Y_TABLET_ERROR("Page collection is already loaded at room " << load->Cookie);
            } else {
                // StageParseMeta constructs both NPageCollection::TPageCollection and TCacheCollection
                Components[load->Cookie].LargeGlobId = load->LargeGlobId;
                Components[load->Cookie].RawMeta = load->PlainData();
            }

            TryLoad();
        }

    private:
        void TryLoad()
        {
            if (!LeftMetas) {
                NTable::TPartComponents parts{
                    .PageCollectionComponents = std::move(Components),
                    .Legacy = std::move(Legacy),
                    .Opaque = std::move(Opaque),
                    .Deltas = std::move(Deltas),
                    .Epoch = Epoch,
                };

                // Pass pre-built collections — StageParseMeta fills null slots from components
                Loader = new NTable::TLoader(std::move(parts), std::move(Prebuilt));

                TryFinalize();
            }
        }

        void TryFinalize()
        {
            if (!LeftReads) {
                if (auto fetch = Loader->Run({.PreloadIndex = true, .PreloadData = false})) {
                    LeftReads += Logic->LoadPages(this, std::move(fetch));
                }
            }

            if (!LeftReads) {
                NTable::TPartView partView = Loader->Result();

                if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Back) << " table " << Table
                        << " part loaded, page collections [";

                    for (auto &cache : partView.As<NTable::TPartStore>()->PageCollections)
                        logl << " " << cache->Id();

                    logl << " ]";
                }

                Back->DatabaseImpl->Merge(Table, std::move(partView));

                Env->Finish(this); /* return self to owner */
            }
        }

    private:
        const ui32 Table = Max<ui32>();

        TAutoPtr<NTable::TLoader> Loader;
        TVector<NPageCollection::TLargeGlobId> LargeGlobIds;
        TVector<NTable::TPageCollectionComponents> Components;
        // Native metadata refs acquired directly from core for this bundle's slots.
        TVector<TSharedCacheCollectionRef> Prebuilt;
        TString Legacy;
        TString Opaque;
        TVector<TString> Deltas;
        NTable::TEpoch Epoch;

        TLeft LeftMetas;
        TLeft LeftReads;
    };
}
}
}
