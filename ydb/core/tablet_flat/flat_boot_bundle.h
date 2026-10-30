#pragma once

#include "flat_boot_iface.h"
#include "flat_boot_back.h"
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
            if (Logic->Cancelled) {
                return;
            }
            TVector<NTable::TPageCollectionComponents> components(LargeGlobIds.size());
            for (ui32 slot = 0; slot < LargeGlobIds.size(); ++slot) {
                components[slot].LargeGlobId = LargeGlobIds[slot];
            }
            NTable::TPartComponents parts{
                .PageCollectionComponents = std::move(components),
                .Legacy = std::move(Legacy),
                .Opaque = std::move(Opaque),
                .Deltas = std::move(Deltas),
                .Epoch = Epoch,
            };
            Loader = new NTable::TLoader(std::move(parts));
            Requests.resize(LargeGlobIds.size());
            for (ui32 slot = 0; slot < LargeGlobIds.size(); ++slot) {
                const auto kind = slot == 0 || slot + 1 < LargeGlobIds.size()
                                      ? NSharedCache::ECollectionLoadKind::Regular
                                      : NSharedCache::ECollectionLoadKind::Unknown;
                Requests[slot] = MakeIntrusive<NSharedCache::TCollectionRequest>(LargeGlobIds[slot], kind);
                LeftMetas += Logic->LoadCollection(this, slot, Requests[slot]);
            }
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

        bool HandleCollection(ui32 slot, NSharedCache::TEvCollectionResult& msg) override
        {
            Y_ENSURE(slot < Requests.size() && Requests[slot] == msg.Request, "Unexpected collection metadata result");
            if (msg.Status != NKikimrProto::OK) {
                if (auto logl = Env->Logger()->Log(ELnLev::Error)) {
                    logl << NFmt::Do(*Back) << " Collection metadata load failed for " << LargeGlobIds[slot].Lead
                         << ": " << msg.Error;
                }
                return false;
            }
            Loader->SetPageCollection(slot, std::move(msg.Collection));
            if (slot == 0) {
                Loader->PrepareRoot();
                if (Requests.size() > 1) {
                    const auto kind = Loader->HasOuterCollection() ? NSharedCache::ECollectionLoadKind::Outer
                                                                   : NSharedCache::ECollectionLoadKind::Regular;
                    Y_ENSURE(Requests.back()->Resolve(kind));
                    Logic->UpdateCollection(Requests.back());
                }
            }
            Requests[slot].Reset();
            LeftMetas -= 1;
            if (!LeftMetas) {
                TryFinalize();
            }
            return true;
        }

    private:
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
        TVector<TIntrusivePtr<NSharedCache::TCollectionRequest>> Requests;
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
