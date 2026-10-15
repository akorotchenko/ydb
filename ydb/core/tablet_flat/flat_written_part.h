#pragma once

#include "flat_part_loader.h"
#include "flat_writer_bundle.h"
#include "shared_sausagecache.h"

#include <ydb/library/actors/core/actor.h>
#include <util/string/builder.h>

namespace NKikimr::NTabletFlatExecutor {

// Assemble written parts through native cache references, one part's metadata at a time.
class TWrittenPartLoader {
public:
    static constexpr ui64 EventCookie = Max<ui64>() - 4;

    struct TResult {
        NTable::TPartView Part;
        TDeque<NTable::TScreen::THole> Growth;
    };

    explicit TWrittenPartLoader(TVector<NWriter::TBundle::TResult> results, const TActorContext& ctx)
        : Written_(std::move(results))
        , Owner_(ctx.SelfID)
    {
        Advance(ctx);
    }

    ~TWrittenPartLoader() {
        if (TlsActivationContext && TActivationContext::AsActorContext().SelfID == Owner_) {
            DetachCurrent(TActivationContext::AsActorContext());
        }
    }

    bool IsComplete() const noexcept {
        return Next_ == Written_.size() && !Loader_ && Error_.empty();
    }

    bool HasError() const noexcept {
        return !Error_.empty();
    }

    const TString& Error() const noexcept {
        return Error_;
    }

    TDeque<TResult> ExtractResults() {
        Y_ENSURE(IsComplete());
        return std::move(Results_);
    }

    void Cancel(const TActorContext& ctx) {
        Loader_.Reset();
        WarmPages_.clear();
        WaitPad_.Drop();
        DetachCurrent(ctx);
    }

    bool Handle(NSharedCache::TEvResult& msg, const TActorContext& ctx) {
        if (HasError() || !WaitPad_ || msg.WaitPad != WaitPad_) {
            return false;
        }
        if (msg.ResourcesReady) {
            ResourcesReady_ = true;
            if (WaitingForResources_) {
                Restart(ctx);
            }
        } else if (msg.ResourcePressure) {
            WaitingForResources_ = true;
            Loader_.Reset(); // Release this attempt's native pins before waiting for capacity.
            if (ResourcesReady_) {
                Restart(ctx);
            }
        } else if (msg.Status != NKikimrProto::OK) {
            Error_ = TStringBuilder() << "Cannot load written part metadata: " << msg.Status;
            Loader_.Reset();
            DetachCurrent(ctx);
        } else {
            Loader_->Save(std::move(msg.Pages));
            WaitPad_.Drop();
            Advance(ctx);
        }
        return true;
    }

private:
    static bool IsMetadata(const NPageCollection::TPageData& page) {
        return NTable::TLoader::NeedIn(page.Location.Type) || page.Location.Type == NTable::NPage::EPage::FlatIndex;
    }

    void StartCurrent(const TActorContext& ctx) {
        auto& written = Written_[Next_];
        Y_ENSURE(written.PageCollections, "Written part has no page collections");
        auto& cache = NSharedCache::TSharedCache::Get();
        for (size_t index = 0; index < written.PageCollections.size(); ++index) {
            auto& pages = written.PageCollections[index];
            Collections_.push_back(cache.AdmitCollection(pages.PageCollection));
            auto saved = MakeHolder<NSharedCache::TEvSaveCompactedPages>(pages.PageCollection);
            saved->CacheItem = Collections_.back().CacheItem();
            const auto addPage = [&](NPageCollection::TPageData& page) {
                saved->Pages.push_back(page.Location);
                if (index == 0 && IsMetadata(page)) {
                    if (auto ref = cache.AdmitPage(pages.PageCollection, page.Location, std::move(page.Data))) {
                        WarmPages_.push_back(std::move(ref));
                    }
                }
            };
            for (auto& page : pages.StickyPages) {
                addPage(page);
            }
            for (auto& page : pages.RegularPages) {
                addPage(page);
            }
            // This precedes this helper's first request/owner registration for the collection.
            ctx.Send(NSharedCache::MakeSharedPageCacheId(), saved.Release());
        }
        MakeLoader();
    }

    void MakeLoader() {
        TVector<NSharedCache::TSharedCacheCollectionRef> collections;
        for (const auto& collection : Collections_) {
            collections.push_back(collection.Acquire());
        }
        NTable::TPartComponents parts{
            .PageCollectionComponents = TVector<NTable::TPageCollectionComponents>(collections.size()),
            .Opaque = Written_[Next_].Overlay,
            .Epoch = NTable::TEpoch::Max(),
        };
        // Temporary native pins protect metadata; a failed attempt must be able to release all its bytes.
        Loader_ = MakeHolder<NTable::TLoader>(std::move(parts), std::move(collections), false);
    }

    void Advance(const TActorContext& ctx) {
        while (Next_ < Written_.size()) {
            if (!Loader_) {
                StartCurrent(ctx);
            }
            auto fetch = Loader_->Run({ .PreloadIndex = false, .PreloadData = false });
            WarmPages_.clear(); // The loader has acquired the available metadata pages.
            if (fetch) {
                WaitPad_ = MakeIntrusive<NPageCollection::TPagesWaitPad>();
                const ui64 pinnedBytes = Loader_->PinnedBytes();
                WaitPad_->WorkingSetBytes = pinnedBytes;
                WaitPad_->HasPinnedPages.store(pinnedBytes != 0, std::memory_order_release);
                for (const auto& page : fetch.Pages) {
                    WaitPad_->WorkingSetBytes += page.Size + TSharedData::OverheadSize;
                }
                ResourcesReady_ = false;
                auto request = MakeHolder<NSharedCache::TEvRequest>(
                    NBlockIO::EPriority::Fast, std::move(fetch.PageCollection), std::move(fetch.Pages));
                request->Sticky = std::move(fetch.Sticky);
                request->WaitPad = WaitPad_;
                ctx.Send(NSharedCache::MakeSharedPageCacheId(), request.Release(), 0, EventCookie);
                return;
            }
            auto part = Loader_->Result();
            Loader_.Reset(); // Do not accumulate native page pins across output parts.
            auto& written = Written_[Next_];
            Results_.push_back({ std::move(part), std::move(written.Growth) });
            DetachCurrent(ctx);
            ++Next_;
        }
        // Sticky data warming must not consume capacity needed to assemble another output part.
        for (auto& written : Written_) {
            for (auto& pages : written.PageCollections) {
                const auto warm = [&](NPageCollection::TPageData& page, bool sticky) {
                    if (page.Data) {
                        NSharedCache::TSharedCache::Get().AdmitPage(
                            pages.PageCollection, page.Location, std::move(page.Data), sticky);
                    }
                };
                for (auto& page : pages.StickyPages) {
                    warm(page, true);
                }
                for (auto& page : pages.RegularPages) {
                    warm(page, false);
                }
            }
            written.PageCollections.clear();
        }
    }

    void Restart(const TActorContext& ctx) {
        WaitPad_.Drop();
        WaitingForResources_ = false;
        MakeLoader();
        Advance(ctx);
    }

    void DetachCurrent(const TActorContext& ctx) {
        for (const auto& collection : Collections_) {
            ctx.Send(NSharedCache::MakeSharedPageCacheId(), new NSharedCache::TEvDetach(collection->Id()));
        }
        Collections_.clear();
    }

    TVector<NWriter::TBundle::TResult> Written_;
    const TActorId Owner_;
    size_t Next_ = 0;
    TVector<NSharedCache::TSharedCacheCollectionRef> Collections_;
    TVector<NSharedCache::TSharedCachePageRef> WarmPages_;
    THolder<NTable::TLoader> Loader_;
    TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad_;
    bool WaitingForResources_ = false;
    bool ResourcesReady_ = false;
    TString Error_;
    TDeque<TResult> Results_;
};

} // namespace NKikimr::NTabletFlatExecutor
