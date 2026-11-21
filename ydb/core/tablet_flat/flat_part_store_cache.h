#pragma once

#include "flat_part_store.h"
#include "flat_sausagecache_lf.h"

namespace NKikimr::NTable {

class TCachePartStore final : public TPartStore {
protected:
    TCachePartStore(const TCachePartStore& src, TEpoch epoch)
        : TPartStore(src, epoch)
        , Pseudo(src.Pseudo.Acquire())
    {
        PageCollections.reserve(src.PageCollections.size());
        for (const auto& collection : src.PageCollections) {
            PageCollections.push_back(collection.Acquire());
        }
    }

public:
    using TPartStore::GetPageCollection;
    using TPartStore::TPartStore;

    ui32 GetPageCollectionCount() const noexcept override {
        return PageCollections.size();
    }

    const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetPageCollectionRef(ui32 slot) const override {
        return PageCollections.at(slot)->PageCollection();
    }

    const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetPseudoPageCollectionRef() const noexcept override {
        if (Pseudo) {
            return Pseudo->PageCollection();
        }
        static const TIntrusiveConstPtr<NPageCollection::IPageCollection> empty;
        return empty;
    }

    using TPartStore::GetPageSize;

    ui64 GetPageSize(ELargeObj lob, ui64 ref) const override {
        auto* cache = Locate(lob, ref);

        return cache->PageCollection()->Page(ref).Size;
    }

    const NPageCollection::IPageCollection* GetPageCollection(ui32 slot) const override {
        Y_ENSURE(slot < PageCollections.size());
        return PageCollections[slot]->PageCollection().Get();
    }

    ui64 GetSlotPageSize(ui32 slot, NPage::TPageId pageId) const override {
        Y_ENSURE(slot < PageCollections.size());
        return PageCollections[slot]->GetPageSize(pageId);
    }

    NPage::EPage GetSlotPageType(ui32 slot, NPage::TPageId pageId) const override {
        Y_ENSURE(slot < PageCollections.size());
        return PageCollections[slot]->GetPageType(pageId);
    }

    ui8 GetSlotChannel(ui32 slot) const override {
        Y_ENSURE(slot < PageCollections.size());
        return PageCollections[slot]->Id().Channel();
    }

    TIntrusiveConstPtr<TPart> CloneWithEpoch(TEpoch epoch) const override {
        return new TCachePartStore(*this, epoch);
    }

    NSharedCache::TCollection* Locate(ELargeObj lob, ui64 ref) const {
        return (IsExternalPageRef(lob, ref) ? Pseudo : PageCollections.at(OuterSlot())).Get();
    }

    TVector<TCacheCollectionRef> PageCollections;
    TCacheCollectionRef Pseudo; /* Cache for NPage::TBlobs */
};

} // namespace NKikimr::NTable
