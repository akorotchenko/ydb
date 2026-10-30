#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_part_writer.h>
#include <ydb/core/tablet_flat/flat_writer_bundle.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/shared_cache_fixture.h>
#include <ydb/core/base/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable {
namespace {

    using NTabletFlatExecutor::NWriter::TBundle;

    struct TWriteOptions {
        bool Outer = false;
        bool Grouped = false;
        bool Dual = false;
        ui32 Step = 1;
    };

    TBundle::TResult WritePart(TWriteOptions options = {}) {
        NTest::TLayoutCook layout;
        layout.Col(0, 1, NScheme::NTypeIds::Uint64)
            .Col(options.Grouped ? 1 : 0, 2, NScheme::NTypeIds::String)
            .Key({ 1 });
        const auto scheme = layout.RowScheme();
        const auto tags = scheme->Tags();
        NTabletFlatExecutor::NWriter::TConf conf;
        conf.Groups.resize(options.Grouped ? 2 : 1);
        for (auto& group : conf.Groups) {
            group.Channel = 1;
            group.MaxBlobSize = 4096;
            group.Cache = NPage::ECache::Ever;
        }
        conf.BlobsChannels = { 1 };
        conf.OuterChannel = 1;
        conf.ExtraChannel = 1;
        conf.ChannelsShares = NUtil::TChannelsShares({ { 1, 1.0f } });
        conf.Slots = { { 1, 0 } };
        conf.WriteBTreeIndexV2 = options.Dual;
        TBundle bundle(TLogoBlobID(77, 1, options.Step, 0, 0, 0), conf);

        NPage::TConf pages{ true, 512 };
        pages.Groups.resize(conf.Groups.size());
        pages.WriteBTreeIndexV2 = options.Dual;
        pages.WriteFlatIndex = !options.Dual;
        for (auto& group : pages.Groups) {
            group.PageSize = 512;
            group.BTreeIndexNodeTargetSize = 512;
        }
        if (options.Outer) {
            pages.SmallEdge = 8;
            pages.LargeEdge = 128;
        }
        TPartWriter writer(new TPartScheme(scheme->Cols), tags, bundle, pages, TEpoch::Zero());
        const TString value(32, 'x');
        for (ui64 key = 0; key < 128; ++key) {
            const TCell keyCell(reinterpret_cast<const char*>(&key), sizeof(key));
            writer.BeginKey({ &keyCell, 1 });
            TRowState row(tags.size());
            row.Touch(ERowOp::Upsert);
            row.Set(1, ECellOp::Set, TCell(value.data(), value.size()));
            writer.AddKeyVersion(row, TRowVersion::Min());
            writer.EndKey();
        }
        UNIT_ASSERT_VALUES_EQUAL(writer.Finish().Parts, 1);
        auto results = bundle.Results();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        return std::move(results.front());
    }

    TPartComponents Components(const TBundle::TResult& written, bool rawMetadata = false) {
        TPartComponents components{ .Epoch = TEpoch::Zero() };
        components.Opaque = written.Overlay;
        for (const auto& collection : written.PageCollections) {
            const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(collection.PageCollection.Get());
            UNIT_ASSERT(packet);
            auto& component = components.PageCollectionComponents.emplace_back();
            component.LargeGlobId = packet->LargeGlobId;
            if (rawMetadata) {
                component.RawMeta = packet->Meta.Raw;
            }
        }
        return components;
    }

    TCacheCollectionRef AdmitCollection(const TBundle::TResult& written, ui32 slot) {
        return NSharedCache::TSharedCache::Get().AdmitCollection(written.PageCollections.at(slot).PageCollection);
    }

    TPartView Finish(TLoader& loader, const TBundle::TResult& written) {
        while (auto fetch = loader.Run({ .PreloadIndex = false, .PreloadData = false })) {
            TVector<NSharedCache::TEvResult::TLoaded> pages;
            for (const auto& location : fetch.Pages) {
                const NPageCollection::TPageData* source = nullptr;
                for (const auto& collection : written.PageCollections) {
                    if (collection.PageCollection->Label() != fetch.PageCollection->Label()) {
                        continue;
                    }
                    for (const auto* group : { &collection.StickyPages, &collection.RegularPages }) {
                        for (const auto& page : *group) {
                            if (page.Location == location) {
                                source = &page;
                            }
                        }
                    }
                }
                UNIT_ASSERT_C(source, "Requested page was not captured by the real writer");
                auto page = NSharedCache::TSharedCache::Get().AdmitPage(
                    fetch.PageCollection, location, TSharedData(source->Data), true);
                UNIT_ASSERT(page);
                pages.emplace_back(location.Offset, location.Size, std::move(page));
            }
            loader.Save(std::move(pages));
        }
        return loader.Result();
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE_F(TPartLoaderRoot, NTest::TTableCacheTest) {
    Y_UNIT_TEST(PrepareRootBeforeOuterCollectionArrives) {
        auto written = WritePart({ .Outer = true });
        UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 2);
        TLoader loader(Components(written));
        UNIT_ASSERT_EXCEPTION(loader.HasOuterCollection(), yexception);
        auto root = AdmitCollection(written, 0);
        loader.SetPageCollection(0, root.Acquire());
        loader.PrepareRoot();
        UNIT_ASSERT(loader.HasOuterCollection());
        UNIT_ASSERT_EXCEPTION(loader.SetPageCollection(0, root.Acquire()), yexception);
        TCacheCollectionRef missing;
        UNIT_ASSERT(NSharedCache::TSharedCache::Get().Find(written.PageCollections[1].PageCollection->Label(),
                        missing) == NSharedCache::ECacheResultStatus::Miss);
        UNIT_ASSERT_EXCEPTION(loader.Run({ .PreloadIndex = false, .PreloadData = false }), yexception);
        loader.SetPageCollection(1, AdmitCollection(written, 1));
        auto result = Finish(loader, written);
        UNIT_ASSERT(result.Part->Small);
        UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
        UNIT_ASSERT_EXCEPTION(loader.SetPageCollection(0, root.Acquire()), yexception);
    }

    Y_UNIT_TEST(PrepareRootBeforeOrdinaryGroupArrives) {
        auto written = WritePart({ .Grouped = true });
        UNIT_ASSERT_VALUES_EQUAL(written.PageCollections.size(), 2);
        TLoader loader(Components(written));
        auto root = AdmitCollection(written, 0);
        loader.SetPageCollection(0, root.Acquire());
        loader.PrepareRoot();
        UNIT_ASSERT(!loader.HasOuterCollection());
        UNIT_ASSERT_EXCEPTION(loader.SetPageCollection(1, root.Acquire()), yexception);
        loader.SetPageCollection(1, AdmitCollection(written, 1));
        auto result = Finish(loader, written);
        UNIT_ASSERT(!result.Part->Small);
        UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
    }

    Y_UNIT_TEST(RepeatedPreparationKeepsTheFirstIndexSelection) {
        auto& flags = AppData()->FeatureFlags;
        flags.SetEnableLocalDBBtreeIndex(true);
        flags.SetEnableLocalDBFlatIndex(true);
        flags.SetEnableLocalDBBtreeIndexV2(true);
        auto written = WritePart({ .Dual = true });
        TLoader loader(Components(written));
        auto root = AdmitCollection(written, 0);
        loader.SetPageCollection(0, root.Acquire());
        loader.PrepareRoot();
        UNIT_ASSERT(root->PageCollection()->SkipBTreeIndexV1Shadow());
        flags.SetEnableLocalDBBtreeIndexV2(false);
        loader.PrepareRoot();
        UNIT_ASSERT(root->PageCollection()->SkipBTreeIndexV1Shadow());
        auto result = Finish(loader, written);
        const auto& index = result.Part->IndexPages.BTreeGroups.at(0);
        UNIT_ASSERT(index.HasRootV2() && !index.HasRootV1());
        UNIT_ASSERT(root->PageCollection()->SkipBTreeIndexV1Shadow());
    }

    Y_UNIT_TEST(PreservesRawAndFullyPrebuiltRunPaths) {
        for (bool prebuilt : { false, true }) {
            auto written = WritePart({ .Step = prebuilt ? 2u : 1u });
            TPartComponents components{ .Epoch = TEpoch::Zero() };
            TVector<TCacheCollectionRef> collections;
            if (prebuilt) {
                components.Opaque = written.Overlay;
                for (ui32 slot = 0; slot < written.PageCollections.size(); ++slot) {
                    collections.push_back(AdmitCollection(written, slot));
                }
            } else {
                components = Components(written, true);
            }
            TLoader loader(std::move(components), std::move(collections));
            auto result = Finish(loader, written);
            UNIT_ASSERT_VALUES_EQUAL(result.Part->Stat.Rows, 128);
        }
    }
}

} // namespace NKikimr::NTable
