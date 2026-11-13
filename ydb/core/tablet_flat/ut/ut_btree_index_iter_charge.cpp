#include <ydb/core/tablet_flat/test/libs/table/cache_fixture.h>
#include "flat_page_btree_index.h"
#include "flat_part_charge_flat_index.h"
#include "flat_part_index_iter_bree_index.h"
#include "flat_part_charge_btree_index.h"
#include "flat_executor_tx_env.h"
#include "flat_part_charge_range.h"
#include "flat_part_charge_create.h"
#include "flat_part_keys.h"
#include "flat_table.h"
#include "flat_database.h"
#include "flat_cxx_database.h"
#include "flat_dbase_naked.h"
#include "test/libs/table/test_writer.h"
#include "test/libs/table/wrap_part.h"
#include <test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable::NPage {

static_assert(std::is_same_v<IOwnedPages::TPageRef, TCachePageRef>);
static_assert(std::is_same_v<IBorrowedPages::TPageRef, TCachePageConstRef>);
static_assert(std::is_base_of_v<IPageEnvironment, IOwnedPages>);
static_assert(std::is_base_of_v<IPageEnvironment, IBorrowedPages>);
static_assert(!std::is_convertible_v<IBorrowedPages*, IOwnedPages*>);
static_assert(std::is_same_v<decltype(std::declval<IOwnedPages&>().TryGetPage(nullptr, {}, {})), TCachePageRef>);
static_assert(
    std::is_same_v<decltype(std::declval<IBorrowedPages&>().TryGetPage(nullptr, {}, {})), TCachePageConstRef>);
static_assert(
    std::is_same_v<decltype(std::declval<IBorrowedPages&>().TryGetIndexPage(nullptr, {}, {})), TCachePageConstRef>);
static_assert(std::is_same_v<IOwnedPages::TResult, IBorrowedPages::TResult>);
static_assert(sizeof(IPageEnvironment::TResult) == 24);

template <class TReader>
concept TCanSelectTemporary =
    requires(TReader&& reader, TRowState& row) { std::move(reader).Select(1, TRawVals{}, TTagsRef{}, row); };

static_assert(!TCanSelectTemporary<TOwningDatabaseReader>);
static_assert(!TCanSelectTemporary<TTransactionReader>);
static_assert(!std::is_copy_constructible_v<TTransactionReader>);
static_assert(!std::is_move_constructible_v<TTransactionReader>);
static_assert(std::is_same_v<TTransactionReader::TIterator, TBorrowedTableIter>);
static_assert(std::is_same_v<TTransactionReader::TReverseIterator, TBorrowedTableReverseIter>);

static_assert(std::is_same_v<TBtreeIndexNode::THeader, TBorrowedBtreeIndexNode::THeader>);
static_assert(std::is_same_v<TBtreeIndexNode::TChild, TBorrowedBtreeIndexNode::TChild>);
static_assert(std::is_same_v<TBtreeIndexNode::TChildV2, TBorrowedBtreeIndexNode::TChildV2>);
static_assert(std::is_same_v<TBtreeIndexNode::TCellsIterable, TBorrowedBtreeIndexNode::TCellsIterable>);
static_assert(std::is_same_v<decltype(std::declval<const TBtreeIndexNode&>().GetChild(0, false)), TPageRef>);
static_assert(std::is_same_v<decltype(std::declval<const TBorrowedBtreeIndexNode&>().GetChild(0, false)), TPageRef>);
static_assert(
    std::is_same_v<decltype(std::declval<const TBorrowedBtreeIndexNode&>().GetPageRef()), const TCachePageConstRef&>);

static_assert(!std::is_constructible_v<TPartGroupBtreeIndexIter, const TPart*, IBorrowedPages*, TGroupId>);
static_assert(!std::is_constructible_v<TPartGroupFlatIndexIter, const TPart*, IBorrowedPages*, TGroupId>);
static_assert(
    std::is_constructible_v<TPartGroupBtreeIndexIterImpl<TCachePageConstRef>, const TPart*, IBorrowedPages*, TGroupId>);
static_assert(
    std::is_constructible_v<TPartGroupFlatIndexIterImpl<TCachePageConstRef>, const TPart*, IBorrowedPages*, TGroupId>);

template <class TReader>
concept THasOwnedKeyPage = requires(const TReader& reader) { reader.GetKeyPage(); };

static_assert(THasOwnedKeyPage<TPartIter>);
static_assert(THasOwnedKeyPage<TRunIter>);
static_assert(!THasOwnedKeyPage<TPartIterImpl<TCachePageConstRef>>);
static_assert(!THasOwnedKeyPage<TRunIterImpl<TCachePageConstRef>>);
static_assert(!std::is_constructible_v<TPartGroupRowIter, const TPart*, IBorrowedPages*, TGroupId>);
static_assert(!std::is_constructible_v<TPartHistoryIter, const TPart*, IBorrowedPages*>);

static_assert(!std::is_constructible_v<TKeysLoader, const TPart*, IBorrowedPages*>);
static_assert(std::is_constructible_v<TKeysLoaderImpl<TCachePageConstRef>, const TPart*, IBorrowedPages*>);
static_assert(!std::is_constructible_v<TChargeBTreeIndex, IBorrowedPages*, const TPart&, TTagsRef, bool>);
static_assert(!std::is_constructible_v<TChargeFlatIndex, IBorrowedPages*, const TPart&, TTagsRef, bool>);

using NTest::TPartStore;

namespace {
    using namespace NTest;
    using TShortChild = TBtreeIndexNode::TShortChild;
    using TChild = TBtreeIndexNode::TChild;

    struct TTouchEnv : public NTest::TTestEnv {
        TCachePageRef TryGetPage(const TPart *part, const TPageLocation& location, TGroupId groupId) override
        {
            if (Touch(groupId, ResolvePageId(part, location, part->DataSlot(groupId)))) {
                return NTest::TTestEnv::TryGetPage(part, location, groupId);
            }
            return {};
        }

        TCachePageRef TryGetIndexPage(
            const TPart* part, const TPageLocation& location, TGroupId groupId) override {
            // index pages are tracked as pages of their page collection
            const ui32 slot = part->IndexSlot(groupId);
            if (Touch(TGroupId(slot), ResolvePageId(part, location, slot))) {
                return NTest::TTestEnv::TryGetIndexPage(part, location, groupId);
            }
            return {};
        }

        bool Touch(TGroupId groupId, TPageId pageId)
        {
            if (Sticky[groupId].contains(pageId)) {
                Loaded[groupId].insert(pageId);
            }

            Touched[groupId].insert(pageId);
            return Loaded[groupId].contains(pageId);
        }

        void LoadTouched() {
            for (const auto &g : Touched) {
                Loaded[g.first].insert(g.second.begin(), g.second.end());
            }
            Touched.clear();
        }

        void StickLoaded() {
            for (const auto &g : Loaded) {
                Sticky[g.first].insert(g.second.begin(), g.second.end());
            }
            Touched.clear();
            Loaded.clear();
        }

        TMap<TGroupId, TSet<TPageId>> Loaded;
        TMap<TGroupId, TSet<TPageId>> Touched;
        TMap<TGroupId, TSet<TPageId>> Sticky;
    };

    class TReadTransaction final : public NTabletFlatExecutor::ITransaction {
        bool Execute(TTransactionContext&, const NActors::TActorContext&) override {
            return true;
        }

        void Complete(const NActors::TActorContext&) override {}
    };

    ui32 ActiveReadOperations() {
        return NSharedCache::SpaceHazardActiveCount(
            NSharedCache::TlsCacheHazard->State.load(std::memory_order_relaxed));
    }

    class TReadEnv : public NTabletFlatExecutor::TPageCollectionReadEnv {
    public:
        using TPageCollectionReadEnv::TPageCollectionReadEnv;

        TCachePageRef TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override {
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            ++DataReads;
            if (ThrowOnData) {
                ythrow yexception() << "read scope data failure";
            }
            return MissData ? TCachePageRef() : Storage_.TryGetPage(part, location, groupId);
        }

        TCachePageRef TryGetIndexPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override {
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            ++IndexReads;
            return Storage_.TryGetIndexPage(part, location, groupId);
        }

        ui32 DataReads = 0;
        ui32 IndexReads = 0;
        bool MissData = false;
        bool ThrowOnData = false;

    private:
        NTest::TTestEnv Storage_;
    };

    void AssertLoadedTheSame(const TPartStore& part, const TTouchEnv& bTree, const TTouchEnv& flat, const TString& message,
            bool allowAdditionalFirstLastPartPages = false, bool allowAdditionalFirstLoadedPage = false, bool allowLastLoadedPageDifference = false) {

        TSet<TGroupId> groupIds;
        for (const auto &c : {bTree.Loaded, flat.Loaded}) {
            for (const auto &g : c) {
                groupIds.insert(g.first);
            }
        }

        for (TGroupId groupId : groupIds) {
            TSet<TPageId> bTreeDataPages, flatDataPages;
            for (TPageId pageId : bTree.Loaded.Value(groupId, TSet<TPageId>{})) {
                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                    bTreeDataPages.insert(pageId);
                }
            }
            for (TPageId pageId : flat.Loaded.Value(groupId, TSet<TPageId>{})) {
                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                    flatDataPages.insert(pageId);
                }
            }

            // Note: it's possible that B-Tree index touches extra first / last page because it doesn't have boundary keys
            // this should be resolved using slices (see ChargeRange)
            if (allowAdditionalFirstLastPartPages) {
                for (auto additionalLoc : {IndexTools::GetFirstPageLocation(part, groupId), IndexTools::GetLastPageLocation(part, groupId)}) {
                    TPageId additionalPageId;
                    if (additionalLoc.Offset.IsByteOffset()) {
                        additionalPageId = part.Store->ResolveByteOffset(groupId.Index, additionalLoc.Offset.AsByteOffset());
                    } else {
                        additionalPageId = additionalLoc.Offset.AsPageIndex();
                    }
                    if (bTreeDataPages.contains(additionalPageId)) {
                        flatDataPages.insert(additionalPageId);
                    }
                }
            }
            // Note: due to implementation details it is possible that B-Tree index touches an extra page
            if (groupId.IsMain() && allowAdditionalFirstLoadedPage && flatDataPages.size() + 1 == bTreeDataPages.size()) {
                flatDataPages.insert(*bTreeDataPages.begin());
            }
            if (groupId.IsMain() && allowLastLoadedPageDifference && flatDataPages.size() + 1 == bTreeDataPages.size()) {
                flatDataPages.insert(*bTreeDataPages.rbegin());
            }
            UNIT_ASSERT_VALUES_EQUAL_C(flatDataPages, bTreeDataPages,
                TStringBuilder() << message << " Group " << groupId);
        }
    }

    struct TTestParams {
        enum ESlices : ui32 {
            None,
            Many,
            Type1,
            Type2,
            Type3,
            Type4,
            Type5,
            Type6,
            Type7,
            // new types should go here

            End
        };

        const ui32 Levels = Max<ui32>();
        const bool Groups = false;
        const bool History = false;
        const ESlices Slices = None;
        const ui32 Rows = 40;
        const bool StickSomePages = false;
        const bool FlatOnly = false;
    };

    void SetupSlices(const TTestParams& params, const TPartStore& part) {
        if (params.Slices) {
            TSlices slices;
            auto partSlices = const_cast<TSlices*>(part.Slices.Get());
            auto add = [&](ui32 pageIndex1Inclusive, ui32 pageIndex2Exclusive) {
                slices.push_back(IndexTools::MakeSlice(part, pageIndex1Inclusive, pageIndex2Exclusive));
            };

            switch (params.Slices) {
            case TTestParams::Many:
                add(0, 2);
                add(3, 4);
                add(4, 6);
                add(7, 8);
                add(8, 9);
                add(10, 14);
                add(16, 17);
                add(17, 19);
                add(19, 20);
                break;
            case TTestParams::Type1:
                add(7, 8);
                add(8, 9);
                break;
            case TTestParams::Type2:
                add(7, 8);
                break;
            case TTestParams::Type3:
                add(7, 9);
                break;
            case TTestParams::Type4:
                add(7, 10);
                break;
            case TTestParams::Type5:
                add(8, 10);
                break;
            case TTestParams::Type6:
                add(9, 10);
                break;
            case TTestParams::Type7:
                add(2, 4);
                add(17, 18);
                break;
            default:
                Y_ABORT("Unknown slices");
            }

            partSlices->clear();
            for (auto s : slices) {
                partSlices->push_back(s);
            }
        }

        // Debug dump runs for every slice mode (None and Many included).
        if (params.Slices <= TTestParams::None + 1) {
            Cerr << DumpPart(part, 3) << Endl;
        }
        Cerr << "Slices";
        part.Slices->Describe(Cerr);
        Cerr << Endl;
    }

    TPartEggs MakePart(TTestParams params) {
        NPage::TConf conf;
        conf.WriteBTreeIndexV2 = false;
        switch (params.Levels) {
        case 0:
            conf.Group(0).PageRows = 999;
            break;
        case 1:
            conf.Group(0).PageRows = 2;
            break;
        case 3:
            conf.Group(0).PageRows = 2;
            conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 2;
            break;
        default:
            Y_TABLET_ERROR("Unknown levels");
        }

        if (params.Groups) {
            conf.Group(1).PageRows = params.Levels ? 1 : 999;
            conf.Group(2).PageRows = 3;
            conf.Group(3).PageRows = 1;

            conf.Group(1).BTreeIndexNodeKeysMin = conf.Group(1).BTreeIndexNodeKeysMax = conf.Group(0).BTreeIndexNodeKeysMax;
            conf.Group(2).BTreeIndexNodeKeysMin = conf.Group(2).BTreeIndexNodeKeysMax = 2;
            conf.Group(3).BTreeIndexNodeKeysMin = conf.Group(3).BTreeIndexNodeKeysMax = 999;
        }

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(params.Groups ? 1 : 0, 2,  NScheme::NTypeIds::Uint32)
            .Col(params.Groups ? 2 : 0, 3,  NScheme::NTypeIds::Uint64)
            .Col(params.Groups ? 3 : 0, 4,  NScheme::NTypeIds::String)
            .Key({0, 1});

        // these tests are based on comparison of flat and b-tree indexes
        conf.WriteBTreeIndexV1 = !params.FlatOnly;
        conf.WriteFlatIndex = true;

        TPartCook cook(lay, conf);

        // making part with key gaps
        const TVector<ui32> secondCells = {1, 3, 4, 6, 7, 8, 10};
        for (ui32 i : xrange<ui32>(0, 40)) {
            for (int ver = params.History ? i % 3 : 0; ver >= 0; ver--) {
                cook.Ver({0, ui64(ver)}).Add(*TSchemedCookRow(*lay).Col(i / 7, secondCells[i % 7], i, static_cast<ui64>(i), TString("xxxxxxxxxx_" + std::to_string(i))));
            }
        }

        TPartEggs eggs = cook.Finish();

        const auto part = *eggs.Lone();

        SetupSlices(params, part);

        if (!params.FlatOnly) {
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[0].LevelCount(), params.Levels);
        }
        if (params.Groups && !params.FlatOnly) {
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[1].LevelCount(), params.Levels);
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[2].LevelCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[3].LevelCount(), 1);
        }

        return eggs;
    }

    TVector<TCell> MakeKey(ui32 firstCell, ui32 secondCell) {
        if (secondCell <= 11) {
            // valid second cell [0 .. 11]
            return {TCell::Make(firstCell), TCell::Make(secondCell)};
        }
        if (secondCell == 12) {
            return {TCell::Make(firstCell)};
        }
        if (secondCell == 13) {
            return { };
        }
        Y_UNREACHABLE();
    }

    EReady Retry(std::function<EReady()> action, TTouchEnv& env, const TString& message, ui32 failsAllowed = 10) {
        for (ui32 attempt = 0; attempt <= failsAllowed; attempt++) {
            env.LoadTouched();
            if (auto ready = action(); ready != EReady::Page) {
                return ready;
            }
        }

        TStringBuilder error;
        error << "Too many fails (" << failsAllowed + 1 << ") " << message << Endl << "Requests ";
        for (const auto& [groupId, pages] : env.Touched) {
            for (auto pageId : pages) {
                if (!env.Loaded[groupId].contains(pageId)) {
                    error << groupId << "#" << pageId << " ";
                }
            }
        }

        UNIT_ASSERT_C(false,  error);
        return EReady::Page;
    }

    EReady Retry(std::function<EReady()> action, TForwardEnv& env, const TString& message, ui32 failsAllowed = 10) {
        Y_UNUSED(env); // loads pages automatically

        for (ui32 attempt = 0; attempt <= failsAllowed; attempt++) {
            if (auto ready = action(); ready != EReady::Page) {
                return ready;
            }
        }

        TStringBuilder error;
        error << "Too many fails (" << failsAllowed + 1 << ") " << message << Endl << "Requests ";
        UNIT_ASSERT_C(false,  error);
        return EReady::Page;
    }

    // V2 twin of TTouchEnv: offset-keyed and Type-aware, mirroring production
    // TPageCollectionReadEnv. TTouchEnv keys on TPageId and ignores location.Type,
    // so it cannot see the offset/Type collision that the V2 charger produces
    // when it mis-tags the deepest index level as EPage::DataPage. This env keys
    // by byte offset (within room, since the same offset names different pages
    // in room 0 vs a column-group room) and validates the requested Type against
    // the store's truth on every access (hits and misses alike), so a wrong Type
    // is caught even for an offset already requested once.
    struct TTouchEnvV2 : public NTest::TTestEnv {
        const TPartStore* Part = nullptr;

        // room -> loaded page bodies keyed by byte offset within that room
        THashMap<ui32, THashMap<NPage::TPageOffset, TSharedData>> Loaded;
        // room -> this round's outstanding misses
        THashMap<ui32, TVector<NPage::TPageLocation>> Touched;

        explicit TTouchEnvV2(const TPartStore* part)
            : Part(part) {}

        TCachePageRef TryGetPage(const TPart* part, const NPage::TPageLocation& location,
                NPage::TGroupId groupId) override {
            return TryGetRoomPage(location, part->DataSlot(groupId));
        }

        TCachePageRef TryGetIndexPage(
            const TPart* part, const NPage::TPageLocation& location, NPage::TGroupId groupId) override {
            return TryGetRoomPage(location, part->IndexSlot(groupId));
        }

        TCachePageRef TryGetRoomPage(const NPage::TPageLocation& location, ui32 room) {
            // Type-aware on every access, cache hits included: the store knows the
            // true page type for this offset/room. Validate the charger's requested
            // Type before serving from cache, so a same-offset request that later
            // arrives with a wrong Type is caught too (it cannot be seen on the
            // miss path alone once the page is already loaded). The buggy charger
            // asks for the deepest index page as EPage::DataPage; the store says it
            // is EPage::BTreeIndexV2 -> this assert catches the type mismatch.
            auto pageId = Part->Store->ResolveByteOffset(room, location.Offset.AsByteOffset());
            auto trueType = Part->GetSlotPageType(room, pageId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                static_cast<ui16>(location.Type), static_cast<ui16>(trueType),
                "Wrong Type for offset " << location.Offset.AsByteOffset()
                << " room " << room
                << ": charger asked " << static_cast<ui16>(location.Type)
                << " store says " << static_cast<ui16>(trueType));
            if (Loaded[room].contains(location.Offset)) {
                return NTest::TTestEnv::TryGetSlotPage(Part, location, room);
            }
            Touched[room].push_back(location);
            return {};
        }

        // Simulate ObtainToLoad -> fetch -> save: every miss becomes available.
        void LoadTouched() {
            for (auto& [room, locs] : Touched) {
                auto& loaded = Loaded[room];
                for (auto& loc : locs) {
                    const auto* data = Part->Store->GetPage(room, loc.Offset);
                    UNIT_ASSERT_C(data, "no page at offset " << loc.Offset.AsByteOffset()
                        << " room " << room);
                    loaded[loc.Offset] = TSharedData::Copy(*data);
                }
            }
            Touched.clear();
        }

        bool TouchedEmpty() const {
            for (const auto& [room, locs] : Touched) {
                if (!locs.empty()) return false;
            }
            return true;
        }
    };
}

Y_UNIT_TEST_SUITE_F(TPageReadScope, ::NKikimr::NTable::NTest::TTableCacheTest) {
    Y_UNIT_TEST(DefaultEnvironmentDoesNotStartAnOperation) {
        TTestEnv env;
        auto scope = env.BeginOpBatch();
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        env.RestartOpBatch();
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
    }

    Y_UNIT_TEST(NestedScopesReuseTheOuterOperation) {
        NTabletFlatExecutor::TSeat seat(1, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TReadTransaction));
        NTabletFlatExecutor::TPageCollectionReadEnv env(seat);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        {
            auto outer = env.BeginOpBatch();
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            {
                auto inner = env.BeginOpBatch();
                UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
                env.RestartOpBatch();
                UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
    }

    Y_UNIT_TEST(BTreeSeekSharesTheOperationThroughTheDataPage) {
        auto eggs = MakePart({ .Levels = 3 });
        const auto& part = *eggs.Lone();
        NTabletFlatExecutor::TSeat seat(1, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TReadTransaction));
        TReadEnv env(seat);
        TPartGroupRowIter iter(&part, &env, {});
        {
            auto outer = env.BeginOpBatch();
            UNIT_ASSERT(iter.Seek(0) == EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(env.IndexReads, 3);
            UNIT_ASSERT_VALUES_EQUAL(env.DataReads, 1);
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        UNIT_ASSERT(iter.Seek(1) == EReady::Data);
        UNIT_ASSERT_VALUES_EQUAL(env.DataReads, 1);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        UNIT_ASSERT(iter.GetRecord());
    }

    Y_UNIT_TEST(PageMissEndsTheScopeBeforeRetry) {
        auto eggs = MakePart({ .Levels = 3 });
        const auto& part = *eggs.Lone();
        NTabletFlatExecutor::TSeat seat(1, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TReadTransaction));
        TReadEnv env(seat);
        TPartGroupRowIter iter(&part, &env, {});
        env.MissData = true;
        UNIT_ASSERT(iter.Seek(0) == EReady::Page);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        env.MissData = false;
        UNIT_ASSERT(iter.Seek(0) == EReady::Data);
        UNIT_ASSERT_VALUES_EQUAL(env.IndexReads, 3);
        UNIT_ASSERT_VALUES_EQUAL(env.DataReads, 2);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
    }

    Y_UNIT_TEST(DataPageExceptionEndsTheScope) {
        auto eggs = MakePart({ .Levels = 3 });
        const auto& part = *eggs.Lone();
        NTabletFlatExecutor::TSeat seat(1, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TReadTransaction));
        TReadEnv env(seat);
        TPartGroupRowIter iter(&part, &env, {});
        env.ThrowOnData = true;
        UNIT_ASSERT_EXCEPTION(iter.Seek(0), yexception);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        env.ThrowOnData = false;
        UNIT_ASSERT(iter.Seek(0) == EReady::Data);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
    }

    Y_UNIT_TEST(ForwardAndBackwardPageCrossingsShareTheOperation) {
        auto eggs = MakePart({ .Levels = 3 });
        const auto& part = *eggs.Lone();
        NTabletFlatExecutor::TSeat seat(1, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TReadTransaction));
        TReadEnv env(seat);
        TPartGroupKeyIter iter(&part, &env, {});
        UNIT_ASSERT(iter.Seek(1) == EReady::Data);
        UNIT_ASSERT(iter.Next() == EReady::Data);
        UNIT_ASSERT_VALUES_EQUAL(iter.GetRowId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        UNIT_ASSERT(iter.Prev() == EReady::Data);
        UNIT_ASSERT_VALUES_EQUAL(iter.GetRowId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.DataReads, 3);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
    }

    Y_UNIT_TEST(CollectionLookupReturnsAnOwnerBeyondTheOperation) {
        auto eggs = MakePart({ .Levels = 3 });
        const auto& part = *eggs.Lone();
        const auto location = IndexTools::GetFirstPageLocation(part, {});
        TTestEnv storage;
        auto original = storage.TryGetPage(&part, location, {});
        UNIT_ASSERT(original);
        TCacheCollectionRef collection;
        UNIT_ASSERT(TCache::Get().Find(part.GetPageCollection(0)->Label(), collection) ==
                    NSharedCache::ECacheResultStatus::Hit);
        TCachePageRef page;
        {
            auto cacheOp = TCache::Get().BeginOperation();
            page = collection->TryGetPage(cacheOp, location);
            UNIT_ASSERT(page);
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        UNIT_ASSERT(page.data() == original.data());
        UNIT_ASSERT_VALUES_EQUAL(page.size(), original.size());
        UNIT_ASSERT(page.GetCollectionId() == part.GetPageCollection(0)->Label());
    }

    Y_UNIT_TEST(TransactionEnvironmentSharesExplicitAndImplicitReadScopes) {
        auto eggs = MakePart({ .Levels = 3 });
        const auto& part = *eggs.Lone();
        const auto location = IndexTools::GetFirstPageLocation(part, {});
        TTestEnv storage;
        auto original = storage.TryGetPage(&part, location, {});
        UNIT_ASSERT(original);
        NTable::TPart::TParams params{
            .Epoch = part.Epoch,
            .Scheme = part.Scheme,
            .IndexPages = part.IndexPages,
            .IndexesRawSize = part.IndexesRawSize,
            .MinRowVersion = part.MinRowVersion,
            .MaxRowVersion = part.MaxRowVersion,
        };
        NTable::TPartStore stored(part.Label, std::move(params), part.Stat);
        TCacheCollectionRef collection;
        UNIT_ASSERT(TCache::Get().Find(part.GetPageCollection(0)->Label(), collection) ==
                    NSharedCache::ECacheResultStatus::Hit);
        stored.PageCollections.push_back(std::move(collection));
        {
            NTabletFlatExecutor::TSeat borrowedSeat(
                2, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TReadTransaction));
            NTabletFlatExecutor::TPageCollectionReadEnv borrowedEnv(borrowedSeat);
            auto& borrowedPages = borrowedEnv.BorrowedPages();
            const auto references = [&] {
                auto cacheOp = TCache::Get().BeginOperation();
                return NSharedCache::THandleState::FromRaw(
                    cacheOp.Handles()[original.CacheItem().Index()].State.load(std::memory_order_relaxed))
                    .Refs();
            };
            const auto before = references();
            auto first = borrowedPages.TryGetPage(&stored, location, {});
            UNIT_ASSERT(first);
            UNIT_ASSERT(first.CacheItem() == original.CacheItem());
            UNIT_ASSERT_VALUES_EQUAL(references(), before + 1);
            UNIT_ASSERT_VALUES_EQUAL(borrowedSeat.Pinned.size(), 1);
            {
                auto outer = borrowedPages.BeginOpBatch();
                auto inner = borrowedEnv.BeginOpBatch();
                for (ui32 visit = 0; visit < 2 * NSharedCache::CacheOperationBatchSize + 1; ++visit) {
                    auto page = borrowedPages.TryGetPage(&stored, location, {});
                    UNIT_ASSERT(page.CacheItem() == first.CacheItem());
                    UNIT_ASSERT(page.data() == original.data());
                    UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
            UNIT_ASSERT_VALUES_EQUAL(references(), before + 1);
            UNIT_ASSERT_VALUES_EQUAL(borrowedEnv.GetStats().NewlyPinnedPages, 1);
            UNIT_ASSERT_VALUES_EQUAL(borrowedEnv.GetStats().NewlyPinnedBytes, original.size());
        }
        NTabletFlatExecutor::TSeat seat(1, TAutoPtr<NTabletFlatExecutor::ITransaction>(new TReadTransaction));
        NTabletFlatExecutor::TPageCollectionReadEnv cacheEnv(seat);
        IPages& env = cacheEnv;
        TCachePageRef retained;
        {
            auto scope = env.BeginOpBatch();
            retained = env.TryGetPage(&stored, location, {});
            auto repeated = env.TryGetPage(&stored, location, {});
            UNIT_ASSERT(retained && repeated);
            UNIT_ASSERT(retained.CacheItem() == repeated.CacheItem());
            for (ui32 visit = 2; visit < 2 * NSharedCache::CacheOperationBatchSize + 1; ++visit) {
                auto page = env.TryGetPage(&stored, location, {});
                UNIT_ASSERT(page && page.CacheItem() == retained.CacheItem());
                UNIT_ASSERT(page.data() == original.data());
                UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            }
            {
                auto inner = env.BeginOpBatch();
                UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            UNIT_ASSERT_VALUES_EQUAL(seat.Pinned.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(cacheEnv.GetStats().NewlyPinnedPages, 1);
            UNIT_ASSERT_VALUES_EQUAL(cacheEnv.GetStats().NewlyPinnedBytes, original.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        UNIT_ASSERT(retained.data() == original.data());
        UNIT_ASSERT(retained.GetCollectionId() == stored.PageCollections.front()->Id());
        retained = env.TryGetPage(&stored, location, {});
        UNIT_ASSERT(retained);
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        UNIT_ASSERT_VALUES_EQUAL(seat.Pinned.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cacheEnv.GetStats().NewlyPinnedPages, 1);

        const auto missing = IndexTools::GetLastPageLocation(part, {});
        // Part construction may have cached this page; remove it before testing misses
        {
            auto cacheOp = TCache::Get().BeginOperation();
            NSharedCache::NDetail::TFoundItem found;
            if (cacheOp.FindPage(stored.PageCollections.front().CacheItem(), static_cast<ui64>(missing.Offset),
                    found) == NSharedCache::ECacheResultStatus::Hit) {
                NSharedCache::TOperationItemRef<NSharedCache::TProdTraits> owner(
                    cacheOp.SpaceOperation(), cacheOp.TryAcquireStructural(found.Item));
                UNIT_ASSERT(owner);
                auto state = NSharedCache::THandleState::FromRaw(
                    cacheOp.Handles()[found.Item.Index()].State.load(std::memory_order_acquire));
                UNIT_ASSERT(cacheOp.TryBeginErase(owner.Get(), state));
                cacheOp.Eviction().FinishPageErase(found.Item);
            }
        }
        {
            auto scope = env.BeginOpBatch();
            for (ui32 visit = 0; visit < NSharedCache::CacheOperationBatchSize + 1; ++visit) {
                UNIT_ASSERT(!env.TryGetPage(&stored, missing, {}));
                UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            }
            {
                auto inner = env.BeginOpBatch();
                UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(ActiveReadOperations(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cacheEnv.GetStats().ToLoadPages, 1);
    }
}

Y_UNIT_TEST_SUITE_F(TPartGroupBtreeIndexIter, ::NKikimr::NTable::NTest::TTableCacheTest) {
    void AssertEqual(const TPartGroupBtreeIndexIter& bTree, EReady bTreeReady, const TPartGroupFlatIndexIter& flat, EReady flatReady, const TString& message, bool allowFirstLastPageDifference = false) {
        // Note: it's possible that B-Tree index don't return Gone status for keys before the first page or keys after the last page
        if (allowFirstLastPageDifference && flatReady == EReady::Gone && bTreeReady == EReady::Data &&
                (bTree.GetRowId() == 0 || bTree.GetNextRowId() == bTree.GetEndRowId())) {
            UNIT_ASSERT_C(bTree.IsValid(), message);
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(bTreeReady, flatReady, message);
        UNIT_ASSERT_VALUES_EQUAL_C(bTree.IsValid(), flat.IsValid(), message);
        if (flat.IsValid()) {
            UNIT_ASSERT_VALUES_EQUAL_C(bTree.GetLocation().Offset, flat.GetLocation().Offset, message);
            UNIT_ASSERT_VALUES_EQUAL_C(bTree.GetRowId(), flat.GetRowId(), message);
            UNIT_ASSERT_VALUES_EQUAL_C(bTree.GetNextRowId(), flat.GetNextRowId(), message);
        }
    }

    EReady SeekRowId(IPartGroupIndexIter& iter, TTouchEnv& env, TRowId rowId, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            return iter.Seek(rowId);
        }, env, message, failsAllowed);
    }

    EReady SeekLast(IPartGroupIndexIter& iter, TTouchEnv& env, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            return iter.SeekLast();
        }, env, message, failsAllowed);
    }

    EReady SeekKey(IPartGroupIndexIter& iter, TTouchEnv& env, ESeek seek, bool reverse, TCells key, const TKeyCellDefaults *keyDefaults, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            if (reverse) {
                return iter.SeekReverse(seek, key, keyDefaults);
            } else {
                return iter.Seek(seek, key, keyDefaults);
            }
        }, env, message, failsAllowed);
    }

    EReady NextPrev(IPartGroupIndexIter& iter, TTouchEnv& env, bool next, const TString& message, ui32 failsAllowed = 10) {
        return Retry([&]() {
            if (next) {
                return iter.Next();
            } else {
                return iter.Prev();
            }
        }, env, message, failsAllowed);
    }

    void CheckSeekRowId(const TPartStore& part) {
        for (TRowId rowId1 : xrange<TRowId>(0, part.Stat.Rows + 1)) {
            for (TRowId rowId2 : xrange<TRowId>(0, part.Stat.Rows + 1)) {
                TTouchEnv bTreeEnv, flatEnv;
                TPartGroupBtreeIndexIter bTree(&part, &bTreeEnv, { });
                TPartGroupFlatIndexIter flat(&part, &flatEnv, { });

                // checking initial seek:
                {
                    TString message = TStringBuilder() << "SeekRowId< " << rowId1;
                    EReady bTreeReady = SeekRowId(bTree, bTreeEnv, rowId1, message);
                    EReady flatReady = SeekRowId(flat, flatEnv, rowId1, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId1 < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }

                // checking repositioning:
                {
                    TString message = TStringBuilder() << "SeekRowId " << rowId1 << " -> " << rowId2;
                    EReady bTreeReady = SeekRowId(bTree, bTreeEnv, rowId2, message);
                    EReady flatReady = SeekRowId(flat, flatEnv, rowId2, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId2 < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }
            }
        }
    }

    void CheckSeekLast(const TPartStore& part) {
        TTouchEnv bTreeEnv, flatEnv;
        TPartGroupBtreeIndexIter bTree(&part, &bTreeEnv, { });
        TPartGroupFlatIndexIter flat(&part, &flatEnv, { });

        TString message = TStringBuilder() << "SeekLast";
        EReady bTreeReady = SeekLast(bTree, bTreeEnv, message);
        EReady flatReady = SeekLast(flat, flatEnv, message);
        UNIT_ASSERT_VALUES_EQUAL(bTreeReady, EReady::Data);
        AssertEqual(bTree, bTreeReady, flat, flatReady, message);
    }

    void CheckSeekKey(const TPartStore& part, const TKeyCellDefaults *keyDefaults) {
        for (bool reverse : {false, true}) {
            for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
                for (ui32 firstCell : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCell : xrange<ui32>(0, 14)) {
                        TVector<TCell> key = MakeKey(firstCell, secondCell);

                        TTouchEnv bTreeEnv, flatEnv;
                        TPartGroupBtreeIndexIter bTree(&part, &bTreeEnv, { });
                        TPartGroupFlatIndexIter flat(&part, &flatEnv, { });

                        TStringBuilder message = TStringBuilder() << (reverse ?  "SeekKeyReverse" : "SeekKey") << "(" << seek << ") ";
                        for (auto c : key) {
                            message << c.AsValue<ui32>() << " ";
                        }

                        EReady bTreeReady = SeekKey(bTree, bTreeEnv, seek, reverse, key, keyDefaults, message);
                        EReady flatReady = SeekKey(flat, flatEnv, seek, reverse, key, keyDefaults, message);
                        AssertEqual(bTree, bTreeReady, flat, flatReady, message, true);
                    }
                }
            }
        }
    }

    void CheckNextPrev(const TPartStore& part) {
        for (bool next : {true, false}) {
            for (TRowId rowId : xrange<TRowId>(0, part.Stat.Rows)) {
                TTouchEnv bTreeEnv, flatEnv;
                TPartGroupBtreeIndexIter bTree(&part, &bTreeEnv, { });
                TPartGroupFlatIndexIter flat(&part, &flatEnv, { });

                // checking initial seek:
                {
                    TString message = TStringBuilder() << "CheckNext " << rowId;
                    EReady bTreeReady = SeekRowId(bTree, bTreeEnv, rowId, message);
                    EReady flatReady = SeekRowId(flat, flatEnv, rowId, message);
                    UNIT_ASSERT_VALUES_EQUAL(bTreeReady, rowId < part.Stat.Rows ? EReady::Data : EReady::Gone);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                }

                // checking next:
                while (true)
                {
                    TString message = TStringBuilder() << "CheckNext " << rowId << " -> " << rowId;
                    EReady bTreeReady = NextPrev(bTree, bTreeEnv, next, message);
                    EReady flatReady = NextPrev(flat, flatEnv, next, message);
                    AssertEqual(bTree, bTreeReady, flat, flatReady, message);
                    if (flatReady == EReady::Gone) {
                        break;
                    }
                }
            }
        }
    }

    void CheckPart(TTestParams params) {
        TPartEggs eggs = MakePart(params);
        const auto part = *eggs.Lone();

        CheckSeekRowId(part);
        CheckSeekLast(part);
        CheckSeekKey(part, eggs.Scheme->Keys.Get());
        CheckNextPrev(part);
    }

    Y_UNIT_TEST(NoNodes) {
        CheckPart({.Levels = 0});
    }

    Y_UNIT_TEST(OneNode) {
        CheckPart({.Levels = 1});
    }

    Y_UNIT_TEST(FewNodes) {
        CheckPart({.Levels = 3});
    }
}

Y_UNIT_TEST_SUITE_F(TChargeBTreeIndex, ::NKikimr::NTable::NTest::TTableCacheTest) {
    void StickSomePages(TTestParams params, const TPartStore& part, TTagsRef tags, const TKeyCellDefaults &keyDefaults, TTouchEnv& bTreeEnv, TTouchEnv& flatEnv) {
        if (params.StickSomePages) {
            TChargeBTreeIndex bTree(&bTreeEnv, part, tags, true);

            for (int times = 0; times < 5; times++) {
                bTree.ICharge::Do(10, 10, keyDefaults, 0, 0);
                bTreeEnv.LoadTouched();
            }

            flatEnv.Loaded = bTreeEnv.Loaded;
            flatEnv.StickLoaded();
            bTreeEnv.StickLoaded();
        }
    }

    ICharge::TResult DoChargeRowId(ICharge &charge, TTouchEnv &env, const TRowId row1, const TRowId row2, ui64 itemsLimit, ui64 bytesLimit,
            bool reverse, const TKeyCellDefaults &keyDefaults, const TString& message, ui32 failsAllowed = 15) {
        ICharge::TResult result;

        Retry([&]() {
            result = reverse
                ? charge.DoReverse(row2, row1, keyDefaults, itemsLimit, bytesLimit)
                : charge.Do(row1, row2, keyDefaults, itemsLimit, bytesLimit);
            return result.Ready ? EReady::Data : EReady::Page;
        }, env, message, failsAllowed);

        return result;
    }

    ICharge::TResult DoChargeKeys(const TPartStore& part, ICharge& charge, TTouchEnv& env, const TCells key1, const TCells key2, ui64 itemsLimit, ui64 bytesLimit,
            bool reverse, const TKeyCellDefaults &keyDefaults, const TString& message, ui32 failsAllowed = 15) {
        ICharge::TResult result;

        Retry([&]() {
            result = reverse
                ? charge.DoReverse(key1, key2, part.Stat.Rows - 1, 0, keyDefaults, itemsLimit, bytesLimit)
                : charge.Do(key1, key2, 0, part.Stat.Rows - 1, keyDefaults, itemsLimit, bytesLimit);
            return result.Ready ? EReady::Data : EReady::Page;
        }, env, message, failsAllowed);

        return result;
    }

    void CheckChargeRowId(TTestParams params, const TPartStore& part, TTagsRef tags, const TKeyCellDefaults *keyDefaults) {
        #if !defined(_tsan_enabled_) && !defined(_msan_enabled_) && !defined(_asan_enabled_)
        for (bool reverse : {false, true}) {
            for (ui64 itemsLimit : TVector<ui64>{0, 1, 2, 5, 13, 19, part.Stat.Rows - 2, part.Stat.Rows - 1}) {
                for (TRowId rowId1 : xrange<TRowId>(0, part.Stat.Rows - 1)) {
                    for (TRowId rowId2 : xrange<TRowId>(rowId1, part.Stat.Rows - 1)) {
        #else
        for (bool reverse : {false, true}) {
            for (ui64 itemsLimit : TVector<ui64>{0, 5, part.Stat.Rows - 1}) {
                for (TRowId rowId1 : xrange<TRowId>(0, part.Stat.Rows - 1, 3)) {
                    for (TRowId rowId2 : xrange<TRowId>(rowId1, part.Stat.Rows - 1, 3)) {
        #endif
                        TTouchEnv bTreeEnv, flatEnv;
                        TChargeBTreeIndex bTree(&bTreeEnv, part, tags, true);
                        TChargeFlatIndex flat(&flatEnv, part, tags, true);
                        StickSomePages(params, part, tags, *keyDefaults, bTreeEnv, flatEnv);

                        TString message = TStringBuilder() << (reverse ? "ChargeRowIdReverse " : "ChargeRowId ") << rowId1 << " " << rowId2 << " items " << itemsLimit;
                        const auto treeChargeResult = DoChargeRowId(bTree, bTreeEnv, rowId1, rowId2, itemsLimit, 0, reverse, *keyDefaults, message);
                        const auto flatChargeResult = DoChargeRowId(flat, flatEnv, rowId1, rowId2, itemsLimit, 0, reverse, *keyDefaults, message);

                        // Overshot is more accurate in the B-Tree implementation
                        if (!itemsLimit || treeChargeResult.Overshot) {
                            UNIT_ASSERT_VALUES_EQUAL_C(treeChargeResult.Overshot, flatChargeResult.Overshot, message);
                        }

                        // The B-Tree implementation ignores the limits when the tree has no levels
                        if ((itemsLimit == 0) || (part.IndexPages.GetBTree({}).LevelCount() == 0)) {
                            UNIT_ASSERT_VALUES_EQUAL_C(treeChargeResult.ItemsPrecharged, rowId2 - rowId1 + 1, message);
                        } else {
                            // The B-Tree implementation can overcharge items by up to the size
                            // of two nodes over the specified limit (the node size is 2 in these tests)
                            TString messageWithValues = TStringBuilder() << message
                                << " treeChargedItems=" << treeChargeResult.ItemsPrecharged
                                << " flatChargedItems=" << flatChargeResult.ItemsPrecharged;

                            UNIT_ASSERT_GE_C(treeChargeResult.ItemsPrecharged, flatChargeResult.ItemsPrecharged, messageWithValues);
                            UNIT_ASSERT_LE_C(treeChargeResult.ItemsPrecharged, flatChargeResult.ItemsPrecharged + 4, messageWithValues);
                        }

                        // The flat implementation always returns 0 bytes charged, if the bytes limit is not set
                        UNIT_ASSERT_VALUES_UNEQUAL_C(treeChargeResult.BytesPrecharged, 0, message);

                        AssertLoadedTheSame(part, bTreeEnv, flatEnv, message,
                            false, reverse && itemsLimit, !reverse && itemsLimit);
                    }
                }
            }
        }
    }

    void CheckChargeKeys(TTestParams params, const TPartStore& part, TTagsRef tags, const TKeyCellDefaults *keyDefaults) {
        #if !defined(_tsan_enabled_) && !defined(_msan_enabled_) && !defined(_asan_enabled_)
        for (bool reverse : {false, true}) {
            for (ui64 itemsLimit : TVector<ui64>{0, 1, 2, 5, 13, 19, part.Stat.Rows - 2, part.Stat.Rows - 1}) {
                for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCellKey1 : xrange<ui32>(0, 14)) {
                        for (ui32 firstCellKey2 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                            for (ui32 secondCellKey2 : xrange<ui32>(0, 14)) {
        #else
        for (bool reverse : {false, true}) {
            for (ui64 itemsLimit : TVector<ui64>{0, 5, part.Stat.Rows - 1}) {
                for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCellKey1 : xrange<ui32>(10, 14)) {
                        for (ui32 firstCellKey2 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                            for (ui32 secondCellKey2 : xrange<ui32>(10, 14)) {
        #endif
                                TVector<TCell> key1 = MakeKey(firstCellKey1, secondCellKey1);
                                TVector<TCell> key2 = MakeKey(firstCellKey2, secondCellKey2);

                                TTouchEnv bTreeEnv, flatEnv;
                                TChargeBTreeIndex bTree(&bTreeEnv, part, tags, true);
                                TChargeFlatIndex flat(&flatEnv, part, tags, true);
                                StickSomePages(params, part, tags, *keyDefaults, bTreeEnv, flatEnv);

                                TStringBuilder message = TStringBuilder() << (reverse ? "ChargeKeysReverse " : "ChargeKeys ") << "(";
                                for (auto c : key1) {
                                    message << c.AsValue<ui32>() << " ";
                                }
                                message << ") (";
                                for (auto c : key2) {
                                    message << c.AsValue<ui32>() << " ";
                                }
                                message << ") items " << itemsLimit;

                                const auto treeChargeResult = DoChargeKeys(part, bTree, bTreeEnv, key1, key2, itemsLimit, 0, reverse, *keyDefaults, message);
                                const auto flatChargeResult = DoChargeKeys(part, flat, flatEnv, key1, key2, itemsLimit, 0, reverse, *keyDefaults, message);

                                // Overshot is more accurate in the B-Tree implementation
                                if (!itemsLimit || treeChargeResult.Overshot) {
                                    UNIT_ASSERT_VALUES_EQUAL_C(treeChargeResult.Overshot, flatChargeResult.Overshot, message);
                                }

                                // The B-Tree implementation ignores the limits and keys when the tree has no levels,
                                // except for the case when the history or the groups are turned on
                                if ((part.IndexPages.GetBTree({}).LevelCount() == 0) && (!params.Groups) && (!params.History)) {
                                    UNIT_ASSERT_VALUES_EQUAL_C(
                                        treeChargeResult.ItemsPrecharged,
                                        part.IndexPages.GetBTree({}).GetRowCount(),
                                        message
                                    );
                                // In some cases the flat implementation does not adjust
                                // the precharged counts based on the precise keys
                                } else if ((!params.Groups) && (!params.History)) {
                                    // The B-Tree implementation can overcharge items by up to the size
                                    // of two nodes over the specified limit (the node size is 2 in these tests)
                                    TString messageWithValues = TStringBuilder() << message
                                        << " treeChargedItems=" << treeChargeResult.ItemsPrecharged
                                        << " flatChargedItems=" << flatChargeResult.ItemsPrecharged;

                                    UNIT_ASSERT_GE_C(treeChargeResult.ItemsPrecharged, flatChargeResult.ItemsPrecharged, messageWithValues);
                                    UNIT_ASSERT_LE_C(treeChargeResult.ItemsPrecharged, flatChargeResult.ItemsPrecharged + 4, messageWithValues);
                                }

                                // The flat implementation always returns 0 bytes charged, if the bytes limit is not set
                                UNIT_ASSERT_VALUES_UNEQUAL_C(treeChargeResult.BytesPrecharged, 0, message);

                                AssertLoadedTheSame(part, bTreeEnv, flatEnv, message,
                                    true, reverse && itemsLimit, !reverse && itemsLimit);
                            }
                        }
                    }
                }
            }
        }
    }

    void CheckChargeBytesLimit(TTestParams params, const TPartStore& part, TTagsRef tags, const TKeyCellDefaults *keyDefaults) {
        #if !defined(_tsan_enabled_) && !defined(_msan_enabled_) && !defined(_asan_enabled_)
        for (bool reverse : {false, true}) {
            for (ui64 bytesLimit : xrange<ui64>(1, part.Stat.Bytes + 100, part.Stat.Bytes / 100)) {
                for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCellKey1 : xrange<ui32>(0, 14)) {
        #else
        for (bool reverse : {false, true}) {
            for (ui64 bytesLimit : TVector<ui64>{1l, part.Stat.Bytes / 3}) {
                for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                    for (ui32 secondCellKey1 : xrange<ui32>(10, 14)) {
        #endif
                        TVector<TCell> key1 = MakeKey(firstCellKey1, secondCellKey1);

                        TTouchEnv limitedEnv, unlimitedEnv;
                        TChargeBTreeIndex limitedCharge(&limitedEnv, part, tags, true);
                        TChargeBTreeIndex unlimitedCharge(&unlimitedEnv, part, tags, true);
                        StickSomePages(params, part, tags, *keyDefaults, limitedEnv, unlimitedEnv);

                        TStringBuilder message = TStringBuilder() << (reverse ? "ChargeBytesLimitReverse " : "ChargeBytesLimit ") << "(";
                        for (auto c : key1) {
                            message << c.AsValue<ui32>() << " ";
                        }
                        message << ") bytes " << bytesLimit;

                        DoChargeKeys(part, unlimitedCharge, unlimitedEnv, key1, { }, 0, 0, reverse, *keyDefaults, message);
                        DoChargeKeys(part, limitedCharge, limitedEnv, key1, { }, 0, bytesLimit, reverse, *keyDefaults, message);

                        TSet<TGroupId> groupIds;
                        for (const auto &c : {limitedEnv.Loaded, unlimitedEnv.Loaded}) {
                            for (const auto &g : c) {
                                groupIds.insert(g.first);
                            }
                        }
                        for (auto groupId : groupIds) {
                            ui64 size = 0;
                            TSet<TPageId> expected, loaded;
                            TVector<TPageId> unlimitedLoaded(unlimitedEnv.Loaded[groupId].begin(), unlimitedEnv.Loaded[groupId].end());
                            if (reverse) {
                                std::reverse(unlimitedLoaded.begin(), unlimitedLoaded.end());
                            }
                            for (auto pageId : limitedEnv.Loaded[groupId]) {
                                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                                    loaded.insert(pageId);
                                }
                            }
                            for (auto pageId : unlimitedLoaded) {
                                if (part.GetPageType(pageId, groupId) == EPage::DataPage) {
                                    if (!groupId.IsHistoric() && (expected || !groupId.IsMain())) {
                                        // do not count first main page
                                        size += part.GetPageSize(pageId, groupId);
                                    }
                                    if (!groupId.IsMain() && !loaded.contains(pageId)) {
                                        // only check that we loaded consecutive pages
                                        if (params.StickSomePages) {
                                            // extra pages may appear after the bytes limit is applied on main pages
                                            continue;
                                        } else {
                                            break;
                                        }
                                    }
                                    expected.insert(pageId);
                                    if (size > bytesLimit) {
                                        break;
                                    }
                                }
                            }

                            UNIT_ASSERT_VALUES_EQUAL_C(expected, loaded,
                                TStringBuilder() << message << " Group " << groupId);
                        }
                    }
                }
            }
        }
    }

    void CheckPart(TTestParams params) {
        TPartEggs eggs = MakePart(params);
        const auto part = *eggs.Lone();

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        CheckChargeRowId(params, part, tags, eggs.Scheme->Keys.Get());
        CheckChargeKeys(params, part, tags, eggs.Scheme->Keys.Get());
        CheckChargeBytesLimit(params, part, tags, eggs.Scheme->Keys.Get());
    }

    Y_UNIT_TEST(NoNodes) {
        CheckPart({.Levels = 0});
    }

    Y_UNIT_TEST(NoNodes_Groups) {
        CheckPart({.Levels = 0, .Groups = true});
    }

    Y_UNIT_TEST(NoNodes_History) {
        CheckPart({.Levels = 0, .History = true});
    }

    Y_UNIT_TEST(NoNodes_Groups_History) {
        CheckPart({.Levels = 0, .Groups = true, .History = true});
    }

    Y_UNIT_TEST(OneNode) {
        CheckPart({.Levels = 1});
    }

    Y_UNIT_TEST(OneNode_Groups) {
        CheckPart({.Levels = 1, .Groups = true});
    }

    Y_UNIT_TEST(OneNode_History) {
        CheckPart({.Levels = 1, .History = true});
    }

    Y_UNIT_TEST(OneNode_Groups_History) {
        CheckPart({.Levels = 1, .Groups = true, .History = true});
    }

    Y_UNIT_TEST(FewNodes) {
        CheckPart({.Levels = 3});
    }

    Y_UNIT_TEST(FewNodes_Groups) {
        CheckPart({.Levels = 3, .Groups = true});
    }

    Y_UNIT_TEST(FewNodes_History) {
        CheckPart({.Levels = 3, .History = true});
    }

    Y_UNIT_TEST(FewNodes_Sticky) {
        CheckPart({.Levels = 3, .StickSomePages = true});
    }

    Y_UNIT_TEST(FewNodes_Groups_History) {
        CheckPart({.Levels = 3, .Groups = true, .History = true});
    }

    Y_UNIT_TEST(FewNodes_Groups_History_Sticky) {
        CheckPart({.Levels = 3, .Groups = true, .History = true, .StickSomePages = true});
    }
}

Y_UNIT_TEST_SUITE_F(TPartBtreeIndexIteration, ::NKikimr::NTable::NTest::TTableCacheTest) {
    void MakeRuns(const TPartEggs& eggs, TRun& btreeRun, TRun& flatRun) {
        const auto part = *eggs.Lone();

        auto flatPart = part.CloneWithEpoch(part.Epoch);
        for (auto& slice : *part.Slices) {
            btreeRun.Insert(eggs.Lone(), slice);
            auto pages = (TVector<TBtreeIndexMeta>*)&flatPart->IndexPages.BTreeGroups;
            pages->clear();
            pages = (TVector<TBtreeIndexMeta>*)&flatPart->IndexPages.BTreeHistoric;
            pages->clear();
            flatRun.Insert(flatPart, slice);
        }
    }

    ui32 GetFailsAllowed(TTestParams params) {
        ui32 result = (params.Levels + 1) * 2;
        if (params.History) {
            result *= 2;
        }
        if (params.Groups) {
            result *= 2;
        }
        return result;
    }

    template<EDirection Direction>
    void AssertEqual(const TWrapPartImpl<Direction>& bTree, EReady bTreeReady, const TWrapPartImpl<Direction>& flat, EReady flatReady, const TString& message) {
        UNIT_ASSERT_VALUES_EQUAL_C(bTreeReady, flatReady, message);
        UNIT_ASSERT_VALUES_EQUAL_C(bTree.Get()->IsValid(), flat.Get()->IsValid(), message);
        UNIT_ASSERT_VALUES_EQUAL_C(bTree.Get()->GetRowId(), flat.Get()->GetRowId(), message);
    }

    template<EDirection Direction>
    EReady Seek(TWrapPartImpl<Direction>& wrap, TTouchEnv& env, const TCells key1, ESeek seek, const TString& message, ui32 failsAllowed) {
        return Retry([&]() {
            return wrap.Seek(key1, seek);
        }, env, message, failsAllowed);
    }

    EReady Seek(TWrapPartImpl<EDirection::Forward>& wrap, TForwardEnv& env, const TCells key1, ESeek seek, const TString& message, ui32 failsAllowed) {
        return Retry([&]() {
            return wrap.Seek(key1, seek);
        }, env, message, failsAllowed);
    }

    template<EDirection Direction>
    EReady Next(TWrapPartImpl<Direction>& wrap, TTouchEnv& env, const TString& message, ui32 failsAllowed) {
        return Retry([&]() {
            return wrap.Next();
        }, env, message, failsAllowed);
    }

    EReady Next(TWrapPartImpl<EDirection::Forward>& wrap, TForwardEnv& env, const TString& message, ui32 failsAllowed) {
        return Retry([&]() {
            return wrap.Next();
        }, env, message, failsAllowed);
    }

    template<EDirection Direction>
    EReady SkipToRowVersion(TWrapPartImpl<Direction>& wrap, TTouchEnv& env, TRowVersion rowVersion, const TString& message, ui32 failsAllowed) {
        return Retry([&]() {
            return wrap.SkipToRowVersion(rowVersion);
        }, env, message, failsAllowed);
    }

    TPrechargeResult Charge(const TRun &run, const TVector<TTag> tags, TTouchEnv& env, const TCells key1, const TCells key2, ui64 itemsLimit, ui64 bytesLimit,
            bool reverse, const TKeyCellDefaults &keyDefaults, const TString& message, ui32 failsAllowed) {
        TPrechargeResult result;

        Retry([&]() {
            result = reverse
                ? ChargeRangeReverse(&env, key1, key2, run, keyDefaults, tags, itemsLimit, bytesLimit, true)
                : ChargeRange(&env, key1, key2, run, keyDefaults, tags, itemsLimit, bytesLimit, true);
            return result.Ready ? EReady::Data : EReady::Page;
        }, env, message, failsAllowed);

        return result;
    }

    template<EDirection Direction>
    void Iterate(const TPartEggs& eggs, TRun& run, TTouchEnv& env, const TCells key1, const TCells key2, ESeek seek, ui64 itemsLimit, bool history, const TString& message, ui32 failsAllowed) {
        TWrapPartImpl<Direction> wrap(eggs, run);
        wrap.StopAfter(key2);
        wrap.Make(&env);

        if (Seek(wrap, env, key1, seek, TStringBuilder() << message << " Seek " << seek, failsAllowed) != EReady::Data) {
            return;
        }
        if (history) {
            UNIT_ASSERT_VALUES_EQUAL(SkipToRowVersion(wrap, env, {0, 1}, message + " Ver", failsAllowed), EReady::Data);
        }

        for (ui32 itemIndex = 1; itemsLimit == 0 || itemIndex < itemsLimit; itemIndex++) {
            if (Next(wrap, env, message + " Next " + std::to_string(itemIndex), failsAllowed) != EReady::Data) {
                return;
            }
            if (history) {
                UNIT_ASSERT_VALUES_EQUAL(SkipToRowVersion(wrap, env, {0, 1}, message + " Ver", failsAllowed), EReady::Data);
            }
        }
    }

    template<EDirection Direction>
    void CheckIterate(TTestParams params, const TPartEggs& eggs) {
        constexpr bool reverse = Direction == EDirection::Reverse;
        const ui32 failsAllowed = GetFailsAllowed(params);
        const auto part = *eggs.Lone();

        TRun btreeRun(*eggs.Scheme->Keys), flatRun(*eggs.Scheme->Keys);
        MakeRuns(eggs, btreeRun, flatRun);

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        #if !defined(_tsan_enabled_) && !defined(_msan_enabled_) && !defined(_asan_enabled_)
        for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
            for (ui32 firstCell : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                for (ui32 secondCell : xrange<ui32>(0, 14)) {
        #else
        for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
            for (ui32 firstCell : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                for (ui32 secondCell : xrange<ui32>(10, 14)) {
        #endif
                    TVector<TCell> key = MakeKey(firstCell, secondCell);

                    TTouchEnv bTreeEnv, flatEnv;
                    TWrapPartImpl<Direction> bTreeIt(eggs, btreeRun);
                    TWrapPartImpl<Direction> flatIt(eggs, flatRun);
                    bTreeIt.Make(&bTreeEnv);
                    flatIt.Make(&flatEnv);

                    {
                        TStringBuilder message = TStringBuilder() << (reverse ?  "IterateReverse" : "Iterate") << "(" << seek << ") ";
                        for (auto c : key) {
                            message << c.AsValue<ui32>() << " ";
                        }
                        EReady bTreeReady = Seek(bTreeIt, bTreeEnv, key, seek, message, failsAllowed);
                        EReady flatReady = Seek(flatIt, flatEnv, key, seek, message, failsAllowed);
                        AssertEqual(bTreeIt, bTreeReady, flatIt, flatReady, message);
                        AssertLoadedTheSame(part, bTreeEnv, flatEnv, message);
                    }

                    for (ui32 steps = 1; steps <= 10; steps++) {
                        TStringBuilder message = TStringBuilder() << (reverse ?  "IterateReverse" : "Iterate") << "(" << seek << ") ";
                        for (auto c : key) {
                            message << c.AsValue<ui32>() << " ";
                        }
                        message << " --> " << steps << " steps ";
                        EReady bTreeReady = Next(bTreeIt, bTreeEnv, message, failsAllowed);
                        EReady flatReady = Next(flatIt, flatEnv, message, failsAllowed);
                        AssertEqual(bTreeIt, bTreeReady, flatIt, flatReady, message);
                        AssertLoadedTheSame(part, bTreeEnv, flatEnv, message);
                    }
                }
            }
        }
    }

    void CheckIterateFwd(TTestParams params, const TPartEggs& eggs) {
        const ui32 failsAllowed = GetFailsAllowed(params);
        const auto part = *eggs.Lone();

        TRun btreeRun(*eggs.Scheme->Keys), flatRun(*eggs.Scheme->Keys);
        MakeRuns(eggs, btreeRun, flatRun);

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        for (auto [readLo, readHi] : TVector<std::pair<ui32, ui32>>{{512, 1024}}) {
        for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
        #if !defined(_tsan_enabled_) && !defined(_msan_enabled_) && !defined(_asan_enabled_)
            for (ui32 firstCell : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                for (ui32 secondCell : xrange<ui32>(0, 14)) {
        #else
            for (ui32 firstCell : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                for (ui32 secondCell : xrange<ui32>(10, 14)) {
        #endif
                    TVector<TCell> key = MakeKey(firstCell, secondCell);

                    TTouchEnv bTreeEnv, flatEnv;
                    TWrapPartImpl<EDirection::Forward> bTreeIt(eggs, btreeRun), flatIt(eggs, flatRun);
                    bTreeIt.Make(&bTreeEnv);
                    flatIt.Make(&flatEnv);

                    TForwardEnv bTreeFwdEnv(readLo, readHi), flatFwdEnv(readLo, readHi);
                    TWrapPartImpl<EDirection::Forward> bTreeFwdIt(eggs, btreeRun), flatFwdIt(eggs, flatRun);
                    bTreeFwdIt.Make(&bTreeFwdEnv);
                    flatFwdIt.Make(&flatFwdEnv);

                    {
                        TStringBuilder message = TStringBuilder() << "IterateFwd(" << readLo << ", " << readHi << ", " << seek << ") ";
                        for (auto c : key) {
                            message << c.AsValue<ui32>() << " ";
                        }

                        EReady bTreeReady = Seek(bTreeIt, bTreeEnv, key, seek, message, failsAllowed);
                        EReady flatReady = Seek(flatIt, flatEnv, key, seek, message, failsAllowed);
                        AssertEqual(bTreeIt, bTreeReady, flatIt, flatReady, message);

                        EReady bTreeFwdReady = Seek(bTreeFwdIt, bTreeFwdEnv, key, seek, message, failsAllowed);
                        EReady flatFwdReady = Seek(flatFwdIt, flatFwdEnv, key, seek, message, failsAllowed);
                        AssertEqual(bTreeFwdIt, bTreeFwdReady, flatFwdIt, flatFwdReady, message);

                        // compare fwd and touch env results:
                        AssertEqual(bTreeFwdIt, bTreeFwdReady, flatIt, flatReady, message);

                        AssertLoadedTheSame(part, bTreeEnv, flatEnv, message); // TODO: check fwd?
                    }

                    for (ui32 steps = 1; steps <= 10; steps++) {
                        TStringBuilder message = TStringBuilder() << "IterateFwd(" << readLo << ", " << readHi << ", " << seek << ") ";
                        for (auto c : key) {
                            message << c.AsValue<ui32>() << " ";
                        }
                        message << " --> " << steps << " steps ";

                        EReady bTreeReady = Next(bTreeIt, bTreeEnv, message, failsAllowed);
                        EReady flatReady = Next(flatIt, flatEnv, message, failsAllowed);
                        AssertEqual(bTreeIt, bTreeReady, flatIt, flatReady, message);

                        EReady bTreeFwdReady = Next(bTreeFwdIt, bTreeFwdEnv, message, failsAllowed);
                        EReady flatFwdReady = Next(flatFwdIt, flatFwdEnv, message, failsAllowed);
                        AssertEqual(bTreeFwdIt, bTreeFwdReady, flatFwdIt, flatFwdReady, message);

                        // compare fwd and touch env results:
                        AssertEqual(bTreeFwdIt, bTreeFwdReady, flatIt, flatReady, message);

                        AssertLoadedTheSame(part, bTreeEnv, flatEnv, message); // TODO: check fwd?
                    }
                }
            }
        }}
    }

    template<EDirection Direction>
    void CheckCharge(TTestParams params, const TPartEggs& eggs) {
        constexpr bool reverse = Direction == EDirection::Reverse;
        const ui32 failsAllowed = GetFailsAllowed(params);
        const auto part = *eggs.Lone();

        TRun btreeRun(*eggs.Scheme->Keys), flatRun(*eggs.Scheme->Keys);
        MakeRuns(eggs, btreeRun, flatRun);

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        #if !defined(_tsan_enabled_) && !defined(_msan_enabled_) && !defined(_asan_enabled_)
        for (ui64 itemsLimit : params.Groups || params.History || params.Slices ? TVector<ui64>{0, 3} : TVector<ui64>{0, 1, 2, 5, 13, 19, part.Stat.Rows - 2, part.Stat.Rows - 1}) {
            for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                for (ui32 secondCellKey1 : xrange<ui32>(0, 14)) {
                    for (ui32 firstCellKey2 : xrange<ui32>(0, itemsLimit ? 1 : part.Stat.Rows / 7 + 1)) {
                        for (ui32 secondCellKey2 : xrange<ui32>(0, itemsLimit ? 1 : 14)) {
        #else
        for (ui64 itemsLimit : params.Groups || params.History || params.Slices ? TVector<ui64>{0, 3} : TVector<ui64>{0, 5, part.Stat.Rows - 1}) {
            for (ui32 firstCellKey1 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                for (ui32 secondCellKey1 : xrange<ui32>(10, 14)) {
                    for (ui32 firstCellKey2 : xrange<ui32>(0, part.Stat.Rows / 7 + 1)) {
                        for (ui32 secondCellKey2 : xrange<ui32>(10, itemsLimit ? 11 : 14)) {
        #endif
                            TVector<TCell> key1 = MakeKey(firstCellKey1, secondCellKey1);
                            TVector<TCell> key2 = MakeKey(firstCellKey2, secondCellKey2);

                            TTouchEnv bTreeEnv, flatEnv;

                            TStringBuilder message = TStringBuilder() << (reverse ? "ChargeReverse " : "Charge ") << "(";
                            for (auto c : key1) {
                                message << c.AsValue<ui32>() << " ";
                            }
                            message << ") (";
                            for (auto c : key2) {
                                message << c.AsValue<ui32>() << " ";
                            }
                            message << ") items " << itemsLimit;

                            const auto treeChargeResult = Charge(btreeRun, tags, bTreeEnv, key1, key2, itemsLimit, 0, reverse, *eggs.Scheme->Keys, message, failsAllowed);
                            const auto flatChargeResult = Charge(flatRun, tags, flatEnv, key1, key2, itemsLimit, 0, reverse, *eggs.Scheme->Keys, message, failsAllowed);

                            // The B-Tree implementation ignores the limits and keys when the tree has no levels,
                            // except for the case when the history or the groups are turned on
                            if ((part.IndexPages.GetBTree({}).LevelCount() == 0) && (!params.Groups) && (!params.History)) {
                                if (treeChargeResult.ItemsPrecharged != 0) {
                                    UNIT_ASSERT_VALUES_EQUAL_C(
                                        treeChargeResult.ItemsPrecharged,
                                        part.IndexPages.GetBTree({}).GetRowCount(),
                                        message
                                    );
                                }
                            // In some cases the flat implementation does not adjust
                            // the precharged counts based on the precise keys
                            } else if ((!params.Groups) && (!params.History)) {
                                // The B-Tree implementation can overcharge items by up to the size
                                // of two nodes over the specified limit (the node size is 2 in these tests)
                                TString messageWithValues = TStringBuilder() << message
                                    << " treeChargedItems=" << treeChargeResult.ItemsPrecharged
                                    << " flatChargedItems=" << flatChargeResult.ItemsPrecharged;

                                UNIT_ASSERT_GE_C(treeChargeResult.ItemsPrecharged, flatChargeResult.ItemsPrecharged, messageWithValues);
                                UNIT_ASSERT_LE_C(treeChargeResult.ItemsPrecharged, flatChargeResult.ItemsPrecharged + 4, messageWithValues);
                            }

                            if (!itemsLimit || part.Slices->size() == 1) {
                                AssertLoadedTheSame(part, bTreeEnv, flatEnv, message,
                                    false, reverse && itemsLimit, !reverse && itemsLimit);
                            }

                            for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
                                Iterate<Direction>(eggs, btreeRun, bTreeEnv, key1, key2, seek, itemsLimit, params.History, message, 0);
                                Iterate<Direction>(eggs, flatRun, flatEnv, key1, key2, seek, itemsLimit, params.History, message, 0);
                            }
                        }
                    }
                }
            }
        }
    }

    void CheckPart(TTestParams params) {
        TPartEggs eggs = MakePart(params);
        const auto part = *eggs.Lone();

        CheckIterate<EDirection::Forward>(params, eggs);
        CheckIterate<EDirection::Reverse>(params, eggs);

        CheckIterateFwd(params, eggs);

        CheckCharge<EDirection::Forward>(params, eggs);
        CheckCharge<EDirection::Reverse>(params, eggs);
    }

    Y_UNIT_TEST(NoNodes) {
        CheckPart({.Levels = 0});
    }

    Y_UNIT_TEST(NoNodes_Groups) {
        CheckPart({.Levels = 0, .Groups = true});
    }

    Y_UNIT_TEST(NoNodes_History) {
        CheckPart({.Levels = 0, .History = true});
    }

    Y_UNIT_TEST(OneNode) {
        CheckPart({.Levels = 1});
    }

    Y_UNIT_TEST(OneNode_Groups) {
        CheckPart({.Levels = 1, .Groups = true});
    }

    Y_UNIT_TEST(OneNode_History) {
        CheckPart({.Levels = 1, .History = true});
    }

    Y_UNIT_TEST(OneNode_Slices) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::End)) {
            CheckPart({.Levels = 1, .Slices = TTestParams::ESlices(slices)});
        }
    }

    Y_UNIT_TEST(OneNode_Groups_Slices) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::Many + 1)) {
            CheckPart({.Levels = 1, .Groups = true, .Slices = TTestParams::ESlices(slices)});
        }
    }

    Y_UNIT_TEST(OneNode_History_Slices) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::Many + 1)) {
            CheckPart({.Levels = 1, .History = true, .Slices = TTestParams::ESlices(slices)});
        }
    }

    Y_UNIT_TEST(OneNode_Groups_History_Slices) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::Many + 1)) {
            CheckPart({.Levels = 1, .Groups = true, .History = true, .Slices = TTestParams::ESlices(slices)});
        }
    }

    Y_UNIT_TEST(FewNodes) {
        CheckPart({.Levels = 3});
    }

    Y_UNIT_TEST(FewNodes_Groups) {
        CheckPart({.Levels = 3, .Groups = true});
    }

    Y_UNIT_TEST(FewNodes_History) {
        CheckPart({.Levels = 3, .History = true});
    }

    Y_UNIT_TEST(FewNodes_Sticky) {
        CheckPart({.Levels = 3, .StickSomePages = true});
    }

    Y_UNIT_TEST(FewNodes_Slices) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::End)) {
            CheckPart({.Levels = 3, .Slices = TTestParams::ESlices(slices)});
        }
    }

    Y_UNIT_TEST(FewNodes_Groups_Slices) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::Many + 1)) {
            CheckPart({.Levels = 3, .Groups = true, .Slices = TTestParams::ESlices(slices)});
        }
    }

    Y_UNIT_TEST(FewNodes_History_Slices) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::Many + 1)) {
            CheckPart({.Levels = 3, .History = true, .Slices = TTestParams::ESlices(slices)});
        }
    }

    Y_UNIT_TEST(FewNodes_Groups_History_Slices) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::Many + 1)) {
            CheckPart({.Levels = 3, .Groups = true, .History = true, .Slices = TTestParams::ESlices(slices)});
        }
    }

    Y_UNIT_TEST(FewNodes_Groups_History_Slices_Sticky) {
        for (auto slices : xrange<ui32>(TTestParams::ESlices::None + 1, TTestParams::ESlices::Many + 1)) {
            CheckPart({.Levels = 3, .Groups = true, .History = true, .Slices = TTestParams::ESlices(slices), .StickSomePages = true});
        }
    }
}

// ========================================================================
// Shared V2 part builders used by both TPartGroupBtreeIndexIterV2 and
// TChargeBTreeIndexV2 suites.
// ========================================================================

namespace {
    NPage::TConf MakeV2Conf(TTestParams params) {
        NPage::TConf conf;
        conf.WriteBTreeIndexV1 = false;
        conf.WriteBTreeIndexV2 = true;
        conf.WriteFlatIndex = false;
        switch (params.Levels) {
        case 0:
            conf.Group(0).PageRows = 999;
            break;
        case 1:
            conf.Group(0).PageRows = 2;
            break;
        case 3:
            conf.Group(0).PageRows = 2;
            conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 2;
            break;
        default:
            Y_TABLET_ERROR("Unknown levels");
        }
        if (params.Groups) {
            conf.Group(1).PageRows = params.Levels ? 1 : 999;
            conf.Group(2).PageRows = 3;
            conf.Group(3).PageRows = 1;
            conf.Group(1).BTreeIndexNodeKeysMin = conf.Group(1).BTreeIndexNodeKeysMax = conf.Group(0).BTreeIndexNodeKeysMax;
            conf.Group(2).BTreeIndexNodeKeysMin = conf.Group(2).BTreeIndexNodeKeysMax = 2;
            conf.Group(3).BTreeIndexNodeKeysMin = conf.Group(3).BTreeIndexNodeKeysMax = 999;
        }
        return conf;
    }

    TPartEggs MakeV2Part(TTestParams params) {
        auto conf = MakeV2Conf(params);

        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(params.Groups ? 1 : 0, 2,  NScheme::NTypeIds::Uint32)
            .Col(params.Groups ? 2 : 0, 3,  NScheme::NTypeIds::Uint64)
            .Col(params.Groups ? 3 : 0, 4,  NScheme::NTypeIds::String)
            .Key({0, 1});

        TPartCook cook(lay, conf);
        const TVector<ui32> secondCells = {1, 3, 4, 6, 7, 8, 10};
        for (ui32 i : xrange<ui32>(0, 40)) {
            for (int ver = params.History ? i % 3 : 0; ver >= 0; ver--) {
                cook.Ver({0, ui64(ver)}).Add(*TSchemedCookRow(*lay).Col(i / 7, secondCells[i % 7], i, static_cast<ui64>(i), TString("xxxxxxxxxx_" + std::to_string(i))));
            }
        }

        auto eggs = cook.Finish();
        const auto part = *eggs.Lone();
        SetupSlices(params, part);
        UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[0].LevelCount(), params.Levels);
        if (params.Groups) {
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[1].LevelCount(), params.Levels);
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[2].LevelCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(part.IndexPages.BTreeGroups[3].LevelCount(), 1);
        }
        return eggs;
    }
}

Y_UNIT_TEST_SUITE_F(TIndexIteratorOwnership, ::NKikimr::NTable::NTest::TTableCacheTest) {
    class TBorrowedEnv final : public IBorrowedPages {
    public:
        explicit TBorrowedEnv(TTouchEnv& source)
            : Source_(source)
        {}

        TResult Locate(const TMemTable* table, ui64 ref, ui32 tag) override {
            return Source_.Locate(table, ref, tag);
        }

        TResult Locate(const TPart* part, ui64 ref, ELargeObj lob) override {
            return Source_.Locate(part, ref, lob);
        }

        TCachePageConstRef TryGetPage(const TPart* part, const TPageLocation& location, TGroupId group) override {
            return Retain(Source_.TryGetPage(part, location, group));
        }

        TCachePageConstRef TryGetIndexPage(const TPart* part, const TPageLocation& location, TGroupId group) override {
            return Retain(Source_.TryGetIndexPage(part, location, group));
        }

    private:
        TCachePageConstRef Retain(TCachePageRef page) {
            if (!page) {
                return {};
            }
            Pages_.push_back(std::move(page));
            return Pages_.back().Borrow();
        }

        TTouchEnv& Source_;
        TVector<TCachePageRef> Pages_;
    };

    void CheckIndex(const NTest::TPartEggs& eggs) {
        const auto& part = *eggs.Lone();
        TTouchEnv ownedEnv, source;
        TBorrowedEnv borrowedEnv(source);
        auto owned = CreateIndexIter(&part, &ownedEnv, {});
        auto borrowed = CreateIndexIter(&part, &borrowedEnv, {});
        ui32 faults = 0;
        const auto step = [&](auto&& action) {
            for (ui32 retry = 0; retry <= 10; ++retry) {
                const auto expected = action(*owned);
                const auto actual = action(*borrowed);
                UNIT_ASSERT(actual == expected);
                if (actual == EReady::Page) {
                    ++faults;
                    ownedEnv.LoadTouched();
                    source.LoadTouched();
                    continue;
                }
                UNIT_ASSERT_VALUES_EQUAL(borrowed->IsValid(), owned->IsValid());
                if (actual == EReady::Data) {
                    UNIT_ASSERT(borrowed->GetLocation() == owned->GetLocation());
                    UNIT_ASSERT_VALUES_EQUAL(borrowed->GetRowId(), owned->GetRowId());
                    UNIT_ASSERT_VALUES_EQUAL(borrowed->GetNextRowId(), owned->GetNextRowId());
                    TSmallVec<TCell> actualKey, expectedKey;
                    borrowed->GetKeyCells(actualKey);
                    owned->GetKeyCells(expectedKey);
                    UNIT_ASSERT_VALUES_EQUAL(actualKey.size(), expectedKey.size());
                    for (ui32 column : xrange(actualKey.size())) {
                        UNIT_ASSERT_VALUES_EQUAL(CompareTypedCells(actualKey[column], expectedKey[column],
                                                     eggs.Scheme->Keys->Types[column]), 0);
                    }
                }
                return actual;
            }
            UNIT_FAIL("Index pages did not become ready");
            return EReady::Page;
        };
        for (auto ready = step([](auto& index) {
                 return index.Seek(0);
             });
             ready == EReady::Data; ready = step([](auto& index) {
                 return index.Next();
             })) {
        }
        for (auto ready = step([](auto& index) {
                 return index.SeekLast();
             });
             ready == EReady::Data; ready = step([](auto& index) {
                 return index.Prev();
             })) {
        }
        for (TRowId row : xrange(part.Stat.Rows + 1)) {
            step([&](auto& index) {
                return index.Seek(row);
            });
        }
        for (ui32 first : { 0u, 1u, 3u, 5u }) {
            for (ui32 second : { 0u, 5u, 12u, 13u }) {
                const auto key = MakeKey(first, second);
                for (ESeek seek : { ESeek::Exact, ESeek::Lower, ESeek::Upper }) {
                    step([&](auto& index) {
                        return index.Seek(seek, key, eggs.Scheme->Keys.Get());
                    });
                    step([&](auto& index) {
                        return index.SeekReverse(seek, key, eggs.Scheme->Keys.Get());
                    });
                }
            }
        }
        if (part.IndexPages.HasBTree() && !part.IndexPages.GetBTree({}).LevelCount()) {
            UNIT_ASSERT_VALUES_EQUAL(faults, 0);
        } else {
            UNIT_ASSERT(faults > 0);
        }
    }

    Y_UNIT_TEST(BorrowedBtreeV1MatchesOwnedIterator) {
        CheckIndex(MakePart({ .Levels = 3 }));
    }

    Y_UNIT_TEST(BorrowedBtreeV2MatchesOwnedIterator) {
        CheckIndex(MakeV2Part({ .Levels = 3 }));
    }

    Y_UNIT_TEST(BorrowedFlatMatchesOwnedIterator) {
        CheckIndex(MakePart({ .Levels = 3, .FlatOnly = true }));
    }

    Y_UNIT_TEST(BorrowedBtreeWithoutNodesMatchesOwnedIterator) {
        CheckIndex(MakePart({ .Levels = 0 }));
    }
}

Y_UNIT_TEST_SUITE_F(TRowReaderOwnership, ::NKikimr::NTable::NTest::TTableCacheTest) {
    template <class TOwned, class TBorrowed>
    void CheckReaders(TOwned & owned, TBorrowed & borrowed, TTouchEnv & ownedEnv, TTouchEnv & source,
        const NTest::TPartEggs& eggs, TTagsRef tags) {
        const auto step = [&](auto&& action) {
            for (ui32 retry = 0; retry <= 10; ++retry) {
                const auto expected = action(owned);
                const auto actual = action(borrowed);
                UNIT_ASSERT(actual == expected);
                if (actual != EReady::Page) {
                    return actual;
                }
                ownedEnv.LoadTouched();
                source.LoadTouched();
            }
            UNIT_FAIL("Reader pages did not become ready");
            return EReady::Page;
        };
        const auto checkRow = [&] {
            UNIT_ASSERT_VALUES_EQUAL(borrowed.GetRowId(), owned.GetRowId());
            UNIT_ASSERT(borrowed.GetRowVersion() == owned.GetRowVersion());
            const auto expectedKey = owned.GetKey().Cells();
            const auto actualKey = borrowed.GetKey().Cells();
            UNIT_ASSERT_VALUES_EQUAL(actualKey.size(), expectedKey.size());
            for (ui32 column : xrange(actualKey.size())) {
                UNIT_ASSERT_VALUES_EQUAL(
                    CompareTypedCells(actualKey[column], expectedKey[column], eggs.Scheme->Keys->Types[column]), 0);
            }
            for (ui32 retry = 0; retry <= 10; ++retry) {
                TRowState expected(tags.size()), actual(tags.size());
                owned.Apply(expected, nullptr, nullptr);
                borrowed.Apply(actual, nullptr, nullptr);
                UNIT_ASSERT_VALUES_EQUAL(actual.Need(), expected.Need());
                if (actual.Need()) {
                    ownedEnv.LoadTouched();
                    source.LoadTouched();
                    continue;
                }
                UNIT_ASSERT(actual.GetRowState() == expected.GetRowState());
                for (ui32 column : xrange(tags.size())) {
                    UNIT_ASSERT(actual.GetCellOp(column) == expected.GetCellOp(column));
                    const auto& type = eggs.Lone()->Scheme->FindColumnByTag(tags[column])->TypeInfo;
                    UNIT_ASSERT_VALUES_EQUAL(CompareTypedCells(actual.Get(column), expected.Get(column), type), 0);
                }
                return;
            }
            UNIT_FAIL("Reader column groups did not become ready");
        };
        for (bool reverse : { false, true }) {
            auto ready = step([&](auto& reader) {
                return reverse ? reader.SeekReverse({}, ESeek::Lower) : reader.Seek({}, ESeek::Lower);
            });
            ui32 rows = 0;
            while (ready == EReady::Data) {
                checkRow();
                UNIT_ASSERT(step([&](auto& reader) {
                    TIteratorStats stats;
                    return reader.SkipToRowVersion({ 0, 0 }, stats, nullptr, nullptr, ITransactionSet::None);
                }) == EReady::Data);
                checkRow();
                ++rows;
                ready = step([&](auto& reader) {
                    return reverse ? reader.Prev() : reader.Next();
                });
            }
            UNIT_ASSERT_VALUES_EQUAL(rows, eggs.Lone()->Stat.Rows);
        }
    }

    void CheckPart(const NTest::TPartEggs& eggs) {
        const TVector<TTag> tags{ 2, 3, 4 };
        TTouchEnv ownedEnv, source;
        TIndexIteratorOwnership::TBorrowedEnv borrowedEnv(source);
        TPartIter owned(eggs.Lone().Get(), tags, eggs.Scheme->Keys, &ownedEnv);
        TPartIterImpl<TCachePageConstRef> borrowed(eggs.Lone().Get(), tags, eggs.Scheme->Keys, &borrowedEnv);
        CheckReaders(owned, borrowed, ownedEnv, source, eggs, tags);
    }

    void CheckRun(const NTest::TPartEggs& eggs) {
        const TVector<TTag> tags{ 2, 3, 4 };
        TRun run(*eggs.Scheme->Keys);
        for (const auto& slice : *eggs.Lone()->Slices) {
            UNIT_ASSERT(run.Insert(eggs.Lone(), slice).second);
        }
        TTouchEnv ownedEnv, source;
        TIndexIteratorOwnership::TBorrowedEnv borrowedEnv(source);
        TRunIter owned(run, tags, eggs.Scheme->Keys, &ownedEnv);
        TRunIterImpl<TCachePageConstRef> borrowed(run, tags, eggs.Scheme->Keys, &borrowedEnv);
        CheckReaders(owned, borrowed, ownedEnv, source, eggs, tags);
    }

    Y_UNIT_TEST(BorrowedPartV1RetainsGroupAndHistoryViews) {
        CheckPart(MakePart({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(BorrowedPartV2RetainsGroupAndHistoryViews) {
        CheckPart(MakeV2Part({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(BorrowedFlatPartRetainsGroupAndHistoryViews) {
        CheckPart(MakePart({ .Levels = 3, .Groups = true, .History = true, .FlatOnly = true }));
    }

    Y_UNIT_TEST(BorrowedRunV1RetainsGroupAndHistoryViews) {
        CheckRun(MakePart({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(BorrowedRunV2RetainsGroupAndHistoryViews) {
        CheckRun(MakeV2Part({ .Levels = 3, .Groups = true, .History = true }));
    }
}

Y_UNIT_TEST_SUITE_F(TPrechargeOwnership, ::NKikimr::NTable::NTest::TTableCacheTest) {
    struct TReaderSchema : NIceDb::Schema {
        struct Rows : Table<1> {
            struct First : Column<0, NScheme::NTypeIds::Uint32> {};

            struct Second : Column<1, NScheme::NTypeIds::Uint32> {};

            struct Value : Column<2, NScheme::NTypeIds::Uint32> {};

            struct WideValue : Column<3, NScheme::NTypeIds::Uint64> {};

            struct Text : Column<4, NScheme::NTypeIds::String> {};

            using TKey = TableKey<First, Second>;
            using TColumns = TableColumns<First, Second, Value, WideValue, Text>;
        };

        struct NoAutoRows : Rows {
            using Precharge = NoAutoPrecharge;
        };

        using TTables = SchemaTables<Rows>;
    };

    TScheme::TTableInfo MakeTableScheme(const NTest::TPartEggs& eggs) {
        TScheme::TTableInfo schema("precharge", 1);
        for (const auto& col : eggs.Scheme->Cols) {
            auto& column = schema.Columns[col.Tag];
            column.Id = col.Tag;
            column.PType = col.TypeInfo;
            column.KeyOrder = col.Key;
            column.Family = eggs.Scheme->Families[col.Group];
        }
        return schema;
    }

    void InitTable(TTable & table, const NTest::TPartEggs& eggs) {
        table.SetScheme(MakeTableScheme(eggs));
        table.Merge(eggs.ToPartView());
    }

    void CheckTablePrecharge(const NTest::TPartEggs& eggs) {
        TTable table(TEpoch::Zero());
        InitTable(table, eggs);
        const TVector<TTag> tags{ 2, 3, 4 };
        const ui32 first[] = { 1, 5 }, last[] = { 4, 5 };
        const TRawTypeValue firstKey[] = {
            TRawTypeValue(&first[0], sizeof(ui32), NScheme::NTypeIds::Uint32),
            TRawTypeValue(&first[1], sizeof(ui32), NScheme::NTypeIds::Uint32),
        };
        const TRawTypeValue lastKey[] = {
            TRawTypeValue(&last[0], sizeof(ui32), NScheme::NTypeIds::Uint32),
            TRawTypeValue(&last[1], sizeof(ui32), NScheme::NTypeIds::Uint32),
        };
        const std::pair<ui64, ui64> limits[] = { { 0, 0 }, { 5, 0 }, { 0, 1024 } };
        for (auto direction : { EDirection::Forward, EDirection::Reverse }) {
            for (bool point : { false, true }) {
                for (const auto& [items, bytes] : limits) {
                    TTouchEnv owned;
                    TTouchEnv source;
                    TIndexIteratorOwnership::TBorrowedEnv borrowed(source);
                    TSelectStats ownedStats, borrowedStats;
                    bool ready = false;
                    for (ui32 retry = 0; retry < 32 && !ready; ++retry) {
                        const auto precharge = [&](auto* env, TSelectStats& stats) {
                            return table.Precharge(firstKey, point ? TRawVals(firstKey) : TRawVals(lastKey), tags, env,
                                EHint::NoByKey, items, bytes, direction, TRowVersion(0, 1), stats);
                        };
                        const auto expected = precharge(&owned, ownedStats);
                        const auto actual = precharge(&borrowed, borrowedStats);
                        UNIT_ASSERT_VALUES_EQUAL(actual.Ready, expected.Ready);
                        UNIT_ASSERT_VALUES_EQUAL(actual.ItemsPrecharged, expected.ItemsPrecharged);
                        UNIT_ASSERT_VALUES_EQUAL(actual.BytesPrecharged, expected.BytesPrecharged);
                        UNIT_ASSERT_VALUES_EQUAL(borrowedStats.Sieved, ownedStats.Sieved);
                        UNIT_ASSERT_VALUES_EQUAL(borrowedStats.Weeded, ownedStats.Weeded);
                        UNIT_ASSERT(source.Touched == owned.Touched);
                        ready = actual.Ready;
                        owned.LoadTouched();
                        source.LoadTouched();
                    }
                    UNIT_ASSERT(ready);
                }
            }
        }
    }

    Y_UNIT_TEST(TablePrechargeSelectsBorrowedV1) {
        CheckTablePrecharge(MakePart({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(TablePrechargeSelectsBorrowedV2) {
        CheckTablePrecharge(MakeV2Part({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(TablePrechargeSelectsBorrowedFlat) {
        CheckTablePrecharge(MakePart({ .Levels = 3, .Groups = true, .History = true, .FlatOnly = true }));
    }

    void CheckTableRowVersions(const NTest::TPartEggs& eggs) {
        TTable table(TEpoch::Zero());
        InitTable(table, eggs);
        const std::pair<ui32, ui32> keys[] = { { 1, 3 }, { 1, 5 }, { 100, 3 } };
        for (const auto& [first, second] : keys) {
            const TRawTypeValue rawKey[] = {
                TRawTypeValue(&first, sizeof(ui32), NScheme::NTypeIds::Uint32),
                TRawTypeValue(&second, sizeof(ui32), NScheme::NTypeIds::Uint32),
            };
            const auto cells = MakeKey(first, second);
            for (bool raw : { false, true }) {
                TTouchEnv owned;
                TTouchEnv source;
                TIndexIteratorOwnership::TBorrowedEnv borrowed(source);
                bool ready = false;
                for (ui32 retry = 0; retry < 32 && !ready; ++retry) {
                    const auto select = [&](auto* env) {
                        return raw ? table.SelectRowVersion(rawKey, env, EHint::NoByKey)
                                   : table.SelectRowVersion(cells, env, EHint::NoByKey);
                    };
                    const auto expected = select(&owned);
                    const auto actual = select(&borrowed);
                    UNIT_ASSERT(actual.Ready == expected.Ready);
                    UNIT_ASSERT(actual.RowVersion == expected.RowVersion);
                    UNIT_ASSERT_VALUES_EQUAL(actual.RowTxId, expected.RowTxId);
                    UNIT_ASSERT(actual.RowOp == expected.RowOp);
                    UNIT_ASSERT(actual.LockMode == expected.LockMode);
                    UNIT_ASSERT_VALUES_EQUAL(actual.LockTxId, expected.LockTxId);
                    UNIT_ASSERT(source.Touched == owned.Touched);
                    ready = actual.Ready != EReady::Page;
                    owned.LoadTouched();
                    source.LoadTouched();
                }
                UNIT_ASSERT(ready);
            }
        }
    }

    Y_UNIT_TEST(TableRowVersionSelectsBorrowedV1) {
        CheckTableRowVersions(MakePart({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(TableRowVersionSelectsBorrowedV2) {
        CheckTableRowVersions(MakeV2Part({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(TableRowVersionSelectsBorrowedFlat) {
        CheckTableRowVersions(MakePart({ .Levels = 3, .Groups = true, .History = true, .FlatOnly = true }));
    }

    void CheckDatabaseReader(EPage indexType, ECodec codec) {
        TLayoutCook lay;
        lay.Col(0, 0, NScheme::NTypeIds::Uint32)
            .Col(0, 1, NScheme::NTypeIds::Uint32)
            .Col(0, 2, NScheme::NTypeIds::Uint32)
            .Col(1, 3, NScheme::NTypeIds::Uint64)
            .Col(1, 4, NScheme::NTypeIds::String)
            .Key({ 0, 1 });
        NPage::TConf conf;
        for (ui32 group : { 0, 1 }) {
            conf.Group(group).PageRows = 2;
            conf.Group(group).PageSize = 256;
            conf.Group(group).Codec = codec;
            conf.Group(group).ForceCompression = codec == ECodec::LZ4;
            conf.Group(group).BTreeIndexNodeKeysMin = conf.Group(group).BTreeIndexNodeKeysMax = 2;
        }
        conf.WriteFlatIndex = indexType == EPage::FlatIndex;
        conf.WriteBTreeIndexV1 = indexType == EPage::BTreeIndex;
        conf.WriteBTreeIndexV2 = indexType == EPage::BTreeIndexV2;
        TPartCook olderCook(lay, conf, TLogoBlobID(801, 2, 3, 1, 0, 0), TEpoch(i64(1)));
        TPartCook newerCook(lay, conf, TLogoBlobID(802, 2, 3, 1, 0, 0), TEpoch(i64(2)));
        const TString payload(512, 'x');
        for (ui32 key = 0; key < 16; ++key) {
            olderCook.AddN(key, ui32(0), key, ui64(key), payload);
            newerCook.AddN(key, ui32(0), key + 100);
        }
        const auto older = olderCook.Finish();
        const auto newer = newerCook.Finish();
        const auto makeDatabase = [&] {
            TAutoPtr<TScheme> schema = new TScheme;
            schema->Tables.emplace(1, MakeTableScheme(older));
            auto database = MakeHolder<TDatabase>(new TDatabaseImpl(0, schema, nullptr));
            database->Merge(1, older.ToPartView());
            database->Merge(1, newer.ToPartView());
            return database;
        };
        auto ownedDatabase = makeDatabase();
        auto borrowedDatabase = makeDatabase();
        const TVector<TTag> tags{ 2, 3, 4 };
        for (bool commit : { false, true }) {
            TTouchEnv ownedEnv, source;
            TIndexIteratorOwnership::TBorrowedEnv borrowedEnv(source);
            TTransactionReader reader(*borrowedDatabase, borrowedEnv);
            ownedDatabase->Begin(1, ownedEnv);
            borrowedDatabase->Begin(1, source);
            TRowState ownedRow, borrowedRow;
            NIceDb::TNiceDb ownedNice(*ownedDatabase);
            NIceDb::TTransactionNiceDb borrowedNice(reader);
            UNIT_ASSERT(&borrowedNice.GetDatabase() == borrowedDatabase.Get());
            UNIT_ASSERT(borrowedNice.HaveTable<TReaderSchema::Rows>());
            const auto checkRowsets = [&](auto makeOwned, auto makeBorrowed, const TVector<ui32>& keys) {
                bool finished = false;
                for (ui32 retry = 0; retry < 128 && !finished; ++retry) {
                    auto owned = makeOwned();
                    auto borrowed = makeBorrowed();
                    ui32 rows = 0;
                    while (owned.IsReady() && borrowed.IsReady()) {
                        UNIT_ASSERT_VALUES_EQUAL(borrowed.IsValid(), owned.IsValid());
                        if (!owned.IsValid()) {
                            UNIT_ASSERT_VALUES_EQUAL(rows, keys.size());
                            finished = true;
                            break;
                        }
                        UNIT_ASSERT(rows < keys.size());
                        UNIT_ASSERT_VALUES_EQUAL(borrowed.template GetValue<TReaderSchema::Rows::First>(), keys[rows]);
                        UNIT_ASSERT_VALUES_EQUAL(
                            borrowed.template GetValue<TReaderSchema::Rows::Value>(), keys[rows] + 100);
                        UNIT_ASSERT_VALUES_EQUAL(
                            borrowed.template GetValue<TReaderSchema::Rows::WideValue>(), keys[rows]);
                        UNIT_ASSERT_VALUES_EQUAL(borrowed.template GetValue<TReaderSchema::Rows::Text>(), payload);
                        UNIT_ASSERT_VALUES_EQUAL(borrowed.template GetValue<TReaderSchema::Rows::Text>(),
                            owned.template GetValue<TReaderSchema::Rows::Text>());
                        ++rows;
                        owned.Next();
                        borrowed.Next();
                    }
                    UNIT_ASSERT_VALUES_EQUAL(borrowed.IsReady(), owned.IsReady());
                    ownedEnv.LoadTouched();
                    source.LoadTouched();
                }
                UNIT_ASSERT(finished);
            };
            checkRowsets(
                [&] {
                    return ownedNice.Table<TReaderSchema::Rows>().Key(ui32(3), ui32(0)).Select();
                },
                [&] {
                    return borrowedNice.Table<TReaderSchema::Rows>().Key(ui32(3), ui32(0)).Select();
                },
                { 3 });
            // The selector keeps the reader binding after the temporary facade is destroyed
            auto ownedSelector = NIceDb::TNiceDb(*ownedDatabase).Table<TReaderSchema::Rows>();
            auto borrowedSelector = NIceDb::TTransactionNiceDb(reader).Table<TReaderSchema::Rows>();
            checkRowsets(
                [&] {
                    return ownedSelector.GreaterOrEqual(ui32(3)).LessOrEqual(ui32(12)).Select();
                },
                [&] {
                    return borrowedSelector.GreaterOrEqual(ui32(3)).LessOrEqual(ui32(12)).Select();
                },
                { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
            checkRowsets(
                [&] {
                    return ownedSelector.GreaterOrEqual(ui32(3)).LessOrEqual(ui32(12)).Reverse().Select();
                },
                [&] {
                    return borrowedSelector.GreaterOrEqual(ui32(3)).LessOrEqual(ui32(12)).Reverse().Select();
                },
                { 12, 11, 10, 9, 8, 7, 6, 5, 4, 3 });
            checkRowsets(
                [&] {
                    return ownedSelector.Prefix(ui32(3)).Select();
                },
                [&] {
                    return borrowedSelector.Prefix(ui32(3)).Select();
                },
                { 3 });
            checkRowsets(
                [&] {
                    return ownedNice.Table<TReaderSchema::NoAutoRows>().Prefix(ui32(3)).Select();
                },
                [&] {
                    return borrowedNice.Table<TReaderSchema::NoAutoRows>().Prefix(ui32(3)).Select();
                },
                { 3 });
            for (ui32 retry = 0; retry < 32; ++retry) {
                const bool expected = ownedNice.Precharge<TReaderSchema>();
                const bool actual = borrowedNice.Precharge<TReaderSchema>();
                UNIT_ASSERT_VALUES_EQUAL(actual, expected);
                ownedEnv.LoadTouched();
                source.LoadTouched();
                if (actual) {
                    break;
                }
                UNIT_ASSERT(retry + 1 < 32);
            }
            const auto checkIterators = [&](auto makeOwned, auto makeBorrowed, const TVector<ui32>& keys) {
                auto owned = makeOwned();
                auto borrowed = makeBorrowed();
                ui32 rows = 0;
                bool finished = false;
                for (ui32 attempt = 0; attempt < 128 && !finished; ++attempt) {
                    const auto expected = owned->Next(ENext::All);
                    const auto actual = borrowed->Next(ENext::All);
                    UNIT_ASSERT(actual == expected);
                    if (actual == EReady::Page) {
                        ownedEnv.LoadTouched();
                        source.LoadTouched();
                        if (rows == 0) {
                            owned = makeOwned();
                            borrowed = makeBorrowed();
                        }
                        continue;
                    }
                    if (actual == EReady::Gone) {
                        finished = true;
                        break;
                    }
                    UNIT_ASSERT(rows < keys.size());
                    UNIT_ASSERT_VALUES_EQUAL(borrowed->GetKey().Cells()[0].template AsValue<ui32>(), keys[rows]);
                    UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec::Serialize(borrowed->GetValues().Cells()),
                        TSerializedCellVec::Serialize(owned->GetValues().Cells()));
                    UNIT_ASSERT_VALUES_EQUAL(
                        borrowed->GetValues().Cells()[0].template AsValue<ui32>(), keys[rows] + 100);
                    UNIT_ASSERT_VALUES_EQUAL(borrowed->GetValues().Cells()[1].template AsValue<ui64>(), keys[rows]);
                    UNIT_ASSERT_VALUES_EQUAL(borrowed->GetValues().Cells()[2].AsBuf(), payload);
                    ++rows;
                }
                UNIT_ASSERT(finished);
                UNIT_ASSERT_VALUES_EQUAL(rows, keys.size());
                ownedEnv.LoadTouched();
                source.LoadTouched();
            };
            const ui32 lower = 3, upper = 12, second = 0;
            const TRawTypeValue lowerKey[] = {
                TRawTypeValue(&lower, sizeof(lower), NScheme::NTypeIds::Uint32),
                TRawTypeValue(&second, sizeof(second), NScheme::NTypeIds::Uint32),
            };
            const TRawTypeValue upperKey[] = {
                TRawTypeValue(&upper, sizeof(upper), NScheme::NTypeIds::Uint32),
                TRawTypeValue(&second, sizeof(second), NScheme::NTypeIds::Uint32),
            };
            const TKeyRange range{ lowerKey, upperKey, false, true };
            checkIterators(
                [&] {
                    return ownedDatabase->IterateRangeGeneric<TTableIter>(1, range, tags);
                },
                [&] {
                    return reader.IterateRangeGeneric<TBorrowedTableIter>(1, range, tags);
                },
                { 4, 5, 6, 7, 8, 9, 10, 11, 12 });
            checkIterators(
                [&] {
                    return ownedDatabase->IterateRangeGeneric<TTableReverseIter>(1, range, tags);
                },
                [&] {
                    return reader.IterateRangeGeneric<TBorrowedTableReverseIter>(1, range, tags);
                },
                { 12, 11, 10, 9, 8, 7, 6, 5, 4 });
            checkIterators(
                [&] {
                    return ownedDatabase->IterateExact(1, lowerKey, tags);
                },
                [&] {
                    return reader.IterateExact(1, lowerKey, tags);
                },
                { 3 });
            const auto prefix = MakeKey(lower, 0);
            const auto expectedPrefix =
                ownedDatabase->SelectRowVersionByKeyPrefix(1, TArrayRef<const TCell>(prefix).Slice(0, 1));
            const auto actualPrefix = reader.SelectRowVersionByKeyPrefix(1, TArrayRef<const TCell>(prefix).Slice(0, 1));
            UNIT_ASSERT(actualPrefix.Ready == expectedPrefix.Ready);
            UNIT_ASSERT(actualPrefix.RowVersion == expectedPrefix.RowVersion);
            UNIT_ASSERT_VALUES_EQUAL(actualPrefix.RowTxId, expectedPrefix.RowTxId);
            ownedEnv.LoadTouched();
            source.LoadTouched();
            for (ui32 key : { 1, 9, 15 }) {
                const ui32 second = 0;
                const TRawTypeValue rawKey[] = {
                    TRawTypeValue(&key, sizeof(key), NScheme::NTypeIds::Uint32),
                    TRawTypeValue(&second, sizeof(second), NScheme::NTypeIds::Uint32),
                };
                EReady ready = EReady::Page;
                for (ui32 retry = 0; retry < 32 && ready == EReady::Page; ++retry) {
                    const auto expected = ownedDatabase->Select(1, rawKey, tags, ownedRow, EHint::NoByKey);
                    ready = reader.Select(1, rawKey, tags, borrowedRow, EHint::NoByKey);
                    UNIT_ASSERT(ready == expected);
                    UNIT_ASSERT(source.Touched == ownedEnv.Touched);
                    ownedEnv.LoadTouched();
                    source.LoadTouched();
                }
                UNIT_ASSERT(ready == EReady::Data);
                UNIT_ASSERT_VALUES_EQUAL(
                    TSerializedCellVec::Serialize(*borrowedRow), TSerializedCellVec::Serialize(*ownedRow));
                UNIT_ASSERT_VALUES_EQUAL(borrowedRow.Get(0).AsValue<ui32>(), key + 100);
                UNIT_ASSERT_VALUES_EQUAL(borrowedRow.Get(1).AsValue<ui64>(), key);
                UNIT_ASSERT_VALUES_EQUAL(borrowedRow.Get(2).AsBuf(), payload);

                reader.Precharge(1, rawKey, rawKey, tags, EHint::NoByKey, 0, 0);
                reader.SelectRowVersion(1, rawKey, EHint::NoByKey);
                UNIT_ASSERT_VALUES_EQUAL(borrowedRow.Get(2).AsBuf(), payload);
                // Keep both environments in the same loaded-page state for the next Select
                ownedDatabase->Precharge(1, rawKey, rawKey, tags, EHint::NoByKey, 0, 0);
                ownedDatabase->SelectRowVersion(1, rawKey, EHint::NoByKey);
                ownedEnv.LoadTouched();
                source.LoadTouched();
                if (key == 9 && !commit) {
                    bool rollbackRan = false;
                    borrowedDatabase->OnRollback([&] {
                        UNIT_ASSERT_VALUES_EQUAL(borrowedRow.Get(2).AsBuf(), payload);
                        rollbackRan = true;
                    });
                    borrowedDatabase->RollbackChanges(reader);
                    ownedDatabase->RollbackChanges();
                    UNIT_ASSERT(rollbackRan);
                }
            }
            bool callbackRan = false;
            ownedNice.Table<TReaderSchema::Rows>()
                .Key(ui32(100), ui32(0))
                .Update<TReaderSchema::Rows::Value>(ui32(999));
            borrowedNice.Table<TReaderSchema::Rows>()
                .Key(ui32(100), ui32(0))
                .Update<TReaderSchema::Rows::Value>(ui32(999));
            const auto callback = [&] {
                UNIT_ASSERT_VALUES_EQUAL(borrowedRow.Get(2).AsBuf(), payload);
                callbackRan = true;
            };
            if (commit) {
                borrowedDatabase->OnCommit(callback);
            } else {
                borrowedDatabase->OnRollback(callback);
            }
            borrowedDatabase->Commit(1, commit, reader);
            ownedDatabase->Commit(1, commit);
            UNIT_ASSERT(callbackRan);
        }
    }

    Y_UNIT_TEST(TypedDatabaseReaderV1) {
        for (auto codec : { ECodec::Plain, ECodec::LZ4 }) {
            CheckDatabaseReader(EPage::BTreeIndex, codec);
        }
    }

    Y_UNIT_TEST(TypedDatabaseReaderV2) {
        for (auto codec : { ECodec::Plain, ECodec::LZ4 }) {
            CheckDatabaseReader(EPage::BTreeIndexV2, codec);
        }
    }

    Y_UNIT_TEST(TypedDatabaseReaderFlat) {
        for (auto codec : { ECodec::Plain, ECodec::LZ4 }) {
            CheckDatabaseReader(EPage::FlatIndex, codec);
        }
    }

    void CheckCharge(const NTest::TPartEggs& eggs) {
        const auto& part = *eggs.Lone();
        const TVector<TTag> tags{ 2, 3, 4 };
        const std::pair<ui64, ui64> limits[] = { { 0, 0 }, { 5, 0 }, { 0, 1024 } };
        for (bool reverse : { false, true }) {
            for (bool byKey : { false, true }) {
                for (const auto& [items, bytes] : limits) {
                    TTouchEnv ownedEnv, source;
                    TIndexIteratorOwnership::TBorrowedEnv borrowedEnv(source);
                    auto owned = CreateCharge(&ownedEnv, part, tags, true);
                    auto borrowed = CreateCharge(&borrowedEnv, part, tags, true);
                    auto first = byKey ? MakeKey(1, 5) : TVector<TCell>();
                    auto last = byKey ? MakeKey(4, 5) : TVector<TCell>();
                    if (reverse) {
                        std::swap(first, last);
                    }
                    bool ready = false;
                    for (ui32 retry = 0; retry < 32 && !ready; ++retry) {
                        const auto charge = [&](ICharge& reader) {
                            return reverse ? reader.DoReverse(
                                                 first, last, part.Stat.Rows - 1, 0, *eggs.Scheme->Keys, items, bytes)
                                           : reader.Do(
                                                 first, last, 0, part.Stat.Rows - 1, *eggs.Scheme->Keys, items, bytes);
                        };
                        const auto expected = charge(*owned);
                        const auto actual = charge(*borrowed);
                        UNIT_ASSERT_VALUES_EQUAL(actual.Ready, expected.Ready);
                        UNIT_ASSERT_VALUES_EQUAL(actual.Overshot, expected.Overshot);
                        UNIT_ASSERT_VALUES_EQUAL(actual.ItemsPrecharged, expected.ItemsPrecharged);
                        UNIT_ASSERT_VALUES_EQUAL(actual.BytesPrecharged, expected.BytesPrecharged);
                        UNIT_ASSERT(source.Touched == ownedEnv.Touched);
                        ready = actual.Ready;
                        ownedEnv.LoadTouched();
                        source.LoadTouched();
                    }
                    UNIT_ASSERT(ready);
                }
            }
        }
    }

    void CheckRange(const NTest::TPartEggs& eggs) {
        const TVector<TTag> tags{ 2, 3, 4 };
        TRun run(*eggs.Scheme->Keys);
        for (const auto& slice : *eggs.Lone()->Slices) {
            UNIT_ASSERT(run.Insert(eggs.Lone(), slice).second);
        }
        const std::pair<ui64, ui64> limits[] = { { 0, 0 }, { 5, 0 }, { 0, 1024 } };
        for (bool reverse : { false, true }) {
            for (const auto& [items, bytes] : limits) {
                TTouchEnv ownedEnv, source;
                TIndexIteratorOwnership::TBorrowedEnv borrowedEnv(source);
                auto first = MakeKey(0, 5);
                auto last = MakeKey(4, 5);
                if (reverse) {
                    std::swap(first, last);
                }
                bool ready = false;
                for (ui32 retry = 0; retry < 32 && !ready; ++retry) {
                    const auto charge = [&](auto* env) {
                        return reverse
                                   ? ChargeRangeReverse(
                                         env, first, last, run, *eggs.Scheme->Keys, tags, items, bytes, true)
                                   : ChargeRange(env, first, last, run, *eggs.Scheme->Keys, tags, items, bytes, true);
                    };
                    const auto expected = charge(&ownedEnv);
                    const auto actual = charge(&borrowedEnv);
                    UNIT_ASSERT_VALUES_EQUAL(actual.Ready, expected.Ready);
                    UNIT_ASSERT_VALUES_EQUAL(actual.ItemsPrecharged, expected.ItemsPrecharged);
                    UNIT_ASSERT_VALUES_EQUAL(actual.BytesPrecharged, expected.BytesPrecharged);
                    UNIT_ASSERT(source.Touched == ownedEnv.Touched);
                    ready = actual.Ready;
                    ownedEnv.LoadTouched();
                    source.LoadTouched();
                }
                UNIT_ASSERT(ready);
            }
        }
    }

    void CheckKeys(const NTest::TPartEggs& eggs) {
        const TIntrusiveConstPtr<TScreen> screens[] = { nullptr, new TScreen({ TScreen::THole(true) }),
            new TScreen({ { 3, 8 }, { 21, 27 }, { 37, 100 } }), new TScreen({ { 50, 100 } }) };
        for (const auto& screen : screens) {
            TIntrusivePtr<TSlices> expected, actual;
            {
                TTouchEnv ownedEnv, source;
                TIndexIteratorOwnership::TBorrowedEnv borrowedEnv(source);
                TKeysLoader owned(eggs.Lone().Get(), &ownedEnv);
                TKeysLoaderImpl<TCachePageConstRef> borrowed(eggs.Lone().Get(), &borrowedEnv);
                for (ui32 retry = 0; retry < 32 && !actual; ++retry) {
                    expected = owned.Do(screen);
                    actual = borrowed.Do(screen);
                    UNIT_ASSERT_VALUES_EQUAL(bool(actual), bool(expected));
                    UNIT_ASSERT(source.Touched == ownedEnv.Touched);
                    ownedEnv.LoadTouched();
                    source.LoadTouched();
                }
            }
            UNIT_ASSERT(actual);
            UNIT_ASSERT_VALUES_EQUAL(actual->size(), expected->size());
            for (ui32 pos : xrange(actual->size())) {
                const auto& a = actual->at(pos);
                const auto& e = expected->at(pos);
                UNIT_ASSERT_VALUES_EQUAL(a.FirstRowId, e.FirstRowId);
                UNIT_ASSERT_VALUES_EQUAL(a.LastRowId, e.LastRowId);
                UNIT_ASSERT_VALUES_EQUAL(a.FirstInclusive, e.FirstInclusive);
                UNIT_ASSERT_VALUES_EQUAL(a.LastInclusive, e.LastInclusive);
                UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec::Serialize(a.FirstKey.GetCells()),
                    TSerializedCellVec::Serialize(e.FirstKey.GetCells()));
                UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec::Serialize(a.LastKey.GetCells()),
                    TSerializedCellVec::Serialize(e.LastKey.GetCells()));
            }
        }
    }

    Y_UNIT_TEST(BorrowedChargeV1MatchesOwned) {
        CheckCharge(MakePart({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(BorrowedChargeV2MatchesOwned) {
        CheckCharge(MakeV2Part({ .Levels = 3, .Groups = true, .History = true }));
    }

    Y_UNIT_TEST(BorrowedChargeFlatMatchesOwned) {
        CheckCharge(MakePart({ .Levels = 3, .Groups = true, .History = true, .FlatOnly = true }));
    }

    Y_UNIT_TEST(BorrowedRangeV1MatchesOwned) {
        CheckRange(MakePart({ .Levels = 3, .Groups = true, .History = true, .Slices = TTestParams::Many }));
    }

    Y_UNIT_TEST(BorrowedRangeV2MatchesOwned) {
        CheckRange(MakeV2Part({ .Levels = 3, .Groups = true, .History = true, .Slices = TTestParams::Many }));
    }

    Y_UNIT_TEST(BorrowedRangeFlatMatchesOwned) {
        CheckRange(
            MakePart({ .Levels = 3, .Groups = true, .History = true, .Slices = TTestParams::Many, .FlatOnly = true }));
    }

    Y_UNIT_TEST(BorrowedKeysV1MatchOwnedSlices) {
        CheckKeys(MakePart({ .Levels = 3 }));
    }

    Y_UNIT_TEST(BorrowedKeysV2MatchOwnedSlices) {
        CheckKeys(MakeV2Part({ .Levels = 3 }));
    }

    Y_UNIT_TEST(BorrowedKeysFlatMatchOwnedSlices) {
        CheckKeys(MakePart({ .Levels = 3, .FlatOnly = true }));
    }
}

Y_UNIT_TEST_SUITE_F(TBtreeIndexNodeOwnership, ::NKikimr::NTable::NTest::TTableCacheTest) {
    void CheckNode(const NTest::TPartEggs& eggs, bool v2Format) {
        const auto& part = *eggs.Lone();
        NTest::TTestEnv env;
        auto owner = env.TryGetIndexPage(&part, part.IndexPages.GetRootLocation(&part, {}), {});
        UNIT_ASSERT(owner);

        TBtreeIndexNode original(std::move(owner), v2Format);
        UNIT_ASSERT(!owner);
        TBorrowedBtreeIndexNode borrowed(original.GetPageRef().Borrow(), v2Format);
        TBtreeIndexNode raw(original.GetPageRef().BuildSharedData(), v2Format);
        auto owned = std::move(original);
        UNIT_ASSERT(!original.GetPageRef());
        UNIT_ASSERT(borrowed.GetPageRef().data() == owned.GetPageRef().data());

        const auto& layout = part.Scheme->GetLayout({});
        const auto* keyDefaults = eggs.Scheme->Keys.Get();
        const auto check = [&](const auto& node) {
            UNIT_ASSERT_VALUES_EQUAL(node.GetKeysCount(), owned.GetKeysCount());
            UNIT_ASSERT_VALUES_EQUAL(node.GetChildrenCount(), owned.GetChildrenCount());
            UNIT_ASSERT_VALUES_EQUAL(node.IsFixedFormat(), owned.IsFixedFormat());
            UNIT_ASSERT_VALUES_EQUAL(node.IsShortChildFormat(), owned.IsShortChildFormat());
            for (TRecIdx pos : xrange(owned.GetChildrenCount())) {
                UNIT_ASSERT(node.GetChild(pos, false) == owned.GetChild(pos, false));
                UNIT_ASSERT_VALUES_EQUAL(node.GetChildRowCount(pos), owned.GetChildRowCount(pos));
            }
            for (TRecIdx pos : xrange(owned.GetKeysCount())) {
                auto actual = node.GetKeyCellsIter(pos, layout.ColsKeyIdx);
                auto expected = owned.GetKeyCellsIter(pos, layout.ColsKeyIdx);
                for (TPos column : xrange(actual.Count())) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        CompareTypedCells(actual.Next(), expected.Next(), layout.KeyTypes[column]), 0);
                }
            }
            for (TRowId row : xrange(part.Stat.Rows)) {
                UNIT_ASSERT_VALUES_EQUAL(node.Seek(row), owned.Seek(row));
            }
            for (ui32 first : { 0u, 1u, 3u, 5u }) {
                for (ui32 second : { 0u, 5u, 12u }) {
                    const auto key = MakeKey(first, second);
                    for (ESeek seek : { ESeek::Exact, ESeek::Lower, ESeek::Upper }) {
                        UNIT_ASSERT_VALUES_EQUAL(node.Seek(seek, key, layout.ColsKeyIdx, keyDefaults),
                            owned.Seek(seek, key, layout.ColsKeyIdx, keyDefaults));
                        UNIT_ASSERT_VALUES_EQUAL(node.SeekReverse(seek, key, layout.ColsKeyIdx, keyDefaults),
                            owned.SeekReverse(seek, key, layout.ColsKeyIdx, keyDefaults));
                    }
                }
            }
        };
        check(borrowed);
        check(raw);
        owner = owned.TakePageRef();
        UNIT_ASSERT(owner);
    }

    Y_UNIT_TEST(BorrowedV1MatchesOwnedNode) {
        CheckNode(MakePart({ .Levels = 3 }), false);
    }

    Y_UNIT_TEST(BorrowedV2MatchesOwnedNode) {
        CheckNode(MakeV2Part({ .Levels = 3 }), true);
    }
}

// ========================================================================
// Section 3.2: V2 twin tests — V2 B-tree produces same results as V1
// ========================================================================

Y_UNIT_TEST_SUITE_F(TPartGroupBtreeIndexIterV2, ::NKikimr::NTable::NTest::TTableCacheTest) {
    using namespace NTest;

    // V2 twin: verify v2 btree iteration produces same results as v1 btree
    void CheckV2Iter(TTestParams params) {
        // Make v1 part (btree+flat)
        TPartEggs v1Eggs = MakePart(params);
        const auto v1Part = *v1Eggs.Lone();

        // Make v2 part (btree only, no flat)
        TPartEggs v2Eggs = MakeV2Part(params);
        const auto v2Part = *v2Eggs.Lone();

        // Both must have the same row count
        UNIT_ASSERT_VALUES_EQUAL(v1Part.Stat.Rows, v2Part.Stat.Rows);

        // Iterate both btree indexes and compare page-by-page. Keep V1 resident,
        // but force V2 through miss/load/resume to validate byte-offset requests.
        TTestEnv v1Env;
        TTouchEnvV2 v2Env(&v2Part);
        auto v1Index = CreateIndexIter(&v1Part, &v1Env, {});
        auto v2Index = CreateIndexIter(&v2Part, &v2Env, {});

        auto retryV2 = [&](auto&& action) -> EReady {
            for (ui32 attempt = 0; attempt <= 10; attempt++) {
                v2Env.LoadTouched();
                if (auto ready = action(); ready != EReady::Page) {
                    return ready;
                }
            }
            UNIT_ASSERT_C(false, "V2 index iteration did not become ready");
            return EReady::Page;
        };

        for (size_t i = 0; ; i++) {
            auto v1Ready = i == 0 ? v1Index->Seek(0) : v1Index->Next();
            auto v2Ready = retryV2([&]() {
                return i == 0 ? v2Index->Seek(0) : v2Index->Next();
            });

            UNIT_ASSERT_VALUES_EQUAL_C(v1Ready, v2Ready,
                "V2 iteration readiness differs at step " + ToString(i));
            if (v1Ready != EReady::Data) {
                break;
            }

            UNIT_ASSERT_VALUES_EQUAL_C(v1Index->GetRowId(), v2Index->GetRowId(),
                "V2 row id mismatch at step " + ToString(i));
            UNIT_ASSERT_VALUES_EQUAL_C(v1Index->GetNextRowId(), v2Index->GetNextRowId(),
                "V2 next row id mismatch at step " + ToString(i));
            UNIT_ASSERT_VALUES_EQUAL_C(v1Index->GetLocation().Size, v2Index->GetLocation().Size,
                "V2 page size mismatch at step " + ToString(i));
        }

        // Key-based seek must land on the same page, not merely return the same status.
        // Run the V2 side on TTouchEnvV2 (miss/load/resume) so the requested Type
        // is validated against the store on the Seek/SeekReverse byte-offset paths.
        const auto* keyDefaults = v1Eggs.Scheme->Keys.Get();
        for (ui32 first : {0u, 1u, 3u, 5u}) {
            for (ui32 second : {0u, 5u, 13u}) {
                for (bool reverse : {false, true}) {
                    for (ESeek seek : {ESeek::Exact, ESeek::Lower, ESeek::Upper}) {
                        TTestEnv v1SeekEnv;
                        TTouchEnvV2 v2SeekEnv(&v2Part);
                        auto v1Seek = CreateIndexIter(&v1Part, &v1SeekEnv, {});
                        auto v2Seek = CreateIndexIter(&v2Part, &v2SeekEnv, {});
                        auto key = MakeKey(first, second);
                        auto v1Ready = reverse
                            ? v1Seek->SeekReverse(seek, key, keyDefaults)
                            : v1Seek->Seek(seek, key, keyDefaults);
                        EReady v2Ready = EReady::Page;
                        for (ui32 attempt = 0; attempt <= 10; attempt++) {
                            v2SeekEnv.LoadTouched();
                            v2Ready = reverse
                                ? v2Seek->SeekReverse(seek, key, keyDefaults)
                                : v2Seek->Seek(seek, key, keyDefaults);
                            if (v2Ready != EReady::Page) {
                                break;
                            }
                        }
                        UNIT_ASSERT_C(v2Ready != EReady::Page,
                            "V2 key seek did not become ready");

                        UNIT_ASSERT_VALUES_EQUAL(v1Ready, v2Ready);
                        UNIT_ASSERT_VALUES_EQUAL(v1Seek->IsValid(), v2Seek->IsValid());
                        if (v1Ready == EReady::Data) {
                            UNIT_ASSERT_VALUES_EQUAL(v1Seek->GetRowId(), v2Seek->GetRowId());
                            UNIT_ASSERT_VALUES_EQUAL(v1Seek->GetNextRowId(), v2Seek->GetNextRowId());
                            UNIT_ASSERT_VALUES_EQUAL(v1Seek->GetLocation().Size, v2Seek->GetLocation().Size);
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(NoNodes_V2) {
        CheckV2Iter({.Levels = 0});
    }

    Y_UNIT_TEST(OneNode_V2) {
        CheckV2Iter({.Levels = 1});
    }

    Y_UNIT_TEST(FewNodes_V2) {
        CheckV2Iter({.Levels = 3});
    }

}

Y_UNIT_TEST_SUITE_F(TChargeBTreeIndexV2, ::NKikimr::NTable::NTest::TTableCacheTest) {
    using namespace NTest;

    // Basic sanity: V2 part can be charged (charge returns ready, produces non-zero results)
    void CheckV2ChargeBasic(TTestParams params) {
        TPartEggs eggs = MakeV2Part(params);
        const auto part = *eggs.Lone();

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }

        TTestEnv env;
        TChargeBTreeIndex charge(&env, part, tags, true);
        const auto& keyDefaults = *eggs.Scheme->Keys.Get();
        const auto& meta = part.IndexPages.BTreeGroups[0];

        // Full-range charge must succeed
        auto result = charge.Do(TCells{}, TCells{}, 0, meta.GetRowCount() - 1, keyDefaults, 0, 0);
        UNIT_ASSERT_C(result.Ready, "V2 full-range charge must be ready");
        UNIT_ASSERT_C(result.ItemsPrecharged > 0, "V2 full-range charge must precharge items");
    }

    // Compare V2 btree charge vs V1 btree charge for the same data
    void CheckV2VsV1Charge(TTestParams params) {
        auto v1Eggs = MakePart(params);
        auto v2Eggs = MakeV2Part(params);

        const auto v1Part = *v1Eggs.Lone();
        const auto v2Part = *v2Eggs.Lone();

        // Verify both have same row count
        UNIT_ASSERT_VALUES_EQUAL_C(v1Part.Stat.Rows, v2Part.Stat.Rows,
            "V1 and V2 parts must have same row count");

        auto tags = TVector<TTag>();
        for (auto c : v1Eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }
        const auto& keyDefaults = *v1Eggs.Scheme->Keys.Get();

        TTestEnv v1Env, v2Env;
        TChargeBTreeIndex v1Charge(&v1Env, v1Part, tags, true);
        TChargeBTreeIndex v2Charge(&v2Env, v2Part, tags, true);

        const auto& v1Meta = v1Part.IndexPages.BTreeGroups[0];
        const auto& v2Meta = v2Part.IndexPages.BTreeGroups[0];

        // V1 and V2 must agree on meta-level stats
        UNIT_ASSERT_VALUES_EQUAL(v1Meta.GetRowCount(), v2Meta.GetRowCount());
        UNIT_ASSERT_VALUES_EQUAL(v1Meta.GetDataSize(), v2Meta.GetDataSize());

        // Full-range charge
        TRowId endRow = v1Meta.GetRowCount() - 1;
        auto v1Result = v1Charge.Do(TCells{}, TCells{}, 0, endRow, keyDefaults, 0, 0);
        auto v2Result = v2Charge.Do(TCells{}, TCells{}, 0, endRow, keyDefaults, 0, 0);

        UNIT_ASSERT_C(v1Result.Ready, "V1 full charge must be ready");
        UNIT_ASSERT_C(v2Result.Ready, "V2 full charge must be ready");
        UNIT_ASSERT_VALUES_EQUAL_C(v1Result.ItemsPrecharged, v2Result.ItemsPrecharged,
            "V1 and V2 must agree on full-range items precharged");
        // V2 index pages are larger (byte-offset children), so V2 bytes must be
        // at least V1 bytes. Both must be non-zero.
        UNIT_ASSERT_C(v1Result.BytesPrecharged > 0, "V1 full charge must precharge bytes");
        UNIT_ASSERT_C(v2Result.BytesPrecharged > 0, "V2 full charge must precharge bytes");
        UNIT_ASSERT_GE_C(v2Result.BytesPrecharged, v1Result.BytesPrecharged,
            "V2 must precharge at least as many bytes as V1 (V2 index pages are larger): "
            << v2Result.BytesPrecharged << " vs " << v1Result.BytesPrecharged);
    }

    // Check charge with items limit produces consistent results
    void CheckV2ChargeWithLimit(TTestParams params) {
        auto eggs = MakeV2Part(params);
        const auto part = *eggs.Lone();

        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }
        const auto& keyDefaults = *eggs.Scheme->Keys.Get();
        const auto& meta = part.IndexPages.BTreeGroups[0];

        TTestEnv env;
        TChargeBTreeIndex charge(&env, part, tags, true);

        // Unlimited baseline — the limit variant must not precharge MORE items.
        auto unlimited = charge.Do(TCells{}, TCells{}, 0, meta.GetRowCount() - 1,
            keyDefaults, 0, 0);
        UNIT_ASSERT_C(unlimited.Ready, "V2 unlimited charge must be ready");

        auto limited = charge.Do(TCells{}, TCells{}, 0, meta.GetRowCount() - 1,
            keyDefaults, 5, 0);
        UNIT_ASSERT_C(limited.Ready, "V2 limited charge must be ready");
        UNIT_ASSERT_GE_C(limited.ItemsPrecharged, 5,
            "V2 full-range charge with limit 5 must precharge at least 5 items");
        UNIT_ASSERT_LE_C(limited.ItemsPrecharged, unlimited.ItemsPrecharged,
            "V2 limited charge must not precharge more items than unlimited ("
            << limited.ItemsPrecharged << " vs " << unlimited.ItemsPrecharged << ")");
    }

    Y_UNIT_TEST(NoNodes_V2) {
        CheckV2ChargeBasic({.Levels = 0});
        CheckV2VsV1Charge({.Levels = 0});
    }

    Y_UNIT_TEST(OneNode_V2) {
        CheckV2ChargeBasic({.Levels = 1});
        CheckV2VsV1Charge({.Levels = 1});
    }

    Y_UNIT_TEST(FewNodes_V2) {
        CheckV2ChargeBasic({.Levels = 3});
        CheckV2VsV1Charge({.Levels = 3});
    }

    Y_UNIT_TEST(NoNodes_Groups_V2) {
        CheckV2ChargeBasic({.Levels = 0, .Groups = true});
        CheckV2VsV1Charge({.Levels = 0, .Groups = true});
    }

    Y_UNIT_TEST(NoNodes_History_V2) {
        CheckV2ChargeBasic({.Levels = 0, .History = true});
        CheckV2VsV1Charge({.Levels = 0, .History = true});
    }

    Y_UNIT_TEST(OneNode_History_V2) {
        CheckV2ChargeBasic({.Levels = 1, .History = true});
        CheckV2VsV1Charge({.Levels = 1, .History = true});
    }

    Y_UNIT_TEST(FewNodes_Groups_History_V2) {
        CheckV2ChargeBasic({.Levels = 3, .Groups = true, .History = true});
        CheckV2VsV1Charge({.Levels = 3, .Groups = true, .History = true});
    }

    Y_UNIT_TEST(ChargeWithLimit_V2) {
        CheckV2ChargeWithLimit({.Levels = 3});
        CheckV2ChargeWithLimit({.Levels = 1});
    }

    Y_UNIT_TEST(NoNodes_Groups_History_V2) {
        CheckV2ChargeBasic({.Levels = 0, .Groups = true, .History = true});
        CheckV2VsV1Charge({.Levels = 0, .Groups = true, .History = true});
    }

    Y_UNIT_TEST(OneNode_Groups_V2) {
        CheckV2ChargeBasic({.Levels = 1, .Groups = true});
        CheckV2VsV1Charge({.Levels = 1, .Groups = true});
    }

    Y_UNIT_TEST(OneNode_Groups_History_V2) {
        CheckV2ChargeBasic({.Levels = 1, .Groups = true, .History = true});
        CheckV2VsV1Charge({.Levels = 1, .Groups = true, .History = true});
    }

    Y_UNIT_TEST(FewNodes_Groups_V2) {
        CheckV2ChargeBasic({.Levels = 3, .Groups = true});
        CheckV2VsV1Charge({.Levels = 3, .Groups = true});
    }

    Y_UNIT_TEST(FewNodes_History_V2) {
        CheckV2ChargeBasic({.Levels = 3, .History = true});
        CheckV2VsV1Charge({.Levels = 3, .History = true});
    }

    // -- V2 resume twins -----------------------------------------------------
    // Drive a V2 part through a miss -> load -> re-Do() resume loop on an
    // offset-keyed, Type-aware env (TTouchEnvV2). This is the production
    // executor reschedule/resume flow that the V1/Type-blind TTouchEnv cannot
    // exercise. The env's GetPageType check fails on the first miss if the
    // charger mis-tags the deepest index level as EPage::DataPage.

    ui32 GetV2FailsAllowed(TTestParams params) {
        ui32 result = (params.Levels + 1) * 2;
        if (params.History) result *= 2;
        if (params.Groups) result *= 2;
        return result;
    }

    ICharge::TResult DoV2Resume(TPartEggs& eggs, TTestParams params, bool reverse,
            bool useKeys, ui64 itemsLimit, ui64 bytesLimit, const TString& message) {
        const auto part = *eggs.Lone();
        auto tags = TVector<TTag>();
        for (auto c : eggs.Scheme->Cols) {
            tags.push_back(c.Tag);
        }
        const auto& keyDefaults = *eggs.Scheme->Keys.Get();

        TTouchEnvV2 env(&part);
        TChargeBTreeIndex charge(&env, part, tags, true);

        // A couple of in-range keys to drive the key1/key2 path (which hits the
        // key-path BuildPageRef(remLevel<=0) that must stay correct, plus the
        // data-page key-resolution branch). For row-id charge pass empty keys.
        TCell k1c[2] = { TCell::Make<ui32>(0), TCell::Make<ui32>(3) };
        TCell k2c[2] = { TCell::Make<ui32>(5), TCell::Make<ui32>(8) };
        TCells key1{k1c, 2};
        TCells key2{k2c, 2};
        const TRowId lastRow = part.Stat.Rows - 1;

        ICharge::TResult result;
        const ui32 failsAllowed = GetV2FailsAllowed(params);
        for (ui32 attempt = 0; attempt <= failsAllowed; attempt++) {
            env.LoadTouched();
            result = (useKeys
                ? (reverse
                    ? charge.DoReverse(key1, key2, lastRow, 0, keyDefaults, itemsLimit, bytesLimit)
                    : charge.Do(key1, key2, 0, lastRow, keyDefaults, itemsLimit, bytesLimit))
                : (reverse
                    ? charge.DoReverse(TCells{}, TCells{}, lastRow, 0, keyDefaults, itemsLimit, bytesLimit)
                    : charge.Do(TCells{}, TCells{}, 0, lastRow, keyDefaults, itemsLimit, bytesLimit)));
            if (result.Ready) {
                break;
            }
        }
        UNIT_ASSERT_C(result.Ready,
            "V2 resume did not become ready: " << message);
        UNIT_ASSERT_C(env.TouchedEmpty(),
            "V2 resume left unresolved misses: " << message);
        return result;
    }

    void CheckV2Resume(TTestParams params) {
        TPartEggs eggs = MakeV2Part(params);
        const TString base = TStringBuilder()
            << "Levels=" << params.Levels
            << " Groups=" << params.Groups
            << " History=" << params.History;
        // Primary reproducer: forward row-id charge on a multi-level tree.
        DoV2Resume(eggs, params, /*reverse=*/false, /*useKeys=*/false, 0, 0,
            "row-id fwd " + base);
        // Reverse row-id charge (DoReverse iterateLevel index-walk).
        DoV2Resume(eggs, params, /*reverse=*/true, /*useKeys=*/false, 0, 0,
            "row-id rev " + base);
        // Keys charge: exercises the key1/key2 path + data-page resolution.
        DoV2Resume(eggs, params, /*reverse=*/false, /*useKeys=*/true, 0, 0,
            "keys fwd " + base);
        // Keys reverse: key-walk in reverse with key-based range boundaries.
        DoV2Resume(eggs, params, /*reverse=*/true, /*useKeys=*/true, 0, 0,
            "keys rev " + base);
        // Items limit on the row-id path.
        DoV2Resume(eggs, params, /*reverse=*/false, /*useKeys=*/false, 5, 0,
            "row-id fwd limit " + base);
        // Bytes limit on the row-id path.
        DoV2Resume(eggs, params, /*reverse=*/false, /*useKeys=*/false, 0, 500,
            "row-id fwd bytes-limit " + base);
    }

    Y_UNIT_TEST(NoNodes_V2_Resume) {
        // LevelCount == 0: root is a single data page; the index loop never
        // runs, so the mis-tagging path is unreachable. Guards small-tree fix.
        CheckV2Resume({.Levels = 0});
    }

    Y_UNIT_TEST(OneNode_V2_Resume) {
        // LevelCount == 1: root is the only index level, children are data.
        // iterateLevel's index-walk path (isLeafLevel=false) is never taken;
        // the post-loop data pass tags children DataPage. Guards small-tree fix.
        CheckV2Resume({.Levels = 1});
    }

    Y_UNIT_TEST(FewNodes_V2_Resume) {
        // PRIMARY REPRODUCER. LevelCount == 3: root -> index -> index -> data.
        // The index-walk iterateLevel builds the deepest index level's children
        // (still index nodes). With the bug they are tagged EPage::DataPage;
        // TTouchEnvV2's GetPageType check fails on the first miss.
        CheckV2Resume({.Levels = 3});
    }

    Y_UNIT_TEST(FewNodes_Groups_V2_Resume) {
        // Column groups: DoGroup()/DoGroupReverse() index-walk path, room-keyed.
        CheckV2Resume({.Levels = 3, .Groups = true});
    }

    Y_UNIT_TEST(FewNodes_History_V2_Resume) {
        // History: DoHistory() index-walk path.
        CheckV2Resume({.Levels = 3, .History = true});
    }

    Y_UNIT_TEST(FewNodes_Groups_History_V2_Resume) {
        // Groups + history: DoHistoricGroups -> DoGroup(true) index-walk.
        CheckV2Resume({.Levels = 3, .Groups = true, .History = true});
    }

    Y_UNIT_TEST(ChargeWithLimit_V2_Resume) {
        // Sweep varying items limits across forward/reverse/keys paths on a
        // multi-level tree. Other resume tests (FewNodes_V2_Resume etc.) run
        // CheckV2Resume which includes a single limit=5 fwd-only variant; here
        // we verify that a range of limits all produce Ready charges (limit
        // does not break the resume loop) and that each limited charge covers
        // no more items than the unlimited baseline.
        TTestParams params{.Levels = 3};
        TPartEggs eggs = MakeV2Part(params);

        const auto unlimited = DoV2Resume(eggs, params, /*reverse=*/false,
            /*useKeys=*/false, 0, 0, "unlimited baseline");

        for (ui64 limit : {1, 3, 5, 10, 100}) {
            // Row-id forward.
            auto r = DoV2Resume(eggs, params, /*reverse=*/false,
                /*useKeys=*/false, limit, 0,
                "row-id fwd limit=" + ToString(limit));
            UNIT_ASSERT_C(r.ItemsPrecharged > 0,
                "row-id fwd limit=" << limit << " must precharge items");
            UNIT_ASSERT_LE_C(r.ItemsPrecharged, unlimited.ItemsPrecharged,
                "row-id fwd limit=" << limit
                << " must not precharge more items than unlimited");

            // Row-id reverse.
            r = DoV2Resume(eggs, params, /*reverse=*/true,
                /*useKeys=*/false, limit, 0,
                "row-id rev limit=" + ToString(limit));
            UNIT_ASSERT_C(r.ItemsPrecharged > 0,
                "row-id rev limit=" << limit << " must precharge items");
            UNIT_ASSERT_LE_C(r.ItemsPrecharged, unlimited.ItemsPrecharged,
                "row-id rev limit=" << limit
                << " must not precharge more items than unlimited");

            // Keys path: the (0, 3)..(5, 8) key range spans nearly all 40 rows,
            // so every tested limit fits within it and precharges items.
            r = DoV2Resume(eggs, params, /*reverse=*/false,
                /*useKeys=*/true, limit, 0,
                "keys fwd limit=" + ToString(limit));
            UNIT_ASSERT_C(r.ItemsPrecharged > 0,
                "keys fwd limit=" << limit << " must precharge items");
            UNIT_ASSERT_LE_C(r.ItemsPrecharged, unlimited.ItemsPrecharged,
                "keys fwd limit=" << limit
                << " must not precharge more items than unlimited");
        }
    }

}

// ========================================================================
// V2 reverse-iteration twin for the raw group index
// ========================================================================

Y_UNIT_TEST_SUITE_F(TPartGroupBtreeIndexIterReverseV2, ::NKikimr::NTable::NTest::TTableCacheTest) {
    using namespace NTest;

    // Reverse iteration
    void CheckV2IterationReverse(const TPartEggs& v1Eggs, const TPartEggs& v2Eggs) {
        const auto v1Part = *v1Eggs.Lone();
        const auto v2Part = *v2Eggs.Lone();

        TTestEnv v1Env, v2Env;
        auto v1Index = CreateIndexIter(&v1Part, &v1Env, {});
        auto v2Index = CreateIndexIter(&v2Part, &v2Env, {});

        // Seek to last row
        auto v1Ready = v1Index->Seek(v1Part.Stat.Rows - 1);
        auto v2Ready = v2Index->Seek(v2Part.Stat.Rows - 1);
        UNIT_ASSERT_VALUES_EQUAL(v1Ready, v2Ready);

        for (size_t i = 0; ; i++) {
            if (v1Ready != EReady::Data) break;
            UNIT_ASSERT_VALUES_EQUAL(v1Index->GetRowId(), v2Index->GetRowId());
            v1Ready = v1Index->Prev();
            v2Ready = v2Index->Prev();
            UNIT_ASSERT_VALUES_EQUAL_C(v1Ready, v2Ready, "V2 prev differs at step " + ToString(i));
        }
    }

    void CheckPart(TTestParams params) {
        auto v1Eggs = MakePart(params);
        auto v2Eggs = MakeV2Part(params);

        CheckV2IterationReverse(v1Eggs, v2Eggs);
    }

    Y_UNIT_TEST(NoNodes_V2) {
        CheckPart({.Levels = 0});
    }

    Y_UNIT_TEST(OneNode_V2) {
        CheckPart({.Levels = 1});
    }

    Y_UNIT_TEST(FewNodes_V2) {
        CheckPart({.Levels = 3});
    }
}

}
