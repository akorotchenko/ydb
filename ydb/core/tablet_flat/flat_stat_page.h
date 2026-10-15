#pragma once

#include "flat_page_btree_index.h"

#include <util/generic/ptr.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NTable::NStats {

// Borrows references from traversal owners; registering a key view never acquires a cache pin.
class TPageTracker {
public:
    TPageTracker() = default;
    TPageTracker(const TPageTracker&) = delete;
    TPageTracker& operator=(const TPageTracker&) = delete;

    ~TPageTracker() {
        Y_DEBUG_ABORT_UNLESS(Empty());
    }

    struct TWorkingSet {
        ui64 Bytes = 0; // Non-Sticky pages released when the traversal unwinds.
        ui64 TotalBytes = 0;
        ui64 Pages = 0;
    };

    class TRegistration {
    public:
        TRegistration(TPageTracker* tracker, const TSharedCachePageRef& page)
            : Tracker_(page ? tracker : nullptr)
            , Page_(&page)
        {
            if (Tracker_) {
                Slot_ = Tracker_->Pages_.size();
                Tracker_->Pages_.push_back(this);
            }
        }

        TRegistration(const TRegistration&) = delete;
        TRegistration& operator=(const TRegistration&) = delete;

        ~TRegistration() {
            if (Tracker_) {
                auto& pages = Tracker_->Pages_;
                pages[Slot_] = pages.back();
                pages[Slot_]->Slot_ = Slot_;
                pages.pop_back();
            }
        }

    private:
        friend class TPageTracker;

        TPageTracker* const Tracker_;
        const TSharedCachePageRef* const Page_;
        size_t Slot_ = 0;
    };

    TWorkingSet WorkingSet() const {
        TWorkingSet result;
        THashSet<ui64> seen;
        seen.reserve(Pages_.size());
        for (const auto* registration : Pages_) {
            const auto& page = *registration->Page_;
            if (seen.insert(page.CacheItem().CacheItem().Raw()).second) {
                const ui64 bytes = page.size() + TSharedData::OverheadSize;
                ++result.Pages;
                result.TotalBytes += bytes;
                if (!page.IsSticky()) {
                    result.Bytes += bytes;
                }
            }
        }
        return result;
    }

    bool Empty() const noexcept {
        return Pages_.empty();
    }

private:
    TVector<TRegistration*> Pages_;
};

// Traversal-owned references are serialized, including across coroutine worker changes.
struct TIndexPage final : TRefCounted<TIndexPage, TExplicitSimpleCounter> {
    explicit TIndexPage(TSharedCachePageRef page, bool v2Format, TPageTracker* tracker = nullptr)
        : Node(std::move(page), v2Format)
        , Registration(tracker, Node.GetPageRef())
    {}

    const NPage::TBtreeIndexNode Node;

private:
    // Destroy the registration before Node releases its native reference.
    TPageTracker::TRegistration Registration;
};

// Key bytes stay in the page. Views share its traversal-local owner, not cache references.
struct TKeyView : NPage::TBtreeIndexNode::TCellsIterable {
    using TCellsIterable = NPage::TBtreeIndexNode::TCellsIterable;

    TKeyView()
        : TCellsIterable(static_cast<const char*>(nullptr), NPage::TBtreeIndexNode::TColumns())
    {}

    TKeyView(TCellsIterable cells, TIntrusivePtr<TIndexPage> owner)
        : TCellsIterable(cells)
        , Owner(std::move(owner))
    {}

    TKeyView(const TKeyView&) noexcept = default;
    TKeyView& operator=(const TKeyView&) noexcept = default;
    TKeyView(TKeyView&&) noexcept = default;
    TKeyView& operator=(TKeyView&&) noexcept = default;

private:
    TIntrusivePtr<TIndexPage> Owner;
};

} // namespace NKikimr::NTable::NStats
