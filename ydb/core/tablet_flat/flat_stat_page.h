#pragma once

#include "flat_page_btree_index.h"

#include <util/generic/ptr.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NTable::NStats {

// Tracks owners transferred to traversal nodes; key views share those nodes
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
        TRegistration(TPageTracker* tracker, TCachePageRef page)
            : Page_(std::move(page))
            , Tracker_(Page_ ? tracker : nullptr)
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

        const TCachePageRef& Page() const noexcept {
            return Page_;
        }

    private:
        friend class TPageTracker;

        TCachePageRef Page_;
        TPageTracker* const Tracker_;
        size_t Slot_ = 0;
    };

    TWorkingSet WorkingSet() const {
        TWorkingSet result;
        THashSet<ui64> seen;
        seen.reserve(Pages_.size());
        for (const auto* registration : Pages_) {
            const auto& page = registration->Page_;
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

    void CollectPageLocations(THashMap<TLogoBlobID, TVector<NPage::TPageLocation>>& locations) const {
        for (const auto* registration : Pages_) {
            const auto& page = registration->Page_;
            locations[page.GetCollectionId()].push_back(page.GetLocation());
        }
    }

    const TSharedData* ProvidePage(TCachePageRef page) noexcept {
        PendingPage_ = std::move(page);
        return PendingPage_ ? &PendingPage_.GetData() : nullptr;
    }

    TCachePageRef TakePage(const TSharedData* bytes) noexcept {
        Y_DEBUG_ABORT_UNLESS(
            !PendingPage_ || (PendingPage_.data() == bytes->data() && PendingPage_.size() == bytes->size()));
        return std::move(PendingPage_);
    }

    void ClearPendingPage() noexcept {
        PendingPage_.Drop();
    }

    bool Empty() const noexcept {
        return Pages_.empty();
    }

private:
    TVector<TRegistration*> Pages_;
    TCachePageRef PendingPage_;
};

// Traversal-owned references are serialized, including across coroutine worker changes.
struct TIndexPage final : TRefCounted<TIndexPage, TExplicitSimpleCounter> {
    explicit TIndexPage(TCachePageRef page, bool v2Format, TPageTracker* tracker = nullptr)
        : Node(page.GetData(), v2Format)
        , Registration(tracker, std::move(page))
    {}

    explicit TIndexPage(const TSharedData* page, bool v2Format, TPageTracker* tracker = nullptr)
        : Node(*page, v2Format)
        , Registration(tracker, tracker ? tracker->TakePage(page) : TCachePageRef())
    {}

    const TCachePageRef& GetPageRef() const noexcept {
        return Registration.Page();
    }

    const NPage::TBtreeIndexNode Node;

private:
    // Unregister and release the owner before destroying the byte parser.
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
