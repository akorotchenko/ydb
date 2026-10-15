#include "flat_stat_part_group_iter_iface.h"
#include "flat_stat_part_group_btree_index.h"
#include "flat_part_index_iter_flat_index.h"

#include <optional>

namespace NKikimr::NTable {

namespace {
    class TStatsPartGroupFlatIndexIter : public TPartGroupFlatIndexIter {
    public:
        TStatsPartGroupFlatIndexIter(
            const TPart* part, IPages* env, NPage::TGroupId groupId, NStats::TPageTracker* pageTracker)
            : TPartGroupFlatIndexIter(part, env, groupId)
            , PageTracker(pageTracker)
        {}

        EReady Start() override {
            const auto ready = TPartGroupFlatIndexIter::Start();
            if (ready != EReady::Page && PageTracker && !Registration) {
                Registration.emplace(PageTracker, TryLoadRaw()->GetPageRef());
            }
            return ready;
        }

    private:
        NStats::TPageTracker* const PageTracker;
        // Removed before the base iterator destroys the index parser.
        std::optional<NStats::TPageTracker::TRegistration> Registration;
    };
} // namespace

THolder<IStatsPartGroupIter> CreateStatsPartGroupIterator(const TPart* part, IPages* env, NPage::TGroupId groupId,
    ui64 rowCountResolution, ui64 dataSizeResolution, const TVector<TRowId>& splitPoints,
    NStats::TPageTracker* pageTracker)
{
    if (groupId.Index < (groupId.IsHistoric() ? part->IndexPages.BTreeHistoric : part->IndexPages.BTreeGroups).size()) {
        return MakeHolder<TStatsPartGroupBtreeIndexIter>(
            part, env, groupId, rowCountResolution, dataSizeResolution, splitPoints, pageTracker);
    } else {
        return MakeHolder<TStatsPartGroupFlatIndexIter>(part, env, groupId, pageTracker);
    }
}
}
