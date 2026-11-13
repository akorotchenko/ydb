
#include "flat_part_index_iter_bree_index.h"
#include "flat_part_index_iter_flat_index.h"

namespace NKikimr::NTable {

namespace {

    template <class TInputRef>
    THolder<IPartGroupIndexIter> CreateTypedIndexIter(
        const TPart* part, TPages<TInputRef>* env, NPage::TGroupId groupId) {
        if (part->IndexPages.HasBTree()) {
            return MakeHolder<TPartGroupBtreeIndexIterImpl<TInputRef>>(part, env, groupId);
        } else {
            return MakeHolder<TPartGroupFlatIndexIterImpl<TInputRef>>(part, env, groupId);
        }
    }

} // namespace

THolder<IPartGroupIndexIter> CreateIndexIter(const TPart* part, IPages* env, NPage::TGroupId groupId) {
    return CreateTypedIndexIter(part, env, groupId);
}

THolder<IPartGroupIndexIter> CreateIndexIter(const TPart* part, IBorrowedPages* env, NPage::TGroupId groupId) {
    return CreateTypedIndexIter(part, env, groupId);
}

}
