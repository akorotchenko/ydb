#include "flat_part_charge_create.h"
#include "flat_part_charge_btree_index.h"
#include "flat_part_charge_flat_index.h"

namespace NKikimr::NTable {

namespace {
    template <class TInputRef>
    THolder<ICharge> CreateTypedCharge(TPages<TInputRef>* env, const TPart& part, TTagsRef tags, bool includeHistory) {
        if (part.IndexPages.HasBTree()) {
            return MakeHolder<TChargeBTreeIndexImpl<TInputRef>>(env, part, tags, includeHistory);
        } else {
            return MakeHolder<TChargeFlatIndexImpl<TInputRef>>(env, part, tags, includeHistory);
        }
    }
} // namespace

THolder<ICharge> CreateCharge(IPages* env, const TPart& part, TTagsRef tags, bool includeHistory) {
    return CreateTypedCharge(env, part, tags, includeHistory);
}

THolder<ICharge> CreateCharge(IBorrowedPages* env, const TPart& part, TTagsRef tags, bool includeHistory) {
    return CreateTypedCharge(env, part, tags, includeHistory);
}

}
