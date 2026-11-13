#pragma once

#include "flat_part_charge_iface.h"
#include "flat_part_iface.h"

namespace NKikimr::NTable {

THolder<ICharge> CreateCharge(IPages *env, const TPart &part, TTagsRef tags, bool includeHistory);
THolder<ICharge> CreateCharge(IBorrowedPages* env, const TPart& part, TTagsRef tags, bool includeHistory);

}
