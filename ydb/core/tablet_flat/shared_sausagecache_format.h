#pragma once

#include "flat_sausage_fetch.h"
#include "shared_cache_event_format.h"

template <>
inline void Out<TVector<NKikimr::NTable::NPage::TPageOffset>>(
    IOutputStream& o, const TVector<NKikimr::NTable::NPage::TPageOffset>& vec) {
    o << "[ ";
    for (const auto& x : vec) {
        o << x << ' ';
    }
    o << "]";
}

template <>
inline void Out<TVector<NKikimr::NPageCollection::TLoadedPageData>>(
    IOutputStream& o, const TVector<NKikimr::NPageCollection::TLoadedPageData>& vec) {
    o << "[ ";
    for (const auto& x : vec) {
        o << x.Offset << ' ';
    }
    o << "]";
}
