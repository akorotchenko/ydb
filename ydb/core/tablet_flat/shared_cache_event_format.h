#pragma once

#include "flat_bio_eggs.h"
#include "flat_page_iface.h"

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

template <>
inline void Out<NKikimr::NTable::NPage::TPageLocation>(
    IOutputStream& o, const NKikimr::NTable::NPage::TPageLocation& val) {
    val.Describe(o);
}

template <>
inline void Out<NKikimr::NTable::NPage::TPageOffset>(IOutputStream& o, const NKikimr::NTable::NPage::TPageOffset& val) {
    val.Describe(o);
}

template <>
inline void Out<TVector<NKikimr::NTable::NPage::TPageLocation>>(
    IOutputStream& o, const TVector<NKikimr::NTable::NPage::TPageLocation>& vec) {
    o << "[ ";
    for (const auto& x : vec) {
        o << x << ' ';
    }
    o << "]";
}

template <>
inline void Out<THashSet<NKikimr::NTable::NPage::TPageOffset>>(
    IOutputStream& o, const THashSet<NKikimr::NTable::NPage::TPageOffset>& set) {
    o << "[ ";
    for (const auto& x : set) {
        o << x << ' ';
    }
    o << "]";
}

template <>
inline void Out<NKikimr::NTabletFlatExecutor::NBlockIO::EPriority>(
    IOutputStream& o, NKikimr::NTabletFlatExecutor::NBlockIO::EPriority value)
{
    switch (value) {
        case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Fast:
            o << "Online";
            break;
        case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Bkgr:
            o << "AsyncLoad";
            break;
        case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Bulk:
            o << "Scan";
            break;
        default:
            o << static_cast<ui32>(value);
            break;
    }
}
