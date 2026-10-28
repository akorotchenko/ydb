#pragma once

namespace NKikimr::NSharedCache {

enum class ECacheResultStatus {
    Miss,
    Inserted,
    Pending,
    Hit,
};

struct TProdTraits;
template <class TTraits>
class TSharedCacheImpl;

template <class TTraits>
class TCacheOperation;
template <class TTraits>
class TCacheOperationView;
template <class TTraits>
class TCacheItemsOperation;
template <class TTraits>
class TCacheCollectionsOperation;
template <class TTraits>
class TCacheEvictionOperation;
template <class TTraits>
class TCacheFetchesOperation;
using TSharedCache = TSharedCacheImpl<TProdTraits>;

template <class TTraits>
class TCachePageRefImpl;
template <class TTraits>
class TCacheCollectionRefImpl;
template <class TTraits>
class TPageFetchImpl;

using TCachePageRef = TCachePageRefImpl<TProdTraits>;
using TCacheCollectionRef = TCacheCollectionRefImpl<TProdTraits>;
using TPageFetch = TPageFetchImpl<TProdTraits>;

} // namespace NKikimr::NSharedCache

namespace NKikimr {

using NSharedCache::TSharedCache;
using NSharedCache::TCacheCollectionRef;
using NSharedCache::TCachePageRef;

} // namespace NKikimr
