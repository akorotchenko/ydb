#pragma once

namespace NKikimr::NSharedCache {

enum class ESharedCacheResultStatus {
    Miss,
    Inserted,
    Pending,
    Hit,
};

struct TProdTraits;
template <class TTraits>
class TSharedCacheImpl;
using TSharedCache = TSharedCacheImpl<TProdTraits>;

template <class TTraits>
class TSharedPageRefImpl;
template <class TTraits>
class TSharedCacheCollectionRefImpl;
template <class TTraits>
class TPageFetchImpl;

using TSharedPageRef = TSharedPageRefImpl<TProdTraits>;
using TSharedCacheCollectionRef = TSharedCacheCollectionRefImpl<TProdTraits>;
using TPageFetch = TPageFetchImpl<TProdTraits>;

} // namespace NKikimr::NSharedCache

namespace NKikimr {

using NSharedCache::TSharedCache;
using NSharedCache::TSharedCacheCollectionRef;
using NSharedCache::TSharedPageRef;

} // namespace NKikimr
