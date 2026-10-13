#pragma once

namespace NKikimr::NSharedCache {

struct TProdTraits;
class TSharedCachePages;

template <class TTraits>
class TSharedCachePageRefImpl;
template <class TTraits>
class TSharedCacheCollectionRefImpl;
template <class TTraits>
class TPageFetchImpl;

using TSharedCachePageRef = TSharedCachePageRefImpl<TProdTraits>;
using TSharedCacheCollectionRef = TSharedCacheCollectionRefImpl<TProdTraits>;
using TPageFetch = TPageFetchImpl<TProdTraits>;

} // namespace NKikimr::NSharedCache

namespace NKikimr {

using NSharedCache::TSharedCacheCollectionRef;
using NSharedCache::TSharedCachePageRef;
using NSharedCache::TSharedCachePages;

} // namespace NKikimr
