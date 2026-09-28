#pragma once

namespace NKikimr::NSharedCache {

struct TProdTraits;

template <class TTraits>
class TSharedCachePageRefImpl;
template <class TTraits>
class TPageFetchImpl;

using TSharedCachePageRef = TSharedCachePageRefImpl<TProdTraits>;
using TPageFetch = TPageFetchImpl<TProdTraits>;

} // namespace NKikimr::NSharedCache
