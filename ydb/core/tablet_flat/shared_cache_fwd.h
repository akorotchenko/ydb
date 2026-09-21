#pragma once

namespace NKikimr::NSharedCache {

struct TProdTraits;

template <class TTraits>
class TSharedCachePageRefImpl;
template <class TTraits>
class TPageFetchTokenImpl;

using TSharedCachePageRef = TSharedCachePageRefImpl<TProdTraits>;
using TPageFetchToken = TPageFetchTokenImpl<TProdTraits>;

} // namespace NKikimr::NSharedCache
