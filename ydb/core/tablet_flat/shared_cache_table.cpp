#include "shared_cache_table.h"
#include "shared_cache_traits.h"

#include <util/generic/bitops.h>

#include <new>

namespace NKikimr::NSharedCache {
namespace {

    Y_FORCE_INLINE ui64 HashCombine(ui64 hash, ui64 value) noexcept {
        hash ^= 0x9E3779B97F4A7C15ULL + value;
        hash ^= hash >> 30;
        hash *= 0xBF58476D1CE4E5B9ULL;
        hash ^= hash >> 27;
        hash *= 0x94D049BB133111EBULL;
        hash ^= hash >> 31;
        return hash;
    }

    Y_FORCE_INLINE ui32 FinishHash(ui64 hash) noexcept {
        return static_cast<ui32>(hash ^ (hash >> 32));
    }

    int CompareWords(const TSharedCacheKey& left, const TSharedCacheKey& right) noexcept {
        const ui32 wordCount = left.IsPageKind() ? 2 : 3;
        for (ui32 index = 0; index < wordCount; ++index) {
            if (left.Word(index) < right.Word(index)) {
                return -1;
            }
            if (left.Word(index) > right.Word(index)) {
                return 1;
            }
        }
        return 0;
    }

} // anonymous namespace

#define SHARED_CACHE_TEMPLATE template <class TTraits>
#define TSharedCache TSharedCacheImpl<TTraits>
#define TSharedCacheItemRef TSharedCacheItemRefImpl<TTraits>
#define TSharedCacheTable TSharedCacheTableImpl<TTraits>
#define TOperationItemRef TOperationItemRef<TTraits>

ui32 HashPageKey(TCollectionCacheItem collection, ui64 offset) noexcept {
    ui64 hash = HashCombine(offset, collection.Raw());
    return FinishHash(hash);
}

ui32 HashCollectionKey(const TLogoBlobID& id) noexcept {
    const ui64* words = id.GetRaw();
    ui64 hash = HashCombine(words[0], words[1]);
    return FinishHash(HashCombine(hash, words[2]));
}

ui32 TSharedCacheKey::Hash() const noexcept {
    return Kind_ == EItemKind::Page
               ? HashPageKey(TCollectionCacheItem::FromValidated(TCacheItem::FromRaw(Words_[0])), Words_[1])
               : HashCollectionKey(TLogoBlobID(Words_.data()));
}

int CompareSharedCacheKeys(
    const TSharedCacheKey& left, ui32 leftHash, const TSharedCacheKey& right, ui32 rightHash) noexcept {
    const ui32 leftOrder = ReverseBits(leftHash);
    const ui32 rightOrder = ReverseBits(rightHash);
    if (leftOrder < rightOrder) {
        return -1;
    }
    if (leftOrder > rightOrder) {
        return 1;
    }
    if (left.Kind() < right.Kind()) {
        return -1;
    }
    if (left.Kind() > right.Kind()) {
        return 1;
    }
    return CompareWords(left, right);
}

void StoreSharedCacheKey(THandle& handle, const TSharedCacheKey& key) noexcept {
    handle.Key0.store(key.Word(0), std::memory_order_relaxed);
    handle.Key1.store(key.Word(1), std::memory_order_relaxed);
    handle.Key2OrSize.store(key.Word(2), std::memory_order_relaxed);
}

TSharedCacheKey LoadSharedCacheKey(const THandle& handle, EItemKind kind) noexcept {
    const ui64 word0 = handle.Key0.load(std::memory_order_relaxed);
    const ui64 word1 = handle.Key1.load(std::memory_order_relaxed);
    const ui64 word2 = handle.Key2OrSize.load(std::memory_order_relaxed);
    return TSharedCacheKey::FromWords(kind, word0, word1, word2);
}

#undef TSharedCacheTable
#undef TSharedCacheItemRef
#undef TOperationItemRef
#undef TSharedCache
#undef SHARED_CACHE_TEMPLATE

} // namespace NKikimr::NSharedCache
