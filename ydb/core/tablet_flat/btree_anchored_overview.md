# B-tree anchored page collection — overview

Branch: `BTreeAncoredPageCollection`

---

## Problem statement

Every page in a tablet_flat SST (sorted string table) is identified by a `TPageId` — a dense `ui32` index into the page collection's metadata array.  This works well for the handful of structural pages (scheme, bloom, frames, …) that are loaded once at part-open time, but it creates a fundamental coupling problem for data pages and b-tree index nodes:

1. **Cache keyed on the wrong identity.**  The shared page cache stores and looks up pages by `(collection-label, TPageId)`.  `TPageId` is meaningful only within one specific page collection; it carries no information about where the page actually lives in blobstorage.  This prevents the cache from being byte-addressed and makes it impossible to share or deduplicate pages across format generations.

2. **B-tree leaf pointers require a collection lookup on the read path.**  A b-tree `TChild` node stores the `TPageId` of its data-page child.  To fetch that page the reader must call back into `IPageCollection::Bounds(pageId)` to learn the blob offset and size — an unnecessary indirection that ties every data-page access to the structural metadata layer.

3. **Scan read-ahead carries pageIds through every layer.**  The forward cache, bio actor, and shared cache all pass `TVector<TPageId>` for prefetch requests.  Each layer must therefore keep a reference to `IPageCollection` just to resolve sizes and blob positions.

4. **`TPageId` space is shared between structurally unrelated pages.**  Scheme, bloom, b-tree internal nodes, and data pages are all numbered in one flat sequence.  There is no type safety at interface boundaries; any `ui32` can be passed as any page.

5. **Flat index is still present** despite being superseded by the b-tree index and past its support lifetime, adding dead weight to every read path.

---

## Current layout

### On-disk: page collection (TMeta)

A page collection is a sequence of blobs in blobstorage.  Its metadata blob has the following binary layout:

```
┌─────────────────────────────────────────────┐
│ THeader  { Magic, Blobs, Pages }            │
├─────────────────────────────────────────────┤
│ TBlobId[Blobs]   (TLogoBlobID per blob)     │
├─────────────────────────────────────────────┤
│ TEntry[Pages]    { Page: end-offset ui64,   │
│                    Inplace: end-offset ui64 }│  ← page boundaries
├─────────────────────────────────────────────┤
│ TExtra[Pages]    { Type: ui32, Crc32: ui32 }│  ← type & checksum
├─────────────────────────────────────────────┤
│ Inbound data                                │
├─────────────────────────────────────────────┤
│ ui32 CRC                                    │
└─────────────────────────────────────────────┘
```

All page types — scheme, bloom, b-tree internal nodes, data pages — live in this single flat array indexed by `TPageId` (0-based position).

### On-disk: b-tree child node (TChild, v0)

Each b-tree internal node stores N keys and N+1 children.  A child entry today is:

```
┌───────────────────────────────────┐
│ PageId_        ui32   (4 bytes)   │  ← index into page collection
│ RowCount_      ui64   (8 bytes)   │
│ DataSize_      ui64   (8 bytes)   │
│ GroupDataSize_ ui64   (8 bytes)   │
│ ErasedRowCount_ ui64  (8 bytes)   │
└───────────────────────────────────┘
  total: 36 bytes
```

`PageId_` is a `ui32` that references either another b-tree internal node (non-leaf level) or a data page (leaf level) inside the same page collection.

### In-memory: read path (current)

```
  B-tree iterator
       │  child.PageId_  (ui32)
       │
       ▼
  IPages::TryGetPage(part, TPageId, TGroupId)
       │
       ▼
  TEnv / forward cache
       │  TPageId
       │
       ▼
  TEvRequest { PageCollection, TVector<TPageId> }
       │
       ▼
  private_sausagecache  (per-tablet)
       │  TPage { TPageId, Size, … }
       │  TPageCollection::PageMap   keyed by TPageId
       │  TPageCollection::StickyPages  keyed by TPageId
       │  GetPageType/GetPageSize call IPageCollection::Page(pageId)
       │
       ├─ cache hit → TSharedData
       │
       └─ shared_sausagecache  (process-wide)
               │  TPage { TPageId, Size, … }
               │  TCollection::PageMap  keyed by TPageId
               │
               ├─ cache hit → TSharedData
               │
               └─ cache miss → bio_actor
                      │  IPageCollection::Bounds(TPageId) → blob range
                      │
                      ▼
                 blobstorage
                      │
                      ▼
                 TLoadedPage { TPageId, TSharedData }
```

Every layer must carry `TPageId` and have access to `IPageCollection` to resolve sizes and blob positions.

---

## Target layout

### On-disk: b-tree child node (TChild, v1)

The leaf (and internal) child entry embeds the full page location inline:

```
┌────────────────────────────────────┐
│ Offset_        ui64   (8 bytes)    │  ← byte offset in page collection
│ Size_          ui32   (4 bytes)    │  ← page size in bytes
│ Crc32_         ui32   (4 bytes)    │  ← page checksum
│ RowCount_      ui64   (8 bytes)    │
│ DataSize_      ui64   (8 bytes)    │
│ GroupDataSize_ ui64   (8 bytes)    │
│ ErasedRowCount_ ui64  (8 bytes)    │
└────────────────────────────────────┘
  total: 44 bytes
```

Format version (v0 vs v1) is carried in `TBtreeIndexMeta.Version` in the part protobuf — absent/0 = v0, 1 = v1.  The `TMeta` binary layout on disk is unchanged.

For v0 parts, `TPageLocation` is derived on demand via `TMeta::GetLocation(PageId_)` — an O(1) read from the already-mapped `Steps` and `Extra` arrays, no allocation.

### New universal page identity

```
TPageLocation { TPageOffset offset;  // ui64 — byte position in page collection
                ui32        size;    // page size in bytes
                ui32        crc32; } // page checksum
```

`TPageOffset` alone is sufficient as a cache key (unique within a collection already partitioned by `TLogoBlobID`).

### In-memory: read path (target)

```
  B-tree iterator
       │  child.GetLocation()  →  TPageLocation   (O(1): inline v1, or TMeta lookup v0)
       │
       ▼
  IPages::TryGetPage(part, TPageLocation, TGroupId)
       │
       ▼
  TEnv / forward cache
       │  TPageLocation
       │
       ▼
  TEvRequest { IDataPageCollection, TVector<TPageLocation> }
       │
       ▼
  private_sausagecache  (per-tablet)
       │  TPage { TPageLocation, … }
       │  TPageCollection::PageMap   keyed by TPageOffset
       │  TPageCollection::StickyPages  keyed by TPageOffset
       │
       ├─ cache hit → TSharedData
       │
       └─ shared_sausagecache  (process-wide)
               │  TPage { TPageLocation, … }
               │  TCollection::PageMap  keyed by TPageOffset
               │
               ├─ cache hit → TSharedData
               │
               └─ cache miss → bio_actor
                      │  IDataPageCollection::Bounds(TPageLocation) → blob range
                      │
                      ▼
                 blobstorage
                      │
                      ▼
                 TLoadedPage { TPageLocation, TSharedData }
```

No layer below the b-tree iterator needs `IPageCollection` for data or b-tree pages.  `TPageId` does not appear at any interface boundary.

---

## Expected effect

| Concern | Before | After |
|---|---|---|
| Data-page identity at interfaces | `TPageId` (ui32, collection-relative) | `TPageLocation` (offset+size+crc32, self-contained) |
| Cache key | `TPageId` — opaque without `IPageCollection` | `TPageOffset` — directly comparable, no collection needed |
| B-tree leaf read | Requires `IPageCollection` lookup for size/blob range | Location embedded in `TChild`; no lookup |
| Scan read-ahead | Passes `TPageId` through all layers | Passes `TPageLocation` extracted directly from leaf nodes |
| Verification | Requires `IPageCollection::Verify(pageId, body)` | `IDataPageCollection::Verify(location, body)` — static, no instance |
| Flat index | Dead code on every read path | Removed entirely |
| Format flexibility | Single flat `TPageId` space for all page types | Structural pages keep `TPageId`; data/b-tree pages use byte-addressed `TPageLocation` |

---

## Migration steps (high level)

1. **New types** — add `TPageOffset`, `TPageLocation` to `flat_page_iface.h`.

2. **`IDataPageCollection`** — new interface for data and b-tree pages; implemented by `TMeta` sharing its blob array with `IPageCollection`.  Add `TMeta::GetLocation(TPageId)` as an O(1) helper for v0 compatibility.

3. **B-tree child format** — add `uint32 Version` to `TBtreeIndexMeta` proto; define v0 and v1 `TChild` / `TShortChild` layouts in C++.  Both formats remain readable and writable; a global switcher selects which format new compactions produce.

4. **`TLoadedPage`** — replace `TPageId` with `TPageLocation`.

5. **Private cache** — replace `TPageId` in `TPrivatePageCache::TPage`, `TPageCollection::PageMap`, `TPageCollection::StickyPages`, and all public methods with `TPageLocation` / `TPageOffset`; remove `GetPageType` / `GetPageSize` helpers that called back into `IPageCollection` for data pages.

6. **Shared cache** — replace `TPageId` in `TPage`, `TCollection::PageMap`, `PendingRequests`, `DroppedPages`, `TEvRequest`, `TEvResult` with `TPageLocation` / `TPageOffset`.

7. **`IPages`** — replace `TryGetPage(part, TPageId, TGroupId)` with `TryGetPage(part, TPageLocation, TGroupId)`; implement in `TEnv`, `TLoaderEnv`, test fakes.

8. **B-tree iterator** — migrate `TPartGroupBtreeIndexIter` to call `TryGetPage(part, child.GetLocation(), groupId)`.

9. **Forward cache** — change `IPageLoadingQueue::AddToQueue` and `IPageLoadingLogic::Get` to take `TPageLocation`; re-key `NFwd::TPage`, `TLoadedPagesCircularBuffer`, `TIndexPageLocator` to `TPageOffset`; wire scan read-ahead to extract locations from leaf nodes via `TChild::GetLocation()`.

10. **Bio actor** — switch to `IDataPageCollection::Bounds(location)` for I/O dispatch.

11. **Writer** — emit v1 `TChild` when switcher is on; keep v0 writer path fully functional when off.

12. **Remove flat index** — delete `TPartGroupFlatIndexIter`, `EPage::FlatIndex`, flat index writer, `TIndexPages::FlatGroups / FlatHistoric`, and all related call sites.
