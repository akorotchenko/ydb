# B-tree anchored page collection — overview

Branch: `BTreeAncoredPageCollection`

---

## Problem statement

### Primary goal: reduce resident memory held by `TMeta`

Each SST carries a `TMeta` metadata blob that is mapped into memory when the part is opened and stays wired for the lifetime of the part.  `TMeta` holds two parallel arrays over all pages in the SST:

- `Index` (`TEntry[]`, 16 B/page) — page end-offsets used to compute blob address and page size
- `Extra` (`TExtra[]`, 8 B/page) — page type and crc32

With a 7 KiB target page size and a 200 MiB SST, a single SST has ~30,000 data pages.  Their combined metadata occupies ~0.69 MiB per SST, or **~69 GiB per node** (100,000 SSTs), all wired and non-swappable.

**Why is `TMeta` forced to stay resident?**  Because `TPageId` — the current page identity at every interface — is opaque: it carries no size, no offset, no checksum.  Every subsystem that touches a data page (private cache, shared cache, bio actor, forward cache) must resolve `TPageId → (blob, offset, size, crc32)` by calling back into `IPageCollection`, which reads directly from the `TMeta` arrays.  As long as any of these subsystems is active, `TMeta` cannot be unloaded.

**The fix.**  Replace `TPageId` at every subsystem interface with `TPageLocation { offset, size, crc32 }` — still collection-relative (the `offset` is into the collection's blob sequence), but it carries everything needed to *use* a page once the collection is known: size for accounting, crc32 for verification, offset for blob-range lookup.  Once `TPageLocation` flows through all layers, no subsystem below the b-tree iterator needs `IPageCollection` for data or b-tree pages.  In v1 parts the per-page metadata for data and b-tree pages is moved into the b-tree's own `TChild` entries, so the writer omits those entries from `TMeta` entirely — `TMeta::Raw` shrinks at the source to only structural-page entries, and the **~69 GiB per node** wired metadata becomes negligible.

### Secondary consequences

- **B-tree leaf pointers require a collection lookup today.**  A `TChild` stores `TPageId`; reaching the data page requires `IPageCollection::Bounds(pageId)`.  The new on-disk format embeds `TPageLocation` directly in `TChild`, eliminating the lookup entirely.

- **Scan read-ahead must carry `TPageId` through every layer.**  Each layer holds a reference to `IPageCollection` just to resolve page sizes and blob positions.  With `TPageLocation` at interfaces this coupling disappears.

- **`TPageId` space is shared between structurally unrelated pages.**  Scheme, bloom, b-tree internal nodes, and data pages share one flat `ui32` sequence with no type safety at interface boundaries.

- **Flat index is removed** as part of this work — it has been superseded by the b-tree index and is dead weight on every read path.

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

Format version (v0 vs v1) is carried in `TLayout.BTreeIndexesFormatVersion` (existing proto field, no new field added) — absent/0 = v0, 1 = v1.  The `TMeta` binary layout on disk is unchanged.

For v0 parts, `TPageLocation` is derived on demand via `TMeta::GetLocation(PageId_)` — an O(1) read from the already-mapped `Steps` and `Extra` arrays, no allocation.

### New universal page identity

```
TPageLocation { TPageOffset offset;  // ui64 — byte position in page collection
                ui32        size;    // page size in bytes
                ui32        crc32; } // page checksum
```

Page identity is still scoped to a collection — full identity is the pair `(TLogoBlobID, TPageOffset)`, same shape as the previous `(TLogoBlobID, TPageId)`.  The cache continues to partition by `TLogoBlobID`, so `TPageOffset` alone — just the `Offset` field — is the cache key within that scope.  `Size` and `Crc32` are *not* part of the key; they ride alongside it as fields of `TPageLocation`, the value passed into request APIs.  What changes is what subsystems need *from* the collection: previously `IPageCollection` was called per page read to translate `TPageId → (size, blob range, crc32)`; now the collection is consulted only once per I/O request, via `IDataPageCollection::Bounds(location) → blob range`.  Size and crc32 ride with the request, so verification and accounting need no collection access at all.

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

No layer below the b-tree iterator needs `IPageCollection` for data or b-tree pages — only `IDataPageCollection`, and only at I/O dispatch.  `TPageId` does not appear at any interface boundary.

---

## Expected effect

| Concern | Before | After |
|---|---|---|
| Data-page identity at interfaces | `TPageId` (ui32, collection-relative, opaque) | `TPageLocation` (offset+size+crc32, collection-relative but carries size & crc32) |
| Cache key | `TPageId` — opaque, requires `IPageCollection` per page read for size/blob range/crc32 | `TPageOffset` alone (within collection partitioned by `TLogoBlobID`).  Size and crc32 are *not* part of the key — they travel alongside it inside `TPageLocation`, the value passed into request APIs.  Collection consulted only for blob range via `IDataPageCollection::Bounds(location)`. |
| B-tree leaf read | Requires `IPageCollection` lookup for size/blob range | Location embedded in `TChild`; no lookup |
| Scan read-ahead | Passes `TPageId` through all layers | Passes `TPageLocation` extracted directly from leaf nodes |
| Verification | Requires `IPageCollection::Verify(pageId, body)` | `IDataPageCollection::Verify(location, body)` — static, no instance |
| Flat index | Dead code on every read path | Removed entirely |
| Format flexibility | Single flat `TPageId` space for all page types | Structural pages keep `TPageId`; data/b-tree pages use byte-addressed `TPageLocation` |

---

## Size impact estimates

### Assumptions

| Parameter | Value |
|---|---|
| SSTs per tablet | 10 |
| Tablet data size | 2 GiB |
| SST size | 2 GiB / 10 = **200 MiB** |
| Tablets per node | 10,000 |
| SSTs per node | 100,000 |
| Data page target size | 7 KiB (`flat_page_conf.h`) |
| B-tree node target size | 7 KiB, ~64 children/node |
| Data pages per SST | 200 MiB / 7 KiB ≈ **30,000** |
| B-tree internal nodes per SST | ~47 (3-level tree: 1 root + 8 L1 + 38 L2) |
| Total children per SST | ~30,050 |

### On-disk: `TMeta` blob (v0 baseline)

Each page occupies 24 bytes in the `TMeta` blob (`TEntry` = 16 B + `TExtra` = 8 B):

```
v0 TMeta blob per SST = 30,000 × 24 B ≈ 0.69 MiB
```

The on-disk binary *layout* (header, blob-id list, `TEntry[]`, `TExtra[]`, inplace data, CRC) is unchanged in v1 — no format version bump.  But the *blob size* shrinks dramatically in v1 because the writer no longer assigns `TPageId`s to data and b-tree pages, so their entries are omitted (see the next section).

### On-disk: `TChild` size change (v0 → v1)

`TChild` in v1 replaces `PageId_` (ui32, 4 B) with `Offset_` (ui64, 8 B) and adds `Size_` (ui32, 4 B) and `Crc32_` (ui32, 4 B):

```
v0 TChild: 36 bytes
v1 TChild: 44 bytes  (+8 bytes per child)
```

| Scope | v0 | v1 | Delta |
|---|---|---|---|
| Per SST | 30,050 × 36 B ≈ 1.03 MiB | 30,050 × 44 B ≈ 1.26 MiB | **+240 KiB** |
| Per tablet (10 SSTs) | ~10.3 MiB | ~12.6 MiB | **+2.4 MiB** |
| Per node (100,000 SSTs) | ~103 MiB | ~126 MiB | **+24 MiB** |

B-tree index pages are ~0.6% of total SST size, so the on-disk bloat is negligible relative to the 200 MiB SST body.

The +8 B/child cost is more than offset by the fact that the corresponding 24 B/page in `TMeta` (`TEntry` 16 B + `TExtra` 8 B) becomes redundant for data and b-tree pages — the same information now lives in `TChild`.  In v1 the writer no longer assigns `TPageId`s to data and b-tree pages at all: only structural pages (scheme, bloom, …) get `TPageId`s, and they form their own small contiguous 0-based sequence indexing `TMeta`'s `TEntry` / `TExtra` arrays.  The on-disk `TMeta` binary *layout* is unchanged (no format version bump), but the *blob shrinks* because it indexes only the handful of structural pages.

| Per SST | v0 | v1 |
|---|---|---|
| `TMeta` entries (data + b-tree + structural) | ~30,050 × 24 B ≈ 720 KiB | ~handful × 24 B (structural only) ≈ <1 KiB |
| `TChild` (b-tree internal nodes) | 30,050 × 36 B ≈ 1.03 MiB | 30,050 × 44 B ≈ 1.26 MiB |
| **Combined on-disk metadata** | **~1.75 MiB** | **~1.26 MiB** |

Net on-disk savings: ~480 KiB/SST, or **~48 GiB per node** at 100,000 SSTs.

### In-memory: `TMeta` resident memory

`TMeta` holds `Raw` (a `TSharedData` blob) with `Index` and `Extra` as direct pointers into it:

- `Index` (`TEntry[]`, 16 B/page) — page end-offsets, used for size and blob-range lookup
- `Extra` (`TExtra[]`, 8 B/page) — page type and crc32

```
Index per SST = 30,000 × 16 B = 0.46 MiB
Extra per SST = 30,000 ×  8 B = 0.23 MiB
─────────────────────────────────────────
TMeta resident per SST          = 0.69 MiB
```

| Scope | TMeta resident |
|---|---|
| Per SST | ~0.69 MiB |
| Per tablet (10 SSTs) | ~6.9 MiB |
| Per node (100,000 SSTs) | **~69 GiB** |

In v1 parts, `Index` (offset, size) and `Extra` (crc32) information is embedded directly in `TChild` nodes, and v1 writers omit the corresponding entries from `TMeta`.  `TMeta::Raw` for a v1 SST therefore holds only structural-page entries — a handful, not 30,000.  Once all SSTs on a node are recompacted to v1, the **~69 GiB per node** of resident `TMeta` shrinks to a negligible amount; no demand-loading / mmap mechanism is needed for `Raw` because there is nothing left to demand-load.

---

## Migration steps (high level)

1. **New types** — add `TPageOffset`, `TPageLocation` to `flat_page_iface.h`.

2. **`IDataPageCollection`** — new interface for data and b-tree pages; implemented by `TMeta` sharing its blob array with `IPageCollection`.  Add `TMeta::GetLocation(TPageId)` as an O(1) helper for v0 compatibility.

3. **B-tree child format** — use existing `TLayout.BTreeIndexesFormatVersion` (0 = v0, 1 = v1) as the discriminator; add root-location fields (`RootOffset`, `RootSize`, `RootCrc32`) to `TBtreeIndexMeta` for v1; define v0 and v1 `TChild` / `TShortChild` layouts in C++.  Both formats remain readable and writable; a global switcher selects which format new compactions produce.

4. **`TLoadedPage`** — replace `TPageId` with `TPageLocation`.

5. **Private cache** — replace `TPageId` in `TPrivatePageCache::TPage`, `TPageCollection::PageMap`, `TPageCollection::StickyPages`, and all public methods with `TPageLocation` / `TPageOffset`; remove `GetPageType` / `GetPageSize` helpers that called back into `IPageCollection` for data pages.

6. **Shared cache** — replace `TPageId` in `TPage`, `TCollection::PageMap`, `PendingRequests`, `DroppedPages`, `TEvRequest`, `TEvResult` with `TPageLocation` / `TPageOffset`.

7. **`IPages`** — replace `TryGetPage(part, TPageId, TGroupId)` with `TryGetPage(part, TPageLocation, TGroupId)`; implement in `TEnv`, `TLoaderEnv`, test fakes.

8. **B-tree iterator** — migrate `TPartGroupBtreeIndexIter` to call `TryGetPage(part, child.GetLocation(), groupId)`.

9. **Forward cache** — change `IPageLoadingQueue::AddToQueue` and `IPageLoadingLogic::Get` to take `TPageLocation`; re-key `NFwd::TPage`, `TLoadedPagesCircularBuffer`, `TIndexPageLocator` to `TPageOffset`; wire scan read-ahead to extract locations from leaf nodes via `TChild::GetLocation()`.

10. **Bio actor** — switch to `IDataPageCollection::Bounds(location)` for I/O dispatch.

11. **Writer** — emit v1 `TChild` when switcher is on; in v1, do not assign `TPageId`s to data and b-tree pages (only structural pages enter `TMeta`'s `TEntry`/`TExtra` arrays); emit root location into `TBtreeIndexMeta::RootOffset/Size/Crc32` instead of `RootPageId`.  Keep v0 writer path fully functional when off.

12. **Remove flat index** — delete `TPartGroupFlatIndexIter`, `EPage::FlatIndex`, flat index writer, `TIndexPages::FlatGroups / FlatHistoric`, and all related call sites.
