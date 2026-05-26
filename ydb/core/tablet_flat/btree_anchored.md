# B-tree anchored page collection design

Branch: `BTreeAncoredPageCollection`

## Goal

Split tablet_flat's page-collection addressing into two distinct id types:

- `TPageId` (ui32) — confined to `IPageCollection` internals and the on-disk old-format `TChild` field. Never crosses an API boundary.
- `TPageLocation { TPageOffset offset; ui32 size; ui32 crc32; }` — universal identity at **all internal API boundaries** for every page type, for both old-format and new-format parts.

The on-disk format distinction is resolved at the single point where `TChild` is read. From that point on only `TPageLocation` / `TPageOffset` flows through every interface — IPages, shared cache, forward cache, bio — regardless of which on-disk format the part uses.

---

## Constraints

- Hot read path **O(1)**. No per-read scans or reverse maps.
- `TPageLocation` / `TPageOffset` is the universal identity at all internal API boundaries for both old-format and new-format parts.
- `TPageId` is confined to: `IPageCollection` implementation internals, `TMeta::GetLocation(pageId)` call sites, and the on-disk old-format `TChild` field. It never appears in IPages, shared cache, forward cache, or bio interfaces.
- No `offsetToPageId` map at any level.
- **Both on-disk formats are fully supported for reading and writing indefinitely.** A global write-time switcher selects which format new compactions produce. Switching back to the old format must produce correct old-format output.

---

## On-disk formats

### Old format (existing, readable and writable)

`TChild::PageId_` is `ui32`. When reading, `TMeta::GetLocation(PageId_)` is called at the `TChild` read site to produce a `TPageLocation`; from that point only `TPageLocation` flows upward. No format awareness leaks into any interface above `TChild`.

### New format (written when global switcher is on)

`TChild` embeds `TPageLocation` `(offset, size, crc32)` directly. `GetLocation()` returns it inline — no collection lookup.

The format discriminator is `TLayout.BTreeIndexesFormatVersion` (already exists in the proto): absent/0 = v0, 1 = v1.  No new version field is needed.  `TMeta` on-disk binary layout is untouched.  The reader checks `BTreeIndexesFormatVersion` and follows the appropriate child layout path.  The writer emits whichever format the global switcher selects; both v0 and v1 writers are kept and tested.

`TBtreeIndexMeta` is the b-tree analogue of `TMeta` — it is the authoritative descriptor for the entire b-tree index (root page reference, level count, row count, data size).

In v1, b-tree internal nodes and data pages are **not** assigned `TPageId`s.  They have no `TEntry` / `TExtra` entries in `TMeta`; they are addressable only by `TPageLocation`.  The `TMeta` array indexes only structural pages (scheme, bloom, …), which form their own small contiguous 0-based `TPageId` sub-sequence — option (b) of the page-id numbering choice.  This is what makes the on-disk `TMeta` shrink in v1 (see overview doc for sizing).

Consequences for `TBtreeIndexMeta`:
- In v0, `TBtreeIndexMeta.RootPageId` references the b-tree root by `TPageId`; the loader resolves it via `TMeta::GetLocation(RootPageId)` once at part-open time.
- In v1, `TBtreeIndexMeta` instead carries the root's `TPageLocation` directly — proto adds `uint64 RootOffset`, `uint32 RootSize`, `uint32 RootCrc32`.  `RootPageId` is unused / unset when `BTreeIndexesFormatVersion == 1`.  This avoids a special-case `TPageId` for the root, which would otherwise be the lone b-tree exception to the "no `TPageId` for b-tree / data pages" rule.

### Bootstrap chain: how `TPageLocation` addressing starts

The part's metadata is reached via `TMeta` (the on-disk page-collection metablob), but the b-tree itself is reached via `TPageLocation`.  These two addressing schemes meet at exactly one point: the scheme page's **inplace data**.

The sausage layer supports attaching a small `inplace` byte string to any page (`TWriter::AddInplace` → `TRecord::PushInplace` → `TMeta::GetPageInplaceData`).  The part writer uses this exactly once, at `flat_part_writer.h:621`:

```cpp
WriteInplace(Current.Scheme, MakeMetaBlob(last));
```

The inplace blob is the serialized `TRoot` proto — which contains `TLayout`, which contains `TBTreeIndexMeta` records.  So the read-time chain is:

```
TMeta entry for scheme page         ← TPageId-addressed (structural)
    → scheme page body + inplace
        → TRoot proto
            → TLayout
                → TBtreeIndexMeta { RootOffset, RootSize, RootCrc32 }   (v1)
                    → b-tree root page                ← TPageLocation
                        → b-tree internal TChild      ← TPageLocation
                            → data page               ← TPageLocation
```

`TPageId`-addressing is confined to the top three lines (structural pages, indexed in `TMeta`).  From `TBtreeIndexMeta` downward, addressing is `TPageLocation` end-to-end.  No b-tree or data page is ever indexed by a `TPageId` in v1.

This also constrains writer ordering: the scheme page is emitted *last*, after the b-tree is finalized, because `MakeMetaBlob` needs the root's offset/size/crc32 to populate `TBtreeIndexMeta`.  Current writer flow already does this (line 621 is at the end of part finalization), so no reordering is required.

---

## Two page-collection interfaces

### `IPageCollection` (unchanged, structural pages only)

```cpp
class IPageCollection {
    virtual const TLogoBlobID& Label() const noexcept = 0;
    virtual ui32    Total() const noexcept = 0;
    virtual TInfo   Page(ui32 pageId) const = 0;      // { Size, Type }
    virtual TBorder Bounds(ui32 pageId) const = 0;
    virtual TGlobId Glob(ui32 blob) const = 0;
    virtual bool    Verify(ui32 pageId, TArrayRef<const char>) const = 0;
    virtual size_t  BackingSize() const noexcept = 0;
};
```

Data pages and b-tree index nodes never call into this interface.

### `IDataPageCollection` (new, data pages and b-tree nodes)

Stores only the blob sequence. Has no knowledge of individual page sizes or types — those are carried in `TPageLocation`.

```cpp
class IDataPageCollection {
    virtual const TLogoBlobID& Label() const noexcept = 0;
    virtual TBorder Bounds(TPageLocation location) const = 0;  // uses offset+size to find blob+skip range
    virtual TGlobId Glob(ui32 blob) const = 0;
    virtual size_t  BackingSize() const noexcept = 0;

    static bool Verify(TPageLocation location, TArrayRef<const char> body) noexcept {
        return location.Size == body.size() && NPageCollection::Checksum(body) == location.Crc32;
    }
};
```

`Bounds(TPageLocation)` uses `location.Offset` and `location.Size` to find the blob+skip range. Called once per I/O request construction, not on the row-read hot path.

`Verify` is static — all required information (size, crc32) is carried in `TPageLocation` itself; no per-instance collection state is needed.

---

## New types

In `flat_page_iface.h` (or new `flat_page_location.h`):

```cpp
using TPageOffset = ui64;

struct TPageLocation {
    TPageOffset Offset = Max<TPageOffset>();
    ui32        Size   = 0;
    ui32        Crc32  = 0;

    explicit operator bool() const noexcept { return Offset != Max<TPageOffset>(); }
    bool operator==(const TPageLocation&) const noexcept = default;
};
```

---

## B-tree child format (`flat_page_btree_index.h`)

### Old format (version 0, existing layout, unchanged on disk)

```cpp
struct TChild {
    TPageId PageId_;           // ui32
    TRowId  RowCount_;
    ui64    DataSize_;
    ui64    GroupDataSize_;
    TRowId  ErasedRowCount_;
} Y_PACKED;
```

### New format (version 1)

```cpp
struct TChild {
    TPageOffset Offset_;       // ui64
    ui32        Size_;
    ui32        Crc32_;
    TRowId      RowCount_;
    ui64        DataSize_;
    ui64        GroupDataSize_;
    TRowId      ErasedRowCount_;

    TPageLocation GetLocation() const noexcept {
        return { Offset_, Size_, Crc32_ };
    }
} Y_PACKED;
```

`TShortChild` follows the same versioned split.

The node header already carries `IsShortChildFormat`; add a version bit to `TBtreeIndexMeta` (not per-node) to select old vs new child layout for the whole part.

---

## Location lookup for old-format parts

`TMeta` already holds `Steps` (page end-offsets) and `Extra` (crc32, type) as parallel arrays indexed by `pageId`. No extra allocation is needed at part-open time. A new method reads directly from those arrays:

```cpp
// In TMeta:
TPageLocation GetLocation(ui32 pageId) const noexcept {
    const ui64 begin = pageId ? Steps[pageId - 1] : 0;
    const ui64 end   = Steps[pageId];
    return { begin, ui32(end - begin), Extra[pageId].Crc32 };
}
```

O(1), no copy. At read time `TChild::GetLocation()` for old-format nodes calls `TMeta::GetLocation(PageId_)` through the part's page collection. Hot path stays O(1).

---

## `IPages` interface

```cpp
struct IPages {
    // Universal: all pages addressed by TPageLocation
    virtual const TSharedData* TryGetPage(const TPart*, TPageLocation, TGroupId) = 0;
};
```

All callers — structural and data alike — pass `TPageLocation`. Callers that previously held a `TPageId` obtain the location via `TMeta::GetLocation(pageId)` once at the call site. B-tree iterator calls `TryGetPage(part, child.GetLocation(), groupId)` — O(1) from the embedded node (new format) or from `TMeta::GetLocation(PageId_)` (old format).

---

## Shared cache (`shared_page.h`, `shared_cache_events.h`)

`TPage` stores `TPageLocation` for all pages — structural and data alike. `TPageId` is no longer stored in `TPage`. `TPageLocation` provides the cache key (`Offset`, unique within a collection since the cache is partitioned by `TLogoBlobID`), `Size` for memory accounting, and `Crc32` for verification.

`TEvRequest` carries `TVector<TPageLocation>` for all page types. `TEvResult` returns `TVector<TLoaded>` where each `TLoaded` carries `TPageOffset` (sufficient to identify the page back to the requester; size and crc32 are already known to the requester from the original `TPageLocation`).

`TCollection` internal structures (`PageMap`, `PendingRequests`, `DroppedPages`) are all re-keyed from `TPageId` to `TPageOffset`.

---

## `TLoadedPage` (`flat_sausage_fetch.h`)

```cpp
struct TLoadedPage {
    TPageLocation Location;   // replaces TPageId PageId
    TSharedData   Data;
};
```

Bio actor receives `TPageLocation`, dispatches I/O via `IDataPageCollection::Bounds(location)`, returns `TLoadedPage { location, data }`.

---

## Forward cache (`flat_fwd_env.h`, `flat_fwd_cache.h`, `flat_fwd_iface.h`)

### Read-ahead of page locations from b-tree leaves

For scan operations the forward cache pre-reads upcoming data page locations directly from b-tree leaf nodes. Because `TChild::GetLocation()` returns `TPageLocation` inline (new format) or via `TMeta::GetLocation(PageId_)` (old format), the read-ahead path extracts locations from leaves without any additional collection lookup — O(1) per leaf entry.

### Interface changes

`IPageLoadingQueue`:
```cpp
// replaces AddToQueue(TPageId, EPage):
virtual ui64 AddToQueue(TPageLocation location, EPage type) = 0;
```

`IPageLoadingLogic`:
```cpp
// replaces Get(queue, TPageId, EPage, lower):
virtual TResult Get(IPageLoadingQueue* head, TPageLocation location, EPage type, ui64 lower) = 0;

// Fill receives TLoadedPage carrying TPageLocation:
virtual void Fill(NPageCollection::TLoadedPage& page, NSharedCache::TSharedPageRef ref, EPage type) = 0;
```

`NFwd::TPage` (internal forward cache page slot) re-keyed from `TPageId` to `TPageOffset`.

`TLoadedPagesCircularBuffer` (scan trace) re-keyed from `TPageId` to `TPageOffset`.

`TIndexPageLocator` (routes b-tree index node fetches to the correct group/level queue) re-keyed from `TPageId` to `TPageOffset`:

```cpp
// current:
void Add(TPageId pageId, TGroupId groupId, ui32 level);
ui32 GetLevel(TPageId pageId) const;
TGroupId GetGroup(TPageId pageId) const;

// after:
void Add(TPageOffset offset, TGroupId groupId, ui32 level);
ui32 GetLevel(TPageOffset offset) const;
TGroupId GetGroup(TPageOffset offset) const;
```

`TIndexPageLocator` is populated in `TBTreeIndexCache::Fill()` as internal nodes are loaded — each child's offset is registered with its `GroupId` and level. It is queried in two places:
- `TBTreeIndexCache::Get()` and `Fill()` call `GetLevel(offset)` to dispatch a fetched page to the correct per-level data structure.
- `TEnv::TryGetPage()` calls `GetGroup(offset)` to redirect an index-page request to the correct group loading queue.

Population in `Fill()` changes from `node.GetShortChild(pos).GetPageId()` to `node.GetShortChild(pos).GetLocation().Offset` (new format) or `TMeta::GetLocation(child.PageId_).Offset` (old format). Both are O(1).

---

## Subsystem boundary summary

```
[B-tree / index iterator]   TPageLocation  (embedded in new format, or TMeta::GetLocation in old)
        │  TryGetPage(part, TPageLocation, groupId)          -- all page types
[IPages / TEnv]             TPageLocation
        │  TEvRequest{ IDataPageCollection, TVector<TPageLocation> }   -- all page types
[shared_sausagecache]       TPage keyed by TPageOffset       -- all page types
        │  IDataPageCollection::Bounds(location) → TBorder
[bio_actor]                 TBorder → blobstorage
        │  TLoadedPage{ TPageLocation, TSharedData }
[shared_sausagecache]       fills TPage, returns TEvResult{ TVector<TLoaded{TPageLocation,...}> }
```

No `offsetToPageId` map anywhere. No `TPageId` at any subsystem boundary.

---

## Migration plan

1. Add `TPageOffset`, `TPageLocation` to `flat_page_iface.h`.
2. Add `IDataPageCollection`; implement in `TMeta` (shares blob array with `IPageCollection`).
3. Add `TMeta::GetLocation(TPageId) -> TPageLocation` method (reads directly from existing `Steps` and `Extra` arrays, no allocation).
4. Add root-location fields (`uint64 RootOffset`, `uint32 RootSize`, `uint32 RootCrc32`) to `TBtreeIndexMeta` in `flat_table_part.proto`, used only when `TLayout.BTreeIndexesFormatVersion == 1` (field already exists); define old (v0) and new (v1) `TChild` / `TShortChild` layouts in C++.
5. Change `TLoadedPage` to carry `TPageLocation`.
6. Change `TPrivatePageCache`: replace `TPageId` in `TPage::Id`, `TPageCollection::PageMap`, `TPageCollection::StickyPages`, and all public methods with `TPageLocation` / `TPageOffset`; remove `GetPageType` / `GetPageSize` helpers that called into `IPageCollection::Page(pageId)` for data pages.
7. Add `TEvDataRequest` / `TEvDataResult` carrying `TVector<TPageLocation>`.
8. Change `TPage` in shared cache to store `TPageLocation` for all page types (key=`Offset`, size for accounting, crc32 for verification); re-key `TCollection::PageMap`, `PendingRequests`, and `DroppedPages` from `TPageId` to `TPageOffset`.
9. Add `TryGetPage(part, TPageLocation, TGroupId)` to `IPages`; implement in `TEnv`, `TLoaderEnv` (re-key `SavedPages` / `NeedPages` from `TPageId` to `TPageOffset`), test fakes.
10. Migrate `TPartGroupBtreeIndexIter` reads to `TryGetPage(part, child.GetLocation(), groupId)`; change `TNodeState::PageId` from `TPageId` to `TPageOffset`.
10a. Migrate `TChargeBTreeIndex`: change `TChildState::PageId` from `TPageId` to `TPageOffset`; migrate `TryGetDataPage` / `HasDataPage` to call `Env->TryGetPage(Part, TPageLocation, groupId)`.
11. Migrate `flat_fwd_cache` / `flat_fwd_warmed`: change `IPageLoadingQueue::AddToQueue` and `IPageLoadingLogic::Get` to take `TPageLocation`; re-key `NFwd::TPage`, `TLoadedPagesCircularBuffer`, and `TIndexPageLocator` from `TPageId` to `TPageOffset`; wire b-tree leaf read-ahead to extract locations via `TChild::GetLocation()`.
12. Migrate `TEvFetch` in `flat_bio_events.h` from `TVector<TPageId>` to `TVector<TPageLocation>`; update bio actor to use `IDataPageCollection::Bounds(location)` for I/O dispatch.
13. Update writer to emit v1 `TChild` when global switcher is on; in v1, do not assign `TPageId`s to data and b-tree pages — only structural pages enter `TMeta`'s `TEntry`/`TExtra` arrays.  Emit root location into `TBtreeIndexMeta::RootOffset/Size/Crc32` instead of `PageId`.  Keep v0 writer path fully functional when switcher is off.
14. Add global write-time switcher (default: off, i.e. v0).
15. Remove all flat index code: `TPartGroupFlatIndexIter`, `EPage::FlatIndex`, flat index writer, `TIndexPages::FlatGroups` / `FlatHistoric`, loader fields `FlatGroupIndexes` / `FlatHistoricIndexes`, and all related call sites.

---

## `TPageId` scope boundary

The following uses of `TPageId` are **explicitly kept** — they address structural pages or are internal to `IPageCollection`:

| Location | Field / usage | Reason kept |
|---|---|---|
| `flat_part_loader.h` | `SchemeId`, `GlobsId`, `LargeId`, `SmallId`, `ByKeyId`, `GarbageStatsId`, `TxIdStatsId` | Structural pages, loaded once, `TPageId` is correct here |
| `flat_part_loader.h` | `TBtreeGroupIndexes`, `TBtreeHistoricIndexes` (root pageIds) | v0 only: b-tree root by `TPageId`, resolved via `TMeta::GetLocation` at load time. v1: root identified by `TPageLocation` carried in `TBtreeIndexMeta`. |
| `TBtreeIndexMeta.RootPageId` | Root node reference | v0 only: loader calls `TMeta::GetLocation(RootPageId)` once. v1: unused; root location is in new `TBtreeIndexMeta` fields (`RootOffset` / `RootSize` / `RootCrc32`). |
| `IPageCollection` internals | `Page(pageId)`, `Bounds(pageId)`, `Verify(pageId,...)` | Interface is for structural pages only; not called for data or b-tree node pages |
| `TMeta::Steps` / `Extra` arrays | Indexed by `TPageId` | Internal to `TMeta`; accessed only via `GetLocation(pageId)` |

The following uses of `TPageId` are **migrated to `TPageOffset` / `TPageLocation`**:

| Location | Field / usage | Migration |
|---|---|---|
| `flat_bio_events.h::TEvFetch` | `TVector<TPageId> Pages` | → `TVector<TPageLocation>` |
| `flat_part_index_iter_bree_index.h::TNodeState` | `TPageId PageId` | → `TPageOffset` (node identified by location in new API) |
| `flat_part_loader.h::TLoaderEnv` | `SavedPages`, `NeedPages` keyed by `TPageId` | → keyed by `TPageOffset` |
| `flat_sausagecache.h::TPage` | `const TPageId Id` | → `TPageLocation` |
| `flat_sausagecache.h::TPageCollection` | `PageMap`, `StickyPages` keyed by `TPageId` | → keyed by `TPageOffset` |
| `shared_page.h::TPage` | `const TPageId PageId` | → `TPageLocation` |
| `shared_sausagecache.cpp::TCollection` | `PageMap`, `PendingRequests`, `DroppedPages` | → keyed by `TPageOffset` |
| `shared_cache_events.h::TEvRequest/TEvResult` | `TVector<TPageId>` | → `TVector<TPageLocation>` |
| `flat_part_charge_btree_index.h::TChildState` | `TPageId PageId` | → `TPageOffset` |
| `flat_part_charge_btree_index.h::TChargeBTreeIndex` | `TryGetDataPage(TPageId, TGroupId)`, `HasDataPage(TPageId, TGroupId)` — call `Env->TryGetPage(Part, pageId, groupId)` | → `TryGetPage(Part, TPageLocation, groupId)` |
| `flat_fwd_iface.h::IPageLoadingQueue` | `AddToQueue(TPageId, EPage)` | → `AddToQueue(TPageLocation, EPage)` |
| `flat_fwd_iface.h::IPageLoadingLogic` | `Get(queue, TPageId, EPage, lower)` | → `Get(queue, TPageLocation, EPage, lower)` |
| `flat_fwd_cache.h::NFwd::TPage` | `TPageId PageId` | → `TPageOffset` |
| `flat_fwd_cache.h::TLoadedPagesCircularBuffer` | keyed by `TPageId` | → keyed by `TPageOffset` |
| `flat_fwd_cache.h::TIndexPageLocator` | `TMap<TPageId, ...>` | → `TMap<TPageOffset, ...>` |
| `flat_sausage_fetch.h::TLoadedPage` | `TPageId PageId` | → `TPageLocation` |

### `TBlobs` outer/extern blob read-ahead (`flat_fwd_blobs.h`)

`TBlobs` manages read-ahead for outer (small) and extern (large) blobs via `TFrames` frame entries. Its `Lower`, `Upper`, `Grow` fields and `FrameTo`, `Propagate`, `Rewind` methods use `TPageId` as a **frame reference number** — an index into `TFrames`, not a page collection page. This is a separate addressing space from page-collection `TPageId`s. These fields are **not migrated** in this branch; `TFrames` references remain `ui32`-typed frame indices.

