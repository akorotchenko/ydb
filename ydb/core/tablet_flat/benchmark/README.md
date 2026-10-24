# Shared-cache comparison

The `TSharedCacheFixture` cases share one dataset, key hash and access order. They compare
`std::unordered_map`, `THashMap`, and the shared cache in three groups:

- Internal table diagnostic (`CacheTable*`): maps return the same token as the cache's table probe
  under `BeginOperation()`. Page readers use the owning cache API described below.
- Owning lookup: maps copy a `TSharedData` owner; the cache returns a native `TSharedPageRef`.
  The result pointer is consumed and its owner released inside every measured iteration.
- Owning lookup plus a byte read: the same lifetime operations also consume the first payload byte.

Miss cases query a populated table using a disjoint set of valid page locations. The maps are
reserved before timing, using the cache bucket count as the reservation hint. Actual bucket
counts are reported because container rounding differs. The raw maps are single-threaded cost
baselines; they do not provide the cache's concurrent lifetime or resize semantics.

The default dataset contains **4,194,304 entries**, with four cache buckets per resident page
and distinct 64-byte buffers. Raw key/value storage alone is at least 160 MiB; cache handles
and bucket arrays add substantially more. This host's measured LLC domain is 16 MiB. A seeded
random permutation visits the entire dataset before repeating; there is no short repeatedly
visited subset or random-number generation inside the timed loop. Each measured sample uses
exactly one full permutation pass (4,194,304 lookups), so every backend visits the same full
working set. The common per-iteration
sequence selects a page, constructs its key, performs the lookup and consumes the result.

Arguments are named `pages` and `native_pins`. `native_pins=1` retains one native cache ref per
page, isolating additional-reader release from the final-reference release in the default case.
All cases use production cache traits. Setup, validation and cleanup are outside timing.
The cache fixture accepts explicit geometry; its existing default test configuration is unchanged.

Build with `./ya make --build release ydb/core/tablet_flat/benchmark --no-bazel-remote-store`.
Run only after **all concurrent builds and tests, including other worktrees, have ended**.
Before running, choose an allowed CPU, check its LLC size against the lookup working set,
pin the single-threaded process to that CPU, and record the CPU, compiler, flags and revision.
Discard measurements if another build/test starts during the run.

Select only `^TSharedCacheFixture/` with `--benchmark_filter`, use `--benchmark_repetitions=5`, and preserve
JSON output. No timing result is claimed by this source change. Historical comparison results
used a different revision, geometry and hardware; compare freshly built variants from this
common harness before drawing performance conclusions.
