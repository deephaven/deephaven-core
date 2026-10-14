# engine/rowset: RowSets and RowSequences

This module implements Deephaven's compressed ordered sets of row keys. Every table in the engine
addresses its data through a `RowSet`: a dynamically-updatable, ordered set of non-negative
`long` row keys that column sources map to storage. `TableUpdate`s describe changes as RowSets
(added/removed/modified) plus `RowSetShiftData`, and bulk data movement consumes row keys through
the chunk-oriented `RowSequence` API. This document describes the public contracts, the internal
data structures, and the ownership/lifecycle rules that are easy to get wrong.

## Public API

- **`RowSet`** — an immutable-by-interface view of an ordered set of row keys. Supports set
  algebra (`union`, `intersect`, `minus`, `invert`), position/rank queries (`get(position)`,
  `find(key)`), subsetting by position or key range, iteration, and chunk filling. A `RowSet` is a
  `RowSequence`, so it can be consumed anywhere a sequence is expected.
- **`WritableRowSet`** — adds in-place mutation (`insert`, `remove`, `retain`, `update`,
  `shiftInPlace`, ...). Mutators are only called from single-threaded contexts (see
  [threading](#threading-and-the-update-graph)).
- **`TrackingRowSet` / `TrackingWritableRowSet`** — adds "previous value" access
  (`sizePrev()`, `prev()`, `copyPrev()`, `findPrev()`, ...). The previous value is a snapshot of
  the RowSet as of the start of the current update-graph cycle, keyed off the `LogicalClock` step;
  the first mutation in a cycle captures it (`preMutationHook`). A plain `WritableRowSet` becomes
  tracking via `toTracking()`, which consumes the original.
- **`RowSequence`** — an ordered stream of row keys designed for chunked consumption:
  `fillRowKeyChunk`, `fillRowKeyRangesChunk`, `forEachRowKey(Range)`,
  `getRowSequenceByPosition` / `getRowSequenceByKeyRange` (sub-views), and
  `getRowSequenceIterator()`. All range and key bounds are **inclusive** unless a name says
  otherwise; positions are 0-based ranks. `NULL_ROW_KEY == -1` is the "no key" sentinel; legal row
  keys are `0 .. Long.MAX_VALUE`.
- **`RowSequence.Iterator`** — forward-only, consuming iteration: each `getNext*` call permanently
  advances the iterator's position (there is no rewind or re-read; the underlying sequence itself
  is never modified):
  `getNextRowSequenceThrough(maxKeyInclusive)`, `getNextRowSequenceWithLength(count)`,
  `advance(key)` (a **no-op** if `key` is at or before the current position — implementations must
  never rewind), `peekNextKey()`, `getRelativePosition()`. The `RowSequence` objects returned by
  `getNext*` are owned by the iterator, valid only until the next call, and must **not** be closed
  by the caller. The iterator itself **must** be closed.
- **Factories and builders** — `RowSetFactory` / `RowSequenceFactory` create sets and sequences.
  `RowSetBuilderSequential` requires strictly ascending, non-overlapping appends (validated by
  default; controlled by the `OrderedLongSet.sequentialBuilderCheck` property) and is the cheap
  path. `RowSetBuilderRandom` accepts keys in any order.
- **`RowSetShiftData`** — an ordered list of non-overlapping `(beginRange, endRange, shiftDelta)`
  triplets describing how surviving rows moved in an update cycle. Shifts are ordered and
  non-overlapping in both pre- and post-shift coordinates; `apply`/`unapply` process
  same-polarity runs in the direction that makes the moves memmove-safe. Build with
  `RowSetShiftData.Builder`, or `SmartCoalescingBuilder` when shifts may be discovered out of
  order relative to an existing set of keys.

## Internal architecture

`WritableRowSetImpl` wraps a single **`OrderedLongSet`** ("inner set") and swaps implementations
as the set changes shape. There are three implementations, ordered by generality:

### `SingleRange`

One contiguous range. Size-specialized subclasses pack the `(start, delta)` pair into fewer bytes
(short/int/long combinations); `SingleRange.make` picks the encoding. All operations are O(1);
anything that breaks contiguity promotes to one of the other implementations.

### `SortedRanges`

A sorted array encoding for modest numbers of ranges. Entry semantics:

- `data[i] >= 0` — a single value, or the start of a range;
- `data[i] < 0` — the previous entry extends into the inclusive range `[data[i-1], -data[i]]`.

Invariants: entries strictly increase in absolute value; no two consecutive negatives; adjacent
ranges are always merged (a gap of at least one key separates ranges). `count` is the number of
array entries and `cardinality` is cached.

Concrete storage adapts: `SortedRangesLong` stores values directly; `SortedRangesInt` /
`SortedRangesShort` (via `SortedRangesPacked`) store `value - offset` to halve or quarter the
footprint, converting back up to long when a value stops fitting. Capacities are deliberately
bounded (defaults: long dense 256 / sparse 4096; int twice that; short 4090), where "dense" means
many elements per 64 KiB block (`elementsPerBlockDenseThreshold`). When an operation would exceed
capacity, the operation returns `null` (or falls through) and the caller converts to `RspBitmap`.
Point mutations are O(log n) search + O(n) arraycopy; bulk set operations are single-pass
two-iterator merges into a per-thread scratch buffer.

### `RspBitmap` / `RspArray`

A paged, 64-bit variant of a roaring bitmap ("Regular Space Partitioned"), for anything large.
The key space is divided into **blocks** of `BLOCK_SIZE = 65536` keys. `RspArray` holds two
parallel arrays ordered by block key, with `size` live entries:

- `spanInfos[i]` (long) — the block key in the high 48 bits, plus per-kind metadata in the low 16;
- `spans[i]` (Object) — the span payload.

A span is one of:

- **singleton**: `spans[i] == null`, `spanInfos[i]` is the full key value;
- **container**: a `Container` from the top-level `Container` project (array, bitmap, run, and
  two-value specializations over unsigned 16-bit values within one block), or a bare `short[]`
  for a packed array container, in which case the low 16 bits of `spanInfo` carry the container's
  cardinality and a shared ("copy on write") bit;
- **full block span**: `flen` consecutive completely-full blocks; small `flen` is packed into the
  low 16 bits of `spanInfo` alongside a marker object, larger `flen` is a boxed `Long` span.
  Full block spans make huge contiguous ranges O(1) in space, so "very large" does not imply
  "not RSP".

Block keys are compared **unsigned** (`uLess`/`uGreater`); searches are binary
(`getSpanIndex`/`keySearch`) with a fixup for multi-block full block spans that cover the probed
block.

**Cardinality cache**: rank/select operations use a cumulative-cardinality array `acc`, maintained
lazily with a watermark `cardData` (`cardData == size - 1` means clean). For small arrays
(`size <= accNullThreshold`, default 8) `acc` is elided and `cardData` holds the full cardinality
as an int — or `-1` when it does not fit, in which case rank queries fall back to linear scans
that carry running cardinality. Never assume `isCardinalityCached()` is true for a quiescent set.

**Unsafe mutation protocol**: bulk mutators come in `...Unsafe`/`...UnsafeNoWriteCheck` flavors
that defer cache maintenance; callers must finish with `finishMutations()` (or
`finishMutationsAndOptimize()`) before any read. Bulk removals mark spans dead by setting
`spanInfo = -1` and compact once at the end; bulk insertion of containers uses a two-pass batched
merge (`orEqualsShiftedUnsafeNoWriteCheck`) to avoid shifting the tail once per span.

## Ownership, copy-on-write, and reference counting

This is the part that causes real bugs; read it twice.

- `SortedRanges` and `RspArray` extend **`RefCountedCow`**: a reference count plus
  `cowRef()` (share), `getWriteRef()` (returns `this` if unshared, else a `deepCopy()`), and
  `acquire()`/`release()`. RSP additionally shares individual containers between arrays via a
  per-container copy-on-write flag.
- **The `ix*` convention**: mutating methods on `OrderedLongSet` (`ixInsert`, `ixRemove`, ...)
  return the resulting set, which **may be a different object** (a copy-on-write copy, or a
  different implementation type entirely). Callers must use the returned reference, and must
  `ixRelease()` a replaced reference they owned — see `WritableRowSetImpl.assign` for the
  canonical pattern. `getWriteRef()`/`deepCopy()` do **not** release the original.
- **Everything `SafeCloseable` must be closed**: `RowSequence.Iterator`s and the `RowSequence`
  views returned by `getRowSequenceByPosition`/`getRowSequenceByKeyRange` acquire a reference on
  the underlying structure and release it on `close()`. A leaked reference does not crash — it
  permanently inflates the refcount, so every later mutation of that rowset pays an unnecessary
  deep copy (and dead structures are retained). Prefer try-with-resources.
- Refcount-neutrality is testable: `RefCountedCow.refCount()` is exposed for tests, and several
  regression tests assert that an operation leaves the count unchanged.

## Implementation transitions

Operations move between implementations as the shape of the set changes: `SingleRange` promotes
to `SortedRanges` on the first gap, `SortedRanges` promotes to `RspBitmap` when it exceeds its
capacity or an operation cannot be represented, and `compact()` (`ixCompact`) attempts demotion
back toward cheaper forms. Nothing demotes automatically — an RSP-backed set that becomes a
single huge range stays RSP until compacted.

Builders adapt too: the sequential builder accumulates a pending range, then a `SortedRanges`,
then an `RspBitmap`; the random builder (`AdaptiveOrderedLongSetBuilderRandom` /
`MixedBuilderRandom` / `RangePriorityQueueBuilder`) accumulates ranges in a heap, flushing to an
accumulated set in O(n log n) batches. Builders are single-use: a second `build()` /
`getOrderedLongSet()` call throws `IllegalStateException`. Appends after a build are not detected
(the hot append path carries no checks) and their effect is undefined.

## Threading and the update graph

RowSets follow the engine's update-graph concurrency model rather than general thread safety:
one mutator at a time, with concurrent readers that operate on the previous snapshot and are
invalidated by the `LogicalClock` if they race a cycle change. `RefCountedCow` explicitly
documents this model. `TrackingWritableRowSetImpl.checkAndGetPrev` is synchronized and
double-checked against the clock step; everything else assumes external coordination.

## Complexity expectations

- `firstRowKey`/`lastRowKey`/`size`: O(1) (subject to the cardinality-cache notes above).
- `get(position)`/`find(key)`: O(log n) on RSP with a clean `acc`; O(count) on `SortedRanges`
  (bounded by its small capacity); O(1) on `SingleRange`.
- Set algebra between two sets: single-pass merges, linear in spans/ranges.
- Iteration: O(n) total; iterators and sequence views carry their position — no implementation
  restarts from the beginning on `advance` or `getNext*`.
- Known super-linear paths (see DH-23407): inserting many new blocks/spans into an existing
  `RspBitmap` one at a time pays an O(size) tail arraycopy per insertion (`insert(LongChunk)`
  with scattered keys, `insert(RowSet)` with a `SortedRanges` argument, unions inserting full
  block spans, and difference operations that split full block spans).

## Pitfalls checklist

- Close every `RowSequence.Iterator` and every sequence view you did not receive from a
  `getNext*` call; never close what `getNext*` returns.
- Use the return value of every `ix*` call; release what it replaced.
- Do not pass a rowset as an argument to its own mutators; the public `WritableRowSetImpl`
  entry points guard this, but internal `OrderedLongSet` code does not.
- Arithmetic near `Long.MAX_VALUE`: keys are legal up to `Long.MAX_VALUE`, so `key + 1`,
  `(a + b) / 2`, and `start + length` idioms must be written in overflow-safe forms
  (subtractions, `i + (j - i) / 2`, saturation). A full-universe set (`[0, Long.MAX_VALUE]`)
  has a cardinality that does not fit in a `long`; treat it as a hostile input.
- After `...Unsafe` mutations, call `finishMutations()` before reading.
- `RowSequence` methods on a mid-mutation (unfinished) RSP array are undefined; the "dirty acc"
  fallbacks exist for internal use, not as a license to read while writing.

## Testing

Tests live in `src/test` with the engine's category conventions: uncategorized tests run under
`./gradlew :engine-rowset:test`; the large randomized suites (`RspBitmapTest`,
`SortedRangesTest`, `WritableRowSetImplTest`, ...) are `@Category(OutOfBandTest.class)` and run
under `./gradlew :engine-rowset:testOutOfBand` (invoked nightly in CI, not by `check`). Test
JVMs enable assertions, so `Assert.assertion` guards and `validate()` calls are live; when
debugging structure corruption, the `RspArray.debug` / `SortedRanges.debug` configuration
properties enable per-mutation validation.
