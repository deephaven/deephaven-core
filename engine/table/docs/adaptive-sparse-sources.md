# Adaptive sparse array sources

This document describes how the sparse array sources (`CharacterSparseArraySource` and its replicas) store values,
record previous values, prepare for parallel population and release memory, and which performance traps shaped them. The
sources address row keys through a page table, as they always have, but a leaf of the page table is stored according to
how many values it holds: a leaf with few values is a compact leaf, and a fuller leaf is a full block. By default every
leaf is a full block, as before; `select` and `update` use compact leaves when
`SparseMemoryColumnSourceFactory.adaptive` is true.

| | |
|---|---|
| **Sources** | `CharacterSparseArraySource`, replicated to Byte, Short, Integer, Long, Float, Double and Object; `BooleanSparseArraySource` over Byte; `InstantSparseArraySource` and `ZonedDateTimeSparseArraySource` over Long, through `NanosBasedTimeSparseArraySource` |
| **Compact leaves** | `sparse/CharCompactLeaf`, with `CharSortedLeaf` (at most 16 values) and `CharBitmapLeaf` (at most 128 values), replicated per type |
| **Base class** | `SparseArrayColumnSource`: `clearBlocks`, `clearBlocksWithoutLiveRows`, `clearVacatedBlocks`, `estimateSize`, `getSparseMemoryColumnSource` |
| **Configuration** | `SparseMemoryColumnSourceFactory.adaptive` (default false) selects the default thresholds for the sources `select` and `update` create; every other source has only full blocks |
| **Parallel population** | `WritableSourceWithPrepareForParallelPopulation.parallelPopulationAlignment` and `prepareForParallelPopulation(rows, alignedDivisions)`; `SelectColumnLayer.divideAligned` |
| **Benchmarks** | `SparseStorageBenchmark` (`jmhRunSparseStorage`), `SparseStorageMemory` (`sparseStorageMemory`), `SparseUpdateBenchmark` (`jmhRunSparseUpdate`), all in `io.deephaven.benchmark.engine.sparse` |
| **Tests** | `TestCharCompactLeaf` and `TestCharacterAdaptiveSparseArraySource` and their replicas, checked against reference maps; `TestBooleanAdaptiveSparseArraySource`, `TestInstantAdaptiveSparseArraySource`, `TestSparseArraySourceNullBlocks`; the `Abstract*ColumnSourceTest` suites |

## Why leaves adapt

A select or update result is written at the row keys of its parent. When the parent is sparse, a source that allocates
a full block of 1024 values for every leaf it touches spends most of its memory on nulls: every 64th row key costs 515
bytes per row of a double column, and every 512th costs 4 KB. The alternative of redirecting into a dense array costs a
redirection hash per row and a hash lookup per read, even for a dense parent.

A leaf that holds few values instead stores only those values and enough structure to find them. The page table above
the leaves is unchanged, so a dense result costs no more than before, and a sparse one costs bytes per value rather
than kilobytes per leaf.

## Layout

Row keys are split as in `SparseConstants`: a leaf covers `BLOCK_SIZE` (1024) consecutive row keys, and three page
table levels above it cover bits 40 and up (`Level0`), bits 25 to 39 (`Level1`) and bits 10 to 24 (`Level2`). Each
level holds its first child in a field and allocates an array of children only for a second one, so a source whose rows lie in one range of keys allocates no arrays above its leaves.

`Level0`, `Level1` and `Level2` are nested classes of each source, typed per type. `Level2` keeps full blocks and
compact leaves in separate arrays (`char[][] blocks` and `CharCompactLeaf[] compacts`), so reading a full block is a
load from a `char[][]` with no type test, as it would be with no compact leaves at all. A leaf is a full block, a
compact leaf, or absent, which reads as null.

Each source has two thresholds:

- `sortedThreshold` (default 16): the most values a sorted leaf holds.
- `compactThreshold` (default 128): the most values a compact leaf holds. 0 makes every leaf a full block, which is what
  the no-argument constructor does.

A leaf takes the next representation when a write would take it past its threshold: absent, then sorted, then bitmap,
then full block. It never returns to a smaller one. Removing values leaves a leaf's representation as it is; releasing a
leaf with no live rows is `clearBlocks`'s job (below).

## Compact leaves

`CharCompactLeaf` is the abstract interface for a leaf of fewer values than a block: `get`, `put`, `putSorted` (a
merge of ascending offsets), `insertAbsent` (add offsets holding null, for parallel preparation), `remove`,
`removeRange`, `copyTo` a block, `copyOffsets`, `overlayRange` and `fillRange` for chunked reads, and `estimateSize`.

### Sorted leaf

`CharSortedLeaf` holds its offsets in ascending order, packed four 16-bit lanes to a `long`, with its values at the
same positions. Unused lanes hold `Short.MAX_VALUE`, which is past every offset. A lookup counts the offsets less than
its target a word at a time, with no branch that depends on the offsets: setting a guard bit in each lane, subtracting
the target replicated into every lane, and counting the guard bits that survive (`lessThan`). The count is the index of
the target if it is present.

`wordsFor` sizes the offsets as one word for up to four offsets and at least four words otherwise, and `lowerBound`
counts those two lengths with straight-line code. The cost is a handful of arithmetic instructions for every lookup in a
leaf of up to 16 values.

### Bitmap leaf

`CharBitmapLeaf` keeps a 1024-bit bitmap of the offsets that hold a value, the values densely in offset order, and
`rankBefore`, a `short` per bitmap word counting the values in the words before it. The value of an offset is at its
rank: `rankBefore[word]` plus the population count of the bits below it in its word. A point read is two loads and a
`bitCount`. The fixed cost is about 240 bytes per leaf (the bitmap, the ranks and the object and array headers) plus
the values, which is why the sorted tier exists below 16 values.

Inserts and removes move the values above the changed offset and adjust the ranks after its word. `putSorted` and
`insertAbsent` merge from the highest offset down, so each value moves at most once, and recompute the ranks once.
`removeRange` clears the range's bits word by word, counts them, moves the following values once and adjusts the ranks
once.

## Writes

`set(key, value)` finds or allocates the `Level2`, records the previous value if needed, then stores into the block or
the compact leaf, promoting the leaf if the new offset takes it past its threshold.

`fillFromChunk` walks the row keys one leaf at a time with a `LeafCursor`: `LeafKeys` over a chunk of row keys, or
`LeafRanges` over a chunk of ranges when the average run length is at least `USE_RANGES_AVERAGE_RUN_LENGTH`. For each
leaf it knows how many row keys the write puts there before touching the leaf, so it picks the representation once:

- an absent leaf receiving more than `compactThreshold` values becomes a full block directly, and the cursor copies the
  values into it by key or by range, without computing offsets;
- a compact leaf receives its offsets in one `putSorted` merge, or becomes a block first if the merge would take it past
  `compactThreshold`;
- a full block is written in place.

`fillFromChunkUnordered` writes one key at a time, reusing the `Level2` while consecutive keys share a leaf.

## Reads

`getChar` reads the block, the compact leaf, or null. Chunked fills (`fillChunk`, `fillPrevChunk`, `fillChunkUnordered`)
keep a `FillState` holding the current leaf, and read a new leaf only when the row key leaves it. A fill by ranges copies
from a block with `copyFromTypedArray`, and from a compact leaf with `fillRange`, which computes the rank of the first
offset once and walks forward. `getChunk` of a contiguous range within one full block aliases the block, as before.

## Previous values

A source that tracks previous values records the value of each row the first time a cycle changes it, in a second page
table (`prevRoot`) with the same levels. A row without a recorded value has not changed this cycle, so its previous
value is its current value. Each leaf of the previous table is either:

- a compact leaf holding the offsets recorded this cycle and their values, or
- once more offsets are recorded than `recordThreshold` allows, a full snapshot of the whole leaf, taken from the
  current leaf with the recorded values laid over it.

`recordThreshold` is `compactThreshold`, or 128 when `compactThreshold` is 0. So a source with only full blocks still
records a few changed rows per leaf in a compact leaf, and snapshots only leaves with many changes.

There is no in-use bitmap. A compact leaf is its own record of which rows it holds, and a snapshot holds every row. The
first write to a leaf in a cycle looks up the previous leaf, and later writes find it there; a bulk write records all of
a leaf's offsets with one `putSorted` (`recordPrevious` with `PrevScratch`).

Snapshot blocks and the arrays of the previous page table come from per-type `SoftRecycler`s. The leaves and `Level2`s
created in a cycle are listed in `prevLeafKeys` and `prevLevel2Keys`, so `commitUpdates` clears exactly the entries it
set and returns each array to its recycler with every entry null, without walking the page table. A snapshot block is
not null-filled when borrowed, because taking a snapshot overwrites all of it.

### Why per-row recording, not per-leaf snapshots

A snapshot costs the same for every leaf it covers, whatever the number of rows changed: it reads 1024 values and writes
1024 values. Recording only the changed rows costs in proportion to them. With a double column, a cycle of 2^16 row keys
touches 64 leaves:

| Pattern | Rows changed per leaf | Snapshot, cache lines per leaf | Recorded rows, cache lines per leaf |
|---|---|---|---|
| Stride512 | 2 | 256 | about 4 |
| Stride64 | 16 | 256 | about 32 |
| Stride8 | 128 | 256 | 256, one row per line |
| Dense | 1024 | 256, one `arraycopy` | snapshot |

Snapshotting every touched leaf made modify cycles on Stride512 take 1.5x as long as recording rows, and Stride64
1.15x; the two meet at Stride8. Past `recordThreshold` rows a leaf is snapshotted, which is the cheaper choice
there.

## Parallel population

`select` and `update` populate a result from several threads when the column allows it. Writers of different leaves must
change no shared structure, so `prepareForParallelPopulation` allocates, before the threads start, the page table
entries above every leaf to be written, for current and previous values, and the arrays of `Level2` that may hold the
leaf (`Level2.prepareIndex`).

The sources report `parallelPopulationAlignment() == BLOCK_SIZE`. `SelectColumnLayer` then divides the rows with
`divideAligned`, ending each division at a leaf boundary, and passes `alignedDivisions = true`. Each writer then owns
whole leaves: it builds them, promotes them and records their previous values itself, and the preparation only counts
the row keys in each leaf to decide which arrays a leaf may need.

When divisions are not aligned (a caller that uses `prepareForParallelPopulation(rows)`, or a Table or RowSet result,
which divides rows singly), several writers may share a leaf, so the preparation does all structural work up front: it
builds each leaf with every offset to be written, holding null (`insertAbsent`, or a block), and records the previous
values. Writers then only store values into offsets that already exist, which `putSorted` does in place when every
offset is present. The preparation reads the row keys in pieces of 64 leaves (`PREPARE_PIECE_MASK`), which bounds the
chunk each piece builds.

`prevLeafKeys` and `prevLevel2Keys` are the only shared state writers add to, under the lock of `prevLeafKeys`.

## Releasing memory

`SelectOrUpdateListener` calls `SparseArrayColumnSource.clearBlocksWithoutLiveRows` with the rows a cycle removed or
vacated, and each source frees, at the end of the cycle, the leaves, `Level2`s and `Level1`s that no longer hold a live
row. A Boolean or Instant result is released through the primitive source it reinterprets to. A level that holds a
single child keeps it in a field, so clearing a child it never had allocates nothing.

`setNull(RowSequence)` nulls a range of a block with `Arrays.fill` and removes a range of a compact leaf with
`removeRange`. A compact leaf is not shrunk when values are removed, and a full block is not demoted; only releasing a
leaf with no live rows returns its memory.

## Boolean and time sources

`BooleanSparseArraySource` stores each value as a byte in a `ByteSparseArraySource` and reinterprets to it, so
`SelectColumnLayer`, which writes through the primitive source, and the byte source share the same leaves. Likewise the
time sources store epoch nanoseconds in a `LongSparseArraySource`. These wrappers delegate previous values, immutability,
shifts and parallel population to the primitive source.

## Replication

`CharacterSparseArraySource`, `CharCompactLeaf`, `CharSortedLeaf`, `CharBitmapLeaf` and their tests are the templates;
`ReplicateSourcesAndChunks` and `ReplicateSourceAndChunkTests` generate the other types. The Object source keeps
references in its leaves and recyclers, and nulls each array slot it releases so that the recyclers retain no values.

## Measured behaviour

All comparisons are against the sparse array sources before compact leaves, in the same build, with 2 forks, over a
double column and 2^23 row keys. `SparseUpdateBenchmark` runs `update("X = V * 2.0")` over a refreshing parent whose
row keys follow the pattern, and its cycles change 2^16 row keys.

With every leaf a full block (the default):

- Memory per row is unchanged.
- Initial updates take 0.91 to 1.01x as long serially and 0.35 to 0.94x in parallel.
- March and modify cycles take 0.34 to 1.00x as long, Dense gaining most, except a serial Stride8 modify at 1.05x.
  Cycles allocate 1 to 5%
  more, except Stride8, which allocates 782 KB against 703 KB per march cycle and 180 KB against 100 KB per modify
  cycle, for the compact leaves that record previous values.
- `fillUnordered` takes 0.56 to 0.65x as long and `randomSet` 0.35 to 0.94x; other storage operations are within 0.89
  to 1.11x.

With compact leaves:

| Pattern | Bytes per row, full blocks | Bytes per row, compact | Initial update, serial | Initial update, parallel |
|---|---|---|---|---|
| Dense | 8.1 | 8.1 | 0.99x | 0.37x |
| Stride8 | 64.5 | 10.2 | 0.60x | 0.20x |
| Stride64 | 515 | 14.5 | 0.17x | 0.15x |
| Stride512 | 4115 | 48.0 | 0.09x | 0.15x |
| Random1 | 807 | 18.6 | 0.20x | 0.19x |
| Clustered | 65.0 | 8.7 | 0.59x | 0.23x |

March cycles take 0.58 to 0.92x as long and allocate a half to a seventh as much except for Dense; modify cycles take
0.77 to 0.96x as long.

A point read in a compact leaf costs a bitmap rank or a sorted-leaf count. Random `get` takes 1.33 to 2.63x as long on
Stride64, Stride512, Random1, Runs64 and Clustered, and 0.65 to 0.93x on Dense, Stride8 and Random10, whose smaller
footprint offsets the rank.

## Traps this code has already fallen into

**Sharing page table levels between types.** Making `Level0` and `Level1` one generic class for every type stores the
child as `Object` and costs a `checkcast` (a class word load, a compare and a branch) per level per lookup, which C2
does not hoist out of the loop. Random `get` on Dense took 1.29x as long, and `fillUnordered` 1.10x. A root class whose
fields are typed as the shared level halves the checks, but one check per lookup remains, and Stride8 `fillUnordered`
stayed at 1.07x: its working set of about 29 MB is close to the L3 size, so six extra instructions in a 93-instruction
loop show. The levels stay per type.

**Binary search in the sorted leaf.** `Arrays.binarySearch` over 16 or fewer `short` offsets mispredicts on random
lookups: 59% of Stride64 `get` time. Branch-free counting cut that by 40%, and the packed lane count (SWAR) to 6.5 ns
against 5.2 ns for a bitmap leaf. Letting C2 auto-vectorize the count over the words made it slower (15 ns on Stride64),
because the vector setup and tail cost more than four scalar words; `lowerBound` counts one and four words without a
loop for that reason.

**An unranked bitmap.** Counting the bits below an offset across all earlier words made random reads 2 to 4x slower than
the ranked bitmap, to save 0.4 to 24 bytes per row. The ranks stay.

**Shared lambdas in hot paths.** Writing through `RowSequence.forAllRowKeyRanges` with a lambda added a second receiver
type at the shared call site in `RowSequence`, so other callers' consumers stopped being scalar-replaced and the
formula's own loop slowed: a serial Dense initial update took 20.0 ms against 14.8. `LeafCursor` walks the row key
chunk directly, and the gap closed (14.6 against 14.7). The cost appeared in the caller, not in the source, which is
where profiling first looked.

**Caching the current `Level2` across a fill.** Keeping the last `Level2` in `FillState` to skip the page table walk
made C2 compile the fill loop worse (111 instructions with stack spills and reloaded chunk fields, against 99 without
the cache) and was no faster.

**Removing a range one offset at a time.** Each removal from a compact leaf moves the values above it, so removing a
range by single removals is quadratic in the leaf's size: clearing a 128-value bitmap leaf moved more than 8,000 values.
`removeRange` moves the values once.

**Comparing across builds.** Layout and inlining differences between builds moved results by more than the effects
measured here. Every comparison above ran the storages being compared in one build, selected by a parameter.
