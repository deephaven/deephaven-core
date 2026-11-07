# Adaptive sparse array sources

This document describes how the adaptive sparse array sources (`CharacterAdaptiveSparseArraySource` and its replicas)
store values, record previous values, prepare for parallel population and release memory, and which performance traps
shaped them. The sources address row keys through a page table whose levels and leaves adapt to the row keys they hold:
a leaf with few values is a compact leaf, and a fuller leaf is a full block. The sources that `select` and `update`
create use compact leaves. The sources that other operations create, through the factory methods of
`SparseArrayColumnSource`, hold every leaf as a full block.

The full block sparse array sources (`CharacterSparseArraySource` and its replicas) remain beside them, unchanged.
Setting `SparseMemoryColumnSourceFactory.adaptive` to false makes `select`, `update` and the factory methods create
those sources instead, so that the escape hatch runs exactly the code it ran before the adaptive sources existed, with
its performance.

| | |
|---|---|
| **Sources** | `CharacterAdaptiveSparseArraySource`, replicated to Byte, Short, Integer, Long, Float, Double and Object; `BooleanAdaptiveSparseArraySource` over Byte; `InstantAdaptiveSparseArraySource` and `ZonedDateTimeAdaptiveSparseArraySource` over Long, through `NanosBasedTimeAdaptiveSparseArraySource` |
| **Full block sources** | `CharacterSparseArraySource` and its replicas, `BooleanSparseArraySource`, and the time sources over `LongSparseArraySource`, on `OneOrNSparseArrayColumnSource` and the page table levels of `sparse/CharOneOrN` and its replicas |
| **Compact leaves** | `sparse/CharCompactLeaf`, with `CharSortedLeaf` (at most 16 values) and `CharBitmapLeaf` (at most 128 values), replicated per type |
| **Page table levels** | `Level0`, `Level1` and `Level2`, nested in each source, each holding its children in a window, or ranked when sparse; `sparse/ChildPositions`, with a sorted tier (at most 16 children) and a ranked bitmap tier, ranks the children of `Level1` and `Level2` for every type; `sparse/PageTableLevels` holds the window limits and the helpers that move children between the forms, shared by every level and type |
| **Base class** | `SparseArrayColumnSource`, of both kinds of source: `clearBlocks`, `clearBlocksWithoutLiveRows`, `clearVacatedBlocks`, `estimateSize`, and the factory methods `getSparseMemoryColumnSource`, which update-by operators, joins and row redirections use to make their sources |
| **Configuration** | `SparseMemoryColumnSourceFactory.adaptive` (default true) makes `select` and `update` create adaptive sources with the default thresholds, and the factory methods create adaptive sources of full blocks; false makes both create the full block sources |
| **Parallel population** | `WritableSourceWithPrepareForParallelPopulation.parallelPopulationAlignment` and `prepareForParallelPopulation(rows, alignedDivisions)`; `SelectColumnLayer.divideAligned` |
| **Benchmarks** | `SparseStorageBenchmark` (`jmhRunSparseStorage`), `SparseStorageMemory` (`sparseStorageMemory`), `SparseUpdateBenchmark` (`jmhRunSparseUpdate`), and for results of few leaves `SparseSmallBenchmark` (`jmhRunSparseSmall`) and `SparseSmallMemory` (`sparseSmallMemory`), all in `io.deephaven.benchmark.engine.sparse` |
| **Tests** | `TestCharCompactLeaf` and `TestCharacterAdaptiveSparseArraySource` and their replicas, checked against reference maps; `TestChildPositions`; `TestBooleanAdaptiveSparseArraySource`, `TestInstantAdaptiveSparseArraySource`, `TestSparseArraySourceNullBlocks`; the `Abstract*ColumnSourceTest` suites |

## Why leaves adapt

A select or update result is written at the row keys of its parent. When the parent is sparse, a source that allocates
a full block of 1024 values for every leaf it touches spends most of its memory on nulls: every 64th row key costs 515
bytes per row of a double column, and every 512th costs 4 KB. The alternative of redirecting into a dense array costs a
redirection hash per row and a hash lookup per read, even for a dense parent.

A leaf that holds few values instead stores only those values and enough structure to find them. The page table above
the leaves is unchanged, so a dense result costs no more than before, and a sparse one costs bytes per value rather
than kilobytes per leaf.

## Layout

A source finds a row key's value by splitting the key into four fields, defined in `SparseConstants`, and using each
field as an index at one level:

| Row key bits | Indexes into | To find | Which covers | Size if indexed by child index |
|---|---|---|---|---|
| 40 to 62 | the source's `Level0` | a `Level1` | 2^40 row keys | 32,768 KB (2^23 references) |
| 25 to 39 | that `Level1` | a `Level2` | 2^25 row keys | 128 KB (2^15 references) |
| 10 to 24 | that `Level2` | a leaf | 1024 (`BLOCK_SIZE`) row keys | 256 KB (two arrays of 2^15 references) |
| 0 to 9 | that leaf | the value | one row key | 1 to 8 KB (a full block of 1024 values) |

So a read walks three page table levels and then looks up its offset in the leaf. The last column is what an array
indexed by child index occupies however few children it holds, with compressed references (the default for heaps under
32 GB); without them, the references take twice as much. Row keys spread across levels are common: a regioned source
numbers the rows of region `r` from `r << 43`, so an `update` of a partitioned table writes under one `Level0` child per
region. So each level adapts how it holds its children to how many it has and how far apart they are (below).

`Level0`, `Level1` and `Level2` are nested classes of each source, typed per type. Each level is held directly by its
parent's array, and `Level0` by the source's `root` field, so a read follows one reference per level. `Level2` keeps
full blocks and compact leaves in separate arrays (`char[][] blocks` and `CharCompactLeaf[] compacts`), so reading a
full block is a load from a `char[][]` with no type test, as it would be with no compact leaves at all; a `Level2` of
one leaf holds it in its own fields instead. A leaf is a full block, a compact leaf, or absent, which reads as null.

### Holding children

A small result would spend more on arrays indexed by child index than on its values, so each level adapts how it holds
its children to how many it has and how far apart they are.

Every level holds its children in one of two forms, chosen by the span of the child indices it holds:

- a window, while the span is at most 64 entries, or eight for each child (`maxWindow`): arrays indexed by child index
  less `base`, the index of their first entry. A full window is an array over the level's entire sub-address space,
  such as the 32,768 leaves of a `Level2`.
- ranked, when the span is wider: the children are held densely, in child index order. `Level0` keeps their indices in a
  sorted `int[]`, searched without a data-dependent branch, since its 23-bit indices fit neither a 16-bit lane nor a
  bitmap of reasonable size; `Level1` and `Level2` find a child's position with a `ChildPositions`.

A level moves between the forms as children are added: a window that would grow past its limit becomes ranked, and a
ranked level whose children become dense enough becomes a window. A ranked level holds at most `MAX_RANKED` (4,096)
children, since more always fit a window.

A window of one entry also records its index (`onlyIndex`), so that finding the only child of a level is one comparison
against a field that does not change, which the JIT can move out of a loop over one source. A `Level2` of one leaf goes
further and holds the leaf in its own fields (`onlyBlock`, `onlyCompact`), with no arrays, so that reading it follows no
reference past the level.

`ChildPositions` follows the leaves' own tiers:

- a sorted tier for up to 16 children: child indices packed four 16-bit lanes to a `long` in four fields. A lookup marks
  the lanes equal to the target and takes the position of the lowest mark, with no branch that depends on the indices,
  looking only at the first word for at most four children;
- a ranked bitmap tier for more: a bit per child and the number of children before each word, as in `CharBitmapLeaf`,
  about 5 KB.

`find`, and the lookups of a bitmap and of at most four sorted children, stay under the JIT's limit for inlining a
method at a call site it rarely reaches (35 bytes of bytecode; see the traps below).

A level is fixed but for these changes made in place: a child is set at its entry or position, or cleared, and a ranked
level appends child indices past its last one, since an append moves no position already held and writes only unused
lanes, clear bits or unused entries of arrays allocated once. A ranked level grows its arrays before appending, and a
read checks a position against the length of the array it reads. Any other change, such as widening a window, merging
indices among those held, switching between a window and ranks, or adding a second leaf to a `Level2` of one, makes a
new level, which the writer publishes in place of the old one in its parent or in `root`. A
concurrent reader, such as a snapshot reading a row that this cycle has not changed, loads each level once and reads its
window or positions and its arrays together, so it never pairs new positions with old arrays. A writer, in turn, uses
the level that its last change returned (`setBlock`, `setCompact`), or reads the page table again after
`makeRoomInLeaf`, which may have changed it.

A serial `fillFromChunk` reserves the positions of every leaf of the chunk before writing, the leaves of each `Level2`
together (`reserveLeaves`, `Level2.reserve`). A level then grows once per chunk rather than once per new leaf: indices
past the last are appended, others are merged with the leaves held in one pass into new arrays, and the arrays double
when they grow, so a level that grows a few leaves at a time is copied a logarithmic number of times.

Each source has two thresholds:

- `sortedThreshold` (default 16): the most values a sorted leaf holds.
- `compactThreshold` (default 128): the most values a compact leaf holds. 0 makes every leaf a full block, which is what
  `fullBlocks()` does; the no-argument constructor uses the defaults.

The representations, from smallest to largest, are absent, sorted, bitmap and full block. A new leaf starts in the
smallest one that holds the values of its first write, and a write that takes a leaf past its representation's
threshold moves it to the smallest one that holds the result. A leaf never returns to a smaller representation: removing
values leaves it as it is, and releasing a leaf with no live rows is `clearBlocks`'s job (below).

## Compact leaves

`CharCompactLeaf` is the abstract class of a leaf of fewer values than a block: `get`, `put`, `putSorted` (a
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
  values into it by key or by range, without computing offsets unless previous values are recorded;
- a compact leaf receives its offsets in one `putSorted` merge, or becomes a block first if the merge would take it past
  `compactThreshold`;
- a full block is written in place.

`fillFromChunkUnordered` writes one key at a time, reusing the `Level2` while consecutive keys share a leaf.

## Reads

`getChar` reads the block, the compact leaf, or null. Chunked fills (`fillChunk`, `fillPrevChunk`, `fillChunkUnordered`)
keep a `FillState` holding the current leaf, and read a new leaf only when the row key leaves it. A fill by ranges
copies from a block with `copyFromTypedArray`, and from a compact leaf with `fillRange`, which computes the rank of the
first offset once and walks forward. `getChunk` of a contiguous range within one full block aliases the block, as
before.

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

Snapshot blocks and the arrays of the previous page table that cover every child index come from per-type
`SoftRecycler`s. Any smaller window and the arrays of a ranked level are the level's own, and are discarded with it when
the cycle completes, as is the previous value `Level0`. `commitUpdates` walks the previous page table
(`recyclePrevious`), returning each snapshot block, and each recycled array with every entry null. A ranked level holds
only its children, and a window has at most 64 entries or eight for each child, so the walk reads at most 64 entries
for each level and eight for each child the cycle recorded; no list of the recorded leaves is needed. A snapshot block
is not null-filled when borrowed, because taking a snapshot overwrites all of it.

### Why per-row recording, not per-leaf snapshots

A snapshot of a leaf reads 1024 values and writes 1024 values, however few of its rows changed. Recording the changed
rows instead reads and writes one value per changed row, so a leaf with few changes costs little. With a double column,
a cycle of 2^16 row keys touches 64 leaves, in the patterns of [Row key patterns](#row-key-patterns):

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
entries above every leaf to be written and reserves each leaf's position in its `Level2`, for current and previous
values (`prepareLeafIndices`). It reserves the leaves of each `Level2` together, as a serial write does, and allocates
the arrays of the `Level2` that its leaves may need (`Level2.withArrays`).

The sources report `parallelPopulationAlignment() == BLOCK_SIZE`. `SelectColumnLayer` then divides the rows with
`divideAligned`, ending each division at a leaf boundary, and passes `alignedDivisions = true`. Each writer then owns
whole leaves: it builds them, promotes them and records their previous values itself, and the preparation only counts
the row keys in each leaf to decide which arrays of the `Level2` the leaf may need. A leaf that may hold more values
than a compact leaf holds needs room for a full block, and any other leaf room for a compact leaf. A writer may write a
leaf in several chunks, so the first chunk of a leaf that will be a full block may hold few values; when its `Level2`
has no array of compact leaves, the writer makes the leaf a full block at once (`writesAlignedDivisions`). The previous
values get room for both a snapshot and a compact leaf, since their arrays last only the cycle.

When divisions are not aligned (a caller that uses `prepareForParallelPopulation(rows)`), several writers may share a
leaf, so the preparation does all structural work up front: after reserving the leaves, it builds each leaf with every
offset to be written, holding null (`insertAbsent`, or a block), and records the previous values. Writers then only
store values into offsets that already exist, which `putSorted` does in place when every offset is present. The
preparation reads the row keys in pieces of 64 leaves (`PREPARE_PIECE_MASK`), which bounds the chunk each piece builds.

A Table or RowSet result divides its rows singly, so its writers share leaves too. Its preparation still runs in
parallel: `prepareForParallelPopulationInDivisions` allocates the page table entries above each leaf and reserves the
leaves, as for aligned divisions, and `SelectColumnLayer` then calls `prepareDivisionForParallelPopulation` from
parallel jobs, each on a division of whole leaves, to build the leaves and record their previous values. The single-row
writers start once every division is prepared.

Writers share no state but the arrays the preparation allocated, and take no lock: a writer sets only the entries of
its own leaves, current and previous, at reserved positions in those arrays, so no writer replaces a level. The
preparation creates every previous value `Level2` to be written, and the end of the cycle finds the previous value
leaves by walking the previous page table.

## Releasing memory

`SelectOrUpdateListener` calls `SparseArrayColumnSource.clearBlocksWithoutLiveRows` with the rows a cycle removed or
vacated, and each source frees, at the end of the cycle, the leaves, `Level2`s and `Level1`s that no longer hold a live
row. A Boolean or Instant result is released through the primitive source it reinterprets to. Each level releases the
children of a cycle together (`clearLeaves`, `clearLevel2s`, `clearLevel1s`): a ranked level builds new positions and
arrays of the remaining children once, rather than moving them under a reader, and a window nulls the released
entries. Clearing a child a level never had changes nothing.

`setNull(RowSequence)` nulls a range of a block with `Arrays.fill` and removes a range of a compact leaf with
`removeRange`. A compact leaf is not shrunk when values are removed, and a full block is not demoted; only releasing a
leaf with no live rows returns its memory.

## Boolean and time sources

`BooleanAdaptiveSparseArraySource` stores each value as a byte in a `ByteAdaptiveSparseArraySource` and reinterprets
to it, so `SelectColumnLayer`, which writes through the primitive source, and the byte source share the same leaves.
Likewise the adaptive time sources store epoch nanoseconds in a `LongAdaptiveSparseArraySource`. These wrappers delegate previous values,
immutability, shifts and parallel population to the primitive source.

## Replication

`CharacterAdaptiveSparseArraySource`, `CharCompactLeaf`, `CharSortedLeaf`, `CharBitmapLeaf` and their tests are the
templates; `ReplicateSourcesAndChunks` and `ReplicateSourceAndChunkTests` generate the other types, and the full block
sources from `CharacterSparseArraySource` and `CharOneOrN` beside them. The Object source keeps
references in its leaves and recyclers, and nulls each array slot it releases so that the recyclers retain no values.

## Row key patterns

The benchmarks, and the measurements below, name the row keys a source holds by pattern. `SparseStorageBenchmark`,
`SparseStorageMemory` and `SparseUpdateBenchmark` use these patterns over 2^23 row keys (8,192 leaves):

| Pattern | Row keys | Rows per leaf |
|---|---|---|
| Dense | every row key | 1,024 |
| Stride8 | every 8th row key | 128 |
| Stride64 | every 64th row key | 16 |
| Stride512 | every 512th row key | 2 |
| Runs64 | the first 64 row keys of every 512 | 128 |
| Random10 | each row key with probability 10% | about 102 |
| Random1 | each row key with probability 1% | about 10 |
| Clustered | every row key of a random eighth of the leaves, and 4 random row keys of each other leaf | 1,024 or 4 |

`SparseSmallMemory` and `SparseSmallBenchmark` use shapes of n rows, each placing a small result where it exercises a
different part of the page table:

| Shape | Row keys | Page table |
|---|---|---|
| Dense | 0 to n - 1 | consecutive leaves of one `Level2` |
| High | n consecutive row keys from 3 * 2^23 | consecutive leaves in the upper quarter of a `Level2` |
| Stride64 | every 64th row key from 0 | 16 rows in each of consecutive leaves |
| Spread | every 8,192nd row key from 0 | one row in every eighth leaf |
| Level2s | every 2^25th row key from 0 | one row in each `Level2` |
| Regions | n / 8 consecutive row keys at the start of each of eight regions 2^43 row keys apart, as a regioned source numbers its rows | eight `Level0` children 8 indices apart |
| FarRegions | n / 2 consecutive row keys at the start of region 0 and of region 100,000 | two `Level0` children 800,000 indices apart |

## Measured behaviour

All comparisons are against the full block sparse array sources (`DoubleSparseArraySource`), in the same build, with 2
forks, over a double column. `SparseUpdateBenchmark` runs `update("X = V * 2.0")` over a refreshing parent whose row
keys follow the pattern, and its cycles change 2^16 row keys.

### Compact leaves

The sources that `select` and `update` create, over 2^23 row keys:

| Pattern | Bytes per row, full block sources | Bytes per row, adaptive | Initial update, serial | Initial update, parallel |
|---|---|---|---|---|
| Dense | 8.1 | 8.1 | 1.02x | 0.42x |
| Stride8 | 64.4 | 9.9 | 0.67x | 0.20x |
| Stride64 | 517 | 13.8 | 0.19x | 0.14x |
| Stride512 | 4,113 | 42.0 | 0.10x | 0.14x |
| Random1 | 809 | 17.6 | 0.22x | 0.20x |
| Clustered | 64.6 | 8.8 | 0.62x | 0.26x |

An initial update allocates 3 to 18% as much, except for Dense, which allocates the same. March cycles take 0.34 to
0.92x as long and allocate 15 to 50% as much, except for Dense, which allocates 1 to 3% more. Modify cycles take 0.50 to
1.02x as long and allocate 5 to 12% more. Stride8 modify cycles allocate 182 KB against 100 KB, for the compact leaves
that record previous values, and Dense modify cycles allocate 14 KB against 9 KB serially and 36 KB against 17 KB in
parallel.

A point read in a compact leaf costs a bitmap rank or a sorted leaf count. Random `get` takes 1.33 to 2.54x as long on
Stride64, Stride512, Random1, Runs64 and Clustered, and 0.72 to 1.07x on Dense, Stride8 and Random10, whose smaller
footprint offsets the rank. Random `set` takes 0.91 to 2.59x as long, the most on Random1 and Random10, and
`sequentialFill` 0.45 to 1.47x, the most on Runs64.

### Full block leaves

The sources that the factory methods of `SparseArrayColumnSource` create adapt their page table levels and hold every
leaf as a full block. Over 2^23 row keys their memory per row is within 1% of the full block sources. `fillUnordered`
takes 0.58 to 0.72x as long, random `set` 0.49 to 0.93x, `randomSubsetFill` 0.54 to 1.23x, `sequentialFill` 0.59 to
1.12x, `populate` 0.96 to 1.03x and random `get` 1.00 to 1.08x.

### Small results

`SparseSmallMemory` measures the bytes per row that results of a few rows retain, in the shapes of
[Row key patterns](#row-key-patterns):

| Shape | Rows | Full block sources | Adaptive, full block leaves | Adaptive, compact leaves |
|---|---|---|---|---|
| Dense | 10 | 861 | 844 | 42.2 |
| Dense | 3,000 | 52.2 | 8.3 | 8.3 |
| Stride64 | 100 | 1,898 | 578 | 16.8 |
| Stride64 | 1,000 | 651 | 519 | 14.1 |
| Spread | 10 | 21,532 | 8,269 | 127 |
| Spread | 100,000 | 8,260 | 8,254 | 104 |
| Level2s | 10 | 21,356 | 8,280 | 144 |
| Level2s | 10,000 | 8,285 | 8,301 | 127 |
| Regions | 10 | 4,200,921 | 6,688 | 180 |
| Regions | 100,000 | 438 | 8.6 | 8.6 |
| FarRegions | 10 | 4,195,968 | 1,680 | 64 |

A `Level0` of the full block sources with more than one child allocates a 32 MB array, which is why every source spread
across regions retains megabytes. Spread, a leaf in every eight, is just dense enough for a `Level2` window, which then
has eight entries per leaf; ranking those leaves instead retained 77 bytes per row.

With 3,000 and 30,000 rows (`SparseSmallBenchmark`), each adaptive source's time divided by the full block sources',
with compact leaves and then with full block leaves:

| Shape | `randomGet` | `fillUnordered` | `sequentialFill` |
|---|---|---|---|
| Dense | 1.02 to 1.10; 1.02 to 1.08 | 0.72 to 0.77; 0.71 to 0.78 | 1.01; 0.97 to 1.00 |
| Stride64 | 2.19 to 3.25; 1.07 to 1.10 | 1.04 to 1.46; 0.60 to 0.69 | 1.17 to 1.38; 0.78 to 0.83 |
| Spread | 1.59 to 1.97; 1.13 to 1.29 | 0.92 to 0.95; 0.58 to 0.82 | 1.21 to 1.47; 1.30 to 1.63 |
| Level2s | 2.10 to 2.16; 1.30 to 1.32 | 1.07 to 1.10; 0.79 to 0.80 | 0.80 to 1.25; 1.04 to 1.21 |
| Regions | 1.09 to 1.47; 1.07 to 1.48 | 0.78 to 0.89; 0.78 to 0.89 | 0.99 to 1.04; 0.98 to 1.04 |
| FarRegions | 2.23 to 2.25; 2.23 to 2.26 | 1.08 to 1.16; 1.09 to 1.16 | 1.00 to 1.05; 0.99 to 1.05 |

Random reads of a small result pay for finding each child's position, which an array indexed by child index gives for
free. FarRegions ranks its two `Level0` children, 800,000 indices apart, so every read searches them. Compact leaves add
the rank or count within each leaf, which is why Stride64, Spread and Level2s, with few rows per leaf, read slowest with
them. With compact leaves, populating a small result takes at most 0.78x as long. With either kind of leaf, populating
one spread across regions takes less than 1% as long, because no 32 MB array is allocated.

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

**Allocating both `Level2` arrays.** An array of a `Level2` indexed by leaf index has 32,768 entries, 128 KB. When the
parallel preparation allocated the array of compact leaves for every leaf not yet a full block, in case a writer's first
chunk made it compact, every multi-leaf `Level2` of a dense parallel result carried an empty 128 KB array beside its
blocks. `QueryTableAjTest.testAjRandomStatic`, which keeps about 80,000 tables alive, ran out of a 3.5 GB heap on 5,760
such empty arrays. The preparation now decides from the row count which array a leaf needs, and a `Level2` of few
leaves sizes its arrays to them.

**Allocating blocks in the parallel preparation.** Making each leaf that would be a full block in the preparation, so
that writers never needed an array of compact leaves, moved the allocation and null fill of every block onto the one
thread that prepares: a parallel Dense initial update over 2^23 rows took 7.6 ms instead of 3.5. The writers allocate
their own blocks; the preparation leaves no room for a compact leaf where none is needed, and the writer makes such a
leaf a full block from its first chunk.

**An immutable holder between a level and its arrays.** Holding a `Level2`'s positions and arrays in a separate
immutable object, which the `Level2` replaced, kept readers consistent but added a dependent load to every read and
write: random `get` on a small Dense result took 1.66 ns against 1.03, and random `set` over 2^23 row keys 0.9 to 1.4 ns
more per row even where the level was indexed by leaf index. Each level is now itself that immutable object, held by its
parent.

**Ranking every level with no single-child path.** Holding the children of `Level0` in a binary-searched sorted array
and of `Level1` in a `ChildPositions`, with no shortcut for one child, made random `get` on a Dense result of 3,000 rows
take 4.9x as long as levels of one child or all: about 110 instructions per read against 35, at about four
instructions a cycle, with no stall. Replacing the counted rank with an equality test changed nothing, since the
instruction count, not `popcnt`, was the limit. Binary search over the eight regions of Regions mispredicted, at 11.4x.
A window with `onlyIndex` costs a compare when a level has one child and a subtraction and a load when it has many.

**A rarely taken call in a read or write loop.** `ChildPositions.Bitmap.find` was 45 bytes of bytecode, over the 35
bytes up to which the JIT inlines a method at a call site that is not hot. In `randomSet` over 2^23 row keys, every
`Level2` was indexed by leaf index, so the call was never taken, but `populate` had reached it, so C2 compiled a call
rather than a trap. A call in the loop meant C2 could not move the loads of `root` and the levels out of it: Clustered
took 3.38 ms against 1.91 for levels of one child or all, and 1.96 ms with the method forced inline. The finds that a
read reaches are kept under 35 bytes, splitting off a helper where needed.

**A data-dependent branch to check a sorted find.** Counting the lanes less than the target and then checking the lane
at that position chose the word to check with a `switch` on the position, which mispredicted when a `Level2` of 15
leaves was read at random: `fillUnordered` on FarRegions at 30,000 rows took 4.84x as long as levels of one child or
all. Marking the equal lanes of all four words and taking the lowest mark has no such branch, and took 2.43x.

**A window with no limit.** Holding `Level0`'s children in a window covering every index between them made FarRegions,
whose two regions are 800,000 `Level0` indices apart, retain 3.2 MB per source: 320,069 bytes per row with 10 rows. A
level ranks its children when a window would have more than 64 entries and more than eight for each child.

**Comparing across builds.** Layout and inlining differences between builds moved results by more than the effects
measured here. Every comparison above ran the storages being compared in one build, selected by a parameter.
