# Finding 22 — with a `_metadata` file, every location pruned against the *first* file's statistics

**Severity:** high — silent wrong results that lose data, on ordinary reads of any multi-file parquet
dataset written with `_metadata`. Nothing throws, and the wrong answer is reported as exact.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/MetadataFileRowGroupStatisticsTest.java`
**Fuzzer seed:** `3109160350218645036L` — the only failure in a 10,734-case sweep.

## Symptom

```
filter=isNull(Col1)
  column Col1:LocalTime nulls=SPARSE indexed
  layout=PARTITIONED rowGroups=default metadataFiles selection=SLICE
filtered results differ: Result table has size 0 vs. expected 1
  result sizes: disk=0 mem=1
```

The row that vanished was `Col2=1, Col0=-2, Col1=(null)`.

## Root cause

`ParquetTableLocation.pushdownRowGroupMetadata` reads the statistics for a row group like this:

```java
final Statistics<?> statistics = blocks.get(rgIdx).getColumns().get(columnIndex).getStatistics();
```

`rgIdx` comes from `iterateRowGroupsAndRowSet`, which walks `getRowGroupReaders()` — **this location's own**
row groups, numbered from zero. `blocks` is `parquetMetadata.getBlocks()`. Those two agree only while
`parquetMetadata` describes this file alone.

Read through a `_metadata` file it describes the **whole dataset**. Instrumenting the failing case makes
it plain — five blocks in the metadata, three single-row-group locations, and every one of them asking
for block 0:

```
[DBG] blocks=5 rgRows=41 rg=0 rows=28 numNulls=0 min=0 max=4000000000 -> maybeOverlaps=false
[DBG] blocks=5 rgRows=41 rg=0 rows=38 numNulls=0 min=0 max=4000000000 -> maybeOverlaps=false
[DBG] blocks=5 rgRows=41 rg=0 rows=34 numNulls=0 min=0 max=4000000000 -> maybeOverlaps=false
```

Three different locations, three different row counts, one identical set of statistics: the first file's.
Its `numNulls=0` was then used to prove that a *different* partition held no nulls, and the row group
holding the null was dropped.

The class already carries the translation. `rowGroupIndices` — from
`ParquetTableLocationKey.getRowGroupIndices()` — is exactly "which of the shared metadata's row groups
belong to this location", and both other readers of the metadata use it: `initialize()` maps through it
to build `rowGroups`, and `getRowGroupReaders()` maps through it to build the readers. Only this path
indexed the shared list directly.

A second detail made the naive fix wrong: `getRowGroupReaders()` **nulls out `rowGroupIndices`** once the
readers are built, and it sorts both the row groups and the readers by `RowGroup::getOrdinal`. So the
translation has to be captured during `initialize()`, in ordinal order, to match the positions
`iterateRowGroupsAndRowSet` produces.

## Why this went unnoticed

- The statistics read are a real row group's, just the wrong one, so nothing is malformed and no check
  fires. `UsabilityEvaluator` and `NullAwareEvaluator` both behaved correctly on the input they were
  given — they were simply given another file's facts.
- With no `_metadata` file, `parquetMetadata` is per file, `rowGroupIndices` is the identity, and the
  bug is invisible. The same is true of a single-file dataset, and of the first location of any dataset.
- It needs the mis-read statistics to actually exclude the filter. Partitions whose value ranges overlap
  usually mask it; the fuzzer hit it because a `SPARSE` null column puts the only null in one partition
  while every other partition truthfully reports `numNulls=0`.

## Fix

`extensions/parquet/table/src/main/java/io/deephaven/parquet/table/location/ParquetTableLocation.java`:

- New field `rowGroupBlockIndices`, computed in `initialize()` as `rowGroupIndices` sorted by the
  corresponding row group's ordinal — the order `getRowGroupReaders()` presents — and **retained**, unlike
  `rowGroupIndices`, because the pushdown path needs it for the life of the location.
- `pushdownRowGroupMetadata` now reads `blocks.get(rowGroupBlockIndices[rgIdx])`.

## Verification

- `MetadataFileRowGroupStatisticsTest`, 8 tests over a five-partition dataset in which each partition
  holds a **disjoint** value band and only the last contains nulls, so reading a neighbour's statistics
  can never be accidentally harmless. **5 fail before the fix, and the 3 controls pass**: the whole table
  still reachable, absent values still matching nothing (so the fix is not just "keep every row group"),
  and the same dataset without `_metadata` unaffected.
- Two of those tests were deliberately strengthened after they passed against the broken code: the range
  test now applies its bounds as two `where` calls, because as one string they parse to a formula
  `ConditionFilter` that the metadata action never serves; and the slice test now also slices into the
  last partition, since the fuzz case's own `slice(size/4, size - size/4)` misses the partition holding
  the nulls.
- The fuzzer seed passes, as do all recorded interesting seeds.
- The full `:extensions-parquet-table` `test` and `testOutOfBand` suites pass.

## Relationship to DH-23488 / PR #8425

That PR reworked this same statistics pushdown and is already an ancestor of this branch. It is not the
cause and does not contain the fix: the defect is in the *indexing of the block list* in
`ParquetTableLocation`, not in the handlers or evaluators it rewrote, all of which behaved correctly on
the statistics they were handed. Its new `PushdownHandlerNullStatisticsTest` covers null statistics at
the handler level, which is a layer below the point where the wrong row group is selected.
