# Finding 9 — parquet-space names leaked into two table-space APIs, dropping rows

**Severity:** high — silent wrong results (rows dropped) from `readTable` plus a filter, reproducible
with **all pushdown disabled**; and, at the second site, an exception from a query that should work.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/RenamedSortedColumnPushdownTest.java`
**Fuzzer seeds (3, all now passing):** `-5347797226962475569L`, `-6688467811848818630L`,
`-7423979211207825555L`

## Symptom

```
filtered results differ: Result table has size 17 vs. expected 28
```

11 of 28 rows silently dropped. The case had a sorted `Instant` column and a read-time
`addColumnNameMapping` that **permutes** names. Reduced to six lines:

```java
// storage: A sorted ascending, B unsorted. The file records sorting column "A".
ParquetTools.writeTable(TableTools.newTable(
        TableTools.intCol("A", 1, 2, 3, 4),
        TableTools.doubleCol("B", -0.0, -1.0, -2.0, -3.0)).sort("A"), file);

// rotate the names: table A is storage B (the unsorted doubles), table B is storage A
final Table disk = ParquetTools.readTable(file, new ParquetInstructions.Builder()
        .addColumnNameMapping("A", "B")
        .addColumnNameMapping("B", "A")
        .build());

disk.coalesce();                  // claimed SortedColumns={A=Ascending} -- A is [-0.0 .. -3.0]
disk.where("A <= -1.0").size();   // 0; the correct answer is 3
```

## Root cause

A parquet file's Deephaven metadata and its parquet schema are written in **parquet** name space.
`ParquetTableLocation`'s public API is in **table** name space — `makeColumnLocation` takes a
table-space name and translates it to parquet space itself. Two places crossed that boundary without
translating.

### Site 1 — the recorded sorting column was never translated

```java
sortingColumns = SortColumnInfo.sortColumns(tableInfo.sortingColumns());
```

straight from the file's `TableInfo`, so `TableLocation.getSortedColumns()` returned parquet-space
names. Both of its consumers read them as table-space:

1. `SourceTable.doCoalesce` calls
   `SortedColumnsAttribute.setOrderForColumn(resultTable, sc.column().name(), order)` — publishing the
   parquet name as the coalesced table's `SORTED_COLUMNS_ATTRIBUTE`. That is a user-visible false claim
   *and* it drives `AbstractRangeFilter`'s binary search, which consults
   `SortedColumnsAttribute.getOrderForColumn` **with no pushdown flag gating it**. So rows are dropped
   even with every pushdown switch off; this is not only a pushdown defect.
2. Every `ParquetColumnRegion*.estimatePushdownAction` compares `firstSortedColumn.column().name()`
   against `ctx.filterColumnToManagerColumnName()`'s output, which is table-space. Its
   `// Need to handle column renames` comment is accurate about the `renameColumns` rename and silent
   about the read-time one.

**Why a permutation was needed to see it.** Under a *uniform* rename the untranslated parquet name
matches no table column, the claim is declined, and the answer is right by accident. Only a
permutation leaves the stale name valid while pointing at a different, unsorted column. Both
pre-existing rename tests — `ParquetTableFilterTest.flatPartitionsColumnRenameTest` and
`flatPartitionsInstructionColumnRenameTest` — rename every column with a uniform `_renamed` suffix,
which is exactly the shape under which the wrong code returns the right answer.

### Site 2 — the dictionary path looked up a location by parquet name

```java
final ParquetColumnLocation<Values> columnLocation =
        (ParquetColumnLocation<Values>) getColumnLocation(parquetColumnName);
```

in `hasDictionaryPage`, and identically in `pushdownFilterDictionary`. `getColumnLocation` translates
table → parquet, so handing it a parquet name translates twice and lands on a different physical
column. `hasDictionaryPage` is reached from `estimatePushdownAction`, so a query fails merely from the
dictionary action being *considered*. With the permuted types above the wrong column cannot convert:

```
TableDataException: Unexpected exception accessing column A
  caused by: IllegalArgumentException: Cannot convert parquet int column to double
```

Both call sites already had the column's `ColumnDefinition` in hand, whose name is the table-space one
they needed.

## Fix

[`ParquetTableLocation`](../../../../../../../main/java/io/deephaven/parquet/table/location/ParquetTableLocation.java)

**Site 1** — a new `translateSortingColumns`, applied in `initialize`, maps each sorting column's name
through `readInstructions.getColumnNameFromParquetColumnName`. One change corrects both consumers,
and this is the only consumer-visible place holding the `ParquetInstructions`.

A sorting column with no table-space name is **dropped** rather than passed through. Passing it
through is only safe when a table column of that name would read that same parquet column; otherwise
the name denotes something else, and a sortedness claim about the wrong column silently drops rows.
Claiming no sort order merely gives up an optimization. `SortColumnInfo` records only the first sorted
column, so this is one mapping per location, not a hot path.

**Site 2** — both lookups now use `columnDefinition.getName()`. `hasDictionaryPage`'s parquet-name
parameter is gone, and `pushdownFilterDictionary` no longer takes `parquetColumnNames`, since the
lookup was its only use.

**Documentation** — `TableLocation.getSortedColumns()`'s javadoc now states the name space, and that
an implementation must omit any sort column it cannot translate. Its silence is what let a producer
and two consumers disagree.

## Verification

- `RenamedSortedColumnPushdownTest`, 11 tests: a range filter and a match filter under a permuted
  rename, each compared against `select()`; that the coalesced table makes no false sort claim; that
  the claim *follows* the rename to the column that really is sorted, so the optimization is not
  simply abandoned; no rename; a uniform rename, which now keeps its claim instead of losing it; a
  partial rename where the sorted parquet column is unmapped and its name taken by another column, so
  the claim must be dropped; descending order surviving translation; and the site-2 cases —
  a conjunction over both permuted columns, a multi-value match, and String columns where a dictionary
  really is written.
- Reverting both fixes fails 8 of the 11; reverting only site 2 fails exactly the range and match
  filter tests. Both fixes are load-bearing.
- Full `:extensions-parquet-table:test`, `:extensions-parquet-base:test` and `:engine-table:test` pass.
- All 3 fuzzer seeds pass and join `INTERESTING_SEEDS`, including `-6688467811848818630L`, which
  `OLD_FINDINGS.md` recorded as its finding 4, and `-7423979211207825555L`, its finding 1's
  parquet-based variant.

## A note on scope

`OLD_FINDINGS.md` finding 4 had already located site 1 and proposed this fix; that analysis is
confirmed here against the current code, which `DH-23488` had rewritten in the meantime. Site 2 was
not previously known — it only became reachable once site 1 stopped failing first.
