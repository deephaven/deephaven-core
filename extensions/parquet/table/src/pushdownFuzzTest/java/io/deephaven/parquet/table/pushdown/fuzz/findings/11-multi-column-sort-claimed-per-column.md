# Finding 11 — every column of a multi-column sort was claimed as independently sorted, over-returning rows

**Severity:** high — silent wrong results: a filter reported a non-matching row as a definite match.
Reachable from `readTable` plus a filter, and not gated by any pushdown switch.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/MultiColumnSortedAttributeTest.java`
**Fuzzer seed:** `1681357320861610709L` — recorded as finding **7a** in `OLD_FINDINGS.md`, open since that
round and described there as "the most serious of the four and the one to reduce first".

## Symptom

```
filtered results differ: Result table has size 7 vs. expected 6
```

The extra row satisfied **neither** disjunct of `or(Col3 >= 55.5658, Col2 != 1970-01-01)`: it had
`Col3 = 8.09598` and `Col2 = 1970-01-01`. The disk path returned it as a match.

## Root cause

`SourceTable.doCoalesce` published *every* sorted column a location reported:

```java
for (final SortColumn sc : includedLocations.iterator().next().getSortedColumns()) {
    SortedColumnsAttribute.setOrderForColumn(resultTable, sc.column().name(), order);
}
```

But `TableLocation.getSortedColumns()` is, in its own javadoc, *"ordered by precedence, representing a
multi-column sort"*. A multi-column sort orders each subsequent column only **within ties** of the
columns before it — so only the leading column is sorted on its own. Publishing the rest as
independent `SortedColumnsAttribute` entries asserts something false, and nothing validates it:
`AbstractRangeFilter` consults `SortedColumnsAttribute.getOrderForColumn` and binary-searches, with
**no pushdown flag gating it**.

### Getting there

The only producer of multi-column sorting metadata is the **parquet data-index writer**. A composite
index on `(A, B)` is written `sort(A, B)` and records both columns as ascending:

```java
final TableInfo.Builder indexTableInfoBuilder = TableInfo.builder().addSortingColumns(
        info.indexColumnNames.stream()
                .map(cn -> SortColumnInfo.of(cn, SortColumnInfo.SortDirection.Ascending))
                .toArray(SortColumnInfo[]::new));
```

(A plain `sort("A","B")` write records only the first, because `ParquetTableWriter` already notes that
`SortedColumnsAttribute` cannot express a multi-column sort. That is why the defect is reachable only
through an index.)

Reading that index table back published `SortedColumns={A=Ascending, B=Ascending}` — and `B` is
plainly not ascending. The index rows, in order, were:

| A | B |
| --- | --- |
| 1900-01-01 | 9.19012 |
| 1900-03-01 | 3004.23000 |
| 1970-01-01 | **8.09598** |
| 1970-01-01 | 116031.00000 |
| 2000-01-01 | **55.56580** |
| 2024-02-29 | 89.85440 |
| 2024-02-29 | 689.79600 |

`AbstractRangeFilter` then binary-searched `B >= 55.5658` as if `B` were ascending, found the first
value at or above the bound (row 1, `3004.23`) and took everything from there on — six index rows
instead of five, wrongly including row 2.

`ParquetTableLocation.pushdownDataIndex` turned that into a wrong answer for the data: it applies the
filter to the index table and returns the result with an **empty maybe-set**, claiming exactness. So
the union of the two disjuncts covered all seven index rows, and all seven data rows were reported as
definite matches.

## Fix

[`SourceTable.doCoalesce`](../../../../../../../../../../../engine/table/src/main/java/io/deephaven/engine/table/impl/SourceTable.java)
publishes only the leading column:

```java
final List<SortColumn> sortedColumns = includedLocations.iterator().next().getSortedColumns();
if (!sortedColumns.isEmpty()) {
    final SortColumn sc = sortedColumns.get(0);
    ...
    SortedColumnsAttribute.setOrderForColumn(resultTable, sc.column().name(), order);
}
```

The claim about the leading column is true and is kept, so single-column indexes and ordinary sorted
files keep their optimization. The two other consumers of `getSortedColumns()` — the
`ParquetColumnRegion*` handlers and `SortedColumnPushdownManager` — already took only `get(0)`, so
`SourceTable` was the outlier.

Note this is the same *class* of defect as finding 9, and in the same loop: a sortedness claim about a
column that is not sorted, acted on without validation. Finding 9 was the wrong **name**; this is the
wrong **count**.

## How it was localized

Reduction by construction failed repeatedly — the data, the query-scope parameters, the
merge-then-head shape and the pushdown toggles all reproduced clean. What found it was adding a
`PushdownFuzzer.forceDisable` knob to the bench, which forces individual pushdown switches off for a
replayed seed. Disabling `dataIndex` or `merged` made the seed pass while `stats`, `sorted` and
`dictionary` did not, which pointed at `pushdownDataIndex`; instrumenting it showed the index table's
`where` returning 7 of 7 where `select().where` returned 6, and its attribute map showed
`SortedColumns=A=Ascending,B=Ascending`. That knob is kept — it turns "which action is wrong?" from
guesswork into one command per action.

## Verification

- `MultiColumnSortedAttributeTest`, 3 tests: the disjunction over a composite data index; each
  disjunct alone, which were always correct, so a regression stays localized; and a single-column
  index, whose claim is true and must survive. Every case is checked against the same filter applied
  after `select()`. Reverting the fix fails `disjunctionOverACompositeDataIndex`.

  A direct assertion on the index table's attribute would be the sharper test, but
  `DataIndexer.getDataIndex(...).table()` returns a table without it — the claim is carried by the
  location-level index that `ParquetTableLocation.readDataIndexTable` produces, which has no public
  accessor. Worth revisiting if one appears.
- The end-to-end tests set `DATA_INDEX_FOR_WHERE_THRESHOLD` explicitly, because
  `pushdownDataIndex` returns early unless the maybe-set exceeds `indexSize / threshold` and the
  default hides the defect on a small table. The fuzz case's toggle profile did the same thing.
- Full `:engine-table:test`, `:extensions-parquet-table:test` and `:extensions-parquet-base:test` pass.
- Seed `1681357320861610709L` passes and joins `INTERESTING_SEEDS`.
