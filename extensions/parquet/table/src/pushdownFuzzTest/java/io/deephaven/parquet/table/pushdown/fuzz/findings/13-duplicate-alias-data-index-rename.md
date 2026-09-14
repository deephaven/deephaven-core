# Finding 13 — a filter naming an indexed column and an alias of it threw on the disk table

**Severity:** medium — a query that works in memory fails against the parquet-backed table, from an
`updateView` alias plus a data index.
**Status:** **FIXED**. Regression coverage is the two fuzzer seeds; see *Verification*.
**Fuzzer seeds (2, both now passing):** `428667830982598836L`, `6656699729815370963L` — part of the
cluster `OLD_FINDINGS.md` recorded as its finding 3.
**Coverage test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/DuplicateAliasDataIndexTest.java`

## Symptom

```
disk table threw but memory table did not:
  IllegalArgumentException: Duplicate source column(s): Col0
    at RenameColumnHelper.createLookupAndValidate
    at QueryTable.renameColumns
    at ParquetTableLocation.pushdownDataIndex
    at ParquetTableLocation.performPushdownAction        (DEFERRED_DATA_INDEX)
```

with the offending pairs, captured from the throw site:

```
[PairImpl{input=Col0, output=Col1}, PairImpl{input=Col0, output=dup0_Col1}]
```

## Root cause

`pushdownDataIndex` must present the data-index table's columns under the *filter's* names. It did that
by inverting `filterColumnToManagerColumnName` into rename pairs:

```java
final Collection<Pair> renamePairs = renameMap.entrySet().stream()
        .map(entry -> Pair.of(ColumnName.of(entry.getValue()), ColumnName.of(entry.getKey())))
        .collect(Collectors.toList());
toFilter = dataIndex.table().renameColumns(renamePairs);
```

That map is filter-name → manager-name, and it is **not injective**. When a filter references a column
and an `updateView` alias of it, both filter names denote the same underlying `ColumnSource`, so
`PushdownPredicateManager.computeRenameMap` maps both to the same manager column. Inverting then
produces two pairs with the same *source*, and `renameColumns` rejects that — correctly, since a rename
cannot produce two columns from one.

## Fix

[`ParquetTableLocation.pushdownDataIndex`](../../../../../../../main/java/io/deephaven/parquet/table/location/ParquetTableLocation.java)
projects with a `view` instead, which *can* name one source twice:

```java
final List<Selectable> projection = new ArrayList<>(renameMap.size() + 1);
renameMap.forEach((filterName, managerName) -> projection
        .add(Selectable.of(ColumnName.of(filterName), ColumnName.of(managerName))));
projection.add(ColumnName.of(dataIndex.rowSetColumnName()));
```

The row-set column is added explicitly, because a `view` drops what it does not name and the caller
reads that column immediately afterwards.

`view` is sequential, so a projection whose target shadows another entry's source would make the later
entry read the wrong column — the hazard of finding 10. That cannot arise from a duplicate alias, but it
can from a permuting rename, so it is detected and the index is declined rather than risked:

```java
final Set<String> sources = new HashSet<>(renameMap.values());
final boolean shadows = renameMap.entrySet().stream()
        .anyMatch(entry -> !entry.getKey().equals(entry.getValue()) && sources.contains(entry.getKey()));
if (shadows) {
    return result.copy();
}
```

Declining is safe and cheap here: the data index is only an optimization, and the method already
returns `result.copy()` on the type-mismatch path.

## Verification

- Fuzzer seeds `428667830982598836L` and `6656699729815370963L` fail without the fix and pass with it,
  and are added to `INTERESTING_SEEDS` so `testInterestingSeeds` replays them. **These are the
  regression test.**
- `DuplicateAliasDataIndexTest`, 8 tests, is *coverage* rather than a repro: an indexed column with one
  and two aliases, the alias alone, a conjunction with an unindexed column, a renamed indexed column,
  and a plain indexed filter — all checked against `select()`. They pass with and without the fix.

  Reaching the defect by hand needs the filter to arrive at the location still carrying two distinct
  names for one manager column, and `DeferredViewTable.applyFilterRenamings` normally renames it into
  manager space on the way down, which collapses them — `AbstractConditionFilter.getColumns()` applies
  `distinct()` after mapping. Several attempts to reconstruct the combination that avoids the collapse
  (rename before alias, coalesce before alias, two aliases, declarative vs. text filters) all reproduced
  clean. The seeds hit it deterministically, so they carry the regression and this class documents the
  neighbourhood.
- Full `:extensions-parquet-table:test` and `:engine-table:test` pass. (`TestEventDrivenUpdateGraph`
  failed once during this work and passes in isolation on a clean tree — a timing-sensitive
  performance-tracker test, unrelated.)
