# Finding 18 — a data index read back with a different column type, so matches silently found nothing

**Severity:** high — silent wrong results, in both directions, from an indexed column and a negated
filter. Nothing throws, and the wrong answer is returned as an *exact* one.
**Status:** **FIXED** (the index is declined); the deeper fix is noted below.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/DataIndexTypeMismatchTest.java`
**Fuzzer seed:** `-7982720036514329702L`

## Symptom

```
filtered results differ: Result table has size 1 vs. expected 0
filter=not(or(Col0 in fuzzP1, Col0 == null, not(Col0 < fuzzP2)))
```

Reduced, on five `BigDecimal` values `-1, 1E-21, 1.000000000000000000000, 1E-21, null` written one per
file with a data index on the column:

```java
disk.where(RawString.of("Col0 in p1"));            // 1  correct
disk.where(Filter.not(RawString.of("Col0 in p1")));// 5  WRONG -- should be 4
disk.where(Filter.or(a, b));                       // 1  WRONG -- should be 2
```

## Root cause

`ParquetTableLocation.readDataIndexTable` reads the index file with **no** `TableDefinition`:

```java
final Table indexTable = ParquetTools.readTable(indexFileMetaData.fileURI.toString(),
        parquetInstructions.withTableDefinitionAndLayout(null, ParquetFileLayout.SINGLE_FILE));
```

so the index's column types are inferred from that file alone, and need not agree with the parent
table's. A `BigDecimal` column whose values happen to have **scale 0** is written as `DECIMAL(p, 0)`,
which infers back as `BigInteger`. Instrumenting the index tables per region showed it directly:

```
idxType=BigInteger  idxVals=[BigInteger(-1)]                      <- the -1 region
idxType=BigDecimal  idxVals=[BigDecimal(1E-21,scale=21)]
idxType=BigDecimal  idxVals=[BigDecimal(1.000000000000000000000,scale=21)]
```

`Col0 in [BigDecimal("-1")]` matched against a `BigInteger` column compares
`BigDecimal.equals(BigInteger)`, which is **always false**. So the index reported that nothing
matched, and `pushdownDataIndex` returned that with an empty maybe-set — an exact claim. Under a
negation every row then appeared to match; in a disjunction the row was simply lost.

Two things made this hard to see:

- The **un-negated** `Col0 in p1` was correct, because a cheaper action resolved that region before
  the index was consulted. Only the negated and disjunctive forms exposed it.
- The mixed scales are what create the mismatch, and they arise naturally from writing one file per
  value — each file computes its own decimal precision and scale. A uniform-scale column indexes and
  matches fine.

This is the **silent** form of the mismatch `DH-19443` is about. The `catch` in `pushdownDataIndex`
was written for that concern, and cannot see this one: nothing throws.

## Fix

[`ParquetTableLocation`](../../../../../../../main/java/io/deephaven/parquet/table/location/ParquetTableLocation.java)
gains `indexTypesMatchFilterColumns`, checked before the index is used: if any filter column's data
type or component type differs from the index column's, the action declines and returns the input
result unchanged, so the filter is evaluated for real.

```java
if (!indexTypesMatchFilterColumns(dataIndex, filterColumnDefinitions, renameMap)) {
    return result.copy();
}
```

The filter columns' definitions come from `filterCtx.columnDefinitions()`, which both call sites
already had.

**The better fix, deliberately not attempted here:** `readDataIndexTable` should pin the index
columns to the parent table's types, so the index stays *usable* rather than merely safe. That means
plumbing the parent's column definitions into the location's index read, which reaches beyond this
defect and belongs with `DH-19443`. What is fixed here is that a type mismatch can no longer produce a
wrong answer — it costs an optimization instead.

## Verification

- `DataIndexTypeMismatchTest`, 7 tests: the negated match, the plain match that was right by luck, a
  disjunction, the full nested shape the fuzzer generated, a conjunction of negations, a
  uniform-scale `BigDecimal` column whose index still works, and an `int` column where the types
  always agree. All compared against the same values held in memory. Four fail without the fix.
- Full `:engine-table:test`, `:extensions-parquet-table:test` and `:extensions-parquet-base:test`
  pass.
- Seed `-7982720036514329702L` passes and joins `INTERESTING_SEEDS`.

## Note

An earlier draft of the guard used `indexTable.getColumnSource(name, null)`, which fails
`Requirement failed: required clazz != null` — turning a wrong answer into an exception. The check
reads `TableDefinition.getColumn` instead, which returns `null` for an absent column and needs no
type token.
