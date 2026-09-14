# Finding 1 — an explicit `RowGroupInfo.maxRows` on a zero-row table fails the write

**Severity:** medium — a plain `writeTable` of an empty table throws, for a row-group setting that is
legal on every non-empty table.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/EmptyTableRowGroupInfoTest.java`
**Fuzzer seed:** `-5347797226962475569L` (`maxTableSize=1000`)
**Share of the run that found it:** 40 of the first 100 failures.

## Symptom

```
io.deephaven.UncheckedDeephavenException: Error writing parquet tables
    caused by: java.lang.IllegalArgumentException: Number of groups must be at least 1, got: 0
```

Reduced to three lines, with no fuzzer machinery:

```java
final Table empty = TableTools.newTable(TableTools.intCol("Value"), TableTools.stringCol("Key"));
ParquetTools.writeTable(empty, dest,
        new ParquetInstructions.Builder().setRowGroupInfo(RowGroupInfo.maxRows(3)).build());
```

## Root cause

`RowGroupTableIteratorVisitor.splitByMaxRows` derived the group count arithmetically:

```java
final long numRowGroups = (input.size() / maxRows) + ((input.size() % maxRows) > 0 ? 1 : 0);
return splitByMaxGroups(input, numRowGroups);
```

For `input.size() == 0` that is `0 + 0 == 0`, and `splitByMaxGroups` rejects anything below one.

Neither number came from the caller. `RowGroupInfo.MaxRows`'s constructor already rejects
`maxRows <= 0` and `RowGroupInfo.MaxGroups`'s rejects `numRowGroups < 1`, so the guard in
`splitByMaxGroups` was unreachable from user input — it fired only on this internally-computed zero.
The `IllegalArgumentException` therefore blamed the caller for a value the caller never supplied.

The inconsistency is sharper against the sibling paths, none of which had a problem with an empty
table:

| `RowGroupInfo` | empty table, before the fix |
| --- | --- |
| `singleGroup()` | one group, containing zero rows |
| `maxGroups(n)` | zero groups (`SplitEvenlyIterator` clamps `numRowGroups` to `size`) |
| `byGroups(...)` | zero groups (`partitionBy` yields no constituents) |
| `maxRows(n)` | **throws** |

And the group count never mattered anyway: `ParquetTableWriter.write` is `if (nRows > 0)`, so a
zero-row group writes nothing. Every one of those four settings produces the same file — a valid
parquet file with no row groups — and only `maxRows` failed to get there.

## Fix

[`RowGroupTableIteratorVisitor.java`](../../../../../../../../main/java/io/deephaven/parquet/table/RowGroupTableIteratorVisitor.java)

`splitByMaxRows` now short-circuits an empty input to a single group, alongside the existing
`maxRows == Long.MAX_VALUE` short-circuit, so it matches `singleGroup()`:

```java
if (maxRows == Long.MAX_VALUE || input.isEmpty()) {
    return List.of(input).iterator();
}
```

`splitByMaxGroups`'s guard became an `IllegalStateException` with a comment recording why it is now
unreachable. It is an internal invariant, not caller validation, and typing it as
`IllegalArgumentException` is what made the original failure read as a user error.

This also removes a latent hazard in `SplitByGroupsIterator.next()`, which calls
`splitByMaxRows(subTable, ...)` and then `subIter.next()` unconditionally: `splitByMaxRows` now
always returns a non-empty iterator.

## Verification

- `EmptyTableRowGroupInfoTest`, 7 tests: the split in isolation for `maxRows`/`maxGroups`; the
  end-to-end write and read-back for `maxRows`, `maxGroups` and `byGroups`; the fuzzer's own shape
  (an empty slice beside populated ones in a flat multi-file layout, filtered after read-back); and
  a non-empty `maxRows(3)` split over 7 rows, asserting the `3/2/2` grouping is unchanged.
- `TestRowGroupTableIteratorVisitor` still passes. Its `testBadParams` asserts against the
  `RowGroupInfo` constructors rather than `splitByMaxGroups`, so the retyped internal guard does not
  affect it.
- Seed `-5347797226962475569L` now writes and reads successfully. It does **not** yet pass: with the
  write unblocked it reaches a filter mismatch (17 rows vs. 28) under a permuting
  `addColumnNameMapping` over a sorted column, which is a separate root cause taken up in the next
  finding. The seed is added to `INTERESTING_SEEDS` once it passes end to end.
