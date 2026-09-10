# Finding 4 — a key-value partitioned write with no partitions crashed inside the writer

**Severity:** medium — an internal `ArrayIndexOutOfBoundsException` with no useful message, from a
legitimate input.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/EmptyPartitionedTableWriteTest.java`
**Fuzzer seed:** `-3203144279381597024L` (`maxTableSize=1000`)
**Share of the run that found it:** 7 of 29 failures.

## Symptom

```
parquet write failed: java.lang.ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0
```

```java
final Table empty = TableTools.newTable(TableTools.stringCol("Key"), TableTools.intCol("Value"));
ParquetTools.writeKeyValuePartitionedTable(empty.partitionBy("Key"), dir, ParquetInstructions.EMPTY);
```

## Root cause

`ParquetTools.writeTablesImpl` picks its channels provider from the first destination:

```java
.load(destinations[0].getScheme(), writeInstructions.getSpecialInstructions());
```

with nothing establishing that the array is non-empty. Every other caller happens to guarantee it —
the public `writeTables` rejects an empty array up front with *"No source tables provided for
writing"* — but `writeKeyValuePartitionedTableImpl` builds its destinations by iterating the
constituents of `partitionBy(...)`, which yields none for an empty table. It is the only path that
could hand zero destinations down, and it did so unguarded.

The failure was also reported as a write error for the whole table, giving no hint that emptiness was
the trigger.

## Fix

[`ParquetTools.writeTablesImpl`](../../../../../../../main/java/io/deephaven/parquet/table/ParquetTools.java)
returns early when there are no destinations:

```java
if (destinations.length == 0) {
    return;
}
```

**Why a no-op rather than an exception.** An incremental pipeline that partitions by day and writes
each day's rows must not fail on a day with no rows, and writing an empty table over an
already-populated directory must leave it intact. Both are covered by tests. Throwing would break
that composition for no benefit; there is genuinely nothing to write.

The `metadataRootDir == null` validation moved above the early return so it still applies, and
`generateMetadataFiles()` is now read once into a local. Metadata files are *not* written for an
empty partitioned table: `ParquetMetadataFileWriterImpl.writeMetadataFiles` rejects an empty file
list (*"No parquet files to write metadata for"*), and there are no row groups or file schemas to
merge.

## The read-back limitation is not a defect

With the crash gone, the write completes and leaves an empty directory. Reading it back still fails:

```
java.lang.IllegalArgumentException: Unable to infer schema for a partitioned parquet table
    when there are no initial parquet files
```

That is correct. `ParquetTools.infer` derives the schema — including the partitioning columns' types,
which come from parsing the directory keys — from a data file location key. The key-value layout
stores the schema nowhere else, so an empty directory carries none. The test asserts this message
specifically, so that the distinction between "clear contract" and "internal crash" stays recorded,
and a regression back to the `ArrayIndexOutOfBoundsException` fails the test.

Accordingly the fuzzer keeps `anyRows` out of the `PARTITIONED` layout, now documented as an API
precondition alongside the existing "at least one non-partitioning column" one, rather than as a
workaround for a bug.

## Verification

- `EmptyPartitionedTableWriteTest`, 5 tests: the empty write is a no-op, with and without
  `generateMetadataFiles`; the read-back reports missing schema rather than crashing; a non-empty
  partitioned write is unchanged; and an empty write over a populated directory leaves it readable.
- Full `:extensions-parquet-table:test` passes.
- Seed `-3203144279381597024L` no longer crashes the write. It does not pass: the case is an empty
  `PARTITIONED` table, so it now hits the schema-inference limitation above, which the fuzzer's
  generator constraint keeps it from producing in future runs.

## Found on the way

`nonEmptyPartitionedWriteIsUnchanged` initially asserted over a `String` partitioning column with
single-character values `"a"`/`"b"`, and `where("Key == \`a\`")` matched **zero** of the two rows. The
column had come back as `char`, not `String`, and the mismatched comparison silently matched nothing
instead of raising a type error. That is finding 5; this test uses multi-character keys to stay clear
of it.
