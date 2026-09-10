# Finding 5 — a partitioning column's type was inferred from its directory names, so it did not round trip

**Severity:** high — silent type change on an ordinary write-then-read, and for two value shapes
outright data loss. A filter written against the original type then matches nothing, with no error.
**Status:** **FIXED**, with regression test.
**Repro test:** `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/PartitioningColumnTypeRoundTripTest.java`
**How it was found:** not by a fuzzer seed directly — it broke an assertion in finding 4's own test,
where `where("Key == \`a\`")` matched **zero** of two rows whose `Key` was `"a"`.

## Symptom

```java
ParquetTools.writeKeyValuePartitionedTable(TableTools.newTable(
        TableTools.stringCol("Key", "a", "a", "b"),
        TableTools.intCol("Value", 1, 2, 3)).partitionBy("Key"), dir, ParquetInstructions.EMPTY);

final Table disk = ParquetTools.readTable(dir);
disk.size();                        // 3, correct
disk.where("Key == `a`").size();    // 0 -- the correct answer is 2
```

`Key` was written as a `String` and came back as a `char`. Comparing a `char` column to a `String`
literal matches nothing, and raises nothing.

Measured across value shapes, all written as a `String` column:

| written keys | read back as | note |
| --- | --- | --- |
| `"aa"`, `"bb"` | `String` | correct by luck |
| `"a"`, `"b"` | `char` | |
| `"1"`, `"2"` | `int` | |
| `"true"`, `"false"` | `Boolean` | |
| `"1.5"`, `"2.5"` | `double` | |
| `"01"`, `"02"` | `int` → **`1`, `2`** | value lost; cannot reproduce its own directory name |
| `"99999999999999999999"` | `double` | precision lost |

The last two are data loss rather than a type change: the value read back no longer formats to the
directory it was read from. And because the inference is content-directed, the schema depends on the
data — one day's partition keys can infer differently from the next day's, so an append can change
the type of an existing column.

## Root cause

A partitioning column's values live in the directory path, not in any file's parquet schema. Given no
explicit `TableDefinition`, `ParquetKeyValuePartitionedLayout` used `LocationTableBuilderCsv`, which
assembles the key strings into a CSV document and hands it to `CsvTools.readCsv` **for type
inference**. `ParquetTools.infer` then took the partitioning columns' types from the classes of the
values that came back.

Text-directed inference is the right thing for a dataset Deephaven did not write — there is nothing
else to go on. The defect is that it was used for datasets Deephaven *did* write, where the type was
known at write time and simply discarded. `writeKeyValuePartitionedTableImpl` computes a
`keyTableDefinition` holding exactly the right types, and used it only to build the
`_common_metadata` schema — and only when `generateMetadataFiles` was set. That is why enabling
metadata files made every case above correct: `readTable` then picks the metadata-driven layout and
gets a real definition. Without it, the type was written nowhere.

The layout's `@implNote` documented the *mechanism* ("type inference ... uses `CsvTools.readCsv` ...
and hence follows the same rules") but not the consequence, that a written column does not read back
as itself.

## Fix

**Record the type.** A new
[`PartitioningColumnInfo`](../../../../../../../main/java/io/deephaven/parquet/table/metadata/PartitioningColumnInfo.java)
(`columnName`, `dataType`) and a new `TableInfo.partitioningColumns()` list. Each leaf file of a
key-value partitioned write now records its partitioning columns and their types;
`writeKeyValuePartitionedTableImpl` passes its `keyTableDefinition` down through `writeTablesImpl`
into `ParquetTableWriter.write`, which adds them to the `TableInfo` builder beside the sorting
columns.

**Use it.** `ParquetKeyValuePartitionedLayout`'s inference path reads the **first** discovered file's
`TableInfo`; when it records partitioning columns, the layout uses `LocationTableBuilderDefinition`
with those types instead of the CSV builder. That builder and its `PartitionParser`s already existed
for the explicit-definition path — this just supplies the definition from the data.

Supporting changes:

- `URIStreamKeyValuePartitionLayout` gained a `protected findKeys(Stream, LocationTableBuilder,
  Consumer)` overload. The builder was previously fixed at construction by a `Supplier`, but this
  choice depends on the files being traversed. The existing two-argument `findKeys` delegates to it,
  so no shared mutable state and a fresh builder per call, as before.
- The stream is not collected. The first URI is pulled off the iterator and the stream rebuilt with
  `Stream.concat`, so a large dataset does not pay for a second copy of every URI — one extra footer
  read in total.
- `PartitioningColumnInfo.columnDefinition()` resolves `dataType()` through an allow-list built from
  `PartitionParser`, not `Class.forName`. File metadata should not be able to name an arbitrary class,
  and a type with no partition-value parser could not be read back anyway.
- `describesPartitionsOf` checks the recorded column names against the first path's actual directory
  keys before committing to them. `LocationTableBuilderDefinition` throws on a mismatch, which would
  turn a dataset whose directories were reorganized after writing into a hard read failure; checking
  first keeps inference as the fallback.

## Compatibility

Additive and safe in both directions:

- **Old reader, new file.** `TableInfo`'s Jackson mapper disables `FAIL_ON_UNKNOWN_PROPERTIES`, so an
  older reader ignores the new field.
- **New reader, old file.** `partitioningColumns()` is empty, and the reader falls back to CSV
  inference exactly as before.
- **Unpartitioned writes are byte-identical.** `TableInfo` is annotated
  `@JsonInclude(NON_EMPTY)`, so an empty list is not serialized at all.

Inference behaviour for datasets without the field is deliberately **unchanged**, including the
`"01"` → `1` case. For a foreign dataset there is no evidence that the key was ever a string, and
someone partitioning by a zero-padded integer may well want the integer. `datasetWithoutRecordedTypes
StillInfers` pins that fallback down, `char` and all.

## Verification

- `PartitioningColumnTypeRoundTripTest`, 8 tests: all six `String`-key shapes round trip as `String`;
  the two data-losing shapes keep their exact values; the single-character case is filterable as a
  string, before and after `select()`; `int`/`long`/`char`/`Boolean`/`double`/`LocalDate` keys keep
  their own types and values, asserted against each source's own definition; `_common_metadata` and an
  explicitly supplied definition still work; and a dataset with no recorded types still infers.
- Full `:extensions-parquet-table:test`, `:extensions-parquet-base:test` and `:engine-table:test`
  pass.

### Follow-up: an existing test pinned the old behaviour

`ParquetTableReadWriteTest.testAllPartitioningColumnTypes` is an `OutOfBandTest`, so it is not in the
`test` task that was run above, and it was missed at the time. It asserted the defect:

```java
// Verify that we can read the partition values, but types like LocalDate or LocalTime will be read as
// strings, and byte, short will be read as integers. Therefore, we cannot compare the tables directly
assertNotEquals(fromDiskPartitioned.getDefinition(), inputData.getDefinition());
```

That is a read with an explicit `KV_PARTITIONED` layout, which bypasses the metadata files. It now
recovers the correct types anyway, because this fix records the partitioning columns in **each data
file's** schema metadata rather than only in `_metadata`, so the directory names are no longer the only
evidence. The assertion was inverted to `assertEquals` and the data compared as well; the stale comment
was replaced. This is the same situation as finding 2's `ZonedDateTimeCodecTest.testMax`, which also
asserted the behaviour being corrected.
