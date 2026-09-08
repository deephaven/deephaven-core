---
title: writeKeyValuePartitionedTable
---

The `writeKeyValuePartitionedTable` method writes a table to disk in Parquet format with partitioning columns written as `key=value` directories. For example, for a partitioning column `Date`, this creates a directory structure like `Date=2021-01-01/<base_name>.parquet`, `Date=2021-01-02/<base_name>.parquet`, etc.

## Syntax

```groovy syntax
ParquetTools.writeKeyValuePartitionedTable(sourceTable, destinationDir, writeInstructions)
ParquetTools.writeKeyValuePartitionedTable(partitionedTable, destinationDir, writeInstructions)
```

## Parameters

<ParamTable>
<Param name="sourceTable" type="Table">

A table with partitioning columns defined in its table definition. The method calls `partitionBy` internally using these columns to create the nested directory structure. This is an advanced overload — most users should use the `PartitionedTable` overload by calling `partitionBy` first.

</Param>
<Param name="partitionedTable" type="PartitionedTable">

A [partitioned table](../../table-operations/group-and-aggregate/partitionBy.md) to write. The partitioned table's key columns are used to create the nested directory structure.

</Param>
<Param name="destinationDir" type="String">

The path or URI to the destination root directory to store partitioned data in nested format. Non-existing directories are created.

</Param>
<Param name="writeInstructions" type="ParquetInstructions">

Instructions for customizations while writing. Build using `ParquetInstructions.builder()` with options including:

- `setBaseNameForPartitionedParquetData(String)` — Sets the base name for individual partitioned files. Supports tokens:
  - `{uuid}` — Replaced with a random UUID
  - `{partitions}` — Replaced with underscore-delimited partition values
  - `{i}` — Replaced with an auto-incremented integer
- `setGenerateMetadataFiles(boolean)` — Whether to generate `_metadata` and `_common_metadata` files
- `addIndexColumns(String...)` — Add index columns to write as sidecar tables
- `setTableDefinition(TableDefinition)` — Set a custom table definition
- Compression options: `setCompressionCodecName("SNAPPY")`, `"GZIP"`, `"ZSTD"`, `"LZ4_RAW"`, `"BROTLI"`, `"UNCOMPRESSED"`

Use `ParquetInstructions.EMPTY` for default settings.

</Param>
</ParamTable>

## Returns

None. Writes partitioned Parquet files to the specified directory.

## Examples

> [!NOTE]
> All examples in this document write data to the `/data` directory in Deephaven. For more information on this directory and how it relates to your local file system, see [Docker data volumes](../../../conceptual/docker-data-volumes.md).

### Write a partitioned table

In this example, `writeKeyValuePartitionedTable` writes a partitioned table with the `X` column as the partitioning key:

```groovy
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions

source = newTable(
    stringCol("X", "A", "B", "B", "C", "B", "A", "B", "B", "C"),
    intCol("Y", 2, 4, 2, 1, 2, 3, 4, 2, 3),
    intCol("Z", 55, 76, 20, 4, 230, 50, 73, 137, 214),
)

partitionedSource = source.partitionBy("X")

ParquetTools.writeKeyValuePartitionedTable(partitionedSource, "/data/partitioned_output", ParquetInstructions.EMPTY)
```

This creates:

```
/data/partitioned_output/
├── X=A/
│   └── <uuid>.parquet
├── X=B/
│   └── <uuid>.parquet
└── X=C/
    └── <uuid>.parquet
```

### Write with custom base name

In this example, `writeKeyValuePartitionedTable` uses a custom base name:

```groovy
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions

source = newTable(
    stringCol("X", "A", "B", "B", "C", "B", "A", "B", "B", "C"),
    intCol("Y", 2, 4, 2, 1, 2, 3, 4, 2, 3),
    intCol("Z", 55, 76, 20, 4, 230, 50, 73, 137, 214),
)

partitionedSource = source.partitionBy("X")

writeInstructions = ParquetInstructions.builder()
    .setBaseNameForPartitionedParquetData("data-{i}")
    .build()

ParquetTools.writeKeyValuePartitionedTable(partitionedSource, "/data/partitioned_output", writeInstructions)
```

This creates files like `X=A/data-0.parquet`, `X=B/data-0.parquet`, etc.

### Write with metadata files

Generate metadata files to speed up reading:

```groovy
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions

source = newTable(
    stringCol("X", "A", "B", "B", "C", "B", "A", "B", "B", "C"),
    intCol("Y", 2, 4, 2, 1, 2, 3, 4, 2, 3),
    intCol("Z", 55, 76, 20, 4, 230, 50, 73, 137, 214),
)

partitionedSource = source.partitionBy("X")

writeInstructions = ParquetInstructions.builder()
    .setBaseNameForPartitionedParquetData("data")
    .setGenerateMetadataFiles(true)
    .build()

ParquetTools.writeKeyValuePartitionedTable(partitionedSource, "/data/partitioned_output", writeInstructions)
```

This creates the partitioned files plus `_metadata` and `_common_metadata` files in the root directory.

## Related documentation

- [Read Parquet files](../../../how-to-guides/data-import-export/parquet-import.md)
- [Write Parquet files](../../../how-to-guides/data-import-export/parquet-export.md)
- [`writeTable`](./writeTable.md)
- [`readTable`](./readTable.md)
- [`partitionBy`](../../table-operations/group-and-aggregate/partitionBy.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#writeKeyValuePartitionedTable(io.deephaven.engine.table.PartitionedTable,java.lang.String,io.deephaven.parquet.table.ParquetInstructions))
