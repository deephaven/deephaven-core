---
title: write_partitioned
---

The `write_partitioned` method writes a table to disk in Parquet format with partitioning columns written as `key=value` directories. For example, for a partitioning column `Date`, this creates a directory structure like `Date=2021-01-01/<base_name>.parquet`, `Date=2021-01-02/<base_name>.parquet`, etc.

## Syntax

```python syntax
write_partitioned(
    table: Union[Table, PartitionedTable],
    destination_dir: str,
    table_definition: TableDefinitionLike = None,
    col_instructions: list[ColumnInstruction] = None,
    compression_codec_name: str = None,
    max_dictionary_keys: int = None,
    max_dictionary_size: int = None,
    target_page_size: int = None,
    base_name: str = None,
    generate_metadata_files: bool = None,
    index_columns: Sequence[Sequence[str]] = None,
    row_group_info: RowGroupInfo = None,
    special_instructions: s3.S3Instructions = None,
)
```

## Parameters

<ParamTable>
<Param name="table" type="Union[Table, PartitionedTable]">

The source table or [partitioned table](../../table-operations/group-and-aggregate/partitionBy.md) to write.

</Param>
<Param name="destination_dir" type="str">

The path or URI to the destination root directory in which the partitioned Parquet data is stored. Non-existing directories in the provided path are created.

</Param>
<Param name="table_definition" type="TableDefinitionLike" optional>

The table definition to use for writing, instead of the definitions implied by the table. Default is `None`, which uses the column definitions implied by the table. This definition can skip some columns or add additional columns with null values.

</Param>
<Param name="col_instructions" type="list[ColumnInstruction]" optional>

One or more optional [`ColumnInstruction`](./ColumnInstruction.md) objects that contain instructions for writing particular columns in the table.

</Param>
<Param name="compression_codec_name" type="str" optional>

The compression codec to use. See the [`write`](./writeTable.md) parameters for available options. If not specified, defaults to `SNAPPY`.

</Param>
<Param name="max_dictionary_keys" type="int" optional>

The maximum number of unique dictionary keys the writer is allowed to add to a dictionary page before switching to non-dictionary encoding. If not specified, defaults to 2^20 (1,048,576).

</Param>
<Param name="max_dictionary_size" type="int" optional>

The maximum number of bytes the writer adds to the dictionary before switching to non-dictionary encoding. Only evaluated for String columns. If not specified, defaults to 2^20 (1,048,576).

</Param>
<Param name="target_page_size" type="int" optional>

The target page size in bytes. If not specified, defaults to 2^20 bytes (1 MiB).

</Param>
<Param name="base_name" type="str" optional>

The base name for the individual partitioned files. If not specified, defaults to `{uuid}`, so files have names of the format `<uuid>.parquet`. The following tokens are available:

- `{uuid}` — Replaced with a random UUID. For example, `table-{uuid}` results in `table-8e8ab6b2-62f2-40d1-8191-1c5b70c5f330.parquet`.
- `{partitions}` — Replaced with an underscore-delimited, concatenated string of partition values. For example, for `{partitions}-table` with columns `PC1` and `PC2`, the result is `PC1=pc1_PC2=pc2-table.parquet`.
- `{i}` — Replaced with an auto-incremented integer for files in a directory. For example, `table-{i}` results in `PC=partition1/table-0.parquet`, `PC=partition1/table-1.parquet`, etc.

</Param>
<Param name="generate_metadata_files" type="bool" optional>

Whether to generate Parquet `_metadata` and `_common_metadata` files. Defaults to `False`. Generating these files speeds up reading partitioned data because they contain metadata (including schema) about the entire dataset.

</Param>
<Param name="index_columns" type="Sequence[Sequence[str]]" optional>

Sequence of sequences containing the column names for indexes to persist. The write operation stores the index info as sidecar tables. For example, `[["Col1"], ["Col1", "Col2"]]` stores indexes for `["Col1"]` and `["Col1", "Col2"]`. By default, indexes to write are determined by those present on the source table.

</Param>
<Param name="row_group_info" type="RowGroupInfo" optional>

Requested RowGroup instructions, as returned by a call to `RowGroupInfo`.

</Param>
<Param name="special_instructions" type="s3.S3Instructions" optional>

Special instructions for writing Parquet files to a non-local file system, like S3. Default is `None`.

</Param>
</ParamTable>

## Returns

None. Writes partitioned Parquet files to the specified directory.

## Examples

> [!NOTE]
> All examples in this document write data to the `/data` directory in Deephaven. For more information on this directory and how it relates to your local file system, see [Docker data volumes](../../../conceptual/docker-data-volumes.md).

### Write a partitioned table

In this example, `write_partitioned` writes a partitioned table with the `X` column as the partitioning key:

```python
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.parquet import write_partitioned

source = new_table(
    [
        string_col("X", ["A", "B", "B", "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, 2, 3]),
        int_col("Z", [55, 76, 20, 4, 230, 50, 73, 137, 214]),
    ]
)

partitioned_source = source.partition_by("X")

write_partitioned(partitioned_source, "/data/partitioned_output")
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

In this example, `write_partitioned` uses a custom base name with an incrementing index:

```python
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.parquet import write_partitioned

source = new_table(
    [
        string_col("X", ["A", "B", "B", "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, 2, 3]),
        int_col("Z", [55, 76, 20, 4, 230, 50, 73, 137, 214]),
    ]
)

partitioned_source = source.partition_by("X")

write_partitioned(partitioned_source, "/data/partitioned_output", base_name="data-{i}")
```

This creates files like `X=A/data-0.parquet`, `X=B/data-0.parquet`, etc.

### Write with metadata files

Generate metadata files to speed up reading:

```python
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.parquet import write_partitioned

source = new_table(
    [
        string_col("X", ["A", "B", "B", "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, 2, 3]),
        int_col("Z", [55, 76, 20, 4, 230, 50, 73, 137, 214]),
    ]
)

partitioned_source = source.partition_by("X")

write_partitioned(
    partitioned_source, "/data/partitioned_output", generate_metadata_files=True
)
```

This creates the partitioned files plus `_metadata` and `_common_metadata` files in the root directory.

## Related documentation

- [Import Parquet files](../../../how-to-guides/data-import-export/parquet-import.md)
- [Export Parquet files](../../../how-to-guides/data-import-export/parquet-export.md)
- [`write`](./writeTable.md)
- [`read_table`](./readTable.md)
- [`partition_by`](../../table-operations/group-and-aggregate/partitionBy.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write_partitioned)
