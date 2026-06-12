---
title: write
---

The `write` method will write a table to a standard Parquet file.

## Syntax

```python syntax
write(
    table: Table,
    path: str,
    table_definition: Optional[TableDefinitionLike] = None,
    col_instructions: Optional[list[ColumnInstruction]] = None,
    compression_codec_name: Optional[str] = None,
    max_dictionary_keys: Optional[int] = None,
    max_dictionary_size: Optional[int] = None,
    target_page_size: Optional[int] = None,
    generate_metadata_files: Optional[bool] = None,
    index_columns: Optional[Sequence[Sequence[str]]] = None,
    row_group_info: Optional[RowGroupInfo] = None,
    special_instructions: Optional[s3.S3Instructions] = None,
)
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The table to write to file.

</Param>
<Param name="path" type="str">

Path name of the file where the table will be stored. The file name should end with the `.parquet` extension. If the path includes non-existing directories, they are created.

</Param>
<Param name="table_definition" type="TableDefinitionLike" optional>

The table definition to use for writing, instead of the definitions implied by the table. This definition can be used to skip some columns or add additional columns with null values.

</Param>
<Param name="col_instructions" type="list[ColumnInstruction]" optional>

One or more optional [`ColumnInstruction`](./ColumnInstruction.md) objects that contain instructions for how to write particular columns in the table.

</Param>
<Param name="compression_codec_name" type="str" optional>

The [compression codec](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.8.1/org/apache/parquet/hadoop/metadata/CompressionCodecName.html) to use.

Options are:

- `SNAPPY`: Aims for high speed, and a reasonable amount of compression. Based on [Google](https://github.com/google/snappy/blob/main/format_description.txt)'s Snappy compression format.
- `UNCOMPRESSED`: The output will not be compressed.
- `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md). Should always be used instead of `LZ4`.
- `LZ4`: **Deprecated** Use `LZ4_RAW` instead.
- `LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `GZIP`: Compression codec based on the GZIP format (not the closely-related "zlib" or "deflate" formats) defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
- `ZSTD`: Compression codec with a high compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).
- `BROTLI`: Compression codec based on [Brotli](https://github.com/google/brotli), offering high compression ratios.

If not specified, defaults to `SNAPPY`.

</Param>
<Param name="max_dictionary_keys" type="int" optional>

The maximum number of unique dictionary keys the writer is allowed to add to a dictionary page before switching to non-dictionary encoding. If not specified, the default value is 2^20 (1,048,576).

</Param>
<Param name="max_dictionary_size" type="int" optional>

The maximum number of bytes the writer should add to the dictionary before switching to non-dictionary encoding. If not specified, the default value is 2^20 (1,048,576).

</Param>
<Param name="target_page_size" type="int" optional>

The target page size in bytes. If not specified, defaults to 2^20 bytes (1 MiB).

</Param>
<Param name="generate_metadata_files" type="bool" optional>

Whether to generate Parquet `_metadata` and `_common_metadata` files. These files can help speed up reading of partitioned Parquet data. Defaults to `False`.

</Param>
<Param name="index_columns" type="Sequence[Sequence[str]]" optional>

Sequence of sequences containing the column names for indexes to persist. The write operation will store the index info for the provided columns as sidecar tables. For example, if the input is `[["Col1"], ["Col1", "Col2"]]`, the write operation will store the index info for `["Col1"]` and for `["Col1", "Col2"]`. By default, data indexes to write are determined by those present on the source table.

</Param>
<Param name="row_group_info" type="RowGroupInfo" optional>

The Row Group configuration for writing. Available options are:

- `RowGroupInfo.single_group()`: All data is within a single Row Group. This is the default.
- `RowGroupInfo.max_rows(max_rows)`: Splits into a number of Row Groups, each of which has no more than the requested number of rows.
- `RowGroupInfo.max_groups(num_row_groups)`: Splits evenly into a pre-defined number of Row Groups.
- `RowGroupInfo.by_groups(groups, max_rows)`: Splits each unique group into a Row Group. If `max_rows` is set, Row Groups exceeding that size are split further.

</Param>
<Param name="special_instructions" type="s3.S3Instructions" optional>

Special instructions for writing Parquet files to S3 or other remote storage. See [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions).

</Param>
</ParamTable>

## Returns

A Parquet file located in the specified path.

## Examples

> [!NOTE]
> All examples in this document write data to the `/data` directory in Deephaven. For more information on this directory and how it relates to your local file system, see [Docker data volumes](../../../conceptual/docker-data-volumes.md).

### Single Parquet file

In this example, `write` writes the source table to `/data/output.parquet`.

```python
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.parquet import write

source = new_table(
    [
        string_col("X", ["A", "B", "B", "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, 2, 3]),
        int_col("Z", [55, 76, 20, 4, 230, 50, 73, 137, 214]),
    ]
)

write(source, "/data/output.parquet")
```

### Compression codec

In this example, `write` writes the source table `/data/output_GZIP.parquet` with `GZIP` compression.

```python
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.parquet import write

source = new_table(
    [
        string_col("X", ["A", "B", "B", "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, 2, 3]),
        int_col("Z", [55, 76, 20, 4, 230, 50, 73, 137, 214]),
    ]
)

write(source, "/data/output_GZIP.parquet", compression_codec_name="GZIP")
```

## Related documentation

- [Import Parquet files](../../../how-to-guides/data-import-export/parquet-import.md)
- [Export Parquet files](../../../how-to-guides/data-import-export/parquet-export.md)
- [`read_table`](./readTable.md)
- [`write_partitioned`](./writePartitioned.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write)
