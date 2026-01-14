---
title: write
---

The `write` method will write a table to a standard Parquet file.

## Syntax

```python syntax
write(
    table: Table,
    path: str,
    col_definitions: list[Column] = None,
    col_instructions: list[ColumnInstruction] = None,
    compression_codec_name: str = None,
    max_dictionary_keys: int = None,
    target_page_size: int = None,
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
<Param name="col_definitions" type="list[Column]" optional>

The column definitions to use. The default is `None`.

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
- `LZ4`: **Deprecated** Compression codec loosely based on the [LZ4 compression algorithm](https://github.com/lz4/lz4), but with an additional undocumented framing scheme. The framing is part of the original Hadoop compression library and was historically copied first in parquet-mr, then emulated with mixed results by parquet-cpp. Note that `LZ4` is deprecated; use `LZ4_RAW` instead.
- `LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `GZIP`: Compression codec based on the GZIP format (not the closely-related "zlib" or "deflate" formats) defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
- `ZSTD`: Compression codec with the highest compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).

If not specified, defaults to `SNAPPY`.

</Param>
<Param name="max_dictionary_keys" type="int" optional>

The maximum number of unique dictionary keys the writer is allowed to add to a dictionary page before switching to non-dictionary encoding. If not specified, the default value is 2^20 (1,048,576).

</Param>
<Param name="target_page_size" type="int" optional>

The target page size in bytes. If not specified, defaults to 2^20 bytes (1 MiB).

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
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write)
