---
title: ColumnInstruction
---

A `ColumnInstruction` specifies the instructions for reading or writing a Parquet column.

## Syntax

```python syntax
ColumnInstruction(
    column_name=None,
    parquet_column_name=None,
    codec_name=None,
    codec_args=None,
    use_dictionary=False,
    unsigned_long_target=None,
) = ColumnInstruction
```

## Parameters

<ParamTable>
<Param name="column_name" type="str" optional>

The name of the column to apply these instructions to.

</Param>
<Param name="parquet_column_name" type="str" optional>

The name of the column in the resulting Parquet file.

</Param>
<Param name="codec_name" type="str" optional>

The [compression codec](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.8.1/org/apache/parquet/hadoop/metadata/CompressionCodecName.html) to use.

Options are:

- `SNAPPY`: (default) Aims for high speed and a reasonable amount of compression. Based on [Google](https://github.com/google/snappy/blob/main/format_description.txt)'s Snappy compression format.
- `UNCOMPRESSED`: The output will not be compressed.
- `LZ4_RAW`: A codec based on the [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md). Should always be used instead of `LZ4`.
- `LZ4`: **Deprecated** Compression codec loosely based on the [LZ4 compression algorithm](https://github.com/lz4/lz4), but with an additional undocumented framing scheme. The framing is part of the original Hadoop compression library and was historically copied first in parquet-mr, then emulated with mixed results by parquet-cpp. Note that `LZ4` is deprecated; use `LZ4_RAW` instead.
- `LZO`: Compression codec based on or interoperable with the [LZO compression library](https://www.oberhumer.com/opensource/lzo/).
- `GZIP`: Compression codec based on the GZIP format (not the closely-related "zlib" or "deflate" formats) defined by [RFC 1952](https://tools.ietf.org/html/rfc1952).
- `ZSTD`: Compression codec with the highest compression ratio based on the Zstandard format defined by [RFC 8478](https://tools.ietf.org/html/rfc8478).

</Param>
<Param name="codec_args" type="str" optional>

An implementation-specific string used to map types to/from bytes. Typically used in cases where there is no obvious language-agnostic representation in Parquet. Default is `None`.

</Param>
<Param name="use_dictionary" type="bool" optional>

Whether or not to use [dictionary-based encoding](https://en.wikipedia.org/wiki/Dictionary_coder) for string columns.

</Param>
<Param name="unsigned_long_target" type="UnsignedLongTarget" optional>

The Deephaven type to read an unsigned 64-bit integer (`UINT_64`) column as. This parameter applies to reads only, and only to columns that carry the `UINT_64` logical type. It is ignored when writing, because Deephaven never writes `UINT_64`. Default is `None`, which is equivalent to `UnsignedLongTarget.BIG_INTEGER`.

Options are:

- `UnsignedLongTarget.BIG_INTEGER`: (default) Read the column as `java.math.BigInteger`, which represents every `UINT_64` value exactly.
- `UnsignedLongTarget.LONG`: Read the column as `long`. Values greater than 2<sup>63</sup> - 1 have no `long` representation, so reading a page that contains one raises an error.
- `UnsignedLongTarget.SIGNED_LONG`: Read the column as `long`, reinterpreting the bit pattern as signed. Values greater than 2<sup>63</sup> - 1 read as negative numbers, and 2<sup>63</sup> reads as `NULL_LONG`, which is indistinguishable from a null.

For an explanation of how Deephaven maps `UINT_64` and other logical types, see [Parquet formats](../../../how-to-guides/data-import-export/parquet-formats.md).

</Param>
</ParamTable>

## Returns

A `ColumnInstruction` object that will give Deephaven instructions for handling a particular column.

## Examples

In this example, we create a `ColumnInstruction` that can be passed into [`read`](./readTable.md) or [`write`](./writeTable.md).

```python order=null
from deephaven.parquet import ColumnInstruction

instruction = ColumnInstruction(
    column_name="X",
    parquet_column_name="X",
    codec_name="GZIP",
    codec_args=None,
    use_dictionary=False,
)
```

In this example, `unsigned_long_target` reads an unsigned 64-bit integer column as a `long` instead of the default `java.math.BigInteger`. The example requires a Parquet file with a `UINT_64` column, which Deephaven does not write.

```python skip-test
from deephaven.parquet import ColumnInstruction, UnsignedLongTarget, read

instruction = ColumnInstruction(
    column_name="UInt64Column",
    parquet_column_name="UInt64Column",
    unsigned_long_target=UnsignedLongTarget.LONG,
)
result = read("/data/unsigned.parquet", col_instructions=[instruction])
```

## Related documentation

- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(java.lang.String))
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write)
