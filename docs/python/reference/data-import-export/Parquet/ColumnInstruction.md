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

The fully qualified name of an [`ObjectCodec`](/core/javadoc/io/deephaven/util/codec/ObjectCodec.html) class that serializes this column's values to and from bytes. Use a codec for types that have no language-agnostic Parquet representation. Default is `None`, which lets Deephaven choose a representation for the column type.

This is not the compression codec. Compression applies to the whole file and is set with the `compression_codec_name` argument to [`write`](./writeTable.md).

Deephaven provides these codecs in the `io.deephaven.util.codec` package:

- `BigDecimalCodec`
- `BigIntegerCodec`
- `LocalDateCodec`
- `LocalTimeCodec`
- `ZonedDateTimeCodec`
- `SimpleByteArrayCodec`
- `UTF8StringAsByteArrayCodec`
- `SerializableCodec`: A general fallback that uses Java serialization.
- `ExternalizableCodec`

</Param>
<Param name="codec_args" type="str" optional>

An implementation-specific argument string passed to the codec named by `codec_name`. The accepted values depend on the codec; `LocalDateCodec`, for example, accepts a domain and a nullability, such as `"Compact,notnull"`. Default is `None`, which uses the codec's own default.

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

In this example, we create a `ColumnInstruction` that maps the Parquet column `PX` to the Deephaven column `X`. It can be passed into [`read`](./readTable.md) or [`write`](./writeTable.md).

```python order=null
from deephaven.parquet import ColumnInstruction

instruction = ColumnInstruction(
    column_name="X",
    parquet_column_name="PX",
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

In this example, the `ColumnInstruction` stores a `LocalDate` column with the `LocalDateCodec`, using its compact, non-nullable representation.

```python order=null
from deephaven.parquet import ColumnInstruction

instruction = ColumnInstruction(
    column_name="TradeDate",
    parquet_column_name="TradeDate",
    codec_name="io.deephaven.util.codec.LocalDateCodec",
    codec_args="Compact,notnull",
)
```

## Related documentation

- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/parquet/table/ParquetTools.html#readTable(java.lang.String))
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.write)
