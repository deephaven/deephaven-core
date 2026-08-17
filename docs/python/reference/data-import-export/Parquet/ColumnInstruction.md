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
