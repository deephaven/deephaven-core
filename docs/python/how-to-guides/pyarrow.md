---
title: Deephaven and PyArrow
---

This guide shows how to use Deephaven's [Arrow integration](./deephaven-python-package.md#arrow) with [PyArrow](https://arrow.apache.org/docs/python/index.html). PyArrow is a Python library for [Apache Arrow](https://arrow.apache.org/), which is a columnar memory format similar to Deephaven's table format. The integration lets you:

- Read [Feather](https://arrow.apache.org/docs/python/feather.html) V2 files into Deephaven tables. Feather V2 is the on-disk form of the Arrow IPC (interprocess communication) format, and PyArrow's [`write_feather`](https://arrow.apache.org/docs/python/generated/pyarrow.feather.write_feather.html) writes it by default.
- Convert between Deephaven tables and PyArrow tables.
- Write Deephaven tables to Feather files, by converting them to PyArrow tables and passing the result to PyArrow's [`write_feather`](https://arrow.apache.org/docs/python/generated/pyarrow.feather.write_feather.html).

The [`deephaven.arrow`](/core/pydoc/code/deephaven.arrow.html#module-deephaven.arrow) submodule provides three functions for these tasks:

- [`read_feather`](../reference/arrow/read-feather.md)
- [`to_arrow`](../reference/arrow/to-arrow.md)
- [`to_table`](../reference/arrow/to-table.md)

## Read a Feather file

Use [`read_feather`](../reference/arrow/read-feather.md) to read a Feather file into a Deephaven table as `iris`:

```python test-set=1 order=iris
from deephaven import arrow as dharrow

iris = dharrow.read_feather("/data/examples/Iris/feather/Iris.feather")
```

## Convert between Deephaven and PyArrow tables

> [!NOTE]
> Converting between Deephaven and PyArrow tables copies the converted columns' data in memory. Converting a large table needs free memory for more than one extra copy of that data while the conversion runs, in the JVM heap as well as in PyArrow's memory.
>
> For a refreshing table, [`to_arrow`](../reference/arrow/to-arrow.md) converts a consistent snapshot of the table's current rows. The PyArrow table doesn't update when the source table ticks.

Use [`to_arrow`](../reference/arrow/to-arrow.md) to convert the `iris` table from [Read a Feather file](#read-a-feather-file) to a PyArrow table twice: once with all columns, and once with only the `Class` column.

```python test-set=1 order=:log
pa_iris = dharrow.to_arrow(iris)
pa_class_only = dharrow.to_arrow(iris, cols=["Class"])

print(pa_iris.schema)
```

The printed schema shows that some columns aren't plain Arrow types. [`to_arrow`](../reference/arrow/to-arrow.md) can return [dictionary-encoded](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout) or [run-end-encoded](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout) columns. A dictionary-encoded column stores each distinct value once, and each row holds an index into that list. A run-end-encoded column stores each run of repeated values once.

Starting in version 43.0, Deephaven picks these encodings itself by default: dictionary encoding for string and other object columns with few distinct values, and run-end encoding for columns whose values repeat in runs. The `Class` column gets both. To control these encodings, see [Barrage schema annotation](./data-import-export/barrage-schema.md).

[`to_table`](../reference/arrow/to-table.md) rejects encoded columns. If you write them to a Feather file, [`read_feather`](../reference/arrow/read-feather.md) can't read them correctly either. Decode them first:

```python test-set=1 order=null
import pyarrow as pa
import pyarrow.compute as pc


def plain_type(arrow_type):
    if pa.types.is_run_end_encoded(arrow_type):
        arrow_type = arrow_type.value_type
    if pa.types.is_dictionary(arrow_type):
        arrow_type = arrow_type.value_type
    return arrow_type


def decode_column(column):
    chunks = []
    for chunk in column.chunks:
        if pa.types.is_run_end_encoded(chunk.type):
            values = chunk.values
            if pa.types.is_dictionary(values.type):
                values = values.cast(values.type.value_type)
            chunk = pc.run_end_decode(
                pa.RunEndEncodedArray.from_arrays(chunk.run_ends, values)
            ).slice(chunk.offset, len(chunk))
        if pa.types.is_dictionary(chunk.type):
            chunk = chunk.cast(chunk.type.value_type)
        chunks.append(chunk)
    return pa.chunked_array(chunks, type=plain_type(column.type))


def decode(pa_table):
    columns = [decode_column(column) for column in pa_table.columns]
    return pa.table(columns, names=pa_table.column_names)


pa_iris = decode(pa_iris)
```

The helper turns dictionary-encoded values back into plain values by casting them to their value type, and expands run-end-encoded columns with PyArrow's [`run_end_decode`](https://arrow.apache.org/docs/python/generated/pyarrow.compute.run_end_decode.html). A column can be both, as `Class` is here: its runs hold dictionary-encoded values. `run_end_decode` can't expand runs of dictionary values, so the helper decodes the values inside each run-end-encoded chunk first, then keeps only the rows the chunk covers, which matters if you sliced the table. Passing the decoded type to `pa.chunked_array` lets the helper handle a column with no chunks, such as one from an empty table.

Then, use [`to_table`](../reference/arrow/to-table.md) to convert `pa_iris` back to a Deephaven table twice. The first call copies all columns. The second call passes `cols` to copy every column except `Class`.

```python test-set=1 order=iris_from_pa,iris_no_class
iris_from_pa = dharrow.to_table(pa_iris)
iris_no_class = dharrow.to_table(
    pa_iris, cols=["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"]
)
```

[`to_table`](../reference/arrow/to-table.md) also converts tables you build with PyArrow itself, as long as their column types are [supported](#supported-column-types):

```python test-set=1 order=from_pyarrow
import pyarrow as pa

pa_table = pa.table({"Id": [1, 2, 3], "Name": ["a", "b", "c"]})
from_pyarrow = dharrow.to_table(pa_table)
```

### Supported column types

[`to_table`](../reference/arrow/to-table.md) accepts these Arrow column types:

- Exact types listed in `deephaven.arrow.SUPPORTED_ARROW_TYPES`, such as `pa.int64()`, `pa.string()`, and `pa.timestamp("ns")`. [`to_table`](../reference/arrow/to-table.md) maps each of these directly to a Deephaven type.
- Some parameterized types, which take arguments such as a precision, an element type, or a time zone: 128-bit and 256-bit decimals (`pa.decimal128` and `pa.decimal256`), lists, list views, fixed-size lists, maps, fixed-size binary, unions, and timestamps with a time zone. `SUPPORTED_ARROW_TYPES` doesn't list these types, so [`to_table`](../reference/arrow/to-table.md) leaves their mapping to the Deephaven engine.

To see the full list of exact types, print `SUPPORTED_ARROW_TYPES`:

```python test-set=1 order=:log
print(dharrow.SUPPORTED_ARROW_TYPES)
```

[`to_table`](../reference/arrow/to-table.md) raises an error for any other column type, including large strings, large binary, large lists, structs, and dictionary-encoded or run-end-encoded columns. [`to_arrow`](../reference/arrow/to-arrow.md) can produce the last two, so decode them as shown in [Convert between Deephaven and PyArrow tables](#convert-between-deephaven-and-pyarrow-tables) before you convert back. The error message points only to `SUPPORTED_ARROW_TYPES`, so check the parameterized types above as well before you decide a type is unsupported.

## Write a Feather file

To write a Deephaven table to a Feather file, convert it to a PyArrow table with [`to_arrow`](../reference/arrow/to-arrow.md) and decode any encoded columns, as in [Convert between Deephaven and PyArrow tables](#convert-between-deephaven-and-pyarrow-tables). Then pass the result to PyArrow's [`write_feather`](https://arrow.apache.org/docs/python/generated/pyarrow.feather.write_feather.html). This example writes the decoded `pa_iris` to a file and reads it back with [`read_feather`](../reference/arrow/read-feather.md):

```python test-set=1 order=iris_roundtrip
import pyarrow.feather as feather

feather.write_feather(pa_iris, "/tmp/iris.feather")
iris_roundtrip = dharrow.read_feather("/tmp/iris.feather")
```

[`read_feather`](../reference/arrow/read-feather.md) reads fewer column types than [`to_table`](../reference/arrow/to-table.md): it can't read list, map, union, duration, or most time-of-day columns. To bring such a file back into Deephaven, read it with PyArrow's [`read_table`](https://arrow.apache.org/docs/python/generated/pyarrow.feather.read_table.html) and convert the result with [`to_table`](../reference/arrow/to-table.md).

## Related documentation

- [`read_feather`](../reference/arrow/read-feather.md)
- [`to_arrow`](../reference/arrow/to-arrow.md)
- [`to_table`](../reference/arrow/to-table.md)
- [Deephaven Python package: `arrow`](./deephaven-python-package.md#arrow)
- [Barrage schema annotation](./data-import-export/barrage-schema.md)
- [Use pandas in Deephaven queries](./use-pandas.md)
- [Use NumPy in Deephaven queries](./use-numpy.md)
- [Read Parquet files](./data-import-export/parquet-import.md)
