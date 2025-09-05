---
title: TableDefinitionLike
---

A `TableDefinitionLike` is an alias for a superset of objects that can be used to define a table in Deephaven. There are a slew of features that accept a `TableDefinitionLike` as an input argument, including (but not limited to):

- [Reading and writing Iceberg tables](../../../how-to-guides/data-import-export/iceberg.md)
- [Reading](../../../how-to-guides/data-import-export/parquet-import.md) and [writing](../../../how-to-guides/data-import-export/parquet-export.md) Parquet files
- [Writing data to a real-time, in-memory table](../../../how-to-guides/table-publisher.md)

## Objects

Four different objects meet the definition of a `TableDefinitionLike`:

- `TableDefinition`

A `TableDefinition` is an object that defines the mapping between column names and column definitions.

- `Mapping[str, DType]`

A `Mapping` is a dictionary-like object that maps column names to their data types. The keys are the column names, and the values are Deephaven's [dtypes](../../python/deephaven-python-types.md).

- `Iterable[ColumnDefinition]`

An iterable is an object that can be iterated over, such as a list or a tuple. It contains `ColumnDefinition` objects, which define the columns of the table.

- `JType`

A `JType` is a Java type that can be used to define the definition of a table. This object covers equivalent Java types that can be used here in conjunction with [jpy](../../../how-to-guides/use-jpy.md).

## Examples

The following example gets a `TableDefinition` from a table using its `definition` attribute:

```python order=:log,source
from deephaven import empty_table

source = empty_table(10).update(
    ["X = i", "Y = 1.5 * i", "Z = `A`", "TrueFalse = randomBool()"]
)

definition = source.definition
print(definition)
```

The following example constructs a `Mapping` of column names to their data types, which meets the `TableDefinitionLike` definition:

```python
from deephaven import dtypes as dht

mapping_definition = {
    "StringColumn": dht.string,
    "Int32Column": dht.int32,
    "DoubleColumn": dht.double,
    "Float32ArrayColumn": dht.float32_array,
    "BoolColumn": dht.bool_,
}
```

The following example constructs an iterable of `ColumnDefinition` objects, which meets the `TableDefinitionLike` definition:

```python
from deephaven.column import col_def
from deephaven import dtypes as dht

iterable_definition = [
    col_def("StringColumn", dht.string),
    col_def("Int32Column", dht.int32),
    col_def("DoubleColumn", dht.double),
    col_def("Float32ArrayColumn", dht.float32_array),
    col_def("BoolColumn", dht.bool_),
]
```

## Related documentation

- [Read and write Iceberg tables](../../../how-to-guides/data-import-export/iceberg.md)
- [Read Parquet files](../../../how-to-guides/data-import-export/parquet-import.md)
- [Write Parquet files](../../../how-to-guides/data-import-export/parquet-export.md)
- [Write data to a real-time, in-memory table](../../../how-to-guides/table-publisher.md)
- [`DynamicTableWriter`](./DynamicTableWriter.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinitionLike)
