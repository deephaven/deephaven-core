---
title: TableDefinitionLike
---

`TableDefinitionLike` is a type alias (a `Union`) for the objects that Deephaven can convert into a [`TableDefinition`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition). Many functions take a `TableDefinitionLike` for a table-definition parameter, including functions for the following tasks:

- Reading, writing, and creating Iceberg tables with [`UnboundResolver`](../../iceberg/unbound-resolver.md), [`TableParquetWriterOptions`](../../iceberg/table-parquet-writer-options.md), and [`IcebergCatalogAdapter.create_table`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.create_table)
- Reading Parquet files with [`read`](../../data-import-export/Parquet/readTable.md)
- Writing Parquet files with [`write`](../../data-import-export/Parquet/writeTable.md), [`write_partitioned`](../../data-import-export/Parquet/writePartitioned.md), and [`batch_write`](/core/pydoc/code/deephaven.parquet.html#deephaven.parquet.batch_write)
- Writing data to a real-time, in-memory table with [`table_publisher`](./TablePublisher.md)
- Creating input tables with [`input_table`](./input-table.md)
- Creating function-generated tables with [`function_generated_table`](./function_generated_table.md)
- [Consuming JSON data from Kafka](../../../how-to-guides/data-import-export/kafka-stream.md#read-kafka-topic-in-json-format) with [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec)
- Creating partitioned tables with [`from_partitioned_table`](../partitioned-tables/from-partitioned-table.md) and [`from_constituent_tables`](../partitioned-tables/from-constituent-tables.md)

## Objects

An object of any of the following four types is a `TableDefinitionLike`:

| Type                         | Description                                                                                                                                                                                                                                                                                               |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TableDefinition`            | A [`TableDefinition`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition) maps each column name to a `ColumnDefinition`. Every table's [`definition`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.definition) attribute is a `TableDefinition`.                        |
| `Mapping[str, DType]`        | A `Mapping` is a dictionary-like object that maps column names to their data types. The keys are the column names, and the values are Deephaven [`DType`s](../../python/deephaven-python-types.md).                                                                                                       |
| `Iterable[ColumnDefinition]` | A list, tuple, or other iterable of [`ColumnDefinition`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnDefinition) objects that define the table's columns. The [`col_def`](/core/pydoc/code/deephaven.column.html#deephaven.column.col_def) function creates `ColumnDefinition` objects. |
| `jpy.JType`                  | The `JType` must wrap a Java `io.deephaven.engine.table.TableDefinition` object, such as one returned by a Java method called through [jpy](../../../how-to-guides/use-jpy.md). Deephaven uses it as-is.                                                                                                  |

## Conversion and errors

Functions that take a `TableDefinitionLike` convert it with the `TableDefinition` constructor. Pass a `TableDefinitionLike` to that constructor to convert it yourself.

Most functions raise a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) when conversion fails. Conversion fails for an object that is not a `TableDefinition`, a Java `TableDefinition`, a `Mapping`, or an iterable. It also fails for a `Mapping` whose keys are not all `str` or whose values are not all `DType`, and for an iterable that contains anything other than `ColumnDefinition` objects. The exception is the Kafka [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec) function: when conversion fails, it falls back to a deprecated form, a sequence of `(name, DType)` tuples, issues a `DeprecationWarning`, and raises a `DHError` only if that form also fails.

An iterable must support more than one pass. Deephaven consumes a one-shot iterator, such as a generator, during validation, which leaves the resulting definition with no columns. Deephaven does not raise an error.

## Examples

The following example gets a `TableDefinition` from a table using its [`definition`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.definition) attribute. A `TableDefinition` is a `TableDefinitionLike`, so any function that accepts a `TableDefinitionLike` also accepts the resulting `definition`:

```python order=:log,source
from deephaven import empty_table

source = empty_table(10).update(
    ["X = i", "Y = 1.5 * i", "Z = `A`", "TrueFalse = randomBool()"]
)

definition = source.definition
print(definition)
```

The following example constructs a `Mapping` of column names to their data types, which is a `TableDefinitionLike`:

```python test-set=1 order=null
from deephaven import dtypes as dht

mapping_definition = {
    "StringColumn": dht.string,
    "Int32Column": dht.int32,
    "DoubleColumn": dht.double,
    "Float32ArrayColumn": dht.float32_array,
    "BoolColumn": dht.bool_,
}
```

The following example constructs an iterable of [`ColumnDefinition`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnDefinition) objects, which is a `TableDefinitionLike`:

```python test-set=1 order=null
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

The following example passes the `Mapping` and the iterable to [`input_table`](./input-table.md), which creates an empty input table with the defined columns:

```python test-set=1 order=from_mapping,from_iterable
from deephaven import input_table

from_mapping = input_table(col_defs=mapping_definition)
from_iterable = input_table(col_defs=iterable_definition)
```

The following example passes the `Mapping` to the `TableDefinition` constructor and uses the [`table`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition.table) attribute to show the result:

```python test-set=1 order=definition_table
from deephaven.table import TableDefinition

definition_table = TableDefinition(mapping_definition).table
```

The following example gets a Java `TableDefinition` from a table's underlying Java object, `j_table`, passes it to the `TableDefinition` constructor, and shows the result as a table:

```python order=java_definition_table
from deephaven import empty_table
from deephaven.table import TableDefinition

source = empty_table(10).update(["X = i", "Y = 1.5 * i"])

j_definition = source.j_table.getDefinition()
java_definition_table = TableDefinition(j_definition).table
```

## Related documentation

- [Read and write Iceberg tables](../../../how-to-guides/data-import-export/iceberg.md)
- [Read Parquet files](../../../how-to-guides/data-import-export/parquet-import.md)
- [Write Parquet files](../../../how-to-guides/data-import-export/parquet-export.md)
- [Write data to a real-time, in-memory table](../../../how-to-guides/table-publisher.md)
- [Connect to a Kafka stream](../../../how-to-guides/data-import-export/kafka-stream.md)
- [Use jpy](../../../how-to-guides/use-jpy.md)
- [Data types in the Deephaven Python dtypes package](../../python/deephaven-python-types.md)
- [`function_generated_table`](./function_generated_table.md)
- [`from_constituent_tables`](../partitioned-tables/from-constituent-tables.md)
- [`from_partitioned_table`](../partitioned-tables/from-partitioned-table.md)
- [`IcebergCatalogAdapter`](../../iceberg/iceberg-catalog-adapter.md)
- [`input_table`](./input-table.md)
- [`read`](../../data-import-export/Parquet/readTable.md)
- [`table_publisher`](./TablePublisher.md)
- [`TableParquetWriterOptions`](../../iceberg/table-parquet-writer-options.md)
- [`UnboundResolver`](../../iceberg/unbound-resolver.md)
- [`write`](../../data-import-export/Parquet/writeTable.md)
- [`write_partitioned`](../../data-import-export/Parquet/writePartitioned.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinitionLike)
