---
title: SchemaProvider
---

The `SchemaProvider` class specifies which Iceberg schema to use when reading from or writing to an Iceberg table. It allows you to select a schema by its current state, by a specific snapshot, or by schema or snapshot ID. This is useful for advanced scenarios such as schema evolution and ensuring a specific schema mapping.

## Syntax

```python syntax
SchemaProvider.from_current()
SchemaProvider.from_current_snapshot()
SchemaProvider.from_schema_id(schema_id: int)
SchemaProvider.from_snapshot_id(snapshot_id: int)
```

## Constructors

- `SchemaProvider.from_current()`

  Use the current schema from the Iceberg table.

- `SchemaProvider.from_current_snapshot()`

  Use the schema from the current snapshot of the Iceberg table.

- `SchemaProvider.from_schema_id(schema_id: int)`

  Use the schema with the specified schema ID.

- `SchemaProvider.from_snapshot_id(snapshot_id: int)`

  Use the schema from the specified snapshot ID.

## Parameters

<ParamTable>
<Param name="schema_id" type="int" Optional>
The unique identifier of the Iceberg schema to use. Only required for `from_schema_id`.
</Param>
<Param name="snapshot_id" type="int" Optional>
The unique identifier of the Iceberg snapshot to use. Only required for `from_snapshot_id`.
</Param>
</ParamTable>

## Methods

None.

## Examples

The following examples show how to create a `SchemaProvider` and use it with other Iceberg classes:

```python
from deephaven.experimental import iceberg
from deephaven import dtypes as dht

source_def = {
    "Area": dht.string,
    "Category": dht.string,
    "Price": dht.double,
}

# Use the current schema
default_schema = iceberg.SchemaProvider.from_current()

# Use a specific schema by ID
schema_by_id = iceberg.SchemaProvider.from_schema_id(1)

# Use a schema from a specific snapshot
schema_by_snapshot = iceberg.SchemaProvider.from_snapshot_id(123456789)

# Pass SchemaProvider to an InferenceResolver
resolver = iceberg.InferenceResolver(schema_provider=default_schema)

# Pass SchemaProvider to a TableParquetWriterOptions
writer_options = iceberg.TableParquetWriterOptions(
    table_definition=source_def, schema_provider=schema_by_id
)
```

## Related documentation

- [`InferenceResolver`](./inference-resolver.md)
- [`UnboundResolver`](./unbound-resolver.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.SchemaProvider)
