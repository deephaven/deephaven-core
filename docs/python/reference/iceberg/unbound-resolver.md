---
title: UnboundResolver
---

The `UnboundResolver` class provides options for explicitly mapping Deephaven columns to Iceberg fields when loading an Iceberg table. Use this class when you know the desired Deephaven table definition and want to control how columns are mapped to Iceberg fields, including handling schema evolution.

## Syntax

```python syntax
UnboundResolver(
    table_definition: TableDefinitionLike,
    column_instructions: Optional[Mapping[str, Union[int, str]]] = None,
    schema_provider: Optional[SchemaProvider] = None
)
```

## Parameters

<ParamTable>
<Param name="table_definition" type="TableDefinitionLike">

The Deephaven table definition to use when loading the Iceberg table.

</Param>
<Param name="column_instructions" type="Optional[Mapping[str, Union[int, str]]]" Optional>

A mapping from Deephaven column names to instructions for mapping to Iceberg columns. An `int` value is treated as a schema field ID, and a `str` value is treated as a schema field name. Columns not in this map are assumed to match by name.

</Param>
<Param name="schema_provider" type="Optional[SchemaProvider]" Optional>

The Iceberg schema to use for mapping. If not specified, the current schema from the Iceberg table is used.

</Param>
</ParamTable>

## Methods

None.

## Constructors

An `UnboundResolver` is constructed directly from the class.

## Examples

An `UnboundResolver` explicitly maps Deephaven columns to Iceberg fields. Consider the following Iceberg table schema:

```json
{
  "type": "struct",
  "fields": [
    {
      "id": 1,
      "name": "area",
      "type": "string",
      "required": false
    },
    {
      "id": 2,
      "name": "item_type",
      "type": "string",
      "required": false
    },
    {
      "id": 3,
      "name": "price",
      "type": "float",
      "required": false
    }
  ],
  "schema-id": 0,
  "identifier-field-ids": []
}
```

This Iceberg schema maps to the following Deephaven table definition:

```python test-set=1 order=null
from deephaven import dtypes as dht

my_definition = {
    "Area": dht.string,
    "Category": dht.string,
    "Price": dht.double,
}
```

The following example constructs an `UnboundResolver` from column instructions that map Deephaven column names to Iceberg column names:

```python test-set=1 order=null
from deephaven.experimental import iceberg

resolver_names = iceberg.UnboundResolver(
    table_definition=my_definition,
    column_instructions={"Area": "area", "Category": "item_type", "Price": "price"},
)
```

The following example constructs an `UnboundResolver` from column instructions that map Deephaven column names to Iceberg field IDs:

```python test-set=1 order=null
from deephaven.experimental import iceberg

resolver_ids = iceberg.UnboundResolver(
    table_definition=my_definition,
    column_instructions={"Area": 1, "Category": 2, "Price": 3},
)
```

> [!NOTE]
> Referencing field ID is generally preferred over field name, as it is more robust to schema evolution. If the Iceberg schema changes, the field ID remains the same, while the field name may change.

## Related documentation

- [`InferenceResolver`](./inference-resolver.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`SchemaProvider`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.UnboundResolver)
