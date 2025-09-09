---
title: InferenceResolver
---

The `InferenceResolver` class provides inference options for mapping Iceberg fields to Deephaven columns when loading an Iceberg table. It is useful when the structure of the table is not known in advance, and the mapping should be inferred from the Iceberg schema.

## Syntax

```python syntax
InferenceResolver(
    infer_partitioning_columns: bool = False,
    fail_on_unsupported_types: bool = False,
    schema_provider: Optional[SchemaProvider] = None
)
```

## Parameters

<ParamTable>
<Param name="infer_partitioning_columns" type="bool" Optional>

Whether partitioning columns should be inferred based on the latest Iceberg `PartitionSpec`. Defaults to `False`. **Warning:** inferring partition columns is only recommended when the latest Iceberg `PartitionSpec` contains identity transforms that are not expected to change.

</Param>
<Param name="fail_on_unsupported_types" type="bool" Optional>

Whether inference should fail if any Iceberg fields cannot be mapped to Deephaven columns. Defaults to `False`.

</Param>
<Param name="schema_provider" type="Optional[SchemaProvider]" Optional>

The Iceberg schema to use for inference. If not specified, the current schema from the Iceberg table is used.

</Param>
</ParamTable>

## Methods

None.

## Constructors

An `InferenceResolver` is constructed directly from the class.

## Examples

Use `InferenceResolver` as the `resolver` argument when loading an Iceberg table if you want Deephaven to infer the mapping from the Iceberg schema.

The following example creates an `InferenceResolver` using the default settings, which does not infer partitioning columns and does not fail on unsupported types:

```python order=null
from deephaven.experimental import iceberg

resolver = iceberg.InferenceResolver()
```

The following example creates an `InferenceResolver` to fail if any Iceberg fields cannot be mapped to Deephaven columns:

```python order=null
from deephaven.experimental import iceberg

resolver = iceberg.InferenceResolver(
    infer_partitioning_columns=False, fail_on_unsupported_types=True
)
```

The resolver can be passed as an input argument to the [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md) [`load_table`](./iceberg-catalog-adapter.md#methods) method.

## Related documentation

- [`UnboundResolver`](./unbound-resolver.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`SchemaProvider`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.InferenceResolver)
