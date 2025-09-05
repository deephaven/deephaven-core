---
title: IcebergWriteInstructions
---

The `IcebergWriteInstructions` class provides instructions intended for writing Deephaven tables as partitions to Iceberg tables.

## Syntax

```python syntax
IcebergWriteInstructions(
    tables: Union[Table, Sequence[Table]],
    partition_paths: Optional[Union[str, Sequence[str]]] = None,
)
```

## Parameters

<ParamTable>
<Param name="tables" type="Union[Table, Sequence[Table]]">

The Deephaven table(s) to write.

</Param>
<Param name="partition_paths" type="Optional[Union[str, Sequence[str]]]" Optional>

The partition paths where each table will be written. As an example, if the Iceberg table is partitioned by `year` and `month`, a partition path could be `year=2021/month=01`. If writing to a partitioned Iceberg table, users must provide partition paths for each table in the `tables` argument in the same order. When writing to a non-partitioned table, users should not provide partition paths. This defaults to `None`, which means the Deephaven tables will be written to the root data directory of the Iceberg table.

</Param>
</ParamTable>

## Methods

None.

## Constructors

An `IcebergWriteInstructions` is constructed directly from the class.

## Examples

The following example creates `IcebergWriteInstructions` for a single table to Iceberg:

```python skip-test
from deephaven.experimental import iceberg
from deephaven import empty_table

source = empty_table(10).update(["X = i", "Y = 0.1 * X", "Z = Y ** 2"])

write_instructions = iceberg.IcebergWriteInstructions(source)
```

The following example creates `IcebergWriteInstructions` for multiple tables to Iceberg:

```python skip-test
from deephaven.experimental import iceberg
from deephaven import empty_table

source_1 = empty_table(10).update(["X = i", "Y = 0.1 * X", "Z = Y ** 2"])
source_2 = empty_table(10).update(["X = i + 100", "Y = 0.1 * X", "Z = sqrt(Y)"])

write_instructions = iceberg.IcebergWriteInstructions([source_1, source_2])
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergWriteInstructions)
