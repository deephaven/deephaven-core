---
title: IcebergTableAdapter
---

The `IcebergTableAdapter` class provides an interface for interacting with Iceberg tables. It enables listing snapshots, retrieving table definitions, and reading Iceberg tables into Deephaven tables.

## Constructors

An `IcebergTableAdapter` is constructed with [`IcebergCatalogAdapter.load_table`](./iceberg-catalog-adapter.md#methods).

## Methods

- [`snapshots`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTableAdapter.snapshots): Returns information on the snapshots of a particular Iceberg table.
- [`definition`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTableAdapter.definition): Returns the Iceberg table definition as a Deephaven table. Can optionally specify custom [`IcebergReadInstructions`](./iceberg-read-instructions.md) to perform column renames, give custom definitions, and specify data instructions.
- [`table`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTableAdapter.table): Reads the Iceberg table using the provided instructions. A snapshot ID can be provided to read a specific snapshot.
- [`table_writer`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTableAdapter.table_writer): Create a new [`IcebergTableWriter`](./iceberg-table-writer.md) using the provided writer options. This method performs schema validation to ensure that the provided table definition is compatible with the Iceberg table schema.

## Examples

<!-- TODO: https://github.com/deephaven/deephaven.io/issues/4111 -->

The following example creates an `IcebergTableAdapter` called `iceberg_taxis` from an `IcebergCatalogAdapter`. The catalog adapter is created using a local MinIO instance and a REST catalog. The table adapter is then used to get a table of all available snapshots, load the table definition, and load the `taxis` Iceberg table into a Deephaven table:

```python docker-config=iceberg order=taxis,taxi_snapshots
from deephaven.experimental import iceberg

local_adapter = iceberg.adapter_s3_rest(
    name="minio-iceberg",
    catalog_uri=catalog_uri,
    warehouse_location=warehouse_location,
    region_name=aws_region,
    access_key_id=aws_access_key_id,
    secret_access_key=aws_secret_access_key,
    end_point_override=s3_endpoint,
)

iceberg_taxis = local_adapter.load_table("nyc.taxis")
taxi_snapshots = iceberg_taxis.snapshots()
taxis_definition = iceberg_taxis.definition()
taxis = iceberg_taxis.table()
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTableAdapter)
