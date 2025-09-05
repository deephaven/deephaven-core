---
title: IcebergCatalogAdapter
---

The `IcebergCatalogAdapter` class provides an interface for interacting with Iceberg catalogs. It enables listing namespaces, tables, and snapshots, as well as reading and writing to and from Iceberg.

## Constructors

An `IcebergCatalogAdapter` is constructed using one of the following methods:

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)

## Methods

- [`namespaces`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.namespaces) - Returns a table of Iceberg namespaces that belong to a given namespace. If no namespace is given, a table of the top-level namespaces is returned.
- [`tables`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.tables) - Returns information on the tables in the specified namespace.
- [`load_table`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.load_table) - Load an Iceberg table into Deephaven from a catalog.

## Examples

The following example creates an `IcebergCatalogAdapter` using an AWS glue catalog:

```python skip-test
from deephaven.experimental import iceberg

glue_adapter = iceberg.adapter_aws_glue(
    name="aws-iceberg",
    catalog_uri="s3://lab-warehouse/sales",
    warehouse_location="s3://lab-warehouse/sales",
)
```

The following example creates an `IcebergCatalogAdapter` using a local MinIO instance and a REST catalog:

```python docker-config=iceberg order=null
from deephaven.experimental import iceberg

rest_adapter = iceberg.adapter_s3_rest(
    name="minio-iceberg",
    catalog_uri=catalog_uri,
    warehouse_location=warehouse_location,
    region_name=aws_region,
    access_key_id=aws_access_key_id,
    secret_access_key=aws_secret_access_key,
    end_point_override=s3_endpoint,
)
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter)
