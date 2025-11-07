---
title: IcebergTable
---

The `IcebergTable` class is a subclass of Deephaven table that allows users to dynamically update the table with new snapshots from an Iceberg catalog.

## Constructors

- [`IcebergTableAdapter.table`](./iceberg-table-adapter.md#methods)

## Methods

- [`update`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTable.update): Updates the table to match the contents of the specified snapshot ID. This is only useable if the update mode is set to [`IcebergUpdateMode.manual_refresh`](./iceberg-update-mode.md#methods). If no snapshot ID is given, the most recent snapshot is used.

## Examples

The following code block demonstrates reading an Iceberg table into Deephaven and using the [`update`](../table-operations/select/update.md) method to pull new data when the backing Iceberg table has been modified.

```python skip-test
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

manual_refresh_instructions = iceberg.IcebergReadInstructions(
    update_mode=iceberg.IcebergUpdateMode.manual_refresh()
)
taxis = iceberg_taxis.table(manual_refresh_instructions)

# Some time later, refresh the Iceberg table
taxis = taxis.update()
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTable)
