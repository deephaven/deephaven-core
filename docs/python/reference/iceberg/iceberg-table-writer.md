---
title: IcebergTableWriter
---

The `IcebergTableWriter` class writes Deephaven tables to an Iceberg table. Each instance is associated with a single [`IcebergTableAdapter`](./iceberg-table-adapter.md). A single instance of an `IcebergTableWriter` can write as many Deephaven tables as needed to the associated Iceberg table if the Deephaven tables all have the same definition. It is far more efficient to create a single `IcebergTableWriter` to write multiple Deephaven tables to the same Iceberg table than to create a new `IcebergTableWriter` for each Deephaven table.

## Constructors

An `IcebergTableWriter` is constructed using the [`table_writer`](./iceberg-table-adapter.md#methods) method of an [`IcebergTableAdapter`](./iceberg-table-adapter.md).

## Methods

- [`append`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTableWriter.append): Append the provided Deephaven table(s) as new partitions to the existing Iceberg table in a single snapshot.

## Examples

The following example writes a Deephaven table to an Iceberg table using an `IcebergTableWriter`:

```python skip-test
from deephaven.experimental import iceberg
from deephaven import empty_table

source = empty_table(10).update(["X = i", "Y = 0.1 * X", "Z = Y ** 2"])

local_adapter = iceberg.adapter_s3_rest(
    name="minio-iceberg",
    catalog_uri=catalog_uri,
    warehouse_location=warehouse_location,
    region_name=aws_region,
    access_key_id=aws_access_key_id,
    secret_access_key=aws_secret_access_key,
    end_point_override=s3_endpoint,
)

writer_options = iceberg.TableParquetWriterOptions(table_definition=source_def)

source_writer = source_adapter.table_writer(writer_options=writer_options)

source_writer.append(iceberg.IcebergWriteInstructions(source))
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergTableWriter)
