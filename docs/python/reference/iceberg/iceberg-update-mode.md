---
title: IcebergUpdateMode
---

The `IcebergUpdateMode` class specifies the update mode for an Iceberg table to be loaded into Deephaven. There are three available update modes. Users will specify one of the modes when reading Iceberg data into Deephaven tables to determine the static or refreshing nature of the table.

## Constructors

An `IcebergUpdateMode` is constructed from one of its [methods](#methods).

## Methods

- [`static`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergUpdateMode.static): Specifies that the Iceberg table should be loaded once and not refreshed.
- [`manual_refresh`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergUpdateMode.manual_refresh): Specifies that the Iceberg table should be loaded once and refreshed manually.
- [`auto_refresh`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergUpdateMode.auto_refresh): Specifies that the Iceberg table should be loaded once and refreshed automatically. A refresh interval in milliseconds can be specified. The default is 60 seconds.

## Examples

<!-- TODO: https://github.com/deephaven/deephaven.io/issues/4111 -->

The following code block creates creates four different custom [`IcebergReadInstructions`](./iceberg-read-instructions.md) that set the update mode to `static`, `manual_refresh`, and `auto_refresh`.

```python order=null
from deephaven.experimental import iceberg

static_mode = iceberg.IcebergUpdateMode.static()
manual_refresh_mode = iceberg.IcebergUpdateMode.manual_refresh()
auto_refresh_mode_60s = iceberg.IcebergUpdateMode.auto_refresh()
auto_refresh_mode_30s = iceberg.IcebergUpdateMode.auto_refresh(auto_refresh_ms=30000)

static_instructions = iceberg.IcebergReadInstructions(update_mode=static_mode)
manual_refresh_instructions = iceberg.IcebergReadInstructions(
    update_mode=manual_refresh_mode
)
auto_refresh_instructions_60s = iceberg.IcebergReadInstructions(
    update_mode=auto_refresh_mode_60s
)
auto_refresh_instructions_30s = iceberg.IcebergReadInstructions(
    update_mode=auto_refresh_mode_30s
)
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergUpdateMode)
