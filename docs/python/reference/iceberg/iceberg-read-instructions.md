---
title: IcebergReadInstructions
---

The `IcebergReadInstructions` class specifies the instructions for reading Iceberg tables into Deephaven. These include special instructions for loading files from cloud storage, the update mode, and the snapshot to read.

> [!NOTE]
> The `table_definition` and `column_renames` parameters are deprecated and have no effect. Passing either one only raises a `DeprecationWarning`. To choose columns, set their types, or map Deephaven column names to Iceberg fields, pass an [`UnboundResolver`](./unbound-resolver.md) to [`IcebergCatalogAdapter.load_table`](./iceberg-catalog-adapter.md) instead.

## Syntax

```python syntax
IcebergReadInstructions(
    table_definition: Union[Dict[str, DType], List[Column]] = None,
    data_instructions: S3Instructions = None,
    column_renames: Dict[str, str] = None,
    update_mode: IcebergUpdateMode = None,
    snapshot_id: int = None,
    ignore_resolving_errors: bool = False,
)
```

## Parameters

<ParamTable>
<Param name="table_definition" type="Union[Dict[str, DType], List[Column]]" Optional>

Deprecated. This parameter has no effect.

</Param>
<Param name="data_instructions" type="S3Instructions" Optional>

Special instructions for reading data files from a non-local file system, such as S3. If not given, the data instructions are derived from the catalog.

</Param>
<Param name="column_renames" type="Dict[str, str]" Optional>

Deprecated. This parameter has no effect.

</Param>
<Param name="update_mode" type="IcebergUpdateMode" Optional>

The update mode for the table. Options include:

- `IcebergUpdateMode.static()`: Specifies that the Iceberg table should be loaded once and not refreshed.
- `IcebergUpdateMode.manual_refresh()`: Specifies that the Iceberg table should be loaded once and refreshed manually.
- `IcebergUpdateMode.auto_refresh()`: Specifies that the Iceberg table should be loaded once and refreshed automatically. The default refresh interval is 60 seconds, but can be changed with the `auto_refresh_ms` input parameter.

</Param>
<Param name="snapshot_id" type="int" Optional>

The snapshot ID to read. If not given, the most recent snapshot ID is used.

</Param>
<Param name="ignore_resolving_errors" type="bool" Optional>

Whether to ignore unexpected errors when resolving columns. If `True`, Deephaven reads null values for a column in any data file where the column should be present but can't be resolved. These errors can be a sign of an incorrect resolver or name mapping, or of an Iceberg metadata or data problem. The default is `False`.

</Param>
</ParamTable>

## Methods

None.

## Constructors

An `IcebergReadInstructions` is constructed directly from the class.

## Examples

The following example creates four `IcebergReadInstructions` objects. The first is for static Iceberg tables, the second is for Iceberg tables that can be manually refreshed, and the third and fourth are for Iceberg tables that will be refreshed automatically. The third uses the default value of 60 seconds, whereas the fourth sets the interval to 30 seconds.

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

The following example creates an `IcebergReadInstructions` object that tells a catalog adapter about the region, access information, and endpoint for reading Iceberg tables from S3 cloud storage:

```python docker-config=iceberg order=null
from deephaven.experimental import iceberg
from deephaven.experimental import s3

s3_instructions = s3.S3Instructions(
    region_name=aws_region,
    access_key_id=aws_access_key_id,
    secret_access_key=aws_secret_access_key,
    endpoint_override=s3_endpoint,
)

iceberg_instructions = iceberg.IcebergReadInstructions(
    data_instructions=s3_instructions
)
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
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`UnboundResolver`](./unbound-resolver.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergReadInstructions)
