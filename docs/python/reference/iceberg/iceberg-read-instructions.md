---
title: IcebergReadInstructions
---

The `IcebergReadInstructions` class specifies the instructions for reading Iceberg tables into Deephaven. These include column renames, table definitions, and special instructions for loading files from cloud storage.

## Syntax

```python syntax
IcebergReadInstructions(
    table_definition: Union[Dict[str, DType], List[Column]] = None,
    data_instructions: S3Instructions = None,
    column_renames: Dict[str, str] = None,
    update_mode: IcebergUpdateMode = None,
    snapshot_id: int = None
)
```

## Parameters

<ParamTable>
<Param name="table_definition" type="Union[Dict[str, DType], List[Column]]" Optional>

The table definition. If not given, the definition is inferred from the Iceberg schema. Setting a definition guarantees the returned table has the given definition. This is mostly used to specify a subset of Iceberg schema columns.

</Param>
<Param name="data_instructions" type="S3Instructions" Optional>

Special instructions for reading data files from S3 cloud storage.

</Param>
<Param name="column_renames" type="Dict[str, str]" Optional>

A mapping of old to new column names for the table. If not given, the column names are the same as the Iceberg schema.

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
</ParamTable>

## Methods

None.

## Constructors

An `IcebergReadInstructions` is constructed directly from the class.

## Examples

The following example creates an `IcebergReadInstructions` object that renames Iceberg columns `region` and `item_type` to `Area` and `Category` in Deephaven, respectively:

```python order=null
from deephaven.experimental import iceberg

custom_instructions = iceberg.IcebergReadInstructions(
    column_renames={"region": "Area", "item_type": "Category"}
)
```

The following example creates an `IcebergReadInstructions` object that renames columns as well as specifies the table definition:

```python order=null
from deephaven.experimental import iceberg
from deephaven import dtypes as dht

custom_instructions = iceberg.IcebergReadInstructions(
    column_renames={"region": "Area", "item_type": "Category", "unit_price": "Price"},
    table_definition={
        "Area": dht.string,
        "Category": dht.string,
        "Price": dht.double,
    },
)
```

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
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergReadInstructions)
