---
title: SortOrderProvider
---

The `SortOrderProvider` class specifies the sort order to use when writing data to an Iceberg table from Deephaven. It allows users to select the sort order by sort order ID, use the default sort order of the table, specify unsorted data, or configure error handling for unmapped columns.

## Constructors

A `SortOrderProvider` is constructed using one of the following class methods:

```python syntax
SortOrderProvider.from_sort_id(sort_order_id: int)
SortOrderProvider.use_table_default()
SortOrderProvider.unsorted()
```

## Methods

- `from_sort_id`: Selects a sort order by its ID from the Iceberg table metadata.
- `use_table_default`: Uses the table's default sort order.
- `unsorted`: Specifies that data should be written without any sort order.
- `with_fail_on_unmapped`: Determines if an existing `SortOrderProvider` will fail or not if any columns in the sort order are not present in the data to be written.
- `with_id`: Returns a new sort order provider that uses the existing provider to determine the columns to sort on, but writes a provided sort order ID to the Iceberg table.

## Parameters

<ParamTable>
<Param name="sort_order_id" type="int">

The ID of the sort order as defined in the Iceberg table's metadata.

</Param>
<Param name="fail_on_unmapped" type="bool">

If `True`, writing will fail if any columns required by the sort order are not present in the data. If `False`, missing columns are ignored.

</Param>
</ParamTable>

## Usage

A `SortOrderProvider` is used as an argument to [`TableParquetWriterOptions`](./table-parquet-writer-options.md) when writing Deephaven tables to Iceberg, allowing control over the sort order of the written data.

## Examples

The following example creates a `TableParquetWriterOptions` that writes data using the table's default sort order:

```python order=null
from deephaven.experimental import iceberg
from deephaven import dtypes as dht

source_def = {"ID": dht.int32, "Name": dht.string, "Value": dht.double}

sort_order_provider = iceberg.SortOrderProvider.use_table_default()
writer_options = iceberg.TableParquetWriterOptions(
    table_definition=source_def, sort_order_provider=sort_order_provider
)
```

To specify a particular sort order by ID and fail on unmapped columns:

```python order=null
from deephaven.experimental import iceberg
from deephaven import dtypes as dht

source_def = {"ID": dht.int32, "Name": dht.string, "Value": dht.double}

sort_order_provider = iceberg.SortOrderProvider.from_sort_id(1).with_fail_on_unmapped(
    True
)
writer_options = iceberg.TableParquetWriterOptions(
    table_definition=source_def, sort_order_provider=sort_order_provider
)
```

To write data without any sort order:

```python order=null
from deephaven.experimental import iceberg
from deephaven import dtypes as dht

source_def = {"ID": dht.int32, "Name": dht.string, "Value": dht.double}

sort_order_provider = iceberg.SortOrderProvider.unsorted()
writer_options = iceberg.TableParquetWriterOptions(
    table_definition=source_def, sort_order_provider=sort_order_provider
)
```

## Related documentation

- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`adapter`](./adapter.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.SortOrderProvider)
