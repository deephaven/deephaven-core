---
title: TableParquetWriterOptions
---

The `TableParquetWriterOptions` class provides specialized instructions for configuring your [`IcebergTableWriter`](./iceberg-table-writer.md) instances.

## Syntax

```python syntax
TableParquetWriterOptions(
    table_definition: TableDefinition | Mapping[str, DType] | Iterable[ColumnDefinition] | JType,
    schema_provider: SchemaProvider | None = None,
    field_id_to_column_name: dict[int, str] | None = None,
    compression_codec_name: str | None = None,
    maximum_dictionary_keys: int | None = None,
    maximum_dictionary_size: int | None = None,
    target_page_size: int | None = None,
    sort_order_provider: SortOrderProvider | None = None,
    data_instructions: s3.S3Instructions | None = None,
)
```

## Parameters

<ParamTable>
<Param name="table_definition" type="TableDefinition | Mapping[str, DType] | Iterable[ColumnDefinition] | JType">

The table definition to use when writing Iceberg data files. The definition can be used to skip some columns or add additional columns with null values. The provided definition should have at least one column.

</Param>
<Param name="schema_provider" type="SchemaProvider | None" Optional>

Used to extract a schema from an Iceberg table. The schema will be used in conjunction with the `field_id_to_column_name` to map Deephaven column from the table definition to Iceberg columns. You can specify how to extract the schema in multiple ways (by ID, snapshot ID, initial schema, etc.). Defaults to `None`, which uses the current schema of the table.

</Param>
<Param name="field_id_to_column_name" type="dict[int, str] | None" Optional>

A one-to-one mapping of Iceberg field IDs from the schema specification to Deephaven column names from the table definition. Defaults to `None`, which means map Iceberg columns to Deephaven columns using column names.

</Param>
<Param name="compression_codec_name" type="str | None" Optional>

The compression codec to use for writing the Parquet file. Allowed values include `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZO`, `LZ4`, `LZ4_RAW`, and `ZSTD`. Defaults to `None`, which uses `SNAPPY`.

</Param>
<Param name="maximum_dictionary_keys" type="int | None" Optional>

The maximum number of unique keys the Parquet writer should add to a dictionary page before switching to non-dictionary encoding. Never used for non-string columns. Defaults to `None`, which uses 2^20 (1,048,576).

</Param>
<Param name="maximum_dictionary_size" type="int | None" Optional>

The maximum number of bytes the Parquet writer should add to the dictionary before switching to non-dictionary encoding. Never used for non-string columns. Defaults to `None`, which uses 2^20 (1,048,576).

</Param>
<Param name="target_page_size" type="int | None" Optional>

The target Parquet file page size in bytes. Defaults to `None`, which uses 2^20 bytes (1 MB).

</Param>
<Param name="sort_order_provider" type="SortOrderProvider | None" Optional>

Specifies the sort order to use for sorting new data
when writing to an Iceberg table with this writer. The sort order is determined at the time the writer is created and does not change if the table's sort order changes later. Defaults to `None`, which means the table's default sort order is used. More details about sort order can be found in the [Iceberg spec](https://iceberg.apache.org/spec/#sorting).

</Param>
<Param name="data_instructions" type="s3.S3Instructions | None" Optional>

Special instructions for writing data files, useful when writing files to a non-local file system, like S3. If omitted, the data instructions will be derived from the catalog.

</Param>
</ParamTable>

## Methods

None.

## Constructors

A `TableParquetWriterOptions` is constructed directly from the class.

## Examples

The following example creates a `TableParquetWriterOptions` object that can be used to write Deephaven tables to an Iceberg table:

```python docker-config=iceberg order=null
from deephaven.experimental import iceberg
from deephaven.experimental import s3
from deephaven import empty_table

source = empty_table(10).update(["X = i", "Y = 0.1 * X", "Z = pow(Y, 2)"])

source_def = source.definition

s3_instructions = s3.S3Instructions(
    region_name=aws_region,
    endpoint_override=s3_endpoint,
    credentials=s3.Credentials.basic(aws_access_key_id, aws_secret_access_key),
)

writer_options = iceberg.TableParquetWriterOptions(
    table_definition=source_def, data_instructions=s3_instructions
)
```

## Related documentation

- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`adapter`](./adapter.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.TableParquetWriterOptions)
