---
title: adapater_aws_glue
---

The `adapter_aws_glue` method creates a catalog adapter using an [AWS Glue](https://aws.amazon.com/glue/) catalog.

## Syntax

```python syntax
adapter_aws_glue(
    catalog_uri: str,
    warehouse_location: str,
    name: str = None
)
```

## Parameters

<ParamTable>
<Param name="catalog_uri" type="str">

The URI of the REST catalog.

</Param>
<Param name="warehouse_location" type="str">

The location of the warehouse.

</Param>
<Param name="name" type="str" Optional>

A descriptive name of the catalog. If not given, the catalog name is inferred from the catalog URI.

</Param>
</ParamTable>

## Returns

An [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md).

## Examples

The following example creates a catalog adapter using an AWS Glue catalog:

```python skip-test
from deephaven.experimental import s3, iceberg

cloud_adapter = iceberg.adapter_aws_glue(
    name="aws-iceberg",
    catalog_uri="s3://lab-warehouse/sales",
    warehouse_location="s3://lab-warehouse/sales",
)
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.adapter_aws_glue)
