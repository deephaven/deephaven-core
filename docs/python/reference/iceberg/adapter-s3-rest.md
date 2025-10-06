---
title: adapter_s3_rest
---

The `adapter_s3_rest` method creates a catalog adapter using an [S3](https://aws.amazon.com/s3/)-compatible provider and a REST catalog.

## Syntax

```python syntax
adapter_s3_rest(
    catalog_uri: str,
    warehouse_location: str,
    name: str = None,
    region_name: str = None,
    access_key_id: str = None,
    secret_access_key: str = None,
    end_point_override: str = None
) -> IcebergCatalogAdapter
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
<Param name="region_name" type="str" Optional>

The S3 region name to use. If not given, the default region will be picked from the AWS SDK in the following order:

- The `aws.region` system property.
- The `AWS_REGION` environment variable.
- `{user.homde}/.aws/credentials`.
- `{user.home}/.aws/config`.
- The EC2 metadata service, if using AWS EC2.

</Param>
<Param name="access_key_id" type="str" Optional>

The access key for reading files. Both this parameter and `secret_access_key` must be given to use static credentials. If not given, default credentials will be used.

</Param>
<Param name="secret_access_key" type="str" Optional>

The secret access key for reading files. Both this parameter and `access_key_id` must be given to use static credentials. If not given, default credentials will be used.

</Param>
<Param name="end_point_override" type="str" Optional>

The S3 endpoint to connect to. Callers connecting to AWS do not typically need to set this parameter. It is typically used to connect to non-AWS, S3-compatible APIs.

</Param>
</ParamTable>

## Returns

An [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md).

## Examples

The following example creates a catalog adapter using a local MinIO instance and REST catalog:

```python docker-config=iceberg order=null
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
```

## Related documentation

- [`adapter`](./adapter.md)
- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter`](./adapter.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.adapter_s3_rest)
