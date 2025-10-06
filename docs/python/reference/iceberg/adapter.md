---
title: adapter
---

The `adapter` method creates an Iceberg catalog adapter from configuration properties. It is a general-purpose constructor method that can be used to create an adapter for a wide variety of Iceberg catalog types.

## Syntax

```python syntax
adapter(
    name: str = None,
    properties: Dict[str, str] = None,
    hadoop_config: Dict[str, str] = None,
    s3_instructions: Optional[S3Instructions] = None,
    enable_property_injection: bool = True,
)
```

## Parameters

<ParamTable>
<Param name="name" type="str" Optional>

A descriptive name of the catalog. If omitted, the catalog name is inferred from the catalog URI.

</Param>
<Param name="properties" type="Dict[str, str]" Optional>

The properties of the catalog. Two properties _must_ be given when creating a catalog adapter:

- `catalog-impl` or `type`
  - `catalog-impl` is the Java catalog implementation class name, such as:
    - `org.apache.iceberg.aws.glue.GlueCatalog`
    - `org.apache.iceberg.rest.RESTCatalog`
  - `type`
    - `type` is the type of adapter to create. The following types are supported:
      - `hive`
      - `hadoop`
      - `rest`
      - `glue`
      - `nessie`
      - `jdbc`
- `uri`
  - The URI of the catalog

Additional optional properties include:

- `warehouse` - the root path of the data warehouse.
- `client.region` - The region of the AWS client.
- `s3.access-key-id` - The access key ID for the S3 client.
- `s3.secret-access-key` - The secret access key for the S3 client.
- `s3.endpoint` - The endpoint for the S3 client.
- `io-impl` - The implementation class for the I/O operations, such as `org.apache.iceberg.aws.s3.S3FileIO` for S3-compatible storage.

</Param>
<Param name="hadoop_config" type="Dict[str, str]" Optional>

Hadoop configuration properties for the catalog.

</Param>
<Param name="s3_instructions" type="S3Instructions" Optional>

The S3 instructions for configuring the Deephaven managed AWS clients. If not provided, the catalog adapter will internally use the Iceberg-managed AWS clients configured using the provided properties.

</Param>
<Param name="enable_property_injection" type="bool" Optional>

Whether to enable Deephaven's automatic injection of additional properties that work around upstream issues and supply defaults needed for Deephaven's Iceberg usage. The injection is strictly additive; any keys already present in the `properties` argument are unchanged. If set to `False`, the property map is forwarded exactly as supplied with no automatic additions. The default is `True`.

</Param>
</ParamTable>

## Returns

An [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md).

## Examples

The following example creates a catalog adapter using a local MinIO instance and REST catalog:

```python docker-config=iceberg order=null
from deephaven.experimental import iceberg

local_adapter = iceberg.adapter(
    name="generic-adapter",
    properties={
        "type": "rest",
        "uri": catalog_uri,
        "client.region": aws_region,
        "s3.access-key-id": aws_access_key_id,
        "s3.secret-access-key": aws_secret_access_key,
        "s3.endpoint": s3_endpoint,
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    },
)
```

The following example creates an Iceberg catalog adapter that connects to an AWS Glue catalog:

```python skip-test
from deephaven.experimental import s3, iceberg

cloud_adapter = iceberg.adapter(
    name="generic-adapter",
    properties={
        "type": "glue",
        "uri": "s3://lab-warehouse/sales",
    },
)
```

## Related documentation

- [`adapter_aws_glue`](./adapter-aws-glue.md)
- [`adapter_s3_rest`](./adapter-s3-rest.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`SortOrderProvider`](./sort-order-provider.md)
- [`TableParquetWriterOptions`](./table-parquet-writer-options.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.adapter)
