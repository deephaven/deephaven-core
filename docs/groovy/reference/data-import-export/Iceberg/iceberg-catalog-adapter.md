---
title: IcebergCatalogAdapter
sidebar_label: IcebergCatalogAdapter
---

`IcebergCatalogAdapter` is a class used to interact with Iceberg catalogs.

## Constructors

An `IcebergCatalogAdapter` is constructed via [`IcebergTools.createAdapter`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTools.html#createAdapter(io.deephaven.iceberg.util.BuildCatalogOptions))

```groovy syntax
import io.deephaven.iceberg.util.*

adapter = IcebergTools.createAdapter(BuildCatalogOptions options)
adapter = IcebergTools.createAdapter(
    String name,
    Map<String, String> properties
)
adapter = IcebergTools.createAdapter(
    String name,
    Map<String, String> properties,
    Map<String, String> hadoopConfig
)
adapter = IcebergTools.createAdapter(org.apache.iceberg.catalog.Catalog catalog)
```

## Parameters

When constructing an `IcebergCatalogAdapter`, the following parameters can be specified:

- [`BuildCatalogOptions`](./build-catalog-options.md): Options for building the catalog.
- `name`: The name of the catalog.
- `properties`: A map of properties for the catalog.
- `hadoopConfig`: A map of Hadoop configuration properties.
- `catalog`: An instance of [`org.apache.iceberg.catalog.Catalog`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/Snapshot.html) to wrap.

## Methods

- [`catalog`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#catalog()): Returns the underlying Iceberg catalog used by the adapter.
- [`createTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#createTable(java.lang.String,io.deephaven.engine.table.TableDefinition)): Create a new table in the catalog with a given `tableIdentifier` and `definition`.
- [`listNamespaces`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#listNamespaces()): List all namespaces in the catalog.
- [`listTables`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#listNamespaces()): List all tables in a given namespace.
- [`loadTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#loadTable(io.deephaven.iceberg.util.LoadTableOptions)): Load a table from the catalog.
- [`namespaces`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#namespaces()): List all namespaces in the catalog as a Deephaven table.
- [`tables`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#tables(java.lang.String...)): List all tables in a given namespace as a Deephaven table.

## Examples

The following example constructs an `IcebergCatalogAdapter` using the Docker deployment defined in the [Iceberg user guide](../../../how-to-guides/data-import-export/iceberg.md#a-deephaven-deployment-for-iceberg).

```groovy docker-config=iceberg
import io.deephaven.iceberg.util.*

restAdapter = IcebergTools.createAdapter(
    "minio-iceberg",
    [
        "type": "rest",
        "uri": "http://rest:8181",
        "client.region": "us-east-1",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.endpoint": "http://minio:9000",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    ]
)
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTable`](./iceberg-table.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`BuildCatalogOptions`](./build-catalog-options.md)
- [`LoadTableOptions`](./load-table-options.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html)
