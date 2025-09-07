---
title: IcebergTable
sidebar_label: IcebergTable
---

The `IcebergTable` interface provides methods for loading Iceberg tables from a catalog as Deephaven tables. The interface extends the Deephaven table, enabling the use of all Deephaven table methods, along with overloads to the [`update`](../../table-operations/select/update.md) method to allow updating the data if the underlying Iceberg table is modified.

## Constructors

An `IcebergTable` is constructed via the [`IcebergTableAdapter.table`](./iceberg-table-adapter.md#methods) method.

```groovy syntax
import io.deephaven.iceberg.util.*

icebergTable = icebergTableAdapter.table(IcebergReadInstructions instructions)
```

## Parameters

An `IcebergTable` is constructed using [`IcebergReadInstructions`](./iceberg-read-instructions.md) as the parameter. These instructions specify column renames, data instructions, behavior on errors, update mode, snapshot ID, and more.

## Methods

An `IcebergTable` provides an overload for the [`update`](../../table-operations/select/update.md) method:

- `update`: Update the table with new data. If no snapshot ID is provided, the latest snapshot is used. Otherwise, a snapshot ID or an [`org.apache.iceberg.Snapshot`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/Snapshot.html) can be specified to update the table with data from a specific snapshot.

## Examples

The following example reads a table from an Iceberg catalog. It uses the Docker deployment defined in the [Iceberg user guide](../../../how-to-guides/data-import-export/iceberg.md#a-deephaven-deployment-for-iceberg).

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

staticInstructions = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.staticMode())
    .build()

icebergTaxis = restAdapter.loadTable("nyc.taxis")

taxis = icebergTaxis.table(staticInstructions)
```

## Related documentation

- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTable.html)
