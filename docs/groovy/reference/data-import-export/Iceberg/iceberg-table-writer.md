---
title: IcebergTableWriter
sidebar_label: IcebergTableWriter
---

The `IcebergTableWriter` class is responsible for writing Deephaven tables to Iceberg tables. Each instance is associated with a single [`IcebergTableAdapter`](./iceberg-table-adapter.md) and is used to write multiple Deephaven tables to a single Iceberg table.

## Constructors

The `IcebergTableWriter` class is constructed with the [`IcebergTableAdapter.tableWriter`](./iceberg-table-adapter.md#methods) method:

```groovy syntax
import io.deephaven.iceberg.util.*

icebergTableWriter = icebergTableAdapter.tableWriter(IcebergWriteInstructions writeInstructions)
```

## Parameters

When constructing an `IcebergTableWriter`, the [`IcebergWriteInstructions`](./iceberg-write-instructions.md) class specifies instructions to use when writing to the Iceberg table.

## Methods

- [`append`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableWriter.html#append(io.deephaven.iceberg.util.IcebergWriteInstructions)): Append the Deephaven table(s) to the Iceberg table using the specified write instructions.
- [`writeDataFiles`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableWriter.html#writeDataFiles(io.deephaven.iceberg.util.IcebergWriteInstructions)): Write data from the Deephaven table(s) to an Iceberg table without creating a new snapshot.

## Examples

The following example creates an `IcebergTableWriter` from an [`IcebergTableAdapter`](./iceberg-table-adapter.md) and appends two tables with identical definitions to an Iceberg table. It uses the Docker deployment defined in the [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md#a-deephaven-deployment-for-iceberg) guide.

```groovy docker-config=iceberg order=sourceData2024,sourceData2025 reset
import io.deephaven.iceberg.util.*
import io.deephaven.extensions.s3.*
import org.apache.iceberg.catalog.*

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

icebergTaxis = restAdapter.loadTable("nyc.taxis")

// Create sample data tables
sourceData2024 = emptyTable(100).update("Year = 2024", "X = i", "Y = 2 * X", "Z = randomDouble(-1, 1)")
sourceData2025 = emptyTable(50).update("Year = 2025", "X = 100 + i", "Y = 3 * X", "Z = randomDouble(-100, 100)")

// Get table definition and create Iceberg table
sourceDef = sourceData2024.getDefinition()
sourceAdapter = restAdapter.createTable("nyc.source", sourceDef)

// Configure writer options
writerOptions = TableParquetWriterOptions.builder()
    .tableDefinition(sourceDef)
    .build()

// Create writer and append data
sourceWriter = sourceAdapter.tableWriter(writerOptions)
sourceWriter.append(IcebergWriteInstructions.builder()
    .addTables(sourceData2024, sourceData2025).build())
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergWriteInstructions`](./iceberg-write-instructions.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableWriter.html)
