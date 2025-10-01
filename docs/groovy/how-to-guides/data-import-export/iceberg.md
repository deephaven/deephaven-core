---
title: Iceberg and Deephaven
sidebar_label: Iceberg
---

[Apache Iceberg](https://iceberg.apache.org/) is a high-performance format for tabular data. Deephaven's Iceberg integration enables users to interact with Iceberg catalogs, namespaces, tables, and snapshots. This guide walks through reading from Iceberg with a single table and snapshot, then writes multiple Deephaven tables to the same Iceberg namespace. The examples presented this guide interact with a [REST catalog](https://www.tabular.io/apache-iceberg-cookbook/getting-started-catalog-background/).

The API enables you to interact with many types of catalogs. They include:

- REST
- AWS Glue
- JDBC
- Hive
- Hadoop
- Nessie

> [!NOTE]
> Some catalog types in the list above require adding dependencies to your classpath.

## Deephaven's Iceberg package

Deephaven's Iceberg integration is provided in the form of six different packages, all prefixed by `io.deephaven.iceberg`. These packages are:

- [`io.deephaven.iceberg.base`](/core/javadoc/io/deephaven/iceberg/base/package-summary.html)
- [`io.deephaven.iceberg.internal`](/core/javadoc/io/deephaven/iceberg/internal/package-summary.html)
- [`io.deephaven.iceberg.layout`](/core/javadoc/io/deephaven/iceberg/layout/package-summary.html)
- [`io.deephaven.iceberg.location`](/core/javadoc/io/deephaven/iceberg/location/package-summary.html)
- [`io.deephaven.iceberg.relative`](/core/javadoc/io/deephaven/iceberg/relative/package-summary.html)
- [`io.deephaven.iceberg.util`](/core/javadoc/io/deephaven/iceberg/util/package-summary.html)

The examples presented in this guide only use [`io.deephaven.iceberg.util`](/core/javadoc/io/deephaven/iceberg/util/package-summary.html). The others are provided for visibility.

When querying Iceberg tables located in any S3-compatible storage provider, the [`io.deephaven.extensions.s3`](/core/javadoc/io/deephaven/extensions/s3/package-summary.html) package is also required.

## A Deephaven deployment for Iceberg

The examples in this guide use the Docker deployment found in the [Deephaven example Iceberg REST catalog deployment](https://github.com/deephaven-examples/deephaven-iceberg-rest-catalog/). It includes a Deephaven server, Iceberg REST catalog, MinIO object store, and MinIO client. The repository's README contains a full description of the deployment.

For this guide, you need only clone the repository, `cd` into the `Groovy` directory, and run `docker compose up`:

```bash
git clone git@github.com:deephaven-examples/deephaven-iceberg-rest-catalog.git
cd deephaven-iceberg-rest-catalog/Groovy
docker compose up
```

The deployment automates the creation of an Iceberg catalog with a single table that is used in this guide.

## Interact with the Iceberg catalog

After creating the Iceberg catalog and table, head to the [Deephaven IDE](http://localhost:10000/ide).

To interact with an Iceberg catalog, you must first create an [`IcebergCatalogAdapter`](/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html) instance. Since this guide uses a REST catalog, the adapter can be created using the generic [`createAdapter`](https://deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTools.html#createAdapter(org.apache.iceberg.catalog.Catalog)) method:

```groovy skip-test
import io.deephaven.iceberg.util.*

restAdapter = IcebergTools.createAdapter(
    "minio-iceberg",
    [
        "type": "rest",
        "uri": catalogUri,
        "client.region": awsRegion,
        "s3.access-key-id": awsAccessKeyId,
        "s3.secret-access-key": awsSecretAccessKey,
        "s3.endpoint": s3Endpoint,
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    ]
)
```

If you are working with a REST catalog backed by S3 storage, you can use the more specific [`createS3Rest`](https://deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergToolsS3.html#createS3Rest(java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String)) method:

```groovy docker-config=iceberg test-set=1 order=null reset
import io.deephaven.iceberg.util.*

restAdapter = IcebergToolsS3.createS3Rest(
    "minio-iceberg",        // catalog name
    catalogUri,             // catalog URI
    warehouseLocation,      // warehouse location
    awsRegion,              // region name
    awsAccessKeyId,         // access key ID
    awsSecretAccessKey,     // secret access key
    s3Endpoint,             // endpoint override
)
```

Once an [`IcebergCatalogAdapter`](/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html) has been created, it can query the namespaces and tables in a catalog. The following code block gets the top-level namespaces and tables in the `nyc` namespace:

```groovy docker-config=iceberg test-set=1 order=namespaces,tables
namespaces = restAdapter.namespaces()
tables = restAdapter.tables("nyc")
```

### Load an Iceberg table into Deephaven

To load the `nyc.taxis` Iceberg table into Deephaven, start by creating an instance of [`IcebergReadInstructions`](/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html) via the builder. Since the table doesn't change, the instructions tell Deephaven that it's static:

```groovy docker-config=iceberg test-set=1 order=null
staticInstructions = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.staticMode())
    .build()
```

This is an optional argument with the default being `static`. See [`IcebergReadInstructions`](/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html) for more information.

At this point, you can load a table from the catalog with [`loadTable`](https://deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#loadTable(java.lang.String)). This returns an [`IcebergTableAdapter`](/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html) rather than a Deephaven table. The table adapter provides you with several methods to read from or write to the underlying Iceberg table.

```groovy docker-config=iceberg test-set=1 order=null
icebergTaxis = restAdapter.loadTable("nyc.taxis")
```

With the table adapter and instructions in hand, the Iceberg table can be read into a Deephaven table.

```groovy docker-config=iceberg test-set=1 order=taxis
taxis = icebergTaxis.table(staticInstructions)
```

For greater control over the resultant Deephaven table and greater resilience to schema changes, use an [`UnboundResolver`](../../reference/data-import-export/Iceberg/unbound-resolver.md) to map the Iceberg table schema to a Deephaven table definition. The following code shows how to do so using field IDs so that if the Iceberg schema changes, the mapping is still valid:

```groovy docker-config=iceberg test-set=1 order=taxis
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

taxisDef = TableDefinition.of(
    ColumnDefinition.ofLong("VendorID"),
    ColumnDefinition.ofTime("PickupTime"),
    ColumnDefinition.ofTime("DropoffTime"),
    ColumnDefinition.ofDouble("NumPassengers"),
    ColumnDefinition.ofDouble("TripDistance"),
    ColumnDefinition.ofDouble("RateCodeID"),
    ColumnDefinition.ofString("StoreAndFwdFlag"),
    ColumnDefinition.ofLong("PickupLocationID"),
    ColumnDefinition.ofLong("DropoffLocationID"),
    ColumnDefinition.ofLong("PaymentType"),
    ColumnDefinition.ofDouble("FareAmount"),
    ColumnDefinition.ofDouble("Extra"),
    ColumnDefinition.ofDouble("MtaTax"),
    ColumnDefinition.ofDouble("Tip"),
    ColumnDefinition.ofDouble("Tolls"),
    ColumnDefinition.ofDouble("ImprovementSurcharge"),
    ColumnDefinition.ofDouble("TotalCost"),
    ColumnDefinition.ofDouble("CongestionSurcharge"),
    ColumnDefinition.ofDouble("AirportFee")
)

resolver = UnboundResolver.builder().
    definition(taxisDef).
    putColumnInstructions("VendorID", ColumnInstructions.schemaField(1)).
    putColumnInstructions("PickupTime", ColumnInstructions.schemaField(2)).
    putColumnInstructions("DropoffTime", ColumnInstructions.schemaField(3)).
    putColumnInstructions("NumPassengers", ColumnInstructions.schemaField(4)).
    putColumnInstructions("TripDistance", ColumnInstructions.schemaField(5)).
    putColumnInstructions("RateCodeID", ColumnInstructions.schemaField(6)).
    putColumnInstructions("StoreAndFwdFlag", ColumnInstructions.schemaField(7)).
    putColumnInstructions("PickupLocationID", ColumnInstructions.schemaField(8)).
    putColumnInstructions("DropoffLocationID", ColumnInstructions.schemaField(9)).
    putColumnInstructions("PaymentType", ColumnInstructions.schemaField(10)).
    putColumnInstructions("FareAmount", ColumnInstructions.schemaField(11)).
    putColumnInstructions("Extra", ColumnInstructions.schemaField(12)).
    putColumnInstructions("MtaTax", ColumnInstructions.schemaField(13)).
    putColumnInstructions("Tip", ColumnInstructions.schemaField(14)).
    putColumnInstructions("Tolls", ColumnInstructions.schemaField(15)).
    putColumnInstructions("ImprovementSurcharge", ColumnInstructions.schemaField(16)).
    putColumnInstructions("TotalCost", ColumnInstructions.schemaField(17)).
    putColumnInstructions("CongestionSurcharge", ColumnInstructions.schemaField(18)).
    putColumnInstructions("AirportFee", ColumnInstructions.schemaField(19)).
    build()

options = LoadTableOptions.builder().id("nyc.taxis").resolver(resolver).build()

icebergTaxis = restAdapter.loadTable(options)

taxis = icebergTaxis.table()
```

### Write Deephaven tables to Iceberg

To write one or more Deephaven tables to Iceberg, first create the table(s) you want to write. This example uses two tables:

```groovy docker-config=iceberg test-set=1 order=source2024,source2025
source2024 = emptyTable(100).update("Year = 2024", "X = i", "Y = 2 * X", "Z = randomDouble(-1, 1)")
source2025 = emptyTable(50).update("Year = 2025", "X = 100 + i", "Y = 3 * X", "Z = randomDouble(-100, 100)")
```

Writing multiple Deephaven tables to the same Iceberg table _requires_ that the tables have the same definition, regardless of whether or not the Iceberg table is partitioned.

#### Unpartitioned Iceberg tables

When writing data to an unpartitioned Iceberg table, you need the Deephaven table definition:

```groovy docker-config=iceberg test-set=1 order=null
sourceDef = source2024.getDefinition()
```

Then, create an [`IcebergTableAdapter`](/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html) from the `source2024` table's definition, and a table identifier, which must include the Iceberg namespace (`nyc`):

<!-- This reset is needed because another example also creates nyc.source table and that throws an error if it already exists -->

```groovy docker-config=iceberg test-set=1 order=null reset
sourceAdapter = restAdapter.createTable("nyc.source", sourceDef)
```

To write the table to Iceberg, you'll need to create an [`IcebergTableWriter`](/core/javadoc/io/deephaven/iceberg/util/IcebergTableWriter.html). A single writer instance with a fixed table definition can write as many Deephaven tables as desired, given that all tables have the same definition as provided to the writer. Most of the heavy lifting is done when the writer is created, so it's more efficient to create a writer once and write many tables than to create a writer for each table.

To create a writer instance, you need to define the [`TableParquetWriterOptions`](/core/javadoc/io/deephaven/iceberg/util/TableParquetWriterOptions.html) to configure the writer:

```groovy docker-config=iceberg test-set=1 order=null
import io.deephaven.extensions.s3.*
import org.apache.iceberg.catalog.*

// Define the writer options
writerOptions = TableParquetWriterOptions.builder()
    .tableDefinition(sourceDef)
    .build()

// Create the writer
sourceWriter = sourceAdapter.tableWriter(writerOptions)
```

Now you can write the data to Iceberg. The following code block writes the `source2024` and `source2025` tables to the Iceberg table `nyc.source`:

```groovy docker-config=iceberg test-set=1 order=null
sourceWriter.append(IcebergWriteInstructions.builder()
    .addTables(source2024, source2025).build())
```

#### Partitioned Iceberg tables

To write data to a partitioned Iceberg table, you must specify one or more partitioning columns in the [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html):

```groovy docker-config=iceberg test-set=1 order=null
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition

sourceDefPartitioned = TableDefinition.of(
    ColumnDefinition.ofInt("Year").withPartitioning(),
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofInt("Y"),
    ColumnDefinition.ofDouble("Z")
)
```

First, create an [`IcebergTableAdapter`](/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html) from the `source` table's definition, and a table identifier, which must include the Iceberg namespace (`nyc`):

```groovy docker-config=iceberg test-set=1 order=null
sourceAdapterPartitioned = restAdapter.createTable("nyc.sourcePartitioned", sourceDefPartitioned)
```

To write the table to Iceberg, you'll need to create an [`IcebergTableWriter`](/core/javadoc/io/deephaven/iceberg/util/IcebergTableWriter.html). A single writer instance with a fixed table definition can write as many Deephaven tables as desired, given that all tables have the same definition as provided to the writer. Most of the heavy lifting is done when the writer is created, so it's more efficient to create a writer once and write many tables than to create a writer for each table.

To create a writer instance, you need to define the [`TableParquetWriterOptions`](/core/javadoc/io/deephaven/iceberg/util/TableParquetWriterOptions.html) to configure the writer:

```groovy docker-config=iceberg test-set=1 order=null
import io.deephaven.extensions.s3.*
import org.apache.iceberg.catalog.*

// Define the writer options
writerOptionsPartitioned = TableParquetWriterOptions.builder()
    .tableDefinition(sourceDefPartitioned)
    .build()

// Create the writer
sourceWriterPartitioned = sourceAdapterPartitioned.tableWriter(writerOptionsPartitioned)
```

Now you can write the data to Iceberg. The following code block writes the `source_2024` and `source_2025` tables to the `nyc.source_partitioned` table. The partition paths are specified in the [`IcebergWriteInstructions`](/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.Builder.html):

```groovy docker-config=iceberg test-set=1 order=null
sourceWriterPartitioned.append(IcebergWriteInstructions.builder()
    .addTables(source2024.dropColumns("Year"), source2025.dropColumns("Year"))
    .addPartitionPaths("Year=2024", "Year=2025")
    .build())
```

> [!NOTE]
> The partitioning column(s) cannot be written to Iceberg, as they are already specified in the partition path. The above example drops them from the Deephaven tables before writing.

#### Check the write operations

Deephaven currently only supports appending data to Iceberg tables. Each append operation creates a new snapshot. When multiple tables are written in a single `append` call, all tables are written in the same snapshot.

Similarly, you can also write to a partitioned Iceberg table by providing the exact partition path where each Deephaven table should be appended. See [`IcebergWriteInstructions`](/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.html) for more information.

Check that the operations worked by reading the Iceberg tables back into Deephaven using the same table adapter:

```groovy docker-config=iceberg test-set=1 order=sourceFromIceberg,sourcePartitionedFromIceberg
sourceFromIceberg = sourceAdapter.table()
sourcePartitionedFromIceberg = sourceAdapterPartitioned.table()
```

### Custom Iceberg instructions

You can set custom instructions when reading from or writing to Iceberg in Deephaven. The following sections deal with different custom instructions you can set.

#### Refreshing Iceberg tables

Deephaven also supports reading refreshing Iceberg tables. The [`IcebergUpdateMode`](/core/javadoc/io/deephaven/iceberg/util/IcebergUpdateMode.html) class has three different supported update modes:

- Static
- Refreshed manually
- Refreshed automatically

The examples above cover the static case. The following code block creates update modes for manually and automatically refreshing Iceberg tables. For automatically refreshing tables, the refresh interval can be set as an integer number of milliseconds. If no interval is set, the default is once per minute.

```groovy order=null
import io.deephaven.iceberg.util.IcebergUpdateMode

// Manually refreshing
manualRefresh = IcebergUpdateMode.manualRefreshingMode()

// Automatically refreshing every minute
autoRefreshEveryMinute = IcebergUpdateMode.autoRefreshingMode()

// Automatically refreshing every 30 seconds
autoRefreshEvery30Seconds = IcebergUpdateMode.autoRefreshingMode(30_000)
```

#### Table definition

You can specify the resultant table definition when building [`IcebergReadInstructions`](/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html). This is useful when Deephaven cannot automatically infer the correct data types for an Iceberg table. The following code block defines a custom table definition to use when reading from Iceberg:

```groovy order=null
import io.deephaven.iceberg.util.IcebergReadInstructions
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition

defInstructions = IcebergReadInstructions.builder()
    .tableDefinition(
        TableDefinition.of(
            ColumnDefinition.ofLong("ID"),
            ColumnDefinition.ofTime("Timestamp"),
            ColumnDefinition.ofString("Operation"),
            ColumnDefinition.ofString("Summary")
        )
    )
    .build()
```

#### Column renames

You can rename columns when reading from Iceberg as well:

```groovy order=null
import io.deephaven.iceberg.util.IcebergReadInstructions

icebergInstructionsRenames = IcebergReadInstructions.builder()
    .putAllColumnRenames(
        Map.of(
            "tpep_pickup_datetime", "PickupTime",
            "tpep_dropoff_datetime", "DropoffTime",
            "passenger_count", "NumPassengers",
            "trip_distance", "Distance",
        )
    )
```

#### Snapshot ID

You can tell Deephaven to read a specific snapshot of an Iceberg table based on its snapshot ID:

```groovy order=null
import io.deephaven.iceberg.util.IcebergReadInstructions

snapshotInstructions = IcebergReadInstructions.builder()
    .snapshotId(1234567890L)
    .build()
```

## Next steps

This guide presented a basic example of reading from and writing to an Iceberg catalog in Deephaven. These examples can be extended to include other catalog types, more complex queries, catalogs with multiple namespaces, snapshots, custom instructions, and more.

## Related documentation

- [`BuildCatalogOptions`](../../reference/data-import-export/Iceberg/build-catalog-options.md)
- [`IcebergCatalogAdapter`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md)
- [`IcebergReadInstructions`](../../reference/data-import-export/Iceberg/iceberg-read-instructions.md)
- [`IcebergTable`](../../reference/data-import-export/Iceberg/iceberg-table.md)
- [`IcebergTableAdapter`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md)
- [`IcebergTableWriter`](../../reference/data-import-export/Iceberg/iceberg-table-writer.md)
- [`IcebergWriteInstructions`](../../reference/data-import-export/Iceberg/iceberg-write-instructions.md)
- [`InferenceResolver`](../../reference/data-import-export/Iceberg/inference-resolver.md)
- [`LoadTableOptions`](../../reference/data-import-export/Iceberg/load-table-options.md)
- [`UnboundResolver`](../../reference/data-import-export/Iceberg/unbound-resolver.md)
- [Javadoc](/core/javadoc/io/deephaven/iceberg/util/package-summary.html)
