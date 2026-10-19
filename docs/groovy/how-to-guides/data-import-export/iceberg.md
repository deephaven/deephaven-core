---
title: Iceberg and Deephaven
---

[Apache Iceberg](https://iceberg.apache.org/) is a high-performance format for tabular data. Deephaven's Iceberg integration enables users to interact with Iceberg catalogs, namespaces, tables, and snapshots. An Iceberg catalog keeps track of a set of Iceberg tables and groups them into namespaces, much as a database groups its tables into schemas. A snapshot is the state of an Iceberg table at one point in time. Each write to the table creates a new snapshot.

This guide walks through the following tasks:

- Connect to an Iceberg catalog.
- Read an Iceberg table into Deephaven as a static table, as a refreshing table, or as of a specific snapshot.
- Write Deephaven tables to new unpartitioned and partitioned Iceberg tables in the same namespace as the table this guide reads.

The examples in this guide use a [REST catalog](https://iceberg.apache.org/rest-catalog-spec/).

Deephaven's Iceberg integration works with many types of catalogs, including:

- REST
- AWS Glue
- JDBC
- Hive
- Hadoop
- Nessie

> [!NOTE]
> Hive and Nessie catalogs require adding their Iceberg catalog modules (`iceberg-hive-metastore` or `iceberg-nessie`) to your classpath, and a JDBC catalog requires a JDBC driver for its database.

## Deephaven's Iceberg API

The user-facing classes of Deephaven's Iceberg integration are in the [`io.deephaven.iceberg.util`](/core/javadoc/io/deephaven/iceberg/util/package-summary.html) package. The examples in this guide use this package, plus [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html) and [`ColumnDefinition`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) from `io.deephaven.engine.table` to define table schemas.

## A Deephaven deployment for Iceberg

The examples in this guide use the Docker deployment found in the [Deephaven example Iceberg REST catalog deployment](https://github.com/deephaven-examples/deephaven-iceberg-rest-catalog/). It includes a Deephaven server, Iceberg REST catalog, MinIO object store, MinIO client, and an Apache Spark server that creates the sample Iceberg table and then shuts down. The repository's README contains a full description of the deployment.

For this guide, you need only clone the repository, `cd` into the `Groovy` directory, and run `docker compose up`:

```bash
git clone https://github.com/deephaven-examples/deephaven-iceberg-rest-catalog.git
cd deephaven-iceberg-rest-catalog/Groovy
docker compose up
```

The deployment creates an Iceberg catalog with one table, `taxis`, in the `nyc` namespace. This guide reads that table.

The deployment also uses [application mode](../application-mode.md) to define the connection variables that the examples use: `catalogUri`, `awsRegion`, `awsAccessKeyId`, `awsSecretAccessKey`, and `s3Endpoint`. Their values come from the deployment's environment variables, so `s3Endpoint` points to the deployment's MinIO server. If you use a different deployment, define these variables yourself before running the examples.

After the deployment starts, open the Deephaven IDE at `http://localhost:10000/ide` to run the examples.

## Interact with the Iceberg catalog

To interact with an Iceberg catalog, you must first create an [`IcebergCatalogAdapter`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md) instance. Create one with the [`createAdapter`](/core/javadoc/io/deephaven/iceberg/util/IcebergTools.html#createAdapter(java.lang.String,java.util.Map)) method, which takes a catalog name and the catalog's configuration properties:

```groovy docker-config=iceberg test-set=1 order=null reset
import io.deephaven.iceberg.util.*

restAdapter = IcebergTools.createAdapter(
    "generic-adapter",
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

The same method creates adapters for the other catalog types listed above. Set the `type` property to the catalog type, such as `"glue"` for an AWS Glue catalog, and supply the properties that catalog needs.

> [!NOTE]
> If your Iceberg tables are in S3-compatible storage, you can control S3 connectivity with [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) from the [`io.deephaven.extensions.s3`](/core/javadoc/io/deephaven/extensions/s3/package-summary.html) package. You can pass them in three places:
>
> - When you create the adapter, use [`IcebergToolsS3.createAdapter`](/core/javadoc/io/deephaven/iceberg/util/IcebergToolsS3.html#createAdapter(io.deephaven.iceberg.util.BuildCatalogOptions,io.deephaven.extensions.s3.S3Instructions)) instead of `IcebergTools.createAdapter`. It takes [`BuildCatalogOptions`](../../reference/data-import-export/Iceberg/build-catalog-options.md) and `S3Instructions`.
> - When you read a table, pass them to the `dataInstructions` method of the [`IcebergReadInstructions`](../../reference/data-import-export/Iceberg/iceberg-read-instructions.md) builder.
> - When you write a table, pass them to the `dataInstructions` method of the [`TableParquetWriterOptions`](/core/javadoc/io/deephaven/iceberg/util/TableParquetWriterOptions.html) builder.
>
> If you don't pass them, Deephaven derives the S3 settings from the catalog properties. The examples in this guide don't need them.

The catalog adapter can query the namespaces and tables in a catalog. The following code block calls [`namespaces`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md#methods) to get the top-level namespaces and [`tables`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md#methods) to get the tables in the `nyc` namespace:

```groovy docker-config=iceberg test-set=1 order=namespaces,tables
namespaces = restAdapter.namespaces()
tables = restAdapter.tables("nyc")
```

## Read Iceberg tables

### Load an Iceberg table into Deephaven

To load the `nyc.taxis` Iceberg table, call [`loadTable`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md#methods). Identify the table as `namespace.table`, here `nyc.taxis`. The method returns an [`IcebergTableAdapter`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md) rather than a Deephaven table. The table adapter provides you with several methods to read from or write to the underlying Iceberg table.

```groovy docker-config=iceberg test-set=1 order=null
icebergTaxis = restAdapter.loadTable("nyc.taxis")
```

To read it into a Deephaven table, pass [`IcebergReadInstructions`](../../reference/data-import-export/Iceberg/iceberg-read-instructions.md) to [`table`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md#methods). This argument is optional. Without it, Deephaven reads the table in static mode. The `IcebergReadInstructions` reference page lists every instruction you can set.

The following instructions set the update mode to [`IcebergUpdateMode.staticMode`](../../reference/data-import-export/Iceberg/iceberg-update-mode.md#constructors), which loads the table once without refreshing it. [Read a refreshing Iceberg table](#read-a-refreshing-iceberg-table) covers the other update modes:

```groovy docker-config=iceberg test-set=1 order=null
staticInstructions = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.staticMode())
    .build()
```

Now that you have the table adapter and the instructions, read the table into a Deephaven table with [`table`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md#methods):

```groovy docker-config=iceberg test-set=1 order=taxis
taxis = icebergTaxis.table(staticInstructions)
```

For greater control over the resultant Deephaven table and greater resilience to schema changes, use an [`UnboundResolver`](../../reference/data-import-export/Iceberg/unbound-resolver.md) to map the Iceberg table schema to a Deephaven table definition. The following code maps columns by field ID. Iceberg gives every column in a schema a numeric field ID that stays the same when the column is renamed, so the mapping still works after columns are renamed or reordered. To load the table with the resolver, pass the table identifier and the resolver to [`loadTable`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md#methods) in a [`LoadTableOptions`](../../reference/data-import-export/Iceberg/load-table-options.md).

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

resolver = UnboundResolver.builder()
    .definition(taxisDef)
    .putColumnInstructions("VendorID", ColumnInstructions.schemaField(1))
    .putColumnInstructions("PickupTime", ColumnInstructions.schemaField(2))
    .putColumnInstructions("DropoffTime", ColumnInstructions.schemaField(3))
    .putColumnInstructions("NumPassengers", ColumnInstructions.schemaField(4))
    .putColumnInstructions("TripDistance", ColumnInstructions.schemaField(5))
    .putColumnInstructions("RateCodeID", ColumnInstructions.schemaField(6))
    .putColumnInstructions("StoreAndFwdFlag", ColumnInstructions.schemaField(7))
    .putColumnInstructions("PickupLocationID", ColumnInstructions.schemaField(8))
    .putColumnInstructions("DropoffLocationID", ColumnInstructions.schemaField(9))
    .putColumnInstructions("PaymentType", ColumnInstructions.schemaField(10))
    .putColumnInstructions("FareAmount", ColumnInstructions.schemaField(11))
    .putColumnInstructions("Extra", ColumnInstructions.schemaField(12))
    .putColumnInstructions("MtaTax", ColumnInstructions.schemaField(13))
    .putColumnInstructions("Tip", ColumnInstructions.schemaField(14))
    .putColumnInstructions("Tolls", ColumnInstructions.schemaField(15))
    .putColumnInstructions("ImprovementSurcharge", ColumnInstructions.schemaField(16))
    .putColumnInstructions("TotalCost", ColumnInstructions.schemaField(17))
    .putColumnInstructions("CongestionSurcharge", ColumnInstructions.schemaField(18))
    .putColumnInstructions("AirportFee", ColumnInstructions.schemaField(19))
    .build()

options = LoadTableOptions.builder().id("nyc.taxis").resolver(resolver).build()

icebergTaxis = restAdapter.loadTable(options)

taxis = icebergTaxis.table()
```

To map a Deephaven column to an Iceberg field by name rather than by field ID, use [`ColumnInstructions.schemaFieldName`](/core/javadoc/io/deephaven/iceberg/util/ColumnInstructions.html#schemaFieldName(java.lang.String)), such as `putColumnInstructions("PickupTime", ColumnInstructions.schemaFieldName("tpep_pickup_datetime"))`.

The resolver sets the table definition and column names. [`IcebergReadInstructions`](../../reference/data-import-export/Iceberg/iceberg-read-instructions.md) does not set either.

### Read a refreshing Iceberg table

Deephaven can also read an Iceberg table as a live, refreshing table. When Deephaven loads a new snapshot of the Iceberg table, the Deephaven table updates, and so does every query built on it. The [`IcebergUpdateMode`](../../reference/data-import-export/Iceberg/iceberg-update-mode.md) class has three update modes:

- [`staticMode`](../../reference/data-import-export/Iceberg/iceberg-update-mode.md#constructors): Deephaven loads the Iceberg table once and never refreshes it.
- [`manualRefreshingMode`](../../reference/data-import-export/Iceberg/iceberg-update-mode.md#constructors): Deephaven loads the table once and refreshes it only when you request a refresh.
- [`autoRefreshingMode`](../../reference/data-import-export/Iceberg/iceberg-update-mode.md#constructors): Deephaven loads the table once and refreshes it at a set interval, which defaults to once per minute.

The examples above use static Iceberg tables. To refresh a manually refreshing table, call [`update`](../../reference/data-import-export/Iceberg/iceberg-table.md#methods) on the [`IcebergTable`](../../reference/data-import-export/Iceberg/iceberg-table.md) that [`table`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md#methods) returns. Without an argument, `update` refreshes the table to the latest snapshot. To refresh to a specific snapshot instead, pass its ID. The snapshot must be newer than the one the table currently shows. [Read a specific snapshot](#read-a-specific-snapshot) shows how to list snapshot IDs, and [Refresh a table after a write](#refresh-a-table-after-a-write) shows a manual refresh picking up new data.

To set a refresh interval other than one minute, pass it in milliseconds to [`autoRefreshingMode`](../../reference/data-import-export/Iceberg/iceberg-update-mode.md#constructors).

The following code block sets the update mode in [`IcebergReadInstructions`](../../reference/data-import-export/Iceberg/iceberg-read-instructions.md) and reads the same Iceberg table in the other two modes:

```groovy docker-config=iceberg test-set=1 ticking-table order=null
// Manually refreshing
manualInstructions = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.manualRefreshingMode())
    .build()
taxisManual = icebergTaxis.table(manualInstructions)

// Automatically refreshing every minute
autoInstructionsEveryMinute = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.autoRefreshingMode())
    .build()
taxisAutoEveryMinute = icebergTaxis.table(autoInstructionsEveryMinute)

// Automatically refreshing every 30 seconds
autoInstructionsEvery30Seconds = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.autoRefreshingMode(30_000))
    .build()
taxisAutoEvery30Seconds = icebergTaxis.table(autoInstructionsEvery30Seconds)
```

### Read a specific snapshot

By default, [`table`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md#methods) reads the most recent snapshot. To read a specific snapshot instead, set its ID with the `snapshotId` method of the [`IcebergReadInstructions`](../../reference/data-import-export/Iceberg/iceberg-read-instructions.md) builder.

The [`snapshots`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md#methods) method returns the snapshots of an Iceberg table as a Deephaven table, with each snapshot's ID in the `Id` column. The [`listSnapshots`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md#methods) method returns them as a list.

The deployment empties the `taxis` table before loading data into it, so the table's first snapshot has no rows. The following code reads the table as of that first snapshot, so `taxisAtSnapshot` has the table's columns but no rows:

```groovy docker-config=iceberg test-set=1 order=taxisSnapshots,taxisAtSnapshot
taxisSnapshots = icebergTaxis.snapshots()

firstSnapshotId = icebergTaxis.listSnapshots().get(0).snapshotId()

snapshotInstructions = IcebergReadInstructions.builder()
    .snapshotId(firstSnapshotId)
    .build()

taxisAtSnapshot = icebergTaxis.table(snapshotInstructions)
```

## Write Deephaven tables to Iceberg

Deephaven writes to Iceberg tables by appending data. Each append operation creates a new snapshot. A single [`append`](../../reference/data-import-export/Iceberg/iceberg-table-writer.md#methods) call that writes multiple tables writes them all in the same snapshot. To write data files without committing a snapshot, so that you can commit them in your own Iceberg transaction, use the writer's [`writeDataFiles`](/core/javadoc/io/deephaven/iceberg/util/IcebergTableWriter.html#writeDataFiles(io.deephaven.iceberg.util.IcebergWriteInstructions)) method.

To write tables, you create an [`IcebergTableWriter`](../../reference/data-import-export/Iceberg/iceberg-table-writer.md). A single writer can write any number of Deephaven tables, as long as each table has the writer's table definition without its partitioning columns:

- For an unpartitioned Iceberg table, each table has the writer's whole table definition.
- For a partitioned Iceberg table, each table leaves out the partitioning columns, and a partition path supplies their values. See [Partitioned Iceberg tables](#partitioned-iceberg-tables).

Each writer belongs to the Iceberg table whose adapter created it. Creating the writer checks its table definition against the Iceberg table's schema, and writes made with it skip that check, so they are faster. Each write still checks that its tables match the writer's table definition and that its partition paths match the table's partitioning.

Because most of the work happens when you create the writer, create one writer per Iceberg table and reuse it for every Deephaven table you append to that table.

To write Deephaven tables to Iceberg, first create the tables you want to write. This example uses two tables:

```groovy docker-config=iceberg test-set=1 order=source2024,source2025
source2024 = emptyTable(100).update("Year = 2024", "X = i", "Y = 2 * X", "Z = randomDouble(-1, 1)")
source2025 = emptyTable(50).update("Year = 2025", "X = 100 + i", "Y = 3 * X", "Z = randomDouble(-100, 100)")
```

### Unpartitioned Iceberg tables

When writing data to an unpartitioned Iceberg table, you need the Deephaven table definition:

```groovy docker-config=iceberg test-set=1 order=null
sourceDef = source2024.getDefinition()
```

Then, create the Iceberg table with [`createTable`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md#methods), which returns an [`IcebergTableAdapter`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md). It takes a table identifier of the form `namespace.table` and a table definition. This example creates the table in the `nyc` namespace, which `createTable` would create if it didn't exist.

`createTable` fails if the table already exists. So that you can run this guide more than once, the following code first drops any existing `nyc.source` table. It calls the [`dropTable`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/catalog/Catalog.html#dropTable(org.apache.iceberg.catalog.TableIdentifier,boolean)) method of the Iceberg catalog that the adapter's [`catalog`](/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#catalog()) method returns. Its second argument, `true`, also deletes the table's data files. If the table doesn't exist, `dropTable` returns `false` instead of failing.

<!-- This reset is needed because another example also creates nyc.source table and that throws an error if it already exists -->

```groovy docker-config=iceberg test-set=1 order=null reset
import org.apache.iceberg.catalog.TableIdentifier

// Drop nyc.source if it exists, so that this example can run more than once
restAdapter.catalog().dropTable(TableIdentifier.parse("nyc.source"), true)

sourceAdapter = restAdapter.createTable("nyc.source", sourceDef)
```

To create a writer, first define the [`TableParquetWriterOptions`](/core/javadoc/io/deephaven/iceberg/util/TableParquetWriterOptions.html) that configure it, then pass them to [`tableWriter`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md#methods):

```groovy docker-config=iceberg test-set=1 order=null
// Define the writer options
writerOptions = TableParquetWriterOptions.builder()
    .tableDefinition(sourceDef)
    .build()

// Create the writer
sourceWriter = sourceAdapter.tableWriter(writerOptions)
```

Now you can write the data to Iceberg. The following code block adds the `source2024` and `source2025` tables to an [`IcebergWriteInstructions`](../../reference/data-import-export/Iceberg/iceberg-write-instructions.md) builder and passes the instructions to [`append`](../../reference/data-import-export/Iceberg/iceberg-table-writer.md#methods), which writes them to the Iceberg table `nyc.source`:

```groovy docker-config=iceberg test-set=1 order=null
sourceWriter.append(IcebergWriteInstructions.builder()
    .addTables(source2024, source2025).build())
```

### Partitioned Iceberg tables

To write data to a partitioned Iceberg table, you must define the [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html) yourself and mark one or more columns as partitioning columns. Call [`withPartitioning`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html#withPartitioning()) on the [`ColumnDefinition`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) of each partitioning column.

Deephaven can write only to partitioned Iceberg tables whose partition values are plain column values (identity partitioning), with partitioning columns of type boolean, int, long, float, double, `String`, or `LocalDate`. For example, it can't append to `nyc.taxis`, which is partitioned by the day of `tpep_pickup_datetime`. Tables you create with [`createTable`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md#methods) always use identity partitioning.

The following code defines a table with the partitioning column `Year`:

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

Then, create the partitioned Iceberg table with [`createTable`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md#methods), which returns an [`IcebergTableAdapter`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md). As before, the code first drops any existing table with the same identifier:

```groovy docker-config=iceberg test-set=1 order=null
// Drop nyc.sourcePartitioned if it exists, so that this example can run more than once
restAdapter.catalog().dropTable(TableIdentifier.parse("nyc.sourcePartitioned"), true)

sourceAdapterPartitioned = restAdapter.createTable("nyc.sourcePartitioned", sourceDefPartitioned)
```

As with unpartitioned tables, create a writer with [`TableParquetWriterOptions`](/core/javadoc/io/deephaven/iceberg/util/TableParquetWriterOptions.html):

```groovy docker-config=iceberg test-set=1 order=null
// Define the writer options
writerOptionsPartitioned = TableParquetWriterOptions.builder()
    .tableDefinition(sourceDefPartitioned)
    .build()

// Create the writer
sourceWriterPartitioned = sourceAdapterPartitioned.tableWriter(writerOptionsPartitioned)
```

Now you can write the data to Iceberg. The `addPartitionPaths` method of the [`IcebergWriteInstructions`](../../reference/data-import-export/Iceberg/iceberg-write-instructions.md) builder takes one partition path per table, in the same order as `addTables`. A partition path names the partition that a table's rows go to, written as `column=value`, with pairs separated by `/` when the table has more than one partitioning column. In this example, `Year=2024` sends the rows of `source2024` to the 2024 partition.

Drop the partitioning column from each table before writing, because the partition path supplies its value. The following code block uses [`dropColumns`](../../reference/table-operations/select/drop-columns.md) to remove the `Year` column from the `source2024` and `source2025` tables and writes them to the `nyc.sourcePartitioned` table:

```groovy docker-config=iceberg test-set=1 order=null
sourceWriterPartitioned.append(IcebergWriteInstructions.builder()
    .addTables(source2024.dropColumns("Year"), source2025.dropColumns("Year"))
    .addPartitionPaths("Year=2024", "Year=2025")
    .build())
```

### Check the write operations

Check that the writes worked by reading each Iceberg table back into Deephaven with the table adapter that created it:

```groovy docker-config=iceberg test-set=1 order=sourceFromIceberg,sourcePartitionedFromIceberg
sourceFromIceberg = sourceAdapter.table()
sourcePartitionedFromIceberg = sourceAdapterPartitioned.table()
```

### Refresh a table after a write

Now that `nyc.source` has data, let's watch a manually refreshing table pick up a new snapshot. The following code block reads `nyc.source` in manual refresh mode and saves a static copy of it with [`snapshot`](../../reference/table-operations/snapshot/snapshot.md). It then appends `source2025` to `nyc.source` a second time, which creates a new snapshot, and calls [`update`](../../reference/data-import-export/Iceberg/iceberg-table.md#methods) to refresh `sourceManual` to that snapshot. `sourceManualBefore` keeps the original 150 rows, while `sourceManual` updates to 200 rows:

```groovy docker-config=iceberg test-set=1 ticking-table order=sourceManualBefore,sourceManual
sourceManual = sourceAdapter.table(IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.manualRefreshingMode())
    .build())

// Save a static copy of the table as it is now
sourceManualBefore = sourceManual.snapshot()

// Append source2025 again, which creates a new snapshot of nyc.source
sourceWriter.append(IcebergWriteInstructions.builder()
    .addTables(source2025).build())

// Refresh sourceManual to the new snapshot
sourceManual.update()
```

Any query you build on `sourceManual` updates along with it. With [`autoRefreshingMode`](../../reference/data-import-export/Iceberg/iceberg-update-mode.md#constructors), Deephaven checks for new snapshots on its own, so your queries keep up with the Iceberg table as other systems write to it.

## Next steps

This guide connected Deephaven to an Iceberg catalog, read an Iceberg table as a static table, a refreshing table, and a past snapshot, and appended Deephaven tables to new Iceberg tables. From here, try the examples against your own catalog, or build real-time queries on refreshing Iceberg tables. The reference pages below cover every option.

## Related documentation

- [`BuildCatalogOptions`](../../reference/data-import-export/Iceberg/build-catalog-options.md)
- [`IcebergCatalogAdapter`](../../reference/data-import-export/Iceberg/iceberg-catalog-adapter.md)
- [`IcebergTools`](/core/javadoc/io/deephaven/iceberg/util/IcebergTools.html)
- [`IcebergReadInstructions`](../../reference/data-import-export/Iceberg/iceberg-read-instructions.md)
- [`IcebergTable`](../../reference/data-import-export/Iceberg/iceberg-table.md)
- [`IcebergTableAdapter`](../../reference/data-import-export/Iceberg/iceberg-table-adapter.md)
- [`IcebergTableWriter`](../../reference/data-import-export/Iceberg/iceberg-table-writer.md)
- [`IcebergUpdateMode`](../../reference/data-import-export/Iceberg/iceberg-update-mode.md)
- [`IcebergWriteInstructions`](../../reference/data-import-export/Iceberg/iceberg-write-instructions.md)
- [`InferenceResolver`](../../reference/data-import-export/Iceberg/inference-resolver.md)
- [`LoadTableOptions`](../../reference/data-import-export/Iceberg/load-table-options.md)
- [`TableParquetWriterOptions`](/core/javadoc/io/deephaven/iceberg/util/TableParquetWriterOptions.html)
- [`UnboundResolver`](../../reference/data-import-export/Iceberg/unbound-resolver.md)
- [Iceberg Javadoc](/core/javadoc/io/deephaven/iceberg/util/package-summary.html)
