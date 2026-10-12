---
title: Import and Export Data
---

Data I/O is mission-critical for any real-time data analysis platform. Deephaven supports a wide variety of data sources and formats, including [CSV](../../reference/cheat-sheets/csv.md), [Parquet](../../reference/cheat-sheets/parquet.md), [Kafka](../../reference/cheat-sheets/kafka.md), and more. This document covers those formats in Deephaven.

## CSV

Deephaven can [read CSV](../../how-to-guides/data-import-export/csv-import.md) files that exist locally or remotely.

> [!TIP]
> To import a CSV without writing code, use **Upload Table from File** in the Console Options menu (⋮). See [Upload a table from a file](../../how-to-guides/user-interface/upload-table-from-file.md).

This example reads a local CSV file.

```groovy test-set=1 order=iris
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv("/data/examples/Iris/csv/iris.csv")
```

It can also [write data to CSV](../../how-to-guides/data-import-export/csv-export.md). The code below writes that same table back to a local CSV file.

```groovy test-set=1 order=null
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(iris, "/data/irisNew.csv")
```

Just to show that it's there:

```groovy test-set=1 order=irisNew
irisNew = readCsv("/data/irisNew.csv")
```

## Parquet

[Apache Parquet](../../reference/cheat-sheets/parquet.md) is a columnar storage format that supports compression to store more data in less space. Deephaven supports reading and writing single Parquet files and partitioned Parquet directories. Parquet data can be stored locally or in [S3](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html).

The example below reads from a local Parquet file.

```groovy test-set=2 order=cryptoTrades
import io.deephaven.parquet.table.ParquetTools

cryptoTrades = ParquetTools.readTable(
    "/data/examples/CryptoCurrencyHistory/Parquet/CryptoTrades_20210922.parquet"
)
```

That same table can be written back to a Parquet file:

```groovy test-set=2 order=null
ParquetTools.writeTable(cryptoTrades, "/data/cryptoTradesNew.parquet")
```

Just to show that it worked:

```groovy test-set=2 order=cryptoTradesNew
cryptoTradesNew = ParquetTools.readTable("/data/cryptoTradesNew.parquet")
```

The example below reads a Parquet file from S3. This example uses [RustFS](https://rustfs.com/) as a local S3-compatible object store. The `S3Instructions` class in Deephaven provides a way to specify how to connect to the S3 instance.

```groovy test-set=7 docker-config=rustfs order=grades
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

// pass the S3 URL, as well as instructions on how to talk to the S3 instance
credentials = Credentials.basic("example_username", "example_password")

grades = ParquetTools.readTable(
    "s3://example-bucket/grades/grades.parquet",
    ParquetInstructions.builder()
        .setSpecialInstructions(
            S3Instructions.builder()
                .regionName("us-east-1")
                .endpointOverride("http://rustfs.example.com:9000")
                .credentials(credentials)
                .build()
        )
        .build()
)
```

## Kafka

[Apache Kafka](../../how-to-guides/data-import-export/kafka-stream.md) is a distributed event streaming platform that can be used to publish and subscribe to streams of records. Deephaven can consume and publish to Kafka streams. The code below consumes a stream.

```groovy test-set=3 docker-config=kafka order=resultAppend
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

resultAppend = KafkaTools.consumeToTable(
    kafkaProps,
    "test.topic",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec("Command", java.lang.String),
    KafkaTools.TableType.append()
)
```

Similarly, this code publishes the data in a Deephaven table to a Kafka stream.

```groovy test-set=4 docker-config=kafka order=source
import io.deephaven.kafka.KafkaTools

// create ticking table to publish to Kafka stream
source = timeTable("PT1s").update("X = i")

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

// publish to time-topic
writeTopic = KafkaTools.produceFromTable(
    source,
    kafkaProps,
    "time-topic",
    KafkaTools.Produce.IGNORE,
    KafkaTools.Produce.simpleSpec('X'),
    false
)
```

## Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is a high-performance, open table format for huge analytic datasets. Deephaven's `io.deephaven.iceberg` package allows you to interact with Iceberg tables. For more on Deephaven's Iceberg integration, see the [Iceberg user guide](../../how-to-guides/data-import-export/iceberg.md).

The following example reads data from an existing Iceberg table into a Deephaven table. It uses a custom Docker deployment found [here](../../how-to-guides/data-import-export/iceberg.md#a-deephaven-deployment-for-iceberg).

```groovy test-set=5 docker-config=iceberg order=deephavenTable
import io.deephaven.iceberg.util.IcebergTools
import io.deephaven.iceberg.util.IcebergReadInstructions
import io.deephaven.iceberg.util.IcebergUpdateMode

// Configure the Iceberg catalog adapter for a REST catalog.
icebergCatalogAdapter = IcebergTools.createAdapter(
    "rustfs-iceberg",
    [
        "type": "rest",
        "uri": "http://rest:8181",
        "warehouse": "s3a://warehouse/wh",
        "client.region": "us-east-1",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.endpoint": "http://rustfs:9000",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    ]
)

// Load the Iceberg table adapter, assuming 'nyc.taxis' exists.
myIcebergTableAdapter = icebergCatalogAdapter.loadTable("nyc.taxis")

// Read the static Iceberg table into Deephaven.
staticInstructions = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.staticMode())
    .build()
deephavenTable = myIcebergTableAdapter.table(staticInstructions)

// Now 'deephavenTable' can be used like any other Deephaven table.
```

Similarly, this code creates a new Iceberg table and writes a Deephaven table to it. [`createTable`](/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#createTable(java.lang.String,io.deephaven.engine.table.TableDefinition)) fails if the table already exists; use [`loadTable`](/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#loadTable(io.deephaven.iceberg.util.LoadTableOptions)) to write to an existing table.

```groovy docker-config=iceberg order=null
import io.deephaven.iceberg.util.IcebergTools
import io.deephaven.iceberg.util.IcebergWriteInstructions
import io.deephaven.iceberg.util.TableParquetWriterOptions

// Configure the Iceberg catalog adapter.
icebergCatalogAdapter = IcebergTools.createAdapter(
    "rustfs-iceberg",
    [
        "type": "rest",
        "uri": "http://rest:8181",
        "warehouse": "s3a://warehouse/wh",
        "client.region": "us-east-1",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.endpoint": "http://rustfs:9000",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    ]
)

// Create a sample Deephaven table.
myDeephavenTable = newTable(
    intCol("ID", 1, 2, 3),
    stringCol("Category", "A", "B", "A"),
    intCol("Value", 100, 200, 300)
)

// Create a new Iceberg table. This fails if 'crashCourseDb.outputTable' already exists.
icebergTargetAdapter = icebergCatalogAdapter.createTable(
    "crashCourseDb.outputTable",
    myDeephavenTable.getDefinition()
)

// Define writer options.
writerOptions = TableParquetWriterOptions.builder()
    .tableDefinition(myDeephavenTable.getDefinition())
    .build()

// Get a table writer and append data.
icebergWriter = icebergTargetAdapter.tableWriter(writerOptions)
icebergWriter.append(IcebergWriteInstructions.builder().addTables(myDeephavenTable).build())
```

## HTML

Deephaven tables can be converted into an HTML representation using the [`html`](/core/javadoc/io/deephaven/engine/util/TableTools.html#html(io.deephaven.engine.table.Table)) method from the `io.deephaven.engine.util.TableTools` class. This is useful for displaying small tables in web pages or for creating simple HTML reports. The HTML string is built in memory, so avoid converting large tables.

```groovy
import io.deephaven.engine.util.TableTools

sourceTable = newTable(
    stringCol("Name", "Alice", "Bob", "Charlie"),
    stringCol("Value", "X", "Y", "Z")
)

htmlString = TableTools.html(sourceTable)

// The htmlString now contains the HTML representation of the table.
// You can print it or write it to a file:
// println(htmlString)
// new File("/data/my_table.html").text = htmlString
```

## Function generated tables

[Function generated tables](../../how-to-guides/function-generated-tables.md) are tables populated by a Groovy function. The function is reevaluated when source tables change or at a regular interval. The following example re-generates data in a table once per second.

```groovy test-set=6 ticking-table order=fgt
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory

// create execution context
defaultCtx = ExecutionContext.getContext()

// define tableGenerator function
tableGenerator = { ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return emptyTable(10).update(
            "Group = randomInt(1, 4)",
            "GroupMean = Group == 1 ? -10.0 : Group == 2 ? 0.0 : Group == 3 ? 10.0 : NULL_DOUBLE",
            "GroupStd = Group == 1 ? 2.5 : Group == 2 ? 0.5 : Group == 3 ? 1.0 : NULL_DOUBLE",
            "X = randomGaussian(GroupMean, GroupStd)"
        )
    }
}

fgt = FunctionGeneratedTableFactory.create(tableGenerator, 1000)
```

[Function generated tables](../../how-to-guides/function-generated-tables.md), on their own, don't do any data I/O. However, Groovy functions evaluated at a regular interval to create a ticking table are a powerful tool for data ingestion from external sources like WebSockets, databases, and much more. Check out this [blog post](/blog/2023/10/06/function-generated-tables#what-is-a-function-generated-table) that uses WebSockets to stream data into Deephaven with function generated tables.
