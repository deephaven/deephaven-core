---
title: Import and Export Data
---

Data I/O is mission-critical for any real-time data analysis platform. Deephaven supports a wide variety of data sources and formats, including [CSV](../../reference/cheat-sheets/csv.md), [Parquet](../../reference/cheat-sheets/parquet.md), [Kafka](../../reference/cheat-sheets/kafka.md), and more. This chapter shows how to:

- Read and write CSV and Parquet data.
- Read Parquet files from S3.
- Consume and publish Kafka streams.
- Read and write Iceberg tables.
- Convert tables to HTML.
- Use function-generated tables to pull in data from other sources.

> [!NOTE]
> The first CSV and Parquet examples read files from Deephaven's [example data](https://github.com/deephaven/examples). A container has those files at `/data/examples` only when you start it from a [deployment that includes example data](../../getting-started/docker-install.md#choose-a-deployment). The `docker run` command in [Get started](./get-started.md) does not include them. To run these examples, start one of those deployments, or download the files into the `data/examples` directory that command mounts, keeping the same paths.

## CSV

Deephaven can [read CSV](../../how-to-guides/data-import-export/csv-import.md) files that exist locally or remotely.

> [!TIP]
> To import a CSV without writing code, use **Upload Table from File** in the Console Options menu (⋮). See [Upload a table from a file](../../how-to-guides/user-interface/upload-table-from-file.md).

This example reads a local CSV file.

```groovy test-set=1 order=iris
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv("/data/examples/Iris/csv/iris.csv")
```

Deephaven can also [write data to CSV](../../how-to-guides/data-import-export/csv-export.md). The code below writes that same table back to a CSV file.

```groovy test-set=1 order=null
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(iris, "/data/irisNew.csv")
```

Just to show that it's there, the code below reads the new file back.

```groovy test-set=1 order=irisNew
irisNew = readCsv("/data/irisNew.csv")
```

## Parquet

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format that supports compression to store more data in less space. Deephaven reads and writes single Parquet files as well as [partitioned Parquet directories](../../how-to-guides/data-import-export/parquet-import.md#partitioned-parquet-directories), where one table's data is split across many Parquet files in a directory. Deephaven reads Parquet data stored locally or in cloud object storage, such as Amazon S3, any S3-compatible object store, and Google Cloud Storage.

The example below reads from a local Parquet file.

```groovy test-set=2 order=cryptoTrades
import io.deephaven.parquet.table.ParquetTools

cryptoTrades = ParquetTools.readTable(
    "/data/examples/CryptoCurrencyHistory/Parquet/CryptoTrades_20210922.parquet"
)
```

The code below writes that same table back to a Parquet file.

```groovy test-set=2 order=null
ParquetTools.writeTable(cryptoTrades, "/data/cryptoTradesNew.parquet")
```

Just to show that it worked, the code below reads the new file back.

```groovy test-set=2 order=cryptoTradesNew
cryptoTradesNew = ParquetTools.readTable("/data/cryptoTradesNew.parquet")
```

The example below reads a Parquet file from a [RustFS](https://rustfs.com/) server at `rustfs.example.com`. RustFS is an S3-compatible object store. The [`S3Instructions`](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html) class specifies how to connect to the S3 instance. To run the example, replace the endpoint, file path, and credentials with those of your own S3 instance. See [Read Parquet from S3](../../how-to-guides/data-import-export/parquet-import.md#from-s3) for more.

```groovy test-set=7 docker-config=rustfs order=grades
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions
import io.deephaven.extensions.s3.S3Instructions
import io.deephaven.extensions.s3.Credentials

// create credentials for the S3 instance
credentials = Credentials.basic("example_username", "example_password")

// pass the S3 URL, as well as instructions on how to talk to the S3 instance
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

[Apache Kafka](https://kafka.apache.org/) is a distributed event streaming platform for publishing and subscribing to streams of records. Deephaven can consume data from Kafka streams and publish data to them. See the [Kafka guide](../../how-to-guides/data-import-export/kafka-stream.md) for more.

The examples below connect to a Kafka broker at `redpanda:9092`. Redpanda is a Kafka-compatible broker. To run the examples, start Deephaven alongside Redpanda as described in [Launching Kafka with Deephaven](../../how-to-guides/data-import-export/kafka-stream.md#launching-kafka-with-deephaven). Run the `docker compose` commands below from the directory that holds the `docker-compose.yml` file from that guide.

The code below consumes a stream.

```groovy test-set=3 docker-config=kafka ticking-table order=null
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

The `resultAppend` table starts empty. Rows appear as records arrive on `test.topic`. To send some, run `docker compose exec redpanda rpk topic produce test.topic` in a terminal and type a few lines. `rpk` is Redpanda's command-line client.

Similarly, this code publishes the data in a Deephaven table to a Kafka stream.

```groovy test-set=4 docker-config=kafka ticking-table order=null
import io.deephaven.kafka.KafkaTools

// create ticking table to publish to Kafka stream
source = timeTable("PT1s").update("X = `Value ` + i")

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

To see the records arrive, run `docker compose exec redpanda rpk topic consume time-topic` in a terminal.

## Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is a high-performance, open table format for huge analytic datasets. Deephaven's [`io.deephaven.iceberg.util`](/core/javadoc/io/deephaven/iceberg/util/package-summary.html) package reads and writes Iceberg tables. For more on Deephaven's Iceberg integration, see the [Iceberg user guide](../../how-to-guides/data-import-export/iceberg.md).

Both Iceberg examples need the custom Docker deployment described in [A Deephaven deployment for Iceberg](../../how-to-guides/data-import-export/iceberg.md#a-deephaven-deployment-for-iceberg). That deployment runs its object store at `http://minio:9000`, so before you run the first example, change the `"s3.endpoint"` value in the adapter call from `http://rustfs:9000` to `http://minio:9000`.

An Iceberg catalog adapter connects Deephaven to an Iceberg catalog. The following example creates one with [`IcebergTools.createAdapter`](/core/javadoc/io/deephaven/iceberg/util/IcebergTools.html#createAdapter(java.lang.String,java.util.Map)) and uses it to read an existing Iceberg table into a Deephaven table.

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
        "s3.endpoint": "http://rustfs:9000", // use http://minio:9000 in the Iceberg deployment
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

Using the same `icebergCatalogAdapter`, this code creates a new Iceberg table and writes a Deephaven table to it. [`createTable`](/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#createTable(java.lang.String,io.deephaven.engine.table.TableDefinition)) fails if the table already exists. To write to an existing table, use [`loadTable`](/core/javadoc/io/deephaven/iceberg/util/IcebergCatalogAdapter.html#loadTable(java.lang.String)) instead.

```groovy test-set=5 order=null
import io.deephaven.iceberg.util.IcebergWriteInstructions
import io.deephaven.iceberg.util.TableParquetWriterOptions

// Create a sample Deephaven table.
myDeephavenTable = newTable(
    intCol("ID", 1, 2, 3),
    stringCol("Category", "A", "B", "A"),
    intCol("Value", 100, 200, 300)
)

// Create a new Iceberg table.
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

The [`html`](/core/javadoc/io/deephaven/engine/util/TableTools.html#html(io.deephaven.engine.table.Table)) method from the [`io.deephaven.engine.util.TableTools`](/core/javadoc/io/deephaven/engine/util/TableTools.html) class converts a Deephaven table into an HTML string. This is useful for displaying small tables in web pages or for creating simple HTML reports. `html` builds the whole string in memory, so avoid converting large tables.

```groovy order=:log,sourceTable
import io.deephaven.engine.util.TableTools

sourceTable = newTable(
    stringCol("Name", "Alice", "Bob", "Charlie"),
    stringCol("Value", "X", "Y", "Z")
)

htmlString = TableTools.html(sourceTable)

// The htmlString now contains the HTML representation of the table.
// You can print it or write it to a file:
println(htmlString)
new File("/data/myTable.html").text = htmlString
```

## Function-generated tables

[Function-generated tables](../../how-to-guides/function-generated-tables.md) are a powerful tool for ingesting data from external sources such as WebSockets, databases, and much more. A Groovy function populates a function-generated table. Deephaven reruns the function at a regular interval, or whenever one of the tables you pass in the `sourceTables` argument of [`FunctionGeneratedTableFactory.create`](../../reference/table-operations/create/create.md) changes. The table does no data I/O on its own, but your function can read from an external source each time it runs.

The following example regenerates data in a table once per second. To keep the example self-contained, the function generates random data instead of reading from an external source.

Deephaven calls the function without the console's [execution context](../../conceptual/execution-context.md), both when it creates the table and on each refresh. The [`update`](../../reference/table-operations/select/update.md) formulas need that context, so the example captures the current context with [`ExecutionContext.getContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#getContext()) and opens it inside the function.

```groovy test-set=6 ticking-table order=null
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory

// capture the current execution context so the generator function can use it every time it runs
defaultCtx = ExecutionContext.getContext()

// define tableGenerator function
tableGenerator = { ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return emptyTable(10).update(
            "Group = randomInt(1, 4)",
            "GroupMean = Group == 1 ? -10.0 : Group == 2 ? 0.0 : 10.0",
            "GroupStd = Group == 1 ? 2.5 : Group == 2 ? 0.5 : 1.0",
            "X = randomGaussian(GroupMean, GroupStd)"
        )
    }
}

fgt = FunctionGeneratedTableFactory.create(tableGenerator, 1000)
```

For an example that polls a web API for weather data, see the [function-generated tables blog post](/blog/2023/10/06/function-generated-tables#weather-api-example). The blog post's code is in Python, but the pattern is the same in Groovy.
