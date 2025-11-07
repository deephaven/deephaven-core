---
title: Import and Export Data
sidebar_label: Data I/O
---

Data I/O is mission-critical for any real-time data analysis platform. Deephaven supports a wide variety of data sources and formats, including [CSV](../../reference/cheat-sheets/csv.md), [Parquet](../../reference/cheat-sheets/parquet.md), Kafka <!--TODO: link to Kafka cheat sheet when it exists-->, and more. This document covers those formats in Deephaven.

We will use this table in several of the following examples:

```groovy test-set=1 order=employees
import static io.deephaven.csv.CsvTools.readCsv

employees = newTable(
    stringCol("FirstName", "James", "John", "Amanda", "Kendall", "Sam", "Alex", "Brian", "Adam", "Katherine", "Jerry"),
    stringCol("LastName", "Smith", "Szabo", "Robinson", "Godinez", "Gardener", "McPherson", "Li", "Sparrow", "Yarrow", "Stone"),
    intCol("IdNumber", 3544, 3665, 1254, 9898, 5893, 1001, 5483, 1000, 7809, 9999)
)
```

## CSV

Deephaven can read and [write CSV](../../how-to-guides/csv-export.md) files to and from local and remote locations. This example writes a table to a local CSV file.

```groovy test-set=1
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(employees, "/data/employees.csv")
```

We can show that the file is there by [importing the CSV](../../how-to-guides/csv-import.md):

```groovy test-set=1
import static io.deephaven.csv.CsvTools.readCsv

employeesNew = readCsv("/data/employees.csv")
```

## Parquet

[Apache Parquet](../../how-to-guides/data-import-export/parquet-import.md) is a columnar storage format that supports compression to store more data in less space. Deephaven supports [reading and writing single](../../how-to-guides/data-import-export/parquet-import.md#read-a-single-parquet-file), [nested, and partitioned](../../how-to-guides/data-import-export/parquet-import.md#partitioned-parquet-directories) Parquet files. Parquet data can be stored locally or in [S3](/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html). The example below writes a table to a local Parquet file.

```groovy test-set=1
import io.deephaven.parquet.table.ParquetTools

ParquetTools.writeTable(employees, "/data/employees.parquet")
```

We can show that the file is there by reading it back in:

```groovy test-set=1
employeesNew = ParquetTools.readTable("/data/employees.parquet")
```

## Kafka

[Apache Kafka](../../how-to-guides/data-import-export/kafka-stream.md) is a distributed event streaming platform that can be used to publish and subscribe to streams of records. Deephaven can consume and publish to Kafka streams. The code below consumes a stream.

```groovy test-set=2 docker-config=kafka order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('deephaven.key.column.type', 'String')
kafkaProps.put('deephaven.value.column.type', 'String')

resultAppend = KafkaTools.consumeToTable(
    kafkaProps,
    "test.topic",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec("Command", String.class),
    KafkaTools.TableType.append(),

)
```

Similarly, this code publishes the data in a Deephaven table to a Kafka stream.

```groovy test-set=3 docker-config=kafka order=null
import io.deephaven.kafka.KafkaPublishOptions
import io.deephaven.kafka.KafkaTools

// create ticking table to publish to Kafka stream
source = timeTable("PT1s").update("X = i")

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

options = KafkaPublishOptions.builder()
    .table(source)
    .topic("time-topic")
    .config(kafkaProps)
    .keySpec(KafkaTools.Produce.simpleSpec('X'))
    .valueSpec(KafkaTools.Produce.IGNORE)
    .build()

// publish to time-topic
writeTopic = KafkaTools.produceFromTable(options)
```

## Function-generated tables

[Function-generated tables](../../how-to-guides/function-generated-tables.md) are tables populated by a Groovy function. The function is reevaluated when source tables change or at a regular interval. The following example re-generates data in a table once per second.

```groovy test-set=4 order=fgt
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory

// create execution context
defaultCtx = ExecutionContext.getContext()

// define tableGenerator function
tableGenerator = { ->
    try (SafeCloseable ignored = defaultCtx.open()) {
    myTable = emptyTable(10).update(
        "Group = randomInt(1, 4)",
        "GroupMean = Group == 1 ? -10.0 : Group == 2 ? 0.0 : Group == 3 ? 10.0 : NULL_DOUBLE",
            "GroupStd = Group == 1 ? 2.5 : Group == 2 ? 0.5 : Group == 3 ? 1.0 : NULL_DOUBLE",
            "X = randomGaussian(GroupMean, GroupStd)"
    )
    }
    return myTable
    }

fgt = FunctionGeneratedTableFactory.create(tableGenerator, 1000)
```

[Function generated tables](../../how-to-guides/function-generated-tables.md), on their own, don't do any data I/O. However, Groovy functions evaluated at a regular interval to create a ticking table are a powerful tool for data ingestion from external sources like WebSockets, databases, and much more.
