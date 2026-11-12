---
title: Kafka cheat sheet
---

This cheat sheet shows common ways to produce Deephaven tables to Kafka and consume Kafka topics into live Deephaven tables that update as new records arrive. The main methods are:

- [`produceFromTable`](../data-import-export/Kafka/produceFromTable.md)
- [`consumeToTable`](../data-import-export/Kafka/consumeToTable.md)
- [`consumeToPartitionedTable`](../data-import-export/Kafka/consumeToPartitionedTable.md)

The examples assume the following setup:

- A Kafka broker (the server that stores topics) at `redpanda:9092`. See [Kafka basic terms](../../conceptual/kafka-basic-terms.md).
- A [schema registry](../../conceptual/kafka-basic-terms.md#formats) at `http://redpanda:8081`, for the Avro example only.
- The `testTopic` topic, which [Simple produce](#simple-produce) writes and [Simple consume](#simple-consume) and [Start from the beginning of a topic](#start-from-the-beginning-of-a-topic) read.
- The `time-topic_group` topic, which [Produce JSON with `lastBy` on key columns](#produce-json-with-lastby-on-key-columns) writes.
- A `share.price` topic with string keys and double values, supplied separately, for [Define the key and value columns](#define-the-key-and-value-columns) and [Ignore the partition and key](#ignore-the-partition-and-key).
- A `share.price.avro` topic of Avro records whose schema is registered as `share.price.record`, supplied separately, for [Consume Avro](#consume-avro).
- An `orders` topic of JSON records with the fields `jsymbol`, `jside`, `jprice`, and `jqty`, supplied separately, for [Consume JSON with a field mapping](#consume-json-with-a-field-mapping).

To run a broker alongside Deephaven, see [Connect to a Kafka stream](../../how-to-guides/data-import-export/kafka-stream.md#launching-kafka-with-deephaven).

## Produce to Kafka

### Simple produce

Publish column `X` as the message key with a simple [key spec](../../conceptual/kafka-basic-terms.md#key-and-value-specification) and ignore the message value. The last argument, `lastByKeyColumns`, is `false`, so the producer publishes every row.

```groovy docker-config=kafka ticking-table order=null
// Create a table
source = timeTable("PT00:00:00.1").update("X = i")

// Send to Kafka, simple usage
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")

writeTopic = KafkaTools.produceFromTable(
    source,
    kafkaProps,
    "testTopic",
    KafkaTools.Produce.simpleSpec("X"),
    KafkaTools.Produce.IGNORE,
    false
)
```

### Produce JSON with `lastBy` on key columns

Publish column `X` as a JSON key and columns `X` and `Y` as a JSON value. Setting the last argument, `lastByKeyColumns`, to `true` runs a [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) on the key column `X` and publishes from the result, so each message carries the latest row for its key. Because `sourceGroup` keeps ticking, the producer still publishes a message each time a key's latest row changes.

```groovy docker-config=kafka ticking-table order=null
// Create a table with random group number
sourceGroup = timeTable("PT00:00:00.1").update(
    "X = randomInt(1, 6)",
    "Y = i"
)

// Send to Kafka, perform lastBy on keys
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")

writeTopicGroup = KafkaTools.produceFromTable(
    sourceGroup,
    kafkaProps,
    "time-topic_group",
    KafkaTools.Produce.jsonSpec(["X"] as String[], null, null),
    KafkaTools.Produce.jsonSpec(["X", "Y"] as String[], null, null),
    true
)
```

## Consume from Kafka

[`consumeToTable`](../data-import-export/Kafka/consumeToTable.md) has no optional arguments, so every call passes all seven: the properties, the topic, the partitions, the starting offsets, the key spec, the value spec, and the table type. The consume examples on this page pass these values for three of them:

- Partitions: [`KafkaTools.ALL_PARTITIONS`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS) reads all [partitions](../../conceptual/kafka-basic-terms.md#partition).
- Starting offsets: [`KafkaTools.ALL_PARTITIONS_DONT_SEEK`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS_DONT_SEEK) starts each partition without seeking, so Kafka picks the starting [offset](../../conceptual/kafka-basic-terms.md#offset). With no `group.id` set, as in these examples, it starts at the end of each partition, so the table receives only records produced after the call. [Start from the beginning of a topic](#start-from-the-beginning-of-a-topic) shows how to read existing records instead.
- Table type: [`KafkaTools.TableType.blink`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html) returns a [blink](../../conceptual/table-types.md#specialization-3-blink) table, and [`KafkaTools.TableType.append`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html) returns an [append-only](../../conceptual/table-types.md#specialization-1-append-only) table. [`KafkaTools.TableType.ring(capacity)`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html) returns a [ring](../../conceptual/table-types.md#specialization-4-ring) table.

[`consumeToPartitionedTable`](../data-import-export/Kafka/consumeToPartitionedTable.md) takes the same arguments as [`consumeToTable`](../data-import-export/Kafka/consumeToTable.md) and returns a [partitioned table](../../how-to-guides/partitioned-tables.md) with one constituent table per Kafka [partition](../../conceptual/kafka-basic-terms.md#partition). See its reference page for an example.

### Simple consume

This example passes [`KafkaTools.FROM_PROPERTIES`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#FROM_PROPERTIES) as the [key and value specs](../../conceptual/kafka-basic-terms.md#key-and-value-specification), so the key and value column types come from the `deephaven.key.column.type` and `deephaven.value.column.type` properties. The properties match what [Simple produce](#simple-produce) writes: an `int` key and no value. The `KafkaKey` column is typed `int`, and the `KafkaValue` column holds only nulls.

```groovy docker-config=kafka ticking-table order=null
// Read from Kafka, simple usage
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")
kafkaProps.put("deephaven.key.column.type", "int")
kafkaProps.put("deephaven.value.column.type", "String")

result = KafkaTools.consumeToTable(
    kafkaProps,
    "testTopic",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.FROM_PROPERTIES,
    KafkaTools.FROM_PROPERTIES,
    KafkaTools.TableType.blink()
)
```

### Start from the beginning of a topic

Pass [`KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS_SEEK_TO_BEGINNING) as the starting offsets to read the records already in the topic, starting at the beginning of each partition, and then the new records as they arrive.

```groovy skip-test
// Read from Kafka, starting at the beginning of each partition
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")
kafkaProps.put("deephaven.key.column.type", "int")
kafkaProps.put("deephaven.value.column.type", "String")

resultBeginning = KafkaTools.consumeToTable(
    kafkaProps,
    "testTopic",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    KafkaTools.FROM_PROPERTIES,
    KafkaTools.FROM_PROPERTIES,
    KafkaTools.TableType.append()
)
```

### Define the key and value columns

Read the key into a `Symbol` string column and the value into a `Price` double column.

```groovy skip-test
// Read from Kafka, define key and value
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")

result = KafkaTools.consumeToTable(
    kafkaProps,
    "share.price",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.simpleSpec("Symbol", java.lang.String),
    KafkaTools.Consume.simpleSpec("Price", double),
    KafkaTools.TableType.append()
)
```

### Ignore the partition and key

Setting [`deephaven.partition.column.name`](../../how-to-guides/data-import-export/kafka-stream.md#standard-data-fields) to an empty string leaves out the `KafkaPartition` column, which [`consumeToTable`](../data-import-export/Kafka/consumeToTable.md) adds by default to hold each record's [partition](../../conceptual/kafka-basic-terms.md#partition) number. [`KafkaTools.Consume.IGNORE`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#IGNORE) leaves out the key.

```groovy skip-test
// Read from Kafka, ignore the partition and key values
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")
kafkaProps.put("deephaven.partition.column.name", "")

resultIgnore = KafkaTools.consumeToTable(
    kafkaProps,
    "share.price",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec("Price", double),
    KafkaTools.TableType.blink()
)
```

### Consume JSON with a field mapping

Parse each JSON value into four typed columns. The `mapping` map, passed as the second argument of [`jsonSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html), maps JSON field names, such as `jsymbol`, to column names.

```groovy skip-test
// Read from Kafka, JSON with mapping
import io.deephaven.kafka.KafkaTools
import io.deephaven.engine.table.ColumnDefinition

kafkaPropsJson = new Properties()
kafkaPropsJson.put("bootstrap.servers", "redpanda:9092")

colDefs = [
    ColumnDefinition.ofString("Symbol"),
    ColumnDefinition.ofString("Side"),
    ColumnDefinition.ofDouble("Price"),
    ColumnDefinition.ofLong("Qty")
] as ColumnDefinition[]

mapping = [
    "jsymbol": "Symbol",
    "jside": "Side",
    "jprice": "Price",
    "jqty": "Qty"
]

resultJson = KafkaTools.consumeToTable(
    kafkaPropsJson,
    "orders",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.jsonSpec(colDefs, mapping),
    KafkaTools.TableType.append()
)
```

### Consume Avro

Parse each value with version `1` of the `share.price.record` [Avro](../../conceptual/kafka-basic-terms.md#avro) schema from the [schema registry](../../conceptual/kafka-basic-terms.md#formats).

```groovy skip-test
// Read from Kafka, Avro
import io.deephaven.kafka.KafkaTools

kafkaPropsAvro = new Properties()
kafkaPropsAvro.put("bootstrap.servers", "redpanda:9092")
kafkaPropsAvro.put("schema.registry.url", "http://redpanda:8081")

resultAvro = KafkaTools.consumeToTable(
    kafkaPropsAvro,
    "share.price.avro",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.avroSpec("share.price.record", "1"),
    KafkaTools.TableType.append()
)
```

## Related documentation

- [A Kafka introduction: basic terms](../../conceptual/kafka-basic-terms.md)
- [Deephaven's table types](../../conceptual/table-types.md)
- [Kafka overview](../../how-to-guides/overview-kafka.md)
- [Connect to a Kafka stream](../../how-to-guides/data-import-export/kafka-stream.md)
- [Partitioned tables](../../how-to-guides/partitioned-tables.md)
- [`consumeToTable`](../data-import-export/Kafka/consumeToTable.md)
- [`consumeToPartitionedTable`](../data-import-export/Kafka/consumeToPartitionedTable.md)
- [`lastBy`](../table-operations/group-and-aggregate/lastBy.md)
- [`produceFromTable`](../data-import-export/Kafka/produceFromTable.md)
- [`timeTable`](../table-operations/create/timeTable.md)
- [`update`](../table-operations/select/update.md)
- [`KafkaTools` Javadoc](/core/javadoc/io/deephaven/kafka/KafkaTools.html)
- [`ColumnDefinition` Javadoc](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html)
