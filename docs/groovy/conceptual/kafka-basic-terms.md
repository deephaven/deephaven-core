---
title: "A Kafka introduction: basic terms"
---

[Apache Kafka](https://kafka.apache.org/) lets you read, write, store, and process streaming events, also called records or messages. Deephaven's Kafka integration enables data interchange between tables and Kafka streams. This guide introduces the Kafka concepts you need to understand both Kafka and Deephaven's Kafka integration.

The first sections cover general Kafka terms: topics, record fields, and formats. The last section covers how Deephaven maps Kafka records to and from tables.

## Topics

Individual Kafka feeds are called topics, and each topic has a name. Kafka doesn't require it, but messages in a topic typically follow a uniform schema. Two roles interact with a topic:

- **Producers** write data to topics.
- **Consumers** read data from topics.

## Record fields

Every record carries its topic and optional headers. Headers are key-value metadata pairs that the producer attaches. A record also has the fields described in the following subsections: partition, offset, timestamp, key, and value.

### Partition

A partition is a part of a topic, identified by an integer starting at zero. By selecting individual partitions, consumers can opt to listen to only a subset of messages from a topic.

A topic may have a single partition or many. When the producer writes a record, it selects the partition in one of the following ways:

1. Explicit partition: The producer sets the partition number on the record.
2. Key hashing: If the producer doesn't set a partition, Kafka's default partitioner hashes the bytes of the record's [key](#key) to choose one. Records with identical keys go to the same partition, as long as the number of partitions in the topic doesn't change.
3. No key: If the record has neither a partition nor a key, Kafka's default partitioner chooses the partition and spreads keyless records across partitions over time.

A producer can also supply a custom partitioner with its own rule. For example, if the key is a stock symbol such as `MSFT`, a custom partitioner could choose the partition from the symbol's first letter. With 26 partitions numbered 0 through 25, one per letter, symbols that start with M go to partition 12.

When choosing how to assign partitions, consider how to balance the load across partitions so that producers and consumers can scale.

Kafka guarantees stable ordering of messages in the same partition. Stable ordering per partition means that the first messages written to a partition are the first messages read from that partition as well. Consider the following example:

1. A producer writes message `A` to partition `0` of topic `test`.
2. A producer writes message `B` to partition `1` of topic `test`.
3. A producer writes message `C` to partition `0` of topic `test`.

When reading topic `test` partition `0`, message `A` comes before `C`.

Consumers can rely on per-partition ordering when the order of the data matters, as it does for stock prices or order updates.

### Offset

An offset is an integer, starting at zero, that identifies each message ever produced to a partition.

When a consumer subscribes to a topic, it can specify the offset at which to start listening. A consumer can start at:

- a defined offset value.
- the oldest available offset (seek to the beginning).
- the end of the partition, in which case the consumer receives only newly produced messages (seek to the end).
- the last offset committed for the consumer's group, without seeking.

A consumer group is a set of consumers that share a group ID, set by the `group.id` Kafka property. Kafka records a committed offset for each group and partition, which marks where that group left off. If no offset has been committed, the consumer's Kafka configuration decides where it starts.

### Timestamp

Each record carries a timestamp. A broker is a Kafka server that stores topics. Either the producer or the broker sets the timestamp, depending on the topic's configuration. With `CreateTime`, the producer sets the timestamp when it creates the record. With `LogAppendTime`, the broker sets the timestamp when it appends the record to the partition.

### Key

A key is a variable-length sequence of bytes that the producer attaches to a record. Keys don't have to be unique: many records can share the same key. Keys can be any type of byte payload. For example:

- a string, such as `MSFT`.
- a complex JSON string with multiple fields (a composite key).
- a binary-encoded double-precision floating-point number in IEEE 754 representation (8 bytes).
- a binary-encoded 32-bit integer (4 bytes).

Kafka treats the key as an opaque sequence of bytes when it hashes the key to choose a partition (see [Partition](#partition)).

A producer can omit the key by setting it to null.

### Value

A value is a variable-length sequence of bytes. Each record carries a value, paired with its key when the record has one.

Like [keys](#key), values can be any type of byte payload, such as a string, a JSON string with multiple fields, or a binary-encoded number.

A value can be null or empty, but it often holds several related fields. For example, a single message in a topic named `weatherReports` might contain these fields and more:

- Temperature
- Humidity
- Cloud cover
- Air quality

## Formats

Producers and consumers must agree ahead of time on the format of the records they exchange. Avro and Protobuf records are described by schemas, which producers and consumers can share through a schema registry: a service that stores those schemas. The following subsections describe the most common Kafka formats that Deephaven supports.

### JSON

[JSON (JavaScript Object Notation)](https://www.json.org/) is a lightweight, human-readable data-interchange format supported in nearly every environment, which makes it a good all-around choice for Kafka. The following is a JSON representation of a message value for Denver from the `weatherReports` topic described in [Value](#value):

```json
{
  "Name": "Denver",
  "LatitudeDegrees": 39.7392,
  "LongitudeDegrees": -104.9903,
  "Temperature": 75,
  "Humidity": 0.22,
  "CloudCover": 0.3,
  "AirQuality": "Average"
}
```

Deephaven's JSON [key and value specifications](#key-and-value-specification) map JSON fields, including nested fields, to and from table columns that you define.

### Avro

[Apache Avro](https://avro.apache.org/) is a compact, row-oriented binary serialization format whose schemas are defined in JSON. Unlike [JSON](#json), Avro is not human-readable. Avro suits topics whose schemas evolve over time.

Deephaven's Avro [key and value specifications](#key-and-value-specification) can both consume and produce Avro-encoded records.

You can supply the schema yourself or name one in a [schema registry](#formats). When producing with a named schema, you can also set `publishSchema` to `true` to have Deephaven generate a schema from the table's columns and register it under that name.

### Protobuf

[Protocol Buffers (Protobuf)](https://protobuf.dev/) is a language- and platform-neutral mechanism for serializing structured data. Deephaven can consume Protobuf-encoded Kafka streams. It gets the message's structure either from a [schema registry](#formats) or from a compiled Protobuf message class available to the server.

## Deephaven Kafka concepts

Deephaven maps Kafka records to and from tables. The following sections describe the parts of that mapping: key and value specifications, the table types Kafka data is consumed into, partitioned tables, the starting offset when consuming, and partition selection when producing.

### Key and value specification

Key and value specifications map between columns in a table and the key and value of each Kafka message. A key specification handles the message key, and a value specification handles the message value. In code, they are `KeyOrValueSpec` objects: [`KafkaTools.Consume.KeyOrValueSpec`](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.KeyOrValueSpec.html) for consuming and [`KafkaTools.Produce.KeyOrValueSpec`](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Produce.KeyOrValueSpec.html) for producing.

Deephaven has consume specifications for all of the [formats](#formats) listed above, and produce specifications for JSON and Avro. It also has these specifications for both consuming and producing:

- [`simpleSpec`](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String)) maps a key or value to a single column.
- [`rawSpec`](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#rawSpec(io.deephaven.qst.column.header.ColumnHeader,java.lang.Class)) uses a Kafka serializer or deserializer you supply.
- [`ignoreSpec`](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#ignoreSpec()) skips the key or value.

Each is defined in both [`KafkaTools.Consume`](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html) and [`KafkaTools.Produce`](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Produce.html).

### Table types

When consuming data from Kafka, Deephaven supports writing that data to three different types of tables:

- [Append-only](./table-types.md#specialization-1-append-only)
  - Append-only tables keep a full data history.
- [Blink](./table-types.md#specialization-3-blink)
  - Blink tables keep only data from the current [update cycle](./table-update-model.md). When a new update cycle begins, the table discards the previous cycle's data.
- [Ring](./table-types.md#specialization-4-ring)
  - Ring tables hold at most `N` rows. Once the table is full, each new row replaces the oldest one.

### Partitioned tables

A [partitioned table](../how-to-guides/partitioned-tables.md) is a table made up of constituent tables. Each constituent table holds the rows that share one value in the partitioning column or columns. You can [consume Kafka directly into a partitioned table](../reference/data-import-export/Kafka/consumeToPartitionedTable.md). When consuming from Kafka, the Kafka partition number is always the partitioning column, so each constituent table holds the records from one Kafka partition.

### Starting offset when consuming

When [consuming from Kafka](../reference/data-import-export/Kafka/consumeToTable.md), the `partitionToInitialOffset` argument of `consumeToTable` and `consumeToPartitionedTable` chooses among the starting points described in [Offset](#offset), using constants such as `KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING`, `KafkaTools.ALL_PARTITIONS_SEEK_TO_END`, and `KafkaTools.ALL_PARTITIONS_DONT_SEEK`.

### Partition selection when producing

When [producing to Kafka](../reference/data-import-export/Kafka/produceFromTable.md) with [`KafkaPublishOptions`](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaPublishOptions.html), two builder methods control which partition each record goes to:

- `partition` sets one partition for every record.
- `partitionColumn` names an `int` column whose value sets each record's partition, taking precedence over `partition`. A record whose `partitionColumn` value is null goes to the partition set by `partition`. If `partition` isn't set either, Kafka chooses the partition.

If you set neither, Kafka chooses the partition as described in [Partition](#partition).

## Related documentation

- [Table update model](./table-update-model.md)
- [Deephaven Core API design](./deephaven-core-api.md)
- [How to connect to a Kafka stream](../how-to-guides/data-import-export/kafka-stream.md)
- [Table types](./table-types.md)
- [`consumeToTable`](../reference/data-import-export/Kafka/consumeToTable.md)
- [`consumeToPartitionedTable`](../reference/data-import-export/Kafka/consumeToPartitionedTable.md)
- [`produceFromTable`](../reference/data-import-export/Kafka/produceFromTable.md)
