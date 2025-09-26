---
title: "A Kafka introduction: basic terms"
sidebar_label: "Basic terms"
---

[Apache Kafka](https://kafka.apache.org/) lets you read, write, store, and process streaming events (also called records). Deephaven's Kafka integration enables data interchange between tables and Kafka streams. This guide introduces Kafka concepts that provide the basis for a fundamental understanding of Kafka itself, as well as Deephaven's Kafka integration.

Each section of this guide covers a different aspect of Kafka as it relates to Deephaven.

## Topics

Individual Kafka feeds are called topics. Topics are are identified by a name. While not explicitly required, messages in a topic typically follow a uniform schema. The following list describes some key concepts related to topics:

- **Producers** write data to topics.
- **Consumers** read data from topics.
- Data from topics arrive as records in sequential order.
- Every record contains exactly five fields:
  - `partition`
  - `offset`
  - `timestamp`
  - `key`
  - `value`

### Partition

Partition is a positive integer value used to divide a topic into parts. By selecting individual partitions, subscribers can opt to listen to only a subset of messages from a topic.

A topic may have a single partition or many. The producer selects the partition for a message when the data is written to the Kafka stream.

When choosing a partition, it is important to consider how to balance the load to maximize load scaling between the producer and consumer:

1. Consistent hashing: The partition may be directly implied from the key's hash value at publishing time. For example, if the key represents a string security symbol (e.g., "MSFT"), the partition may be calculated from the first letter of the security symbol "M". If each letter is a possible partition calculator, then M takes the value of 12 from possible values in [0, 25] from the alphabet.
2. Randomly: The partition may be assigned at random to balance the load most efficiently.

Kafka guarantees stable ordering of messages in the same partition. Stable ordering per partition means that the first messages written to a partition are the first messages read from that partition as well. Consider the following example:

1. Message `A` is written to topic `test` partition `0`.
2. Message `B` is written to topic `test` partition `1`.
3. Message `C` is written to topic `test` partition `0`.

When reading topic `test` partition `0`, message `A` comes before `C`. Consumers can take advantage of that knowledge.

This is important when processing messages on consumers that require ordering preservation, such as stock market prices or order updates.

### Offset

An offset is an integer number starting at zero that can be used to identify any message ever produced in a given partition.

When a consumer subscribes to a topic, it can specify the offset at which to start listening. A consumer can start at:

- a defined offset value.
- the oldest available value.
- the current offset, in which case the consumer will receive only newly produced messages.

### Timestamp

A timestamp value is inserted by the producer at publication time (i.e., when the message is written to the topic).

### Key

- A key is a sequence of bytes without a predefined length used as an identifier. Keys can be any type of byte payload. For example:

  - a string, such as `MSFT`.
  - a complex JSON string with multiple fields.
  - a binary encoded double-precision floating-point number in IEEE 754 representation (8 bytes).
  - a binary encoded 32-bit integer (4 bytes).

Kafka may need to hash the key to produce a partition value. When computing the hash, the key is treated as an opaque sequence of bytes. A key can be empty, in which case partition assignments will be effectively random. A key can also comprise multiple parts, which is called a composite key. Lastly, identical keys are always written to the same partition.

### Value

Values are a sequence of bytes without a predefined length. Keys map to values.

- Values can be any type of byte payload. For example:

  - a simple string, such as `Deephaven`.
  - a complex JSON string with multiple fields.
  - a binary encoded double-precision floating-point number in IEEE 754 representation (8 bytes).
  - a binary encoded 32-bit integer (4 bytes).

Values may be empty. Most commonly, multidimensional data associated with a key is represented as a composite that contains multiple fields. Consider a weather example. A single message in a topic called `weatherReports` may contain all of the following fields and more:

- Temperature
- Humidity
- Cloud cover
- Air quality

## Format

It's important for producers and consumers to agree on a format for what's being published and subscribed to ahead of time. The following subsections present the most commonly used Kafka formats that can be used in Deephaven.

### JSON

[JSON (JavaScript Object Notation)](https://json-schema.org/) is a lightweight data-interchange format widely used for being both human and machine-readable. It is widely supported across many environments. JSON is a good all-around choice for applications that use Kafka because it's so popular and human-readable. The following is an example of a JSON representation of weather data for Denver:

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

Kafka's JSON schema provides ways to encode keys and values as a wide variety of basic data types, as well as encode more complex multi-field or structured types.

### Avro

[Apache Avro](https://avro.apache.org/) offers a serialization format for recording data through a row-oriented RPC call. It is not human-readable like [JSON](#json), despite using JSON to define data types and protocols. Avro is particularly useful in cases where schemas may evolve over time.

### Protobuf

[Protocol Buffers (Protobuf)](https://protobuf.dev/) is a language and platform-neutral mechanism for serializing structured data. Deephaven uses Protobuf in many different ways, including Kafka. Protobuf is a great choice for interoperability, especially when using Deephaven as a Kafka producer. Consumers on basically any other platform and using any other language can consume it with ease.

## Kafka-specific Deephaven topics

The previous sections introduced Kafka concepts. The following sections describe Kafka concepts that are specific to Deephaven.

### Key and value specification

A key specification and a value specification, called a `KeyValueSpec` in code, maps between columnar data in tables and key-value pairs in Kafka messages. Deephaven has specifications for all of the [formats](#format) listed above. Key specifications specify the mapping between table columns and Kafka message keys, while value specifications specify the mapping between table columns and Kafka message values.

### Table types

When consuming data from Kafka, Deephaven supports writing that data to three different types of tables:

- [Append-only](../conceptual/table-types.md#specialization-1-append-only)
  - Append-only tables keep a full data history.
- [Blink](../conceptual/table-types.md#specialization-3-blink)
  - Blink tables keep only data from the current update cycle. As soon as a new update cycle begins, the previous cycle's data is discarded.
- [Ring](../conceptual/table-types.md#specialization-4-ring)
  - Ring tables can grow to a maximum size of `N` rows. When the maximum size is exceeded, the oldest rows are removed until `N` are left.

### Partitioned tables

A partitioned table is a special type of table that is partitioned into constituent tables by one or more key columns. You [consume Kafka directly into a partitioned table](../reference/data-import-export/Kafka/consume-to-partitioned-table.md). When consuming from Kafka, the resulting partitioned table is always partitioned on the Kafka partition.

When producing to Kafka, you can specify which column corresponds to the Kafka partition. If not specified, Kafka will choose based on the key's hash value. If no key is present, partitions are assigned in round-robin fashion.

## Related documentation

- [Table update model](./table-update-model.md)
- [Deephaven Core API design](./deephaven-core-api.md)
- [How to connect to a Kafka stream](../how-to-guides/data-import-export/kafka-stream.md)
- [Table types](../conceptual/table-types.md)
- [`consume`](../reference/data-import-export/Kafka/consume.md)
- [`consume_to_partitioned_table`](../reference/data-import-export/Kafka/consume-to-partitioned-table.md)
