---
title: Write your own custom parser for Kafka
subtitle: Custom parser
---

Kafka topics often contain data that does not fit neatly into Deephaven's built-in formats such as simple, JSON, Avro, or Protobuf. In these cases, you can write your own parser or use an object processor that converts raw bytes from Kafka into rich objects and table columns.

This guide shows how to:

- **Understand when you need a custom parser**.
- **Consume raw bytes or structured data from Kafka into a Deephaven table**.
- **Apply custom parsing logic to build a domain object**.
- **Project that object into regular Deephaven columns**.

> [!NOTE]
> If you are new to Kafka in Deephaven, read [Connect to a Kafka stream](./kafka-stream.md) and [Kafka basic terminology](../../conceptual/kafka-basic-terms.md) first.

## When to use a custom parser

Built-in Kafka specs such as [`simpleSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String)), [`jsonSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition[])), [`avroSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(org.apache.avro.Schema)), and [`protobufSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions)) cover the most common patterns.

A custom parser is useful when:

- **The payload is a non-standard encoding**.
- **The payload structure changes frequently but maps to a stable internal model**.
- **You need complex validation or transformation during parsing**.
- **You want to parse into a domain object and then derive multiple columns from it**.

In this guide, you will:

1. Define a domain class for your records.
2. Use JSON tools to map Kafka values into that class.
3. Expose the parsed object fields as columns in a Deephaven table.

## Prerequisites

- Kafka is running with a topic you can read from.
- Deephaven Groovy server is running with access to that Kafka cluster.
- You are comfortable with basic Groovy and classes.
- You understand the basics of [Kafka in Deephaven](../../conceptual/kafka-basic-terms.md).

## Step 1: Define a domain class

Start by defining a Groovy class that represents the logical payload you want to work with.

```groovy docker-config=kafka order=null
class Person {
    int age
    String name

    Person(int age, String name) {
        this.age = age
        this.name = name
    }
}
```

This example assumes each Kafka value contains an `age` and `name` field.

## Step 2: Describe the payload with column definitions

Next, you define column definitions that describe the columns you want in the Deephaven table, and a mapping from JSON field names to those column names.

```groovy docker-config=kafka order=null
import io.deephaven.engine.table.ColumnDefinition

ageDef = ColumnDefinition.ofInt('Age')
nameDef = ColumnDefinition.ofString('Name')

ColumnDefinition[] colDefs = [ageDef, nameDef]

mapping = ['age': 'Age', 'name': 'Name']
```

- `colDefs` describes the Deephaven columns you want.
- `mapping` explains how JSON field names map onto those columns.

## Step 3: Create a JSON spec and consume the topic

You can now build a JSON spec using `KafkaTools.Consume.jsonSpec` and pass it to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md).

```groovy docker-config=kafka order=null
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

// Define column definitions for JSON deserialization
ageDef = ColumnDefinition.ofInt('Age')
nameDef = ColumnDefinition.ofString('Name')

// Create column definitions array
ColumnDefinition[] colDefs = [ageDef, nameDef]

// Create mapping from JSON field names to column names
mapping = ['age': 'Age', 'name': 'Name']

// Set up Kafka properties
kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('schema.registry.url', 'http://redpanda:8081')

// Create JSON spec for Kafka consumption
jsonSpec = KafkaTools.Consume.jsonSpec(colDefs, mapping, null)

// Consume the Kafka topic with JSON deserialization
personTable = KafkaTools.consumeToTable(
    kafkaProps,
    'test.topic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_SEEK_TO_END,
    KafkaTools.Consume.IGNORE,
    jsonSpec,
    KafkaTools.TableType.append()
)
```

The resulting `personTable` has the columns:

- **`Age`** as an `int`.
- **`Name`** as a `String`.

From here, you can:

- Compute aggregates like average age.
- Join with other tables on `Name`.
- Filter or sort based on derived columns.

## Alternative: Use an object processor spec

For more advanced custom parsing, you can use [`objectProcessorSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(org.apache.kafka.common.serialization.Deserializer,io.deephaven.processor.NamedObjectProcessor)) with a JSON provider such as [`JacksonProvider`](https://deephaven.io/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html) to describe how to interpret Kafka values.

The Kafka streaming guide's JSON section shows a pattern like this:

```groovy docker-config=kafka order=null
import io.deephaven.json.jackson.JacksonProvider
import io.deephaven.json.ObjectValue
import io.deephaven.json.StringValue
import io.deephaven.json.DoubleValue
import io.deephaven.json.IntValue
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

fields = ObjectValue.builder()
    .putFields('symbol', StringValue.strict())
    .putFields('price', DoubleValue.strict())
    .putFields('qty', IntValue.strict())
    .build()

provider = JacksonProvider.of(fields)

jacksonSpec = KafkaTools.Consume.objectProcessorSpec(provider)

ordersTable = KafkaTools.consumeToTable(
    kafkaProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    jacksonSpec,
    KafkaTools.TableType.append()
)
```

By changing the `fields` description and the provider configuration, you can express sophisticated parsing logic while keeping your Groovy query code clean and declarative.

## Tips for designing your custom parser

- **Validate input early**.

  - Check for missing fields, invalid types, or malformed payloads.
  - Fail fast or route bad messages to a separate table when possible.

- **Keep your domain model stable**.

  - Map changing payloads into a stable `Person` or similar class.
  - Add fields in backward-compatible ways when schemas evolve.

- **Avoid heavy work inside parsing logic**.

  - Do not perform blocking network calls or expensive I/O while parsing.
  - Keep parsing focused on decoding and basic validation.

- **Test with sample payloads**.

  - Produce test messages into Kafka using `docker compose exec redpanda rpk topic produce`.
  - Verify that the resulting Deephaven table has the expected rows and types.

## Related documentation

- [Connect to a Kafka stream](./kafka-stream.md).
- [Kafka basic terminology](../../conceptual/kafka-basic-terms.md).
- [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md).
- [Table operations `update`](../../reference/table-operations/select/update.md).
