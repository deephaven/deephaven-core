---
title: Write your own custom parser for Kafka
subtitle: Custom parser
---

Kafka topics often contain data that does not fit neatly into Deephaven's built-in formats such as simple, JSON, Avro, or Protobuf. In these cases, you can write your own parser that converts raw bytes from Kafka into Groovy objects and table columns, or build a key or value spec with [`objectProcessorSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(org.apache.kafka.common.serialization.Deserializer,io.deephaven.processor.NamedObjectProcessor)), as described in [Alternative: Use an object processor spec](#alternative-use-an-object-processor-spec).

This guide shows how to:

- **Understand when you need a custom parser**.
- **Consume raw bytes from Kafka into a Deephaven table**.
- **Apply a Groovy parser to turn those bytes into a domain object**.
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

1. Consume a topic as raw bytes using [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md).
2. Convert each record to a `Person` object using a Groovy parser class.
3. Extract `Age` and `Name` columns from that object.

## Prerequisites

- Kafka is running with a topic you can read from.
- Deephaven is running with access to that Kafka cluster.
- You are comfortable with basic Groovy and classes.
- You understand the basics of [Kafka in Deephaven](../../conceptual/kafka-basic-terms.md).

## Step 1: Consume raw bytes from Kafka

The first step is to consume the Kafka value as a `byte[]`. This preserves the payload exactly as it appears on the wire, letting you apply any parsing you need.

```groovy docker-config=kafka test-set=1 order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

rawTable = KafkaTools.consumeToTable(
    kafkaProps,
    'test.topic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_SEEK_TO_END,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec('Bytes', byte[].class),
    KafkaTools.TableType.append()
)
```

In this example:

- **`Bytes`** is the column that holds the raw Kafka value as a `byte[]`.
- **`KafkaTools.Consume.IGNORE`** skips the Kafka key.
- **`ALL_PARTITIONS_SEEK_TO_END`** starts reading from the latest offsets only.
- **`TableType.append`** creates an append-only table that keeps every message it receives.

## Step 2: Define a domain class and parser

Next, you define a Groovy class to represent the logical payload, and a parser class with a method that converts raw bytes into that object.

```groovy docker-config=kafka test-set=1 order=null
import groovy.json.JsonSlurper

class Person {
    public int age
    public String name

    Person(int age, String name) {
        this.age = age
        this.name = name
    }
}

class PersonParser {
    Person parse(byte[] rawBytes) {
        def jsonObject = new JsonSlurper().parseText(new String(rawBytes, 'UTF-8'))
        return new Person(jsonObject.age as int, jsonObject.name as String)
    }
}

parser = new PersonParser()
```

This example assumes that each Kafka value is a JSON object of the form:

```json
{ "age": 42, "name": "Alice" }
```

You can adjust `PersonParser.parse` to match any format your topic uses, such as CSV, custom binary, or nested JSON structures.

## Step 3: Apply the parser to each row

With the raw table and parser in place, you can call [`update`](../../reference/table-operations/select/update.md) to create a column that holds the parsed object, and then project that into regular columns.

```groovy docker-config=kafka test-set=1 order=parsedTable
parsedTable = rawTable.update('Person = parser.parse(Bytes)').view(
    'Age = Person.age',
    'Name = Person.name'
)
```

The resulting `parsedTable` has the following columns:

- **`Age`** as an `int`.
- **`Name`** as a `String`.

Because [`view`](../../reference/table-operations/select/view.md) keeps only the columns it lists, `parsedTable` drops both the original `Bytes` column and the intermediate `Person` column. To keep them, replace the `view` call with `update`.

## Alternative: Use an object processor spec

An object processor spec is a Kafka key or value spec built from an object processor: a component that turns each record's raw bytes into values for one or more named, typed columns. You pass the spec to `consumeToTable` as the key or value spec, and the processor fills those columns as records arrive, so no parsing step is needed in your query.

For some advanced use cases, you may want to use [`objectProcessorSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(org.apache.kafka.common.serialization.Deserializer,io.deephaven.processor.NamedObjectProcessor)) with a JSON provider such as [`JacksonProvider`](https://deephaven.io/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html). This is especially useful when:

- You want to encapsulate parsing logic and configuration.
- Multiple tables or topics will share the same parsing behavior.
- You need to plug in a provider implementation such as the Jackson JSON provider.

For example:

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
  - Log or handle errors instead of letting them propagate silently.

- **Keep your domain model stable**.

  - Prefer mapping changing payloads into a stable `Person` or similar class.
  - Add new fields in a backward-compatible way when possible.

- **Avoid heavy work in the parser**.

  - Do not perform expensive I/O or blocking operations inside the parser.
  - Keep parsing focused on decoding and basic validation.

- **Test with sample payloads**.

  - Produce test messages into Kafka using tools like `rpk topic produce`.
  - Verify that the resulting Deephaven table has the expected rows and types.

## Related documentation

- [Connect to a Kafka stream](./kafka-stream.md).
- [Kafka in Deephaven](../../conceptual/kafka-basic-terms.md).
- [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md).
- [Table operations `update`](../../reference/table-operations/select/update.md).
