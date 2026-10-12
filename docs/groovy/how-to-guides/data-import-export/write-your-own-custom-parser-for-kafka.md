---
title: Write your own custom parser for Kafka
subtitle: Custom parser
---

Kafka topics often contain data that does not fit neatly into Deephaven's built-in formats such as single-column simple values, JSON, Avro, or Protobuf. In these cases, you can write your own parser that converts raw bytes from Kafka into Groovy objects and table columns.

In this guide, you:

1. Consume a topic as raw bytes using [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md).
2. Define a `Person` class and a parser class that converts raw bytes into it.
3. Apply the parser to each row and extract `Age` and `Name` columns.

## When to use a custom parser

Built-in Kafka [key and value specs](../../conceptual/kafka-basic-terms.md#key-and-value-specification) map each message's key or value to table columns. The built-in specs [`simpleSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String)), [`jsonSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition[])), [`avroSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(org.apache.avro.Schema)), and [`protobufSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions)) cover the most common patterns.

If your payload is JSON with a fixed shape, such as the `{ "age": 42, "name": "Alice" }` payload this guide uses, `jsonSpec` or an [object processor spec](#alternative-use-an-object-processor-spec) built on [`JacksonProvider`](https://deephaven.io/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html) can parse it while consuming, with no parsing step in your query. This guide parses that payload by hand only to keep the parser short.

A custom parser is useful when:

- The payload uses a non-standard encoding.
- The payload structure changes frequently but maps to a stable internal model.
- You need complex validation or transformation during parsing.
- You want to parse into a domain object and then derive multiple columns from it.

## Prerequisites

- Kafka is running with a topic you can read from.
- Deephaven is running with access to that Kafka cluster.
- You are comfortable with basic Groovy and classes.
- You know how to [connect to a Kafka stream](./kafka-stream.md) and understand the [Kafka basic terms](../../conceptual/kafka-basic-terms.md).

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
- **[`KafkaTools.Consume.IGNORE`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#IGNORE)** skips the Kafka key.
- **[`ALL_PARTITIONS`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS)** consumes from every partition of the topic.
- **[`ALL_PARTITIONS_SEEK_TO_END`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS_SEEK_TO_END)** starts reading from the latest offsets only. To read messages that already exist in the topic, use [`KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS_SEEK_TO_BEGINNING) instead.
- **[`TableType.append`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html#append())** creates an append-only table that keeps every message it receives.

## Step 2: Define a domain class and parser

Next, define a Groovy class that represents the logical payload and a parser class with a method that converts raw bytes into that object.

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

Adjust `PersonParser.parse` to match the format your topic uses, such as CSV or a custom binary encoding.

## Step 3: Apply the parser to each row

With the raw table and parser in place, you can call [`update`](../../reference/table-operations/select/update.md) to create a column that holds the parsed object, and then use [`view`](../../reference/table-operations/select/view.md) to project its fields into regular columns.

```groovy docker-config=kafka test-set=1 order=parsedTable
parsedTable = rawTable.update('Person = parser.parse(Bytes)').view(
    'Age = Person.age',
    'Name = Person.name'
)
```

The resulting `parsedTable` has the following columns:

- **`Age`** as an `int`.
- **`Name`** as a `String`.

Because `view` keeps only the columns it lists, `parsedTable` drops both the original `Bytes` column and the intermediate `Person` column. To keep them, replace the `view` call with `update`.

The consumer starts at the latest offsets, so the table stays empty until new messages arrive. To see rows, run the query and then produce a message in the format shown in [Step 2](#step-2-define-a-domain-class-and-parser). For example, with the Redpanda setup from [Connect to a Kafka stream](./kafka-stream.md), run `docker compose exec redpanda rpk topic produce test.topic` and type the JSON object.

## Tips for designing your custom parser

- **Validate input early**.

  - Check for missing fields, invalid types, or malformed payloads.
  - Log or otherwise handle malformed records instead of letting one bad message throw an exception and fail the table.

- **Keep your domain model stable**.

  - When the payload format changes over time, map each payload explicitly into a stable `Person` or similar class.
  - Add new fields in a backward-compatible way when possible.

- **Avoid heavy work in the parser**.

  - Do not perform expensive I/O or blocking operations inside the parser.
  - Keep parsing focused on decoding and basic validation.

- **Test with sample payloads**.

  - Produce sample payloads, including malformed ones, with a tool such as `rpk topic produce`.
  - Verify that the resulting Deephaven table has the expected rows and types.

## Alternative: Use an object processor spec

An object processor spec is a Kafka key or value spec built from an object processor. An object processor turns each record's raw bytes into values for one or more named, typed columns. Pass the spec to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) as the key or value spec. The processor fills those columns as records arrive, so your query needs no parsing step.

To build the spec, pass a named object processor provider to [`objectProcessorSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(io.deephaven.processor.NamedObjectProcessor.Provider)). A provider supplies the object processor along with the names of the columns it fills. [`JacksonProvider`](https://deephaven.io/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html) is one such provider.

An object processor spec is especially useful when:

- You want to encapsulate parsing logic and configuration.
- Multiple tables or topics share the same parsing behavior.
- You want declarative, typed JSON parsing through a provider such as Jackson.

For example, the following query parses the same `Person` payload from `test.topic` into `Age` and `Name` columns. The Jackson provider names each column after its JSON field, so the query renames the columns:

```groovy docker-config=kafka order=null
import io.deephaven.json.jackson.JacksonProvider
import io.deephaven.json.ObjectValue
import io.deephaven.json.StringValue
import io.deephaven.json.IntValue
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

fields = ObjectValue.builder()
    .putFields('age', IntValue.standard())
    .putFields('name', StringValue.standard())
    .build()

provider = JacksonProvider.of(fields)

jacksonSpec = KafkaTools.Consume.objectProcessorSpec(provider)

personTable = KafkaTools.consumeToTable(
    kafkaProps,
    'test.topic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_SEEK_TO_END,
    KafkaTools.Consume.IGNORE,
    jacksonSpec,
    KafkaTools.TableType.append()
).renameColumns('Age = age', 'Name = name')
```

To parse a different payload shape, change the `fields` description that you pass to [`JacksonProvider.of`](https://deephaven.io/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html#of(io.deephaven.json.Value)). The [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reference shows more ways to consume Kafka topics.

## Related documentation

- [Connect to a Kafka stream](./kafka-stream.md)
- [Kafka basic terms](../../conceptual/kafka-basic-terms.md)
- [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md)
- [`update`](../../reference/table-operations/select/update.md)
