---
title: Write your own custom parser for Kafka
subtitle: Custom parser
---

Kafka topics often contain data that does not fit neatly into Deephaven's built-in formats such as simple, JSON, Avro, or Protobuf. In these cases, you can write your own parser that converts raw bytes from Kafka into Python objects and table columns.

This guide shows how to:

- **Understand when you need a custom parser**.
- **Consume raw bytes or structured data from Kafka into a Deephaven table**.
- **Apply a Python function to parse those bytes into a rich object**.
- **Project that object into regular Deephaven columns**.

> [!NOTE]
> If you are new to Kafka in Deephaven, read [Connect to a Kafka stream](./kafka-stream.md) and [Kafka basic terminology](../../conceptual/kafka-basic-terms.md) first.

## When to use a custom parser

Built-in Kafka specs such as [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec), [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec), [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec), and [`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec) cover the most common patterns.

A custom parser is useful when:

- **The payload is a non-standard encoding**.
- **The payload structure changes frequently but maps to a stable internal model**.
- **You need complex validation or transformation during parsing**.
- **You want to parse into a domain object and then derive multiple columns from it**.

In this guide, you will:

1. Consume a topic as raw bytes using [`consume`](../../reference/data-import-export/Kafka/consume.md).
2. Convert each record to a `Person` object using a Python function.
3. Extract `Age` and `Name` columns from that object.

## Prerequisites

- Kafka is running with a topic you can read from.
- Deephaven is running with access to that Kafka cluster.
- You are comfortable with basic Python and functions.
- You understand the basics of [Kafka in Deephaven](../../conceptual/kafka-basic-terms.md).

## Step 1: Consume raw bytes from Kafka

The first step is to consume the Kafka value as a `byte_array`. This preserves the payload exactly as it appears on the wire, letting you apply any parsing you need.

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

raw_table = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
    },
    "test.topic",
    table_type=kc.TableType.append(),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Bytes", dht.byte_array),
    offsets=kc.ALL_PARTITIONS_SEEK_TO_END,
)
```

In this example:

- **`Bytes`** is the column that will hold the raw Kafka value as a `byte_array`.
- **`KeyValueSpec.IGNORE`** skips the Kafka key.
- **`ALL_PARTITIONS_SEEK_TO_END`** starts reading from the latest offsets only.
- **`TableType.append()`** creates an append-only table of all messages.

## Step 2: Define a domain object and parser function

Next, you define a Python data class to represent the logical payload, and a parser function that converts raw bytes into that object.

```python docker-config=kafka order=null
from dataclasses import dataclass
import json


@dataclass
class Person:
    age: int
    name: str


def parse_person(raw_bytes) -> Person:
    json_object = json.loads(bytes(raw_bytes))
    return Person(age=json_object["age"], name=json_object["name"])
```

This example assumes that each Kafka value is a JSON object of the form:

```json
{ "age": 42, "name": "Alice" }
```

You can adjust `parse_person` to match any format your topic uses, such as CSV, custom binary, or nested JSON structures.

## Step 3: Apply the parser to each row

With the raw table and parser in place, you can call [`update`](../../reference/table-operations/select/update.md) to create a column that holds the parsed object, and then project that into regular columns.

```python syntax
from jpy import PyObject

parsed_table = raw_table.update(["Person = (PyObject) parse_person(Bytes)"]).view(
    [
        "Age = (int) Person.age",
        "Name = (String) Person.name",
    ]
)
```

This pattern stores a Python object in a Deephaven column and then projects its attributes into regular Deephaven column types.

The resulting `parsed_table` has the following columns:

- **`Age`** as an `int`.
- **`Name`** as a `String`.

You can still keep the original `Bytes` column or drop it if you no longer need it.

## Alternative: Use an object processor spec

For some advanced use cases, you may want to register a reusable parser implementation and reference it via [`object_processor_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec). This is especially useful when:

- You want to encapsulate parsing logic and configuration.
- Multiple tables or topics will share the same parsing behavior.
- You need to plug in a provider implementation such as the Jackson JSON provider.

For example, the [`consume`](../../reference/data-import-export/Kafka/consume.md) reference shows how to use a Jackson-based JSON provider with `object_processor_spec` to parse Kafka values into columns.

## Tips for designing your custom parser

- **Validate input early**.

  - Check for missing fields, invalid types, or malformed payloads.
  - Log or handle errors instead of letting them propagate silently.

- **Keep your domain model stable**.

  - Prefer mapping changing payloads into a stable `dataclass` or class.
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
- [`consume`](../../reference/data-import-export/Kafka/consume.md).
- [Table operations `update`](../../reference/table-operations/select/update.md).
