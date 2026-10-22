---
title: Write your own custom parser for Kafka
subtitle: Custom parser
---

Kafka topics often contain data that does not fit neatly into Deephaven's built-in formats such as single-column simple values, JSON, Avro, or Protobuf. In these cases, you can write your own parser that converts raw bytes from Kafka into Python objects and table columns.

In this guide, you:

1. Consume a topic as raw bytes using [`consume`](../../reference/data-import-export/Kafka/consume.md).
2. Define a `Person` data class and a Python function that parses raw bytes into it.
3. Apply the parser to each row and extract `Age` and `Name` columns.

## When to use a custom parser

Built-in Kafka [key and value specs](../../conceptual/kafka-basic-terms.md#key-and-value-specification) map each message's key or value to table columns. The built-in specs [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec), [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec), [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec), and [`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec) cover the most common patterns.

If your payload is JSON with a fixed shape, such as the `{ "age": 42, "name": "Alice" }` payload this guide uses, `json_spec` or an [object processor spec](#alternative-use-an-object-processor-spec) built on the [Jackson JSON provider](/core/pydoc/code/deephaven.json.jackson.html) can parse it while consuming, with no parsing step in your query. This guide parses that payload by hand only to keep the parser short.

A custom parser is useful when:

- The payload uses a non-standard encoding.
- The payload structure changes frequently but maps to a stable internal model.
- You need complex validation or transformation during parsing.
- You want to parse into a domain object and then derive multiple columns from it.

## Prerequisites

- Kafka is running with a topic you can read from.
- Deephaven is running with access to that Kafka cluster.
- You are comfortable with basic Python and functions.
- You know how to [connect to a Kafka stream](./kafka-stream.md) and understand the [Kafka basic terms](../../conceptual/kafka-basic-terms.md).

## Step 1: Consume raw bytes from Kafka

The first step is to consume the Kafka value as a `byte_array`. This preserves the payload exactly as it appears on the wire, letting you apply any parsing you need.

```python docker-config=kafka test-set=1 order=null
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

- **`Bytes`** is the column that holds the raw Kafka value as a `byte_array`.
- **[`KeyValueSpec.IGNORE`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec)** skips the Kafka key.
- **[`ALL_PARTITIONS_SEEK_TO_END`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ALL_PARTITIONS_SEEK_TO_END)** starts reading from the latest offsets only. To read messages that already exist in the topic, use [`kc.ALL_PARTITIONS_SEEK_TO_BEGINNING`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ALL_PARTITIONS_SEEK_TO_BEGINNING) instead.
- **[`TableType.append`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.TableType)** creates an append-only table that keeps every message it receives.

## Step 2: Define a domain object and parser function

Next, define a Python data class that represents the logical payload and a parser function that converts raw bytes into that object.

```python docker-config=kafka test-set=1 order=null
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

Adjust `parse_person` to match the format your topic uses, such as CSV or a custom binary encoding.

## Step 3: Apply the parser to each row

With the raw table and parser in place, you can call [`update`](../../reference/table-operations/select/update.md) to create a column that holds the parsed object, and then use [`view`](../../reference/table-operations/select/view.md) to project its attributes into regular columns.

```python docker-config=kafka test-set=1 order=parsed_table
parsed_table = raw_table.update(
    ["Person = (org.jpy.PyObject) parse_person(Bytes)"]
).view(
    [
        "Age = (int) Person.age",
        "Name = (String) Person.name",
    ]
)
```

The casts set each column's type. `(org.jpy.PyObject)` makes `Person` a column of Python objects, `(int)` makes `Age` an `int` column, and `(String)` makes `Name` a `String` column.

Because `view` keeps only the columns it lists, `parsed_table` drops both the original `Bytes` column and the intermediate `Person` column. To keep them, replace the `view` call with `update`.

The consumer starts at the latest offsets, so the table stays empty until new messages arrive. To see rows, run the query and then produce a message in the format shown in [Step 2](#step-2-define-a-domain-object-and-parser-function). For example, with the Redpanda setup from [Connect to a Kafka stream](./kafka-stream.md), run `docker compose exec redpanda rpk topic produce test.topic` and type the JSON object.

## Tips for designing your custom parser

- **Validate input early**.

  - Check for missing fields, invalid types, or malformed payloads.
  - Log or otherwise handle malformed records instead of letting one bad message raise an error and fail the table.

- **Keep your domain model stable**.

  - When the payload format changes over time, map each payload explicitly into a stable `dataclass` or class.
  - Add new fields in a backward-compatible way when possible.

- **Avoid heavy work in the parser**.

  - Do not perform expensive I/O or blocking operations inside the parser.
  - Keep parsing focused on decoding and basic validation.

- **Test with sample payloads**.

  - Produce sample payloads, including malformed ones, with a tool such as `rpk topic produce`.
  - Verify that the resulting Deephaven table has the expected rows and types.

## Alternative: Use an object processor spec

An object processor spec is a Kafka key or value spec built from an object processor. An object processor turns each record's raw bytes into values for one or more named, typed columns. Pass the spec to [`consume`](../../reference/data-import-export/Kafka/consume.md) as the `key_spec` or `value_spec`. The processor fills those columns as records arrive, so your query needs no parsing step.

To build the spec, pass a named object processor provider to [`object_processor_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec). A provider supplies the object processor along with the names of the columns it fills. The [Jackson JSON provider](/core/pydoc/code/deephaven.json.jackson.html) is one such provider.

An object processor spec is especially useful when:

- You want to encapsulate parsing logic and configuration.
- Multiple tables or topics share the same parsing behavior.
- You want declarative, typed JSON parsing through a provider such as Jackson.

For example, the following query parses the same `Person` payload from `test.topic` into `Age` and `Name` columns. The Jackson provider names each column after its JSON field, so the query renames the columns:

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
from deephaven.json import jackson, int_val

person_table = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
    },
    "test.topic",
    table_type=kc.TableType.append(),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.object_processor_spec(
        jackson.provider({"age": int_val(), "name": str})
    ),
    offsets=kc.ALL_PARTITIONS_SEEK_TO_END,
).rename_columns(["Age = age", "Name = name"])
```

To parse a different payload shape, change the JSON value description that you pass to [`jackson.provider`](/core/pydoc/code/deephaven.json.jackson.html#deephaven.json.jackson.provider). The [`consume`](../../reference/data-import-export/Kafka/consume.md) reference shows another Jackson-based example.

## Related documentation

- [Connect to a Kafka stream](./kafka-stream.md)
- [Kafka basic terms](../../conceptual/kafka-basic-terms.md)
- [`consume`](../../reference/data-import-export/Kafka/consume.md)
- [`update`](../../reference/table-operations/select/update.md)
