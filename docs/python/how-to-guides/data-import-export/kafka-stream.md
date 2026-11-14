---
title: Connect to a Kafka stream
---

Kafka is a distributed event streaming platform that lets you read, write, store, and process events, also called records.

Deephaven turns a Kafka topic into a live table that updates as records arrive, so you can [filter](../use-filters.md), [join](../joins-exact-relational.md), and [aggregate](../combined-aggregations.md) a stream with the same table operations you use on static data.

Kafka record keys and values come in many formats, such as [simple types](#consume-a-kafka-stream) like strings and numbers, [JSON](#read-kafka-topic-in-json-format), [Avro](#read-kafka-topic-in-avro-format), or [Protobuf](#read-kafka-topic-in-protobuf-format). In this guide, we show you how to read each of these formats into Deephaven tables, and how to [write a Deephaven table to Kafka](#write-to-a-kafka-stream).

See [A Kafka introduction: basic terms](../../conceptual/kafka-basic-terms.md) for a detailed discussion of Kafka topics and supported formats. See the [Apache Kafka documentation](https://kafka.apache.org/documentation/) for full details on how to use Kafka.

## Launch Kafka with Deephaven

Deephaven has an official [Docker Compose file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples-redpanda/docker-compose.yml) that runs the Deephaven images alongside [Redpanda](https://www.redpanda.com/), one of many Kafka-compatible event streaming platforms that work with Deephaven. Redpanda's command-line tool lets you send records to a Kafka topic from the terminal.

Save the Docker Compose file locally as `docker-compose.yml`, and start it with `docker compose up`. The Deephaven IDE is then at `http://localhost:10000/ide`.

## Key and value

Each Kafka record has a key and a value. Kafka writes each record to a partition at an offset, with a timestamp. For example, a list of Kafka records might have a stock ticker as the key and its price as the value.

The key and the value can each be nearly any sequence of bytes.

A _key spec_ and a _value spec_ tell Deephaven how to turn the key and the value into table columns. You pass them to [`consume`](../../reference/data-import-export/Kafka/consume.md) as its `key_spec` and `value_spec` arguments. You create them with functions from the [`deephaven.stream.kafka.consumer`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#module-deephaven.stream.kafka.consumer) module, which the examples on this page import as `kc`.

The key and the value can each be:

- [read as a simple type](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec)
- [decoded from JSON](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec)
- [decoded from Avro](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec)
- [decoded from Protobuf](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec)
- [parsed by an object processor](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec), such as a Jackson JSON provider
- named and typed from consumer properties, with [`kc.KeyValueSpec.FROM_PROPERTIES`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec)
- ignored, with [`kc.KeyValueSpec.IGNORE`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec)

You can't ignore both the key and the value.

If you omit `key_spec` or `value_spec`, Deephaven uses [`kc.KeyValueSpec.FROM_PROPERTIES`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec) for it, which needs a column type from the consumer properties. See [Name key and value columns with properties](#name-key-and-value-columns-with-properties).

## Table types

Deephaven Kafka tables can be [append-only](../../conceptual/table-types.md#specialization-1-append-only), [blink](../../conceptual/table-types.md#specialization-3-blink), or [ring](../../conceptual/table-types.md#specialization-4-ring). Set the type with the `table_type` argument of [`consume`](../../reference/data-import-export/Kafka/consume.md). Its value is a [`TableType`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.TableType). If you don't pass `table_type`, the table is a [blink table](../../conceptual/table-types.md#specialization-3-blink).

- [Append-only](../../conceptual/table-types.md#specialization-1-append-only) tables keep every row. The table and its memory use can grow without limit. To use this type, pass [`kc.TableType.append()`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.TableType) as `table_type`.
- [Blink](../../conceptual/table-types.md#specialization-3-blink) tables keep only the rows from the current [update cycle](../../conceptual/table-update-model.md). Each new record appears as a row for one [update cycle](../../conceptual/table-update-model.md) and then disappears. To set this type explicitly, pass [`kc.TableType.blink()`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.TableType) as `table_type`.
- [Ring](../../conceptual/table-types.md#specialization-4-ring) tables keep only the last `N` rows. When the table grows beyond `N` rows, it discards the oldest rows until `N` remain. To use this type, pass [`kc.TableType.ring(N)`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.TableType) as `table_type`.

To keep results after blink rows disappear, see the [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) example in [Ring and blink tables](#ring-and-blink-tables).

## Consume a Kafka stream

In this example, we consume a Kafka topic (`test.topic`) as a Deephaven table. You populate the Kafka topic by entering commands in the terminal.

For demonstration purposes, we use an [append-only](../../conceptual/table-types.md#specialization-1-append-only) table and ignore the Kafka key.

```python docker-config=kafka test-set=2 order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

result_append = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "test.topic",
    table_type=kc.TableType.append(),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Command", dht.string),
)
```

[`consume`](../../reference/data-import-export/Kafka/consume.md) creates a Deephaven table from a Kafka topic. Its arguments do the following:

- `{"bootstrap.servers": "redpanda:9092"}` is a dictionary that describes how to connect to Kafka. `bootstrap.servers` lists the initial hosts that a Kafka client connects to. The value `redpanda:9092` is the address of the Redpanda service from the [Docker Compose file](#launch-kafka-with-deephaven). To connect to your own Kafka cluster, replace `redpanda:9092` with your brokers' `host:port` list.
- `table_type` is [`kc.TableType.append()`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.TableType), which creates an [append-only table](../../conceptual/table-types.md#specialization-1-append-only).
- `key_spec` is [`kc.KeyValueSpec.IGNORE`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec), which ignores the Kafka key.
- `value_spec` uses [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec) to read each record's value into a `String` column named `Command`.

The `result_append` table now reads from every partition that the `test.topic` topic has when the table is created.

### Input Kafka data for testing

To send data to the `test.topic` topic that the `result_append` table consumes, run the following command in a terminal:

```shell
docker compose exec redpanda rpk topic produce test.topic
```

The command waits for input from the terminal and sends each line you enter to the `test.topic` topic as a record. Press **Ctrl + D** when you're done to stop producing.

Each record you send appears in the `result_append` table without rerunning the query.

<LoopedVideo src='../../assets/how-to/kafka1.mp4' />

### Ring and blink tables

The following example shows how to create [ring and blink tables](#table-types) to read from the `test.topic` topic:

```python docker-config=kafka test-set=2 order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

result_ring = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "test.topic",
    table_type=kc.TableType.ring(3),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Command", dht.string),
)

result_blink = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "test.topic",
    table_type=kc.TableType.blink(),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Command", dht.string),
)
```

Run `docker compose exec redpanda rpk topic produce test.topic` again and enter a few more lines. The `result_append` table contains all the records, the `result_ring` table contains the last three, and the `result_blink` table shows each new row for only one [update cycle](../../conceptual/table-update-model.md).

Because rows disappear from the `result_blink` table after one [update cycle](../../conceptual/table-update-model.md), let's add a table that uses [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) to keep the last row added to the `result_blink` table.

```python docker-config=kafka test-set=2 order=null
last_blink = result_blink.last_by()
```

### Read the key and choose partitions and offsets

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price` into an [append-only table](../../conceptual/table-types.md#specialization-1-append-only). Unlike the previous examples, it reads both the key and the value, and it sets the partitions and the starting offsets explicitly.

When reading a Kafka topic, you can choose which partitions to read and where reading starts in each one:

- **Partitions**: By default, Deephaven reads all partitions.
- **Starting point**: Reading can start at the beginning of each partition, at the end, or at a specific offset. Kafka calls moving the read position _seeking_. By default, Deephaven doesn't seek, so reading starts where Kafka's consumer settings say.

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

result = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "share.price",
    partitions=None,
    offsets=kc.ALL_PARTITIONS_DONT_SEEK,
    key_spec=kc.simple_spec("Symbol", dht.string),
    value_spec=kc.simple_spec("Price", dht.string),
    table_type=kc.TableType.append(),
)
```

Let's walk through the arguments in this query.

- `partitions` is set to `None`, which reads all partitions. `None` is also the default when you don't pass `partitions`.
- `offsets` is set to [`kc.ALL_PARTITIONS_DONT_SEEK`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ALL_PARTITIONS_DONT_SEEK), which doesn't seek. Kafka then picks the starting point:
  - If you set the [`group.id`](https://kafka.apache.org/44/configuration/consumer-configs/#consumerconfigs_group.id) property and that consumer group has a committed offset, reading starts there.
  - Otherwise, Kafka's [`auto.offset.reset`](https://kafka.apache.org/44/configuration/consumer-configs/#consumerconfigs_auto.offset.reset) property decides. Its default, `latest`, reads only new records. This example doesn't set `group.id`, so the table shows only records sent after you create it.
- `key_spec` uses [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec) to read each record's key into a `String` column named `Symbol`. To drop the `Symbol` column, pass [`kc.KeyValueSpec.IGNORE`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec) as `key_spec` instead, as in the first example.
- `value_spec` uses [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec) to read each record's value into a `String` column named `Price`. The column is a string because `rpk`, Redpanda's command-line tool, sends `135.60` as text. With `dht.double`, Deephaven would decode the value with Kafka's `DoubleDeserializer`, which expects 8 binary bytes, so text input fails.
- `table_type` is set to [`kc.TableType.append()`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.TableType), which creates an [append-only table](../../conceptual/table-types.md#specialization-1-append-only).

To change which partitions the example reads or where it starts, change `partitions` and `offsets`.

To read specific partitions, pass them as a list of integers, such as `partitions=[1, 3, 5]`.

To choose a different starting point, pass one of these values as `offsets`:

- [`kc.ALL_PARTITIONS_SEEK_TO_END`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ALL_PARTITIONS_SEEK_TO_END) always starts with new records only.
- [`kc.ALL_PARTITIONS_SEEK_TO_BEGINNING`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ALL_PARTITIONS_SEEK_TO_BEGINNING) starts at the beginning of every partition.
- A dictionary that maps partition numbers to offsets, such as `{0: 100, 1: 250}`, starts at those offsets. Partitions that the dictionary doesn't list don't seek.

> [!NOTE]
> Starting at the beginning of a partition reads only the records Kafka still retains. Kafka's retention settings delete older records once a topic passes a configured age or size, and log compaction can remove older records for a key, keeping at least the latest one.

Now let's add some entries to our Kafka stream.

Run the following command:

```shell
docker compose exec redpanda rpk topic produce share.price -f '%k %v\n'
```

Enter as many key-value pairs as you want. Put a space between each key and its value, and put each pair on its own line:

```text
AAPL 135.60
AAPL 135.99
AAPL 136.82
```

Each pair you enter appears as a row in the `result` table.

### Read Kafka topic in JSON format

The following two examples read a Kafka topic called `orders` in JSON format.

The first example uses [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec):

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

result = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "orders",
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.json_spec(
        {"Symbol": dht.string, "Price": dht.double, "Qty": dht.int64},
        mapping={"symbol": "Symbol", "price": "Price", "qty": "Qty"},
    ),
    table_type=kc.TableType.append(),
)
```

Here, the `value_spec` argument uses [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec), which parses each record's value as JSON.

The first argument to [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec) is a table definition: a dictionary that maps each column name in the result table to its Deephaven type (for example, `dht.double`).

The `mapping` keyword argument of [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec) is a dictionary that maps JSON field names to table column names. Each column name must appear in the table definition from the first argument.

In the example, the entry `"price": "Price"` reads the JSON field `price` into the `Price` column of the result table. For any column that `mapping` doesn't mention, or for every column when you omit `mapping`, Deephaven reads the JSON field with the same name as the column.

The second example uses [`object_processor_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec) with a [Jackson provider](/core/pydoc/code/deephaven.json.jackson.html):

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
from deephaven.json import jackson

result = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "orders",
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.object_processor_spec(
        jackson.provider(
            {
                "symbol": str,
                "price": float,
                "qty": int,
            }
        )
    ),
    table_type=kc.TableType.append(),
).rename_columns(["Symbol = symbol", "Price = price", "Qty = qty"])
```

Here, [`object_processor_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec) takes a [Jackson provider](/core/pydoc/code/deephaven.json.jackson.html) built from a dictionary that maps each JSON field name to its type. The resulting columns take the JSON field names, so the example renames them with [`rename_columns`](../../reference/table-operations/select/rename-columns.md).

To test either example, run the following command:

```shell
docker compose exec redpanda rpk topic produce orders -f "%v\n"
```

Then enter the following values:

```text
{"symbol": "AAPL", "price": 135, "qty": 5}
{"symbol": "TSLA", "price": 730, "qty": 3}
```

### Read Kafka topic in Avro format

A [schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html) stores each Kafka event schema under a _subject_ name and tracks the subject's versions, so that producers and consumers can share them. To register a schema, see Redpanda's [schema registry documentation](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/).

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price.avro` in [Avro](https://avro.apache.org/) format. This example assumes that a schema with the subject name `share.price.avro.record` is registered in the [schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html) of the [Redpanda](https://www.redpanda.com/) instance from the [Docker Compose file](#launch-kafka-with-deephaven).

```python skip-test
from deephaven.stream.kafka import consumer as kc

result = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "schema.registry.url": "http://redpanda:8081",
    },
    "share.price.avro",
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.avro_spec("share.price.avro.record", schema_version="1"),
    table_type=kc.TableType.append(),
)
```

The configuration dictionary in this query also includes a `schema.registry.url` entry. It gives the URL of a schema registry that has a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The `value_spec` argument uses [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec), which reads each record's value as Avro.

The first positional argument in the [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec) call specifies the Avro schema to use. In this case, [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec) gets the schema named `share.price.avro.record` from the schema registry. Alternatively, the first argument can be a JSON-encoded Avro schema definition string.

[`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec) also takes three optional keyword arguments:

- `schema_version` is the version of the named schema to get from the schema registry. The default, `latest`, gets the latest available version.
- `mapping` is a dictionary that maps Avro field paths to table column names. A field inside a nested record uses its dotted path, such as `parent.child`. Unless `mapped_only` is `True`, Deephaven maps each Avro field that `mapping` doesn't name to a column named with its full field path. For a nested field, that column name is the dotted path, such as `parent.child`, not the `parent__child` name Deephaven uses when you don't pass `mapping`. To keep nested column names predictable, name every nested field you keep in `mapping`.
- `mapped_only` is a boolean that defaults to `False`. When it is `True` and `mapping` is set, Deephaven leaves Avro fields that `mapping` doesn't name out of the resulting table. To keep a field inside a nested record, also name the parent record in `mapping`, as in `{"parent": "parent", "parent.child": "Child"}`. Otherwise Deephaven leaves out the whole record. Without `mapping`, `mapped_only` has no effect.

If you don't pass `mapping`, Deephaven maps each top-level Avro field to a column with the same name, and each field of a nested record to a column named with the field path joined by `__`, such as `parent__child`.

### Read Kafka topic in Protobuf format

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price.proto` in [Protobuf](https://protobuf.dev/) format. Protobuf is Google's open-source, language-neutral format for serializing structured data.

This example assumes that a schema with the subject name `share.price.proto.record` is registered in the [schema registry](#read-kafka-topic-in-avro-format) of the [Redpanda](https://www.redpanda.com/) instance from the [Docker Compose file](#launch-kafka-with-deephaven). To register a schema, see Redpanda's [schema registry documentation](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/).

```python skip-test
from deephaven.stream.kafka import consumer as kc

result = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "schema.registry.url": "http://redpanda:8081",
    },
    "share.price.proto",
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.protobuf_spec("share.price.proto.record", schema_version=1),
    table_type=kc.TableType.append(),
)
```

The configuration dictionary in this query also includes a `schema.registry.url` entry. It gives the URL of a schema registry that has a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The `value_spec` argument uses [`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec), which reads each record's value as Protobuf.

[`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec) takes the following arguments:

- `schema` is the schema subject name, `share.price.proto.record` in the example. When set, Deephaven fetches the Protobuf message descriptor from the schema registry. Set either this or `message_class`, but not both.
- `message_class` is the fully qualified Java class name for the Protobuf message on the current classpath, for example `com.example.MyMessage` or `com.example.OuterClass$MyMessage`. When this is set, Deephaven doesn't use the schema registry. Set either this or `schema`, but not both.
- `schema_version` specifies the schema version. The default, `None`, fetches the latest version. We recommend setting it so that the resulting table definition doesn't change across restarts.
- `schema_message_name` is the fully qualified Protobuf message name, for example `com.example.MyMessage`. This message's descriptor is the basis for the resulting table's definition. The default, `None`, uses the first message descriptor in the Protobuf schema. We recommend setting it for the same reason.
- `include` is a list of `/`-separated field paths to include. End a path with `/*` to also include every field path that starts with it. A field is included when any path in the list matches it. The default, `None`, includes all fields.
  - `include=["/foo/bar"]` includes the field `bar` inside `foo`, along with its parents: the top-level message and `foo`.
  - `include=["/foo/bar/*"]` also includes every field nested under `/foo/bar`, such as `/foo/bar/baz` and `/foo/bar/baz/zap`.
- `protocol` is the wire protocol of the payload, as a [`kc.ProtobufProtocol`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ProtobufProtocol). The default is [`kc.ProtobufProtocol.serdes()`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ProtobufProtocol.serdes) when `schema` is set and [`kc.ProtobufProtocol.raw()`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ProtobufProtocol.raw) when `message_class` is set.
  - [`kc.ProtobufProtocol.serdes()`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ProtobufProtocol.serdes) is the Kafka Protobuf serializer/deserializer (serdes) format, which puts a magic byte, a 4-byte schema ID, and message indexes before the Protobuf data.
  - [`kc.ProtobufProtocol.raw()`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ProtobufProtocol.raw) is the Protobuf binary encoding with no header.

Each resulting column is named with the field path joined by `_`, such as `foo_bar` for the field `bar` inside `foo`.

### Name key and value columns with properties

Deephaven can take a key or value column's name and type from consumer properties instead of from the spec:

- [`kc.KeyValueSpec.FROM_PROPERTIES`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec) takes both the column name and the type from properties. Omitting `key_spec` or `value_spec` does the same.
- [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec) without `data_type`, as in `kc.simple_spec("Symbol")`, sets the column name and takes the type from properties.

With [`kc.KeyValueSpec.FROM_PROPERTIES`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec), Deephaven reads the key or value into a single column. These properties in the configuration dictionary passed to [`consume`](../../reference/data-import-export/Kafka/consume.md) name it:

- `deephaven.key.column.name` names the key column. The default is `KafkaKey`.
- `deephaven.value.column.name` names the value column. The default is `KafkaValue`.

Deephaven chooses the column type in this order:

1. The `data_type` passed to [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec), if there is one.
2. The `deephaven.key.column.type` or `deephaven.value.column.type` property. It accepts `short`, `int`, `long`, `float`, `double`, `byte[]`, or `String` (also accepted as `string`).
3. The Kafka deserializer set in the `key.deserializer` or `value.deserializer` consumer property. Deephaven recognizes the numeric, byte-array, `UUID`, `ByteBuffer`, and `Bytes` deserializers.

For a `String` column, set the type property or pass `dht.string` to [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec).

If none of these sets a type, [`consume`](../../reference/data-import-export/Kafka/consume.md) raises an error that names the column.

### Standard data fields

Besides the key and value, each record's partition, offset, and timestamp become columns in the table that Deephaven creates when it consumes the topic. You can rename these columns, but you can't change their types.

You can also add optional columns for the receive time and for the key and value sizes. The receive time is the [date-time](../../reference/query-language/types/date-time.md) immediately after Deephaven observes the record.

| Column          | Type                                                                   | Property                            | Default name     |
| --------------- | ---------------------------------------------------------------------- | ----------------------------------- | ---------------- |
| Partition       | `int`                                                                  | `deephaven.partition.column.name`   | `KafkaPartition` |
| Offset          | `long`                                                                 | `deephaven.offset.column.name`      | `KafkaOffset`    |
| Kafka timestamp | [`Instant`](../../reference/query-language/types/date-time.md#instant) | `deephaven.timestamp.column.name`   | `KafkaTimestamp` |
| Receive time    | [`Instant`](../../reference/query-language/types/date-time.md#instant) | `deephaven.receivetime.column.name` | Not present      |
| Key size        | `int` (bytes)                                                          | `deephaven.keybytes.column.name`    | Not present      |
| Value size      | `int` (bytes)                                                          | `deephaven.valuebytes.column.name`  | Not present      |

Consumer properties control these columns. Set them in the configuration dictionary passed as the first argument to [`consume`](../../reference/data-import-export/Kafka/consume.md):

- To add an optional column, set its property to the column name you want.
- To disable a column that is present by default, set its property to an empty string, not `None`.

For example, you might disable the partition column when the topic has only one partition.

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

# Consume test.topic without the KafkaPartition column.
result_no_partition = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "deephaven.partition.column.name": "",
    },
    "test.topic",
    table_type=kc.TableType.append(),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Command", dht.string),
)
```

The `result_no_partition` table has `KafkaOffset`, `KafkaTimestamp`, and `Command` columns, and no `KafkaPartition` column.

### Join and aggregate two Kafka streams

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads two Kafka topics, `quotes` and `orders`, into Deephaven as [blink tables](../../conceptual/table-types.md#specialization-3-blink). The example uses [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) to track the latest data from each topic, [`natural_join`](../../reference/table-operations/join/natural-join.md) to join the streams, and [`agg.sum_`](../../reference/table-operations/group-and-aggregate/AggSum.md) and [`agg.weighted_sum`](../../reference/table-operations/group-and-aggregate/AggWSum.md) to aggregate the results.

The `quotes` consumer doesn't pass `key_spec`. Instead, the `deephaven.key.column.name` and `deephaven.key.column.type` properties read each record's key into a `String` column named `Symbol` (see [Name key and value columns with properties](#name-key-and-value-columns-with-properties)).

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import agg
from deephaven import dtypes as dht

price_table = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "deephaven.key.column.name": "Symbol",
        "deephaven.key.column.type": "String",
    },
    "quotes",
    table_type=kc.TableType.blink(),
    value_spec=kc.json_spec({"Price": dht.double}),
)

last_price = price_table.last_by(by=["Symbol"])

orders_blink = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "orders",
    value_spec=kc.json_spec(
        {
            "Symbol": dht.string,
            "Id": dht.string,
            "LimitPrice": dht.double,
            "Qty": dht.int64,
        }
    ),
    table_type=kc.TableType.blink(),
    key_spec=kc.KeyValueSpec.IGNORE,
)

orders_with_current_price = orders_blink.last_by("Id").natural_join(
    table=last_price, on=["Symbol"], joins=["LastPrice = Price"]
)

agg_list = [agg.sum_("Shares = Qty"), agg.weighted_sum("Qty", "Notional = LastPrice")]

total_notional = orders_with_current_price.agg_by(agg_list, by=["Symbol"])
```

Next, let's add records to the two topics. In a terminal, first run the following command to start writing to the `quotes` topic:

```shell
docker compose exec redpanda rpk topic produce quotes -f '%k %v\n'
```

Then add the following entries:

```text
AAPL {"Price": 135}
AAPL {"Price": 133}
TSLA {"Price": 730}
TSLA {"Price": 735}
```

After submitting the entries to the `quotes` topic, use the following command to write to the `orders` topic:

```shell
docker compose exec redpanda rpk topic produce orders -f "%v\n"
```

Then add the following entries in the terminal:

```text
{"Symbol": "AAPL", "Id":"o1", "LimitPrice": 136, "Qty": 7}
{"Symbol": "AAPL", "Id":"o2", "LimitPrice": 132, "Qty": 2}
{"Symbol": "TSLA", "Id":"o3", "LimitPrice": 725, "Qty": 1}
{"Symbol": "TSLA", "Id":"o4", "LimitPrice": 730, "Qty": 9}
```

The tables update as you add each entry to the Kafka streams. The final results are in the `total_notional` table.

### Consume a Kafka stream into a partitioned table

[`consume_to_partitioned_table`](../../reference/data-import-export/Kafka/consume-to-partitioned-table.md) consumes a topic into a [partitioned table](../partitioned-tables.md) with one subtable for each Kafka partition. Its key column, `Partition`, holds the Kafka partition number. See the [partitioned tables guide](../partitioned-tables.md) for the operations you can run on it.

Two things to know about [`consume_to_partitioned_table`](../../reference/data-import-export/Kafka/consume-to-partitioned-table.md):

- It takes the same arguments as [`consume`](../../reference/data-import-export/Kafka/consume.md).
- It reads all partitions by default. To read specific partitions, pass a list such as `partitions=[1, 3, 5]`.

```python docker-config=kafka test-set=2 order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

result_partitioned = kc.consume_to_partitioned_table(
    {"bootstrap.servers": "redpanda:9092"},
    "test.topic",
    table_type=kc.TableType.append(),
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Command", dht.string),
)
```

### Custom Kafka parser

Some use cases call for custom parsing of Kafka streams, such as when payloads use non-standard encodings or need complex transformation.

See the dedicated guide, [Write your own custom parser for Kafka](./write-your-own-custom-parser-for-kafka.md), for a step-by-step walkthrough and complete examples.

## Write to a Kafka stream

Deephaven can write tables to Kafka streams as well. When data in a table changes with real-time updates, Deephaven also writes those changes to Kafka. The [Kafka producer module](/core/pydoc/code/deephaven.stream.kafka.producer.html#module-deephaven.stream.kafka.producer) defines functions to do this, including [`produce`](../../reference/data-import-export/Kafka/produce.md).

When producing, a key spec and a value spec work in the other direction. They say which table columns Deephaven writes as each record's key and value.

Kafka's default partitioner hashes a non-null key to choose a partition. All records with the same key go to the same partition as long as the topic's partition count doesn't change.

In this example, we write a [time table](../time-table.md) to a topic called `time-topic`. The example writes column `X` as each record's key and ignores the value.

```python docker-config=kafka test-set=1 ticking-table order=null
from deephaven import time_table
from deephaven.stream.kafka import producer as pk

source = time_table("PT00:00:00.1").update(formulas=["X = i"])

write_topic = pk.produce(
    source,
    {"bootstrap.servers": "redpanda:9092"},
    "time-topic",
    pk.simple_spec("X"),
    pk.KeyValueSpec.IGNORE,
)
```

To see the records arrive, run `docker compose exec redpanda rpk topic consume time-topic` in a terminal.

[`produce`](../../reference/data-import-export/Kafka/produce.md) returns a callback. Keep a reference to it, such as `write_topic`, for as long as you want to publish. Call `write_topic()` to stop publishing and release its resources.

Now we write a [time table](../time-table.md) to a topic called `time-topic_group`. The last argument, `True`, sets `last_by_key_columns`, which tells the producer to perform a [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) on the key columns before writing to the stream.

```python docker-config=kafka test-set=1 ticking-table order=null
from deephaven import time_table
from deephaven.stream.kafka import producer as pk

source_group = time_table("PT00:00:00.1").update(
    formulas=["X = randomInt(1, 5)", "Y = i"]
)

write_topic_group = pk.produce(
    source_group,
    {"bootstrap.servers": "redpanda:9092"},
    "time-topic_group",
    pk.json_spec(["X"]),
    pk.json_spec(
        [
            "X",
            "Y",
        ]
    ),
    True,
)
```

## Related documentation

- [Kafka basic terminology](../../conceptual/kafka-basic-terms.md)
- [Table types](../../conceptual/table-types.md)
- [Incremental update model](../../conceptual/table-update-model.md)
- [Custom parser for Kafka](./write-your-own-custom-parser-for-kafka.md)
- [Kafka overview](../overview-kafka.md)
- [Partitioned tables](../partitioned-tables.md)
- [Create a time table](../time-table.md)
- [Filter table data](../use-filters.md)
- [Exact and relational joins](../joins-exact-relational.md)
- [Multi-aggregation](../combined-aggregations.md)
- [`consume`](../../reference/data-import-export/Kafka/consume.md)
- [`consume_to_partitioned_table`](../../reference/data-import-export/Kafka/consume-to-partitioned-table.md)
- [`produce`](../../reference/data-import-export/Kafka/produce.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`natural_join`](../../reference/table-operations/join/natural-join.md)
- [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md)
- [`agg.sum_`](../../reference/table-operations/group-and-aggregate/AggSum.md)
- [`agg.weighted_sum`](../../reference/table-operations/group-and-aggregate/AggWSum.md)
- [`rename_columns`](../../reference/table-operations/select/rename-columns.md)
- [Date-times](../../reference/query-language/types/date-time.md)
- [`consumer` Pydoc](/core/pydoc/code/deephaven.stream.kafka.consumer.html)
- [`producer` Pydoc](/core/pydoc/code/deephaven.stream.kafka.producer.html)
- [`jackson` Pydoc](/core/pydoc/code/deephaven.json.jackson.html)
