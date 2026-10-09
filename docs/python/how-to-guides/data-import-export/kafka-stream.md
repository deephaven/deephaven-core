---
title: Connect to a Kafka stream
---

Kafka is a distributed event streaming platform that lets you read, write, store, and process events, also called records.

Kafka topics take on many forms, such as raw input, [JSON](#read-kafka-topic-in-json-format), [Avro](#read-kafka-topic-in-avro-format), or [Protobuf](#read-kafka-topic-in-protobuf-format). In this guide, we show you how to read each of these formats into Deephaven tables, and how to [write a Deephaven table to Kafka](#write-to-a-kafka-stream).

See [A Kafka introduction: basic terms](../../conceptual/kafka-basic-terms.md) for a detailed discussion of Kafka topics and supported formats. See the [Apache Kafka documentation](https://kafka.apache.org/documentation/) for full details on how to use Kafka.

## Key and value

Each Kafka record has a key and a value. Kafka writes each record to a partition at an offset, with a timestamp. For example, a list of Kafka messages might have a stock ticker as the key and its price as the value.

The key and value are similar in that they can be nearly any sequence of bytes. The primary difference is that Kafka's default partitioner hashes a non-null key to choose a partition, so all records with the same key go to the same partition as long as the topic's partition count doesn't change.

A _key spec_ and a _value spec_ tell Deephaven how to turn the key and the value into table columns.

When a single-column key spec doesn't name its column, the name comes from the `deephaven.key.column.name` consumer property, an entry in the configuration dictionary passed as the first argument to [`consume`](../../reference/data-import-export/Kafka/consume.md). If that property isn't set, the name defaults to `KafkaKey`. Value specs work the same way with `deephaven.value.column.name` and `KafkaValue`.

Deephaven chooses the column type in this order:

1. The type set in the spec, if there is one.
2. The `deephaven.key.column.type` or `deephaven.value.column.type` property. It accepts `short`, `int`, `long`, `float`, `double`, `byte[]`, or `String`.
3. The Kafka deserializer set in the `key.deserializer` or `value.deserializer` consumer property. Deephaven recognizes the numeric, byte-array, `UUID`, `ByteBuffer`, and `Bytes` deserializers.

For a `String` column, set the type property or pass the type to the spec.

The key and the value can each be read as:

- [simple type](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec)
- [JSON encoded](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec)
- [Avro encoded](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec)
- [Protobuf encoded](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec)
- [parsed by an object processor](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec), such as a Jackson JSON provider
- ignored

You can't ignore both the key and the value.

## Standard data fields

Besides the key and value, each record's partition, offset, and timestamp become columns in the table that Deephaven creates when it consumes the topic. You can change the column names, but not the column types. You can also add optional columns for the receive time and for the key and value sizes. The receive time is the [date-time](../../reference/query-language/types/date-time.md) immediately after Deephaven observes the record.

| Column          | Type                                                                   | Property                            | Default name     |
| --------------- | ---------------------------------------------------------------------- | ----------------------------------- | ---------------- |
| Partition       | `int`                                                                  | `deephaven.partition.column.name`   | `KafkaPartition` |
| Offset          | `long`                                                                 | `deephaven.offset.column.name`      | `KafkaOffset`    |
| Kafka timestamp | [`Instant`](../../reference/query-language/types/date-time.md#instant) | `deephaven.timestamp.column.name`   | `KafkaTimestamp` |
| Receive time    | [`Instant`](../../reference/query-language/types/date-time.md#instant) | `deephaven.receivetime.column.name` | Not present      |
| Key size        | `int` (bytes)                                                          | `deephaven.keybytes.column.name`    | Not present      |
| Value size      | `int` (bytes)                                                          | `deephaven.valuebytes.column.name`  | Not present      |

Consumer properties control these columns. These are entries in the configuration dictionary passed as the first argument to [`consume`](../../reference/data-import-export/Kafka/consume.md). To add an optional column, set its property to the column name you want. To disable a column that is present by default, set its property to an empty string. The property doesn't accept a null value. For example, you might disable the partition column when the topic has only one partition.

```python skip-test
...
# Kafka consumer with the Partition column suppressed.

result = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "deephaven.partition.column.name": "",
    },
...
```

## Table types

Deephaven Kafka tables can be append-only, blink, or ring. Set the type with the `table_type` argument, using the `TableType` class from `deephaven.stream.kafka.consumer`, imported as `kc` in the examples on this page.

- [Append-only](../../conceptual/table-types.md#specialization-1-append-only) tables keep every row. The table and its memory use can grow without limit. To use this type, pass `table_type=kc.TableType.append()`.
- [Blink](../../conceptual/table-types.md#specialization-3-blink) tables keep only the rows from the current [update cycle](../../conceptual/table-update-model.md). Each new message appears as a row for one update cycle and then disappears. Blink is the default table type. To set it explicitly, use `table_type=kc.TableType.blink()`.
- [Ring](../../conceptual/table-types.md#specialization-4-ring) tables keep only the last `N` rows. When the table grows beyond `N` rows, it discards the oldest rows until `N` remain. To use this type, pass `table_type=kc.TableType.ring(N)`.

Combine a blink table with a stateful aggregation such as [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) to keep results after the rows disappear.

## Launching Kafka with Deephaven

Deephaven has an official [Docker Compose file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples-redpanda/docker-compose.yml) that contains the Deephaven images along with a [Redpanda](https://github.com/redpanda-data/redpanda) image. Redpanda lets you input data directly into a Kafka stream from the terminal. Redpanda is one of many Kafka-compatible event streaming platforms that work with Deephaven.

Save this locally as a `docker-compose.yml` file, and launch with `docker compose up`.

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

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) creates a Deephaven table from a Kafka topic. Here, `{"bootstrap.servers": "redpanda:9092"}` is a dictionary that describes how to connect to the Kafka infrastructure. `bootstrap.servers` provides the initial hosts that a Kafka client uses to connect. In this case, `bootstrap.servers` is set to `redpanda:9092`.

The `table_type` argument, `kc.TableType.append()`, creates an append-only table. The `key_spec` argument, `kc.KeyValueSpec.IGNORE`, ignores the Kafka key. The `value_spec` argument, [`simple_spec`](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec) `("Command", dht.string)`, reads each record's value into a `String` column named `Command`.

The `result_append` table is now subscribed to all partitions in the `test.topic` topic. When you send data to the `test.topic` topic, it appears in the table.

### Input Kafka data for testing

For this example, you enter information into the Kafka topic from a terminal. To do this, run:

```shell
docker compose exec redpanda rpk topic produce test.topic
```

This waits for input from the terminal and sends each line you enter to the `test.topic` topic as a record. Press **Ctrl + D** when you're done to stop producing.

Once sent, that information appears automatically in your Deephaven table.

<LoopedVideo src='../../assets/how-to/kafka1.mp4' />

### Ring and blink tables

The following example shows how to create [ring and blink tables](#table-types) to read from the `test.topic` topic:

```python docker-config=kafka test-set=2 order=null
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

Let's run a few more `docker compose exec redpanda rpk topic produce test.topic` commands to input additional data into the Kafka stream. As you can see, the `result_append` table contains all the data, the `result_ring` table contains the last three entries, and the `result_blink` table shows each new row for only one update cycle.

Because rows disappear from the `result_blink` table after one update cycle, let's add a table that uses [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) to keep the last row added to the `result_blink` table.

```python docker-config=kafka test-set=2 order=null
last_blink = result_blink.last_by()
```

### Read the key and choose partitions and offsets

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price` into an append-only table. Unlike the previous examples, it reads both the key and the value, and it sets the partitions and the starting offsets explicitly.

When reading a Kafka topic, you can select which partitions to listen to. By default, Deephaven reads all partitions. You can also choose where reading starts in each partition: at the beginning, at the end, at a specific offset, or without seeking. By default, Deephaven doesn't seek. Reading then starts where Kafka's consumer settings say. The `offsets` bullet below explains those settings.

> [!NOTE]
> Starting at the beginning of a partition reads only the messages Kafka still retains. Kafka can be configured to keep messages up to a maximum age, or to keep only the last message for each key.

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
import deephaven.dtypes as dht

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

- `partitions` is set to `None`, which specifies that we want to listen to all partitions. This is the default behavior if `partitions` is not explicitly defined.
- `offsets` is set to `kc.ALL_PARTITIONS_DONT_SEEK`, which doesn't seek. If the consumer's [consumer group](https://kafka.apache.org/documentation/#intro_consumers), set with the `group.id` property, has a committed offset, the consumer starts there. Otherwise, Kafka's `auto.offset.reset` property decides where to start. Its default, `latest`, reads only new messages.
- `key_spec` is set to [`simple_spec`](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec) `('Symbol', dht.string)`, which instructs the consumer to expect messages with a Kafka `key` field, and creates a `Symbol` column of type `String` to store the information.
- `value_spec` is set to `simple_spec('Price', dht.string)`, which instructs the consumer to expect messages with a Kafka `value` field, and creates a `Price` column of type `String` to store the information. Deephaven reads the price as a string because Redpanda's command-line tool, `rpk`, sends it as text. A numeric type such as `dht.double` would use Kafka's binary deserializer, which expects binary values.
- `table_type` is set to `kc.TableType.append()`, which creates an append-only table.

To choose a different starting point, pass one of these values as `offsets`:

- `kc.ALL_PARTITIONS_SEEK_TO_END` always starts with new messages only.
- `kc.ALL_PARTITIONS_SEEK_TO_BEGINNING` starts at the beginning of every partition.
- A dictionary that maps partition numbers to offsets, such as `{0: 100, 1: 250}`, starts at those offsets. Partitions that the dictionary doesn't list don't seek.

To listen to specific partitions, pass them as a list of integers, such as `partitions=[1, 3, 5]`.

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

To drop the `Symbol` column, pass `key_spec=kc.KeyValueSpec.IGNORE` instead, as in the first example.

### Read Kafka topic in JSON format

The following two examples read a Kafka topic called `orders` in JSON format.

This example uses [`json_spec`](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec):

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
import deephaven.dtypes as dht

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

Here, the `value_spec` argument uses `json_spec`, which parses each record's value as JSON.

The first argument to `json_spec` is a table definition: a dictionary that maps each column name in the result table to its Deephaven type (for example, `dht.double`).

The `mapping` keyword argument of `json_spec` is a dictionary that maps JSON field names to table column names. Each column name must appear in the table definition from the first argument. The `mapping` dictionary may contain fewer entries than the total number of columns defined in the first argument.

In the example, the map entry `"price": "Price"` reads the JSON field `price` into the `Price` column of the result table. For a column that the map doesn't mention, Deephaven reads the JSON field with the same name as the column.

If you omit `mapping`, Deephaven assumes that JSON field names match column names.

This example uses [`object_processor_spec`](https://deephaven.io/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec) with a [Jackson provider](/core/pydoc/code/deephaven.json.jackson.html):

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

Here, `object_processor_spec` takes a Jackson provider built from a dictionary that maps each JSON field name to its type. The resulting columns take the JSON field names, so the example renames them with [`rename_columns`](../../reference/table-operations/select/rename-columns.md).

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

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price` in [Avro](https://avro.apache.org/) format. This example assumes that a schema named `share.price.record` is registered in the schema registry of the [Redpanda](https://www.redpanda.com/) instance from the [Docker Compose file](#launching-kafka-with-deephaven).

A [schema registry](https://medium.com/slalom-technology/introduction-to-schema-registry-in-kafka-915ccf06b902) stores Kafka event schema definitions and tracks their versions so that producers and consumers can share them. To register a schema, see Redpanda's [schema registry documentation](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/).

```python skip-test
from deephaven.stream.kafka import consumer as kc

result = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "schema.registry.url": "http://redpanda:8081",
    },
    "share.price",
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.avro_spec("share.price.record", schema_version="1"),
    table_type=kc.TableType.append(),
)
```

In this query, the first argument includes an additional entry for `schema.registry.url` to specify the URL for a schema registry with a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The `value_spec` argument uses [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec), which specifies an Avro format for the Kafka `value` field.

The first positional argument in the `avro_spec` call specifies the Avro schema to use. In this case, `avro_spec` gets the schema named `share.price.record` from the schema registry. Alternatively, the first argument can be a JSON-encoded Avro schema definition string.

`avro_spec` also takes three optional keyword arguments:

- `schema_version` is the version of the named schema to get from the schema registry. The default, `latest`, gets the latest available version.
- `mapping` is a dictionary that maps Avro field names to table column names. Unless `mapped_only` is `True`, Deephaven maps each Avro field that `mapping` doesn't name to a column of the same name.
- `mapped_only` is a boolean that defaults to `False`. When it is `True` and `mapping` is set, Deephaven leaves Avro fields that `mapping` doesn't name out of the resulting table. Without `mapping`, it has no effect.

### Read Kafka topic in Protobuf format

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price` in [Protobuf](https://protobuf.dev/) format. Protobuf is Google's open-source, language-neutral format for serializing structured data.

This example assumes that a schema with the subject name `share.price.record` is registered in the [schema registry](#read-kafka-topic-in-avro-format) of the [Redpanda](https://www.redpanda.com/) instance from the [Docker Compose file](#launching-kafka-with-deephaven). A schema registry stores each schema under a _subject_ name and tracks the subject's versions. To register a schema, see Redpanda's [schema registry documentation](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/).

```python skip-test
from deephaven.stream.kafka import consumer as kc

result = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "schema.registry.url": "http://redpanda:8081",
    },
    "share.price",
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.protobuf_spec("share.price.record", schema_version=1),
    table_type=kc.TableType.append(),
)
```

In this query, the first argument includes an additional entry for `schema.registry.url` to specify the URL for a schema registry with a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The `value_spec` argument uses [`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec), which specifies a Protobuf format for the Kafka `value` field.

`protobuf_spec` takes the following arguments:

- `schema` is the schema subject name, `share.price.record` in the example. When set, Deephaven fetches the Protobuf message descriptor from the schema registry. Set either this or `message_class`, but not both.
- `message_class` is the fully qualified Java class name for the Protobuf message on the current classpath, for example `com.example.MyMessage` or `com.example.OuterClass$MyMessage`. When this is set, Deephaven doesn't use the schema registry. Set either this or `schema`, but not both.
- `schema_version` specifies the schema version, or `None` (the default) for latest. In cases where restarts cause schema changes, we recommend setting this to ensure the resulting table definition doesn't change.
- `schema_message_name` is the fully qualified Protobuf message name, for example `com.example.MyMessage`. This message's descriptor is the basis for the resulting table's definition. If `None` (the default), Deephaven uses the first message descriptor in the Protobuf schema. We recommend setting this explicitly.
- `include` is a list of `/`-separated field paths to include. End a path with `/*` to also include every field path that starts with it. A field is included when any path in the list matches it. The default, `None`, includes all fields.
  - For example, `include=["/foo/bar"]` includes the field `bar` inside `foo`, along with its parents: the top-level message and `foo`.
  - `include=["/foo/bar/*"]` also includes every field nested under `/foo/bar`, such as `/foo/bar/baz` and `/foo/bar/baz/zap`.
- `protocol` is the wire protocol for this payload, as a `kc.ProtobufProtocol`.
  - When `schema` is set, the default is `kc.ProtobufProtocol.serdes()`.
  - When `message_class` is set, the default is `kc.ProtobufProtocol.raw()`.

### Join and aggregate two Kafka streams

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads two Kafka topics, `quotes` and `orders`, into Deephaven as blink tables. The example uses [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) to track the latest data from each topic, [`natural_join`](../../reference/table-operations/join/natural-join.md) to join the streams, and [`agg.sum_`](../../reference/table-operations/group-and-aggregate/AggSum.md) and [`agg.weighted_sum`](../../reference/table-operations/group-and-aggregate/AggWSum.md) to aggregate the results.

The `quotes` consumer doesn't pass `key_spec`. Instead, the `deephaven.key.column.name` and `deephaven.key.column.type` properties read each record's key into a `String` column named `Symbol` (see [Key and value](#key-and-value)).

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import agg
import deephaven.dtypes as dht

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

Next, let's add records to the two topics. In a terminal, first run the following command to start writing to
the `quotes` topic:

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

[Partitioned tables](../partitioned-tables.md) are Deephaven tables that are partitioned into subtables by one or more key columns. They have their own operations and are useful when working with large data sets.

To consume from Kafka directly into a partitioned table, use [`consume_to_partitioned_table`](../../reference/data-import-export/Kafka/consume-to-partitioned-table.md). Its syntax is similar to that of [`consume`](../../reference/data-import-export/Kafka/consume.md). The result is always partitioned by Kafka partition. By default, it reads all partitions. To read specific partitions, pass them as a list with the `partitions` argument, such as `partitions=[1, 3, 5]`, as in [Read the key and choose partitions and offsets](#read-the-key-and-choose-partitions-and-offsets).

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

In this example, we write a simple [time table](../../reference/table-operations/create/timeTable.md) to a topic called `time-topic`. The example writes column `X` as each record's key and ignores the value.

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

Now we write a time table to a topic called `time-topic_group`. The last argument, `True`, sets `last_by_key_columns`, which tells the producer to perform a [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) on the key columns before writing to the stream.

```python docker-config=kafka test-set=1 ticking-table order=null
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
- [Custom parser for Kafka](./write-your-own-custom-parser-for-kafka.md)
- [`consume`](../../reference/data-import-export/Kafka/consume.md)
- [`consume_to_partitioned_table`](../../reference/data-import-export/Kafka/consume-to-partitioned-table.md)
- [`produce`](../../reference/data-import-export/Kafka/produce.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [Kafka Pydoc](/core/pydoc/code/deephaven.stream.kafka.html)
