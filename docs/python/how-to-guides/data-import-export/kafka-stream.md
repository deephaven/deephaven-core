---
title: Connect to a Kafka stream
---

Kafka is a distributed event streaming platform that lets you read, write, store, and process events, also called records.

Kafka topics take on many forms, such as raw input, [JSON](#read-kafka-topic-in-json-format), [AVRO](#read-kafka-topic-in-avro-format), or [Protobuf](#read-kafka-topic-in-protobuf-format) In this guide, we show you how to import each of these formats as Deephaven tables.

Please see our overview, [Kafka in Deephaven: Basic terms](../../conceptual/kafka-in-deephaven.md), for a detailed discussion of Kafka topics and supported formats. See the [Apache Kafka Documentation](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) for full details on how to use Kafka.

## Standard data fields

Kafka has the standard data fields of `partition`, `offset`, and `timestamp`. Each field becomes a column in the new table that stores the Kafka stream. The column names can be changed, but the column type is set. The standard names and types for these values are:

- `KafkaPartition`: int
- `KafkaOffset`: long
- `KafkaTimestamp`: [DateTime](../../reference/query-language/types/date-time.md)

You can also add optional columns for:

- Receive time: The [date-time](../../reference/query-language/types/date-time.md) value immediately after Deephaven observed the records.
- Key size (in bytes): int
- Value size (in bytes): int

These additional columns are controlled by setting consumer properties. To disable columns present by default, set their names to an empty String (null values are not allowed).

```python skip-test
...
# Kafka consumer with the Partition column suppressed.

result = kc.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "schema.registry.url": "http://redpanda:8081",
        "deephaven.partition.column.name":"",
    },
...
```

| Column          | Property                            | Default name     |
| --------------- | ----------------------------------- | ---------------- |
| Partition       | `deephaven.partition.column.name`   | `KafkaPartition` |
| Offset          | `deephaven.offset.column.name`      | `KafkaOffset`    |
| Kafka timestamp | `deephaven.timestamp.column.name`   | `KafkaTimestamp` |
| Receive time    | `deephaven.receivetime.column.name` | Not present      |
| Key size        | `deephaven.keybytes.column.name`    | Not present      |
| Value Size      | `deephaven.valuebytes.column.name`  | Not present      |

When reading a Kafka topic, you can select which partitions to listen to. By default, all partitions are read. Additionally, topics can be read from the beginning, latest offset, or first unprocessed offset. By default, all partitions are read from the latest offset.

> [!NOTE]
> The Kafka infrastructure can retain old messages for a maximum given age or retain the last message for individual keys.

While these three fields are traditionally included in the new table, you can choose to ignore them, such as when there is only one partition.

## Key and value

Kafka streams store data in the `KafkaKey` and `KafkaValue` columns. This information is logged onto the partition with an offset at a certain time. For example, a list of Kafka messages might have a stock ticker as the key and its price as the value.

`KafkaKey` and `KafkaValue` are similar in that they can be nearly any sequence of bytes. The primary difference is that the key is used to create a hash that will facilitate load balancing. By default, each key and value are stored with column names of either `KafkaKey` or `KafkaValue`, and String type.

The `KafkaKey` and `KafkaValue` attributes can be:

- [simple type](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec)
- [JSON encoded](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec)
- [Avro encoded](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec)
- [Protbuf encoded](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec)
- ignored (cannot ignore both key and value)

## Table types

Deephaven Kafka tables can be append-only, blink, or ring.

- [Append-only](../../conceptual/table-types.md#specialization-1-append-only) tables keep every row. Table size (and thus, memory consumption) can grow indefinitely. Set this value with `table_type = TableType.append()`.
- [Blink](../../conceptual/table-types.md#specialization-3-blink) tables only keep the set of rows from the current update cycle. This forms the basis for more advanced use cases when combined with stateful table aggregations like [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md). For blink tables without any downstream table operations, aggregations or listeners, the new messages will appear as rows in the table for one update cycle, then disappear on the next update cycle. You can set this value to `table_type = TableType.blink()` to be explicit, but this is not required, as this is the default.
- [Ring](../../conceptual/table-types.md#specialization-4-ring) tables keep only the last `N` rows. When the table grows beyond `N` rows, the oldest are discarded until `N` remain. Set this value with `table_type = TableType.ring(N)`.

## Launching Kafka with Deephaven

Deephaven has an official [docker-compose file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples-redpanda/docker-compose.yml) that contains the Deephaven images along with images from [Redpanda](https://github.com/redpanda-data/redpanda). Redpanda lets us input data directly into a Kafka stream from the terminal. This is just one of the supported Kafka-compatible event streaming platforms. Many more are available.

Save this locally as a `docker-compose.yml` file, and launch with `docker compose up`.

## Consume a Kafka stream

In this example, we consume a Kafka topic (`test.topic`) as a Deephaven table. The Kafka topic is populated by commands entered into the terminal.

For demonstration purposes, we will be using an `append` table and ignoring the Kafka key.

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

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) creates a Deephaven table from a Kafka topic. Here, `{'bootstrap.servers': 'redpanda:9092'}` is a dictionary describing how the Kafka infrastructure is configured. `bootstrap.servers` provides the initial hosts that a Kafka client uses to connect. In this case, `bootstrap.servers` is set to `redpanda:9092`.

`table_type` is set to `kc.TableType.append()` to create an append-only table, and `key_spec` is set to `kc.KeyValueSpec.IGNORE` to ignore the Kafka key.

The `result` table is now subscribed to all partitions in the `test.topic` topic. When data is sent to the `test.topic` topic, it will appear in the table.

### Input Kafka data for testing

For this example, information is entered into the Kafka topic via a terminal. To do this, run:

```bash
docker compose exec redpanda rpk topic produce test.topic
```

This will wait for input from the terminal and will send any input to the `test.topic` topic. Enter the information and use the keyboard shortcut **Ctrl + D** to send.

Once sent, that information will automatically appear in your Deephaven table.

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

Let's run a few more `docker compose exec redpanda rpk topic produce test.topic` commands to input additional data into the Kafka stream. As you can see, the `result_append` table contains all the data, the `result_ring` table contains the last three entries, and the `result_blink` only shows rows before the next table update cycle is executed.

Since the `result_blink` table doesn't show the values in the topic, let's add a table to store the last value added to the `result_blink` table.

```python docker-config=kafka test-set=2 order=null
last_blink = result_blink.last_by()
```

### Import a Kafka stream with append

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price`. The specific key and value result in a table that appends new rows.

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
import deephaven.dtypes as dht

result = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "share.price",
    partitions=None,
    offsets=kc.ALL_PARTITIONS_DONT_SEEK,
    key_spec=kc.simple_spec("Symbol", dht.string),
    value_spec=kc.simple_spec("Price", dht.double),
    table_type=kc.TableType.append(),
)
```

Let's walk through this query, focusing on the new optional arguments we've set.

- `partitions` is set to `None`, which specifies that we want to listen to all partitions. This is the default behavior if `partitions` is not explicitly defined. To listen to specific partitions, we can define them as a list of integers (e.g., `partitions=[1, 3, 5]`).
- `offsets` is set to `ALL_PARTITIONS_DONT_SEEK`, which only listens to new messages produced after this call is processed.
- `key_spec` is set to `simple('Symbol')`, which instructs the consumer to expect messages with a Kafka `key` field, and creates a `Symbol` column of type String to store the information.
- `value_spec` is set to `simple('Price')`, which instructs the consumer to expect messages with a Kafka `value` field, and creates a `Price` column of type String to store the information.
- `table_type` is set to `append`, which creates an append-only table.

Now let's add some entries to our Kafka stream.

Run `docker compose exec redpanda rpk topic produce share.price -f '%k %v\n'` and enter as many key-value pairs as you want, separated by spaces and new lines:

```
AAPL 135.60
AAPL 135.99
AAPL 136.82
```

### Import a Kafka stream ignoring keys

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price` and ignores the partition and key values.

Run the same `docker compose exec redpanda rpk topic produce share.price -f '%k %v\n'` command from the previous section and enter the sample key-value pairs.

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
import deephaven.dtypes as dht

result = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "share.price",
    key_spec=kc.KeyValueSpec.IGNORE,
    value_spec=kc.simple_spec("Price", dht.double),
    table_type=kc.TableType.append(),
)
```

As you can see, the key column is not included in the output table.

### Read Kafka topic in JSON format

The following two examples read a Kafka topic called `orders` in JSON format.

This example uses [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec):

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

This example uses [`object_processor_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec) with a [Jackson provider](/core/pydoc/code/deephaven.json.jackson.html):

```python docker-config=kafka order=null
from deephaven import kafka_consumer as kc
from deephaven.stream.kafka.consumer import (
    TableType,
    KeyValueSpec,
    object_processor_spec,
)
from deephaven.json import jackson

result = kc.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "orders",
    key_spec=KeyValueSpec.IGNORE,
    value_spec=object_processor_spec(
        jackson.provider(
            {
                "symbol": str,
                "price": float,
                "qty": int,
            }
        )
    ),
    table_type=TableType.append(),
).rename_columns(["Symbol=symbol", "Price=price", "Qty=qty"])
```

Run `docker compose exec redpanda rpk topic produce orders -f "%v\n"` in your terminal and enter the following values:

```
{"symbol": "AAPL", "price": 135, "qty": 5}
{"symbol": "TSLA", "price": 730, "qty": 3}
```

In this query, the `value_spec` argument uses [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec). A JSON parameterization is used for the `KafkaValue` field.

After this, we see an ordered list of Python tuples specifying column definitions.

- The first element in each tuple is a string for the column name in the result table.
- The second element in each tuple is a string for the column data type in the result table.

Within the `value_spec` argument, the keyword argument of `mapping` is given. This is a Python dictionary specifying a mapping from JSON field names to resulting table column names. Column names should be in the list provided in the first argument described above. The `mapping` dictionary may contain fewer entries than the total number of columns defined in the first argument.

In the example, the map entry `'price' : 'Price'` specifies the incoming messages are expected to contain a JSON field named `price`, whose value will be mapped to the `Price` column in the resulting table. The columns not mentioned are mapped from matching JSON fields.

If the `mapping` keyword argument is not provided, it is assumed that JSON field names and column names will match.

### Read Kafka topic in Avro format

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price` in [Avro](https://avro.apache.org/) format. This example uses an external schema definition registered in the development testing [Redpanda](https://www.redpanda.com/) instance that can be seen below.

A [Kafka Schema Registry](https://medium.com/slalom-technology/introduction-to-schema-registry-in-kafka-915ccf06b902) allows sharing and versioning of Kafka event schema definitions.

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

The first positional argument in the [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec) call specifies the Avro schema to use. In this case, [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec) gets the schema named `share.price.record` from the schema registry. Alternatively, the first argument can be an `org.apache.avro.Schema` object obtained from [`getAvroSchema`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#getAvroSchema(java.lang.String)).

Three optional keyword arguments are supported:

- `schema_version` specifies the version of the schema to get, for the given name, from the schema registry. If not specified, the default of `latest` is assumed. This will retrieve the latest available schema version.
- `mapping` expects a dictionary value and, if provided, specifies a name mapping for Avro field names to table column names. Any Avro field name not mentioned is mapped to a column of the same name.
- `mapping_only` expects a dictionary value and, if provided, specifies a name mapping for Avro field names to table column names. Any Avro field name not mentioned is omitted from the resulting table.
- When `mapping` and `mapping_only` are both omitted, all Avro schema fields are mapped to columns using the field name as column name.

### Read Kafka topic in Protobuf format

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads the Kafka topic `share.price` in [Protobuf](https://protobuf.dev/) format, Google’s open-source, language-neutral, cross-platform data format used to serialize structured data.

This example uses an external schema definition registered in the development testing [Redpanda](https://www.redpanda.com/) instance that can be seen below.

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

The `value_spec` argument uses [`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec), which specifies a Protocol Buffer format for the Kafka `value` field.

The first positional argument in the [`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec) call specifies the schema name -- in this case, `share.price.record` from the schema registry. Alternatively, this could be the fully-qualified Java class name for the protobuf message on the current classpath, for example “com.example.MyMessage” or “com.example.OuterClass$MyMessage”.

Several optional keyword arguments are supported:

- `schema` is the schema subject name as used above. When set, this will fetch the protobuf message descriptor from the schema registry. Either this or `message_class` must be set.
- `message_class` is the fully-qualified Java class name for the protobuf message on the current classpath, for example “com.example.MyMessage” or “com.example.OuterClass$MyMessage”. When this is set, the schema registry will not be used. Either this or `schema` must be set.
- `schema_version` specifies the schema version, or `None` (the default) for latest. In cases where restarts cause schema changes, we recommend setting this to ensure the resulting table definition will not change.
- `schema_message_name` is the fully-qualified protobuf message name, for example “com.example.MyMessage”. This message’s descriptor will be used as the basis for the resulting table’s definition. If `None` (the default), the first message descriptor in the protobuf schema will be used. It is advisable for callers to explicitly set this.
- `include` contains `/` separated paths to include. The final path may be a `*` to additionally match everything that starts with the path.
  - For example, `include=[“/foo/bar”]` will include the field path name paths `[]`, `[“foo”]`, and `[“foo”, “bar”]`.
  - `include=[“/foo/bar/*”]` will additionally include any field path name paths that start with `[“foo”, “bar”]: [“foo”, “bar”, “baz”], [“foo”, “bar”, “baz”, “zap”]`, etc. When multiple includes are specified, the fields will be included when any of the components match. The default is `None`, which includes all paths.
- `protocol` is the wire protocol for this payload.
  - When `schema` is set, `ProtobufProtocol.serdes()` will be used by default.
  - When `message_class` is set, `ProtobufProtocol.raw()` will be used by default.

### Perform multiple operations

In this example, [`consume`](../../reference/data-import-export/Kafka/consume.md) reads two Kafka topics, `quotes` and `orders`, into Deephaven as blink tables. Table operations are used to track the latest data from each topic (using [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)), join the streams together ([`natural_join`](../../reference/table-operations/join/natural-join.md)), and aggregate ([`agg.sum_`](../../reference/table-operations/group-and-aggregate/AggSum.md)) the results.

```python docker-config=kafka order=null
from deephaven.stream.kafka import consumer as kc
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

from deephaven import agg

agg_list = [agg.sum_("Shares = Qty"), agg.weighted_sum("Qty", "Notional = LastPrice")]

total_notional = orders_with_current_price.agg_by(agg_list, by=["Symbol"])
```

Next, let's add records to the two topics. In a terminal, first run the following command to start writing to
the `quotes` topic:

```shell
docker compose exec redpanda rpk topic produce quotes -f '%k %v\n'
```

and add the following entries:

```
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

```
{"Symbol": "AAPL", "Id":"o1", "LimitPrice": 136, "Qty": 7}
{"Symbol": "AAPL", "Id":"o2", "LimitPrice": 132, "Qty": 2}
{"Symbol": "TSLA", "Id":"o3", "LimitPrice": 725, "Qty": 1}
{"Symbol": "TSLA", "Id":"o4", "LimitPrice": 730, "Qty": 9}
```

The tables will update as each entry is added to the Kafka streams. The final results are in the `total_notional` table.

### Consume a Kafka stream into a partitioned table

[Partitioned tables](../partitioned-tables.md) are Deephaven tables that are partitioned into subtables by one or more key columns. They have special semantics and operations, and are particularly useful when working with big data. Deephaven enables you to consume directly from Kafka into a partitioned table via [`consume_to_partitioned_table`](../../reference/data-import-export/Kafka/consume-to-partitioned-table.md). When doing so, the resulting partitioned table is always partitioned by the Kafka partition. You may choose to pick which partitions to read from, or read from all partitions by default. The syntax is very similar to [`consume`](../../reference/data-import-export/Kafka/consume.md).

```python docker-config=kafka test-set=2 order=null
from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht

result_append = kc.consume_to_partitioned_table(
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

Deephaven can write tables to Kafka streams as well. When data in a table changes with real-time updates, those changes are also written to Kafka. The [Kafka producer module](/core/pydoc/code/deephaven.stream.kafka.producer.html#module-deephaven.stream.kafka.producer) defines methods to do this.

In this example, we write a simple [time table](../../reference/table-operations/create/timeTable.md) to a topic called `time-topic`. With only one data point, we use the `X` as a key and ignore the value.

```python docker-config=kafka test-set=1
from deephaven import time_table
from deephaven import kafka_producer as pk
from deephaven.stream.kafka.producer import KeyValueSpec

source = time_table("PT00:00:00.1").update(formulas=["X = i"])

write_topic = pk.produce(
    source,
    {"bootstrap.servers": "redpanda:9092"},
    "time-topic",
    pk.simple_spec("X"),
    KeyValueSpec.IGNORE,
)
```

Now we write a [time table](../../reference/table-operations/create/timeTable.md) to a topic called `time-topic_group`. The last argument is `True` for `last_by_key_columns`, which indicates we want to perform a [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) on the keys before writing to the stream.

```python test-set=1
import random

source_group = time_table("PT00:00:00.1").update(
    formulas=["X = random.randint(1, 5)", "Y = i"]
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

- [Kafka basic terminology](../../conceptual/kafka-in-deephaven.md)
- [Kafka in Deephaven](../../conceptual/kafka-in-deephaven.md)
- [Custom parser for Kafka](./write-your-own-custom-parser-for-kafka.md)
- [`consume`](../../reference/data-import-export/Kafka/consume.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [Kafka Pydoc](/core/pydoc/code/deephaven.stream.kafka.html)
