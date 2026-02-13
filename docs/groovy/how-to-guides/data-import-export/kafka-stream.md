---
title: Connect to a Kafka stream
---

Kafka is a distributed event streaming platform that lets you read, write, store, and process events, also called records.

Kafka topics take on many forms, such as raw input, [JSON](#read-kafka-topic-in-json-format), [AVRO](#read-kafka-topic-in-avro-format), or [Protobuf](#read-kafka-topic-in-protobuf-format) In this guide, we show you how to read each of these formats into Deephaven tables.

Please see our overview, [Kafka in Deephaven: Basic terms](../../conceptual/kafka-basic-terms.md), for a detailed discussion of Kafka topics and supported formats. See the [Apache Kafka Documentation](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) for full details on how to use Kafka.

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

```groovy skip-test
...
// Snippet of Groovy consumer with the Partition column suppressed.

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('schema.registry.url', 'http://redpanda:8081')
kafkaProps.put('deephaven.partition.column.name':'')
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

- [simple type](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String))
- [JSON encoded](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition%5B%5D))
- [Avro encoded](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(org.apache.avro.Schema))
- [Protbuf encoded](https://docs.deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions))
- ignored (cannot ignore both key and value)

## Table types

Deephaven Kafka tables can be append-only, blink, or ring.

- [Append-only](../../conceptual/table-types.md#specialization-1-append-only) tables keep every row. Table size (and thus, memory consumption) can grow indefinitely. Set this value with `table_type = TableType.append()`.
- [Blink](../../conceptual/table-types.md#specialization-3-blink) tables only keep the set of rows from the current update cycle. This forms the basis for more advanced use cases when combined with stateful table aggregations like [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md). For blink tables without any downstream table operations, aggregations or listeners, the new messages will appear as rows in the table for one update cycle, then disappear on the next update cycle. You can set this value to `table_type = TableType.blink()` to be explicit, but this is not required, as this is the default.
- [Ring](../../conceptual/table-types.md#specialization-4-ring) tables keep only the last `N` rows. When the table grows beyond `N` rows, the oldest are discarded until `N` remain. Set this value with `table_type = TableType.ring(N)`.

## Launching Kafka with Deephaven

Deephaven has an official [docker-compose file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples-redpanda/docker-compose.yml) that contains the Deephaven images along with images from [Redpanda](https://github.com/redpanda-data/redpanda). Redpanda allows us to input data directly into a Kafka stream from the terminal. This is just one of the supported Kafka-compatible event streaming platforms. Many more are available.

> [!IMPORTANT]
> The `docker-compose.yml` file linked above uses Deephaven's Python server image. To change this to Groovy, change `server` to `server-slim` on line 5 of the file.

Save this locally as a `docker-compose.yml` file, and launch with `docker compose up`.

## Consume a Kafka stream

In this example, we consume a Kafka topic (`test.topic`) as a Deephaven table. The Kafka topic is populated by commands entered into the terminal.

For demonstration purposes, we will be using an append-only table and ignoring the Kafka key.

```groovy docker-config=kafka test-set=2 order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

resultAppend = KafkaTools.consumeToTable(
    kafkaProps,
    'test.topic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec('Command', java.lang.String),
    KafkaTools.TableType.append()
)
```

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) creates a Deephaven table from a Kafka topic. Here, `{'bootstrap.servers': 'redpanda:9092'}` is a dictionary describing how the Kafka infrastructure is configured. `bootstrap.servers` provides the initial hosts that a Kafka client uses to connect. In this case, `bootstrap.servers` is set to `redpanda:9092`.

`table_type` is set to `kc.TableType.append()` to create an append-only table, and `key_spec` is set to `kc.KeyValueSpec.IGNORE` to ignore the Kafka key.

The `result` table is now subscribed to all partitions in the `test.topic` topic. When data is sent to the `test.topic` topic, it will appear in the table.

### Input Kafka data for testing

For this example, information is entered into the Kafka topic via a terminal. To do this, run:

```sh
docker compose exec redpanda rpk topic produce test.topic
```

This will wait for and send any input to the `test.topic` topic. Enter the information and use the keyboard shortcut **Ctrl + D** to send.

Once sent, that information will automatically appear in your Deephaven table.

<LoopedVideo src='../../assets/how-to/kafka1.mp4' />

### Ring and blink tables

The following example shows how to create [ring and blink tables](#table-types) to read from the `test.topic` topic:

```groovy docker-config=kafka test-set=2 order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

resultRing = KafkaTools.consumeToTable(
    kafkaProps,
    'test.topic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec('Command', java.lang.String),
    KafkaTools.TableType.ring(3)
)

resultBlink = KafkaTools.consumeToTable(
    kafkaProps,
    'test.topic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec('Command', java.lang.String),
    KafkaTools.TableType.blink()
)
```

Let's run a few more `docker compose exec redpanda rpk topic produce test.topic` commands to input additional data into the Kafka stream. As you can see, the `resultAppend` table contains all of the data, the `resultRing` table contains the last three entries, and the `resultBlink` table only shows rows before the next table update cycle is executed.

Since the `resultBlink` table doesn't show the values in the topic, let's add a table to store the last value added to the `resultBlink` table.

```groovy docker-config=kafka test-set=2 order=null
lastBlink = resultBlink.lastBy()
```

## Read a Kafka stream with append

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price`. The specific key and value result in a table that appends new rows.

```groovy docker-config=kafka order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

resultRing = KafkaTools.consumeToTable(
    kafkaProps,
    'share.price',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.simpleSpec('Symbol', java.lang.String),
    KafkaTools.Consume.simpleSpec('Price', double),
    KafkaTools.TableType.append()
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

## Read a Kafka stream ignoring keys

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price` and ignores the partition and key values.

Run the same `docker compose exec redpanda rpk topic produce share.price -f '%k %v\n'` command from the previous section and enter the sample key-value pairs.

```groovy docker-config=kafka order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

resultAppend = KafkaTools.consumeToTable(
    kafkaProps,
    'share.price',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec('Price', double),
    KafkaTools.TableType.append()
)
```

As you can see, the key column is not included in the output table.

## Read Kafka topic in JSON format

The following two examples read the Kafka topic `orders` in JSON format.

This example uses [`jsonSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition[])):

```groovy docker-config=kafka order=null
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

symbolDef = ColumnDefinition.ofString('Symbol')
priceDef = ColumnDefinition.ofDouble('Price')
qtyDef = ColumnDefinition.ofInt('Qty')

ColumnDefinition[] colDefs = [symbolDef, priceDef, qtyDef]
mapping = ['symbol': 'Symbol', 'price': 'Price', 'qty': 'Qty']

spec = KafkaTools.Consume.jsonSpec(colDefs, mapping, null)

result = KafkaTools.consumeToTable(
    kafkaProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    spec,
    KafkaTools.TableType.append()
)
```

This example uses [`objectProcessorSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(org.apache.kafka.common.serialization.Deserializer,io.deephaven.processor.NamedObjectProcessor)) with a [Jackson provider](/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html).

```groovy docker-config=kafka order=null
import io.deephaven.json.jackson.JacksonProvider
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools
import io.deephaven.json.jackson.JacksonProvider
import io.deephaven.json.ObjectValue
import io.deephaven.json.StringValue
import io.deephaven.json.DoubleValue
import io.deephaven.json.IntValue

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

fields = ObjectValue.builder()
    .putFields('symbol', StringValue.strict())
    .putFields('price', DoubleValue.strict())
    .putFields('qty', IntValue.strict())
    .build()

provider = JacksonProvider.of(fields)

jacksonSpec = KafkaTools.Consume.objectProcessorSpec(provider)

result = KafkaTools.consumeToTable(
    kafkaProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    jacksonSpec,
    KafkaTools.TableType.append()
)
```

Run `docker compose exec redpanda rpk topic produce orders -f "%v\n"` in your terminal and enter the following values:

```
{"symbol": "AAPL", "price": 135, "qty": 5}
{"symbol": "TSLA", "price": 730, "qty": 3}
```

In this query, the `valueSpec` argument uses [`jsonSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition[])). A JSON parameterization is used for the `KafkaValue` field.

After this, we see an ordered list of Groovy strings specifying column definitions.

- The first element in each is a string for the column name in the result table.
- The second element in each is a string for the column data type in the result table.

Within the `valueSpec` argument, the keyword argument of `mapping` is given. This is a dictionary specifying a mapping from JSON field names to resulting table column names. Column names should be in the list provided in the first argument described above. The `mapping` dictionary may contain fewer entries than the total number of columns defined in the first argument.

In the example, the map entry `'price' : 'Price'` specifies the incoming messages are expected to contain a JSON field named `price`, whose value will be mapped to the `Price` column in the resulting table. The columns not mentioned are mapped from matching JSON fields.

If the `mapping` keyword argument is not provided, it is assumed that JSON field names and column names will match.

## Read Kafka topic in Avro format

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price` in [Avro](https://avro.apache.org/) format. This example uses an external schema definition registered in the deployment testing [Redpanda](https://www.redpanda.com/) instance that can be seen below.

A [Kafka Schema Registry](https://medium.com/slalom-technology/introduction-to-schema-registry-in-kafka-915ccf06b902) allows sharing and versioning of Kafka event schema definitions.

```groovy skip-test
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('schema.registry.url', 'http://redpanda:8081')

result = KafkaTools.consumeToTable(
    kafkaProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.avroSpec('share.price.record', '1'),
    KafkaTools.TableType.append()
)
```

In this query, the first argument included an additional entry for `schema.registry.url` to specify the URL for a schema registry with a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The `valueSpec` argument uses [`avroSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(org.apache.avro.Schema)), which specifies an Avro format for the Kafka `value` field.

The first positional argument in the [`avroSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(org.apache.avro.Schema)) call specifies the Avro schema to use. In this case, [`avroSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(org.apache.avro.Schema)) gets the schema named `share.price.record` from the schema registry. Alternatively, the first argument can be an `org.apache.avro.Schema` object obtained from [`getAvroSchema`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#getAvroSchema(java.lang.String)).

Three optional keyword arguments are supported:

- `schema_version` specifies the version of the schema to get, for the given name, from the schema registry. If not specified, the default of `latest` is assumed. This will retrieve the latest available schema version.
- `mapping` expects a dictionary value, and if provided, specifies a name mapping for Avro field names to table column names. Any Avro field name not mentioned is mapped to a column of the same name.
- `mapping_only` expects a dictionary value, and if provided, specifies a name mapping for Avro field names to table column names. Any Avro field name not mentioned is omitted from the resulting table.
- When `mapping` and `mapping_only` are both omitted, all Avro schema fields are mapped to columns using the field name as column name.

## Read Kafka topic in Protobuf format

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price` in [protobuf](https://protobuf.dev/) format, Google’s open-source, language-neutral, cross-platform data format used to serialize structured data.

This example uses an external schema definition registered in the development testing [Redpanda](https://www.redpanda.com/) instance that can be seen below.

```groovy skip-test
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions
import io.deephaven.kafka.protobuf.DescriptorSchemaRegistry
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('schema.registry.url', 'http://redpanda:8081')

protoOpts = ProtobufConsumeOptions.builder().
    descriptorProvider(DescriptorSchemaRegistry.builder().
        subject('share.price.record').
        version(1).
        build()
    ).
build()

result = KafkaTools.consumeToTable(
    kafkaProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.protobufSpec(protoOpts),
    KafkaTools.TableType.append()
)
```

In this query, the first argument includes an additional entry for `schema.registry.url` to specify the URL for a schema registry with a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The first positional argument in the [`protobufSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions)) call specifies the Protobuf schema to use. In this case, [`protobufSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions)) gets the schema named `share.price.record` from the schema registry. Alternatively, this could be the fully-qualified Java class name for the protobuf message on the current classpath, for example “com.example.MyMessage” or “com.example.OuterClass$MyMessage”.

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

## Perform multiple operations

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads two Kafka topics, `quotes` and `orders`, into Deephaven as blink tables. Table operations are used to track the latest data from each topic (using [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md)), join the streams together ([`naturalJoin`](../../reference/table-operations/join/natural-join.md)), and aggregate ([`AggSum`](../../reference/table-operations/group-and-aggregate/AggSum.md)) the results.

```groovy docker-config=kafka order=null
import static io.deephaven.api.agg.Aggregation.AggWSum
import static io.deephaven.api.agg.Aggregation.AggSum
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

// Define only the Price column for the price table's value spec
// Symbol will come from the key spec
priceDef = ColumnDefinition.ofDouble('Price')
ColumnDefinition[] priceTableDefs = [priceDef]

// Create JSON spec with column definitions (only Price)
priceSpec = KafkaTools.Consume.jsonSpec(priceTableDefs)

priceProps = new Properties()
priceProps.put('bootstrap.servers', 'redpanda:9092')
// Explicitly set the key deserializer type
priceProps.put('deephaven.key.column.name', 'Symbol')
priceProps.put('deephaven.key.column.type', 'String')
priceProps.put('key.deserializer', 'org.apache.kafka.common.serialization.StringDeserializer')

// Create a key spec that uses String deserializer for the Symbol column
keySpec = KafkaTools.Consume.simpleSpec('Symbol', java.lang.String)

priceTable = KafkaTools.consumeToTable(
    priceProps,
    'quotes',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    keySpec,  // Use explicit key spec instead of FROM_PROPERTIES
    priceSpec,
    KafkaTools.TableType.blink()
)

lastPrice = priceTable.lastBy('Symbol')

// Define columns for the orders table
orderSymbolDef = ColumnDefinition.ofString('Symbol')
idDef = ColumnDefinition.ofString('Id')
limitPriceDef = ColumnDefinition.ofDouble('LimitPrice')
qtyDef = ColumnDefinition.ofInt('Qty')
ColumnDefinition[] orderTableDefs = [orderSymbolDef, idDef, limitPriceDef, qtyDef]
orderSpec = KafkaTools.Consume.jsonSpec(orderTableDefs)

orderProps = new Properties()
orderProps.put('bootstrap.servers', 'redpanda:9092')
orderProps.put('key.deserializer', 'org.apache.kafka.common.serialization.StringDeserializer')

// For orders, we'll use IGNORE for the key since we don't need it for joining
ordersBlink = KafkaTools.consumeToTable(
    orderProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,  // No key needed for orders
    orderSpec,
    KafkaTools.TableType.blink()
)

ordersWithCurrentPrice = ordersBlink.lastBy('Id').naturalJoin(lastPrice, 'Symbol', 'LastPrice = Price')

aggList = [
    AggSum('Shares = Qty'),
    AggWSum('Qty', 'Notional = LastPrice')
]

totalNotional = ordersWithCurrentPrice.aggBy(aggList, 'Symbol')
```

Next, let's add records to the two topics. In a terminal, first run the following command to start writing to the `quotes` topic:

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

After submitting these entries to the `quotes` topic, use the following command to write to the `orders` topic:

```shell
docker compose exec redpanda rpk topic produce orders -f "%v\n"
```

Then add the following entries in the terminal:

```shell
{"Symbol": "AAPL", "Id":"o1", "LimitPrice": 136, "Qty": 7}
{"Symbol": "AAPL", "Id":"o2", "LimitPrice": 132, "Qty": 2}
{"Symbol": "TSLA", "Id":"o3", "LimitPrice": 725, "Qty": 1}
{"Symbol": "TSLA", "Id":"o4", "LimitPrice": 730, "Qty": 9}
```

The tables will update as each entry is added to the Kafka streams. The final results are in the `totalNotional` table.

### Consume a Kafka stream into a partitioned table

[Partitioned tables](../partitioned-tables.md) are Deephaven tables that are partitioned into subtables by one or more key columns. They have special semantics and operations, and are particularly useful when working with big data. Deephaven enables you to consume directly from Kafka into a partitioned table via [`consumeToPartitionedTable`](../../reference/data-import-export/Kafka/consumeToPartitionedTable.md). When doing so, the resulting partitioned table is always partitioned by the Kafka partition. You may choose to pick which partitions to read from, or read from all partitions by default. The syntax is very similar to [`consume`](../../reference/data-import-export/Kafka/consumeToTable.md).

```groovy docker-config=kafka test-set=2 order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

resultAppend = KafkaTools.consumeToPartitionedTable(
    kafkaProps,
    'test.topic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec('Command', java.lang.String),
    KafkaTools.TableType.append()
)
```

### Custom Kafka parser

Some use cases call for custom parsing of Kafka streams, such as when payloads use non-standard encodings or need complex transformation.

See the dedicated guide, [Write your own custom parser for Kafka](./write-your-own-custom-parser-for-kafka.md), for a step-by-step walkthrough and complete examples.

## Write to a Kafka stream

Deephaven can write tables to Kafka streams as well. When data in a table changes with real-time updates, those changes are also written to Kafka. The [Kafka producer module](/core/pydoc/code/deephaven.stream.kafka.producer.html#module-deephaven.stream.kafka.producer) defines methods to do this.

In this example, we write a simple [time table](../../reference/table-operations/create/timeTable.md) to a topic called `time-topic`. With only one data point, we use the `X` as a key and ignore the value.

```groovy docker-config=kafka test-set=1
import io.deephaven.kafka.KafkaPublishOptions
import io.deephaven.kafka.KafkaTools

source = timeTable('PT00:00:00.1').update('X = i')

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

options = KafkaPublishOptions.
    builder().
    table(source).
    topic('time-topic').
    config(kafkaProps).
    keySpec(KafkaTools.Produce.simpleSpec('X')).
    valueSpec(KafkaTools.Produce.IGNORE).
    build()

runnable = KafkaTools.produceFromTable(options)
```

Now we write a [time table](../../reference/table-operations/create/timeTable.md) to a topic called `time-topic_group`. The last argument is `True` for `last_by_key_columns`, which indicates we want to perform a [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) on the keys before writing to the stream.

```groovy test-set=1
import io.deephaven.kafka.KafkaPublishOptions
import io.deephaven.kafka.KafkaTools

sourceGroup = timeTable('PT00:00:00.1')
    .update('X = randomInt(1, 5)', 'Y = i')

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

options = KafkaPublishOptions.
    builder().
    table(sourceGroup).
    topic('time-topic_group').
    config(kafkaProps).
    keySpec(KafkaTools.Produce.jsonSpec(['X'] as String[], null, null)).
    valueSpec(KafkaTools.Produce.jsonSpec(['X', 'Y'] as String[], null, null)).
    lastBy(true).
    build()

runnable = KafkaTools.produceFromTable(options)
```

## Related documentation

- [Kafka basic terminology](../../conceptual/kafka-basic-terms.md)
- [Custom parser for Kafka](./write-your-own-custom-parser-for-kafka.md)
- [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md)
