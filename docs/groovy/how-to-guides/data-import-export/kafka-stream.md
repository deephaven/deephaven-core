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

When a single-column key spec doesn't name its column, the name comes from the `deephaven.key.column.name` consumer property, an entry in the `Properties` object passed as the first argument to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md). If that property isn't set, the name defaults to `KafkaKey`. Value specs work the same way with `deephaven.value.column.name` and `KafkaValue`.

Deephaven chooses the column type in this order:

1. The type set in the spec, if there is one.
2. The `deephaven.key.column.type` or `deephaven.value.column.type` property. It accepts `short`, `int`, `long`, `float`, `double`, `byte[]`, or `String` (also accepted as `string`).
3. The Kafka deserializer set in the `key.deserializer` or `value.deserializer` consumer property. Deephaven recognizes the numeric, byte-array, `UUID`, `ByteBuffer`, and `Bytes` deserializers.

For a `String` column, set the type property or pass the type to the spec.

The key and the value can each be read as:

- [simple type](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String))
- [JSON encoded](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition%5B%5D))
- [Avro encoded](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(org.apache.avro.Schema))
- [Protobuf encoded](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions))
- [parsed by an object processor](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(io.deephaven.processor.NamedObjectProcessor.Provider)), such as a Jackson JSON provider
- [read with a custom Kafka deserializer](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#rawSpec(io.deephaven.qst.column.header.ColumnHeader,java.lang.Class))
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

Consumer properties control these columns. These are entries in the `Properties` object passed as the first argument to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md). To add an optional column, set its property to the column name you want. To disable a column that is present by default, set its property to an empty string. The property doesn't accept a null value. For example, you might disable the partition column when the topic has only one partition.

```groovy skip-test
...
// Snippet of Groovy consumer with the Partition column suppressed.

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('deephaven.partition.column.name', '')
...
```

## Table types

Deephaven Kafka tables can be append-only, blink, or ring. Pass the table type as the last argument to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md).

- [Append-only](../../conceptual/table-types.md#specialization-1-append-only) tables keep every row. The table and its memory use can grow without limit. To use this type, pass `KafkaTools.TableType.append()`.
- [Blink](../../conceptual/table-types.md#specialization-3-blink) tables keep only the rows from the current [update cycle](../../conceptual/table-update-model.md). Each new message appears as a row for one update cycle and then disappears. To use this type, pass `KafkaTools.TableType.blink()`.
- [Ring](../../conceptual/table-types.md#specialization-4-ring) tables keep only the last `N` rows. When the table grows beyond `N` rows, it discards the oldest rows until `N` remain. To use this type, pass `KafkaTools.TableType.ring(N)`.

Combine a blink table with a stateful aggregation such as [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) to keep results after the rows disappear.

## Launching Kafka with Deephaven

Deephaven has an official [Docker Compose file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy-examples-redpanda/docker-compose.yml) that contains the Deephaven images along with a [Redpanda](https://github.com/redpanda-data/redpanda) image. Redpanda lets you input data directly into a Kafka stream from the terminal. Redpanda is one of many Kafka-compatible event streaming platforms that work with Deephaven.

Save this locally as a `docker-compose.yml` file, and launch with `docker compose up`.

## Consume a Kafka stream

In this example, we consume a Kafka topic (`test.topic`) as a Deephaven table. You populate the Kafka topic by entering commands in the terminal.

For demonstration purposes, we use an [append-only](../../conceptual/table-types.md#specialization-1-append-only) table and ignore the Kafka key.

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

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) creates a Deephaven table from a Kafka topic. Here, `kafkaProps` is a `Properties` object that describes how to connect to the Kafka infrastructure. `bootstrap.servers` provides the initial hosts that a Kafka client uses to connect. In this case, `bootstrap.servers` is set to `redpanda:9092`.

The third and fourth arguments, `KafkaTools.ALL_PARTITIONS` and `KafkaTools.ALL_PARTITIONS_DONT_SEEK`, read every partition and start where Kafka's consumer settings say. [Read the key and choose partitions and offsets](#read-the-key-and-choose-partitions-and-offsets) covers other choices. The key spec, `KafkaTools.Consume.IGNORE`, ignores the Kafka key. The value spec, [`simpleSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String)) `('Command', java.lang.String)`, reads each record's value into a `String` column named `Command`. The table type, `KafkaTools.TableType.append()`, creates an append-only table.

The `resultAppend` table is now subscribed to all partitions in the `test.topic` topic. When you send data to the `test.topic` topic, it appears in the table.

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

Let's run a few more `docker compose exec redpanda rpk topic produce test.topic` commands to input additional data into the Kafka stream. As you can see, the `resultAppend` table contains all the data, the `resultRing` table contains the last three entries, and the `resultBlink` table shows each new row for only one update cycle.

Because rows disappear from the `resultBlink` table after one update cycle, let's add a table that uses [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) to keep the last row added to the `resultBlink` table.

```groovy docker-config=kafka test-set=2 order=null
lastBlink = resultBlink.lastBy()
```

### Read the key and choose partitions and offsets

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price` into an append-only table. Unlike the previous examples, it reads both the key and the value.

When reading a Kafka topic, you can select which partitions to listen to. You can also choose where reading starts in each partition: at the beginning, at the end, at a specific offset, or without seeking. The bullets after the example describe the values it uses.

> [!NOTE]
> Starting at the beginning of a partition reads only the messages Kafka still retains. Kafka can be configured to keep messages up to a maximum age, or to keep only the last message for each key.

```groovy docker-config=kafka order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

result = KafkaTools.consumeToTable(
    kafkaProps,
    'share.price',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.simpleSpec('Symbol', java.lang.String),
    KafkaTools.Consume.simpleSpec('Price', java.lang.String),
    KafkaTools.TableType.append()
)
```

Let's walk through the arguments in this query.

- The partition filter is `KafkaTools.ALL_PARTITIONS`, which specifies that we want to listen to all partitions.
- The initial offset is `KafkaTools.ALL_PARTITIONS_DONT_SEEK`, which doesn't seek. If the consumer's [consumer group](https://kafka.apache.org/documentation/#intro_consumers), set with the `group.id` property, has a committed offset, the consumer starts there. Otherwise, Kafka's `auto.offset.reset` property decides where to start. Its default, `latest`, reads only new messages.
- The key spec is [`simpleSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String)) `('Symbol', java.lang.String)`, which instructs the consumer to expect messages with a Kafka `key` field, and creates a `Symbol` column of type `String` to store the information.
- The value spec is `simpleSpec('Price', java.lang.String)`, which instructs the consumer to expect messages with a Kafka `value` field, and creates a `Price` column of type `String` to store the information. Deephaven reads the price as a string because Redpanda's command-line tool, `rpk`, sends it as text. A numeric type such as `double` would use Kafka's binary deserializer, which expects binary values.
- The table type is `KafkaTools.TableType.append()`, which creates an append-only table.

To choose a different starting point, pass one of these values as the initial offset:

- `KafkaTools.ALL_PARTITIONS_SEEK_TO_END` always starts with new messages only.
- `KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING` starts at the beginning of every partition.
- `KafkaTools.partitionToOffsetFromParallelArrays(new int[]{0, 1}, new long[]{100, 250})` starts at the given offset in each listed partition. Partitions that aren't listed don't seek.

To listen to specific partitions, use `KafkaTools.partitionFilterFromArray(new int[]{1, 3, 5})`.

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

To drop the `Symbol` column, pass `KafkaTools.Consume.IGNORE` as the key spec instead, as in the first example.

### Read Kafka topic in JSON format

The following two examples read a Kafka topic called `orders` in JSON format.

This example uses [`jsonSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition%5B%5D)):

```groovy docker-config=kafka order=null
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

symbolDef = ColumnDefinition.ofString('Symbol')
priceDef = ColumnDefinition.ofDouble('Price')
qtyDef = ColumnDefinition.ofLong('Qty')

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

Here, the value spec uses `jsonSpec`, which parses each record's value as JSON.

The first argument to `jsonSpec` is an array of [`ColumnDefinition`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) objects that gives each column's name and type in the result table.

The second argument, `mapping`, is a `Map` from JSON field names to table column names. Each column name must appear in the array from the first argument. The map may contain fewer entries than the total number of columns defined in the first argument.

In the example, the map entry `'price': 'Price'` reads the JSON field `price` into the `Price` column of the result table. For a column that the map doesn't mention, Deephaven reads the JSON field with the same name as the column.

If you omit the `mapping` argument, Deephaven assumes that JSON field names match column names.

The third argument is a custom Jackson `ObjectMapper`. Pass `null` to use the default.

This example uses [`objectProcessorSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(io.deephaven.processor.NamedObjectProcessor.Provider)) with a [Jackson provider](/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html):

```groovy docker-config=kafka order=null
import io.deephaven.kafka.KafkaTools
import io.deephaven.json.jackson.JacksonProvider
import io.deephaven.json.ObjectValue
import io.deephaven.json.StringValue
import io.deephaven.json.DoubleValue
import io.deephaven.json.LongValue

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

fields = ObjectValue.builder()
    .putFields('symbol', StringValue.standard())
    .putFields('price', DoubleValue.standard())
    .putFields('qty', LongValue.standard())
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
).renameColumns('Symbol = symbol', 'Price = price', 'Qty = qty')
```

Here, `objectProcessorSpec` takes a Jackson provider built from an [`ObjectValue`](https://deephaven.io/core/javadoc/io/deephaven/json/ObjectValue.html) that names each JSON field and its type. The resulting columns take the JSON field names, so the example renames them with [`renameColumns`](../../reference/table-operations/select/rename-columns.md).

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

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price` in [Avro](https://avro.apache.org/) format. This example assumes that a schema named `share.price.record` is registered in the schema registry of the [Redpanda](https://www.redpanda.com/) instance from the [Docker Compose file](#launching-kafka-with-deephaven).

A [schema registry](https://medium.com/slalom-technology/introduction-to-schema-registry-in-kafka-915ccf06b902) stores Kafka event schema definitions and tracks their versions so that producers and consumers can share them. To register a schema, see Redpanda's [schema registry documentation](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/).

```groovy skip-test
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('schema.registry.url', 'http://redpanda:8081')

result = KafkaTools.consumeToTable(
    kafkaProps,
    'share.price',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.avroSpec('share.price.record', '1'),
    KafkaTools.TableType.append()
)
```

In this query, the first argument includes an additional entry for `schema.registry.url` to specify the URL for a schema registry with a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The value spec uses [`avroSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(java.lang.String,java.lang.String)), which specifies an Avro format for the Kafka `value` field.

The first positional argument in the `avroSpec` call specifies the Avro schema to use. In this case, `avroSpec` gets the schema named `share.price.record` from the schema registry. Alternatively, the first argument can be an `org.apache.avro.Schema` object obtained from [`getAvroSchema`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#getAvroSchema(java.lang.String)).

When the schema comes from the schema registry, `avroSpec` overloads include:

- `avroSpec(schemaName)` fetches the latest version of the schema.
- `avroSpec(schemaName, schemaVersion)` fetches the given version of the schema, as in the example above.
- `avroSpec(schemaName, schemaVersion, fieldNameToColumnName)` also takes a `Function<String, String>` that maps each Avro field name to a column name. The resulting table omits any field that the function maps to `null`.

Without a `fieldNameToColumnName` function, Deephaven maps each Avro schema field to a column with the same name.

### Read Kafka topic in Protobuf format

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price` in [Protobuf](https://protobuf.dev/) format. Protobuf is Google's open-source, language-neutral format for serializing structured data.

This example assumes that a schema with the subject name `share.price.record` is registered in the [schema registry](#read-kafka-topic-in-avro-format) of the [Redpanda](https://www.redpanda.com/) instance from the [Docker Compose file](#launching-kafka-with-deephaven). A schema registry stores each schema under a _subject_ name and tracks the subject's versions. To register a schema, see Redpanda's [schema registry documentation](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/).

```groovy skip-test
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
    'share.price',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.protobufSpec(protoOpts),
    KafkaTools.TableType.append()
)
```

In this query, the first argument includes an additional entry for `schema.registry.url` to specify the URL for a schema registry with a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The only argument to [`protobufSpec`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions)) is a [`ProtobufConsumeOptions`](https://deephaven.io/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.html) object, created with `ProtobufConsumeOptions.builder`. The builder methods include:

- `descriptorProvider` sets where the Protobuf message descriptor comes from. It is required.
  - `DescriptorSchemaRegistry.builder` fetches the descriptor from the schema registry, as in the example above. Its builder methods are:
    - `subject`: the schema subject name, `share.price.record` in the example.
    - `version`: the schema version. When not set, Deephaven fetches the latest version.
    - `messageName`: the fully qualified Protobuf message name, for example `com.example.MyMessage`. When not set, Deephaven uses the first message descriptor in the schema.

    We recommend setting `version` and `messageName` so that the resulting table definition does not change across restarts.
  - `DescriptorMessageClass.of(MyMessage.class)` reads the descriptor from a Protobuf message class on the current classpath and does not contact the schema registry.
- `parserOptions` takes a [`ProtobufDescriptorParserOptions`](https://deephaven.io/core/javadoc/io/deephaven/protobuf/ProtobufDescriptorParserOptions.html) object, which controls how the descriptor is parsed, such as which field paths to include.
- `protocol` sets the wire protocol for this payload, as an [`io.deephaven.kafka.protobuf.Protocol`](https://deephaven.io/core/javadoc/io/deephaven/kafka/protobuf/Protocol.html).
  - With `DescriptorSchemaRegistry`, the default is `Protocol.serdes()`.
  - With `DescriptorMessageClass`, the default is `Protocol.raw()`.

### Join and aggregate two Kafka streams

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads two Kafka topics, `quotes` and `orders`, into Deephaven as blink tables. The example uses [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) to track the latest data from each topic, [`naturalJoin`](../../reference/table-operations/join/natural-join.md) to join the streams, and [`AggSum`](../../reference/table-operations/group-and-aggregate/AggSum.md) and [`AggWSum`](../../reference/table-operations/group-and-aggregate/AggWSum.md) to aggregate the results.

```groovy docker-config=kafka order=null
import static io.deephaven.api.agg.Aggregation.AggWSum
import static io.deephaven.api.agg.Aggregation.AggSum
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

// Define only the Price column for the price table's value spec
// Symbol comes from the key spec
priceDef = ColumnDefinition.ofDouble('Price')
ColumnDefinition[] priceTableDefs = [priceDef]

// Create JSON spec with column definitions (only Price)
priceSpec = KafkaTools.Consume.jsonSpec(priceTableDefs)

priceProps = new Properties()
priceProps.put('bootstrap.servers', 'redpanda:9092')

// Create a key spec that reads the key into a String column named Symbol
keySpec = KafkaTools.Consume.simpleSpec('Symbol', java.lang.String)

priceTable = KafkaTools.consumeToTable(
    priceProps,
    'quotes',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    keySpec,
    priceSpec,
    KafkaTools.TableType.blink()
)

lastPrice = priceTable.lastBy('Symbol')

// Define columns for the orders table
orderSymbolDef = ColumnDefinition.ofString('Symbol')
idDef = ColumnDefinition.ofString('Id')
limitPriceDef = ColumnDefinition.ofDouble('LimitPrice')
qtyDef = ColumnDefinition.ofLong('Qty')
ColumnDefinition[] orderTableDefs = [orderSymbolDef, idDef, limitPriceDef, qtyDef]
orderSpec = KafkaTools.Consume.jsonSpec(orderTableDefs)

orderProps = new Properties()
orderProps.put('bootstrap.servers', 'redpanda:9092')

// The orders key isn't needed for the join, so ignore it
ordersBlink = KafkaTools.consumeToTable(
    orderProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
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

Then add the following entries:

```text
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

```text
{"Symbol": "AAPL", "Id":"o1", "LimitPrice": 136, "Qty": 7}
{"Symbol": "AAPL", "Id":"o2", "LimitPrice": 132, "Qty": 2}
{"Symbol": "TSLA", "Id":"o3", "LimitPrice": 725, "Qty": 1}
{"Symbol": "TSLA", "Id":"o4", "LimitPrice": 730, "Qty": 9}
```

The tables update as you add each entry to the Kafka streams. The final results are in the `totalNotional` table.

### Consume a Kafka stream into a partitioned table

[Partitioned tables](../partitioned-tables.md) are Deephaven tables that are partitioned into subtables by one or more key columns. They have their own operations and are useful when working with large data sets.

To consume from Kafka directly into a partitioned table, use [`consumeToPartitionedTable`](../../reference/data-import-export/Kafka/consumeToPartitionedTable.md). Its syntax is similar to that of [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md). The result is always partitioned by Kafka partition. To read every partition, pass `KafkaTools.ALL_PARTITIONS` as the partition filter, as in the example below. To read specific partitions, pass a filter such as `KafkaTools.partitionFilterFromArray(new int[]{1, 3, 5})`.

```groovy docker-config=kafka test-set=2 order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

resultPartitioned = KafkaTools.consumeToPartitionedTable(
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

Deephaven can write tables to Kafka streams as well. When data in a table changes with real-time updates, Deephaven also writes those changes to Kafka. The [`KafkaTools.Produce`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.Produce.html) class and [`produceFromTable`](../../reference/data-import-export/Kafka/produceFromTable.md) method do this. You describe what to write with a [`KafkaPublishOptions`](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaPublishOptions.html) object.

In this example, we write a simple [time table](../../reference/table-operations/create/timeTable.md) to a topic called `time-topic`. The example writes column `X` as each record's key and ignores the value.

```groovy docker-config=kafka test-set=1 ticking-table order=null
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

To see the records arrive, run `docker compose exec redpanda rpk topic consume time-topic` in a terminal.

Now we write a time table to a topic called `time-topic_group`. The `KafkaPublishOptions` builder's `lastBy(true)` option tells the producer to perform a [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) on the key columns before writing to the stream.

```groovy docker-config=kafka test-set=1 ticking-table order=null
import io.deephaven.kafka.KafkaPublishOptions
import io.deephaven.kafka.KafkaTools

sourceGroup = timeTable('PT00:00:00.1')
    .update('X = randomInt(1, 5)', 'Y = i')

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

optionsGroup = KafkaPublishOptions.
    builder().
    table(sourceGroup).
    topic('time-topic_group').
    config(kafkaProps).
    keySpec(KafkaTools.Produce.jsonSpec(['X'] as String[], null, null)).
    valueSpec(KafkaTools.Produce.jsonSpec(['X', 'Y'] as String[], null, null)).
    lastBy(true).
    build()

runnableGroup = KafkaTools.produceFromTable(optionsGroup)
```

## Related documentation

- [Kafka basic terminology](../../conceptual/kafka-basic-terms.md)
- [Custom parser for Kafka](./write-your-own-custom-parser-for-kafka.md)
- [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md)
- [`consumeToPartitionedTable`](../../reference/data-import-export/Kafka/consumeToPartitionedTable.md)
- [`produceFromTable`](../../reference/data-import-export/Kafka/produceFromTable.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [KafkaTools Javadoc](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html)
