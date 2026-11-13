---
title: Connect to a Kafka stream
---

Kafka is a distributed event streaming platform that lets you read, write, store, and process events, also called records.

Deephaven turns a Kafka topic into a live table that updates as records arrive, so you can [filter](../use-filters.md), [join](../joins-exact-relational.md), and [aggregate](../combined-aggregations.md) a stream with the same table operations you use on static data.

Kafka record keys and values come in many formats, such as [simple types](#consume-a-kafka-stream) like strings and numbers, [JSON](#read-kafka-topic-in-json-format), [Avro](#read-kafka-topic-in-avro-format), or [Protobuf](#read-kafka-topic-in-protobuf-format). In this guide, we show you how to read each of these formats into Deephaven tables, and how to [write a Deephaven table to Kafka](#write-to-a-kafka-stream).

See [A Kafka introduction: basic terms](../../conceptual/kafka-basic-terms.md) for a detailed discussion of Kafka topics and supported formats. See the [Apache Kafka documentation](https://kafka.apache.org/documentation/) for full details on how to use Kafka.

## Launch Kafka with Deephaven

Deephaven has an official [Docker Compose file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/groovy-examples-redpanda/docker-compose.yml) that runs the Deephaven images alongside [Redpanda](https://www.redpanda.com/), one of many Kafka-compatible event streaming platforms that work with Deephaven. Redpanda's command-line tool lets you send records to a Kafka topic from the terminal.

Save the Docker Compose file locally as `docker-compose.yml`, and start it with `docker compose up`. The Deephaven IDE is then at `http://localhost:10000/ide`.

## Key and value

Each Kafka record has a key and a value. Kafka writes each record to a partition at an offset, with a timestamp. For example, a list of Kafka records might have a stock ticker as the key and its price as the value.

The key and the value can each be nearly any sequence of bytes.

A _key spec_ and a _value spec_ tell Deephaven how to turn the key and the value into table columns. You pass them to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) as its fifth and sixth arguments, and you create them with methods of [`KafkaTools.Consume`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html).

The key and the value can each be:

- [read as a simple type](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String,java.lang.Class))
- [decoded from JSON](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition%5B%5D))
- [decoded from Avro](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(org.apache.avro.Schema))
- [decoded from Protobuf](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions))
- [parsed by an object processor](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(io.deephaven.processor.NamedObjectProcessor.Provider)), such as a Jackson JSON provider
- [read with a custom Kafka deserializer](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#rawSpec(io.deephaven.qst.column.header.ColumnHeader,java.lang.Class))
- named and typed from consumer properties, with [`KafkaTools.FROM_PROPERTIES`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#FROM_PROPERTIES), as described in [Name key and value columns with properties](#name-key-and-value-columns-with-properties)
- ignored, with [`KafkaTools.Consume.IGNORE`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#IGNORE)

You can't ignore both the key and the value.

## Table types

Deephaven Kafka tables can be [append-only](../../conceptual/table-types.md#specialization-1-append-only), [blink](../../conceptual/table-types.md#specialization-3-blink), or [ring](../../conceptual/table-types.md#specialization-4-ring). Pass a [`KafkaTools.TableType`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html) as the last argument to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md).

- [Append-only](../../conceptual/table-types.md#specialization-1-append-only) tables keep every row. The table and its memory use can grow without limit. To use this type, pass [`KafkaTools.TableType.append()`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html).
- [Blink](../../conceptual/table-types.md#specialization-3-blink) tables keep only the rows from the current [update cycle](../../conceptual/table-update-model.md). Each new record appears as a row for one [update cycle](../../conceptual/table-update-model.md) and then disappears. To use this type, pass [`KafkaTools.TableType.blink()`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html).
- [Ring](../../conceptual/table-types.md#specialization-4-ring) tables keep only the last `N` rows. When the table grows beyond `N` rows, it discards the oldest rows until `N` remain. To use this type, pass [`KafkaTools.TableType.ring(N)`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html).

To keep results after blink rows disappear, see the [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) example in [Ring and blink tables](#ring-and-blink-tables).

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

[`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) creates a Deephaven table from a Kafka topic. Its arguments do the following:

- The connection properties, `kafkaProps`, describe how to connect to Kafka. `bootstrap.servers` lists the initial hosts that a Kafka client connects to. The value `redpanda:9092` is the address of the Redpanda service from the [Docker Compose file](#launch-kafka-with-deephaven). To connect to your own Kafka cluster, replace `redpanda:9092` with your brokers' `host:port` list.
- The partition filter, [`KafkaTools.ALL_PARTITIONS`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS), reads every partition.
- The initial offset, [`KafkaTools.ALL_PARTITIONS_DONT_SEEK`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS_DONT_SEEK), starts where Kafka's consumer settings say. [Read the key and choose partitions and offsets](#read-the-key-and-choose-partitions-and-offsets) covers other choices.
- The key spec, [`KafkaTools.Consume.IGNORE`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#IGNORE), ignores the Kafka key.
- The value spec uses [`simpleSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String,java.lang.Class)) to read each record's value into a `String` column named `Command`.
- The table type, [`KafkaTools.TableType.append()`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html), creates an [append-only table](../../conceptual/table-types.md#specialization-1-append-only).

The `resultAppend` table now reads from every partition that the `test.topic` topic has when the table is created.

### Input Kafka data for testing

To send data to the `test.topic` topic that the `resultAppend` table consumes, run the following command in a terminal:

```shell
docker compose exec redpanda rpk topic produce test.topic
```

The command waits for input from the terminal and sends each line you enter to the `test.topic` topic as a record. Press **Ctrl + D** when you're done to stop producing.

Each record you send appears in the `resultAppend` table without rerunning the query.

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

Run `docker compose exec redpanda rpk topic produce test.topic` again and enter a few more lines. The `resultAppend` table contains all the records, the `resultRing` table contains the last three, and the `resultBlink` table shows each new row for only one [update cycle](../../conceptual/table-update-model.md).

Because rows disappear from the `resultBlink` table after one [update cycle](../../conceptual/table-update-model.md), let's add a table that uses [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) to keep the last row added to the `resultBlink` table.

```groovy docker-config=kafka test-set=2 order=null
lastBlink = resultBlink.lastBy()
```

### Read the key and choose partitions and offsets

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price` into an [append-only table](../../conceptual/table-types.md#specialization-1-append-only). Unlike the previous examples, it reads both the key and the value.

When reading a Kafka topic, you can choose which partitions to read and where reading starts in each one:

- **Partitions**: You can read all partitions or only some of them.
- **Starting point**: Reading can start at the beginning of each partition, at the end, or at a specific offset. Kafka calls moving the read position _seeking_. If you don't seek, reading starts where Kafka's consumer settings say.

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

- The partition filter is [`KafkaTools.ALL_PARTITIONS`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS), which reads all partitions.
- The initial offset is [`KafkaTools.ALL_PARTITIONS_DONT_SEEK`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS_DONT_SEEK), which doesn't seek. Kafka then picks the starting point:
  - If you set the [`group.id`](https://kafka.apache.org/44/configuration/consumer-configs/#consumerconfigs_group.id) property and that consumer group has a committed offset, reading starts there.
  - Otherwise, Kafka's [`auto.offset.reset`](https://kafka.apache.org/44/configuration/consumer-configs/#consumerconfigs_auto.offset.reset) property decides. Its default, `latest`, reads only new records. This example doesn't set `group.id`, so the table shows only records sent after you create it.
- The key spec uses [`simpleSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String,java.lang.Class)) to read each record's key into a `String` column named `Symbol`. To drop the `Symbol` column, pass [`KafkaTools.Consume.IGNORE`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#IGNORE) as the key spec instead, as in the first example.
- The value spec uses [`simpleSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String,java.lang.Class)) to read each record's value into a `String` column named `Price`. The column is a string because `rpk`, Redpanda's command-line tool, sends `135.60` as text. With `double`, Deephaven would decode the value with Kafka's `DoubleDeserializer`, which expects 8 binary bytes, so text input fails.
- The table type is [`KafkaTools.TableType.append()`](/core/javadoc/io/deephaven/kafka/KafkaTools.TableType.html), which creates an [append-only table](../../conceptual/table-types.md#specialization-1-append-only).

To change which partitions the example reads or where it starts, change the partition filter and the initial offset.

To read specific partitions, pass [`KafkaTools.partitionFilterFromArray(new int[]{1, 3, 5})`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#partitionFilterFromArray(int%5B%5D)) as the partition filter.

To choose a different starting point, pass one of these values as the initial offset:

- [`KafkaTools.ALL_PARTITIONS_SEEK_TO_END`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS_SEEK_TO_END) always starts with new records only.
- [`KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS_SEEK_TO_BEGINNING) starts at the beginning of every partition.
- [`KafkaTools.partitionToOffsetFromParallelArrays(new int[]{0, 1}, new long[]{100, 250})`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#partitionToOffsetFromParallelArrays(int%5B%5D,long%5B%5D)) starts at the given offset in each listed partition. Partitions that aren't listed don't seek.

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

The first example uses [`jsonSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition%5B%5D,java.util.Map,com.fasterxml.jackson.databind.ObjectMapper)):

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

Here, the value spec uses [`jsonSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition%5B%5D,java.util.Map,com.fasterxml.jackson.databind.ObjectMapper)), which parses each record's value as JSON.

The first argument to [`jsonSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#jsonSpec(io.deephaven.engine.table.ColumnDefinition%5B%5D,java.util.Map,com.fasterxml.jackson.databind.ObjectMapper)) is an array of [`ColumnDefinition`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) objects that gives each column's name and type in the result table.

The second argument, `mapping`, is a `Map` from JSON field names to table column names. Each column name must appear in the array from the first argument.

In the example, the entry `'price': 'Price'` reads the JSON field `price` into the `Price` column of the result table. For any column that the map doesn't mention, or for every column when you omit `mapping`, Deephaven reads the JSON field with the same name as the column.

The third argument is a custom Jackson `ObjectMapper`. Pass `null` to use the default.

The second example uses [`objectProcessorSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(io.deephaven.processor.NamedObjectProcessor.Provider)) with a [Jackson provider](/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html):

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

Here, [`objectProcessorSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#objectProcessorSpec(io.deephaven.processor.NamedObjectProcessor.Provider)) takes a [Jackson provider](/core/javadoc/io/deephaven/json/jackson/JacksonProvider.html) built from an [`ObjectValue`](/core/javadoc/io/deephaven/json/ObjectValue.html) that names each JSON field and its type. The resulting columns take the JSON field names, so the example renames them with [`renameColumns`](../../reference/table-operations/select/rename-columns.md).

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

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price.avro` in [Avro](https://avro.apache.org/) format. This example assumes that a schema with the subject name `share.price.avro.record` is registered in the [schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html) of the [Redpanda](https://www.redpanda.com/) instance from the [Docker Compose file](#launch-kafka-with-deephaven).

```groovy skip-test
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('schema.registry.url', 'http://redpanda:8081')

result = KafkaTools.consumeToTable(
    kafkaProps,
    'share.price.avro',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.avroSpec('share.price.avro.record', '1'),
    KafkaTools.TableType.append()
)
```

The `Properties` object in this query also includes a `schema.registry.url` entry. It gives the URL of a schema registry that has a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The value spec uses [`avroSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(java.lang.String,java.lang.String)), which reads each record's value as Avro.

The first positional argument in the [`avroSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(java.lang.String,java.lang.String)) call specifies the Avro schema to use. In this case, [`avroSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(java.lang.String,java.lang.String)) gets the schema named `share.price.avro.record` from the schema registry. Alternatively, the first argument can be an `org.apache.avro.Schema` object obtained from [`getAvroSchema`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#getAvroSchema(java.lang.String)).

When the schema comes from the schema registry, [`avroSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(java.lang.String,java.lang.String)) overloads include:

- [`avroSpec(schemaName)`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(java.lang.String)) fetches the latest version of the schema.
- [`avroSpec(schemaName, schemaVersion)`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(java.lang.String,java.lang.String)) fetches the given version of the schema, as in the example above.
- [`avroSpec(schemaName, schemaVersion, fieldNameToColumnName)`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#avroSpec(java.lang.String,java.lang.String,java.util.function.Function)) also takes a `Function<String, String>` that maps each Avro field path, such as `parent.child` for a nested field, to a column name. The resulting table omits any field that the function maps to `null`. If the function maps a nested record's own path, such as `parent`, to `null`, the table omits every field inside that record.

Without a `fieldNameToColumnName` function, Deephaven maps each top-level Avro field to a column with the same name, and each field of a nested record to a column named with the field path joined by `__`, such as `parent__child`.

### Read Kafka topic in Protobuf format

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads the Kafka topic `share.price.proto` in [Protobuf](https://protobuf.dev/) format. Protobuf is Google's open-source, language-neutral format for serializing structured data.

This example assumes that a schema with the subject name `share.price.proto.record` is registered in the [schema registry](#read-kafka-topic-in-avro-format) of the [Redpanda](https://www.redpanda.com/) instance from the [Docker Compose file](#launch-kafka-with-deephaven). To register a schema, see Redpanda's [schema registry documentation](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/).

```groovy skip-test
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions
import io.deephaven.kafka.protobuf.DescriptorSchemaRegistry
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('schema.registry.url', 'http://redpanda:8081')

protoOpts = ProtobufConsumeOptions.builder().
    descriptorProvider(DescriptorSchemaRegistry.builder().
        subject('share.price.proto.record').
        version(1).
        build()
    ).
    build()

result = KafkaTools.consumeToTable(
    kafkaProps,
    'share.price.proto',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.protobufSpec(protoOpts),
    KafkaTools.TableType.append()
)
```

The `Properties` object in this query also includes a `schema.registry.url` entry. It gives the URL of a schema registry that has a REST API compatible with [Confluent's schema registry specification](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).

The only argument to [`protobufSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#protobufSpec(io.deephaven.kafka.protobuf.ProtobufConsumeOptions)) is a [`ProtobufConsumeOptions`](/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.html) object, created with [`ProtobufConsumeOptions.builder`](/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.html#builder()).

The example builds a [`DescriptorSchemaRegistry`](/core/javadoc/io/deephaven/kafka/protobuf/DescriptorSchemaRegistry.html), which fetches the Protobuf message descriptor from the schema registry. Its builder methods are:

- [`subject`](/core/javadoc/io/deephaven/kafka/protobuf/DescriptorSchemaRegistry.Builder.html#subject(java.lang.String)): the schema subject name, `share.price.proto.record` in the example.
- [`version`](/core/javadoc/io/deephaven/kafka/protobuf/DescriptorSchemaRegistry.Builder.html#version(int)): the schema version. When not set, Deephaven fetches the latest version. We recommend setting it so that the resulting table definition doesn't change across restarts.
- [`messageName`](/core/javadoc/io/deephaven/kafka/protobuf/DescriptorSchemaRegistry.Builder.html#messageName(java.lang.String)): the fully qualified Protobuf message name, for example `com.example.MyMessage`. When not set, Deephaven uses the first message descriptor in the schema. We recommend setting it for the same reason.

[`ProtobufConsumeOptions.builder`](/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.html#builder()) takes that provider and these other options:

- [`descriptorProvider`](/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.Builder.html#descriptorProvider(io.deephaven.kafka.protobuf.DescriptorProvider)) sets where the Protobuf message descriptor comes from, and it is required. The example passes a [`DescriptorSchemaRegistry`](/core/javadoc/io/deephaven/kafka/protobuf/DescriptorSchemaRegistry.html). To read the descriptor from a Protobuf message class on the current classpath instead, pass `DescriptorMessageClass.of(MyMessage.class)`. A [`DescriptorMessageClass`](/core/javadoc/io/deephaven/kafka/protobuf/DescriptorMessageClass.html) doesn't contact the schema registry.
- [`parserOptions`](/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.Builder.html#parserOptions(io.deephaven.protobuf.ProtobufDescriptorParserOptions)) takes a [`ProtobufDescriptorParserOptions`](/core/javadoc/io/deephaven/protobuf/ProtobufDescriptorParserOptions.html) object, which controls how the descriptor is parsed, such as which field paths to include.
- [`protocol`](/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.Builder.html#protocol(io.deephaven.kafka.protobuf.Protocol)) sets the wire protocol of the payload, as an [`io.deephaven.kafka.protobuf.Protocol`](/core/javadoc/io/deephaven/kafka/protobuf/Protocol.html). The default is [`Protocol.serdes()`](/core/javadoc/io/deephaven/kafka/protobuf/Protocol.html#serdes()) with [`DescriptorSchemaRegistry`](/core/javadoc/io/deephaven/kafka/protobuf/DescriptorSchemaRegistry.html) and [`Protocol.raw()`](/core/javadoc/io/deephaven/kafka/protobuf/Protocol.html#raw()) with [`DescriptorMessageClass`](/core/javadoc/io/deephaven/kafka/protobuf/DescriptorMessageClass.html).

[`Protocol.serdes()`](/core/javadoc/io/deephaven/kafka/protobuf/Protocol.html#serdes()) is the Kafka Protobuf serializer/deserializer (serdes) format, which puts a magic byte, a 4-byte schema ID, and message indexes before the Protobuf data. [`Protocol.raw()`](/core/javadoc/io/deephaven/kafka/protobuf/Protocol.html#raw()) is the Protobuf binary encoding with no header.

Each resulting column is named with the field path joined by `_`, such as `foo_bar` for the field `bar` inside `foo`. Set [`pathToColumnName`](/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.Builder.html#pathToColumnName(io.deephaven.kafka.protobuf.ProtobufConsumeOptions.FieldPathToColumnName)) on the builder to change it.

### Name key and value columns with properties

Deephaven can take a key or value column's name and type from consumer properties instead of from the spec:

- [`KafkaTools.FROM_PROPERTIES`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#FROM_PROPERTIES) takes both the column name and the type from properties.
- [`simpleSpec(columnName)`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String)) sets the column name and takes the type from properties.

With [`KafkaTools.FROM_PROPERTIES`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#FROM_PROPERTIES) as the key spec or value spec, Deephaven reads the key or value into a single column. These properties in the `Properties` object passed to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) name it:

- `deephaven.key.column.name` names the key column. The default is `KafkaKey`.
- `deephaven.value.column.name` names the value column. The default is `KafkaValue`.

Deephaven chooses the column type in this order:

1. The class passed to [`simpleSpec(columnName, dataType)`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String,java.lang.Class)), if there is one.
2. The `deephaven.key.column.type` or `deephaven.value.column.type` property. It accepts `short`, `int`, `long`, `float`, `double`, `byte[]`, or `String` (also accepted as `string`).
3. The Kafka deserializer set in the `key.deserializer` or `value.deserializer` consumer property. Deephaven recognizes the numeric, byte-array, `UUID`, `ByteBuffer`, and `Bytes` deserializers.

For a `String` column, set the type property or pass `java.lang.String` to [`simpleSpec`](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html#simpleSpec(java.lang.String,java.lang.Class)).

If none of these sets a type, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) throws an error that names the column.

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

Consumer properties control these columns. Set them in the `Properties` object passed as the first argument to [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md):

- To add an optional column, set its property to the column name you want.
- To disable a column that is present by default, set its property to an empty string. The property doesn't accept a null value.

For example, you might disable the partition column when the topic has only one partition.

```groovy docker-config=kafka order=null
import io.deephaven.kafka.KafkaTools

// Consume test.topic without the KafkaPartition column.
kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('deephaven.partition.column.name', '')

resultNoPartition = KafkaTools.consumeToTable(
    kafkaProps,
    'test.topic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec('Command', java.lang.String),
    KafkaTools.TableType.append()
)
```

The `resultNoPartition` table has `KafkaOffset`, `KafkaTimestamp`, and `Command` columns, and no `KafkaPartition` column.

### Join and aggregate two Kafka streams

In this example, [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md) reads two Kafka topics, `quotes` and `orders`, into Deephaven as [blink tables](../../conceptual/table-types.md#specialization-3-blink). The example uses [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) to track the latest data from each topic, [`naturalJoin`](../../reference/table-operations/join/natural-join.md) to join the streams, and [`AggSum`](../../reference/table-operations/group-and-aggregate/AggSum.md) and [`AggWSum`](../../reference/table-operations/group-and-aggregate/AggWSum.md) to aggregate the results.

The `quotes` consumer passes [`KafkaTools.FROM_PROPERTIES`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#FROM_PROPERTIES) as the key spec, so the `deephaven.key.column.name` and `deephaven.key.column.type` properties read each record's key into a `String` column named `Symbol` (see [Name key and value columns with properties](#name-key-and-value-columns-with-properties)).

```groovy docker-config=kafka order=null
import static io.deephaven.api.agg.Aggregation.AggWSum
import static io.deephaven.api.agg.Aggregation.AggSum
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

// Define only the Price column for the price table's value spec
// Symbol comes from the key, named and typed by consumer properties
priceDef = ColumnDefinition.ofDouble('Price')
ColumnDefinition[] priceTableDefs = [priceDef]

// Create JSON spec with column definitions (only Price)
priceSpec = KafkaTools.Consume.jsonSpec(priceTableDefs)

priceProps = new Properties()
priceProps.put('bootstrap.servers', 'redpanda:9092')
priceProps.put('deephaven.key.column.name', 'Symbol')
priceProps.put('deephaven.key.column.type', 'String')

priceTable = KafkaTools.consumeToTable(
    priceProps,
    'quotes',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.FROM_PROPERTIES,
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

[`consumeToPartitionedTable`](../../reference/data-import-export/Kafka/consumeToPartitionedTable.md) consumes a topic into a [partitioned table](../partitioned-tables.md) with one subtable for each Kafka partition. Its key column, `Partition`, holds the Kafka partition number. See the [partitioned tables guide](../partitioned-tables.md) for the operations you can run on it.

Two things to know about [`consumeToPartitionedTable`](../../reference/data-import-export/Kafka/consumeToPartitionedTable.md):

- It takes the same arguments as [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md).
- To read every partition, pass [`KafkaTools.ALL_PARTITIONS`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#ALL_PARTITIONS) as the partition filter, as in the example below. To read specific partitions, pass a filter such as [`KafkaTools.partitionFilterFromArray(new int[]{1, 3, 5})`](/core/javadoc/io/deephaven/kafka/KafkaTools.html#partitionFilterFromArray(int%5B%5D)).

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

Deephaven can write tables to Kafka streams as well. When data in a table changes with real-time updates, Deephaven also writes those changes to Kafka. The [`KafkaTools.Produce`](/core/javadoc/io/deephaven/kafka/KafkaTools.Produce.html) class and [`produceFromTable`](../../reference/data-import-export/Kafka/produceFromTable.md) method do this. You describe what to write with a [`KafkaPublishOptions`](/core/javadoc/io/deephaven/kafka/KafkaPublishOptions.html) object.

When producing, a key spec and a value spec work in the other direction. They say which table columns Deephaven writes as each record's key and value.

Kafka's default partitioner hashes a non-null key to choose a partition. All records with the same key go to the same partition as long as the topic's partition count doesn't change.

In this example, we write a [time table](../time-table.md) to a topic called `time-topic`. The example writes column `X` as each record's key and ignores the value.

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

[`produceFromTable`](../../reference/data-import-export/Kafka/produceFromTable.md) returns a `Runnable`. Keep a reference to it, such as `runnable`, for as long as you want to publish. Call `runnable.run()` to stop publishing and release its resources.

Now we write a [time table](../time-table.md) to a topic called `time-topic_group`. The [`KafkaPublishOptions`](/core/javadoc/io/deephaven/kafka/KafkaPublishOptions.html) builder's [`lastBy(true)`](/core/javadoc/io/deephaven/kafka/KafkaPublishOptions.Builder.html#lastBy(boolean)) option tells the producer to perform a [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md) on the key columns before writing to the stream.

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
- [Table types](../../conceptual/table-types.md)
- [Incremental update model](../../conceptual/table-update-model.md)
- [Custom parser for Kafka](./write-your-own-custom-parser-for-kafka.md)
- [Kafka overview](../overview-kafka.md)
- [Partitioned tables](../partitioned-tables.md)
- [Create a time table](../time-table.md)
- [Filter table data](../use-filters.md)
- [Exact and relational joins](../joins-exact-relational.md)
- [Multi-aggregation](../combined-aggregations.md)
- [`consumeToTable`](../../reference/data-import-export/Kafka/consumeToTable.md)
- [`consumeToPartitionedTable`](../../reference/data-import-export/Kafka/consumeToPartitionedTable.md)
- [`produceFromTable`](../../reference/data-import-export/Kafka/produceFromTable.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`naturalJoin`](../../reference/table-operations/join/natural-join.md)
- [`aggBy`](../../reference/table-operations/group-and-aggregate/aggBy.md)
- [`AggSum`](../../reference/table-operations/group-and-aggregate/AggSum.md)
- [`AggWSum`](../../reference/table-operations/group-and-aggregate/AggWSum.md)
- [`renameColumns`](../../reference/table-operations/select/rename-columns.md)
- [Date-times](../../reference/query-language/types/date-time.md)
- [`KafkaTools` Javadoc](/core/javadoc/io/deephaven/kafka/KafkaTools.html)
- [`KafkaPublishOptions` Javadoc](/core/javadoc/io/deephaven/kafka/KafkaPublishOptions.html)
- [`ProtobufConsumeOptions` Javadoc](/core/javadoc/io/deephaven/kafka/protobuf/ProtobufConsumeOptions.html)
