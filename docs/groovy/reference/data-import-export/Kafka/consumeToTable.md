---
title: consumeToTable
---

`consumeToTable` reads a Kafka stream into an in-memory table.

## Syntax

```groovy syntax
consumeToTable(
    kafkaProperties,
    topic,
    partitionFilter,
    partitionToInitialOffset,
    keySpec,
    valueSpec,
    tableType
)
```

## Parameters

<ParamTable>
<Param name="kafkaProperties" type="Properties">

Properties to configure the result and also to be passed to create the `KafkaConsumer`.

</Param>
<Param name="topic" type="String">

The Kafka topic name.

</Param>
<Param name="partitionFilter" type="IntPredicate">

A predicate returning true for the partitions to consume. The convenience constant `ALL_PARTITIONS` is defined to facilitate requesting all partitions.

</Param>
<Param name="partitionToInitialOffset" type="IntToLongFunction">

A function specifying the desired initial offset for each partition consumed.

</Param>
<Param name="keySpec" type="KafkaTools.Consume.KeyOrValueSpec">

Conversion specification for Kafka record keys.

</Param>
<Param name="valueSpec" type="KafkaTools.Consume.KeyOrValueSpec">

Conversion specification for Kafka record values.

</Param>
<Param name="tableType" type="KafkaTools.TableType">

The expected table type of the resultant table.

</Param>
</ParamTable>

## Returns

An in-memory table.

## Examples

In the following example, `consumeToTable` is used to read the Kafka topic `test.topic` into a Deephaven table. `KafkaTools.Consume.FROM_PROPERTIES` allows the key and value column types to be inferred by the properties passed in.

- The host and port for the Kafka server to use to bootstrap are specified by `kafkaProps`.
  - The value `redpanda:9092` corresponds to the current setup for development testing with Docker images (which uses an instance of [redpanda](https://github.com/redpanda-data/redpanda)).
- The topic name is `testTopic`.

```groovy docker-config=kafka order=null
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('deephaven.key.column.type', 'String')
kafkaProps.put('deephaven.value.column.type', 'String')

result = KafkaTools.consumeToTable(
    kafkaProps,
    'testTopic',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.FROM_PROPERTIES,
    KafkaTools.Consume.FROM_PROPERTIES,
    KafkaTools.TableType.append()
)
```

![The above `result` table](../../../assets/reference/data-import-export/kafka1.png)

In the following example, `consumeToTable` is used to read Kafka topic `share_price` with key and value specifications.

- The key column name is `Symbol`, which will be `String` type.
- The value column name is `Price`, which will be `double` type.

```groovy skip-test
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

result = KafkaTools.consumeToTable(
    kafkaProps,
    'share_price',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.simpleSpec('Symbol', java.lang.String),
    KafkaTools.Consume.simpleSpec('Price', double),
    KafkaTools.TableType.append()
)
```

![The above `result` table](../../../assets/reference/data-import-export/kafka2.png)

The following example sets the `deephaven.partition.column.name` as `null`, which ignores it.

- This results in the table not having a column for the partition field.
- The key specification is also set to `IGNORE`, so `result` does not contain the Kafka key column.

```groovy skip-test
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')
kafkaProps.put('deephaven.partition.column.name', null)

result = KafkaTools.consumeToTable(
    kafkaProps,
    'share_price',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec('Price', double),
    KafkaTools.TableType.blink()
)
```

![The above `result` table](../../../assets/reference/data-import-export/kafka3.png)

In the following example, `consumeToTable` reads the Kafka topic `share_price` in `JSON` format.

```groovy skip-test
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

symbolDef = ColumnDefinition.ofString('Symbol')
sideDef = ColumnDefinition.ofString('Side')
priceDef = ColumnDefinition.ofDouble('Price')
qtyDef = ColumnDefinition.ofInt('Qty')

ColumnDefinition[] colDefs = [symbolDef, sideDef, priceDef, qtyDef]
mapping = ['jsymbol': 'Symbol', 'jside': 'Side', 'jprice': 'Price', 'jqty': 'Qty']

spec = KafkaTools.Consume.jsonSpec(colDefs, mapping, null)

result = KafkaTools.consumeToTable(
    kafkaProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    spec,
    KafkaTools.TableType.blink()
)
```

![The above `result` table](../../../assets/reference/data-import-export/kafka4.png)

In the following example, `consumeToTable` reads the Kafka topic `share_price` in `Avro` format. The schema name and version are specified.

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
    KafkaTools.Consume.avroSpec('share_price_record', '1'),
    KafkaTools.TableType.blink()
)
```

![The Avro file that the above example reads from](../../../assets/reference/data-import-export/kafka5.png)
![The above result table](../../../assets/reference/data-import-export/kafka4.png)

<!--TODO: protobuf example-->

## Related documentation

- [How to connect to a Kafka stream](../../../how-to-guides/data-import-export/kafka-stream.md)
