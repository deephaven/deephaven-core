---
title: consumeToPartitionedTable
---

The `consumeToPartitionedTable` method reads a Kafka stream into an in-memory partitioned table.

## Syntax

```groovy syntax
consumeToPartitionedTable(
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

Configuration for the associated Kafka consumer and the resulting table. Once the table-specific properties are stripped, the remaining one is used to call the constructor of `org.apache.kafka.clients.consumer.KafkaConsumer`; pass any `KafkaConsumer`-specific desired configuration here.

</Param>
<Param name="topic" type="str">

The Kafka topic name.

</Param>
<Param name="partitionFilter" type="IntPredicate">

A predicate returning true for the partitions to consume. The convenience constant ALL_PARTITIONS is defined to facilitate requesting all partitions.

</Param>
<Param name="partitionToInitialOffset" type="IntToLongFunction">

A function specifying the desired initial offset for each partition consumed.

</Param>
<Param name="keySpec" type="KeyOrValueSpec">

Conversion specification for Kafka record keys.

</Param>
<Param name="valueSpec" type="KeyOrValueSpec">

Conversion specification for Kafka record values.

</Param>
<Param name="tableType" type="TableType">

A `TableType` enum. The default is `TableType.blink()`.

- `TableType.append()` - Create an [append-only](../../../conceptual/table-types.md#specialization-1-append-only) table.
- `TableType.blink()` - Create a [blink](../../../conceptual/table-types.md#specialization-3-blink) table (default).
- `TableType.ring(N)` - Create a [ring](../../../conceptual/table-types.md#specialization-4-ring) table of size `N`.

</Param>
</ParamTable>

## Returns

An in-memory partitioned table.

## Examples

In the following example, `consumeToPartitionedTable` is used to read the Kafka topic `orders` into a Deephaven table. `KafkaTools.Consume.FROM_PROPERTIES` allows the key and value column types to be inferred by the properties passed in.

- The host and port for the Kafka server to use to bootstrap are specified by `kafkaProps`.
  - The value `redpanda:9092` corresponds to the current setup for development testing with Docker images (which uses an instance of [redpanda](https://github.com/redpanda-data/redpanda)).
- The topic name is `orders`.

```groovy skip-test docker-config=kafka
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

pt = KafkaTools.consumeToPartitionedTable(
    kafkaProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    spec,
    KafkaTools.TableType.append()
)
```

## Related documentation

- [`consumeToTable`](./consumeToTable.md)
- [`produceFromTable`](./produceFromTable.md)
- [How to connect to a Kafka stream](../../../how-to-guides/data-import-export/kafka-stream.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#consumeToPartitionedTable(java.util.Properties,java.lang.String,java.util.function.IntPredicate,java.util.function.IntToLongFunction,io.deephaven.kafka.KafkaTools.Consume.KeyOrValueSpec,io.deephaven.kafka.KafkaTools.Consume.KeyOrValueSpec,io.deephaven.kafka.KafkaTools.TableType))
