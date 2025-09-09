---
title: produceFromTable
---

`produceFromTable` writes a Kafka stream from an in-memory table.

## Syntax

```groovy syntax
produceFromTable(options)
produceFromTable(table, kafkaProperties, topic, keySpec, valueSpec, lastByKeyColumns)
```

## Parameters

<ParamTable>
<Param name="options" type="KafkaPublishOptions">

Kafka publishing options that include the source table, properties, string, key and value specifications, and whether or not to perform a [`lastBy`](../../table-operations/group-and-aggregate/lastBy.md) on the specified key column(s) before publishing.

</Param>
<Param name="table" type="Table">

The table to use as the source of data for Kafka.

</Param>
<Param name="kafkaProperties" type="Properties">

Properties to be passed to create the associated producer.

</Param>
<Param name="topic" type="String">

The Kafka topic name.

</Param>
<Param name="keySpec" type="KeyOrValueSpec">

Conversion specification for Kafka record keys from table column data.

</Param>
<Param name="valueSpec" type="KeyOrValueSpec">

Conversion specification for Kafka record values from table column data.

</Param>
<Param name="lastByKeyColumns" type="boolean">

Whether to publish only the last record for each unique key. Ignored when `keySpec` is `IGNORE`. Otherwise, if `true`, this method will internally perform a [`lastBy`](../../table-operations/group-and-aggregate/lastBy.md) aggregation using the key column(s) in `keySpec` before publishing to Kafka.

</Param>
</ParamTable>

## Returns

A callback to stop producing and shut down the associated table listener.

## Examples

In the following example, `produceFromTable` is used to write to the Kafka topic `testTopic` from a Deephaven table.

```groovy docker-config=kafka order=null
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

In the following example, `produceFromTable` is used to write the Kafka topic with `lastByKeyColumns` as `true`. This indicates we want to perform a [`lastBy`](../../table-operations/group-and-aggregate/lastBy.md) with the specified key columns before publishing the data.

```groovy docker-config=kafka ticking-table order=null
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

- [How to connect to a Kafka stream](../../../how-to-guides/data-import-export/kafka-stream.md)
- [`consumeToTable`](./consumeToTable.md)
- [`lastBy`](../../table-operations/group-and-aggregate/lastBy.md)
- [`timeTable`](../../table-operations/create/timeTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#produceFromTable(io.deephaven.kafka.KafkaPublishOptions))
