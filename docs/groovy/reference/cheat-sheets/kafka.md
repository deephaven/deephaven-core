---
title: Kafka Cheat Sheet
sidebar_label: Kafka
---

- [`consumeToTable`](../data-import-export/Kafka/consumeToTable.md)
- [`produceFromTable`](../data-import-export/Kafka/produceFromTable.md)

```groovy docker-config=kafka
// Create a table
source = timeTable("PT00:00:00.1").update("X = i")

// Send to Kafka, simple usage
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")

writeTopic = KafkaTools.produceFromTable(
    source,
    kafkaProps,
    "testTopic",
    KafkaTools.Produce.simpleSpec("X"),
    KafkaTools.Produce.IGNORE,
    false
)
```

```groovy docker-config=kafka order=source,sourceGroup
// Create a table with random group number
source = timeTable("PT00:00:00.1").update("X = i")

sourceGroup = timeTable("PT00:00:00.1").update(
    "X = randomInt(1, 6)",
    "Y = i"
)

// Send to Kafka, perform last_by on keys
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")

writeTopicGroup = KafkaTools.produceFromTable(
    sourceGroup,
    kafkaProps,
    "time-topic_group",
    KafkaTools.Produce.jsonSpec(["X"] as String[], null, null),
    KafkaTools.Produce.jsonSpec(["X", "Y"] as String[], null, null),
    true
)
```

```groovy docker-config=kafka
// Read from Kafka, simple usage
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")
kafkaProps.put("deephaven.key.column.type", "String")
kafkaProps.put("deephaven.value.column.type", "String")

result = KafkaTools.consumeToTable(
    kafkaProps,
    "testTopic",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.FROM_PROPERTIES,
    KafkaTools.Consume.FROM_PROPERTIES,
    KafkaTools.TableType.append()
)
```

```groovy skip-test
// Read from Kafka, define key and value
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")

result = KafkaTools.consumeToTable(
    kafkaProps,
    "share.price",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.simpleSpec("Symbol", java.lang.String),
    KafkaTools.Consume.simpleSpec("Price", double),
    KafkaTools.TableType.append()
)
```

```groovy skip-test
// Read from Kafka, ignores the partition and key values
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put("bootstrap.servers", "redpanda:9092")
kafkaProps.put("deephaven.partition.column.name", "")

result = KafkaTools.consumeToTable(
    kafkaProps,
    "share.price",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.simpleSpec("Price", double),
    KafkaTools.TableType.append()
)

// Read from Kafka, JSON with mapping
import io.deephaven.engine.table.ColumnDefinition

colDefs = [
    ColumnDefinition.ofString("Symbol"),
    ColumnDefinition.ofString("Side"),
    ColumnDefinition.ofDouble("Price"),
    ColumnDefinition.ofLong("Qty")
] as ColumnDefinition[]

mapping = [
    "jsymbol": "Symbol",
    "jside": "Side",
    "jprice": "Price",
    "jqty": "Qty"
]

result = KafkaTools.consumeToTable(
    kafkaProps,
    "orders",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.jsonSpec(colDefs, mapping, null),
    KafkaTools.TableType.append()
)

// Read from Kafka, AVRO
kafkaPropsAvro = new Properties()
kafkaPropsAvro.put("bootstrap.servers", "redpanda:9092")
kafkaPropsAvro.put("schema.registry.url", "http://redpanda:8081")

result = KafkaTools.consumeToTable(
    kafkaPropsAvro,
    "share.price",
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    KafkaTools.Consume.avroSpec("share.price.record"),
    KafkaTools.TableType.append()
)
```

## Related documentation

- [`consumeToTable`](../data-import-export/Kafka/consumeToTable.md)
- [`produceFromTable`](../data-import-export/Kafka/produceFromTable.md)
- [How to connect to a Kafka stream](../../how-to-guides/data-import-export/kafka-stream.md)
- [Javadoc](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html)
