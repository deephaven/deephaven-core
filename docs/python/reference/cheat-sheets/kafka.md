---
title: Kafka cheat sheet
---

This cheat sheet shows common ways to produce Deephaven tables to Kafka and consume Kafka topics into live Deephaven tables that update as new records arrive. The main methods are:

- [`produce`](../data-import-export/Kafka/produce.md)
- [`consume`](../data-import-export/Kafka/consume.md)
- [`consume_to_partitioned_table`](../data-import-export/Kafka/consume-to-partitioned-table.md)

The examples assume the following setup:

- A Kafka broker (the server that stores topics) at `redpanda:9092`. See [Kafka basic terms](../../conceptual/kafka-basic-terms.md).
- A [schema registry](../../conceptual/kafka-basic-terms.md#formats) at `http://redpanda:8081`, for the Avro example only.
- The `testTopic` topic, which [Simple produce](#simple-produce) writes and [Simple consume](#simple-consume) and [Start from the beginning of a topic](#start-from-the-beginning-of-a-topic) read.
- The `time-topic_group` topic, which [Produce JSON with `last_by` on key columns](#produce-json-with-last_by-on-key-columns) writes.
- A `share.price` topic with string keys and double values, supplied separately, for [Define the key and value columns](#define-the-key-and-value-columns) and [Ignore the partition and key](#ignore-the-partition-and-key).
- A `share.price.avro` topic of Avro records whose schema is registered as `share.price.record`, supplied separately, for [Consume Avro](#consume-avro).
- An `orders` topic of JSON records with the fields `jsymbol`, `jside`, `jprice`, and `jqty`, supplied separately, for [Consume JSON with a field mapping](#consume-json-with-a-field-mapping).

To run a broker alongside Deephaven, see [Connect to a Kafka stream](../../how-to-guides/data-import-export/kafka-stream.md#launching-kafka-with-deephaven).

## Produce to Kafka

### Simple produce

Publish column `X` as the message key with a simple [key spec](../../conceptual/kafka-basic-terms.md#key-and-value-specification) and ignore the message value.

```python docker-config=kafka ticking-table order=null
# Create a table
from deephaven import time_table

source = time_table("PT00:00:00.1").update(formulas=["X = i"])

# Send to Kafka, simple usage
from deephaven import kafka_producer as pk
from deephaven.stream.kafka.producer import KeyValueSpec

write_topic = pk.produce(
    source,
    {"bootstrap.servers": "redpanda:9092"},
    "testTopic",
    pk.simple_spec("X"),
    KeyValueSpec.IGNORE,
)
```

### Produce JSON with `last_by` on key columns

Publish column `X` as a JSON key and columns `X` and `Y` as a JSON value. Setting `last_by_key_columns` to `True` runs a [`last_by`](../table-operations/group-and-aggregate/lastBy.md) on the key column `X` and publishes from the result, so each message carries the latest row for its key. Because `source_group` keeps ticking, the producer still publishes a message each time a key's latest row changes.

```python docker-config=kafka ticking-table order=null
# Create a table with random group number
from deephaven import time_table

source_group = time_table("PT00:00:00.1").update(
    formulas=["X = randomInt(1, 6)", "Y = i"]
)

# Send to Kafka, perform last_by on keys
from deephaven import kafka_producer as pk

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
    last_by_key_columns=True,
)
```

## Consume from Kafka

By default, [`consume`](../data-import-export/Kafka/consume.md) reads all [partitions](../../conceptual/kafka-basic-terms.md#partition) and returns a [blink](../../conceptual/table-types.md#specialization-3-blink) table. It does not seek, so Kafka picks the starting [offset](../../conceptual/kafka-basic-terms.md#offset). With no `group.id` set, as in these examples, it starts at the end of each partition, so the table receives only records produced after the call. [Start from the beginning of a topic](#start-from-the-beginning-of-a-topic) shows how to read existing records instead. The examples that pass `table_type=TableType.append()` return an [append-only](../../conceptual/table-types.md#specialization-1-append-only) table instead.

[`consume_to_partitioned_table`](../data-import-export/Kafka/consume-to-partitioned-table.md) takes the same arguments as [`consume`](../data-import-export/Kafka/consume.md) and returns a [partitioned table](../../how-to-guides/partitioned-tables.md) with one constituent table per Kafka [partition](../../conceptual/kafka-basic-terms.md#partition). See its reference page for an example.

### Simple consume

Without a [key or value spec](../../conceptual/kafka-basic-terms.md#key-and-value-specification), the key and value column types come from the `deephaven.key.column.type` and `deephaven.value.column.type` properties. The properties match what [Simple produce](#simple-produce) writes: an `int` key and no value. The `KafkaKey` column is typed `int`, and the `KafkaValue` column holds only nulls.

```python docker-config=kafka ticking-table order=null
# Read from Kafka, simple usage
from deephaven import kafka_consumer as ck

result = ck.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "deephaven.key.column.type": "int",
        "deephaven.value.column.type": "String",
    },
    "testTopic",
)
```

### Start from the beginning of a topic

Pass [`ALL_PARTITIONS_SEEK_TO_BEGINNING`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.ALL_PARTITIONS_SEEK_TO_BEGINNING) as `offsets` to read the records already in the topic, starting at the beginning of each partition, and then the new records as they arrive.

```python skip-test
# Read from Kafka, starting at the beginning of each partition
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType

result_beginning = ck.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "deephaven.key.column.type": "int",
        "deephaven.value.column.type": "String",
    },
    "testTopic",
    offsets=ck.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    table_type=TableType.append(),
)
```

### Define the key and value columns

Read the key into a `Symbol` string column and the value into a `Price` double column.

```python skip-test
# Read from Kafka, define key and value
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType
import deephaven.dtypes as dht

result = ck.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "share.price",
    key_spec=ck.simple_spec("Symbol", dht.string),
    value_spec=ck.simple_spec("Price", dht.double),
    table_type=TableType.append(),
)
```

### Ignore the partition and key

Setting [`deephaven.partition.column.name`](../../how-to-guides/data-import-export/kafka-stream.md#standard-data-fields) to an empty string leaves out the `KafkaPartition` column, which [`consume`](../data-import-export/Kafka/consume.md) adds by default to hold each record's [partition](../../conceptual/kafka-basic-terms.md#partition) number. [`KeyValueSpec.IGNORE`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.KeyValueSpec) leaves out the key.

```python skip-test
# Read from Kafka, ignore the partition and key values
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import KeyValueSpec
import deephaven.dtypes as dht

result_ignore = ck.consume(
    {"bootstrap.servers": "redpanda:9092", "deephaven.partition.column.name": ""},
    "share.price",
    key_spec=KeyValueSpec.IGNORE,
    value_spec=ck.simple_spec("Price", dht.double),
)
```

### Consume JSON with a field mapping

Parse each JSON value into four typed columns. The `mapping` argument maps JSON field names, such as `jsymbol`, to column names.

```python skip-test
# Read from Kafka, JSON with mapping
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
import deephaven.dtypes as dht

result_json = ck.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "orders",
    key_spec=KeyValueSpec.IGNORE,
    value_spec=ck.json_spec(
        {
            "Symbol": dht.string,
            "Side": dht.string,
            "Price": dht.double,
            "Qty": dht.int64,
        },
        mapping={
            "jsymbol": "Symbol",
            "jside": "Side",
            "jprice": "Price",
            "jqty": "Qty",
        },
    ),
    table_type=TableType.append(),
)
```

### Consume Avro

Parse each value with version `1` of the `share.price.record` [Avro](../../conceptual/kafka-basic-terms.md#avro) schema from the [schema registry](../../conceptual/kafka-basic-terms.md#formats).

```python skip-test
# Read from Kafka, Avro
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec

result_avro = ck.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "schema.registry.url": "http://redpanda:8081",
    },
    "share.price.avro",
    key_spec=KeyValueSpec.IGNORE,
    value_spec=ck.avro_spec("share.price.record", schema_version="1"),
    table_type=TableType.append(),
)
```

## Related documentation

- [A Kafka introduction: basic terms](../../conceptual/kafka-basic-terms.md)
- [Deephaven's table types](../../conceptual/table-types.md)
- [Kafka overview](../../how-to-guides/overview-kafka.md)
- [Connect to a Kafka stream](../../how-to-guides/data-import-export/kafka-stream.md)
- [Partitioned tables](../../how-to-guides/partitioned-tables.md)
- [`consume`](../data-import-export/Kafka/consume.md)
- [`consume_to_partitioned_table`](../data-import-export/Kafka/consume-to-partitioned-table.md)
- [`last_by`](../table-operations/group-and-aggregate/lastBy.md)
- [`produce`](../data-import-export/Kafka/produce.md)
- [`time_table`](../table-operations/create/timeTable.md)
- [`update`](../table-operations/select/update.md)
- [`consume` Pydoc](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.consume)
- [`consume_to_partitioned_table` Pydoc](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.consume_to_partitioned_table)
- [`produce` Pydoc](/core/pydoc/code/deephaven.stream.kafka.producer.html#deephaven.stream.kafka.producer.produce)
