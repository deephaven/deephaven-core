---
title: produce
---

`produce` writes a Kafka stream from an in-memory table.

## Syntax

```python syntax
produce(
  table: Table,
  kafka_config: dict,
  topic: str,
  key_spec: KeyValueSpec,
  value_spec: KeyValueSpec,
  last_by_key_columns: bool = False,
)
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

Source table with data to write to the Kafka stream.

</Param>
<Param name="kafka_config" type="dict">

Configuration for the associated Kafka producer. This is used to call the constructor of `org.apache.kafka.clients.producer.KafkaProducer`. Pass any `KafkaProducer`-specific desired configuration here.

</Param>
<Param name="topic" type="String">

Name of the topic to write to.

</Param>
<Param name="key_spec" type="KeyValueSpec">

Specifies how to map table column(s) to the Key field in produced Kafka messages. It can be any of the following, found in `deephaven.stream.kafka.producer`:

- [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.producer.html#deephaven.stream.kafka.producer.simple_spec) - Specify a single column to be deserialized from the field in the Kafka event.
- [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.producer.html#deephaven.stream.kafka.producer.json_spec) - Specify multiple columns to be deserialized from the field in the Kafka event by parsing it as JSON.
- [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.producer.html#deephaven.stream.kafka.producer.avro_spec) - Specify multiple columns to be deserialized from the field in the Kafka event by parsing it as Avro.
- `KeyValueSpec.IGNORE` - Ignore the field in the Kafka event.

</Param>
<Param name="value_spec" type="KeyValueSpec">

Specifies how to map table column(s) to the Value field in produced Kafka messages. It can be any of the following, found in `deephaven.stream.kafka.producer`:

- [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.producer.html#deephaven.stream.kafka.producer.simple_spec) - Specify a single column to be deserialized from the field in the Kafka event.
- [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.producer.html#deephaven.stream.kafka.producer.json_spec) - Specify multiple columns to be deserialized from the field in the Kafka event by parsing it as JSON.
- [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.producer.html#deephaven.stream.kafka.producer.avro_spec) - Specify multiple columns to be deserialized from the field in the Kafka event by parsing it as Avro.
- `KeyValueSpec.IGNORE` - Ignore the field in the Kafka event.

</Param>
<Param name="last_by_key_columns" type="bool" optional>

- `True` - Perform a `last_by_key_columns` on keys before writing to stream.
- `False` - (default) Write all data to stream.

</Param>
</ParamTable>

## Returns

A Kafka stream from an in-memory table.

## Examples

In the following example, `produce` is used to write to the Kafka topic `testTopic` from a Deephaven table.

- The first positional argument, `{'bootstrap.servers': 'redpanda:9092'}`, is a Python dictionary that is standard for any Python library consuming messages from Kafka. This dictionary gives the underlying Kafka library information about the Kafka infrastructure.
  - Here, we specify the host and port for the Kafka server to use to bootstrap the subscription. This is the minimal amount of required information.
  - The value `redpanda:9092` corresponds to the current setup for development testing with Docker images (which uses an instance of [redpanda](https://github.com/vectorizedio/redpanda)).
- The second positional argument is the name of the topic (`testTopic`).
- The third positional argument writes column `X` to the stream.
- The fourth positional argument says to ignore values.

```python docker-config=kafka test-set=1
from deephaven import time_table
from deephaven import kafka_producer as pk
from deephaven.stream.kafka.producer import KeyValueSpec

source = time_table("PT00:00:00.1").update(formulas=["X = i"])

write_topic = pk.produce(
    source,
    {"bootstrap.servers": "redpanda:9092"},
    "testTopic",
    pk.simple_spec("X"),
    KeyValueSpec.IGNORE,
)
```

In the following example, `produce` is used to write Kafka topic `topic_group` with additional settings `last_by_key_columns` as `True`. This indicates that we want to perform a [`last_by`](../../table-operations/group-and-aggregate/lastBy.md) on the keys before writing to the stream.

```python docker-config=kafka test-set=1
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

- [How to connect to a Kafka stream](../../../how-to-guides/data-import-export/kafka-stream.md)
- [`consume`](./consume.md)
- [`last_by`](../../table-operations/group-and-aggregate/lastBy.md)
- [time table](../..//table-operations/create/timeTable.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/kafka/KafkaTools.html#produceFromTable(io.deephaven.engine.table.Table,java.util.Properties,java.lang.String,io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec,io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec,boolean))
- [Pydoc](/core/pydoc/code/deephaven.stream.kafka.producer.html#deephaven.stream.kafka.producer.produce)
