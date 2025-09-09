---
title: Kafka Cheat Sheet
sidebar_label: Kafka
---

- [`consume`](../data-import-export/Kafka/consume.md)
- [`produce`](../data-import-export/Kafka/produce.md)

```python docker-config=kafka
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

```python docker-config=kafka order=source,source_group
# Create a table with random group number
# Create a table
from deephaven import time_table

source = time_table("PT00:00:00.1").update(formulas=["X = i"])

import random

source_group = time_table("PT00:00:00.1").update(
    formulas=["X = random.randint(1, 5)", "Y = i"]
)

# Send to Kafka, perform last_by on keys
from deephaven import kafka_producer as pk
from deephaven.stream.kafka.producer import KeyValueSpec

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

```python docker-config=kafka
# Read from Kafka, simple usage
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
import deephaven.dtypes as dht

result = ck.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "deephaven.key.column.type": "String",
        "deephaven.value.column.type": "String",
    },
    "testTopic",
)
```

```python skip-test
# Read from Kafka, define key and value
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
import deephaven.dtypes as dht

result = ck.consume(
    {"bootstrap.servers": "redpanda:9092"},
    "share.price",
    partitions=ck.ALL_PARTITIONS,
    offsets=ck.ALL_PARTITIONS_DONT_SEEK,
    key_spec=ck.simple_spec("Symbol", dht.string),
    value_spec=ck.simple_spec("Price", dht.double),
    table_type=TableType.Append,
)
```

```python skip-test
# Read from Kafka, ignores the partition and key values
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
import deephaven.dtypes as dht

result = ck.consume(
    {"bootstrap.servers": "redpanda:9092", "deephaven.partition.column.name": None},
    "share.price",
    key_spec=KeyValueSpec.IGNORE,
    value_spec=ck.simple_spec("Price", dht.double),
)

# Read from Kafka, JSON with mapping
result = ck.consume(
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

# Read from Kafka, AVRO
result = ck.consume(
    {
        "bootstrap.servers": "redpanda:9092",
        "schema.registry.url": "http://redpanda:8081",
    },
    "share.price",
    key_spec=KeyValueSpec.IGNORE,
    value_spec=ck.avro_spec("share.price.record", schema_version="1"),
    table_type=TableType.Append,
)
```

## Related documentation

- [`consume`](../data-import-export/Kafka/consume.md)
- [`produce`](../data-import-export/Kafka/produce.md)
- [How to connect to a Kafka stream](../../how-to-guides/data-import-export/kafka-stream.md)
- [Javadoc](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html)
- [Pydoc](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.consume)
