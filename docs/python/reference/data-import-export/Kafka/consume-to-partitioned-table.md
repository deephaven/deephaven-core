---
title: consume_to_partitioned_table
---

The `consume_to_partitioned_table` method reads a Kafka stream into an in-memory partitioned table.

## Syntax

```python syntax
consume_to_partitioned_table(
  kafka_config: dict,
  topic: str,
  partitions: list[int],
  offsets: dict[int, int],
  key_spec: KeyValueSpec,
  value_spec: KeyValueSpec,
  table_type: TableType,
) -> Table
```

## Parameters

<ParamTable>
<Param name="kafka_config" type="dict">

Configuration for the associated Kafka consumer and the resulting table. Once the table-specific properties are stripped, the remaining one is used to call the constructor of `org.apache.kafka.clients.consumer.KafkaConsumer`; pass any `KafkaConsumer`-specific desired configuration here.

</Param>
<Param name="topic" type="str">

The Kafka topic name.

</Param>
<Param name="partitions" type="list[int]" optional>

- Subscribe to all partitions by default.
- Specify partitions as a comma-separated list; e.g.,`partitions=[1, 3, 5 ]` to only listen to partitions 1, 3, and 5.

</Param>
<Param name="offsets" type="dict[int, int]" optional>

- `ALL_PARTITIONS_DONT_SEEK` (default) - Only receive new messages produced after this call is processed. This matches the default if `offsets` is not specified.
- `ALL_PARTITIONS_SEEK_TO_END` - Go to the newest available message for every partition.
- `ALL_PARTITIONS_SEEK_TO_BEGINNING` - Go to the oldest available message for every partition.
- `{ _number_ : _offset_, ... }` - Map partition numbers to numeric offsets or one of the constants `DONT_SEEK`, `SEEK_TO_END`, `SEEK_TO_BEGINNING`.
  - For example, `{ 1 : SEEK_TO_END, 3 : SEEK_TO_BEGINNING, 5 : 27 }` means:
    - Start from the last message sent in partition 1.
    - Go to the oldest available message in partition 3.
    - Start with offset 27 in partition 5.

</Param>
<Param name="key_spec" type="KeyValueSpec" optional>

Specifies how to map the Key field in Kafka messages to table column(s). Any of the following found in `deephaven.stream.kafka.consumer` can be used:

- [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec) - Creates a spec that defines how a single column receives a key or value field from a Kafka message when consuming a Kafka stream.
- [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec) - Creates a spec that defines how to use an Avro schema when consuming a Kafka stream.
- [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec) - Creates a spec for how to use JSON data when consuming a Kafka stream.
- [`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec) - Creates a spec for parsing a Kafka protobuf stream. Uses the `schema`, `schema_version`, and `schema_message_name` to fetch the schema from the schema registry, or uses `message_class` to get the schema from the classpath.
- [`object_processor_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec) - Creates a Kafka key-value spec implementation from a named object processor provider.
- `KeyValueSpec.IGNORE` - Ignore the field in the Kafka event.
- `KeyValueSpec.FROM_PROPERTIES` (default) - The `kafka_config` parameter should include values for dictionary keys `deephaven.key.column.name` and `deephaven.key.column.type` for the single resulting column name and type.

</Param>
<Param name="value_spec" type="KeyValueSpec" optional>

Specifies how to map the Value field in Kafka messages to table column(s). Any of the following specifications found in [`deephaven.stream.kafka.consumer`](/core/pydoc/code/deephaven.stream.kafka.consumer.html) can be used:

- [`simple_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.simple_spec) - Creates a spec that defines how a single column receives a key or value field from a Kafka message when consuming a Kafka stream.
- [`avro_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.avro_spec) - Creates a spec that defines how to use an Avro schema when consuming a Kafka stream.
- [`json_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.json_spec) - Creates a spec for how to use JSON data when consuming a Kafka stream.
- [`protobuf_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.protobuf_spec) - Creates a spec for parsing a Kafka protobuf stream. Uses the `schema`, `schema_version`, and `schema_message_name` to fetch the schema from the schema registry, or uses `message_class` to get the schema from the classpath.
- [`object_processor_spec`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.object_processor_spec) - Creates a Kafka key-value spec implementation from a named object processor provider.
- `KeyValueSpec.IGNORE` - Ignore the field in the Kafka event.
- `KeyValueSpec.FROM_PROPERTIES` (default) - The `kafka_config` parameter should include values for dictionary keys `deephaven.key.column.name` and `deephaven.key.column.type` for the single resulting column name and type.

</Param>
<Param name="table_type" type="TableType" optional>

One of the following `TableType` enums:

- `TableType.append()` - Create an [append-only](../../../conceptual/table-types.md#specialization-1-append-only) table.
- `TableType.blink()` (default) - Create a [blink](../../../conceptual/table-types.md#specialization-3-blink) table.
- `TableType.ring(N)` - Create a [ring](../../../conceptual/table-types.md#specialization-4-ring) table of size `N`.

</Param>
</ParamTable>

## Returns

An in-memory partitioned table.

## Examples

In the following example, `consume_to_partitioned_table` reads the Kafka topic `orders` into a partitioned table. It uses the JSON format to parse the stream. Since we do not provide `partitions` or `offsets` values, the consumer will include all partitions and start from the first message received after the `consume_to_partitioned_table` call.

```python docker-config=kafka order=null
from deephaven import kafka_consumer as kc
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
from deephaven import dtypes as dht

pt = kc.consume_to_partitioned_table(
    {"bootstrap.servers": "redpanda:9092"},
    "orders",
    key_spec=KeyValueSpec.IGNORE,
    value_spec=kc.json_spec(
        {
            "Symbol": dht.string,
            "Side": dht.string,
            "Price": dht.double,
            "Qty": dht.int64,
            "Tstamp": dht.Instant,
        },
        mapping={
            "jsymbol": "Symbol",
            "jside": "Side",
            "jprice": "Price",
            "jqty": "Qty",
            "jts": "Tstamp",
        },
    ),
    table_type=TableType.append(),
)
```

The following example, like the one above, reads the JSON-formatted Kafka topic `orders` into a Partitioned table. This time, though, it uses a [Jackson provider](/core/pydoc/code/deephaven.json.jackson.html) object processor specification.

```python docker-config=kafka order=null
from deephaven import kafka_consumer as kc
from deephaven.stream.kafka.consumer import (
    TableType,
    KeyValueSpec,
    object_processor_spec,
)
from deephaven import dtypes as dht
from deephaven.json import jackson
from datetime import datetime

pt = (
    kc.consume_to_partitioned_table(
        {"bootstrap.servers": "redpanda:9092"},
        "orders",
        key_spec=KeyValueSpec.IGNORE,
        value_spec=object_processor_spec(
            jackson.provider(
                {
                    "jsymbol": str,
                    "jside": str,
                    "jprice": float,
                    "jqty": int,
                    "jts": datetime,
                }
            )
        ),
        table_type=TableType.append(),
    )
    .proxy()
    .view(["Symbol=jsymbol", "Side=jside", "Price=jprice", "Qty=jqty", "Tstamp=jts"])
    .target
)
```

## Related documentation

- [`consume`](./consume.md)
- [`produce`](./produce.md)
- [How to connect to a Kafka stream](../../../how-to-guides/data-import-export/kafka-stream.md)
- [Javadoc](/core/javadoc/io/deephaven/kafka/KafkaTools.Consume.html)
- [Pydoc](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.consume)
