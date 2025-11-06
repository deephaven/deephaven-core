---
title: "How can I construct a ring table of the first row of the last N update cycles of a blink table?"
sidebar_label: "How can I store the first row of the last N blinks of a table?"
---

_I have a blink table, from which I want to extract the first row of the last N blinks into a separate ring table. How can I do that?_

To achieve this:

- Use a [Table listener](../../how-to-guides/table-listeners-python.md) to listen to the source blink table.
- Use a [Table publisher](../../how-to-guides/table-publisher.md) to publish the first row each update cycle.
- Convert the result to a ring table.

Here's a complete example:

```python skip-test
from deephaven.stream.table_publisher import table_publisher
from deephaven.execution_context import get_exec_ctx
from deephaven.stream import blink_to_append_only
from deephaven.table_factory import ring_table
from deephaven.table_listener import listen
from deephaven.time import to_j_instant
from deephaven import dtypes as dht
from deephaven import empty_table
from deephaven import time_table

ctx = get_exec_ctx()

source = time_table("PT0.2s", blink_table=True).update("X = (double)ii")

result_blink, my_publisher = table_publisher(
    name="Example", col_defs={"Timestamp": dht.Instant, "X": dht.double}
)


def add_table(ts, x_val):
    with ctx:
        my_publisher.add(empty_table(1).update(["Timestamp = ts", "X = x_val"]))


def listener_function(update, is_replay):
    added = update.added()
    first_timestamp = to_j_instant(added["Timestamp"][0])
    first_x = added["X"][0]
    add_table(first_timestamp, first_x)


handle = listen(source, listener_function)

result = blink_to_append_only(result_blink)
result_ring = ring_table(result, 10)
```

![First row of last 10 blinks](../../assets/reference/faq/blink-ring.gif)

This example shows that the solution works, since the `X` column contains only the value `0`, which is the value from the first row on every update cycle.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
