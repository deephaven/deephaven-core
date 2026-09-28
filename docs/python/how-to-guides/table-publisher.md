---
title: Write data to an in-memory, real-time table
sidebar_label: Write data to a real-time table
---

This guide covers publishing data to in-memory ticking tables with two methods:

- [`table_publisher`](../reference/table-operations/create/TablePublisher.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)

A Table Publisher publishes data to a [blink table](../conceptual/table-types.md#specialization-3-blink), while a Dynamic Table Writer writes data to an append-only table. Both methods are great ways to ingest and publish data from external sources such as WebSockets and other live data sources. However, we recommend [`table_publisher`](../reference/table-operations/create/TablePublisher.md) in most cases, as it provides a newer and more refined API, as well as native support for blink tables.

## Table publisher

A table publisher uses the [`table_publisher`](../reference/table-operations/create/TablePublisher.md) factory function to create an instance of the [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher) as well as its linked [blink table](../conceptual/table-types.md#specialization-3-blink). From there:

- Add data to the [blink table](../conceptual/table-types.md#specialization-3-blink) with [`add`](../reference/table-operations/create/TablePublisher.md#methods).
- (Optionally) Store [data history](#data-history) in a downstream table.
- (Optionally) Shut the publisher down when finished.

More sophisticated use cases will add steps but follow the same basic formula.

### The factory function

The [`table_publisher`](../reference/table-operations/create/TablePublisher.md) factory function returns a [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher) and its linked [blink table](../conceptual/table-types.md#specialization-3-blink), in that order. The following code block creates a table publisher named `My table publisher` that publishes to a [blink table](../conceptual/table-types.md#specialization-3-blink) with two columns, `X` and `Y`, which are `int` and `double` data types, respectively.

```python syntax
from deephaven.stream.table_publisher import table_publisher
from deephaven import dtypes as dht

my_table, my_publisher = table_publisher(
    name="My table publisher", col_defs={"X": dht.int32, "Y": dht.double}
)
```

### Example: Getting started

The following example creates a table with three columns (`X`, `Y`, and `Z`). The columns initially contain no data because `add_table` has not yet been called.

```python test-set=1 order=my_table
from deephaven.stream.table_publisher import table_publisher
from deephaven import dtypes as dht
from deephaven import empty_table

coldefs = {"X": dht.int32, "Y": dht.double, "Z": dht.double}


def on_shutdown():
    print("Table publisher is shut down.")


my_table, publisher = table_publisher(
    name="My table", col_defs=coldefs, on_shutdown_callback=on_shutdown
)


def add_table(n):
    publisher.add(
        empty_table(n).update(
            [
                "X = randomInt(0, 10)",
                "Y = randomDouble(0.0, 1.0)",
                "Z = randomDouble(10.0, 100.0)",
            ]
        )
    )


def when_done():
    publisher.publish_failure(RuntimeError("Publisher shut down by user."))
```

Subsequent calls of `add_table` will add data to `my_table`.

```python test-set=1 order=my_table
add_table(10)
```

The `TablePublisher` can be shut down by calling [`publish_failure`](../reference/table-operations/create/TablePublisher.md#methods). In this case, the `when_done` function invokes it.

```python test-set=1 order=null
when_done()
```

### Example: threading

The previous example required manual calls to `add_table` to populate `my_table` with data. In most real-world use cases, adding data to the table should be automated at some regular interval. This can be achieved using Python's [threading](https://docs.python.org/3/library/threading.html) module. The following example adds between 5 and 10 rows of data to `my_table` via [`empty_table`](../reference/table-operations/create/emptyTable.md) every second for 5 seconds.

> [!IMPORTANT]
> A ticking table in a thread must be updated from within an [execution context](../conceptual/execution-context.md).

```python ticking-table order=null
from deephaven.stream.table_publisher import table_publisher
from deephaven.execution_context import get_exec_ctx
from deephaven import dtypes as dht
from deephaven import empty_table
import asyncio, random, threading, time

coldefs = {"X": dht.int32, "Y": dht.double}


def shut_down():
    print("Shutting down table publisher.")


my_table, my_publisher = table_publisher(
    name="Publisher", col_defs=coldefs, on_shutdown_callback=shut_down
)


def when_done():
    my_publisher.publish_failure(RuntimeError("when_done invoked"))


def add_table(n):
    my_publisher.add(
        empty_table(n).update(["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"])
    )


ctx = get_exec_ctx()


def thread_func():
    with ctx:
        for i in range(5):
            add_table(random.randint(5, 10))
            time.sleep(1)


thread = threading.Thread(target=thread_func)
thread.start()
```

![The above table](../assets/how-to/publisher-threaded.gif)

### Example: asyncio

The following code block uses asynchronous execution to pull crypto data from Coinbase's WebSocket feed. The asynchronous execution is used for ingesting the external data to minimize idle CPU time.

> [!NOTE]
> The [websockets](https://pypi.org/project/websockets/) package is required to run the code below.

```python skip-test
from deephaven.stream.table_publisher import table_publisher, TablePublisher
from deephaven.column import string_col, double_col, datetime_col, long_col
from deephaven.dtypes import int64, string, double, Instant
from deephaven.time import to_j_instant
from deephaven.table import Table
from deephaven import new_table

import asyncio, json, websockets
from dataclasses import dataclass
from typing import Callable
from threading import Thread
from datetime import datetime
from concurrent.futures import CancelledError

COINBASE_WSFEED_URL = "wss://ws-feed.exchange.coinbase.com"


@dataclass
class Match:
    type: str
    trade_id: int
    maker_order_id: str
    taker_order_id: str
    side: str
    size: str
    price: str
    product_id: str
    sequence: int
    time: str


async def handle_matches(
    product_ids: list[str], message_handler: Callable[[Match], None]
):
    async for websocket in websockets.connect(COINBASE_WSFEED_URL):
        await websocket.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "product_ids": product_ids,
                    "channels": ["matches"],
                }
            )
        )
        # Skip subscribe response
        await websocket.recv()
        # Skip the last_match messages
        for _ in product_ids:
            await websocket.recv()
        async for message in websocket:
            message_handler(Match(**json.loads(message)))


def to_table(matches: list[Match]):
    return new_table(
        [
            datetime_col("Time", [to_j_instant(x.time) for x in matches]),
            long_col("TradeId", [x.trade_id for x in matches]),
            string_col("MakerOrderId", [x.maker_order_id for x in matches]),
            string_col("TakerOrderId", [x.taker_order_id for x in matches]),
            string_col("Side", [x.side for x in matches]),
            double_col("Size", [float(x.size) for x in matches]),
            double_col("Price", [float(x.price) for x in matches]),
            string_col("ProductId", [x.product_id for x in matches]),
            long_col("Sequence", [x.sequence for x in matches]),
        ]
    )


def create_matches(
    product_ids: list[str], event_loop
) -> tuple[Table, Callable[[], None]]:
    on_shutdown_callbacks = []

    def on_shutdown():
        nonlocal on_shutdown_callbacks
        for c in on_shutdown_callbacks:
            c()

    my_matches: list[Match] = []

    def on_flush(tp: TablePublisher):
        nonlocal my_matches
        # We need to take a shallow copy to ensure we don't allow asyncio additions to
        # my_matches while we are in java (where we drop the GIL)
        my_matches_copy = my_matches.copy()
        my_matches.clear()
        tp.add(to_table(my_matches_copy))

    table, publisher = table_publisher(
        f"Matches for {product_ids}",
        {
            "Time": Instant,
            "TradeId": int64,
            "MakerOrderId": string,
            "TakerOrderId": string,
            "Side": string,
            "Size": double,
            "Price": double,
            "ProductId": string,
            "Sequence": int64,
        },
        on_flush_callback=on_flush,
        on_shutdown_callback=on_shutdown,
    )

    future = asyncio.run_coroutine_threadsafe(
        handle_matches(product_ids, my_matches.append), event_loop
    )

    def on_future_done(f):
        nonlocal publisher
        try:
            e = f.exception(timeout=0) or RuntimeError("completed")
        except CancelledError as c:
            e = RuntimeError("cancelled")
        publisher.publish_failure(e)

    future.add_done_callback(on_future_done)

    on_shutdown_callbacks.append(future.cancel)

    return table, future.cancel


my_event_loop = asyncio.new_event_loop()
Thread(target=my_event_loop.run_forever).start()


def subscribe_stats(product_ids: list[str]):
    blink_table, on_done = create_matches(product_ids, my_event_loop)
    return blink_table, on_done


t1, t1_cancel = subscribe_stats(["BTC-USD"])
t2, t2_cancel = subscribe_stats(["ETH-USD", "BTC-USDT", "ETH-USDT"])

# call these to explicitly cancel
# t1_cancel()
# t2_cancel()
```

![The above `t1` and `t2` tables](../assets/how-to/table-publisher-coinbase.gif)

## Data history

Table publishers create blink tables. Blink tables do not store any data history - data is gone forever at the start of a new update cycle. In most use cases, you will want to store some or all of the rows written during previous update cycles. There are two ways to do this:

- Store some data history by creating a downstream ring table with [`ring_table`](../reference/table-operations/create/ringTable.md).
- Store all data history by creating a downstream append-only table with [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md).

See the [table types user guide](../conceptual/table-types.md) for more information on these table types, including which one is best suited for your application.

To show the storage of data history, we will extend the [threading example](#example-threading) by creating a downstream ring table and append-only table.

```python ticking-table order=null
from deephaven.stream.table_publisher import table_publisher
from deephaven.execution_context import get_exec_ctx
from deephaven.stream import blink_to_append_only
from deephaven import dtypes as dht
from deephaven import empty_table
from deephaven import ring_table
import asyncio, random, threading, time

coldefs = {"X": dht.int32, "Y": dht.double}


def shut_down():
    print("Shutting down table publisher.")


my_table, my_publisher = table_publisher(
    name="Publisher", col_defs=coldefs, on_shutdown_callback=shut_down
)


def when_done():
    my_publisher.publish_failure(RuntimeError("when_done invoked"))


def add_table(n):
    my_publisher.add(
        empty_table(n).update(["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"])
    )


ctx = get_exec_ctx()


def thread_func():
    with ctx:
        for i in range(5):
            add_table(random.randint(5, 10))
            time.sleep(1)


thread = threading.Thread(target=thread_func)
thread.start()

# Downstream ring table that stores the most recent 15 rows
my_ring_table = ring_table(my_table, 15, initialize=True)

# Downstream append-only table
my_append_only_table = blink_to_append_only(my_table)
```

![The above `my_table`, `my_ring_table`, and `my_append_only_table` tables](../assets/how-to/pub-table-types.gif)

## DynamicTableWriter

[`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) writes data into live, in-memory tables by specifying the name and data types of each column. The use of [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) to write data to an in-memory ticking table generally follows a formula:

- Create the [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md).
- Get the table that [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) will write data to.
- Write data to the table (done in a separate thread).
- Close the table writer.

> [!IMPORTANT]
> In most cases, a [table publisher](#table-publisher) is the preferred way to write data to a live table. However, it may be more convenient to use [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) if you are adding very few rows (i.e., one) at a time and you prefer a simple interface. It is almost always more flexible and performant to use [`table_publisher`](../reference/table-operations/create/TablePublisher.md).

### Example: Getting started

The following example creates a table with two columns (`A` and `B`). The columns contain randomly generated integers and strings, respectively. Every second, for ten seconds, a new row is added to the table.

```python order=null ticking-table reset
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

import random, string, threading, time

# Create a DynamicTableWriter with two columns: `A`(int) and `B`(String)
table_writer = DynamicTableWriter({"A": dht.int64, "B": dht.string})

result = table_writer.table


# Function to log data to the dynamic table
def thread_func():
    # for loop that defines how much data to populate to the table
    for i in range(10):
        # the data to put into the table
        a = random.randint(1, 100)
        b = random.choice(string.ascii_letters)

        # The write_row method adds a row to the table
        table_writer.write_row(a, b)

        # seconds between new rows inserted into the table
        time.sleep(1)


# Thread to log data to the dynamic table
thread = threading.Thread(target=thread_func)
thread.start()
```

<LoopedVideo src='../assets/how-to/DynamicTableWriter_Video1.mp4' />

### Example: Trig Functions

The following example writes rows containing `X`, `sin(X)`, `cos(X)`, and `tan(X)` and plots the functions as the table updates.

```python order=null ticking-table reset
from deephaven import DynamicTableWriter
from deephaven.plot.figure import Figure
import deephaven.dtypes as dht
import numpy as np

import threading
import time

table_writer = DynamicTableWriter(
    {"X": dht.double, "SinX": dht.double, "CosX": dht.double, "TanX": dht.double}
)

trig_functions = table_writer.table


def write_data_live():
    for i in range(628):
        start = time.time()
        x = 0.01 * i
        y1 = np.sin(x)
        y2 = np.cos(x)
        y3 = np.tan(x)
        table_writer.write_row(x, y1, y2, y3)
        end = time.time()
        time.sleep(0.2 - (start - end))


thread = threading.Thread(target=write_data_live)
thread.start()

figure = Figure()
trig_fig = (
    figure.plot_xy(series_name="Sin(X)", t=trig_functions, x="X", y="SinX")
    .plot_xy(series_name="Cos(X)", t=trig_functions, x="X", y="CosX")
    .plot_xy(series_name="Tan(X)", t=trig_functions, x="X", y="TanX")
)
trig_fig = trig_fig.chart_title(title="Trig Functions")
trig_plot = trig_fig.show()
```

<LoopedVideo src='../assets/how-to/dtw_trig_functions.mp4' />

<LoopedVideo src='../assets/how-to/dtw_trig_functions_plot.mp4' />

### DynamicTableWriter and the Update Graph

Both the Python interpreter and the [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) require the [Update Graph (UG) lock](../conceptual/query-engine/engine-locking.md#query-engine-locks) to execute. As a result, new rows will not appear in output tables until the next UG cycle. As an example, what would you expect the `print` statement below to produce?

```python order=result test-set=1 reset
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

column_definitions = {"Numbers": dht.int32, "Words": dht.string}
table_writer = DynamicTableWriter(column_definitions)
result = table_writer.table
table_writer.write_row(1, "Testing")
table_writer.write_row(2, "Dynamic")
table_writer.write_row(3, "Table")
table_writer.write_row(4, "Writer")
print(result.j_table.isEmpty())
```

You may be surprised, but the table does not contain rows when the `print` statement is reached. The Python interpreter holds the UG lock while the code block executes, preventing `result` from being updated with the new rows until the next UG cycle. Because `print` is in the code block, it sees the table before rows are added.

However, calling the same `print` statement as a second command produces the expected result.

```python test-set=1
print(result.j_table.isEmpty())
```

All table updates emanate from the Periodic Update Graph. An understanding of how the Update Graph works can greatly improve query writing.

## Related documentation

- [Create new tables](./new-and-empty-table.md#empty_table)
- [Deephaven data types](./data-types.md)
- [Deephaven's table update model](../conceptual/table-update-model.md)
- [DynamicTableWriter](../reference/table-operations/create/DynamicTableWriter.md)
- [Execution Context](../conceptual/execution-context.md)
- [Install and use Python packages in Deephaven](./install-and-use-python-packages.md)
- [Query strings](./query-string-overview.md)
- [`table_publisher`](../reference/table-operations/create/TablePublisher.md)
- [Use Java packages in query strings](./install-and-use-java-packages.md#use-java-packages-in-query-strings)
