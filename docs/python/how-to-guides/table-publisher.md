---
title: Write data to an in-memory, real-time table
---

This guide covers two ways to publish data to in-memory [ticking tables](../conceptual/table-update-model.md):

- [`table_publisher`](../reference/table-operations/create/TablePublisher.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)

[`table_publisher`](../reference/table-operations/create/TablePublisher.md) publishes data to a [blink table](../conceptual/table-types.md#specialization-3-blink), while [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) writes data to an [append-only table](../conceptual/table-types.md#specialization-1-append-only). Both can ingest data from external live sources, such as WebSockets. We recommend `table_publisher` in most cases: it has a newer, more flexible, and more performant API, and it supports blink tables natively. `DynamicTableWriter` can be more convenient when you add very few rows (for example, one) at a time and want a simple interface.

## Table publisher

The [`table_publisher`](../reference/table-operations/create/TablePublisher.md) factory function creates a [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher) and its linked [blink table](../conceptual/table-types.md#specialization-3-blink). From there:

- Add data to the [blink table](../conceptual/table-types.md#specialization-3-blink) with [`add`](../reference/table-operations/create/TablePublisher.md#methods).
- (Optionally) Store [data history](#data-history) in a downstream table.
- (Optionally) Shut the publisher down when finished.

More sophisticated use cases add steps but follow the same basic formula.

### Example: getting started with a table publisher

The first four arguments of [`table_publisher`](../reference/table-operations/create/TablePublisher.md#parameters) are:

- `name`: the publisher's name.
- `col_defs`: the blink table's column definitions, such as a dictionary that maps column names to data types. See [`TableDefinitionLike`](../reference/table-operations/create/TableDefinitionLike.md) for all accepted forms.
- `on_flush_callback`: an optional function that runs at the start of each [update cycle](../conceptual/table-update-model.md).
- `on_shutdown_callback`: an optional function that runs once when the publisher shuts down.

This example passes an on-shutdown callback and no on-flush callback. [`table_publisher`](../reference/table-operations/create/TablePublisher.md) returns the linked blink table and the [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher), in that order.

The following code creates a blink table with three columns (`X`, `Y`, and `Z`). The columns contain no data until you call [`add`](../reference/table-operations/create/TablePublisher.md#methods), which the example's `add_table` function wraps.

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

Each call to `add_table` adds rows to `my_table`. The rows appear on the next [update cycle](../conceptual/table-update-model.md) after the command that adds them finishes. For details, see [Data appears on the next update cycle](#data-appears-on-the-next-update-cycle).

```python ticking-table test-set=1 order=null
add_table(10)
```

![The `my_table` blink table after data has been added](../assets/how-to/table-publisher-getting-started.png)

To stop the [`TablePublisher`](/core/pydoc/code/deephaven.stream.table_publisher.html#deephaven.stream.table_publisher.TablePublisher), call [`publish_failure`](../reference/table-operations/create/TablePublisher.md#methods). This notifies the blink table's listeners of the failure, so downstream tables show an error. It also invokes the on-shutdown callback, and later calls to [`add`](../reference/table-operations/create/TablePublisher.md#methods) return without publishing. In this example, the `when_done` function calls `publish_failure`.

```python test-set=1 order=null
when_done()
```

### Example: threading

The [getting started example](#example-getting-started-with-a-table-publisher) adds data to its blink table only when you call `add_table` by hand. In most real-world use cases, you want to add data automatically at a regular interval. Python's [threading](https://docs.python.org/3/library/threading.html) module can do this. The following example adds between 5 and 10 rows of data to `my_table` via [`empty_table`](../reference/table-operations/create/emptyTable.md) every second for 5 seconds.

> [!IMPORTANT]
> Table operations that run in a separate thread and compile query strings, such as [`update`](../reference/table-operations/select/update.md), need an [execution context](../conceptual/execution-context.md). Without one, they raise an exception. Capture the current context with [`get_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.get_exec_ctx) and open it in the thread with a `with` statement.

```python ticking-table order=null reset test-set=2
from deephaven.stream.table_publisher import table_publisher
from deephaven.execution_context import get_exec_ctx
from deephaven import dtypes as dht
from deephaven import empty_table
import random, threading, time

coldefs = {"X": dht.int32, "Y": dht.double}


def shut_down():
    print("Shutting down table publisher.")


my_table, my_publisher = table_publisher(
    name="Publisher", col_defs=coldefs, on_shutdown_callback=shut_down
)


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

### Data history

Table publishers create blink tables. Deephaven processes new data in periodic [update cycles](../conceptual/table-update-model.md). A blink table keeps only the rows added in the current cycle and drops them when the next cycle starts, so it stores no data history. In most use cases, you want to store some or all of the rows written during previous update cycles. There are two common ways to do this:

- Store some data history by creating a downstream [ring table](../conceptual/table-types.md#specialization-4-ring) with [`ring_table`](../reference/table-operations/create/ringTable.md).
- Store all data history by creating a downstream [append-only table](../conceptual/table-types.md#specialization-1-append-only) with [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md).

See the [table types user guide](../conceptual/table-types.md) for more information on these table types, including which one is best suited for your application.

The following code block builds on the [threading example](#example-threading). It reuses that example's `my_table` and `thread_func`, creates a downstream ring table and append-only table from `my_table`, and then starts a new thread that adds more data to `my_table`.

```python ticking-table order=null test-set=2
from deephaven.stream import blink_to_append_only
from deephaven import ring_table
import threading

# Downstream ring table that stores the most recent 15 rows
my_ring_table = ring_table(my_table, 15, initialize=True)

# Downstream append-only table
my_append_only_table = blink_to_append_only(my_table)

# Add more data to my_table in a new thread
thread = threading.Thread(target=thread_func)
thread.start()
```

![The above `my_table`, `my_ring_table`, and `my_append_only_table` tables](../assets/how-to/pub-table-types.gif)

### Example: asyncio

The following code block pulls cryptocurrency trade data from Coinbase's WebSocket feed. It ingests the data asynchronously on an `asyncio` event loop that runs in a background thread, so a single thread can serve several WebSocket subscriptions without blocking the console.

The code collects incoming WebSocket messages in the `my_matches` list instead of adding them to the publisher one at a time. The `on_flush_callback` passed to [`table_publisher`](../reference/table-operations/create/TablePublisher.md) runs once at the beginning of each [update cycle](../conceptual/table-update-model.md), so `on_flush` adds everything collected since the previous cycle as one table. Keep the flush callback fast, because it blocks the update cycle while it runs.

The `on_shutdown_callback` cancels the WebSocket task when the publisher shuts down.

> [!NOTE]
> Install the [websockets](https://pypi.org/project/websockets/) package before you run the code below.

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
from threading import Thread, Lock
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
    # The event loop thread appends while on_flush copies and clears, so both hold this lock
    my_matches_lock = Lock()

    def add_match(match: Match):
        with my_matches_lock:
            my_matches.append(match)

    def on_flush(tp: TablePublisher):
        # Copy and clear under the lock so no match arrives between the two steps.
        # Build the table after releasing the lock, so the event loop isn't blocked.
        with my_matches_lock:
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
        handle_matches(product_ids, add_match), event_loop
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

## `DynamicTableWriter`

[`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) writes data into a live, in-memory table whose column names and data types you define. To use it:

- Create the [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md).
- Get the table that the writer writes to.
- Write data to the table, typically from a separate thread so that rows appear while writing continues (see [Data appears on the next update cycle](#data-appears-on-the-next-update-cycle)).
- Close the writer.

### Example: getting started with `DynamicTableWriter`

The following example creates a table with two columns (`A` and `B`). The columns contain randomly generated integers and strings, respectively. A separate thread adds a new row every second for ten seconds. When the thread finishes writing, it closes the writer with [`close`](../reference/table-operations/create/DynamicTableWriter.md#methods).

```python order=null ticking-table reset
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

import random, string, threading, time

# Create a DynamicTableWriter with two columns: `A` (int64) and `B` (string)
table_writer = DynamicTableWriter({"A": dht.int64, "B": dht.string})

result = table_writer.table


# Function to log data to the dynamic table
def thread_func():
    # for loop that defines how much data to populate to the table
    for i in range(10):
        # the data to put into the table
        a = random.randint(1, 100)
        b = random.choice(string.ascii_letters)

        # write_row queues a row; it appears on the next update cycle
        table_writer.write_row(a, b)

        # seconds between new rows inserted into the table
        time.sleep(1)

    # Close the writer once all rows are written
    table_writer.close()


# Thread to log data to the dynamic table
thread = threading.Thread(target=thread_func)
thread.start()
```

<LoopedVideo src='../assets/how-to/DynamicTableWriter_Video1.mp4' />

### Example: trig functions

The following example writes rows with an `X` column and its sine, cosine, and tangent in the `SinX`, `CosX`, and `TanX` columns, and plots the sine and cosine with [Deephaven Express](/core/plotly/docs/) as the table updates.

```python order=null ticking-table reset
from deephaven import DynamicTableWriter
import deephaven.plot.express as dx
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
        time.sleep(max(0, 0.2 - (end - start)))

    table_writer.close()


thread = threading.Thread(target=write_data_live)
thread.start()

trig_plot = dx.line(trig_functions, x="X", y=["SinX", "CosX"], title="Trig Functions")
```

<LoopedVideo src='../assets/how-to/dtw_trig_functions.mp4' />

<LoopedVideo src='../assets/how-to/dtw_trig_functions_plot.mp4' />

## Data appears on the next update cycle

Neither a table publisher nor a [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) adds rows to its table immediately. A table publisher's [`add`](../reference/table-operations/create/TablePublisher.md#methods) and a writer's [`write_row`](../reference/table-operations/create/DynamicTableWriter.md#methods) both queue the new rows. The queued rows reach the table when the table's update source next refreshes, which happens once per cycle of the [Update Graph (UG)](../conceptual/dag.md#update-graph-ug-cycles). The UG is the engine component that processes table updates. Rows queued from a background thread can appear in a cycle that is already running, if they arrive before that refresh. Rows added from a table publisher's on-flush callback, which runs at the start of a cycle, appear in that same cycle.

The Python script session holds the exclusive [UG lock](../conceptual/query-engine/engine-locking.md#query-engine-locks) while a command executes, so that cycle cannot run until the command finishes. As a result, new rows do not appear in output tables while the command that wrote them is still running.

The following example shows this with a [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md), whose append-only table keeps the rows so you can inspect them afterward. What would you expect the `print` statement below to produce?

```python ticking-table order=:log test-set=3 reset
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

column_definitions = {"Numbers": dht.int32, "Words": dht.string}
table_writer = DynamicTableWriter(column_definitions)
result = table_writer.table
table_writer.write_row(1, "Testing")
table_writer.write_row(2, "Dynamic")
table_writer.write_row(3, "Table")
table_writer.write_row(4, "Writer")
print(result.size == 0)
```

The `print` statement prints `True`: it runs in the same command as the `write_row` calls, so the UG cycle that adds the queued rows to `result` has not run yet.

Run the same `print` statement as a second command, and it prints `False`.

```python test-set=3 order=:log
print(result.size == 0)
```

In a standard Deephaven server, the [Periodic Update Graph](../conceptual/periodic-update-graph-configuration.md) drives these update cycles. To learn how update cycles propagate changes through your queries, see [Deephaven's table update model](../conceptual/table-update-model.md).

## Related documentation

- [Create static tables](./new-and-empty-table.md#empty_table)
- [Data types in Deephaven and Python](./data-types.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)
- [Execution Context](../conceptual/execution-context.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Install and use Python packages in Deephaven](./install-and-use-python-packages.md)
- [Query strings](./query-string-overview.md)
- [`table_publisher`](../reference/table-operations/create/TablePublisher.md)
- [Use Java packages in query strings](./install-and-use-java-packages.md#use-java-packages-in-query-strings)
