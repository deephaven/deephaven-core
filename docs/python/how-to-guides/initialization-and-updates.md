---
title: Initialization and updates
---

Deephaven code often looks like it runs sequentially, line by line, just like standard Python. But with ticking tables, which update as new data arrives, the data your code reads may not have arrived yet — and this surprises many developers.

This guide explains:

- Why ticking table code behaves differently than standard Python.
- The two phases of execution: **initialization** and **updates**.
- How to safely mix Python code with Deephaven ticking tables.
- Clear "do this, don't do that" guidelines.

## Prerequisites

Before reading this guide, you should understand:

- [How to create and use tables](./new-and-empty-table.md)
- [Table types](../conceptual/table-types.md) (static vs refreshing)
- Basic [table operations](./use-select-view-update.md)

For details on how Deephaven processes updates internally, see [Incremental update model](../conceptual/table-update-model.md).

## The surprise: a new ticking table starts empty

Consider this code:

```python syntax
from deephaven import kafka_consumer
import deephaven.pandas as dhpd

# This LOOKS like it reads the first trade, but the table is still empty here!
trades = kafka_consumer.consume(...)  # Create ticking table
first_price = dhpd.to_pandas(trades.first_by("Symbol"))["Price"][0]  # Get first price
print(f"First price: {first_price}")
```

In standard Python, this would work fine — create the data source, then read from it. But with a [Kafka consumer](./data-import-export/kafka-stream.md), or any other external source whose data arrives only in later update cycles, the table is **empty** when you try to read from it. The data hasn't arrived yet, and it can't arrive while your script is running unless the script waits for it with [`await_update`](../reference/table-operations/table-listeners/await-update.md), as [Update cycles and the exclusive lock](#update-cycles-and-the-exclusive-lock) explains.

This is the core issue: **Deephaven table operations define relationships that the engine keeps up to date as data arrives. They don't run once over data that already exists.**

## Two phases: initialization and updates

Deephaven execution has two distinct phases:

### Phase 1: Initialization

When your script runs, table operations execute and build the [DAG](../conceptual/dag.md) (dependency graph), and initial calculations run on any existing data.

**Tables fed by external sources, such as [Kafka](./data-import-export/kafka-stream.md), have no data from those sources yet.**

### Phase 2: Real-time updates

After initialization completes, or while the script waits in [`await_update`](../reference/table-operations/table-listeners/await-update.md):

1. Data flows in from sources such as [Kafka](./data-import-export/kafka-stream.md) and [time tables](./time-table.md).
2. The engine propagates changes through the DAG.
3. Derived tables update incrementally.

The problem occurs when you try to extract data to Python during initialization, before the real-time data has arrived.

## Update cycles and the exclusive lock

To see why the data can't arrive while your script runs, it helps to understand the update cycle in more detail.

The engine component that processes real-time updates is the _update graph_. During the real-time updates phase, it repeats _update cycles_: in each cycle, it applies new data from sources and propagates the changes through the DAG. Between cycles, it is idle.

- **Between cycles (idle)**: Tables are consistent. Console commands and [Application Mode](./application-mode.md) scripts run here.
- **During a cycle (updating)**: Sources receive data, changes propagate through the DAG, and [listeners](./table-listeners-python.md) fire. Listeners receive the cycle's changes as a consistent [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) object, as [Pattern 4](#pattern-4-use-listeners-with-tableupdate) shows.

Deephaven runs each console command and [Application Mode](./application-mode.md) script while holding the update graph's [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock), so no update cycle can run while your script runs. This has two consequences:

- Reads in your script see a consistent table. Your script doesn't need a lock or a snapshot for consistency.
- Data from external sources can't arrive until the script finishes, or until it calls [`await_update`](../reference/table-operations/table-listeners/await-update.md), which releases the lock while it waits.

Code on threads you start yourself doesn't hold this lock. To keep an update cycle from starting while that code reads, hold the update graph's [shared lock](../conceptual/query-engine/engine-locking.md#shared-lock), as [Pattern 6](#pattern-6-use-locking-for-consistent-reads-from-your-own-threads-advanced) shows.

## Quick reference

Here's a quick summary of the patterns. Details follow in the sections below.

### Do this, don't do that

| DO                                                                                                                                                                                   | DON'T                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| Use table operations — let the engine handle consistency.                                                                                                                            | Pull values into Python variables when a table operation can compute the result.       |
| Wait for data with [`await_update`](../reference/table-operations/table-listeners/await-update.md) before extracting.                                                                | Extract data right after creating a ticking table, assuming the data is already there. |
| Use [listeners](./table-listeners-python.md) for reacting to updates, and declare every other table they read as a [dependency](./table-listeners-python.md#dependent-tables).       | Access tables other than the source or declared dependencies inside listeners.         |
| Compare before/after values within one update with [`modified_prev`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev).             | Mix data from different update cycles.                                                 |
| Take the [shared lock](../conceptual/query-engine/engine-locking.md#shared-lock) only for quick extractions, then process outside it.                                                | Hold the update graph lock during long computations.                                   |
| Initialize state with [`do_replay=True`](./table-listeners-python.md#replay-data) or with a [`snapshot`](../reference/table-operations/snapshot/snapshot.md) taken before listening. | Start a stateful listener without the table's existing rows.                           |

### Common scenarios

| Scenario                                        | Recommended approach                                                                                                                                                                                                                                                                                                     |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Wait for Kafka/external data before extracting  | [`await_update`](../reference/table-operations/table-listeners/await-update.md) before extraction.                                                                                                                                                                                                                       |
| Export ticking data to pandas periodically      | [`snapshot_when`](../reference/table-operations/snapshot/snapshot-when.md) with a trigger table, and a [listener](./table-listeners-python.md) on the result. Each trigger delivers the whole new snapshot as [`added`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added) rows. |
| React to updates and call an external API       | [Listener](./table-listeners-python.md) with [`added`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added) data access.                                                                                                                                                           |
| Aggregate ticking data with custom Python logic | Built-in aggregations with [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) or [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) where they cover the logic; otherwise, call [Python functions](./python-functions.md) in formulas.                                    |
| Initialize ML model from table state            | [`do_replay=True`](./table-listeners-python.md#replay-data) in listener, or [`snapshot`](../reference/table-operations/snapshot/snapshot.md) before listening.                                                                                                                                                           |
| Join ticking data with an external database     | Take a [`snapshot`](../reference/table-operations/snapshot/snapshot.md) of the ticking table, then join in Python.                                                                                                                                                                                                       |
| Debug why data looks inconsistent               | Check whether your own threads read several tables without holding one shared lock around all the reads. Check whether listeners read tables they don't declare as dependencies. Add logging to confirm which cycle each read sees.                                                                                      |

## Waiting for data: the `await_update` pattern

The most common initialization problem is trying to read data before it arrives. In an [Application Mode](./application-mode.md) startup script that needs a value from an external source once, at startup, use [`await_update`](../reference/table-operations/table-listeners/await-update.md) to pause until the table receives an update:

```python syntax
from deephaven import kafka_consumer as ck
from deephaven import agg
import deephaven.pandas as dhpd
import deephaven.dtypes as dht

# Create a Kafka consumer (ticking table)
trades = ck.consume(
    {"bootstrap.servers": "redpanda:29092"},
    "trades",
    key_spec=ck.KeyValueSpec.IGNORE,
    value_spec=ck.json_spec(
        {
            "Timestamp": dht.Instant,
            "Symbol": dht.string,
            "Size": dht.double,
            "Price": dht.double,
        },
        mapping={
            "ts": "Timestamp",
            "symbol": "Symbol",
            "size": "Size",
            "price": "Price",
        },
    ),
    table_type=ck.TableType.ring(10000),
)

# Wait for the first update before extracting data
while trades.size == 0:
    trades.await_update()

# Now it's safe to extract - data has arrived
first_ts = trades.agg_by([agg.min_("Timestamp")])
first_ts_py = dhpd.to_pandas(first_ts).iloc[0, 0]
```

> [!NOTE]
> [`await_update`](../reference/table-operations/table-listeners/await-update.md) waits for the table to update. It can occasionally return early on a spurious wakeup, so the example loops until the table has rows.
>
> You can also specify a timeout in milliseconds: `trades.await_update(timeout=5000)` returns `False` if the timeout is reached before an update occurs.

### When not to use `await_update`

- **In listeners** — Listeners already receive data when it's ready.
- **For continuous processing** — Use table operations or listeners instead.
- **In the console** — Console code runs interactively. Wait for data to arrive, then run the next command.

## Safe patterns for reading table data

Once data has arrived, or when you don't need to wait for it, these patterns get results out of ticking tables: keep the logic in the engine, keep a static copy, or take copies on a schedule.

### Pattern 1: Let the engine do the work

The safest approach is to express your logic using Deephaven table operations rather than extracting data to Python. The engine guarantees consistency for all table operations.

```python test-set=init-safe ticking-table order=null
from deephaven import time_table

# DO: Use table operations - engine handles consistency
source = time_table("PT1s").update(["X = ii", "Y = X * 2"])

# Filtering and aggregating - all safe
filtered = source.where("X > 10")
aggregated = source.view(["X", "Y"]).sum_by()
```

If you need custom Python logic, consider calling a [Python function](./python-functions.md) in a formula. Keep the function to its arguments: the engine keeps the column values it passes consistent, but not other state the function reads.

```python test-set=init-safe ticking-table order=null
def custom_calculation(x: int, y: int) -> int:
    # Your Python logic here
    return x**2 + y


# DO: Python functions in formulas - the engine passes consistent column values
result = source.update("Z = custom_calculation(X, Y)")
```

### Pattern 2: Use `snapshot` for point-in-time extraction

When you want a static Deephaven table that holds a ticking table's current rows, use [`snapshot`](../reference/table-operations/snapshot/snapshot.md). This example starts the [time table](./time-table.md) 10 seconds in the past, so it already has rows when the snapshot runs:

```python ticking-table order=null
from deephaven import time_table
import deephaven.pandas as dhpd
import datetime

ten_seconds_ago = datetime.datetime.now() - datetime.timedelta(seconds=10)
source = time_table("PT0.5s", start_time=ten_seconds_ago).update("X = ii")

# DO: Keep a static copy that later update cycles won't change
static_copy = source.snapshot()
df = dhpd.to_pandas(static_copy)

# static_copy and df both keep these rows after the source ticks
print(f"Captured {len(df)} rows")
```

The snapshot creates a static table that won't change, so you can process it in Python without later update cycles changing the data underneath you.

> [!NOTE]
> [`to_pandas`](../reference/pandas/to-pandas.md) takes its own snapshot of a refreshing table, so calling it directly on a ticking table also returns a consistent DataFrame. Use [`snapshot`](../reference/table-operations/snapshot/snapshot.md) when you want a static Deephaven table to keep working with.

### Pattern 3: Use `snapshot_when` for controlled timing

When you need to extract data at specific intervals or in sync with other events, use [`snapshot_when`](../reference/table-operations/snapshot/snapshot-when.md):

```python ticking-table order=null
from deephaven import time_table

# Data source updating frequently
source = time_table("PT0.1s").update("X = ii")

# The "trigger" table updates every 5 seconds
trigger = time_table("PT5s").rename_columns("TriggerTime = Timestamp")

# On every change to "trigger", snapshot with a consistent view
periodic_snapshot = source.snapshot_when(trigger)
```

[`snapshot_when`](../reference/table-operations/snapshot/snapshot-when.md) is useful for:

- Reducing update frequency for downstream processing.
- Synchronizing data extraction with external systems.
- Batching updates for efficiency.

To export each snapshot to pandas, attach a [listener](./table-listeners-python.md) to `periodic_snapshot`. Each trigger delivers the whole new snapshot as [`added`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added) rows, so the dictionary that `added` returns holds the full set of rows to export.

## Safe patterns for reacting to updates

### Pattern 4: Use listeners with `TableUpdate`

When you use [table listeners](./table-listeners-python.md), the [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) object gives you safe, consistent access to changed data:

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen


def on_update(update, is_replay):
    # DO: Access data through TableUpdate methods
    added_data = update.added()
    if added_data:
        print(f"New rows: {added_data}")


source = time_table("PT1s").update("X = ii")
handle = listen(source, on_update)
```

The [`added`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added), [`modified`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified), [`removed`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.removed), and [`modified_prev`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev) methods return dictionaries with column names as keys and NumPy arrays as values. This data is a consistent snapshot from the current update cycle.

> [!CAUTION]
> Keep listener execution fast. Until a running listener returns, the current update cycle can't finish and the next one can't start.

### Pattern 5: Initialize listener state from existing rows

A listener that keeps state, such as a running total, usually needs to start from the rows the table already has. There are two ways to do that:

- **Replay with [`do_replay=True`](./table-listeners-python.md#replay-data)**: The listener receives the existing rows as its first update. Use this when one function can handle both initialization and later updates.
- **[Snapshot](../reference/table-operations/snapshot/snapshot.md) before listening**: Your code processes a snapshot of the existing rows, then the listener handles later updates. Use this when initialization needs whole-table logic, such as a pandas computation.

#### With `do_replay`

When setting up a listener with [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen), use [`do_replay=True`](./table-listeners-python.md#replay-data) to receive the table's existing data as the first update:

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen
import datetime


class StatefulListener:
    def __init__(self):
        self.running_sum = 0

    def on_update(self, update, is_replay):
        added = update.added()
        if added:
            # DO: Initialize from replay, then process updates
            self.running_sum += sum(added.get("X", []))
            if is_replay:
                print(f"Initialized with sum: {self.running_sum}")
            else:
                print(f"Updated sum: {self.running_sum}")


ten_seconds_ago = datetime.datetime.now() - datetime.timedelta(seconds=10)
source = time_table("PT1s", start_time=ten_seconds_ago).update("X = ii")
listener = StatefulListener()

# do_replay=True delivers existing rows as first update
handle = listen(source, listener.on_update, do_replay=True)
```

#### With a snapshot before listening

When initialization needs whole-table logic, take a [`snapshot`](../reference/table-operations/snapshot/snapshot.md) first, then attach a listener for ongoing updates. Because the script holds the [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock), no update cycle runs between the snapshot and the [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) call, so the listener doesn't miss any rows:

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen
import deephaven.pandas as dhpd
import datetime


def initialize_from_table(table):
    """Initialize state from current table contents."""
    snapshot = table.snapshot()
    df = dhpd.to_pandas(snapshot)
    initial_state = {"count": len(df), "sum": df["X"].sum() if len(df) > 0 else 0}
    print(f"Initialized: {initial_state}")
    return initial_state


def create_update_handler(state):
    """Create a listener that updates the state."""

    def on_update(update, is_replay):
        added = update.added()
        if added and "X" in added:
            state["count"] += len(added["X"])
            state["sum"] += sum(added["X"])
            print(f"Updated: {state}")

    return on_update


ten_seconds_ago = datetime.datetime.now() - datetime.timedelta(seconds=10)
source = time_table("PT1s", start_time=ten_seconds_ago).update("X = ii")

# DO: Initialize from snapshot, then listen for updates
state = initialize_from_table(source)
handle = listen(source, create_update_handler(state))
```

## Safe patterns for your own threads

### Pattern 6: Use locking for consistent reads from your own threads (advanced)

Code on a thread you start yourself doesn't hold the [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock) that script code runs under. When several reads on such a thread must see the same update cycle, hold the update graph's shared lock around all of them with [`shared_lock`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.shared_lock).

A single [`to_pandas`](../reference/pandas/to-pandas.md) call doesn't need the lock, because `to_pandas` already snapshots its table.

In this example, extracting two tables under one lock guarantees both DataFrames come from the same cycle:

```python ticking-table order=null
from deephaven import time_table
from deephaven.execution_context import get_exec_ctx
from deephaven.update_graph import shared_lock
import deephaven.pandas as dhpd
import threading

source = time_table("PT0.5s").update("X = ii")
totals = source.view("X").sum_by()

# Reuse the script's execution context
ctx = get_exec_ctx()


def extract_data_safely():
    with ctx:
        # DO: Hold one lock so both extractions see the same update cycle
        with shared_lock(source):
            df = dhpd.to_pandas(source)
            totals_df = dhpd.to_pandas(totals)
    print(
        f"Extracted {len(df)} rows and {len(totals_df)} total row(s) from the same cycle"
    )


# Run the extraction on a thread you start yourself
thread = threading.Thread(target=extract_data_safely)
thread.start()
```

The thread reuses the script's [execution context](../conceptual/execution-context.md), because threads you start don't have one.

The thread can't take the shared lock until the script finishes and releases the exclusive lock. Don't call `thread.join` in the same script, or the script hangs.

> [!CAUTION]
> Keep lock duration minimal. See [Don't hold locks longer than necessary](#dont-hold-locks-longer-than-necessary).

## Unsafe patterns

### Don't read undeclared tables inside listeners

```python syntax
from deephaven.table_listener import listen
import deephaven.pandas as dhpd


def bad_listener(update, is_replay):
    # DON'T: Read tables that aren't declared as dependencies
    other_data = dhpd.to_pandas(
        other_table
    )  # Dangerous - may not reflect this update cycle!


handle = listen(source, bad_listener)
```

**What can go wrong:**

- The engine doesn't guarantee that other tables have finished updating for the current cycle.
- You may see partial or stale data from other tables.

**Do instead:** Within a listener, access only the table you are listening to and the tables you declare as [dependencies](./table-listeners-python.md#dependent-tables):

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen

source = time_table("PT1s").update("X = ii")

# DO: Create derived tables BEFORE the listener
filtered = source.where("X > 5")
derived = source.update("Y = X * 2")


def good_listener(update, is_replay):
    # Safe to READ from dependencies
    print(f"Source update: {update.added()}, filtered rows: {filtered.size}")


# Add derived tables as dependencies
handle = listen(source, good_listener, dependencies=[filtered, derived])
```

### Don't mix data from different update cycles

```python syntax
class BadListener:
    def __init__(self):
        self.previous_data = None

    def on_update(self, update, is_replay):
        current = update.added()
        if self.previous_data is not None:
            # DON'T: Compare data across cycles without care
            # self.previous_data is from a DIFFERENT cycle
            diff = compare(self.previous_data, current)
        self.previous_data = current  # Stale next cycle!
```

**What can go wrong:**

- `self.previous_data` holds the rows added in the previous cycle, not the earlier values of the rows in this update, so the comparison pairs unrelated rows.
- If you store row keys instead, they may no longer be valid in a later cycle, because rows can be removed or shifted. See [Incremental update model](../conceptual/table-update-model.md).

**Do instead:** Choose one of these approaches:

- For a before/after comparison, compare the previous and current values of modified rows within the same update. [`modified_prev`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev) returns the previous values of the modified rows.
- To carry information to the next cycle, store only computed results. Don't store raw row data.

### Don't hold locks longer than necessary

When code on a thread you start yourself holds the [shared lock](../conceptual/query-engine/engine-locking.md#shared-lock), as in [Pattern 6](#pattern-6-use-locking-for-consistent-reads-from-your-own-threads-advanced), no update cycle can run.

Script code holds the [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock) from start to finish, so a long computation in the script itself also blocks updates. Run long work on your own thread, or in a listener that hands it off to another thread.

```python syntax
from deephaven.execution_context import get_exec_ctx
from deephaven.update_graph import shared_lock
import deephaven.pandas as dhpd

# source and totals are the tables from Pattern 6
ctx = get_exec_ctx()


def extract_and_process_badly():
    with ctx:
        with shared_lock(source):
            df = dhpd.to_pandas(source)
            totals_df = dhpd.to_pandas(totals)
            # DON'T: Long computation while holding the lock
            result = expensive_ml_training(df, totals_df)  # Blocks ALL updates!
            save_to_database(result)  # I/O while locked!
```

**What can go wrong:**

- The lock blocks the entire update graph.
- All ticking tables freeze.
- Console commands wait, because they need the exclusive lock.

**Do instead:** Extract data quickly, release the lock, then process:

```python syntax
from deephaven.execution_context import get_exec_ctx
from deephaven.update_graph import shared_lock
import deephaven.pandas as dhpd

# source and totals are the tables from Pattern 6
ctx = get_exec_ctx()


def extract_then_process():
    with ctx:
        # DO: Minimize lock duration
        with shared_lock(source):
            df = dhpd.to_pandas(source)  # Quick extractions only
            totals_df = dhpd.to_pandas(totals)

    # Process OUTSIDE the lock
    result = expensive_ml_training(df, totals_df)
    save_to_database(result)
```

## Related documentation

- [Table listeners](./table-listeners-python.md) — React to table updates
- [Incremental update model](../conceptual/table-update-model.md) — Deep dive into how updates work
- [Synchronization and locking](../conceptual/query-engine/engine-locking.md) — Advanced locking mechanisms
- [Table types](../conceptual/table-types.md) — Static vs refreshing tables
- [Reduce update frequency](./performance/reduce-update-frequency.md) — Optimize update processing
