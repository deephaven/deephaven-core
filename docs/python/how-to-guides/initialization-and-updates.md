---
title: Initialization and updates
sidebar_label: Initialization and updates
---

Deephaven code often looks like it runs sequentially, line by line, just like standard Python. But with ticking tables, that's not quite what happens — and this surprises many developers.

This guide explains:

- Why ticking table code behaves differently than standard Python.
- The two phases of execution: **initialization** and **updates**.
- How to safely mix Python code with Deephaven ticking tables.
- Clear "do this, don't do that" guidelines.

## The surprise: code isn't always sequential

Consider this code:

```python syntax
# This LOOKS sequential, but it's not!
trades = kafka_consumer.consume(...)  # Create ticking table
first_price = trades.first_by("Symbol").to_pandas()["Price"][0]  # Get first price
print(f"First price: {first_price}")
```

In standard Python, this would work fine — create the data source, then read from it. But with a Kafka consumer (or any external data source), the table might be **empty** when you try to read from it. The data hasn't arrived yet.

This is the core issue: **Deephaven table operations define relationships, not sequential data transformations.**

## Two phases: initialization and updates

Deephaven execution has two distinct phases:

### Phase 1: Initialization

When your script runs:

1. Table operations execute and build the [DAG](../conceptual/dag.md) (dependency graph).
2. Initial calculations run on any existing data.
3. **External data sources may not have data yet.**

### Phase 2: Real-time updates

After initialization completes:

1. Data flows in from sources (Kafka, time tables, etc.).
2. Changes propagate through the DAG automatically.
3. Derived tables update incrementally.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Script Execution                             │
├─────────────────────────────┬───────────────────────────────────┤
│  INITIALIZATION             │  REAL-TIME UPDATES                │
│  (script runs)              │  (automatic, continuous)          │
├─────────────────────────────┼───────────────────────────────────┤
│ • Tables created            │ • Data arrives from sources       │
│ • DAG built                 │ • Changes propagate through DAG   │
│ • Initial calcs on existing │ • Listeners fire                  │
│   data (may be empty!)      │ • UI updates                      │
│ • Python extractions run    │                                   │
│   HERE - possibly too early │                                   │
└─────────────────────────────┴───────────────────────────────────┘
```

The problem occurs when you try to extract data to Python during initialization, before the real-time data has arrived.

## Prerequisites

Before reading this guide, you should understand:

- [How to create and use tables](./new-and-empty-table.md)
- [Table types](../conceptual/table-types.md) (static vs refreshing)
- Basic [table operations](./use-select-view-update.md)

For details on how Deephaven processes updates internally, see [Incremental update model](../conceptual/table-update-model.md).

## Do this, don't do that

Here's the quick reference. Details follow in the sections below.

| DO                                                                                                                 | DON'T                                                                               |
| ------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| Use table operations — let the engine handle consistency.                                                          | Extract data to Python immediately after creating a ticking table.                  |
| Wait for data with [`await_update()`](../reference/table-operations/await_update.md) before extracting.            | Assume data exists just because you created the table.                              |
| Use [`snapshot()`](../reference/table-operations/snapshot/snapshot.md) to get a static copy for Python processing. | Call [`to_pandas()`](../reference/pandas/to-pandas.md) directly on a ticking table. |
| Use [listeners](./table-listeners-python.md) for reacting to updates.                                              | Perform table operations inside listeners.                                          |
| Initialize state with `do_replay=True`.                                                                            | Mix data from different update cycles.                                              |

## Waiting for data: the `await_update()` pattern

The most common initialization problem is trying to read data before it arrives. Use [`await_update()`](../reference/table-operations/await_update.md) to pause until the table receives an update:

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
        [
            ("ts", dht.Instant),
            ("symbol", dht.string),
            ("size", dht.double),
            ("price", dht.double),
        ]
    ),
    table_type=ck.TableType.ring(10000),
)

# Wait for the first update before extracting data
trades.await_update()

# Now it's safe to extract - data has arrived
first_ts = trades.agg_by([agg.min_("ts")])
first_ts_py = dhpd.to_pandas(first_ts).iloc[0, 0]
```

> [!NOTE]
> `await_update()` blocks until the table receives at least one update. In app mode startup scripts, this ensures your initialization code waits for data to flow in before proceeding.
>
> You can also specify a timeout in milliseconds: `trades.await_update(timeout=5000)` returns `False` if the timeout is reached before an update occurs.

### When to use `await_update()`

- **App mode startup scripts** — When you need to extract initial values from external data sources.
- **Sequential initialization** — When later code depends on data from earlier tables.
- **One-time setup** — When you need a value once at startup, not continuously.

### When NOT to use `await_update()`

- **In listeners** — Listeners already receive data when it's ready.
- **For continuous processing** — Use table operations or listeners instead.
- **In the console** — Console code runs interactively; just wait and re-run.

## Safe patterns for initialization

### Pattern 1: Let the engine do the work

The safest approach is to express your logic using Deephaven table operations rather than extracting data to Python. The engine guarantees consistency for all table operations.

```python test-set=init-safe order=null
from deephaven import time_table

# DO: Use table operations - engine handles consistency
source = time_table("PT1s").update(["X = ii", "Y = X * 2"])

# Filtering, aggregating, joining - all safe
filtered = source.where("X > 10")
aggregated = source.view(["X", "Y"]).sum_by()
```

If you need custom Python logic, consider using it in a formula via [Python functions](./python-functions.md):

```python test-set=init-safe order=null
def custom_calculation(x, y):
    # Your Python logic here
    return x**2 + y


# DO: Python functions in formulas - engine handles consistency
result = source.update("Z = custom_calculation(X, Y)")
```

### Pattern 2: Use `snapshot()` for point-in-time extraction

When you need a static copy of ticking data for Python processing, use [`snapshot()`](../reference/table-operations/snapshot/snapshot.md):

```python ticking-table order=null
from deephaven import time_table
import deephaven.pandas as dhpd

source = time_table("PT0.5s").update("X = ii")

# DO: Snapshot first, then extract
static_copy = source.snapshot()
df = dhpd.to_pandas(static_copy)

# Now df is a regular pandas DataFrame you can process safely
print(f"Captured {len(df)} rows")
```

The snapshot creates a static table that won't change, making it safe to extract and process in Python.

### Pattern 3: Use `snapshot_when()` for controlled timing

When you need to extract data at specific intervals or synchronized with other events, use [`snapshot_when()`](../reference/table-operations/snapshot/snapshot-when.md):

```python ticking-table order=null
from deephaven import time_table

# Data source updating frequently
source = time_table("PT0.1s").update("X = ii")

# Trigger table controls when snapshots occur
trigger = time_table("PT5s").rename_columns("TriggerTime = Timestamp")

# DO: Snapshot at controlled intervals
periodic_snapshot = source.snapshot_when(trigger)

# periodic_snapshot updates every 5 seconds with a consistent view of source
```

This is useful for:

- Reducing update frequency for downstream processing.
- Synchronizing data extraction with external systems.
- Batching updates for efficiency.

## Safe patterns for reacting to updates

### Pattern 4: Use listeners with `TableUpdate`

When using [table listeners](./table-listeners-python.md), the `TableUpdate` object provides safe, consistent access to changed data:

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

The `.added()`, `.modified()`, `.removed()`, and `.modified_prev()` methods return dictionaries with column names as keys and NumPy arrays as values. This data is a consistent snapshot from the update cycle.

### Pattern 5: Initialize state with `do_replay`

When setting up a listener, use `do_replay=True` to receive the table's existing data as the first update:

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen


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


source = time_table("PT1s").update("X = ii")
listener = StatefulListener()

# do_replay=True delivers existing rows as first update
handle = listen(source, listener.on_update, do_replay=True)
```

### Pattern 6: Initialize with a snapshot before listening

For complex initialization, take a snapshot first, then attach a listener for ongoing updates:

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen
import deephaven.pandas as dhpd


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


source = time_table("PT1s").update("X = ii")

# DO: Initialize from snapshot, then listen for updates
state = initialize_from_table(source)
handle = listen(source, create_update_handler(state))
```

### Pattern 7: Use locking for direct table access (advanced)

For advanced use cases where you need to read directly from a ticking table, use the update graph lock:

```python ticking-table order=null
from deephaven import time_table
from deephaven.update_graph import exclusive_lock
import deephaven.pandas as dhpd

source = time_table("PT0.5s").update("X = ii")


def extract_data_safely():
    # DO: Use lock for direct access to ticking data
    with exclusive_lock(source):
        # Table is guaranteed consistent while lock is held
        df = dhpd.to_pandas(source)
        return df


# Call when needed
df = extract_data_safely()
print(f"Extracted {len(df)} rows safely")
```

> [!CAUTION]
> Keep lock duration minimal. While you hold the lock, the update graph cannot process new data. Long-running computations under the lock will block all table updates.

## Unsafe patterns

### DON'T read ticking data without synchronization

```python should-fail
from deephaven import time_table

source = time_table("PT0.1s").update("X = ii")

# DON'T: Direct extraction from ticking table
df = source.to_pandas()  # May get inconsistent data!
```

**What can go wrong:**

- The table might be mid-update when you read.
- Some columns could have new values, others old values.
- Row keys might have shifted.

**Do instead:** Use `snapshot()` first, or use locking.

### DON'T perform table operations inside listeners

```python should-fail
from deephaven.table_listener import listen


def bad_listener(update, is_replay):
    # DON'T: Create derived tables in listeners
    filtered = source.where("X > 10")  # Dangerous!
    new_table = source.update("Y = X * 2")  # Also dangerous!


handle = listen(source, bad_listener)
```

**What can go wrong:**

- Update graph conflicts and exceptions.
- Derived tables may be in inconsistent states.
- Memory leaks from repeatedly created tables.

**Do instead:** Create derived tables before attaching the listener, then add them as [dependencies](./table-listeners-python.md#dependent-tables):

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen

source = time_table("PT1s").update("X = ii")

# DO: Create derived tables BEFORE the listener
filtered = source.where("X > 5")
derived = source.update("Y = X * 2")


def good_listener(update, is_replay):
    # Safe to READ from dependencies
    print(f"Source update: {update.added()}")


# Add derived tables as dependencies
handle = listen(source, good_listener, dependencies=[filtered, derived])
```

### DON'T mix data from different update cycles

```python should-fail
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

- Row keys from previous cycles may no longer be valid.
- Comparisons assume consistency that doesn't exist.

**Do instead:** Use `modified_prev()` for before/after comparisons within the same update, or store only computed results (not raw data) between cycles.

### DON'T hold locks longer than necessary

```python should-fail
from deephaven.update_graph import exclusive_lock

with exclusive_lock(source):
    df = source.to_pandas()
    # DON'T: Long computation while holding the lock
    result = expensive_ml_training(df)  # Blocks ALL updates!
    save_to_database(result)  # I/O while locked!
```

**What can go wrong:**

- The entire update graph is blocked.
- All ticking tables freeze.
- User interfaces become unresponsive.

**Do instead:** Extract data quickly, release the lock, then process:

```python syntax
from deephaven.update_graph import exclusive_lock

# DO: Minimize lock duration
with exclusive_lock(source):
    df = source.to_pandas()  # Quick extraction

# Process OUTSIDE the lock
result = expensive_ml_training(df)
save_to_database(result)
```

## Common scenarios

| Scenario                                        | Recommended approach                                                                 |
| ----------------------------------------------- | ------------------------------------------------------------------------------------ |
| Wait for Kafka/external data before extracting  | `table.await_update()` before extraction.                                            |
| Export ticking data to pandas periodically      | `snapshot_when()` with a trigger table, then `to_pandas()`.                          |
| React to updates and call external API          | Listener with `added()` data access.                                                 |
| Aggregate ticking data with custom Python logic | Use table operations (`.agg_by()`, `.update_by()`), or Python functions in formulas. |
| Initialize ML model from table state            | `do_replay=True` in listener, or snapshot before listening.                          |
| Join ticking data with external database        | Snapshot the ticking table, then join in Python.                                     |
| Debug why data looks inconsistent               | Check if you're reading without synchronization; add logging to listener.            |

## Understanding the update cycle

For debugging and advanced use cases, it helps to understand the update cycle in more detail:

1. **Idle phase**: Tables are consistent. Console commands run here (under exclusive lock automatically).
2. **Update phase**: Changes propagate through the DAG. Listeners fire. Data is in flux.

```
┌─────────────────────────────────────────────────────────────┐
│                      Update Cycle                            │
├─────────────┬─────────────────────────────┬─────────────────┤
│  Idle       │  Updating                   │  Idle           │
│  (safe)     │  (data in flux)             │  (safe)         │
├─────────────┼─────────────────────────────┼─────────────────┤
│ • Console   │ • Sources receive data      │ • Console       │
│   commands  │ • DAG propagates changes    │   commands      │
│ • Snapshots │ • Listeners fire            │ • Snapshots     │
│   complete  │ • DON'T read directly       │   complete      │
└─────────────┴─────────────────────────────┴─────────────────┘
```

The exclusive lock ensures you're in the idle phase. Listeners fire during the update phase but receive consistent data through `TableUpdate`.

## Related documentation

- [Table listeners](./table-listeners-python.md) — React to table updates
- [Incremental update model](../conceptual/table-update-model.md) — Deep dive into how updates work
- [Synchronization and locking](../conceptual/query-engine/engine-locking.md) — Advanced locking mechanisms
- [Table types](../conceptual/table-types.md) — Static vs refreshing tables
- [Reduce update frequency](./performance/reduce-update-frequency.md) — Optimize update processing
