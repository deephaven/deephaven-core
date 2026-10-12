---
title: Listen to ticking tables
---

Deephaven queries update in real time. When a table updates, Deephaven sends a message describing the changes to each of the table's listeners. This mechanism makes ticking queries work, and you can use it to build your own dynamic functionality.

As an example, consider using a Deephaven query to create a dynamic table that monitors for situations needing human intervention. You can create a table listener that sends a Slack message every time one or more tables tick. Similarly, you could have a table of orders to buy or sell stocks. When rows are added to the order table, a listener sends new orders to the broker, and when rows are removed, it cancels those orders with the broker.

This guide shows you how to create your own table listeners in Python.

## What is a table listener?

A table listener is an object that listens to one or more tables for updates. When connected to a ticking table, a listener receives one or more [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) objects, which you use to access the added, modified, or removed data.

Deephaven processes changes to ticking tables in batches called update cycles. In each update cycle, a listener receives one [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) for each of its tables that changed. Each `TableUpdate` describes the rows added, modified, and removed during that cycle. For details, see the [table update model](../conceptual/table-update-model.md).

When listening to changes in a single table, use the Deephaven [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) function, and when listening to changes in multiple tables, use the Deephaven [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen) function. Both functions accept either a listener function or an instance of a listener class.

## Listen to one ticking table

Pass a listener function or a listener class instance to [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) to register it with a table. The table must be refreshing; `listen` raises an error for a static table. The listener starts receiving updates as soon as it is registered.

### With a listener function

A listener function takes two arguments:

- `update`: A [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) object, which contains added, modified, and removed data.
- `is_replay`: A boolean that is `True` when the call delivers rows the table already held at registration. See [Replay data](#replay-data).

The following example listens to a [time table](../reference/table-operations/create/timeTable.md).

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    print(f"FUNCTION LISTENER: update={update}")
    print(f"is_replay: {is_replay}")


table = time_table("PT1S").update(formulas=["X = i"]).tail(5)
handle = listen(table, listener_function)
```

![`table` updates while the listener prints updates to the log](../assets/how-to/listener-basic.gif)

### With a listener class

Subclass [`TableListener`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableListener) for more control when listening to table changes. It requires defining an [`on_update`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableListener.on_update) method that takes the same arguments as a [listener function](#with-a-listener-function), `update` and `is_replay`. Deephaven calls `on_update` every time the associated table updates.

Listener classes are useful in cases where the listener must keep track of state. In this example, the listener keeps track of how many times it has been called.

```python ticking-table order=null reset
from deephaven.table_listener import listen, TableListener
from deephaven import time_table


class ExampleListener(TableListener):
    def __init__(self):
        self.counter = 0

    def on_update(self, update, is_replay):
        self.counter += 1
        print(f"CLASS LISTENER: counter={self.counter} update={update}")
        print(f"is_replay: {is_replay}")


listener_instance = ExampleListener()

table = time_table("PT1S").update(formulas=["X = i"]).tail(5)
handle = listen(table, listener_instance)
```

![`table` updates while the listener prints updates to the log](../assets/how-to/listener-class.gif)

## Listen to multiple ticking tables

Pass a merged listener function or a [`MergedListener`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.MergedListener) instance to [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen), along with the list of tables to listen to. The list must contain at least two tables, and each must be refreshing. The listener starts receiving updates as soon as it is registered.

### With a merged listener function

A merged listener function takes two arguments:

- `updates`: A dictionary of [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) objects, with the input tables as keys.
- `is_replay`: A boolean that is `True` when the call delivers rows the tables already held at registration. See [Replay data](#replay-data).

The following example listens to two time tables, one ticking every two seconds and the other ticking every three seconds. `updates[t1]` and `updates[t2]` return a [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) object if that table changed during the update cycle, and `None` otherwise.

```python ticking-table order=null reset
from deephaven.table_listener import merged_listen
from deephaven import time_table

t1 = time_table("PT2S").update(["X = i"])
t2 = time_table("PT3S").update(["Y = ii"])


def listener_function(updates, is_replay):
    if tu1 := updates[t1]:
        print(f"t1: {tu1.added()}")
    if tu2 := updates[t2]:
        print(f"t2: {tu2.added()}")


handle = merged_listen([t1, t2], listener_function)
```

![`t1` and `t2` update while the listener prints updates to the log](../assets/how-to/merged-listen-walrus.gif)

> [!IMPORTANT]
> `updates` contains `None` values for any table that has not changed during the update cycle. Check for `None` before using an entry, or the listener raises an error.

### With a merged listener class

Subclass [`MergedListener`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.MergedListener) for more control when listening to multiple table changes. It requires defining an [`on_update`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.MergedListener.on_update) method that takes the same arguments as a [merged listener function](#with-a-merged-listener-function), `updates` and `is_replay`. Deephaven calls `on_update` every time one of the associated tables updates.

Listener classes are useful in cases where the listener must keep track of state. In this example, the listener keeps track of how many times it has been called.

```python ticking-table order=null reset
from deephaven.table_listener import merged_listen, MergedListener
from deephaven import time_table

source1 = time_table("PT1S").update(formulas=["X = i"]).tail(5)
source2 = time_table("PT2S").update(formulas=["Y = ii"]).tail(5)


class ExampleListener(MergedListener):
    def __init__(self):
        self.counter = 0

    def on_update(self, updates, is_replay):
        self.counter += 1
        print(f"CLASS LISTENER: counter={self.counter}")
        if tu1 := updates[source1]:
            print(f"Source1: {tu1.added()}")
        if tu2 := updates[source2]:
            print(f"Source2: {tu2.added()}")
        print(f"is_replay: {is_replay}")


listener_instance = ExampleListener()
handle = merged_listen([source1, source2], listener_instance)
```

![Log output from the listener](../assets/how-to/merged-listen-class.gif)

## Access table data

Regardless of whether you're listening to one or multiple tables, a [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) object contains the added, modified, and removed rows from a table. There are several ways to access this data.

The following methods return a dictionary with column names as keys and [NumPy arrays](https://numpy.org/doc/stable/reference/generated/numpy.array.html) as values:

- [`added`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added): Rows added during the current update cycle.
- [`modified`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified): Rows modified during the current update cycle.
- [`removed`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.removed): Rows removed during the current update cycle.
- [`modified_prev`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev): Previous values of rows modified during the current update cycle.

The following example listens to added rows during each update cycle. It prints the data as the listener receives it.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    data_added = update.added()
    print(data_added)


source = time_table("PT1S").update("X = i").tail(5)

handle = listen(source, listener_function)
```

![`source` updates while the listener prints added rows](../assets/how-to/listen-added.gif)

The following example uses [`modified_prev`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev) and [`modified`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified) to compare previous and current values. It listens to modified rows during each update cycle and prints the previous and current values of the modified rows for the `X` column.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    prev_modified = update.modified_prev()
    curr_modified = update.modified()

    if len(prev_modified.get("X", [])) == 0:
        print("No previous values")
        return

    for prev, curr in zip(prev_modified["X"], curr_modified["X"]):
        print(f"Change previous={prev} current={curr}")


table = time_table("PT0.1S").update("X = i").last_by()

handle = listen(table, listener_function)
```

![`table` updates while the listener prints modified rows](../assets/how-to/listen-modified-prev.gif)

Each chunked accessor takes a required `chunk_size` argument and returns a generator. Each iteration of the generator yields a subset (chunk) of the changed rows as a dictionary, with column names as keys and [NumPy arrays](https://numpy.org/doc/stable/reference/generated/numpy.array.html) as values. To see all rows, iterate over all chunks in the generator. The chunked accessors are:

- [`added_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added_chunks): Rows added during the current update cycle.
- [`modified_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_chunks): Rows modified during the current update cycle.
- [`removed_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.removed_chunks): Rows removed during the current update cycle.
- [`modified_prev_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev_chunks): Previous values of rows modified during the current update cycle.

Use chunked accessors when updates are large. The [`added`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added), [`modified`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified), [`removed`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.removed), and [`modified_prev`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev) methods load an entire update into memory at once. Chunked accessors load it in smaller pieces, which limits memory usage. The following example splits added data into chunks of 100 rows at a time and prints the size of each chunk.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    data_added_chunks = update.added_chunks(chunk_size=100)
    for idx, chunk in enumerate(data_added_chunks):
        curr_x_chunk = chunk["X"]
        print(f"Chunk #{idx + 1}: {len(curr_x_chunk)} rows.")


source = time_table("PT0.001S").update("X = i")

handle = listen(source, listener_function)
```

![`source` updates while the listener prints added rows](../assets/how-to/listen-added-chunks.gif)

## Error handling

You can give a table listener an error handler. Deephaven calls the handler when the listener function or class raises an exception, or when a table the listener listens to fails. It passes the handler a [`RuntimeError`](https://docs.python.org/3/library/exceptions.html#RuntimeError) whose message contains the stack trace of the original error.

The handler can take any action, such as notifying the user, client APIs, or other services. How you supply it depends on the kind of listener:

- For a listener function, pass the error handler to the `on_error` parameter of [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) or [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen). If you don't pass one, a default handler prints the error.
- For a [`TableListener`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableListener) or [`MergedListener`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.MergedListener) subclass, override its [`on_error`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableListener.on_error) method instead. The default `on_error` method prints the error. Passing `on_error` to [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) or [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen) together with a listener class instance raises an error.

After a listener raises an exception, it stops receiving updates.

In the following example, the listener raises an error if it receives a value greater than 10, and an error handler prints a message to the console.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    added = update.added()
    if not added:
        return

    if any([item > 10 for item in added["X"]]):
        raise Exception("Value over 10 detected!")


def error_function(e: Exception):
    print("A value over 10 was detected. Please investigate.")


tt = time_table("PT1S").update("X = i + 6")
handle = listen(t=tt, listener=listener_function, on_error=error_function)
```

![`tt` updates, the listener raises an error, and the error handler prints a message](../assets/how-to/listener-on-error.gif)

## Add and remove listeners

Most applications keep a table listener registered for their whole lifetime. To register one for only part of that time, use the handle that registration returns. [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) returns a [`TableListenerHandle`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableListenerHandle), and [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen) returns a [`MergedListenerHandle`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.MergedListenerHandle). Call the handle's [`stop`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableListenerHandle.stop) method to deregister the listener and its [`start`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableListenerHandle.start) method to register it again.

Keep a reference to the handle. If the handle is garbage collected, the listener stops receiving updates.

The following example uses [`threading.Timer`](https://docs.python.org/3/library/threading.html#timer-objects) to deregister a listener after 3 seconds and then re-register it after 6 seconds. The timer callbacks run on a background thread. `start` acquires the update graph lock it needs automatically, because [`auto_locking`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking) is enabled by default. See [Update graph locks and thread safety](#update-graph-locks-and-thread-safety).

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table

from threading import Timer


def listener_function(update, is_replay):
    print(f"FUNCTION LISTENER: update={update}")
    print(f"is_replay: {is_replay}")


def stop_listener(handle):
    handle.stop()


def start_listener(handle):
    handle.start()


table = time_table("PT1S").update(formulas=["X = i"]).tail(5)
handle = listen(table, listener_function)

Timer(3, stop_listener, args=[handle]).start()
Timer(6, start_listener, args=[handle]).start()
```

![`handle` is registered and deregistered](../assets/how-to/listener-deregister.gif)

## Replay data

A table listener can receive the rows a table already holds when the listener is registered. For example, a listener that isn't registered until 10 seconds after a time table starts ticking can receive the rows the table added during those 10 seconds.

To make a listener receive the rows that already exist, set the `do_replay` parameter to `True` when calling [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) or [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen). The listener's first call then receives those rows as added rows, with `is_replay` set to `True`.

The following example creates a time table:

```python ticking-table order=null test-set=replay reset
from deephaven import time_table

source = time_table("PT0.3S").update("X = i")
```

After a few seconds, run the following code to register two listeners with the table. Only the one that sets `do_replay` to `True` receives the rows the table already holds when it is registered.

```python ticking-table order=null test-set=replay reset
from deephaven.table_listener import listen


def no_replay_listener(update, is_replay):
    print(f"NO REPLAY LISTENER: is_replay={is_replay}, update={update}")


def replay_listener(update, is_replay):
    print(f"REPLAY LISTENER: is_replay={is_replay}, update={update}")


handle_no_replay = listen(source, no_replay_listener, do_replay=False)
handle_replay = listen(source, replay_listener, do_replay=True)
```

![`handle_replay` receives all the data the table started with, while `handle_no_replay` only receives the updates after it was registered](../assets/how-to/listener-replay.gif)

When you no longer need the listeners, stop them:

```python order=null test-set=replay reset
handle_no_replay.stop()
handle_replay.stop()
```

## Update graph locks and thread safety

When code running outside the Deephaven console creates a ticking table and adds a listener, the table may update between the moment you create it and the moment you register the listener. If this happens, the listener misses those updates.

The [Update Graph](../conceptual/table-update-model.md) coordinates all table updates in Deephaven. By holding a lock while creating the table _and_ adding the listener, you ensure no updates occur in between.

Deephaven provides two types of locks:

- **Shared lock** ([`shared_lock`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.shared_lock)): Pauses table updates while your code runs. Use this for table operations like creating tables and adding listeners.
- **Exclusive lock** ([`exclusive_lock`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.exclusive_lock)): Blocks all Update Graph activity. Use this only for advanced cases, such as waiting for specific update conditions.

![Diagram showing how locks prevent missed updates](../assets/how-to/update-graph-lock.png)

> [!NOTE]
> Code you run in the Deephaven console already runs under the Update Graph's exclusive lock, so no updates can slip between the lines of a single console execution. Updates can still happen between separate executions. This section applies to code running on other threads, such as timer callbacks or background workers.

The [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) and [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen) functions automatically acquire a lock when they register the listener. They do this when [`auto_locking`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking) is enabled, which is the default. This lock protects only the registration. It does not protect the creation of the table.

> [!NOTE]
> Background threads don't have an execution context by default. Table operations like [`time_table`](../reference/table-operations/create/timeTable.md) require a valid context. Capture the context before dispatching, then reopen it on the background thread. See [Execution context](../conceptual/execution-context.md) for details.

**Separate locks (race condition):**

If you create a table in one lock and add the listener in another, a race window exists between them:

```python skip-test
from deephaven import time_table, update_graph
from deephaven.execution_context import get_exec_ctx
from deephaven.table_listener import listen

# Assumes listener_function is defined as in the earlier examples

# Before dispatching to background thread: capture the context
ctx = get_exec_ctx()

# On background thread with reopened context
with ctx:
    # First lock: create the table
    with update_graph.shared_lock(ctx.update_graph):
        table = time_table("PT1S").update("X = i")
    # RACE WINDOW: table can update here before listener is added
    handle = listen(table, listener_function)  # listen() locks, but too late
```

**Single lock (safe):**

Wrap both operations in a single lock to eliminate the race:

```python skip-test
from deephaven import time_table, update_graph
from deephaven.execution_context import get_exec_ctx
from deephaven.table_listener import listen

# Assumes listener_function is defined as in the earlier examples

# Before dispatching to background thread: capture the context
ctx = get_exec_ctx()

# On background thread: reopen context, then lock both operations together
with ctx:
    with update_graph.shared_lock(ctx.update_graph):
        table = time_table("PT1S").update("X = i")
        handle = listen(table, listener_function)
```

> [!NOTE]
> Keep locks brief. Tables cannot update while a lock is held, so long-held locks can delay data.

For more details on locks and thread safety, see [Synchronization and locking](../conceptual/query-engine/engine-locking.md).

## Dependent tables

A listener can read tables other than the one it listens to. Pass those tables to the `dependencies` parameter of [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) or [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen), and the query engine waits to call the listener until it has processed all of them. A table that isn't a dependency may be in an inconsistent state when the listener reads it.

For example, `source_a` and `source_b` tick at the same time but share no key column, so instead of joining them, a listener on `source_a` reads `source_b` directly. Without a dependency, that listener might run before the engine finishes processing `source_b`.

The following example creates `source_b_y`, a view of `source_b`'s `Y` column, before registering the listener, and passes it as the dependency. The listener reads `source_b_y` with [`to_numpy`](../reference/numpy/to-numpy.md).

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven.numpy import to_numpy
from deephaven import time_table
from random import choice


def rand_letter() -> str:
    return choice(["A", "B", "C", "D"])


def listen_func(update, is_replay):
    added = update.added()
    print(f"From Source A: {added['X']}")
    dependent_data = to_numpy(source_b_y)[:, 0]
    print(f"From Source B: {', '.join([str(item) for item in dependent_data])}")


source_a = time_table("PT2S").update_view(["X = ii"])
source_b = (
    time_table("PT2S")
    .update(["Letter = rand_letter()", "Y = randomDouble(0, 10)"])
    .drop_columns("Timestamp")
    .last_by("Letter")
)
source_b_y = source_b.view("Y")

handle = listen(t=source_a, listener=listen_func, dependencies=source_b_y)
```

![Log readout from the listener](../assets/how-to/listener-dependency.gif)

> [!WARNING]
> Don't create derived tables inside the listener. A table you derive inside the listener is not a dependency, so reading it may return inconsistent data. Derive tables before you register the listener, and register the derived tables as dependencies, as the example above does with `source_b_y`.

## Reduce data volumes

Tables often tick at high frequencies and with large quantities of incoming data. Listen only to the data an operation needs. [Filters](./use-filters.md) and [reduced tick frequencies](./performance/reduce-update-frequency.md) cut both the amount of data a listener receives and how often it arrives.

The following example filters a table and reduces its tick frequency before listening to it.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    print(f"FUNCTION LISTENER for even values: update={update}")


source = time_table("PT0.5S").update(formulas=["X = i"]).tail(5)
# Rename Timestamp so it doesn't clash with source's Timestamp column in the snapshot
trigger = time_table("PT2S").rename_columns("DateTime = Timestamp")
result = source.where(filters=["X % 2 = 0"]).snapshot_when(trigger_table=trigger)
handle = listen(result, listener_function)
```

![`handle` receives filtered and downsampled updates](../assets/how-to/listener-downsampled.gif)

## Example: alert on outlier values

The following example reads each update's added rows and prints any value greater than 9. In a real-world use case, the listener could send a notification by email, Slack, Discord, or another service instead. The source is a [blink table](../conceptual/table-types.md#specialization-3-blink), which holds only the rows added in the current update cycle, so the table doesn't grow as values arrive.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    added = update.added()

    if not added:
        return

    outliers = [item for item in added["X"] if item > 9]
    if outliers:
        print(f"Values over 9 detected: {outliers}")


source = (
    time_table("PT0.33S", blink_table=True)
    .update("X = randomDouble(0, 10)")
    .drop_columns("Timestamp")
)

handle = listen(source, listener_function, do_replay=False)
```

![Log readout from the listener](../assets/how-to/listener-usage.gif)

## Related documentation

- [NumPy and Deephaven](./use-numpy.md)
- [`to_numpy`](../reference/numpy/to-numpy.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate)
- [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
- [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen)
- [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen)
- [Synchronization and locking](../conceptual/query-engine/engine-locking.md)
- [`update_graph`](/core/pydoc/code/deephaven.update_graph.html)
