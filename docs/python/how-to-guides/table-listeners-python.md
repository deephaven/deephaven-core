---
title: Listen to ticking tables
sidebar_label: Table listeners
---

Deephaven makes it easy to create dynamic queries that update in real time. When a table updates, a message describing the changes is sent to all listeners of the table. This mechanism is what makes ticking queries work. It can also be used to create new, dynamic functionality.

As an example, consider using a Deephaven query to create a dynamic table that monitors for situations needing human intervention. You can create a table listener that sends a Slack message every time one or more tables tick. Similarly, you could have a table of orders to buy or sell stocks. If rows are added to the order table, new orders are sent to the broker, and if rows are removed from the order table, orders are canceled with the broker.

This guide will show you how to create your own table listeners in Python.

## What is a table listener?

A table listener is an object that listens to one or more tables for updates. When connected to a ticking table, a listener receives one or more [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) objects that can be used to access the added, modified, or removed data.

When listening to changes in a single table, use the Deephaven [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) function, and when listening to changes in multiple tables use the Deephaven [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen) function. Both functions accept either a listener function or a listener class.

## Listen to one ticking table

Regardless of whether you use a listener function or a listener class, you will need to use the [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen) function to register the listener with a table. Once a listener is registered, it will begin receiving updates.

### With a listener function

A listener function takes two inputs:

- `update`: a [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) object, which contains added, modified, and removed data.
- `is_replay`: A boolean value that is `True` when replaying the initial snapshot and `False` otherwise. This will only ever be `True` when the listener receives its first update and `do_replay` is set to true when calling [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen).

The following example listens to a [time table](../reference/table-operations/create/timeTable.md).

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    print(f"FUNCTION LISTENER: update={update}")
    print(f"is_replay: {is_replay}")


table = time_table("PT1S").update(formulas=["X=i"]).tail(5)
handle = listen(table, listener_function)
```

![`table` updates while `handle` prints updates to the log](../assets/how-to/listener-basic.gif)

### With a listener class

The table listener class gives more control when listening to table changes. It requires defining an `on_update` function that takes the same arguments as a [listener function](#with-a-listener-function), `update` and `is_replay`. The `on_update` function is called every time the associated table is updated.

Listener classes are useful in cases where the listener must keep track of state. In this example, the listener will keep track of how many times it has been called.

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


listener_class = ExampleListener()

table = time_table("PT1S").update(formulas=["X=i"]).tail(5)
handle = listen(table, listener_class)
```

![`table` updates while `handle` prints updates to the log](../assets/how-to/listener-class.gif)

## Listen to multiple ticking tables

Regardless of whether you use a listener function or a listener class, you will need to use the [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen) function to register the listener with a table. Once a listener is registered, it will begin receiving updates.

### With a listener function

A merged listener function takes two inputs:

- `update`: a dictionary of [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) objects, with the input tables as keys.
- `is_replay`: A boolean value that is `True` when replaying the initial snapshot and `False` otherwise. This will only ever be `True` when the listener receives its first update and `do_replay` is set to `true` when calling [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen).

The following example listens to two time tables, one ticking every two seconds and the other ticking every three seconds. `updates[t1]` and `updates[t2]` return a `TableUpdate` object for each table if it has been updated and `None` otherwise.

```python ticking-table order=null
from deephaven.table_listener import merged_listen
from deephaven import time_table

t1 = time_table("PT2s").update(["X = i"])
t2 = time_table("PT3s").update(["Y = ii"])


def listener_function(updates, is_replay):
    if tu1 := updates[t1]:
        print(f"t1: {tu1.added()}")
    if tu2 := updates[t2]:
        print(f"t2: {tu2.added()}")


handle = merged_listen([t1, t2], listener_function)
```

![`t1` and `t2` update while `handle` prints updates to the log](../assets/how-to/merged-listen-walrus.gif)

> [!IMPORTANT]
> `updates` contains `None` values for any table that has not changed during the update cycle. These `None` values must be handled to avoid raising errors.

### With a listener class

The merged listener class gives more control when listening to multiple table changes. It requires defining an `on_update` function that takes the same arguments as a [listener function](#with-a-listener-function), `update` and `is_replay`. The `on_update` function is called every time the associated tables are updated.
Listener classes are useful in cases where the listener must keep track of state. In this example, the listener will keep track of how many times it has been called.

```python ticking-table order=null
from deephaven.table_listener import merged_listen, MergedListener
from deephaven import time_table


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


listener_class = ExampleListener()

source1 = time_table("PT1S").update(formulas=["X=i"]).tail(5)
source2 = time_table("PT2S").update(formulas=["Y=ii"]).tail(5)
handle = merged_listen([source1, source2], listener_class)
```

![Log output from `handle`](../assets/how-to/merged-listen-class.gif)

## Access table data

Regardless of whether you're listening to one or multiple tables, a [`TableUpdate`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) object contains the added, modified, and removed rows from a table. There are several ways to access this data.

The following methods return a dict with column names as keys and [NumPy arrays](https://numpy.org/doc/stable/reference/generated/numpy.array.html) as values:

- [`added`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added) - rows added during the current update cycle.
- [`modified`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified) - rows modified during the current update cycle.
- [`removed`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.removed) - rows removed during the current update cycle.
- [`modified_prev`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev) - rows modified during the previous update cycle.

The following example listens to added rows during each update cycle. It prints the data as the listener receives it.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    data_added = update.added()
    print(data_added)


source = time_table("PT1s").update("X = i").tail(5)

handle = listen(source, listener_function)
```

![`source` updates while `handle` prints added rows](../assets/how-to/listen-added.gif)

The following methods are chunked accessors and return a generator. Each call to the generator returns data on a subset (chunk) of the changed rows. To see all rows, iterate over all chunks in the generator. The chunk of changed rows is a dictionary with column names as keys and [NumPy arrays](https://numpy.org/doc/stable/reference/generated/numpy.array.html) as values.

- [`added_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.added_chunks) - rows added during the current update cycle.
- [`modified_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_chunks) - rows modified during the current update cycle.
- [`removed_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.removed_chunks) - rows removed during the current update cycle.
- [`modified_prev_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev_chunks) - rows modified during the previous update cycle.

Chunked accessors are typically used when updates are large. While normal accessors load an entire update into memory all at once, chunked accessors load the update in smaller pieces, thus limiting memory usage. The following example splits added data into chunks of 100 rows at a time and prints the size of each chunk.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    data_added_chunks = update.added_chunks(chunk_size=100)
    for idx, chunk in enumerate(data_added_chunks):
        curr_x_chunk = chunk["X"]
        print(f"Chunk #{idx + 1}: {curr_x_chunk} rows.")


source = time_table("PT0.001s").update("X = i")

handle = listen(source, listener_function)
```

![`source` updates while `handle` prints added rows](../assets/how-to/listen-added-chunks.gif)

To see the previous values for modified rows, use:

- [`modified_prev`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev)
- [`modified_prev_chunks`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev_chunks)

The following example listens to modified rows during each update cycle. It prints the current and previous values of the modified rows for the `X` column.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    prev_modified = update.modified_prev()
    curr_modified = update.modified()

    if not prev_modified:
        print("No previous values")
        return

    for prev, curr in zip(prev_modified["X"], curr_modified["X"]):
        print(f"Change previous={prev} current={curr}")


table = time_table("PT0.1s").update("X = i").last_by()

handle = listen(table, listener_function)
```

![`table` updates while `handle` prints modified rows](../assets/how-to/listen-modified-prev.gif)

## Error handling

A table listener can also be supplied with a method to handle errors. This method gets called as soon as the listener function or class raises an exception. The method takes an exception as input, and can be made to take any action necessary to handle the error or notify the user, client APIs, or other services. Error handling is done identically for both single and merged listeners.

Consider an example where a listener raises an error if it receives a value greater than 10. An error handling method is used to print a message about the error to the console.

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


tt = time_table("PT1s").update("X = i + 6")
handle = listen(t=tt, listener=listener_function, on_error=error_function)
```

![`tt` updates while `handle` prints error messages](../assets/how-to/listener-on-error.gif)

## Add and remove listeners

Most applications that require the use of a table listener do so for the entirety of the application's lifetime. If a listener should only be registered for a specified period of time, a listener handle can be registered and deregistered using the [`start`](/core/pydoc/code/deephaven.table_listener.html?#deephaven.table_listener.TableListenerHandle.start) and [`stop`](/core/pydoc/code/deephaven.table_listener.html?#deephaven.table_listener.TableListenerHandle.stop) methods.

The following example uses [`threading.Timer`](https://docs.python.org/3/library/threading.html#timer-objects) to deregister a listener after 3 seconds and then re-register it after 6 seconds.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table

import time
from threading import Timer


def listener_function(update, is_replay):
    print(f"FUNCTION LISTENER: update={update}")
    print(f"is_replay: {is_replay}")


def stop_listener(handle):
    handle.stop()


def start_listener(handle):
    handle.start()


table = time_table("PT1S").update(formulas=["X=i"]).tail(5)
handle = listen(table, listener_function)

Timer(3, stop_listener, args=[handle]).start()
Timer(6, start_listener, args=[handle]).start()
```

![`handle` is registered and deregistered](../assets/how-to/listener-deregister.gif)

## Reduce data volumes

Tables often tick at high frequencies and with large quantities of incoming data. It's best practice to only listen to what's required for an operation. In such cases, applying [filters](./use-filters.md) and/or [reducing tick frequencies](./performance/reduce-update-frequency.md) will reduce both the quantity and frequency of incoming data to a listener.

The following example listens to a table that has been filtered and had its tick frequency reduced to reduce the rate at which the listener receives data.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    print(f"FUNCTION LISTENER for even values: update={update}")


source = time_table("PT0.5s").update(formulas=["X=i"]).tail(5)
trigger = time_table("PT2s").rename_columns("DateTime = Timestamp")
result = source.where(filters=["X % 2 = 0"]).snapshot_when(trigger_table=trigger)
handle = listen(result, listener_function)
```

![`handle` receives filtered and downsampled updates](../assets/how-to/listener-downsampled.gif)

## Replay data

A table listener can listen to data that existed before the listener was registered. For example, a listener that isn't registered until 10 seconds after a table starts ticking can be made to listen to the data that was created during those 10 seconds.

To make a listener listen to previously existing data, set the `do_replay` parameter to `True` when calling [`listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen).

The following example registers two listeners with a time table two seconds after it's created. Only the one that sets `do_replay` to `True` receives data when it's first registered.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table

import time


def listener_function(update, is_replay):
    print(f"FUNCTION LISTENER for even values: update={update}")
    print(f"is_replay={is_replay}")


source = time_table("PT0.3s").update("X = i")

# The following code is run two seconds after the code above
handle_no_replay = listen(source, listener_function, do_replay=False)
handle_replay = listen(source, listener_function, do_replay=True)

handle_no_replay.stop()
handle_replay.stop()
```

![`handle_replay` recieves all the data the table started with, while `handle_no_replay` only receives the updates after it was registered](../assets/how-to/listener-replay.gif)

## Dependent tables

Listeners can use data from tables other than the one they are listening to if the additional tables are configured as dependencies. When one or more tables are listed as a dependency to a listener, the query engine will wait to call the listener until all dependent tables have been processed. When a table is not listed as a dependency, it may be in an inconsistent state when accessed.

> [!WARNING]
> Don't do table operations inside the listener. While performing operations on the dependent tables in the listener is safe, it is not recommended because reading or operating on the result tables of those operations may not be safe. It is best to perform the operations on the dependent tables beforehand and then add the result tables as dependencies to the listener so that they can be safely read in it.

For example, consider two tables, `source_a` and `source_b`, that tick simultaneously but cannot be joined. When listening to `source_a`, it is not guaranteed that `source_b` will have its updates processed in full before the listener receives the update from `source_a`. To guarantee that all data is processed before the listener triggers, `source_b` must be registered as a dependency for the listener.

```python ticking-table order=null
from deephaven.table_listener import listen
from deephaven.numpy import to_numpy
from deephaven import time_table
from random import choice


def rand_letter() -> str:
    return choice(["A", "B", "C", "D"])


def listen_func(update, is_replay):
    added = update.added()
    print(f"From Source A: {added['X'].item()}")
    dependent_data = to_numpy(source_b.view("Y")).squeeze()
    if dependent_data.ndim > 0:
        print(
            f"From Source B: {', '.join([str(item) for item in dependent_data.squeeze()])}"
        )


source_a = time_table("PT2s").update_view(["X = ii"])
source_b = (
    time_table("PT2s")
    .update(["Letter = rand_letter()", "Y = randomDouble(0, 10)"])
    .drop_columns("Timestamp")
    .last_by("Letter")
)

handle = listen(t=source_a, listener=listen_func, dependencies=source_b)
```

![Log readout from the listener](../assets/how-to/listener-dependency.gif)

## Example

Table listeners are often used to trigger actions based on table updates. For example, a listener could notify Slack or send an email when data meets some criteria. The following example prints values that meet certain criteria. In a real-world use case, rather than print an outlier value, a notification could be sent to relevant parties via email, Slack, Discord, or other service.

```python ticking-table order=null reset
from deephaven.table_listener import listen
from deephaven import time_table


def listener_function(update, is_replay):
    added = update.added()

    if not added:
        return

    if any([item > 9 for item in added["X"]]):
        print("Value over 9 detected!")


source = (
    time_table("PT0.33s", blink_table=True)
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
- [TableUpdate](/core/pydoc/code/deephaven.table_listener.html?#deephaven.table_listener.TableUpdate)
- [Table](/core/javadoc/io/deephaven/engine/table/Table.html)
- [`listen()`](/core/pydoc/code/deephaven.table_listener.html?#deephaven.table_listener.listen)
