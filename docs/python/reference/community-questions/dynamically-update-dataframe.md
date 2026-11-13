---
title: How do I dynamically update a pandas DataFrame from a Deephaven table?
---

<em>I have a pandas DataFrame that I want to update automatically when my Deephaven table updates. How can I write a query that does this?</em>

<p></p>

There are two basic ways to do this. The first uses a [table listener](../../how-to-guides/table-listeners-python.md), which calls a function each time the table updates. The second uses threading to update the DataFrame at regular intervals.

## With a table listener

The listener approach uses [`snapshot_when`](../table-operations/snapshot/snapshot-when.md) to capture the table's state at regular intervals, and a [table listener](../../how-to-guides/table-listeners-python.md) to convert the snapshot to a pandas DataFrame each time it updates. This is the recommended approach because it avoids managing a dedicated polling thread.

```python ticking-table order=null reset
import threading
from deephaven import time_table
from deephaven.pandas import to_pandas
from deephaven.table_listener import listen

source = time_table("PT1s").update("X=ii").tail(5)

# Create a snapshot of the table every 5 seconds
source_snapshot = source.snapshot_when(time_table("PT5s").view("TSnap=Timestamp"))

# Convert the snapshot to a pandas DataFrame
df = to_pandas(source_snapshot)


# Define a function to update the DataFrame
def update_df(update, is_replay):
    global df
    df = to_pandas(source_snapshot)
    print(f"DataFrame updated:\n{df}")


# Create a table listener that listens for updates to `source_snapshot`,
# and calls `update_df` when an update is received
handle = listen(source_snapshot, update_df)


# Define a function to terminate the listener
def terminate_listener():
    print("Terminating listener...")
    handle.stop()
    print("Listener has terminated.")


# Create a timer to terminate the listener after 20 seconds
timer = threading.Timer(20, terminate_listener)
timer.start()
```

## With Python threading

The threading approach polls [`to_pandas`](../pandas/to-pandas.md) on a fixed interval. It requires managing the thread's lifecycle.

```python ticking-table order=null reset
import threading
from deephaven import time_table
from deephaven.pandas import to_pandas

# Create a table that updates every second
source = time_table("PT1s").update("X=ii").tail(5)

# Convert the table to a pandas DataFrame
df = to_pandas(source)

# Event used to signal the thread to stop
stop_event = threading.Event()


# Define a function to update the DataFrame at regular intervals
def update_df(t, interval):
    global df
    while not stop_event.is_set():
        df = to_pandas(t)
        print(f"Thread: {threading.current_thread().name}, DataFrame:\n{df}")
        stop_event.wait(interval)


# Create and start the thread to update the DataFrame every 5 seconds
thread = threading.Thread(target=update_df, args=(source, 5), name="UpdateThread")
thread.start()


# Define a function to stop the thread cleanly
def stop_thread():
    print("Stopping thread...")
    stop_event.set()
    thread.join()
    print("Thread has stopped.")


# Stop the thread after 20 seconds
threading.Timer(20, stop_thread).start()
```

## Related documentation

- [Table listeners](../../how-to-guides/table-listeners-python.md)
- [How to use pandas](../../how-to-guides/use-pandas.md)
- [`to_pandas`](../pandas/to-pandas.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
