---
title: How do I dynamically update a Python DataFrame from a Deephaven table?
sidebar_label: How do I dynamically update a DataFrame from a table?
---

<em>I have a Python DataFrame that I want to update automatically when my Deephaven table updates. How can I write a query that does this? </em>

<p></p>

There are two basic ways to do this. The first uses a table listener, and the second uses threading to update the DataFrame at regular intervals.

## Dynamically update a DataFrame from a Deephaven table

### With a table listener

```python order=t,tsnap,df
import threading
import time
from deephaven import time_table
from deephaven.pandas import to_pandas
from deephaven.table_listener import listen

t = time_table("PT1s").update("X=ii").tail(5)

# create a snapshot of the table every 5 seconds
tsnap = t.snapshot_when(time_table("PT5s").view("TSnap=Timestamp"))

# convert the snapshot to a pandas dataframe
df = to_pandas(tsnap)


# define a function to update the dataframe
def update_df(update, is_replay):
    global df
    df = to_pandas(tsnap)
    print(f"Update: Value: {df}")


# create a table listener that listens for updates to `tsnap`,
# and calls `update_df` when an update is received
handle = listen(tsnap, update_df)


# define a function to terminate the listener
def terminate_listener():
    print("Terminating listener...")
    handle.stop()
    print("Listener has terminated.")


# create a timer to terminate the listener after 20 seconds
timer = threading.Timer(20, terminate_listener)
timer.start()
```

### With Python threading

```python order=t,df
import threading
import time
from deephaven import time_table
from deephaven.pandas import to_pandas

# create a table that updates every second
t = time_table("PT1s").update("X=ii").tail(5)

# convert the table to a pandas dataframe
df = to_pandas(t)


# define a function to update the dataframe at regular intervals
def update_df(t, interval):
    global df
    while True:
        df = to_pandas(t)
        print(f"Thread: {threading.current_thread().name}, Value: {df}")
        time.sleep(interval)


# Create and start the thread to update the dataframe every 5 seconds
thread = threading.Thread(target=update_df, args=(t, 5), name="UpdateThread")
thread.start()

# Let the thread run for 20 seconds
time.sleep(20)

# Terminate the thread (in this case, forcefully)
print("Terminating thread...")
thread.join(timeout=1)
if thread.is_alive():
    print("Thread is still running. Exiting anyway.")
else:
    print("Thread has terminated.")
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
