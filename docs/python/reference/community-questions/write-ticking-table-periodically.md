---
title: How can I periodically write a ticking table to disk?
---

It's common to have queries on ticking tables where it's prudent to write that data to disk periodically. There are a few ways to do this.

## Python timers

There are several ways to periodically call functions in Python. The following example uses Python's [`threading.Timer`](https://docs.python.org/3/library/threading.html#threading.Timer) to write a ticking table to disk every 30 seconds.

```python skip-test
from deephaven import time_table
from deephaven import write_csv
import threading
import traceback
import time

# The ticking table to write to disk
my_data = time_table("PT0.2s").update(["X = i", "Y = randomDouble(0, 100)"])


# A function that runs another function periodically
def run_periodically(delay, task):
    next_time = time.time() + delay
    while True:
        time.sleep(max(0, next_time - time.time()))
        try:
            task()
        except Exception:
            traceback.print_exc()
        next_time += (time.time() - next_time) // delay * delay + delay


# The function to be called by `run_periodically`
def write_to_disk() -> None:
    write_csv(my_data, "/data/my_data.csv")


# A threading Timer to write to disk every 30 seconds
t = threading.Thread(target=lambda: run_periodically(30, write_to_disk)).start()
```

## Time table

You can also just use a time table to trigger writes. For instance, if you want to write every 30 seconds, have a time table tick every 30 seconds and call a Python function that writes the ticking table of interest to disk.

```python skip-test
from deephaven import time_table, ring_table
from deephaven import write_csv

# The ticking table to write to disk
my_data = time_table("PT0.2s").update(
    ["X = randomDouble(0, 100)", "Y = randomInt(-100, 100)", "Z = randomBool()"]
)


# A function to write data to disk - returns True if successful, False otherwise
def write_to_disk() -> bool:
    try:
        write_csv(my_data_ring, f"/data/my_data.csv")
    except:
        return False
    return True


# Your write trigger table
write_trigger = time_table("PT30s").update("WriteSuccessful = write_to_disk()")
```
