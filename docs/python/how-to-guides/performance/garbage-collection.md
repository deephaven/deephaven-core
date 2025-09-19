---
title: How to run full Garbage Collection
sidebar_label: Run Garbage Collection
---

This guide shows how to perform full Garbage Collection (GC) using the convenience method [`garbage_collect`](../../reference/garbage-collect.md).

Garbage collection is a memory management process that automatically collects unused objects created on the heap and deletes them to free up memory. The `garbage_collect` method performs GC in Python first; then, the underlying JVM runs its garbage collector _twice_ due to the cross-referencing nature of the Python/Java integration in Deephaven.

> [!NOTE]
> Since there is no way to force the Java garbage collector to run, the effect of calling this function is non-deterministic.
> Users should also be mindful of the overhead that running garbage collection generally incurs.

## Example

In this example, we use Deephaven's [Performance Monitor](/core/pydoc/code/deephaven.perfmon.html?module-deephaven.perfmon=) library's [`server_state`](/core/pydoc/code/deephaven.perfmon.html?module-deephaven.perfmon=#deephaven.perfmon.server_state) method to print the amount of memory Deephaven is currently using at various points throughout the query. This shows memory consumption at the beginning, when objects are created and deleted, and then after `garbage_collect` is called. The `garbage_collect` method does not take any arguments. The data in the `UsedMemMiB` column tracks how much memory is currently being used.

```python order=mem
# Segment 1
from deephaven.perfmon import server_state
from deephaven import empty_table, garbage_collect
import gc, jpy

rt = jpy.get_type("java.lang.Runtime").getRuntime()
mem = server_state().view(["IntervalStart", "UsedMemMiB"]).reverse()
print(f"START {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")

x = [0]

# Segment 2
x[0] = empty_table(100_000_000).update("X = sqrt(i)")
print(f"CREATE {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")

# Segment 3
x[0] = empty_table(100_000_000).update("X = sqrt(i)")
print(f"CREATE {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
del x
print(f"DELETE {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
gc.collect()
garbage_collect()
print(f"GC {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
```

![The console, displaying the results of the print statments in the above example](../../assets/reference/python/gc-perfmon.png)

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.garbage_collect)
