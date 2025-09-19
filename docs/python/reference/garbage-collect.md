---
title: garbage_collect
---

The `garbage_collect` method runs full garbage collection. It is performed in Python first; then, the underlying JVM runs its garbage collector _twice_ due to the cross-referencing nature of the Python/Java integration in Deephaven.

> [!NOTE]
> Since there is no way to force the Java garbage collector to run, the effect of calling this function is non-deterministic.
> Users should also be mindful of the overhead that running garbage collection generally incurs.

## Syntax

```python syntax
garbage_collect()
```

## Parameters

This method takes no arguments.

## Example

The following example uses Deephaven's [`perfmon`](/core/pydoc/code/deephaven.perfmon.html?module-deephaven.perfmon=) (performance monitor) library's [`server_state`](/core/pydoc/code/deephaven.perfmon.html?module-deephaven.perfmon=#deephaven.perfmon.server_state) method to print the amount of memory Deephaven is currently using at various points throughout the query - at the beginning, when objects are created and deleted, and finally after `garbage_collect` is called. The data in the `UsedMemMiB` column tracks how much memory is currently being used.

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

![The log readout from the above example](../assets/reference/python/gc-perfmon.png)

## Related Documentation

- [Create a time table](../how-to-guides/time-table.md)
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.garbage_collect)
