---
title: garbage_collect
---

The `garbage_collect` method runs full garbage collection in Python first. It then requests that the JVM run its garbage collector twice, because Python and Java objects in Deephaven can hold references to each other.

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

The following example prints the JVM's used memory at each step: at the start, after each table is created, after the reference is deleted, and after `garbage_collect` is called. It also creates the `mem` table with the [`server_state`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state) method of Deephaven's [`perfmon`](/core/pydoc/code/deephaven.perfmon.html) (performance monitor) module. The `UsedMemMiB` column in `mem` tracks how much memory is in use over time.

```python order=mem
# Segment 1
from deephaven.perfmon import server_state
from deephaven import empty_table, garbage_collect
import jpy

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
garbage_collect()
print(f"GC {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
```

For a step-by-step version of this example, with the printed output and what it shows, see [How to run full garbage collection](../how-to-guides/performance/garbage-collection.md).

## Related documentation

- [How to run full garbage collection](../how-to-guides/performance/garbage-collection.md)
- [How does memory work in the JVM?](./community-questions/memory-jvm.md)
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.garbage_collect)
