---
title: How to run full garbage collection
---

Garbage collection (GC) is a memory management process that automatically collects unused objects created on the heap and deletes them to free up memory. This guide shows how to use the convenience function [`garbage_collect`](../../reference/garbage-collect.md) to run garbage collection in Python and request a full garbage collection from the JVM.

The [`garbage_collect`](../../reference/garbage-collect.md) function runs garbage collection in Python first. It then requests that the JVM run its garbage collector twice, because Python and Java objects in Deephaven can hold references to each other.

> [!NOTE]
> Java provides no way to force its garbage collector to run, so the effect of calling [`garbage_collect`](../../reference/garbage-collect.md) is non-deterministic.

To run garbage collection, import and call the function:

```python order=null
from deephaven import garbage_collect

garbage_collect()
```

## When to run garbage collection

You don't need to call [`garbage_collect`](../../reference/garbage-collect.md) for Deephaven to free memory. The JVM runs its garbage collector on its own when it needs space for new objects, so used memory normally rises and falls in a sawtooth pattern. [How does memory work in the JVM?](../../reference/community-questions/memory-jvm.md) explains this pattern.

Call [`garbage_collect`](../../reference/garbage-collect.md) when you want unused memory reclaimed now, rather than when the JVM next needs space. For example:

- After you delete large tables, to measure how much memory your remaining objects use. [Measure memory before and after garbage collection](#measure-memory-before-and-after-garbage-collection) shows how.
- When table columns that hold Python objects aren't being freed, as described in [The Python-Java boundary](../../conceptual/python-java-boundary.md).

Running garbage collection has overhead, so don't call it routinely.

For tables created by a query on [ticking](../../conceptual/table-types.md#standard-streaming-tables) data, a [liveness scope](../../conceptual/liveness-scope-concept.md) can be a better tool than [`garbage_collect`](../../reference/garbage-collect.md). Unreferenced ticking tables keep updating until the JVM collects them. A liveness scope lets you stop their updates as soon as you're done with them. In both cases, the JVM reclaims their memory the next time it collects garbage.

## Measure memory before and after garbage collection

This example creates a large table, replaces it with a second one of the same size, deletes the list that holds it, and then calls [`garbage_collect`](../../reference/garbage-collect.md). It measures how many MiB of JVM heap memory are in use in two ways:

- A `print` at each step shows the effect of that step immediately, as total JVM memory minus free memory.
- A `mem` table shows the memory samples the server takes every 15 seconds by default, so it shows the same trend over time.

The JVM can also collect unused objects on its own at any point. Your numbers differ from run to run, and a step can show less memory in use than the one before it.

The example holds two tables of about 763 MiB each at the same time, so it needs a JVM heap of at least 2 GB. If your server has a smaller heap, reduce the row count.

First, create a table that tracks memory over time, and print the starting memory use. The code does the following:

- The [`deephaven.perfmon`](/core/pydoc/code/deephaven.perfmon.html) module's [`server_state`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state) function returns a table of server statistics. Its `UsedMemMiB` column records how much memory is in use. The [Server State section of Performance tables](./performance-tables.md#server-state) describes the other columns.
- [`view`](../../reference/table-operations/select/view.md) keeps the `IntervalStart` and `UsedMemMiB` columns, and [`reverse`](../../reference/table-operations/sort/reverse.md) puts the newest sample at the top.
- [jpy](../use-jpy.md) gets the JVM's `java.lang.Runtime` object, `rt`, which the `print` calls use to read total and free memory.
- The list `x` holds the tables that the later steps create and delete.

```python test-set=1 ticking-table order=:log,mem
from deephaven.perfmon import server_state
from deephaven import empty_table, garbage_collect
import jpy

rt = jpy.get_type("java.lang.Runtime").getRuntime()
mem = server_state().view(["IntervalStart", "UsedMemMiB"]).reverse()
print(f"START {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")

x = [0]
```

The `mem` table starts with no rows and gains one at each sample. It can also show samples from before you created it. That's because all [`server_state`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state) results, from any session, share one table of samples until every earlier result has been garbage collected.

Next, use [`empty_table`](../../reference/table-operations/create/emptyTable.md) and [`update`](../../reference/table-operations/select/update.md) to create a table with 100 million rows and one `double` column, and store it in `x[0]`.

```python test-set=1 order=:log
x[0] = empty_table(100_000_000).update("X = sqrt(i)")
print(f"CREATE {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
```

The new column holds 100 million 8-byte values, about 763 MiB, so used memory can rise by about that much.

Then, replace the table with a second one of the same size, and delete `x`. Typically, neither reassigning `x[0]` nor running `del x` frees memory by itself. The memory stays in use until the JVM collects it.

```python test-set=1 order=:log
x[0] = empty_table(100_000_000).update("X = sqrt(i)")
print(f"CREATE {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
del x
print(f"DELETE {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
```

Finally, run garbage collection in its own block:

```python test-set=1 order=:log
garbage_collect()
print(f"GC {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
```

One call doesn't always reclaim every unused table, so run [`garbage_collect`](../../reference/garbage-collect.md) a second time:

```python test-set=1 order=:log
garbage_collect()
print(f"GC2 {(rt.totalMemory() - rt.freeMemory()) / 1024 / 1024}")
```

The following screenshot shows the printed output from one run, above the `mem` table. The table includes samples from before and after garbage collection:

![The printed memory values from the example, above the mem table, whose UsedMemMiB column drops from 1,716 to 91 after the two garbage collection calls](../../assets/reference/python/gc-perfmon.png)

The printed values typically show the following:

- The first `CREATE` value is higher than `START`, by up to the size of the table. The increase can be smaller if the JVM collects leftover garbage while it allocates the table.
- The second `CREATE` value includes both tables. Replacing `x[0]` removes the reference to the first table, but that table's memory stays in use.
- `DELETE` shows no change, for the same reason.
- `GC` is usually about 770 MiB lower than `DELETE`, the size of one table. The JVM treats a collection request as advisory, so a single call doesn't always reclaim everything that's unused.
- `GC2` is lower again, by about the size of the other table. Together, the two calls reclaim both tables.

## Related documentation

- [Liveness scopes](../../conceptual/liveness-scope-concept.md)
- [The Python-Java boundary](../../conceptual/python-java-boundary.md)
- [Table types](../../conceptual/table-types.md)
- [Performance tables](./performance-tables.md)
- [How to use jpy](../use-jpy.md)
- [How does memory work in the JVM?](../../reference/community-questions/memory-jvm.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`garbage_collect`](../../reference/garbage-collect.md)
- [`reverse`](../../reference/table-operations/sort/reverse.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`garbage_collect` Pydoc](/core/pydoc/code/deephaven.html#deephaven.garbage_collect)
- [`server_state` Pydoc](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state)
