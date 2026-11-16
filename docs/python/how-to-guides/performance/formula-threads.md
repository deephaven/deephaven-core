---
title: Threads for formula evaluation
---

The Deephaven query engine runs as a Java process and uses multiple threads to process requests. This guide shows which thread evaluates the [formulas](../formulas.md) in your query operations, so you can interpret thread-dependent results and write formulas that work on whichever thread runs them. It covers the Deephaven server application, whether you run it from a Docker container, with the Gradle `server-jetty-app:run` task, or as a Core+ worker in Deephaven Enterprise. If you embed the query engine as a library, your application provides the threading model.

## Thread pools

Most interaction with the Deephaven engine is via gRPC. The Java gRPC library first handles a gRPC request on a web-server thread. Depending on the request, the server then dispatches it to one of two thread pools:

- The serial executor handles requests using a single thread. Executing console commands, binding tables to console variables, and publishing an object to a named variable in the [query scope](../query-scope.md) through a scope ticket run on the serial executor. Its single thread provides a well-defined order for script code execution and changes to variables.
- The concurrent executor handles most other requests. These requests can run concurrently, and the default [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) provides the locking they need.

A query operation begins on one of these thread pools, but formula evaluation may move to another thread depending on the operation. The engine can also evaluate formulas on two more thread pools:

- The operation initialization thread pool parallelizes the work of creating a table.
- The [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) thread pool processes table updates.

Each thread's name is its pool's prefix, a hyphen, and a number, such as `DeephavenApiServer-Scheduler-Serial-1`:

| Thread pool                                                                      | Thread name prefix                                         | Default size             | Configuration property                      |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------ | ------------------------------------------- |
| Serial executor                                                                  | `DeephavenApiServer-Scheduler-Serial`                      | 1                        | None                                        |
| Concurrent executor                                                              | `DeephavenApiServer-Scheduler-Concurrent`                  | 4                        | `scheduler.poolSize`                        |
| Operation initialization thread pool                                             | `OperationInitializationThreadPool-initializationExecutor` | One thread per processor | `OperationInitializationThreadPool.threads` |
| [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) | `PeriodicUpdateGraph-updateExecutor`                       | One thread per processor | `PeriodicUpdateGraph.updateThreads`         |

The Periodic Update Graph thread pool and the operation initialization thread pool can each be set to one thread. In that case, the engine doesn't hand work to that pool:

- With one update thread, the [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) processes updates on the thread that runs each update cycle, `PeriodicUpdateGraph.DEFAULT.refreshThread`.
- With one operation initialization thread, initialization work runs on the thread that calls the operation.

The following table summarizes which thread runs a formula in each kind of operation. The sections below explain each case.

| Operations                                                                                                                                                                                | When the formula runs               | Thread that runs it                                                                                         |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), [`where`](../../reference/table-operations/filter/where.md) | When the table is created           | An operation initialization thread. The calling thread if the formula is serial or the pool has one thread. |
| [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), [`where`](../../reference/table-operations/filter/where.md) | When the source table updates       | A [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) thread.                  |
| [`view`](../../reference/table-operations/select/view.md), [`update_view`](../../reference/table-operations/select/update-view.md)                                                        | Each time something reads the value | The thread that reads the value.                                                                            |

## `select` and `update`

The [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md) operations use the same threading behavior: both eagerly compute results during initialization or in response to a table update. Consider the following snippet executed from the console:

```python
from deephaven.table import Selectable
from deephaven import empty_table

thread_name = empty_table(1).update(
    [Selectable.parse("Thr = java.lang.Thread.currentThread().getName()").with_serial()]
)
```

The [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial) method marks the `Thr` formula serial. The engine never evaluates a serial formula concurrently with itself, and it evaluates the rows one at a time, in table order. Because the engine doesn't parallelize a serial formula, the formula runs on the thread that calls [`update`](../../reference/table-operations/select/update.md). A console command runs on the serial executor, so `Thr` holds `DeephavenApiServer-Scheduler-Serial-1`.

For comparison, let's remove [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial) and run the same query:

```python
from deephaven import empty_table

thread_name = empty_table(1).update(
    ["Thr = java.lang.Thread.currentThread().getName()"]
)
```

This formula is eligible for [parallel evaluation](../../conceptual/query-engine/parallelization.md), so the engine hands the work to the operation initialization thread pool, even for a single row. `Thr` holds a value such as `OperationInitializationThreadPool-initializationExecutor-3`.

### Refreshing `select` and `update`

Each time a source table updates, an [update graph](../../conceptual/dag.md#update-graph-cycles) evaluates the downstream effects. The default [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) evaluates them on its own thread pool. The following query keeps the first two rows of a [time table](../time-table.md) that adds a row once per second:

```python ticking-table order=null
from deephaven import time_table

thread_name = (
    time_table("PT1s")
    .head(2)
    .update(["Thr = java.lang.Thread.currentThread().getName()"])
)
```

In this case, the formula runs on a [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) thread, such as `PeriodicUpdateGraph-updateExecutor-6`.

## `view` and `update_view`

Unlike [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md), the [`view`](../../reference/table-operations/select/view.md) and [`update_view`](../../reference/table-operations/select/update-view.md) operations compute a value only when something reads it, so the formula runs on whichever thread does the reading. For example, when another query operation reads the results, the formula runs on the thread or threads that operation uses:

```python order=thread_name,distinct_threads
from deephaven import empty_table

thread_name = empty_table(1).view(["Thr = java.lang.Thread.currentThread().getName()"])
distinct_threads = thread_name.select_distinct()
```

The value of `Thr` in `distinct_threads` is `DeephavenApiServer-Scheduler-Serial-1` — the thread that executed the [`select_distinct`](../../reference/table-operations/select/select-distinct.md) operation.

When you open `thread_name` in the web UI, though, `Thr` shows a different thread, such as `DeephavenApiServer-Scheduler-Concurrent-4`. The web UI reads the table through a [Barrage](../../conceptual/what-is-barrage.md) subscription. The server takes the [Barrage](../../conceptual/what-is-barrage.md) snapshot of a one-column table like this one on a concurrent executor thread. It can read a table with several columns in parallel on the operation initialization thread pool instead.

The engine recomputes the value each time the UI reads the cell, such as when you reload or scroll the table. Each read can run on a different thread, so the value can change every time you look at it.

## `where`

Like [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md), the [`where`](../../reference/table-operations/filter/where.md) operation evaluates its formula eagerly. Let's record each thread that evaluates the [filter](../filters.md):

```python order=:log
import jpy
from deephaven import empty_table

thr = jpy.get_type("java.lang.Thread")

used_threads = set()


def record_thread(x: int) -> bool:
    used_threads.add(thr.currentThread().getName())
    return True


empty_table(5).update("Row = i").where("(boolean)record_thread(Row)")
print(used_threads)
```

The printed set holds one or more operation initialization thread names, such as `{'OperationInitializationThreadPool-initializationExecutor-1'}`. A plain Python `set` is thread-safe here because a single `add` call on a built-in `set` is atomic. Other shared state must be thread-safe, or you must mark the [filter](../filters.md) serial, as described in [Write formulas that work on any thread](#write-formulas-that-work-on-any-thread).

### Refreshing `where`

The engine evaluates a refreshing [`where`](../../reference/table-operations/filter/where.md) operation on the [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) thread pool:

```python test-set=1 ticking-table order=null
import jpy
from deephaven import time_table

thr = jpy.get_type("java.lang.Thread")

used_threads = set()


def record_thread(x: int) -> bool:
    used_threads.add(thr.currentThread().getName())
    return True


recorded_threads = (
    time_table("PT1s").head(2).update("Row = i").where("(boolean)record_thread(Row)")
)
```

Let's wait a few seconds for the table to tick, then print `used_threads` in a separate command. Don't call `time.sleep` in the same command to wait. Console code runs under the [update graph](../../conceptual/dag.md#update-graph-cycles)'s [exclusive lock](../../conceptual/query-engine/engine-locking.md), so the table can't tick until the command finishes.

<!-- skip-test: the snapshotter runs this block right after the setup block, before the time table ticks, so it would render an empty set. -->

```python test-set=1 skip-test
print(used_threads)
```

The output is a set of one or more [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) thread names, such as `{'PeriodicUpdateGraph-updateExecutor-6'}`.

## Write formulas that work on any thread

A formula should not depend on which thread runs it. The engine can evaluate one formula on several threads at once, and it can re-evaluate a [`view`](../../reference/table-operations/select/view.md) or [`update_view`](../../reference/table-operations/select/update-view.md) formula later on a different thread. If a formula updates shared state, such as a counter or a collection, use one of these approaches:

- Make the shared state thread-safe.
- Mark a [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), or [`where`](../../reference/table-operations/filter/where.md) [formula](../formulas.md) or [filter](../filters.md) serial with [`with_serial`](../../reference/query-language/types/ConcurrencyControl.md#with_serial) so the engine never evaluates it concurrently with itself.

You can't mark a [`view`](../../reference/table-operations/select/view.md) or [`update_view`](../../reference/table-operations/select/update-view.md) formula serial. Both take only [formula strings](../formulas.md), not [`Selectable`](/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) objects. They compute values on whichever thread reads them, in any order, so make the shared state those formulas use thread-safe.

If several formulas in the same [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), or [`where`](../../reference/table-operations/filter/where.md) call share the state, also order them with [Barriers](../../reference/query-language/types/Barrier.md), as described in [Parallelizing queries](../../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where). Formulas in different operations can run at the same time on different threads, so any state they share must be thread-safe.

## Table operations in formulas

A formula can itself run table operations, as a [partitioned table](../partitioned-tables.md) [`transform`](../../reference/table-operations/partitioned-tables/transform.md) does. These operations run on the same threads as any other [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) formula, so they might not run on the thread that started them. They run in an [`ExecutionContext`](../../conceptual/execution-context.md).

- The [`ExecutionContext`](../../conceptual/execution-context.md) controls which variables and methods formulas can use, and it carries information about the user and query initialization.
- Each thread has its own current context, so a worker thread does not automatically have the context your script was using.
- A user context is any context that is not [systemic](../../conceptual/execution-context.md).
- The console runs your script in a [systemic](../../conceptual/execution-context.md) context, so a console script has no user context open unless you open one.

Which context the nested operations get depends on whether a user context is open when you call [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md):

- If no user [`ExecutionContext`](../../conceptual/execution-context.md) is open, [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md) use a newly created context.
- If a user [`ExecutionContext`](../../conceptual/execution-context.md) is open, the engine uses that context for evaluation, and table operations inside the formula work.

The newly created context uses the source table's [update graph](../../conceptual/dag.md#update-graph-cycles), so nested operations still run on the thread pools described above. It has no [query scope](../query-scope.md), no query library, and no query compiler, so:

- Formulas cannot see your variables or use your imported classes and functions.
- Any table operation that compiles a formula or other code fails. This includes [`where`](../../reference/table-operations/filter/where.md), [`select`](../../reference/table-operations/select/select.md), and [`update`](../../reference/table-operations/select/update.md) with a formula, and some [exact and relational joins](../joins-exact-relational.md), [time-series joins](../joins-timeseries-range.md), [aggregations](../../conceptual/aggregations.md), and [sorts](../sort.md).

[Partitioned table](../partitioned-tables.md) [`transform`](../../reference/table-operations/partitioned-tables/transform.md) follows the same rule. It uses the enclosing context only when that context is a user context.

When a transform function runs table operations, create a user context with [`make_user_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.make_user_exec_ctx) before you call [`transform`](../../reference/table-operations/partitioned-tables/transform.md). Create it outside the function, because a context created inside the function copies the limited context the function runs in. Then open it in one of these ways:

- Open it in a `with` block around the [`transform`](../../reference/table-operations/partitioned-tables/transform.md) call.
- Open it inside the function with a `with ctx:` block, where `ctx` is the context you created outside the function.

Without a user context, the transform function's [`update`](../../reference/table-operations/select/update.md) call fails, because the newly created context has no query scope:

```python should-fail
from deephaven import empty_table

source = empty_table(6).update(["Key = i % 2", "X = i"])
partitioned = source.partition_by("Key")


def add_column(t):
    return t.update("Y = X * 2")


result = partitioned.transform(add_column).merge()
```

The error ends with `No ExecutionContext registered, or current ExecutionContext has no QueryScope`.

The following example opens a user context around the [`transform`](../../reference/table-operations/partitioned-tables/transform.md) call instead:

```python order=source,result
from deephaven.execution_context import make_user_exec_ctx
from deephaven import empty_table

source = empty_table(6).update(["Key = i % 2", "X = i"])
partitioned = source.partition_by("Key")


def add_column(t):
    return t.update("Y = X * 2")


with make_user_exec_ctx():
    result = partitioned.transform(add_column).merge()
```

### Keep tables created in formulas alive

When a [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) formula on a refreshing table returns a [`LivenessReferent`](/core/javadoc/io/deephaven/engine/liveness/LivenessReferent.html), such as a table, the result table keeps that object alive until its row is removed or its value is replaced.

You can't use [`view`](../../reference/table-operations/select/view.md) or [`update_view`](../../reference/table-operations/select/update-view.md) to create a column of tables or other [`LivenessReferent`](/core/javadoc/io/deephaven/engine/liveness/LivenessReferent.html) objects, because their results would not have a well-defined [liveness scope](../../conceptual/liveness-scope-concept.md). By default, the engine rejects such a column and tells you to use [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) instead.

## Related documentation

- [Aggregations in Deephaven](../../conceptual/aggregations.md)
- [Create a time table](../time-table.md)
- [Deephaven's live Directed-Acyclic-Graph (DAG)](../../conceptual/dag.md)
- [Exact and relational joins](../joins-exact-relational.md)
- [Execution context](../../conceptual/execution-context.md)
- [Filters](../filters.md)
- [Formulas](../formulas.md)
- [How to use liveness scopes](../../conceptual/liveness-scope-concept.md)
- [Inexact, time-series, and range joins](../joins-timeseries-range.md)
- [Multithreading: Synchronization, locks, and snapshots](../../conceptual/query-engine/engine-locking.md)
- [Parallelizing queries](../../conceptual/query-engine/parallelization.md)
- [Partitioned tables](../partitioned-tables.md)
- [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md)
- [Query scope](../query-scope.md)
- [Sort table data](../sort.md)
- [Track processing time and measure latency](track-processing-time.md)
- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [`Barrier`](../../reference/query-language/types/Barrier.md)
- [`ConcurrencyControl`](../../reference/query-language/types/ConcurrencyControl.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`head`](../../reference/table-operations/filter/head.md)
- [`merge`](../../reference/table-operations/partitioned-tables/partitioned-table-merge.md)
- [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`select_distinct`](../../reference/table-operations/select/select-distinct.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`transform`](../../reference/table-operations/partitioned-tables/transform.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`update_view`](../../reference/table-operations/select/update-view.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`LivenessReferent` Javadoc](/core/javadoc/io/deephaven/engine/liveness/LivenessReferent.html)
- [`make_user_exec_ctx` Pydoc](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.make_user_exec_ctx)
- [`Selectable` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable)
