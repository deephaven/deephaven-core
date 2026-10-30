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
| [`view`](../../reference/table-operations/select/view.md), [`updateView`](../../reference/table-operations/select/update-view.md)                                                         | Each time something reads the value | The thread that reads the value.                                                                            |

## `select` and `update`

The [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md) operations use the same threading behavior: both eagerly compute results during initialization or in response to a table update. Consider the following snippet executed from the console:

```groovy
threadName = emptyTable(1).update([
    Selectable.parse("Thr = java.lang.Thread.currentThread().getName()").withSerial()
])
```

The [`withSerial`](../../reference/query-language/types/ConcurrencyControl.md#withserial) method marks the `Thr` formula serial. The engine never evaluates a serial formula concurrently with itself, and it evaluates the rows one at a time, in table order. Because the engine doesn't parallelize a serial formula, the formula runs on the thread that calls [`update`](../../reference/table-operations/select/update.md). A console command runs on the serial executor, so `Thr` holds `DeephavenApiServer-Scheduler-Serial-1`.

For comparison, let's remove [`withSerial`](../../reference/query-language/types/ConcurrencyControl.md#withserial) and run the same query:

```groovy
threadName = emptyTable(1).update("Thr = java.lang.Thread.currentThread().getName()")
```

This formula is eligible for [parallel evaluation](../../conceptual/query-engine/parallelization.md), so the engine hands the work to the operation initialization thread pool, even for a single row. `Thr` holds a value such as `OperationInitializationThreadPool-initializationExecutor-3`.

### Refreshing `select` and `update`

Each time a source table updates, an [update graph](../../conceptual/dag.md#update-graph-cycles) evaluates the downstream effects. The default [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) evaluates them on its own thread pool. The following query keeps the first two rows of a [time table](../time-table.md) that adds a row once per second:

```groovy ticking-table order=null
threadName = timeTable("PT1s").head(2).update("Thr = java.lang.Thread.currentThread().getName()")
```

In this case, the formula runs on a [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) thread, such as `PeriodicUpdateGraph-updateExecutor-6`.

## `view` and `updateView`

Unlike [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md), the [`view`](../../reference/table-operations/select/view.md) and [`updateView`](../../reference/table-operations/select/update-view.md) operations compute a value only when something reads it, so the formula runs on whichever thread does the reading. For example, when another query operation reads the results, the formula runs on the thread or threads that operation uses:

```groovy order=threadName,distinctThreads
threadName = emptyTable(1).view("Thr = java.lang.Thread.currentThread().getName()")
distinctThreads = threadName.selectDistinct()
```

The value of `Thr` in `distinctThreads` is `DeephavenApiServer-Scheduler-Serial-1` — the thread that executed the [`selectDistinct`](../../reference/table-operations/select/select-distinct.md) operation.

When you open `threadName` in the web UI, though, `Thr` shows a different thread, such as `DeephavenApiServer-Scheduler-Concurrent-4`. The web UI reads the table through a [Barrage](../../conceptual/what-is-barrage.md) subscription. The server takes the [Barrage](../../conceptual/what-is-barrage.md) snapshot of a one-column table like this one on a concurrent executor thread. It can read a table with several columns in parallel on the operation initialization thread pool instead.

The engine recomputes the value each time the UI reads the cell, such as when you reload or scroll the table. Each read can run on a different thread, so the value can change every time you look at it.

## `where`

Like [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md), the [`where`](../../reference/table-operations/filter/where.md) operation evaluates its formula eagerly. Let's record each thread that evaluates the [filter](../filters.md):

```groovy order=:log
usedThreads = Collections.synchronizedSet(new LinkedHashSet<>())
recordThread = { int x ->
    usedThreads.add(java.lang.Thread.currentThread().getName())
    return true
}

emptyTable(5).update("Row = i").where("(boolean)recordThread(Row)")
println(usedThreads)
```

The printed set holds one or more operation initialization thread names, such as `[OperationInitializationThreadPool-initializationExecutor-1]`. The set is synchronized because a filter can run on several threads at once, as described in [Write formulas that work on any thread](#write-formulas-that-work-on-any-thread).

### Refreshing `where`

The engine evaluates a refreshing [`where`](../../reference/table-operations/filter/where.md) operation on the [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) thread pool:

```groovy test-set=1 ticking-table order=null
usedThreads = Collections.synchronizedSet(new LinkedHashSet<>())
recordThread = { int x ->
    usedThreads.add(java.lang.Thread.currentThread().getName())
    return true
}

recordedThreads = timeTable("PT1s").head(2).update("Row = i").where("(boolean)recordThread(Row)")
```

Let's wait a few seconds for the table to tick, then print `usedThreads` in a separate command. Don't call `sleep` in the same command to wait. Console code runs under the [update graph](../../conceptual/dag.md#update-graph-cycles)'s [exclusive lock](../../conceptual/query-engine/engine-locking.md), so the table can't tick until the command finishes.

<!-- skip-test: the snapshotter runs this block right after the setup block, before the time table ticks, so it would render an empty set. -->

```groovy test-set=1 skip-test
println(usedThreads)
```

The output is a set of one or more [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) thread names, such as `[PeriodicUpdateGraph-updateExecutor-6]`.

## Write formulas that work on any thread

A formula should not depend on which thread runs it. The engine can evaluate one formula on several threads at once, and it can re-evaluate a [`view`](../../reference/table-operations/select/view.md) or [`updateView`](../../reference/table-operations/select/update-view.md) formula later on a different thread. If a formula updates shared state, such as a counter or a collection, use one of these approaches:

- Make the shared state thread-safe.
- Mark a [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), or [`where`](../../reference/table-operations/filter/where.md) [formula](../formulas.md) or [filter](../filters.md) serial with [`withSerial`](../../reference/query-language/types/ConcurrencyControl.md#withserial) so the engine never evaluates it concurrently with itself.

You can't mark a [`view`](../../reference/table-operations/select/view.md) or [`updateView`](../../reference/table-operations/select/update-view.md) formula serial. By default, the engine rejects a serial column in those operations, because they compute values on whichever thread reads them, in any order. For those formulas, make the shared state thread-safe.

If several formulas in the same [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), or [`where`](../../reference/table-operations/filter/where.md) call share the state, also order them with [Barriers](../../reference/query-language/types/Barrier.md), as described in [Parallelizing queries](../../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where). Formulas in different operations can run at the same time on different threads, so any state they share must be thread-safe.

## Table operations in formulas

A formula can itself run table operations, as a [partitioned table](../partitioned-tables.md) [`transform`](../../reference/table-operations/partitioned-tables/transform.md) does. These operations run on the same threads as any other [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) formula, so they might not run on the thread that started them. They run in an [`ExecutionContext`](../../conceptual/execution-context.md).

- The [`ExecutionContext`](../../conceptual/execution-context.md) controls which variables and methods formulas can use, and it carries information about the user and query initialization.
- Each thread has its own current context, so a worker thread does not automatically have the context your script was using.
- A user context is any context that is not [systemic](../../conceptual/execution-context.md#systemic-vs-separate-executioncontext).
- The console runs your script in a [systemic](../../conceptual/execution-context.md#systemic-vs-separate-executioncontext) context, so a console script has no user context open unless you open one.

Which context the nested operations get depends on whether a user context is open when you call [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md):

- If no user [`ExecutionContext`](../../conceptual/execution-context.md) is open, [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md) use a newly created context.
- If a user [`ExecutionContext`](../../conceptual/execution-context.md) is open, the engine uses that context for evaluation, and table operations inside the formula work.

The newly created context uses the source table's [update graph](../../conceptual/dag.md#update-graph-cycles), so nested operations still run on the thread pools described above. It has no [query scope](../query-scope.md), no query library, and no query compiler, so:

- Formulas cannot see your variables or use your imported classes and functions.
- Any table operation that compiles a formula or other code fails. This includes [`where`](../../reference/table-operations/filter/where.md), [`select`](../../reference/table-operations/select/select.md), and [`update`](../../reference/table-operations/select/update.md) with a formula, and some [exact and relational joins](../joins-exact-relational.md), [time-series joins](../joins-timeseries-range.md), [aggregations](../../conceptual/aggregations.md), and [sorts](../sort.md).

[Partitioned table](../partitioned-tables.md) [`transform`](../../reference/table-operations/partitioned-tables/transform.md) follows the same rule. It uses the enclosing context only when that context is a user context.

When a transform function runs table operations, create a user context by calling [`makeExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#makeExecutionContext(boolean)) with `isSystemic` set to `false` before you call [`transform`](../../reference/table-operations/partitioned-tables/transform.md). Create it outside the function, because a context created inside the function copies the limited context the function runs in. Then use it in one of these ways:

- Open it in a try-with-resources block around the [`transform`](../../reference/table-operations/partitioned-tables/transform.md) call.
- Open it inside the function with a try-with-resources block, using the context you created outside the function.
- Pass it to the [`transform`](../../reference/table-operations/partitioned-tables/transform.md) overload that takes an [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html).

Without a user context, the transform function's [`update`](../../reference/table-operations/select/update.md) call fails, because the newly created context has no query scope:

```groovy should-fail
source = emptyTable(6).update("Key = i % 2", "X = i")
partitioned = source.partitionBy("Key")

addColumn = { t -> t.update("Y = X * 2") }

result = partitioned.transform(addColumn).merge()
```

The error ends with `No ExecutionContext registered, or current ExecutionContext has no QueryScope`.

The following example opens a user context around the [`transform`](../../reference/table-operations/partitioned-tables/transform.md) call instead:

```groovy order=source,result
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

source = emptyTable(6).update("Key = i % 2", "X = i")
partitioned = source.partitionBy("Key")

addColumn = { t -> t.update("Y = X * 2") }

try (SafeCloseable ignored = ExecutionContext.makeExecutionContext(false).open()) {
    result = partitioned.transform(addColumn).merge()
}
```

### Keep tables created in formulas alive

When a [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) formula on a refreshing table returns a [`LivenessReferent`](/core/javadoc/io/deephaven/engine/liveness/LivenessReferent.html), such as a table, the result table keeps that object alive until its row is removed or its value is replaced.

You can't use [`view`](../../reference/table-operations/select/view.md) or [`updateView`](../../reference/table-operations/select/update-view.md) to create a column of tables or other [`LivenessReferent`](/core/javadoc/io/deephaven/engine/liveness/LivenessReferent.html) objects, because their results would not have a well-defined [liveness scope](../../conceptual/liveness-scope-concept.md). By default, the engine rejects such a column and tells you to use [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) instead.

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
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md)
- [`head`](../../reference/table-operations/filter/head.md)
- [`merge`](../../reference/table-operations/partitioned-tables/merge.md)
- [`partitionBy`](../../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`selectDistinct`](../../reference/table-operations/select/select-distinct.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`transform`](../../reference/table-operations/partitioned-tables/transform.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`ExecutionContext` Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
- [`LivenessReferent` Javadoc](/core/javadoc/io/deephaven/engine/liveness/LivenessReferent.html)
- [`Selectable` Javadoc](/core/javadoc/io/deephaven/api/Selectable.html)
