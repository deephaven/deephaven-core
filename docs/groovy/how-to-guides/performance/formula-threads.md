---
title: Threads for formula evaluation
sidebar_label: Formulas and threads
---

The Deephaven query engine executes as a Java process and makes use of multiple threads to process requests. When the Deephaven engine executes a query, it does so with an ExecutionContext that controls which variables and methods are available to formulas and provides information about the user and query initialization. The query engine can be used as a library, in which case the application developer is responsible for providing a consistent threading model. This guide describes the standard Deephaven server application, as executed via our Docker containers, the Gradle `server-jetty-app:run` task, or as a Core+ worker in the Deephaven Enterprise system.

Most interaction with the Deephaven engine is via gRPC. When processing a gRPC request, it is initially handled on a web-server thread as part of the Java gRPC library. Depending on the request, it is then dispatched to one of two thread pools for request handling. The Serial executor handles requests using a single thread. Evaluating script commands from a Code Studio and resolving scope tickets execute on the serial queue. Other requests are handled on the concurrent executor, which has four threads by default, but is user-configurable by setting the configuration property `scheduler.poolSize`. The single-threaded executor pool provides a well-defined order for script code execution and resolving variables. Other operations may execute concurrently, with necessary locking handled by the default [Periodic Update Graph](https://deephaven.io/core/groovy/docs/conceptual/periodic-update-graph-configuration/).

A query operation begins on one of these thread pools, but evaluation may move to another thread depending on the operation. Consider the following snippet executed from a code studio:

```groovy
x=emptyTable(1).update([Selectable.parse("Thr=java.lang.Thread.currentThread()").withSerial()])
```

```python
from deephaven.table import Selectable
from deephaven import empty_table

x = empty_table(1).update(
    [Selectable.parse("Thr=java.lang.Thread.currentThread()").with_serial()]
)
```

The `withSerial` method indicates to the Deephaven engine that the `Thr` column must be evaluated in order, and therefore it is not multi-threaded. The result is that the table contains `Thread[DeephavenApiServer-Scheduler-Serial-1,5,main]`, indicating that it has executed on the serial executor thread.

To illustrate, we'll remove the `withSerial` method and execute the following query:

```groovy
x=emptyTable(1).update("Thr=java.lang.Thread.currentThread()")
```

```python
from deephaven import empty_table

x = empty_table(1).update(["Thr=java.lang.Thread.currentThread()"])
```

The Deephaven engine may parallelize evaluation, thus resulting in a value of `Thr` of `Thread[OperationInitializationThreadPool-initializationExecutor-3,5,OperationInitializationThreadPool]`, indicating that the formula was evaluated on the operation initialization thread pool.

Similarly, each time a source table updates, the downstream effects are evaluated by an Update Graph. The default Periodic Update Graph uses a thread pool that has the same number of threads as the machine has processors (the number of threads can be configured by the property `PeriodicUpdateGraph.updateThreads`).

```groovy
x=timeTable("PT1s").head(2).update("Thr=java.lang.Thread.currentThread()")
```

```python
from deephaven import time_table

x = time_table(1).head(2).update(["Thr=java.lang.Thread.currentThread()"])
```

In this case, the formula is evaluated on one of the update executor threads (e.g., `Thread[PeriodicUpdateGraph-updateExecutor-6,5,PeriodicUpdateGraph-updateExecutors]`).

The `select` and `update` operations behave identically to each other, eagerly computing the result during initialization or in response to a table update.

## `view` and `updateView`

The `view` and `updateView` operations, on the other hand, only compute the result when the result is accessed. This can happen on a variety of threads. For example, when performing another query operation, the results are read from the thread executing that operation. For example:

```groovy order=x,sd
x=emptyTable(1).view("Thr=java.lang.Thread.currentThread()")
sd=x.selectDistinct()
```

```python order=x,sd
from deephaven import empty_table

x = empty_table(1).view(["Thr=java.lang.Thread.currentThread()"])
sd = x.select_distinct()
```

The value of `Thr` in `y` is `Thread[DeephavenApiServer-Scheduler-Serial-1,5,main]` - the thread that executed the `selectDistinct` operation. However, when viewing the table `x`, the `Thr` column takes on a value like `Thread[DeephavenApiServer-Scheduler-Concurrent-4,5,main]` because that is the thread that the barrage snapshot operation read the value on. Each time a cell is accessed (e.g., by reloading or scrolling around a table), the value is recomputed potentially on another thread.

## `where`

The `where` operation operates similarly to `select` and `update`, evaluating the formula eagerly. In the following snippet, we record the thread used by the evaluation and can see that the function was evaluated on the initialization thread pool:

```groovy
used_threads = new LinkedHashSet<>()
record_thread = { int x -> 
    used_threads.add(java.lang.Thread.currentThread().getName())
    return true
}

x=emptyTable(5).update("Row=i").where("(boolean)record_thread(Row)")
println(used_threads)
```

```python
import jpy
from deephaven import empty_table

thr = jpy.get_type("java.lang.Thread")

used_threads = set()


def record_thread(x: int) -> bool:
    used_threads.add(thr.currentThread().getName())
    return True


x = empty_table(5).update("Row=i").where("(boolean)record_thread(Row)")
print(used_threads)
```

Similarly, a refreshing `where` operation is evaluated on the Update Graph thread pool:

```groovy test-set=1
used_threads = new LinkedHashSet<>()
record_thread = { int x -> 
    used_threads.add(java.lang.Thread.currentThread().getName())
    return true
}

x=timeTable("PT1s").head(2).update("Row=i").where("(boolean)record_thread(Row)")
println(used_threads)
```

```python test-set=2
import jpy
from deephaven import time_table

thr = jpy.get_type("java.lang.Thread")

used_threads = set()


def record_thread(x: int) -> bool:
    used_threads.add(thr.currentThread().getName())
    return True


x = time_table("PT1s").head(2).update("Row=i").where("(boolean)record_thread(Row)")
```

After waiting for the table to tick, we can print the value of `used_threads`:

```groovy test-set=1
println(used_threads)
```

```python test-set=2
print(used_threads)
```

## Table operations in formulas

The Deephaven engine can create a new table by evaluating a formula, which is how a [Partitioned Table](../partitioned-tables.md) transform is implemented. A `select` or `update` that has a return type of [LivenessReferent](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessReferent.html) (of which a Table is a subtype) maintains the liveness of the resulting object, until it is removed or replaced in the result table. It is an error to use `view` or `updateView` to create a column of new Tables, because the `view` result does not have a well-defined [liveness scope](https://deephaven.io/core/groovy/docs/conceptual/liveness-scope-concept/).

The threads used for formulas that result in a Table are evaluated in exactly the same manner as other `select` and `update` operations described above. This means that your table operations may not be executed on the same thread as you initiated them. If you have not explicitly defined an [`ExecutionContext`](https://deephaven.io/core/groovy/docs/conceptual/periodic-update-graph-configuration/) before instantiating your operation, then `select` and `update` use a newly created context that shares the source table's update graph. The newly created context does not have a query library or query scope; therefore, you may not use table operations that include a formula. If you have opened an explicit ExecutionContext, the context is used for evaluation, and you may use table operations that include a formula. Partitioned tables automatically use the current context for `transform`.

## Related documentation

- [Parallelizing queries](https://deephaven.io/core/groovy/docs/conceptual/query-engine/parallelization/)
- [Periodic Update Graph](https://deephaven.io/core/groovy/docs/conceptual/periodic-update-graph-configuration/)
