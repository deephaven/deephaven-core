---
title: Execution context
---

An [`ExecutionContext`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.ExecutionContext) is the environment the engine evaluates a query in. It bundles four things:

- The query library: the imports that query strings can use.
- The query compiler: it turns query strings into code.
- The [query scope](../how-to-guides/query-scope.md): the variables that query strings can use.
- The [update graph](./dag.md#update-graph-ug-cycles): it schedules when [ticking tables](./table-types.md) refresh.

Most of the time you never see it. When a Deephaven script session starts, it creates a default context and runs every query you enter in the console inside it. That default context is _systemic_: the system supplies it, not your code. A systemic context is unrelated to [systemic object marking](./systemic-object-marking.md).

You need a context of your own in two situations: when a table operation runs on a thread you start, and when an operation does deferred work that the engine runs later. This page covers both, then shows when and how to create additional contexts.

Contexts are also a tool in their own right. Because each context can carry its own [query scope](../how-to-guides/query-scope.md) and [update graph](./dag.md#update-graph-ug-cycles), you can use contexts to keep independent real-time workloads from stepping on each other's variables or update cycles.

## When an `ExecutionContext` is needed

The script session supplies a context for the code you run in the console. You must supply one yourself when:

- A table operation runs in a thread other than the script session's thread.
- A table operation does deferred work: the engine runs part of it later, outside the script session, such as when a new partition arrives in a [partitioned table](../how-to-guides/partitioned-tables.md).

### Table operations in a separate thread

Each thread has its own current context. A thread you start has no usable context until you open one in it.

Take, for instance, the following code, which attempts to use a [`TablePublisher`](../reference/table-operations/create/TablePublisher.md) to write data to a [blink table](./table-types.md#specialization-3-blink) in a separate thread once per second:

```python ticking-table should-fail
from deephaven.stream.table_publisher import table_publisher
from deephaven import dtypes as dht
from deephaven import empty_table
import random, threading, time

col_defs = {"X": dht.int32, "Y": dht.double}


def shut_down():
    print("Shutting down table publisher.")


source, p = table_publisher(
    name="Publisher", col_defs=col_defs, on_shutdown_callback=shut_down
)


def add_table(n):
    p.add(
        empty_table(n).update(["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"])
    )


def thread_func():
    for i in range(5):
        add_table(random.randint(5, 10))
        time.sleep(1)


thread = threading.Thread(target=thread_func)
thread.start()
```

The table operation in the new thread fails, and the server log shows the following error:

```
No ExecutionContext registered, or current ExecutionContext has no QueryScope. If this is being run in a thread, did you specify an ExecutionContext for the thread? Please refer to the documentation on ExecutionContext for details.
```

This occurs because `add_table` runs in a separate thread, and the table operations it calls ([`empty_table`](../reference/table-operations/create/emptyTable.md) and [`update`](../reference/table-operations/select/update.md)) don't run inside an open [`ExecutionContext`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.ExecutionContext).

A simple fix lets the code run. Get the script session's context with [`get_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.get_exec_ctx), then open it in the thread with a Python [`with`](https://peps.python.org/pep-0343/) statement:

```python reset ticking-table order=null
from deephaven.stream.table_publisher import table_publisher
from deephaven.execution_context import get_exec_ctx  # New import
from deephaven import dtypes as dht
from deephaven import empty_table
import random, threading, time

col_defs = {"X": dht.int32, "Y": dht.double}


def shut_down():
    print("Shutting down table publisher.")


source, p = table_publisher(
    name="Publisher", col_defs=col_defs, on_shutdown_callback=shut_down
)


def add_table(n):
    p.add(
        empty_table(n).update(["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"])
    )


def thread_func():
    with ctx:  # Encapsulate table operations in the execution context
        for i in range(5):
            add_table(random.randint(5, 10))
            time.sleep(1)


ctx = get_exec_ctx()  # Get the systemic execution context
thread = threading.Thread(target=thread_func)
thread.start()
```

![The ticking `source` table](../assets/conceptual/exec-ctx-publisher.gif)

The `source` [blink table](./table-types.md#specialization-3-blink) now receives a batch of 5 to 10 random rows about once per second, five times. Several threads can use the same execution context, so any other thread you start can open `ctx` the same way.

### Table operations that do deferred work

A [partitioned table transform](../how-to-guides/partitioned-tables.md#transform) can do deferred work: if a new partition arrives later, the engine re-evaluates the transform function for that partition.

Operations that record a context for later work don't record a systemic one. Because the script session's context is systemic, [`transform`](../reference/table-operations/partitioned-tables/transform.md) doesn't record it for that deferred work. The function then runs without access to the query library, [query scope](../how-to-guides/query-scope.md), or query compiler unless it opens an execution context itself.

A [`time_table`](../reference/table-operations/create/timeTable.md) starts empty, so in the following code every partition arrives after [`transform`](../reference/table-operations/partitioned-tables/transform.md) is set up, and each call to `max_date` is deferred work:

- `source` is a [time table](../how-to-guides/time-table.md) that adds a row every second. Its `Key` column is `A` for the first five rows and `B` after that.
- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) splits `source` into a [partitioned table](../how-to-guides/partitioned-tables.md) with one partition per key.
- [`transform`](../reference/table-operations/partitioned-tables/transform.md) calls `max_date` on each partition. `max_date` opens the script session's context, `main_exec_ctx`, around its table operations.

```python ticking-table order=null
from deephaven import time_table
from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from deephaven.updateby import cum_max

main_exec_ctx = get_exec_ctx()


def max_date(t: Table) -> Table:
    with main_exec_ctx:
        return (
            t.update_by([cum_max(["MaxTimestamp = Timestamp"])])
            .update_view(["Date = formatDate(MaxTimestamp, timeZone(`PT`))"])
            .drop_columns(["MaxTimestamp"])
        )


source = time_table("PT1s").update("Key = ii < 5 ? `A` : `B`")
my_partitioned_table = source.partition_by("Key").transform(max_date)
```

Alternatively, open a non-systemic context around the [`transform`](../reference/table-operations/partitioned-tables/transform.md) call. Called with no arguments, [`make_user_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.make_user_exec_ctx) returns a _user_ execution context: a copy of the current context that isn't systemic. Because it isn't systemic, [`transform`](../reference/table-operations/partitioned-tables/transform.md) records it, so `max_date` runs in it without its own [`with`](https://peps.python.org/pep-0343/) block:

```python ticking-table order=null
from deephaven import time_table
from deephaven.execution_context import make_user_exec_ctx
from deephaven.table import Table
from deephaven.updateby import cum_max

user_ctx = make_user_exec_ctx()  # Create a new execution context


def max_date(t: Table) -> Table:
    return (
        t.update_by([cum_max(["MaxTimestamp = Timestamp"])])
        .update_view(["Date = formatDate(MaxTimestamp, timeZone(`PT`))"])
        .drop_columns(["MaxTimestamp"])
    )


source = time_table("PT1s").update("Key = ii < 5 ? `A` : `B`")

with user_ctx:
    my_partitioned_table = source.partition_by("Key").transform(max_date)
```

## When and how to create additional execution contexts

For most applications where a single user runs Deephaven, the systemic execution context is enough. Each [`ExecutionContext`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.ExecutionContext) can have its own [update graph](./dag.md#update-graph-ug-cycles), query library, and [query scope](../how-to-guides/query-scope.md), which lets you compartmentalize units of code so they work independently of one another. That matters in these more complex use cases:

- **Freezing query-scope variables**: Some work must keep the values its variables had when it was set up. For example, [transforms](../reference/table-operations/partitioned-tables/transform.md) set up in a loop each need their own iteration's value, and a thread might need to be shielded from changes that other threads make to the [query scope](../how-to-guides/query-scope.md). Pass `freeze_vars` to [`make_user_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.make_user_exec_ctx), described in [Create a context from the current one](#create-a-context-from-the-current-one).
- **Isolating update processing**: A secondary [update graph](./dag.md#update-graph-ug-cycles) runs its own update cycles, so an expensive operation on it doesn't delay the update cycles of the main graph. Separate [update graphs](./dag.md#update-graph-ug-cycles) refresh independently, so their update cycles can run in parallel on different cores. Build a context with its own [update graph](./dag.md#update-graph-ug-cycles), described in [Create an execution context from scratch](#create-an-execution-context-from-scratch).
- **Separating workloads**: When multiple users share a Deephaven server, or a single user wants to keep each workload's variables, imports, and update processing separate from the others, each workload can run in a context with its own [query scope](../how-to-guides/query-scope.md), query library, and [update graph](./dag.md#update-graph-ug-cycles). Build each context from scratch, described in [Create an execution context from scratch](#create-an-execution-context-from-scratch).

### Create a context from the current one

The [thread](#table-operations-in-a-separate-thread) example and the first [partitioned table transform](#table-operations-that-do-deferred-work) example above reuse the script session's own context, which [`get_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.get_exec_ctx) returns. [`make_user_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.make_user_exec_ctx) creates a user execution context, which isn't systemic.

- Called with no arguments, it shares the current [query scope](../how-to-guides/query-scope.md), query library, query compiler, and [update graph](./dag.md#update-graph-ug-cycles), as in the [deferred-work example](#table-operations-that-do-deferred-work).
- Called with `freeze_vars`, it gets its own [query scope](../how-to-guides/query-scope.md) (shown below).

`freeze_vars` is a name or sequence of names from the current [query scope](../how-to-guides/query-scope.md). The new context's [query scope](../how-to-guides/query-scope.md) contains only those names, with their current values, and later changes to those variables aren't visible in it.

The following code block freezes `value1`, `value2`, and `value3`, then sets `value1` to 100. It runs the same [`update`](../reference/table-operations/select/update.md) on an [`empty_table`](../reference/table-operations/create/emptyTable.md) inside and outside the new context:

```python order=inside,outside
from deephaven import empty_table
from deephaven.execution_context import make_user_exec_ctx

value1, value2, value3 = 1, 2, 3

user_ctx = make_user_exec_ctx(freeze_vars=["value1", "value2", "value3"])

value1 = 100

with user_ctx:
    inside = empty_table(1).update("X = value1")

outside = empty_table(1).update("X = value1")
```

`inside` shows 1, the value `value1` had when the context was created, and `outside` shows 100.

Neither form gives the context its own [update graph](./dag.md#update-graph-ug-cycles). For that, [create an execution context from scratch](#create-an-execution-context-from-scratch).

### Create an execution context from scratch

Create an [`ExecutionContext`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.ExecutionContext) from scratch, instead of using the systemic context or [`make_user_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.make_user_exec_ctx), when you need a context with its own query library or its own [update graph](./dag.md#update-graph-ug-cycles), such as a separate [`PeriodicUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html) or an [`EventDrivenUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html) that refreshes only on request.

The following example builds an [`ExecutionContext`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.ExecutionContext) from scratch by calling the Java builder through [`jpy`](../how-to-guides/use-jpy.md):

```python skip-test
import jpy

ExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
QueryCompilerImpl = jpy.get_type("io.deephaven.engine.context.QueryCompilerImpl")
PeriodicUpdateGraph = jpy.get_type(
    "io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph"
)
OperationInitializer = jpy.get_type(
    "io.deephaven.engine.updategraph.OperationInitializer"
)

update_graph = PeriodicUpdateGraph.newBuilder("MyCustomGraph").build()

execution_context = (
    ExecutionContext.newBuilder()
    .newQueryLibrary()
    .newQueryScope()
    .setOperationInitializer(OperationInitializer.NON_PARALLELIZABLE)
    .setUpdateGraph(update_graph)
    .setQueryCompiler(QueryCompilerImpl.create())
    .build()
)

update_graph.start()
```

A context also has an operation initializer, which controls whether operations parallelize their initial work. It also carries the [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html) that was current when the context was built. Deephaven records that [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html) with the work done in that context, for example in the performance logs.

The builder lets you specify each component:

- **Query library**: A new query library with only the default imports, via [`newQueryLibrary`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#newQueryLibrary()).
- **[Query scope](../how-to-guides/query-scope.md)**: A new, empty [query scope](../how-to-guides/query-scope.md), via [`newQueryScope`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#newQueryScope()).
- **Operation initializer**: Whether and how the engine parallelizes the initial work of table operations in this context, via [`setOperationInitializer`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#setOperationInitializer(io.deephaven.engine.updategraph.OperationInitializer)). The example passes [`OperationInitializer.NON_PARALLELIZABLE`](/core/javadoc/io/deephaven/engine/updategraph/OperationInitializer.html#NON_PARALLELIZABLE), which runs that work on the calling thread.
- **[Update graph](./dag.md#update-graph-ug-cycles)**: A custom [update graph](./dag.md#update-graph-ug-cycles), such as a [`PeriodicUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html) or an [`EventDrivenUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html), via [`setUpdateGraph`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#setUpdateGraph(io.deephaven.engine.updategraph.UpdateGraph)).
- **Query compiler**: The compiler that turns query strings into code, via [`setQueryCompiler`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#setQueryCompiler(io.deephaven.engine.context.QueryCompiler)). [`QueryCompilerImpl.create`](/core/javadoc/io/deephaven/engine/context/QueryCompilerImpl.html) also accepts an optional directory to add to the compiler's classpath and an optional parent class loader.

[`newBuilder`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#newBuilder()) starts from the current thread's [update graph](./dag.md#update-graph-ug-cycles), operation initializer, and [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html). The example replaces the [update graph](./dag.md#update-graph-ug-cycles) and operation initializer and keeps the [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html).

A new [`PeriodicUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html) doesn't refresh until you call its [`start`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html#start()) method, as the example does.

The builder chain returns `execution_context` as a Java object, and a Python [`with`](https://peps.python.org/pep-0343/) statement can't open it directly. Wrap it in the Python [`ExecutionContext`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.ExecutionContext) class first:

```python skip-test
from deephaven.execution_context import ExecutionContext as PyExecutionContext

ctx = PyExecutionContext(j_exec_ctx=execution_context)

with ctx:
    ...
```

Because the [query scope](../how-to-guides/query-scope.md) is new and empty, query strings in this context can't see the script's Python variables or functions. Query strings that use them fail with a `Cannot find variable or class` or `Cannot find method` error. Put any values they need in the context's [query scope](../how-to-guides/query-scope.md) first, for example with `execution_context.getQueryScope().putParam("my_value", my_value)`.

Refreshing tables that belong to different [update graphs](./dag.md#update-graph-ug-cycles) can't be used together in one table operation. For example, a [join](../how-to-guides/joins-exact-relational.md) between a table that ticks on `MyCustomGraph` and a table that ticks on the server's main [update graph](./dag.md#update-graph-ug-cycles) fails.

> [!NOTE]
> [Update graph](./dag.md#update-graph-ug-cycles) names must be unique within the JVM.
>
> - Building a graph with a name that's already in use, including re-running this example, throws an `IllegalStateException`.
> - The Deephaven server's own [update graph](./dag.md#update-graph-ug-cycles) is named `"DEFAULT"`, so don't use that name.
> - To reuse an existing graph when you re-run the code, call the graph builder's [`existingOrBuild`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.Builder.html#existingOrBuild()) method instead of [`build`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.Builder.html#build()). Both [`PeriodicUpdateGraph.Builder`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.Builder.html) and [`EventDrivenUpdateGraph.Builder`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.Builder.html) provide it.

To refresh only on demand, build an [`EventDrivenUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html) instead of the [`PeriodicUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html) and pass it to [`setUpdateGraph`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#setUpdateGraph(io.deephaven.engine.updategraph.UpdateGraph)) in the builder chain above. An event-driven graph has no `start` method, so leave out the [`start`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html#start()) call. Instead, it runs one update cycle each time you call its [`requestRefresh`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html#requestRefresh()) method:

```python skip-test
import jpy

EventDrivenUpdateGraph = jpy.get_type(
    "io.deephaven.engine.updategraph.impl.EventDrivenUpdateGraph"
)

event_driven_graph = EventDrivenUpdateGraph.newBuilder("EventDriven").build()

# Pass event_driven_graph to setUpdateGraph when building the execution context.
# Then run an update cycle whenever new data should be processed:
event_driven_graph.requestRefresh()
```

## Related documentation

- [Live DAG](./dag.md)
- [Table types](./table-types.md)
- [Create an empty table](../how-to-guides/new-and-empty-table.md#empty_table)
- [Partitioned tables](../how-to-guides/partitioned-tables.md)
- [Query scope](../how-to-guides/query-scope.md)
- [Time tables](../how-to-guides/time-table.md)
- [Use jpy](../how-to-guides/use-jpy.md)
- [Deephaven data types](../reference/python/deephaven-python-types.md)
- [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`TablePublisher`](../reference/table-operations/create/TablePublisher.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`deephaven.execution_context` Pydoc](/core/pydoc/code/deephaven.execution_context.html#module-deephaven.execution_context)
- [`ExecutionContext` Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
- [`PeriodicUpdateGraph` Javadoc](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html)
- [`EventDrivenUpdateGraph` Javadoc](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html)
