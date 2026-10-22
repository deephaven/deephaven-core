---
title: Execution context
---

An [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) is the environment the engine evaluates a query in. It bundles four things:

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

```groovy ticking-table should-fail
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y")
)

shutDown = { -> println "Shutting down table publisher." }

publisher = TablePublisher.of("My Publisher", definition, null, shutDown)

table = publisher.table()

addTables = { ->
    for (int i = 0; i < 5; ++i) {
        publisher.add(emptyTable(new Random().nextInt(6) + 5).update("X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"))
        sleep(1000)
    }
    return
}

thread = Thread.start(addTables)
```

The table operation in the new thread fails, and the server log shows the following error:

```
No ExecutionContext registered, or current ExecutionContext has no QueryScope. If this is being run in a thread, did you specify an ExecutionContext for the thread? Please refer to the documentation on ExecutionContext for details.
```

This occurs because the `addTables` closure runs in a separate thread, and the table operations it calls ([`emptyTable`](../reference/table-operations/create/emptyTable.md) and [`update`](../reference/table-operations/select/update.md)) don't run inside an open [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html).

A simple fix lets the code run. Get the script session's context with [`getContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#getContext()), then call its [`open`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#open()) method in a try-with-resources block around the table operation. [`open`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#open()) returns a [`SafeCloseable`](/core/javadoc/io/deephaven/util/SafeCloseable.html) that restores the previous context when the block ends:

```groovy reset ticking-table order=null
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y")
)

shutDown = { -> println "Shutting down table publisher." }

publisher = TablePublisher.of("My Publisher", definition, null, shutDown)

table = publisher.table()

addTables = { ->
    for (int i = 0; i < 5; ++i) {
        try (SafeCloseable ignored = defaultCtx.open()) {
            publisher.add(emptyTable(new Random().nextInt(6) + 5).update("X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"))
        }
        sleep(1000)
    }
    return
}

thread = Thread.start(addTables)
```

The `table` [blink table](./table-types.md#specialization-3-blink) now receives a batch of 5 to 10 random rows about once per second, five times. Several threads can use the same execution context, so any other thread you start can open `defaultCtx` the same way.

### Table operations that do deferred work

A [partitioned table transform](../how-to-guides/partitioned-tables.md#transform) can do deferred work: if a new partition arrives later, the engine re-evaluates the transform closure for that partition.

Operations that record a context for later work don't record a systemic one. Because the script session's context is systemic, the [`transform`](../reference/table-operations/partitioned-tables/transform.md) overload that takes no [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) argument doesn't record it for that deferred work. The closure then runs without access to the query library, [query scope](../how-to-guides/query-scope.md), or query compiler unless it opens an execution context itself.

A [`timeTable`](../reference/table-operations/create/timeTable.md) starts empty, so in the following code every partition arrives after [`transform`](../reference/table-operations/partitioned-tables/transform.md) is set up, and each call to `maxDate` is deferred work:

- `source` is a [time table](../how-to-guides/time-table.md) that adds a row every second. Its `Key` column is `A` for the first five rows and `B` after that.
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) splits `source` into a [partitioned table](../how-to-guides/partitioned-tables.md) with one partition per key.
- [`transform`](../reference/table-operations/partitioned-tables/transform.md) calls `maxDate` on each partition. `maxDate` opens the script session's context, `defaultCtx`, around its table operations.

```groovy ticking-table order=null
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

maxDate = { t ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t.updateBy(CumMax("MaxTimestamp = Timestamp"))
            .updateView("Date = formatDate(MaxTimestamp, timeZone(`PT`))")
            .dropColumns("MaxTimestamp")
    }
}

source = timeTable("PT1s").update("Key = ii < 5 ? `A` : `B`")
myPartitionedTable = source.partitionBy("Key").transform(maxDate)
```

A usable context can come from any of three places:

- **The closure opens one**, as `maxDate` does above.
- **You pass one to [`transform`](../reference/table-operations/partitioned-tables/transform.md).** The [`transform(ExecutionContext, UnaryOperator, boolean, Dependency...)`](/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#transform(io.deephaven.engine.context.ExecutionContext,java.util.function.UnaryOperator,boolean,io.deephaven.engine.updategraph.NotificationQueue.Dependency...)) overload runs the closure in the context you pass. Its `expectRefreshingResults` flag must be `true` when the underlying table of the [partitioned table](../how-to-guides/partitioned-tables.md) is static but the closure can return refreshing tables. The last example on the [`transform`](../reference/table-operations/partitioned-tables/transform.md) reference page shows this overload.
- **You open a non-systemic context around the [`transform`](../reference/table-operations/partitioned-tables/transform.md) call.** [`makeExecutionContext(false)`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#makeExecutionContext(boolean)) returns a copy of the current context that isn't systemic. Its argument says whether the new context is systemic. It shares the current [query scope](../how-to-guides/query-scope.md), query library, query compiler, and [update graph](./dag.md#update-graph-ug-cycles). The [`transform`](../reference/table-operations/partitioned-tables/transform.md) overload that takes no [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) argument records it, so `maxDate` runs in it without its own try-with-resources block:

```groovy ticking-table order=null
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

userCtx = ExecutionContext.makeExecutionContext(false)

maxDate = { t ->
    return t.updateBy(CumMax("MaxTimestamp = Timestamp"))
        .updateView("Date = formatDate(MaxTimestamp, timeZone(`PT`))")
        .dropColumns("MaxTimestamp")
}

source = timeTable("PT1s").update("Key = ii < 5 ? `A` : `B`")

try (SafeCloseable ignored = userCtx.open()) {
    myPartitionedTable = source.partitionBy("Key").transform(maxDate)
}
```

## When and how to create additional execution contexts

For most applications where a single user runs Deephaven, the systemic execution context is enough. Each [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) can have its own [update graph](./dag.md#update-graph-ug-cycles), query library, and [query scope](../how-to-guides/query-scope.md), which lets you compartmentalize units of code so they work independently of one another. That matters in these more complex use cases:

- **Freezing query-scope variables**: Some work must keep the values its variables had when it was set up. For example, [transforms](../reference/table-operations/partitioned-tables/transform.md) set up in a loop each need their own iteration's value, and a thread might need to be shielded from changes that other threads make to the [query scope](../how-to-guides/query-scope.md). Use [`captureQueryScopeVars`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#captureQueryScopeVars(java.lang.String...)), described in [Create a context from the current one](#create-a-context-from-the-current-one).
- **Isolating update processing**: A secondary [update graph](./dag.md#update-graph-ug-cycles) runs its own update cycles, so an expensive operation on it doesn't delay the update cycles of the main graph. Separate [update graphs](./dag.md#update-graph-ug-cycles) refresh independently, so their update cycles can run in parallel on different cores. Build a context with its own [update graph](./dag.md#update-graph-ug-cycles), described in [Create an execution context from scratch](#create-an-execution-context-from-scratch).
- **Separating workloads**: When multiple users share a Deephaven server, or a single user wants to keep each workload's variables, imports, and update processing separate from the others, each workload can run in a context with its own [query scope](../how-to-guides/query-scope.md), query library, and [update graph](./dag.md#update-graph-ug-cycles). Build each context from scratch, described in [Create an execution context from scratch](#create-an-execution-context-from-scratch).

### Create a context from the current one

The [thread](#table-operations-in-a-separate-thread) example and the first [partitioned table transform](#table-operations-that-do-deferred-work) example above reuse the script session's own context, which [`getContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#getContext()) returns. You can create additional execution contexts, which aren't systemic, with [`makeExecutionContext(false)`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#makeExecutionContext(boolean)) or the [`ExecutionContext` builder](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html).

- [`makeExecutionContext(false)`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#makeExecutionContext(boolean)) returns a context that shares the current [query scope](../how-to-guides/query-scope.md), query library, query compiler, and [update graph](./dag.md#update-graph-ug-cycles), as in the [deferred-work example](#table-operations-that-do-deferred-work).
- A context built with [`newBuilder`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#newBuilder()) and [`captureQueryScopeVars`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#captureQueryScopeVars(java.lang.String...)) has its own [query scope](../how-to-guides/query-scope.md) (shown below).

To give the new execution context its own [query scope](../how-to-guides/query-scope.md) that holds only selected variables from the current one, build it with [`captureQueryScopeVars`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#captureQueryScopeVars(java.lang.String...)). The new [query scope](../how-to-guides/query-scope.md) gets those variables with their current values, and later changes to them aren't visible in the new context.

[`newBuilder`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#newBuilder()) starts from the current thread's [update graph](./dag.md#update-graph-ug-cycles), operation initializer, and [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html), with no query library, [query scope](../how-to-guides/query-scope.md), or query compiler. The following code adds them:

- [`captureQueryCompiler`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#captureQueryCompiler()) reuses the current context's query compiler.
- [`captureQueryLibrary`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#captureQueryLibrary()) reuses the current context's query library.
- [`captureQueryScopeVars`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#captureQueryScopeVars(java.lang.String...)) copies `value1`, `value2`, and `value3` into a new [query scope](../how-to-guides/query-scope.md).

The code then sets `value1` to 100 and runs the same [`update`](../reference/table-operations/select/update.md) on an [`emptyTable`](../reference/table-operations/create/emptyTable.md) inside and outside the new context:

```groovy order=inside,outside
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

value1 = 1
value2 = 2
value3 = 3

userCtx = ExecutionContext.newBuilder()
    .captureQueryCompiler()
    .captureQueryLibrary()
    .captureQueryScopeVars("value1", "value2", "value3")
    .build()

value1 = 100

try (SafeCloseable ignored = userCtx.open()) {
    inside = emptyTable(1).update("X = value1")
}

outside = emptyTable(1).update("X = value1")
```

`inside` shows 1, the value `value1` had when the context was created, and `outside` shows 100.

Neither form gives the context its own [update graph](./dag.md#update-graph-ug-cycles). For that, [create an execution context from scratch](#create-an-execution-context-from-scratch).

### Create an execution context from scratch

Create an [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) from scratch, instead of capturing components from an existing context, when:

- You embed the Deephaven engine in a standalone Java application, where no script session provides a context.
- You need a context with its own [update graph](./dag.md#update-graph-ug-cycles), such as a separate [`PeriodicUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html) or an [`EventDrivenUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html) that refreshes only on request.

The following example demonstrates how to build an [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) from scratch using the builder pattern:

```groovy skip-test
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.engine.context.QueryCompilerImpl
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph
import io.deephaven.engine.updategraph.OperationInitializer

updateGraph = PeriodicUpdateGraph.newBuilder("MyCustomGraph").build()

executionContext = ExecutionContext.newBuilder()
    .newQueryLibrary()
    .newQueryScope()
    .setOperationInitializer(OperationInitializer.NON_PARALLELIZABLE)
    .setUpdateGraph(updateGraph)
    .setQueryCompiler(QueryCompilerImpl.create())
    .build()

updateGraph.start()
```

A context also has an operation initializer, which controls whether operations parallelize their initial work. It also carries the [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html) that was current when the context was built. Deephaven records that [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html) with the work done in that context, for example in the performance logs.

The builder lets you specify each component:

- **Query library**: A new query library with only the default imports, via [`newQueryLibrary`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#newQueryLibrary()).
- **[Query scope](../how-to-guides/query-scope.md)**: A new, empty [query scope](../how-to-guides/query-scope.md), via [`newQueryScope`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#newQueryScope()).
- **Operation initializer**: Whether and how the engine parallelizes the initial work of table operations in this context, via [`setOperationInitializer`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#setOperationInitializer(io.deephaven.engine.updategraph.OperationInitializer)). The example passes [`OperationInitializer.NON_PARALLELIZABLE`](/core/javadoc/io/deephaven/engine/updategraph/OperationInitializer.html#NON_PARALLELIZABLE), which runs that work on the calling thread.
- **[Update graph](./dag.md#update-graph-ug-cycles)**: A custom [update graph](./dag.md#update-graph-ug-cycles), such as a [`PeriodicUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html) or an [`EventDrivenUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html), via [`setUpdateGraph`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#setUpdateGraph(io.deephaven.engine.updategraph.UpdateGraph)).
- **Query compiler**: The compiler that turns query strings into code, via [`setQueryCompiler`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#setQueryCompiler(io.deephaven.engine.context.QueryCompiler)). [`QueryCompilerImpl.create`](/core/javadoc/io/deephaven/engine/context/QueryCompilerImpl.html) also accepts an optional directory to add to the compiler's classpath and an optional parent class loader.

As described in [Create a context from the current one](#create-a-context-from-the-current-one), [`newBuilder`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#newBuilder()) starts from the current thread's [update graph](./dag.md#update-graph-ug-cycles), operation initializer, and [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html). The example replaces the [update graph](./dag.md#update-graph-ug-cycles) and operation initializer and keeps the [`AuthContext`](/core/javadoc/io/deephaven/auth/AuthContext.html).

A new [`PeriodicUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html) doesn't refresh until you call its [`start`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html#start()) method, as the example does.

Open the new context around the work that should run in it:

```groovy skip-test
import io.deephaven.util.SafeCloseable

try (SafeCloseable ignored = executionContext.open()) {
    // table operations here run in executionContext
}
```

Refreshing tables that belong to different [update graphs](./dag.md#update-graph-ug-cycles) can't be used together in one table operation. For example, a [join](../how-to-guides/joins-exact-relational.md) between a table that ticks on `MyCustomGraph` and a table that ticks on the server's main [update graph](./dag.md#update-graph-ug-cycles) fails.

> [!NOTE]
> [Update graph](./dag.md#update-graph-ug-cycles) names must be unique within the JVM.
>
> - Building a graph with a name that's already in use, including re-running this example, throws an `IllegalStateException`.
> - The Deephaven server's own [update graph](./dag.md#update-graph-ug-cycles) is named `"DEFAULT"`, so don't use that name.
> - To reuse an existing graph when you re-run the code, call the graph builder's [`existingOrBuild`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.Builder.html#existingOrBuild()) method instead of [`build`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.Builder.html#build()). Both [`PeriodicUpdateGraph.Builder`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.Builder.html) and [`EventDrivenUpdateGraph.Builder`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.Builder.html) provide it.

To refresh only on demand, build an [`EventDrivenUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html) instead of the [`PeriodicUpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html) and pass it to [`setUpdateGraph`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html#setUpdateGraph(io.deephaven.engine.updategraph.UpdateGraph)) in the builder chain above. An event-driven graph has no `start` method, so leave out the [`start`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html#start()) call. Instead, it runs one update cycle each time you call its [`requestRefresh`](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html#requestRefresh()) method:

```groovy skip-test
import io.deephaven.engine.updategraph.impl.EventDrivenUpdateGraph

eventDrivenGraph = EventDrivenUpdateGraph.newBuilder("EventDriven").build()

// Pass eventDrivenGraph to setUpdateGraph when building the execution context.
// Then run an update cycle whenever new data should be processed:
eventDrivenGraph.requestRefresh()
```

## Related documentation

- [Live DAG](./dag.md)
- [Table types](./table-types.md)
- [Create an empty table](../how-to-guides/new-and-empty-table.md#emptytable)
- [Partitioned tables](../how-to-guides/partitioned-tables.md)
- [Query scope](../how-to-guides/query-scope.md)
- [Time tables](../how-to-guides/time-table.md)
- [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md)
- [`dropColumns`](../reference/table-operations/select/drop-columns.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`TablePublisher`](../reference/table-operations/create/TablePublisher.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`ExecutionContext` Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
- [`PartitionedTable` Javadoc](/core/javadoc/io/deephaven/engine/table/PartitionedTable.html)
- [`PeriodicUpdateGraph` Javadoc](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html)
- [`EventDrivenUpdateGraph` Javadoc](/core/javadoc/io/deephaven/engine/updategraph/impl/EventDrivenUpdateGraph.html)
