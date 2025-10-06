---
title: Execution Context
---

An [`ExecutionContext`](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html) represents a specific combination of query library, compiler, and scope under which a query is evaluated. It acts as an isolated environment within which specific operations and tasks can be run.

When Deephaven is started, a default `ExecutionContext` is created. It's used to evaluate queries submitted through the script session. In order to evaluate a table operation in a deferred manner, such as in a thread separate from the script session, an application must _explicitly_ build or obtain an `ExecutionContext`. In such a case, a try-with-resources block, coupled with a Deephaven [`SafeCloseable`](/core/javadoc/io/deephaven/util/SafeCloseable.html) should be used to enclose the query.

An `ExecutionContext` can be shared across multiple threads. Typical use patterns obtain the script session's systemic `ExecutionContext` and use it to wrap a query run in a thread created by the user. More complex usage patterns can use more than one `ExecutionContext` to keep certain objects completely separate from one another.

## Motivation

There are a few key benefits that the `ExecutionContext` brings to Deephaven:

- Each `ExecutionContext` can have its own update graph, libraries, and query scope. This allows users to compartmentalize different units of code to work completely independently of one another.
- The compartmentalization minimizes resource conflicts. For instance, an operation with a high computational cost can be isolated as to not slow down other critical processes.
- Multiple `ExecutionContext`s can run in parallel, enabling Deephaven to better leverage multi-core processor architectures.
- In multi-user environments, each `ExecutionContext` can have specific authentication and authorization settings so that sensitive data and operations are securely encapsulated.

## When an `ExecutionContext` is needed

An `ExecutionContext` must be used if:

- A table operation takes place in a separate thread or context.
- A table operation may cause downstream operations to take place at a future point in time after the query scope has had the chance to change.

### Table operations in a separate thread

Take, for instance, the following code, which attempts to use a [`TablePublisher`](../reference/table-operations/create/TablePublisher.md) to write data to a blink table in a separate thread once per second:

```groovy ticking-table should-fail
import io.deephaven.csv.util.MutableBoolean
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher

defaultCtx = ExecutionContext.getContext()

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y")
)

shutDown = { -> println "Finished."}

onShutdown = new MutableBoolean()

publisher = TablePublisher.of("My Publisher", definition, null, shutDown)

table = publisher.table()

addTables = { ->
    Random rand = new Random()
    for (int i = 0; i < 5; ++i) {
        publisher.add(emptyTable(5).update("X = randomInt(0, 10)", "Y = randomDouble(0.0, 100.0)"))
        sleep(1000)
    }
    return
}

thread = Thread.start(addTables)
```

This causes Deephaven to crash with the following error:

```
No ExecutionContext registered, or current ExecutionContext has no QueryScope. If this is being run in a thread, did you specify an ExecutionContext for the thread? Please refer to the documentation on ExecutionContext for details.
```

This occurs because [`addTables`](../reference/table-operations/create/TablePublisher.md#methods), which is called in a separate thread, performs table operations and isn't encapsulated in an execution context. By importing `ExecutionContext` and `SafeClosable`, then putting the table operation inside a try-with-resources block, the code will run:

```groovy reset ticking-table order=null
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.csv.util.MutableBoolean
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y")
)

shutDown = { -> println "Finished."}

onShutdown = new MutableBoolean()

publisher = TablePublisher.of("My Publisher", definition, null, shutDown)

table = publisher.table()

addTables = { ->
    Random rand = new Random()
    for (int i = 0; i < 5; ++i) {
        try (SafeCloseable ignored = defaultCtx.open()) {
            publisher.add(emptyTable(5).update("X = randomInt(0, 10)", "Y = randomDouble(0.0, 100.0)"))
        }
        sleep(1000)
    }
    return
}

thread = Thread.start(addTables)
```

### Table operations that produced deferred results

<!-- TODO: Link to transform doc(s) https://github.com/deephaven/deephaven.io/issues/3089 -->

Below is a block of Groovy code that contains a table operation that can produce a deferred result. The `maxDate` closure is called in the `transform` method of a `PartitionedTable`:

```groovy skip-test
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

maxDate = { ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t.updateBy(cumMax("MaxTimestamp=Timestamp"))
            .updateView("Date=formatDate(MaxTimestamp, timeZone(`PT`))")
            .dropColumns("MaxTimestamp")
    }
}

myPartitionedTable = consumeToPartitionedTable(
    ...
).transform(maxDate)
```

A partitioned table transform can be a deferred operation. If a new partition is added to `myPartitionedTable` at a later time, `maxDate` will be re-evaluated for the new partition. The query scope may be different at that time, so the `ExecutionContext` must be explicitly specified.

## Systemic vs Separate `ExecutionContext`

For most applications where a single user runs Deephaven, the systemic execution context will be enough for their needs. There are more complex use cases where multiple execution contexts can be critical.

- A secondary update graph can provide resource isolation, improved performance, and user-specific workflows.
- Fixing variables that can be found in the query scope. Deferred table creation in loops must be parameterized correctly.
- Multithreaded queries may want separate execution contexts for safe execution of deferred operations.
- If multiple users are using the same Deephaven server, each user can keep their work completely separate from one another.
- If a single user wishes to avoid any crossing of data between separate workloads, each workload can be done in an isolated environment.
- Restriction of allowed operations in deferred execution (if supported by the AuthContext in use).

### Multiple execution contexts

The previous examples in this guide have only shown the use of the systemic execution context. In applications where it's beneficial or even necessary to isolate workflows from one another, multiple execution contexts can be created. Any execution context outside the systemic one must be created with [`makeExecutionContext`](https://deephaven.io/core/javadoc/io/deephaven/engine/context/ExecutionContext.html#makeExecutionContext(boolean)). The following code block writes to a [table publisher](../reference/table-operations/create/TablePublisher.md) in the systemic execution context, then uses a separate user execution context to perform a [partitioned table transform](../how-to-guides/partitioned-tables.md#transform):

```groovy skip-test
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.csv.util.MutableBoolean
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.stream.TablePublisher
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

definition = TableDefinition.of(
    ColumnDefinition.ofInt("X"),
    ColumnDefinition.ofDouble("Y")
)

shutDown = { -> println "Finished."}

onShutdown = new MutableBoolean()

publisher = TablePublisher.of("My Publisher", definition, null, shutDown)

table = publisher.table()

addTables = { ->
    Random rand = new Random()
    for (int i = 0; i < 5; ++i) {
        try (SafeCloseable ignored = defaultCtx.open()) {
            publisher.add(emptyTable(5).update("X = randomInt(0, 10)", "Y = randomDouble(0.0, 100.0)"))
        }
        sleep(1000)
    }
    return
}

thread = Thread.start(addTables)

userCtx = ExecutionContext.makeExecutionContext(false)

maxDate = { ->
    try (SafeCloseable ignored = userCtx.open()) {
        return t.updateBy(cumMax("MaxTimestamp=Timestamp"))
            .updateView("Date=formatDate(MaxTimestamp, timeZone(`PT`))")
            .dropColumns("MaxTimestamp")
    }
}

myPartitionedTable = consumeToPartitionedTable(
    ...
).transform(maxDate)
```

When instantiating a new execution context, you may also initialize its new query scope with values from the current execution context. To do so, you'll need to use the [`ExecutionContext` builder](/core/javadoc/io/deephaven/engine/context/ExecutionContext.Builder.html). For instance, this query captures the variables `value1`, `value2`, and `value3` in the new execution context:

```groovy skip-test
import io.deephaven.engine.context.ExecutionContext

userCtx = ExecutionContext.newBuilder()
    .captureQueryCompiler()
    .captureQueryLibrary()
    .captureQueryScopeVars("value1", "value2", "value3")
    .captureUpdateGraph()
    .build()
```

## Related documentation

- [Create an empty table](../how-to-guides/new-and-empty-table.md#emptytable)
- [`update`](../reference/table-operations/select/update.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
