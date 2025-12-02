---
title: Execution Context
---

An [`ExecutionContext`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.ExecutionContext) represents a specific combination of query library, compiler, and scope under which a query is evaluated. It acts as an isolated environment within which specific operations and tasks can be run.

When Deephaven is started, a default `ExecutionContext` is created. It's used to evaluate queries submitted through the script session. To evaluate a table operation in a deferred manner, such as in a thread separate from the script session, an application must _explicitly_ build or obtain an `ExecutionContext`. In such a case, a Python [`with`](https://peps.python.org/pep-0343/) statement should be used to enclose the query.

An `ExecutionContext` can be shared across multiple threads. Typical use patterns obtain the script session's systemic `ExecutionContext` and use it to wrap a query run in a thread created by the user. More complex usage patterns can use more than one `ExecutionContext` to keep certain objects completely separate from one another.

## Motivation

There are a few key benefits that the `ExecutionContext` brings to Deephaven:

- Each `ExecutionContext` can have its own update graph, libraries, and query scope. This allows users to compartmentalize different units of code to work independently of one another.
- The compartmentalization minimizes resource conflicts. For instance, an operation with a high computational cost can be isolated so as not to slow down other critical processes.
- Multiple `ExecutionContext`s can run in parallel, enabling Deephaven to better leverage multi-core processor architectures.
- In multi-user environments, each `ExecutionContext` can have specific authentication and authorization settings to securely encapsulate sensitive data and operations.

## When an `ExecutionContext` is needed

An `ExecutionContext` must be used if:

- A table operation takes place in a separate thread or context.
- A table operation may cause downstream operations to occur at a future point in time after the query scope has had the chance to change.

### Table operations in a separate thread

Take, for instance, the following code, which attempts to use a [`TablePublisher`](../reference/table-operations/create/TablePublisher.md) to write data to a [blink table](../conceptual/table-types.md#create-a-blink-table-from-an-add-only-table) in a separate thread once per second:

```python ticking-table should-fail
from deephaven.stream.table_publisher import table_publisher
from deephaven import dtypes as dht
from deephaven import empty_table
import asyncio, random, threading, time

coldefs = {"X": dht.int32, "Y": dht.double}


def shut_down():
    print("Shutting down table publisher.")


source, p = table_publisher(
    name="Publisher", col_defs=coldefs, on_shutdown_callback=shut_down
)


def when_done():
    p.publish_failure(RuntimeError("shut_down"))


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

Note the following in the error message:

```
No ExecutionContext registered, or current ExecutionContext has no QueryScope. If this is being run in a thread, did you specify an ExecutionContext for the thread? Please refer to the documentation on ExecutionContext for details.
```

This occurs because [`add_table`](../reference/table-operations/create/TablePublisher.md#methods), called in a separate thread, performs table operations and isn't encapsulated in an Execution Context. A fix as simple as the following will allow the code to run:

```python reset ticking-table order=null
from deephaven.stream.table_publisher import table_publisher
from deephaven.execution_context import get_exec_ctx  # New import
from deephaven import dtypes as dht
from deephaven import empty_table
import random, threading, time

coldefs = {"X": dht.int32, "Y": dht.double}


def shut_down():
    print("Shutting down table publisher.")


source, p = table_publisher(
    name="Publisher", col_defs=coldefs, on_shutdown_callback=shut_down
)


def when_done():
    p.publish_failure(RuntimeError("shut_down"))


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

### Table operations that produce deferred results

Below is a block of Python code that contains a table operation that produces a deferred result. The `max_date` function is called in the [`transform`](../reference/table-operations/partitioned-tables/transform.md) method of a `PartitionedTable`:

```python skip-test
from deephaven.execution_context import get_exec_ctx

main_exec_ctx = get_exec_ctx()

def max_date(t: Table) -> Table:
    with main_exec_ctx:
        return (
            t.update_by([cum_max(["MaxTimestamp = Timestamp"])])
            .update_view(["Date=formatDate(MaxTimestamp, timeZone(`PT`))"])
            .drop_columns(["MaxTimestamp"])
        )

my_partitioned_table = (
    consume_to_partitioned_table(
        ...
    )
    .transform(max_date)
    ...
)
```

A partitioned table transform can be a deferred operation. For instance, if a new partition is added to `my_partitioned_table` at a later time, `max_date` will be re-evaluated for the new partition. The query scope may be different then, so the `ExecutionContext` must be explicitly specified.

## Systemic vs Separate `ExecutionContext`

For most applications where a single user runs Deephaven, the systemic execution context will be enough for their needs. There are more complex use cases where multiple execution contexts can be critical.

- A secondary update graph can provide resource isolation, improved performance, and user-specific workflows.
- Fixing variables that can be found in the query scope. Deferred table creation in loops must be parameterized correctly.
- Multithreaded queries may want separate execution contexts for safe execution of deferred operations.
- If multiple users are using the same Deephaven server, each user can keep their work completely separate from one another.
- If a single user wishes to avoid any crossing of data between separate workloads, each workload can be done in an isolated environment.
- Restriction of allowed operations in deferred execution (if supported by the AuthContext in use).

### Multiple execution contexts

The previous examples in this guide have only shown the use of the systemic execution context. In applications where it's beneficial or even necessary to isolate workflows from one another, multiple execution contexts can be created. Any execution context outside the systemic one must be created with [`make_user_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.make_user_exec_ctx). The following code block writes to a [table publisher](../reference/table-operations/create/TablePublisher.md) in the systemic execution context, then uses a separate user execution context to perform a [partitioned table transform](../how-to-guides/partitioned-tables.md#transform):

```python skip-test
from deephaven.stream.table_publisher import table_publisher
from deephaven.execution_context import get_exec_ctx, make_user_exec_ctx
from deephaven import dtypes as dht
from deephaven import empty_table
import random, threading, time

coldefs = {"X": dht.int32, "Y": dht.double}


def shut_down():
    print("Shutting down table publisher.")


source, p = table_publisher(
    name="Publisher", col_defs=coldefs, on_shutdown_callback=shut_down
)


def when_done():
    p.publish_failure(RuntimeError("shut_down"))


def add_table(n):
    p.add(
        empty_table(n).update(["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"])
    )


def thread_func():
    with systemic_ctx:  # Encapsulate table operations in the execution context
        for i in range(5):
            add_table(random.randint(5, 10))
            time.sleep(1)


systemic_ctx = get_exec_ctx()  # Get the systemic execution context
thread = threading.Thread(target=thread_func)
thread.start()

user_ctx = make_user_exec_ctx()  # Create a new execution context

def max_date(t: Table) -> Table:
    with user_ctx:
        return (
            t.update_by([cum_max(["MaxTimestamp = Timestamp"])])
            .update_view(["Date=formatDate(MaxTimestamp, timeZone(`PT`))"])
            .drop_columns(["MaxTimestamp"])
        )

my_partitioned_table = (
    consume_to_partitioned_table(
        ...
    )
    .transform(max_date)
    ...
)
```

[`make_user_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.make_user_exec_ctx) can take an optional parameter `freeze_vars`. By passing one or more variables in a sequence, the variables will be frozen in the new execution context. The following code block freezes `value1`, `value2`, and `value3`:

```python skip-test
from deephaven.execution_context import make_user_exec_ctx

user_ctx = make_user_exec_ctx(freeze_vars=["value1", "value2", "value3"])
```

### Creating execution contexts from scratch

In some scenarios, you may need to create an `ExecutionContext` completely from scratch rather than using the systemic context or `make_user_exec_ctx`. This is particularly useful when:

- Working with the Java client where no default execution context exists.
- Creating isolated environments with custom update graphs.
- Using an `EventDrivenUpdateGraph` for specific use cases.
- Building completely independent execution environments.

The following example demonstrates how to build an `ExecutionContext` from scratch using Java interop:

```python skip-test
import jpy
from deephaven.jcompat import j_hashmap
import tempfile

ExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
QueryCompilerImpl = jpy.get_type("io.deephaven.engine.context.QueryCompilerImpl")
PeriodicUpdateGraph = jpy.get_type(
    "io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph"
)
OperationInitializer = jpy.get_type(
    "io.deephaven.engine.updategraph.OperationInitializer"
)

temp_dir = tempfile.mkdtemp(prefix="qc_")

execution_context = (
    ExecutionContext.newBuilder()
    .newQueryLibrary()
    .newQueryScope()
    .setOperationInitializer(OperationInitializer.NON_PARALLELIZABLE)
    .setUpdateGraph(PeriodicUpdateGraph.newBuilder("MyCustomGraph").build())
    .setQueryCompiler(
        QueryCompilerImpl.create(
            jpy.get_type("java.io.File")(temp_dir),
            jpy.get_type("java.lang.ClassLoader").getSystemClassLoader(),
        )
    )
    .build()
)
```

> [!NOTE]
> When creating a new update graph, you must provide a unique name. If you're running this in a Deephaven server where a "DEFAULT" update graph already exists, use a different name like "MyCustomGraph" to avoid conflicts.

This approach allows you to specify:

- **Query library**: A new, empty query library via `newQueryLibrary()`.
- **Query scope**: A new, empty query scope via `newQueryScope()`.
- **Operation initializer**: Parallelization behavior of operations.
- **Update graph**: A custom update graph (e.g., `PeriodicUpdateGraph` or `EventDrivenUpdateGraph`).
- **Query compiler**: A compiler instance with a specified working directory and class loader.

For use cases requiring event-driven updates instead of periodic updates, you can substitute an `EventDrivenUpdateGraph`:

```python skip-test
import jpy
import tempfile

ExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
QueryCompilerImpl = jpy.get_type("io.deephaven.engine.context.QueryCompilerImpl")
EventDrivenUpdateGraph = jpy.get_type(
    "io.deephaven.engine.updategraph.impl.EventDrivenUpdateGraph"
)
OperationInitializer = jpy.get_type(
    "io.deephaven.engine.updategraph.OperationInitializer"
)

temp_dir = tempfile.mkdtemp(prefix="qc_")

event_driven_graph = EventDrivenUpdateGraph.newBuilder("EventDriven").build()

execution_context = (
    ExecutionContext.newBuilder()
    .newQueryLibrary()
    .newQueryScope()
    .setOperationInitializer(OperationInitializer.NON_PARALLELIZABLE)
    .setUpdateGraph(event_driven_graph)
    .setQueryCompiler(
        QueryCompilerImpl.create(
            jpy.get_type("java.io.File")(temp_dir),
            jpy.get_type("java.lang.ClassLoader").getSystemClassLoader(),
        )
    )
    .build()
)
```

## Related documentation

- [Create an empty table](../how-to-guides/new-and-empty-table.md#empty_table)
- [Deephaven data types](../reference/python/deephaven-python-types.md)
- [`TablePublisher`](../reference/table-operations/create/TablePublisher.md)
- [`update`](../reference/table-operations/select/update.md)
- [Pydoc](/core/pydoc/code/deephaven.execution_context.html#module-deephaven.execution_context)
