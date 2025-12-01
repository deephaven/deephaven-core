---
title: Parallelizing queries
sidebar_label: Parallelization
---

Deephaven supports using multiple processors to speed up query evaluation. The extent to which Deephaven employs multiple processors depends on both the phase of operation and the query itself.

## Query initialization and updates

When considering Deephaven query performance, there are two distinct phases to consider: initialization and updates.

### Query initialization

Every table operation method — [`.where`](../../reference/table-operations/filter/where.md),
[`.update`](../../reference/table-operations/select/update.md),
[`.natural_join`](../../reference/table-operations/join/natural-join.md), etc. — undergoes an _initialization_ phase when the method is called. Initialization produces a result table based on the data in the source table. For example, with a 100,000-row table called `myTable`, running `myTable.update("X = random()")` will run the `random()` method 100,000 times (once per row).

If an operation's source table is [refreshing](<https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/BaseTable.html#isRefreshing()>), then initialization will create a new node in the [update graph](../dag.md) as well.

### Query updates

After initialization, the Update Graph Processor monitors source tables for changes and process _updates_ to any table. For example, if 25,000 rows are added to `myTable`, the Update Graph will run the `random()` method 25,000 more times, calculating the value of column `X` for each of the new rows.

## Parallelizing queries

### Parallelizing query initialization

Deephaven is a column-oriented query engine — it focuses on handling data one column at a time, instead of one row at a time like many traditional databases. Since
Deephaven [column sources](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) support random access to data, different segments of a column can be processed in parallel. When possible, the Deephaven engine will do this automatically, based on the number of threads in the Operation Initialization Thread Pool.

### Parallelizing query updates

As with query initialization, some operations can process different sections of a column in parallel. However, update processing can also be parallelized across independent nodes of the [DAG](../dag.md). Parallel processing of updates depends on the size of the Update Graph Processor Thread Pool.

Consider the following hypothetical example:

```python skip-test
## Retrieve a live table:
my_table = get_my_kafka_feed_table()

## Run several independent query operations on 'my_table':
my_table_updated = my_table.update(
    "MyCalculation = computeValue(Col1, Col2, ColRed, ColBlue)"
)
my_table_filtered1 = my_table.where("ColX < 10000")
my_table_filtered2 = my_table.where("ColY > ColZ")

## Create a result table that depends on the three prior tables:
from deephaven import merge

merged_tables = merge(my_table_updated, my_table_filtered1, my_table_filtered2)
```

The three intermediate tables `my_table_updated`, `my_table_filtered1` and `my_table_filtered2` all depend on only one other table — the original `my_table`. Since they are independent of each other, when `my_table` is updated with new or modified rows it is possible for the query engine to process the new rows into `my_table_updated`, `my_table_filtered1` and `my_table_filtered2` at the same time. However, since `merged_tables` depends on those three tables, the query engine cannot update the result of the [`merge`](../../reference/table-operations/merge/merge.md) operation until after
the [`update`](../../reference/table-operations/select/update.md) and [`where`](../../reference/table-operations/filter/where.md)s for those three tables have been processed.

### Controlling Concurrency for `select`, `update` and `where`

The [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) operations can parallelize within a single where clause or column expression. This can greatly improve throughput by using multiple threads to read existing columns or compute functions.

Deephaven can only parallelize an expression if it is _stateless_, meaning it does not depend on any mutable external inputs or the order in which rows are evaluated. Many operations, such as string manipulation or arithmetic on one or more input columns, are stateless.

By default, the Deephaven engine assumes that expressions are stateful (not stateless). For [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md), you can change the configuration property `QueryTable.statelessSelectByDefault` to `true` to make columns stateless by default. For filters, change the property `QueryTable.statelessFiltersByDefault`.

> [!NOTE]
> In a future version of Deephaven, filters and selectables will be stateless by default.

The [`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) interface allows you to control the behavior of [`Filter`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) (where clause) and [`Selectable`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) objects (update and select table operations).

> [!NOTE]
> Most queries don't need serial execution or barriers. Use these features only when you have:
>
> - Operations with **stateful side effects** (e.g., updating global variables)
> - Operations that must execute in a **specific order** due to dependencies
> - **Race conditions** causing incorrect results in parallel execution

#### Example: When you need ordering control

This example demonstrates a common problem that requires ordering control. Two columns call a function that maintains global state:

```python order=null
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


t = empty_table(1_000_000).update(
    ["A = get_and_increment_counter()", "B = get_and_increment_counter()"]
)
```

Deephaven's default behavior treats both `A` and `B` as stateful, evaluating them in order. However, if marked as stateless, parallel evaluation could cause race conditions. The following sections show how to control evaluation order explicitly.

#### Key terms

Three concepts work together to control parallelization:

- **Selectable**: A column expression object used in `select` or `update` operations. Created using `Selectable.parse(formula="ColumnName = expression")`.
- **Serial**: A property applied to a Selectable or Filter that forces in-order row evaluation. Applied using `.with_serial()`.
- **Barrier**: An explicit ordering mechanism that controls when Selectables or Filters can begin evaluation. One Selectable declares a barrier, and another respects it.

#### Choosing the right approach

Use this guide to decide which concurrency control method fits your needs:

**Use `.with_serial()`** if:

- You need rows evaluated **in order within a single operation** (e.g., sequential state updates)
- You have a single Filter or Selectable with order-dependent logic
- Example: Processing events in chronological sequence

**Use explicit barriers** if:

- You need to control **ordering between different operations** (operation A must finish before operation B starts)
- Multiple Filters or Selectables have dependencies
- Example: Filter A populates a cache that Filter B reads from

**Use implicit barriers** (serial Selectables in stateful mode) if:

- Multiple operations share global state and shouldn't run concurrently
- You want the engine to automatically prevent concurrent execution
- This is the default behavior when operations are marked stateful

#### Using serial Selectables and Filters

To explicitly mark a Selectable or Filter as stateful, use the `with_serial` method.

- A serial Filter cannot be reordered with respect to other Filters. Every input row to a serial Filter is evaluated in order.
- When a Selectable is serial, every row for that column is evaluated in order.

> [!IMPORTANT] > `ConcurrencyControl` cannot be applied to Selectables passed to [`view`](../../reference/table-operations/select/view.md) or [`update_view`](../../reference/table-operations/select/update-view.md). These operations compute results on demand and cannot enforce ordering constraints. Use [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) instead when serial evaluation or barriers are needed.

**Serial Filter example:**

Serial filters are needed when filter evaluation has stateful side effects or when order matters between multiple filters. When using string-based filters in `where()`, filters are automatically parallelized if stateless. To force serial evaluation, construct Filter objects explicitly using the `with_serial()` method:

```python order=null
from deephaven import empty_table
from deephaven.filters import is_null, not_

# Create filters with serial evaluation
filter1 = is_null("X").with_serial()
filter2 = not_(is_null("Y")).with_serial()

result = (
    empty_table(1000)
    .update(["X = i % 10 == 0 ? null : i", "Y = i % 5 == 0 ? null : i"])
    .where([filter1, filter2])
)
```

**When to use serial vs barriers:**

- Use **`.with_serial()`** when you need in-order row evaluation within a single Filter or Selectable (e.g., maintaining sequential state).
- Use **explicit barriers** when you need to control the order between different Filters or Selectables (e.g., Filter A must complete before Filter B starts).

**Filter barrier example:**

Filters can also use explicit barriers to control evaluation order. This is useful when one filter depends on side effects from another:

```python order=null
from deephaven import empty_table
from deephaven.filters import is_null
from deephaven.concurrency_control import Barrier

# Create barrier to enforce ordering
barrier = Barrier()

# Filter1 declares the barrier
filter1 = is_null("X").with_declared_barriers(barrier)

# Filter2 respects the barrier and won't start until filter1 completes
filter2 = is_null("Y").with_respected_barriers(barrier)

result = (
    empty_table(1000)
    .update(["X = i % 10 == 0 ? null : i", "Y = i % 5 == 0 ? null : i"])
    .where([filter1, filter2])
)
```

**Advanced: Implicit barriers and serial Selectables**

Serial Selectables can create implicit barriers between each other, controlled by `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS`:

- **Stateful mode** (default): Serial Selectables act as barriers to other serial Selectables, preventing concurrent execution
- **Stateless mode**: Serial Selectables only enforce in-order row evaluation within themselves; use explicit barriers for cross-operation ordering

Most users can rely on the default stateful behavior. Use explicit barriers when you need fine-grained control.

#### Using explicit barriers

Filters and Selectables may declare a [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier). A barrier is an opaque object (compared using reference equality) used to control evaluation order between Filters or Selectables.

Subsequent Filters or Selectables may respect a previously declared barrier:

- If a Filter respects a barrier, it cannot begin evaluation until the Filter that declared the barrier has been completely evaluated.
- If a Selectable respects a barrier, it cannot begin evaluation until the Selectable that declared the barrier has been completely evaluated.

#### Detailed example: Two approaches for ordering control

Returning to the counter example shown earlier, here are two ways to ensure column `A` completes before column `B` starts when expressions are marked stateless:

**Approach 1: Using explicit barriers**

To ensure that all rows of `A` are evaluated before any rows of `B` begin evaluation, use a barrier:

```python order=null
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


barrier = Barrier()
col_a = Selectable.parse(
    formula="A = get_and_increment_counter()"
).with_declared_barriers(barrier)
col_b = Selectable.parse(
    formula="B = get_and_increment_counter()"
).with_respected_barriers(barrier)

t = empty_table(1_000_000).update([col_a, col_b])
```

**Approach 2: Using serial Selectables**

Alternatively, you can ensure that values of `A` are evaluated in order by using `with_serial`. When `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is true (the default in stateful mode), marking `col_a` as serial creates an implicit barrier that prevents `col_b` from evaluating concurrently:

```python order=null
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


barrier = Barrier()
col_a = Selectable.parse(formula="A = get_and_increment_counter()").with_serial()
col_b = Selectable.parse(formula="B = get_and_increment_counter()")

t = empty_table(1_000_000).update([col_a, col_b])
```

### Managing thread pool sizes

The maximum parallelism of query initialization and update processing is determined by the Operation Initialization
Thread Pool and the Update Graph Processor Thread Pool. The size of these values is controlled using the properties
described in the table below:

| Thread Pool Property                      | Default Value | Description                                                                                                     |
| ----------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------- |
| OperationInitializationThreadPool.threads | -1            | Determines the number of threads available for parallel processing of initialization operations.                |
| PeriodicUpdateGraph.updateThreads         | -1            | Determines the number of threads available for parallel processing of the Update Graph Processor refresh cycle. |

Setting either of these properties to `-1` instructs Deephaven to use all available processors. The number of available processors is retrieved from the Java Virtual Machine at Deephaven startup, using [Runtime.availableProcessors()](<https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html#availableProcessors()>).

### Related documentation

- [Deephaven’s Directed-Acyclic-Graph (DAG)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
