---
title: Parallelizing queries
sidebar_label: Parallelization
---

Deephaven supports using multiple processors to speed up query evaluation. The extent to which Deephaven employs
multiple processors depends on both the phase of operation and the query itself.

## Query initialization and updates

When considering Deephaven query performance, there are two distinct phases to consider: initialization and updates.

### Query initialization

Every table operation method — [`.where`](../../reference/table-operations/filter/where.md),
[`.update`](../../reference/table-operations/select/update.md),
[`.natural_join`](../../reference/table-operations/join/natural-join.md), etc. — undergoes an _initialization_ phase
when the method is called. Initialization produces a result table based on the data in the source table. For example,
with a 100,000-row table called `myTable`, running `myTable.update("X = random()")` will run the `random()` method
100,000 times (once per row).

If an operation's source table is
[refreshing](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/BaseTable.html#isRefreshing()),
then initialization will create a new node in the [update graph](../dag.md) as well.

### Query updates

After initialization, the Update Graph Processor monitors source tables for changes and process _updates_ to any
table. For example, if 25,000 rows are added to `myTable`, the Update Graph will run the `random()` method 25,000
more times, calculating the value of column `X` for each of the new rows.

## Parallelizing queries

### Parallelizing query initialization

Deephaven is a column-oriented query engine — it focuses on handling data one column at a time, instead of one row at
a time like many traditional databases. Since
Deephaven [column sources](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) support random
access to data, different segments of a column can be processed in parallel. When possible, the Deephaven
engine will do this automatically, based on the number of threads in the Operation Initialization Thread Pool.

### Parallelizing query updates

As with query initialization, some operations can process different sections of a column in parallel. However, update
processing can also be parallelized across independent nodes of the [DAG](../dag.md). Parallel processing
of updates depends on the size of the Update Graph Processor Thread Pool.

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

The three intermediate tables `my_table_updated`, `my_table_filtered1` and `my_table_filtered2` all depend on only one
other table — the original `my_table`. Since they are independent of each other, when `my_table` is updated with new or
modified rows it is possible for the query engine to process the new rows into `my_table_updated`, `my_table_filtered1`
and `my_table_filtered2` at the same time. However, since `merged_tables` depends on those three tables, the query
engine cannot update the result of the [`merge`](../../reference/table-operations/merge/merge.md) operation until after
the [`update`](../../reference/table-operations/select/update.md) and [`where`](../../reference/table-operations/filter/where.md)s for those three tables have been processed.

### Controlling Concurrency for `select`, `update` and `where`

The [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) operations can parallelize within a single where clause or column expression. This can greatly improve throughput by using multiple threads to read existing columns or compute functions.

Deephaven can only parallelize an expression if it is _stateless_, meaning it does not depend on any mutable external inputs or the order in which rows are evaluated. Many operations, such as string manipulation or arithmetic on one or more input columns, are stateless.

By default, the Deephaven engine assumes that are stateless. For [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md), you can change the configuration property `QueryTable.statelessSelectByDefault` to `true` to make columns stateless by default. For filters, change the property `QueryTable.statelessFiltersByDefault`.

> [!NOTE]
> In Deephaven 41.0 and later, filters and selectables are _stateless_ by default. In previous versions, filters and selectables were _stateful_ by default.

> [!NOTE]
> In Python builds that use the GIL (global interpreter lock), parallelizing filters and selectables can negatively impact query performance. To prevent performance regressions, even stateless filters and selectables that use Python objects are not parallelized unless the Python build is free-threaded.

The [`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) interface allows you to control the behavior of [`Filter`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) (where clause) and [`Selectable`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) objects (update and select table operations).

To explicitly mark a Selectable or Filter as stateful, use the `with_serial` method.

- A serial Filter cannot be reordered with respect to other Filters. Every input row to a serial Filter is evaluated in order.
- When a Selectable is serial, every row for that column is evaluated in order.
- For Selectables, additional ordering constraints are controlled by `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS`, which is set by the property `QueryTable.serialSelectImplicitBarriers`. The default value is the inverse of `QueryTable.statelessSelectByDefault`:
  - When Selectables are stateless by default, no implicit barriers are added (`QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false).
  - When Selectables are stateful by default, implicit barriers are added (`QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is true).
- If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is false, no additional ordering between expressions is imposed. As with every [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) call, if column B references column A, then column A is evaluated before column B. To impose further ordering constraints, use barriers.
- If `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is true, a serial Selectable acts as an absolute barrier with respect to all other serial Selectables. This prohibits serial Selectables from being evaluated concurrently, permitting them to access global state. Non-serial Selectables may be reordered with respect to a serial Selectable.

Filters and Selectables may declare a [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier). A barrier is an opaque object (compared using reference equality) used to control evaluation order between Filters or Selectables.

Subsequent Filters or Selectables may respect a previously declared barrier:

- If a Filter respects a barrier, it cannot begin evaluation until the Filter that declared the barrier has been completely evaluated.
- If a Selectable respects a barrier, it cannot begin evaluation until the Selectable that declared the barrier has been completely evaluated.

In this code block, two columns call a Python stateful function that is not thread-safe:

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

Deephaven's default behavior is to treat both `A` and `B` statefully, therefore the table is equivalent to:

```python order=null
from deephaven import empty_table

t = empty_table(1_000_000).update(["A=i", "B=1_000_000 + i"])
```

However, if the columns were marked as stateless (e.g., if `QueryTable.statelessSelectByDefault` were `true`), the rows from either column could be evaluated in any order, potentially causing race conditions. To ensure that all rows of `A` are evaluated before any rows of `B` begin evaluation, use a barrier:

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

Alternatively, you can ensure that values of `A` are evaluated in order by using `with_serial` on a Selectable:

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

Setting either of these properties to `-1` instructs Deephaven to use all available processors. The number of available
processors is retrieved from the Java Virtual Machine at Deephaven startup, using [Runtime.availableProcessors()](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html#availableProcessors()).

### Related documentation

- [Deephaven’s Directed-Acyclic-Graph (DAG)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
