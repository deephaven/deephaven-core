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
[`.naturalJoin`](../../reference/table-operations/join/natural-join.md), etc. — undergoes an _initialization_ phase
when the method is called. Initialization produces a result table based on the data in the source table. For example,
with a 100,000-row table called `myTable`, running `myTable.update("X = random()")` will run the `random()` method
100,000 times (once per row).

If an operation's source table is
[refreshing](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/BaseTable.html#isRefreshing()),
then initialization will create a new node in the [update graph](../dag.md) as well.

### Query updates

After initialization, the Periodic Update Graph (UG) monitors source tables for changes and process _updates_ to any
table. For example, if 25,000 rows are added to `myTable`, the UG will run the `random()` method 25,000
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
of updates depends on the size of the Periodic Update Graph Thread Pool.

Consider the following hypothetical example:

```groovy skip-test
// Retrieve a live table:
my_table = get_my_kafka_feed_table()

// Run several independent query operations on 'my_table':
my_table_updated = my_table.update("MyCalculation = computeValue(Col1, Col2, ColRed, ColBlue)")
my_table_filtered1 = my_table.where("ColX < 10000")
my_table_filtered2 = my_table.where("ColY > ColZ")

// Create a result table that depends on the three prior tables:
merged_tables = merge(
    my_table_updated,
    my_table_filtered1,
    my_table_filtered2
)
```

The three intermediate tables `my_table_updated`, `my_table_filtered1` and `my_table_filtered2` all depend on only one
other table — the original `my_table`. Since they are independent of each other, when `my_table` is updated with new or
modified rows it is possible for the query engine to process the new rows into `my_table_updated`, `my_table_filtered1`
and `my_table_filtered2` at the same time. However, since `merged_tables` depends on those three tables, the query
engine cannot update the result of the [`merge`](../../reference/table-operations/merge/merge.md) operation until after
the `update()` and `where()`s for those three tables have been processed.

### Controlling Concurrency for `select`, `update` and `where`

The `select`, `update`, and `where` operations can parallelize within a single where clause or column expression. This can greatly improve throughput by using multiple threads to read existing columns or compute functions. Deephaven can only parallelize an expression if it is _stateless_, meaning it does not depend on any mutable external inputs or the order in which rows are evaluated. Many operations, such as String manipulation or arithmetic on one or more input columns are stateless. By default, the Deephaven engine assumes that expressions are not stateless. For `select` and `update`, you can change the configuration property `QueryTable.statelessSelectByDefault` to `true` to make columns stateless by default. For filters, change the property `QueryTable.statelessFiltersByDefault`.

> [!NOTE]
> In a future version of Deephaven, filters and selectables will be stateless by default.

The [`ConcurrencyControl`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) interface allows you to control the behavior of [`Filter`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) (where clause) and [`Selectable`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) (column formula) objects.

#### Key terms

Three concepts work together to control parallelization:

- **Selectable**: A column expression object used in `select` or `update` operations. Created using `Selectable.of()`.
- **Serial**: A property applied to a Selectable or Filter that forces in-order row evaluation. Applied using `.withSerial()`.
- **Barrier**: An explicit ordering mechanism that controls when Selectables or Filters can begin evaluation. One Selectable declares a barrier, and another respects it.

You can control ordering in two ways:

1. **Mark a Selectable as serial** - Ensures rows are evaluated in order; may also create implicit barriers between serial Selectables (config-dependent).
2. **Use explicit barriers** - Provides fine-grained control over which Selectables must complete before others begin.

#### Using serial Selectables and Filters

To explicitly mark a Selectable or Filter as stateful, use the `withSerial` method.

- A serial Filter cannot be reordered with respect to other Filters. Every input row to a serial Filter is evaluated in order.
- When a Selectable is serial, every row for that column is evaluated in order.

> [!IMPORTANT] > `ConcurrencyControl` cannot be applied to Selectables passed to `view` or `updateView`. These operations compute results on demand and cannot enforce ordering constraints. Use `select` or `update` instead when serial evaluation or barriers are needed.

**Implicit barriers and serial Selectables:**

Serial Selectables have different behavior depending on the `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` configuration:

- **When `true` (default for stateful mode)**: Each serial Selectable acts as an absolute barrier with respect to all other serial Selectables. This means serial Selectables cannot run concurrently with each other, allowing them to safely access shared global state. Non-serial Selectables may still be reordered.
- **When `false` (default for stateless mode)**: Serial Selectables enforce in-order row evaluation within their own column, but don't create barriers between different Selectables. Natural column dependencies still apply (if column B references column A, then A evaluates before B). Use explicit barriers for additional ordering constraints.

The `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` value is controlled by the `QueryTable.serialSelectImplicitBarriers` property, and defaults to the inverse of `QueryTable.statelessSelectByDefault`.

#### Using explicit barriers

Filters and Selectables may declare a [`Barrier`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.Barrier.html). A barrier is an opaque object (compared using reference equality) used to control evaluation order between Filters or Selectables.

Subsequent Filters or Selectables may respect a previously declared barrier:

- If a Filter respects a barrier, it cannot begin evaluation until the Filter that declared the barrier has been completely evaluated.
- If a Selectable respects a barrier, it cannot begin evaluation until the Selectable that declared the barrier has been completely evaluated.

In this code block, two columns reference the AtomicInteger `a`:

```groovy order=null
import java.util.concurrent.atomic.AtomicInteger

a = new AtomicInteger(0)
t = emptyTable(1_000_000).update("A=a.getAndIncrement()", "B=a.getAndIncrement()")
```

Deephaven's default behavior is to treat both `A` and `B` statefully, therefore the table is equivalent to:

```groovy order=null
t = emptyTable(1_000_000).update("A=i", "B=1_000_000 + i")
```

However, when the columns are stateless, then the rows from either column can be evaluated in any order. To indicate that `A` must be evaluated before `B`, we can use a barrier:

```groovy order=null
import java.util.concurrent.atomic.AtomicInteger

a = new AtomicInteger(0)
t = emptyTable(1_000_000).update(List.of(
        Selectable.of(ColumnName.of("A"), RawString.of("a.getAndIncrement()")).withDeclaredBarriers(a),
        Selectable.of(ColumnName.of("B"), RawString.of("a.getAndIncrement()")).withRespectedBarriers(a)))
```

Similarly, we can prevent values of A from appearing out of order using `withSerial`:

```groovy order=null
import java.util.concurrent.atomic.AtomicInteger

a = new AtomicInteger(0)
t=emptyTable(1_000_000).update(List.of(
        Selectable.of(ColumnName.of("A"), RawString.of("a.getAndIncrement()")).withSerial(),
        Selectable.of(ColumnName.of("B"), RawString.of("a.getAndIncrement()"))))
```

### Managing thread pool sizes

The maximum parallelism of query initialization and update processing is determined by the Operation Initialization
Thread Pool and the Periodic Update Graph Thread Pool. The size of these values is controlled using the properties
described in the table below:

| Thread Pool Property                      | Default Value | Description                                                                                                    |
| ----------------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------- |
| OperationInitializationThreadPool.threads | -1            | Determines the number of threads available for parallel processing of initialization operations.               |
| PeriodicUpdateGraph.updateThreads         | -1            | Determines the number of threads available for parallel processing of the Periodic Update Graph refresh cycle. |

Setting either of these properties to `-1` instructs Deephaven to use all available processors. The number of available
processors is retrieved from the Java Virtual Machine at Deephaven startup,
using [Runtime.availableProcessors()](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html#availableProcessors()).

### Related documentation

- [Deephaven’s Directed-Acyclic-Graph (DAG)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
