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
the `update()` and `where()`s for those three tables have been processed.

### Managing thread pool sizes

The maximum parallelism of query initialization and update processing is determined by the Operation Initialization
Thread Pool and the Update Graph Processor Thread Pool. The size of these values is controlled using the properties
described in the table below:

| Thread Pool Property                      | Default Value | Description                                                                                                     |
| ----------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------- |
| OperationInitializationThreadPool.threads | -1            | Determines the number of threads available for parallel processing of initialization operations.                |
| PeriodicUpdateGraph.updateThreads         | -1            | Determines the number of threads available for parallel processing of the Update Graph Processor refresh cycle. |

Setting either of these properties to `-1` instructs Deephaven to use all available processors. The number of available
processors is retrieved from the Java Virtual Machine at Deephaven startup,
using [Runtime.availableProcessors()](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html#availableProcessors()).

### Related documentation

- [Deephaven’s Directed-Acyclic-Graph (DAG)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
