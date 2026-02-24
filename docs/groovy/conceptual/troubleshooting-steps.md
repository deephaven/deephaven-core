---
title: Troubleshoot query performance
---

This guide discusses common performance issues in queries, as well as some steps you can take to resolve them.

## Concepts

It's important to understand some Deephaven-specific concepts before continuing. Each subsection below describes a concept relevant to query performance that is important to understand for the [troubleshooting steps](#troubleshooting-steps) that follow.

### Column types

There are three different types of columns in tables that you should be familiar with. These are not the only three column types in Deephaven, but they are the most common and are created from table operations. They are:

- In-memory
  - An in-memory column has all of its values computed immediately and stored in memory.
- Formula
  - A formula column stores only the formula immediately, then computes values on demand as needed.
- Memoized
  - A memoized column caches the results of calculations so that subsequent requests for the same value do not require recalculation.

A deeper dive on these column types and the table operations that produce them can be found in the [Select and create columns](../how-to-guides/use-select-view-update.md) guide.

### Ratio

Deephaven performs calculations in update cycles. Each cycle lasts a certain amount of time, which is one second by default. During each update cycle, the engine spends a portion of that time performing various operations such as adding columns, performing aggregations, and so on. The fraction of time spent on all operations in a given update cycle is called the "ratio". So, the ratio is a measurement of the percentage of time spent doing data processing during each update cycle. For example, if the engine spends 200 ms processing data during a one-second update cycle, the ratio is 0.2 (or 20%).

The ratio can be calculated from data found in Deephaven's performance tables. For an example showing the calculation, see [Performance tables](../how-to-guides/performance/performance-tables.md#example-use-case).

### Row change notification

In Deephaven, a row change notification occurs when data in a row is modified. When that happens, the engine re-evaluates formulas (like those in calls to [`update`](../reference/table-operations/select/update.md), [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md), and other table operations).

In static tables, a formula need not be re-evaluated because the table does not change. However, in ticking tables, row change notifications trigger regularly. Performant queries on ticking tables minimize the number of row change notifications that occur.

### Tick amplification

Tick amplification occurs any time an operation produces a downstream update that changes a larger number of cells than the upstream update it is processing. There are certain operations in Deephaven where the engine can't know exactly which cells in a table will change. As a result, the engine _must_ check every cell that could possibly change to ensure the results are correct. For instance, a grouping and ungrouping operation may only change a single value, but every single member of the group must be checked to ensure the results are correct.

For an example, see [Tick amplification](../how-to-guides/partitioned-tables.md#tick-amplification).

## Troubleshooting steps

### Reduce ratio

Performant queries minimize the [ratio](#ratio) to reduce the amount of time spent processing data in each update cycle. The following subsections describe some steps to reduce ratio in your queries.

#### Create in-memory columns

In-memory columns are created with the following two selection operations:

- [`select`](../reference/table-operations/select/select.md)
- [`update`](../reference/table-operations/select/update.md)

When calculations are complex or expensive, it's typically best practice to perform them at once and store the results. This way, Deephaven accesses and evaluates the results directly rather than recalculating them each time a downstream operation is performed. Keep in mind that storing large datasets requires a large amount of memory. See [choose the right selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) for more information.

#### Reduce tick frequency

You can use [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) to reduce the frequency at which a table ticks. Keep in mind that snapshotting a table pulls all of its data into memory.

#### Reorder join operations

In queries on ticking tables, it's best practice to join tables in order of how often they tick. Generally speaking, it's best to join the tables that tick most often last.

Formulas in table operations need only be evaluated once when the table is static. In ticking tables, these need to be re-evaluated on any rows that change each time the table ticks.

Consider three tables:

- `staticTable` is a static table.
- `tableA` ticks once every 5 seconds.
- `tableB` ticks 10 times per second.

You need to do two things with these three tables:

- Create a new column in the result that is a formula based on the values in `tableA`.
- Join all three of them together.

You could do this in two ways:

```groovy skip-test
// Ordering: join table b first, then table a, then create a new column
result = (
    staticTable.naturalJoin(tableB, keyColumns)
    .naturalJoin(tableA, keyColumns)
    .update("NewColumn = someFormula(OldColumn)")
)
```

Or:

```groovy skip-test
// Ordering: join table a first, then create a new column, then join table b
result = (
    staticTable.naturalJoin(tableA, keyColumns)
    .update("NewColumn = some_formula(OldColumn)")
    .naturalJoin(tableB, keyColumns)
)
```

Consider each case:

1. In this case, `tableB` is joined first, which ticks 50x more than `tableA`. Not only that, but the [`update`](../reference/table-operations/select/update.md) operation is performed on the result of the join, which means that the formula is evaluated 50x more often than needed.
2. In this case, `tableA` is joined first. Then, the [`update`](../reference/table-operations/select/update.md) operation is applied, which means it gets evaluated once every 5 seconds. Lastly, `tableB` is joined.

The second case is _much_ more performant for two reasons:

- The formula is evaluated much less often.
- The joins are ordered so that the table that ticks most often is joined last, which means it has the least impact on the overall performance of the query.

#### Use dynamic filters

You can reduce computational load by applying dynamic filters in your queries via [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md). These operations allow you to filter a table based on values in another table. By defining a table with values of interest, such as the highest-volume stocks, or all stocks in a specific sector, you can reduce the number of rows that need to be processed in your query. When the values in the values-of-interest table change, the result of the dynamic filter updates in tandem. The changes propagate through downstream query operations.

#### Avoid tick amplification

Tick amplification can take place in some of the following operations:

- [Grouping and ungrouping](../how-to-guides/grouping-data.md)
  - Very small parent changes may unnecessarily mark the entire table as changed.
- [Cross joins](../reference/table-operations/join/join.md) (if the `on` parameter is not given)
  - Even when the left table is static, any right table changes affect k-times as many cells as the original right table update.

You can typically minimize the effect of tick amplification with [partitioned tables](../how-to-guides/partitioned-tables.md). For each case mentioned above, they help in the following way:

- In grouping and ungrouping, partitioned tables group smaller sets of data, meaning parent changes affect fewer cells.
- In cross joins, each partition can be processed in parallel. Additionally, the working size of each partition is smaller than the whole. This means that the number of cells affected by a right table change is smaller than the whole table.

### Insufficient memory

Every instance of Deephaven has a predefined maximum amount of memory. When memory usage approaches the configured maximum, query performance can degrade significantly. If a query requires more memory than what's available, it will crash.

When working with live data, the amount of memory a query requires typically grows over time. The extent of this growth depends on a number of factors such as the growth rate of the data itself, the operations involved, and the [table types](./table-types.md) used in the query.

#### Create formula columns

Unlike [in-memory columns](#create-in-memory-columns), formula columns store only the formula itself. Values are calculated as they are needed, such as for downstream operations. This has some performance implications:

- It allows you to create tables far larger than the amount of memory would typically allow you to.
- It leads to slower performance if formulas are complex or reused often.
- It can lead to more row change notifications, which can also degrade performance.
- It can lead to inconsistent results if a formula is not deterministic.

High memory usage is often unavoidable when working with large datasets. Deephaven offers two operations to create formula columns in tables:

- [`view`](../reference/table-operations/select/view.md)
- [`updateView`](../reference/table-operations/select/update-view.md)

See [choose the right selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) for more information.

#### Create memoized columns

Memoized columns are a special type that can only be created with [`lazyUpdate`](../reference/table-operations/select/lazy-update.md). A memoized column is one that stores the unique results of a formula so that subsequent requests for the same value do not require recalculation.

This column type is best used when a calculation is performed on a small set of input values. Since the _unique results_ are stored, the column can grow indefinitely without consuming more memory than necessary. Any time an incoming value is repeated, the engine can simply return the cached result rather than recalculating it.

#### Filter before joins

Filtering tables before joining can significantly reduce memory overhead. The size of a join operation's state depends on the number of rows in the respective left and right tables, as well as the number of unique keys in the supplied key column(s).

## Related documentation

- [Select and create columns](../how-to-guides/use-select-view-update.md)
- [Grouping and ungrouping](../how-to-guides/grouping-data.md)
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Time-series and range joins](../how-to-guides/joins-timeseries-range.md)
- [Partitioned tables](../how-to-guides/partitioned-tables.md)
- [`select`](../reference/table-operations/select/select.md)
- [`update`](../reference/table-operations/select/update.md)
- [`view`](../reference/table-operations/select/view.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
