---
title: Troubleshoot query performance
---

This guide covers the two most common performance problems in Deephaven queries: a high update-cycle ratio and insufficient memory. On ticking tables, both problems show up as results that refresh late or a server that slows down as data arrives. It pays to catch them early. The guide first shows [how to tell which problem a query has](#identify-the-problem). It then explains the concepts the troubleshooting steps rely on, followed by the steps that address each problem.

## Identify the problem

Before you change a query, find out which problem it has. Deephaven's [performance tables](../how-to-guides/performance/performance-tables.md) record what the engine does over time:

- The [Server State](../how-to-guides/performance/performance-tables.md#server-state) table samples memory use and update-cycle durations. [`PerformanceQueries.serverState`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#serverState()) builds it from the [Server State Log](../how-to-guides/performance/performance-tables.md#server-state-log).
  - Its `UGPTimeRatio` column is the [ratio](#update-cycle-ratio), measured as the fraction of each sampling interval that the engine spent running update cycles. A ratio close to 1 means the engine spends almost all of its time running update cycles, with little idle time between them. If the ratio stays close to 1, see [High update-cycle ratio](#high-update-cycle-ratio).
  - Its `UsedMemRatio` column is the fraction of the server's maximum memory in use. If it approaches 1, see [Insufficient memory](#insufficient-memory).
- The [Update Performance Log](../how-to-guides/performance/performance-tables.md#update-performance-log) records how long each operation spends processing updates. When the ratio is high, the [find slow operations example](../how-to-guides/performance/performance-tables.md#in-the-performance-logs) shows how to calculate each operation's share of the time and find the slowest operations.

## Concepts

It's important to understand some Deephaven-specific concepts before continuing. Each subsection below describes a concept used in the [troubleshooting steps](#troubleshooting-steps) that follow.

### Update-cycle ratio

Deephaven performs calculations in update cycles. By default, the engine [targets a cycle length](./periodic-update-graph-configuration.md#targetcycledurationmillis) of one second. A cycle can take longer when there is more work than fits in that time.

The ratio is the fraction of each update cycle that the engine spends processing data, such as adding columns and computing [aggregations](../how-to-guides/dedicated-aggregations.md). For example, if the engine spends 200 ms processing data during a one-second update cycle, the ratio is 0.2 (20%).

[Identify the problem](#identify-the-problem) shows where Deephaven reports the ratio for the whole engine and for each operation.

### In-memory, formula, and memoized columns

Table operations create three common column types:

- In-memory
  - The engine computes all of an in-memory column's values immediately and stores them in memory.
- Formula
  - A formula column stores only the formula and computes values on demand.
- Memoized
  - A memoized column caches the results of calculations so that subsequent requests for the same input values do not require recalculation.

Each type trades memory against computation:

- In-memory columns store every value, so downstream operations read them without recalculating. [High update-cycle ratio](#high-update-cycle-ratio) recommends them.
- Formula columns store almost nothing and recalculate values on every read. [Insufficient memory](#insufficient-memory) recommends them.
- Memoized columns store one result for each distinct set of input values. They use less memory than in-memory columns only when a column has few distinct inputs. With many distinct inputs, the cache can use more memory than an in-memory column.

The [Select and create columns](../how-to-guides/use-select-view-update.md) guide covers these column types and the operations that produce them in more depth.

### Row change notification

In Deephaven, a row change notification occurs when data in a row is modified. When that happens, the engine re-evaluates the operations that depend on the modified columns, such as formulas in [`update`](../reference/table-operations/select/update.md) and aggregations in [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md).

[Static tables](./table-types.md#static-tables) never produce row change notifications, because the table does not change. However, in [ticking tables](./table-types.md), row change notifications trigger regularly. Performant queries on [ticking tables](./table-types.md) minimize the number of row change notifications that occur.

### Tick amplification

Tick amplification occurs when an operation's downstream update reports more modified cells than the upstream update it processes. It has two causes:

- Some operations, such as [grouping and ungrouping](../how-to-guides/grouping-data.md), can't tell which cells changed. They _must_ mark every cell that could have changed as modified, even when only one value changed.
- Other operations, such as [joins](../how-to-guides/joins-exact-relational.md), really do change more cells. One changed right table row changes every left table row that pairs with it.

[Avoid tick amplification](#avoid-tick-amplification) lists the common sources, shows an example, and describes how to address each.

## Troubleshooting steps

### High update-cycle ratio

When the [ratio](#update-cycle-ratio) nears 1, update cycles run longer than their target, so ticking results refresh less often. The following subsections describe some steps to reduce the ratio in your queries.

#### Create in-memory columns

[In-memory columns](#in-memory-formula-and-memoized-columns) trade memory for computation. When calculations are complex or expensive, perform them once and store the results. Downstream operations then read the stored values instead of recalculating them. Two selection operations create in-memory columns:

- [`select`](../reference/table-operations/select/select.md)
- [`update`](../reference/table-operations/select/update.md)

An in-memory column stores a value for every row, so its memory use grows with the table. See [Choose the right column selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) for more information.

#### Reduce tick frequency

You can use [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) to reduce the frequency at which a table ticks. Keep in mind that snapshotting a table pulls all of its data into memory. [How to dial down the update frequency of ticking tables](../how-to-guides/performance/reduce-update-frequency.md) shows examples.

#### Reorder join operations

In queries on [ticking tables](./table-types.md), [join](../how-to-guides/joins-exact-relational.md) the tables that tick most often last.

Each tick of a joined table sends [row change notifications](#row-change-notification) to every operation downstream of the [join](../how-to-guides/joins-exact-relational.md), even operations whose inputs did not change.

Consider three tables:

- `staticTable` is a [static table](./table-types.md#static-tables).
- `tableA` ticks once every 5 seconds.
- `tableB` ticks every update cycle (once per second by default).

You need to do two things with these three tables:

- Create a new column, `NewColumn`, from the `OldColumn` column that `tableA` contributes.
- [Join](../reference/table-operations/join/natural-join.md) all three of them together.

You could do this in two ways. Option 1 [joins](../reference/table-operations/join/natural-join.md) `tableB` first:

```groovy syntax
// Ordering: join table b first, then table a, then create a new column
result = (
    staticTable.naturalJoin(tableB, keyColumns)
    .naturalJoin(tableA, keyColumns)
    .update("NewColumn = someFormula(OldColumn)")
)
```

Option 2 [joins](../reference/table-operations/join/natural-join.md) `tableB` last:

```groovy syntax
// Ordering: join table a first, then create a new column, then join table b
result = (
    staticTable.naturalJoin(tableA, keyColumns)
    .update("NewColumn = someFormula(OldColumn)")
    .naturalJoin(tableB, keyColumns)
)
```

Option 2 is _much_ more performant. `tableB` ticks five times as often as `tableA`, and in option 2 fewer operations process those ticks:

- In option 1, every `tableB` tick passes through the second [`naturalJoin`](../reference/table-operations/join/natural-join.md), the [`update`](../reference/table-operations/select/update.md) operation, and everything downstream of them. Each of these operations processes the update. None of them recomputes `NewColumn`, because its input `OldColumn` comes from `tableA` and didn't change.
- In option 2, the first [`naturalJoin`](../reference/table-operations/join/natural-join.md) and the [`update`](../reference/table-operations/select/update.md) operation sit upstream of the `tableB` [join](../reference/table-operations/join/natural-join.md). They process updates only when `tableA` ticks, once every 5 seconds. Each `tableB` tick notifies only the final [join](../reference/table-operations/join/natural-join.md) and the operations downstream of it.

#### Use dynamic filters

[Dynamic filters](../how-to-guides/use-filters.md#filter-one-table-based-on-another) reduce computational load by filtering a table based on the values in another table. [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) apply dynamic filters.

Define a table that holds the values of interest, such as the highest-volume stocks or all stocks in a specific sector, and filter on it to reduce the number of rows your query processes. When the values in that table change, the filtered result updates, and the changes propagate to downstream operations.

#### Avoid tick amplification

[Tick amplification](#tick-amplification) happens when an operation's update reports more modified cells than the update it processed. Common sources include:

- [Grouping and ungrouping](../how-to-guides/grouping-data.md)
  - Adding or changing one row marks every existing row in its group as modified after [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md).
- [Cross joins](../reference/table-operations/join/join.md) without `columnsToMatch`
  - Even when the left table is static, a change to one right table row changes one result row for every row in the left table.
- [Keyed joins](../how-to-guides/joins-exact-relational.md), such as [`join`](../reference/table-operations/join/join.md) with `columnsToMatch` and [`naturalJoin`](../reference/table-operations/join/natural-join.md)
  - A change to one right table row changes every left table row with a matching key.

For [grouping and ungrouping](../how-to-guides/grouping-data.md), you can typically avoid tick amplification with [partitioned tables](../how-to-guides/partitioned-tables.md). The following example adds a column `Y` to the [ticking table](./table-types.md) `t1` in two ways:

- Group/ungroup: [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) groups the rows by `A`, [`update`](../reference/table-operations/select/update.md) adds `Y` to each group, and [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) expands the groups back into rows.
- Partition/merge: [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) splits `t1` into a [partitioned table](../how-to-guides/partitioned-tables.md) with one constituent table for each value of `A`, [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) applies [`update`](../reference/table-operations/select/update.md) to each constituent, and [`merge`](../reference/table-operations/partitioned-tables/merge.md) combines the constituents back into one table.

A [listener](../how-to-guides/table-listeners-groovy.md) on each table prints the number of rows added or modified in each update:

```groovy ticking-table order=null
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

t1 = timeTable("PT5s").update("A = ii % 2", "X = ii")

// Group/ungroup
t2 = t1.groupBy("A").update("Y = X + 1").ungroup()

// Partition/merge
t3 = t1.partitionBy("A").proxy().update("Y = X + 1").target().merge()

h1 = new InstrumentedTableUpdateListenerAdapter(t1, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        def numChanges1 = len(upstream.added()) + len(upstream.modified())
        println "TICK PROPAGATION: RAW             " + numChanges1 + " changes"
    }
}

h2 = new InstrumentedTableUpdateListenerAdapter(t2, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        def numChanges2 = len(upstream.added()) + len(upstream.modified())
        println "TICK PROPAGATION: GROUP/UNGROUP   " + numChanges2 + " changes"
    }
}

h3 = new InstrumentedTableUpdateListenerAdapter(t3, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        def numChanges3 = len(upstream.added()) + len(upstream.modified())
        println "TICK PROPAGATION: PARTITION/MERGE " + numChanges3 + " changes"
    }
}

t1.addUpdateListener(h1)
t2.addUpdateListener(h2)
t3.addUpdateListener(h3)
```

Initially, the output shows one change for each approach:

```
TICK PROPAGATION: RAW             1 changes
TICK PROPAGATION: GROUP/UNGROUP   1 changes
TICK PROPAGATION: PARTITION/MERGE 1 changes
```

After the query runs for a while, the group/ungroup listener reports a growing number of changes while the partition/merge listener stays at one:

```
TICK PROPAGATION: RAW             1 changes
TICK PROPAGATION: GROUP/UNGROUP   10 changes
TICK PROPAGATION: PARTITION/MERGE 1 changes
```

The group/ungroup version amplifies ticks because [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) marks every row of a changed group as modified. The partition/merge version produces the same rows without amplification. The engine updates each constituent independently, so adding or changing one row affects only that row in the merged result.

For [joins](../how-to-guides/joins-exact-relational.md), the extra changes are real changes to the result, so [partitioning](../how-to-guides/partitioned-tables.md) the tables cannot avoid them without changing the result. Instead, [reduce how often the right table ticks](#reduce-tick-frequency) and [filter the left table before the join](#filter-before-joins).

### Insufficient memory

Every Deephaven server has a configured [maximum amount of memory](../how-to-guides/heap-size.md). When memory usage approaches that maximum, query performance can degrade significantly. If a query requires more memory than is available, it fails with a Java `OutOfMemoryError`, and the server can become unstable.

When working with live data in [ticking tables](./table-types.md), the amount of memory a query requires typically grows over time. The extent of this growth depends on several factors, such as the growth rate of the data itself, the operations involved, and the [table types](./table-types.md) used in the query. High memory usage is often unavoidable with large datasets, but the following steps can reduce it.

#### Create formula columns

[Formula columns](#in-memory-formula-and-memoized-columns) trade computation for memory. Deephaven calculates values when they are needed, such as when a downstream operation reads them. Deephaven offers two operations to create formula columns in tables:

- [`view`](../reference/table-operations/select/view.md)
- [`updateView`](../reference/table-operations/select/update-view.md)

Formula columns have trade-offs:

- They let you create tables far larger than available memory could otherwise hold.
- They lead to slower performance if formulas are complex or downstream operations read their values often.
- They can lead to inconsistent results if a formula is not deterministic.

See [Choose the right column selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) for more information.

#### Create memoized columns

[Memoized columns](#in-memory-formula-and-memoized-columns) work best when a calculation has a small number of distinct input values. Only [`lazyUpdate`](../reference/table-operations/select/lazy-update.md) creates memoized columns. When an input value repeats, the engine returns the cached result instead of recalculating it. The column stores one result for each distinct set of input values, so its memory use grows with the number of distinct inputs, not with the number of rows. The cache never shrinks, so on a ticking table, memoized columns save memory only while the number of distinct inputs stays small.

#### Filter before joins

[Filtering](../how-to-guides/use-filters.md) tables before [joining](../how-to-guides/joins-exact-relational.md) them can significantly reduce memory overhead. The size of a [join](../how-to-guides/joins-exact-relational.md) operation's state depends on the number of rows in the respective left and right tables, as well as the number of unique keys in the key columns.

Filtering the left table first also lowers the [ratio](#update-cycle-ratio) for [keyed joins](#avoid-tick-amplification). Fewer left table rows match each changed right table row, so each right table tick changes fewer result rows.

## Related documentation

- [Periodic Update Graph](./periodic-update-graph-configuration.md)
- [Table types](./table-types.md)
- [Select and create columns](../how-to-guides/use-select-view-update.md)
- [Filter table data](../how-to-guides/use-filters.md)
- [Group and ungroup data](../how-to-guides/grouping-data.md)
- [Single aggregation](../how-to-guides/dedicated-aggregations.md)
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Create and use partitioned tables](../how-to-guides/partitioned-tables.md)
- [Listen to ticking tables](../how-to-guides/table-listeners-groovy.md)
- [Performance tables](../how-to-guides/performance/performance-tables.md)
- [How to dial down the update frequency of ticking tables](../how-to-guides/performance/reduce-update-frequency.md)
- [Configure and adjust heap size](../how-to-guides/heap-size.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`merge`](../reference/table-operations/partitioned-tables/merge.md)
- [`join`](../reference/table-operations/join/join.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- [`select`](../reference/table-operations/select/select.md)
- [`update`](../reference/table-operations/select/update.md)
- [`view`](../reference/table-operations/select/view.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)
- [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- [`PerformanceQueries` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html)
- [`InstrumentedTableUpdateListenerAdapter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html)
