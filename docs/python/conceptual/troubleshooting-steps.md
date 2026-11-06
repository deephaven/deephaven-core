---
title: Troubleshoot query performance
---

This guide covers the two most common performance problems in Deephaven queries: a high update-cycle ratio and insufficient memory. On ticking tables, both problems show up as results that refresh late or a server that slows down as data arrives. It pays to catch them early. The guide first shows [how to tell which problem a query has](#identify-the-problem). It then explains the concepts the troubleshooting steps rely on, followed by the steps that address each problem.

## Identify the problem

Before you change a query, find out which problem it has. Deephaven's [performance tables](../how-to-guides/performance/performance-tables.md) record what the engine does over time:

- The [Server State](../how-to-guides/performance/performance-tables.md#server-state) table samples memory use and update-cycle durations. [`server_state`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state) builds it from the [Server State Log](../how-to-guides/performance/performance-tables.md#server-state-log).
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

In Deephaven, a row change notification occurs when data in a row is modified. When that happens, the engine re-evaluates the operations that depend on the modified columns, such as formulas in [`update`](../reference/table-operations/select/update.md) and aggregations in [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md).

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

You can use [`snapshot_when`](../reference/table-operations/snapshot/snapshot-when.md) to reduce the frequency at which a table ticks. Keep in mind that snapshotting a table pulls all of its data into memory. [How to dial down the update frequency of ticking tables](../how-to-guides/performance/reduce-update-frequency.md) shows examples.

#### Reorder join operations

In queries on [ticking tables](./table-types.md), [join](../how-to-guides/joins-exact-relational.md) the tables that tick most often last.

Each tick of a joined table sends [row change notifications](#row-change-notification) to every operation downstream of the [join](../how-to-guides/joins-exact-relational.md), even operations whose inputs did not change.

Consider three tables:

- `static_table` is a [static table](./table-types.md#static-tables).
- `table_a` ticks once every 5 seconds.
- `table_b` ticks every update cycle (once per second by default).

You need to do two things with these three tables:

- Create a new column, `NewColumn`, from the `OldColumn` column that `table_a` contributes.
- [Join](../reference/table-operations/join/natural-join.md) all three of them together.

You could do this in two ways. Option 1 [joins](../reference/table-operations/join/natural-join.md) `table_b` first:

```python syntax
# Ordering: join table b first, then table a, then create a new column
result = (
    static_table.natural_join(table_b, key_columns)
    .natural_join(table_a, key_columns)
    .update("NewColumn = some_formula(OldColumn)")
)
```

Option 2 [joins](../reference/table-operations/join/natural-join.md) `table_b` last:

```python syntax
# Ordering: join table a first, then create a new column, then join table b
result = (
    static_table.natural_join(table_a, key_columns)
    .update("NewColumn = some_formula(OldColumn)")
    .natural_join(table_b, key_columns)
)
```

Option 2 is _much_ more performant. `table_b` ticks five times as often as `table_a`, and in option 2 fewer operations process those ticks:

- In option 1, every `table_b` tick passes through the second [`natural_join`](../reference/table-operations/join/natural-join.md), the [`update`](../reference/table-operations/select/update.md) operation, and everything downstream of them. Each of these operations processes the update. None of them recomputes `NewColumn`, because its input `OldColumn` comes from `table_a` and didn't change.
- In option 2, the first [`natural_join`](../reference/table-operations/join/natural-join.md) and the [`update`](../reference/table-operations/select/update.md) operation sit upstream of the `table_b` [join](../reference/table-operations/join/natural-join.md). They process updates only when `table_a` ticks, once every 5 seconds. Each `table_b` tick notifies only the final [join](../reference/table-operations/join/natural-join.md) and the operations downstream of it.

#### Use dynamic filters

[Dynamic filters](../how-to-guides/use-filters.md#filter-one-table-based-on-another) reduce computational load by filtering a table based on the values in another table. [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) apply dynamic filters.

Define a table that holds the values of interest, such as the highest-volume stocks or all stocks in a specific sector, and filter on it to reduce the number of rows your query processes. When the values in that table change, the filtered result updates, and the changes propagate to downstream operations.

#### Avoid tick amplification

[Tick amplification](#tick-amplification) happens when an operation's update reports more modified cells than the update it processed. Common sources include:

- [Grouping and ungrouping](../how-to-guides/grouping-data.md)
  - Adding or changing one row marks every existing row in its group as modified after [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md).
- [Cross joins](../reference/table-operations/join/join.md) without the `on` parameter
  - Even when the left table is static, a change to one right table row changes one result row for every row in the left table.
- [Keyed joins](../how-to-guides/joins-exact-relational.md), such as [`join`](../reference/table-operations/join/join.md) with the `on` parameter and [`natural_join`](../reference/table-operations/join/natural-join.md)
  - A change to one right table row changes every left table row with a matching key.

For [grouping and ungrouping](../how-to-guides/grouping-data.md), you can typically avoid tick amplification with [partitioned tables](../how-to-guides/partitioned-tables.md). The following example adds a column `Y` to the [ticking table](./table-types.md) `t1` in two ways:

- Group/ungroup: [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) groups the rows by `A`, [`update`](../reference/table-operations/select/update.md) adds `Y` to each group, and [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) expands the groups back into rows.
- Partition/merge: [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) splits `t1` into a [partitioned table](../how-to-guides/partitioned-tables.md) with one constituent table for each value of `A`, [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) applies [`update`](../reference/table-operations/select/update.md) to each constituent, and [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md) combines the constituents back into one table.

A [listener](../how-to-guides/table-listeners-python.md) on each table prints the number of rows added or modified in each update:

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen


def print_changes(label, update, is_replay):
    added = update.added()
    modified = update.modified()
    n_added = len(added["X"]) if "X" in added else 0
    n_modified = len(modified["X"]) if "X" in modified else 0
    changes = n_added + n_modified
    print(f"TICK PROPAGATION: {label} {changes} changes")


t1 = time_table("PT5s").update(["A = ii % 2", "X = ii"])

# Group/ungroup
t2 = t1.group_by("A").update("Y = X + 1").ungroup()

# Partition/merge
t3 = t1.partition_by("A").proxy().update("Y = X + 1").target.merge()

h1 = listen(
    t1, lambda update, is_replay: print_changes("RAW            ", update, is_replay)
)
h2 = listen(
    t2, lambda update, is_replay: print_changes("GROUP/UNGROUP  ", update, is_replay)
)
h3 = listen(
    t3, lambda update, is_replay: print_changes("PARTITION/MERGE", update, is_replay)
)
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
- [`update_view`](../reference/table-operations/select/update-view.md)

Formula columns have trade-offs:

- They let you create tables far larger than available memory could otherwise hold.
- They lead to slower performance if formulas are complex or downstream operations read their values often.
- They can lead to inconsistent results if a formula is not deterministic.

See [Choose the right column selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) for more information.

#### Create memoized columns

[Memoized columns](#in-memory-formula-and-memoized-columns) work best when a calculation has a small number of distinct input values. Only [`lazy_update`](../reference/table-operations/select/lazy-update.md) creates memoized columns. When an input value repeats, the engine returns the cached result instead of recalculating it. The column stores one result for each distinct set of input values, so its memory use grows with the number of distinct inputs, not with the number of rows. The cache never shrinks, so on a ticking table, memoized columns save memory only while the number of distinct inputs stays small.

Deephaven can vectorize a query-string call to a [Python function](../how-to-guides/python-functions.md), so that one call processes a chunk of rows instead of one row. [`lazy_update`](../reference/table-operations/select/lazy-update.md) doesn't cache vectorized calls. For those calls, the formula runs for every cell, as it does in [`update_view`](../reference/table-operations/select/update-view.md). See [Cache results with `lazy_update`](../how-to-guides/use-select-view-update.md#cache-results-with-lazy_update).

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
- [Listen to ticking tables](../how-to-guides/table-listeners-python.md)
- [Python functions in query strings](../how-to-guides/python-functions.md)
- [Performance tables](../how-to-guides/performance/performance-tables.md)
- [How to dial down the update frequency of ticking tables](../how-to-guides/performance/reduce-update-frequency.md)
- [Configure and adjust heap size](../how-to-guides/heap-size.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md)
- [`join`](../reference/table-operations/join/join.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
- [`select`](../reference/table-operations/select/select.md)
- [`update`](../reference/table-operations/select/update.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`lazy_update`](../reference/table-operations/select/lazy-update.md)
- [`snapshot_when`](../reference/table-operations/snapshot/snapshot-when.md)
- [`where_in`](../reference/table-operations/filter/where-in.md)
- [`where_not_in`](../reference/table-operations/filter/where-not-in.md)
- [`listen` Pydoc](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.listen)
- [`server_state` Pydoc](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state)
