---
title: Troubleshoot query performance
---

This guide covers the two most common performance problems in Deephaven queries: a high [update-cycle ratio](#update-cycle-ratio) and insufficient memory. On [ticking tables](./table-types.md), both problems show up as results that refresh late or a server that slows down as data arrives. It pays to catch them early. The guide first shows [how to tell which problem a query has](#identify-the-problem). It then explains the concepts the troubleshooting steps rely on, followed by the steps that address each problem.

## Identify the problem

Before you change a query, find out which problem it has. Deephaven's [performance tables](../how-to-guides/performance/performance-tables.md) record what the engine does over time:

- The [Server State](../how-to-guides/performance/performance-tables.md#server-state) table samples memory use and update-cycle durations. [`server_state`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state) builds it from the [Server State Log](../how-to-guides/performance/performance-tables.md#server-state-log).
  - Its `UGPTimeRatio` column is the [ratio](#update-cycle-ratio) for each sampling interval. If the ratio stays close to 1, see [High update-cycle ratio](#high-update-cycle-ratio).
  - Its `UsedMemRatio` column is the fraction of the server's maximum memory in use, and its `GcTimeRatio` column is the fraction of each sampling interval spent collecting garbage. If `UsedMemRatio` stays close to 1 while `GcTimeRatio` stays high, see [Insufficient memory](#insufficient-memory).
- The [Update Performance Log](../how-to-guides/performance/performance-tables.md#update-performance-log) records how long each operation spends processing updates. When the ratio is high, the [example that finds slow operations](../how-to-guides/performance/performance-tables.md#in-the-performance-logs) shows how to calculate each operation's share of the time and find the slowest operations.

## Concepts

It's important to understand some Deephaven-specific concepts before continuing. Each subsection below describes a concept used in the [troubleshooting steps](#troubleshooting-steps) that follow.

### Update-cycle ratio

Deephaven performs calculations in update cycles. By default, the engine [starts a new update cycle every second](./periodic-update-graph-configuration.md#targetcycledurationmillis). When a cycle has more work than fits in that interval, it runs longer and the next cycle starts late. Update cycles do the query's work, such as adding columns and computing [aggregations](../how-to-guides/dedicated-aggregations.md).

The ratio is the fraction of time the engine spends running update cycles, rather than waiting idle for the next one. For example, if the engine spends 200 ms of each second running update cycles, the ratio is 0.2 (20%).

[Identify the problem](#identify-the-problem) shows where to find the ratio for the whole engine and how to compute each operation's share of the time.

### In-memory, formula, and memoized columns

Table operations create three common column types. Each trades memory against computation:

- In-memory: the column stores a value for every row, computed immediately, so downstream operations read stored values without recalculating. Memory use grows with the number of rows. The [High update-cycle ratio](#high-update-cycle-ratio) section recommends them.
- Formula: the column stores only its formula and computes values each time they are read, so it uses almost no memory but repeats the calculation. The [Insufficient memory](#insufficient-memory) section recommends them.
- Memoized: the column caches one result for each distinct set of input values and reuses it when those inputs repeat. With few distinct inputs it uses less memory than an in-memory column; with many, it can use more. The [Insufficient memory](#insufficient-memory) section covers when to use them.

The [Select and create columns](../how-to-guides/use-select-view-update.md) guide covers these column types and the operations that produce them in more depth.

### Row change notification

A row change notification is the update the engine sends downstream when a table's rows change. When a table changes:

- Each operation that listens to it receives the notification and processes it.
- The operation computes values for added rows, and recomputes modified rows only when their input columns changed. Examples include formulas in [`update`](../reference/table-operations/select/update.md) and aggregations in [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md).
- If its result changed, the operation notifies its own downstream operations.

Processing a notification costs time even when nothing is recomputed.

[Static tables](./table-types.md#static-tables) never produce row change notifications, because they don't change. [Ticking tables](./table-types.md) produce one in each update cycle in which rows are added, removed, or modified. Each update cycle in which a table changes is called a tick. Performant queries on [ticking tables](./table-types.md) minimize the number of row change notifications that occur.

### Tick amplification

Tick amplification occurs when the [row change notification](#row-change-notification) an operation sends downstream marks more cells as modified than the notification it received. Every downstream operation that reads a modified cell recomputes its result for that row, so amplification multiplies the work later in the query and raises the [ratio](#update-cycle-ratio). It has two causes:

- Some operations, such as [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) after [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), can't tell which cells changed. They _must_ mark every cell that could have changed as modified, even when only one value changed.
- Other operations, such as [joins](../how-to-guides/joins-exact-relational.md), really do change more cells. One changed right table row changes every left table row that pairs with it.

[Avoid tick amplification](#avoid-tick-amplification) lists the common sources, shows an example, and describes how to address each.

## Troubleshooting steps

### High update-cycle ratio

When the [ratio](#update-cycle-ratio) nears 1, update cycles take up most or all of the time between cycle starts, so ticking results refresh less often. The following subsections describe some steps to reduce the ratio in your queries. [Filter before joins](#filter-before-joins), under Insufficient memory, also lowers the ratio when a query uses [keyed joins](../how-to-guides/joins-exact-relational.md).

#### Create in-memory columns

When calculations are complex or expensive, use [in-memory columns](#in-memory-formula-and-memoized-columns) so the engine performs them once. Two selection operations create in-memory columns:

- [`select`](../reference/table-operations/select/select.md)
- [`update`](../reference/table-operations/select/update.md)

In-memory columns cost memory in proportion to the table's size. See [Choose the right column selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) for more information.

#### Reduce tick frequency

You can use [`snapshot_when`](../reference/table-operations/snapshot/snapshot-when.md) to reduce the frequency at which a table ticks. Keep in mind that [snapshotting](../reference/table-operations/snapshot/snapshot-when.md) a table pulls all of its data into memory. [How to dial down the update frequency of ticking tables](../how-to-guides/performance/reduce-update-frequency.md) shows examples.

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

Option 2 does less work on every `table_b` tick. `table_b` ticks five times as often as `table_a`, and in option 2 fewer operations process those ticks:

- In option 1, every `table_b` tick passes through the second [`natural_join`](../reference/table-operations/join/natural-join.md), the [`update`](../reference/table-operations/select/update.md) operation, and everything downstream of them. Each of these operations processes the row change notification. None of them recomputes `NewColumn`, because its input `OldColumn` comes from `table_a` and didn't change.
- In option 2, the first [`natural_join`](../reference/table-operations/join/natural-join.md) and the [`update`](../reference/table-operations/select/update.md) operation sit upstream of the `table_b` [join](../reference/table-operations/join/natural-join.md). They process row change notifications only when `table_a` ticks, once every 5 seconds. Each `table_b` tick notifies only the final [join](../reference/table-operations/join/natural-join.md) and the operations downstream of it.

#### Use dynamic filters

[Dynamic filters](../how-to-guides/use-filters.md#filter-one-table-based-on-another) reduce computational load by filtering a table based on the values in another table. [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) apply dynamic filters.

Define a table that holds the values of interest, such as the highest-volume stocks or all stocks in a specific sector, and [filter on it](../how-to-guides/use-filters.md#filter-one-table-based-on-another) to reduce the number of rows your query processes. When the values in that table change, the filtered result updates, and the changes propagate to downstream operations. Each time a value enters or leaves the values table, the filter re-checks the source rows it previously rejected or kept, so dynamic filters save the most when the set of values changes less often than the table it filters.

#### Avoid tick amplification

Common sources of [tick amplification](#tick-amplification) include:

- [Grouping and ungrouping](../how-to-guides/grouping-data.md)
  - In the output of [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md), adding or changing one row marks every existing row in its group as modified.
- [Cross joins](../reference/table-operations/join/join.md) without the `on` parameter
  - Even when the left table is static, a change to one right table row changes one result row for every row in the left table.
- [Keyed joins](../how-to-guides/joins-exact-relational.md), such as [`join`](../reference/table-operations/join/join.md) with the `on` parameter and [`natural_join`](../reference/table-operations/join/natural-join.md)
  - Only left table rows whose key matches the changed right table row are affected, so the amplification grows with the number of left table rows for each key.

For [grouping and ungrouping](../how-to-guides/grouping-data.md), you can typically avoid tick amplification with [partitioned tables](../how-to-guides/partitioned-tables.md). The following example adds a column `Y` to the [ticking table](./table-types.md) `t1` in two ways:

- Group/ungroup: [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) groups the rows by `A`, [`update`](../reference/table-operations/select/update.md) adds `Y` to each group, and [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) expands the groups back into rows.
- Partition/merge: [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) splits `t1` into a [partitioned table](../how-to-guides/partitioned-tables.md) with one constituent table for each value of `A`, [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) applies [`update`](../reference/table-operations/select/update.md) to each constituent, and [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md) combines the constituents back into one table.

A [listener](../how-to-guides/table-listeners-python.md) on each table prints the number of rows added or modified in each update:

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen


def print_changes(label, update, is_replay):
    added = update.added("X")
    modified = update.modified("X")
    n_added = len(added["X"]) if added else 0
    n_modified = len(modified["X"]) if modified else 0
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

After the query runs for a while, the group/ungroup count keeps growing while the partition/merge count stays at one. For example, after about 20 ticks (100 seconds), the output looks like this:

```
TICK PROPAGATION: RAW             1 changes
TICK PROPAGATION: GROUP/UNGROUP   10 changes
TICK PROPAGATION: PARTITION/MERGE 1 changes
```

The group/ungroup version amplifies ticks because [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) marks every row of a changed group as modified. The partition/merge version produces the same rows without amplification. The engine updates each constituent independently, so adding or changing one row affects only that row in the merged result.

For [joins](../how-to-guides/joins-exact-relational.md), the extra changes are real changes to the result, so [partitioning](../how-to-guides/partitioned-tables.md) the tables cannot avoid them without changing the result. Instead, [reduce how often the right table ticks](#reduce-tick-frequency) and [filter the left table before the join](#filter-before-joins).

### Insufficient memory

Every Deephaven server has a configured [maximum amount of memory](../how-to-guides/heap-size.md). When memory usage approaches that maximum, query performance can degrade significantly. If a query requires more memory than is available, it fails with a Java `OutOfMemoryError`, and the server can become unstable.

On [ticking tables](./table-types.md), a query's memory use usually grows over time. How fast depends on how quickly the data grows, which operations the query uses, and its [table types](./table-types.md). Large datasets often need a lot of memory, but the following steps can reduce it.

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

[Memoized columns](#in-memory-formula-and-memoized-columns) save memory when a calculation has few distinct input values. Only [`lazy_update`](../reference/table-operations/select/lazy-update.md) creates them. Two limits apply:

- The cache never shrinks, so on a [ticking table](./table-types.md), memoized columns keep saving memory only while the number of distinct inputs stays small.
- [`lazy_update`](../reference/table-operations/select/lazy-update.md) doesn't cache [vectorized](./python-java-boundary.md#vectorize-the-python-calls-you-keep) calls. A vectorized [query-string](../how-to-guides/query-string-overview.md) call passes a chunk of rows to a [Python function](../how-to-guides/python-functions.md) at once instead of one row at a time. For those calls, the formula runs for every cell, as it does in [`update_view`](../reference/table-operations/select/update-view.md).

See [Cache results with `lazy_update`](../how-to-guides/use-select-view-update.md#cache-results-with-lazy_update).

#### Filter before joins

[Filtering](../how-to-guides/use-filters.md) tables before [joining](../how-to-guides/joins-exact-relational.md) them reduces two costs:

- Memory: the size of a [join](../how-to-guides/joins-exact-relational.md) operation's state depends on the number of rows in the left and right tables and on the number of unique keys in the key columns, so filtering first makes it smaller.
- [Update-cycle ratio](#update-cycle-ratio): in a [keyed join](#avoid-tick-amplification), filtering the left table first leaves fewer left table rows to match each changed right table row.

## Related documentation

- [Periodic Update Graph](./periodic-update-graph-configuration.md)
- [Table types](./table-types.md)
- [The Python-Java boundary](./python-java-boundary.md)
- [Select and create columns](../how-to-guides/use-select-view-update.md)
- [Filter table data](../how-to-guides/use-filters.md)
- [Group and ungroup data](../how-to-guides/grouping-data.md)
- [Single aggregation](../how-to-guides/dedicated-aggregations.md)
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Create and use partitioned tables](../how-to-guides/partitioned-tables.md)
- [Listen to ticking tables](../how-to-guides/table-listeners-python.md)
- [Python functions in query strings](../how-to-guides/python-functions.md)
- [Query string overview](../how-to-guides/query-string-overview.md)
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
- [`TableUpdate` Pydoc](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate)
- [`server_state` Pydoc](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state)
