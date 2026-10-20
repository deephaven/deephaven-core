---
title: Aggregations in Deephaven
---

An aggregation summarizes the values in a group of rows, such as an average or a count. Deephaven offers several table operations that group or aggregate data, and each returns a different kind of result. They work on live data as well as static data: when a ticking source table changes, each result updates with it, as described in [Aggregations on ticking tables](#aggregations-on-ticking-tables). This guide explains what each operation returns and how to choose among them.

## What is an aggregation?

An aggregation breaks a table into groups of rows and calculates one or more summary values for each group, such as a sum, an average, or a count. A _single aggregation_, such as [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md), calculates one statistic. A _multi-aggregation_, also called a _combined aggregation_, calculates several in one [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) operation.

The groups come from key columns, which you pass in the `by` argument. When you pass no key columns, the whole table is one group.

## The table operations

The following table compares the operations that group or aggregate data. The sections that follow it describe each operation and link to its methods.

| Operation                                                                                                                                                                                                                     | Result rows                 | Result columns                                                                                                                                                                                 |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Group](../how-to-guides/grouping-data.md) ([`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md))                                                                                                       | One per group               | The key columns, plus a vector of each group's values for every other column                                                                                                                   |
| [Single aggregation](../how-to-guides/dedicated-aggregations.md) ([`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md), and others) | One per group               | The key columns, plus the statistic's results (usually one column per non-key column, or a single count column for [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md)) |
| [Multi-aggregation](../how-to-guides/combined-aggregations.md) ([`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md))                                                                                       | One per group               | The key columns, plus any mix of aggregations you choose                                                                                                                                       |
| [Rolling aggregation](../how-to-guides/rolling-aggregations.md) ([`update_by`](../reference/table-operations/update-by-operations/updateBy.md))                                                                               | The same rows as the source | The source columns, plus new or replaced columns                                                                                                                                               |

The following example runs three of these operations on the same five-row table. `grouped` has one row per `Sym` that holds a vector of that symbol's prices. `summed` has one row per `Sym` that holds the total price. `running` keeps all five rows and adds a running total for each `Sym`.

```python order=grouped,summed,running,source
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.updateby import cum_sum

source = new_table(
    [
        string_col("Sym", ["A", "B", "A", "B", "A"]),
        int_col("Price", [10, 20, 30, 40, 50]),
    ]
)

grouped = source.group_by(by="Sym")
summed = source.sum_by(by="Sym")
running = source.update_by(ops=cum_sum(cols="Total = Price"), by="Sym")
```

### Group

[`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) collects the rows of each group together. Every non-key column becomes a column of [vectors](./arrays-vs-vectors.md). Use it when you want the group's values themselves, not a summary of them.

### Single aggregation

A single aggregation calculates one statistic for each group. Most, such as [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md), [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md), and [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md), apply it to every non-key column, so the result has one column for each input column.

[`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) is the exception: it returns one new column that holds the number of rows in each group, and you choose that column's name.

A numeric statistic such as [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) raises an error if any non-key column has a type it can't compute, such as a string column. Drop those columns first, or use [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) to choose the input columns. On a Boolean column, [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md) counts the `true` values.

[`agg_all_by`](../reference/table-operations/group-and-aggregate/AggAllBy.md) also applies one statistic to every non-key column, but it takes that statistic as an aggregation from [`deephaven.agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg), so you can use one that has no dedicated method, such as [`agg.pct`](../reference/table-operations/group-and-aggregate/AggPct.md).

### Multi-aggregation

[`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) applies a list of aggregations, such as [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md) or [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md), in one operation. Each aggregation can use different input columns, and [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md) lets you write a custom calculation. Import the aggregations with `from deephaven import agg`. Because [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) computes all of its aggregations in one pass, it is more efficient than running a separate single aggregation for each statistic.

### Rolling aggregation

[`update_by`](../reference/table-operations/update-by-operations/updateBy.md) keeps every row of the source table and adds columns calculated by one or more update-by operations. The calculations can be cumulative (such as [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md)), exponentially weighted (such as [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md)), computed over a window of rows or time (such as [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md)), or based on the previous row (such as [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md) or [`delta`](../reference/table-operations/update-by-operations/delta.md)). With key columns, each group is calculated independently, but the rows stay in their original positions.

### Operations that return other table types

Two more operations group data but do not return a regular table:

- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) returns a [partitioned table](../how-to-guides/partitioned-tables.md) that holds a separate sub-table for each group.
- [`rollup`](../reference/table-operations/create/rollup.md) returns a [rollup table](../how-to-guides/rollup-table.md), an expandable hierarchy of aggregates for the UI.

### Operations that return source values

Three more operations return values taken from the source rows instead of summaries of them:

- [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md) and [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md) return the first or last _n_ rows of each group.
- [`select_distinct`](../reference/table-operations/select/select-distinct.md) returns one row for each unique combination of values in the columns you name.

## Choose an operation

The following figure presents a flowchart to help choose the right operation for your query.

<Svg src='../assets/conceptual/aggregation-chart.svg' style={{height: 'auto', maxWidth: '100%'}} />

The same questions in text form, in order:

1. If you need a value on every source row, use [`update_by`](../reference/table-operations/update-by-operations/updateBy.md):
   - To fill null values with the last non-null value, or to calculate the change from the previous row, use [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md) or [`delta`](../reference/table-operations/update-by-operations/delta.md).
   - For a cumulative calculation, use a `cum_*` operation, such as [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) or [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md).
   - To weight recent values more heavily, use an exponentially weighted operation, such as [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md), [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md), or [`ems_tick`](../reference/table-operations/update-by-operations/ems-tick.md).
   - Otherwise, use a `rolling_*_time` operation, such as [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md), for a window sized by time, or a `rolling_*_tick` operation, such as [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md), for a window sized by row count.
2. If you need a separate table for each group, use [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md).
3. If you need an expandable hierarchy in the UI, use [`rollup`](../reference/table-operations/create/rollup.md).
4. If you need each group's rows kept together as vectors, use [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md).
5. If you need more than one kind of statistic, different statistics for different columns, or a custom formula, use [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md).
6. If you need the first or last row of each group, use [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md) or [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md).
7. Otherwise, use a single aggregation such as [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md), or [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md).

## Aggregations on ticking tables

On a ticking source table, these operations update incrementally.

- **[`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), [single aggregations](../how-to-guides/dedicated-aggregations.md), and [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md):** The engine updates only the groups that contain added, removed, or modified source rows.
  - A new key value adds a result row.
  - A group that has no rows left is removed from the result, unless you set `preserve_empty=True` in [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md).
  - If a modified row changes its key value, the row moves from one group to another.
- **[`update_by`](../reference/table-operations/update-by-operations/updateBy.md):** Added and removed rows pass through to the result. A change to one source row can also modify other rows, because a row's value can depend on the rows in its cumulative or rolling window.
- **[Blink tables](./table-types.md#specialization-3-blink):** A blink table holds only the rows added in the current update cycle.
  - [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) and the [single aggregations](../how-to-guides/dedicated-aggregations.md) ignore the rows a blink table removes, so their results summarize every row seen since the operation started.
  - [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md), [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md), [`agg.partition`](../reference/table-operations/group-and-aggregate/AggPartition.md), and [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md) raise an error on a blink table.
  - [`rollup`](../reference/table-operations/create/rollup.md) raises an error on a blink table if you set `include_constituents=True` or use a first, last, group, or formula aggregation.
  - [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) with key columns raises an error on a blink table. Without key columns, it sees only the rows in the current update cycle, so a cumulative operation such as [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) starts over each cycle.

## Related documentation

- [Arrays vs vectors](./arrays-vs-vectors.md)
- [Incremental update model](./table-update-model.md)
- [Table operations overview](./table-operations-overview.md)
- [Deephaven's table types](./table-types.md)
- [Create and use partitioned tables](../how-to-guides/partitioned-tables.md)
- [Create a hierarchical rollup table](../how-to-guides/rollup-table.md)
- [Group and ungroup data](../how-to-guides/grouping-data.md)
- [Multi-aggregation](../how-to-guides/combined-aggregations.md)
- [Rolling aggregations](../how-to-guides/rolling-aggregations.md)
- [Single aggregation](../how-to-guides/dedicated-aggregations.md)
- [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`agg.partition`](../reference/table-operations/group-and-aggregate/AggPartition.md)
- [`agg.pct`](../reference/table-operations/group-and-aggregate/AggPct.md)
- [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`agg_all_by`](../reference/table-operations/group-and-aggregate/AggAllBy.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md)
- [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md)
- [`delta`](../reference/table-operations/update-by-operations/delta.md)
- [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md)
- [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md)
- [`ems_tick`](../reference/table-operations/update-by-operations/ems-tick.md)
- [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md)
- [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md)
- [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`select_distinct`](../reference/table-operations/select/select-distinct.md)
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`deephaven.agg` Pydoc](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg)
- [`deephaven.table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
- [`deephaven.updateby` Pydoc](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby)
