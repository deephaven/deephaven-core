---
title: Aggregations in Deephaven
---

An aggregation summarizes the values in a group of rows, such as an average or a count. Deephaven offers several table operations that group or aggregate data, and each returns a different kind of result. They work on live data as well as static data: when a ticking source table changes, each result updates with it, as described in [Aggregations on ticking tables](#aggregations-on-ticking-tables). This guide explains what each operation returns and how to choose among them.

## What is an aggregation?

An aggregation breaks a table into groups of rows and calculates one or more summary values for each group, such as a sum, an average, or a count. A _single aggregation_, such as [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md), calculates one statistic. A _multi-aggregation_, also called a _combined aggregation_, calculates several in one [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) operation.

The groups come from key columns, which you pass to the operation by name, as strings. When you pass no key columns, the whole table is one group.

## The table operations

The following table compares the operations that group or aggregate data. The sections that follow it describe each operation and link to its methods.

| Operation                                                                                                                                                                                                                   | Result rows                 | Result columns                                                                                                                                                                                |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Group](../how-to-guides/grouping-data.md) ([`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md))                                                                                                      | One per group               | The key columns, plus a vector of each group's values for every other column                                                                                                                  |
| [Single aggregation](../how-to-guides/dedicated-aggregations.md) ([`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md), and others) | One per group               | The key columns, plus the statistic's results (usually one column per non-key column, or a single count column for [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md)) |
| [Multi-aggregation](../how-to-guides/combined-aggregations.md) ([`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md))                                                                                      | One per group               | The key columns, plus any mix of aggregations you choose                                                                                                                                      |
| [Rolling aggregation](../how-to-guides/rolling-aggregations.md) ([`updateBy`](../reference/table-operations/update-by-operations/updateBy.md))                                                                              | The same rows as the source | The source columns, plus new or replaced columns                                                                                                                                              |

The following example runs three of these operations on the same five-row table. `grouped` has one row per `Sym` that holds a vector of that symbol's prices. `summed` has one row per `Sym` that holds the total price. `running` keeps all five rows and adds a running total for each `Sym`.

```groovy order=grouped,summed,running,source
source = newTable(
    stringCol("Sym", "A", "B", "A", "B", "A"),
    intCol("Price", 10, 20, 30, 40, 50)
)

grouped = source.groupBy("Sym")
summed = source.sumBy("Sym")
running = source.updateBy(CumSum("Total = Price"), "Sym")
```

### Group

[`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) collects the rows of each group together. Every non-key column becomes a column of [vectors](./arrays-vs-vectors.md). Use it when you want the group's values themselves, not a summary of them.

### Single aggregation

A single aggregation calculates one statistic for each group. Most, such as [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md), [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md), and [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md), apply it to every non-key column, so the result has one column for each input column.

[`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) is the exception: it returns one new column that holds the number of rows in each group, and you choose that column's name.

A numeric statistic such as [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) raises an error if any non-key column has a type it can't compute, such as a string column. Drop those columns first, or use [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) to choose the input columns. On a Boolean column, [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) counts the `true` values.

[`aggAllBy`](../reference/table-operations/group-and-aggregate/AggAllBy.md) also applies one statistic to every non-key column, but it takes that statistic as an [`AggSpec`](/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html), so you can use one that has no dedicated method, such as [`AggSpec.percentile`](/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html#percentile(double)).

### Multi-aggregation

[`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) applies a list of aggregations, such as [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) or [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md), in one operation. Each aggregation can use different input columns, and [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) lets you write a custom calculation. The Groovy console imports these methods statically, so you can call them by name. Because [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) computes all of its aggregations in one pass, it is more efficient than running a separate single aggregation for each statistic.

### Rolling aggregation

[`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) keeps every row of the source table and adds columns calculated by one or more update-by operations. The calculations can be cumulative (such as [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md)), exponentially weighted (such as [`Ema`](../reference/table-operations/update-by-operations/ema.md)), computed over a window of rows or time (such as [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md)), or based on the previous row (such as [`Fill`](../reference/table-operations/update-by-operations/fill.md) or [`Delta`](../reference/table-operations/update-by-operations/delta.md)). With key columns, each group is calculated independently, but the rows stay in their original positions.

### Operations that return other table types

Two more operations group data but do not return a regular table:

- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) returns a [partitioned table](../how-to-guides/partitioned-tables.md) that holds a separate sub-table for each group.
- [`rollup`](../reference/table-operations/create/rollup.md) returns a [rollup table](../how-to-guides/rollup-table.md), an expandable hierarchy of aggregates for the UI.

### Operations that return source values

Three more operations return values taken from the source rows instead of summaries of them:

- [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md) and [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md) return the first or last _n_ rows of each group.
- [`selectDistinct`](../reference/table-operations/select/select-distinct.md) returns one row for each unique combination of values in the columns you name.

## Choose an operation

The following figure presents a flowchart to help choose the right operation for your query.

<Svg src='../assets/conceptual/aggregation-chart.svg' style={{height: 'auto', maxWidth: '100%'}} />

The same questions in text form, in order:

1. If you need a value on every source row, use [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md):
   - To fill null values with the last non-null value, or to calculate the change from the previous row, use [`Fill`](../reference/table-operations/update-by-operations/fill.md) or [`Delta`](../reference/table-operations/update-by-operations/delta.md).
   - For a cumulative calculation, use [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md), [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md), or a related operation.
   - To weight recent values more heavily, use an exponentially weighted operation, such as [`Ema`](../reference/table-operations/update-by-operations/ema.md), [`Ems`](../reference/table-operations/update-by-operations/ems.md), or [`EmStd`](../reference/table-operations/update-by-operations/em-std.md).
   - Otherwise, use a rolling operation such as [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md). Pass a timestamp column and a duration to size the window by time, or a row count to size it by rows.
2. If you need a separate table for each group, use [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md).
3. If you need an expandable hierarchy in the UI, use [`rollup`](../reference/table-operations/create/rollup.md).
4. If you need each group's rows kept together as vectors, use [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md).
5. If you need more than one kind of statistic, different statistics for different columns, or a custom formula, use [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md).
6. If you need the first or last row of each group, use [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md) or [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md).
7. Otherwise, use a single aggregation such as [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md), or [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md).

## Aggregations on ticking tables

On a ticking source table, these operations update incrementally.

- **[`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), [single aggregations](../how-to-guides/dedicated-aggregations.md), and [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md):** The engine updates only the groups that contain added, removed, or modified source rows.
  - A new key value adds a result row.
  - A group that has no rows left is removed from the result, unless you set `preserveEmpty` to `true` in [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md).
  - If a modified row changes its key value, the row moves from one group to another.
- **[`updateBy`](../reference/table-operations/update-by-operations/updateBy.md):** Added and removed rows pass through to the result. A change to one source row can also modify other rows, because a row's value can depend on the rows in its cumulative or rolling window.
- **[Blink tables](./table-types.md#specialization-3-blink):** A blink table holds only the rows added in the current update cycle.
  - [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) and the [single aggregations](../how-to-guides/dedicated-aggregations.md) ignore the rows a blink table removes, so their results summarize every row seen since the operation started.
  - [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md), [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md), [`AggPartition`](../reference/table-operations/group-and-aggregate/AggPartition.md), and [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) throw an error on a blink table.
  - [`rollup`](../reference/table-operations/create/rollup.md) throws an error on a blink table if you pass `true` for `includeConstituents` or use a first, last, group, or formula aggregation.
  - [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) with key columns throws an error on a blink table. Without key columns, it sees only the rows in the current update cycle, so a cumulative operation such as [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) starts over each cycle.

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
- [`aggAllBy`](../reference/table-operations/group-and-aggregate/AggAllBy.md)
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`AggPartition`](../reference/table-operations/group-and-aggregate/AggPartition.md)
- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md)
- [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md)
- [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md)
- [`Delta`](../reference/table-operations/update-by-operations/delta.md)
- [`Ema`](../reference/table-operations/update-by-operations/ema.md)
- [`EmStd`](../reference/table-operations/update-by-operations/em-std.md)
- [`Ems`](../reference/table-operations/update-by-operations/ems.md)
- [`Fill`](../reference/table-operations/update-by-operations/fill.md)
- [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md)
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`selectDistinct`](../reference/table-operations/select/select-distinct.md)
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`Aggregation` Javadoc](/core/javadoc/io/deephaven/api/agg/Aggregation.html)
- [`AggSpec` Javadoc](/core/javadoc/io/deephaven/api/agg/spec/AggSpec.html)
- [`UpdateByOperation` Javadoc](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html)
