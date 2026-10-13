---
title: Aggregations in Deephaven
---

An aggregation summarizes the values in a group of rows, such as an average or a count. Deephaven offers several table operations that group or aggregate data, and each returns a different kind of result. This guide explains what each operation returns and how to choose among them.

## What is an aggregation?

Deephaven calls the process of breaking a table into subgroups and then calculating one or more summary values for each subgroup a "combined aggregation." The term combines two ideas: most operations summarize the data within a group (aggregation), and you can compute more than one operation at once (combined).

The groups come from key columns, which you pass as key column names. When you pass no key columns, the whole table is one group.

## The table operations

The following table compares the operations that group or aggregate data. The sections after it link to each method.

| Operation                                                                                       | Result rows                 | Result columns                                                                                                        |
| ----------------------------------------------------------------------------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [Group](../how-to-guides/grouping-data.md) (`groupBy`)                                          | One per group               | The key columns, plus a vector of each group's values for every other column                                          |
| [Single aggregation](../how-to-guides/dedicated-aggregations.md) (`sumBy`, `avgBy`, and others) | One per group               | The key columns, plus the statistic's results (one column per non-key column, or a single count column for `countBy`) |
| [Multi-aggregation](../how-to-guides/combined-aggregations.md) (`aggBy`)                        | One per group               | The key columns, plus any mix of aggregations you choose                                                              |
| [Rolling aggregation](../how-to-guides/rolling-aggregations.md) (`updateBy`)                    | The same rows as the source | The source columns, plus new or replaced columns                                                                      |

### Group

[`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) collects the rows of each group together. Every non-key column becomes a column of [vectors](./arrays-vs-vectors.md). Use it when you want the group's values themselves, not a summary of them.

### Single aggregation

A single aggregation calculates one statistic for each group, and most apply it to every non-key column. Examples include [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md), [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md), and [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md). [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) is a single aggregation too, but it returns one new column that holds the count, and you give it that column's name. Numeric statistics such as `sumBy` raise an error if a non-key column is not numeric.

### Multi-aggregation

[`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) applies a list of aggregations, such as [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) or [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md), in one operation. Each aggregation can use different input columns, and [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) lets you write a custom calculation. Use a static import from `io.deephaven.api.agg.Aggregation` to call them by name. Because `aggBy` computes all of its aggregations in one pass, it is more efficient than running a separate single aggregation for each statistic.

### Rolling aggregation

[`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) keeps every row of the source table and adds columns calculated by one or more update-by operations. The calculations can be cumulative (such as [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md)), exponentially weighted (such as [`Ema`](../reference/table-operations/update-by-operations/ema.md)), computed over a window of rows or time (such as [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md)), or based on the previous row (such as [`Fill`](../reference/table-operations/update-by-operations/fill.md) or [`Delta`](../reference/table-operations/update-by-operations/delta.md)). With key columns, each group is calculated independently, but the rows stay in their original positions.

### Operations that return other table types

Two more operations group data but do not return a regular table:

- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) returns a [partitioned table](../how-to-guides/partitioned-tables.md) that holds a separate sub-table for each group.
- [`rollup`](../reference/table-operations/create/rollup.md) returns a [rollup table](../how-to-guides/rollup-table.md), an expandable hierarchy of aggregates for the UI.

## Choose an operation

The following figure presents a flowchart to help choose the right operation for your query.

<Svg src='../assets/conceptual/aggregation-chart.svg' style={{height: 'auto', maxWidth: '100%'}} />

The same questions in text form, in order:

1. If you need a value on every source row, use `updateBy`:
   - To fill null values with the last non-null value, or to calculate the change from the previous row, use `Fill` or `Delta`.
   - For a cumulative calculation, use `CumSum`, `CumMax`, or a related operation.
   - To weight recent values more heavily, use `Ema`.
   - Otherwise, use a rolling operation such as `RollingAvg` with a time window for a window sized by time, or with a tick window for a window sized by row count.
2. If you need a separate table for each group, use `partitionBy`.
3. If you need an expandable hierarchy in the UI, use `rollup`.
4. If you need each group's rows kept together as vectors, use `groupBy`.
5. If you need several statistics, different statistics for different columns, or a custom formula, use `aggBy`.
6. If you need the first or last row of each group, use `firstBy` or `lastBy`.
7. Otherwise, use a single aggregation such as `sumBy`, `avgBy`, or `countBy`.

## Updating tables

On a ticking source table, these operations update incrementally.

- **`groupBy`, single aggregations, and `aggBy`:** The engine updates only the groups that contain added, removed, or modified source rows.
  - A new key value adds a result row.
  - A group that has no rows left is removed from the result, unless you set `preserveEmpty` to `true` in `aggBy`.
  - If a modified row changes its key value, the row moves from one group to another.
- **`updateBy`:** Added and removed rows pass through to the result. A change to one source row can also modify other rows, because a row's value can depend on the rows in its cumulative or rolling window.
- **Blink tables:** `groupBy`, `partitionBy`, and `AggFormula` are not supported. Most other aggregations summarize all rows seen since the operation started, not only the rows in the current update. See [table types](./table-types.md) for details on blink tables.

## Related documentation

- [Incremental update model](./table-update-model.md)
- [Table operations overview](./table-operations-overview.md)
