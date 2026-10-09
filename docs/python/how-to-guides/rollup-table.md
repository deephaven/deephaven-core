---
title: Create a hierarchical rollup table programmatically
---

This guide shows you how to create a hierarchical rollup table programmatically.

A rollup table combines Deephaven's aggregations with a hierarchical structure: it aggregates values using increasing levels of grouping and shows the value of each aggregation at each level. For example, the `insurance_rollup` table from the [Static data](#static-data) example groups data by `region` and then by `age`:

![A rollup table grouped by region and age](../assets/how-to/rollup-example.png)

The web UI adds a `Group` column that shows the rollup table's hierarchy. Click the right-facing arrow in the `Group` column to expand a row, and the down-facing arrow to collapse it.

The topmost row, which aggregates all rows, is the _root node_. The rows at the lowest aggregated level are the _leaf nodes_. The original source rows that each leaf node aggregates are its _constituents_. If you set `include_constituents=True`, the constituents appear one level below the leaf node.

![A diagram displaying the structure of a rollup table](../assets/how-to/rollup-diagram.png)

In this diagram, `Root` is the root node, the `A` rows are the first level of grouping, and the `B` rows are the leaf nodes.

If each row instead names its parent row by ID, and you want to show that parent/child hierarchy rather than aggregate groups of rows, use a [tree table](./tree-table.md).

## `rollup`

Create a rollup table with the [`rollup`](../reference/table-operations/create/rollup.md) method:

```python syntax
result = source.rollup(aggs=agg_list, by=by_list, include_constituents=False)
```

The `rollup` method takes three parameters. Only `aggs` is required.

1. `aggs`: The aggregations to compute at each level. Pass one aggregation on its own or several in a list. As with [combined aggregations](./combined-aggregations.md#syntax), you can define the list before the `rollup` call. Pass an empty list (`aggs=[]`) to build the hierarchy without computing any values. See [Supported aggregations](#supported-aggregations).
2. `by` (optional): The columns that define the table's hierarchy. Each column adds one level, from left to right. For example, with `by=["ColumnOne", "ColumnTwo"]`, each unique value in `ColumnOne` expands to show the `ColumnTwo` values that belong to it. The default, `None`, aggregates all rows into a single root node.
3. `include_constituents` (optional): Whether to show each leaf node's constituents one level below it. The default is `False`. Not supported when the source is a [blink table](../conceptual/table-types.md#specialization-3-blink).

> [!NOTE]
> Each level of a rollup table groups by fewer columns than the level below it. On each level, any `by` column that the level doesn't group on shows null values.

### Supported aggregations

`rollup` supports most of the aggregations available to [combined aggregations](./combined-aggregations.md):

| Aggregation                                                                               | Supported by `rollup` |
| ----------------------------------------------------------------------------------------- | --------------------- |
| [`abs_sum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md)               | <Check/>              |
| [`avg`](../reference/table-operations/group-and-aggregate/AggAvg.md)                      | <Check/>              |
| [`count_`](../reference/table-operations/group-and-aggregate/AggCount.md)                 | <Check/>              |
| [`count_distinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md) | <Check/>              |
| [`count_where`](../reference/table-operations/group-and-aggregate/AggCountWhere.md)       | <Check/>              |
| [`distinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)            | <Check/>              |
| [`first`](../reference/table-operations/group-and-aggregate/AggFirst.md)                  | <Check/>              |
| [`formula`](../reference/table-operations/group-and-aggregate/AggFormula.md)              | <Check/>              |
| [`group`](../reference/table-operations/group-and-aggregate/AggGroup.md)                  | <Check/>              |
| [`last`](../reference/table-operations/group-and-aggregate/AggLast.md)                    | <Check/>              |
| [`max_`](../reference/table-operations/group-and-aggregate/AggMax.md)                     | <Check/>              |
| [`median`](../reference/table-operations/group-and-aggregate/AggMed.md)                   | <RedX/>               |
| [`min_`](../reference/table-operations/group-and-aggregate/AggMin.md)                     | <Check/>              |
| [`partition`](../reference/table-operations/group-and-aggregate/AggPartition.md)          | <RedX/>               |
| [`pct`](../reference/table-operations/group-and-aggregate/AggPct.md)                      | <RedX/>               |
| [`sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)     | <Check/>              |
| [`sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)       | <Check/>              |
| [`std`](../reference/table-operations/group-and-aggregate/AggStd.md)                      | <Check/>              |
| [`sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)                     | <Check/>              |
| [`unique`](../reference/table-operations/group-and-aggregate/AggUnique.md)                | <Check/>              |
| [`var`](../reference/table-operations/group-and-aggregate/AggVar.md)                      | <Check/>              |
| [`weighted_avg`](../reference/table-operations/group-and-aggregate/AggWAvg.md)            | <Check/>              |
| [`weighted_sum`](../reference/table-operations/group-and-aggregate/AggWSum.md)            | <Check/>              |

`formula` is supported when the formula string names its output and input columns, for example `agg.formula("Total = sum(Value)")`. By default, `rollup` applies the formula to the source rows in each group at every level. To apply the formula to the results of the level below instead, pass `reaggregating=True` to `agg.formula`. Above the lowest level, the formula reads its input from the level below, so the output column must have the same name as the input column. For example, `agg.formula("Value = sum(Value)", reaggregating=True)` sums the source `Value` rows at the lowest level and sums the `Value` results from the level below at each higher level. The deprecated form that uses `formula_param` is not supported.

If the source is a [blink table](../conceptual/table-types.md#specialization-3-blink), `rollup` doesn't support `first`, `last`, `sorted_first`, `sorted_last`, `group`, `formula`, or `include_constituents=True`. [Convert the blink table to an append-only table](../conceptual/table-types.md#create-an-append-only-table-from-a-blink-table) first.

## Examples

### Static data

This example uses the [insurance dataset](https://github.com/deephaven/examples/tree/main/Insurance) from the Deephaven [examples repository](https://github.com/deephaven/examples), read with [`read_csv`](../reference/data-import-export/CSV/readCsv.md). It creates two rollup tables, both grouped by `region` and then `age`:

- `no_agg_rollup` passes an empty aggregation list, so it builds the hierarchy without computing any values.
- `insurance_rollup` builds the same hierarchy and averages the `bmi` and `expenses` columns at each level.

Both set `include_constituents=True`, so each `age` row expands to show the rows from the original table that it aggregates.

```python order=insurance,no_agg_rollup,insurance_rollup
from deephaven import read_csv, agg

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

agg_list = [agg.avg(cols=["bmi", "expenses"])]
by_list = ["region", "age"]

no_agg_rollup = insurance.rollup(aggs=[], by=by_list, include_constituents=True)
insurance_rollup = insurance.rollup(
    aggs=agg_list, by=by_list, include_constituents=True
)
```

### Real-time data

The following example uses [`time_table`](../reference/table-operations/create/timeTable.md) to create a ticking table that adds one row per second. Each row gets a random `Group` from 0 to 9 and a random `Subgroup` of `A` or `B`. `Value` is centered on `Group * 10`, with more random spread in subgroup `B` than in subgroup `A`.

The rollup groups by `Group` and then `Subgroup`, and computes the average (`AvgValue`) and standard deviation (`StdValue`) of `Value` at each level. On the `Subgroup` level, once enough rows have ticked in, `StdValue` is typically larger for `B` rows than for `A` rows.

```python ticking-table order=null
from deephaven import time_table
from deephaven import agg

source = time_table("PT1s").update(
    [
        "Group = randomInt(0, 10)",
        "Subgroup = randomBool() == true ? `A` : `B`",
        "Value = Group * 10 + randomGaussian(0.0, Subgroup == `A` ? 1.0 : 4.0)",
    ]
)

agg_list = [agg.avg(cols="AvgValue = Value"), agg.std(cols="StdValue = Value")]
by_list = ["Group", "Subgroup"]

result = source.rollup(aggs=agg_list, by=by_list)
```

In the result, the first `Group` column is the hierarchy column the UI adds. The second is the `Group` column from `source`.

![Creating a rollup table](../assets/how-to/new-rollup.gif)

## Related documentation

- [How to create a hierarchical tree table](./tree-table.md)
- [How to perform dedicated aggregations](./dedicated-aggregations.md)
- [How to perform combined aggregations](./combined-aggregations.md)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
