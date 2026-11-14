---
title: Create a hierarchical rollup table programmatically
---

<!-- TODO: Link to the hierarchical tables concept guide when it exists (DOC-1977, https://deephaven.atlassian.net/browse/DOC-1977) -->

This guide shows you how to create a hierarchical rollup table programmatically. To build one from the web UI instead, use [Rollup Rows](./user-interface/formatting-tables.md#rollup-rows).

A rollup table combines Deephaven's [aggregations](../conceptual/aggregations.md) with a hierarchical structure. It groups rows at several levels, from a single group of all rows down to the most detailed grouping, and shows the aggregated values at each level. For example, the `insurance_rollup` table from the [Averages over static data](#averages-over-static-data) example groups data by `region` and then by `age`:

![A rollup table grouped by region and age](../assets/how-to/rollup-example.png)

The web UI adds a `Group` column that shows the rollup table's hierarchy. Click the right-facing arrow in the `Group` column to expand a row, and the down-facing arrow to collapse it.

Each level of a rollup table groups by fewer columns than the level below it. On each level, the grouping columns that level doesn't use show null values. For example, on the `region` rows of `insurance_rollup`, the `age` column is null.

Like any Deephaven table built from a [ticking](../conceptual/table-types.md#standard-streaming-tables) source, a rollup table updates as new data arrives, so every level stays current.

Three terms name the parts of a rollup table:

- The _root node_ is the topmost row. It aggregates all of the groups.
- The _leaf nodes_ are the rows at the most detailed grouping level, one for each unique combination of all the grouping columns.
- The _constituents_ are the rows from the source table that each leaf node aggregates. If you set `include_constituents=True`, they appear one level below each leaf node.

![A diagram displaying the structure of a rollup table](../assets/how-to/rollup-diagram.png)

In this diagram, `Root` is the root node, the `A` rows are the first level of grouping, and the `B` rows are the leaf nodes.

To show a parent/child hierarchy that your data already encodes, with each row naming its parent's ID, use a [tree table](./tree-table.md) instead.

## `rollup`

Create a rollup table with the [`rollup`](../reference/table-operations/create/rollup.md) method:

```python syntax
result = source.rollup(aggs=agg_list, by=by_list, include_constituents=False)
```

The [`rollup`](../reference/table-operations/create/rollup.md) method takes three parameters. Only `aggs` is required.

1. `aggs`: One [aggregation](../conceptual/aggregations.md), or a list of them, to compute at each level. See [Supported aggregations](#supported-aggregations). To build the hierarchy without computing any values, pass an empty list (`aggs=[]`).
2. `by` (optional): The columns that define the table's hierarchy. Each column adds one level, from left to right. For example, with `by=["ColumnOne", "ColumnTwo"]`, each unique value in `ColumnOne` expands to show the `ColumnTwo` values that belong to it. The default, `None`, aggregates all rows into a single root node.
3. `include_constituents` (optional): Whether to show each leaf node's constituents one level below it. The default is `False`. You can't set it to `True` when the source is a [blink table](../conceptual/table-types.md#specialization-3-blink). [Supported aggregations](#supported-aggregations) lists the other blink-table restrictions and how to work around them.

### Supported aggregations

[`rollup`](../reference/table-operations/create/rollup.md) supports most of the [aggregations](../conceptual/aggregations.md) available to [combined aggregations](./combined-aggregations.md):

| Aggregation                                                                               | Supported by [`rollup`](../reference/table-operations/create/rollup.md) |
| ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| [`abs_sum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md)               | <Check/>                                                                |
| [`avg`](../reference/table-operations/group-and-aggregate/AggAvg.md)                      | <Check/>                                                                |
| [`count_`](../reference/table-operations/group-and-aggregate/AggCount.md)                 | <Check/>                                                                |
| [`count_distinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md) | <Check/>                                                                |
| [`count_where`](../reference/table-operations/group-and-aggregate/AggCountWhere.md)       | <Check/>                                                                |
| [`distinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)            | <Check/>                                                                |
| [`first`](../reference/table-operations/group-and-aggregate/AggFirst.md)                  | <Check/>                                                                |
| [`formula`](../reference/table-operations/group-and-aggregate/AggFormula.md)              | <Check/>                                                                |
| [`group`](../reference/table-operations/group-and-aggregate/AggGroup.md)                  | <Check/>                                                                |
| [`last`](../reference/table-operations/group-and-aggregate/AggLast.md)                    | <Check/>                                                                |
| [`max_`](../reference/table-operations/group-and-aggregate/AggMax.md)                     | <Check/>                                                                |
| [`median`](../reference/table-operations/group-and-aggregate/AggMed.md)                   | <RedX/>                                                                 |
| [`min_`](../reference/table-operations/group-and-aggregate/AggMin.md)                     | <Check/>                                                                |
| [`partition`](../reference/table-operations/group-and-aggregate/AggPartition.md)          | <RedX/>                                                                 |
| [`pct`](../reference/table-operations/group-and-aggregate/AggPct.md)                      | <RedX/>                                                                 |
| [`sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)     | <Check/>                                                                |
| [`sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)       | <Check/>                                                                |
| [`std`](../reference/table-operations/group-and-aggregate/AggStd.md)                      | <Check/>                                                                |
| [`sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)                     | <Check/>                                                                |
| [`unique`](../reference/table-operations/group-and-aggregate/AggUnique.md)                | <Check/>                                                                |
| [`var`](../reference/table-operations/group-and-aggregate/AggVar.md)                      | <Check/>                                                                |
| [`weighted_avg`](../reference/table-operations/group-and-aggregate/AggWAvg.md)            | <Check/>                                                                |
| [`weighted_sum`](../reference/table-operations/group-and-aggregate/AggWSum.md)            | <Check/>                                                                |

If the source is a [blink table](../conceptual/table-types.md#specialization-3-blink), [`rollup`](../reference/table-operations/create/rollup.md) doesn't support these [aggregations](../conceptual/aggregations.md): [`first`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`last`](../reference/table-operations/group-and-aggregate/AggLast.md), [`sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md), [`sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md), [`group`](../reference/table-operations/group-and-aggregate/AggGroup.md), and [`formula`](../reference/table-operations/group-and-aggregate/AggFormula.md). You also can't set `include_constituents=True`. To use any of them, [convert the blink table to an append-only table](../conceptual/table-types.md#create-an-append-only-table-from-a-blink-table) first.

### Formula aggregations

[`rollup`](../reference/table-operations/create/rollup.md) supports [`formula`](../reference/table-operations/group-and-aggregate/AggFormula.md) when the [formula string](./formulas.md) names its output and input columns, for example `agg.formula("Total = sum(Value)")`. It doesn't support the deprecated form that uses `formula_param`.

The `reaggregating` parameter of [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md) sets what [`rollup`](../reference/table-operations/create/rollup.md) applies the formula to:

- `reaggregating=False` (the default): At every level, the formula runs on the source rows in each group.
- `reaggregating=True`: At the leaf nodes, the formula runs on the source rows. At each higher level, it runs on the results of the level below. For example, `agg.formula("Value = sum(Value)", reaggregating=True)` sums the source `Value` rows at the leaf nodes and sums the `Value` results from the level below at each higher level.

Reaggregating keeps the formula's input small at each higher level: the formula reads one result from each child group on the level below instead of every source row under the group. On a [ticking](../conceptual/table-types.md#standard-streaming-tables) source, each time a group's formula is recomputed, it reads fewer values.

A reaggregating formula reads from the source table at the leaf nodes and from the rollup's own output at every higher level, so it has three requirements:

- Each input column must have the same name in the source table and in the rollup's output. The `by` columns always do.
- Any other input column needs an output column of the same name, written by this formula or by another [aggregation](../conceptual/aggregations.md) in the list. In `agg.formula("Value = sum(Value)", reaggregating=True)`, the formula writes its own output to `Value`, so `Value` exists at every level. If another [aggregation](../conceptual/aggregations.md) writes the column, the formula reads the source values at the leaf nodes and that [aggregation](../conceptual/aggregations.md)'s results at each higher level, so make sure combining them gives the result you want.
- The formula must return the same type at every level.

For a worked example, including the error you get when an input column doesn't exist at every level, see [Formula reaggregation](../reference/table-operations/create/rollup.md#formula-reaggregation).

## Examples

### Averages over static data

This example uses the [insurance dataset](https://github.com/deephaven/examples/tree/main/Insurance) from the Deephaven [examples repository](https://github.com/deephaven/examples), read with [`read_csv`](../reference/data-import-export/CSV/readCsv.md). It creates two rollup tables, both grouped by `region` and then `age`:

- `no_agg_rollup` passes an empty [aggregation](../conceptual/aggregations.md) list, so it builds the hierarchy without computing any values.
- `insurance_rollup` builds the same hierarchy and [averages](../reference/table-operations/group-and-aggregate/AggAvg.md) the `bmi` and `expenses` columns at each level.

Both set `include_constituents=True`, so each `age` row expands to show the rows from the source table that it aggregates.

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

### Weighted averages and grouped values

[`rollup`](../reference/table-operations/create/rollup.md) also supports [`weighted_avg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) and [`group`](../reference/table-operations/group-and-aggregate/AggGroup.md). In this example, `AvgPrice` is each group's average price weighted by `Qty`, and `Prices` holds each group's prices as a vector. At the `Region` level, both columns cover every store in the region.

```python order=sales,sales_rollup
from deephaven import new_table, agg
from deephaven.column import string_col, double_col, int_col

sales = new_table(
    [
        string_col("Region", ["East", "East", "East", "West", "West"]),
        string_col("Store", ["A", "A", "B", "C", "C"]),
        double_col("Price", [10.0, 20.0, 15.0, 12.0, 18.0]),
        int_col("Qty", [1, 3, 2, 4, 1]),
    ]
)

sales_rollup = sales.rollup(
    aggs=[
        agg.weighted_avg(wcol="Qty", cols="AvgPrice = Price"),
        agg.group(cols="Prices = Price"),
    ],
    by=["Region", "Store"],
)
```

### Averages and standard deviations over real-time data

The following example uses [`time_table`](../reference/table-operations/create/timeTable.md) to create a [ticking table](../conceptual/table-types.md#standard-streaming-tables) that adds one row per second. Each row gets a random `Group` from 0 to 9 and a random `Subgroup` of `A` or `B`. `Value` follows a normal distribution centered on `Group * 10`, with a standard deviation of 1 in subgroup `A` and 4 in subgroup `B`.

The rollup groups by `Group` and then `Subgroup`, and computes the [average](../reference/table-operations/group-and-aggregate/AggAvg.md) (`AvgValue`) and [standard deviation](../reference/table-operations/group-and-aggregate/AggStd.md) (`StdValue`) of `Value` at each level. On the `Subgroup` level, once enough rows have arrived, `StdValue` is typically larger for `B` rows than for `A` rows.

```python ticking-table order=null
from deephaven import time_table, agg

source = time_table("PT1s").update(
    [
        "Group = randomInt(0, 10)",
        "Subgroup = randomBool() ? `A` : `B`",
        "Value = Group * 10 + randomGaussian(0.0, Subgroup == `A` ? 1.0 : 4.0)",
    ]
)

agg_list = [agg.avg(cols="AvgValue = Value"), agg.std(cols="StdValue = Value")]
by_list = ["Group", "Subgroup"]

result = source.rollup(aggs=agg_list, by=by_list)
```

As `source` ticks, `result` updates with it. New `Group` and `Subgroup` rows appear, and `AvgValue` and `StdValue` change at every level as rows arrive. In the result, the first `Group` column is the hierarchy column the UI adds. The second is the `Group` column from `source`.

![Creating a rollup table](../assets/how-to/new-rollup.gif)

## Related documentation

- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Deephaven's table types](../conceptual/table-types.md)
- [How to create a hierarchical tree table](./tree-table.md)
- [Rollup Rows in the web UI](./user-interface/formatting-tables.md#rollup-rows)
- [How to perform dedicated aggregations](./dedicated-aggregations.md)
- [How to perform combined aggregations](./combined-aggregations.md)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [Formulas](./formulas.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`abs_sum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md)
- [`avg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`count_`](../reference/table-operations/group-and-aggregate/AggCount.md)
- [`count_distinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md)
- [`count_where`](../reference/table-operations/group-and-aggregate/AggCountWhere.md)
- [`distinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)
- [`first`](../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`formula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`group`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`last`](../reference/table-operations/group-and-aggregate/AggLast.md)
- [`max_`](../reference/table-operations/group-and-aggregate/AggMax.md)
- [`median`](../reference/table-operations/group-and-aggregate/AggMed.md)
- [`min_`](../reference/table-operations/group-and-aggregate/AggMin.md)
- [`partition`](../reference/table-operations/group-and-aggregate/AggPartition.md)
- [`pct`](../reference/table-operations/group-and-aggregate/AggPct.md)
- [`sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)
- [`sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)
- [`std`](../reference/table-operations/group-and-aggregate/AggStd.md)
- [`sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`unique`](../reference/table-operations/group-and-aggregate/AggUnique.md)
- [`var`](../reference/table-operations/group-and-aggregate/AggVar.md)
- [`weighted_avg`](../reference/table-operations/group-and-aggregate/AggWAvg.md)
- [`weighted_sum`](../reference/table-operations/group-and-aggregate/AggWSum.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`rollup` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.rollup)
- [`deephaven.agg` Pydoc](/core/pydoc/code/deephaven.agg.html)
