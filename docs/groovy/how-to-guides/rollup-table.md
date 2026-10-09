---
title: Create a hierarchical rollup table programmatically
---

<!-- TODO: Link to the hierarchical tables concept guide when it exists (DOC-1977, https://deephaven.atlassian.net/browse/DOC-1977) -->

This guide shows you how to create a hierarchical rollup table programmatically.

A rollup table combines Deephaven's aggregations with a hierarchical structure: it aggregates values using increasing levels of grouping and shows the value of each aggregation at each level. For example, the `insuranceRollup` table from the [Static data](#static-data) example groups data by `region` and then by `age`:

![A rollup table grouped by region and age](../assets/how-to/rollup-example-gr.png)

The web UI adds a `Group` column that shows the rollup table's hierarchy. Click the right-facing arrow in the `Group` column to expand a row, and the down-facing arrow to collapse it.

The topmost row, which contains all of the groups, is known as the _root node_. The lowest-level nodes are known as _leaf nodes_. The rows from the source table that each leaf node aggregates are known as _constituents_. If you pass `true` for `includeConstituents`, the constituents appear one level below each leaf node.

![A diagram displaying the structure of a rollup table](../assets/how-to/rollup-diagram.png)

In this diagram, `Root` is the root node, the `A` rows are the first level of grouping, and the `B` rows are the leaf nodes.

> [!NOTE]
> A column that is no longer part of the aggregation key is replaced with a null value on each level.

If each row instead names its parent row by ID, and you want to show that parent/child hierarchy rather than aggregate groups of rows, use a [tree table](./tree-table.md).

## `rollup`

Create a rollup table with the [`rollup`](../reference/table-operations/create/rollup.md) method:

```groovy syntax
result = source.rollup(aggregations)
result = source.rollup(aggregations, includeConstituents)
result = source.rollup(aggregations, groupByColumns...)
result = source.rollup(aggregations, includeConstituents, groupByColumns...)
```

The `rollup` method takes up to three parameters. Only `aggregations` is required.

1. `aggregations`: A collection, such as a Groovy list, of the aggregations to compute at each level. Pass a list even for a single aggregation, for example `[AggAvg("Value")]`. As with [combined aggregations](./combined-aggregations.md#syntax), you can define the list before the `rollup` call. Pass an empty list (`[]`) to build the hierarchy without computing any values. See [Supported aggregations](#supported-aggregations).
2. `includeConstituents` (optional): Whether to show each leaf node's constituents one level below it. The default is `false`. Not supported when the source is a [blink table](../conceptual/table-types.md#specialization-3-blink).
3. `groupByColumns` (optional): The columns that define the table's hierarchy, passed as separate arguments (varargs), so you can pass any number of them. Each column adds one level, from left to right. For example, with `"ColumnOne", "ColumnTwo"`, each unique value in `ColumnOne` expands to show the `ColumnTwo` values that belong to it. With no grouping columns, the rollup aggregates all rows into a single root node.

### Supported aggregations

`rollup` supports most of the aggregations available to [combined aggregations](./combined-aggregations.md):

| Aggregation                                                                                 | Supported by `rollup` |
| ------------------------------------------------------------------------------------------- | --------------------- |
| [`AggAbsSum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md)               | <Check/>              |
| [`AggApproxPct`](../reference/table-operations/group-and-aggregate/AggApproxPct.md)         | <RedX/>               |
| [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)                     | <Check/>              |
| [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md)                 | <Check/>              |
| [`AggCountDistinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md) | <Check/>              |
| [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md)       | <Check/>              |
| [`AggDistinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)           | <Check/>              |
| [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md)                 | <Check/>              |
| `AggFirstRowKey`                                                                            | <RedX/>               |
| [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)             | <Check/>              |
| `AggFreeze`                                                                                 | <RedX/>               |
| [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)                 | <Check/>              |
| [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md)                   | <Check/>              |
| `AggLastRowKey`                                                                             | <RedX/>               |
| [`AggMax`](../reference/table-operations/group-and-aggregate/AggMax.md)                     | <Check/>              |
| [`AggMed`](../reference/table-operations/group-and-aggregate/AggMed.md)                     | <RedX/>               |
| [`AggMin`](../reference/table-operations/group-and-aggregate/AggMin.md)                     | <Check/>              |
| [`AggPartition`](../reference/table-operations/group-and-aggregate/AggPartition.md)         | <RedX/>               |
| [`AggPct`](../reference/table-operations/group-and-aggregate/AggPct.md)                     | <RedX/>               |
| [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)     | <Check/>              |
| [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)       | <Check/>              |
| [`AggStd`](../reference/table-operations/group-and-aggregate/AggStd.md)                     | <Check/>              |
| [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)                     | <Check/>              |
| `AggTDigest`                                                                                | <RedX/>               |
| [`AggUnique`](../reference/table-operations/group-and-aggregate/AggUnique.md)               | <Check/>              |
| [`AggVar`](../reference/table-operations/group-and-aggregate/AggVar.md)                     | <Check/>              |
| [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md)                   | <Check/>              |
| [`AggWSum`](../reference/table-operations/group-and-aggregate/AggWSum.md)                   | <Check/>              |

`AggFormula` is supported in its formula-string forms, for example `AggFormula("Total = sum(Value)")` or `AggFormula("Total", "sum(Value)")`. By default, `rollup` applies the formula to the source rows in each group at every level. To apply the formula to the results of the level below instead, call `asReaggregating` on the aggregation. Above the lowest level, the formula's input columns refer to the results of the level below, so each input must be a column that the rollup produces at every level: either the formula's own output or the output of another aggregation in the list. For example, `AggFormula("Value = sum(Value)").asReaggregating()` sums the source `Value` rows at the lowest level and sums the `Value` results from the level below at each higher level. The deprecated form that takes a `paramToken` is not supported.

If the source is a [blink table](../conceptual/table-types.md#specialization-3-blink), `rollup` doesn't support `AggFirst`, `AggLast`, `AggSortedFirst`, `AggSortedLast`, `AggGroup`, `AggFormula`, or `includeConstituents = true`. [Convert the blink table to an append-only table](../conceptual/table-types.md#create-an-append-only-table-from-a-blink-table) first.

## Examples

### Static data

This example uses the [insurance dataset](https://github.com/deephaven/examples/tree/main/Insurance) from the Deephaven [examples repository](https://github.com/deephaven/examples), read with [`readCsv`](../reference/data-import-export/CSV/readCsv.md). It creates two rollup tables, both grouped by `region` and then `age`:

- `noAggRollup` passes an empty aggregation list, so it builds the hierarchy without computing any values.
- `insuranceRollup` builds the same hierarchy and averages the `bmi` and `expenses` columns at each level.

Both pass `true` for `includeConstituents`, so each `age` row expands to show the rows from the original table that it aggregates.

```groovy order=insurance,noAggRollup,insuranceRollup
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.api.agg.Aggregation

insurance = readCsv(
  "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

aggList = [Aggregation.AggAvg("bmi", "expenses")]

noAggRollup = insurance.rollup([], true, "region", "age")
insuranceRollup = insurance.rollup(aggList, true, "region", "age")
```

### Weighted averages and grouped values

`rollup` also supports [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) and [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md). In this example, `AvgPrice` is each group's average price weighted by `Qty`, and `Prices` holds each group's prices as an array. At the `Region` level, both columns cover every store in the region.

```groovy order=sales,salesRollup
import static io.deephaven.api.agg.Aggregation.AggGroup
import static io.deephaven.api.agg.Aggregation.AggWAvg

sales = newTable(
    stringCol("Region", "East", "East", "East", "West", "West"),
    stringCol("Store", "A", "A", "B", "C", "C"),
    doubleCol("Price", 10.0, 20.0, 15.0, 12.0, 18.0),
    intCol("Qty", 1, 3, 2, 4, 1)
)

salesRollup = sales.rollup([AggWAvg("Qty", "AvgPrice = Price"), AggGroup("Prices = Price")], "Region", "Store")
```

### Real-time data

The following example uses [`timeTable`](../reference/table-operations/create/timeTable.md) to create a ticking table that adds one row per second. Each row gets a random `Group` from 0 to 9 and a random `Subgroup` of `A` or `B`. `Value` is centered on `Group * 10`, with more random spread in subgroup `B` than in subgroup `A`.

The rollup groups by `Group` and then `Subgroup`, and computes the average (`AvgValue`) and standard deviation (`StdValue`) of `Value` at each level. On the `Subgroup` level, once enough rows have ticked in, `StdValue` is typically larger for `B` rows than for `A` rows.

```groovy ticking-table order=null
import io.deephaven.api.agg.Aggregation

source = timeTable("PT1s").update(
  "Group = randomInt(0, 10)",
  "Subgroup = randomBool() == true ? `A` : `B`",
  "Value = Group * 10 + randomGaussian(0.0, Subgroup == `A` ? 1.0 : 4.0)"
)

aggList = [Aggregation.AggAvg("AvgValue = Value"), Aggregation.AggStd("StdValue = Value")]

result = source.rollup(aggList, "Group", "Subgroup")
```

In the result, the first `Group` column is the hierarchy column the UI adds. The second is the `Group` column from `source`.

![Creating a rollup table](../assets/how-to/new-rollup.gif)

## Related documentation

- [How to create a hierarchical tree table](./tree-table.md)
- [How to perform dedicated aggregations](./dedicated-aggregations.md)
- [How to perform combined aggregations](./combined-aggregations.md)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`readCsv`](../reference/data-import-export/CSV/readCsv.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
