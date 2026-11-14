---
title: Create a hierarchical rollup table programmatically
---

<!-- TODO: Link to the hierarchical tables concept guide when it exists (DOC-1977, https://deephaven.atlassian.net/browse/DOC-1977) -->

This guide shows you how to create a hierarchical rollup table programmatically. To build one from the web UI instead, use [Rollup Rows](./user-interface/formatting-tables.md#rollup-rows).

A rollup table combines Deephaven's [aggregations](../conceptual/aggregations.md) with a hierarchical structure. It groups rows at several levels, from a single group of all rows down to the most detailed grouping, and shows the aggregated values at each level. For example, the `insuranceRollup` table from the [Averages over static data](#averages-over-static-data) example groups data by `region` and then by `age`:

![A rollup table grouped by region and age](../assets/how-to/rollup-example-gr.png)

The web UI adds a `Group` column that shows the rollup table's hierarchy. Click the right-facing arrow in the `Group` column to expand a row, and the down-facing arrow to collapse it.

Each level of a rollup table groups by fewer columns than the level below it. On each level, the grouping columns that level doesn't use show null values. For example, on the `region` rows of `insuranceRollup`, the `age` column is null.

Like any Deephaven table built from a [ticking](../conceptual/table-types.md#standard-streaming-tables) source, a rollup table updates as new data arrives, so every level stays current.

Three terms name the parts of a rollup table:

- The _root node_ is the topmost row. It aggregates all of the groups.
- The _leaf nodes_ are the rows at the most detailed grouping level, one for each unique combination of all the grouping columns.
- The _constituents_ are the rows from the source table that each leaf node aggregates. If you pass `true` for `includeConstituents`, they appear one level below each leaf node.

![A diagram displaying the structure of a rollup table](../assets/how-to/rollup-diagram.png)

In this diagram, `Root` is the root node, the `A` rows are the first level of grouping, and the `B` rows are the leaf nodes.

To show a parent/child hierarchy that your data already encodes, with each row naming its parent's ID, use a [tree table](./tree-table.md) instead.

## `rollup`

Create a rollup table with the [`rollup`](../reference/table-operations/create/rollup.md) method:

```groovy syntax
result = source.rollup(aggregations)
result = source.rollup(aggregations, includeConstituents)
result = source.rollup(aggregations, groupByColumns...)
result = source.rollup(aggregations, includeConstituents, groupByColumns...)
```

The [`rollup`](../reference/table-operations/create/rollup.md) method takes up to three parameters. Only `aggregations` is required.

1. `aggregations`: A collection, such as a Groovy list, of the [aggregations](../conceptual/aggregations.md) to compute at each level. Pass a list even for one [aggregation](../conceptual/aggregations.md), for example `[AggAvg("Value")]`. To build the hierarchy without computing any values, pass `[]`. See [Supported aggregations](#supported-aggregations).
2. `includeConstituents` (optional): Whether to show each leaf node's constituents one level below it. The default is `false`. You can't pass `true` when the source is a [blink table](../conceptual/table-types.md#specialization-3-blink). [Supported aggregations](#supported-aggregations) lists the other blink-table restrictions and how to work around them.
3. `groupByColumns` (optional): The columns that define the table's hierarchy, passed as separate arguments (varargs), so you can pass any number of them. Each column adds one level, from left to right. For example, with `"ColumnOne", "ColumnTwo"`, each unique value in `ColumnOne` expands to show the `ColumnTwo` values that belong to it. With no grouping columns, the rollup aggregates all rows into a single root node.

### Supported aggregations

[`rollup`](../reference/table-operations/create/rollup.md) supports most of the [aggregations](../conceptual/aggregations.md) available to [combined aggregations](./combined-aggregations.md):

| Aggregation                                                                                              | Supported by [`rollup`](../reference/table-operations/create/rollup.md) |
| -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| [`AggAbsSum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md)                            | <Check/>                                                                |
| [`AggApproxPct`](../reference/table-operations/group-and-aggregate/AggApproxPct.md)                      | <RedX/>                                                                 |
| [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)                                  | <Check/>                                                                |
| [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md)                              | <Check/>                                                                |
| [`AggCountDistinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md)              | <Check/>                                                                |
| [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md)                    | <Check/>                                                                |
| [`AggDistinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)                        | <Check/>                                                                |
| [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md)                              | <Check/>                                                                |
| [`AggFirstRowKey`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggFirstRowKey(java.lang.String)) | <RedX/>                                                                 |
| [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)                          | <Check/>                                                                |
| [`AggFreeze`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggFreeze(java.lang.String...))        | <RedX/>                                                                 |
| [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)                              | <Check/>                                                                |
| [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md)                                | <Check/>                                                                |
| [`AggLastRowKey`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggLastRowKey(java.lang.String))   | <RedX/>                                                                 |
| [`AggMax`](../reference/table-operations/group-and-aggregate/AggMax.md)                                  | <Check/>                                                                |
| [`AggMed`](../reference/table-operations/group-and-aggregate/AggMed.md)                                  | <RedX/>                                                                 |
| [`AggMin`](../reference/table-operations/group-and-aggregate/AggMin.md)                                  | <Check/>                                                                |
| [`AggPartition`](../reference/table-operations/group-and-aggregate/AggPartition.md)                      | <RedX/>                                                                 |
| [`AggPct`](../reference/table-operations/group-and-aggregate/AggPct.md)                                  | <RedX/>                                                                 |
| [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)                  | <Check/>                                                                |
| [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)                    | <Check/>                                                                |
| [`AggStd`](../reference/table-operations/group-and-aggregate/AggStd.md)                                  | <Check/>                                                                |
| [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)                                  | <Check/>                                                                |
| [`AggTDigest`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggTDigest(java.lang.String...))      | <RedX/>                                                                 |
| [`AggUnique`](../reference/table-operations/group-and-aggregate/AggUnique.md)                            | <Check/>                                                                |
| [`AggVar`](../reference/table-operations/group-and-aggregate/AggVar.md)                                  | <Check/>                                                                |
| [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md)                                | <Check/>                                                                |
| [`AggWSum`](../reference/table-operations/group-and-aggregate/AggWSum.md)                                | <Check/>                                                                |

If the source is a [blink table](../conceptual/table-types.md#specialization-3-blink), [`rollup`](../reference/table-operations/create/rollup.md) doesn't support these [aggregations](../conceptual/aggregations.md): [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md), [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md), [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md), [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md), and [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md). You also can't pass `true` for `includeConstituents`. To use any of them, [convert the blink table to an append-only table](../conceptual/table-types.md#create-an-append-only-table-from-a-blink-table) first.

### Formula aggregations

[`rollup`](../reference/table-operations/create/rollup.md) supports [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) in its [formula-string](./formulas.md) forms, for example `AggFormula("Total = sum(Value)")` or `AggFormula("Total", "sum(Value)")`. It doesn't support the deprecated form that takes a `paramToken`.

Calling [`asReaggregating`](/core/javadoc/io/deephaven/api/agg/Formula.html#asReaggregating()) on an [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) aggregation changes what [`rollup`](../reference/table-operations/create/rollup.md) applies the formula to:

- Without [`asReaggregating`](/core/javadoc/io/deephaven/api/agg/Formula.html#asReaggregating()) (the default): At every level, the formula runs on the source rows in each group.
- With [`asReaggregating`](/core/javadoc/io/deephaven/api/agg/Formula.html#asReaggregating()): At the leaf nodes, the formula runs on the source rows. At each higher level, it runs on the results of the level below. For example, `AggFormula("Value = sum(Value)").asReaggregating()` sums the source `Value` rows at the leaf nodes and sums the `Value` results from the level below at each higher level.

Reaggregating keeps the formula's input small at each higher level: the formula reads one result from each child group on the level below instead of every source row under the group. On a [ticking](../conceptual/table-types.md#standard-streaming-tables) source, each time a group's formula is recomputed, it reads fewer values.

A reaggregating formula reads from the source table at the leaf nodes and from the rollup's own output at every higher level, so it has three requirements:

- Each input column must have the same name in the source table and in the rollup's output. The grouping columns always do.
- Any other input column needs an output column of the same name, written by this formula or by another [aggregation](../conceptual/aggregations.md) in the list. In `AggFormula("Value = sum(Value)").asReaggregating()`, the formula writes its own output to `Value`, so `Value` exists at every level. If another [aggregation](../conceptual/aggregations.md) writes the column, the formula reads the source values at the leaf nodes and that [aggregation](../conceptual/aggregations.md)'s results at each higher level, so make sure combining them gives the result you want.
- The formula must return the same type at every level.

For a worked example, including the error you get when an input column doesn't exist at every level, see [Formula reaggregation](../reference/table-operations/create/rollup.md#formula-reaggregation).

## Examples

### Averages over static data

This example uses the [insurance dataset](https://github.com/deephaven/examples/tree/main/Insurance) from the Deephaven [examples repository](https://github.com/deephaven/examples), read with [`readCsv`](../reference/data-import-export/CSV/readCsv.md). It creates two rollup tables, both grouped by `region` and then `age`:

- `noAggRollup` passes an empty [aggregation](../conceptual/aggregations.md) list, so it builds the hierarchy without computing any values.
- `insuranceRollup` builds the same hierarchy and [averages](../reference/table-operations/group-and-aggregate/AggAvg.md) the `bmi` and `expenses` columns at each level.

Both pass `true` for `includeConstituents`, so each `age` row expands to show the rows from the source table that it aggregates.

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

[`rollup`](../reference/table-operations/create/rollup.md) also supports [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) and [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md). In this example, `AvgPrice` is each group's average price weighted by `Qty`, and `Prices` holds each group's prices as a vector. At the `Region` level, both columns cover every store in the region.

```groovy order=sales,salesRollup
import static io.deephaven.api.agg.Aggregation.AggGroup
import static io.deephaven.api.agg.Aggregation.AggWAvg

sales = newTable(
    stringCol("Region", "East", "East", "East", "West", "West"),
    stringCol("Store", "A", "A", "B", "C", "C"),
    doubleCol("Price", 10.0, 20.0, 15.0, 12.0, 18.0),
    intCol("Qty", 1, 3, 2, 4, 1)
)

salesRollup = sales.rollup(
    [AggWAvg("Qty", "AvgPrice = Price"), AggGroup("Prices = Price")],
    "Region",
    "Store"
)
```

### Averages and standard deviations over real-time data

The following example uses [`timeTable`](../reference/table-operations/create/timeTable.md) to create a [ticking table](../conceptual/table-types.md#standard-streaming-tables) that adds one row per second. Each row gets a random `Group` from 0 to 9 and a random `Subgroup` of `A` or `B`. `Value` follows a normal distribution centered on `Group * 10`, with a standard deviation of 1 in subgroup `A` and 4 in subgroup `B`.

The rollup groups by `Group` and then `Subgroup`, and computes the [average](../reference/table-operations/group-and-aggregate/AggAvg.md) (`AvgValue`) and [standard deviation](../reference/table-operations/group-and-aggregate/AggStd.md) (`StdValue`) of `Value` at each level. On the `Subgroup` level, once enough rows have arrived, `StdValue` is typically larger for `B` rows than for `A` rows.

```groovy ticking-table order=null
import io.deephaven.api.agg.Aggregation

source = timeTable("PT1s").update(
    "Group = randomInt(0, 10)",
    "Subgroup = randomBool() ? `A` : `B`",
    "Value = Group * 10 + randomGaussian(0.0, Subgroup == `A` ? 1.0 : 4.0)"
)

aggList = [Aggregation.AggAvg("AvgValue = Value"), Aggregation.AggStd("StdValue = Value")]

result = source.rollup(aggList, "Group", "Subgroup")
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
- [`AggAbsSum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md)
- [`AggApproxPct`](../reference/table-operations/group-and-aggregate/AggApproxPct.md)
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md)
- [`AggCountDistinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md)
- [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md)
- [`AggDistinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)
- [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md)
- [`AggMax`](../reference/table-operations/group-and-aggregate/AggMax.md)
- [`AggMed`](../reference/table-operations/group-and-aggregate/AggMed.md)
- [`AggMin`](../reference/table-operations/group-and-aggregate/AggMin.md)
- [`AggPartition`](../reference/table-operations/group-and-aggregate/AggPartition.md)
- [`AggPct`](../reference/table-operations/group-and-aggregate/AggPct.md)
- [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)
- [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)
- [`AggStd`](../reference/table-operations/group-and-aggregate/AggStd.md)
- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`AggUnique`](../reference/table-operations/group-and-aggregate/AggUnique.md)
- [`AggVar`](../reference/table-operations/group-and-aggregate/AggVar.md)
- [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md)
- [`AggWSum`](../reference/table-operations/group-and-aggregate/AggWSum.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`readCsv`](../reference/data-import-export/CSV/readCsv.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
- [`Aggregation` Javadoc](/core/javadoc/io/deephaven/api/agg/Aggregation.html)
- [`Formula` Javadoc](/core/javadoc/io/deephaven/api/agg/Formula.html)
