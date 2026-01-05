---
title: Create a hierarchical rollup table programmatically
sidebar_label: Rollup tables
---

<!-- TODO: Link to conceptual guide on hierarchy https://github.com/deephaven/deephaven.io/issues/2079 -->

This guide will show you how to create a hierarchical rollup table programmatically. A rollup table combines Deephaven's powerful aggregations with an easy-to-navigate hierarchical structure.

![A diagram displaying the structure of a rollup table](../assets/how-to/rollup-diagram.png)

A rollup table aggregates values using increasing levels of grouping to produce a hierarchical table that shows the value for each aggregation at each level. For example, the following rollup table contains data that is grouped by `region`, and then by `age`:

![A rollup table grouped by region and age](../assets/how-to/rollup-example-gr.png)

The `Group` column contains the rollup table's hierarchy and has UI controls for expanding and collapsing individual groups.

Groupings are clearly represented and easy to navigate in a rollup table. The topmost row, which contains all of the groups, is known as the _root node_. Members of groups are known as _constituents_, and the lowest-level nodes are _leaf nodes_.

> [!NOTE]
> A column that is no longer part of the aggregation key is replaced with a null value on each level.

## `rollup`

Rollup tables are created with the [`rollup`](../reference/table-operations/create/rollup.md) method.

The basic syntax is as follows:

```
result = source.rollup(aggregations)
result = source.rollup(aggregations, includeConstituents)
result = source.rollup(aggregations, groupByColumns...)
result = source.rollup(aggregations, includeConstituents, groupByColumns...)
```

In the result table, only the first and second levels are initially expanded. Levels can be expanded by clicking on the right-facing arrow in a corresponding `by` column.

1. `aggregations`: One or more aggregations.

The following aggregations are supported:

| Aggregation                                                                                 | Supported by `rollup()` |
| ------------------------------------------------------------------------------------------- | ----------------------- |
| [`AggAbsSum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md)               | <Check/>                |
| [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)                     | <Check/>                |
| [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md)                 | <Check/>                |
| [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md)       | <Check/>                |
| [`AggCountDistinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md) | <Check/>                |
| [`AggDistinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md)           | <RedX/>                 |
| [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md)                 | <Check/>                |
| [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)             | <RedX/>                 |
| [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md)                 | <RedX/>                 |
| [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md)                   | <Check/>                |
| [`AggMax`](../reference/table-operations/group-and-aggregate/AggMax.md)                     | <Check/>                |
| [`AggMed`](../reference/table-operations/group-and-aggregate/AggMed.md)                     | <RedX/>                 |
| [`AggMin`](../reference/table-operations/group-and-aggregate/AggMin.md)                     | <Check/>                |
| [`AggPartition`](../reference/table-operations/group-and-aggregate/AggPartition.md)         | <RedX/>                 |
| [`AggPct`](../reference/table-operations/group-and-aggregate/AggPct.md)                     | <RedX/>                 |
| [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md)     | <Check/>                |
| [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md)       | <Check/>                |
| [`AggStd`](../reference/table-operations/group-and-aggregate/AggStd.md)                     | <Check/>                |
| [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)                     | <Check/>                |
| [`AggUnique`](../reference/table-operations/group-and-aggregate/AggUnique.md)               | <Check/>                |
| [`AggVar`](../reference/table-operations/group-and-aggregate/AggVar.md)                     | <Check/>                |
| [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md)                   | <Check/>                |
| [`AggWSum`](../reference/table-operations/group-and-aggregate/AggWSum.md)                   | <Check/>                |

In the case of a rollup table with a single aggregation, that aggregation can be on its own or in a single-element list. When more than one aggregation is used, the aggregations must be in a list. The aggregation(s) can be defined outside of the `rollup` call just like with [combined aggregations](./combined-aggregations.md#syntax).

2. `includeConstituents`: A boolean to indicate whether or not the table will include an additional level at each leaf that displays the rows from the original table that were aggregated. The default value is `false`, so that no rows from the original table will be included in the result.

3. `groupByColumns`: The set of columns that define the hierarchy of the table. These columns are what you will be able to expand and collapse with the arrows in the UI. The hierarchy is determined in a left-to-right order, so if the columns are specified `"ColumnOne", "ColumnTwo"`, `ColumnOne` can be expanded to show all values of `ColumnTwo` that belong to each unique value in `ColumnOne`.

## Examples

### Static data

In our [examples repository](https://github.com/deephaven/examples), we have an [insurance dataset](https://github.com/deephaven/examples/tree/main/Insurance) that can show a simple real-world use case of aggregations and hierarchy.

In this example, two rollup tables are created. The first performs zero aggregations, but creates a hierarchy from the `region` and `age` columns. The second calculates an aggregated average of the `bmi` and `expenses` columns. Each rollup table specifies `include_constituents=True` as the second argument to include the rows from the original table that made up each aggregation.

```groovy order=insurance,insuranceRollup
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.api.agg.Aggregation

insurance = readCsv(
  "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

aggList = [Aggregation.AggAvg("bmi", "expenses")]

testRollup = insurance.rollup([], true, "region", "age")
insuranceRollup = insurance.rollup(aggList, true, "region", "age")
```

### Real-time data

The following example creates ticking source data that simulates groups, subgroups, and values. An aggregated average and standard deviation of all the values is performed for each group and subgroup. The table is rolled up by the `Group` and `Subgroup` columns, respectively.

```groovy ticking-table order=null
import io.deephaven.api.agg.Aggregation

source = timeTable("PT1s").update(
  "Group = randomInt(0, 10)",
  "Subgroup = randomBool() == true ? `A` : `B`",
  "Value = Group * 10 + randomGaussian(0.0, Subgroup == `A` ? 1.0 : 4.0)",
)

aggList = [Aggregation.AggAvg("AvgValue=Value"), Aggregation.AggStd("StdValue=Value")]

result = source.rollup(aggList, "Group", "Subgroup")
```

![Creating a rollup table](../assets/how-to/new-rollup.gif)

Note that rollup tables can only be created from String or primitive columns. Timestamps result in an error:

```groovy skip-test
import io.deephaven.api.agg.Aggregation

t = newTable(
    stringCol("Sym", "AAPL", "AAPL", "GOOGL", "GOOGL", "AAPL"),
    doubleCol("Last", 150.25, 151.50, 920.75, 922.10, 152.00),
    intCol("Size", 100, 200, 50, 150, 300),
    instantCol("ExchangeTimestamp",
        parseInstant("2017-08-25T09:30:00 UTC"),
        parseInstant("2017-08-25T10:15:00 UTC"),
        parseInstant("2017-08-25T11:45:00 UTC"),
        parseInstant("2017-08-25T14:20:00 UTC"),
        parseInstant("2017-08-25T15:50:00 UTC")
    )
)

t = t.update("LocalExchangeTimestampDate=toLocalDate(ExchangeTimestamp, timeZone(`UTC`))")

aggList = [Aggregation.AggAvg("Last", "Size")]

tRollup = t.rollup(aggList, "LocalExchangeTimestampDate")
```

![An error message stating that Deephaven can't parse the LOCAL_DATE data type](../assets/how-to/cant-parse-local-date.png)

## Related documentation

- [How to create a hierarchical tree table](./tree-table.md)
- [How to perform dedicated aggregations](./dedicated-aggregations.md)
- [How to perform combined aggregations](./combined-aggregations.md)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
