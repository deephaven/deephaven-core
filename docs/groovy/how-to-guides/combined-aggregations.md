---
title: Multi-aggregation
---

This guide will show you how to collect summary information for groups of data using combined aggregations.

Often when working with data, you will want to break the data into subgroups and then perform calculations on the grouped data. For example, a large multi-national corporation may want to know their average employee salary by country, or a teacher might want to analyze test scores for various classes.

The process of breaking a table into subgroups and then performing one or more calculations on the subgroups is known as "combined aggregation." The term comes from most operations creating a summary of data within a group (aggregation), and from more than one operation being computed at once (combined).

## Why use combined aggregations?

Deephaven provides many [dedicated aggregations](./dedicated-aggregations.md), such as [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) and [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md). These are good options if only one type of aggregation is needed. If more than one aggregation is needed or if you have a custom aggregation, combined aggregations are a more efficient and more flexible solution.

## Syntax

Aggregators are defined in an `agg_list`, then applied to data by the [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) method:

The general syntax follows:

```groovy syntax
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggLast

agg_list = [
    AggAvg(sourceColumns),                 // first aggregation
    AggLast("inputColumn = outputColumn")  // second aggregation
]

result = source.aggBy(agg_list, groupingColumns...) // apply the aggregations to data
```

## What aggregations are available?

A number of built-in aggregations are available:

- [`AggAbsSum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md) - Sum of absolute values for each group.
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md) - Average value for each group.
- [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md) - Number of rows for each group.
- [`AggCountDistinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md) - Number of unique values for each group.
- [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md) - Number of values for each group that pass a set of filters.
- [`AggDistinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md) - Array of unique values for each group.
- [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md) - First value for each group.
- [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) - Custom formula for each group.
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md) - Array of values for each group.
- [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md) - Last value for each group.
- [`AggMax`](../reference/table-operations/group-and-aggregate/AggMax.md) - Maximum value for each group.
- [`AggMed`](../reference/table-operations/group-and-aggregate/AggMed.md) - Median value for each group.
- [`AggMin`](../reference/table-operations/group-and-aggregate/AggMin.md) - Minimum value for each group.
- [`AggPartition`](../reference/table-operations/group-and-aggregate/AggPartition.md) - Creates partition for the aggregation group.
- [`AggPct`](../reference/table-operations/group-and-aggregate/AggPct.md) - Percentile of values for each group.
- [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md) - Sorts in ascending order, then computes the first value for each group.
- [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) - Sorts in descending order, then computes the last value for each group.
- [`AggStd`](../reference/table-operations/group-and-aggregate/AggStd.md) - Sample standard deviation for each group.
- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) - Sum of values for each group.
- [`AggUnique`](../reference/table-operations/group-and-aggregate/AggUnique.md) - Returns one single value for a column, or a default.
- [`AggVar`](../reference/table-operations/group-and-aggregate/AggVar.md) - Sample variance for each group.
- [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) - Weighted average for each group.
- [`AggWSum`](../reference/table-operations/group-and-aggregate/AggWSum.md) - Weighted sum for each group.

## Example 1

In this example, we have math and science test results for classes during periods 1 and 2. We want to summarize this information to see if students perform better in one period or the other.

Although designed for multiple, simultaneous aggregations, [`Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) can also be used for a single aggregation. In this first example, we group and [average](../reference/table-operations/group-and-aggregate/AggAvg.md) the test scores by `Period`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAvg

source = newTable(
    stringCol("Period", "1", "2", "2", "2", "1", "2", "1", "2", "1"),
    stringCol("Subject", "Math", "Math", "Math", "Science", "Science", "Science", "Math", "Science", "Math"),
    intCol("Test", 55, 76, 20, 90, 83, 95, 73, 97, 84),
)

result = source.aggBy([AggAvg("AVG = Test")], "Period")
```

The data can also be grouped and [averaged](../reference/table-operations/group-and-aggregate/AggAvg.md) by `Subject`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAvg

source = newTable(
    stringCol("Period", "1", "2", "2", "2", "1", "2", "1", "2", "1"),
    stringCol("Subject", "Math", "Math", "Math", "Science", "Science", "Science", "Math", "Science", "Math"),
    intCol("Test", 55, 76, 20, 90, 83, 95, 73, 97, 84),
)

result = source.aggBy([AggAvg("AVG = Test")], "Subject")
```

We can also group the data by `Subject` and `Period` to see the total [average](../reference/table-operations/group-and-aggregate/AggAvg.md) in a period and subject.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAvg

source = newTable(
    stringCol("Period", "1", "2", "2", "2", "1", "2", "1", "2", "1"),
    stringCol("Subject", "Math", "Math", "Math", "Science", "Science", "Science", "Math", "Science", "Math"),
    intCol("Test", 55, 76, 20, 90, 83, 95, 73, 97, 84),
)

result = source.aggBy([AggAvg("AVG = Test")], "Subject", "Period")
```

## Example 2

In this example, we want to know the first and last test results for each subject and period. To achieve this, we can use [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md) to return the first test value and [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md) to return the last test value. The results are grouped by `Subject` and `Period`, so there are four results in this example.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFirst
import static io.deephaven.api.agg.Aggregation.AggLast

source = newTable(
    stringCol("Period", "1", "2", "2", "2", "1", "2", "1", "2", "1"),
    stringCol("Subject", "Math", "Math", "Math", "Science", "Science", "Science", "Math", "Science", "Math"),
    intCol("Test", 55, 76, 20, 90, 83, 95, 73, 97, 84),
)

agg_list = [
    AggFirst("FirstTest = Test"),
    AggLast("LastTest = Test")
]

result = source.aggBy(agg_list, "Subject", "Period")
```

## Example 3

In this example, tests are weighted differently in computing the final grade.

- The weights are in the `Weight` column.
- [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) is used to compute the weighted average test score, stored in the `WAvg` column.
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md) is used to compute the unweighted average test score, stored in the `Avg` column.
- [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md) is used to compute the number of tests in each group.
- Test results are grouped by `Period`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWAvg
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggCount

source = newTable(
    stringCol("Period", "1", "2", "2", "2", "1", "2", "1", "2", "1"),
    stringCol("Subject", "Math", "Math", "Math", "Science", "Science", "Science", "Math", "Science", "Math"),
    intCol("Test", 55, 76, 20, 90, 83, 95, 73, 97, 84),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
)

agg_list = [
    AggWAvg("Weight", "WAvg = Test"),
    AggAvg("Avg = Test"),
    AggCount("NumTests")
]

result = source.aggBy(agg_list, "Period")
```

## Related documentation

- [Create a new table](../how-to-guides/new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations](./dedicated-aggregations.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
