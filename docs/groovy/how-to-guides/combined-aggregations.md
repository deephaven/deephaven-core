---
title: Multi-aggregation
---

This guide shows you how to collect summary information for groups of data using multi-aggregation, also called combined aggregation: several aggregations applied in a single operation.

When working with data, you often need to break the data into subgroups and then perform calculations on each group. For example, a large multinational corporation may want to know its average employee salary by country, or a teacher might want to analyze test scores for various classes.

The process of breaking a table into subgroups and then performing one or more calculations on the subgroups is known as "combined aggregation." The term comes from most operations creating a summary of data within a group (aggregation), and from more than one operation being computed at once (combined).

## Why use combined aggregations?

Deephaven provides many [dedicated aggregations](./dedicated-aggregations.md), such as [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md) and [`minBy`](../reference/table-operations/group-and-aggregate/minBy.md). These are good options when you need only one type of aggregation. When you need more than one aggregation, or a custom calculation with [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md), combined aggregations are more flexible, and more efficient because they group the data once instead of once per calculation.

## Syntax

Define the aggregations in a list, such as `aggList`, then apply them to data with the [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) method:

```groovy syntax
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggLast

aggList = [
    AggAvg("SourceColumns..."),            // first aggregation
    AggLast("OutputColumn = InputColumn")  // second aggregation
]

result = source.aggBy(aggList, "GroupingColumns...") // apply the aggregations to data
```

Most aggregations take the columns to aggregate as strings. Write a column as `"OutputColumn = InputColumn"` to put the result in a new column named `OutputColumn`, or give only the input column's name to keep that name. Every argument after the list is a column that defines the groups.

## What aggregations are available?

A number of built-in aggregations are available. The following list covers the most common ones. For the full set, see the [`Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) Javadoc.

- [`AggAbsSum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md) - Sum of absolute values for each group.
- [`AggApproxPct`](../reference/table-operations/group-and-aggregate/AggApproxPct.md) - Approximate percentile of values for each group.
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md) - Average value for each group.
- [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md) - Number of rows for each group.
- [`AggCountDistinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md) - Number of unique values for each group.
- [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md) - Number of rows for each group that pass a set of filters.
- [`AggDistinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md) - Vector of unique values for each group.
- [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md) - First value for each group.
- [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) - Custom formula for each group.
- [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md) - Vector of values for each group.
- [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md) - Last value for each group.
- [`AggMax`](../reference/table-operations/group-and-aggregate/AggMax.md) - Maximum value for each group.
- [`AggMed`](../reference/table-operations/group-and-aggregate/AggMed.md) - Median value for each group.
- [`AggMin`](../reference/table-operations/group-and-aggregate/AggMin.md) - Minimum value for each group.
- [`AggPartition`](../reference/table-operations/group-and-aggregate/AggPartition.md) - Subtable of the rows in each group.
- [`AggPct`](../reference/table-operations/group-and-aggregate/AggPct.md) - Percentile of values for each group.
- [`AggSortedFirst`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md) - First value for each group after sorting by the sort column in ascending order.
- [`AggSortedLast`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) - Last value for each group after sorting by the sort column in ascending order.
- [`AggStd`](../reference/table-operations/group-and-aggregate/AggStd.md) - Sample standard deviation for each group.
- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) - Sum of values for each group.
- [`AggUnique`](../reference/table-operations/group-and-aggregate/AggUnique.md) - The single unique non-null value for each group, or null if the group has more than one distinct value or only nulls.
- [`AggVar`](../reference/table-operations/group-and-aggregate/AggVar.md) - Sample variance for each group.
- [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) - Weighted average for each group.
- [`AggWSum`](../reference/table-operations/group-and-aggregate/AggWSum.md) - Weighted sum for each group.

## Example data

The examples on this page use the following table of math and science test results for classes in periods 1 and 2. The `Weight` column holds the weight of each test in the final grade, which the [weighted average example](#combine-weighted-and-unweighted-averages-with-a-count) uses.

```groovy test-set=1 order=source
source = newTable(
    stringCol("Period", "1", "2", "2", "2", "1", "2", "1", "2", "1"),
    stringCol("Subject", "Math", "Math", "Math", "Science", "Science", "Science", "Math", "Science", "Math"),
    intCol("Test", 55, 76, 20, 90, 83, 95, 73, 97, 84),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2)
)
```

## Apply a single aggregation

Although designed for multiple, simultaneous aggregations, [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) can also apply a single aggregation. Unlike [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md), which averages every column except the grouping columns, `aggBy` lets you choose the column to average and name the result, so it works even when other columns hold strings.

The following example uses [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) to group and [average](../reference/table-operations/group-and-aggregate/AggAvg.md) the test scores in three ways:

- By `Period`, to see if students perform better in one period or the other.
- By `Subject`.
- By `Subject` and `Period`, to see the average test score for each combination of subject and period.

```groovy test-set=1 order=resultByPeriod,resultBySubject,resultBySubjectPeriod
import static io.deephaven.api.agg.Aggregation.AggAvg

resultByPeriod = source.aggBy([AggAvg("Avg = Test")], "Period")
resultBySubject = source.aggBy([AggAvg("Avg = Test")], "Subject")
resultBySubjectPeriod = source.aggBy([AggAvg("Avg = Test")], "Subject", "Period")
```

## Combine first and last values

This example finds the first and last test results for each subject and period. [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md) returns the first test value, and [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md) returns the last test value. Grouping by `Subject` and `Period` produces four rows, one for each combination.

```groovy test-set=1 order=result
import static io.deephaven.api.agg.Aggregation.AggFirst
import static io.deephaven.api.agg.Aggregation.AggLast

aggList = [
    AggFirst("FirstTest = Test"),
    AggLast("LastTest = Test")
]

result = source.aggBy(aggList, "Subject", "Period")
```

## Combine weighted and unweighted averages with a count

In this example, each test carries a different weight in the final grade.

- The weights are in the `Weight` column.
- [`AggWAvg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) takes the weight column, `Weight`, as its first argument and the column to average, `Test`, as its second. It computes the weighted average test score into the `WAvg` column.
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md) computes the unweighted average test score into the `Avg` column.
- [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md) has no input column. Its argument names the output column, `NumTests`, which holds the number of tests in each group.
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) groups the rows by `Period` and applies the three aggregations to each group.

```groovy test-set=1 order=result
import static io.deephaven.api.agg.Aggregation.AggWAvg
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggCount

aggList = [
    AggWAvg("Weight", "WAvg = Test"),
    AggAvg("Avg = Test"),
    AggCount("NumTests")
]

result = source.aggBy(aggList, "Period")
```

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations](./dedicated-aggregations.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
