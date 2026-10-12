---
title: Multi-aggregation
---

This guide shows you how to collect summary information for groups of data using multi-aggregation, also called combined aggregation: several aggregations applied in a single operation.

When working with data, you often need to break the data into subgroups and then perform calculations on each group. For example, a large multinational corporation may want to know its average employee salary by country, or a teacher might want to analyze test scores for various classes.

Each aggregation summarizes the data within each group, such as an average or a count.

## Why use combined aggregations?

Deephaven provides many [dedicated aggregations](./dedicated-aggregations.md), such as [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) and [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md). These are good options when you need only one type of aggregation. When you need more than one aggregation, or a custom calculation with [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md), combined aggregations are more flexible, and more efficient because they group the data once instead of once per calculation.

## Syntax

Define the aggregations in a list, such as `agg_list`, then apply them to data with the [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) method:

```python syntax
from deephaven import agg

agg_list = [
    agg.avg(cols="SourceColumns..."),  # first aggregation
    agg.last(cols=["OutputColumn = InputColumn"]),  # second aggregation
]

result = source.agg_by(
    agg_list, by=["GroupingColumns..."]
)  # apply the aggregations to data
```

Most aggregations take the columns to aggregate in `cols`. Write a column as `"OutputColumn = InputColumn"` to put the result in a new column named `OutputColumn`, or give only the input column's name to keep that name. The `by` argument names the columns that define the groups.

## What aggregations are available?

A number of built-in aggregations are available:

- [`agg.abs_sum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md) - Sum of absolute values for each group.
- [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md) - Average value for each group.
- [`agg.count_`](../reference/table-operations/group-and-aggregate/AggCount.md) - Number of rows for each group.
- [`agg.count_distinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md) - Number of unique values for each group.
- [`agg.count_where`](../reference/table-operations/group-and-aggregate/AggCountWhere.md) - Number of rows for each group that pass a set of filters.
- [`agg.distinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md) - Array of unique values for each group.
- [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md) - First value for each group.
- [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md) - Custom formula for each group.
- [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) - Array of values for each group.
- [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md) - Last value for each group.
- [`agg.max_`](../reference/table-operations/group-and-aggregate/AggMax.md) - Maximum value for each group.
- [`agg.median`](../reference/table-operations/group-and-aggregate/AggMed.md) - Median value for each group.
- [`agg.min_`](../reference/table-operations/group-and-aggregate/AggMin.md) - Minimum value for each group.
- [`agg.partition`](../reference/table-operations/group-and-aggregate/AggPartition.md) - Subtable of the rows in each group.
- [`agg.pct`](../reference/table-operations/group-and-aggregate/AggPct.md) - Percentile of values for each group.
- [`agg.sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md) - First value for each group after sorting by the `order_by` column in ascending order.
- [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) - Last value for each group after sorting by the `order_by` column in ascending order.
- [`agg.std`](../reference/table-operations/group-and-aggregate/AggStd.md) - Sample standard deviation for each group.
- [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md) - Sum of values for each group.
- [`agg.unique`](../reference/table-operations/group-and-aggregate/AggUnique.md) - The single unique non-null value for each group, or null if the group has more than one distinct value or only nulls.
- [`agg.var`](../reference/table-operations/group-and-aggregate/AggVar.md) - Sample variance for each group.
- [`agg.weighted_avg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) - Weighted average for each group.
- [`agg.weighted_sum`](../reference/table-operations/group-and-aggregate/AggWSum.md) - Weighted sum for each group.

## Example data

The examples on this page use the following table of math and science test results for classes in periods 1 and 2. The `Weight` column holds the weight of each test in the final grade, which the [weighted average example](#combine-weighted-and-unweighted-averages-with-a-count) uses.

```python test-set=1 order=source
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("Period", ["1", "2", "2", "2", "1", "2", "1", "2", "1"]),
        string_col(
            "Subject",
            [
                "Math",
                "Math",
                "Math",
                "Science",
                "Science",
                "Science",
                "Math",
                "Science",
                "Math",
            ],
        ),
        int_col("Test", [55, 76, 20, 90, 83, 95, 73, 97, 84]),
        int_col("Weight", [1, 2, 1, 3, 2, 1, 4, 1, 2]),
    ]
)
```

## Apply a single aggregation

Although designed for multiple, simultaneous aggregations, [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) can also apply a single aggregation. Unlike [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md), which averages every column not named in `by`, `agg_by` lets you choose the column to average and name the result, so it works even when other columns hold strings.

The following example uses [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) to group and [average](../reference/table-operations/group-and-aggregate/AggAvg.md) the test scores in three ways:

- By `Period`, to see if students perform better in one period or the other.
- By `Subject`.
- By `Subject` and `Period`, to see the average test score for each combination of subject and period.

```python test-set=1 order=result_by_period,result_by_subject,result_by_subject_period
from deephaven import agg

result_by_period = source.agg_by([agg.avg(cols=["Avg = Test"])], by=["Period"])
result_by_subject = source.agg_by([agg.avg(cols=["Avg = Test"])], by=["Subject"])
result_by_subject_period = source.agg_by(
    [agg.avg(cols=["Avg = Test"])], by=["Subject", "Period"]
)
```

## Combine first and last values

This example finds the first and last test results for each subject and period. [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md) returns the first test value, and [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md) returns the last test value. Grouping by `Subject` and `Period` produces four rows, one for each combination.

```python test-set=1 order=result
from deephaven import agg

agg_list = [
    agg.first(cols=["FirstTest = Test"]),
    agg.last(cols=["LastTest = Test"]),
]

result = source.agg_by(agg_list, by=["Subject", "Period"])
```

## Combine weighted and unweighted averages with a count

In this example, each test carries a different weight in the final grade.

- The weights are in the `Weight` column.
- [`agg.weighted_avg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) takes the weight column, `Weight`, in its `wcol` argument and computes the weighted average test score into the `WAvg` column.
- [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md) computes the unweighted average test score into the `Avg` column.
- [`agg.count_`](../reference/table-operations/group-and-aggregate/AggCount.md) has no input column. Its `col` argument names the output column, `NumTests`, which holds the number of tests in each group.
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) groups the rows by `Period` and applies the three aggregations to each group.

```python test-set=1 order=result
from deephaven import agg

agg_list = [
    agg.weighted_avg(wcol="Weight", cols=["WAvg = Test"]),
    agg.avg(cols=["Avg = Test"]),
    agg.count_(col="NumTests"),
]

result = source.agg_by(agg_list, by=["Period"])
```

## Related documentation

- [Create a new table](./new-and-empty-table.md#new_table)
- [How to perform dedicated aggregations](./dedicated-aggregations.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
