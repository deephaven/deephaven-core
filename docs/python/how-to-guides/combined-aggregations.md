---
title: Multi-aggregation
---

This guide will show you how to collect summary information for groups of data using combined aggregations.

Often when working with data, you will want to break the data into subgroups and then perform calculations on the grouped data. For example, a large multi-national corporation may want to know their average employee salary by country, or a teacher might want to analyze test scores for various classes.

The process of breaking a table into subgroups and then performing one or more calculations on the subgroups is known as "combined aggregation." The term comes from most operations creating a summary of data within a group (aggregation), and from more than one operation being computed at once (combined).

## Why use combined aggregations?

Deephaven provides many [dedicated aggregations](./dedicated-aggregations.md), such as [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md) and [`min_by`](../reference/table-operations/group-and-aggregate/minBy.md). These are good options if only one type of aggregation is needed. If more than one aggregation is needed or if you have a custom aggregation, combined aggregations are a more efficient and more flexible solution.

## Syntax

Aggregators are defined in an `agg_list`, then applied to data by the [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) method:

The general syntax follows:

```python syntax
from deephaven import agg

agg_list = [
    agg.avg(cols="SourceColumns..."),  # first aggregation
    agg.last(cols=["InputColumn = OutputColumn"]),  # second aggregation
]

result = source.agg_by(
    agg_list, by=["GroupingColumns..."]
)  # apply the aggregations to data
```

## What aggregations are available?

A number of built-in aggregations are available:

- [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md) - Average value for each group.
- [`agg.count_`](../reference/table-operations/group-and-aggregate/AggCount.md) - Number of rows for each group.
- [`agg.count_distinct`](../reference/table-operations/group-and-aggregate/AggCountDistinct.md) - Number of unique values for each group.
- [`agg.count_where`](../reference/table-operations/group-and-aggregate/AggCountWhere.md) - Number of values for each group that pass a set of filters.
- [`agg.distinct`](../reference/table-operations/group-and-aggregate/AggDistinct.md) - Distinct values for each group.
- [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md) - First value for each group.
- [`agg.formula`](../reference/table-operations/group-and-aggregate/AggFormula.md) - Custom formula for each group.
- [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md) - Array of values for each group.
- [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md) - Last value for each group.
- [`agg.max_`](../reference/table-operations/group-and-aggregate/AggMax.md) - Maximum value for each group.
- [`agg.median`](../reference/table-operations/group-and-aggregate/AggMed.md) - Median value for each group.
- [`agg.min_`](../reference/table-operations/group-and-aggregate/AggMin.md) - Minimum value for each group.
- [`agg.partition`](../reference/table-operations/group-and-aggregate/AggPartition.md) - Creates partition for the aggregation group.
- [`agg.pct`](../reference/table-operations/group-and-aggregate/AggPct.md) - Percentile of values for each group.
- [`agg.sorted_first`](../reference/table-operations/group-and-aggregate/AggSortedFirst.md) - Sorts in ascending order, then computes the first value for each group.
- [`agg.sorted_last`](../reference/table-operations/group-and-aggregate/AggSortedLast.md) - Sorts in descending order, then computes the last value for each group.
- [`agg.std`](../reference/table-operations/group-and-aggregate/AggStd.md) - Sample standard deviation for each group.
- [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md) - Sum of values for each group.
- [`agg.unique`](../reference/table-operations/group-and-aggregate/AggUnique.md) - Returns one single value for a column, or a default.
- [`agg.var`](../reference/table-operations/group-and-aggregate/AggVar.md) - Sample variance for each group.
- [`agg.weighted_avg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) - Weighted average for each group.
- [`agg.weighted_sum`](../reference/table-operations/group-and-aggregate/AggWSum.md) - Weighted sum for each group.

## Example 1

In this example, we have math and science test results for classes during periods 1 and 2. We want to summarize this information to see if students perform better in one period or the other.

Although designed for multiple, simultaneous aggregations, aggregators can also be used for a single aggregation. In this first example, we group and [average](../reference/table-operations/group-and-aggregate/AggAvg.md) the test scores by `Period`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

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
    ]
)

result = source.agg_by([agg.avg(cols=["AVG = Test"])], by=["Period"])
```

The data can also be grouped and [averaged](../reference/table-operations/group-and-aggregate/AggAvg.md) by `Subject`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg


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
    ]
)

result = source.agg_by([agg.avg(cols=["AVG = Test"])], by=["Subject"])
```

We can also group the data by `Subject` and `Period` to see the total [average](../reference/table-operations/group-and-aggregate/AggAvg.md) in a period and subject.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg


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
    ]
)

result = source.agg_by([agg.avg(cols=["AVG = Test"])], by=["Subject", "Period"])
```

## Example 2

In this example, we want to know the first and last test results for each subject and period. To achieve this, we can use [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md) to return the first test value and [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md) to return the last test value. The results are grouped by `Subject` and `Period`, so there are four results in this example.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

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
    ]
)

agg_list = [
    agg.first(cols=["FirstTest = Test"]),
    agg.last(cols=["LastTest = Test"]),
]

result = source.agg_by(agg_list, by=["Subject", "Period"])
```

## Example 3

In this example, tests are weighted differently in computing the final grade.

- The weights are in the `Weight` column.
- [`agg.weighted_avg`](../reference/table-operations/group-and-aggregate/AggWAvg.md) is used to compute the weighted average test score, stored in the `WAvg` column.
- [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md) is used to compute the unweighted average test score, stored in the `Avg` column.
  - [`agg.count_`](../reference/table-operations/group-and-aggregate/AggCount.md) is used to compute the number of tests in each group.
- Test results are grouped by `Period`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

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

agg_list = [
    agg.weighted_avg(wcol="Weight", cols=["WAvg = Test"]),
    agg.avg(cols=["Avg = Test"]),
    agg.count_(col="NumTests"),
]

result = source.agg_by(agg_list, by=["Period"])
```

## Related documentation

- [Create a new table](../how-to-guides/new-and-empty-table.md#new_table)
- [How to perform dedicated aggregations](./dedicated-aggregations.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
