---
title: Basic Table Operations
sidebar_label: Table Operations
---

This section will cover some table operations that appear in almost all queries. These table operations use query strings extensively, which are discussed in detail in the [next section](./query-strings.md).

Table operations are integral to the Deephaven Query Language (DQL). The previous sections of the crash course used five:

- [`update_view`](../../reference/table-operations/select/update-view.md), which adds columns to or modifies existing columns in a table.
- [`sum_by`](../../reference/table-operations/group-and-aggregate/sumBy.md), which computes the sum of all columns in a table by a grouping column.
- [`drop_columns`](../../reference/table-operations/select/drop-columns.md), which drops columns from a table.
- [`sort`](../../reference/table-operations/sort/sort.md), which sorts a table by the given columns from least to greatest.
- [`sort_descending`](../../reference/table-operations/sort/sort-descending.md), which sorts a table by the given columns from greatest to least.

Table operations are an integral component of DQL. You've already seen several: [`update_view`](../../reference/table-operations/select/update-view.md), [`sum_by`](../../reference/table-operations/group-and-aggregate/sumBy.md), [`drop_columns`](../../reference/table-operations/select/drop-columns.md), [`sort`](../../reference/table-operations/sort/sort.md) and [`sort_descending`](../../reference/table-operations/sort/sort-descending.md). You can think of these as different transformations being applied to the data in the table. This section will outline some basic table operations that make up the backbones of the most common queries.

Many of the code blocks in this notebook use the following table, `t`, as the root table. This is a simple table with 100 rows and contains only a `Timestamp` column.

```python test-set=1
from deephaven import empty_table

t = empty_table(100).update("Timestamp = '2015-01-01T00:00:00 ET' + 'PT1m' * ii")
```

## Basic column manipulation

### Add and modify columns

[`update`](../../reference/table-operations/select/update.md) creates _in-memory_ columns. An in-memory column is one where the calculations are performed immediately, and results are stored in memory. The following code block adds three columns to `t`.

- `IntRowIndex`: 32-bit integer row indices.
- `LongRowIndex`: 64-bit integer row indices.
- `Group`: modulo of `IntRowIndex` and 5.

```python test-set=1
t_updated = t.update(
    ["IntRowIndex = i", "LongRowIndex = ii", "Group = IntRowIndex % 5"]
)
```

> [!NOTE]
> In this case, the three query strings passed to [`update`](../../reference/table-operations/select/update.md) are enclosed in `[]` - this is required to pass multiple query strings to table operations.

[`update_view`](../../reference/table-operations/select/update-view.md) creates _formula_ columns. A formula column is one where only the formula is stored in memory when called. Results are then computed on the fly as needed. [`update_view`](../../reference/table-operations/select/update-view.md) is best used when calculations are simple or when only small subsets of the data are used.

The following code block adds two new columns to `t`.

- `IntRowIndex`: 32-bit integer row indices.
- `Group`: modulo of `IntRowIndex` and 5.

```python test-set=1
t_update_viewed = t.update_view(["IntRowIndex = i", "Group = IntRowIndex % 5"])
```

[`lazy_update`](../../reference/table-operations/select/lazy-update.md) creates _memoized_ columns. A memoized column performs calculations immediately and stores them in memory, but a calculation is only performed and stored once for each unique set of input values.

The following code block adds two new columns to `t_updated`.

- `GroupSqrt`: square root of `Group`.
- `GroupSquared`: square of `Group`.

Because `Group` has only five unique values, only 10 calculations are needed to populate the two new columns.

```python test-set=1
t_lazy_updated = t_updated.lazy_update(
    ["GroupSqrt = sqrt(Group)", "GroupSquared = Group * Group"]
)
```

### Select columns

[`select`](../../reference/table-operations/select/select.md) and [`view`](../../reference/table-operations/select/view.md) both create tables containing subsets of input columns and new columns computed from input columns. The big difference between the methods is how much memory they allocate and when formulas are evaluated. Performance and memory considerations dictate the best method for a particular use case.

[`select`](../../reference/table-operations/select/select.md), like [`update`](../../reference/table-operations/select/update.md), creates _in-memory_ columns. The following code block selects two existing columns from `t_updated` and adds a new column.

```python test-set=1
t_selected = t_updated.select(
    ["Timestamp", "IntRowIndex", "RowPlusOne = IntRowIndex + 1"]
)
```

[`view`](../../reference/table-operations/select/view.md), like [`update_view`](../../reference/table-operations/select/update-view.md), creates _formula_ columns. The following code block selects two existing columns from `t_updated` and adds a new column.

```python test-set=1
t_viewed = t_updated.view(
    ["Timestamp", "LongRowIndex", "RowPlusOne = LongRowIndex + 1"]
)
```

### Drop columns

[`drop_columns`](../../reference/table-operations/select/drop-columns.md) removes columns from a table.

```python test-set=1
t_dropped = t_updated.drop_columns(["IntRowIndex", "LongRowIndex"])
```

Alternatively, [`view`](../../reference/table-operations/select/view.md) and [`select`](../../reference/table-operations/select/select.md) can be used to remove columns by omission. Both tables created below drop `IntRowIndex` and `LongRowIndex` by not including them in the list of columns passed in.

```python test-set=1 order=t_dropped_via_view,t_dropped_via_select
t_dropped_via_view = t_updated.view(["Timestamp", "Group"])
t_dropped_via_select = t_updated.select(["Timestamp", "Group"])
```

There's a lot to consider when choosing between in-memory, formula, and memoized columns in queries. For more information, see [choose the right selection method](../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method).

## Filter

### By condition

[`where`](../../reference/table-operations/filter/where.md) filters rows from a table based on a condition. The following code block keeps only rows in `t_updated` where `IntRowIndex` is an even number.

```python test-set=1
t_filtered = t_updated.where("IntRowIndex % 2 == 0")
```

Multiple filters can be applied in a single statement.

```python test-set=1
t_filtered_2 = t_updated.where(["Group == 3", "IntRowIndex % 2 == 0"])
```

A [`where`](../../reference/table-operations/filter/where.md) operation that applies multiple conditions keeps only data that meets _all_ of the criteria set forth. To keep rows that meet _one or more_ of the specified criteria, use [`where_one_of`](../../reference/table-operations/filter/where-one-of.md).

```python test-set=1
t_filtered_3 = t_updated.where_one_of(["Group == 3", "IntRowIndex % 2 == 0"])
```

In addition to [`where`](../../reference/table-operations/filter/where.md) and [`where_one_of`](../../reference/table-operations/filter/where-one-of.md), Deephaven offers [`where_in`](../../reference/table-operations/filter/where-in.md) and [`where_not_in`](../../reference/table-operations/filter/where-not-in.md), which filter table data based on another table.

See the [filtering guide](../../how-to-guides/use-filters.md) for more information.

### By row position

Methods that filter data by row position only keep a portion of data at the top, middle, or bottom of a table.

- [`head`](../../reference/table-operations/filter/head.md): Keep the first `n` rows.
- [`head_pct`](../../reference/table-operations/filter/head-pct.md): Keep the first `n%` of rows.
- [`tail`](../../reference/table-operations/filter/tail.md): Keep the last `n` rows.
- [`tail_pct`](../../reference/table-operations/filter/tail-pct.md): Keep the last `n%` of rows.
- [`slice`](../../reference/table-operations/filter/slice.md): Keep rows between `start` and `end`.
- [`slice_pct`](../../reference/table-operations/filter/slice-pct.md): Keep rows between `start%` and `end%`.

```python test-set=1 order=t_head,t_head_pct,t_tail,t_tail_pct,t_slice,t_slice_pct
# 10 rows at the top
t_head = t_updated.head(10)

# 10% of rows at the top
t_head_pct = t_updated.head_pct(0.1)

# 15 rows at the bottom
t_tail = t_updated.tail(15)

# 15% of rows at the bottom
t_tail_pct = t_updated.tail_pct(0.15)

# 20 rows in the middle
t_slice = t_updated.slice(40, 60)

# 20% of rows in the middle
t_slice_pct = t_updated.slice_pct(0.4, 0.6)
```

See the [filtering guide](../../how-to-guides/use-filters.md) for more information.

## Sort

The [`sort`](../../reference/table-operations/sort/sort.md) method sorts a table based on one or more columns. The following code block sorts `t_static` by `X` in ascending order.

```python test-set=1
t_sorted = t_updated.sort("Group")
```

Tables can be sorted by more than one column.

```python test-set=1
t_sorted_multiple = t_updated.sort(["Group", "IntRowIndex"])
```

To sort in descending order, use [`sort_descending`](../../reference/table-operations/sort/sort-descending.md).

```python test-set=1
t_sorted_desc = t_updated.sort_descending("LongRowIndex")
```

To sort multiple columns in different directions, use [`sort`](../../reference/table-operations/sort/sort.md) with a [`SortDirection`](/core/pydoc/code/deephaven.html#deephaven.SortDirection) for each column.

```python test-set=1
from deephaven import SortDirection

t_sort_multi = t_updated.sort(
    ["Group", "IntRowIndex"], [SortDirection.ASCENDING, SortDirection.DESCENDING]
)
```

See the [sorting guide](../../how-to-guides/sort.md) for more information.

## Group and aggregate data

Grouping data places rows into groups based on zero or more supplied key columns. Aggregation calculates summary statistics over a group of data. Grouping and aggregation are key components of data analysis, especially in Deephaven queries.

The examples in this section will use the following table.

```python test-set=2
from deephaven import empty_table

t = empty_table(100).update(
    [
        "Timestamp = '2015-01-01T00:00:00 ET' + 'PT1m' * ii",
        "X = randomInt(0, 100)",
        "Y = randomDouble(0, 10)",
        "Group = i % 5",
        "Letter = (i % 2 == 0) ? `A` : `B`",
    ]
)
```

### Group and ungroup data

[`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md) groups table data into [arrays](../../how-to-guides/work-with-arrays.md). Entire tables can be grouped.

```python test-set=2
t_grouped = t.group_by()
```

Data can be grouped by one or more key columns.

```python test-set=2 order=t_grouped_by_group,t_grouped_by_multiple
t_grouped_by_group = t.group_by("Group")
t_grouped_by_multiple = t.group_by(["Group", "Letter"])
```

[`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md) is the inverse of [`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md).

```python test-set=2 order=t_ungrouped,t_ungrouped_2,t_ungrouped_3
t_ungrouped = t_grouped.ungroup()
t_ungrouped_2 = t_grouped_by_group.ungroup()
t_ungrouped_3 = t_grouped_by_multiple.ungroup()
```

See the [grouping and ungrouping guide](../../how-to-guides/grouping-data.md) for more information.

### Single aggregations

[Single aggregations](../../how-to-guides/dedicated-aggregations.md) apply a single aggregation to an entire table. See [here](../../how-to-guides/dedicated-aggregations.md#single-aggregators) for a list of single aggregators.

The following code uses [`avg_by`](../../reference/table-operations/group-and-aggregate/avgBy.md) to calculate the aggregated average of columns `X` and `Y` from the table `t`. No grouping columns are given, so the averages are calculated over the entire table.

```python test-set=2
t_avg = t.view(["X", "Y"]).avg_by()
```

Aggregations are often calculated for groups of data. The following example calculates the average of `X` and `Y` for each unique value in `Group`.

```python test-set=2
t_avg_by_group = t.view(["Group", "X", "Y"]).avg_by("Group")
```

Single aggregations can be performed using multiple grouping columns.

```python test-set=2
t_avg_by_multiple = t.view(["Group", "Letter", "X", "Y"]).avg_by(["Group", "Letter"])
```

### Multiple aggregations

To apply multiple aggregations in a single operation, pass one or more of the [aggregators](../../how-to-guides/combined-aggregations.md#what-aggregations-are-available) into [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md).

The following code block calculates the average of `X` and the median of `Y`, grouped by `Group` and `Letter`. It renames the resultant columns `AvgX` and `MedianY`, respectively.

```python test-set=2
from deephaven import agg

agg_list = [agg.avg("AvgX = X"), agg.median("MedianY = Y")]

t_multiple_aggs = t.view(["Group", "Letter", "X", "Y"]).agg_by(
    agg_list, ["Group", "Letter"]
)
```

### Rolling aggregations

Most platforms offer aggregation functionality similar to the dedicated and multiple aggregations presented above (though none will work so easily on real-time data). However, Deephaven is unique and powerful in its vast library of cumulative, moving, and windowed calculations, facilitated by the [`update_by`](../../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`deephaven.updateby`](/core/pydoc/code/deephaven.updateby.html) Python module.

The following code block calculates the cumulative sum of `X` in `t`.

```python test-set=2
from deephaven.updateby import cum_sum

t_cum_sum = t.view("X").update_by(cum_sum(cols="SumX = X"))
```

Aggregations with [`update_by`](../../reference/table-operations/update-by-operations/updateBy.md) show the running total as it progresses through the table.

[`update_by`](../../reference/table-operations/update-by-operations/updateBy.md) can also limit these summary statistics to subsets of table data defined by a number of rows or amount of time backward, forward, or both. The following code block calculates the sum of the prior 10 rows in column `X` of table `t`.

```python test-set=2
from deephaven.updateby import rolling_sum_tick

t_windowed_sum = t.view("X").update_by(rolling_sum_tick("TenRowSumX = X", rev_ticks=10))
```

[`update_by`](../../reference/table-operations/update-by-operations/updateBy.md) also supports performing these calculations over groups of data. The following code block performs the same calculations as above, but groups the data by `Group` and `Letter`.

```python test-set=2
from deephaven.updateby import rolling_sum_tick, cum_sum

update_by_ops = [rolling_sum_tick("TenRowSumX = X", rev_ticks=10), cum_sum("SumX = X")]

t_updated_by_grouped = t.update_by(update_by_ops, ["Group", "Letter"])
```

Additionally, calculations can be windowed by time. The following code block calculates a 16-second rolling average of `X`, grouped by `Group`.

```python test-set=2
from deephaven.updateby import rolling_avg_time

t_rolling_avg_time = t.update_by(
    rolling_avg_time("Timestamp", "AvgX = X", rev_time="PT16s"), "Group"
)
```

Windows can look backward, forward, or both ways. The following example calculates the rolling average of the following windows:

- The previous 9 seconds.
- The current row and the previous 8 rows.
- The current row, the previous 10 rows, and the next 10 rows.
- The next 8 rows.

```python test-set=2
from deephaven.updateby import rolling_avg_time, rolling_avg_tick

update_by_ops = [
    rolling_avg_time("Timestamp", "BackwardTimeAvgX = X", rev_time="PT9s"),
    rolling_avg_tick("BackwardRowAvgX = X", rev_ticks=9),
    rolling_avg_tick("CenteredRowAvgX = X", rev_ticks=11, fwd_ticks=10),
    rolling_avg_tick("ForwardRowAvgX = X", rev_ticks=0, fwd_ticks=8),
]

t_windowed = t.update_by(update_by_ops)
```

> **_NOTE:_** A backward-looking window counts the current row as the first row backward. A forward-looking window counts the row ahead of the current row as the first row forward.

See the [`update_by` user guide](../../how-to-guides/rolling-aggregations.md) to learn more.

## Combine tables

There are two different ways to combine tables in Deephaven: merging and joining. Merging tables can be visualized as a vertical stacking of tables, whereas joining is more horizontal in nature, appending rows from one table to another based on common columns.

Each subsection below defines its own tables to demonstrate merging and joining tables in Deephaven.

### Merge tables

[`merge`](../../how-to-guides/merge-tables.md) combines an arbitrary number of tables provided they all have the same schema.

```python test-set=3 order=t_merged,t1,t2,t3
from deephaven import empty_table
from deephaven import merge

t1 = empty_table(10).update(
    ["Table = 1", "X = randomDouble(0, 10)", "Y = randomBool()"]
)
t2 = empty_table(6).update(["Table = 2", "X = 1.1 * i", "Y = randomBool()"])
t3 = empty_table(3).update(["Table = 3", "X = sin(0.1 * i)", "Y = true"])

t_merged = merge([t1, t2, t3])
```

### Join tables

Joining tables combines two tables based on one or more key columns. The key column(s) define data that is commonly shared between the two tables. The table on which the join operation is called is the left table, and the table passed as an argument is the right table.

Consider the following three tables.

```python test-set=4 order=t1,t2,t3
from deephaven.column import string_col, int_col, bool_col
from deephaven import new_table

t1 = new_table(
    [
        string_col("Letter", ["A", "B", "C", "B", "C", "F"]),
        int_col("Value", [5, 9, 19, 3, 11, 1]),
        bool_col("Truth", [True, True, True, False, False, True]),
    ]
)

t2 = new_table(
    [
        string_col("Letter", ["C", "A", "B", "D", "E", "F"]),
        string_col("Color", ["Blue", "Blue", "Green", "Yellow", "Red", "Orange"]),
        int_col("Count", [35, 19, 12, 20, 26, 7]),
        int_col("Remaining", [5, 21, 28, 20, 14, 8]),
    ]
)

t3 = new_table(
    [
        string_col("Letter", ["A", "E", "D", "B", "C"]),
        string_col("Color", ["Blue", "Red", "Yellow", "Green", "Black"]),
        int_col("Value", [5, 9, 19, 3, 11]),
        bool_col("Truth", [True, True, True, False, False]),
    ]
)
```

The tables `t1` and `t2` have the common column `Letter`. Moreover, `Letter` contains matching values in both tables. So, these tables can be joined on the `Letter` column.

```python test-set=4
t_joined = t1.natural_join(t2, "Letter")
```

Joins can use more than one key column. The tables `t2` and `t3` have both the `Letter` and `Color` columns in common, and they both have matching values. The following code block joins the two tables on both columns.

```python test-set=4
t_joined_2 = t2.natural_join(t3, ["Letter", "Color"])
```

By default, every join operation in Deephaven appends _all_ columns from the right table onto the left table. An optional third argument can be used to specify which columns to append. The following code block joins `t2` and `t3` on the `Letter` column, but only appends the `Value` column from `t3`.

```python test-set=4
t_joined_subset = t2.natural_join(t3, "Letter", "Value")
```

`t2` and `t3` share the `Color` column, so any attempt to append that onto `t2` results in a name conflict error. This can be avoided by either [renaming the column](../../reference/table-operations/select/rename-columns.md), or by using the `joins` argument to specify which columns to append.

The following example renames `Color` in t3 to `Color2` when joining the tables.

```python test-set=4
t_joined_rename = t2.natural_join(t3, "Letter", ["Color2 = Color", "Value"])
```

Join operations in Deephaven come in two distinct flavors: [exact and relational joins](../../how-to-guides/joins-exact-relational.md) and [time series and range joins](../../how-to-guides/joins-timeseries-range.md).

### Exact and relational joins

[Exact and relational joins](../../how-to-guides/joins-exact-relational.md) combine data from two tables based on exact matches in one or more related key columns.

#### Exact joins

Exact joins keep all rows from a left table, and append columns from a right table onto the left table.

Consider the following tables.

```python test-set=5 order=t_left_1,t_right_1
from deephaven.column import double_col, int_col, string_col
from deephaven import new_table

t_left_1 = new_table(
    [
        string_col("Color", ["Blue", "Magenta", "Yellow", "Magenta", "Beige", "Blue"]),
        int_col("Count", [5, 0, 2, 3, 7, 1]),
    ]
)

t_right_1 = new_table(
    [
        string_col("Color", ["Beige", "Yellow", "Blue", "Magenta", "Green"]),
        double_col("Weight", [2.3, 0.9, 1.4, 1.6, 3.0]),
    ]
)
```

`t_left_1` and `t_right_1` have a column of the same name and data type called `Color`. In `t_right_1`, `Color` has no duplicates. Additionally, all colors in `t_left_1` have an exact match in `t_right_1`. In cases like this, [`exact_join`](../../reference/table-operations/join/exact-join.md) is the most appropriate join operation.

```python test-set=5
t_exact_joined = t_left_1.exact_join(t_right_1, "Color")
```

Consider the following tables, which are similar to the previous example. However, in this case, `t_left_2` contains the color `Purple`, which is not in `t_right_2`.

```python test-set=6 order=t_left_2,t_right_2
from deephaven.column import double_col, int_col, string_col
from deephaven import new_table

t_left_2 = new_table(
    [
        string_col("Color", ["Blue", "Magenta", "Yellow", "Magenta", "Beige", "Green"]),
        int_col("Count", [5, 0, 2, 3, 7, 1]),
    ]
)

t_right_2 = new_table(
    [
        string_col("Color", ["Beige", "Yellow", "Blue", "Magenta", "Red"]),
        double_col("Weight", [2.3, 0.9, 1.4, 1.6, 3.0]),
    ]
)
```

In this case, an [`exact_join`](../../reference/table-operations/join/exact-join.md) will fail. Instead, use [`natural_join`](../../reference/table-operations/join/natural-join.md), which appends a null value where no match exists.

```python test-set=6
t_natural_joined = t_left_2.natural_join(t_right_2, "Color")
```

#### Relational joins

Relational joins are similar to SQL joins.

Consider the following tables.

```python test-set=7 order=t_left_3,t_right_3
from deephaven.column import double_col, int_col, string_col
from deephaven import new_table

t_left_3 = new_table(
    [
        string_col("Color", ["Blue", "Yellow", "Magenta", "Beige", "Black"]),
        int_col("Count", [5, 2, 3, 7, 6]),
    ]
)

t_right_3 = new_table(
    [
        string_col(
            "Color",
            ["Beige", "Yellow", "Blue", "Magenta", "Green", "Red", "Yellow", "Magenta"],
        ),
        double_col("Weight", [2.3, 0.9, 1.4, 1.6, 3.0, 0.5, 1.1, 2.8]),
    ]
)
```

[`join`](../../reference/table-operations/join/join.md) includes only rows where key columns in both tables contain an exact match, including multiple exact matches.

```python test-set=7
t_joined = t_left_3.join(table=t_right_3, on="Color")
```

[`left_outer_join`](../../reference/table-operations/join/left-outer-join.md) includes all rows from the left table as well as rows from the right table where an exact match exists. Null values are inserted where no match exists.

```python test-set=7
from deephaven.experimental.outer_joins import left_outer_join

t_left_outer_joined = left_outer_join(l_table=t_left_3, r_table=t_right_3, on="Color")
```

[`full_outer_join`](../../reference/table-operations/join/full-outer-join.md) includes all rows from both tables, regardless of whether an exact match exists. Null values are inserted where no match exists.

```python test-set=7
from deephaven.experimental.outer_joins import full_outer_join

t_full_outer_joined = full_outer_join(l_table=t_left_3, r_table=t_right_3, on="Color")
```

#### Time-series (inexact) joins

Time-series (inexact) joins are joins where the key column(s) used to join the tables may not match exactly. Instead, the closest value is used to match the data when no exact match exists.

Consider the following tables, which contain quotes and trades for two different stocks.

```python test-set=8 order=trades,quotes
from deephaven.column import datetime_col, double_col, int_col, string_col
from deephaven import new_table

trades = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]),
        datetime_col(
            "Timestamp",
            [
                "2021-04-05T09:10:00 ET",
                "2021-04-05T09:31:00 ET",
                "2021-04-05T16:00:00 ET",
                "2021-04-05T16:00:00 ET",
                "2021-04-05T16:30:00 ET",
            ],
        ),
        double_col("Price", [2.5, 3.7, 3.0, 100.50, 110]),
        int_col("Size", [52, 14, 73, 11, 6]),
    ]
)

quotes = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "IBM", "IBM", "IBM"]),
        datetime_col(
            "Timestamp",
            [
                "2021-04-05T09:11:00 ET",
                "2021-04-05T09:30:00 ET",
                "2021-04-05T16:00:00 ET",
                "2021-04-05T16:30:00 ET",
                "2021-04-05T17:00:00 ET",
            ],
        ),
        double_col("Bid", [2.5, 3.4, 97, 102, 108]),
        int_col("BidSize", [10, 20, 5, 13, 23]),
        double_col("Ask", [2.5, 3.4, 105, 110, 111]),
        int_col("AskSize", [83, 33, 47, 15, 5]),
    ]
)
```

[`aj`](../../reference/table-operations/join/aj.md) joins row values in the left table with the closest in the right table _without going over_. To see the quote at the time of a trade, use [`aj`](../../reference/table-operations/join/aj.md).

```python test-set=8
result_aj = trades.aj(quotes, ["Ticker", "Timestamp"])
```

[`raj`](../../reference/table-operations/join/raj.md) joins row values in the left table with the closest in the right table _without going under_. To see the first quote that comes after a trade, use [`raj`](../../reference/table-operations/join/raj.md).

```python test-set=8
result_raj = trades.raj(quotes, ["Ticker", "Timestamp"])
```

### More about joins

Every join operation presented in this notebook works on real-time data. Don't believe us? Try it for yourself! For more information about joins, see:

- [Joins: Exact and Relational](../../how-to-guides/joins-exact-relational.md)
- [Joins: Time-Series and Range](../../how-to-guides/joins-timeseries-range.md)
