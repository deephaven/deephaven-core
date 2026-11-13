---
title: Basic Table Operations
---

This page covers the table operations that appear in almost every query: operations for column manipulation, filtering, sorting, grouping and aggregation, and combining tables. These operations use query strings extensively. The [Query Strings](./query-strings.md) chapter, later in this crash course, covers query strings in detail.

Table operations are integral to the Deephaven Query Language (DQL). Besides creating tables, the previous chapters of the crash course used five table operations:

- [`update_view`](../../reference/table-operations/select/update-view.md), which adds columns to or modifies existing columns in a table.
- [`sum_by`](../../reference/table-operations/group-and-aggregate/sumBy.md), which groups rows by the columns you name and sums every other column within each group.
- [`rename_columns`](../../reference/table-operations/select/rename-columns.md), which renames columns in a table.
- [`sort`](../../reference/table-operations/sort/sort.md), which sorts a table by the given columns from least to greatest.
- [`sort_descending`](../../reference/table-operations/sort/sort-descending.md), which sorts a table by the given columns from greatest to least.

You can think of these as different transformations applied to the data in a table. The rest of this page builds on them with the basic table operations that make up the backbone of the most common queries.

The operations on this page also work on ticking tables, such as the ones in [Create Tables](./create-tables.md#create-ticking-tables), the same way they work on the static tables used in these examples. [Blink tables](../../conceptual/table-types.md#specialization-3-blink), ticking tables that keep only the rows added in the current update cycle, are the exception: a few operations on this page behave differently on them or [aren't supported](../../conceptual/table-types.md#unsupported-operations).

Many of the code blocks on this page use the following table, `t`, as the root table. This is a simple table with 100 rows and contains only a `Timestamp` column.

```python test-set=1
from deephaven import empty_table

t = empty_table(100).update("Timestamp = '2015-01-01T00:00:00 ET' + 'PT1m' * ii")
```

## Basic column manipulation

### Add columns

[`update`](../../reference/table-operations/select/update.md) creates _in-memory_ columns. An in-memory column computes all of its values immediately and stores the results in memory. The following code block adds three columns to `t`. Most examples in the rest of this section and in the Filter and Sort sections use the result, `t_updated`.

- `IntRowIndex`: 32-bit integer row indices.
- `LongRowIndex`: 64-bit integer row indices.
- `Group`: modulo of `IntRowIndex` and 5.

```python test-set=1
t_updated = t.update(
    ["IntRowIndex = i", "LongRowIndex = ii", "Group = IntRowIndex % 5"]
)
```

> [!NOTE]
> This code encloses the three query strings it passes to [`update`](../../reference/table-operations/select/update.md) in `[]`. To pass multiple query strings to a table operation, pass them together as one list, not as separate arguments.

[`update_view`](../../reference/table-operations/select/update-view.md) creates _formula_ columns. A formula column stores only its formula. Deephaven computes each value on the fly when something reads it. Use `update_view` when calculations are simple or when queries read only small subsets of the data.

The following code block adds two new columns to `t`.

- `IntRowIndex`: 32-bit integer row indices.
- `Group`: modulo of `IntRowIndex` and 5.

```python test-set=1
t_update_viewed = t.update_view(["IntRowIndex = i", "Group = IntRowIndex % 5"])
```

[`lazy_update`](../../reference/table-operations/select/lazy-update.md) creates _memoized_ columns. Like a formula column, a memoized column computes values on demand. Unlike a formula column, it caches the results. It computes the result for each unique set of input values only once and returns the cached result for later requests with the same inputs.

The following code block adds two new columns to `t_updated`.

- `GroupSqrt`: square root of `Group`.
- `GroupSquared`: square of `Group`.

Because `Group` has only five unique values, Deephaven performs only 10 calculations to populate the two new columns.

```python test-set=1
t_lazy_updated = t_updated.lazy_update(
    ["GroupSqrt = sqrt(Group)", "GroupSquared = Group * Group"]
)
```

### Select columns

[`select`](../../reference/table-operations/select/select.md) and [`view`](../../reference/table-operations/select/view.md) both create tables that contain only the columns you list: existing columns and new columns computed from them. They differ in the kind of column they create, which determines how much memory they allocate and when formulas are evaluated. Performance and memory considerations dictate the best method for a particular use case.

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

There's a lot to consider when choosing between in-memory, formula, and memoized columns in queries. For guidance, see [Choose the right column selection method](../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method).

### Drop columns

[`drop_columns`](../../reference/table-operations/select/drop-columns.md) removes columns from a table.

```python test-set=1
t_dropped = t_updated.drop_columns(["IntRowIndex", "LongRowIndex"])
```

You can also use [`view`](../../reference/table-operations/select/view.md) and [`select`](../../reference/table-operations/select/select.md) to remove columns by omission. Both tables below drop `IntRowIndex` and `LongRowIndex` because the column list leaves them out.

```python test-set=1 order=t_dropped_via_view,t_dropped_via_select
t_dropped_via_view = t_updated.view(["Timestamp", "Group"])
t_dropped_via_select = t_updated.select(["Timestamp", "Group"])
```

## Filter

### By condition

[`where`](../../reference/table-operations/filter/where.md) filters rows from a table based on a condition. The following code block keeps only rows in `t_updated` where `IntRowIndex` is an even number.

```python test-set=1
t_filtered = t_updated.where("IntRowIndex % 2 == 0")
```

You can apply multiple filters in a single statement.

```python test-set=1
t_filtered_2 = t_updated.where(["Group == 3", "IntRowIndex % 2 == 0"])
```

A [`where`](../../reference/table-operations/filter/where.md) operation that applies multiple conditions keeps only rows that meet _all_ of them. To keep rows that meet _one or more_ of the conditions, use [`where_one_of`](../../reference/table-operations/filter/where-one-of.md).

```python test-set=1
t_filtered_3 = t_updated.where_one_of(["Group == 3", "IntRowIndex % 2 == 0"])
```

In addition to [`where`](../../reference/table-operations/filter/where.md) and [`where_one_of`](../../reference/table-operations/filter/where-one-of.md), Deephaven offers [`where_in`](../../reference/table-operations/filter/where-in.md) and [`where_not_in`](../../reference/table-operations/filter/where-not-in.md), which filter table data based on another table.

See the [filtering guide](../../how-to-guides/use-filters.md) for more information.

### By row position

Methods that filter data by row position keep only a portion of data at the top, middle, or bottom of a table.

- [`head`](../../reference/table-operations/filter/head.md): Keep the given number of rows from the top.
- [`head_pct`](../../reference/table-operations/filter/head-pct.md): Keep the first fraction of rows, where `0.1` means 10%.
- [`tail`](../../reference/table-operations/filter/tail.md): Keep the given number of rows from the bottom.
- [`tail_pct`](../../reference/table-operations/filter/tail-pct.md): Keep the last fraction of rows, where `0.1` means 10%.
- [`slice`](../../reference/table-operations/filter/slice.md): Keep rows from a start position up to, but not including, an end position.
- [`slice_pct`](../../reference/table-operations/filter/slice-pct.md): Keep rows between two fractions of the table, such as `0.4` and `0.6`.

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

You used [`sort`](../../reference/table-operations/sort/sort.md) in the previous chapter to order a table by one column. The following code block sorts `t_updated` by `Group` in ascending order.

```python test-set=1
t_sorted = t_updated.sort("Group")
```

You can also sort by more than one column.

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

> [!TIP]
> You can also filter and sort tables directly in the UI without writing code. See [Navigate the GUI](../../how-to-guides/user-interface/navigating-the-ui.md) to learn more.

## Group and aggregate data

Grouping data places rows into groups based on zero or more key columns. Rows that have the same values in the key columns go into the same group. Aggregation calculates summary statistics over a group of data. Grouping and aggregation are key components of data analysis, especially in Deephaven queries. On a blink table, aggregations such as [`avg_by`](../../reference/table-operations/group-and-aggregate/avgBy.md) and [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md) cover every row the table delivers after the aggregation is created, not only the rows in the current update cycle.

The examples in this section use a new `t` with random `X` and `Y` columns plus `Group` and `Letter` columns to group by. It replaces the root table `t` from the top of this page.

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

[`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md) groups table data into [array columns](../../how-to-guides/work-with-arrays.md). You can group an entire table.

```python test-set=2
t_grouped = t.group_by()
```

You can also group data by one or more key columns.

```python test-set=2 order=t_grouped_by_group,t_grouped_by_multiple
t_grouped_by_group = t.group_by("Group")
t_grouped_by_multiple = t.group_by(["Group", "Letter"])
```

[`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md) expands array columns back into separate rows. It reverses [`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md), except that rows come back in group order and any key columns come first.

```python test-set=2 order=t_ungrouped,t_ungrouped_2,t_ungrouped_3
t_ungrouped = t_grouped.ungroup()
t_ungrouped_2 = t_grouped_by_group.ungroup()
t_ungrouped_3 = t_grouped_by_multiple.ungroup()
```

See the [grouping and ungrouping guide](../../how-to-guides/grouping-data.md) for more information.

### Single aggregations

[Single aggregations](../../how-to-guides/dedicated-aggregations.md) apply one type of aggregation to a table, either over the whole table or for each group. See the [list of single aggregators](../../how-to-guides/dedicated-aggregations.md#single-aggregators).

The following code uses [`avg_by`](../../reference/table-operations/group-and-aggregate/avgBy.md) to calculate the average of columns `X` and `Y` in the table `t`. The code passes no key columns, so `avg_by` averages over the entire table.

```python test-set=2
t_avg = t.view(["X", "Y"]).avg_by()
```

Aggregations often operate on groups of data. The following example calculates the average of `X` and `Y` for each unique value in `Group`.

```python test-set=2
t_avg_by_group = t.view(["Group", "X", "Y"]).avg_by("Group")
```

A single aggregation can also use multiple key columns.

```python test-set=2
t_avg_by_multiple = t.view(["Group", "Letter", "X", "Y"]).avg_by(["Group", "Letter"])
```

### Multiple aggregations

To apply multiple aggregations in a single operation, pass one or more of the [aggregators](../../how-to-guides/combined-aggregations.md#what-aggregations-are-available) into [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md).

The following code block calculates the average of `X` and the median of `Y`, grouped by `Group` and `Letter`. It names the resulting columns `AvgX` and `MedianY`, respectively.

```python test-set=2
from deephaven import agg

agg_list = [agg.avg("AvgX = X"), agg.median("MedianY = Y")]

t_multiple_aggs = t.view(["Group", "Letter", "X", "Y"]).agg_by(
    agg_list, ["Group", "Letter"]
)
```

### Cumulative and rolling aggregations

Deephaven also offers a large library of cumulative and rolling calculations. You use them through the [`update_by`](../../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`deephaven.updateby`](/core/pydoc/code/deephaven.updateby.html) Python module. Unlike [`avg_by`](../../reference/table-operations/group-and-aggregate/avgBy.md) and [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md), which return one row per group, `update_by` keeps every row of the table and adds its results as new columns.

The following code block calculates the cumulative sum of `X` in `t`.

```python test-set=2
from deephaven.updateby import cum_sum

t_cum_sum = t.view("X").update_by(cum_sum("SumX = X"))
```

In the result of [`update_by`](../../reference/table-operations/update-by-operations/updateBy.md), each row of `SumX` holds the running total of `X` from the first row through that row.

[`update_by`](../../reference/table-operations/update-by-operations/updateBy.md) can also limit these calculations to a window of rows. The following code block calculates the sum of the current row and the previous 9 rows in column `X` of table `t`.

```python test-set=2
from deephaven.updateby import rolling_sum_tick

t_windowed_sum = t.view("X").update_by(rolling_sum_tick("TenRowSumX = X", rev_ticks=10))
```

> [!NOTE]
> A backward-looking window counts the current row as the first row backward, so `rev_ticks=10` covers the current row and the previous 9 rows.

[`update_by`](../../reference/table-operations/update-by-operations/updateBy.md) also supports performing these calculations over groups of data. The following code block repeats the cumulative sum and the 10-row rolling sum from the previous examples, this time grouped by `Group` and `Letter`.

```python test-set=2
from deephaven.updateby import rolling_sum_tick, cum_sum

update_by_ops = [rolling_sum_tick("TenRowSumX = X", rev_ticks=10), cum_sum("SumX = X")]

t_updated_by_grouped = t.update_by(update_by_ops, ["Group", "Letter"])
```

You can also define windows by time. The following code block calculates a 16-minute rolling average of `X`, grouped by `Group`.

```python test-set=2
from deephaven.updateby import rolling_avg_time

t_rolling_avg_time = t.update_by(
    rolling_avg_time("Timestamp", "AvgX = X", rev_time="PT16m"), "Group"
)
```

Windows can look backward, forward, or both ways. A forward-looking window counts the row ahead of the current row as the first row forward. The following example calculates rolling averages over these windows:

- The current row and the previous 9 minutes.
- The current row and the previous 8 rows.
- The current row, the previous 10 rows, and the next 10 rows.
- The next 8 rows.

```python test-set=2
from deephaven.updateby import rolling_avg_time, rolling_avg_tick

update_by_ops = [
    rolling_avg_time("Timestamp", "BackwardTimeAvgX = X", rev_time="PT9m"),
    rolling_avg_tick("BackwardRowAvgX = X", rev_ticks=9),
    rolling_avg_tick("CenteredRowAvgX = X", rev_ticks=11, fwd_ticks=10),
    rolling_avg_tick("ForwardRowAvgX = X", rev_ticks=0, fwd_ticks=8),
]

t_windowed = t.update_by(update_by_ops)
```

See the [`update_by` user guide](../../how-to-guides/rolling-aggregations.md) to learn more.

## Combine tables

Deephaven combines tables in two ways: merging and joining. You can visualize merging as stacking tables vertically, whereas joining is horizontal: it appends columns from matching rows of another table, based on values in key columns.

Each subsection below defines its own tables to demonstrate merging and joining tables in Deephaven.

### Merge tables

[`merge`](../../reference/table-operations/merge/merge.md) combines any number of tables, as long as they all have the same schema: the same column names and data types. See the [merge tables guide](../../how-to-guides/merge-tables.md) for more information.

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

Joining combines two tables based on one or more key columns. Key columns hold the values that both tables share. The table you call the join method on is the left table, and the table you pass as an argument is the right table. The outer-join functions take the left table as `l_table` and the right table as `r_table`.

This page shows three kinds of join: exact joins, relational joins, and time-series joins. The [exact and relational joins](../../how-to-guides/joins-exact-relational.md) guide covers the first two in depth. The [inexact, time-series, and range joins](../../how-to-guides/joins-timeseries-range.md) guide covers time-series joins and range joins.

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

The tables `t1` and `t2` have the common column `Letter`, and `Letter` contains matching values in both tables. The following example joins them on `Letter` with [`natural_join`](../../reference/table-operations/join/natural-join.md), which keeps every row of the left table and appends the columns of the matching row from the right table. Where the right table has no matching row, the appended columns are null. [Exact joins](#exact-joins) covers `natural_join` in more detail.

```python test-set=4
t_joined = t1.natural_join(t2, "Letter")
```

Joins can use more than one key column. The tables `t2` and `t3` have both the `Letter` and `Color` columns in common, and they both have matching values. The following code block joins the two tables on both columns.

```python test-set=4
t_joined_2 = t2.natural_join(t3, ["Letter", "Color"])
```

By default, the join operations on this page append _all_ non-key columns from the right table onto the left table. The optional `joins` argument specifies which columns to append. The following code block joins `t2` and `t3` on the `Letter` column, but only appends the `Value` column from `t3`.

```python test-set=4
t_joined_subset = t2.natural_join(t3, "Letter", "Value")
```

`t2` and `t3` share the `Color` column, so appending `Color` from `t3` onto `t2` results in a name conflict error. To avoid the error, rename the column with [`rename_columns`](../../reference/table-operations/select/rename-columns.md) before the join, or rename it in the `joins` argument.

The following example renames `Color` in `t3` to `Color2` in the `joins` argument.

```python test-set=4
t_joined_rename = t2.natural_join(t3, "Letter", ["Color2 = Color", "Value"])
```

#### Exact joins

Deephaven has two [exact joins](../../how-to-guides/joins-exact-relational.md): [`natural_join`](../../reference/table-operations/join/natural-join.md), which you used above, and [`exact_join`](../../reference/table-operations/join/exact-join.md). Like `natural_join`, `exact_join` keeps every row of the left table. `exact_join` always raises an error if a key value appears more than once in the right table, and `natural_join` does so by default.

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

`t_left_1` and `t_right_1` both have a `Color` column with the same data type. In `t_right_1`, `Color` has no duplicates. Additionally, all colors in `t_left_1` have an exact match in `t_right_1`. In cases like this, [`exact_join`](../../reference/table-operations/join/exact-join.md) is the most appropriate join operation.

```python test-set=5
t_exact_joined = t_left_1.exact_join(t_right_1, "Color")
```

The following tables are like the previous pair, except that `t_left_2` contains the color `Green`, which is not in `t_right_2`.

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

Here, [`exact_join`](../../reference/table-operations/join/exact-join.md) fails because it requires every key in the left table to have a match in the right table. Instead, use [`natural_join`](../../reference/table-operations/join/natural-join.md), which fills unmatched rows with nulls.

```python test-set=6
t_natural_joined = t_left_2.natural_join(t_right_2, "Color")
```

#### Relational joins

[Relational joins](../../how-to-guides/joins-exact-relational.md) are similar to SQL joins: a left-table row can match several right-table rows, and the result has one row for each match. The two outer joins on this page, [`left_outer_join`](../../reference/table-operations/join/left-outer-join.md) and [`full_outer_join`](../../reference/table-operations/join/full-outer-join.md), also keep rows that have no match. Both are experimental, so their API may change.

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

[`join`](../../reference/table-operations/join/join.md) keeps only rows whose key values match in both tables.

```python test-set=7
t_inner_joined = t_left_3.join(t_right_3, "Color")
```

[`left_outer_join`](../../reference/table-operations/join/left-outer-join.md) includes all rows from the left table as well as rows from the right table where a match exists. The result contains null values where no match exists.

```python test-set=7
from deephaven.experimental.outer_joins import left_outer_join

t_left_outer_joined = left_outer_join(l_table=t_left_3, r_table=t_right_3, on="Color")
```

[`full_outer_join`](../../reference/table-operations/join/full-outer-join.md) includes all rows from both tables, regardless of whether a match exists. The result contains null values where no match exists.

```python test-set=7
from deephaven.experimental.outer_joins import full_outer_join

t_full_outer_joined = full_outer_join(l_table=t_left_3, r_table=t_right_3, on="Color")
```

#### Time-series (inexact) joins

[Time-series (inexact) joins](../../how-to-guides/joins-timeseries-range.md) match on zero or more exact key columns plus a final key column that does not need to match exactly. The final key column is usually a timestamp. If the right table has no row that qualifies, the appended columns are null.

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

[`aj`](../../reference/table-operations/join/aj.md) matches each row in the left table with the closest row in the right table _without going over_. To see the most recent quote at or before a trade, use `aj`.

```python test-set=8
result_aj = trades.aj(quotes, ["Ticker", "Timestamp"])
```

[`raj`](../../reference/table-operations/join/raj.md) matches each row in the left table with the closest row in the right table _without going under_. To see the first quote at or after a trade, use `raj`.

```python test-set=8
result_raj = trades.raj(quotes, ["Ticker", "Timestamp"])
```

Next, [Time and Calendars](./time-calendars.md) shows how to work with date-time data and business calendars in these operations.
