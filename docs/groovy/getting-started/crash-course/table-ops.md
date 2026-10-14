---
title: Basic Table Operations
---

This page covers the table operations that appear in almost every query: operations for column manipulation, filtering, sorting, grouping and aggregation, and combining tables. These operations use query strings extensively. The [Query Strings](./query-strings.md) chapter, later in this crash course, covers query strings in detail.

Table operations are integral to the Deephaven Query Language (DQL). Besides creating tables, the previous chapters of the crash course used five table operations:

- [`updateView`](../../reference/table-operations/select/update-view.md), which adds columns to or modifies existing columns in a table.
- [`sumBy`](../../reference/table-operations/group-and-aggregate/sumBy.md), which groups rows by the columns you name and sums every other column within each group.
- [`renameColumns`](../../reference/table-operations/select/rename-columns.md), which renames columns in a table.
- [`sort`](../../reference/table-operations/sort/sort.md), which sorts a table by the given columns from least to greatest.
- [`sortDescending`](../../reference/table-operations/sort/sort-descending.md), which sorts a table by the given columns from greatest to least.

You can think of these as different transformations applied to the data in a table. The rest of this page builds on them with the basic table operations that make up the backbone of the most common queries.

The operations on this page also work on ticking tables, such as the ones in [Create Tables](./create-tables.md#create-ticking-tables), the same way they work on the static tables used in these examples. [Blink tables](../../conceptual/table-types.md#specialization-3-blink), ticking tables that keep only the rows added in the current update cycle, are the exception: a few operations on this page behave differently on them or [aren't supported](../../conceptual/table-types.md#unsupported-operations).

Many of the code blocks on this page use the following table, `t`, as the root table. This is a simple table with 100 rows and contains only a `Timestamp` column.

```groovy test-set=1
t = emptyTable(100).update("Timestamp = '2015-01-01T00:00:00 ET' + 'PT1m' * ii")
```

## Basic column manipulation

### Add columns

[`update`](../../reference/table-operations/select/update.md) creates _in-memory_ columns. An in-memory column computes all of its values immediately and stores the results in memory. The following code block adds three columns to `t`. Most examples in the rest of this section and in the Filter and Sort sections use the result, `tUpdated`.

- `IntRowIndex`: 32-bit integer row indices.
- `LongRowIndex`: 64-bit integer row indices.
- `Group`: modulo of `IntRowIndex` and 5.

```groovy test-set=1
tUpdated = t.update("IntRowIndex = i", "LongRowIndex = ii", "Group = IntRowIndex % 5")
```

[`updateView`](../../reference/table-operations/select/update-view.md) creates _formula_ columns. A formula column stores only its formula. Deephaven computes each value on the fly when something reads it. Use `updateView` when calculations are simple or when queries read only small subsets of the data.

The following code block adds two new columns to `t`.

- `IntRowIndex`: 32-bit integer row indices.
- `Group`: modulo of `IntRowIndex` and 5.

```groovy test-set=1
tUpdateViewed = t.updateView("IntRowIndex = i", "Group = IntRowIndex % 5")
```

[`lazyUpdate`](../../reference/table-operations/select/lazy-update.md) creates _memoized_ columns. Like a formula column, a memoized column computes values on demand. Unlike a formula column, it caches the results. It computes the result for each unique set of input values only once and returns the cached result for later requests with the same inputs.

The following code block adds two new columns to `tUpdated`.

- `GroupSqrt`: square root of `Group`.
- `GroupSquared`: square of `Group`.

Because `Group` has only five unique values, Deephaven performs only 10 calculations to populate the two new columns.

```groovy test-set=1
tLazyUpdated = tUpdated.lazyUpdate("GroupSqrt = sqrt(Group)", "GroupSquared = Group * Group")
```

### Select columns

[`select`](../../reference/table-operations/select/select.md) and [`view`](../../reference/table-operations/select/view.md) both create tables that contain only the columns you list: existing columns and new columns computed from them. They differ in the kind of column they create, which determines how much memory they allocate and when formulas are evaluated. Performance and memory considerations dictate the best method for a particular use case.

[`select`](../../reference/table-operations/select/select.md), like [`update`](../../reference/table-operations/select/update.md), creates _in-memory_ columns. The following code block selects two existing columns from `tUpdated` and adds a new column.

```groovy test-set=1
tSelected = tUpdated.select("Timestamp", "IntRowIndex", "RowPlusOne = IntRowIndex + 1")
```

[`view`](../../reference/table-operations/select/view.md), like [`updateView`](../../reference/table-operations/select/update-view.md), creates _formula_ columns. The following code block selects two existing columns from `tUpdated` and adds a new column.

```groovy test-set=1
tViewed = tUpdated.view("Timestamp", "LongRowIndex", "RowPlusOne = LongRowIndex + 1")
```

There's a lot to consider when choosing between in-memory, formula, and memoized columns in queries. For guidance, see [Choose the right column selection method](../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method).

### Drop columns

[`dropColumns`](../../reference/table-operations/select/drop-columns.md) removes columns from a table.

```groovy test-set=1
tDropped = tUpdated.dropColumns("IntRowIndex", "LongRowIndex")
```

You can also use [`view`](../../reference/table-operations/select/view.md) and [`select`](../../reference/table-operations/select/select.md) to remove columns by omission. Both tables below drop `IntRowIndex` and `LongRowIndex` because the column list leaves them out.

```groovy test-set=1 order=tDroppedViaView,tDroppedViaSelect
tDroppedViaView = tUpdated.view("Timestamp", "Group")
tDroppedViaSelect = tUpdated.select("Timestamp", "Group")
```

## Filter

### By condition

[`where`](../../reference/table-operations/filter/where.md) filters rows from a table based on a condition. The following code block keeps only rows in `tUpdated` where `IntRowIndex` is an even number.

```groovy test-set=1
tFiltered = tUpdated.where("IntRowIndex % 2 == 0")
```

You can apply multiple filters in a single statement.

```groovy test-set=1
tFiltered2 = tUpdated.where("Group == 3", "IntRowIndex % 2 == 0")
```

A [`where`](../../reference/table-operations/filter/where.md) operation that applies multiple conditions keeps only rows that meet _all_ of them. To keep rows that meet _one or more_ of the conditions, use `where` with the logical OR operator, `||`.

```groovy test-set=1
tFiltered3 = tUpdated.where("Group == 3 || IntRowIndex % 2 == 0")
```

In addition to [`where`](../../reference/table-operations/filter/where.md), Deephaven offers [`whereIn`](../../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../../reference/table-operations/filter/where-not-in.md), which filter table data based on another table.

See the [filtering guide](../../how-to-guides/use-filters.md) for more information.

### By row position

Methods that filter data by row position keep only a portion of data at the top, middle, or bottom of a table.

- [`head`](../../reference/table-operations/filter/head.md): Keep the given number of rows from the top.
- [`headPct`](../../reference/table-operations/filter/head-pct.md): Keep the first fraction of rows, where `0.1` means 10%.
- [`tail`](../../reference/table-operations/filter/tail.md): Keep the given number of rows from the bottom.
- [`tailPct`](../../reference/table-operations/filter/tail-pct.md): Keep the last fraction of rows, where `0.1` means 10%.
- [`slice`](../../reference/table-operations/filter/slice.md): Keep rows from a start position up to, but not including, an end position.
- [`slicePct`](../../reference/table-operations/filter/slice-pct.md): Keep rows between two fractions of the table, such as `0.4` and `0.6`.

```groovy test-set=1 order=tHead,tHeadPct,tTail,tTailPct,tSlice,tSlicePct
// 10 rows at the top
tHead = tUpdated.head(10)

// 10% of rows at the top
tHeadPct = tUpdated.headPct(0.1)

// 15 rows at the bottom
tTail = tUpdated.tail(15)

// 15% of rows at the bottom
tTailPct = tUpdated.tailPct(0.15)

// 20 rows in the middle
tSlice = tUpdated.slice(40, 60)

// 20% of rows in the middle
tSlicePct = tUpdated.slicePct(0.4, 0.6)
```

See the [filtering guide](../../how-to-guides/use-filters.md) for more information.

## Sort

You used [`sort`](../../reference/table-operations/sort/sort.md) in the previous chapter to order a table by one column. The following code block sorts `tUpdated` by `Group` in ascending order.

```groovy test-set=1
tSorted = tUpdated.sort("Group")
```

You can also sort by more than one column.

```groovy test-set=1
tSortedMultiple = tUpdated.sort("Group", "IntRowIndex")
```

To sort in descending order, use [`sortDescending`](../../reference/table-operations/sort/sort-descending.md).

```groovy test-set=1
tSortedDesc = tUpdated.sortDescending("LongRowIndex")
```

To sort multiple columns in different directions, pass [`sort`](../../reference/table-operations/sort/sort.md) a list of [`SortColumn`](/core/javadoc/io/deephaven/api/SortColumn.html) objects built with `SortColumn.asc` and `SortColumn.desc`. Both methods take a column name wrapped in [`ColumnName.of`](/core/javadoc/io/deephaven/api/ColumnName.html).

```groovy test-set=1
sortColumns = [
    SortColumn.asc(ColumnName.of("Group")),
    SortColumn.desc(ColumnName.of("IntRowIndex"))
]

tSortMulti = tUpdated.sort(sortColumns)
```

See the [sorting guide](../../how-to-guides/sort.md) for more information.

> [!TIP]
> You can also filter and sort tables directly in the UI without writing code. See [Navigate the GUI](../../how-to-guides/user-interface/navigating-the-ui.md) to learn more.

## Group and aggregate data

Grouping data places rows into groups based on zero or more key columns. Rows that have the same values in the key columns go into the same group. Aggregation calculates summary statistics over a group of data. Grouping and aggregation are key components of data analysis, especially in Deephaven queries. On a blink table, aggregations such as `avgBy` and `aggBy` cover every row the table delivers after the aggregation is created, not only the rows in the current update cycle.

The examples in this section use a new `t` with random `X` and `Y` columns plus `Group` and `Letter` columns to group by. It replaces the root table `t` from the top of this page.

```groovy test-set=2
t = emptyTable(100).update(
        "Timestamp = '2015-01-01T00:00:00 ET' + 'PT1m' * ii",
        "X = randomInt(0, 100)",
        "Y = randomDouble(0, 10)",
        "Group = i % 5",
        "Letter = (i % 2 == 0) ? `A` : `B`",
)
```

### Group and ungroup data

[`groupBy`](../../reference/table-operations/group-and-aggregate/groupBy.md) groups table data into [array columns](../../how-to-guides/work-with-arrays.md). You can group an entire table.

```groovy test-set=2
tGrouped = t.groupBy()
```

You can also group data by one or more key columns.

```groovy test-set=2 order=tGroupedByGroup,tGroupedByMultiple
tGroupedByGroup = t.groupBy("Group")
tGroupedByMultiple = t.groupBy("Group", "Letter")
```

[`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md) expands array columns back into separate rows. It reverses [`groupBy`](../../reference/table-operations/group-and-aggregate/groupBy.md), except that rows come back in group order and any key columns come first.

```groovy test-set=2 order=tUngrouped,tUngrouped2,tUngrouped3
tUngrouped = tGrouped.ungroup()
tUngrouped2 = tGroupedByGroup.ungroup()
tUngrouped3 = tGroupedByMultiple.ungroup()
```

See the [grouping and ungrouping guide](../../how-to-guides/grouping-data.md) for more information.

### Single aggregations

[Single aggregations](../../how-to-guides/dedicated-aggregations.md) apply one type of aggregation to a table, either over the whole table or for each group. See the [list of single aggregators](../../how-to-guides/dedicated-aggregations.md#single-aggregators).

The following code uses [`avgBy`](../../reference/table-operations/group-and-aggregate/avgBy.md) to calculate the average of columns `X` and `Y` in the table `t`. The code passes no key columns, so `avgBy` averages over the entire table.

```groovy test-set=2
tAvg = t.view("X", "Y").avgBy()
```

Aggregations often operate on groups of data. The following example calculates the average of `X` and `Y` for each unique value in `Group`.

```groovy test-set=2
tAvgByGroup = t.view("Group", "X", "Y").avgBy("Group")
```

A single aggregation can also use multiple key columns.

```groovy test-set=2
tAvgByMultiple = t.view("Group", "Letter", "X", "Y").avgBy("Group", "Letter")
```

### Multiple aggregations

To apply multiple aggregations in a single operation, pass one or more of the [aggregators](../../how-to-guides/combined-aggregations.md#what-aggregations-are-available) into [`aggBy`](../../reference/table-operations/group-and-aggregate/aggBy.md).

The following code block calculates the average of `X` and the median of `Y`, grouped by `Group` and `Letter`. It names the resulting columns `AvgX` and `MedianY`, respectively.

```groovy test-set=2
aggList = [AggAvg("AvgX = X"), AggMed("MedianY = Y")]

tMultipleAggs = t.view("Group", "Letter", "X", "Y").aggBy(
    aggList, "Group", "Letter")
```

### Cumulative and rolling aggregations

Deephaven also offers a large library of cumulative and rolling calculations. You use them through the [`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`io.deephaven.api.updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) package. Unlike [`avgBy`](../../reference/table-operations/group-and-aggregate/avgBy.md) and [`aggBy`](../../reference/table-operations/group-and-aggregate/aggBy.md), which return one row per group, `updateBy` keeps every row of the table and adds its results as new columns.

The following code block calculates the cumulative sum of `X` in `t`.

```groovy test-set=2
tCumSum = t.view("X").updateBy(CumSum("SumX = X"))
```

In the result of [`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md), each row of `SumX` holds the running total of `X` from the first row through that row.

[`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md) can also limit these calculations to a window of rows. The following code block calculates the sum of the current row and the previous 9 rows in column `X` of table `t`.

```groovy test-set=2
tWindowedSum = t.view("X").updateBy(RollingSum(10, "TenRowSumX = X"))
```

> [!NOTE]
> A backward-looking window counts the current row as the first row backward, so a backward window of 10 rows covers the current row and the previous 9 rows.

[`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md) also supports performing these calculations over groups of data. The following code block repeats the cumulative sum and the 10-row rolling sum from the previous examples, this time grouped by `Group` and `Letter`.

```groovy test-set=2
updateByOps = [RollingSum(10, "TenRowSumX = X"), CumSum("SumX = X")]

tUpdatedByGrouped = t.updateBy(updateByOps, "Group", "Letter")
```

You can also define windows by time. The following code block calculates a 16-minute rolling average of `X`, grouped by `Group`.

```groovy test-set=2
tRollingAvgTime = t.updateBy(RollingAvg("Timestamp", parseDuration("PT16m"), "AvgX = X"), "Group")
```

Windows can look backward, forward, or both ways. A forward-looking window counts the row ahead of the current row as the first row forward. The following example calculates rolling averages over these windows:

- The current row and the previous 9 minutes.
- The current row and the previous 8 rows.
- The current row, the previous 10 rows, and the next 10 rows.
- The next 8 rows.

```groovy test-set=2
updateByOps = [
    RollingAvg("Timestamp", parseDuration("PT9m"), "BackwardTimeAvgX = X"),
    RollingAvg(9, "BackwardRowAvgX = X"),
    RollingAvg(11, 10, "CenteredRowAvgX = X"),
    RollingAvg(0, 8, "ForwardRowAvgX = X"),
]

tWindowed = t.updateBy(updateByOps)
```

See the [`updateBy` user guide](../../how-to-guides/rolling-aggregations.md) to learn more.

## Combine tables

Deephaven combines tables in two ways: merging and joining. Merging can be visualized as stacking tables vertically, whereas joining is more horizontal in nature: it appends columns from matching rows of another table, based on values in key columns.

Each subsection below defines its own tables to demonstrate merging and joining tables in Deephaven.

### Merge tables

[`merge`](../../reference/table-operations/merge/merge.md) combines any number of tables, as long as they all have the same schema: the same column names and data types. See the [merge tables guide](../../how-to-guides/merge-tables.md) for more information.

```groovy test-set=3 order=tMerged,t1,t2,t3
t1 = emptyTable(10).update("Table = 1", "X = randomDouble(0, 10)", "Y = randomBool()")
t2 = emptyTable(6).update("Table = 2", "X = 1.1 * i", "Y = randomBool()")
t3 = emptyTable(3).update("Table = 3", "X = sin(0.1 * i)", "Y = true")

tMerged = merge(t1, t2, t3)
```

### Join tables

Joining combines two tables based on one or more key columns. Key columns hold the values that both tables share. The table you call the join method on is the left table, and the table you pass as an argument is the right table. The `OuterJoinTools` methods take the left table first and the right table second.

This page shows three kinds of join: exact joins, relational joins, and time-series joins. The [exact and relational joins](../../how-to-guides/joins-exact-relational.md) guide covers the first two in depth. The [inexact, time-series, and range joins](../../how-to-guides/joins-timeseries-range.md) guide covers time-series joins and range joins.

Consider the following three tables.

```groovy test-set=4 order=t1,t2,t3
t1 = newTable(
        stringCol("Letter", "A", "B", "C", "B", "C", "F"),
        intCol("Value", 5, 9, 19, 3, 11, 1),
        booleanCol("Truth", true, true, true, false, false, true),
)

t2 = newTable(
        stringCol("Letter", "C", "A", "B", "D", "E", "F"),
        stringCol("Color", "Blue", "Blue", "Green", "Yellow", "Red", "Orange"),
        intCol("Count", 35, 19, 12, 20, 26, 7),
        intCol("Remaining", 5, 21, 28, 20, 14, 8),
)

t3 = newTable(
        stringCol("Letter", "A", "E", "D", "B", "C"),
        stringCol("Color", "Blue", "Red", "Yellow", "Green", "Black"),
        intCol("Value", 5, 9, 19, 3, 11),
        booleanCol("Truth", true, true, true, false, false),
)
```

The tables `t1` and `t2` have the common column `Letter`, and `Letter` contains matching values in both tables. The following example joins them on `Letter` with [`naturalJoin`](../../reference/table-operations/join/natural-join.md), which keeps every row of the left table and appends the columns of the matching row from the right table. Where the right table has no matching row, the appended columns are null. [Exact joins](#exact-joins) covers `naturalJoin` in more detail.

```groovy test-set=4
tJoined = t1.naturalJoin(t2, "Letter")
```

Joins can use more than one key column. The tables `t2` and `t3` have both the `Letter` and `Color` columns in common, and they both have matching values. The following code block joins the two tables on both columns. It passes the key columns as one comma-separated string, the format every join example on this page uses.

```groovy test-set=4
tJoined2 = t2.naturalJoin(t3, "Letter, Color")
```

By default, the join operations on this page append _all_ non-key columns from the right table onto the left table. The optional `columnsToAdd` argument, also a comma-separated string, specifies which columns to append. The following code block joins `t2` and `t3` on the `Letter` column, but only appends the `Value` column from `t3`.

```groovy test-set=4
tJoinedSubset = t2.naturalJoin(t3, "Letter", "Value")
```

`t2` and `t3` share the `Color` column, so appending `Color` from `t3` onto `t2` results in a name conflict error. To avoid the error, rename the column with [`renameColumns`](../../reference/table-operations/select/rename-columns.md) before the join, or rename it in the `columnsToAdd` argument.

The following example renames `Color` in `t3` to `Color2` in the `columnsToAdd` argument.

```groovy test-set=4
tJoinedRename = t2.naturalJoin(t3, "Letter", "Color2 = Color, Value")
```

#### Exact joins

Deephaven has two [exact joins](../../how-to-guides/joins-exact-relational.md): [`naturalJoin`](../../reference/table-operations/join/natural-join.md), which you used above, and [`exactJoin`](../../reference/table-operations/join/exact-join.md). Like `naturalJoin`, `exactJoin` keeps every row of the left table. `exactJoin` always throws an error if a key value appears more than once in the right table, and `naturalJoin` does so by default.

Consider the following tables.

```groovy test-set=5 order=tLeft1,tRight1
tLeft1 = newTable(
        stringCol("Color", "Blue", "Magenta", "Yellow", "Magenta", "Beige", "Blue"),
        intCol("Count", 5, 0, 2, 3, 7, 1),
)

tRight1 = newTable(
        stringCol("Color", "Beige", "Yellow", "Blue", "Magenta", "Green"),
        doubleCol("Weight", 2.3, 0.9, 1.4, 1.6, 3.0),
)
```

`tLeft1` and `tRight1` both have a `Color` column with the same data type. In `tRight1`, `Color` has no duplicates. Additionally, all colors in `tLeft1` have an exact match in `tRight1`. In cases like this, [`exactJoin`](../../reference/table-operations/join/exact-join.md) is the most appropriate join operation.

```groovy test-set=5
tExactJoined = tLeft1.exactJoin(tRight1, "Color")
```

The following tables are like the previous pair, except that `tLeft2` contains the color `Green`, which is not in `tRight2`.

```groovy test-set=6 order=tLeft2,tRight2
tLeft2 = newTable(
        stringCol("Color", "Blue", "Magenta", "Yellow", "Magenta", "Beige", "Green"),
        intCol("Count", 5, 0, 2, 3, 7, 1),
)

tRight2 = newTable(
        stringCol("Color", "Beige", "Yellow", "Blue", "Magenta", "Red"),
        doubleCol("Weight", 2.3, 0.9, 1.4, 1.6, 3.0),
)
```

Here, [`exactJoin`](../../reference/table-operations/join/exact-join.md) fails because it requires every key in the left table to have a match in the right table. Instead, use [`naturalJoin`](../../reference/table-operations/join/natural-join.md), which fills unmatched rows with nulls.

```groovy test-set=6
tNaturalJoined = tLeft2.naturalJoin(tRight2, "Color")
```

#### Relational joins

[Relational joins](../../how-to-guides/joins-exact-relational.md) are similar to SQL joins: a left-table row can match several right-table rows, and the result has one row for each match. The two outer joins on this page, [`leftOuterJoin`](../../reference/table-operations/join/left-outer-join.md) and [`fullOuterJoin`](../../reference/table-operations/join/full-outer-join.md), also keep rows that have no match. Both are experimental, so their API may change.

Consider the following tables.

```groovy test-set=7 order=tLeft3,tRight3
tLeft3 = newTable(
        stringCol("Color", "Blue", "Yellow", "Magenta", "Beige", "Black"),
        intCol("Count", 5, 2, 3, 7, 6),
)

tRight3 = newTable(
        stringCol("Color", "Beige", "Yellow", "Blue", "Magenta", "Green", "Red", "Yellow", "Magenta"),
        doubleCol("Weight", 2.3, 0.9, 1.4, 1.6, 3.0, 0.5, 1.1, 2.8),
)
```

[`join`](../../reference/table-operations/join/join.md) keeps only rows whose key values match in both tables.

```groovy test-set=7
tInnerJoined = tLeft3.join(tRight3, "Color")
```

[`leftOuterJoin`](../../reference/table-operations/join/left-outer-join.md) includes all rows from the left table as well as rows from the right table where a match exists. The result contains null values where no match exists.

```groovy test-set=7
import io.deephaven.engine.util.OuterJoinTools

tLeftOuterJoined = OuterJoinTools.leftOuterJoin(tLeft3, tRight3, "Color")
```

[`fullOuterJoin`](../../reference/table-operations/join/full-outer-join.md) includes all rows from both tables, regardless of whether a match exists. The result contains null values where no match exists.

```groovy test-set=7
import io.deephaven.engine.util.OuterJoinTools

tFullOuterJoined = OuterJoinTools.fullOuterJoin(tLeft3, tRight3, "Color")
```

#### Time-series (inexact) joins

[Time-series (inexact) joins](../../how-to-guides/joins-timeseries-range.md) match on zero or more exact key columns plus a final key column that does not need to match exactly. The final key column is usually a timestamp. If the right table has no row that qualifies, the appended columns are null.

Consider the following tables, which contain quotes and trades for two different stocks.

```groovy test-set=8 order=trades,quotes
trades = newTable(
        stringCol("Ticker", "AAPL", "AAPL", "AAPL", "IBM", "IBM"),
        instantCol(
            "Timestamp",
            parseInstant("2021-04-05T09:10:00 ET"),
            parseInstant("2021-04-05T09:31:00 ET"),
            parseInstant("2021-04-05T16:00:00 ET"),
            parseInstant("2021-04-05T16:00:00 ET"),
            parseInstant("2021-04-05T16:30:00 ET"),
        ),
        doubleCol("Price", 2.5, 3.7, 3.0, 100.50, 110),
        intCol("Size", 52, 14, 73, 11, 6),
)

quotes = newTable(
        stringCol("Ticker", "AAPL", "AAPL", "IBM", "IBM", "IBM"),
        instantCol(
            "Timestamp",
            parseInstant("2021-04-05T09:11:00 ET"),
            parseInstant("2021-04-05T09:30:00 ET"),
            parseInstant("2021-04-05T16:00:00 ET"),
            parseInstant("2021-04-05T16:30:00 ET"),
            parseInstant("2021-04-05T17:00:00 ET"),
        ),
        doubleCol("Bid", 2.5, 3.4, 97, 102, 108),
        intCol("BidSize", 10, 20, 5, 13, 23),
        doubleCol("Ask", 2.5, 3.4, 105, 110, 111),
        intCol("AskSize", 83, 33, 47, 15, 5),
)
```

[`aj`](../../reference/table-operations/join/aj.md) matches each row in the left table with the closest row in the right table _without going over_. To see the most recent quote at or before a trade, use `aj`.

```groovy test-set=8
resultAj = trades.aj(quotes, "Ticker, Timestamp")
```

[`raj`](../../reference/table-operations/join/raj.md) matches each row in the left table with the closest row in the right table _without going under_. To see the first quote at or after a trade, use `raj`.

```groovy test-set=8
resultRaj = trades.raj(quotes, "Ticker, Timestamp")
```

Next, [Time and Calendars](./time-calendars.md) shows how to work with date-time data and business calendars in these operations.
