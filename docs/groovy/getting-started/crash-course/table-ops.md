---
title: Basic Table Operations
sidebar_label: Table Operations
---

This section will cover some table operations that appear in almost all queries. These table operations use query strings extensively, which are discussed in detail in the [next section](./query-strings.md).

Table operations are integral to the Deephaven Query Language (DQL). The previous sections of the crash course used five:

- [`updateView`](../../reference/table-operations/select/update-view.md), which adds columns to or modifies existing columns in a table.
- [`sumBy`](../../reference/table-operations/group-and-aggregate/sumBy.md), which computes the sum of all columns in a table by a grouping column.
- [`dropColumns`](../../reference/table-operations/select/drop-columns.md), which drops columns from a table.
- [`sort`](../../reference/table-operations/sort/sort.md), which sorts a table by the given columns from least to greatest.
- [`sortDescending`](../../reference/table-operations/sort/sort-descending.md), which sorts a table by the given columns from greatest to least.

Table operations are an integral component of DQL. You've already seen several: [`updateView`](../../reference/table-operations/select/update-view.md), [`sumBy`](../../reference/table-operations/group-and-aggregate/sumBy.md), [`dropColumns`](../../reference/table-operations/select/drop-columns.md), [`sort`](../../reference/table-operations/sort/sort.md) and [`sortDescending`](../../reference/table-operations/sort/sort-descending.md). You can think of these as different transformations being applied to the data in the table. This section will outline some basic table operations that make up the backbones of the most common queries.

Many of the code blocks in this notebook use the following table, `t`, as the root table. This is a simple table with 100 rows and contains only a `Timestamp` column.

```groovy test-set=1
t = emptyTable(100).update("Timestamp = '2015-01-01T00:00:00 ET' + 'PT1m' * ii")
```

## Basic column manipulation

### Add and modify columns

[`update`](../../reference/table-operations/select/update.md) creates _in-memory_ columns. An in-memory column is one where the calculations are performed immediately, and results are stored in memory. The following code block adds three columns to `t`.

- `IntRowIndex`: 32-bit integer row indices.
- `LongRowIndex`: 64-bit integer row indices.
- `Group`: modulo of `IntRowIndex` and 5.

```groovy test-set=1
tUpdated = t.update("IntRowIndex = i", "LongRowIndex = ii", "Group = IntRowIndex % 5")
```

[`updateView`](../../reference/table-operations/select/update-view.md) creates _formula_ columns. A formula column is one where only the formula is stored in memory when called. Results are then computed on the fly as needed.

The following code block adds two new columns to `t`.

- `IntRowIndex`: 32-bit integer row indices.
- `Group`: modulo of `IntRowIndex` and 5.

```groovy test-set=1
tUpdateViewed = t.updateView("IntRowIndex = i", "Group = IntRowIndex % 5")
```

[`lazyUpdate`](../../reference/table-operations/select/lazy-update.md) creates _memoized_ columns. A memoized column performs calculations immediately and stores them in memory, but a calculation is only performed and stored once for each unique set of input values.

The following code block adds two new columns to `tUpdated`.

- `GroupSqrt`: square root of `Group`.
- `GroupSquared`: square of `Group`.

Because `Group` has only five unique values, only 10 calculations are needed to populate the two new columns.

```groovy test-set=1
tLazyUpdated = tUpdated.lazyUpdate("GroupSqrt = sqrt(Group)", "GroupSquared = Group * Group")
```

### Select columns

[`select`](../../reference/table-operations/select/select.md) and [`view`](../../reference/table-operations/select/view.md) both create tables containing subsets of input columns and new columns computed from input columns. The big difference between the methods is how much memory they allocate and when formulas are evaluated. Performance and memory considerations dictate the best method for a particular use case.

[`select`](../../reference/table-operations/select/select.md), like [`update`](../../reference/table-operations/select/update.md), creates _in-memory_ columns. The following code block selects two existing columns from `t_updated` and adds a new column.

```groovy test-set=1
tSelected = tUpdated.select("Timestamp", "IntRowIndex", "RowPlusOne = IntRowIndex + 1")
```

[`view`](../../reference/table-operations/select/view.md), like [`updateView`](../../reference/table-operations/select/update-view.md), creates _formula_ columns. The following code block selects two existing columns from `tUpdated` and adds a new column.

```groovy test-set=1
tViewed = tUpdated.view("Timestamp", "LongRowIndex", "RowPlusOne = LongRowIndex + 1")
```

### Drop columns

[`dropColumns`](../../reference/table-operations/select/drop-columns.md) removes columns from a table.

```groovy test-set=1
tDropped = tUpdated.dropColumns("IntRowIndex", "LongRowIndex")
```

Alternatively, [`view`](../../reference/table-operations/select/view.md) and [`select`](../../reference/table-operations/select/select.md) can be used to remove columns by omission. Both tables created below drop `IntRowIndex` and `LongRowIndex` by not including them in the list of columns passed in.

```groovy test-set=1 order=tDroppedViaView,tDroppedViaSelect
tDroppedViaView = tUpdated.view("Timestamp", "Group")
tDroppedViaSelect = tUpdated.select("Timestamp", "Group")
```

There's a lot to consider when choosing between in-memory, formula, and memoized columns in queries. For more information, see [choose the right selection method](../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method).

## Filter

### By condition

[`where`](../../reference/table-operations/filter/where.md) filters rows from a table based on a condition. The following code block keeps only rows in `tUpdated` where `IntRowIndex` is an even number.

```groovy test-set=1
tFiltered = tUpdated.where("IntRowIndex % 2 == 0")
```

Multiple filters can be applied in a single statement.

```groovy test-set=1
tFiltered2 = tUpdated.where("Group == 3", "IntRowIndex % 2 == 0")
```

A [`where`](../../reference/table-operations/filter/where.md) operation that applies multiple conditions keeps only data that meets _all_ of the criteria set forth. To keep rows that meet _one or more_ of the specified criteria, use [`where`](../../reference/table-operations/filter/where.md) with the `or` operator(`||`).

```groovy test-set=1
tFiltered3 = tUpdated.where("Group == 3 || IntRowIndex % 2 == 0")
```

In addition to [`where`](../../reference/table-operations/filter/where.md), Deephaven offers [`whereIn`](../../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../../reference/table-operations/filter/where-not-in.md), which filter table data based on another table.

See the [filtering guide](../../how-to-guides/filters.md) for more information.

### By row position

Methods that filter data by row position only keep a portion of data at the top, middle, or bottom of a table.

- [`head`](../../reference/table-operations/filter/head.md): Keep the first `n` rows.
- [`headPct`](../../reference/table-operations/filter/head-pct.md): Keep the first `n%` of rows.
- [`tail`](../../reference/table-operations/filter/tail.md): Keep the last `n` rows.
- [`tailPct`](../../reference/table-operations/filter/tail-pct.md): Keep the last `n%` of rows.
- [`slice`](../../reference/table-operations/filter/slice.md): Keep rows between `start` and `end`.
- [`slicePct`](../../reference/table-operations/filter/slice-pct.md): Keep rows between `start%` and `end%`.

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

See the [filtering guide](../../how-to-guides/filters.md) for more information.

## Sort

The [`sort`](../../reference/table-operations/sort/sort.md) method sorts a table based on one or more columns. The following code block sorts `tStatic` by `X` in ascending order.

```groovy test-set=1
tSorted = tUpdated.sort("Group")
```

Tables can be sorted by more than one column.

```groovy test-set=1
tSortedMultiple = tUpdated.sort("Group", "IntRowIndex")
```

To sort in descending order, use [`sortDescending`](../../reference/table-operations/sort/sort-descending.md).

```groovy test-set=1
tSortedDesc = tUpdated.sortDescending("LongRowIndex")
```

To sort multiple columns in different directions, use [`sort`](../../reference/table-operations/sort/sort.md) with [`SortColumn`](/core/javadoc/io/deephaven/api/SortColumn.html)'s `asc` and `desc` methods.

```groovy test-set=1
sortColumns = [
    SortColumn.asc(ColumnName.of("Group")),
    SortColumn.desc(ColumnName.of("IntRowIndex"))
]

tSortMulti = tUpdated.sort(sortColumns)
```

See the [sorting guide](../../how-to-guides/sort.md) for more information.

## Group and aggregate data

Grouping data places rows into groups based on zero or more supplied key columns. Aggregation calculates summary statistics over a group of data. Grouping and aggregation are key components of data analysis, especially in Deephaven queries.

The examples in this section will use the following table.

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

[`groupBy`](../../reference/table-operations/group-and-aggregate/groupBy.md) groups table data into [arrays](../../how-to-guides/work-with-arrays.md). Entire tables can be grouped.

```groovy test-set=2
tGrouped = t.groupBy()
```

Data can be grouped by one or more key columns.

```groovy test-set=2 order=tGroupedByGroup,tGroupedByMultiple
tGroupedByGroup = t.groupBy("Group")
tGroupedByMultiple = t.groupBy("Group", "Letter")
```

[`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md) is the inverse of [`groupBy`](../../reference/table-operations/group-and-aggregate/groupBy.md).

```groovy test-set=2 order=tUngrouped,tUngrouped2,tUngrouped3
tUngrouped = tGrouped.ungroup()
tUngrouped2 = tGroupedByGroup.ungroup()
tUngrouped3 = tGroupedByMultiple.ungroup()
```

See the [grouping and ungrouping guide](../../how-to-guides/grouping-data.md) for more information.

### Single aggregations

[Single aggregations](../../how-to-guides/dedicated-aggregations.md) apply a single aggregation to an entire table. See [here](../../how-to-guides/dedicated-aggregations.md#single-aggregators) for a list of single aggregators.

The following code uses [`avgBy`](../../reference/table-operations/group-and-aggregate/avgBy.md) to calculate the aggregated average of columns `X` and `Y` from the table `t`. No grouping columns are given, so the averages are calculated over the entire table.

```groovy test-set=2
tAvg = t.view("X", "Y").avgBy()
```

Aggregations are often calculated for groups of data. The following example calculates the average of `X` and `Y` for each unique value in `Group`.

```groovy test-set=2
tAvgByGroup = t.view("Group", "X", "Y").avgBy("Group")
```

Single aggregations can be performed using multiple grouping columns.

```groovy test-set=2
tAvgByMultiple = t.view("Group", "Letter", "X", "Y").avgBy("Group", "Letter")
```

### Multiple aggregations

To apply multiple aggregations in a single operation, pass one or more of the [aggregators](../../how-to-guides/combined-aggregations.md#what-aggregations-are-available) into [`aggBy`](../../reference/table-operations/group-and-aggregate/aggBy.md).

The following code block calculates the average of `X` and the median of `Y`, grouped by `Group` and `Letter`. It renames the resultant columns `AvgX` and `MedianY`, respectively.

```groovy test-set=2
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggMed

aggList = [AggAvg("AvgX = X"), AggMed("MedianY = Y")]

tMultipleAggs = t.view("Group", "Letter", "X", "Y").aggBy(
    aggList, "Group", "Letter")
```

### Rolling aggregations

Most platforms offer aggregation functionality similar to the dedicated and multiple aggregations presented above (though none will work so easily on real-time data). However, Deephaven is unique and powerful in its vast library of cumulative, moving, and windowed calculations, facilitated by the [`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`io.deephaven.api.updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) module.

The following code block calculates the cumulative sum of `X` in `t`.

```groovy test-set=2
tCumSum = t.view("X").updateBy(CumSum("SumX = X"))
```

Aggregations with [`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md) show the running total as it progresses through the table.

[`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md) can also limit these summary statistics to subsets of table data defined by a number of rows or amount of time backward, forward, or both. The following code block calculates the sum of the prior 10 rows in column `X` of table `t`.

```groovy test-set=2
tWindowedSum = t.view("X").updateBy(RollingSum(10, "TenRowSumX = X"))
```

[`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md) also supports performing these calculations over groups of data. The following code block performs the same calculations as above, but groups the data by `Group` and `Letter`.

```groovy test-set=2
updateByOps = [RollingSum(10, "TenRowSumX = X"), CumSum("SumX = X")]

tUpdatedByGrouped = t.updateBy(updateByOps, "Group", "Letter")
```

Additionally, calculations can be windowed by time. The following code block calculates a 16-second rolling average of `X`, grouped by `Group`.

```groovy test-set=2
tRollingAvgTime = t.updateBy(RollingAvg("Timestamp", parseDuration("PT16s"), "AvgX = X"), "Group")
```

Windows can look backward, forward, or both ways. The following example calculates the rolling average of the following windows:

- The previous 9 seconds.
- The current row and the previous 8 rows.
- The current row, the previous 10 rows, and the next 10 rows.
- The next 8 rows.

```groovy test-set=2
updateByOps = [
    RollingAvg("Timestamp", parseDuration("PT9s"), "BackwardTimeAvgX = X"),
    RollingAvg(9, "BackwardRowAvgX = X"),
    RollingAvg(11, 10, "CenteredRowAvgX = X"),
    RollingAvg(0, 8, "ForwardRowAvgX = X"),
]

tWindowed = t.updateBy(updateByOps)
```

> **_NOTE:_** A backward-looking window counts the current row as the first row backward. A forward-looking window counts the row ahead of the current row as the first row forward.

See the [`updateBy` user guide](../../how-to-guides/rolling-aggregations.md) to learn more.

## Combine tables

There are two different ways to combine tables in Deephaven: merging and joining. Merging tables can be visualized as a vertical stacking of tables, whereas joining is more horizontal in nature, appending rows from one table to another based on common columns.

Each subsection below defines its own tables to demonstrate merging and joining tables in Deephaven.

### Merge tables

[`merge`](../../how-to-guides/merge-tables.md) combines an arbitrary number of tables provided they all have the same schema.

```groovy test-set=3 order=tMerged,t1,t2,t3
t1 = emptyTable(10).update("Table = 1", "X = randomDouble(0, 10)", "Y = randomBool()")
t2 = emptyTable(6).update("Table = 2", "X = 1.1 * i", "Y = randomBool()")
t3 = emptyTable(3).update("Table = 3", "X = sin(0.1 * i)", "Y = true")

tMerged = merge(t1, t2, t3)
```

### Join tables

Joining tables combines two tables based on one or more key columns. The key column(s) define data that is commonly shared between the two tables. The table on which the join operation is called is the left table, and the table passed as an argument is the right table.

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

The tables `t1` and `t2` have the common column `Letter`. Moreover, `Letter` contains matching values in both tables. So, these tables can be joined on the `Letter` column.

```groovy test-set=4
tJoined = t1.naturalJoin(t2, "Letter")
```

Joins can use more than one key column. The tables `t2` and `t3` have both the `Letter` and `Color` columns in common, and they both have matching values. The following code block joins the two tables on both columns.

```groovy test-set=4
tJoined2 = t2.naturalJoin(t3, "Letter, Color")
```

By default, every join operation in Deephaven appends _all_ columns from the right table onto the left table. An optional third argument can be used to specify which columns to append. The following code block joins `t2` and `t3` on the `Letter` column, but only appends the `Value` column from `t3`.

```groovy test-set=4
tJoinedSubset = t2.naturalJoin(t3, "Letter", "Value")
```

`t2` and `t3` share the `Color` column, so any attempt to append that onto `t2` results in a name conflict error. This can be avoided by either [renaming the column](../../reference/table-operations/select/rename-columns.md), or by using the `joins` argument to specify which columns to append.

The following example renames `Color` in t3 to `Color2` when joining the tables.

```groovy test-set=4
tJoinedRename = t2.naturalJoin(t3, "Letter", "Color2 = Color, Value")
```

Join operations in Deephaven come in two distinct flavors: [exact and relational joins](../../how-to-guides/joins-exact-relational.md) and [time series and range joins](../../how-to-guides/joins-timeseries-range.md).

### Exact and relational joins

[Exact and relational joins](../../how-to-guides/joins-exact-relational.md) combine data from two tables based on exact matches in one or more related key columns.

#### Exact joins

Exact joins keep all rows from a left table, and append columns from a right table onto the left table.

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

`tLeft1` and `tRight1` have a column of the same name and data type called `Color`. In `tRight1`, `Color` has no duplicates. Additionally, all colors in `tLeft1` have an exact match in `tRight1`. In cases like this, [`exactJoin`](../../reference/table-operations/join/exact-join.md) is the most appropriate join operation.

```groovy test-set=5
tExactJoined = tLeft1.exactJoin(tRight1, "Color")
```

Consider the following tables, which are similar to the previous example. However, in this case, `tLeft2` contains the color `Purple`, which is not in `tRight2`.

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

In this case, an [`exactJoin`](../../reference/table-operations/join/exact-join.md) will fail. Instead, use [`naturalJoin`](../../reference/table-operations/join/natural-join.md), which appends a null value where no match exists.

```groovy test-set=6
tNaturalJoined = tLeft2.naturalJoin(tRight2, "Color")
```

#### Relational joins

Relational joins are similar to SQL joins.

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

[`join`](../../reference/table-operations/join/join.md) includes only rows where key columns in both tables contain an exact match, including multiple exact matches.

```groovy test-set=7
tJoined = tLeft3.join(tRight3, "Color")
```

[`leftOuterJoin`](../../reference/table-operations/join/left-outer-join.md) includes all rows from the left table as well as rows from the right table where an exact match exists. Null values are inserted where no match exists.

```groovy test-set=7
import io.deephaven.engine.util.OuterJoinTools

tLeftOuterJoined = OuterJoinTools.leftOuterJoin(tLeft3, tRight3, "Color")
```

[`fullOuterJoin`](../../reference/table-operations/join/full-outer-join.md) includes all rows from both tables, regardless of whether an exact match exists. Null values are inserted where no match exists.

```groovy test-set=7
import io.deephaven.engine.util.OuterJoinTools

tFullOuterJoined = OuterJoinTools.fullOuterJoin(tLeft3, tRight3, "Color")
```

#### Time-series (inexact) joins

Time-series (inexact) joins are joins where the key column(s) used to join the tables may not match exactly. Instead, the closest value is used to match the data when no exact match exists.

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

[`aj`](../../reference/table-operations/join/aj.md) joins row values in the left table with the closest in the right table _without going over_. To see the quote at the time of a trade, use [`aj`](../../reference/table-operations/join/aj.md).

```groovy test-set=8
resultAj = trades.aj(quotes, "Ticker, Timestamp")
```

[`raj`](../../reference/table-operations/join/raj.md) joins row values in the left table with the closest in the right table _without going under_. To see the first quote that comes after a trade, use [`raj`](../../reference/table-operations/join/raj.md).

```groovy test-set=8
resultRaj = trades.raj(quotes, "Ticker, Timestamp")
```

### More about joins

Every join operation presented in this notebook works on real-time data. Don't believe us? Try it for yourself! For more information about joins, see:

- [Joins: Exact and Relational](../../how-to-guides/joins-exact-relational.md)
- [Joins: Time-Series and Range](../../how-to-guides/joins-timeseries-range.md)
