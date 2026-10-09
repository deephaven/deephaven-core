---
title: Inexact, time-series, and range joins
---

This guide covers the joins in Deephaven that don't require an exact match on every key: [`aj`](../reference/table-operations/join/aj.md), [`raj`](../reference/table-operations/join/raj.md), and [`rangeJoin`](../reference/table-operations/join/range-join.md). It shows how to use each one and when to choose it.

## Which method should you use?

You typically use the as-of joins, [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), to compare time-series data.

- Use `aj` to find the closest match _before_ or at an event.
- Use `raj` to find the closest match _after_ or at an event.
- Use [`rangeJoin`](../reference/table-operations/join/range-join.md) when your tables are static and you want to group the right-table data that falls in a range defined by each left-table row, such as all events in each left-table time window.

The following flowchart helps you choose among the [exact joins](./joins-exact-relational.md), the as-of joins, and the range joins.

<Svg src='../assets/conceptual/joins3.svg' style={{height: 'auto', maxWidth: '100%'}} />

## As-of (time-series) joins

As-of joins, also called time-series joins, are common when no exact match between key values is guaranteed, such as when you join two tables on event timestamps.

The output table contains all of the rows and columns of the left table, plus additional columns that contain data from the right table. If a left-table row has no match in the right table, the appended columns hold null values in that row.

### As-of join syntax

The syntax for performing an as-of join is as follows, where `joinMethod` is [`aj`](../reference/table-operations/join/aj.md) or [`raj`](../reference/table-operations/join/raj.md):

```groovy syntax
result = leftTable.joinMethod(rightTable, "InexactColumnToMatch")

result = leftTable.joinMethod(rightTable, "ExactColumnsToMatch, InexactColumnToMatch")

result = leftTable.joinMethod(rightTable, "ExactColumnsToMatch, InexactColumnToMatch", "ColumnsToJoin")
```

An as-of join matches on zero or more exact match columns, whose values must be equal, followed by exactly one inexact match column. The inexact match column must have an ordered type, such as a numeric, date-time, or other sortable (`Comparable`) column. The join matches it to the closest value in one direction: `aj` looks at or below the left-table value, and `raj` looks at or above it. The list of match columns _must_ end in that single inexact match column.

As-of joins take the following parameters:

- `rightTable`: The right table, which supplies the data the join adds to the left table.
- `columnsToMatch`: The column(s) on which to join the two tables, as a comma-separated `String`.

The third argument is optional:

- `columnsToAdd`: The column(s) in the right table to join to the left table. If you omit it, the join adds every right-table column except those with the same name as a left-table match column.

#### Match columns with different names

The match columns of two tables often don't have identical names. The following example joins the left and right tables on `ColumnToMatchLeft` and `ColumnToMatchRight`:

```groovy syntax
result = leftTable.joinMethod(rightTable, "ColumnToMatchLeft = ColumnToMatchRight", "ColumnsToJoin")
```

#### Rename joined columns

If you join a right-table column that has the same name as a left-table column, the join raises a name conflict error. This includes a match column such as `Timestamp` when you list it under its own name. In such a case, `aj` and `raj` let you rename joined columns. The following example renames `OldColumnName` from the right table to `NewColumnName` as it adds the column to the left table:

```groovy syntax
result = leftTable.joinMethod(rightTable, "ColumnsToMatch", "NewColumnName = OldColumnName")
```

### `aj`

The as-of join, `aj`, joins each left-table row to the right-table row whose inexact match value is closest to the left-table value _without going over_. `aj` relates the inexact match columns with `>` or `>=`:

- `>` joins on inexact matches only.
- `>=` joins on an exact or inexact match. This is the implied relation when no relation is specified (e.g., `"ColumnToMatch"`).

The following example uses `aj` to join the `left` and `right` tables. The match columns `X` (in `left`) and `Y` (in `right`) contain identical values. The first result table, `resultInexactExact`, uses `>=` to relate the two match columns, so every row of `left` gets the `right` row with the same value. The second result table, `resultInexactOnly`, uses `>`. Its first row has null values in the appended columns because the first value of `X` isn't greater than any value of `Y`.

```groovy order=resultInexactExact,resultInexactOnly,left,right
left = emptyTable(10).update("X = i", "LeftVals = randomInt(1, 100)")
right = emptyTable(10).update("Y = i", "RightVals = randomInt(1, 100)")

resultInexactExact = left.aj(right, "X >= Y")
resultInexactOnly = left.aj(right, "X > Y")
```

The next example uses market data. Quotes are the published prices and sizes at which people are willing to trade a security. Trades record the prices and sizes at which trades actually executed.

The example uses `aj` to join the `quotes` table to the `trades` table, first on the `Ticker` column as an exact match, then on `Timestamp` as the inexact match. For each trade, it finds the most recent quote at or before the trade's `Timestamp`. The `columnsToAdd` argument renames the right table's `Timestamp` column to `QuoteTime`, because the left table already has a `Timestamp` column.

```groovy test-set=1 order=result,trades,quotes
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
        doubleCol("Bid", 2.45, 3.2, 97, 102, 108),
        intCol("BidSize", 10, 20, 5, 13, 23),
        doubleCol("Ask", 2.5, 3.4, 105, 110, 111),
        intCol("AskSize", 83, 33, 47, 15, 5),
)

result = trades.aj(quotes, "Ticker, Timestamp", "QuoteTime = Timestamp, Bid, Ask")
```

### `raj`

The reverse as-of join, `raj`, works like `aj` in the opposite direction. By default, `aj` takes the right-table row with the same or the closest lower value. `raj` takes the right-table row with the same or the closest higher value. The syntax is the same as for `aj`, but `raj` relates the inexact match columns with `<` or `<=`:

- `<` joins on inexact matches only.
- `<=` joins on an exact or inexact match. This is the implied relation when no relation is specified (e.g., `"ColumnToMatch"`).

The following example uses `raj` with `<=` (exact or inexact) and `<` (inexact only). In `resultInexactOnly`, the last row has null values in the appended columns because no value of `Y` is greater than the last value of `X`.

```groovy order=resultInexactExact,resultInexactOnly,left,right
left = emptyTable(10).update("X = i", "LeftVals = randomInt(1, 100)")
right = emptyTable(10).update("Y = i", "RightVals = randomInt(1, 100)")

resultInexactExact = left.raj(right, "X <= Y")
resultInexactOnly = left.raj(right, "X < Y")
```

The following example joins the `trades` and `quotes` tables from the `aj` example with `raj`. For each trade, it finds the first quote at or after the trade's `Timestamp`.

```groovy test-set=1 order=result
result = trades.raj(quotes, "Ticker, Timestamp", "QuoteTime = Timestamp, Bid, Ask")
```

## Range joins

[`rangeJoin`](../reference/table-operations/join/range-join.md) creates a new table containing _all_ of the rows and columns of the left table, plus additional columns containing aggregated data from the right table. It is a join plus an aggregation that:

- Joins arrays of data from the right table onto the left table.
- Aggregates over the joined data.

Each cell in an appended column aggregates the right-table rows that fall in the range its left-table row defines. This set of rows is the row's _responsive range_.

### Range join syntax

The syntax for performing a range join is as follows:

```groovy syntax
result = leftTable.rangeJoin(
    rightTable,
    List.of("ExactColumnsToMatch", "LeftStartColumn < RightRangeColumn < LeftEndColumn"),
    List.of(AggGroup("ColumnsToGroup")),
)
```

The last entry in the second argument, `columnsToMatch`, is a range match expression of the form `LeftStartColumn < RightRangeColumn < LeftEndColumn`.

`rangeJoin` takes the following parameters:

- `rightTable`: The right table, which supplies the data the join adds to the left table.
- `columnsToMatch`: A `Collection<String>` that holds zero or more exact match columns followed by one range match expression.
- `aggregations` (required): The aggregation(s) to perform over each left-table row's responsive range. `rangeJoin` currently supports only the [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md) aggregation.

The [match expressions](../reference/table-operations/join/range-join.md#match-expressions) section of the reference page describes the full range match syntax, including the optional `<-` marker before the expression and `->` marker after it. Each marker requires `<=` on its side of the range. When no right-table value equals the left-table row's start value, `<-` also includes the closest right-table row before the start. When no right-table value equals the end value, `->` also includes the closest right-table row after the end.

> [!NOTE]
> The _right range column_ is the right-table column that the range match compares against. `rangeJoin` has the following restrictions:
>
> - It supports only static tables.
> - It discards right-table rows whose right range column holds `null` or `NaN`.
> - You must sort the remaining right-table rows by the right range column within each set of rows that share the same exact-match key values.

`rangeJoin` also has an overload that takes the exact and range matches as objects instead of strings:

```groovy syntax
result = leftTable.rangeJoin(rightTable, exactMatches, rangeMatch, aggregations)
```

Where:

- `exactMatches` is a collection of [`JoinMatch`](/core/javadoc/io/deephaven/api/JoinMatch.html) objects that dictate exact-match criteria.
- `rangeMatch` is a [`RangeJoinMatch`](/core/javadoc/io/deephaven/api/RangeJoinMatch.html) that specifies the range match criteria.

### Range join examples

The following example joins two tables with `rangeJoin`. It uses only a range match, with no exact-match columns. The range match expression matches each left-table row to the right-table rows whose `RightValue` is greater than that row's `LeftStartValue` and less than its `LeftEndValue`. The `AggGroup` aggregation groups the right table's `Y` values for each left-table row into the `Y` column of `result`.

```groovy test-set=2 order=result,left,right
left = emptyTable(20).updateView("X = ii", "LeftStartValue = ii / 0.7", "LeftEndValue = ii / 0.1")
right = emptyTable(20).updateView("X = ii", "RightValue = ii / 0.3", "Y = X % 5")

result = left.rangeJoin(right, List.of("LeftStartValue < RightValue < LeftEndValue"), List.of(AggGroup("Y")))
```

For a similar example that adds an exact-match column, with a row-by-row explanation of its output, see the [`rangeJoin` reference examples](../reference/table-operations/join/range-join.md#examples).

Queries often follow a `rangeJoin` with an [`update`](../reference/table-operations/select/update.md) or [`updateView`](../reference/table-operations/select/update-view.md) that processes the grouped column. The following code block uses the built-in [`sum`](../reference/query-language/query-library/auto-imported/math.md) function to sum each group in the `result` table from the previous example.

```groovy test-set=2 order=resultSummed
resultSummed = result.update("SumY = sum(Y)")
```

The following example uses `rangeJoin` with date-time columns as the range columns. This is a common use case, since it groups all of the events that happened in each time window. Both tables have a `Y` column, so the `AggGroup` aggregation names its output `RightY` to avoid replacing the left table's `Y`. As in the previous example, the built-in `sum` function then sums each group.

```groovy order=resultSummed,result,left,right
left = emptyTable(20).update(
        "StartTime = '2024-01-01T08:00:00 ET' + i * SECOND",
        "EndTime = StartTime + 5 * SECOND",
        "X = ii",
        "Y = X % 5"
)

right = emptyTable(20).update("Timestamp = '2024-01-01T08:00:03 ET' + i * SECOND", "X = ii", "Y = X % 6")

result = left.rangeJoin(right, List.of("StartTime < Timestamp < EndTime"), List.of(AggGroup("RightY = Y")))

resultSummed = result.update("SumRightY = sum(RightY)")
```

## Related documentation

- [Exact and relational joins](./joins-exact-relational.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`raj`](../reference/table-operations/join/raj.md)
- [`rangeJoin`](../reference/table-operations/join/range-join.md)
