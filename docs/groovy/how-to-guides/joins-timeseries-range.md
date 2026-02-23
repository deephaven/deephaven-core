---
title: Inexact, time-series, and range joins
---

This guide covers as-of, reverse-as-of, and range joins in Deephaven. As-of joins are often referred to as time-series joins because they provide a mechanism for joining tables based on time columns, largely with the assumption that the match will often be inexact. All of these joins combine columns from two tables based on either (i) a single inexact key, like an ordered timestamp column, or (ii) one or more exact, relational keys and a single inexact key.

- As-of joins ([`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md)) use inexact matches to join the data by looking for the closest match in the respective join-key column if no exact match exists. Think: "Go grab data from (i) the row in the right table that has a timestamp equal to the timestamp in this row of the left table, or (ii) the best candidate row from the right table with the timestamp closest to this timestamp.
- A [`rangeJoin`](../reference/table-operations/join/rangeJoin.md) de facto finds all of the rows from the right table matching the range criteria. One can imagine a range join performing an `aj()` and an `raj()` at the same time and providing all of the rows in between. This is often a set up for an aggregation: Sum all the records in Column XYZ in the right table between the Time1 and Time2 in this row of the left table.

## Syntax

The syntax for performing an as-of join is as follows:

```groovy syntax
result = leftTable.joinMethod(rightTable, "InexactColumnToMatch")

result = leftTable.joinMethod(rightTable, "ExactColumnsToMatch", "InexactColumnToMatch")

result = leftTable.joinMethod(rightTable, "ExactColumnsToMatch, InexactColumnToMatch", "ColumnsToJoin")
```

When using an as-of join, it's important to remember:

1. Though there can be many exact match columns, the list of join keys _must_ end in a single inexact match column.
2. In the right table, the `InexactColumnToMatch` (e.g. the timestamp join key) must be sorted.

The syntax for performing a range join is as follows:

```groovy syntax
result = leftTable.rangeJoin(rightTable, exactMatches, rangeMatch, aggregations)
```

Where:

- `rightTable` is the table to join with.
- `exactMatches` is a collection of [`JoinMatch`](/core/javadoc/io/deephaven/api/JoinMatch.html) objects that dictate exact-match criteria.
- `rangeMatch` specifies the range match criteria for determining the responsive rows from `rightTable` for each row from the left table.
- `aggregations` are the aggregations to perform over the responsive ranges from `rightTable` for each row from the left table.

> [!NOTE]
> [`rangeJoin`](../reference/table-operations/join/rangeJoin.md) only supports static tables and the [`group`](../reference/table-operations/group-and-aggregate/AggGroup.md) aggregation. `null` and `NaN` values in the right range column are discarded. For all rows that are not discarded, the right table must be sorted according to the right range column for all rows within a group.

The two types of joins have some common parameters:

- `table`: The right table, which is the source of data to be added to the left table.
- `columnsToMatch/exactMatches`: The key column(s) on which to join the two tables.

For [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), the third argument is optional:

- `columnsToAdd`: The column(s) in the right table to join to the left table. If not specified, all columns are joined.

For [`rangeJoin`](../reference/table-operations/join/rangeJoin.md), the third argument is also optional:

- `aggregations`: The aggregation(s) to perform over the responsive ranges from the right table for each row from the left table. If not specified, no aggregations are performed. Currently, only the [`AggGroup`](../reference/table-operations/group-and-aggregate/AggGroup.md) aggregation is supported.

### Multiple match columns

Tables can be joined on more than one match column. This is done by listing all of the key columns within the `columnsToMatch` argument as comma-separated values.

For a join with two different `columnsToMatch`, the format would look like this, with each matching column listed and separated by commas within the quotes containing the second argument:

```groovy syntax
result = leftTable.joinMethod(right_table, "ExactMatchColumn1, ExactMatchColumnN", "InexactMatchColumn")
```

### Match columns with different names

When two tables can be joined, their match column(s) often don't have identical names. The following example joins the left and right tables on `ColumnToMatchLeft` and `ColumnToMatchRight`:

```groovy syntax
result = leftTable.joinMethod(right_table, "ColumnToMatchLeft = ColumnToMatchRight", "ColumnsToJoin")
```

### Rename joined columns

If two tables are joined with matching column names that are _not_ one of the supplied key columns, a name conflict will raise an error. In such a case, [aj](../reference/table-operations/join/aj.md) and [raj](../reference/table-operations/join/raj.md) allow you to rename joined columns. The following example renames `OldColumnName` from the right table to `NewColumnName` as it joins it to and adds it as a column in the left table.

```groovy syntax
result = leftTable.joinMethod(rightTable, "ColumnsToMatch", "NewColumnName = OldColumnName")
```

## As-of (time-series) joins

As-of (time series) joins combine data from a pair of tables - a left and right table - based on one or more match columns. The match columns establish key identifiers in the left table that will be used to find data in the right table. The last key column in the list will provide the contemplated inexact match; all other keys are exact matches. Columns of any data type can be chosen as a key column.

These joins are _inexact_ joins. Instead of looking for a precise match in the right table, the operation looks for "(i) the exact match if it exists, then (ii) if no exact match exists, the best candidate before the exact match" for [`aj`](../reference/table-operations/join/aj.md) (and the opposite for [`raj`](../reference/table-operations/join/raj.md)). These are commonly used in cases where no exact match between key column row values is guaranteed, such as when joining two tables based on the timestamp of events.

The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table. If no matching key exists in the right table, appended row values are `NULL`.

> [!IMPORTANT]
> The right table _must_ be sorted in respect to the column of the last (inexact) join key.

### `aj`

In an as-of join, [`aj`](../reference/table-operations/join/aj.md), row values equal those from the right table where the keys from the left table most closely match the keys from the right table _without going over_. When using [`aj`](../reference/table-operations/join/aj.md), the first `N - 1` match columns are exact, and the final match column is an inexact match. [`aj`](../reference/table-operations/join/aj.md) uses either `>` or `>=` to relate the match column(s):

- `>` will join on inexact matches only.
- `>=` will join on an exact or inexact match. This is the implied relation when no relation is specified (e.g., `on=["ColumnToMatch"]`)

The following example uses [`aj`](../reference/table-operations/join/aj.md) to join a `left` and `right` table. The key columns used are identical (`X` in the `left` table and `Y` in the `right` table). The first resultant table, `result_inexact_exact`, uses `>=` to relate the two key columns. As a result, the resultant table contains _all_ data from `right` appended to `left`. The second resultant table, `resultInexactOnly`, uses `>` to relate the two key columns. As a result, the resultant table has `NULL` values appended to the first row, since the first row of `X` in `left` is not greater than any row of `Y` in `right`.

```groovy order=resultInexactExact,resultInexactOnly,left,right
left = emptyTable(10).update("X = i", "LeftVals = randomInt(1, 100)")
right = emptyTable(10).update("Y = i", "RightVals = randomInt(1, 100)")

resultInexactExact = left.aj(right, "X >= Y")
resultInexactOnly = left.aj(right, "X > Y")
```

The following example uses [`aj`](../reference/table-operations/join/aj.md) to join two tables first on the `Ticker` column (as an exact match), then on `Timestamp`. The operation finds the quote (from a proverbial right table) at the time of a trade event (as recorded with a `Timestamp` in the left table).

```groovy order=result,trades,quotes
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

result = trades.aj(
    quotes,
    "Ticker, Timestamp",
    "Quote_Time = Timestamp, Bid, Ask",
)
```

### `raj`

The reverse as-of join, [`raj`](../reference/table-operations/join/raj.md), is conceptually identical, but instead of seeking a respective row that is "the same or prior to" the left-table's join-value, it seeks the value that is the "the same or just after." Compared to [`aj`](../reference/table-operations/join/aj.md), the syntax and mental model are the same, except, as you'd expect [`raj`](../reference/table-operations/join/raj.md) uses either `<`, `<=`, or `=`:

- `>` will join on inexact matches only.
- `>=` will join on an exact or inexact match. This is the implied relation when no relation is specified (e.g., `on=["ColumnToMatch"]`)

```groovy order=resultInexactExact,resultInexactOnly,left,right
left = emptyTable(10).update("X = i", "LeftVals = randomInt(1, 100)")
right = emptyTable(10).update("Y = i", "RightVals = randomInt(1, 100)")

resultInexactExact = left.raj(right, "X <= Y")
resultInexactOnly = left.raj(right, "X < Y")
```

The following example uses [`raj`](../reference/table-operations/join/raj.md) to join two tables on a `Timestamp` and `Ticker` column. The operation finds the quote immediately after a trade. Quotes are the published prices and sizes at which people are willing to trade a security, while trades are the actual prices and sizes of trades.

```groovy order=result,trades,quotes
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

result = trades.raj(quotes, "Ticker, Timestamp", "Quote_Time = Timestamp, Bid, Ask")
```

## `rangeJoin`

[`rangeJoin`](../reference/table-operations/join/rangeJoin.md) creates a new table containing _all_ of the rows and columns of the left table, plus additional columns containing aggregated data from the right table. It is a join plus an aggregation that:

- Joins arrays of data from the right table onto the left table.
- Aggregates over the joined data.

For columns appended to the left table (joins), cell values equal aggregations over vectors of values from the right table. These vectors are formed from all values in the right table where the right table keys fall within the ranges of keys defined by the left table (responsive ranges).

> [!NOTE]
> Reminders: (i) [`rangeJoin`](../reference/table-operations/join/rangeJoin.md) currently only supports static tables, not yet live, real-time data; and (ii) the only aggregation currently supported is the `group` operation.

The following example joins two tables with [`rangeJoin`](../reference/table-operations/join/rangeJoin.md). The `right` table is joined to `left` on the `Y` column. The **range match expression** specifies that matching rows should contain a value in the `RightValue` column that is greater than the corresponding `LeftStartValue` row and less than the corresponding `LeftEndValue` row. The last argument groups the `result` table's `X` column.

```groovy test-set=1 order=result,left,right
left = emptyTable(20).updateView("X = ii", "LeftStartValue = ii / 0.7", "LeftEndValue = ii / 0.1")
right = emptyTable(20).updateView("X = ii", "RightValue = ii / 0.3", "Y = X % 5")

result = left.rangeJoin(right, List.of("LeftStartValue < RightValue < LeftEndValue"), List.of(AggGroup("Y")))
```

For a detailed explanation of this example, see [`rangeJoin`](../reference/table-operations/join/rangeJoin.md#examples).

Queries often follow up a [`rangeJoin`](../reference/table-operations/join/rangeJoin.md) with an [`update`](../reference/table-operations/select/update.md) or [`updateView`](../reference/table-operations/select/update-view.md) that calls a Groovy closure that operates on the result. The following code block updates the `result` table from the previous example with a [user-defined function](./groovy-closures.md).

```groovy test-set=1 order=resultSummed
sumGroup = { arr ->
    if (!arr) {
        return 0
    } else {
        return arr.sum()
    }
}

resultSummed = result.update("SumY = sumGroup(Y)")
```

The following example uses [`rangeJoin`](../reference/table-operations/join/rangeJoin.md) using date-time columns as range keys. This is the most common use case, since it groups all events that happened in a given time frame. Like the previous example, the resultant grouped column is summed.

```groovy order=resultSummed,result,left,right
left = emptyTable(20).update(
        "StartTime = '2024-01-01T08:00:00 ET' + i * SECOND",
        "EndTime = StartTime + 5 * SECOND",
        "X = ii",
        "Y = X % 5",
)

right = emptyTable(20).update("Timestamp = '2024-01-01T08:00:03 ET' + i * SECOND", "X = ii", "Y = X % 6")

result = left.rangeJoin(right, List.of("StartTime < Timestamp < EndTime"), List.of(AggGroup("Y")))

sumArr = { arr ->
    if (!arr) {
        return 0
    } else {
        return arr.sum()
    }
}

resultSummed = result.update("SumY = sumArr(Y)")
```

## Which method should you use?

Inexact join methods like [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md) are typically used when comparing time series data.

- Use [`aj`](../reference/table-operations/join/aj.md) to find the closest match _before_ or at an event.
- Use [`raj`](../reference/table-operations/join/raj.md) to find the closest match _after_ or at an event.

A [`rangeJoin`](../reference/table-operations/join/rangeJoin.md) currently only supports static tables and the [`group`](../reference/table-operations/group-and-aggregate/AggGroup.md) aggregation.

- Use [`rangeJoin`](../reference/table-operations/join/rangeJoin.md) when data is static, and you want to group data that falls in a range between values in each table.

The following figure presents a flowchart to help choose the right join method for your query.

<Svg src='../assets/conceptual/joins3.svg' style={{height: 'auto', maxWidth: '100%'}} />

## Related documentation

- [Exact and relational joins](./joins-exact-relational.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`raj`](../reference/table-operations/join/raj.md)
- [`rangeJoin`](../reference/table-operations/join/rangeJoin.md)
