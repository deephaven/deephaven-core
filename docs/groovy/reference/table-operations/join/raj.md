---
title: raj
---

`raj`, reverse-as-of join, joins data from a pair of tables - a left and right table - based upon one or more match columns (`columnsToMatch`). The match columns establish key identifiers in the left table that will be used to find data in the right table. Any data types can be chosen as keys.

When using `raj`, the first `N-1` match columns are exactly matched. The last match column is used to find the key values from the right table that are closest to the values in the left table without going under the left value. For example, if the right table contains a value `5` and the left table contains values `4` and `6`, the right table's `5` will be matched on the left table's `4`.

The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table. For columns appended to the left table (`columnsToAdd`), row values equal the row values from the right table where the keys from the left table most closely match the keys from the right table, as defined above. If there is no matching key in the right table, appended row values are `NULL`.

## Syntax

```
leftTable.raj(rightTable, columnsToMatch)
leftTable.raj(rightTable, columnsToMatch, columnsToAdd)
```

## Parameters

<ParamTable>
<Param name="rightTable" type="Table">

The table data is added from.

</Param>
<Param name="columnsToMatch" type="String">

Columns from the left and right tables used to join on.

- `"A = B"` will join when column `A` from the left table matches column `B` from the right table.
- `"X"` will join on column `X` from both the left and right table. Equivalent to `"X = X"`.
- `"X, A = B"` will join when column `X` matches from both the left and right tables, and when column `A` from the left table matches column `B` from the right table.

The first `N-1` match columns are exactly matched. The last match column is used to find the key values from the right table that are closest to the values in the left table without going under.

</Param>
<Param name="columnsToAdd" type="String">

The columns from the right table to be added to the left table based on key:

- `NULL` will add all columns from the right table to the left table.
- `"X"` will add column `X` from the right table to the left table as column `X`.
- `"Y = X"` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
</ParamTable>

## Returns

A new table containing all of the rows and columns of the left table plus additional columns containing data from the right table. For columns appended to the left table (`columnsToAdd`), row values equal the row values from the right table where the keys from the left table most closely match the keys from the right table, as defined above. If there is no matching key in the right table, appended row values are `NULL`.

## Examples

These examples look at stock quotes and trades. Quotes are the published prices and sizes people are willing to trade a security at, while trades are the prices and sizes of actual trades. `raj` is used to find the first quote immediately after.

The following example joins all quote columns onto the trade table.

```groovy order=trades,quotes,result
trades = newTable(
    stringCol("Ticker", "AAPL", "AAPL", "AAPL", "IBM", "IBM"),
    instantCol("Timestamp", parseInstant("2021-04-05T09:10:00 ET"), parseInstant("2021-04-05T09:31:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:30:00 ET")),
    doubleCol("Price", 2.5, 3.7, 3.0, 100.50, 110),
    intCol("Size", 52, 14, 73, 11, 6)
)

quotes = newTable(
    stringCol("Ticker", "AAPL", "AAPL", "IBM", "IBM", "IBM"),
    instantCol("Timestamp", parseInstant("2021-04-05T09:11:00 ET"), parseInstant("2021-04-05T09:30:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:30:00 ET"), parseInstant("2021-04-05T17:00:00 ET")),
    doubleCol("Bid", 2.5, 3.4, 97, 102, 108),
    intCol("BidSize", 10, 20, 5, 13, 23),
    doubleCol("Ask", 2.5, 3.4, 105, 110, 111),
    intCol("AskSize", 83, 33, 47, 15, 5),
)

result = trades.raj(quotes, "Ticker, Timestamp")
```

The following example illustrates joining on columns of different names as well as joining a subset of columns, some with renames.

```groovy order=trades,quotes,result
trades = newTable(
    stringCol("Ticker", "AAPL", "AAPL", "AAPL", "IBM", "IBM"),
    instantCol("TradeTime", parseInstant("2021-04-05T09:10:00 ET"), parseInstant("2021-04-05T09:31:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:30:00 ET")),
    doubleCol("Price", 2.5, 3.7, 3.0, 100.50, 110),
    intCol("Size", 52, 14, 73, 11, 6)
)

quotes = newTable(
    stringCol("Ticker", "AAPL", "AAPL", "IBM", "IBM", "IBM"),
    instantCol("QuoteTime", parseInstant("2021-04-05T09:11:00 ET"), parseInstant("2021-04-05T09:30:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:30:00 ET"), parseInstant("2021-04-05T17:00:00 ET")),
    doubleCol("Bid", 2.5, 3.4, 97, 102, 108),
    intCol("BidSize", 10, 20, 5, 13, 23),
    doubleCol("Ask", 2.5, 3.4, 105, 110, 111),
    intCol("AskSize", 83, 33, 47, 15, 5),
)


result = trades.raj(quotes, "Ticker, TradeTime <= QuoteTime", "Bid, Offer = Ask")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose a join method](../../../how-to-guides/joins-exact-relational.md#which-method-should-you-use)
- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#raj(TABLE,java.lang.String))
