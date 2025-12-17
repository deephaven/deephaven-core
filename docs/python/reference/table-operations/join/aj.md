---
title: aj
---

`aj`, as-of join, joins data from a pair of tables - a left and right table - based upon one or more match columns. The match columns establish key identifiers in the left table that will be used to find data in the right table. Any data types can be chosen as keys.

`aj` is an _inexact_ match join. For columns appended to the left table, row values equal those from the right table where the keys from the left table most closely match the keys from the right table _without going over_. If there is no matching key in the right table, appended row values are `NULL`.

`aj` is typically used in cases where no exact match between key column row values is guaranteed. A common use case is joining data on date-time columns to find rows where the time of measurement in the right table is the closest _prior_ to that in the left table.

When using `aj`, the first `N-1` match columns are exactly matched. The last match column is used to find the key values from the right table that are closest to the values in the left table without going over the left value. For example, if the right table contains a value `5` and the left table contains values `4` and `6`, the left table's `6` will be matched on the right table's `5`.

The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table. For columns optionally appended to the left table, row values equal the row values from the right table where the keys from the left table most closely match the keys from the right table, as defined above. If there is no matching key in the right table, appended row values are `NULL`.

## Syntax

```
left.aj(
    table: Table,
    on: Union[str, Sequence[str]],
    joins: Union[str, Sequence[str]] = None,
) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The table data is added from (the right table).

</Param>
<Param name="on" type="Union[str, Sequence[str]]">

Columns from the left and right tables used to join on.

- `["X"]` will join on column `X` from both the left and right table. Equivalent to `"X >= X"`
- `["X > X"]` will join on inexact matches only from column `X` in both the left and right table.
- `["A >= B"]` will join using column `A` from the left table and column `B` from the right table as key columns. Exact matches are joined.
- `["A > B"]` will join using column `A` from the left table and column `B` from the right table as key columns. Exact matches are _not_ joined.
- `["X, A >= B"]` will join on `X` in both tables, as well as on exact and inexact matches from column `A` in the left table and column `B` in the right table.
- `["X, A > B"]` will join on `X` in both tables, as well as on inexact matches only from column `A` in the left table and column `B` in the right table.

The first `N-1` match columns are exactly matched. The last match column is used to find the key values from the right table that are closest to the values in the left table without going over.

</Param>
<Param name="joins" type="Union[str, Sequence[str]]" optional>

Columns from the right table to be added to the left table based on key may be specified in this list:

- `[]` will add all columns from the right table to the left table (default).
- `["X"]` will add column `X` from the right table to the left table as column `X`.
- `["Y = X"]` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
</ParamTable>

## Returns

A new table containing all of the rows and columns of the left table, plus additional columns containing data from the right table. For columns appended to the left table, row values equal the row values from the right table where the keys from the left table most closely match the keys from the right table, as defined above. If there is no matching key in the right table, appended row values are `NULL`.

## Examples

The first two examples join a `left` and `right` table on numeric columns named `X` and `Y`, respectively. Each column contains the integer row index.

In this example, no `join` columns are given, so _all_ columns from the `right` table are appended in `result`. Every row in the `left` table's `X` column has a row in the `right` table where the criteria (`X >= Y`) is met, so the `result` table has _all_ data from `right` appended.

```python order=left,right,result
from deephaven import empty_table

left = empty_table(10).update(["X = i", "LeftVals = randomInt(1, 100)"])
right = empty_table(10).update(["Y = i", "RightVals = randomInt(1, 100)"])

result = left.aj(table=right, on=["X >= Y"])
```

The following example uses `X > Y` instead of `X >= Y` as the `on` parameter. The first row in `X` no longer has a corresponding row in `Y` where the criteria are met. Thus, `NULL` values are appended in the first `row` of `result`.

```python order=left,right,result
from deephaven import empty_table

left = empty_table(10).update(["X = i", "LeftVals = randomInt(1, 100)"])
right = empty_table(10).update(["Y = i", "RightVals = randomInt(1, 100)"])

result = left.aj(table=right, on=["X > Y"])
```

The next two examples look at stock quotes and trades. Quotes are the published prices and sizes at which people are willing to trade a security, while trades are the prices and sizes of actual trades. `aj` is used to find the quote at the time of a trade.

The following example joins all quote columns onto the trade table.

```python order=trades,quotes,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col, datetime_col
from deephaven.time import to_j_instant

trades = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]),
        datetime_col(
            "Timestamp",
            [
                to_j_instant("2021-04-05T09:10:00 ET"),
                to_j_instant("2021-04-05T09:31:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:30:00 ET"),
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
                to_j_instant("2021-04-05T09:11:00 ET"),
                to_j_instant("2021-04-05T09:30:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:30:00 ET"),
                to_j_instant("2021-04-05T17:00:00 ET"),
            ],
        ),
        double_col("Bid", [2.5, 3.4, 97, 102, 108]),
        int_col("BidSize", [10, 20, 5, 13, 23]),
        double_col("Ask", [2.5, 3.4, 105, 110, 111]),
        int_col("AskSize", [83, 33, 47, 15, 5]),
    ]
)

result = trades.aj(table=quotes, on=["Ticker", "Timestamp"])
```

The following example illustrates the exclusion of exact matches by using `>` rather than `>=`. The date-time columns are also named differently, as specified in the `on` input parameter.

```python order=trades,quotes,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col, datetime_col
from deephaven.time import to_j_instant

trades = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]),
        datetime_col(
            "Datetime",
            [
                to_j_instant("2021-04-05T09:10:00 ET"),
                to_j_instant("2021-04-05T09:31:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:30:00 ET"),
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
                to_j_instant("2021-04-05T09:11:00 ET"),
                to_j_instant("2021-04-05T09:30:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:30:00 ET"),
                to_j_instant("2021-04-05T17:00:00 ET"),
            ],
        ),
        double_col("Bid", [2.5, 3.4, 97, 102, 108]),
        int_col("BidSize", [10, 20, 5, 13, 23]),
        double_col("Ask", [2.5, 3.4, 105, 110, 111]),
        int_col("AskSize", [83, 33, 47, 15, 5]),
    ]
)

result = trades.aj(table=quotes, on=["Ticker", "Datetime > Timestamp"])
```

The following example illustrates joining on columns of different names as well as joining a subset of columns, some with renames.

```python order=trades,quotes,result
from deephaven import new_table
from deephaven.table import Table
from deephaven.column import string_col, int_col, double_col, datetime_col
from deephaven.time import to_j_instant

trades = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]),
        datetime_col(
            "TradeTime",
            [
                to_j_instant("2021-04-05T09:10:00 ET"),
                to_j_instant("2021-04-05T09:31:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:30:00 ET"),
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
            "QuoteTime",
            [
                to_j_instant("2021-04-05T09:11:00 ET"),
                to_j_instant("2021-04-05T09:30:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:30:00 ET"),
                to_j_instant("2021-04-05T17:00:00 ET"),
            ],
        ),
        double_col("Bid", [2.5, 3.4, 97, 102, 108]),
        int_col("BidSize", [10, 20, 5, 13, 23]),
        double_col("Ask", [2.5, 3.4, 105, 110, 111]),
        int_col("AskSize", [83, 33, 47, 15, 5]),
    ]
)

result = trades.aj(table=quotes, on=["Ticker"], joins=["Bid", "Offer = Ask"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [Joins: Time-Series and Range](../../../how-to-guides/joins-timeseries-range.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#aj(TABLE,java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.aj)
