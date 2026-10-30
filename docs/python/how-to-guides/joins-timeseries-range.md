---
title: Inexact, time-series, and range joins
---

This guide covers the joins in Deephaven that don't require an exact match on every key: [`aj`](../reference/table-operations/join/aj.md), [`raj`](../reference/table-operations/join/raj.md), and [`range_join`](../reference/table-operations/join/range-join.md). It shows how to use each one and when to choose it.

## Which method should you use?

You typically use the as-of joins, [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), to compare time-series data.

- Use `aj` to find the closest match _before_ or at an event.
- Use `raj` to find the closest match _after_ or at an event.
- Use [`range_join`](../reference/table-operations/join/range-join.md) when your tables are static and you want to group the right-table data that falls in a range defined by each left-table row, such as all events in each left-table time window.

The following flowchart helps you choose among the [exact joins](./joins-exact-relational.md), the as-of joins, and the range joins.

<Svg src='../assets/conceptual/joins3.svg' style={{height: 'auto', maxWidth: '100%'}} />

## As-of (time-series) joins

As-of joins, also called time-series joins, are common when no exact match between key values is guaranteed, such as when you join two tables on event timestamps.

The output table contains all of the rows and columns of the left table, plus additional columns that contain data from the right table. If a left-table row has no match in the right table, the appended columns hold null values in that row.

### As-of join syntax

The syntax for performing an as-of join is as follows, where `join_method` is [`aj`](../reference/table-operations/join/aj.md) or [`raj`](../reference/table-operations/join/raj.md):

```python syntax
result = left_table.join_method(table=right_table, on=["InexactColumnToMatch"])

result = left_table.join_method(
    table=right_table, on=["ExactColumnsToMatch", "InexactColumnToMatch"]
)

result = left_table.join_method(
    table=right_table,
    on=["ExactColumnsToMatch", "InexactColumnToMatch"],
    joins=["ColumnsToJoin"],
)
```

An as-of join matches on zero or more exact match columns, whose values must be equal, followed by exactly one inexact match column. The inexact match column must have an ordered type, such as a numeric, date-time, or other sortable (Java `Comparable`) column. The join matches it to the closest value in one direction: `aj` looks at or below the left-table value, and `raj` looks at or above it. The list of match columns _must_ end in that single inexact match column.

As-of joins take the following parameters:

- `table`: The right table, which supplies the data the join adds to the left table.
- `on`: The column(s) on which to join the two tables.

The third argument is optional:

- `joins`: The column(s) in the right table to join to the left table. If you omit it, the join adds every right-table column except those with the same name as a left-table match column.

#### Match columns with different names

The match columns of two tables often don't have identical names. The following example joins the left and right tables on `ColumnToMatchLeft` and `ColumnToMatchRight`:

```python syntax
result = left_table.join_method(
    table=right_table,
    on=["ColumnToMatchLeft = ColumnToMatchRight"],
    joins=["ColumnsToJoin"],
)
```

#### Rename joined columns

If you join a right-table column that has the same name as a left-table column, the join raises a name conflict error. This includes a match column such as `Timestamp` when you list it under its own name. In such a case, `aj` and `raj` let you rename joined columns. The following example renames `OldColumnName` from the right table to `NewColumnName` as it adds the column to the left table:

```python syntax
result = left_table.join_method(
    table=right_table, on=["ColumnsToMatch"], joins=["NewColumnName = OldColumnName"]
)
```

### `aj`

The as-of join, `aj`, joins each left-table row to the right-table row whose inexact match value is closest to the left-table value _without going over_. `aj` relates the inexact match columns with `>` or `>=`:

- `>` joins on inexact matches only.
- `>=` joins on an exact or inexact match. This is the implied relation when no relation is specified (e.g., `on=["ColumnToMatch"]`).

The following example uses `aj` to join the `left` and `right` tables. The match columns `X` (in `left`) and `Y` (in `right`) contain identical values. The first result table, `result_inexact_exact`, uses `>=` to relate the two match columns, so every row of `left` gets the `right` row with the same value. The second result table, `result_inexact_only`, uses `>`. Its first row has null values in the appended columns because the first value of `X` isn't greater than any value of `Y`.

```python order=result_inexact_exact,result_inexact_only,left,right
from deephaven import empty_table

left = empty_table(10).update(["X = i", "LeftVals = randomInt(1, 100)"])
right = empty_table(10).update(["Y = i", "RightVals = randomInt(1, 100)"])

result_inexact_exact = left.aj(table=right, on=["X >= Y"])
result_inexact_only = left.aj(table=right, on=["X > Y"])
```

The next example uses market data. Quotes are the published prices and sizes at which people are willing to trade a security. Trades record the prices and sizes at which trades actually executed.

The example uses `aj` to join the `quotes` table to the `trades` table, first on the `Ticker` column as an exact match, then on `Timestamp` as the inexact match. For each trade, it finds the most recent quote at or before the trade's `Timestamp`. The `joins` argument renames the right table's `Timestamp` column to `QuoteTime`, because the left table already has a `Timestamp` column.

```python test-set=1 order=result,trades,quotes
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
        double_col("Bid", [2.45, 3.2, 97, 102, 108]),
        int_col("BidSize", [10, 20, 5, 13, 23]),
        double_col("Ask", [2.5, 3.4, 105, 110, 111]),
        int_col("AskSize", [83, 33, 47, 15, 5]),
    ]
)

result = trades.aj(
    table=quotes,
    on=["Ticker", "Timestamp"],
    joins=["QuoteTime = Timestamp", "Bid", "Ask"],
)
```

### `raj`

The reverse as-of join, `raj`, works like `aj` in the opposite direction. By default, `aj` takes the right-table row with the same or the closest lower value. `raj` takes the right-table row with the same or the closest higher value. The syntax is the same as for `aj`, but `raj` relates the inexact match columns with `<` or `<=`:

- `<` joins on inexact matches only.
- `<=` joins on an exact or inexact match. This is the implied relation when no relation is specified (e.g., `on=["ColumnToMatch"]`).

The following example uses `raj` with `<=` (exact or inexact) and `<` (inexact only). In `result_inexact_only`, the last row has null values in the appended columns because no value of `Y` is greater than the last value of `X`.

```python order=result_inexact_exact,result_inexact_only,left,right
from deephaven import empty_table

left = empty_table(10).update(["X = i", "LeftVals = randomInt(1, 100)"])
right = empty_table(10).update(["Y = i", "RightVals = randomInt(1, 100)"])

result_inexact_exact = left.raj(table=right, on=["X <= Y"])
result_inexact_only = left.raj(table=right, on=["X < Y"])
```

The following example joins the `trades` and `quotes` tables from the `aj` example with `raj`. For each trade, it finds the first quote at or after the trade's `Timestamp`.

```python test-set=1 order=result
result = trades.raj(
    table=quotes,
    on=["Ticker", "Timestamp"],
    joins=["QuoteTime = Timestamp", "Bid", "Ask"],
)
```

## Range joins

[`range_join`](../reference/table-operations/join/range-join.md) creates a new table containing _all_ of the rows and columns of the left table, plus additional columns containing aggregated data from the right table. It is a join plus an aggregation that:

- Joins arrays of data from the right table onto the left table.
- Aggregates over the joined data.

Each cell in an appended column aggregates the right-table rows that fall in the range its left-table row defines. This set of rows is the row's _responsive range_.

### Range join syntax

The syntax for performing a range join is as follows:

```python syntax
result = left_table.range_join(
    table=right_table,
    on=["ExactColumnsToMatch", "LeftStartColumn < RightRangeColumn < LeftEndColumn"],
    aggs=[group("ColumnsToGroup")],
)
```

The last entry in `on` is a range match expression of the form `LeftStartColumn < RightRangeColumn < LeftEndColumn`.

`range_join` takes the following parameters:

- `table`: The right table, which supplies the data the join adds to the left table.
- `on`: Zero or more exact match columns followed by one range match expression.
- `aggs` (required): The aggregation(s) to perform over each left-table row's responsive range. `range_join` currently supports only the [`group`](../reference/table-operations/group-and-aggregate/AggGroup.md) aggregation.

The [match expressions](../reference/table-operations/join/range-join.md#match-expressions) section of the reference page describes the full range match syntax, including the optional `<-` marker before the expression and `->` marker after it. Each marker requires `<=` on its side of the range. When no right-table value equals the left-table row's start value, `<-` also includes the closest right-table row before the start. When no right-table value equals the end value, `->` also includes the closest right-table row after the end.

> [!NOTE]
> The _right range column_ is the right-table column that the range match compares against. `range_join` has the following restrictions:
>
> - It supports only static tables.
> - It discards right-table rows whose right range column holds `null` or `NaN`.
> - You must sort the remaining right-table rows by the right range column within each set of rows that share the same exact-match key values.

### Range join examples

The following example joins two tables with `range_join`. It uses only a range match, with no exact-match columns. The range match expression matches each left-table row to the right-table rows whose `RightValue` is greater than that row's `LeftStartValue` and less than its `LeftEndValue`. The `group` aggregation groups the right table's `Y` values for each left-table row into the `Y` column of `result`.

```python test-set=2 order=result,left,right
from deephaven import empty_table
from deephaven.agg import group

left = empty_table(20).update_view(
    ["X = ii", "LeftStartValue = ii / 0.7", "LeftEndValue = ii / 0.1"]
)
right = empty_table(20).update_view(["X = ii", "RightValue = ii / 0.3", "Y = X % 5"])

result = left.range_join(
    table=right, on=["LeftStartValue < RightValue < LeftEndValue"], aggs=group("Y")
)
```

For a similar example that adds an exact-match column, with a row-by-row explanation of its output, see the [`range_join` reference examples](../reference/table-operations/join/range-join.md#examples).

Queries often follow a `range_join` with an [`update`](../reference/table-operations/select/update.md) or [`update_view`](../reference/table-operations/select/update-view.md) that processes the grouped column. The following code block uses the built-in [`sum`](../reference/query-language/query-library/auto-imported/math.md) function to sum each group in the `result` table from the previous example.

```python test-set=2 order=result_summed
result_summed = result.update(["SumY = sum(Y)"])
```

The following example uses `range_join` with date-time columns as the range columns. This is a common use case, since it groups all of the events that happened in each time window. Both tables have a `Y` column, so the `group` aggregation names its output `RightY` to avoid replacing the left table's `Y`. As in the previous example, the built-in `sum` function then sums each group.

```python order=result_summed,result,left,right
from deephaven.agg import group
from deephaven import empty_table

left = empty_table(20).update(
    [
        "StartTime = '2024-01-01T08:00:00 ET' + i * SECOND",
        "EndTime = StartTime + 5 * SECOND",
        "X = ii",
        "Y = X % 5",
    ]
)

right = empty_table(20).update(
    ["Timestamp = '2024-01-01T08:00:03 ET' + i * SECOND", "X = ii", "Y = X % 6"]
)

result = left.range_join(
    table=right, on=["StartTime < Timestamp < EndTime"], aggs=group("RightY = Y")
)

result_summed = result.update(["SumRightY = sum(RightY)"])
```

## Related documentation

- [Exact and relational joins](./joins-exact-relational.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`raj`](../reference/table-operations/join/raj.md)
- [`range_join`](../reference/table-operations/join/range-join.md)
