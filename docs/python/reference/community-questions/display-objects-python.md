---
title: How do I control what objects are displayed in the console?
---

In Python, you can mark variables, methods, and classes with a leading underscore to indicate that they are **non-public**. In the Deephaven IDE, the console doesn't automatically open a panel for a variable whose name starts with an underscore. The variable still exists and you can open it manually.

Consider the following query that creates three named tables:

```python order=crypto_trades,last_trades,last_trades_with_price_times_size
from deephaven import read_csv

crypto_trades = read_csv(
    "/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
last_trades = crypto_trades.last_by("Instrument")
last_trades_with_price_times_size = last_trades.update_view(
    "PriceTimesSize = Price * Size"
)
```

When this query runs, panels for `crypto_trades`, `last_trades`, and `last_trades_with_price_times_size` open in the IDE. However, if you do not want to display `last_trades`, you can add a leading underscore to the table name:

```python order=crypto_trades,last_trades_with_price_times_size
from deephaven import read_csv

crypto_trades = read_csv(
    "/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
_last_trades = crypto_trades.last_by("Instrument")
last_trades_with_price_times_size = _last_trades.update_view(
    "PriceTimesSize = Price * Size"
)
```

`_last_trades` does not open automatically in the UI, but there are still references to that table itself, as it is still feeding the `last_trades_with_price_times_size` table. `_last_trades` falls between `crypto_trades` and `last_trades_with_price_times_size` in your [query graph](../../conceptual/dag.md), the chain of tables that Deephaven updates in order as data changes. However, generally speaking, this does not improve memory usage or performance, since you will still have a reference to the table.

You can display only the final table in the query by chaining the operations together:

```python order=last_trades_with_price_times_size
from deephaven import read_csv

last_trades_with_price_times_size = (
    read_csv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
    .last_by("Instrument")
    .update_view("PriceTimesSize = Price * Size")
)
```

You may have fewer intermediate table objects that are visible to you at the end of the script, but the query still creates the same number of tables and does the same amount of work:

1. Read a CSV into a table,
2. take the last row for each `Instrument`, and
3. add a `PriceTimesSize` column.

## Variables in loops

How many variables you keep in scope matters more than whether they're displayed. In a loop, reusing one variable name lets earlier tables be cleaned up:

```python order=t,result
from deephaven import read_csv

dates = ["2013-01-17", "2013-01-22", "2013-01-29", "2013-02-14"]

for date in dates:
    # Read the initial table from the CSV file
    t = read_csv("/data/examples/TechStockPortfolio/csv/tech_stock_portfolio_slim.csv")

    # Apply an operation to the table, assigning it to `result` - this gets overwritten on all but the last iteration
    result = t.update("X = i")
```

Here, `t` and `result` are global variables, but because they are replaced on each iteration, the tables created by previous iterations can be cleaned up. However, adding a new variable to `globals` on each iteration prevents this cleanup, because every table stays referenced by its own global name:

```python order=t,result_2013_01_17,result_2013_01_22,result_2013_01_29,result_2013_02_14
from deephaven import read_csv

dates = ["2013-01-17", "2013-01-22", "2013-01-29", "2013-02-14"]

for date in dates:
    # Read the initial table from the CSV file
    t = read_csv("/data/examples/TechStockPortfolio/csv/tech_stock_portfolio_slim.csv")

    # Create a new table for each date
    globals()[f"result_{date.replace('-', '_')}"] = t.update("X = i")
```

## Related documentation

- [Navigate the GUI](../../how-to-guides/user-interface/navigating-the-ui.md)
- [Does chaining operations together improve performance?](./chained-operations.md)
