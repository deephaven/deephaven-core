---
title: How does the console determine what objects tables and charts to display?
sidebar_label: How do I control what objects are displayed in the console?
---

In Python, you can mark variables, methods, and classes with a leading underscore to indicate that they are **non-public**. In Deephaven, a leading underscore in a table name prevents the table from being displayed in the IDE.

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

When this query runs, tabs for `crypto_trades`, `last_trades`, and `last_trades_with_price_times_size` will appear in the console. However, if you do not want to display `last_trades`, you can add a leading underscore to the table name:

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

`_last_trades` does not appear in the UI, but there are still references to that table itself, as it is still feeding the `last_trades_with_price_times_size` table. `_last_trades` falls between `crypto_trades` and `last_trades_with_price_times_size` in your query graph. However, generally speaking, this does not improve memory usage or performance, since you will still have a reference to the table.

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

1. Get a table,
2. filter it,
3. look at last rows by `Instrument`, and
4. calculate a weighted mid.

However, choosing not to display tables can make a meaningful difference if you are performing an operation in a loop. For example:

```python order=t,result
from deephaven import read_csv

dates = ["2013-01-17", "2013-01-22", "2013-01-29", "2013-02-14"]

for date in dates:
    # Read the initial table from the CSV file
    t = read_csv("/data/examples/TechStockPortfolio/csv/tech_stock_portfolio_slim.csv")

    # Apply an operation to the table, assigning it to `result` - this gets overwritten on all but the last iteration
    result = t.update("X = i")
```

Here, `t` and `result` are in the binding, but because they are replaced on each iteration, the tables created by previous iterations can be cleaned up. However, creating new tables with `globals` prevents this cleanup because a brand new object is being created on every iteration:

```python order=t
from deephaven import read_csv

dates = ["2013-01-17", "2013-01-22", "2013-01-29", "2013-02-14"]

for date in dates:
    # Read the initial table from the CSV file
    t = read_csv("/data/examples/TechStockPortfolio/csv/tech_stock_portfolio_slim.csv")

    # Create a new table for each date
    globals()[f"result_{date.replace('-', '_')}"] = t.update("X = i")
```

## Related documentation

- [Navigating the GUI](../../how-to-guides/user-interface/navigating-the-ui.md)
- [Does chaining operations together improve performance?](./chained-operations.md)
