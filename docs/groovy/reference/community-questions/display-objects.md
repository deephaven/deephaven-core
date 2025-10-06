---
title: How does the console determine what objects tables and charts to display?
sidebar_label: How do I control what objects are displayed in the console?
---

In Groovy, you can often mark variables, methods, and classes with a leading underscore to indicate that they are for internal use only. In Deephaven, a leading underscore in a table name prevents the table from being displayed in the IDE.

Consider the following query that creates three named tables:

```groovy order=cryptoTrades,lastTrades,lastTradesWithPriceTimesSize
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
lastTrades = cryptoTrades.lastBy("Instrument")
lastTradesWithPriceTimesSize = lastTrades.updateView("PriceTimesSize = Price * Size")
```

When this query runs, tabs for `cryptoTrades`, `lastTrades`, and `lastTradesWithPriceTimesSize` will appear in the console. However, if you do not want to display `lastTrades`, you can add a leading underscore to the table name:

```groovy order=cryptoTrades,lastTradesWithPriceTimesSize
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
_lastTrades = cryptoTrades.lastBy("Instrument")
lastTradesWithPriceTimesSize = _lastTrades.updateView("PriceTimesSize = Price * Size")
```

Using `def` in Groovy allows you to create variables that are only accessible within the current scope, keeping your workspace cleaner by hiding intermediate results. In the following example, `lastTrades` is defined with `def`, which means it is a local variable and not added to the global binding. As a result, it will not appear in the console when the script finishes.

```groovy order=cryptoTrades,lastTradesWithPriceTimesSize
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
def lastTrades = cryptoTrades.lastBy("Instrument")
lastTradesWithPriceTimesSize = lastTrades.updateView("PriceTimesSize = Price * Size")
```

Here is a third version of the query:

```groovy skip-test
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
Table lastTrades = cryptoTrades.lastBy("Instrument")
lastTradesWithPriceTimesSize = lastTrades.updateView("PriceTimesSize = Price * Size")
```

`lastTrades` is no longer in the binding, but there are still references to that table itself, as it is still feeding the `lastTradesWithPriceTimesSize` table. `lastTrades` falls between `cryptoTrades` and `lastTradesWithPriceTimesSize` in your query graph. However, generally speaking, this does not improve memory usage or performance, since you will still have a reference to the table.

You can display only the final table in the query by chaining the operations together:

```groovy order=lastTradesWithPriceTimesSize
lastTradesWithPriceTimesSize = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv").lastBy("Instrument").updateView("PriceTimesSize = Price * Size")
```

You may have fewer intermediate table objects that are visible to you at the end of the script, but the query still creates the same number of tables and does the same amount of work:

1. get a table,
2. filter it,
3. look at last rows by `Instrument`, and
4. calculate a weighted mid.

However, choosing not to display tables can make a meaningful difference if you are performing an operation in a loop. For example:

```groovy order=t,result
import static io.deephaven.csv.CsvTools.readCsv

dates = ["2013-01-17", "2013-01-22", "2013-01-29", "2013-02-14"]

for(String date : dates) {
    t = readCsv("/data/examples/TechStockPortfolio/csv/tech_stock_portfolio_slim.csv")

    // Apply some operation to the table, assigning it to the `result` variable - this gets overwritten on all but the last iteration
    result = t.update("X = i")
}
```

Here, `t` and `result` are in the binding, but because they are replaced on each iteration, the tables created by previous iterations can be cleaned up. However, putting the results for each individual date in the binding with `publishVariable` prevents this cleanup because a brand new variable is being added to the binding on every iteration:

```groovy order=t,result_2013_01_17,result_2013_01_22,result_2013_01_29,result_2013_02_14
import static io.deephaven.csv.CsvTools.readCsv

dates = ["2013-01-17", "2013-01-22", "2013-01-29", "2013-02-14"]

for(String date : dates) {
    // Read the initial table from the CSV file
    t = readCsv("/data/examples/TechStockPortfolio/csv/tech_stock_portfolio_slim.csv")

    // Create a new table for each date
    publishVariable("result_${date.replaceAll("-", "_")}", t.update("X = i"))
}
```

## Related documentation

- [Navigating the GUI](../../how-to-guides/user-interface/navigating-the-ui.md)
- [Does chaining operations together improve performance?](./chained-operations.md)
