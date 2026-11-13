---
title: How do I control what objects are displayed in the console?
---

In the Deephaven IDE, the console doesn't automatically open a panel for a variable whose name starts with an underscore. The variable still exists and you can open it manually.

Consider the following query that creates three named tables:

```groovy order=cryptoTrades,lastTrades,lastTradesWithPriceTimesSize
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
lastTrades = cryptoTrades.lastBy("Instrument")
lastTradesWithPriceTimesSize = lastTrades.updateView("PriceTimesSize = Price * Size")
```

When this query runs, panels for `cryptoTrades`, `lastTrades`, and `lastTradesWithPriceTimesSize` open in the IDE. However, if you do not want to display `lastTrades`, you can add a leading underscore to the table name:

```groovy order=cryptoTrades,lastTradesWithPriceTimesSize
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
_lastTrades = cryptoTrades.lastBy("Instrument")
lastTradesWithPriceTimesSize = _lastTrades.updateView("PriceTimesSize = Price * Size")
```

Variables that a Groovy script assigns without `def` or a type go into the script's binding. The binding is the set of named variables that the console keeps after the script finishes and that the IDE can display. In the following example, `lastTrades` is defined with `def`, so it is a local variable and is not added to the binding.

```groovy order=cryptoTrades,lastTradesWithPriceTimesSize
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
def lastTrades = cryptoTrades.lastBy("Instrument")
lastTradesWithPriceTimesSize = lastTrades.updateView("PriceTimesSize = Price * Size")
```

Declaring the variable with an explicit type, such as `Table`, has the same effect as `def`: the variable is local to the script and isn't added to the binding.

```groovy order=cryptoTrades,lastTradesWithPriceTimesSize
import static io.deephaven.csv.CsvTools.readCsv

cryptoTrades = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
Table lastTrades = cryptoTrades.lastBy("Instrument")
lastTradesWithPriceTimesSize = lastTrades.updateView("PriceTimesSize = Price * Size")
```

In both versions, `lastTrades` is no longer in the binding, but there are still references to that table itself, as it is still feeding the `lastTradesWithPriceTimesSize` table. `lastTrades` falls between `cryptoTrades` and `lastTradesWithPriceTimesSize` in your [query graph](../../conceptual/dag.md), the chain of tables that Deephaven updates in order as data changes. However, generally speaking, this does not improve memory usage or performance, since you will still have a reference to the table.

You can display only the final table in the query by chaining the operations together:

```groovy order=lastTradesWithPriceTimesSize
import static io.deephaven.csv.CsvTools.readCsv

lastTradesWithPriceTimesSize = readCsv("/data/examples/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
    .lastBy("Instrument")
    .updateView("PriceTimesSize = Price * Size")
```

You may have fewer intermediate table objects that are visible to you at the end of the script, but the query still creates the same number of tables and does the same amount of work:

1. Read a CSV into a table,
2. take the last row for each `Instrument`, and
3. add a `PriceTimesSize` column.

## Variables in loops

How many variables you keep in scope matters more than whether they're displayed. In a loop, reusing one variable name lets earlier tables be cleaned up:

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

- [Navigate the GUI](../../how-to-guides/user-interface/navigating-the-ui.md)
- [Does chaining operations together improve performance?](./chained-operations.md)
