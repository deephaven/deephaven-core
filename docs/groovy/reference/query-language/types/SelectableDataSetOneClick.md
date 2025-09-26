---
title: SelectableDataSetOneClick
---

A `SelectableDataSetOneClick` is used to provide a view of a selectable subset of a table. For example, in some selectable data sets, a GUI click can be used to select a portion of a table.

`SelectableDataSetOneClick` objects are useful for creating [plots](../../../how-to-guides/plotting/api-plotting.md) and figures with Deephaven methods that dynamically update when filters are applied. Dynamic plots can be created with the [`one_click`](../../plot/oneClick.md) method.

## one_click

In this example, we create a new table with [`readCsv`](../../data-import-export/CSV/readCsv.md), and then convert that table into a `SelectableDataSetOneClick` with [`oneClick`](../../plot/oneClick.md).

```groovy order=null
import static io.deephaven.csv.CsvTools.readCsv

source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")
result = oneClick(source, true, "Instrument")
```

![An overlay appears with buttons to add Input Filters or open the Linker Tool](../../../assets/how-to/oc-true.png)

## Methods that use SelectableDataSetOneClick

The following methods take a `SelectableDataSet` as an input:

- [`catHistPlot`](../../plot/catHistPlot.md)
- [`catPlot`](../../plot/catPlot.md)
- [`histPlot`](../../plot/histPlot.md)
- [`ohlcPlot`](../../plot/ohlcPlot.md)
- [`piePlot`](../../plot/piePlot.md)
- [`plot`](../../plot/plot.md)

## Related Documentation

- [How to create plots with the built-in API](../../../how-to-guides/plotting/api-plotting.md)
- [`readCsv`](../../data-import-export/CSV/readCsv.md)
- [`oneClick`](../../plot/oneClick.md)
