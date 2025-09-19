---
title: ohlcPlotBy
---

The `ohlcPlotBy` method creates muliptle [OHLC series plots](./ohlcPlot.md) per distinct grouping value specified in `byColumns`.

## Syntax

```
ohlcPlotBy(seriesName, t, time, open, high, low, close, byColumns)
ohlcPlotBy(seriesName, sds, time, open, high, low, close, byColumns)
```

## Parameters

<ParamTable>
<Param name="seriesName" type="Comparable">

The name you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="Table">

The table.

</Param>
<Param name="sds" type="SelectableDataSet">

Selectable data set (e.g., OneClick filterable table).

</Param>
<Param name="time" type="String">

The column in `t` that holds the time data.

</Param>
<Param name="open" type="String">

The column in `t` that holds the open data.

</Param>
<Param name="high" type="String">

The column in `t` that holds the high data.

</Param>
<Param name="low" type="String">

The column in `t` that holds the low data.

</Param>
<Param name="close" type="String">

The column in `t` that holds the close data.

</Param>
<Param name="byColumns" type="list[String]">

Column(s) in `t` that holds the grouping data.

</Param>
</ParamTable>

## Returns

An OHLC series plot with multiple series.

## Examples

The following example plots data from a Deephaven table.

```groovy order=plotByOHLC,stockOHLC
import static io.deephaven.csv.CsvTools.readCsv

stockOHLC = readCsv('https://media.githubusercontent.com/media/deephaven/examples/main/TechStockPortfolio/csv/tech_stock_portfolio_slim.csv').\
    update('Date = parseInstant(Date + `T00:00:00 UTC`)')

plotByOHLC = ohlcPlotBy('Stock OHLC data', stockOHLC, 'Date', 'Open', 'High', 'Low', 'Close', 'Symbol').show()
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Arrays](../query-language/types/arrays.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/plot/Axes.html#ohlcPlotBy(java.lang.Comparable,io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String...))
