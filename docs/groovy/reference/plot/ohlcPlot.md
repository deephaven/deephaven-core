---
sidebar_label: ohlcPlot
---

The `ohlcPlot` method creates open, high, low, close (OHLC) plots using data from tables.

## Syntax

```
ohlcPlot(seriesName, t, time, open, high, low, close)
ohlcPlot(seriesName, time, open high, low, close)
ohlcPlot(seriesName, sds, time, open, high, low, close)
ohlcPlot(seriesName, t, time, open, high, low, close)
```

<!--

(seriesName, [x], [y])
(seriesName, function)
-->

## Parameters

<ParamTable>
<Param name="seriesName" type="String">

The name you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="table">

The table that holds the data to be plotted.

</Param>
<Param name="sds" type="SelectableDataSet">

A selectable data set (e.g., OneClick filterable table).

</Param>
<Param name="time" type="String">

Column in `t` or `sds` that holds time data.

</Param>
<Param name="time" type="IndexableNumericData">

Time data.

</Param>
<Param name="time" type="DateTime">

Time data.

</Param>
<Param name="time" type="Date">

Time data.

</Param>
<Param name="open" type="String">

The column in `t` or `sds` that contains the open data.

</Param>
<Param name="open" type="IndexableNumericData">

Open data.

</Param>
<Param name="open" type="double[]">

Open data.

</Param>
<Param name="open" type="float[]">

Open data.

</Param>
<Param name="open" type="int[]">

Open data.

</Param>
<Param name="open" type="long[]">

Open data.

</Param>
<Param name="open" type="short[]">

Open data.

</Param>
<Param name="open" type="List<T>">

Open data.

</Param>
<Param name="open" type="<T>[]">

Open data.

</Param>
<Param name="high" type="String">

The column in `t` or `sds` that contains the high data.

</Param>
<Param name="high" type="IndexableNumericData">

High data.

</Param>
<Param name="high" type="double[]">

High data.

</Param>
<Param name="high" type="float[]">

High data.

</Param>
<Param name="high" type="int[]">

High data.

</Param>
<Param name="high" type="long[]">

High data.

</Param>
<Param name="high" type="short[]">

High data.

</Param>
<Param name="high" type="List<T>">

High data.

</Param>
<Param name="high" type="<T>[]">

High data.

</Param>
<Param name="low" type="String">

The column in `t` or `sds` that contains the low data.

</Param>
<Param name="low" type="IndexableNumericData">

Low data.

</Param>
<Param name="low" type="double[]">

Low data.

</Param>
<Param name="low" type="float[]">

Low data.

</Param>
<Param name="low" type="int[]">

Low data.

</Param>
<Param name="low" type="long[]">

Low data.

</Param>
<Param name="low" type="short[]">

Low data.

</Param>
<Param name="low" type="List<T>">

Low data.

</Param>
<Param name="low" type="<T>[]">

Low data.

</Param>
<Param name="close" type="String">

The column in `t` or `sds` that contains the close data.

</Param>
<Param name="close" type="IndexableNumericData">

Close data.

</Param>
<Param name="close" type="double[]">

Close data.

</Param>
<Param name="close" type="float[]">

Close data.

</Param>
<Param name="close" type="int[]">

Close data.

</Param>
<Param name="close" type="long[]">

Close data.

</Param>
<Param name="close" type="short[]">

Close data.

</Param>
<Param name="close" type="List<T>">

Close data.

</Param>
<Param name="close" type="<T>[]">

Close data.

</Param>
</ParamTable>

## Returns

An open, high, low, close (OHLC) plot with a single axes.

## Examples

The following example plots data from a Deephaven table.

```groovy test-set=1 order=cryptoTrades,btcBin,tOHLC,plotOHLC default=plotOHLC
import static io.deephaven.csv.CsvTools.readCsv
import static io.deephaven.api.agg.Aggregation.AggAvg

cryptoTrades = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")

agg_list = [
    AggLast("CloseTime = Timestamp"),\
    AggFirst("OpenTime = Timestamp"),\
    AggMax("High = Price"),\
    AggMin("Low = Price"),
]

btcBin = cryptoTrades.where("Instrument=`BTC/USD`").update("TimestampBin = lowerBin(Timestamp, MINUTE)")
tOHLC = btcBin.aggBy(agg_list, "TimestampBin").\
 join(btcBin,"CloseTime = Timestamp","Close = Price").\
 join(btcBin,"OpenTime = Timestamp","Open = Price")

plotOHLC = ohlcPlot("BTC", tOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
   .chartTitle("BTC OHLC - Aug 22 2021")\
   .show()
```

## Shared Axes

Just like [XY series](./plot.md) plots, the Open, High, Low and Close plot can also be used to present multiple series on the same chart, including the use of multiple X or Y axes. An example of this follows:

```groovy test-set=1 order=btcBin,ethBin,btcOHLC,ethOHLC,plotOHLC default=plotOHLC
btcBin = cryptoTrades.where("Instrument=`BTC/USD`").update("TimestampBin = lowerBin(Timestamp, MINUTE)")
ethBin = cryptoTrades.where("Instrument=`ETH/USD`").update("TimestampBin = lowerBin(Timestamp, MINUTE)")

agg_list = [
    AggLast("CloseTime = Timestamp"),\
    AggFirst("OpenTime = Timestamp"),\
    AggMax("High = Price"),\
    AggMin("Low = Price"),
]

btcOHLC = btcBin.aggBy(agg_list, "TimestampBin").\
 join(btcBin,"CloseTime = Timestamp","Close = Price").\
 join(btcBin,"OpenTime = Timestamp","Open = Price")

ethOHLC = ethBin.aggBy(agg_list, "TimestampBin").\
 join(ethBin,"CloseTime = Timestamp","Close = Price").\
 join(ethBin,"OpenTime = Timestamp","Open = Price")

plotOHLC = ohlcPlot("BTC", btcOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
   .chartTitle("BTC and ETC OHLC - Aug 22 2021")\
   .twinX()\
   .ohlcPlot("ETH", ethOHLC, "TimestampBin", "Open", "High", "Low", "Close")\
   .show()
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create OHLC plots with the built-in API](../../how-to-guides/plotting/api-plotting.md#ohlc)
