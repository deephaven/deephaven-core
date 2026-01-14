---
title: plotBy
---

The `plotBy` method creates muliptle XY series plots per distinct grouping value specified in `byColumns`.

## Syntax

```
plotBy(seriesName, t, x, y, byColumns)
plotBy(seriesName, sds, x, y, byColumns)
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
<Param name="x" type="String">

The column in `t` contains the X variable data.

</Param>
<Param name="y" type="String">

The column in `t` that contains the Y variable data.

</Param>
<Param name="byColumns" type="list[String]">

Column(s) in `t` that holds the grouping data.

</Param>
</ParamTable>

## Returns

An XY series plot with multiple series.

## Examples

The following example plots data from a Deephaven table.

```groovy order=null
import static io.deephaven.csv.CsvTools.readCsv

source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv")

result = plotBy("Sept22", source, "Timestamp", "Price", "Instrument").show()
```

![The above `result` plot](../../assets/reference/plotby.png)

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Arrays](../query-language/types/arrays.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/plot/Axes.html#plotBy(java.lang.Comparable,io.deephaven.plot.filters.SelectableDataSet,java.lang.String,java.lang.String,java.lang.String...))
