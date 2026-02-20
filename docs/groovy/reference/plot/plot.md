---
title: plot
---

The `plot` method creates XY series plots using data from tables.

## Syntax

```
plot(seriesName, x, y)
plot(seriesName, function)
plot(seriesName, t, x, y)
plot(seriesName, x, y, hasXTimeAxis, hasYTimeAxis)
plot(seriesName, sds, x, y)
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
<Param name="t" type="Table">

The table.

</Param>
<Param name="sds" type="SelectableDataSet">

Selectable data set (e.g. OneClick filterable table).

</Param>
<Param name="x" type="String">

The column in `t` or `sds` that contains the X variable data.

</Param>
<Param name="x" type="double[]">

X values.

</Param>
<Param name="x" type="float[]">

X values.

</Param>
<Param name="x" type="int[]">

X values.

</Param>
<Param name="x" type="long[]">

X values.

</Param>
<Param name="x" type="short[]">

X values.

</Param>
<Param name="x" type="IndexableNumericData[]">

X values.

</Param>
<Param name="x" type="DateTime[]">

X values.

</Param>
<Param name="x" type="Date[]">

X values.

</Param>
<Param name="x" type="List<T>">

X values.

</Param>
<Param name="x" type="<T>[]">

X values.

</Param>
<Param name="y" type="String">

The column in `t` or `sds` that contains the Y variable data.

</Param>
<Param name="y" type="double[]">

Y values.

</Param>
<Param name="y" type="float[]">

Y values.

</Param>
<Param name="y" type="int[]">

Y values.

</Param>
<Param name="y" type="long[]">

Y values.

</Param>
<Param name="y" type="short[]">

Y values.

</Param>
<Param name="y" type="IndexableNumericData[]">

Y values.

</Param>
<Param name="y" type="DateTime[]">

Y values.

</Param>
<Param name="y" type="Date[]">

Y values.

</Param>
<Param name="y" type="List<T>">

Y values.

</Param>
<Param name="y" type="<T>[]">

Y values.

</Param>
<Param name="hasXTimeAxis" type="boolean">

Whether or not to treat the X values as time data.

</Param>
<Param name="hasYTimeAxis" type="boolean">

Whether or not to treat the Y values as time data.

</Param>
<Param name="function" type="groovy.lang.CLosure<T>">

Function to plot.

</Param>
<Param name="function" type="DoubleUnaryOperator">

Function to plot.

</Param>
</ParamTable>

## Returns

An XY series plot with a single axes.

## Examples

The following example plots data from a Deephaven table.

```groovy order=source,plot_single default=plot_single
import static io.deephaven.csv.CsvTools.readCsv

//source the data
source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv")

//plot the data
plot_single = plot("Distance",  source.where("SpeedKPH > 0"), "Time", "DistanceMeters").show()
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Arrays](../query-language/types/arrays.md)
- [`plotStyle`](./plotStyle.md)
<!--TODO: add Javadoc link-->