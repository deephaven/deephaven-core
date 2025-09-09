---
title: histPlot
---

The `histPlot` method creates histograms using data from Deephaven tables or [arrays](../query-language/types/arrays.md).

## Syntax

```
histPlot(seriesName, x, xmin, xmax, nbins)
histPlot(seriesName, x, nbins)
histPlot(seriesName, x, xmin, xmax, nbins)
histPlot(seriesName, t)
histPlot(seriesName, t, x, xmin, xmax, nbins)
histPlot(seriesName, t, x, nbins)
histPlot(seriesName, sds, x, xmin, xmax, nbins)
histPlot(seriesName, sds, x, nbins)
```

## Parameters

<ParamTable>
<Param name="seriesName" type="String">

The name (as a [String](../query-language/types/strings.md)) you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="Table">

The table that holds the data to be plotted.

</Param>
<Param name="sds" type="SelectableDataSet">

A selectable data set - e.g., OneClick filterable table.

</Param>
<Param name="nbins" type="int">

The number of intervals to use in the chart.

</Param>
<Param name="x" type="double[]">

The data to be used for the X values.

</Param>
<Param name="x" type="float[]">

The data to be used for the X values.

</Param>
<Param name="x" type="int[]">

The data to be used for the X values.

</Param>
<Param name="x" type="long[]">

The data to be used for the X values.

</Param>
<Param name="x" type="short[]">

The data to be used for the X values.

</Param>
<Param name="x" type="String">

The name of a column in `t` or `sds`.

</Param>
<Param name="x" type="List<T>">

The data to be used for the X values.

</Param>
<Param name="x" type="<T>[]">

The data to be used for the X values.

</Param>

<Param name="xmin" type="double">

The minimum X value in the range.

</Param>
<Param name="xmax" type="double">

The maximum X value in the range.

</Param>
</ParamTable>

## Returns

A histogram.

## Examples

The following example plots data from a Deephaven table.

```groovy order=source,result default=result
source = newTable(
    intCol("Values", 1, 2, 2, 3, 3, 3, 4, 4, 5)
)

result = histPlot("Histogram Values", source, "Values", 5)
    .chartTitle("Histogram Of Values")
    .show()
```

The following example plots data from an array.

```groovy
source = [1, 2, 2, 3, 3, 3, 4, 4, 5] as int[]

result = histPlot("Histogram Values", source, 5)
    .chartTitle("Histogram Of Values")
    .show()
```

<!--TODO: https://github.com/deephaven/deephaven.io/issues/2459 Add code examples -->

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create histograms](../../how-to-guides/plotting/api-plotting.md#histogram)
- [Arrays](../query-language/types/arrays.md)
- [plotStyle](./plotStyle.md)
