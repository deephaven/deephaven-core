---
title: plotStyle
---

`plotStyle` alters the style of a XY series and category charts.

## Syntax

`plotStyle(plotStyle)`

## Parameters

<ParamTable>
<Param name="plotStyle" type="String">

The plot style to apply. The following options are available:

- `Area`
- `Bar`
- `Error_Bar`
- `Histogram`
- `Line`
- `OHLC`
- `Pie`
- `Scatter`
- `Stacked_Area`
- `Stacked_Bar`
- `Step`
- `Treemap`

These arguments are case insensitive.

</Param>
</ParamTable>

## Examples

The following example creates a single series plot using the `stacked_area` style.

```groovy order=source,plot_single_stacked_area default=plot_single_stacked_area
import static io.deephaven.csv.CsvTools.readCsv

//source the data
source = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv")

//apply a plot style
plot_single_stacked_area= plot("Heart_rate", source, "Time", "HeartRate").plotStyle("stacked_area")\
    .show()
```

The following example creates a category plot with the `stacked_bar` style.

```groovy order=sourceOne,sourceTwo,result default=result
sourceOne = newTable(
    stringCol("Categories", "A", "B", "C"),
    intCol("Values", 1, 3, 5)
)
sourceTwo = newTable(
    stringCol("Categories", "A", "B", "C"),
    intCol("Values", 2, 4, 6)
)

result = catPlot("Categories Plot One", sourceOne, "Categories", "Values")
    .catPlot("Categories Plot Two", sourceTwo, "Categories", "Values")
    .plotStyle("stacked_bar")
    .chartTitle("Categories And Values")
    .show()
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots programmatically](../../how-to-guides/plotting/api-plotting.md)
