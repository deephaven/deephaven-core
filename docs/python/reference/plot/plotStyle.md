---
title: plot_style
---

`plot_style` alters the style of a XY series and category charts.

## Syntax

`plot_style(style)`

## Parameters

<ParamTable>
<Param name="style" type="str">

The plot style to apply. The following options are available:

- `BAR`
- `STACKED_BAR`
- `LINE`
- `AREA`
- `STACKED_AREA`
- `SCATTER`
- `STEP`

These arguments are case insensitive.

</Param>
</ParamTable>

## Examples

The following example creates a single series plot using the `STACKED_AREA` style.

```python order=plot_single_stacked_area,source
from deephaven import read_csv
from deephaven.plot.figure import Figure
from deephaven.plot import PlotStyle

# source the data
source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv"
)

# apply a plot style
plot_single_stacked_area = (
    Figure()
    .axes(plot_style=PlotStyle.STACKED_AREA)
    .plot_xy(series_name="Heart_rate", t=source, x="Time", y="HeartRate")
    .show()
)
```

The following example creates a category plot with the `STACKED_BAR` style.

```python order=result,source_one,source_two
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.plot.figure import Figure
from deephaven.plot import PlotStyle

source_one = new_table(
    [string_col("Categories", ["A", "B", "C"]), int_col("Values", [1, 3, 5])]
)

source_two = new_table(
    [string_col("Categories", ["A", "B", "C"]), int_col("Values", [2, 4, 6])]
)

result = (
    Figure()
    .axes(plot_style=PlotStyle.STACKED_BAR)
    .plot_cat(series_name="source_one", t=source_one, category="Categories", y="Values")
    .plot_cat(series_name="source_two", t=source_two, category="Categories", y="Values")
    .show()
)
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [`plot`](./plot.md)
- [`plot_cat`](./catPlot.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.plotstyle.html#module-deephaven.plot.plotstyle)
