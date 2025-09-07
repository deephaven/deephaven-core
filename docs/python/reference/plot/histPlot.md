---
title: plot_xy_hist
---

The `plot_xy_hist` method creates XY histograms using data from Deephaven tables or [arrays](../query-language/types/arrays.md).

## Syntax

```python syntax
plot_xy_hist(
    series_name: str,
    t: Union[Table, SelectableDataSet],
    x: Union[str, List[int], List[float], List[DateTime]],
    nbins: int,
    xmin: float = None,
    xmax: float = None,
) -> Figure
```

## Parameters

<ParamTable>
<Param name="series_name" type="str">

The name (as a [String](../query-language/types/strings.md)) you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="Union[Table, SelectableDataSet]">

The table (or other selectable data set) that holds the data to be plotted.

</Param>
<Param name="x" type="Union[str, list[int], list[float], list[DateTime]]">

The name of the column containing the discrete values. This parameter is a list only when data is not sourced from a table; otherwise, it will be an array.

</Param>
<Param name="nbins" type="int">

The number of intervals (bins) to use in the chart.

</Param>
<Param name="xmin" type="float" optional>

The minimum value in the X column to plot.

</Param>
<Param name="xmax" type="float" optional>

The maximum value in the X column to plot.

</Param>
</ParamTable>

## Returns

A histogram.

## Examples

The following example plots data from a Deephaven table.

```python order=new_plot,source
from deephaven.plot.figure import Figure
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("Values", [1, 2, 2, 3, 3, 3, 4, 4, 5])])

new_plot = (
    Figure()
    .plot_xy_hist(series_name="Histogram Values", t=source, x="Values", nbins=5)
    .chart_title(title="Histogram of Values")
    .show()
)
```

The following example plots data from an array.

```python order=new_plot
from deephaven.plot.figure import Figure

import numpy as np

np_source = np.array([1, 2, 2, 3, 3, 3, 4, 4, 5])

new_plot = (
    Figure()
    .plot_xy_hist(
        series_name="Histogram Values", x=list(np_source), xmin=2.0, xmax=4.0, nbins=5
    )
    .chart_title(title="Histogram of Values")
    .show()
)
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Arrays](../query-language/types/arrays.md)
- [`plot_style`](./plotStyle.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.plot_cat_hist)
