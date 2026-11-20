---
title: plot_xy
---

The `plot_xy` method creates XY series plots using data from tables.

## Syntax

```python syntax
plot_xy(
    series_name: str,
    t: Table | SelectableDataSet,
    x: str | list[int] | list[float] | list[DateTime],
    y: str | list[int] | list[float] | list[DateTime],
    x_low: str | list[int] | list[float] | list[DateTime] | None = None,
    x_high: str | list[int] | list[float] | list[DateTime] | None = None,
    y_low: str | list[int] | list[float] | list[DateTime] | None = None,
    y_high: str | list[int] | list[float] | list[DateTime] | None = None,
    function: Callable = None,
    by: list[str] = None,
    x_time_axis: bool = None,
    y_time_axis: bool = None,
) -> Figure
```

## Parameters

<ParamTable>
<Param name="series_name" type="str">

The name you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="Table | SelectableDataSet">

The table (or other selectable data set) that holds the data to be plotted.

</Param>
<Param name="x" type="str | list[int] | list[float] | list[DateTime]">

The name of the column of data to be used for the X value.

</Param>
<Param name="y" type="str | list[int] | list[float] | list[DateTime]">

The name of the column of data to be used for the Y value.

</Param>
<Param name="x_low" type="str | list[int] | list[float] | list[DateTime]" optional>

Lower X error bar.

</Param>
<Param name="x_high" type="str | list[int] | list[float] | list[DateTime]" optional>

Upper X error bar.

</Param>
<Param name="y_low" type="str | list[int] | list[float] | list[DateTime]" optional>

Lower Y error bar.

</Param>
<Param name="y_high" type="str | list[int] | list[float] | list[DateTime]" optional>

Upper Y error bar.

</Param>
<Param name="function" type="Callable" optional>

A built-in or user-defined function.

</Param>
<Param name="by" type="list[str]" optional>

Columns that hold grouping data.

</Param>
<Param name="x_time_axis" type="bool" optional>

Whether to treat the X values as times.

</Param>
<Param name="y_time_axis" type="bool" optional>

Whether to treat the Y values as times.

</Param>
</ParamTable>

## Returns

An XY series plot with a single axes.

## Examples

The following example plots data from a Deephaven table.

```python order=plot_single,source
from deephaven import read_csv
from deephaven.plot.figure import Figure

source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv"
)

plot_single = (
    Figure()
    .plot_xy(
        series_name="Distance",
        t=source.where(filters=["SpeedKPH > 0"]),
        x="Time",
        y="DistanceMeters",
    )
    .show()
)
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Arrays](../query-language/types/arrays.md)
- [`plot_style`](./plotStyle.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.plot_xy)
