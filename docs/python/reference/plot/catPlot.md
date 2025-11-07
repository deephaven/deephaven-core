---
title: plot_cat
---

The `plot_cat` method uses data from Deephaven tables to create a plot with discrete categories on the X axis.

## Syntax

```python syntax
plot_cat(
    series_name: str,
    t: Union[Table, SelectableDataSet],
    category: Union[str, list[str], list[int], list[float]],
    y: Union[str, list[int], list[float], list[DateTime]],
    y_low: Union[str, list[int], list[float], list[DateTime]] = None,
    y_high: Union[str, list[int], list[float], list[DateTime]] = None,
    by: list[str] = None,
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
<Param name="category" type="Union[str, list[str], list[int], list[float]]">

The names of the columns to be used for the categories.

</Param>

<Param name="y" type="Union[str, list[int], list[float], list[DateTime]]">

The name of the column containing the discrete values.

</Param>

<Param name="y_low" type="Union[str, list[int], list[float], list[DateTime]]" optional>

The name of the column containing lower Y error bar data.

</Param>
<Param name="y_high" type="Union[str, list[int], list[float], list[DateTime]]" optional>

The name of the column containing upper Y error bar data.

</Param>
<Param name="by" type="list[str]" optional>

A list of one or more columns that contain grouping data.

</Param>
</ParamTable>

## Returns

A category plot.

## Examples

The following example plots data from a Deephaven table.

```python order=new_plot,source
from deephaven.plot.figure import Figure
from deephaven import new_table
from deephaven.column import int_col, string_col

source = new_table(
    [string_col("Categories", ["A", "B", "C"]), int_col("Values", [1, 3, 5])]
)

new_plot = (
    Figure()
    .plot_cat(
        series_name="Cagetories Plot", t=source, category="Categories", y="Values"
    )
    .chart_title(title="Categories and Values")
    .show()
)
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.plot_cat)
