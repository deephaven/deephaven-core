---
title: plot_cat_hist
---

The `plot_cat_hist` method creates category histograms using data from Deephaven tables or [arrays](../query-language/types/arrays.md).

## Syntax

```python syntax
plot_cat_hist(
    series_name: str,
    t: Union[Table, SelectableDataSet],
    category: Union[str, list[str], list[int], list[float]]
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

The list containing the discrete values.

</Param>
</ParamTable>

## Returns

A category histogram plot.

## Examples

The following example plots data from a Deephaven table.

```python order=new_plot,source
from deephaven.plot.figure import Figure
from deephaven.column import int_col
from deephaven import new_table

source = new_table([int_col("Keys", [3, 2, 2, 1, 1, 1])])

new_plot = (
    Figure()
    .plot_cat_hist(series_name="Keys count", t=source, category="Keys")
    .chart_title(title="Count of Each Key")
    .show()
)
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Arrays](../query-language/types/arrays.md)
- [Figure](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure)
- [Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.plot_cat_hist)
