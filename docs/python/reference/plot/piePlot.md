---
title: plot_pie
---

The `plot_pie` method creates pie charts using data from Deephaven tables.

## Syntax

```python syntax
plot_pie(
    series_name: str,
    t: Union[Table, SelectableDataSet],
    category: Union[str, list[int], list[float]],
    y: Union[str, list[int], list[float], list[DateTime]],
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

The name of the column containing category values.

</Param>
<Param name="y" type="Union[str, list[int], list[float], list[DateTime]]">

The name of the column containing discrete values.

</Param>
</ParamTable>

## Returns

A pie chart.

## Examples

The following example plots data from a Deephaven table.

```python order=new_plot,insurance,insurance_by_region
from deephaven.plot.figure import Figure
from deephaven import read_csv

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)
insurance_by_region = insurance.view(formulas=["region", "expenses"]).sum_by(["region"])

new_plot = (
    Figure()
    .plot_pie(
        series_name="Insurance charges by region",
        t=insurance_by_region,
        category="region",
        y="expenses",
    )
    .chart_title(title="Expenses per region")
    .show()
)
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.plot_pie)
