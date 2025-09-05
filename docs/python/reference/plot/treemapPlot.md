---
title: plot_treemap
---

The `plot_treemap` method creates treemap charts using data from Deephaven tables.

## Syntax

```python syntax
plot_treemap(
    series_name: str,
    t: Union[Table, SelectableDataSet],
    id: str,
    parent: str,
    label: str = None,
    value: str = None,
    hover_text: str = None,
    color: str = None,
) -> tree map
```

## Parameters

<ParamTable>
<Param name="series_name" type="str">

The name (as a [String](../query-language/types/strings.md)) to identify the series on the plot itself.

</Param>
<Param name="t" type="Union[Table, SelectableDataSet]">

The table (or other selectable data set) that holds the data to be plotted.

</Param>
<Param name="id" type="str">

The name of the column containing ID values.

</Param>
<Param name="parent" type="str">

The name of the column containing parent ID values.

</Param>
<Param name="label" type="str" optional>

The name of the column containing labels. If not provided, labels default to the ID value.

</Param>
<Param name="value" type="str" optional>

The name of the column containing values for how large each section in relation to other sections.

</Param>
<Param name="hover_text" type="str" optional>

The name of the column containing hover text shown when hovering over a section in the treemap.

</Param>
<Param name="color" type="str" optional>

The name of the column containing hexadecimal color to use for each section.

</Param>
</ParamTable>

## Returns

A treemap chart.

## Examples

The following example plots data from a Deephaven table.

```python order=s_and_p,source
from deephaven import read_csv
from deephaven.plot.figure import Figure

# source the data
source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/StandardAndPoors500Financials/csv/s_and_p_500_market_cap.csv"
)

# plot the data
s_and_p = (
    Figure()
    .plot_treemap(
        series_name="S&P 500 Companies by Sector", t=source, id="Label", parent="Parent"
    )
    .show()
)
```

In the following example, `plot_treemap` is used to plot data from a Deephaven table, including the optional `label`, `value`, `hover_text`, and `color` parameters.

```python order=s_and_p_marketcap,source
from deephaven import read_csv
from deephaven.plot.figure import Figure

# source the data
source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/StandardAndPoors500Financials/csv/s_and_p_500_market_cap.csv"
)

# plot the data
s_and_p_marketcap = (
    Figure()
    .plot_treemap(
        series_name="S&P 500 Market Cap",
        t=source,
        id="Label",
        parent="Parent",
        label="Label",
        value="Value",
        hover_text="HoverText",
        color="Color",
    )
    .show()
)
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.plot_treemap)
