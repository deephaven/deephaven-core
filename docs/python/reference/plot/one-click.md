---
title: one_click
---

The `one_click` method creates a [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) with the specified columns from a source table. This is useful for dynamic plotting, where Deephaven requires a [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) instead of a standard table to execute certain operations.

## Syntax

```python syntax
one_click(t: Table, by: list[str] = None, require_all_filters: bool = False) -> SelectableDataSet
```

## Parameters

<ParamTable>
<Param name="t" type="Table">

The source table.

</Param>
<Param name="by" type="list[str]" optional>

The selected columns.

</Param>
<Param name="require_all_filters" type="bool" optional>

Whether to display data when some, but not all, [input filters](../../how-to-guides/user-interface/filters.md) are applied.

- `False` (default) will display data when not all filters are applied.
- `True` will only display data when appropriate filters are applied.

</Param>
</ParamTable>

## Returns

A [`SelectableDataSet`](../query-language/types/SelectableDataSet.md).

## Examples

In this example, we create a source table, then use `one_click` to create a [`SelectableDataSet`](../query-language/types/SelectableDataSet.md) copy of the table. Then, we use [`plot_xy`](./plot.md) to turn our [`SelectableDataSet`](../query-language/types/SelectableDataSet.md) into a plot, which can then be filtered via [**Controls > Input Filter**](../../how-to-guides/user-interface/filters.md#input-filters) in the user interface.

```python order=null
from deephaven import read_csv
from deephaven.plot.selectable_dataset import one_click
from deephaven.plot.figure import Figure

source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
oc = one_click(t=source, by=["Instrument"])
plot = Figure().plot_xy(series_name="Plot", t=oc, x="Timestamp", y="Price").show()
```

![A user creates a plot using the Input Filter tool](../../assets/reference/plot/oneclick.gif)

## Related documentation

- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [`read_csv`](../../reference/data-import-export/CSV/readCsv.md)
- [`plot_xy`](./plot.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.selectable_dataset.html#deephaven.plot.selectable_dataset.one_click)
