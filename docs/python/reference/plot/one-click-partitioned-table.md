---
title: one_click_partitioned_table
---

The `one_click_partitioned_table` method creates a [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) with the specified columns from a table map. This is useful in dynamic plotting, where Deephaven requires a [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) instead of a standard table to execute certain operations.

## Syntax

```python syntax
one_click_partitioned_table(
    pt: PartitionedTable,
    require_all_filters: bool = False
    ) -> SelectableDataSet
```

## Parameters

<ParamTable>
<Param name="pt" type="PartitionedTable">

The source table.

</Param>
<Param name="require_all_filters" type="bool" optional>

Whether to display data when some, but not all, [Input Filters](../../how-to-guides/user-interface/filters.md#input-filters) are applied. `False` will display data when not all filters are selected; `True` will only display data when appropriate filters are selected.

</Param>
</ParamTable>

## Returns

A [`SelectableDataSet`](../query-language/types/SelectableDataSet.md).

## Examples

In this example, we create a source table, then use [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md) to create a partitioned table. Next, we use `one_click_partitioned_table` to create a [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) copy of the table. Finally, we use [`plot_xy`](../../reference/plot/plot.md) to turn our [`SelectableDataSet`](../../reference/query-language/types/SelectableDataSet.md) into a plot, which can then be filtered via [**Controls > Input Filter**](../../how-to-guides/user-interface/filters.md#input-filters) in the user interface.

```python skip-test
from deephaven import read_csv
from deephaven.plot.selectable_dataset import one_click_partitioned_table
from deephaven.plot.figure import Figure

source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)

partitioned_source = source.partition_by("Exchange")

oc = one_click_partitioned_table(pt=partitioned_source, require_all_filters=True)
plot = Figure().plot_xy(series_name="Plot", t=oc, x="Timestamp", y="Price").show()
```

![A user applies a one-click filter to a partitioned table](../../assets/reference/plot/oneclick-partitioned.gif)

Note that multiple filters can be used at once. For example:

![Four filters are applied to a table at once](../../assets/reference/plot/multi-filter.png)

## Related documentation

- [How to create plots with the built-in API](../../how-to-guides/plotting/api-plotting.md)
- [`read_csv`](../../reference/data-import-export/CSV/readCsv.md)
- [`plot_xy`](./plot.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.selectable_dataset.html#deephaven.plot.selectable_dataset.one_click)
