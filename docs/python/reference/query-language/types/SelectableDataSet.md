---
title: SelectableDataSet
---

A `SelectableDataSet` provides a view of a selectable subset of a table. For example, in some selectable data sets, a GUI click can be used to select a portion of a table.

`SelectableDataSet` objects are useful for creating [plots](../../../how-to-guides/plotting/api-plotting.md#xy-series) and figures with Deephaven methods that dynamically update when filters are applied. Dynamic plots can be created with either the [`one_click`](../../plot/one-click.md) or [`one_click_partitioned_table`](../../plot/one-click-partitioned-table.md) methods.

## one_click

In this example, we create a new table with [`read_csv`](../../data-import-export/CSV/readCsv.md), and then convert that table into a `SelectableDataSet` with [`one_click`](../../plot/one-click.md).

```python skip-test
from deephaven import read_csv
from deephaven.plot.selectable_dataset import one_click
from deephaven.plot import Figure

source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
result = one_click(t=source, by=["Instrument"], require_all_filters=True)
f = Figure()
plot = f.plot_xy(series_name="Plot", t=result, x="Timestamp", y="Price").show()
```

![An overlay appears with buttons to add Input Filters or open the Linker Tool](../../../assets/reference/required-clicks-true.png)

## one_click_partitioned_table

In this example, we take the table from the previous example, partition it with [`partition_by`](../../table-operations/group-and-aggregate/partitionBy.md), and then convert it to a `SelectableDataSet` using [`one_click_partitioned_table`](../../plot/one-click-partitioned-table.md).

```python skip-test
from deephaven import read_csv
from deephaven.plot.selectable_dataset import one_click_partitioned_table
from deephaven.plot import Figure

source = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
partitioned_source = source.partition_by("Exchange")

result = one_click_partitioned_table(pt=partitioned_source, require_all_filters=True)
f = Figure()
plot = f.plot_xy(series_name="Plot", t=result, x="Timestamp", y="Price").show()
```

![An overlay appears with buttons to add Input Filters or open the Linker Tool](../../../assets/reference/required-clicks-true.png)

## Methods that use SelectableDataSet

The following methods take a `SelectableDataSet` as an input:

- [`plot_cat_hist`](../../plot/catHistPlot.md)
- [`plot_cat`](../../plot/catPlot.md)
- [`plot_xy_hist`](../../plot/histPlot.md)
- [`plot_ohlc`](../../plot/ohlcPlot.md)
- [`plot_pie`](../../plot/piePlot.md)
- [`plot_xy`](../../plot/plot.md)
- [`plot_treemap`](../../plot/treemapPlot.md)

## Related Documentation

- [`read_csv`](../../data-import-export/CSV/readCsv.md)
- [`one_click`](../../plot/one-click.md)
- [`one_click_partitioned_table`](../../plot/one-click-partitioned-table.md)
