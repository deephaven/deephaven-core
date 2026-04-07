---
title: plot_ohlc
---

The `plot_ohlc` method creates open, high, low, close (OHLC) plots using data from tables.

## Syntax

```python skip-test
plot_ohlc(
    series_name: str,
    t: Union[Table, SelectableDataSet],
    x: Union[str, List[DateTime]],
    open: Union[str, list[int], list[float], list[DateTime]],
    high: Union[str, list[int], list[float], list[DateTime]],
    low: Union[str, list[int], list[float], list[DateTime]],
    close: Union[str, list[int], list[float], list[DateTime]],
    by: list[str] = None,
) -> Figure
```

## Parameters

<ParamTable>
<Param name="series_name" type="str">

The name you want to use to identify the series on the plot itself.

</Param>
<Param name="t" type="Union[Table, SelectableDataSet]">

The table (or other selectable data set) that holds the data to be plotted.

</Param>
<Param name="x" type="Union[str, List[DateTime]]">

The name of the column of data to be used for the X value.

</Param>
<Param name="open" type="Union[str, List[int], List[float], List[DateTime]]">

The name of the column of data to be used for the open value.

</Param>
<Param name="high" type="Union[str, list[int], list[float], list[DateTime]]">

The name of the column of data to be used for the high value.

</Param>
<Param name="low" type="Union[str, list[int], list[float], list[DateTime]]">

The name of the column of data to be used for the low value.

</Param>
<Param name="close" type="Union[str, list[int], list[float], list[DateTime]]">

The name of the column of data to be used for the close value.

</Param>
<Param name="by" type="list[str]" optional>

Columns that hold grouping data.

</Param>
</ParamTable>

## Returns

An open, high, low, close (OHLC) plot with a single axes.

## Examples

The following example plots data from a Deephaven table.

```python test-set=1 order=null
from deephaven import read_csv
from deephaven import agg
from deephaven.plot.figure import Figure

crypto_trades = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)

agg_list = [
    agg.last(cols=["CloseTime = Timestamp"]),
    agg.first(cols=["OpenTime = Timestamp"]),
    agg.max_(cols=["High = Price"]),
    agg.min_(cols=["Low = Price"]),
]

btc_bin = crypto_trades.where(filters=["Instrument=`BTC/USD`"]).update(
    formulas=["TimestampBin = lowerBin(Timestamp, MINUTE)"]
)

t_ohlc = (
    btc_bin.agg_by(agg_list, by=["TimestampBin"])
    .join(table=btc_bin, on=["CloseTime = Timestamp"], joins=["Close = Price"])
    .join(table=btc_bin, on=["OpenTime = Timestamp"], joins=["Open = Price"])
)

plot_ohlc = (
    Figure()
    .figure_title("BTC OHLC - Aug 22 2021")
    .plot_ohlc(
        series_name="BTC",
        t=t_ohlc,
        x="TimestampBin",
        open="Open",
        high="High",
        low="Low",
        close="Close",
    )
    .show()
)
```

## Shared Axes

Just like [XY series](./plot.md) plots, the Open, High, Low and Close plot can also be used to present multiple series on the same chart, including the use of multiple X or Y axes. An example of this follows:

```python test-set=1 order=null
from deephaven import agg as agg
from deephaven.plot.figure import Figure

btc_bin = crypto_trades.where(filters=["Instrument=`BTC/USD`"]).update(
    formulas=["TimestampBin = lowerBin(Timestamp, MINUTE)"]
)

eth_bin = crypto_trades.where(filters=["Instrument=`ETH/USD`"]).update(
    formulas=["TimestampBin = lowerBin(Timestamp, MINUTE)"]
)

agg_list = [
    agg.last(cols=["CloseTime = Timestamp"]),
    agg.first(cols=["OpenTime = Timestamp"]),
    agg.max_(["High = Price"]),
    agg.min_(["Low = Price"]),
]

btc_ohlc = (
    btc_bin.agg_by(agg_list, by=["TimestampBin"])
    .join(table=btc_bin, on=["CloseTime = Timestamp"], joins=["Close = Price"])
    .join(table=btc_bin, on=["OpenTime = Timestamp"], joins=["Open = Price"])
)

eth_ohlc = (
    eth_bin.agg_by(agg_list, by=["TimestampBin"])
    .join(table=eth_bin, on=["CloseTime = Timestamp"], joins=["Close = Price"])
    .join(table=eth_bin, on=["OpenTime = Timestamp"], joins=["Open = Price"])
)

plot_ohlc = (
    Figure()
    .figure_title("BTC and ETC OHLC - Aug 22 2021")
    .plot_ohlc(
        series_name="BTC",
        t=t_ohlc,
        x="TimestampBin",
        open="Open",
        high="High",
        low="Low",
        close="Close",
    )
    .x_twin()
    .plot_ohlc(
        series_name="ETH",
        t=t_ohlc,
        x="TimestampBin",
        open="Open",
        high="High",
        low="Low",
        close="Close",
    )
    .show()
)
```

## Related documentation

- [How to use the Chart Builder](../../how-to-guides/user-interface/chart-builder.md)
- [How to create OHLC plots with the built-in API](../../how-to-guides/plotting/api-plotting.md#ohlc)
- [`plot`](./plot.md)
- [Pydoc](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.plot_ohlc)
