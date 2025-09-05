---
title: Deephaven Community Core Quickstart for Jupyter
sidebar_label: Jupyter Quickstart
---

Deephaven Community Core + [Jupyter](https://jupyter.org) is a powerful real-time data science workflow that few frameworks can hope to match. You can start and use a Deephaven server directly from Jupyter with [pip-installed Deephaven](../getting-started/pip-install.md). Alternatively, you can use the [Deephaven Python client](/core/client-api/python/) from Jupyter to connect to an already-running Deephaven server.

## 0. Install Jupyter

> [!NOTE]
> We recommend using a Python [virtual environment](https://docs.python.org/3/library/venv.html) to decouple and isolate Python installs and associated packages.

Deephaven can operate in [JupyterLab](https://jupyter.org) or [Jupyter Notebook](https://jupyter.org) - the choice is yours! Both are installed with `pip`:

```bash
# install JupyterLab
pip install jupyterlab
# or install Jupyter Notebook
pip install notebook
```

## 1. Install and Launch Deephaven

> [!NOTE]
> To run Deephaven with Python, you must have Java installed on your computer. See [this guide](../getting-started/launch-build.md#prerequisites) for OS-specific instructions.

The [`deephaven-server`](https://pypi.org/project/deephaven-server/) package enables you to use Deephaven directly from Jupyter. Additionally, the [`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/) package allows Deephaven tables and plots to be rendered in a Jupyter notebook. Install them both in the same environment as your Jupyter installation:

```sh
pip install deephaven-server deephaven-ipywidgets
```

Now, start an instance of Jupyter:

```sh
# start JupyterLab
jupyter lab
# or start Jupyter notebook
jupyter notebook
```

When using Deephaven from Jupyter, you _must_ start a Deephaven server before importing any Deephaven packages. The following code block starts a Deephaven server on port `10000` with 4GB of heap memory and anonymous authentication. Run it in your Jupyter instance:

<!--TODO: Change to PSK when https://github.com/deephaven/deephaven-ipywidgets/issues/38 is resolved -->

```python skip-test
from deephaven_server import Server

s = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
)
s.start()
```

For more advanced configuration options, see our [pip installation guide](../getting-started/pip-install.md). This includes [an additional instruction](../getting-started/pip-install.md#m2-macs) for users on an M2 Mac.

## 2. Import static and streaming data

Deephaven empowers users to wrangle static and streaming data with ease. It supports ingesting data from [CSV files](../how-to-guides/data-import-export/csv-import.md), [Parquet files](../how-to-guides/data-import-export/parquet-import.md), and [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md).

### Load a CSV

Run the command below inside a Deephaven console to ingest a million-row CSV of crypto trades. All you need is a path or URL for the data:

```python test-set=1 order=crypto_from_csv skip-test
from deephaven import read_csv

crypto_from_csv = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
```

The resulting table can then be displayed using `DeephavenWidget` from [`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/). This enables any Deephaven widget to be rendered from within a Jupyter context:

```python skip-test
from deephaven_ipywidgets import DeephavenWidget

display(DeephavenWidget(crypto_from_csv))
```

The table widget now in view is highly interactive:

- Click on a table and press <kbd>Ctrl</kbd> + <kbd>F</kbd> (Windows) or <kbd>⌘F</kbd> (Mac) to open quick filters.
- Click the funnel icon in the filter field to create sophisticated filters or use auto-filter UI features.
- Hover over column headers to see data types.
- Right-click headers to access more options, like adding or changing sorts.
- Click the **Table Options** hamburger menu at right to plot from the UI, create and manage columns, and download CSVs.

![Animated GIF showing interactive Deephaven table widget rendered inside a Jupyter notebook](../assets/tutorials/quickstart-jupyter/jquickstart-1.gif)

### Replay Historical Data

Ingesting real-time data is one of Deephaven's superpowers, and you can learn more about supported formats from the links at the end of this guide. However, streaming pipelines can be complicated to set up and are outside the scope of this discussion. For a streaming data example, we'll use Deephaven's [Table Replayer](../reference/table-operations/create/Replayer.md) to replay historical cryptocurrency data back in real time.

The following code takes fake historical crypto trade data from a CSV file and replays it in real time based on timestamps. This is only one of multiple ways to create real-time data in just a few lines of code. Replaying historical data is a great way to test real-time algorithms before deployment into production.

```python test-set=1 order=null ticking-table skip-test
from deephaven import TableReplayer, read_csv
from deephaven.time import to_j_instant

fake_crypto_data = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
)

start_time = to_j_instant("2023-02-09T12:09:18 ET")
end_time = to_j_instant("2023-02-09T12:58:09 ET")

replayer = TableReplayer(start_time, end_time)

crypto_streaming = replayer.add_table(fake_crypto_data, "Timestamp")

replayer.start()

display(DeephavenWidget(crypto_streaming))
```

![Animated GIF showing live streaming cryptocurrency trades table updating in real time](../assets/tutorials/quickstart-jupyter/jquickstart-2.gif)

## 3. Working with Deephaven Tables

In Deephaven, static and dynamic data are represented as tables. New tables can be derived from parent tables, and data efficiently flows from parents to their dependents. See the concept guide on the [table update model](../conceptual/table-update-model.md) if you're interested in what's under the hood.

Deephaven represents data transformations as operations on tables. This is a familiar paradigm for data scientists using [Pandas](https://pandas.pydata.org), [Polars](https://pola.rs), [R](https://www.r-project.org/), [Matlab](https://www.mathworks.com/) and more. Deephaven's table operations are special - **they are indifferent to whether the underlying data sources are static or streaming!** This means that code written for static data will work seamlessly on live data.

There are a ton of table operations to cover, so we'll keep it short and give you the highlights.

### Manipulating data

First, reverse the ticking table with [`reverse`](../reference/table-operations/sort/reverse.md) so that the newest data appears at the top:

```python test-set=1 order=null ticking-table skip-test
crypto_streaming_rev = crypto_streaming.reverse()
display(DeephavenWidget(crypto_streaming_rev))
```

![Animated GIF showing table reversed so most recent rows appear first](../assets/tutorials/quickstart-jupyter/jquickstart-3.gif)

> [!TIP]
> Many table operations can also be performed from the UI. For example, right-click on a column header in the UI and choose **Reverse Table**.

Add a column with [`update`](../reference/table-operations/select/update.md):

```python test-set=1 order=null ticking-table skip-test
# Note the enclosing [] - this is optional when there is a single argument
crypto_streaming_rev = crypto_streaming_rev.update(["TransactionTotal = Price * Size"])
display(DeephavenWidget(crypto_streaming_rev))
```

![Animated GIF showing new TransactionTotal column added via update](../assets/tutorials/quickstart-jupyter/jquickstart-4.gif)

Use [`select`](../reference/table-operations/select/select.md) or [`view`](../reference/table-operations/select/view.md) to pick out particular columns:

```python test-set=1 order=null ticking-table skip-test
# Note the enclosing [] - this is not optional, since there are multiple arguments
crypto_streaming_prices = crypto_streaming_rev.view(["Instrument", "Price"])
display(DeephavenWidget(crypto_streaming_prices))
```

![Animated GIF showing table reduced to Instrument and Price columns](../assets/tutorials/quickstart-jupyter/jquickstart-5.gif)

Remove columns with [`drop_columns`](../reference/table-operations/select/drop-columns.md):

```python test-set=1 order=null ticking-table skip-test
# Note the lack of [] - this is permissible since there is only a single argument
crypto_streaming_rev = crypto_streaming_rev.drop_columns("TransactionTotal")
display(DeephavenWidget(crypto_streaming_rev))
```

![Animated GIF showing TransactionTotal column removed using drop_columns](../assets/tutorials/quickstart-jupyter/jquickstart-6.gif)

Next, Deephaven offers many operations for filtering tables. These include [`where`](../reference/table-operations/filter/where.md), [`where_one_of`](../reference/table-operations/filter/where-one-of.md), [`where_in`](../reference/table-operations/filter/where-in.md), [`where_not_in`](../reference/table-operations/filter/where-not-in.md), and [more](../how-to-guides/use-filters.md).

The following code uses [`where`](../reference/table-operations/filter/where.md) and [`where_one_of`](../reference/table-operations/filter/where-one-of.md) to filter for only Bitcoin transactions, and then for Bitcoin and Ethereum transactions:

```python test-set=1 order=null ticking-table skip-test
btc_streaming = crypto_streaming_rev.where("Instrument == `BTC/USD`")
etc_btc_streaming = crypto_streaming_rev.where_one_of(
    ["Instrument == `BTC/USD`", "Instrument == `ETH/USD`"]
)
display(DeephavenWidget(etc_btc_streaming))
```

![Animated GIF filtering table to show Bitcoin and Ethereum trades](../assets/tutorials/quickstart-jupyter/jquickstart-7.gif)

### Aggregating data

Deephaven's [dedicated aggregations suite](../how-to-guides/dedicated-aggregations.md) provides several table operations that enable efficient column-wise aggregations and support aggregations by group.

Use [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) to count the number of transactions from each exchange:

```python test-set=1 order=null ticking-table skip-test
exchange_count = crypto_streaming.count_by("Count", by="Exchange")
display(DeephavenWidget(exchange_count))
```

![Animated GIF showing count of transactions per exchange using count_by](../assets/tutorials/quickstart-jupyter/jquickstart-8.gif)

Then, get the average price for each instrument with [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md):

```python test-set=1 order=null ticking-table skip-test
instrument_avg = crypto_streaming.view(["Instrument", "Price"]).avg_by(by="Instrument")
display(DeephavenWidget(instrument_avg))
```

![Animated GIF showing average price per instrument using avg_by](../assets/tutorials/quickstart-jupyter/jquickstart-9.gif)

Find the largest transaction per instrument with [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md):

```python test-set=1 order=null ticking-table skip-test
max_transaction = (
    crypto_streaming.update("TransactionTotal = Price * Size")
    .view(["Instrument", "TransactionTotal"])
    .max_by("Instrument")
)
display(DeephavenWidget(max_transaction))
```

![Animated GIF showing largest transaction per instrument using max_by](../assets/tutorials/quickstart-jupyter/jquickstart-10.gif)

While dedicated aggregations are powerful, they only enable you to perform one aggregation at a time. However, you often need to [perform multiple aggregations](../how-to-guides/combined-aggregations.md) on the same data. For this, Deephaven provides the [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) table operation and the [`deephaven.agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg) Python module.

First, use [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) to compute the [mean](../reference/table-operations/group-and-aggregate/AggAvg.md) and [standard deviation](../reference/table-operations/group-and-aggregate/AggStd.md) of each instrument's price, grouped by exchange:

```python test-set=1 order=null ticking-table skip-test
from deephaven import agg

summary_prices = crypto_streaming.agg_by(
    [agg.avg("AvgPrice=Price"), agg.std("StdPrice=Price")],
    by=["Instrument", "Exchange"],
).sort(["Instrument", "Exchange"])
display(DeephavenWidget(summary_prices))
```

![Animated GIF showing summary table of average and standard deviation price per instrument and exchange](../assets/tutorials/quickstart-jupyter/jquickstart-11.gif)

Then, add a column containing the coefficient of variation for each instrument, measuring the relative risk of each:

```python test-set=1 order=null ticking-table skip-test
summary_prices = summary_prices.update("PctVariation = 100 * StdPrice / AvgPrice")
display(DeephavenWidget(summary_prices))
```

![Animated GIF showing summary table updated with percentage variation column](../assets/tutorials/quickstart-jupyter/jquickstart-12.gif)

Finally, create a minute-by-minute Open-High-Low-Close table using the [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) [built-in function](../reference/query-language/query-library/auto-imported-functions.md) along with [`first`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`max_`](../reference/table-operations/group-and-aggregate/AggMax.md), [`min_`](../reference/table-operations/group-and-aggregate/AggMin.md), and [`last`](../reference/table-operations/group-and-aggregate/AggLast.md):

```python test-set=1 order=null ticking-table skip-test
ohlc_by_minute = (
    crypto_streaming.update("BinnedTimestamp = lowerBin(Timestamp, MINUTE)")
    .agg_by(
        [
            agg.first("Open=Price"),
            agg.max_("High=Price"),
            agg.min_("Low=Price"),
            agg.last("Close=Price"),
        ],
        by=["Instrument", "BinnedTimestamp"],
    )
    .sort(["Instrument", "BinnedTimestamp"])
)
display(DeephavenWidget(ohlc_by_minute))
```

![Animated GIF showing minute-bucket OHLC table generated with agg_by](../assets/tutorials/quickstart-jupyter/jquickstart-13.gif)

You may want to perform window-based calculations, compute moving or cumulative statistics, or look at pair-wise differences. Deephaven's [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`deephaven.updateby`](/core/pydoc/code/deephaven.updateby.html) Python module are the right tools for the job.

Compute the moving average and standard deviation of each instrument's price using [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md) and [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md):

```python test-set=1 order=null ticking-table skip-test
import deephaven.updateby as uby

instrument_rolling_stats = crypto_streaming.update_by(
    [
        uby.rolling_avg_time("Timestamp", "AvgPrice30Sec=Price", "PT30s"),
        uby.rolling_avg_time("Timestamp", "AvgPrice5Min=Price", "PT5m"),
        uby.rolling_std_time("Timestamp", "StdPrice30Sec=Price", "PT30s"),
        uby.rolling_std_time("Timestamp", "StdPrice5Min=Price", "PT5m"),
    ],
    by="Instrument",
).reverse()
display(DeephavenWidget(instrument_rolling_stats))
```

![Animated GIF showing rolling averages and standard deviations calculated with update_by](../assets/tutorials/quickstart-jupyter/jquickstart-14.gif)

These statistics can be used to determine "extreme" instrument prices, where the instrument's price is significantly higher or lower than the average of the prices preceding it in the window:

```python test-set=1 order=null ticking-table skip-test
instrument_extremity = instrument_rolling_stats.update(
    [
        "Z30Sec = (Price - AvgPrice30Sec) / StdPrice30Sec",
        "Z5Min = (Price - AvgPrice5Min) / StdPrice5Min",
        "Extreme30Sec = Math.abs(Z30Sec) > 1.645 ? true : false",
        "Extreme5Min = Math.abs(Z5Min) > 1.645 ? true : false",
    ]
).view(
    [
        "Timestamp",
        "Instrument",
        "Exchange",
        "Price",
        "Size",
        "Extreme30Sec",
        "Extreme5Min",
    ]
)
display(DeephavenWidget(instrument_extremity))
```

![Animated GIF highlighting extreme price rows based on Z-score thresholds](../assets/tutorials/quickstart-jupyter/jquickstart-15.gif)

There's a lot more to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). See the [user guide](../how-to-guides/rolling-aggregations.md) for more information.

### Combining tables

Combining datasets can often yield powerful insights. Deephaven offers two primary ways to combine tables - the _merge_ and _join_ operations.

The [`merge`](../reference/table-operations/merge/merge.md) operation stacks tables on top of one another. This is ideal when several tables have the same schema. They can be static, ticking, or a mix of both:

```python test-set=1 order=null ticking-table skip-test
from deephaven import merge

combined_crypto = merge([fake_crypto_data, crypto_streaming]).sort("Timestamp")
display(DeephavenWidget(combined_crypto))
```

![Animated GIF showing merged static and streaming crypto tables using merge](../assets/tutorials/quickstart-jupyter/jquickstart-16.gif)

The ubiquitous join operation is used to combine tables based on columns that they have in common. Deephaven offers many variants of this operation, such as [`join`](../reference/table-operations/join/join.md), [`natural_join`](../reference/table-operations/join/natural-join.md), [`exact_join`](../reference/table-operations/join/exact-join.md), and [many more](../how-to-guides/joins-timeseries-range.md).

For example, read in an older dataset containing price data on the same coins from the same exchanges. Then, use [`join`](../reference/table-operations/join/join.md) to combine the aggregated prices to see how current prices compare to those in the past:

```python test-set=1 order=null ticking-table skip-test
more_crypto = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)

more_summary_prices = more_crypto.agg_by(
    [agg.avg("AvgPrice=Price"), agg.std("StdPrice=Price")],
    by=["Instrument", "Exchange"],
).sort(["Instrument", "Exchange"])

price_comparison = (
    summary_prices.drop_columns("PctVariation")
    .rename_columns(["AvgPriceFeb2023=AvgPrice", "StdPriceFeb2023=StdPrice"])
    .join(
        more_summary_prices,
        on=["Instrument", "Exchange"],
        joins=["AvgPriceSep2021=AvgPrice", "StdPriceSep2021=StdPrice"],
    )
)
display(DeephavenWidget(price_comparison))
```

![Animated GIF comparing February 2023 and September 2021 price summaries via join](../assets/tutorials/quickstart-jupyter/jquickstart-17.gif)

In many real-time data applications, data must be combined based on timestamps. Traditional join operations often fail this task, as they require _exact_ matches in both datasets. To remedy this, Deephaven provides [time series joins](../how-to-guides/joins-timeseries-range.md#as-of-time-series-joins), such as [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), that can join tables on timestamps with _approximate_ matches.

Here's an example where [`aj`](../reference/table-operations/join/aj.md) is used to find the Ethereum price at or immediately preceding a Bitcoin price:

```python test-set=1  order=null ticking-table skip-test
crypto_btc = crypto_streaming.where(filters=["Instrument = `BTC/USD`"])
crypto_eth = crypto_streaming.where(filters=["Instrument = `ETH/USD`"])

time_series_join = (
    crypto_btc.view(["Timestamp", "Price"])
    .aj(crypto_eth, on="Timestamp", joins=["EthTime = Timestamp", "EthPrice = Price"])
    .rename_columns(cols=["BtcTime = Timestamp", "BtcPrice = Price"])
)
display(DeephavenWidget(time_series_join))
```

![Animated GIF displaying time-series join aligning Ethereum prices with Bitcoin timestamps](../assets/tutorials/quickstart-jupyter/jquickstart-18.gif)

To learn more about our join methods, see the guides on [exact and relational joins](../how-to-guides/joins-exact-relational.md) and [time-series and range joins](../how-to-guides/joins-timeseries-range.md).

## 5. Plot data via query or the UI

Deephaven has a rich plotting API that supports _updating, real-time plots_. It can be called programmatically:

```python test-set=1 order=null ticking-table skip-test
from deephaven.plot import Figure

btc_data = instrument_rolling_stats.where("Instrument == `BTC/USD`").reverse()

btc_plot = (
    Figure()
    .plot_xy("Bitcoin Prices", btc_data, x="Timestamp", y="Price")
    .plot_xy("Rolling Average", btc_data, x="Timestamp", y="AvgPrice30Sec")
    .show()
)
display(DeephavenWidget(btc_plot))
```

![Animated GIF showing real-time line plot of Bitcoin prices and rolling average](../assets/tutorials/quickstart-jupyter/jquickstart-19.gif)

Additionally, Deephaven supports an integration with the popular [plotly-express library](https://plotly.com/python/) that enables real-time plotly-express plots.

<!--TODO: Link out to deephaven-plotly-express documentation -->

## 6. Export data to popular formats

It's easy to export your data out of Deephaven to popular open formats.

To export a table to a CSV file, use the [`write_csv`](../reference/data-import-export/CSV/readCsv.md) method with the table name and the location to which you want to save the file. The file path should be absolute. This code writes the CSV to the current working directory:

```python test-set=1 order=null skip-test
import os
from deephaven import write_csv

write_csv(instrument_rolling_stats, os.getcwd() + "/crypto_prices_stats.csv")
```

If the table is dynamically updating, Deephaven will automatically snapshot the data before writing it to the file.

Similarly, for Parquet:

```python test-set=1 order=null skip-test
from deephaven.parquet import write

write(instrument_rolling_stats, os.getcwd() + "/crypto_prices_stats.parquet")
```

To create a static [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html), use the [`to_pandas`](../how-to-guides/use-pandas.md) method:

```python test-set=1 skip-test
from deephaven.pandas import to_pandas

data_frame = to_pandas(instrument_rolling_stats)
```

## 7. What to do next

Now that you've imported data, created tables, and manipulated static and real-time data, we suggest heading to the [Crash Course in Deephaven](../getting-started/crash-course/get-started.md) to learn more about Deephaven's real-time data platform.
