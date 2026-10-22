---
title: Deephaven Community Core Quickstart for Jupyter
---

Deephaven Community Core and [Jupyter](https://jupyter.org) together make a powerful workflow for real-time data science. You can start and use a Deephaven server directly from Jupyter with [pip-installed Deephaven](../getting-started/pip-install.md). Alternatively, you can use the [Deephaven Python client](/core/client-api/python/) from Jupyter to connect to an already-running Deephaven server. This guide covers pip-installed Deephaven. For the Python client, see [Use Deephaven in Jupyter](../how-to-guides/jupyter.md#the-python-client).

> [!NOTE]
> Before you start, make sure you have what pip-installed Deephaven needs:
>
> - Java 17 or later, with your `JAVA_HOME` environment variable pointing to your Java installation.
> - Python 3.9 or later.
> - On Windows, [WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install). Do every step in this guide, including installing Jupyter, inside WSL 2.
> - On WSL 2, one of the [WSL 2 time-sync workarounds](../reference/community-questions/wsl2-clock-drift.md), applied before you run Deephaven, so ticking (live-updating) tables don't stall.
>
> See [supported operating systems](../getting-started/pip-install.md#supported-operating-systems) and [prerequisites](../getting-started/pip-install.md#prerequisites) for details.

## 1. Install Jupyter

> [!NOTE]
> We recommend using a Python [virtual environment](https://docs.python.org/3/library/venv.html) to decouple and isolate Python installs and associated packages.

Deephaven can operate in [JupyterLab](https://jupyterlab.readthedocs.io/en/latest/) or [Jupyter Notebook](https://jupyter-notebook.readthedocs.io/en/latest/). The choice is yours! Install either one with `pip`:

```sh
# install JupyterLab
pip install jupyterlab
# or install Jupyter Notebook
pip install notebook
```

## 2. Install and launch Deephaven

The [`deephaven-server`](https://pypi.org/project/deephaven-server/) package enables you to use Deephaven directly from Jupyter. The [`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/) package renders Deephaven tables and plots in Jupyter. Install them both in the same environment as your Jupyter installation:

```sh
pip install deephaven-server deephaven-ipywidgets
```

If you plan to plot with [Deephaven Express](/core/plotly/docs/) after this guide, also run `pip install deephaven-plugin-plotly-express` now. Plugin packages must be installed before you start the server.

Now, start an instance of Jupyter:

```sh
# start JupyterLab
jupyter lab
# or start Jupyter Notebook
jupyter notebook
```

<!--TODO: Confirm whether DeephavenWidget works with a PSK-authenticated embedded server. If it does, switch this example to PSK. If it doesn't, link the issue that tracks PSK support here. (deephaven-ipywidgets#38 tracks shared export tickets, not PSK.) -->

The following code block starts a Deephaven server on port `10000` with a 4 GB maximum heap and [anonymous authentication](../how-to-guides/authentication/auth-anon.md). Run it in your Jupyter instance:

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

> [!NOTE]
> On M2 Macs, add `"-Dprocess.info.system-info.enabled=false"` to the `jvm_args` list.

> [!CAUTION]
> When using Deephaven from Jupyter, you **must** create a `deephaven_server.Server` **before** importing the `deephaven` package. If you import `deephaven` first, the import raises a `RuntimeError`.

> [!NOTE]
> Anonymous authentication provides no application security.

For more advanced configuration options, see the [pip installation guide](../getting-started/pip-install.md).

## 3. Import static and streaming data

Deephaven empowers you to wrangle static and streaming data with ease. It can ingest data from [CSV files](../how-to-guides/data-import-export/csv-import.md), [Parquet files](../how-to-guides/data-import-export/parquet-import.md), and [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md).

### Load a CSV

Run the code below in a Jupyter cell to ingest a million-row CSV of crypto trades with [`read_csv`](../reference/data-import-export/CSV/readCsv.md). All you need is a path or URL for the data:

```python test-set=1 order=crypto_from_csv skip-test
from deephaven import read_csv

crypto_from_csv = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
```

Display the resulting table with `DeephavenWidget` from [`deephaven-ipywidgets`](https://pypi.org/project/deephaven-ipywidgets/), which renders Deephaven tables and plots right inside Jupyter:

```python skip-test
from deephaven_ipywidgets import DeephavenWidget

display(DeephavenWidget(crypto_from_csv))
```

The table widget now in view is highly interactive:

- Click on a table and press <kbd>Ctrl</kbd> + <kbd>F</kbd> (Windows/Linux) or <kbd>⌘</kbd> + <kbd>F</kbd> (Mac) to open the [Quick Filters](../how-to-guides/user-interface/filters.md#quick-filters) bar, a filter row above the column headers.
- Click the funnel icon in a Quick Filters field to open the [Advanced Filters](../how-to-guides/user-interface/filters.md#advanced-filters) panel and build more detailed filters.
- Hover over column headers to see data types.
- Right-click headers to access more options, like adding or changing sorts.
- Click the **Table Options** hamburger menu at right to plot from the UI, create and manage columns, and download CSVs.

![Animated GIF showing interactive Deephaven table widget rendered inside a Jupyter notebook](../assets/tutorials/quickstart-jupyter/jquickstart-1.gif)

### Replay historical data

Ingesting real-time data is one of Deephaven's core strengths. However, streaming pipelines such as [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md) can be complicated to set up and are outside the scope of this guide. For a streaming data example, this guide uses Deephaven's [`TableReplayer`](../reference/table-operations/create/Replayer.md) to replay historical cryptocurrency data in real time.

The following code takes fake historical crypto trade data from a CSV file and replays it in real time based on timestamps. Replaying a table is one of several ways to create real-time data in just a few lines of code. Replaying historical data is a great way to test real-time algorithms before deployment into production.

```python test-set=1 order=null ticking-table skip-test
from deephaven import TableReplayer, read_csv

fake_crypto_data = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
)

start_time = "2023-02-09T12:09:18 ET"
end_time = "2023-02-09T12:58:09 ET"

replayer = TableReplayer(start_time, end_time)

crypto_streaming = replayer.add_table(fake_crypto_data, "Timestamp")

replayer.start()

display(DeephavenWidget(crypto_streaming))
```

![Animated GIF showing live streaming cryptocurrency trades table updating in real time](../assets/tutorials/quickstart-jupyter/jquickstart-2.gif)

## 4. Work with Deephaven tables

Deephaven represents both static and streaming data as tables. You derive new tables from parent tables, and data efficiently flows from parents to their dependents. See the concept guide on the [table update model](../conceptual/table-update-model.md) if you're interested in what's under the hood.

Deephaven represents data transformations as operations on tables. This is a familiar paradigm for data scientists using [pandas](https://pandas.pydata.org), [Polars](https://pola.rs), [R](https://www.r-project.org/), [MATLAB](https://www.mathworks.com/), and more.

Deephaven's table operations are special — **they work the same way whether the underlying data is static or streaming!** This means that code written for static data generally works seamlessly on live data. A few formulas are exceptions, such as those that use the [special variables](../reference/query-language/variables/special-variables.md) `i`, `ii`, and `k`.

There are many table operations to cover, so this guide keeps it short and gives you the highlights.

### Manipulate data

First, reverse the ticking (live-updating) table with [`reverse`](../reference/table-operations/sort/reverse.md) so that the newest data appears at the top:

```python test-set=1 order=null ticking-table skip-test
crypto_streaming_rev = crypto_streaming.reverse()
display(DeephavenWidget(crypto_streaming_rev))
```

![Animated GIF showing table reversed so most recent rows appear first](../assets/tutorials/quickstart-jupyter/jquickstart-3.gif)

> [!TIP]
> You can also perform many table operations from the UI. For example, right-click on a column header in the UI and choose **Reverse Table**.

Add a column with [`update`](../reference/table-operations/select/update.md):

```python test-set=1 order=null ticking-table skip-test
# Note the enclosing [] — this is optional when there is a single argument
crypto_streaming_rev = crypto_streaming_rev.update(["TransactionTotal = Price * Size"])
display(DeephavenWidget(crypto_streaming_rev))
```

![Animated GIF showing new TransactionTotal column added via update](../assets/tutorials/quickstart-jupyter/jquickstart-4.gif)

Use [`view`](../reference/table-operations/select/view.md) to pick out particular columns. [`select`](../reference/table-operations/select/select.md) does the same but computes the columns and stores them in memory, while `view` computes them on demand:

```python test-set=1 order=null ticking-table skip-test
# Note the enclosing [] — this is not optional, since there are multiple arguments
crypto_streaming_prices = crypto_streaming_rev.view(["Instrument", "Price"])
display(DeephavenWidget(crypto_streaming_prices))
```

![Animated GIF showing table reduced to Instrument and Price columns](../assets/tutorials/quickstart-jupyter/jquickstart-5.gif)

Remove columns with [`drop_columns`](../reference/table-operations/select/drop-columns.md):

```python test-set=1 order=null ticking-table skip-test
# Note the lack of [] — this is permissible since there is only a single argument
crypto_streaming_rev = crypto_streaming_rev.drop_columns("TransactionTotal")
display(DeephavenWidget(crypto_streaming_rev))
```

![Animated GIF showing TransactionTotal column removed using drop_columns](../assets/tutorials/quickstart-jupyter/jquickstart-6.gif)

Next, filter the table. Deephaven offers many filtering operations, including [`where`](../reference/table-operations/filter/where.md), [`where_one_of`](../reference/table-operations/filter/where-one-of.md), [`where_in`](../reference/table-operations/filter/where-in.md), and [`where_not_in`](../reference/table-operations/filter/where-not-in.md). See the [filtering guide](../how-to-guides/use-filters.md) for the full set.

The following code uses [`where`](../reference/table-operations/filter/where.md) and [`where_one_of`](../reference/table-operations/filter/where-one-of.md) to filter for only Bitcoin transactions, and then for Bitcoin and Ethereum transactions:

```python test-set=1 order=null ticking-table skip-test
btc_streaming = crypto_streaming_rev.where("Instrument == `BTC/USD`")
display(DeephavenWidget(btc_streaming))

eth_btc_streaming = crypto_streaming_rev.where_one_of(
    ["Instrument == `BTC/USD`", "Instrument == `ETH/USD`"]
)
display(DeephavenWidget(eth_btc_streaming))
```

![Animated GIF filtering table to show Bitcoin and Ethereum trades](../assets/tutorials/quickstart-jupyter/jquickstart-7.gif)

### Aggregate data

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

Find the largest transaction per instrument with [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md). This example starts from `crypto_streaming`, so it recomputes the `TransactionTotal` column first:

```python test-set=1 order=null ticking-table skip-test
max_transaction = (
    crypto_streaming.update("TransactionTotal = Price * Size")
    .view(["Instrument", "TransactionTotal"])
    .max_by("Instrument")
)
display(DeephavenWidget(max_transaction))
```

![Animated GIF showing largest transaction per instrument using max_by](../assets/tutorials/quickstart-jupyter/jquickstart-10.gif)

While [dedicated aggregations](../how-to-guides/dedicated-aggregations.md) are powerful, they only perform one aggregation at a time. However, you often need to [perform multiple aggregations](../how-to-guides/combined-aggregations.md) on the same data. For this, Deephaven provides the [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) table operation and the [`deephaven.agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg) Python module.

First, use [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) to compute the [mean](../reference/table-operations/group-and-aggregate/AggAvg.md) and [standard deviation](../reference/table-operations/group-and-aggregate/AggStd.md) of the price, grouped by instrument and exchange, and [`sort`](../reference/table-operations/sort/sort.md) the result:

```python test-set=1 order=null ticking-table skip-test
from deephaven import agg

summary_prices = crypto_streaming.agg_by(
    [agg.avg("AvgPrice = Price"), agg.std("StdPrice = Price")],
    by=["Instrument", "Exchange"],
).sort(["Instrument", "Exchange"])
display(DeephavenWidget(summary_prices))
```

![Animated GIF showing summary table of average and standard deviation price per instrument and exchange](../assets/tutorials/quickstart-jupyter/jquickstart-11.gif)

Then, add a column containing the [coefficient of variation](https://en.wikipedia.org/wiki/Coefficient_of_variation) for each instrument and exchange, measuring the relative risk of each:

```python test-set=1 order=null ticking-table skip-test
summary_prices = summary_prices.update("PctVariation = 100 * StdPrice / AvgPrice")
display(DeephavenWidget(summary_prices))
```

![Animated GIF showing summary table updated with percentage variation column](../assets/tutorials/quickstart-jupyter/jquickstart-12.gif)

Finally, create a minute-by-minute [Open-High-Low-Close](https://en.wikipedia.org/wiki/Open-high-low-close_chart) table. First, bin each timestamp to the minute with [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)), one of Deephaven's [built-in time functions](../reference/query-language/query-library/auto-imported/time.md). Then, aggregate each bin with [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) and the [`first`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`max_`](../reference/table-operations/group-and-aggregate/AggMax.md), [`min_`](../reference/table-operations/group-and-aggregate/AggMin.md), and [`last`](../reference/table-operations/group-and-aggregate/AggLast.md) aggregations:

```python test-set=1 order=null ticking-table skip-test
ohlc_by_minute = (
    crypto_streaming.update("BinnedTimestamp = lowerBin(Timestamp, MINUTE)")
    .agg_by(
        [
            agg.first("Open = Price"),
            agg.max_("High = Price"),
            agg.min_("Low = Price"),
            agg.last("Close = Price"),
        ],
        by=["Instrument", "BinnedTimestamp"],
    )
    .sort(["Instrument", "BinnedTimestamp"])
)
display(DeephavenWidget(ohlc_by_minute))
```

![Animated GIF showing minute-bucket OHLC table generated with agg_by](../assets/tutorials/quickstart-jupyter/jquickstart-13.gif)

### Window calculations

You may want to perform window-based calculations, compute rolling or cumulative statistics, or look at pairwise differences. Deephaven's [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`deephaven.updateby`](/core/pydoc/code/deephaven.updateby.html) Python module are the right tools for the job.

Compute the rolling average and standard deviation of each instrument's price using [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md) and [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md):

```python test-set=1 order=null ticking-table skip-test
import deephaven.updateby as uby

instrument_rolling_stats = crypto_streaming.update_by(
    [
        uby.rolling_avg_time("Timestamp", "AvgPrice30Sec = Price", "PT30s"),
        uby.rolling_avg_time("Timestamp", "AvgPrice5Min = Price", "PT5m"),
        uby.rolling_std_time("Timestamp", "StdPrice30Sec = Price", "PT30s"),
        uby.rolling_std_time("Timestamp", "StdPrice5Min = Price", "PT5m"),
    ],
    by="Instrument",
).reverse()
display(DeephavenWidget(instrument_rolling_stats))
```

![Animated GIF showing rolling averages and standard deviations calculated with update_by](../assets/tutorials/quickstart-jupyter/jquickstart-14.gif)

Use these statistics to find "extreme" instrument prices, where the price is more than 1.645 standard deviations above or below the rolling average for the window that ends at that row's timestamp:

```python test-set=1 order=null ticking-table skip-test
instrument_extremity = instrument_rolling_stats.update(
    [
        "Z30Sec = (Price - AvgPrice30Sec) / StdPrice30Sec",
        "Z5Min = (Price - AvgPrice5Min) / StdPrice5Min",
        "Extreme30Sec = Math.abs(Z30Sec) > 1.645",
        "Extreme5Min = Math.abs(Z5Min) > 1.645",
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

There's a lot more to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). See the guide on [rolling aggregations](../how-to-guides/rolling-aggregations.md) for more information.

### Combine tables

Combining datasets can often yield powerful insights. Deephaven offers two primary ways to combine tables: [merges](../reference/table-operations/merge/merge.md) and joins. Deephaven has two kinds of joins. [Exact and relational joins](../how-to-guides/joins-exact-relational.md) match rows on equal key values. [Time-series and range joins](../how-to-guides/joins-timeseries-range.md) match rows on the closest value or a range.

The [`merge`](../reference/table-operations/merge/merge.md) operation stacks tables on top of one another. The tables must have the same columns with the same types. The tables you merge can be static, ticking, or a mix of both. For example, combine the static September 2021 trades with the ticking February 2023 replay:

```python test-set=1 order=null ticking-table skip-test
from deephaven import merge

combined_crypto = merge([crypto_from_csv, crypto_streaming]).sort("Timestamp")
display(DeephavenWidget(combined_crypto))
```

The ubiquitous join operation combines tables based on columns they have in common. Exact and relational joins include [`join`](../reference/table-operations/join/join.md), [`natural_join`](../reference/table-operations/join/natural-join.md), and [`exact_join`](../reference/table-operations/join/exact-join.md). The [exact and relational joins guide](../how-to-guides/joins-exact-relational.md) covers each of them.

For example, summarize the older September 2021 trades in `crypto_from_csv`, the table you loaded at the start of this guide. Then, use [`rename_columns`](../reference/table-operations/select/rename-columns.md) and [`join`](../reference/table-operations/join/join.md) to combine that summary with `summary_prices`, the summary of the replayed February 2023 trades from [Aggregate data](#aggregate-data). The result shows both periods' price statistics side by side for the instruments and exchanges that appear in both datasets. A `join` drops rows that have no match, so PAXG/USD and YFI/USD aren't in the result:

```python test-set=1 order=null ticking-table skip-test
more_summary_prices = crypto_from_csv.agg_by(
    [agg.avg("AvgPrice = Price"), agg.std("StdPrice = Price")],
    by=["Instrument", "Exchange"],
).sort(["Instrument", "Exchange"])

price_comparison = (
    summary_prices.drop_columns("PctVariation")
    .rename_columns(["AvgPriceFeb2023 = AvgPrice", "StdPriceFeb2023 = StdPrice"])
    .join(
        more_summary_prices,
        on=["Instrument", "Exchange"],
        joins=["AvgPriceSep2021 = AvgPrice", "StdPriceSep2021 = StdPrice"],
    )
)
display(DeephavenWidget(price_comparison))
```

![Animated GIF comparing February 2023 and September 2021 price summaries via join](../assets/tutorials/quickstart-jupyter/jquickstart-17.gif)

Many real-time data applications combine data based on timestamps. [Exact and relational joins](../how-to-guides/joins-exact-relational.md) can't do this, because they require _exact_ matches in both datasets. To remedy this, Deephaven provides [time-series joins](../how-to-guides/joins-timeseries-range.md#as-of-time-series-joins), such as [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), that join tables on timestamps with _approximate_ matches.

The following example uses [`aj`](../reference/table-operations/join/aj.md) to find the Ethereum price at or immediately before the timestamp of each Bitcoin trade:

```python test-set=1 order=null ticking-table skip-test
crypto_btc = crypto_streaming.where("Instrument == `BTC/USD`")
crypto_eth = crypto_streaming.where("Instrument == `ETH/USD`")

time_series_join = (
    crypto_btc.view(["Timestamp", "Price"])
    .aj(crypto_eth, on="Timestamp", joins=["EthTime = Timestamp", "EthPrice = Price"])
    .rename_columns(["BtcTime = Timestamp", "BtcPrice = Price"])
)
display(DeephavenWidget(time_series_join))
```

![Animated GIF displaying time-series join aligning Ethereum prices with Bitcoin timestamps](../assets/tutorials/quickstart-jupyter/jquickstart-18.gif)

To learn more about our join methods, see the guides on [exact and relational joins](../how-to-guides/joins-exact-relational.md) and [time-series and range joins](../how-to-guides/joins-timeseries-range.md).

## 5. Plot data

Deephaven has rich plotting libraries that support _updating, real-time plots_. The following example uses [`plot_xy`](../reference/plot/plot.md) from Deephaven's [built-in plotting API](../how-to-guides/plotting/api-plotting.md). The code plots the Bitcoin price and its 30-second rolling average, and `DeephavenWidget` renders the resulting figure:

```python test-set=1 order=null ticking-table skip-test
from deephaven.plot import Figure

# instrument_rolling_stats is newest-first; reverse it back to time order for plotting
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

For most new work, use [Deephaven Express](/core/plotly/docs/), built on top of the popular [Plotly Express](https://plotly.com/python/) library, instead of the [built-in plotting API](../how-to-guides/plotting/api-plotting.md). Deephaven Express needs the `deephaven-plugin-plotly-express` package, which you must install before you start the server, as shown in [Install and launch Deephaven](#2-install-and-launch-deephaven). To compare Deephaven's plotting options, see the [plotting overview](../how-to-guides/plotting/overview.md).

## 6. Export data to popular formats

It's easy to export your data out of Deephaven to popular open formats such as CSV and Parquet, or to convert tables to pandas DataFrames.

To export a table to a CSV file, use [`write_csv`](../reference/data-import-export/CSV/writeCsv.md) with the table and the path where you want to save the file.

A ticking table can change while you write it, and code in a Jupyter cell doesn't hold the [lock](../conceptual/query-engine/engine-locking.md) that keeps it consistent. Take a static [`snapshot`](../reference/table-operations/snapshot/snapshot.md) of a ticking table first. The following code writes the CSV to the current working directory:

```python test-set=1 order=null skip-test
import os
from deephaven import write_csv

stats_snapshot = instrument_rolling_stats.snapshot()

write_csv(stats_snapshot, os.getcwd() + "/crypto_prices_stats.csv")
```

Similarly, use [`write`](../reference/data-import-export/Parquet/writeTable.md) for Parquet:

```python test-set=1 order=null skip-test
from deephaven.parquet import write

write(stats_snapshot, os.getcwd() + "/crypto_prices_stats.parquet")
```

To create a static [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html), use the [`to_pandas`](../reference/pandas/to-pandas.md) function. It takes the [snapshot](../reference/table-operations/snapshot/snapshot.md) for you, so you can pass the ticking table directly:

```python test-set=1 order=null skip-test
from deephaven.pandas import to_pandas

data_frame = to_pandas(instrument_rolling_stats)
```

See the [pandas guide](../how-to-guides/use-pandas.md) for more details.

## 7. What to do next

Now that you've imported data, created tables, and manipulated static and real-time data, we suggest heading to [_A Crash Course in Deephaven_](../getting-started/crash-course/get-started.md) to learn more about Deephaven's real-time data platform.
