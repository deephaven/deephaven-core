---
title: Deephaven Community Core Quickstart
sidebar_label: Quickstart
---

Deephaven Community Core can be installed with [Docker](https://docs.docker.com/engine/install/) or [pip](https://packaging.python.org/en/latest/tutorials/installing-packages/). Docker-installed Deephaven runs in a [Docker container](https://www.docker.com/resources/what-container/) and requires [Docker](https://docs.docker.com/engine/install/) to be installed on your machine, while pip-installed Deephaven runs natively on your machine in a Python environment and requires the [pip](https://en.wikipedia.org/wiki/Pip_(package_manager)) package manager. If you don't have a preference, we recommend starting with [Docker](https://docs.docker.com/engine/install/).

## 1. Install and Launch Deephaven

### With Docker

Install and launch Deephaven via [Docker](https://docs.docker.com/engine/install/) with a one-line command:

```sh
docker run --rm --name deephaven -p 10000:10000 -v data:/data --env START_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE" ghcr.io/deephaven/server:latest
```

> [!CAUTION]
> Replace "YOUR_PASSWORD_HERE" with a more secure passkey to keep your session safe.

For additional configuration options, see the [install guide for Docker](./docker-install.md).

### With Pip

> [!NOTE]
> For pip-installed Deephaven, we recommend using a Python [virtual environment](https://docs.python.org/3/library/venv.html) to decouple and isolate Python installs and associated packages.
>
> To install Deephaven with pip, you must have Java installed on your computer. See [this guide](../getting-started/launch-build.md#prerequisites) for OS-specific instructions.

To install Deephaven, install the [`deephaven-server`](https://pypi.org/project/deephaven-server/) Python package:

```sh
pip3 install deephaven-server
```

Then, launch Deephaven:

```sh
deephaven server --jvm-args "-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE"
```

> [!CAUTION]
> Replace "YOUR_PASSWORD_HERE" with a more secure passkey to keep your session safe.

For more advanced configuration options, see our [pip installation guide](./pip-install.md). This includes [an additional instruction](./pip-install.md#m2-macs) needed for users on an M2 Mac.

If you prefer not to use Docker or pip, you can use the [Deephaven production application](../getting-started/production-application.md).

## 2. The Deephaven IDE

Navigate to [http://localhost:10000/](http://localhost:10000/) and enter your password in the token field:

![Screenshot of Deephaven launch page prompting for a password token](../assets/tutorials/deephaven_launch_password.png)

You're ready to go! The Deephaven IDE is a fully-featured scripting IDE. Here's a brief overview of some of its basic functionalities.

![Annotated screenshot of Deephaven IDE highlighting console, notebook controls, and save buttons](../assets/tutorials/ide_basics.png)

1. <font color="#FF40FF">Write and execute commands</font>

   Use this console to write and execute Python and Deephaven commands.

2. <font color="#FF2501">Create new notebooks</font>

   Click this button to create new notebooks where you can write scripts.

3. <font color="09F900">Edit active notebook</font>

   Edit the currently active notebook.

4. <font color="#0BFDFF">Run entire notebook</font>

   Click this button to execute all of the code in the active notebook, from top to bottom.

5. <font color="#FFFC00">Run selected code</font>

   Click this button to run only the selected code in the active notebook.

6. <font color="#FF9302">Save your work</font>

   Save your work in the active notebook. Do this often!

To learn more about the Deephaven IDE, check out [the navigating the UI guide](../how-to-guides/user-interface/navigating-the-ui.md) for a tour of the available menus and tools, and the accompanying guides on [graphical column manipulation](../how-to-guides/user-interface/formatting-tables.md), the [IDE chart-builder](../how-to-guides/user-interface/chart-builder.md), and more.

Now that you have Deephaven installed and open, the rest of this guide will briefly highlight some key features of using Deephaven.

For a more exhaustive introduction to Deephaven and an in-depth exploration of our design principles and APIs, check out our [Crash Course series](./crash-course/get-started.md).

## 3. Import static and streaming data

Deephaven empowers users to wrangle static and streaming data with ease. It supports ingesting data from [CSV files](../how-to-guides/data-import-export/csv-import.md), [Parquet files](../how-to-guides/data-import-export/parquet-import.md), and [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md).

### Load a CSV

Run the command below inside a Deephaven console to ingest a million-row CSV of crypto trades. All you need is a path or URL for the data:

```python test-set=1 order=crypto_from_csv
from deephaven import read_csv

crypto_from_csv = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
```

The table widget now in view is highly interactive:

- Click on a table and press <kbd>Ctrl</kbd> + <kbd>F</kbd> (Windows) or <kbd>⌘F</kbd> (Mac) to open quick filters.
- Click the funnel icon in the filter field to create sophisticated filters or use auto-filter UI features.
- Hover over column headers to see data types.
- Right-click headers to access more options, like adding or changing sorts.
- Click the **Table Options** hamburger menu at right to plot from the UI, create and manage columns, and download CSVs.

![Animated GIF showing Deephaven table widget interactivity such as filtering, sorting, and table options](../assets/tutorials/quickstart/quickstart-0.gif)

### Replay Historical Data

Ingesting real-time data is one of Deephaven's superpowers, and you can learn more about supported formats from the links at the end of this guide. However, streaming pipelines can be complicated to set up and are outside the scope of this discussion. For a streaming data example, we'll use Deephaven's [Table Replayer](../reference/table-operations/create/Replayer.md) to replay historical cryptocurrency data back in real time.

The following code takes fake historical crypto trade data from a CSV file and replays it in real time based on timestamps. This is only one of multiple ways to create real-time data in just a few lines of code. Replaying historical data is a great way to test real-time algorithms before deployment into production.

```python test-set=1 order=null ticking-table
from deephaven import TableReplayer, read_csv

fake_crypto_data = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
)

start_time = "2023-02-09T12:09:18 ET"
end_time = "2023-02-09T12:58:09 ET"

replayer = TableReplayer(start_time, end_time)

crypto_streaming = replayer.add_table(fake_crypto_data, "Timestamp")

replayer.start()
```

![Animated GIF of Deephaven Table Replayer streaming historical cryptocurrency trades in real time](../assets/tutorials/quickstart/quickstart-1.gif)

## 4. Working with Deephaven Tables

In Deephaven, static and dynamic data are represented as tables. New tables can be derived from parent tables, and data efficiently flows from parents to their dependents. See the concept guide on the [table update model](../conceptual/table-update-model.md) if you're interested in what's under the hood.

Deephaven represents data transformations as operations on tables. This is a familiar paradigm for data scientists using [Pandas](https://pandas.pydata.org), [Polars](https://pola.rs), [R](https://www.r-project.org/), [Matlab](https://www.mathworks.com/) and more. Deephaven's table operations are special - **they are indifferent to whether the underlying data sources are static or streaming!** This means that code written for static data will work seamlessly on live data.

There are a ton of table operations to cover, so we'll keep it short and give you the highlights.

### Manipulating data

First, reverse the ticking table with [`reverse`](../reference/table-operations/sort/reverse.md) so that the newest data appears at the top:

```python test-set=1 order=null ticking-table
crypto_streaming_rev = crypto_streaming.reverse()
```

![Animated GIF showing the table reversed so newest rows appear at the top](../assets/tutorials/quickstart/quickstart-2.gif)

> [!TIP]
> Many table operations can also be done from the UI. For example, right-click on a column header in the UI and choose **Reverse Table**.

Add a column with [`update`](../reference/table-operations/select/update.md):

```python test-set=1 order=null ticking-table
# Note the enclosing [] - this is optional when there is a single argument
crypto_streaming_rev = crypto_streaming_rev.update(["TransactionTotal = Price * Size"])
```

![Animated GIF displaying new TransactionTotal column added via update operation](../assets/tutorials/quickstart/quickstart-3.gif)

Use [`select`](../reference/table-operations/select/select.md) or [`view`](../reference/table-operations/select/view.md) to pick out particular columns:

```python test-set=1 order=null ticking-table
# Note the enclosing [] - this is not optional, since there are multiple arguments
crypto_streaming_prices = crypto_streaming_rev.view(["Instrument", "Price"])
```

![Animated GIF demonstrating selection of Instrument and Price columns with view](../assets/tutorials/quickstart/quickstart-4.gif)

Remove columns with [`drop_columns`](../reference/table-operations/select/drop-columns.md):

```python test-set=1 order=null ticking-table
# Note the lack of [] - this is permissible since there is only a single argument
crypto_streaming_rev = crypto_streaming_rev.drop_columns("TransactionTotal")
```

![Animated GIF showing removal of TransactionTotal column using drop_columns](../assets/tutorials/quickstart/quickstart-5.gif)

Next, Deephaven offers many operations for filtering tables. These include [`where`](../reference/table-operations/filter/where.md), [`where_one_of`](../reference/table-operations/filter/where-one-of.md), [`where_in`](../reference/table-operations/filter/where-in.md), [`where_not_in`](../reference/table-operations/filter/where-not-in.md), and [more](../how-to-guides/use-filters.md).

The following code uses [`where`](../reference/table-operations/filter/where.md) and [`where_one_of`](../reference/table-operations/filter/where-one-of.md) to filter for only Bitcoin transactions, and then for Bitcoin and Ethereum transactions:

```python test-set=1 order=null ticking-table
btc_streaming = crypto_streaming_rev.where("Instrument == `BTC/USD`")
etc_btc_streaming = crypto_streaming_rev.where_one_of(
    ["Instrument == `BTC/USD`", "Instrument == `ETH/USD`"]
)
```

![Animated GIF illustrating filtering a table for Bitcoin and Ethereum trades](../assets/tutorials/quickstart/quickstart-6.gif)

### Aggregating data

Deephaven's [dedicated aggregations suite](../how-to-guides/dedicated-aggregations.md) provides a number of table operations that enable efficient column-wise aggregations. These operations also support aggregations by group.

Use [`count_by`](../reference/table-operations/group-and-aggregate/countBy.md) to count the number of transactions from each exchange:

```python test-set=1 order=null ticking-table
exchange_count = crypto_streaming.count_by("Count", by="Exchange")
```

![Animated GIF showing count_by aggregation of transaction counts per exchange](../assets/tutorials/quickstart/quickstart-7.gif)

Then, get the average price for each instrument with [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md):

```python test-set=1 order=null ticking-table
instrument_avg = crypto_streaming.view(["Instrument", "Price"]).avg_by(by="Instrument")
```

![Animated GIF showing avg_by aggregation calculating average price per instrument](../assets/tutorials/quickstart/quickstart-8.gif)

Find the largest transaction per instrument with [`max_by`](../reference/table-operations/group-and-aggregate/maxBy.md):

```python test-set=1 order=null ticking-table
max_transaction = (
    crypto_streaming.update("TransactionTotal = Price * Size")
    .view(["Instrument", "TransactionTotal"])
    .max_by("Instrument")
)
```

![Animated GIF displaying max_by aggregation to find largest transaction per instrument](../assets/tutorials/quickstart/quickstart-9.gif)

While dedicated aggregations are powerful, they only enable you to perform one aggregation at a time. However, you often need to [perform multiple aggregations](../how-to-guides/combined-aggregations.md) on the same data. For this, Deephaven provides the [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) table operation and the [`deephaven.agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg) Python module.

First, use [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) to compute the [mean](../reference/table-operations/group-and-aggregate/AggAvg.md) and [standard deviation](../reference/table-operations/group-and-aggregate/AggStd.md) of each instrument's price, grouped by exchange:

```python test-set=1 order=null ticking-table
from deephaven import agg

summary_prices = crypto_streaming.agg_by(
    [agg.avg("AvgPrice=Price"), agg.std("StdPrice=Price")],
    by=["Instrument", "Exchange"],
).sort(["Instrument", "Exchange"])
```

![Animated GIF demonstrating agg_by to compute mean and standard deviation of prices grouped by instrument and exchange](../assets/tutorials/quickstart/quickstart-10.gif)

Then, add a column containing the [coefficient of variation](https://en.wikipedia.org/wiki/Coefficient_of_variation) for each instrument, measuring the relative risk of each:

```python test-set=1 order=null ticking-table
summary_prices = summary_prices.update("PctVariation = 100 * StdPrice / AvgPrice")
```

![Animated GIF showing update that adds percentage variation column to summary table](../assets/tutorials/quickstart/quickstart-11.gif)

Finally, create a minute-by-minute [Open-High-Low-Close](https://en.wikipedia.org/wiki/Open-high-low-close_chart) table using the [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) [built-in function](../reference/query-language/query-library/auto-imported-functions.md) along with [`first`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`max_`](../reference/table-operations/group-and-aggregate/AggMax.md), [`min_`](../reference/table-operations/group-and-aggregate/AggMin.md), and [`last`](../reference/table-operations/group-and-aggregate/AggLast.md):

```python test-set=1 order=null ticking-table
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
```

![Animated GIF illustrating creation of OHLC table aggregated by one-minute bins](../assets/tutorials/quickstart/quickstart-12.gif)

You may want to perform window-based calculations, compute moving or cumulative statistics, or look at pair-wise differences. Deephaven's [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`deephaven.updateby`](/core/pydoc/code/deephaven.updateby.html) Python module are the right tools for the job.

Compute the moving average and standard deviation of each instrument's price using [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md) and [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md):

```python test-set=1 order=null ticking-table
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
```

![Animated GIF showing rolling window calculations producing moving averages and standard deviations](../assets/tutorials/quickstart/quickstart-13.gif)

These statistics can be used to determine "extreme" instrument prices, where the instrument's price is significantly higher or lower than the average of the prices preceding it in the window:

```python test-set=1 order=null ticking-table
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
```

![Animated GIF highlighting extremity detection using Z-scores derived from rolling statistics](../assets/tutorials/quickstart/quickstart-14.gif)

There's a lot more to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). See the [user guide](../how-to-guides/rolling-aggregations.md) for more information.

### Combining tables

Combining datasets can often yield powerful insights. Deephaven offers two primary ways to combine tables - the _merge_ and _join_ operations.

The [`merge`](../reference/table-operations/merge/merge.md) operation stacks tables on top of one-another. This is ideal when several tables have the same schema. They can be static, ticking, or a mix of both:

```python test-set=1 order=null ticking-table
from deephaven import merge

combined_crypto = merge([fake_crypto_data, crypto_streaming]).sort("Timestamp")
```

![Animated GIF demonstrating merge operation combining static and streaming crypto tables](../assets/tutorials/quickstart/quickstart-15.gif)

The ubiquitous join operation is used to combine tables based on columns that they have in common. Deephaven offers many variants of this operation such as [`join`](../reference/table-operations/join/join.md), [`natural_join`](../reference/table-operations/join/natural-join.md), [`exact_join`](../reference/table-operations/join/exact-join.md), and [many more](../how-to-guides/joins-timeseries-range.md).

For example, read in an older dataset containing price data on the same coins from the same exchanges. Then, use [`join`](../reference/table-operations/join/join.md) to combine the aggregated prices to see how current prices compare to those in the past:

```python test-set=1 order=null ticking-table
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
```

![Animated GIF displaying join operation comparing February 2023 and September 2021 price summaries](../assets/tutorials/quickstart/quickstart-16.gif)

In many real-time data applications, data needs to be combined based on timestamps. Traditional join operations often fail this task, as they require _exact_ matches in both datasets. To remedy this, Deephaven provides [time series joins](../how-to-guides/joins-timeseries-range.md), such as [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), that can join tables on timestamps with _approximate_ matches.

Here's an example where [`aj`](../reference/table-operations/join/aj.md) is used to find the Ethereum price at or immediately preceding a Bitcoin price:

```python test-set=1  order=null ticking-table
crypto_btc = crypto_streaming.where(filters=["Instrument = `BTC/USD`"])
crypto_eth = crypto_streaming.where(filters=["Instrument = `ETH/USD`"])

time_series_join = (
    crypto_btc.view(["Timestamp", "Price"])
    .aj(crypto_eth, on="Timestamp", joins=["EthTime = Timestamp", "EthPrice = Price"])
    .rename_columns(cols=["BtcTime = Timestamp", "BtcPrice = Price"])
)
```

![Animated GIF showing time-series aj join aligning Ethereum prices to Bitcoin timestamps](../assets/tutorials/quickstart/quickstart-17.gif)

To learn more about our join methods, see the guides: [Join: Exact and Relational](../how-to-guides/joins-exact-relational.md) and [Join: Time-Series and Range](../how-to-guides/joins-timeseries-range.md).

## 5. Plot data via query or the UI

Deephaven has a rich plotting API that supports _updating, real-time plots_. It can be called programmatically:

```python test-set=1 order=null ticking-table
from deephaven.plot import Figure

btc_data = instrument_rolling_stats.where("Instrument == `BTC/USD`").reverse()

btc_plot = (
    Figure()
    .plot_xy("Bitcoin Prices", btc_data, x="Timestamp", y="Price")
    .plot_xy("Rolling Average", btc_data, x="Timestamp", y="AvgPrice30Sec")
    .show()
)
```

![Animated GIF of real-time line plot of Bitcoin price and rolling average created via code](../assets/tutorials/quickstart/quickstart-18.gif)

Or with the web UI:

![Animated GIF demonstrating plot creation through Deephaven web UI chart builder](../assets/tutorials/quickstart/quickstart-19.gif)

Additionally, Deephaven supports an integration with the popular [plotly-express library](https://plotly.com/python/) that enables real-time plotly-express plots.

<!--TODO: Link out to deephaven-plotly-express documentation -->

## 6. Export data to popular formats

It's easy to export your data out of Deephaven to popular open formats.

To export a table to a CSV file, use the [`write_csv`](../reference/data-import-export/CSV/readCsv.md) method with the table name and the location to which you want to save the file. If you are using Docker, see managing [Docker volumes](../conceptual/docker-data-volumes.md) for more information on how to save files to your local machine.

```python test-set=1 order=null
from deephaven import write_csv

write_csv(instrument_rolling_stats, "/data/crypto_prices_stats.csv")
```

If the table is dynamically updating, Deephaven will automatically snapshot the data before writing it to the file.

Similarly, for Parquet:

```python test-set=1 order=null
from deephaven.parquet import write

write(instrument_rolling_stats, "/data/crypto_prices_stats.parquet")
```

To create a static [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html), use the [`to_pandas`](../how-to-guides/use-pandas.md) method:

```python test-set=1
from deephaven.pandas import to_pandas

data_frame = to_pandas(instrument_rolling_stats)
```

## 7. What to do next

Now that you've imported data, created tables, and manipulated static and real-time data, we suggest heading to the [Crash Course in Deephaven](../getting-started/crash-course/get-started.md) to learn more about Deephaven's real-time data platform.
