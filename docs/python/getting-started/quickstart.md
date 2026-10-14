---
title: Deephaven Community Core Quickstart
---

This guide shows you how to install and launch Deephaven Community Core, then takes you on a short tour of the Deephaven IDE: importing static and streaming data, transforming and aggregating tables, plotting, and exporting results.

You can install Deephaven Community Core with [Docker](https://docs.docker.com/engine/install/), [pip](https://packaging.python.org/en/latest/tutorials/installing-packages/), or the [production application](./production-application.md), which runs Deephaven natively on your machine. If you don't have a preference, we recommend starting with Docker.

## 1. Install and launch Deephaven

### With Docker

Docker-installed Deephaven runs in a [Docker container](https://www.docker.com/resources/what-container/), so you need [Docker](https://docs.docker.com/engine/install/) installed on your machine. Install and launch Deephaven with a one-line command:

```sh
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE" ghcr.io/deephaven/server:latest
```

> [!CAUTION]
> Replace `YOUR_PASSWORD_HERE` with a secure password of your own. The `-Dauthentication.psk` option sets the password (a pre-shared key) that you use to log in to Deephaven.

The `-v` option mounts the `data` folder in your current directory at `/data` inside the container, so files that Deephaven writes to `/data` appear in that folder on your machine. Docker creates the folder if it doesn't exist.

For additional configuration options, see the [install guide for Docker](./docker-install.md).

### With pip

To install Deephaven with pip, you need Java 17 or later and Python 3.9 or later, and your `JAVA_HOME` environment variable must point to your Java installation. On Windows, pip-installed Deephaven also requires [WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install). See the [pip install prerequisites](./pip-install.md#prerequisites) for details.

> [!NOTE]
> For pip-installed Deephaven, we recommend using a Python [virtual environment](https://docs.python.org/3/library/venv.html) to decouple and isolate Python installs and associated packages.

To install Deephaven, install the [`deephaven-server`](https://pypi.org/project/deephaven-server/) Python package. This guide's plotting example also uses [Deephaven Express](/core/plotly/docs/), so install its plugin at the same time:

```sh
pip3 install deephaven-server deephaven-plugin-plotly-express
```

Then, launch Deephaven:

```sh
deephaven server --jvm-args "-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE"
```

If you use an M2 Mac, add `-Dprocess.info.system-info.enabled=false` to `--jvm-args`, as described in [Start a Deephaven server](./pip-install.md#start-a-deephaven-server).

> [!CAUTION]
> Replace `YOUR_PASSWORD_HERE` with a secure password of your own. The `-Dauthentication.psk` option sets the password (a pre-shared key) that you use to log in to Deephaven.

For more advanced configuration options, see the [pip installation guide](./pip-install.md).

### With the production application

To run Deephaven without Docker or pip, follow the [production application guide](./production-application.md). This guide's plotting example uses [Deephaven Express](/core/plotly/docs/), so after you [create the virtual environment](./production-application.md#create-the-virtual-environment), also run `pip install deephaven-plugin-plotly-express` in the activated virtual environment before you [start the server](./production-application.md#run-the-server).

## 2. The Deephaven IDE

If you started pip-installed Deephaven with the `deephaven server` command, by default it opens the IDE in your browser and logs you in automatically. Otherwise, navigate to [http://localhost:10000/](http://localhost:10000/) and enter your password in the token field.

![Screenshot of Deephaven launch page prompting for a password token](../assets/tutorials/deephaven_launch_password.png)

You're ready to go. The Deephaven IDE is a full scripting environment. Here's a brief overview of its basic features.

![Annotated screenshot of Deephaven IDE highlighting console, notebook controls, and save buttons](../assets/tutorials/ide_basics.png)

1. <font color="#FF40FF">Write and execute commands</font>

   Use this console to write and execute Python and Deephaven commands.

2. <font color="#FF2501">Create new notebooks</font>

   Click this button to create new notebooks where you can write scripts.

3. <font color="#09F900">Edit active notebook</font>

   Edit the currently active notebook.

4. <font color="#0BFDFF">Run entire notebook</font>

   Click this button to execute all of the code in the active notebook, from top to bottom.

5. <font color="#FFFC00">Run selected code</font>

   Click this button to run only the selected code in the active notebook.

6. <font color="#FF9302">Save your work</font>

   Save your work in the active notebook. Do this often!

Now that you have Deephaven installed and open, you can explore some of its key features in the rest of this guide.

## 3. Import static and streaming data

Deephaven works with both static and streaming data, and it can ingest data from [CSV files](../how-to-guides/data-import-export/csv-import.md), [Parquet files](../how-to-guides/data-import-export/parquet-import.md), [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md), and more. This section loads a static CSV file, then replays historical data to simulate a stream.

### Load a CSV

Run the command below inside a Deephaven console to ingest a million-row CSV of crypto trades with [`read_csv`](../reference/data-import-export/CSV/readCsv.md). All you need is a path or URL for the data:

```python test-set=1 order=crypto_from_csv
from deephaven import read_csv

crypto_from_csv = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)
```

The table widget now in view is highly interactive:

- Click on a table and press <kbd>Ctrl</kbd> + <kbd>F</kbd> (Windows/Linux) or <kbd>⌘</kbd> + <kbd>F</kbd> (Mac) to open the [Quick Filters](../how-to-guides/user-interface/filters.md#quick-filters) bar, a filter row above the column headers.
- Click the funnel icon in a quick filter field to open the [Advanced Filters](../how-to-guides/user-interface/filters.md#advanced-filters) panel and build more detailed filters.
- Hover over column headers to see data types.
- Right-click headers to access more options, like adding or changing sorts.
- Click the **Table Options** hamburger menu at right to plot from the UI, create and manage columns, and download CSVs.

![Animated GIF showing Deephaven table widget interactivity such as filtering, sorting, and table options](../assets/tutorials/quickstart/quickstart-0.gif)

### Replay historical data

Ingesting real-time data is one of Deephaven's core strengths. However, streaming pipelines can be complicated to set up and are outside the scope of this guide. For a streaming data example, this guide uses Deephaven's [`TableReplayer`](../reference/table-operations/create/Replayer.md) to replay historical cryptocurrency data in real time.

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

![Animated GIF of Deephaven TableReplayer streaming historical cryptocurrency trades in real time](../assets/tutorials/quickstart/quickstart-1.gif)

## 4. Work with Deephaven tables

Deephaven represents both static and streaming data as tables. You can derive new tables from parent tables, and data flows efficiently from parents to their dependents. See the concept guide on the [table update model](../conceptual/table-update-model.md) if you're interested in what's under the hood.

Deephaven represents data transformations as operations on tables. This is a familiar paradigm for data scientists using [pandas](https://pandas.pydata.org), [Polars](https://pola.rs), [R](https://www.r-project.org/), [MATLAB](https://www.mathworks.com/), and more. Deephaven's table operations have one key difference — **they work the same way whether the underlying data is static or streaming.** This means that code written for static data generally also works on streaming data. A few formulas are exceptions, such as those that use the [special variables](../reference/query-language/variables/special-variables.md) `i`, `ii`, and `k`.

There are many table operations to cover, so this guide keeps it short and covers the highlights.

### Manipulate data

First, reverse the ticking (live-updating) `crypto_streaming` table with [`reverse`](../reference/table-operations/sort/reverse.md) so that the newest data appears at the top:

```python test-set=1 order=null ticking-table
crypto_streaming_rev = crypto_streaming.reverse()
```

![Animated GIF showing the table reversed so newest rows appear at the top](../assets/tutorials/quickstart/quickstart-2.gif)

> [!TIP]
> You can also perform many table operations from the UI. For example, right-click on a column header in the UI and choose **Reverse Table**.

Add a column with [`update`](../reference/table-operations/select/update.md):

```python test-set=1 order=null ticking-table
# Note the enclosing [] — this is optional when there is a single argument
crypto_streaming_rev = crypto_streaming_rev.update(["TransactionTotal = Price * Size"])
```

![Animated GIF displaying new TransactionTotal column added via update operation](../assets/tutorials/quickstart/quickstart-3.gif)

Use [`view`](../reference/table-operations/select/view.md) to pick out particular columns. [`select`](../reference/table-operations/select/select.md) does the same but computes the columns and stores them in memory, while `view` computes them on demand:

```python test-set=1 order=null ticking-table
# Note the enclosing [] — this is not optional, since there are multiple arguments
crypto_streaming_prices = crypto_streaming_rev.view(["Instrument", "Price"])
```

![Animated GIF demonstrating selection of Instrument and Price columns with view](../assets/tutorials/quickstart/quickstart-4.gif)

Remove columns with [`drop_columns`](../reference/table-operations/select/drop-columns.md):

```python test-set=1 order=null ticking-table
# Note the lack of [] — this is permissible since there is only a single argument
crypto_streaming_rev = crypto_streaming_rev.drop_columns("TransactionTotal")
```

![Animated GIF showing removal of TransactionTotal column using drop_columns](../assets/tutorials/quickstart/quickstart-5.gif)

Deephaven offers many operations for filtering tables, including [`where`](../reference/table-operations/filter/where.md), [`where_one_of`](../reference/table-operations/filter/where-one-of.md), [`where_in`](../reference/table-operations/filter/where-in.md), [`where_not_in`](../reference/table-operations/filter/where-not-in.md), and others. See the [filtering guide](../how-to-guides/use-filters.md) for the full set.

The following code uses `where` and `where_one_of` to filter for only Bitcoin transactions, and then for Bitcoin and Ethereum transactions:

```python test-set=1 order=null ticking-table
btc_streaming = crypto_streaming_rev.where("Instrument == `BTC/USD`")
eth_btc_streaming = crypto_streaming_rev.where_one_of(
    ["Instrument == `BTC/USD`", "Instrument == `ETH/USD`"]
)
```

![Animated GIF illustrating filtering a table for Bitcoin and Ethereum trades](../assets/tutorials/quickstart/quickstart-6.gif)

### Aggregate data

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
    .max_by(by="Instrument")
)
```

![Animated GIF displaying max_by aggregation to find largest transaction per instrument](../assets/tutorials/quickstart/quickstart-9.gif)

Each dedicated aggregation performs one aggregation at a time. You often need to [perform multiple aggregations](../how-to-guides/combined-aggregations.md) on the same data. For this, Deephaven provides the [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) table operation and the [`deephaven.agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg) Python module.

First, use `agg_by` to compute the [mean](../reference/table-operations/group-and-aggregate/AggAvg.md) and [standard deviation](../reference/table-operations/group-and-aggregate/AggStd.md) of the price, grouped by instrument and exchange:

```python test-set=1 order=null ticking-table
from deephaven import agg

summary_prices = crypto_streaming.agg_by(
    [agg.avg("AvgPrice = Price"), agg.std("StdPrice = Price")],
    by=["Instrument", "Exchange"],
).sort(["Instrument", "Exchange"])
```

![Animated GIF demonstrating agg_by to compute mean and standard deviation of prices grouped by instrument and exchange](../assets/tutorials/quickstart/quickstart-10.gif)

Then, add a column containing the [coefficient of variation](https://en.wikipedia.org/wiki/Coefficient_of_variation) for each instrument and exchange, measuring the relative risk of each:

```python test-set=1 order=null ticking-table
summary_prices = summary_prices.update("PctVariation = 100 * StdPrice / AvgPrice")
```

![Animated GIF showing update that adds percentage variation column to summary table](../assets/tutorials/quickstart/quickstart-11.gif)

Finally, create a minute-by-minute [Open-High-Low-Close](https://en.wikipedia.org/wiki/Open-high-low-close_chart) table using [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)), one of Deephaven's [built-in time functions](../reference/query-language/query-library/auto-imported/time.md), along with [`first`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`max_`](../reference/table-operations/group-and-aggregate/AggMax.md), [`min_`](../reference/table-operations/group-and-aggregate/AggMin.md), and [`last`](../reference/table-operations/group-and-aggregate/AggLast.md):

```python test-set=1 order=null ticking-table
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
```

![Animated GIF illustrating creation of OHLC table aggregated by one-minute bins](../assets/tutorials/quickstart/quickstart-12.gif)

### Window calculations

You may want to perform window-based calculations, compute moving or cumulative statistics, or look at pairwise differences. Deephaven's [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation and the [`deephaven.updateby`](/core/pydoc/code/deephaven.updateby.html) Python module are the right tools for the job.

Compute the moving average and standard deviation of each instrument's price using [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md) and [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md):

```python test-set=1 order=null ticking-table
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
```

![Animated GIF showing rolling window calculations producing moving averages and standard deviations](../assets/tutorials/quickstart/quickstart-13.gif)

Use these statistics to find "extreme" instrument prices, where the price is more than 1.645 standard deviations above or below the rolling average for the window that ends at that row's timestamp:

```python test-set=1 order=null ticking-table
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
```

![Animated GIF highlighting extremity detection using Z-scores derived from rolling statistics](../assets/tutorials/quickstart/quickstart-14.gif)

There's a lot more to `update_by`. See the guide on [rolling aggregations](../how-to-guides/rolling-aggregations.md) for more information.

### Combine tables

Combining datasets can often yield powerful insights. Deephaven offers two primary ways to combine tables — the _merge_ and _join_ operations.

The [`merge`](../reference/table-operations/merge/merge.md) operation stacks tables on top of one another. The tables must have the same columns with the same types. The tables you merge can be static, ticking, or a mix of both. For example, combine the static September 2021 trades with the ticking February 2023 replay:

```python test-set=1 order=null ticking-table
from deephaven import merge

combined_crypto = merge([crypto_from_csv, crypto_streaming]).sort("Timestamp")
```

![Animated GIF demonstrating merge operation combining static and streaming crypto tables](../assets/tutorials/quickstart/quickstart-15.gif)

The ubiquitous join operation combines tables based on columns that they have in common. Deephaven offers many variants of this operation, such as [`join`](../reference/table-operations/join/join.md), [`natural_join`](../reference/table-operations/join/natural-join.md), and [`exact_join`](../reference/table-operations/join/exact-join.md). The [exact and relational joins guide](../how-to-guides/joins-exact-relational.md) covers the rest.

For example, summarize the older September 2021 trades in `crypto_from_csv`, the table you loaded at the start of this guide. Then, use `join` to combine that summary with the February 2023 `summary_prices` table from [Aggregate data](#aggregate-data) to compare February 2023 prices with September 2021 prices:

```python test-set=1 order=null ticking-table
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
```

![Animated GIF displaying join operation comparing February 2023 and September 2021 price summaries](../assets/tutorials/quickstart/quickstart-16.gif)

Many real-time data applications combine data based on timestamps. Traditional join operations often fail this task, as they require _exact_ matches in both datasets. To remedy this, Deephaven provides [time-series joins](../how-to-guides/joins-timeseries-range.md), such as [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), that can join tables on timestamps with _approximate_ matches.

The following example uses `aj` to find the Ethereum price at or immediately before the timestamp of each Bitcoin trade:

```python test-set=1 order=null ticking-table
crypto_btc = crypto_streaming.where("Instrument == `BTC/USD`")
crypto_eth = crypto_streaming.where("Instrument == `ETH/USD`")

time_series_join = (
    crypto_btc.view(["Timestamp", "Price"])
    .aj(crypto_eth, on="Timestamp", joins=["EthTime = Timestamp", "EthPrice = Price"])
    .rename_columns(["BtcTime = Timestamp", "BtcPrice = Price"])
)
```

![Animated GIF showing time-series aj join aligning Ethereum prices to Bitcoin timestamps](../assets/tutorials/quickstart/quickstart-17.gif)

## 5. Plot data via query or the UI

Deephaven supports _updating, real-time plots_. [Deephaven Express](/core/plotly/docs/), a plotting plugin, is the recommended plotting library for most tasks. It builds real-time [Plotly Express](https://plotly.com/python/plotly-express/) plots directly from Deephaven tables. See [how to plot real-time data](../how-to-guides/plotting/overview.md) to compare Deephaven's plotting options.

The `ghcr.io/deephaven/server` Docker image includes Deephaven Express. If you installed with pip, you installed it [along with Deephaven](#with-pip). If you run the production application, you installed it [before starting the server](#with-the-production-application).

The following example uses Deephaven Express to plot the Bitcoin price and its 30-second rolling average:

```python test-set=1 order=null ticking-table
import deephaven.plot.express as dx

btc_data = instrument_rolling_stats.where("Instrument == `BTC/USD`").reverse()

btc_plot = dx.line(btc_data, x="Timestamp", y=["Price", "AvgPrice30Sec"])
```

![Animated GIF of real-time line plot of Bitcoin price and rolling average created via code](../assets/tutorials/quickstart/quickstart-18.gif)

To use the [built-in plotting API](../how-to-guides/plotting/api-plotting.md) instead, start with [`plot_xy`](../reference/plot/plot.md).

You can also create plots from the web UI:

![Animated GIF demonstrating plot creation through Deephaven web UI chart builder](../assets/tutorials/quickstart/quickstart-19.gif)

## 6. Export data

You can export your data from Deephaven to popular open file formats or to a pandas DataFrame.

The file examples below write to `/data`, the data directory in the Deephaven Docker image. If you started Deephaven with the Docker command above, the files appear in the `data` folder of the directory where you ran it. To mount other directories, see [Add a second volume](./docker-install.md#add-a-second-volume). If you installed Deephaven with pip or are running the production application, replace `/data` with a directory on your machine that you can write to.

To export a table to a CSV file, use the [`write_csv`](../reference/data-import-export/CSV/writeCsv.md) function with the table and the path where you want to save the file:

```python test-set=1 order=null
from deephaven import write_csv

write_csv(instrument_rolling_stats, "/data/crypto_prices_stats.csv")
```

Similarly, use [`write`](../reference/data-import-export/Parquet/writeTable.md) from the [`deephaven.parquet`](/core/pydoc/code/deephaven.parquet.html#module-deephaven.parquet) module to export a Parquet file:

```python test-set=1 order=null
from deephaven.parquet import write

write(instrument_rolling_stats, "/data/crypto_prices_stats.parquet")
```

If a table is ticking, each exported file captures the table's state at the moment the script runs.

To create a static [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html), use the [`to_pandas`](../reference/pandas/to-pandas.md) function:

```python test-set=1 order=null
from deephaven.pandas import to_pandas

data_frame = to_pandas(instrument_rolling_stats)
```

## 7. What to do next

Now that you've imported data, created tables, and manipulated static and real-time data, we suggest heading to the [Crash Course in Deephaven](./crash-course/get-started.md) for a more in-depth introduction to Deephaven's design and APIs.

To go further with the topics in this guide, see:

- [Read CSV files](../how-to-guides/data-import-export/csv-import.md)
- [Read Parquet files](../how-to-guides/data-import-export/parquet-import.md)
- [Connect to a Kafka stream](../how-to-guides/data-import-export/kafka-stream.md)
- [Filter table data](../how-to-guides/use-filters.md)
- [Plot real-time data](../how-to-guides/plotting/overview.md)
- [Use pandas in Deephaven queries](../how-to-guides/use-pandas.md)
- [Navigate the Deephaven UI](../how-to-guides/user-interface/navigating-the-ui.md)
