---
title: Deephaven Community Core Quickstart
---

This guide shows you how to install and launch Deephaven Community Core, then takes you on a short tour of the Deephaven IDE: importing static and streaming data, transforming and aggregating tables, plotting, and exporting results.

You can install Deephaven Community Core with [Docker](https://docs.docker.com/engine/install/) or with the [production application](./production-application.md), which runs Deephaven natively on your machine. If you don't have a preference, we recommend starting with Docker.

## 1. Install and launch Deephaven

### With Docker

Docker-installed Deephaven runs in a [Docker container](https://www.docker.com/resources/what-container/), so you need [Docker](https://docs.docker.com/engine/install/) installed on your machine. Install and launch Deephaven with a one-line command:

```sh
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE" ghcr.io/deephaven/server-slim:latest
```

> [!CAUTION]
> Replace `YOUR_PASSWORD_HERE` with a secure password of your own. The `-Dauthentication.psk` option sets the password (a pre-shared key) that you use to log in to Deephaven.

The `-v` option mounts the `data` folder in your current directory at `/data` inside the container, so files that Deephaven writes to `/data` appear in that folder on your machine. Docker creates the folder if it doesn't exist.

For additional configuration options, see the [install guide for Docker](./docker-install.md).

### With the production application

> [!NOTE]
> Running the production application requires Java 17 or later. See the [production application prerequisites](./production-application.md#prerequisites) for details.

Download the Deephaven `server-jetty-<version>.tar` file from the assets of the [latest release](https://github.com/deephaven/deephaven-core/releases/latest) using your browser or the command line, unpack the tar file, and start Deephaven. The commands below use a `DH_VERSION` environment variable. Replace `LATEST_VERSION_HERE` with the version number of the latest release, without the leading `v`.

```sh
export DH_VERSION=LATEST_VERSION_HERE
wget https://github.com/deephaven/deephaven-core/releases/download/v${DH_VERSION}/server-jetty-${DH_VERSION}.tar
tar xvf server-jetty-${DH_VERSION}.tar
START_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE -Ddeephaven.console.type=groovy" server-jetty-${DH_VERSION}/bin/start
```

> [!CAUTION]
> Replace `YOUR_PASSWORD_HERE` with a secure password of your own. The `-Dauthentication.psk` option sets the password (a pre-shared key) that you use to log in to Deephaven.

The `-Ddeephaven.console.type=groovy` option starts the console in Groovy instead of the default Python.

For more details, see the [production application guide](./production-application.md).

## 2. The Deephaven IDE

Navigate to [http://localhost:10000/](http://localhost:10000/) and enter your password in the token field:

![Screenshot of Deephaven launch page prompting for a password token](../assets/tutorials/deephaven_launch_password.png)

You're ready to go. The Deephaven IDE is a full scripting environment. Here's a brief overview of its basic features.

![Annotated screenshot of Deephaven IDE highlighting console, notebook controls, and save buttons](../assets/tutorials/ide_basics.png)

1. <font color="#FF40FF">Write and execute commands</font>

   Use this console to write and execute Groovy and Deephaven commands.

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

Run the command below inside a Deephaven console to ingest a million-row CSV of crypto trades with [`readCsv`](../reference/data-import-export/CSV/readCsv.md). All you need is a path or URL for the data:

```groovy test-set=1 order=cryptoFromCsv
import static io.deephaven.csv.CsvTools.readCsv

cryptoFromCsv = readCsv(
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

Ingesting real-time data is one of Deephaven's core strengths. However, streaming pipelines can be complicated to set up and are outside the scope of this guide. For a streaming data example, this guide uses Deephaven's [`Replayer`](../reference/table-operations/create/Replayer.md) to replay historical cryptocurrency data in real time.

The following code takes fake historical crypto trade data from a CSV file and replays it in real time based on timestamps. This is only one of multiple ways to create real-time data in just a few lines of code. Replaying historical data is a great way to test real-time algorithms before deployment into production.

```groovy test-set=1 order=null ticking-table
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.engine.table.impl.replay.Replayer

fakeCryptoData = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv")

start = parseInstant("2023-02-09T12:09:18 ET")
end = parseInstant("2023-02-09T12:58:09 ET")

replayer = new Replayer(start, end)

cryptoStreaming = replayer.replay(fakeCryptoData, "Timestamp")

replayer.start()
```

![Animated GIF of Deephaven Replayer streaming historical cryptocurrency trades in real time](../assets/tutorials/quickstart/quickstart-1.gif)

## 4. Work with Deephaven tables

Deephaven represents both static and streaming data as tables. You can derive new tables from parent tables, and data flows efficiently from parents to their dependents. See the concept guide on the [table update model](../conceptual/table-update-model.md) if you're interested in what's under the hood.

Deephaven represents data transformations as operations on tables. This is a familiar paradigm for data scientists using [pandas](https://pandas.pydata.org), [Polars](https://pola.rs), [R](https://www.r-project.org/), [MATLAB](https://www.mathworks.com/), and more. Deephaven's table operations have one key difference — **they work the same way whether the underlying data is static or streaming.** This means that code written for static data generally also works on streaming data. A few formulas are exceptions, such as those that use the [special variables](../reference/query-language/variables/special-variables.md) `i`, `ii`, and `k`.

There are many table operations to cover, so this guide keeps it short and covers the highlights.

### Manipulate data

First, reverse the ticking (live-updating) `cryptoStreaming` table with [`reverse`](../reference/table-operations/sort/reverse.md) so that the newest data appears at the top:

```groovy test-set=1 order=null ticking-table
cryptoStreamingRev = cryptoStreaming.reverse()
```

![Animated GIF showing the table reversed so newest rows appear at the top](../assets/tutorials/quickstart/quickstart-2.gif)

> [!TIP]
> You can also perform many table operations from the UI. For example, right-click on a column header in the UI and choose **Reverse Table**.

Add a column with [`update`](../reference/table-operations/select/update.md):

```groovy test-set=1 order=null ticking-table
cryptoStreamingRev = cryptoStreamingRev.update("TransactionTotal = Price * Size")
```

![Animated GIF displaying new TransactionTotal column added via update operation](../assets/tutorials/quickstart/quickstart-3.gif)

Use [`view`](../reference/table-operations/select/view.md) to pick out particular columns. [`select`](../reference/table-operations/select/select.md) does the same but computes the columns and stores them in memory, while `view` computes them on demand:

```groovy test-set=1 order=null ticking-table
cryptoStreamingPrices = cryptoStreamingRev.view("Instrument", "Price")
```

![Animated GIF demonstrating selection of Instrument and Price columns with view](../assets/tutorials/quickstart/quickstart-4.gif)

Remove columns with [`dropColumns`](../reference/table-operations/select/drop-columns.md):

```groovy test-set=1 order=null ticking-table
cryptoStreamingRev = cryptoStreamingRev.dropColumns("TransactionTotal")
```

![Animated GIF showing removal of TransactionTotal column using dropColumns](../assets/tutorials/quickstart/quickstart-5.gif)

Deephaven offers many operations for filtering tables, including [`where`](../reference/table-operations/filter/where.md), [`whereIn`](../reference/table-operations/filter/where-in.md), [`whereNotIn`](../reference/table-operations/filter/where-not-in.md), and others. See the [filtering guide](../how-to-guides/use-filters.md) for the full set.

The following code uses `where` to filter for only Bitcoin transactions, and then uses `where` with the `in` match operator to filter for Bitcoin and Ethereum transactions:

```groovy test-set=1 order=null ticking-table
btcStreaming = cryptoStreamingRev.where("Instrument == `BTC/USD`")
ethBtcStreaming = cryptoStreamingRev.where(
    "Instrument in `BTC/USD`, `ETH/USD`"
)
```

![Animated GIF illustrating filtering a table for Bitcoin and Ethereum trades](../assets/tutorials/quickstart/quickstart-6.gif)

### Aggregate data

Deephaven's [dedicated aggregations suite](../how-to-guides/dedicated-aggregations.md) provides a number of table operations that enable efficient column-wise aggregations. These operations also support aggregations by group.

Use [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) to count the number of transactions from each exchange:

```groovy test-set=1 order=null ticking-table
exchangeCount = cryptoStreaming.countBy("Count", "Exchange")
```

![Animated GIF showing countBy aggregation of transaction counts per exchange](../assets/tutorials/quickstart/quickstart-7.gif)

Then, get the average price for each instrument with [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md):

```groovy test-set=1 order=null ticking-table
instrumentAvg = cryptoStreaming.view("Instrument", "Price").avgBy("Instrument")
```

![Animated GIF showing avgBy aggregation calculating average price per instrument](../assets/tutorials/quickstart/quickstart-8.gif)

Find the largest transaction per instrument with [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md):

```groovy test-set=1 order=null ticking-table
maxTransaction = (
    cryptoStreaming.update("TransactionTotal = Price * Size")
    .view("Instrument", "TransactionTotal")
    .maxBy("Instrument")
)
```

![Animated GIF displaying maxBy aggregation to find largest transaction per instrument](../assets/tutorials/quickstart/quickstart-9.gif)

Each dedicated aggregation performs one aggregation at a time. You often need to [perform multiple aggregations](../how-to-guides/combined-aggregations.md) on the same data. For this, Deephaven provides the [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) table operation and the [`io.deephaven.api.agg.Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) Java API.

First, use `aggBy` to compute the [mean](../reference/table-operations/group-and-aggregate/AggAvg.md) and [standard deviation](../reference/table-operations/group-and-aggregate/AggStd.md) of the price, grouped by instrument and exchange:

```groovy test-set=1 order=null ticking-table
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggStd

summaryPrices = cryptoStreaming.aggBy(
    [AggAvg("AvgPrice = Price"), AggStd("StdPrice = Price")],
    "Instrument", "Exchange"
).sort("Instrument", "Exchange")
```

![Animated GIF demonstrating aggBy to compute mean and standard deviation of prices grouped by instrument and exchange](../assets/tutorials/quickstart/quickstart-10.gif)

Then, add a column containing the [coefficient of variation](https://en.wikipedia.org/wiki/Coefficient_of_variation) for each instrument and exchange, measuring the relative risk of each:

```groovy test-set=1 order=null ticking-table
summaryPrices = summaryPrices.update("PctVariation = 100 * StdPrice / AvgPrice")
```

![Animated GIF showing update that adds percentage variation column to summary table](../assets/tutorials/quickstart/quickstart-11.gif)

Finally, create a minute-by-minute [Open-High-Low-Close](https://en.wikipedia.org/wiki/Open-high-low-close_chart) table using [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)), one of Deephaven's [built-in time functions](../reference/query-language/query-library/auto-imported/time.md), along with [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`AggMax`](../reference/table-operations/group-and-aggregate/AggMax.md), [`AggMin`](../reference/table-operations/group-and-aggregate/AggMin.md), and [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md):

```groovy test-set=1 order=null ticking-table
import static io.deephaven.api.agg.Aggregation.AggFirst
import static io.deephaven.api.agg.Aggregation.AggMax
import static io.deephaven.api.agg.Aggregation.AggMin
import static io.deephaven.api.agg.Aggregation.AggLast

ohlcByMinute = (
    cryptoStreaming.update("BinnedTimestamp = lowerBin(Timestamp, MINUTE)")
    .aggBy(
        [
            AggFirst("Open = Price"),
            AggMax("High = Price"),
            AggMin("Low = Price"),
            AggLast("Close = Price")
        ],
        "Instrument", "BinnedTimestamp"
    )
    .sort("Instrument", "BinnedTimestamp")
)
```

![Animated GIF illustrating creation of OHLC table aggregated by one-minute bins](../assets/tutorials/quickstart/quickstart-12.gif)

### Window calculations

You may want to perform window-based calculations, compute moving or cumulative statistics, or look at pairwise differences. Deephaven's [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation is the right tool for the job.

Compute the moving average and standard deviation of each instrument's price using [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) and [`RollingStd`](../reference/table-operations/update-by-operations/rolling-std.md):

```groovy test-set=1 order=null ticking-table
import static io.deephaven.api.updateby.UpdateByOperation.RollingAvg
import static io.deephaven.api.updateby.UpdateByOperation.RollingStd
import java.time.Duration

instrumentRollingStats = cryptoStreaming.updateBy(
    [
        RollingAvg("Timestamp", Duration.parse("PT30s"), "AvgPrice30Sec = Price"),
        RollingAvg("Timestamp", Duration.parse("PT5m"), "AvgPrice5Min = Price"),
        RollingStd("Timestamp", Duration.parse("PT30s"), "StdPrice30Sec = Price"),
        RollingStd("Timestamp", Duration.parse("PT5m"), "StdPrice5Min = Price")
    ],
    "Instrument"
).reverse()
```

![Animated GIF showing rolling window calculations producing moving averages and standard deviations](../assets/tutorials/quickstart/quickstart-13.gif)

Use these statistics to find "extreme" instrument prices, where the price is more than 1.645 standard deviations above or below the rolling average for the window that ends at that row's timestamp:

```groovy test-set=1 order=null ticking-table
instrumentExtremity = instrumentRollingStats.update(
    "Z30Sec = (Price - AvgPrice30Sec) / StdPrice30Sec",
    "Z5Min = (Price - AvgPrice5Min) / StdPrice5Min",
    "Extreme30Sec = Math.abs(Z30Sec) > 1.645",
    "Extreme5Min = Math.abs(Z5Min) > 1.645"
).view(
    "Timestamp",
    "Instrument",
    "Exchange",
    "Price",
    "Size",
    "Extreme30Sec",
    "Extreme5Min"
)
```

![Animated GIF highlighting extremity detection using Z-scores derived from rolling statistics](../assets/tutorials/quickstart/quickstart-14.gif)

There's a lot more to `updateBy`. See the guide on [rolling aggregations](../how-to-guides/rolling-aggregations.md) for more information.

### Combine tables

Combining datasets can often yield powerful insights. Deephaven offers two primary ways to combine tables — the _merge_ and _join_ operations.

The [`merge`](../reference/table-operations/merge/merge.md) operation stacks tables on top of one another. The tables must have the same columns with the same types. The tables you merge can be static, ticking, or a mix of both. For example, combine the static September 2021 trades with the ticking February 2023 replay:

```groovy test-set=1 order=null ticking-table
combinedCrypto = merge(cryptoFromCsv, cryptoStreaming).sort("Timestamp")
```

![Animated GIF demonstrating merge operation combining static and streaming crypto tables](../assets/tutorials/quickstart/quickstart-15.gif)

The ubiquitous join operation combines tables based on columns that they have in common. Deephaven offers many variants of this operation, such as [`join`](../reference/table-operations/join/join.md), [`naturalJoin`](../reference/table-operations/join/natural-join.md), and [`exactJoin`](../reference/table-operations/join/exact-join.md). The [exact and relational joins guide](../how-to-guides/joins-exact-relational.md) covers the rest.

For example, summarize the older September 2021 trades in `cryptoFromCsv`, the table you loaded at the start of this guide. Then, use `join` to combine that summary with the February 2023 `summaryPrices` table from [Aggregate data](#aggregate-data) to compare February 2023 prices with September 2021 prices:

```groovy test-set=1 order=null ticking-table
moreSummaryPrices = cryptoFromCsv.aggBy(
    [AggAvg("AvgPrice = Price"), AggStd("StdPrice = Price")],
    "Instrument", "Exchange"
).sort("Instrument", "Exchange")

priceComparison = (
    summaryPrices.dropColumns("PctVariation")
    .renameColumns("AvgPriceFeb2023 = AvgPrice", "StdPriceFeb2023 = StdPrice")
    .join(
        moreSummaryPrices,
        "Instrument, Exchange",
        "AvgPriceSep2021 = AvgPrice, StdPriceSep2021 = StdPrice"
    )
)
```

![Animated GIF displaying join operation comparing February 2023 and September 2021 price summaries](../assets/tutorials/quickstart/quickstart-16.gif)

Many real-time data applications combine data based on timestamps. Traditional join operations often fail this task, as they require _exact_ matches in both datasets. To remedy this, Deephaven provides [time-series joins](../how-to-guides/joins-timeseries-range.md), such as [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), that can join tables on timestamps with _approximate_ matches.

The following example uses `aj` to find the Ethereum price at or immediately before the timestamp of each Bitcoin trade:

```groovy test-set=1 order=null ticking-table
cryptoBtc = cryptoStreaming.where("Instrument == `BTC/USD`")
cryptoEth = cryptoStreaming.where("Instrument == `ETH/USD`")

timeSeriesJoin = cryptoBtc.view("Timestamp", "Price")
    .aj(cryptoEth, "Timestamp", "EthTime = Timestamp, EthPrice = Price")
    .renameColumns("BtcTime = Timestamp", "BtcPrice = Price")
```

![Animated GIF showing time-series aj join aligning Ethereum prices to Bitcoin timestamps](../assets/tutorials/quickstart/quickstart-17.gif)

## 5. Plot data via query or the UI

Deephaven has a rich [plotting API](../how-to-guides/plotting/api-plotting.md) that supports _updating, real-time plots_. You can call it programmatically, for example with [`plot`](../reference/plot/plot.md):

```groovy test-set=1 order=null ticking-table
btcData = instrumentRollingStats.where("Instrument == `BTC/USD`").reverse()

btcPlot = figure()
    .plot("Bitcoin Prices", btcData, "Timestamp", "Price")
    .plot("Rolling Average", btcData, "Timestamp", "AvgPrice30Sec")
    .show()
```

![Animated GIF of real-time line plot of Bitcoin price and rolling average created via code](../assets/tutorials/quickstart/quickstart-18.gif)

You can also create plots from the web UI:

![Animated GIF demonstrating plot creation through Deephaven web UI chart builder](../assets/tutorials/quickstart/quickstart-19.gif)

## 6. Export data

You can export your data from Deephaven to popular open file formats.

The examples below write to `/data`, the data directory in the Deephaven Docker image. If you started Deephaven with the Docker command above, the files appear in the `data` folder of the directory where you ran it. To mount other directories, see [Add a second volume](./docker-install.md#add-a-second-volume). If you run the production application, replace `/data` with a directory on your machine that you can write to.

To export a table to a CSV file, use the [`writeCsv`](../reference/data-import-export/CSV/writeCsv.md) method with the table and the path where you want to save the file:

```groovy test-set=1 order=null
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(instrumentRollingStats, "/data/crypto_prices_stats.csv")
```

Similarly, use [`writeTable`](../reference/data-import-export/Parquet/writeTable.md) to export a Parquet file:

```groovy test-set=1 order=null
import static io.deephaven.parquet.table.ParquetTools.writeTable

writeTable(instrumentRollingStats, "/data/crypto_prices_stats.parquet")
```

If a table is ticking, each exported file captures the table's state at the moment the script runs.

## 7. What to do next

Now that you've imported data, created tables, and manipulated static and real-time data, we suggest heading to the [Crash Course in Deephaven](./crash-course/get-started.md) for a more in-depth introduction to Deephaven's design and APIs.

To go further with the topics in this guide, see:

- [Read CSV files](../how-to-guides/data-import-export/csv-import.md)
- [Read Parquet files](../how-to-guides/data-import-export/parquet-import.md)
- [Connect to a Kafka stream](../how-to-guides/data-import-export/kafka-stream.md)
- [Filter table data](../how-to-guides/use-filters.md)
- [Plot with the built-in plotting API](../how-to-guides/plotting/api-plotting.md)
- [Navigate the Deephaven UI](../how-to-guides/user-interface/navigating-the-ui.md)
