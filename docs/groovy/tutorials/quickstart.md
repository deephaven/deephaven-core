---
title: Deephaven Community Core Quickstart
sidebar_label: Quickstart
---

Deephaven Community Core can be installed with [Docker](https://docs.docker.com/engine/install/) or the [production application](./production-application.md). If you are familiar with Docker or already have it installed, the single-line Docker command is an easy way to get started with Deephaven. If you wish to not use Docker, use the production application.

## 1. Install and Launch Deephaven

### With Docker

Install and launch Deephaven via [Docker](https://docs.docker.com/engine/install/) with a one-line command:

```sh
docker run --rm --name deephaven -p 10000:10000 --env START_OPTS="-Dauthentication.psk=PASSWORD" ghcr.io/deephaven/server-slim:latest
```

> [!CAUTION]
> Replace "PASSWORD" with a more secure passkey to keep your session safe.

For additional configuration options, see the [install guide for Docker](./docker-install.md).

### With Native Deephaven

Download the latest Deephaven `server-jetty-<version>.tar` from the [Latest Release Assets](https://github.com/deephaven/deephaven-core/releases/latest) using your browser or the command line, unpack the tar file, and start Deephaven.

```sh
wget https://github.com/deephaven/deephaven-core/releases/download/v0.33.3/server-jetty-0.33.3.tar
tar xvf server-jetty-0.33.3.tar
START_OPTS="-Dauthentication.psk=PASSWORD -Ddeephaven.console.type=groovy" server-jetty-0.33.3/bin/start
```

## 2. The Deephaven IDE

Navigate to [http://localhost:10000/](http://localhost:10000/) and enter your password in the token field:

![Screenshot of Deephaven launch page prompting for a password token](../assets/tutorials/deephaven_launch_password.png)

You're ready to go! The Deephaven IDE is a fully-featured scripting IDE. Here's a brief overview of some of its basic functionalities.

![Annotated screenshot of Deephaven IDE highlighting console, notebook controls, and save buttons](../assets/tutorials/ide_basics.png)

1. <font color="#FF40FF">Write and execute commands</font>

   Use this console to write and execute Groovy and Deephaven commands.

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

Now that you have Deephaven installed and open, the rest of this guide will briefly highlight some key features of using Deephaven.

For a more exhaustive introduction to Deephaven and an in-depth exploration of our design principles and APIs, check out the [Crash Course series](./crash-course/get-started.md).

## 3. Import static and streaming data

Deephaven empowers users to wrangle static and streaming data with ease. It supports ingesting data from [CSV files](../how-to-guides/csv-import.md), [Parquet files](../how-to-guides/data-import-export/parquet-import.md), and [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md).

### Load a CSV

Run the command below inside a Deephaven console to ingest a million-row CSV of crypto trades. All you need is a path or URL for the data:

```groovy test-set=1 order=cryptoFromCsv
import static io.deephaven.csv.CsvTools.readCsv

cryptoFromCsv = readCsv(
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

![Animated GIF of Deephaven Table Replayer streaming historical cryptocurrency trades in real time](../assets/tutorials/quickstart/quickstart-1.gif)

## 4. Working with Deephaven Tables

In Deephaven, static and dynamic data are represented as tables. New tables can be derived from parent tables, and data efficiently flows from parents to their dependents. See the concept guide on the [table update model](../conceptual/table-update-model.md) if you're interested in what's under the hood.

Deephaven represents data transformations as operations on tables. This is a familiar paradigm for data scientists using [Pandas](https://pandas.pydata.org), [Polars](https://pola.rs), [R](https://www.r-project.org/), [Matlab](https://www.mathworks.com/) and more. Deephaven's table operations are special - **they are indifferent to whether the underlying data sources are static or streaming!** This means that code written for static data will work seamlessly on live data.

There are a ton of table operations to cover, so we'll keep it short and give you the highlights.

### Manipulating data

First, reverse the ticking table with [`reverse`](../reference/table-operations/sort/reverse.md) so that the newest data appears at the top:

```groovy test-set=1 order=null ticking-table
cryptoStreamingRev = cryptoStreaming.reverse()
```

![Animated GIF showing the table reversed so newest rows appear at the top](../assets/tutorials/quickstart/quickstart-2.gif)

> [!TIP]
> Many table operations can also be done from the UI. For example, right-click on a column header in the UI and choose **Reverse Table**.

Add a column with [`update`](../reference/table-operations/select/update.md):

```groovy test-set=1 order=null ticking-table
cryptoStreamingRev = cryptoStreamingRev.update("TransactionTotal = Price * Size")
```

![Animated GIF displaying new TransactionTotal column added via update operation](../assets/tutorials/quickstart/quickstart-3.gif)

Use [`select`](../reference/table-operations/select/select.md) or [`view`](../reference/table-operations/select/view.md) to pick out particular columns:

```groovy test-set=1 order=null ticking-table
cryptoStreamingPrices = cryptoStreamingRev.view("Instrument", "Price")
```

![Animated GIF demonstrating selection of Instrument and Price columns with view](../assets/tutorials/quickstart/quickstart-4.gif)

Remove columns with [`dropColumns`](../reference/table-operations/select/drop-columns.md):

```groovy test-set=1 order=null ticking-table
cryptoStreamingRev = cryptoStreamingRev.dropColumns("TransactionTotal")
```

![Animated GIF showing removal of TransactionTotal column using drop_columns](../assets/tutorials/quickstart/quickstart-5.gif)

Next, Deephaven offers many operations for filtering tables. These include [`where`](../reference/table-operations/filter/where.md), [`whereIn`](../reference/table-operations/filter/where-in.md), [`whereNotIn`](../reference/table-operations/filter/where-not-in.md), and [more](../how-to-guides/filters.md).

The following code uses [`where`](../reference/table-operations/filter/where.md) to filter for only Bitcoin transactions, and then for Bitcoin and Ethereum transactions:

```groovy test-set=1 order=null ticking-table
btcStreaming = cryptoStreamingRev.where("Instrument == `BTC/USD`")
etcBtcStreaming = cryptoStreamingRev.where(
    "Instrument in `BTC/USD`, `ETH/USD`"
)
```

![Animated GIF illustrating filtering a table for Bitcoin and Ethereum trades](../assets/tutorials/quickstart/quickstart-6.gif)

### Aggregating data

Deephaven's [dedicated aggregations suite](../how-to-guides/dedicated-aggregations.md) provides a number of table operations that enable efficient column-wise aggregations. These operations also support aggregations by group.

Use [`countBy`](../reference/table-operations/group-and-aggregate/countBy.md) to count the number of transactions from each exchange:

```groovy test-set=1 order=null ticking-table
exchangeCount = cryptoStreaming.countBy("Count", "Exchange")
```

![Animated GIF showing count_by aggregation of transaction counts per exchange](../assets/tutorials/quickstart/quickstart-7.gif)

Then, get the average price for each instrument with [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md):

```groovy test-set=1 order=null ticking-table
instrumentAvg = cryptoStreaming.view("Instrument", "Price").avgBy("Instrument")
```

![Animated GIF showing avg_by aggregation calculating average price per instrument](../assets/tutorials/quickstart/quickstart-8.gif)

Find the largest transaction per instrument with [`maxBy`](../reference/table-operations/group-and-aggregate/maxBy.md):

```groovy test-set=1 order=null ticking-table
maxTransaction = (
    cryptoStreaming.update("TransactionTotal = Price * Size")
    .view("Instrument", "TransactionTotal")
    .maxBy("Instrument")
)
```

![Animated GIF displaying max_by aggregation to find largest transaction per instrument](../assets/tutorials/quickstart/quickstart-9.gif)

While dedicated aggregations are powerful, they only enable you to perform one aggregation at a time. However, you often need to [perform multiple aggregations](../how-to-guides/combined-aggregations.md) on the same data. For this, Deephaven provides the [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) table operation and the [`io.deephaven.api.agg.Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) Java API.

First, use [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) to compute the [mean](../reference/table-operations/group-and-aggregate/AggAvg.md) and [standard deviation](../reference/table-operations/group-and-aggregate/AggStd.md) of each instrument's price, grouped by exchange:

```groovy test-set=1 order=null ticking-table
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggStd

summaryPrices = cryptoStreaming.aggBy(
    [AggAvg("AvgPrice=Price"), AggStd("StdPrice=Price")],
    "Instrument", "Exchange",
).sort("Instrument", "Exchange")
```

![Animated GIF demonstrating agg_by to compute mean and standard deviation of prices grouped by instrument and exchange](../assets/tutorials/quickstart/quickstart-10.gif)

Then, add a column containing the [coefficient of variation](https://en.wikipedia.org/wiki/Coefficient_of_variation) for each instrument, measuring the relative risk of each:

```groovy test-set=1 order=null ticking-table
summaryPrices = summaryPrices.update("PctVariation = 100 * StdPrice / AvgPrice")
```

![Animated GIF showing update that adds percentage variation column to summary table](../assets/tutorials/quickstart/quickstart-11.gif)

Finally, create a minute-by-minute [Open-High-Low-Close](https://en.wikipedia.org/wiki/Open-high-low-close_chart) table using the [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) [built-in function](../reference/query-language/formulas/auto-imported-functions.md) along with [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md), [`AggMax`](../reference/table-operations/group-and-aggregate/AggMax.md), [`AggMin`](../reference/table-operations/group-and-aggregate/AggMin.md), and [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md):

```groovy test-set=1 order=null ticking-table
import static io.deephaven.api.agg.Aggregation.AggFirst
import static io.deephaven.api.agg.Aggregation.AggMax
import static io.deephaven.api.agg.Aggregation.AggMin
import static io.deephaven.api.agg.Aggregation.AggLast

ohlcByMinute = (
    cryptoStreaming.update("BinnedTimestamp = lowerBin(Timestamp, MINUTE)")
    .aggBy(
        [
            AggFirst("Open=Price"),
            AggMax("High=Price"),
            AggMin("Low=Price"),
            AggLast("Close=Price")
        ],
        "Instrument", "BinnedTimestamp",
    )
    .sort("Instrument", "BinnedTimestamp")
)
```

![Animated GIF illustrating creation of OHLC table aggregated by one-minute bins](../assets/tutorials/quickstart/quickstart-12.gif)

You may want to perform window-based calculations, compute moving or cumulative statistics, or look at pair-wise differences. Deephaven's [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation is the right tools for the job.

Compute the moving average and standard deviation of each instrument's price using [`rollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) and [`rollingStd`](../reference/table-operations/update-by-operations/rolling-std.md):

```groovy test-set=1 order=null ticking-table
import java.time.Duration

instrumentRollingStats = cryptoStreaming.updateBy(
    [
        RollingAvg("Timestamp", Duration.parse("PT30s"), "AvgPrice30Sec=Price"),
        RollingAvg("Timestamp", Duration.parse("PT5m"), "AvgPrice5Min=Price"),
        RollingStd("Timestamp", Duration.parse("PT30s"), "StdPrice30Sec=Price"),
        RollingStd("Timestamp", Duration.parse("PT5m"), "StdPrice5Min=Price")
    ],
    "Instrument"
).reverse()
```

![Animated GIF showing rolling window calculations producing moving averages and standard deviations](../assets/tutorials/quickstart/quickstart-13.gif)

These statistics can be used to determine "extreme" instrument prices, where the instrument's price is significantly higher or lower than the average of the prices preceding it in the window:

```groovy test-set=1 order=null ticking-table
instrumentExtremity = instrumentRollingStats.update(
    "Z30Sec = (Price - AvgPrice30Sec) / StdPrice30Sec",
    "Z5Min = (Price - AvgPrice5Min) / StdPrice5Min",
    "Extreme30Sec = Math.abs(Z30Sec) > 1.645 ? true : false",
    "Extreme5Min = Math.abs(Z5Min) > 1.645 ? true : false"
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

There's a lot more to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md). See the [user guide](../how-to-guides/use-update-by.md) for more information.

### Combining tables

Combining datasets can often yield powerful insights. Deephaven offers two primary ways to combine tables - the _merge_ and _join_ operations.

The [`merge`](../reference/table-operations/merge/merge.md) operation stacks tables on top of one-another. This is ideal when several tables have the same schema. They can be static, ticking, or a mix of both:

```groovy test-set=1 order=null ticking-table
combinedCrypto = merge(fakeCryptoData, cryptoStreaming).sort("Timestamp")
```

![Animated GIF demonstrating merge operation combining static and streaming crypto tables](../assets/tutorials/quickstart/quickstart-15.gif)

The ubiquitous join operation is used to combine tables based on columns that they have in common. Deephaven offers many variants of this operation such as [`join`](../reference/table-operations/join/join.md), [`naturalJoin`](../reference/table-operations/join/natural-join.md), [`exactJoin`](../reference/table-operations/join/exact-join.md), and many more.

For example, read in an older dataset containing price data on the same coins from the same exchanges. Then, use [`join`](../reference/table-operations/join/join.md) to combine the aggregated prices to see how current prices compare to those in the past:

```groovy test-set=1 order=null ticking-table
moreCrypto = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv"
)

moreSummaryPrices = moreCrypto.aggBy(
    [AggAvg("AvgPrice=Price"), AggStd("StdPrice=Price")],
    "Instrument", "Exchange"
).sort("Instrument", "Exchange")

priceComparison = (
    summaryPrices.dropColumns("PctVariation")
    .renameColumns("AvgPriceFeb2023=AvgPrice", "StdPriceFeb2023=StdPrice")
    .join(
        moreSummaryPrices,
        "Instrument, Exchange",
        "AvgPriceSep2021=AvgPrice, StdPriceSep2021=StdPrice"
    )
)
```

![Animated GIF displaying join operation comparing February 2023 and September 2021 price summaries](../assets/tutorials/quickstart/quickstart-16.gif)

In many real-time data applications, data needs to be combined based on timestamps. Traditional join operations often fail this task, as they require _exact_ matches in both datasets. To remedy this, Deephaven provides [time series joins](../how-to-guides/joins-timeseries-range.md), such as [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md), that can join tables on timestamps with _approximate_ matches.

Here's an example where [`aj`](../reference/table-operations/join/aj.md) is used to find the Ethereum price at or immediately preceding a Bitcoin price:

```groovy test-set=1  order=null ticking-table
cryptoBtc = cryptoStreaming.where("Instrument = `BTC/USD`")
cryptoEth = cryptoStreaming.where("Instrument = `ETH/USD`")

timeSeriesJoin = cryptoBtc.view("Timestamp", "Price")
    .aj(cryptoEth, "Timestamp", "EthTime = Timestamp, EthPrice = Price")
    .renameColumns("BtcTime = Timestamp", "BtcPrice = Price")
```

![Animated GIF showing time-series aj join aligning Ethereum prices to Bitcoin timestamps](../assets/tutorials/quickstart/quickstart-17.gif)

To learn more about our join methods, see the guides on [exact and relational joins](../how-to-guides/joins-exact-relational.md) and [time series and range joins](../how-to-guides/joins-timeseries-range.md).

## 5. Plot data via query or the UI

Deephaven has a rich plotting API that supports _updating, real-time plots_. It can be called programmatically:

```groovy test-set=1 order=null ticking-table
btcData = instrumentRollingStats.where("Instrument == `BTC/USD`").reverse()

btcPlot = figure()
    .plot("Bitcoin Prices", btcData, x="Timestamp", y="Price")
    .plot("Rolling Average", btcData, x="Timestamp", y="AvgPrice30Sec")
    .show()
```

![Animated GIF of real-time line plot of Bitcoin price and rolling average created via code](../assets/tutorials/quickstart/quickstart-18.gif)

Or with the web UI:

![Animated GIF demonstrating plot creation through Deephaven web UI chart builder](../assets/tutorials/quickstart/quickstart-19.gif)

## 6. Export data to popular formats

It's easy to export your data out of Deephaven to popular open formats.

To export a table to a CSV file, use the [`writeCsv`](../reference/data-import-export/CSV/writeCsv.md) method with the table name and the location to which you want to save the file. If you are using Docker, see managing [Docker volumes](../conceptual/docker-data-volumes.md) for more information on how to save files to your local machine.

```groovy test-set=1 order=null
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(instrumentRollingStats, "/data/crypto_prices_stats.csv")
```

If the table is dynamically updating, Deephaven will automatically snapshot the data before writing it to the file.

Similarly, for Parquet:

```groovy test-set=1 order=null
import static io.deephaven.parquet.table.ParquetTools.writeTable

writeTable(instrumentRollingStats, "/data/crypto_prices_stats.parquet")
```

## 7. What to do next

<!--TODO: For a more exhaustive introduction to Deephaven and an in-depth exploration of our design principles and APIs, check out 30 Minutes to Deephaven. add link -->

Now that you've imported data, created tables, and manipulated static and real-time data, we suggest heading to our user guide. There, you can explore anything from complex data aggregations to detailed performance comparisons and more.

- [Import CSV files](../how-to-guides/csv-import.md)
- [Read Parquet files](../how-to-guides/data-import-export/parquet-import.md)
- [Write Parquet files](../how-to-guides/data-import-export/parquet-export.md)
- [Connect to a Kafka stream](../how-to-guides/data-import-export/kafka-stream.md)
- [Create and organize notebooks using the File Explorer](../how-to-guides/user-interface/file-explorer.md)
- [Create plots without writing queries using the Chart Builder](../how-to-guides/user-interface/chart-builder.md)
- [Filter your data without writing queries](../how-to-guides/user-interface/filters.md)
- [Perform combined aggregations](../how-to-guides/combined-aggregations.md)
- [Group data](../how-to-guides/grouping-data.md)
- [Joins: Exact and relational](../how-to-guides/joins-exact-relational.md)
- [Joins: Time series and range joins](../how-to-guides/joins-timeseries-range.md)
- [Select, view, and update data](../how-to-guides/use-select-view-update.md)
- [Use filters](../how-to-guides/filters.md)
- [Write a Groovy function](../how-to-guides/groovy-closures.md)
