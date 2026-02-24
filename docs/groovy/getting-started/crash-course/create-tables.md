---
title: Create Your First Tables
sidebar_label: Create Tables
---

## Static tables

The simplest way to create static tables from scratch is with [`newTable`](../../reference/table-operations/create/newTable.md) and [`emptyTable`](../../reference/table-operations/create/emptyTable.md).

```groovy test-set=1 order=staticTable1,staticTable2
staticTable1 = newTable(
        intCol("IntColumn", 0, 1, 2, 3, 4),
        longCol("LongColumn", 0, 1, 2, 3, 4),
        doubleCol("DoubleColumn", 0.12345, 2.12345, 4.12345, 6.12345, 8.12345)
)

staticTable2 = emptyTable(5).updateView(
        "IntColumn = i",
        "LongColumn = ii",
        "DoubleColumn = IntColumn + LongColumn + 0.12345"
)
```

> **_NOTE:_** The variables `i` and `ii` correspond to `int` and `long` row indices, respectively. They are only supported in [append-only tables](../../conceptual/table-types.md#specialization-1-append-only).

The two tables look identical but are created differently.

- [`newTable`](../../reference/table-operations/create/newTable.md) builds a table directly from column type specifications and raw data.
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md) builds an empty table with no columns and a specified number of rows. The Deephaven Query Language (DQL) can add new columns programmatically.

## Ticking tables

You can create ticking tables to get a feel for live data in Deephaven. The [`timeTable`](../../reference/table-operations/create/timeTable.md) method works similarly to [`emptyTable`](../../reference/table-operations/create/emptyTable.md) in that DQL is used to populate the table with more data. It creates a table with just a `Timestamp` column. The resultant table ticks at a regular interval specified by the input argument.

```groovy test-set=2 ticking-table order=null
tickingTable = timeTable("PT1s")
```

![A GIF showing the creation and updating of a ticking table in Deephaven](../../assets/tutorials/crash-course/crash-course-3.gif)

The `PT1s` argument is an [ISO-8641 formatted duration string](https://www.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm) that indicates the table will tick once every second.

New ticking tables can be derived from existing ones using DQL, just as in the static case.

```groovy test-set=2 ticking-table order=null
newTickingTable = tickingTable.updateView("TimestampPlusOneSecond = Timestamp + 'PT1s'")
```

![A GIF showing a new ticking table updating in tandem with a source ticking table](../../assets/tutorials/crash-course/crash-course-4.gif)

This exemplifies Deephaven's use of the Directed Acyclic Graph (DAG). The table `tickingTable` is a root node in the DAG, and `newTickingTable` is a downstream node. Because the source table is ticking, the new table is also ticking. Every update to `tickingTable` propagates down the DAG to `newTickingTable`, and the compute-on-deltas model ensures that only the updated rows are re-evaluated with each update cycle.

## Ingesting static data

Deephaven supports reading from various common file formats like [CSV](../../reference/data-import-export/CSV/readCsv.md) and [Parquet](../../reference/data-import-export/Parquet/readTable.md). The following code block reads CSV data from a URL directly into a table.

```groovy test-set=3
import static io.deephaven.csv.CsvTools.readCsv

crypto = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
)
```

If you're running Deephaven in a Docker container, reading your own files requires that you mount the local directory containing the files to a volume in the Docker container, which you can learn more about in the guide on [Docker data volumes](../../conceptual/docker-data-volumes.md).

## Real-world ticking data

Real-time data is Deephaven's mission statement. One of the easiest ways to work with realistic ticking data is by using the [`Replayer`](../../reference/table-operations/create/Replayer.md) to replay the static data. Use it to replay the data ingested above:

```groovy test-set=3 order=null
import io.deephaven.engine.table.impl.replay.Replayer

resultReplayer = new Replayer(parseInstant("2023-02-09T12:09:18 ET"), parseInstant("2023-02-09T12:58:09 ET"))

replayedCrypto = resultReplayer.replay(
    crypto.sort("Timestamp"), "Timestamp"
).sortDescending("Timestamp")

resultReplayer.start()
```

Most real-world use cases for ticking data involve connecting to data streams that are constantly being updated. For this, Deephaven's [Apache Kafka integration](../../how-to-guides/data-import-export/kafka-stream.md) is first-in-class, and almost any imaginable real-time streaming source can be wrangled with the [`TablePublisher`](../../how-to-guides/table-publisher.md#table-publisher) or [`DynamicTableWriter`](../../how-to-guides/table-publisher.md#dynamictablewriter). That said, setting up the pipelines for Kafka streams or other real-time data sources can be very complex, and is outside the scope of this guide.
