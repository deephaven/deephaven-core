---
title: Create Your First Tables
---

Static tables hold data that doesn't change, and ticking tables update as new data arrives. This chapter shows how to create static tables from scratch, read data from a file or URL into a table, create ticking tables, and replay static data as a ticking table.

## Create static tables from scratch

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

The two tables hold the same data, but each method builds it in a different way.

- [`newTable`](../../reference/table-operations/create/newTable.md) builds a table directly from column type specifications and raw data.
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md) builds a table with a specified number of rows and no columns. The example then adds columns with [`updateView`](../../reference/table-operations/select/update-view.md) and [query strings](./query-strings.md) such as `"IntColumn = i"`.

> [!NOTE]
> The [special variables](../../reference/query-language/variables/special-variables.md) `i` and `ii` hold each row's position as an `int` and a `long`, respectively. They're safe in static tables like this one. The [Query Strings](./query-strings.md) chapter covers the limits on using them in ticking tables.

## Read data from a file or URL

Deephaven can read common file formats such as [CSV](../../reference/data-import-export/CSV/readCsv.md) and [Parquet](../../reference/data-import-export/Parquet/readTable.md). The following code block reads CSV data from a URL directly into a table.

```groovy test-set=3
import static io.deephaven.csv.CsvTools.readCsv

crypto = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
)
```

> [!NOTE]
> To read your own file, put it in a directory that your Docker command mounts into the container and pass its container path to `readCsv`. The Docker command in [Get Started](./get-started.md) mounts the `data` directory in your current working directory at `/data`, so you read a file saved as `data/my-file.csv` on your machine with `readCsv("/data/my-file.csv")`. See [Docker data volumes](../../conceptual/docker-data-volumes.md) for more.

## Create ticking tables

You can create ticking tables to get a feel for live data in Deephaven. The [Architecture Overview](./architecture-overview.md#immutable-schema-mutable-data) briefly used [`timeTable`](../../reference/table-operations/create/timeTable.md). `timeTable` creates a ticking table with a single `Timestamp` column and adds a new row at a regular interval that you set.

```groovy test-set=2 ticking-table order=null
tickingTable = timeTable("PT1s")
```

![A GIF showing the creation and updating of a ticking table in Deephaven](../../assets/tutorials/crash-course/crash-course-3.gif)

The `PT1s` argument is an ISO 8601 [duration string](../../reference/query-language/types/durations.md) that sets the period between rows. Consecutive rows have `Timestamp` values one second apart. In each [update cycle](./architecture-overview.md#the-engine), the engine usually adds one row to this table, but a delayed cycle can add several rows at once.

As in the [Architecture Overview](./architecture-overview.md), you derive new ticking tables from existing ones with the same table operations and [query strings](./query-strings.md) you used in the static case. This time, watch how the two tables relate.

```groovy test-set=2 ticking-table order=null
newTickingTable = tickingTable.updateView("TimestampPlusOneSecond = Timestamp + 'PT1s'")
```

![A GIF showing a new ticking table updating in tandem with a source ticking table](../../assets/tutorials/crash-course/crash-course-4.gif)

This is the [DAG](./architecture-overview.md#the-engine) from the Architecture Overview at work. `tickingTable` is the parent (upstream) table, and `newTickingTable` is its child (downstream) table. The new table therefore ticks along with its source. In each update cycle that adds rows to `tickingTable`, the engine sends `newTickingTable` a change summary that lists only the newly added rows, so `newTickingTable` handles just those rows instead of the whole table.

Most real-world use cases for ticking data involve connecting to data streams that update continuously. Setting up these connections is outside the scope of this crash course. When you need one, see Deephaven's [Apache Kafka integration](../../how-to-guides/data-import-export/kafka-stream.md), or use [`TablePublisher`](../../how-to-guides/table-publisher.md#table-publisher) or [`DynamicTableWriter`](../../how-to-guides/table-publisher.md#dynamictablewriter) for other real-time sources.

## Replay static data as ticking data

Real-time data is Deephaven's mission statement. One of the easiest ways to work with realistic ticking data is to replay static data with the [`Replayer`](../../reference/table-operations/create/Replayer.md). Use it to replay the `crypto` table ingested above:

```groovy test-set=3 ticking-table order=null
import io.deephaven.engine.table.impl.replay.Replayer

replayer = new Replayer(parseInstant("2023-02-09T12:09:18 ET"), parseInstant("2023-02-09T12:58:09 ET"))

replayedCrypto = replayer.replay(
    crypto.sort("Timestamp"), "Timestamp"
).sortDescending("Timestamp")

replayer.start()
```

When the replay starts, its clock begins at the start time passed to [`Replayer`](../../reference/table-operations/create/Replayer.md) and advances in real time. Each row appears in `replayedCrypto` once the clock reaches the row's `Timestamp`. The code uses [`sort`](../../reference/table-operations/sort/sort.md) to order `crypto` by `Timestamp` first, because the replayer can't replay timestamps that decrease. [`sortDescending`](../../reference/table-operations/sort/sort-descending.md) then puts the newest rows at the top.

Next, [Basic Table Operations](./table-ops.md) covers the table operations that appear in almost every query.
