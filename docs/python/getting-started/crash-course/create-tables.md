---
title: Create Your First Tables
---

Static tables hold data that doesn't change, and ticking tables update as new data arrives. This chapter shows how to create static tables from scratch, read data from a file or URL into a table, create ticking tables, and replay static data as a ticking table.

## Create static tables from scratch

The simplest way to create static tables from scratch is with [`new_table`](../../reference/table-operations/create/newTable.md) and [`empty_table`](../../reference/table-operations/create/emptyTable.md).

```python test-set=1 order=static_table1,static_table2
from deephaven import new_table, empty_table
from deephaven.column import int_col, long_col, double_col

static_table1 = new_table(
    [
        int_col("IntColumn", [0, 1, 2, 3, 4]),
        long_col("LongColumn", [0, 1, 2, 3, 4]),
        double_col("DoubleColumn", [0.12345, 2.12345, 4.12345, 6.12345, 8.12345]),
    ]
)

static_table2 = empty_table(5).update_view(
    [
        "IntColumn = i",
        "LongColumn = ii",
        "DoubleColumn = IntColumn + LongColumn + 0.12345",
    ]
)
```

The two tables hold the same data, but each method builds it in a different way.

- [`new_table`](../../reference/table-operations/create/newTable.md) builds a table directly from column type specifications and raw data.
- [`empty_table`](../../reference/table-operations/create/emptyTable.md) builds a table with a specified number of rows and no columns. The example then adds columns with [`update_view`](../../reference/table-operations/select/update-view.md) and [query strings](./query-strings.md) such as `"IntColumn = i"`.

> [!NOTE]
> The [special variables](../../reference/query-language/variables/special-variables.md) `i` and `ii` hold each row's position as an `int` and a `long`, respectively. They're safe in static tables like this one. The [Query Strings](./query-strings.md) chapter covers the limits on using them in ticking tables.

## Read data from a file or URL

Deephaven can read common file formats such as [CSV](../../reference/data-import-export/CSV/readCsv.md), [Parquet](../../reference/data-import-export/Parquet/readTable.md), and [Arrow Feather](../../reference/arrow/read-feather.md). The following code block reads CSV data from a URL directly into a table.

```python test-set=3
from deephaven import read_csv

crypto = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
)
```

> [!NOTE]
> To read your own file, pass its path to `read_csv`. If you run Deephaven in Docker, the file must be in a directory that your Docker command mounts into the container. The Docker command in [Get Started](./get-started.md) mounts the `data` directory in your current working directory at `/data`, so you read a file saved as `data/my-file.csv` on your machine with `read_csv("/data/my-file.csv")`. If you installed Deephaven with pip, use the file's path on your machine. See [Docker data volumes](../../conceptual/docker-data-volumes.md) for more.

## Create ticking tables

You can create ticking tables to get a feel for live data in Deephaven. The [Architecture Overview](./architecture-overview.md#immutable-schema-mutable-data) briefly used [`time_table`](../../reference/table-operations/create/timeTable.md). `time_table` creates a ticking table with a single `Timestamp` column and adds a new row at a regular interval that you set.

```python test-set=2 ticking-table order=null
from deephaven import time_table

ticking_table = time_table("PT1s")
```

![A GIF showing the creation and updating of a ticking table in Deephaven](../../assets/tutorials/crash-course/crash-course-3.gif)

The `PT1s` argument is an ISO 8601 [duration string](../../reference/query-language/types/durations.md) that sets the period between rows. Consecutive rows have `Timestamp` values one second apart. In each [update cycle](./architecture-overview.md#the-engine), the engine usually adds one row to this table, but a delayed cycle can add several rows at once.

As in the Architecture Overview, you derive new ticking tables from existing ones with the same table operations and [query strings](./query-strings.md) you used in the static case. This time, watch how the two tables relate.

```python test-set=2 ticking-table order=null
new_ticking_table = ticking_table.update_view(
    "TimestampPlusOneSecond = Timestamp + 'PT1s'"
)
```

![A GIF showing a new ticking table updating in tandem with a source ticking table](../../assets/tutorials/crash-course/crash-course-4.gif)

This is the [DAG](./architecture-overview.md#the-engine) from the Architecture Overview at work. `ticking_table` is the parent (upstream) table, and `new_ticking_table` is its child (downstream) table. The new table therefore ticks along with its source. In each update cycle that adds rows to `ticking_table`, the engine sends `new_ticking_table` a change summary that lists only the newly added rows, so `new_ticking_table` handles just those rows instead of the whole table.

Most real-world use cases for ticking data involve connecting to data streams that update continuously. Setting up these connections is outside the scope of this crash course. When you need one, see Deephaven's [Apache Kafka integration](../../how-to-guides/data-import-export/kafka-stream.md), or use [`TablePublisher`](../../how-to-guides/table-publisher.md#table-publisher) or [`DynamicTableWriter`](../../how-to-guides/table-publisher.md#dynamictablewriter) for other real-time sources.

## Replay static data as ticking data

Real-time data is Deephaven's mission statement. One of the easiest ways to work with realistic ticking data is to replay static data with the [`TableReplayer`](../../reference/table-operations/create/Replayer.md). Use it to replay the `crypto` table ingested above:

```python test-set=3 ticking-table order=null
from deephaven.replay import TableReplayer

replayer = TableReplayer("2023-02-09T12:09:18 ET", "2023-02-09T12:58:09 ET")
replayed_crypto = replayer.add_table(
    crypto.sort("Timestamp"), "Timestamp"
).sort_descending("Timestamp")
replayer.start()
```

When the replay starts, its clock begins at the start time passed to [`TableReplayer`](../../reference/table-operations/create/Replayer.md) and advances in real time. Each row appears in `replayed_crypto` once the clock reaches the row's `Timestamp`. The code uses [`sort`](../../reference/table-operations/sort/sort.md) to order `crypto` by `Timestamp` first, because the replayer can't replay timestamps that decrease. [`sort_descending`](../../reference/table-operations/sort/sort-descending.md) then puts the newest rows at the top.

Next, [Basic Table Operations](./table-ops.md) covers the table operations that appear in almost every query.
