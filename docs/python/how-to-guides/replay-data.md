---
title: Replay data from static tables
sidebar_label: Replay
---

This guide will show you how to replay historical data as if it was live data in Deephaven.

Deephaven excels at handling live data. Integrating historical data into real-time analysis is common in a multitude of fields including machine learning, validation, modeling, simulation, and forecasting.

For learning, testing, and other purposes, it can be useful to replay pre-recorded data as if it were live.

In this guide, we will take historical data and play it back as real-time data based on timestamps in a table. This example could be easily extended towards a variety of real-world applications.

## Get a historical data table

To replay historical data, we need a table with timestamps in [`DateTime`](../reference/query-language/types/date-time.md) format. Let's grab one from Deephaven's [examples](https://github.com/deephaven/examples/) repository. We'll use data from a 100 km bike ride in a file called `metriccentury.csv`.

```python test-set=1 order=null
from deephaven import read_csv

metric_century = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv"
)
```

## Replay the data

The data is in memory. We can replay it with the following steps:

- Import [`TableReplayer`](../reference/table-operations/create/Replayer.md).
- Set a start and end time for data replay.
  - These times correspond to those in the table itself.
- Create the replayer using the start and end time.
- Call [`add_table`](../reference/table-operations/create/Replayer.md#methods) to prepare the replayed table.
  - This takes two inputs: the table and the `DateTime` column name.
- Call [`start`](../reference/table-operations/create/Replayer.md#methods) to start replaying data.

```python test-set=1 order=null ticking-table
from deephaven.replay import TableReplayer
from deephaven.time import to_j_instant

start_time = to_j_instant("2019-08-25T15:34:55Z")
end_time = to_j_instant("2019-08-25T17:10:22Z")

replayer = TableReplayer(start_time, end_time)
replayed_table = replayer.add_table(metric_century, "Time")
replayer.start()
```

## Replay a table with no date-time column

Some historical data tables don't have a date-time column.

```python test-set=2 order=null
from deephaven import read_csv

iris = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
```

In such a case, a date-time column can be added.

```python test-set=2 order=null
from deephaven.time import to_j_instant

start_time = to_j_instant("2022-01-01T00:00:00 ET")

iris_with_datetimes = iris.update(["Timestamp = start_time + i * SECOND"])
```

Then, the data can be replayed just as before.

```python test-set=2 order=null ticking-table
from deephaven.replay import TableReplayer
from deephaven.time import to_j_instant

start_time = to_j_instant("2022-01-01T00:00:00 ET")
end_time = to_j_instant("2022-01-01T00:02:30 ET")

replayer = TableReplayer(start_time, end_time)
replayed_iris = replayer.add_table(iris_with_datetimes, "Timestamp")
replayer.start()
```

## Replay multiple tables

Real-time applications in Deephaven commonly involve more than a single ticking table. These tables tick simultaneously. A table replayer can be used to replay multiple tables at the same time, provided that the timestamps overlap.

The following code creates two tables with timestamps that overlap.

```python test-set=3 order=source_1,source_2
from deephaven import empty_table

source_1 = empty_table(20).update(["Timestamp = '2024-01-01T08:00:00 ET' + i * SECOND"])
source_2 = empty_table(25).update(
    ["Timestamp = '2024-01-01T08:00:00 ET' + i * (int)(0.8 * SECOND)"]
)
```

To replay multiple tables with the same replayer, simply call `add_table` twice before `start`.

```python test-set=3 order=replayed_source_1,replayed_source_2 ticking-table
from deephaven.replay import TableReplayer

replayer = TableReplayer(
    start_time="2024-01-01T08:00:00 ET", end_time="2024-01-01T08:00:20 ET"
)

replayed_source_1 = replayer.add_table(table=source_1, col="Timestamp")
replayed_source_2 = replayer.add_table(table=source_2, col="Timestamp")
replayer.start()
```

## Related documentation

- [Time in Deephaven](../conceptual/time-in-deephaven.md)
- [TableReplayer](../reference/table-operations/create/Replayer.md)
