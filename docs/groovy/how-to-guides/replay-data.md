---
title: Replay data from static tables
---

This guide shows you how to replay historical data as if it were live. A replayer adds each row of a static table to a [ticking table](../conceptual/table-types.md) when its replay clock reaches the row's timestamp. A ticking table is a table whose rows change as new data arrives. The replay clock starts at a start time you choose and advances in real time. Replaying pre-recorded data is useful for learning and testing, and for fields that integrate historical data into real-time analysis, such as machine learning, validation, modeling, simulation, and forecasting. This guide covers replaying a table, replaying a table that has no timestamp column, and replaying multiple tables in sync.

## Prepare a historical data table

To replay historical data, you need a table with a timestamp column of type [`Instant`](../reference/query-language/types/date-time.md). The timestamp column must not contain null values, and its values must not decrease from one row to the next. Sort the table on that column with [`sort`](../reference/table-operations/sort/sort.md) first if needed. If your table has no timestamp column, see [Replay a table with no timestamp column](#replay-a-table-with-no-timestamp-column).

Let's grab a table from Deephaven's [examples](https://github.com/deephaven/examples/) repository. This example uses data from a 100 km bike ride in a file called `metriccentury.csv`. Its `Time` column holds the timestamps, already in ascending order.

```groovy test-set=1 order=null
import static io.deephaven.csv.CsvTools.readCsv

metricCentury = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv")
```

## Replay a table

Load the `metricCentury` table from [Prepare a historical data table](#prepare-a-historical-data-table) first. Then replay it with the following steps:

- Import [`Replayer`](../reference/table-operations/create/Replayer.md).
- Choose a start time and an end time for the replay.
  - The replay clock starts at the start time and advances in real time. Replay stops at the end time.
  - To replay the whole table, use its first and last timestamps, as this example does with the first and last values in `Time`.
- Create the replayer with those start and end times.
  - The `Replayer` constructor takes [`Instant`](../reference/query-language/types/date-time.md) values, so use [`parseInstant`](../reference/time/datetime/parseInstant.md) to convert date-time strings.
- Call [`replay`](../reference/table-operations/create/Replayer.md#methods) with the source table and the name of its timestamp column.
  - It returns a new ticking table. The table starts with every source row whose timestamp is at or before the start time. In this example, that's the first row.
  - The replayer adds each remaining row to the new table when the replay clock reaches that row's timestamp.
  - The source table itself doesn't change.
  - The timestamp column must meet the requirements in [Prepare a historical data table](#prepare-a-historical-data-table).
- Call [`start`](../reference/table-operations/create/Replayer.md#methods) to start replaying data.

```groovy test-set=1 order=null ticking-table
import io.deephaven.engine.table.impl.replay.Replayer

startTime = parseInstant("2019-08-25T15:34:56Z")
endTime = parseInstant("2019-08-25T21:10:21Z")

replayer = new Replayer(startTime, endTime)
replayedTable = replayer.replay(metricCentury, "Time")
replayer.start()
```

The ride lasts about five and a half hours, so this replay takes that long to finish. Call [`shutdown`](../reference/table-operations/create/Replayer.md#methods) to stop replaying early.

## Replay a table with no timestamp column

Some historical data tables don't have a timestamp column.

```groovy test-set=2 order=null
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv")
```

In such a case, add a timestamp column of type [`Instant`](../reference/query-language/types/date-time.md).

```groovy test-set=2 order=null
startTime = parseInstant("2022-01-01T00:00:00 ET")

irisWithDatetimes = iris.update("Timestamp = startTime + i * SECOND")
```

Replay `irisWithDatetimes` with a [`Replayer`](../reference/table-operations/create/Replayer.md), the same way as in [Replay a table](#replay-a-table). Pass the new `Timestamp` column as the timestamp column. This example reuses `startTime` from the previous block as the replay start time. The table has 150 rows one second apart, so an end time 2 minutes 30 seconds later replays every row.

```groovy test-set=2 order=null ticking-table
import io.deephaven.engine.table.impl.replay.Replayer

endTime = parseInstant("2022-01-01T00:02:30 ET")

replayer = new Replayer(startTime, endTime)
replayedIris = replayer.replay(irisWithDatetimes, "Timestamp")
replayer.start()
```

## Replay multiple tables

Real-time applications in Deephaven commonly involve more than one [ticking table](../conceptual/table-types.md). A single replayer can replay multiple tables at the same time. All of them share the same [replay clock](#replay-a-table), which starts at the replayer's start time and advances in real time. Each table's rows appear when that clock reaches their timestamps, so the replayed tables stay in sync.

The following code creates two tables with timestamps that overlap.

```groovy test-set=3 order=source1,source2
source1 = emptyTable(20).update("Timestamp = '2024-01-01T08:00:00 ET' + i * SECOND")
source2 = emptyTable(25).update("Timestamp = '2024-01-01T08:00:00 ET' + i * (long)(0.8 * SECOND)")
```

To replay multiple tables with the same replayer, call [`replay`](../reference/table-operations/create/Replayer.md#methods) once per table before [`start`](../reference/table-operations/create/Replayer.md#methods).

```groovy test-set=3 order=null ticking-table
import io.deephaven.engine.table.impl.replay.Replayer

startTime = parseInstant("2024-01-01T08:00:00 ET")
endTime = parseInstant("2024-01-01T08:00:20 ET")

replayer = new Replayer(startTime, endTime)

replayedSource1 = replayer.replay(source1, "Timestamp")
replayedSource2 = replayer.replay(source2, "Timestamp")
replayer.start()
```

## Related documentation

- [Time in Deephaven](../conceptual/time-in-deephaven.md)
- [Write data to an in-memory, real-time table](./table-publisher.md)
- [`Replayer`](../reference/table-operations/create/Replayer.md)
