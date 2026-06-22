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

```groovy test-set=1 order=null
import static io.deephaven.csv.CsvTools.readCsv

metricCentury = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/MetricCentury/csv/metriccentury.csv")
```

## Replay the data

The data is in memory. We can replay it with the following steps:

- Import [`Replayer`](../reference/table-operations/create/Replayer.md).
- Set a start and end time for data replay.
  - These times correspond to those in the table itself.
- Create the replayer using the set start and end time.
- Call [`replay`](../reference/table-operations/create/Replayer.md#methods) to prepare the replayed table.
  This takes the table to replay and the timestamp column name as input.
- Call [`start`](../reference/table-operations/create/Replayer.md#methods) to start replaying data.

```groovy test-set=1 order=replayedResult
import io.deephaven.engine.table.impl.replay.Replayer

startTime = parseInstant("2019-08-25T11:34:56.000 ET")
endTime = parseInstant("2019-08-25T17:10:21.000 ET")

resultReplayer = new Replayer(startTime, endTime)

replayedResult = resultReplayer.replay(metricCentury, "Time")
resultReplayer.start()
```

## Replay a table with no date-time column

Some historical data tables don't have a date-time column.

```groovy test-set=2 order=null
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv")
```

In such a case, a date-time column can be added.

```groovy test-set=2 order=null
startTime = parseInstant("2022-01-01T00:00:00 ET")

irisWithDatetimes = iris.update("Timestamp = startTime + i * SECOND")
```

Then, the data can be replayed just as before.

```groovy test-set=2 order=null ticking-table
import io.deephaven.engine.table.impl.replay.Replayer


startTime = parseInstant("2022-01-01T00:00:00 ET")
endTime = parseInstant("2022-01-01T00:02:30 ET")

replayer = new Replayer(startTime, endTime)
replayedIris = replayer.replay(irisWithDatetimes, "Timestamp")
replayer.start()
```

## Replay multiple tables

Real-time applications in Deephaven commonly involve more than a single ticking table. These tables tick simultaneously. A table replayer can be used to replay multiple tables at the same time, provided that the timestamps overlap.

The following code creates two tables with timestamps that overlap.

```groovy test-set=3 order=source1,source2
source1 = emptyTable(20).update("Timestamp = '2024-01-01T08:00:00 ET' + i * SECOND")
source2 = emptyTable(25).update("Timestamp = '2024-01-01T08:00:00 ET' + i * (int)(0.8 * SECOND)")
```

To replay multiple tables with the same replayer, simply call `replay` twice before `start`.

```groovy test-set=3 order=replayedSource1,replayedSource2 ticking-table
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
- [How to write data to a real-time in-memory table](./table-publisher.md)
- [Replayer](../reference/table-operations/create/Replayer.md)
