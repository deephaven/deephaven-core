---
title: Performance tables cheat sheet
sidebar_label: Performance tables
---

Deephaven makes several performance log tables available.

> [!NOTE]
> The word "log" in this context refers to the append-only nature of these tables, not to
> log files in the traditional sense.
>
> These tables track various metrics across Deephaven, and in normal
> circumstances will be populated with raw performance data
> pertaining to your running queries and server state.

```groovy order=upl,qopl,qpl,pml,pil,ssl,qptt,qoptt
import io.deephaven.engine.table.impl.util.PerformanceQueries

upl = updatePerformanceLog()
qopl = queryOperationPerformanceLog()
qpl = queryPerformanceLog()
pml = processMetricsLog()
pil = processInfoLog()
ssl = serverStateLog()

qoptt = PerformanceQueries.queryPerformanceAsTreeTable()
qptt = PerformanceQueries.queryOperationPerformanceAsTreeTable()
```

Several derived tables are defined on top of them, typically by removing the suffix `Log` from the name; these derived tables
accumulate data and present it in more user-friendly column types (e.g., time units in fractional seconds instead of nanoseconds for
CPU time metrics), and as such are more useful for dashboards, where the `Log` tables are more useful as sources of raw data
for user-defined computation of performance statistics. For the derived tables in the list below that are related to query performance,
the functions that create them take one query evaluation number id as a parameter to show results for a particular query (where the `Log`
tables show data for all queries).

```groovy order=qup,qp,qop,ss
import io.deephaven.engine.table.impl.util.PerformanceQueries

qup = PerformanceQueries.queryUpdatePerformance(1)
qp = PerformanceQueries.queryPerformance(1)
qop = PerformanceQueries.queryOperationPerformance(1)
ss = PerformanceQueries.serverState().tail(32)
```

## Barrage Performance

Barrage is the name of our IPC table transport. There are two sets of statistics. The `subscription`
metrics are recorded for ticking subscriptions. The `snapshot` metrics are recorded for one-off
requests such as via an Arrow Flight `DoGet`.

Tables are identified by their `TableId` and `TableKey`. The `TableId` is determined by the source table's
`System.identityHashCode()`. The `TableKey` defaults to `Table#getDescription()` but can be overridden by setting a
table attribute.

```groovy order=null
table = emptyTable(0)
table.setAttribute(Table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE, "MyTableKey")
```

> [!NOTE]
> The web client applies transformations to every table that it subscribes to. If a table is also subscribed by a non-web
> client then statistics for the original table and the transformed table will both appear in the metrics table. Their
> `TableId` will differ. Most transformations clear the `TableKey` attribute, such as when a column is sorted or filtered
> by the user through the GUI.

Here are server side flags that change the behavior of barrage metrics.

- `-DBarragePerformanceLog.enableAll`: record metrics for tables that do not have an explicit `TableKey` (default: `true`)
- `-DBarragePerformanceLog.cycleDurationMillis`: the interval to flush aggregated statistics (default: `60000` - once per minute)

You can access these tables as follows:

```groovy order=subs,snaps
import io.deephaven.extensions.barrage.BarragePerformanceLog

subs = BarragePerformanceLog.getInstance().getSubscriptionTable()
snaps = BarragePerformanceLog.getInstance().getSnapshotTable()
```

### Barrage Subscription Metrics

Subscription statistics are presented in percentiles bucketed over a time period.

Here are the various metrics that are recorded by the deephaven-core server:

| Stat Type             | Sender / Receiver | Description                                                                         |
| --------------------- | ----------------- | ----------------------------------------------------------------------------------- |
| EnqueueMillis         | Sender            | The time it took to record changes that occurred during a single update graph cycle |
| AggregateMillis       | Sender            | The time it took to aggregate multiple updates within the same interval             |
| PropagateMillis       | Sender            | The time it took to deliver an aggregated message to all subscribers                |
| SnapshotMillis        | Sender            | The time it took to snapshot data for a new or changed subscription                 |
| UpdateJobMillis       | Sender            | The time it took to run one full cycle of the off-thread propagation logic          |
| WriteMillis           | Sender            | The time it took to write the update to a single subscriber                         |
| WriteMegabits         | Sender            | The payload size of the update in megabits                                          |
| DeserializationMillis | Receiver          | The time it took to read and deserialize the update from the wire                   |
| ProcessUpdateMillis   | Receiver          | The time it took to apply a single update during the update graph cycle             |
| RefreshMillis         | Receiver          | The time it took to apply all queued updates during a single udpate graph cycle     |

### Barrage Snapshot Metrics

Snapshot statistics are presented once per request.

| Stat Type      | Description                                                             |
| -------------- | ----------------------------------------------------------------------- |
| QueueMillis    | The time it took waiting for a thread to process the request            |
| SnapshotMillis | The time it took to construct a consistent snapshot of the source table |
| WriteMillis    | The time it took to write the snapshot                                  |
| WriteMegabits  | The payload size of the snapshot in megabits                            |
