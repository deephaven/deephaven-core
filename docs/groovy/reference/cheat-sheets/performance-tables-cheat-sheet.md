---
title: Performance tables cheat sheet
---

Deephaven makes several performance tables available. They track metrics about running queries and server state. The tables fall into three groups: performance log tables of raw data, derived tables built from those logs, and [Barrage](../../conceptual/what-is-barrage.md) performance tables.

The performance log tables hold the raw data and suit custom performance statistics. They record queries and update cycles across the whole server. By default, the query performance logs leave out an operation that takes less than a millisecond and performs no data reads or metadata operations, and the update performance log combines very short updates into one `Aggregated Small Updates` row per interval. The derived tables suit dashboards: the per-query tables filter to one [evaluation number](../../how-to-guides/performance/performance-tables.md#evaluation-numbers), and the [tree tables](../../how-to-guides/tree-table.md) organize every query or operation as a tree.

For what the performance log and derived tables contain and how to use them, see [How to use Deephaven's performance tables](../../how-to-guides/performance/performance-tables.md). For what the Barrage tables record, see [Interpret Barrage metrics](../../conceptual/barrage-metrics.md).

> [!NOTE]
> The word "log" in this context refers to the [append-only](../../conceptual/table-types.md#specialization-1-append-only) nature of these tables, not to log files.

## Performance log tables

The log-table methods are static methods of [`TableLoggers`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html), which the Groovy console statically imports, so you can call them without a class name. Each returns one of the raw performance log tables:

| Method                                                                                                                                 | What it records                                                                              |
| -------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [`updatePerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#updatePerformanceLog())                   | Update performance data for each operation that processes updates, over each interval        |
| [`updatePerformanceAncestorsLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#updatePerformanceAncestorsLog()) | The ancestors of each operation in the update graph, for tracing an operation to its sources |
| [`queryOperationPerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#queryOperationPerformanceLog())   | Performance data for the individual operations within each query                             |
| [`queryPerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#queryPerformanceLog())                     | Performance data for each query as a whole                                                   |
| [`processMetricsLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#processMetricsLog())                         | Metrics collected for the current Deephaven engine process                                   |
| [`processInfoLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#processInfoLog())                               | A static table of information about the current Deephaven engine process                     |
| [`serverStateLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#serverStateLog())                               | Memory use, update graph, and garbage collection statistics, sampled periodically            |

```groovy order=upl,upal,qopl,qpl,pml,pil,ssl
upl = updatePerformanceLog()
upal = updatePerformanceAncestorsLog()
qopl = queryOperationPerformanceLog()
qpl = queryPerformanceLog()
pml = processMetricsLog()
pil = processInfoLog()
ssl = serverStateLog()
```

## Derived tables

Several derived tables are views over the log tables. Their names typically drop the `Log` suffix. The derived tables add computed columns, such as:

- `TimeSecs`, the elapsed time in seconds.
- `QueryMemUsedPct`, memory use as a fraction of the maximum heap size.

The derived-table methods are static methods of [`PerformanceQueries`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html). The per-query methods take that query's [evaluation number](../../how-to-guides/performance/performance-tables.md#evaluation-numbers), the unique ID Deephaven assigns to each query. Find the evaluation number in the `EvaluationNumber` column of the [Query Performance Log](../../how-to-guides/performance/performance-tables.md#query-performance-log). The other methods take no argument. The main derived tables are:

| Method                                                                                                                                                     | What it records                                                                                                                                                                                                                                                                                                      |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`queryPerformance`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryPerformance(long))                                     | Performance data for one query, given its evaluation number                                                                                                                                                                                                                                                          |
| [`queryOperationPerformance`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryOperationPerformance(long))                   | Performance data for each operation in one query, such as each `where` or `update` call, given the query's evaluation number                                                                                                                                                                                         |
| [`queryUpdatePerformance`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryUpdatePerformance(long))                         | Update performance data for the operations in one query, given its evaluation number                                                                                                                                                                                                                                 |
| [`queryUpdatePerformanceMap`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryUpdatePerformanceMap(long))                   | A map of summary tables for one query's updates, such as the worst intervals and summary statistics, given its evaluation number                                                                                                                                                                                     |
| [`serverState`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#serverState())                                                   | The derived view of the [Server State Log](../../how-to-guides/performance/performance-tables.md#server-state-log): memory use and availability as fractions of the maximum heap, the fraction of time spent in garbage collection, and [update graph cycle](../../conceptual/dag.md#update-graph-cycles) statistics |
| [`queryPerformanceAsTreeTable`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryPerformanceAsTreeTable())                   | Every query, as a [tree table](../../how-to-guides/tree-table.md)                                                                                                                                                                                                                                                    |
| [`queryOperationPerformanceAsTreeTable`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryOperationPerformanceAsTreeTable()) | Every query and its operations, merged into one [tree table](../../how-to-guides/tree-table.md)                                                                                                                                                                                                                      |

The Groovy console also defines a `queryUpdatePerformanceSet` closure. It takes an evaluation number and assigns each table in the `queryUpdatePerformanceMap` result to a script variable named by its key.

The following example creates the derived tables other than `queryUpdatePerformanceMap`, using `1` as the evaluation number:

```groovy order=qup,qp,qop,ss,qptt,qoptt
import io.deephaven.engine.table.impl.util.PerformanceQueries

qup = PerformanceQueries.queryUpdatePerformance(1)
qp = PerformanceQueries.queryPerformance(1)
qop = PerformanceQueries.queryOperationPerformance(1)
ss = PerformanceQueries.serverState()
qptt = PerformanceQueries.queryPerformanceAsTreeTable()
qoptt = PerformanceQueries.queryOperationPerformanceAsTreeTable()
```

## Barrage performance

[Barrage](../../conceptual/what-is-barrage.md) is Deephaven's interprocess communication (IPC) protocol for streaming tables between processes. Deephaven records two sets of statistics for it:

- Subscription metrics, for subscriptions, including a snapshot that [`BarrageTableResolver.snapshot`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) takes through a subscription.
- Snapshot metrics, for snapshot requests such as an Arrow Flight `DoGet`.

Which process records a statistic depends on its side of the connection:

- **Sender statistics:** the process that publishes the table records them.
- **Receiver statistics:** a process that subscribes to or snapshots a table through the Java Barrage client records them, in its own subscription metrics table. This includes the [Java client](../../how-to-guides/java-client.md) and a Deephaven server that uses a [`BarrageSession`](../../how-to-guides/capture-tables.md), a [URI](../../how-to-guides/use-uris.md), or [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html).
  Clients such as `pydeephaven` and the JavaScript client record no receiver statistics.

For details, see [Barrage metrics for performance monitoring](../../how-to-guides/performance/barrage-performance.md).

Access the subscription and snapshot metrics tables as follows:

```groovy order=subs,snaps
import io.deephaven.extensions.barrage.BarragePerformanceLog

subs = BarragePerformanceLog.getInstance().getSubscriptionTable()
snaps = BarragePerformanceLog.getInstance().getSnapshotTable()
```

### Identify a table

The `TableId` and `TableKey` columns identify what each row describes:

- For sender statistics, `TableId` is the source table's `System.identityHashCode`, written in hexadecimal.
- For receiver statistics, `TableId` is the identity hash code of the subscriber's local copy of the table.
- Subscriptions to one table at different [update intervals](../../how-to-guides/performance/barrage-performance.md#update-interval) are tracked separately but share the same `TableId` and `TableKey`.
- For [tree table](../../how-to-guides/tree-table.md) and [rollup table](../../how-to-guides/rollup-table.md) subscriptions, `TableId` is the identity hash code of the subscription rather than of a table.
- For every table, `TableKey` defaults to the table's description, which [`Table.getDescription`](/core/javadoc/io/deephaven/engine/table/Table.html#getDescription()) returns. To override it, set the [`Table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE) table attribute with [`withAttributes`](../table-operations/select/withAttributes.md). For a tree or rollup table subscription, the key defaults to the tree or rollup table's description; set the attribute on the tree or rollup table itself.

```groovy order=null
t = emptyTable(0)
attrTable = t.withAttributes([(Table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE): "MyTableKey"])
```

`withAttributes` returns a new table. Publish `attrTable`, not `t`, to record metrics under `MyTableKey`.

> [!NOTE]
> The web client often applies transformations, such as [sorts](../table-operations/sort/sort.md) and [filters](../table-operations/filter/where.md), to a table before it subscribes to it. Most transformations drop the [`Table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE) attribute, which has these effects:
>
> - Deephaven records the transformed table only when `BarragePerformanceLog.enableAll` is `true`, which is the default.
> - The transformed table's `TableKey` is its own description.
> - If a non-web client also subscribes to the original table, the metrics table shows rows for both tables, with different `TableId` values.

### Barrage subscription metrics

Deephaven aggregates subscription statistics over each flush interval, which `BarragePerformanceLog.cycleDurationMillis` sets, and records them as percentiles. Each row holds one statistic for one tracked source over one flush interval. A tracked source is one of the following:

- A table at one [update interval](../../how-to-guides/performance/barrage-performance.md#update-interval).
- A [tree table](../../how-to-guides/tree-table.md) or [rollup table](../../how-to-guides/rollup-table.md) subscription.
- A receiver's local copy of a table.

A statistic with no samples in an interval has no row. The subscription metrics table has the following columns:

| Column                            | Description                                                                                                                                 |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| TableId                           | The identity hash code of the tracked source's table, subscription, or local copy, as described under [Identify a table](#identify-a-table) |
| TableKey                          | The tracked source's key, which defaults to the table's description, as described under [Identify a table](#identify-a-table)               |
| StatType                          | The statistic the row records, from the next table                                                                                          |
| Time                              | The time at which the flush interval ended                                                                                                  |
| Count                             | The number of samples in the flush interval                                                                                                 |
| Pct50, Pct75, Pct90, Pct95, Pct99 | The 50th, 75th, 90th, 95th, and 99th percentiles of the samples                                                                             |
| Max                               | The largest sample                                                                                                                          |

The following table lists the `StatType` values. Its Sender / Receiver column says which side records each one. For where in an update's life each statistic is recorded, see [Interpret Barrage metrics](../../conceptual/barrage-metrics.md).

| StatType             | Sender / Receiver | Description                                                                                                                                                                                                                                                                          |
| -------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| EnqueueNanos         | Sender            | The time it took to record changes that occurred during a single [update graph cycle](../../conceptual/dag.md#update-graph-cycles)                                                                                                                                                   |
| AggregateNanos       | Sender            | The time it took to aggregate pending updates into a message; recorded for every compaction and for every range a propagation packages, even a range of one update                                                                                                                   |
| PropagateNanos       | Sender            | The elapsed time of one propagation phase: writing one message to the subscribers it goes to                                                                                                                                                                                         |
| SnapshotNanos        | Sender            | The time it took to take one snapshot step for a new or changed subscription; for [tree table](../../how-to-guides/tree-table.md) and [rollup table](../../how-to-guides/rollup-table.md) subscriptions, every update is sent as a snapshot and records a value                      |
| UpdateJobNanos       | Sender            | The time it took to run one full cycle of the off-thread propagation logic                                                                                                                                                                                                           |
| WriteNanos           | Sender            | The elapsed time of one write of the update, to one subscriber or to every subscriber that shares an update                                                                                                                                                                          |
| WriteBytes           | Sender            | The size in bytes of one write of the update, every message in it including dictionary batches; recorded once for each subscriber it was sent to                                                                                                                                     |
| PendingDeltaCount    | Sender            | How many [pending updates](../../how-to-guides/performance/barrage-performance.md#compact-pending-deltas) the server was holding, un-propagated, after recording an update graph cycle's changes; a compacted update counts once, even though it stands for every cycle it coalesced |
| PendingDeltaBytes    | Sender            | Approximate heap footprint, in bytes, of the chunk storage those pending updates own                                                                                                                                                                                                 |
| DeserializationNanos | Receiver          | The time it took to read and deserialize one incoming gRPC message; an update split across several messages records one value per message                                                                                                                                            |
| ProcessUpdateNanos   | Receiver          | The time it took to apply a single message (update or snapshot): during an [update graph cycle](../../conceptual/dag.md#update-graph-cycles) for a subscription, or as the message arrives for a snapshot                                                                            |
| RefreshNanos         | Receiver          | The time it took to apply all queued messages during a single [update graph cycle](../../conceptual/dag.md#update-graph-cycles); recorded only for subscriptions, once per cycle, even when nothing is queued                                                                        |

### Barrage snapshot metrics

Deephaven records snapshot metrics for snapshots requested with a Barrage snapshot request or an Arrow Flight `DoGet`. It records one row per snapshot request for a refreshing table. For a static table, Deephaven sends the snapshot in chunks and records one row per chunk, all with the same `RequestTime`. Besides `TableId` and `TableKey`, the snapshot metrics table has the following columns:

| Column        | Description                                                                                                                        |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| RequestTime   | The time at which the server received the snapshot request                                                                         |
| QueueNanos    | The time from receiving the request until work on it began, including waiting for the requested table to be ready and for a thread |
| SnapshotNanos | The time it took to construct a consistent snapshot of the source table                                                            |
| WriteNanos    | The time it took to write the snapshot or chunk                                                                                    |
| WriteBytes    | The size in bytes of the snapshot or chunk, every message in it including dictionary batches                                       |

For a chunked static-table snapshot, `SnapshotNanos` in each chunk's row is the total snapshot time so far, while `WriteNanos` and `WriteBytes` cover only that chunk.

### Configuration

The following server-side flags change the behavior of [Barrage metrics](../../how-to-guides/performance/barrage-performance.md):

- `-DBarragePerformanceLog.enableAll`: whether to record metrics for tables that do not have an explicit `TableKey` (default: `true`)
- `-DBarragePerformanceLog.cycleDurationMillis`: the interval at which to flush aggregated statistics (default: `60000`, once per minute)

## Related documentation

- [Interpret Barrage metrics](../../conceptual/barrage-metrics.md)
- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [Deephaven's live directed acyclic graph (DAG)](../../conceptual/dag.md)
- [Deephaven's table types](../../conceptual/table-types.md)
- [How to use Deephaven's performance tables](../../how-to-guides/performance/performance-tables.md)
- [Barrage metrics for performance monitoring](../../how-to-guides/performance/barrage-performance.md)
- [Capture remote tables with Barrage](../../how-to-guides/capture-tables.md)
- [Extract data with a Java client](../../how-to-guides/java-client.md)
- [Use URIs to share tables](../../how-to-guides/use-uris.md)
- [Tree tables](../../how-to-guides/tree-table.md)
- [Rollup tables](../../how-to-guides/rollup-table.md)
- [`emptyTable`](../table-operations/create/emptyTable.md)
- [`sort`](../table-operations/sort/sort.md)
- [`where`](../table-operations/filter/where.md)
- [`withAttributes`](../table-operations/select/withAttributes.md)
- [`BarragePerformanceLog` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/BarragePerformanceLog.html)
- [`BarrageTableResolver` Javadoc](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html)
- [`PerformanceQueries` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html)
- [`TableLoggers` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
