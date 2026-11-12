---
title: Performance tables cheat sheet
---

Deephaven makes several performance tables available. They track metrics about running queries and server state. The tables fall into three groups: performance log tables of raw data, derived tables built from those logs, and [Barrage](../../conceptual/what-is-barrage.md) performance tables.

The performance log tables hold the raw data and suit custom performance statistics. They record queries and update cycles across the whole server. By default, the query performance logs leave out an operation that takes less than a millisecond and performs no data reads or metadata operations, and the update performance log combines very short updates into one `Aggregated Small Updates` row per interval. The derived tables suit dashboards: the per-query tables filter to one [evaluation number](../../how-to-guides/performance/performance-tables.md#evaluation-numbers), and the [tree tables](../../how-to-guides/tree-table.md) organize every query or operation as a tree.

For what the performance log and derived tables contain and how to use them, see [How to use Deephaven's performance tables](../../how-to-guides/performance/performance-tables.md). For what the Barrage tables record, see [Interpret Barrage metrics](../../conceptual/barrage-metrics.md).

> [!NOTE]
> The word "log" in this context refers to the [append-only](../../conceptual/table-types.md#specialization-1-append-only) nature of these tables, not to log files.

## Performance log tables

The log-table functions are in the [`deephaven.perfmon`](/core/pydoc/code/deephaven.perfmon.html#module-deephaven.perfmon) module. Each returns one of the raw performance log tables:

| Function                                                                                                                         | What it records                                                                              |
| -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [`update_performance_log`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.update_performance_log)                     | Update performance data for each operation that processes updates, over each interval        |
| [`update_performance_ancestors_log`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.update_performance_ancestors_log) | The ancestors of each operation in the update graph, for tracing an operation to its sources |
| [`query_operation_performance_log`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.query_operation_performance_log)   | Performance data for the individual operations within each query                             |
| [`query_performance_log`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.query_performance_log)                       | Performance data for each query as a whole                                                   |
| [`process_metrics_log`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.process_metrics_log)                           | Metrics collected for the current Deephaven engine process                                   |
| [`process_info_log`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.process_info_log)                                 | A static table of information about the current Deephaven engine process                     |
| [`server_state_log`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state_log)                                 | Memory use, update graph, and garbage collection statistics, sampled periodically            |

```python order=upl,upal,qopl,qpl,pml,pil,ssl
import deephaven.perfmon as pm

upl = pm.update_performance_log()
upal = pm.update_performance_ancestors_log()
qopl = pm.query_operation_performance_log()
qpl = pm.query_performance_log()
pml = pm.process_metrics_log()
pil = pm.process_info_log()
ssl = pm.server_state_log()
```

## Derived tables

Several derived tables are views over the log tables. Their names typically drop the `_log` suffix. The derived tables add computed columns, such as:

- `TimeSecs`, the elapsed time in seconds.
- `QueryMemUsedPct`, memory use as a fraction of the maximum heap size.

The per-query functions take that query's [evaluation number](../../how-to-guides/performance/performance-tables.md#evaluation-numbers), the unique ID Deephaven assigns to each query. Find the evaluation number in the `EvaluationNumber` column of the [Query Performance Log](../../how-to-guides/performance/performance-tables.md#query-performance-log). The other functions take no argument. The main derived tables are:

| Function                                                                                                                                     | What it records                                                                                                                                                                                                                                                                                                      |
| -------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`query_performance`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.query_performance)                                           | Performance data for one query, given its evaluation number                                                                                                                                                                                                                                                          |
| [`query_operation_performance`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.query_operation_performance)                       | Performance data for each operation in one query, such as each `where` or `update` call, given the query's evaluation number                                                                                                                                                                                         |
| [`query_update_performance`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.query_update_performance)                             | Update performance data for the operations in one query, given its evaluation number                                                                                                                                                                                                                                 |
| [`query_update_performance_map`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.query_update_performance_map)                     | A dictionary of summary tables for one query's updates, such as the worst intervals and summary statistics, given its evaluation number                                                                                                                                                                              |
| [`server_state`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.server_state)                                                     | The derived view of the [Server State Log](../../how-to-guides/performance/performance-tables.md#server-state-log): memory use and availability as fractions of the maximum heap, the fraction of time spent in garbage collection, and [update graph cycle](../../conceptual/dag.md#update-graph-cycles) statistics |
| [`query_performance_tree_table`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.query_performance_tree_table)                     | Every query, as a [tree table](../../how-to-guides/tree-table.md)                                                                                                                                                                                                                                                    |
| [`query_operation_performance_tree_table`](/core/pydoc/code/deephaven.perfmon.html#deephaven.perfmon.query_operation_performance_tree_table) | Every query and its operations, merged into one [tree table](../../how-to-guides/tree-table.md)                                                                                                                                                                                                                      |

The following example creates the derived tables other than `query_update_performance_map`, using `1` as the evaluation number:

```python order=qup,qp,qop,ss,qptt,qoptt
import deephaven.perfmon as pm

qup = pm.query_update_performance(1)
qp = pm.query_performance(1)
qop = pm.query_operation_performance(1)
ss = pm.server_state()
qptt = pm.query_performance_tree_table()
qoptt = pm.query_operation_performance_tree_table()
```

## Barrage performance

[Barrage](../../conceptual/what-is-barrage.md) is Deephaven's interprocess communication (IPC) protocol for streaming tables between processes. Deephaven records two sets of statistics for it:

- Subscription metrics, for subscriptions to static or ticking tables.
- Snapshot metrics, for one-off requests such as an Arrow Flight `DoGet`.

Which process records a statistic depends on its side of the connection:

- **Sender statistics:** the process that publishes the table records them.
- **Receiver statistics:** a process that subscribes to or snapshots a table through the Java Barrage client records them, in its own subscription metrics table. This includes the [Java client](../../how-to-guides/java-client.md) and a Deephaven server that uses [`barrage_session`](../data-import-export/barrage/barrage-session.md) or a [URI](../../how-to-guides/use-uris.md).
  Clients such as [`pydeephaven`](../../getting-started/pyclient-quickstart.md) and the JavaScript client record no receiver statistics.

For details, see [Barrage metrics for performance monitoring](../../how-to-guides/performance/barrage-performance.md).

Access the subscription and snapshot metrics tables as follows:

```python order=subs,snaps
from deephaven.perfmon import (
    barrage_subscription_performance_log,
    barrage_snapshot_performance_log,
)

subs = barrage_subscription_performance_log()
snaps = barrage_snapshot_performance_log()
```

### Identify a table

The `TableId` and `TableKey` columns identify what each row describes:

- For sender statistics, `TableId` is the source table's `System.identityHashCode`, written in hexadecimal.
- For receiver statistics, `TableId` is the identity hash code of the subscriber's local copy of the table.
- Subscriptions to one table at different [update intervals](../../how-to-guides/performance/barrage-performance.md#update-interval) are tracked separately but share the same `TableId` and `TableKey`.
- For [tree table](../../how-to-guides/tree-table.md) and [rollup table](../../how-to-guides/rollup-table.md) subscriptions, `TableId` is the identity hash code of the subscription rather than of a table.
- For every table, `TableKey` defaults to the table's description, which [`Table.getDescription`](/core/javadoc/io/deephaven/engine/table/Table.html#getDescription()) returns. To override it, set the [`BARRAGE_PERFORMANCE_KEY_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE) table attribute with [`with_attributes`](../table-operations/create/withAttributes.md). For a tree or rollup table subscription, the key defaults to the tree or rollup table's description; set the attribute on the tree or rollup table itself.

```python order=null
import jpy
from deephaven import empty_table

t = empty_table(0)
_JTable = jpy.get_type("io.deephaven.engine.table.Table")
attr_table = t.with_attributes(
    {_JTable.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE: "MyTableKey"}
)
```

`with_attributes` returns a new table. Publish `attr_table`, not `t`, to record metrics under `MyTableKey`.

> [!NOTE]
> The web client often applies transformations, such as [sorts](../table-operations/sort/sort.md) and [filters](../table-operations/filter/where.md), to a table before it subscribes to it. Most transformations drop the [`BARRAGE_PERFORMANCE_KEY_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE) attribute, which has these effects:
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
- [Extract data with a Java client](../../how-to-guides/java-client.md)
- [Use URIs to share tables](../../how-to-guides/use-uris.md)
- [Tree tables](../../how-to-guides/tree-table.md)
- [Rollup tables](../../how-to-guides/rollup-table.md)
- [`barrage_session`](../data-import-export/barrage/barrage-session.md)
- [`empty_table`](../table-operations/create/emptyTable.md)
- [`sort`](../table-operations/sort/sort.md)
- [`where`](../table-operations/filter/where.md)
- [`with_attributes`](../table-operations/create/withAttributes.md)
- [`perfmon` Pydoc](/core/pydoc/code/deephaven.perfmon.html#module-deephaven.perfmon)
- [`with_attributes` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_attributes)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
