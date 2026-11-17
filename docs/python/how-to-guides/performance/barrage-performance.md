---
title: Monitor and tune Barrage performance
---

[Barrage](../../conceptual/what-is-barrage.md) is Deephaven's inter-process communication (IPC) protocol for streaming tables. It is what lets a client watch a ticking table update in real time. Its cost grows with how fast tables tick and how many clients are watching, and the metrics this guide covers show where that cost goes. This guide explains what statistics Barrage records, how to access them, and how to use them to troubleshoot. It also shows how server properties and table settings tune Barrage memory use, throughput, and compression. For a step-by-step account of where in the life of an update each statistic is recorded, see [Interpret Barrage metrics](../../conceptual/barrage-metrics.md).

## Access Barrage metrics tables

Deephaven records Barrage metrics in two tables. The subscriptions table summarizes [subscriptions](../../conceptual/what-is-barrage.md#subscriptions-vs-snapshots), which stream ongoing updates, and the snapshots table records one-time snapshot requests. Open both tables with the following code:

```python order=null
from deephaven.perfmon import (
    barrage_subscription_performance_log,
    barrage_snapshot_performance_log,
)

subs = barrage_subscription_performance_log()
snaps = barrage_snapshot_performance_log()
```

This is what the subscriptions table looks like when there are live subscriptions:

![The subscriptions table](../../assets/how-to/barragePerformance_subscriptions.png)

This is what the snapshots table looks like after processing a few requests:

![The snapshots table](../../assets/how-to/barragePerformance_snapshots.png)

### Barrage subscription metrics summary

Barrage buckets subscription statistics into percentiles over a fixed period, which you can set as described in [Configure metrics collection](#configure-metrics-collection). Each row describes one statistic, over one period, for one of the following:

- A published table at one [update interval](#update-interval).
- A [tree](../tree-table.md) or [rollup](../rollup-table.md) subscription.
- A receiver's local copy of a table.

A statistic with no samples in the period has no row. The columns are:

- `StatType`: the statistic.
- `Count`: the number of samples in the period.
- `Pct50`, `Pct75`, `Pct90`, `Pct95`, `Pct99`, and `Max`: the distribution of the sampled values.
- `TableId` and `TableKey`: the table, subscription, or local copy that the row describes.
- `Time`: the end of the period.

The server that publishes a table records its publisher statistics. The process that subscribes to or snapshots the table records receiver statistics in its own subscription metrics table. It does so only when it uses the Java Barrage client, as in these cases:

- A Deephaven server that fetches the table with [`barrage_session`](../../reference/data-import-export/barrage/barrage-session.md) or a [URI](../use-uris.md).
- An application built on the Java client.

Clients such as `pydeephaven` and the JavaScript client record no receiver statistics.

Two terms appear in the table. A _delta_ is the set of changes from one update graph cycle, and the server holds _pending deltas_ until it next sends to subscribers. A _propagation phase_ is one pass in which the server sends one message, such as a snapshot or the accumulated deltas, to the subscribers that need it.

These are the values of `StatType`:

| `StatType`             | Publisher / Receiver | Description                                                                                                                                                                                                                                  |
| ---------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EnqueueNanos`         | Publisher            | The time it took to record changes that occurred during a single [update graph cycle](../../conceptual/periodic-update-graph-configuration.md)                                                                                               |
| `AggregateNanos`       | Publisher            | The time it took to combine pending deltas into one. The server records it for every [compaction](#compact-pending-deltas). It also records it for every range of deltas it packages into a message, even a range of one delta               |
| `PropagateNanos`       | Publisher            | The elapsed time of one propagation phase, which writes one message to each subscriber it goes to                                                                                                                                            |
| `SnapshotNanos`        | Publisher            | The time it took to take one snapshot step for a new or changed subscription. For [tree](../tree-table.md) and [rollup](../rollup-table.md) subscriptions, the server sends every update as a snapshot, so every update records a value      |
| `UpdateJobNanos`       | Publisher            | The time it took to run one full pass of the background job that takes snapshots for new or changed subscriptions and sends pending deltas to subscribers                                                                                    |
| `WriteNanos`           | Publisher            | The elapsed time of one write of the update, to one subscriber or to every subscriber that [shares a write](#share-one-write-among-identical-subscribers)                                                                                    |
| `WriteBytes`           | Publisher            | The size in bytes of one write of the update. It counts every message in the write, including dictionary batches. The server records it once for each subscriber that receives the write                                                     |
| `PendingDeltaCount`    | Publisher            | How many pending deltas (recorded update graph cycles not yet sent) the server was holding after recording an update graph cycle's changes. A [compacted](#compact-pending-deltas) delta counts once, even though it combines several cycles |
| `PendingDeltaBytes`    | Publisher            | Approximate heap footprint, in bytes, of the chunks those pending deltas own. A chunk is a block of column values that holds a delta's rows                                                                                                  |
| `DeserializationNanos` | Receiver             | The time it took to read and deserialize one incoming gRPC message. An update split across several messages records one value per message                                                                                                    |
| `ProcessUpdateNanos`   | Receiver             | The time it took to apply a single update or snapshot message. A subscription applies it during an update graph cycle. A snapshot applies it as the message arrives                                                                          |
| `RefreshNanos`         | Receiver             | The time it took to apply all queued messages during a single update graph cycle. The receiver records it only for subscriptions, once per cycle, even when nothing is queued                                                                |

> [!NOTE]
> `PendingDeltaCount` and `PendingDeltaBytes` are gauges, not durations. The server samples each one once per update graph cycle in which the table changes while it has at least one subscriber. Over a period, the `Max` column is therefore more useful than the median, `Pct50`. Both measure what the server holds for subscribers it has not yet served. [Compact pending deltas](#compact-pending-deltas) explains what makes them grow and how to limit it.
>
> `PendingDeltaBytes` is approximate. It counts only the chunks a pending delta owns:
>
> - It counts each chunk's capacity, which is what the server allocated, not the rows stored in it.
> - It charges each row of a `String` or other object column eight bytes, the widest a reference can be. This overstates the reference size wherever the JVM uses compressed references.
> - It never counts the objects those references point to.
>
> For a subscription with object columns, `PendingDeltaBytes` therefore measures chunk storage, not the memory those rows retain, and it can be higher or lower than that memory.

### Barrage snapshot metrics summary

The server records snapshot statistics for each Barrage snapshot request and each Arrow Flight `DoGet`. Each row holds the table's `TableId` and `TableKey` and the `RequestTime` at which the server received the request. How many rows a request produces depends on the table:

- A refreshing table: one row per request.
- A static table: one row per part, all with the same `RequestTime`. In each row, `SnapshotNanos` is the total so far, and `WriteNanos` and `WriteBytes` are for that part. An empty static table records no row.

| Column          | Description                                                                                                                        |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `QueueNanos`    | The time from receiving the request until work on it began, including waiting for the requested table to be ready and for a thread |
| `SnapshotNanos` | The time it took to construct a consistent snapshot of the source table                                                            |
| `WriteNanos`    | The time it took to write the snapshot (or part)                                                                                   |
| `WriteBytes`    | The size in bytes of the snapshot or part, counting every message in it, including dictionary batches                              |

### Units and conversions

All durations are in nanoseconds and all payload sizes are in bytes, stored as `long`. This matches the [query and update performance logs](./performance-tables.md#raw-performance-logs). Convert them in a query when you want other units. In the snapshot table, for example, `WriteMillis = WriteNanos / 1e6` gives milliseconds. In the subscription table, `WriteNanos` and `WriteBytes` are `StatType` values, so [filter](../use-filters.md) on `StatType` and convert a percentile column, such as `MaxMegabits = Max * 8 / 1e6`.

`WriteBytes` is a size, not a rate. To compare it with link bandwidth, estimate the bytes sent in one period:

1. Multiply a `WriteBytes` row's `Count` by a typical size, such as its `Pct50`.
2. Divide by the period's length, which [`BarragePerformanceLog.cycleDurationMillis`](#configure-metrics-collection) sets to one minute by default.

`WriteBytes` counts bytes before [transport compression](#compress-barrage-data), so turning compression on does not change it. For a compressed table, fewer bytes cross the link than this estimate.

## Identify a table

The metrics tables identify each table by its `TableId` and `TableKey`. The `TableId` is an identity hash code ([`System.identityHashCode`](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/System.html#identityHashCode(java.lang.Object))), written in hexadecimal, of:

- The source table, for publisher statistics.
- The subscriber's local copy of the table, for receiver statistics.
- The subscription, for [tree](../tree-table.md) and [rollup](../rollup-table.md) table subscriptions.

The `TableKey` defaults to [`Table.getDescription`](/core/javadoc/io/deephaven/engine/table/Table.html#getDescription()). To override it, set the `BarragePerformanceTableKey` attribute on the table you publish with [`with_attributes`](../../reference/table-operations/create/withAttributes.md).

```python order=attr_table,:log
import jpy
from deephaven import empty_table

system = jpy.get_type("java.lang.System")
integer = jpy.get_type("java.lang.Integer")
j_table_type = jpy.get_type("io.deephaven.engine.table.Table")

t = empty_table(0)

# with_attributes returns a new table; publish this one to have its metrics recorded under "MyTableKey"
attr_table = t.with_attributes(
    {j_table_type.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE: "MyTableKey"}
)

table_id = integer.toHexString(system.identityHashCode(attr_table.j_table))

print(f"TableId: {table_id}")
```

Once `attr_table` is published and has a subscriber, [filter](../use-filters.md) the metrics tables on this `TableId`, or on a `TableKey` of `MyTableKey`, to find its rows.

> [!NOTE]
> Subscriptions to the same table at different [update intervals](#update-interval) are served separately, and each records its own statistics under the same `TableId` and `TableKey`. One table can therefore have several rows for the same statistic in one reporting period, and no column tells them apart. See [Interpret Barrage metrics](../../conceptual/barrage-metrics.md#2-barrage-waits-for-the-update-interval-and-coalesces-deltas).

> [!NOTE]
> For [tree](../tree-table.md) and [rollup](../rollup-table.md) tables, the attribute must be on the tree or rollup table itself. The source table's attribute isn't carried over. Without it, the key defaults to that table's description. The Python [`RollupTable`](../rollup-table.md) and [`TreeTable`](../tree-table.md) classes have no [`with_attributes`](../../reference/table-operations/create/withAttributes.md) method, so in Python a rollup or tree table is recorded under its description, or not at all when [`BarragePerformanceLog.enableAll`](#configure-metrics-collection) is `false`.

> [!NOTE]
> The web client often applies transformations, such as [sorts](../../reference/table-operations/sort/sort.md), [filters](../use-filters.md), and [flattening](../../reference/table-operations/select/flatten.md), to a table before subscribing to it. If a non-web client also subscribes to the table, the metrics table shows statistics for both the original table and the transformed one, each with its own `TableId`. Sorting, filtering, and most other transformations don't carry the `BarragePerformanceTableKey` attribute to the result, whether a query or the GUI applies them. Flattening does carry it. With [`BarragePerformanceLog.enableAll`](#configure-metrics-collection) set to `true` (the default), the transformed table is still recorded, under a `TableKey` taken from its own description.

## Configure metrics collection

Set the server properties on this page as JVM arguments, such as `-DBarragePerformanceLog.cycleDurationMillis=10000`. With the Deephaven Docker images, put them in the `START_OPTS` environment variable. You can also set them, without the `-D`, in the server's configuration file. See [Create a Deephaven configuration file](../configuration/config-file.md).

These server properties change how Barrage records metrics:

- `-DBarragePerformanceLog.enableAll`: Whether to record metrics for tables that do not have an explicit `TableKey`. Default: `true`.
- `-DBarragePerformanceLog.cycleDurationMillis`: How often, in milliseconds, the server writes aggregated statistics to the metrics tables. Default: `60000` (once per minute).

## Configure update interval and batching

These server properties set how often the server sends updates to subscribers and how it splits what it sends into messages.

### Update interval

- `-Dbarrage.minUpdateInterval`: The default update interval, in milliseconds, for subscriptions that do not request one. Default: `1000` (1 second).

When the table ticks, the server waits at least this long after the last update it sent before sending the next. The server sends immediately, without waiting, for:

- A new, changed, or removed subscription.
- Each step of a [growing subscription](#control-subscription-snapshot-size).

When the server sends immediately, it also delivers the table's pending changes to its other subscribers at the same update interval. While a subscription is growing, the others can therefore receive updates more often than the interval.

Lower `minUpdateInterval` values reduce latency but increase CPU and network usage.

### Message batching

- `-DBarrageMessageWriterImpl.batchSize`: The maximum number of rows in an Arrow [record batch](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format), and of values in a dictionary batch, when the client does not specify a batch size. A record batch is a block of rows sent as one unit. A dictionary batch holds the distinct values of a dictionary-encoded column. Default: `Integer.MAX_VALUE`. Reduce this if clients have trouble processing very large batches.
- `-DBarrageMessageWriterImpl.initialBatchSize`: The starting row count for the first record batch of a message's added rows, and again for its modified rows. Default: `4096`. The value never exceeds `batchSize`, and the server retries with a smaller batch if the first one is too large. Later batches in the same message are sized from the observed bytes per row, with a margin, to stay under the maximum message size.
- `-DBarrageMessageWriterImpl.maxOutboundMessageSize`: Maximum size (in bytes) for outbound messages when the client does not specify one. Default: `104857600` (100 MiB). This matches the default incoming message limit for Java clients. The server splits record batches, and the dictionary batches of dictionary-encoded columns, to stay under it.

## Control subscription snapshot size

When a client subscribes to a table, or changes an existing subscription (for example, by moving its [viewport](../../conceptual/what-is-barrage.md#viewports)), the server sends a snapshot of the requested data. For large tables, constructing this snapshot requires holding the data in memory on the server side, which can lead to out-of-memory errors.

To address this, Deephaven can break a large snapshot into a series of smaller parts. A subscription whose snapshot is still arriving in parts is a growing subscription. It grows by one part at a time until it holds all the requested data. The following property turns this on or off:

- `-DBarrageMessageProducer.subscriptionGrowthEnabled`: When `true` (the default), the server limits the size of each snapshot part. When `false`, the server sends the entire snapshot at once (unlimited size).

The following properties set the part size. They apply to two cases:

- Growing subscriptions, when subscription growth is enabled.
- Every snapshot of a static table requested with a Barrage snapshot request or `DoGet`. The server always sends these in parts.

The part size properties are:

- `-DBarrageUtil.minSnapshotCellCount`: The minimum number of cells (rows × columns) per snapshot part. Default: `8192`.
- `-DBarrageUtil.maxSnapshotCellCount`: The maximum number of cells per snapshot part. Default: `16777216` (approximately 16 million).

The server adaptively adjusts the part size between these bounds based on how long each snapshot takes to generate, targeting a percentage of the update graph cycle time:

- `-DBarrageUtil.targetSnapshotPercentage`: The fraction of the update graph's target cycle duration that each snapshot part should take to generate. Default: `0.25`.

### Reduce publisher memory usage

For systems serving very large tables to many subscribers, you can reduce publisher-side memory usage by lowering `maxSnapshotCellCount`. Setting `minSnapshotCellCount` equal to `maxSnapshotCellCount` fixes the part size and disables adaptive sizing:

```text
-DBarrageMessageProducer.subscriptionGrowthEnabled=true
-DBarrageUtil.minSnapshotCellCount=1000000
-DBarrageUtil.maxSnapshotCellCount=1000000
```

This configuration limits each snapshot part to at most 1 million cells, well below the 16 million default maximum. Subscribers receive the full table data as a series of snapshot parts rather than all at once.

> [!NOTE]
> Setting smaller snapshot sizes increases the time required for subscribers to receive the initial table state but reduces peak memory usage on the server. These settings affect two kinds of snapshot: the snapshot for a new or changed subscription, such as an initial subscription or a viewport change, and a snapshot request for a static table. Incremental updates are unaffected, and the server still holds them in memory.

## Compact pending deltas

A delta is the set of changes from a single update graph cycle. The server records one delta for each table and [update interval](#update-interval), and every subscriber to that table with that interval shares it. When the interval elapses, the server sends the accumulated deltas.

When subscribers are served less often than the table ticks:

- The server holds every intervening cycle's delta until the interval elapses.
- The message that subscribers eventually receive is usually the size of the combined change, not the sum of the deltas. For a [blink table](../../conceptual/table-types.md#specialization-3-blink), it is the sum, because every row must be delivered.
- Pending memory grows with both the number of cycles per interval and the size of each cycle's changes.

The `PendingDeltaCount` and `PendingDeltaBytes` metrics in the subscription table measure this memory, so check them when you tune the properties below.

To limit the growth of pending memory, the server combines the pending deltas in the background, before the interval elapses. Compacting costs processor time and saves memory, so the server pays for it only where there is memory to reclaim. The server never compacts:

- A [blink table](../../conceptual/table-types.md#specialization-3-blink).
- A queue of fewer than two pending deltas.

Otherwise, it compares the storage the pending deltas occupy against the storage they would occupy compacted, and compacts when the saving clears both of two thresholds:

```text
held - compacted >= max(compactionFloorBytes, compactionMinFreedFraction * held)
```

In the formula:

- `held` is the storage the pending deltas occupy, which the server reports as `PendingDeltaBytes`.
- `compacted` is an estimate of the storage they would occupy after compacting.
- `compactionMinFreedFraction` tests whether compacting would release a large enough share of `held`.
- `compactionFloorBytes` tests whether the bytes released are enough to justify the copy.

Both `held` and `compacted` count the capacity of the allocated chunks, not the rows stored in them, so a queue of many small updates is measured by the memory it holds. The note under [Barrage subscription metrics summary](#barrage-subscription-metrics-summary) explains how that capacity is counted.

The following properties turn compaction on or off, set its thresholds, and set the chunk size that `held` and `compacted` are measured in:

- `-DBarrageMessageProducer.compactionEnabled`: When `true` (the default), the server compacts a table's pending deltas between update intervals. When `false`, deltas accumulate untouched until the interval elapses.
- `-DBarrageMessageProducer.compactionMinFreedFraction`: The fraction of the pending deltas' storage that compacting must release for the server to do it. Default: `0.5`. A higher value copies less data but lets the pending deltas grow larger. At the default `0.5`, the server copies at most about as many bytes as it records and holds about twice the compacted footprint. At `0.9`, it copies about a ninth as much data and holds about ten times the compacted footprint.
- `-DBarrageMessageProducer.compactionFloorBytes`: The number of bytes compacting must release, whatever fraction of the total that represents. Default: `4194304` (4 MiB). Without it, a stream of very small updates could compact on every cycle. Each compaction has the same fixed cost however little it reclaims, so the floor makes the server wait until the saving is worth that cost.
- `-DBarrageMessageProducer.deltaChunkSize`: The number of rows in each chunk a delta records, except the last chunk of each column. Default: `65536`, the largest pooled chunk capacity. That capacity is 2 to the power of `-DChunkPoolConstants.largestPooledChunkLog2Capacity`, whose default is `16`. A value that is not a power of two rounds up to the next one.

## Write to subscribers in parallel

The server can write one update to several subscribers at once. A write serializes the message into the gRPC stream's buffers. That is processor work that does not wait for the client.

Each propagation phase makes one write for each group of identical full-table subscribers it goes to (see [Share one write among identical subscribers](#share-one-write-among-identical-subscribers)) and one for each other subscriber it goes to, such as a [viewport](../../conceptual/what-is-barrage.md#viewports) subscriber. Each write holds only the columns, rows, and encoding its subscribers asked for. The writes run in parallel when:

- The phase has more than one write.
- `BarrageMessageProducer.propagationThreads` is greater than `1`.

The following property sets the thread count:

- `-DBarrageMessageProducer.propagationThreads`: The maximum number of threads that write one update to a table's subscribers at once. The count includes the propagation thread, which runs the background job that `UpdateJobNanos` times. Default: the number of available processors.

How the setting behaves:

- Above `1`: the extra threads come from a pool that every table shares. The pool holds at most this number minus one. When the pool is busy, the propagation thread writes to the remaining subscribers itself.
- `1` or less: the propagation thread writes to subscribers one after another, and the server creates no pool.

The same threads copy a [shared write](#share-one-write-among-identical-subscribers) to its subscribers. With `1` or less, the server copies a shared write to one subscriber after another.

> [!NOTE]
> An update can take several propagation phases, such as a snapshot for new subscribers and a delta for the others, and each records its own `PropagateNanos` sample. With parallel writes a phase can take much less than the sum of its writes' `WriteNanos`. A single subscriber's write still runs on one thread, while a shared write can copy to its subscribers on several.

## Share one write among identical subscribers

A subscriber to a whole table, rather than to a [viewport](../../conceptual/what-is-barrage.md#viewports) of it, is a full-table subscriber. Full-table subscribers that would receive identical bytes share one write of each update. The server writes the update once and copies the bytes to each of them, which saves the processor time of writing it again for every subscriber.

Full-table subscribers share a write when they:

- Have received the whole table, rather than still receiving it in parts (see [Control subscription snapshot size](#control-subscription-snapshot-size)).
- Use the same subscription options, such as the batch size, the maximum message size, and how nulls are sent.
- Subscribe to the same columns.
- Use the same viewport direction (forward or reverse).

[Viewport](../../conceptual/what-is-barrage.md#viewports) subscribers always get a write of their own.

The server copies a shared write to its subscribers one message at a time:

- Each message holds exactly one batch, either a record batch or a dictionary batch. A message never holds both.
- For a table with dictionary-encoded columns, each dictionary batch is its own message, sent ahead of the record batch that uses its values.
- The server copies each message to every subscriber before it writes the next, so sharing holds at most one extra batch in memory for each write in progress.
- The server copies a large message to its subscribers in parallel, on the threads used for [parallel writes](#write-to-subscribers-in-parallel). `BarrageMessageProducer.parallelCopyMinBytes`, below, sets what counts as large.
- The extra batch held in memory is bounded by the subscriber's maximum message size: no record batch or dictionary batch is larger than `BarrageMessageWriterImpl.maxOutboundMessageSize` unless the subscriber sets its own (see [Message batching](#message-batching)). The exception is a single row, or a single dictionary value, too large to fit, which the server sends anyway.

When a table has dictionary-encoded columns, subscribers that share a write may have been sent different amounts of each dictionary, for example when one of them subscribed more recently. Before the shared write, the server sends each lagging subscriber the dictionary values it lacks. It sends one message per dictionary, or several when the values would exceed the maximum message size. After that, subscribers that share a write stay in sync, so they need no more of these messages until another subscriber joins them.

The following properties turn shared writes on or off and set when the server copies a shared message in parallel:

- `-DBarrageMessageProducer.dedupIdenticalSubscribers`: When `true` (the default), identical full-table subscribers share one write of each update. When `false`, every subscriber gets a write of its own.
- `-DBarrageMessageProducer.parallelCopyMinBytes`: The copy volume, in bytes, at which the server copies a message to sharing subscribers in parallel. The copy volume is the message's size times one less than the number of sharing subscribers. Default: `4194304` (4 MiB). The server copies smaller messages to one subscriber after another.

> [!NOTE]
> A shared write records one `WriteNanos` sample, and one `WriteBytes` sample for each subscriber that was sent all of it. `WriteNanos` includes copying the write to every subscriber, whether or not the copies run in parallel. Each `WriteBytes` sample is the size of one copy, so the `WriteBytes` statistics match what separate writes to each subscriber would show. The `WriteNanos` count, by contrast, equals the number of writes, not the number of subscribers. The dictionary values sent before a shared write to subscribers that were behind are a write of their own, recorded the same way.

## Compress Barrage data

By default, the server sends Barrage snapshots and subscriptions uncompressed. To compress, give a table, or the whole server, a list of compression types, most preferred first. When a client fetches the table:

- The client lists the types it can decode in its `grpc-accept-encoding` request header.
- The server uses the first type on the table's list that the client also lists.
- With no match, or no list from the client, the server sends uncompressed data.

This negotiation applies to the default Jetty-based server. A server built on Netty can compress only with `gzip` and skips any other type on the table's list.

### Compress a table

To let the server compress a table's data, call the table's [`with_transport_compression`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_transport_compression) method with the names of the compression types, in order of preference, such as the built-in `"zstd"`, `"gzip"`, `"snappy"`, and `"deflate"`.

```python order=null
from deephaven import empty_table

t = empty_table(1_000_000).update(["X = ii", "Sym = `S` + (ii % 32)"])
t_compressed = t.with_transport_compression(["zstd", "gzip"])
```

[`with_transport_compression`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_transport_compression) stores the list in the table's `TransportCompression` attribute.

These operations keep the attribute, because their result holds only the table's values:

- [Filtering](../use-filters.md), [sorting](../../reference/table-operations/sort/sort.md), [reversing](../../reference/table-operations/sort/reverse.md), [flattening](../../reference/table-operations/select/flatten.md), and [dropping](../../reference/table-operations/select/drop-columns.md) or [renaming](../../reference/table-operations/select/rename-columns.md) columns.
- [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md) constituents.
- [`first_by`](../../reference/table-operations/group-and-aggregate/firstBy.md) and [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md).

These operations drop it:

- Operations that compute new values or bring in another table's data, such as [`view`](../../reference/table-operations/select/view.md), [`update_view`](../../reference/table-operations/select/update-view.md), [joins](../joins-exact-relational.md), [rollups](../rollup-table.md), and [trees](../tree-table.md).
- [`head`](../../reference/table-operations/filter/head.md), [`select`](../../reference/table-operations/select/select.md), and [`update`](../../reference/table-operations/select/update.md).

A result without the attribute is sent uncompressed unless the server has a [server-wide list](#set-a-server-wide-default). For a [rollup](../rollup-table.md) or [tree](../tree-table.md) table, call the [`RollupTable.with_transport_compression`](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable.with_transport_compression) or [`TreeTable.with_transport_compression`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable.with_transport_compression) method rather than calling it on its source.

### Set a server-wide default

To compress tables without changing the queries that create them, set a server-wide list of compression types, most preferred first, with the `TransportCompression.defaultTypes` property, for example `-DTransportCompression.defaultTypes=zstd,snappy`. The server uses the server-wide list for every table that has no `TransportCompression` attribute set. Without the property, only tables with their own `TransportCompression` attribute are compressed.

With a server-wide list, compression is opt-out rather than opt-in:

- A table with its own `TransportCompression` attribute uses its own list instead of the server-wide list.
- To exclude a table, call [`with_transport_compression([])`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_transport_compression). The empty list means "never compress".
- To make the server-wide list apply again, call [`reset_transport_compression`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.reset_transport_compression), or pass `None` to [`with_transport_compression`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_transport_compression). Either one removes the attribute.
- Operations keep or drop an empty list the same way they keep or drop any other list. A table made by an operation that drops the attribute, such as a [join](../joins-exact-relational.md) or [`view`](../../reference/table-operations/select/view.md), uses the server-wide list even when its source was excluded.

### Compression types each client accepts

These are the compression types each client lists by default. A client that lists none always receives uncompressed data.

| Client                                     | Compression types listed                                                  |
| ------------------------------------------ | ------------------------------------------------------------------------- |
| Java client                                | `gzip`, `zstd`, `snappy`, `deflate`, and any added types on its classpath |
| Python (`pydeephaven`), C++, and R clients | `gzip` and `deflate`                                                      |
| Web UI, JavaScript API, and Go client      | None, so data is always sent uncompressed                                 |

Remote tables between servers are a separate case. A Deephaven server that fetches a table from another server, with [`barrage_session`](../../reference/data-import-export/barrage/barrage-session.md) or by resolving a [`dh://` URI](../use-uris.md), connects through the Java client that runs inside the server, even though the query that fetches the table is written in Python. So it lists the same compression types as the Java client, not the ones `pydeephaven` lists.

### Choose compression types

Choose the list based on what the server's CPU can afford. The following figures come from [`TransportCompressionBenchmark`](https://github.com/deephaven/deephaven-core/blob/main/extensions/barrage/benchmark/src/main/java/io/deephaven/benchmark/barrage/TransportCompressionBenchmark.java), a Java Microbenchmark Harness (JMH) benchmark in the Deephaven Core repository, run on a one-million-row table of mixed trade data. The benchmark ran with JDK 21 on one thread of an Apple M1 Max and of an Intel Core i9-14900KS. The speeds are megabytes of uncompressed data per second. Absolute speeds depend on the hardware and the data, so use them to compare the compression types rather than to predict throughput:

| Compression type | Size after compression | Compression speed (M1 Max / i9-14900KS) | Decompression speed (M1 Max / i9-14900KS) |
| ---------------- | ---------------------- | --------------------------------------- | ----------------------------------------- |
| `zstd`           | 31% of the original    | 380 / 550 MB/s                          | 1010 / 1270 MB/s                          |
| `gzip`           | 28% of the original    | 18 / 30 MB/s                            | 400 / 600 MB/s                            |
| `snappy`         | 59% of the original    | 800 / 880 MB/s                          | 1460 / 1080 MB/s                          |
| `deflate`        | 28% of the original    | 18 / 30 MB/s                            | 360 / 550 MB/s                            |

How to use each type:

- `zstd` suits most tables.
- `gzip` compresses slightly smaller than `zstd` but is about 20 times slower to compress. List it last, and only when clients that cannot decode `zstd`, such as `pydeephaven`, should also receive compressed data.
- `deflate` is the `gzip` algorithm in the zlib format. It compresses to the same size at the same speed and decompresses slightly slower. Every client that decodes `deflate` also decodes `gzip`, so there is little reason to list `deflate` ahead of `gzip`.
- `snappy` compresses fastest but saves the least, and it saves nothing on data that is already close to random.

### Configure client-side compression

To narrow the compression types a client lists, use the setting for that client. An empty list asks for uncompressed data.

- A Java client: `ClientConfig.builder().acceptCompression(...)`. See [`ClientConfig`](/core/javadoc/io/deephaven/client/impl/ClientConfig.html).
- A remote table fetched with [`barrage_session`](../../reference/data-import-export/barrage/barrage-session.md): its `accept_compression` argument.
- A remote table fetched by resolving a `dh://` URI: the `-DBarrageTableResolver.acceptCompression` property on the fetching server, for example `-DBarrageTableResolver.acceptCompression=zstd`.

The Java client can also compress the data it uploads with `DoPut`, including the rows it adds to [input tables](../input-tables.md). Uploads are uncompressed by default. To compress them, set `requestCompression(...)` on the [`ClientConfig`](/core/javadoc/io/deephaven/client/impl/ClientConfig.html) builder, for example to `"zstd"`. A server that cannot decode the type fails the upload. When the server might not support the other types, choose `gzip`. Every Deephaven server decodes `gzip` unless its `TransportCompression.gzip.enabled` property is `false` (see [Add or turn off a compression type](#add-or-turn-off-a-compression-type)).

### Add or turn off a compression type

To add a compression type beyond `zstd`, `gzip`, `snappy`, and `deflate`, such as `lz4`:

1. Write a Java class that implements [`io.deephaven.grpc.compression.CompressionCodecProvider`](/core/javadoc/io/deephaven/grpc/compression/CompressionCodecProvider.html) and returns an `io.grpc.Codec` for the type, named by the codec's `getMessageEncoding` method.
2. Register the class as a Java `ServiceLoader` service, for example with `@AutoService(CompressionCodecProvider.class)`.
3. Put its jar on the server's classpath.

Tables can then list the type by its name, and the server uses it for clients that list it. A Java client lists the type by default when the same jar is on its classpath. Other clients list only the types in the [table of client defaults](#compression-types-each-client-accepts). A class whose codec has the name of a built-in type, such as `zstd`, replaces the built-in codec wherever the jar is on the classpath, for example to compress at a different level.

To turn off a compression type on a server, set the type's `TransportCompression.<type>.enabled` property to `false`, for example `-DTransportCompression.gzip.enabled=false`, or put the same setting in the server's [configuration file](../configuration/config-file.md). Every type is enabled unless its property disables it, and the property applies to added types too. When a type is disabled:

- The server never compresses data with it.
- A table that lists it is sent with the next type on its list.
- A client request compressed with it fails.

## Troubleshooting

Use the Barrage subscription and snapshot metrics tables from [Access Barrage metrics tables](#access-barrage-metrics-tables) to diagnose common Barrage performance issues. [Subscription errors](#subscription-errors), at the end of this section, covers errors the metrics tables don't record.

### High `SnapshotNanos`

If `SnapshotNanos` is consistently high:

- The source table may be very large. Consider using [viewports](../../conceptual/what-is-barrage.md#viewports) or [filtering](../use-filters.md) data before subscription.
- For [tree](../tree-table.md) and [rollup](../rollup-table.md) tables, the server sends every update as a snapshot, so `SnapshotNanos` reflects ordinary update cost.
- The update graph may be holding a lock. Check for long-running operations blocking the cycle.
- Make sure subscription growth has not been disabled, and consider smaller snapshot parts (see [Control subscription snapshot size](#control-subscription-snapshot-size)).

### High `WriteNanos` or `WriteBytes`

If `WriteNanos` is high or `WriteBytes` is large:

- `WriteNanos` measures encoding each message and handing it to gRPC, not network transmission. Large messages take longer to encode.
- Consider subscribing to fewer columns or using [viewports](../../conceptual/what-is-barrage.md#viewports) to reduce data volume.
- A longer update interval sends fewer, larger messages. It reduces total bytes when the same rows change repeatedly between updates, but each message, and each `WriteBytes` value, gets larger.

### High `PropagateNanos`

If `PropagateNanos` is consistently high:

- `PropagateNanos` includes writing each subscriber's message, so check `WriteNanos` first.
- Many subscribers may be connected to the same table. Their writes overlap only as far as `BarrageMessageProducer.propagationThreads` and idle processors allow. Check that the property is above `1` and not well below the number of separate writes: one for each group of identical full-table subscribers and one for every other subscriber. See [Write to subscribers in parallel](#write-to-subscribers-in-parallel).
- The server's processors may already be busy. Consider load balancing across multiple server instances.
- Subscribers whose options or [viewports](../../conceptual/what-is-barrage.md#viewports) differ each need a write of their own. Full-table subscribers with matching options share one (see [Share one write among identical subscribers](#share-one-write-among-identical-subscribers)).
- The server may be under memory pressure. Check JVM heap usage and garbage collection metrics.

### Memory issues

If the server experiences out-of-memory errors during subscriptions:

- Make sure subscription growth has not been disabled (`-DBarrageMessageProducer.subscriptionGrowthEnabled` defaults to `true`).
- Lower `maxSnapshotCellCount` or `targetSnapshotPercentage` to reduce peak memory usage.
- Check the maximum `PendingDeltaBytes` and `PendingDeltaCount` for each table. High values mean the server is holding many update graph cycles of changes between update intervals. Make sure compaction has not been disabled, consider a lower `compactionMinFreedFraction`, or shorten the update interval (see [Compact pending deltas](#compact-pending-deltas)).

### Subscription errors

Common subscription issues:

- **Ticket not found**: An error such as `Could not resolve '...': ticket '<id>' not found` means one of three things. The ticket ID is wrong, the published table was released, or the session that published it closed. Keep the publishing session open, and keep the published table from being released, for as long as others need the ticket.
- **Dependency released**: A table released while a request for it is still pending can fail with a `dependency released` error instead.
- **Authentication failures**: Verify that the `auth_type` and `auth_token` arguments to [`barrage_session`](../../reference/data-import-export/barrage/barrage-session.md) match the server configuration. URI resolution requires anonymous authentication. See [Use URIs to share tables](../use-uris.md).
- **Connection refused**: Ensure the server is running and the host/port are correct. Check firewall rules.

## Related documentation

- [Interpret Barrage metrics](../../conceptual/barrage-metrics.md)
- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md)
- [Capture Python client tables with Barrage](../capture-tables.md)
- [Use URIs to share tables](../use-uris.md)
- [Create a hierarchical tree table](../tree-table.md)
- [Create a hierarchical rollup table programmatically](../rollup-table.md)
- [Filter table data](../use-filters.md)
- [Exact and relational joins](../joins-exact-relational.md)
- [Create and use input tables](../input-tables.md)
- [Deephaven's table types](../../conceptual/table-types.md)
- [Create a Deephaven configuration file](../configuration/config-file.md)
- [How to use Deephaven's performance tables for system monitoring](./performance-tables.md)
- [`barrage_session`](../../reference/data-import-export/barrage/barrage-session.md)
- [`with_attributes`](../../reference/table-operations/create/withAttributes.md)
- [`drop_columns`](../../reference/table-operations/select/drop-columns.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`first_by`](../../reference/table-operations/group-and-aggregate/firstBy.md)
- [`flatten`](../../reference/table-operations/select/flatten.md)
- [`head`](../../reference/table-operations/filter/head.md)
- [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`rename_columns`](../../reference/table-operations/select/rename-columns.md)
- [`reverse`](../../reference/table-operations/sort/reverse.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`sort`](../../reference/table-operations/sort/sort.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`update_view`](../../reference/table-operations/select/update-view.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`deephaven.perfmon` Pydoc](/core/pydoc/code/deephaven.perfmon.html)
- [`with_transport_compression` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_transport_compression)
- [`reset_transport_compression` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.reset_transport_compression)
- [`RollupTable.with_transport_compression` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.RollupTable.with_transport_compression)
- [`TreeTable.with_transport_compression` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable.with_transport_compression)
- [`ClientConfig` Javadoc](/core/javadoc/io/deephaven/client/impl/ClientConfig.html)
- [`CompressionCodecProvider` Javadoc](/core/javadoc/io/deephaven/grpc/compression/CompressionCodecProvider.html)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
