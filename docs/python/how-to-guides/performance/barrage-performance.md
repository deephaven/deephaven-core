---
title: Barrage metrics for performance monitoring
sidebar_label: Barrage metrics
---

Barrage is the name of Deephaven's IPC table transport. This guide explains what statistics are recorded and how to access them.

## Access Barrage metrics tables

You can access these tables as follows:

```python order=null
import jpy

bpl = jpy.get_type(
    "io.deephaven.extensions.barrage.BarragePerformanceLog"
).getInstance()

subs = bpl.getSubscriptionTable()
snaps = bpl.getSnapshotTable()
```

This is what the subscriptions table looks like when there are live subscriptions:

![The subscriptions table](../../assets/how-to/barragePerformance_subscriptions.png)

This is what the snapshots table looks like after processing a few requests:

![The snapshots table](../../assets/how-to/barragePerformance_snapshots.png)

### Barrage subscription metrics summary

Subscription statistics are presented in percentiles bucketed over a time period. Here are the various metrics that are recorded by the Deephaven server:

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
| RefreshMillis         | Receiver          | The time it took to apply all queued updates during a single update graph cycle     |

### Barrage snapshot metrics summary

Snapshot statistics are presented once per request.

| Stat Type      | Description                                                             |
| -------------- | ----------------------------------------------------------------------- |
| QueueMillis    | The time it took waiting for a thread to process the request            |
| SnapshotMillis | The time it took to construct a consistent snapshot of the source table |
| WriteMillis    | The time it took to write the snapshot                                  |
| WriteMegabits  | The payload size of the snapshot in megabits                            |

## Identify a table

Tables are identified by their `TableId` and `TableKey`. The `TableId` is determined by the source table's [`System.identityHashCode()`](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/System.html#identityHashCode(java.lang.Object)). The `TableKey` defaults to [`Table.getDescription()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getDescription()) but can be overridden by setting the table attribute via [`with_attributes`](https://deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_attributes).

```python order=t,:log
import jpy
from deephaven import empty_table

system = jpy.get_type("java.lang.System")

t = empty_table(0)

identity_hashcode = system.identityHashCode(t.j_table)

print(f"TableId: {identity_hashcode}")

table = jpy.get_type("io.deephaven.engine.table.Table")
attr_table = t.with_attributes({table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE: "MyTableKey"})
```

> [!NOTE]
> The web client applies transformations to every table to which it subscribes. If a table is also subscribed to by a non-web client, then statistics for the original table and the transformed table will both appear in the metrics table. Their `TableId` will differ. Most transformations clear the `TableKey` attribute, such as when a column is sorted or filtered, both programmatically and through the GUI.

## Extra configuration

Here are server-side flags that change the behavior of Barrage metrics.

- `-DBarragePerformanceLog.enableAll`: record metrics for tables that do not have an explicit `TableKey` (default: `true`).
- `-DBarragePerformanceLog.cycleDurationMillis`: the interval to flush aggregated statistics (default: `60000` - once per minute).

## Control subscription snapshot size

When a client subscribes to a ticking table, the server sends an initial snapshot of the table data. For large tables, constructing this snapshot requires holding the data in memory on the server side, which can lead to out-of-memory (OOM) errors.

To address this, Deephaven can break the initial snapshot into smaller chunks spread across multiple update cycles. This behavior is controlled by the following properties:

- `-DBarrageMessageProducer.subscriptionGrowthEnabled`: When `true` (the default), the server limits the size of each snapshot chunk. When `false`, the server sends the entire snapshot at once (unlimited size).

When subscription growth is enabled, these additional properties control the chunk size:

- `-DBarrageUtil.minSnapshotCellCount`: The minimum number of cells (rows × columns) per snapshot chunk. Default: `8192`.
- `-DBarrageUtil.maxSnapshotCellCount`: The maximum number of cells per snapshot chunk. Default: `16777216` (approximately 16 million).

The server adaptively adjusts the chunk size between these bounds based on how long each snapshot takes to generate, targeting a percentage of the update graph cycle time.

### Reducing publisher memory usage

For systems serving very large tables to many subscribers, you can reduce publisher-side memory usage by lowering `maxSnapshotCellCount`. Setting `minSnapshotCellCount` equal to `maxSnapshotCellCount` fixes the chunk size and disables adaptive sizing:

```bash
-DBarrageMessageProducer.subscriptionGrowthEnabled=true
-DBarrageUtil.minSnapshotCellCount=1000000
-DBarrageUtil.maxSnapshotCellCount=1000000
```

This configuration limits each snapshot chunk to exactly 1 million cells — well below the 16 million default maximum. Subscribers receive the full table data incrementally over multiple update cycles rather than all at once.

> [!NOTE]
> Setting smaller snapshot sizes increases the time required for subscribers to receive the initial table state but reduces peak memory usage on the server. These settings only affect initial snapshots — incremental updates are unaffected and must still be maintained in memory.

## Additional Barrage configuration

The following properties control other aspects of Barrage behavior:

### Update interval

- `-Dbarrage.minUpdateInterval`: The minimum interval (in milliseconds) between update batches sent to subscribers. Default: `1000` (1 second). Lower values reduce latency but increase CPU and network usage.

### Message batching

- `-DBarrageMessageWriterImpl.batchSize`: Maximum rows per Arrow record batch. Default: `Integer.MAX_VALUE`. Reduce this if clients have trouble processing very large batches.
- `-DBarrageMessageWriterImpl.initialBatchSize`: Initial batch size for the first message. Default: `4096`. A smaller initial batch ensures clients receive data quickly while the server calibrates optimal batch sizes.
- `-DBarrageMessageWriterImpl.maxOutboundMessageSize`: Maximum size (in bytes) for outbound messages. Default: `104857600` (100 MB). This matches the default incoming message limit for Java clients.

## Troubleshooting

Use the metrics tables described above to diagnose common Barrage issues.

### High SnapshotMillis

If `SnapshotMillis` is consistently high:

- The source table may be very large. Consider using viewports or filtering data before subscription.
- The update graph may be holding a lock. Check for long-running operations blocking the cycle.
- Consider enabling subscription growth with smaller chunk sizes (see [Control subscription snapshot size](#control-subscription-snapshot-size)).

### High WriteMillis or WriteMegabits

If `WriteMillis` is high or `WriteMegabits` is large:

- Network bandwidth may be saturated. Check network utilization.
- Consider subscribing to fewer columns or using viewports to reduce data volume.
- Increase `barrage.minUpdateInterval` to batch more updates together.

### High PropagateMillis

If `PropagateMillis` is consistently high:

- Many subscribers may be connected to the same table. Consider load balancing across multiple server instances.
- The server may be under memory pressure. Check JVM heap usage and garbage collection metrics.

### Subscription errors

Common subscription issues:

- **"Ticket not found"**: The table was released or the session that published it closed. Ensure the publishing session remains active.
- **Authentication failures**: Verify that the `auth_type` and `auth_token` match the server configuration.
- **Connection refused**: Ensure the server is running and the host/port are correct. Check firewall rules.

### Memory issues

If the server experiences out-of-memory errors during subscriptions:

- Enable subscription growth: `-DBarrageMessageProducer.subscriptionGrowthEnabled=true`
- Lower snapshot cell counts to reduce peak memory usage.
- Monitor the metrics tables to identify which tables consume the most resources.

## Related documentation

- [Interpret Barrage metrics](../../conceptual/barrage-metrics.md)
