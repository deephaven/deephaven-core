---
title: Interpret Barrage metrics
---

This guide explains what each statistic Deephaven records about [Barrage](./what-is-barrage.md) activity measures, and where in the life of an update or snapshot it is recorded. Barrage is Deephaven's extension of Apache Arrow Flight for incrementally updating data sets. To query these statistics, see [Barrage metrics](../how-to-guides/performance/barrage-performance.md).

## Metrics overview

Barrage records statistics in two places:

- **The publisher** (the server that owns the table) records the time it takes to capture, coalesce, snapshot, propagate, and write updates.
- **The receiver** records the time it takes to deserialize and apply updates, but only when the receiver runs the Deephaven engine: the Java client, or a Deephaven server that subscribes with [`barrage_session`](../reference/data-import-export/barrage/barrage-session.md) or a [URI](../how-to-guides/use-uris.md). Receiver statistics appear in the receiving process's own metrics tables, not the publisher's. Clients that do not run the engine, such as `pydeephaven` and the JavaScript client, record no receiver statistics.

### Life of a Barrage update

1. UpdateGraph source table updates

Barrage listens to UpdateGraph changes from the source table. Whenever the source table ticks, Barrage records `EnqueueNanos`, the time it took to record the changes that occurred during that update graph cycle.

![Diagram reading "Delta -> Delta -> Delta"](../assets/how-to/barrage-deltas.png)

Each recorded change is held as a pending delta until the next Barrage update. After adding a delta, Barrage records two gauges: `PendingDeltaCount`, the number of pending deltas it is holding, and `PendingDeltaBytes`, the approximate memory those deltas' chunks occupy.

2. `PeriodicUpdateGraph.targetCycleDurationMillis` versus `barrage.minUpdateInterval`

The Periodic Update Graph (UG) targets a cycle duration set by [`PeriodicUpdateGraph.targetCycleDurationMillis`](./periodic-update-graph-configuration.md#targetcycledurationmillis); this is a target, not a guarantee, and a cycle that runs long delays the next one. Barrage sends updates to subscribers no more often than each subscription's update interval, which defaults to [`barrage.minUpdateInterval`](../how-to-guides/performance/barrage-performance.md#update-interval) (ms) when the subscription does not request one. Many UG cycles can elapse between Barrage updates. Barrage records `AggregateNanos`, the time it took to coalesce pending deltas into a message. This can happen more than once per interval: Barrage may [compact pending deltas](../how-to-guides/performance/barrage-performance.md#compact-pending-deltas) before the interval elapses, and a snapshot splits the pending deltas into ranges before and after the snapshot, each of which is coalesced separately when it is not empty. Each of these aggregations is recorded.

!["Coalesced Delta"](../assets/how-to/barrage-coalesced-delta.png)

3. Synchronizing Barrage state to the UG cycle

New subscriptions and subscription changes require initializing table state built from a series of snapshots. Barrage records `SnapshotNanos`, the time it took to retrieve a single snapshot. The generated snapshot occurs within the stream of updates. Due to raciness on table updates versus acquiring the snapshot, there might be coalesced deltas on either side (or both) of the snapshot.

!["Coalesced Delta -> Snapshot -> Coalesced Delta"](../assets/how-to/barrage-coalesced-snapshot.png)

4. Propagation to gRPC listeners

Each delta and snapshot is then propagated to subscribers. Barrage records `PropagateNanos`, the time it took to pass the message to the list of subscribers. Because each subscriber's message is written to its gRPC stream during propagation, `PropagateNanos` includes the write time described in the next step.

> [!NOTE]
> Barrage also records `UpdateJobNanos`, the aggregate time it took to coalesce deltas, fetch the snapshot, propagate, and housekeep.

5. Writing to the gRPC stream

For each subscriber, Barrage writes the subscriber-specific filtered view of the update (including coalesced deltas and snapshots) to the subscriber's gRPC stream. Barrage records `WriteNanos`, the time it took to write that view, and `WriteBytes`, the number of bytes written for that update to that subscriber. A single update may be split across several gRPC messages. To compare against the bandwidth allowed by the connected hardware, convert to megabits in a query: `Megabits = WriteBytes * 8 / 1e6`.

6. Receiver deserializes gRPC messages into an update (coalesced or snapshot)

On the receiver, Barrage records `DeserializationNanos`, the time it took to read and parse the data from the InputStream to assemble an entire Barrage message.

!["Deserialized Delta -> Deserialized Delta -> Deserialized Delta"](../assets/how-to/barrage-deserialized-deltas.png)

7. Receiver applies updates to the local table

Barrage records `ProcessUpdateNanos`, the amount of time it took to apply a single deserialized delta.

The receiver's Barrage table refreshes once per update graph cycle on the receiver, which targets the receiver's `PeriodicUpdateGraph.targetCycleDurationMillis` interval. Potentially, many messages have arrived over the wire during this time. Barrage records `RefreshNanos`, the amount of time it took to apply all queued deltas and to propagate the result to any receiver-side table listeners.

### Life of a snapshot

In addition to subscribing to ticking data, Barrage supports fetching a full synchronized snapshot of a table, either through a Barrage snapshot request or through Arrow Flight's `DoGet`. These statistics are recorded in the publisher's snapshot metrics table.

1. Request is received and queued

The snapshot request is queued for processing. Barrage records `QueueNanos`, the time the message queued waiting for an available thread to satisfy the snapshot request.

2. The snapshot is constructed

The snapshot request is then fulfilled. Barrage first tries to build the snapshot concurrently with the UG, without taking a lock. If those attempts cannot produce a consistent snapshot — for example, because the table keeps changing while a large snapshot is being read — Barrage makes a final attempt while holding the UG shared lock. Barrage records `SnapshotNanos`, the time it took to construct the snapshot for the listener.

Similar to subscription requests, Barrage records `WriteNanos` and `WriteBytes`, the time it took to write, and how many bytes were written.

### Hierarchical tables

Subscriptions to tree and rollup tables also record statistics in the subscription metrics table, but only `SnapshotNanos`, `WriteNanos`, and `WriteBytes`. Their `TableId` identifies the subscription rather than a table.

## Related documentation

- [What is Barrage?](./what-is-barrage.md)
- [Barrage metrics](../how-to-guides/performance/barrage-performance.md)
- [Periodic Update Graph configuration](./periodic-update-graph-configuration.md)
- [Incremental update model](./table-update-model.md)
- [Barrage protocol documentation](/barrage/docs)
