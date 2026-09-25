---
title: Interpret Barrage metrics
---

This guide explains what each statistic Deephaven records about [Barrage](./what-is-barrage.md) activity measures, and where in the life of an update or snapshot it is recorded. Barrage is Deephaven's extension of Apache Arrow Flight for incrementally updating data sets. To query these statistics, see [Barrage metrics](../how-to-guides/performance/barrage-performance.md).

## Metrics overview

Barrage records statistics in two places:

- **The publisher** (the server that owns the table) records the time it takes to capture, coalesce, snapshot, propagate, and write updates.
- **The receiver** records the time it takes to deserialize and apply updates, but only when the receiver runs the Deephaven engine: the Java client, or a Deephaven server that subscribes to or snapshots a remote table through a [URI](../how-to-guides/use-uris.md) or `BarrageTableResolver`. Receiver statistics appear in the receiving process's own metrics tables, not the publisher's. Clients that do not run the engine, such as `pydeephaven` and the JavaScript client, record no receiver statistics.

### Life of a Barrage update

1. UpdateGraph source table updates

Barrage listens to UpdateGraph changes from the source table. Whenever the source table ticks while it has at least one subscriber, Barrage records `EnqueueNanos`, the time it took to record the changes that occurred during that update graph cycle.

![Diagram reading "Delta -> Delta -> Delta"](../assets/how-to/barrage-deltas.png)

Each recorded change is held as a pending delta until the next Barrage update. After adding a delta, Barrage records two gauges: `PendingDeltaCount`, the number of pending deltas it is holding, and `PendingDeltaBytes`, the approximate memory those deltas' chunks occupy.

2. `PeriodicUpdateGraph.targetCycleDurationMillis` versus `barrage.minUpdateInterval`

The Periodic Update Graph (UG) targets a cycle duration set by [`PeriodicUpdateGraph.targetCycleDurationMillis`](./periodic-update-graph-configuration.md#targetcycledurationmillis); this is a target, not a guarantee, and a cycle that runs long delays the next one. When the table ticks, Barrage waits until the update interval has elapsed since the last update it sent before sending again. The interval is shared by every subscription to that table that uses the same value, and defaults to [`barrage.minUpdateInterval`](../how-to-guides/performance/barrage-performance.md#update-interval) (ms) when a subscription does not request one. Many UG cycles can elapse between Barrage updates. Adding, changing, or removing a subscription, and each step of a growing initial snapshot, sends immediately, which also flushes pending deltas to existing subscribers. Barrage records `AggregateNanos`, the time it took to coalesce pending deltas into a message. This can happen more than once per interval: Barrage may [compact pending deltas](../how-to-guides/performance/barrage-performance.md#compact-pending-deltas) before the interval elapses, and a snapshot splits the pending deltas into ranges before and after the snapshot. Each non-empty range is packaged and timed separately, including a range of a single delta that needs no coalescing (except on a table's first subscription, where the deltas before the snapshot are dropped). Compaction and each packaged range are all recorded.

!["Coalesced Delta"](../assets/how-to/barrage-coalesced-delta.png)

3. Synchronizing Barrage state to the UG cycle

New subscriptions and subscription changes require initializing table state built from a series of snapshots. Barrage records `SnapshotNanos`, the time it took to take a single snapshot step. The generated snapshot occurs within the stream of updates. Due to raciness on table updates versus acquiring the snapshot, there might be coalesced deltas on either side (or both) of the snapshot.

!["Coalesced Delta -> Snapshot -> Coalesced Delta"](../assets/how-to/barrage-coalesced-snapshot.png)

4. Propagation to gRPC listeners

Each delta and snapshot is then propagated to subscribers. Barrage records `PropagateNanos`, the time it took to pass a delta to all subscribers, or a snapshot to one subscriber. Because each subscriber's message is written to its gRPC stream during propagation, `PropagateNanos` includes the write time described in the next step.

> [!NOTE]
> Barrage also records `UpdateJobNanos`, the aggregate time it took to coalesce deltas, fetch the snapshot, propagate, and housekeep. Compaction runs separately and is not included.

5. Writing to the gRPC stream

For each subscriber, Barrage writes the subscriber-specific filtered view of the update (including coalesced deltas and snapshots) to the subscriber's gRPC stream. Barrage records `WriteNanos`, the time it took to encode that view and hand it to gRPC, and `WriteBytes`, the size of the record batches written for that update to that subscriber (dictionary messages are not counted, except in a message that carries no rows). A single update may be split across several gRPC messages. `WriteBytes` is a size per write, not a rate; see [Barrage metrics](../how-to-guides/performance/barrage-performance.md) for converting it in a query.

6. Receiver deserializes gRPC messages into an update (coalesced or snapshot)

On the receiver, Barrage records `DeserializationNanos`, the time it took to read and parse one incoming gRPC message. A Barrage message split across several gRPC messages records one value per gRPC message.

!["Deserialized Delta -> Deserialized Delta -> Deserialized Delta"](../assets/how-to/barrage-deserialized-deltas.png)

7. Receiver applies updates to the local table

Barrage records `ProcessUpdateNanos`, the amount of time it took to apply a single deserialized message (delta or snapshot).

A subscribed Barrage table applies queued messages on each update graph cycle of the receiver, and an arriving message asks the receiver's update graph to start its next cycle right away. Several messages may have arrived since the last cycle. Barrage records `RefreshNanos`, the amount of time it took to apply all queued messages and enqueue change notifications for receiver-side listeners (the listeners' own processing is not included). A snapshot receiver applies each message as it arrives, outside any update graph cycle, and records no `RefreshNanos`.

### Life of a snapshot

In addition to subscribing to ticking data, Barrage supports fetching a full or partial synchronized snapshot of a table. Snapshots requested through a Barrage snapshot request or Arrow Flight's `DoGet` are recorded in the publisher's snapshot metrics table. Snapshots taken through a subscription that ends once the data arrives, such as `BarrageTableResolver.snapshot`, follow the life of an update above instead and are recorded in the subscription metrics table. For both a Barrage snapshot request and a subscription-based snapshot, a receiver that uses the Java Barrage client also records `DeserializationNanos` and `ProcessUpdateNanos` in its own subscription metrics table.

1. Request is received and queued

The snapshot request is queued for processing. Barrage records `QueueNanos`, the time from receiving the request until work on it began, including waiting for the requested table to be ready and for an available thread.

2. The snapshot is constructed

The snapshot request is then fulfilled. For a refreshing table, Barrage first tries to build the snapshot concurrently with the UG, without taking a lock. If those attempts cannot produce a consistent snapshot — for example, because the table keeps changing while a large snapshot is being read — Barrage makes a final attempt while holding the UG shared lock. Barrage records `SnapshotNanos`, the time it took to construct the snapshot for the listener, and `WriteNanos` and `WriteBytes`, the time it took to write, and how many bytes were written.

A static table is snapshotted in chunks, without locking. Each chunk that is written produces its own row, with that chunk's `WriteNanos` and `WriteBytes` and the `SnapshotNanos` accumulated so far, so a large static snapshot can produce several rows.

### Hierarchical tables

Subscriptions to tree and rollup tables also record statistics in the subscription metrics table, but only `SnapshotNanos`, `WriteNanos`, and `WriteBytes`. Every update to a tree or rollup subscription is sent as a snapshot, so each update records a `SnapshotNanos` value. Their `TableId` identifies the subscription rather than a table.

## Related documentation

- [What is Barrage?](./what-is-barrage.md)
- [Barrage metrics](../how-to-guides/performance/barrage-performance.md)
- [Periodic Update Graph configuration](./periodic-update-graph-configuration.md)
- [Incremental update model](./table-update-model.md)
- [Barrage protocol documentation](/barrage/docs)
