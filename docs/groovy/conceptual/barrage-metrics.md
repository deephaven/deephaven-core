---
title: Interpret Barrage metrics
---

This guide covers what statistics Deephaven records on Barrage activity. Barrage is an extension of Apache Arrow Flight, with a particular focus on incrementally updating data sets.

## Metrics overview

### Life of a Barrage update

1. UpdateGraph Source Table Updates

Barrage listens to UpdateGraph changes from the source table. Whenever the source table ticks, Barrage records `enqueueMillis`, the time it took to record relevant information.

![Diagram reading "Delta -> Delta -> Delta"](../assets/how-to/barrage-deltas.png)

2. `PeriodicUpdateGraph.targetCycleDurationMillis` vs `barrage.minUpdateInterval`

The Periodic Update Graph (UG) ticks at at an interval specified by the parameter `-DPeriodicUpdateGraph.targetCycleDurationMillis`. Barrage ticks at an interval specified by the parameter `-Dbarrage.minUpdateInterval` (ms). Barrage records `AggregateMillis`, the time it took to coalesce all upstream updates into a single update.

!["Coalesced Delta"](../assets/how-to/barrage-coalesced-delta.png)

3. Synchronizing Barrage state to the UG cycle

New subscriptions and subscription changes require initializing table state built from a series of snapshots. Barrage records `snapshotMillis`, the time it took to retrieve a single snapshot. The generated snapshot occurs within the stream of updates. Due to raciness on table updates versus acquiring the snapshot, there might be coalesced deltas on either side (or both) of the snapshot.

!["Coalesced Delta -> Snapshot -> Coalesced Delta"](../assets/how-to/barrage-coalesced-snapshot.png)

4. Propagaion to gRPC listeners

Each delta and snapshot is then propagated to subscribers. Barrage records `PropagateMillis`, the time it took to pass the message to the list of listeners.

> [!NOTE]
> Barrage also records `UpdateJobMillis`, the aggregate time it took to coalesce deltas, fetch the snapshot, propagate, and housekeep.

5. Writing to the OutputStream

Another thread writes the resulting bytes to the actual gRPC stream. Barrage records `WriteMillis`, the time it took to drain the subscriber-specific filtered view of the update (including coalesced and snapshots) to the OutputStream. Barrage records `WriteMegabits`, the number of megabits (Mb) that were written on a single message. The choice to record this in Mb is for easy comparison against the bandwidth allowed by the connected hardware.

6. Receiver bundles gRPC messages into an update (coalesced or snapshot)

Barrage records `DeserializationMillis`, the time it took to read and parse the data from the InputStream to assemble an entire Barrage message.

!["Deserialized Delta -> Deserialized Delta -> Deserialized Delta"](../assets/how-to/barrage-deserialized-deltas.png)

7. Receiver bundles gRPC messages into an update (coalesced or snapshot)

Barrage records `ProcessUpdateMillis`, the amount of time it took to apply a single deserialized delta.

The Barrage table refreshes once per the `-DPeriodicUpdateGraph.targetCycleDurationMillis` interval. Potentially, many messages have arrived over the wire during this time. Barrage records `RefreshMillis`, the amount of time it took to coalesce deltas and to propagate the result to any receiver-side table listeners.

### Life of a snapshot

In addition to subscribing to ticking data, we support fetching a full synchronized snapshot of a table. This enables functionality such as Arrow FlightService's `DoGet`.

1. Request is received and queued

The snapshot request is queued for processing. Barrage records `QueueMillis`, the time the message queued waiting for an available thread to satisfy the snapshot request.

2. The snapshot is constructed

The snapshot request is then fulfilled. The process typically occurs concurrently with the UG, but larger snapshots may require holding the UG exclusive lock. Barrage records `SnapshotMillis`, the time it took to construct the snapshot for the listener.

Similar to subscription requests, Barrage records `WriteMillis` and `WriteMegabits`, the time it took to write, and how many Mb were written.

## Related documentation

- [How to use Barrage metrics for performance monitoring](../how-to-guides/performance/barrage-performance.md)
