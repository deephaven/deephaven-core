---
title: Interpret Barrage metrics
---

This guide explains what each [Barrage](./what-is-barrage.md) statistic measures and where in the life of an update or snapshot Deephaven records it. Barrage is Deephaven's extension of Apache Arrow Flight for incrementally updating data sets. To query these statistics, see [Monitor and tune Barrage performance](../how-to-guides/performance/barrage-performance.md).

When a ticking table falls behind its source on a client or a remote server, these statistics help you find which stage is slow: the server that owns the table preparing and writing the changes, or the receiver reading and applying them. Let's follow one update from the source table to the receiver, then one snapshot.

## Metrics overview

Barrage records statistics in two places:

- **The publisher** (the server that owns the table) records how long it takes to enqueue, coalesce, snapshot, propagate, and write updates, and how long snapshot requests wait before work on them begins. It also records the size of pending deltas (changes waiting to be sent, defined in [step 1](#1-the-source-table-ticks)) and of each write.
- **The receiver** records the time it takes to deserialize and apply updates.

Only receivers that run the Deephaven engine record statistics. These are the [Java Barrage client](../how-to-guides/java-client.md) and a Deephaven server that subscribes to or snapshots a remote table through a [`BarrageSession`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html), a [URI](../how-to-guides/use-uris.md), or [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html). Clients that do not run the engine, such as `pydeephaven` and the JavaScript client, record none. A receiver writes its statistics to its own [subscription metrics table](../how-to-guides/performance/barrage-performance.md#barrage-subscription-metrics-summary), not the publisher's.

Deephaven stores these statistics in two tables:

- The [subscription metrics table](../how-to-guides/performance/barrage-performance.md#barrage-subscription-metrics-summary) summarizes each subscription statistic over a reporting interval, one minute by default, as a sample count (`Count`), percentiles, and the maximum (`Max`) of the sampled values.
- The [snapshot metrics table](../how-to-guides/performance/barrage-performance.md#barrage-snapshot-metrics-summary) records a row for each snapshot request, or several rows for a static table sent in parts.

By default, Barrage records statistics for every table. To open the tables, see [Access Barrage metrics tables](../how-to-guides/performance/barrage-performance.md#access-barrage-metrics-tables); to limit collection to selected tables, see [Configure metrics collection](../how-to-guides/performance/barrage-performance.md#configure-metrics-collection).

The following table shows where each statistic is recorded:

| Statistic                                                | Recorded by | Table        | Step                                                                                                                               |
| -------------------------------------------------------- | ----------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------- |
| `EnqueueNanos`, `PendingDeltaCount`, `PendingDeltaBytes` | Publisher   | Subscription | [1. The source table ticks](#1-the-source-table-ticks)                                                                             |
| `AggregateNanos`                                         | Publisher   | Subscription | [2. Barrage waits for the update interval and coalesces deltas](#2-barrage-waits-for-the-update-interval-and-coalesces-deltas)     |
| `SnapshotNanos`                                          | Publisher   | Subscription | [3. Barrage snapshots the table for new or changed subscriptions](#3-barrage-snapshots-the-table-for-new-or-changed-subscriptions) |
| `PropagateNanos`                                         | Publisher   | Subscription | [4. Barrage propagates the update to subscribers](#4-barrage-propagates-the-update-to-subscribers)                                 |
| `WriteNanos`, `WriteBytes`                               | Publisher   | Subscription | [5. Barrage writes to each subscriber's gRPC stream](#5-barrage-writes-to-each-subscribers-grpc-stream)                            |
| `UpdateJobNanos`                                         | Publisher   | Subscription | [Steps 2 through 5](#life-of-a-barrage-update)                                                                                     |
| `DeserializationNanos`                                   | Receiver    | Subscription | [6. The receiver deserializes gRPC messages into an update](#6-the-receiver-deserializes-grpc-messages-into-an-update)             |
| `ProcessUpdateNanos`, `RefreshNanos`                     | Receiver    | Subscription | [7. The receiver applies updates to the local table](#7-the-receiver-applies-updates-to-the-local-table)                           |
| `QueueNanos`                                             | Publisher   | Snapshot     | [Life of a snapshot, 1. Barrage receives and queues the request](#1-barrage-receives-and-queues-the-request)                       |
| `SnapshotNanos`, `WriteNanos`, `WriteBytes`              | Publisher   | Snapshot     | [Life of a snapshot, 2. Barrage constructs the snapshot](#2-barrage-constructs-the-snapshot)                                       |
| `DeserializationNanos`, `ProcessUpdateNanos`             | Receiver    | Subscription | [Life of a snapshot, 3. The receiver deserializes and applies the snapshot](#3-the-receiver-deserializes-and-applies-the-snapshot) |
| `SnapshotNanos`, `WriteNanos`, `WriteBytes`              | Publisher   | Subscription | [Hierarchical tables](#hierarchical-tables)                                                                                        |

## Life of a Barrage update

Steps 1 through 5 happen on the publisher, and steps 6 and 7 happen on the receiver. Barrage also records `UpdateJobNanos`, the total time of one pass of the publisher's update job. A pass applies pending subscription changes, coalesces, snapshots, propagates, and writes as steps 2 through 5 describe, and then cleans up. The wait for the update interval happens between passes and isn't included. [Compaction](../how-to-guides/performance/barrage-performance.md#compact-pending-deltas) runs separately, and `UpdateJobNanos` doesn't include it either.

### 1. The source table ticks

Barrage listens to [UpdateGraph](./dag.md#update-graph-cycles) changes from the source table. A table [ticks](./table-update-model.md) when it changes during an update graph cycle. Whenever the source table ticks while it has at least one subscriber, Barrage records `EnqueueNanos`, the time it took to record that cycle's changes.

A _delta_ is the set of rows added, removed, modified, and shifted in one update graph cycle, together with the data Barrage needs to send for the added and modified rows.

![Diagram reading "Delta -> Delta -> Delta"](../assets/how-to/barrage-deltas.png)

Barrage holds each recorded change as a pending delta until the next Barrage update. After adding a delta, Barrage records two gauges. A gauge reports a current level rather than a duration, so its maximum over a reporting interval is the useful value.

- `PendingDeltaCount` is the number of pending deltas Barrage is holding. A [compacted](../how-to-guides/performance/barrage-performance.md#compact-pending-deltas) delta counts once, even though it covers several update graph cycles.
- `PendingDeltaBytes` is the approximate memory those deltas' chunks occupy.

### 2. Barrage waits for the update interval and coalesces deltas

When the table ticks, Barrage waits until the update interval has elapsed since the last update it sent before sending again. The update interval is separate from the update graph's cycle, so many update graph cycles can elapse between Barrage updates. Because the cycle's [target duration](./periodic-update-graph-configuration.md#targetcycledurationmillis) is not a guarantee, the number of cycles in one update interval varies. Three rules decide when Barrage sends:

- Every subscription to a table that requests the same interval shares it. Subscriptions that request different intervals are served separately. Each interval records its own statistics under the same `TableId`, so one table can have more than one row per statistic in a reporting interval.
- A subscription that requests no interval uses the publisher's [default update interval](../how-to-guides/performance/barrage-performance.md#update-interval).
- Barrage sends right away when a subscription is added, changed, or removed, and at each step of the initial snapshot that a new or changed subscription receives (see [step 3](#3-barrage-snapshots-the-table-for-new-or-changed-subscriptions)). Each of these sends also flushes pending deltas to existing subscribers.

Before it sends an update, Barrage _coalesces_ the pending deltas: it combines them into one delta that has the same effect as applying them in order. Barrage records `AggregateNanos`, the time it took to coalesce pending deltas into a message. Barrage records a value for each of the following, so there can be several values per update interval:

- Each [compaction of pending deltas](../how-to-guides/performance/barrage-performance.md#compact-pending-deltas) before the update interval elapses.
- Each non-empty range of pending deltas that Barrage packages for sending, including a range of a single delta that needs no coalescing.

![Diagram reading "Coalesced Delta"](../assets/how-to/barrage-coalesced-delta.png)

### 3. Barrage snapshots the table for new or changed subscriptions

To initialize a new or changed subscription, Barrage sends the table state in one or more snapshot steps. Each step covers some of the requested rows, and Barrage keeps sending steps until the subscriber has all of them. To control how large each step is, see [Control subscription snapshot size](../how-to-guides/performance/barrage-performance.md#control-subscription-snapshot-size).

Barrage records `SnapshotNanos`, the time it took to take a single snapshot step.

The snapshot appears within the stream of updates and reflects the table as of one update graph cycle, so it splits the pending deltas into two ranges:

- Deltas from cycles up to the one the snapshot reflects come before it.
- Deltas from later cycles, including cycles that finish while Barrage takes the snapshot, come after it.

Barrage coalesces each range separately and records an `AggregateNanos` value for each, as described in [step 2](#2-barrage-waits-for-the-update-interval-and-coalesces-deltas). When no other subscriber at that update interval is active, Barrage drops the deltas before the snapshot instead of sending them: no subscriber needs them, and the snapshot already contains their changes.

![Diagram reading "Coalesced Delta -> Snapshot -> Coalesced Delta"](../assets/how-to/barrage-coalesced-snapshot.png)

### 4. Barrage propagates the update to subscribers

Barrage then propagates each message to its subscribers: the coalesced deltas before the snapshot, the snapshot, and the coalesced deltas after it. Barrage records one `PropagateNanos` value per message, the time it took to pass that message to every subscriber that receives it. For a [blink table](./table-types.md#specialization-3-blink) that is not empty, Barrage also propagates a flush that clears the table before the snapshot, which records an extra `PropagateNanos` value. Because Barrage writes each subscriber's message to its gRPC stream during propagation, `PropagateNanos` includes the write time described in the next step. When a message goes to more than one subscriber, the publisher can write it to several of them at once (see [Write to subscribers in parallel](../how-to-guides/performance/barrage-performance.md#write-to-subscribers-in-parallel)).

### 5. Barrage writes to each subscriber's gRPC stream

Barrage writes each subscriber's view of the update, either coalesced deltas or a snapshot, to that subscriber's gRPC stream. For each subscriber, Barrage records two statistics:

- `WriteNanos`: the time it took to encode one write of the update and hand it to gRPC.
- `WriteBytes`: the number of bytes in that write, counting every message in it, including the dictionary batches that carry the distinct values of dictionary-encoded columns (see [Message batching](../how-to-guides/performance/barrage-performance.md#message-batching)). A single update can span several gRPC messages.

A whole-table subscriber receives every row of the table rather than a viewport of selected rows, though it can still choose its columns. By default, whole-table subscribers share one write of each update when:

- They would receive identical bytes, which means they use the same subscription options and columns.
- Each has already received the whole table.

To turn sharing off, see [Share one write among identical subscribers](../how-to-guides/performance/barrage-performance.md#share-one-write-among-identical-subscribers). For a shared write:

- Barrage records `WriteNanos` once, including the time to copy the bytes to every subscriber that shares the write.
- Barrage records `WriteBytes` once for each subscriber that received the whole write. Each value is the size of one copy, the same value Barrage would record if that subscriber had a write of its own.

When subscribers that share a write are missing different dictionary values, the subscribers that are behind first receive the values they lack in a catch-up write:

- Each catch-up covers one dictionary and every subscriber that lacks the same values, so one update can need several catch-ups.
- Barrage records a catch-up like a shared write: one `WriteNanos` sample for the catch-up and one `WriteBytes` sample for each subscriber that receives it.

As a result, the `Count` for `WriteNanos` is the number of writes, and the `Count` for `WriteBytes` is the number of times Barrage sent a write to a subscriber. Sharing lowers the first count but not the second.

`WriteBytes` is a size, not a rate. To estimate bandwidth from it, see [Units and conversions](../how-to-guides/performance/barrage-performance.md#units-and-conversions).

### 6. The receiver deserializes gRPC messages into an update

On the receiver, Barrage records `DeserializationNanos`, the time it took to read and parse one incoming gRPC message. When a Barrage message spans several gRPC messages, Barrage records one value per gRPC message.

![Diagram reading "Deserialized Delta -> Deserialized Delta -> Deserialized Delta"](../assets/how-to/barrage-deserialized-deltas.png)

### 7. The receiver applies updates to the local table

Barrage records `ProcessUpdateNanos`, the time it took to apply a single deserialized message (delta or snapshot).

A subscribed Barrage table applies its queued messages once per update graph cycle of the receiver. An arriving message asks the receiver's update graph to start its next cycle right away, and more messages can arrive before that cycle runs, so one cycle can apply several messages.

Barrage records `RefreshNanos` once per receiver cycle, even when no message is queued:

- It covers applying all queued messages and enqueuing change notifications for the receiver's table listeners.
- It does not include the listeners' own processing.
- Its `Count` is the number of cycles, not the number of messages.

## Life of a snapshot

In addition to subscribing to ticking data, Barrage supports fetching a full or partial synchronized snapshot of a table. The publisher records a snapshot in its [snapshot metrics table](../how-to-guides/performance/barrage-performance.md#barrage-snapshot-metrics-summary) when a client requests it with a Barrage snapshot request or with Arrow Flight's `DoGet`. The [`snapshot`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html#snapshot(io.deephaven.qst.table.TableSpec,io.deephaven.extensions.barrage.BarrageSnapshotOptions)) method of a [`BarrageSession`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html) sends a Barrage snapshot request.

A snapshot taken through a subscription that ends once the data arrives, such as the [`BarrageTableResolver.snapshot`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) method, follows the life of an update instead, and Barrage records it in the [subscription metrics table](../how-to-guides/performance/barrage-performance.md#barrage-subscription-metrics-summary).

Steps 1 and 2 happen on the publisher, and step 3 happens on the receiver.

### 1. Barrage receives and queues the request

Barrage queues the snapshot request for processing. Barrage records `QueueNanos`, the time from receiving the request until work on it began, including waiting for the requested table to be ready and for an available thread.

### 2. Barrage constructs the snapshot

Barrage then fulfills the snapshot request. For a refreshing table, Barrage normally builds the snapshot concurrently with the update graph, retrying if the table changes during an attempt. If those concurrent attempts keep failing, or a failed attempt takes too long, Barrage holds the update graph's [shared lock](./query-engine/engine-locking.md#shared-lock) for a final attempt. Barrage records three statistics for the snapshot:

- `SnapshotNanos`: the time it took to construct the snapshot.
- `WriteNanos`: the time it took to write the snapshot.
- `WriteBytes`: the size of every record batch and dictionary batch written for the snapshot. It does not count a schema message sent ahead of the data.

Barrage snapshots a static table in parts, without locking. Each part Barrage writes produces its own row in the snapshot metrics table. That row holds the part's `WriteNanos` and `WriteBytes` and the `SnapshotNanos` accumulated so far, so a large static snapshot can produce several rows. Every row repeats the request's `QueueNanos`.

### 3. The receiver deserializes and applies the snapshot

For a Barrage snapshot request, a receiver that runs the Deephaven engine (see [Metrics overview](#metrics-overview)) records `DeserializationNanos` and `ProcessUpdateNanos` in its own [subscription metrics table](../how-to-guides/performance/barrage-performance.md#barrage-subscription-metrics-summary). It applies each message as it arrives, outside any update graph cycle, so it records no `RefreshNanos`. A client that uses Arrow Flight's `DoGet` records no receiver statistics.

## Hierarchical tables

Subscriptions to [tree tables](../how-to-guides/tree-table.md) and [rollup tables](../how-to-guides/rollup-table.md) also record statistics in the [subscription metrics table](../how-to-guides/performance/barrage-performance.md#barrage-subscription-metrics-summary), but only `SnapshotNanos`, `WriteNanos`, and `WriteBytes`. Barrage sends every update to these subscriptions as a snapshot, so each update records a `SnapshotNanos` value. For these subscriptions, the `TableId` column identifies the subscription rather than a table (see [Identify a table](../how-to-guides/performance/barrage-performance.md#identify-a-table)).

## Read the statistics together

Each statistic measures one stage, but several of them overlap or count different things. Keep these relationships in mind when you compare rows:

- `UpdateJobNanos` covers a whole pass of the publisher's update job, so it includes the `AggregateNanos`, `SnapshotNanos`, and `PropagateNanos` values recorded during that pass. It does not include the `AggregateNanos` values from [compaction](../how-to-guides/performance/barrage-performance.md#compact-pending-deltas).
- `PropagateNanos` spans the writes made while Barrage propagates that message, so it includes their `WriteNanos`. When the publisher writes to several subscribers at once, those writes overlap, and their `WriteNanos` values can add up to more than the `PropagateNanos`.
- The `Count` for `WriteNanos` is the number of writes, and the `Count` for `WriteBytes` is the number of times Barrage sent a write to a subscriber. With shared writes, the second is larger.
- On the receiver, `RefreshNanos` includes the `ProcessUpdateNanos` of every message applied in that cycle. Its `Count` is the number of cycles, while the `Count` for `ProcessUpdateNanos` is the number of messages.
- `PendingDeltaCount` and `PendingDeltaBytes` are gauges, so read their `Max` rather than their percentiles.

## Related documentation

- [What is Barrage?](./what-is-barrage.md)
- [Periodic Update Graph](./periodic-update-graph-configuration.md)
- [Incremental update model](./table-update-model.md)
- [Deephaven's table types](./table-types.md)
- [Deephaven's live directed acyclic graph (DAG)](./dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./query-engine/engine-locking.md)
- [Monitor and tune Barrage performance](../how-to-guides/performance/barrage-performance.md)
- [Extract data with a Java client](../how-to-guides/java-client.md)
- [Capture remote tables with Barrage](../how-to-guides/capture-tables.md)
- [Use URIs to share tables](../how-to-guides/use-uris.md)
- [Create a hierarchical tree table](../how-to-guides/tree-table.md)
- [Create a hierarchical rollup table programmatically](../how-to-guides/rollup-table.md)
- [Barrage protocol documentation](/barrage/docs)
- [`BarrageSession` Javadoc](/core/javadoc/io/deephaven/client/impl/BarrageSession.html)
- [`BarrageTableResolver` Javadoc](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html)
