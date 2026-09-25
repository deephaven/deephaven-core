---
title: What is Barrage?
---

Barrage is Deephaven's streaming data protocol for efficient, real-time table transport between servers and clients. Built on top of [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html), Barrage extends the standard Flight protocol to support Deephaven's [incremental update model](./table-update-model.md)—enabling clients to receive only the data that has changed rather than entire table snapshots.

## Why Barrage?

When working with real-time data, traditional approaches have significant limitations:

- **Polling**: Repeatedly fetching entire tables wastes bandwidth and CPU cycles, especially for large tables with small changes.
- **Standard Flight**: Apache Arrow Flight provides efficient data transport, but lacks native support for incrementally updating datasets.

Barrage solves these problems by:

- Streaming only the rows that have been added, removed, or modified.
- Supporting viewports so clients can subscribe to just the visible portion of a table.
- Batching updates to balance latency against network efficiency.

## When to use Barrage

| Use Case                                                                        | Barrage Feature                            |
| ------------------------------------------------------------------------------- | ------------------------------------------ |
| **Real-time dashboards**: Display live-updating tables in a UI                  | Subscribe to streaming updates             |
| **Server-to-server data sharing**: Move tables between Deephaven instances      | Subscribe or snapshot via shared tickets   |
| **Point-in-time analysis**: Capture table state for offline processing          | Snapshot to get a static copy              |
| **Large table visualization**: Display a scrollable view of a million-row table | Viewports (web UI, JS client, Java client) |

## Key concepts

### Subscriptions vs. snapshots

Barrage supports two primary modes of retrieving data:

- **Subscription**: Opens a persistent connection that streams updates as the source table changes. The client receives an initial snapshot followed by incremental updates. Use this for ticking tables you want to monitor in real time.

- **Snapshot**: Retrieves a one-time, static copy of the table. The request completes once the data is delivered. Use this for static tables or when you need a point-in-time capture.

From a Groovy server, [`ResolveTools.resolve`](../how-to-guides/use-uris.md) subscribes to a remote table by URI. `BarrageTableResolver` provides both subscriptions and snapshots (a `BarrageTableResolver` snapshot is taken through a subscription that ends once the data arrives):

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve
import io.deephaven.server.uri.BarrageTableResolver
import io.deephaven.uri.RemoteUri

uri = "dh+plain://remote-server:10000/scope/my_table"

// Subscription: receives ongoing updates
streamingTable = resolve(uri)

// Snapshot: one-time static copy
staticTable = BarrageTableResolver.get().snapshot(RemoteUri.of(URI.create(uri))).get()
```

### Shared tickets

Shared tickets are endpoints that allow tables to be published and consumed across different sessions. A client, such as the Python client (`pydeephaven`) or the Java client, can publish a table to a shared ticket, and other clients (or servers) can subscribe to or snapshot that ticket. From a Groovy server, pass the ticket's bytes to `BarrageTableResolver`:

```groovy skip-test
import io.deephaven.server.uri.BarrageTableResolver
import io.deephaven.qst.table.TableSpec

// ticketBytes: the bytes of a shared ticket published by a client
streamingTable = BarrageTableResolver.get().subscribe("dh+plain://remote-server:10000", TableSpec.ticket(ticketBytes)).get()
```

See [Capture remote tables with Barrage](../how-to-guides/capture-tables.md) for complete examples.

### Viewports

A viewport defines a window over a table — a set of row positions (typically a contiguous range) and a subset of columns. Viewports are essential for interactive applications where users scroll through large tables. Rather than streaming millions of rows, the server sends only the data visible in the current view.

Deephaven's web UI manages viewports automatically: when a user scrolls or resizes a table view, it updates its viewport subscription accordingly. JavaScript client code sets a viewport with `setViewport`. The Java client can also request a viewport, a subset of columns, or both when it subscribes.

> [!NOTE]
> `ResolveTools.resolve` subscribes to entire tables only. To limit the rows or columns you receive, either publish a filtered or narrowed table (for example, with [`where`](../reference/table-operations/filter/where.md) or [`view`](../reference/table-operations/select/view.md)) and resolve that instead, or use the `BarrageTableResolver.subscribe` and `snapshot` overloads that accept a viewport `RowSet` and a column `BitSet`.

### Update intervals and batching

Barrage aggregates table updates before sending them to subscribers. This batching reduces network overhead when tables update frequently. The update interval is configurable:

- **Server default**: Set via `-Dbarrage.minUpdateInterval` (milliseconds). Default: 1000 (1 second).
- **Per-subscription**: The Java and JavaScript clients can request a different interval when initiating a subscription. The interval is fixed for the life of the subscription; to use a different interval, create a new subscription. `ResolveTools.resolve` always uses the server default; the `BarrageTableResolver.subscribe` overloads that take a `BarrageSubscriptionOptions` can set one with `minUpdateIntervalMs`.

A shorter interval reduces latency but increases network traffic. A longer interval reduces traffic but introduces delay.

## Architecture overview

![Barrage architecture](../assets/conceptual/remote_and_local_server.png)

1. **Remote server** hosts a table referenced by a ticket — the ticket is just a reference, not the data itself. Tickets can be scope tickets (variables in the global scope), application tickets, export tickets, or shared tickets for cross-session access.
2. **Barrage protocol** transports the actual data using Arrow Flight with incremental update metadata.
3. **Local server** subscribes via a URI or `BarrageTableResolver` and receives a full local copy of the data that stays synchronized with the source. This local table can participate in downstream queries (joins, filters, aggregations) that execute on the local server.

> [!NOTE]
> Only receivers that run the Deephaven engine can perform downstream computation on a subscribed table locally: a Deephaven server (Groovy or Python) that subscribes with a [URI](../how-to-guides/use-uris.md) or a Barrage session, and the Java client. Other clients (`pydeephaven`, JavaScript, C++) receive data but rely on the server for query execution.

## Barrage vs. Arrow Flight

| Feature                  | Standard Arrow Flight | Barrage               |
| ------------------------ | --------------------- | --------------------- |
| Data format              | Arrow columnar format | Arrow columnar format |
| Static table transfer    | ✅ Supported          | ✅ Supported          |
| Incremental updates      | ❌ Not supported      | ✅ Native support     |
| Viewports                | ❌ Not supported      | ✅ Supported          |
| Update batching          | ❌ N/A                | ✅ Configurable       |
| Row shifts/modifications | ❌ N/A                | ✅ Efficient encoding |

Barrage is fully compatible with Arrow Flight — you can use a standard Flight client to fetch static snapshots via `DoGet`. The incremental update features require a Barrage-aware client.

## Performance considerations

- **Large initial snapshots**: When subscribing to a large table, the initial snapshot can be memory-intensive. By default, Barrage breaks large initial snapshots into smaller chunks; tune the chunk size with the [subscription growth controls](../how-to-guides/performance/barrage-performance.md#control-subscription-snapshot-size).

- **High-frequency updates**: Tables that tick rapidly can generate significant network traffic. Consider increasing [`barrage.minUpdateInterval`](../how-to-guides/performance/barrage-performance.md#update-interval) or filtering data before subscription.

- **Column selection**: Subscribe only to the columns you need. Fewer columns means less data to transfer. Publish a narrowed table (for example, with [`view`](../reference/table-operations/select/view.md)), or pass a column `BitSet` to `BarrageTableResolver.subscribe`.

- **Monitoring**: Use the [Barrage performance tables](../how-to-guides/performance/barrage-performance.md) to track subscription health and identify bottlenecks.

## Related documentation

- [Capture remote tables with Barrage](../how-to-guides/capture-tables.md) - Create, share, and capture remote tables from a Groovy server
- [Use URIs to share tables](../how-to-guides/use-uris.md) - Subscribe to remote tables by URI
- [Arrow Flight and Deephaven](../how-to-guides/data-import-export/arrow-flight.md) - Use standard Arrow Flight clients with Deephaven
- [Barrage metrics](../how-to-guides/performance/barrage-performance.md) - Monitor Barrage performance
- [Interpret Barrage metrics](./barrage-metrics.md) - Understand what the metrics mean
- [Barrage schema annotation](../how-to-guides/data-import-export/barrage-schema.md) - Annotate schemas for complex types
- [Incremental update model](./table-update-model.md) - How Deephaven represents table changes
- [Core API design](./deephaven-core-api.md) - Technical details on the Deephaven API
- [Barrage protocol documentation](/barrage/docs) - Low-level wire format reference
