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

```python skip-test
from deephaven.barrage import barrage_session

session = barrage_session(host="remote-server", port=10000)

# Subscription: receives ongoing updates
streaming_table = session.subscribe(ticket_bytes)

# Snapshot: one-time static copy
static_table = session.snapshot(ticket_bytes)
```

### Shared tickets

Shared tickets are endpoints that allow tables to be published and consumed across different sessions. A client can publish a table to a shared ticket, and other clients (or servers) can subscribe to or snapshot that ticket.

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket

# Publish a table to a shared ticket
client = Session(host="remote-server", port=10000)
ticket = SharedTicket.random_ticket()
client.publish_table(ticket, my_table_ref)
```

See [Capture Python client tables](../how-to-guides/capture-tables.md) for complete examples.

### Viewports

A viewport defines a window over a table — a set of row positions (typically a contiguous range) and a subset of columns. Viewports are essential for interactive applications where users scroll through large tables. Rather than streaming millions of rows, the server sends only the data visible in the current view.

Deephaven's web UI manages viewports automatically: when a user scrolls or resizes a table view, it updates its viewport subscription accordingly. JavaScript client code sets a viewport with `setViewport`. The Java client can also request a viewport, a subset of columns, or both when it subscribes.

> [!NOTE]
> The Python `BarrageSession` API subscribes to and snapshots entire tables only. To limit the rows or columns you receive from Python, publish a filtered or narrowed table (for example, with [`where`](../reference/table-operations/filter/where.md) or [`view`](../reference/table-operations/select/view.md)) and subscribe to that instead. Viewports and column subsets are available through the JavaScript and Java clients.

### Update intervals and batching

Barrage aggregates table updates before sending them to subscribers. This batching reduces network overhead when tables update frequently. The update interval is configurable:

- **Server default**: Set via `-Dbarrage.minUpdateInterval` (milliseconds). Default: 1000 (1 second).
- **Per-subscription**: The Java and JavaScript clients can request a different interval when initiating a subscription. The interval is fixed for the life of the subscription; to use a different interval, create a new subscription. The Python `BarrageSession` API always uses the server default.

A shorter interval reduces latency but increases network traffic. A longer interval reduces traffic but introduces delay.

## Architecture overview

![Barrage architecture](../assets/conceptual/remote_and_local_server.png)

1. **Remote server** hosts a table referenced by a ticket — the ticket is just a reference, not the data itself. Tickets can be scope tickets (variables in the global scope), application tickets, export tickets, or shared tickets for cross-session access.
2. **Barrage protocol** transports the actual data using Arrow Flight with incremental update metadata.
3. **Local server** subscribes via `BarrageSession` and receives a full local copy of the data that stays synchronized with the source. This local table can participate in downstream queries (joins, filters, aggregations) that execute on the local server.

> [!NOTE]
> Only receivers that run the Deephaven engine can perform downstream computation on a subscribed table locally: a Deephaven server (Python or Groovy) that subscribes with `BarrageSession` or a [URI](../how-to-guides/use-uris.md), and the Java client. Other clients (`pydeephaven`, JavaScript, C++) receive data but rely on the server for query execution.

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

- **Column selection**: Subscribe only to the columns you need. Fewer columns means less data to transfer. From Python, publish a narrowed table (for example, with [`view`](../reference/table-operations/select/view.md)) and subscribe to that.

- **Monitoring**: Use the [Barrage performance tables](../how-to-guides/performance/barrage-performance.md) to track subscription health and identify bottlenecks.

## Related documentation

- [`barrage_session` reference](../reference/data-import-export/barrage/barrage-session.md) - API reference for creating Barrage sessions
- [`subscribe`](../reference/data-import-export/barrage/subscribe.md) and [`snapshot`](../reference/data-import-export/barrage/snapshot-barrage.md) - API reference for retrieving remote tables
- [Capture Python client tables](../how-to-guides/capture-tables.md) - Complete tutorial for using Barrage with the Python client
- [Use URIs to share tables](../how-to-guides/use-uris.md) - Subscribe to remote tables by URI
- [Arrow Flight and Deephaven](../how-to-guides/data-import-export/arrow-flight.md) - Use standard Arrow Flight clients with Deephaven
- [Barrage metrics](../how-to-guides/performance/barrage-performance.md) - Monitor Barrage performance
- [Interpret Barrage metrics](./barrage-metrics.md) - Understand what the metrics mean
- [Barrage schema annotation](../how-to-guides/data-import-export/barrage-schema.md) - Annotate schemas for complex types
- [Incremental update model](./table-update-model.md) - How Deephaven represents table changes
- [Core API design](./deephaven-core-api.md) - Technical details on the Deephaven API
- [Barrage protocol documentation](/barrage/docs) - Low-level wire format reference
