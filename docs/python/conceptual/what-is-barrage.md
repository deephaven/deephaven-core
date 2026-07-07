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
- Maintaining zero-copy data transport where possible.

## When to use Barrage

| Use Case                                                                        | Barrage Feature                          |
| ------------------------------------------------------------------------------- | ---------------------------------------- |
| **Real-time dashboards**: Display live-updating tables in a UI                  | Subscribe to streaming updates           |
| **Server-to-server data sharing**: Move tables between Deephaven instances      | Subscribe or snapshot via shared tickets |
| **Point-in-time analysis**: Capture table state for offline processing          | Snapshot to get a static copy            |
| **Large table visualization**: Display a scrollable view of a million-row table | Viewports (via web UI or JS client)      |

## Key concepts

### Subscriptions vs. snapshots

Barrage supports two primary modes of retrieving data:

- **Subscription**: Opens a persistent connection that streams updates as the source table changes. The client receives an initial snapshot followed by incremental updates. Use this for ticking tables you want to monitor in real time.

- **Snapshot**: Retrieves a one-time, static copy of the table. The connection closes after the data is delivered. Use this for static tables or when you need a point-in-time capture.

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

A viewport defines a window over a table—a range of row positions and a subset of columns. Viewports are essential for interactive applications where users scroll through large tables. Rather than streaming millions of rows, the server sends only the data visible in the current view.

Viewports are automatically managed by Deephaven's web UI and JavaScript client. When a user scrolls or resizes a table view, the client updates its viewport subscription accordingly.

> [!NOTE]
> The Python `BarrageSession` API currently supports full-table subscriptions. Viewport functionality is available through the JavaScript client or by subscribing to pre-filtered tables.

### Update intervals and batching

Barrage aggregates table updates before sending them to subscribers. This batching reduces network overhead when tables update frequently. The update interval is configurable:

- **Server default**: Set via `-Dbarrage.minUpdateInterval` (milliseconds). Default: 1000 (1 second).
- **Per-subscription**: Configurable when initiating the subscription (advanced use).

A shorter interval reduces latency but increases network traffic. A longer interval reduces traffic but introduces delay.

## Architecture overview

```text
┌─────────────────────────────────────────────────────────────────┐
│                         Remote Server                           │
│                                                                 │
│   ┌───────────┐     publishes to      ┌──────────────────┐     │
│   │   Table   │ ─────────────────────►│  Shared Ticket   │     │
│   └───────────┘                       └──────────────────┘     │
│                                               │                 │
└───────────────────────────────────────────────┼─────────────────┘
                                                │
                                    Barrage (Arrow Flight + metadata)
                                                │
                                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Local Server                            │
│                                                                 │
│   ┌─────────────────┐         ┌───────────────────────────────┐ │
│   │ BarrageSession  │ ──────► │  Local Table (real DH table)  │ │
│   └─────────────────┘         └───────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

1. **Remote server** hosts a table and publishes it to a shared ticket
2. **Barrage protocol** transports the data using Arrow Flight with incremental update metadata
3. **Local server** subscribes via `BarrageSession` and receives a true Deephaven table that can participate in queries

## Barrage vs. Arrow Flight

| Feature                  | Standard Arrow Flight | Barrage               |
| ------------------------ | --------------------- | --------------------- |
| Data format              | Arrow columnar format | Arrow columnar format |
| Static table transfer    | ✅ Supported          | ✅ Supported          |
| Incremental updates      | ❌ Not supported      | ✅ Native support     |
| Viewports                | ❌ Not supported      | ✅ Supported          |
| Update batching          | ❌ N/A                | ✅ Configurable       |
| Row shifts/modifications | ❌ N/A                | ✅ Efficient encoding |

Barrage is fully compatible with Arrow Flight—you can use a standard Flight client to fetch static snapshots via `DoGet`. The incremental update features require a Barrage-aware client.

## Performance considerations

- **Large initial snapshots**: When subscribing to a large table, the initial snapshot can be memory-intensive. Use [subscription growth controls](../how-to-guides/performance/barrage-performance.md#control-subscription-snapshot-size) to break large snapshots into smaller chunks.

- **High-frequency updates**: Tables that tick rapidly can generate significant network traffic. Consider increasing `barrage.minUpdateInterval` or filtering data before subscription.

- **Column selection**: Subscribe only to the columns you need. Fewer columns means less data to transfer.

- **Monitoring**: Use the [Barrage performance tables](../how-to-guides/performance/barrage-performance.md) to track subscription health and identify bottlenecks.

## Related documentation

- [`barrage_session` reference](../reference/data-import-export/barrage/barrage-session.md) - API reference for creating Barrage sessions
- [Capture Python client tables](../how-to-guides/capture-tables.md) - Complete tutorial for using Barrage with the Python client
- [Barrage metrics](../how-to-guides/performance/barrage-performance.md) - Monitor Barrage performance
- [Interpret Barrage metrics](./barrage-metrics.md) - Understand what the metrics mean
- [Barrage schema annotation](../how-to-guides/data-import-export/barrage-schema.md) - Annotate schemas for complex types
- [Incremental update model](./table-update-model.md) - How Deephaven represents table changes
- [Core API design](./deephaven-core-api.md) - Technical details on the Deephaven API
- [Barrage protocol documentation](/barrage/docs) - Low-level wire format reference
