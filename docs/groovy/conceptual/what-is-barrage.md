---
title: What is Barrage?
---

Barrage is Deephaven's streaming data protocol for efficient, real-time table transport between servers and clients. Barrage extends [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) to support Deephaven's [incremental update model](./table-update-model.md), so subscribers receive only the data that has changed rather than the entire table on every update.

## Why Barrage?

Two common ways to move real-time data between a server and a client have limitations:

- **Polling**: Repeatedly fetching entire tables wastes bandwidth and CPU cycles, especially for large tables with small changes.
- **Arrow Flight alone**: [Apache Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md) provides efficient data transport, but lacks native support for incrementally updating datasets. See [Barrage vs. Arrow Flight](#barrage-vs-arrow-flight) for a feature-by-feature comparison.

Barrage solves these problems by:

- Streaming only the rows that have been added, removed, or modified.
- Supporting viewports so clients can subscribe to just the visible portion of a table.
- Batching updates to balance latency against network efficiency.

## When to use Barrage

The following table matches common use cases to the Barrage feature that serves each one. [Key concepts](#key-concepts) explains each feature.

| Use case                                                                        | Barrage feature                                                                                                                                                                                                                                    |
| ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Real-time dashboards**: Display live-updating tables in a UI                  | [Subscribe](#subscriptions-vs-snapshots) to streaming updates                                                                                                                                                                                      |
| **Server-to-server data sharing**: Move tables between Deephaven instances      | [Subscribe or snapshot](#subscriptions-vs-snapshots) via [shared tickets](#shared-tickets)                                                                                                                                                         |
| **Point-in-time analysis**: Work with a fixed copy of a table's current state   | [Snapshot](#subscriptions-vs-snapshots) to get a static copy                                                                                                                                                                                       |
| **Large table visualization**: Display a scrollable view of a million-row table | [Viewports](#viewports) (via the web UI, the [JavaScript client](../how-to-guides/use-jsapi.md), the [Java client](../how-to-guides/java-client.md), or [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html)) |

## Architecture overview

In the most common setup, one Deephaven server (the local server) captures a table that lives on another (the remote server):

![A table on the remote server is referenced by a ticket. Barrage carries its data to a local table on the local server, which local queries use](../assets/conceptual/remote_and_local_server.png)

1. **Remote server** hosts a table that a [ticket](#shared-tickets) refers to.
2. **Barrage protocol** transports the actual data using Arrow Flight with incremental update metadata.
3. **Local server** [subscribes to or snapshots](#subscriptions-vs-snapshots) the table and receives a local copy of the data: the whole table, or only a [viewport](#viewports) of it. It connects through [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html), which opens a Barrage session to the remote server. A subscription's copy stays synchronized with the source.

The local copy is an ordinary Deephaven table. Queries on the local server can [join](../how-to-guides/joins-exact-relational.md), [filter](../how-to-guides/use-filters.md), or [aggregate](./aggregations.md) it.

> [!NOTE]
> Clients that run the full Deephaven engine can perform downstream computation locally. These include the [Java client](../how-to-guides/java-client.md) and any Deephaven server, Python or Groovy, that captures tables over Barrage. Other clients, such as the Python client [`pydeephaven`](/core/client-api/python/), the [JavaScript client](../how-to-guides/use-jsapi.md), and the [C++ client](/core/client-api/cpp/), receive data but rely on the server for query execution.

## Key concepts

### Shared tickets

A ticket is a reference to a table on a server. It points to the table but does not contain its data. Deephaven has several kinds of ticket, including:

- A _scope ticket_, which names a variable in the remote server's global scope. Any session, including the local server's Barrage session, can use it.
- An _application ticket_, which names a field that an [Application Mode](../how-to-guides/application-mode.md) application exports on the remote server. Any session, including the local server's Barrage session, can use it.
- A _shared ticket_, which names a table that one session publishes for other sessions to use.
- An _export ticket_, which refers to a table that a session created and exported to its own client. An export ticket belongs to the client session that created it, so the local server's Barrage session can't use it.

To capture a scope variable or an application field, you can also [resolve its URI](../how-to-guides/use-uris.md) with [`ResolveTools.resolve`](../reference/data-import-export/uri.md). URI resolution works only against a remote server that allows [anonymous authentication](../how-to-guides/authentication/auth-anon.md).

Other clients or servers can subscribe to or snapshot a shared ticket, which lets a session share a table that isn't in the global scope. The shared ticket stays valid only while the publishing session is open and the published table is alive.

In the following example, the Python client, [`pydeephaven`](/core/client-api/python/), publishes the table. The client runs in its own Python process, not in your Groovy server. Your Groovy server is the local server from the [architecture overview](#architecture-overview). The code connects to a remote Deephaven server, creates a [ticking table](./table-types.md) there, and publishes the table to a shared ticket. It then prints the ticket's bytes as a hex string for you to paste into the local server:

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket

# Connect to the remote server and create a ticking table on it
client = Session(host="remote-server", port=10000)
table_ref = client.time_table("PT1s").update(["X = i"])

# Publish the table to a shared ticket
ticket = SharedTicket.random_ticket()
client.publish_table(ticket, table_ref)
print(ticket.bytes.hex())
```

`table_ref` is a reference to the table on the remote server, not a Deephaven table on the local server. Keep the Python process running and `client` open while the local server captures the table, because the ticket stops working when the publishing session closes. To get the data onto the local server, capture it through the shared ticket, as the next section shows.

### Subscriptions vs. snapshots

Barrage supports two primary modes of retrieving data:

- **Subscription**: Opens a persistent connection that streams updates as the source table changes. The client receives an initial snapshot followed by incremental updates. Use this for [ticking tables](./table-types.md) you want to monitor in real time.

- **Snapshot**: Retrieves a one-time, static copy of the table that doesn't update after Barrage delivers it. Use this for [static tables](./table-types.md) or when you need a point-in-time capture.

This example continues the [shared ticket example](#shared-tickets), which printed the shared ticket as a hex string. To capture the table on the local server:

1. Build a [`TableSpec`](/core/javadoc/io/deephaven/qst/table/TableSpec.html) from the hex string.
2. Write the remote server's address as a [Deephaven URI](../how-to-guides/use-uris.md#deephaven-uri-structure) target, such as `dh+plain://remote-server:10000`.
3. Pass both to the [`subscribe`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html#subscribe(java.lang.String,io.deephaven.qst.table.TableSpec)) or [`snapshot`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html#snapshot(java.lang.String,io.deephaven.qst.table.TableSpec)) method of [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html).

Each method returns a `Future`. Its `get` method returns a Deephaven table on the local server:

```groovy skip-test
import io.deephaven.qst.table.TableSpec
import io.deephaven.server.uri.BarrageTableResolver

remoteTarget = "dh+plain://remote-server:10000"
// Paste the hex string that the Python client printed
ticketSpec = TableSpec.ticket("<shared ticket hex>".decodeHex())

// Subscription: receives ongoing updates
streamingTable = BarrageTableResolver.get().subscribe(remoteTarget, ticketSpec).get()

// Snapshot: one-time static copy
staticTable = BarrageTableResolver.get().snapshot(remoteTarget, ticketSpec).get()
```

[`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) keeps its connection to the remote server open for later requests.

> [!NOTE]
> [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) does not send credentials, so this example works only against a remote server that allows [anonymous authentication](../how-to-guides/authentication/auth-anon.md). A server that allows only the default [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md) rejects the connection. The Python publishing code in the [shared ticket example](#shared-tickets) also connects anonymously, the default for [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session). It can instead pass `auth_type` and `auth_token` to use another method that the remote server accepts.

See [Capture Python client tables with Barrage](../how-to-guides/capture-tables.md) for a complete walkthrough that publishes a table from the Python client and captures it on a Groovy server.

### Viewports

A viewport defines a window over a table: a set of row positions, typically a contiguous range, and a subset of columns. Viewports are essential for interactive applications where users scroll through large tables. Rather than streaming millions of rows, the server sends only the data visible in the current view.

Deephaven's web UI manages viewports for you. When a user scrolls or resizes a table view, the UI updates its viewport subscription. Applications built on the [JavaScript client](../how-to-guides/use-jsapi.md) set the viewport themselves with [`setViewport`](/core/client-api/javascript/classes/dh.Table.html#setViewport).

Viewports are also available from:

- Groovy, through the [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) [`subscribe`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html#subscribe(java.lang.String,io.deephaven.qst.table.TableSpec,io.deephaven.engine.rowset.RowSet,java.util.BitSet)) and [`snapshot`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html#snapshot(java.lang.String,io.deephaven.qst.table.TableSpec,io.deephaven.engine.rowset.RowSet,java.util.BitSet)) overloads that take a viewport [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) and a column `BitSet`.
- The [Java client](../how-to-guides/java-client.md), through [`BarrageSubscription.partialTable`](/core/javadoc/io/deephaven/client/impl/BarrageSubscription.html).

The following example continues the [subscription example](#subscriptions-vs-snapshots). It builds the viewport with [`RowSetFactory`](/core/javadoc/io/deephaven/engine/rowset/RowSetFactory.html) and subscribes to the first 100 row positions of the `X` column, which is column 1 of the published table:

```groovy skip-test
import io.deephaven.engine.rowset.RowSetFactory
import io.deephaven.server.uri.BarrageTableResolver

// Row positions 0 through 99
viewport = RowSetFactory.fromRange(0, 99)

// Column 1 only (X); column 0 is Timestamp
columns = new BitSet()
columns.set(1)

viewportTable = BarrageTableResolver.get().subscribe(remoteTarget, ticketSpec, viewport, columns).get()
```

> [!NOTE]
> [`ResolveTools.resolve`](../reference/data-import-export/uri.md) subscribes to entire tables only. To limit the rows or columns you receive through it, create a table on the remote server that is already filtered with [`where`](../reference/table-operations/filter/where.md) or narrowed with [`view`](../reference/table-operations/select/view.md), and resolve that table instead. You can also use the viewport overloads of [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) shown above.

### Update intervals and batching

Barrage batches table updates before sending them to subscribers. This batching reduces network overhead when tables update frequently. The update interval is configurable:

- **Server default**: The server applies one default interval to every subscription that doesn't request its own. This includes [`ResolveTools.resolve`](../reference/data-import-export/uri.md) and [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) [`subscribe`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html#subscribe(java.lang.String,io.deephaven.qst.table.TableSpec)) calls made without options. The [update interval](../how-to-guides/performance/barrage-performance.md#update-interval) section of the Barrage performance guide names the property that sets this default and gives its value.
- **Per-subscription**: A client can request its own interval for each subscription. From Groovy:
  1. Build a [`BarrageSubscriptionOptions`](/core/javadoc/io/deephaven/extensions/barrage/BarrageSubscriptionOptions.html) with [`minUpdateIntervalMs`](/core/javadoc/io/deephaven/extensions/barrage/BarrageSubscriptionOptions.Builder.html#minUpdateIntervalMs(int)) set to the interval you want. Also set [`useDeephavenNulls(true)`](/core/javadoc/io/deephaven/extensions/barrage/BarrageSubscriptionOptions.Builder.html#useDeephavenNulls(boolean)), which the resolver's default options set.
  2. Pass the options to a [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) [`subscribe`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html#subscribe(io.deephaven.uri.DeephavenTarget,io.deephaven.qst.table.TableSpec,io.deephaven.extensions.barrage.BarrageSubscriptionOptions)) overload that takes them. These overloads take a [`DeephavenTarget`](/core/javadoc/io/deephaven/uri/DeephavenTarget.html), not a URI string.

  The [JavaScript client](../how-to-guides/use-jsapi.md) and the [Java client](../how-to-guides/java-client.md) also accept a per-subscription interval.

The interval is fixed for the life of the subscription. To use a different interval, create a new subscription.

The following example continues the [subscription example](#subscriptions-vs-snapshots). It subscribes to the same table with a five-second interval:

```groovy skip-test
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions
import io.deephaven.server.uri.BarrageTableResolver
import io.deephaven.uri.DeephavenTarget

options = BarrageSubscriptionOptions.builder()
    .useDeephavenNulls(true)
    .minUpdateIntervalMs(5000)
    .build()

slowTable = BarrageTableResolver.get().subscribe(DeephavenTarget.of(URI.create(remoteTarget)), ticketSpec, options).get()
```

A shorter interval reduces latency but increases network traffic. A longer interval reduces traffic but introduces delay.

## Barrage vs. Arrow Flight

Barrage uses the same columnar data format as [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md) and adds the features this page has described for ticking tables:

| Feature                                                 | Arrow Flight          | Barrage               |
| ------------------------------------------------------- | --------------------- | --------------------- |
| Data format                                             | Arrow columnar format | Arrow columnar format |
| Static table transfer                                   | ✅ Supported          | ✅ Supported          |
| Incremental updates                                     | ❌ Not supported      | ✅ Native support     |
| Viewports                                               | ❌ Not supported      | ✅ Supported          |
| Update batching                                         | ❌ N/A                | ✅ Configurable       |
| [Row shifts](./table-update-model.md) and modifications | ❌ N/A                | ✅ Efficient encoding |

Barrage builds on [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md), so a plain [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md) client can fetch a static snapshot of a table with the Flight [`DoGet`](https://arrow.apache.org/docs/format/Flight.html#downloading-data) call, which downloads a stream of data for a ticket. The incremental update features require a Barrage-aware client.

## Performance considerations

- **Large initial snapshots**: When a client subscribes to a large table, the initial snapshot can be memory-intensive. By default, the server breaks a large initial snapshot into smaller chunks and sends them over several [update cycles](./table-update-model.md). Use the [subscription growth controls](../how-to-guides/performance/barrage-performance.md#control-subscription-snapshot-size) to tune the chunk size or turn off chunking.

- **High-frequency updates**: Tables that tick rapidly can generate significant network traffic. Consider requesting a longer interval for that subscription (see [Update intervals and batching](#update-intervals-and-batching)), increasing the server's [default update interval](../how-to-guides/performance/barrage-performance.md#update-interval), or [filtering](../how-to-guides/use-filters.md) the table before you subscribe to it.

- **Wide tables**: Subscribe only to the columns you need. Either share a table that contains only those columns, for example one made with [`view`](../reference/table-operations/select/view.md), or pass a column `BitSet` to a [`BarrageTableResolver`](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html) viewport overload. To keep every row, pass `null` as the viewport. Fewer columns means less data to transfer.

- **Unknown bottlenecks**: Use the [Barrage performance tables](../how-to-guides/performance/barrage-performance.md) to track subscription health and identify bottlenecks.

## Related documentation

- [Interpret Barrage metrics](./barrage-metrics.md)
- [Incremental update model](./table-update-model.md)
- [Deephaven's table types](./table-types.md)
- [Aggregations in Deephaven](./aggregations.md)
- [Designing the Deephaven Core API](./deephaven-core-api.md)
- [Capture Python client tables with Barrage](../how-to-guides/capture-tables.md)
- [Barrage metrics for performance monitoring](../how-to-guides/performance/barrage-performance.md)
- [Barrage schema annotation](../how-to-guides/data-import-export/barrage-schema.md)
- [Arrow Flight and Deephaven](../how-to-guides/data-import-export/arrow-flight.md)
- [Use URIs to share tables](../how-to-guides/use-uris.md)
- [Application Mode](../how-to-guides/application-mode.md)
- [Use the JS API](../how-to-guides/use-jsapi.md)
- [Extract data with a Java client](../how-to-guides/java-client.md)
- [Filter table data](../how-to-guides/use-filters.md)
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Enable anonymous authentication](../how-to-guides/authentication/auth-anon.md)
- [Configure and use pre-shared key authentication](../how-to-guides/authentication/auth-psk.md)
- [URI](../reference/data-import-export/uri.md)
- [`view`](../reference/table-operations/select/view.md)
- [`where`](../reference/table-operations/filter/where.md)
- [Barrage protocol documentation](/barrage/docs)
- [`BarrageTableResolver` Javadoc](/core/javadoc/io/deephaven/server/uri/BarrageTableResolver.html)
- [`TableSpec` Javadoc](/core/javadoc/io/deephaven/qst/table/TableSpec.html)
- [`BarrageSubscriptionOptions` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/BarrageSubscriptionOptions.html)
- [`BarrageSubscription` Javadoc](/core/javadoc/io/deephaven/client/impl/BarrageSubscription.html)
- [`DeephavenTarget` Javadoc](/core/javadoc/io/deephaven/uri/DeephavenTarget.html)
- [`RowSet` Javadoc](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)
- [`RowSetFactory` Javadoc](/core/javadoc/io/deephaven/engine/rowset/RowSetFactory.html)
