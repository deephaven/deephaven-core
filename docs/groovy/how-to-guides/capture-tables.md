---
title: Capture remote tables with Barrage
---

A Groovy Deephaven server can use the Deephaven Java client, which is included with the server, to create tables on a remote Deephaven server and capture them with [Barrage](../conceptual/what-is-barrage.md). In this guide, _capturing_ a table means either subscribing to its real-time data stream or taking a static snapshot of its data. The captured tables are true Deephaven tables on the local server, so you can use them in any query. To let other sessions capture the same tables, publish them to _shared tickets_ — endpoints (references) for Deephaven tables that clients and servers can share.

> [!NOTE]
> URIs and shared tickets are two different ways to pull tables, and both work on static or dynamic tables. A URI pulls a table that already exists in a remote server's scope via a URL-like string, while shared tickets also let you create tables on the remote server and share them. See the [URI guide](./use-uris.md). For the Python version of this workflow, which uses the `pydeephaven` client, see [Capture Python client tables with Barrage](/core/docs/how-to-guides/capture-tables/).

## Setup

This guide covers capturing tables from a _remote_ Deephaven server to a _local_ Deephaven server. The servers do not need to run on different hosts, but the terminology helps distinguish the two.

The local server opens a Barrage session to the remote server. The [`BarrageSession`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/BarrageSession.html) retrieves tables from the remote server. It wraps a Java client [`Session`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/Session.html), available through its `session` method, that performs operations on the remote server.

No installation is required. The Java client classes used in this guide are part of the Deephaven server.

## Connect to the remote Deephaven server

To connect, describe the remote server with a [`ClientConfig`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/ClientConfig.html), then create a Barrage session through the local server's session factory. The target is a URI with the `dh+plain` scheme (no TLS) or the `dh` scheme (TLS).

Suppose that the "remote" Deephaven server is running locally on port `9999` with [anonymous authentication](./authentication/auth-anon.md). Connect to it as follows:

```groovy skip-test
import io.deephaven.client.impl.ClientConfig
import io.deephaven.server.runner.DeephavenApiServer
import io.deephaven.uri.DeephavenTarget

clientConfig = ClientConfig.builder()
    .target(DeephavenTarget.of(URI.create("dh+plain://localhost:9999")))
    .build()

factory = DeephavenApiServer.getInstance().sessionFactoryCreator().barrageFactory(clientConfig)
barrageSession = factory.newBarrageSession()
```

Suppose instead that the "remote" Deephaven server is running at IP address `192.168.0.1` on port `11000`, using [pre-shared key authentication](./authentication/auth-psk.md), where the password is `D33phavenR0cks!`. Pass the authentication type and value, separated by a space, in a [`SessionConfig`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/SessionConfig.html):

```groovy skip-test
import io.deephaven.client.impl.ClientConfig
import io.deephaven.client.impl.SessionConfig
import io.deephaven.server.runner.DeephavenApiServer
import io.deephaven.uri.DeephavenTarget

clientConfig = ClientConfig.builder()
    .target(DeephavenTarget.of(URI.create("dh+plain://192.168.0.1:11000")))
    .build()

factory = DeephavenApiServer.getInstance().sessionFactoryCreator().barrageFactory(clientConfig)
barrageSession = factory.newBarrageSession(
    SessionConfig.builder()
        .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler D33phavenR0cks!")
        .build()
)
```

## Create or look up tables on the remote server

Once you've established a connection to the remote Deephaven server, you can use the session to create new tables on the server or retrieve references to existing tables.

As an example, you can create a new static table on the server. Build the whole query as a [`TableSpec`](https://deephaven.io/core/javadoc/io/deephaven/qst/table/TableSpec.html), then send it to the server once with `session.execute`:

```groovy skip-test
import io.deephaven.qst.table.TableSpec

session = barrageSession.session()

tableRef = session.execute(TableSpec.empty(10).update("X = i", "Y = X / 2"))
```

`tableRef` is not a Deephaven table itself, but a [`TableHandle`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/TableHandle.html): a _reference_ to a Deephaven table on the remote server. Calling a table operation such as `update` on a handle runs it on the remote server and returns a second handle, and the first handle stays open. Building the query as a `TableSpec` first creates one handle for the whole query, so there is nothing intermediate to close.

Similarly, you can create a ticking table on the server with a `TimeTable`:

```groovy skip-test
import io.deephaven.qst.table.TimeTable
import java.time.Duration

tableRef = session.execute(TimeTable.of(Duration.ofSeconds(1)).update("X = i", "Y = X / 2"))
```

If a table already exists in the remote server's query scope, you can retrieve a reference to it by name:

```groovy skip-test
import io.deephaven.qst.table.TicketTable

tableRef = session.of(TicketTable.fromQueryScopeField("table_on_server"))
```

## Capture a table

To capture the table that a handle refers to, pass the handle to the Barrage session. Subscribing creates a local table that updates in real time when the remote table changes:

```groovy skip-test
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions

subOptions = BarrageSubscriptionOptions.builder().useDeephavenNulls(true).build()

localTStreaming = barrageSession.subscribe(tableRef, subOptions).entireTable().get()
```

Alternatively, you can get a static snapshot of the table:

```groovy skip-test
import io.deephaven.extensions.barrage.BarrageSnapshotOptions

snapOptions = BarrageSnapshotOptions.builder().useDeephavenNulls(true).build()

localTStatic = barrageSession.snapshot(tableRef, snapOptions).entireTable().get()
```

You now have `localTStreaming` and `localTStatic`, which are true Deephaven tables on the local server. Unlike `tableRef`, they aren't references, so you can use them in any query.

`subscribe` and `snapshot` also have `partialTable` variants that capture only a viewport of rows, a subset of columns, or both. See [What is Barrage?](../conceptual/what-is-barrage.md#viewports).

### Choose between subscribe and snapshot

Use **subscribe** when:

- You need real-time updates as the source table changes.
- You're building a live dashboard or monitoring system.
- The source table is ticking (updating periodically).

Use **snapshot** when:

- You need a one-time, static copy of the data.
- The source table is static and won't change.
- You want to capture a point-in-time state for analysis.
- You need to reduce ongoing resource consumption.

## Share a table with a shared ticket

A table handle belongs to the session that created it. To let other sessions capture the same table — another server, a Python client, or a second Barrage session — publish it to a shared ticket. Create the ticket with [`SharedId`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/SharedId.html), and publish the table to it:

```groovy skip-test
import io.deephaven.client.impl.SharedId

sharedId = SharedId.newRandom()
session.publish(sharedId, tableRef).get()
```

Any session connected to the remote server can now capture the table from the ticket:

```groovy skip-test
localFromTicket = barrageSession.subscribe(sharedId.ticketId().table(), subOptions).entireTable().get()
```

A session in another process needs the ticket's ID. The `asHexString` method of `sharedId` returns the 16-byte ID as a `0x`-prefixed hexadecimal string that you can pass along; the receiver decodes the hex digits after `0x` into bytes and constructs `new SharedId(bytes)`. A Python client can also publish tables to shared tickets that a Groovy server captures; see [What is Barrage?](../conceptual/what-is-barrage.md#shared-tickets).

## Manage subscriptions

This section covers what an active subscription costs, how to close the session, how to recover from connection problems, and how to keep large subscriptions small.

### Subscription resource usage

Each active subscription consumes resources on both the remote server and the local (subscribing) server:

| Resource | Remote server impact                                                                      | Local server impact                               |
| -------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------- |
| Memory   | Maintains subscriber state and pending updates                                            | Stores table data and applies incremental updates |
| CPU      | Aggregates updates once per table and update interval, then writes each subscriber's view | Deserializes and processes incoming updates       |
| Network  | Sends periodic update batches to each subscriber                                          | Receives and buffers incoming data                |

For tables with frequent updates or many subscribers, these costs can add up. Monitor subscription health using the [Barrage performance tables](./performance/barrage-performance.md).

### Close the session when finished

When you no longer need the connection, close the Barrage session to release resources. Closing the session does not close the connection's channel, so shut that down as well:

```groovy skip-test
import java.util.concurrent.TimeUnit

barrageSession.close()

channel = factory.managedChannel()
channel.shutdownNow()
channel.awaitTermination(10, TimeUnit.SECONDS)
```

### Handle connection issues

Network interruptions can break Barrage subscriptions. Consider these patterns for production applications:

- **Reconnection**: Subscriptions do not resume automatically after a disconnect. Create a new Barrage session and resubscribe. If the session that published a shared ticket was lost (in this guide, the same session), the server released its exports: publish the table again from the new session before resubscribing.

- **Ticket lifetime**: A shared ticket remains valid only while the published table is still exported by the publishing session. Closing the publishing session releases all of its exports and invalidates the ticket. Java client handles are reference counted: the export is released only after every handle that refers to it is closed, including other handles for the same table in that session and the reference an active subscription created from the handle holds. Garbage collection never releases a handle, so close handles explicitly once others no longer need the ticket.

- **Authentication expiry**: The client refreshes its session token automatically while the session is open. Credentials are needed again only when you open a new session, for example after reconnecting, so make sure they are still valid then.

### Memory considerations for large tables

When subscribing to large ticking tables:

- **Initial snapshot size**: A subscription starts with a complete snapshot of the table. For very large tables, this can consume significant memory. By default, the server sends a large snapshot in several chunks (see [snapshot size control](./performance/barrage-performance.md#control-subscription-snapshot-size)), and the local table is returned once the whole snapshot has arrived.

- **Incremental updates**: After the initial snapshot, only changes are sent: data for added rows, the changed columns of modified rows, and row keys for removed or shifted rows. This is typically much smaller than the full table.

- **Server-side filtering**: If you only need a subset of the data, consider filtering the table on the remote server before subscribing. This reduces both network and memory usage. This is distinct from viewports, which define a scrollable window over row positions. The following example reuses `session`, `subOptions`, and the `TicketTable` import from earlier:

```groovy skip-test
// The filter runs on the remote server
filteredRef = session.execute(TicketTable.fromQueryScopeField("large_table").where("Region = `EAST`"))

// The subscriber receives only the filtered data
localFiltered = barrageSession.subscribe(filteredRef, subOptions).entireTable().get()
```

## Related documentation

- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Barrage metrics](./performance/barrage-performance.md)
- [Share tables with URIs](./use-uris.md)
- [Capture Python client tables with Barrage](/core/docs/how-to-guides/capture-tables/)
- [Anonymous authentication](./authentication/auth-anon.md)
- [Pre-shared key authentication](./authentication/auth-psk.md)
- [Keycloak authentication](./authentication/auth-keycloak.md)
- [mTLS authentication](./authentication/auth-mtls.md)
- [Username/password authentication](./authentication/auth-uname-pw.md)
