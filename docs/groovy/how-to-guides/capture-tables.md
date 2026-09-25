---
title: Capture remote tables with Barrage
sidebar_label: Capture remote tables
---

> [!NOTE]
> In this guide, "capturing" a Deephaven table refers to either subscribing to its real-time data stream or producing a static snapshot of its data. Subscribing is only appropriate for streaming tables, while snapshots can be made of static or streaming tables.

A Groovy Deephaven server can use the Deephaven Java client, which is included with the server, to create tables on a remote Deephaven server and capture them with [Barrage](../conceptual/what-is-barrage.md). The captured tables are true Deephaven tables on the local server, so you can use them in any query. Tables can also be published to _shared tickets_ — endpoints (references) for Deephaven tables that clients and servers can share.

> [!NOTE]
> URI and Shared Tickets are two different ways to pull tables. Both work on static or dynamic tables. A URI pulls a table that already exists in a remote server's scope, via a URL-like string. The approach in this guide also lets you create tables on the remote server and share them through shared tickets. Learn more about using URIs with Deephaven in the [URI guide](./use-uris.md). For the Python version of this workflow, which uses the `pydeephaven` client, see [Capture Python client tables](/core/docs/how-to-guides/capture-tables/).

## Setup

This guide covers capturing tables from a "remote" Deephaven server to a "local" Deephaven server. The servers do not need to run on different hosts, but the terminology helps distinguish the two.

The local server opens a Barrage session to the remote server. The [`BarrageSession`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/BarrageSession.html) retrieves tables from the remote server, and it wraps a Java client [`Session`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/Session.html), available through its `session` method, that performs operations on the remote server.

No installation is required. The Java client classes used in this guide are part of the Deephaven server.

## Connect to the remote Deephaven server

To connect, describe the remote server with a `ClientConfig`, then create a Barrage session through the local server's session factory. The target is a URI with the `dh+plain` scheme (no TLS) or the `dh` scheme (TLS).

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

Suppose instead that the "remote" Deephaven server is running at IP address `192.168.0.1` on port `11000`, using [pre-shared key authentication](./authentication/auth-psk.md), where the password is `D33phavenR0cks!`. Pass the authentication type and value, separated by a space, in a `SessionConfig`:

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

## Retrieve table references using the client

Once you've established a connection to the remote Deephaven server, you can use the session to create new tables on the server or retrieve references to existing tables.

As an example, you can create a new static table on the server using `emptyTable`:

```groovy skip-test
session = barrageSession.session()

tableRef = session.emptyTable(10).update("X = i", "Y = X / 2")
```

`tableRef` is not a Deephaven table itself, but a [`TableHandle`](https://deephaven.io/core/javadoc/io/deephaven/client/impl/TableHandle.html): a _reference_ to a Deephaven table on the remote server. Table operations on a handle, such as `update`, run on the remote server and return a new handle.

Similarly, you can use `timeTable` to create a ticking table on the server:

```groovy skip-test
import java.time.Duration

tableRef = session.timeTable(Duration.ofSeconds(1)).update("X = i", "Y = X / 2")
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

Alternatively, you can get a static snapshot of the table. This is the recommended approach if the table is static or if you want a static representation of a ticking table:

```groovy skip-test
import io.deephaven.extensions.barrage.BarrageSnapshotOptions

snapOptions = BarrageSnapshotOptions.builder().useDeephavenNulls(true).build()

localTStatic = barrageSession.snapshot(tableRef, snapOptions).entireTable().get()
```

Voila! You now have _real_ Deephaven server tables called `localTStreaming` and `localTStatic`. These are not just references to Deephaven tables — they are _real_ Deephaven server tables that can be used in any Deephaven query.

`subscribe` and `snapshot` also have `partialTable` variants that capture only a viewport of rows and a subset of columns. See [What is Barrage?](../conceptual/what-is-barrage.md#viewports).

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

A session in another process needs the ticket's ID. The `asHexString` method of `sharedId` returns it as a hexadecimal string that you can pass along. A Python client can also publish tables to shared tickets that a Groovy server captures; see [What is Barrage?](../conceptual/what-is-barrage.md#shared-tickets).

## Subscription lifecycle management

Understanding when and how to manage Barrage subscriptions helps you build efficient, reliable applications.

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

### Subscription resource usage

Each active subscription consumes resources on both the server and client:

| Resource | Server Impact                                    | Client Impact                                     |
| -------- | ------------------------------------------------ | ------------------------------------------------- |
| Memory   | Maintains subscriber state and pending updates   | Stores table data and applies incremental updates |
| CPU      | Aggregates and serializes updates per subscriber | Deserializes and processes incoming updates       |
| Network  | Sends periodic update batches to each subscriber | Receives and buffers incoming data                |

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

Barrage subscriptions can be affected by network interruptions. Consider these patterns for production applications:

- **Reconnection**: If the session disconnects, you'll need to create a new Barrage session and resubscribe. To resubscribe to a shared ticket, the remote table must still be published to the same ticket.

- **Ticket lifetime**: A shared ticket remains valid only while the published table is still exported by the publishing session. Closing the publishing session, or calling `close` on the table handle you published (`tableRef`), releases the table and invalidates the ticket. The Java client keeps a handle's export alive until you close it, so garbage collection does not release it: close handles explicitly once others no longer need the ticket.

- **Authentication expiry**: If using authenticated connections, ensure tokens or credentials remain valid for the duration of long-running subscriptions.

### Memory considerations for large tables

When subscribing to large ticking tables:

- **Initial snapshot size**: The first update contains a complete snapshot of the table. For very large tables, this can consume significant memory. The server breaks large snapshots into chunks by default (see [snapshot size control](./performance/barrage-performance.md#control-subscription-snapshot-size)).

- **Incremental updates**: After the initial snapshot, only changed rows are transmitted. This is typically much smaller than the full table.

- **Server-side filtering**: If you only need a subset of the data, consider filtering the table on the remote server before subscribing. This reduces both network and memory usage. (Note: this is distinct from viewports, which define a scrollable window over row positions.)

```groovy skip-test
// The filter runs on the remote server
filteredRef = session.of(TicketTable.fromQueryScopeField("large_table")).where("Region = `EAST`")

// The subscriber receives only the filtered data
localFiltered = barrageSession.subscribe(filteredRef, subOptions).entireTable().get()
```

## Related documentation

- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Barrage metrics](./performance/barrage-performance.md)
- [Share tables with URIs](./use-uris.md)
- [Capture Python client tables](/core/docs/how-to-guides/capture-tables/)
- [Anonymous authentication](./authentication/auth-anon.md)
- [Pre-shared key authentication](./authentication/auth-psk.md)
- [Keycloak authentication](./authentication/auth-keycloak.md)
- [mTLS Authentication](./authentication/auth-mtls.md)
- [Username/password Authentication](./authentication/auth-uname-pw.md)
