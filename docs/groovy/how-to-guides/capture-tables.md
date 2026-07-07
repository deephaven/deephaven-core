---
title: Capture Python client tables with Barrage
sidebar_label: Capture Python client tables
---

> [!NOTE]
> In this guide, "capturing" a Deephaven table refers to either subscribing to its real-time data stream or producing a static snapshot of its data. Subscribing is only appropriate for streaming tables, while snapshots can be made of static or streaming tables.

The [Deephaven Python client](/core/client-api/python/) can create new tables and retrieve references to tables on a remote Deephaven server. However, these references cannot be used in a typical Deephaven server query, since they are not true Deephaven tables. To get true Deephaven tables from a remote server, you must subscribe to them using [Barrage](https://github.com/deephaven/barrage) shared tickets. Shared tickets are endpoints (references) for Deephaven tables that clients and servers can share.

> [!NOTE]
> URI and Shared Tickets are two different ways to pull tables. Both work on static or dynamic tables. URI pulls tables already on the server via a URL-like string. Shared Tickets let you pull tables you create or access via the Python Client. Learn more about using URI with Deephaven in the [URI guide](../how-to-guides/use-uris.md).

## Setup

This guide covers capturing tables from a "remote" Deephaven server to a "local" Deephaven server. The servers do not need to run on different hosts, but the terminology helps distinguish the two.

The local server creates two connections to the remote server. The first connection is from the Deephaven Python client, [`pydeephaven`](/core/client-api/python/). It is used to perform queries on the remote server. The second connection is a [Barrage session](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession) used to retrieve tables from the remote server. Here is a diagram of the setup:

![A diagram showing one Deephaven server sending data from Barrage and pydeephaven to a second Deephaven server](../assets/how-to/servers.png)

You need an installation of [`pydeephaven`](/core/client-api/python/) on the local server:

```bash
pip install pydeephaven
```

See the guide on [installing Python packages](/core/docs/how-to-guides/install-and-use-python-packages/) for more information. To learn about the Deephaven Python client, check out the [Python client Quickstart](/core/docs/getting-started/pyclient-quickstart/).

## Connect the Python client to the remote Deephaven server

The Deephaven Python client creates and maintains a connection to the remote Deephaven server through a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session). This object is also what you use to create tables on the remote server. To connect to a remote server via a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session), you must supply it with information about the remote server's hostname or URL, port, and authentication requirements.

Suppose that the "remote" Deephaven server is running locally on port `9999` with [anonymous authentication](./authentication/auth-anon.md). Connect to it as follows:

```python skip-test
from pydeephaven import Session

# this is short for Session(host="localhost", port=9999, auth_type="Anonymous")
client_session = Session(port=9999)
```

By default, a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) assumes that the remote Deephaven server is using [anonymous authentication](./authentication/auth-anon.md). This is not always the case. Suppose that the "remote" Deephaven server is running at IP address `192.168.0.1` on port `11000`, using [pre-shared key authentication](./authentication/auth-psk.md), where the password is `D33phavenR0cks!`. Connect to it as follows:

```python skip-test
from pydeephaven import Session

client_session = Session(
    host="192.168.0.1",
    port=11000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="D33phavenR0cks!",
)
```

See the [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) API documentation for more configuration options.

## Retrieve table references using the client

Once you've established a client connection to the remote Deephaven server, you can use the [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) instance to create new tables on the server or retrieve references to existing tables.

As an example, you can create a new static table on the server using [`empty_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.empty_table):

```python skip-test
table_ref = client_session.empty_table(10).update(["X = i", "Y = X / 2"])
```

`table_ref` is not a Deephaven table itself, but a _reference_ to a Deephaven table on the server. See this [overview of the Python client design](/core/docs/getting-started/pyclient-quickstart/) for more information on table references.

Similarly, you can use [`time_table`](/core/client-api/python/code/pydeephaven.html#pydeephaven.Session.time_table) to create a ticking table on the server:

```python skip-test
table_ref = client_session.time_table("PT1s").update(["X = i", "Y = X / 2"])
```

If a table already exists on the remote server, you can retrieve a reference to it with the [`open_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.open_table) method:

```python skip-test
table_ref = client_session.open_table("table_on_server")
```

## Create a shared ticket and capture

Once you have a reference to a table on the server, it's easy to publish it with a shared ticket. First, create a shared ticket with the [`SharedTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket) class, and publish the table to the ticket:

<!--- TODO: link https://github.com/deephaven/deephaven.io/issues/3918 when complete.-->

```python skip-test
from pydeephaven.session import SharedTicket

ticket = SharedTicket.random_ticket()
client.publish_table(ticket, table_ref)
```

Next, you will need a Barrage session to subscribe to the ticket. To create the session, use the [`barrage_session`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.barrage_session) function. Notice that [`barrage_session`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.barrage_session) takes the same connection arguments as [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session):

```python skip-test
from deephaven.barrage import barrage_session

my_barrage_session = barrage_session(
    host="192.168.0.1",
    port=11000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="D33phavenR0cks!",
)
```

By subscribing to the ticket, you can create a local table that updates in real time when the remote table changes:

```python skip-test
local_t_streaming = my_barrage_session.subscribe(ticket.bytes)
```

Alternatively, you can get a static snapshot of a table by using [snapshot](../reference/table-operations/snapshot/snapshot.md). This is the recommended approach if the table is static or if you want a static representation of a ticking table:

```python skip-test
local_t_static = my_barrage_session.snapshot(ticket.bytes)
```

Voila! You now have _real_ Deephaven server tables called `local_t_streaming` and `local_t_static`. These are not just references to Deephaven tables - they are _real_ Deephaven server tables that can be used in any Deephaven query.

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

### Close subscriptions when finished

When you no longer need real-time updates, close the Barrage session to release resources:

```python skip-test
# Close the session when done
my_barrage_session.close()
```

If you need to keep the session open for other subscriptions but want to release a specific table, you can drop the reference to the subscribed table. However, the underlying subscription may remain active until the session is closed.

### Handle connection issues

Barrage subscriptions can be affected by network interruptions. Consider these patterns for production applications:

- **Reconnection**: If the session disconnects, you'll need to create a new `barrage_session` and resubscribe. The remote table must still be published to the same shared ticket.

- **Ticket lifetime**: Shared tickets remain valid as long as the publishing session is active. If the publishing session closes, the ticket becomes invalid and subscribers will lose their connection.

- **Authentication expiry**: If using authenticated connections, ensure tokens or credentials remain valid for the duration of long-running subscriptions.

### Memory considerations for large tables

When subscribing to large ticking tables:

- **Initial snapshot size**: The first update contains a complete snapshot of the table. For very large tables, this can consume significant memory. The server breaks large snapshots into chunks by default (see [snapshot size control](./performance/barrage-performance.md#control-subscription-snapshot-size)).

- **Incremental updates**: After the initial snapshot, only changed rows are transmitted. This is typically much smaller than the full table.

- **Viewport filtering**: If you only need a subset of the data, consider filtering the table on the remote server before publishing it to the shared ticket. This reduces both network and memory usage.

```python skip-test
# On the remote server: filter before publishing
filtered_ref = client_session.open_table("large_table").where("Region = `EAST`")
client_session.publish_table(ticket, filtered_ref)

# The subscriber now receives only the filtered data
```

## Related documentation

<!--- TODO: link https://github.com/deephaven/deephaven.io/issues/3918 when complete.-->

- [Python client Quickstart](/core/docs/getting-started/pyclient-quickstart/)
- [Share tables with URIs](./use-uris.md)
- [Anonymous authentication](./authentication/auth-anon.md)
- [Pre-shared key authentication](./authentication/auth-psk.md)
- [Keycloak authentication](./authentication/auth-keycloak.md)
- [mTLS Authentication](./authentication/auth-mtls.md)
- [Username/password Authentication](./authentication/auth-uname-pw.md)
