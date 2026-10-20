---
title: Capture Python client tables with Barrage
---

The [Deephaven Python client](/core/client-api/python/) can create new tables and retrieve references to tables on a remote Deephaven server. These references are not true Deephaven tables, so you can't use them in a typical Deephaven server query.

To get true Deephaven tables from a remote server, you _capture_ them. Capturing a table means either subscribing to its real-time updates or taking a static snapshot of its data. You capture tables over [Barrage](../conceptual/what-is-barrage.md) using shared tickets, which are identifiers that clients and servers can both use to address the same Deephaven table.

Shared tickets let you capture tables that you create or access with the Python client, whether they are static or [ticking](../conceptual/table-types.md). To pull a table that already exists on the remote server by name, you can use a URI instead, as long as the remote server accepts anonymous authentication. See the [URI guide](./use-uris.md).

## Setup

This guide covers capturing tables from a "remote" Deephaven server to a "local" Deephaven server. The servers do not need to run on different hosts, but the terminology helps distinguish the two.

The local server creates two connections to the remote server. The first connection is from the Deephaven Python client, [`pydeephaven`](/core/client-api/python/). The local server uses it to run queries on the remote server. The second connection is a [Barrage session](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession) that retrieves tables from the remote server. Here is a diagram of the setup:

![A diagram showing one Deephaven server sending data from Barrage and pydeephaven to a second Deephaven server](../assets/how-to/servers.png)

You need an installation of [`pydeephaven`](/core/client-api/python/) on the local server:

```bash
pip install pydeephaven
```

See the guide on [installing Python packages](./install-and-use-python-packages.md) for more information. To learn about the Deephaven Python client, check out the [Python client Quickstart](../getting-started/pyclient-quickstart.md).

## Connect the Python client to the remote Deephaven server

The Deephaven Python client creates and maintains a connection to the remote Deephaven server through a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session). You also use this object to create tables on the remote server. To connect, you supply the remote server's host name or IP address, port, and authentication details.

If you omit any of these, the [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) uses a default:

- Host: the `DH_HOST` environment variable, or `localhost` if that variable isn't set.
- Port: the `DH_PORT` environment variable, or `10000` if that variable isn't set.
- Authentication: anonymous authentication.

The local server already uses the default port, `10000`, so suppose that the "remote" Deephaven server is running locally on port `9999` with [anonymous authentication](./authentication/auth-anon.md). Connect to it as follows:

```python skip-test
from pydeephaven import Session

# with DH_HOST unset, this is short for Session(host="localhost", port=9999, auth_type="Anonymous")
client_session = Session(port=9999)
```

If the remote server uses a different authentication method, pass `auth_type` and `auth_token`. Suppose that the "remote" Deephaven server is running at IP address `192.168.0.1` on port `11000`, using [pre-shared key authentication](./authentication/auth-psk.md), where the key is `D33phavenR0cks!`. Connect to it as follows:

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

`table_ref` is not a Deephaven table itself, but a _reference_ to a Deephaven table on the server. See this [overview of the Python client design](../getting-started/pyclient-quickstart.md#2-a-brief-overview-of-the-python-client-design) for more information on table references.

Similarly, you can use [`time_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.time_table) to create a ticking table on the server:

```python skip-test
table_ref = client_session.time_table("PT1s").update(["X = i", "Y = X / 2"])
```

If a table already exists on the remote server, you can retrieve a reference to it with the [`open_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.open_table) method:

```python skip-test
table_ref = client_session.open_table("table_on_server")
```

## Publish the table to a shared ticket

Once you have a reference to a table on the server, you can publish it with a shared ticket. First, create a shared ticket with the [`SharedTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket) class, and publish the table to the ticket with [`publish_table`](../reference/client-api/session/publish-table.md):

```python skip-test
from pydeephaven.ticket import SharedTicket

ticket = SharedTicket.random_ticket()
client_session.publish_table(ticket, table_ref)
```

The ticket stays valid only while `table_ref` is alive and `client_session` is open. If the session closes, or `table_ref` is closed or garbage-collected, new subscriptions to the ticket and new snapshots of it fail. Keep both for as long as subscribers need the table.

## Capture the table with a Barrage session

To capture the table on the local server, use the [`barrage_session`](../reference/data-import-export/barrage/barrage-session.md) function to create a Barrage session. It takes the same connection arguments as [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session): `host`, `port`, `auth_type`, and `auth_token`, plus `use_tls` and `tls_root_certs` for a server that uses TLS. It also takes `extra_headers`, a dictionary of extra headers to set on the gRPC channel.

Unlike [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session), [`barrage_session`](../reference/data-import-export/barrage/barrage-session.md) doesn't read `DH_HOST` or `DH_PORT`. `host` is required, and `port` defaults to `10000`, so pass both explicitly. For the anonymous server on port `9999`, use `barrage_session(host="localhost", port=9999)`. The following example matches the pre-shared key connection above:

```python skip-test
from deephaven.barrage import barrage_session

my_barrage_session = barrage_session(
    host="192.168.0.1",
    port=11000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="D33phavenR0cks!",
)
```

The Barrage session can capture the table in two ways.

Use [`subscribe`](../reference/data-import-export/barrage/subscribe.md) when:

- You need real-time updates as the source table changes.
- You're building a live dashboard or monitoring system.
- The source table is ticking (updating periodically).

Use [`snapshot`](../reference/data-import-export/barrage/snapshot-barrage.md) when:

- You need a one-time, static copy of the data.
- The source table is static and won't change.
- You want to capture a point-in-time state for analysis.
- You need to reduce ongoing resource consumption.

By subscribing to the ticket, you can create a local table that updates in real time when the remote table changes:

```python skip-test
local_t_streaming = my_barrage_session.subscribe(ticket.bytes)
```

Alternatively, take a static snapshot:

```python skip-test
local_t_static = my_barrage_session.snapshot(ticket.bytes)
```

You now have `local_t_streaming` and `local_t_static`, which are real Deephaven tables on the local server. Unlike `table_ref`, they aren't references, so you can use them in any Deephaven query.

## Manage subscriptions

This section covers what an active subscription costs, how to release it, how to recover from connection problems, and how to keep large subscriptions small.

### Subscription resource usage

Each active subscription consumes resources on both the remote server and the local (subscribing) server:

| Resource | Remote server impact                             | Local server impact                               |
| -------- | ------------------------------------------------ | ------------------------------------------------- |
| Memory   | Maintains subscriber state and pending updates   | Stores table data and applies incremental updates |
| CPU      | Aggregates and serializes updates per subscriber | Deserializes and processes incoming updates       |
| Network  | Sends periodic update batches to each subscriber | Receives and buffers incoming data                |

For tables with frequent updates or many subscribers, these costs can add up. Monitor subscription health using the [Barrage performance tables](./performance/barrage-performance.md).

### Close the Barrage session when finished

When you no longer need the session's subscriptions or snapshots, close the Barrage session with [`close`](../reference/data-import-export/barrage/close.md) to release resources:

```python skip-test
# Close the session when done
my_barrage_session.close()
```

If you need to keep the session open for other subscriptions but want to release a specific table, you can drop the reference to the subscribed table. However, the underlying subscription may remain active until you close the session.

### Handle connection issues

Network interruptions can break Barrage subscriptions. Consider these patterns for production applications:

- **Reconnection**: If the Barrage session disconnects, create a new session with [`barrage_session`](../reference/data-import-export/barrage/barrage-session.md) and resubscribe.
- **Ticket lifetime**: If the published table or `client_session` has gone away, re-publish before resubscribing. See [Publish the table to a shared ticket](#publish-the-table-to-a-shared-ticket).
- **Authentication expiry**: If your sessions use authenticated connections, make sure their tokens or credentials stay valid for as long as your subscriptions run.

### Memory considerations for large tables

When subscribing to large ticking tables:

- **Initial snapshot size**: A subscription starts with a snapshot of the whole table. For very large tables, this can consume significant memory. By default, the server breaks large snapshots into chunks. See [snapshot size control](./performance/barrage-performance.md#control-subscription-snapshot-size).
- **Incremental updates**: After the initial snapshot, the remote server sends only changed rows. These updates are typically much smaller than the full table.
- **Server-side filtering**: If you only need a subset of the data, consider filtering the table on the remote server before subscribing. This reduces both network and memory usage.

```python skip-test
# Filter on the remote server before publishing
filtered_ref = client_session.open_table("large_table").where("Region = `EAST`")
filtered_ticket = SharedTicket.random_ticket()
client_session.publish_table(filtered_ticket, filtered_ref)

# The subscriber now receives only the filtered data
local_t_filtered = my_barrage_session.subscribe(filtered_ticket.bytes)
```

## Related documentation

- [Python client Quickstart](../getting-started/pyclient-quickstart.md)
- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Share tables with URIs](./use-uris.md)
- [Anonymous authentication](./authentication/auth-anon.md)
- [Pre-shared key authentication](./authentication/auth-psk.md)
- [Keycloak authentication](./authentication/auth-keycloak.md)
- [mTLS authentication](./authentication/auth-mtls.md)
- [Username/password authentication](./authentication/auth-uname-pw.md)
- [`barrage_session`](../reference/data-import-export/barrage/barrage-session.md)
- [`subscribe`](../reference/data-import-export/barrage/subscribe.md)
- [`snapshot`](../reference/data-import-export/barrage/snapshot-barrage.md)
- [`close`](../reference/data-import-export/barrage/close.md)
- [`publish_table`](../reference/client-api/session/publish-table.md)
