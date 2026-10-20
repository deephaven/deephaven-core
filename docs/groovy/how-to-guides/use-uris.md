---
title: Use URIs to share tables
---

This guide shows you how to use Deephaven URIs and [`io.deephaven.uri.ResolveTools`](https://deephaven.io/core/javadoc/io/deephaven/uri/ResolveTools.html) to share tables across server instances and networks.

A URI, short for [Uniform Resource Identifier](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier), is a sequence of characters that identifies a resource on the web. Think of a URI as a generalization of a URL. A Deephaven URI identifies a table on a server instance. By linking to a URI, you can work with live tables from other Deephaven server instances without rebuilding the data pipelines or queries that created them.

> [!NOTE]
> If you found this page while looking for `ui.resolve`, note that the [Deephaven UI URI component](https://deephaven.io/core/ui/docs/components/uri/) is a separate `deephaven.ui` feature that currently accepts only **Deephaven Enterprise** Persistent Query (`pq://`) URIs. It is not the same as [`io.deephaven.uri.ResolveTools.resolve`](../reference/data-import-export/uri.md) documented here.

> [!NOTE]
> URIs and shared tickets are two different ways to pull tables. Both work on static tables and on ticking tables, which update as new data arrives. A URI pulls a table that already exists on a server, using a URL-like string. Shared tickets let you pull tables you create or access via the Python client. Learn more in [Capture Python client tables with Barrage](./capture-tables.md).

> [!IMPORTANT]
> A server that resolves a URI connects to the remote server with [anonymous authentication](./authentication/auth-anon.md), so the remote server you resolve tables from must allow anonymous authentication. Deephaven doesn't currently support URI resolution with [pre-shared key (PSK) authentication](./authentication/auth-psk.md), which servers use by default. Resolving a URI that points to a server that requires PSK fails. This is a known limitation tracked in GitHub issues [#5383](https://github.com/deephaven/deephaven-core/issues/5383) and [#3421](https://github.com/deephaven/deephaven-core/issues/3421).

## Why use URIs?

Deephaven URIs provide several key benefits:

- **Canonicalized resource identification**: Access [query-scope](./query-scope.md) tables and [application fields](./application-mode.md) through a standardized string format that works across server instances.
- **Simplified data sharing**: Share tables between Deephaven instances without duplicating the queries that create them.
- **Distributed computing**: Build systems that spread processing across multiple Deephaven servers.
- **Real-time access**: Access live, updating tables from remote sources that reflect the latest data.
- **Cross-language compatibility**: A Python server can resolve tables from a Groovy server, and the reverse. See the [Python version of this guide](/core/docs/how-to-guides/use-uris).
- **Environmental isolation**: Access data across different containers, servers, or networks.

By using URIs, you let others work with your live tables without rebuilding your data pipeline, understanding your query logic, or keeping a copy of the data up to date themselves. This is particularly valuable in collaborative environments and distributed systems.

## Syntax

Like a URL such as `https://deephaven.io/core/docs`, a Deephaven URI has a scheme (`https`), an authority (the host, `deephaven.io`), and a path (`/core/docs`). In a Deephaven URI, the path is the scope followed by the resource name, and the authority can include a port.

### Deephaven URI structure

Deephaven URIs use this syntax:

```syntax
dh://<host>[:<port>]/<scope>/<resource_name>
dh+plain://<host>[:<port>]/<scope>/<resource_name>
```

The components are:

- **`dh+plain`** or **`dh`** is the scheme, which identifies the protocol for accessing Deephaven resources.
  - `dh://` indicates a secure connection (TLS/SSL).
  - `dh+plain://` indicates an insecure connection (no encryption).
- **`<host>`** is the host, which is either:
  - A Docker container name or Docker Compose service name (for container-to-container communication within the same Docker network).
  - A hostname or IP address (for network communication).
- **`<port>`** is optional. Without it, the client connects to the scheme's default port: `10000` for `dh+plain://`, `443` for `dh://`. Include the port when the remote Deephaven instance listens on a different port, for example `dh+plain://hostname:9876/scope/tableName`.
- **`<scope>`** identifies the namespace where the resource exists, usually `scope` or `app/<app_name>/field`. See [Resource scopes and paths](#resource-scopes-and-paths).
- **`<resource_name>`** is the exact name of the table you want to access. Resolving a remote `dh` or `dh+plain` URI returns a table, so the name must refer to a table.

### Resource scopes and paths

A **scope** in a Deephaven URI is a namespace that identifies where a resource exists within a Deephaven server instance. Scopes provide:

- **Separate namespaces**: A table named `trades` in the [query scope](./query-scope.md) is separate from a field named `trades` in an [application scope](./application-mode.md).
- **Clear resource organization**: You know immediately whether a resource comes from interactive work or a pre-built application.

This guide covers the two scopes you use most. The URI for each one has this form:

```syntax
// Query scope variable (most common)
dh+plain://hostname/scope/tableName
dh://hostname/scope/tableName

// Application field
dh+plain://hostname/app/myApplication/field/myField
dh://hostname/app/myApplication/field/myField
```

[`resolve`](../reference/data-import-export/uri.md) also reads CSV files, from a local path (`csv:///data/file.csv`) or over HTTP(S) (`csv+https://host/path/file.csv`), and local Parquet files or directories (`parquet:///data/file.parquet`).

#### Query scope (`scope`)

The [query scope](./query-scope.md) holds the variables that a session creates. A table or other object goes into the query scope when you:

- Create it in the Deephaven IDE console.
- Create it in a script that a client runs on the server.
- Bind a client table to a name, for example with the Python client's [`Session.bind_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.bind_table).

```groovy
// This creates a table in the query scope
myTable = emptyTable(100).update("X = i", "Y = i * 2")
// Accessible via: dh+plain://hostname/scope/myTable
```

#### Application scope (`app/<app_name>/field`)

The application scope contains fields exported by [Application Mode](./application-mode.md) applications, which are available as soon as the server starts. The `<app_name>` in the URI is the application's ID: the `id` in the `.app` file for a script application, or the ID that the application class sets for a static or dynamic application. For example, a field named `marketData` in an application with `id=tradingApp` is available at `dh://hostname/app/tradingApp/field/marketData`.

## Resolve a URI

To access a table via its URI, use the [`resolve`](../reference/data-import-export/uri.md) method from [`io.deephaven.uri.ResolveTools`](https://deephaven.io/core/javadoc/io/deephaven/uri/ResolveTools.html):

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

// Basic usage
table = resolve("dh+plain://hostname/scope/tableName")

// With explicit port
table = resolve("dh+plain://hostname:9876/scope/tableName")
```

For a `dh://` or `dh+plain://` URI, [`resolve`](../reference/data-import-export/uri.md) connects to the specified Deephaven instance, creates a [Barrage](../conceptual/what-is-barrage.md) subscription to the remote table, and waits for the initial snapshot of its data before it returns. The table it returns is always a [refreshing table](../conceptual/table-types.md), which means it can receive updates, even when the remote table is static. It receives the remote table's later changes in [update cycles](../conceptual/table-update-model.md).

```groovy skip-test
remoteTable = resolve("dh+plain://hostname/scope/someTable")
println remoteTable.size()  // Current size of the resolved table; changes as the remote table ticks
```

To wait for the next change from a ticking remote table, use [`awaitUpdate`](../reference/table-operations/table-listeners/await-update.md). Pass a timeout in case the remote table is static, because a static table never sends another update:

```groovy skip-test
remoteTable.awaitUpdate(10_000)  // Waits up to 10 seconds for the next update
```

If the network connection behind a resolved table drops, the subscription ends with an error and the resolved table stops updating. It doesn't reconnect, so call [`resolve`](../reference/data-import-export/uri.md) again to get a new subscription.

To work with a fixed copy of a resolved table, for example when every operation in an analysis must see the same data, use [`snapshot`](../reference/table-operations/snapshot/snapshot.md). The copy is static and holds the table state from the moment you take it, while the resolved table keeps receiving updates.

Let's explore this with two complete examples: sharing a table between two Docker containers on one machine, and sharing a table across a network.

## Share tables locally

This first example uses two Docker containers that run Deephaven with Groovy, each exposed on a different host port.

### Docker Compose

Spinning up multiple Deephaven instances from Docker takes only a short Compose file. This example creates two containers. Both listen on port `10000` inside their containers. The host reaches `table-producer` on port `10000` and `table-consumer` on port `9999`. The `docker-compose.yml` file looks like this:

```yml
services:
  table-producer:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "10000:10000"
    environment:
      - START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
  table-consumer:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "9999:10000"
    environment:
      - START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

After a `docker compose pull` and `docker compose up -d`, both instances are up and running.

> [!NOTE]
> `table-producer` needs [anonymous authentication](./authentication/auth-anon.md) because `table-consumer` connects to it anonymously when it resolves the URI, and URI resolution doesn't support [pre-shared key authentication](./authentication/auth-psk.md). The setting on `table-consumer` is there only so that you can open its IDE without a pre-shared key.
>
> To keep pre-shared key logins for the producer's own users, list both handlers: `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler,io.deephaven.authentication.psk.PskAuthenticationHandler`.

### Without Docker Compose

If you're not using Docker Compose, you can create a Docker network manually to enable container name resolution:

```bash
docker network create dh-net

docker run -d --network dh-net --name table-producer -p 10000:10000 \
  --env START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler \
  ghcr.io/deephaven/server-slim:latest

docker run -d --network dh-net --name table-consumer -p 9999:10000 \
  --env START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler \
  ghcr.io/deephaven/server-slim:latest
```

### Create a table on the producer

Open the `table-producer` IDE at `http://localhost:10000/ide` and create a real-time table with [`timeTable`](../reference/table-operations/create/timeTable.md):

```groovy ticking-table order=null
myTable = timeTable("PT1S").update("X = 0.1 * i", "Y = sin(X)")
```

### Resolve the table on the consumer

Open the `table-consumer` IDE at `http://localhost:9999/ide`. To get the producer's table, the consumer needs its URI. The URI consists of the scheme, the producer's Docker container name (its service name when you use Docker Compose), the `scope` namespace, and the table name. In this example, the URI is `dh+plain://table-producer/scope/myTable`.

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

resolvedTable = resolve("dh+plain://table-producer/scope/myTable")
```

![The resolved table ticking in the table-consumer IDE (shown here from the Python version of this example)](../assets/how-to/resolved-table-uri.gif)

In the `table-consumer` IDE, `resolvedTable` now ticks as `myTable` on the producer adds rows.

> [!NOTE]
> Container names resolve only within the same Docker network. The URI above omits the port because `table-producer` listens on port `10000` inside its container, which is the `dh+plain` default. If you include a port when resolving by container name, use the port inside the container, not the port it is mapped to on the host. For example, another container reaches `table-consumer` at `dh+plain://table-consumer:10000/...`, not `:9999`.

## Share tables across a network

You can also share tables across networks, public or private. This works the same way as [sharing tables locally](#share-tables-locally), where the consumer passes the producer's URI to [`resolve`](../reference/data-import-export/uri.md). In the URI, replace the container name with the hostname or IP address of the instance that produces the table. Add the port only when that instance doesn't listen on the scheme's default port, as described in [Deephaven URI structure](#deephaven-uri-structure).

### Create a table on your colleague's server

Suppose you're on a private network, and your colleague runs Deephaven on port `9876` on a machine with IP `192.168.5.1`. Their server must allow [anonymous authentication](./authentication/auth-anon.md), because your server connects to it anonymously when it resolves the URI. For example, they can start it with `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler`, as in the [Docker Compose](#docker-compose) example.

On that server, your colleague creates a table:

```groovy order=null
colleaguesTable = emptyTable(50).update("X = i", "Y = i % 2")
```

### Resolve your colleague's table

Once again, the URI consists of the scheme, the colleague's IP address and port, the `scope` namespace, and the table name:

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

myColleaguesTable = resolve("dh+plain://192.168.5.1:9876/scope/colleaguesTable")
```

If you know the hostname of your colleague's machine, you can use it in place of the IP address. If the machine is public, use its public hostname or IP address. Keep in mind that anyone who can reach a server with [anonymous authentication](./authentication/auth-anon.md) can connect to it, so expose such a server only on a network you trust, and use `dh://` so that the connection is encrypted.

## Performance considerations

When you share tables across instances with URIs, particularly over networks, consider these performance factors:

### Network impact

- **Latency**: Table access over a network introduces latency that varies based on network conditions. When an operation needs low latency, run the instances on the same machine or network.
- **Bandwidth**: Deephaven's [Barrage protocol](../conceptual/what-is-barrage.md) transmits only changes after the initial snapshot rather than full table refreshes, so bandwidth use depends mostly on the table characteristics below.
- **Connection reliability**: A resolved table doesn't reconnect after a dropped connection (see [Resolve a URI](#resolve-a-uri)). Co-locate the instances, or resolve over a reliable network link, when the consumer can't tolerate an interrupted subscription.

### Table characteristics

- **Full-table subscription**: `resolve` subscribes to every row and column of the remote table, and the consumer keeps its own copy of the table in memory.
- **Initial snapshot**: When you first resolve a URI, the remote server sends a snapshot of the current table state. Larger tables require more resources for this initial transfer.
- **Update frequency**: Tables that tick often send more data over the network after the initial snapshot, so they use more bandwidth.
- **Column types**: Columns that hold large values, such as long strings, arrays, or nested structures, send more data during the initial snapshot and each later update.

### Optimization strategies

- **Only share what's needed**: Filtering a resolved table on the consumer doesn't reduce what crosses the network, so share only the data a downstream consumer needs: apply [filters](./use-filters.md) at the source, [select only the necessary columns](./use-select-view-update.md), and [pre-aggregate](./dedicated-aggregations.md) large datasets to reduce the volume of transferred data.
- **Avoid repeated URI resolution**: Store resolved table references in variables rather than calling [`resolve`](../reference/data-import-export/uri.md) multiple times for the same URI. `resolve` reuses an existing connection when a URI names the same scheme, host, and port as an earlier one, but each call creates a new [Barrage](../conceptual/what-is-barrage.md) subscription to the remote table.

## Related documentation

- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`resolve`](../reference/data-import-export/uri.md)
- [Enterprise URIs](https://deephaven.io/enterprise/docs/deephaven-database/remote-tables-groovy/#uris)
- [Capture Python client tables](./capture-tables.md)
- [Application Mode](./application-mode.md)
- [URI cheat sheet](../reference/cheat-sheets/uri-cheat-sheet.md)
- [`io.deephaven.uri` Javadoc](https://deephaven.io/core/javadoc/io/deephaven/uri/package-summary.html)
