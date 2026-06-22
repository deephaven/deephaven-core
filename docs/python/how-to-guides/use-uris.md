---
title: Use URIs to share tables
sidebar_label: URI
---

This guide shows you how to use Deephaven's [URIs](https://docs.deephaven.io/core/pydoc/code/deephaven.uri.html#module-deephaven.uri) to share tables across server instances and networks.

A URI, short for [Uniform Resource Identifier](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier), is a sequence of characters that identifies a resource on the web. Think of a URI as a generalization of a URL. A Deephaven URI identifies a table on a server instance. By linking to a URI, you can access and work with tables from other Deephaven server instances without needing to replicate the data or queries that created them.

> [!NOTE]
> URIs can be used to share tables across Groovy and Python instances interchangeably. For how to use URIs in Groovy, see [the equivalent guide](/core/groovy/docs/how-to-guides/use-uris).

> [!NOTE]
> URI and Shared Tickets are two different ways to pull tables. Both work on static or dynamic tables. URI pulls tables already on the server via a URL-like string. Shared Tickets let you pull tables you create or access via the Python Client. Learn more about using Shared Tickets with Deephaven in the [Shared Tickets guide](./capture-tables.md).

> [!IMPORTANT]
> URI resolution in Deephaven Community (Core) requires **anonymous authentication**. PSK (pre-shared key) authentication is not currently supported — attempting to resolve a URI when PSK is enabled will fail. This is a known limitation tracked in GitHub issues [#5383](https://github.com/deephaven/deephaven-core/issues/5383) and [#3421](https://github.com/deephaven/deephaven-core/issues/3421).
>
> If you found this page while looking for `ui.resolve`, note that the [Deephaven UI URI component](https://deephaven.io/core/ui/docs/components/uri/) is a separate, **Deephaven Enterprise**-only feature for Persistent Queries (PQ). It is not the same as `deephaven.uri.resolve` documented here.

## Why use URIs?

Deephaven URIs provide several key benefits:

- **Canonicalized resource identification**: Access resources through a standardized string format that works across server instances.
- **Simplified data sharing**: Share tables between different Deephaven instances without duplicating data or queries.
- **Distributed computing**: Build systems where processing is distributed across multiple Deephaven nodes.
- **Real-time access**: Access live, updating tables from remote sources that reflect the latest data.
- **Resource abstraction**: Reference remote tables and application fields using a consistent pattern regardless of location.
- **Cross-language compatibility**: Access the same data from both Python and Groovy scripts.
- **Environmental isolation**: Access data across different containers, servers, or networks.

By using URIs, you enable others to directly access your tables without needing to replicate your data pipeline, understand your query logic, or maintain duplicate datasets. This is particularly valuable in collaborative environments and distributed systems.

## Syntax

URLs (Uniform Resource Locators) are a common example of URIs. Their syntax typically looks something like this:

`https://deephaven.io/core/docs`

The above URL can be broken down as follows:

- Scheme
  - The scheme, in this case, is `https`, which is short for "hypertext transfer protocol secure".
- Authority
  - The authority, in this case, is `deephaven.io`. It is the host name of the web resource.
- Path
  - The path, in this case, is `/core/docs`. It is a path on the authority.

### Deephaven URI structure

Deephaven URIs use a similar syntax:

```
dh://<authority>[:<port>]/<scope>/<resource_name>
dh+plain://<authority>[:<port>]/<scope>/<resource_name>
```

The components are:

- **`dh+plain`** or **`dh`** is the scheme.
  - `dh://` indicates a secure connection (TLS/SSL).
  - `dh+plain://` indicates an insecure connection (no encryption).
  - The scheme identifies the protocol for accessing Deephaven resources.
  - All Deephaven URIs use one of these schemes, regardless of the application type (script, static, dynamic, qst) configured in [Application Mode](./application-mode.md).
- **`<authority>`** is the authority, which will be either:
  - A Docker container name (for container-to-container communication within the same Docker network).
  - A hostname/IP address (for network communication).
- **`<port>`** is optional and only needed when:
  - The Deephaven instance is running on a non-default port (something other than 10000).
  - You're connecting across a network to a specific port.
- **`<scope>`** identifies the namespace where the resource exists. This is typically `scope` for variables created in interactive console sessions, or `app/<app_name>/field` for resources exported from Application Mode applications.
- **`<resource_name>`** is the exact name of the table or resource you want to access.

### Resolving URIs in your code

To access a table via its URI, use the [`resolve`](../reference/data-import-export/uri.md#parameters) function from the `deephaven.uri` module:

```python skip-test
from deephaven.uri import resolve

# Basic usage
table = resolve("dh+plain://hostname/scope/table_name")

# With explicit port
table = resolve("dh+plain://hostname:9876/scope/table_name")
```

The `resolve` function connects to the specified Deephaven instance, retrieves the table, and returns it as a local reference that you can use in your code.

> [!CAUTION]
> When you resolve a URI, the first update cycle of the subscribed table is empty. If you run `resolve` and immediately operate on the table data in the same execution, you may see an empty table. To work with the actual data, either:
>
> - **In a notebook**: Run the `resolve` call in a separate execution before operating on the table.
> - **In a script**: Use [`await_update`](../reference/table-operations/table-listeners/await-update.md) to wait for the table to populate before accessing its data.

For example, the following code often prints 0:

```python skip-test
remote_table = resolve("dh+plain://hostname/scope/some_table")
print(remote_table.size)  # Often prints 0 before the table populates
```

To get the actual table size, use [`await_update`](../reference/table-operations/table-listeners/await-update.md) to wait for the table to populate:

```python skip-test
remote_table = resolve("dh+plain://hostname/scope/some_table")
remote_table.await_update()
print(remote_table.size)  # Prints the actual size
```

Let's explore this with a couple of examples.

## Share tables locally

For this first example, we will spin up two Docker containers that run Deephaven with Python on different ports.

### Docker compose

Spinning up multiple Deephaven instances from Docker is simple. In order to do so, we will create two containers, which we will name `table-producer`, which runs on port `10000`, and `table-consumer`, which runs on port `9999`. Our `docker-compose.yml` file will look like this:

```yml
services:
  table-producer:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "10000:10000"
    environment:
      - START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
  table-consumer:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "9999:10000"
    environment:
      - START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

After a `docker compose pull` and `docker compose up --build -d`, both instances are up and running.

> [!NOTE]
> Anonymous authentication is explicitly configured because URI resolution requires it. PSK (pre-shared key) authentication is not supported for URI resolution.

### Create a table

In the `table-producer` container running on port `10000`, we create a real-time table with [`time_table`](../reference/table-operations/create/timeTable.md).

```python order=null
from deephaven import time_table

my_table = time_table("PT1S").update(["X = 0.1 * i", "Y = sin(X)"])
```

### Get the table via a URI

In order to acquire a table from some producer, the consumer needs its URI. The URI consists of the scheme, Docker container, and table name. In the case of this example, that URI is `dh+plain://table-producer/scope/my_table`.

```python skip-test
from deephaven.uri import resolve

resolved_table = resolve("dh+plain://table-producer/scope/my_table")
```

![The above ticking table](../assets/how-to/resolved-table-uri.gif)

By resolving the URI, we acquire `my_table` from the `table-producer` container using the syntax given above.

### Alternative: Docker network without compose

If you're not using Docker Compose, you can create a Docker network manually to enable container name resolution:

```bash
docker network create dh-net

docker run -d --network dh-net --name table-producer -p 10000:10000 \
  --env START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler \
  ghcr.io/deephaven/server:latest

docker run -d --network dh-net --name table-consumer -p 9999:10000 \
  --env START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler \
  ghcr.io/deephaven/server:latest
```

> [!NOTE]
> Container names only resolve within the same Docker network. When resolving by container name, use the container port (10000), not the host port:
>
> ```python skip-test
> # Correct: use container port
> table = resolve("dh+plain://table-producer:10000/scope/my_table")
> ```

## Resource scopes and paths

A **scope** in a Deephaven URI is a namespace that identifies where a resource exists within a Deephaven server instance. Think of scopes as organizational containers that prevent naming conflicts and provide context for how resources were created.

Deephaven uses scopes to separate resources based on their origin and purpose:

### Query scope (`scope`)

The query scope contains variables created in interactive console sessions - when you create tables, variables, or other objects directly in the Deephaven IDE console or through client connections.

```python
# This creates a table in the query scope
from deephaven import empty_table

my_table = empty_table(100).update(["X = i", "Y = i * 2"])
# Accessible via: dh://hostname/scope/my_table
```

### Application scope (`app/<app_name>/field`)

The application scope contains fields exported from [Application Mode](./application-mode.md) applications. These are pre-configured resources that are available when the server starts, defined by application scripts.

```python syntax
# In an Application Mode script, this exports a field
# Accessible via: dh://hostname/app/trading_app/field/market_data
```

Scopes ensure that:

- **No naming conflicts**: A table named `trades` in the query scope is completely separate from a field named `trades` in an application scope.
- **Clear resource organization**: You know immediately whether a resource comes from interactive work or a pre-built application.
- **Proper access control**: Different scopes can have different permission models.

### URI format by scope type

```syntax
# Query scope variable (most common)
dh+plain://hostname/scope/table_name
dh://hostname/scope/table_name

# Application field
dh+plain://hostname/app/my_application/field/my_field
dh://hostname/app/my_application/field/my_field
```

> [!NOTE]
> When using URIs to access resources, you must have appropriate permissions to access the resources in those scopes.

## Share tables across a network

You can also share tables across networks, public or private. Just like the previous example of sharing across a machine, this works in the same way. Rather than the container name, you only need the hostname/IP and port of the instance producing the table.

> [!NOTE]
>
> - When sharing tables across a network, you do **not** need to specify the port if Deephaven is running on the default port `10000`.
> - You **must** specify the port in the URI when:
>   - The remote Deephaven instance runs on a non-default port (not 10000).
>   - You're connecting to a custom port forwarding configuration.
>
> Example format with port: `dh+plain://hostname:9876/scope/table_name`

### Create a table

Let's assume we're on a private network, and our colleague is running Deephaven on port `9876` on a machine with IP `192.168.5.1`. From there, they create a table:

```python order=null
from deephaven import empty_table

my_table = empty_table(50).update(["X = i", "Y = i % 2"])
```

### Get the table via a URI

Once again, we need only the IP, port, and table name to resolve its URI.

```python skip-test
from deephaven.uri import resolve

my_colleagues_table = resolve("dh+plain://192.168.5.1:9876/scope/my_table")
```

If we have the hostname of our colleague's machine, that can be used in place of the IP address.

## Share tables publicly

If the machine on which a table exists is public, then consuming that table is done the same way as if it were a private network. All that's needed is the hostname/IP and table name.

## Performance considerations

When using URIs to share tables across instances, particularly over networks, there are several performance factors to consider:

### Network impact

- **Latency**: Table access over a network introduces latency that varies based on network conditions. For operations requiring low latency, consider co-locating instances when possible.
- **Bandwidth**: The initial table snapshot and subsequent incremental updates consume bandwidth. Deephaven's [Barrage protocol](../conceptual/barrage-metrics.md) optimizes this by transmitting only changes rather than full table refreshes.
- **Connection reliability**: Unstable network connections can affect the reliability of table access. Implement appropriate error handling for network disruptions.

### Table characteristics

- **Initial snapshot**: When first resolving a URI, Deephaven sends a snapshot of the current table state. Larger tables require more resources for this initial transfer.
- **Update frequency**: Tables with high update frequencies generate more incremental updates over the network. Deephaven's Barrage protocol efficiently transmits only the changes (additions, removals, modifications).
- **Column types**: Tables with complex data types like large strings or nested structures may have higher overhead during the initial snapshot and subsequent updates.

### Optimization strategies

- **Only share what's needed**: Filter, aggregate, and limit the amount of data you're sharing to only what a downstream consumer actually needs. This includes applying filters at the source, projecting only necessary columns, and pre-aggregating large datasets to reduce the volume of transferred data.
- **Avoid repeated URI resolution**: Store resolved table references in variables rather than calling `resolve` multiple times for the same URI. Each call to `resolve` creates a new connection, so reuse the table reference when possible within your application.
- **Use appropriate data consistency models**: For analysis requiring consistent data across multiple operations, use table snapshots instead of live updating tables. Point-in-time consistency ensures all your data represents the same moment in time, preventing issues where some data updates mid-analysis while other data remains static. Snapshots freeze the table state at a specific moment, guaranteeing consistent results and reducing network overhead from continuous updates.

## Related documentation

- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [Capture Python client tables](./capture-tables.md)
- [Application Mode](./application-mode.md)
- [URI cheat sheet](../reference/cheat-sheets/uri-cheat-sheet.md)
- [Enterprise URIs](https://deephaven.io/enterprise/docs/deephaven-database/remote-tables-python/#uris)
- [Pydoc](https://deephaven.io/core/pydoc/code/deephaven.uri.html)
