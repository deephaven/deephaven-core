---
id: use-uris
title: Use URIs to share tables
sidebar_label: URI
---

This guide will show you to use Deephaven's [URIs](https://deephaven.io/core/pydoc/code/deephaven.uri.html#module-deephaven.uri) to share tables across instances and networks.

A URI, short for [Uniform Resource Identifier](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier), is a sequence of characters that identifies a resource on the web. Think of a URI as a generalization of a URL. A Deephaven URI specifically identifies a table in a Deephaven instance. By using URIs, you can share your tables with others without them needing to replicate your setup or know the details of your queries.

:::note
URIs can be used to share tables across Groovy and Python instances interchangeably. For how to use URIs in Groovy, see [the equivalent guide](/core/groovy/docs/how-to-guides/use-uris).
:::

## Why use URIs?

Deephaven URIs provide several key benefits:

- **Canonicalized resource identification**: Access resources through a standardized string format that works across client and server contexts.
- **Simplified data sharing**: Share tables between different Deephaven instances without duplicating data or queries.
- **Distributed computing**: Build systems where processing is distributed across multiple Deephaven nodes.
- **Real-time access**: Access live, updating tables from remote sources that reflect the latest data.
- **Resource abstraction**: Reference remote tables and application fields using a consistent pattern regardless of location.
- **Cross-language compatibility**: Access the same data from both Python and Groovy scripts.
- **Environmental isolation**: Access data across different containers, servers, or networks.

By using URIs, you enable others to directly access your tables without needing to replicate your data pipeline, understand your query logic, or maintain duplicate datasets. This is particularly valuable in collaborative environments and distributed systems.

:::note
URI and Shared Tickets are two different ways to pull tables. Both work on static or dynamic tables. URI pulls tables already on the server via a URL-like string. Shared Tickets let you pull tables you create or access via the Python Client. Learn more about using Shared Tickets with Deephaven in the [Shared Tickets guide](../how-to-guides/capture-tables.md).
:::

## Syntax

URLs (Uniform Resource Locators) are a common example of URIs. Their syntax typically looks something like this:

`https://deephaven.io/core/docs`

The above URL can be broken down as follows:

- Scheme
  - The scheme, in this case, is `https`, which is short for `hypertext transfer protocol secure`.
- Authority
  - The authority, in this case, is `deephaven.io`. It is the host name of the web resource.
- Path
  - The path, in this case, is `/core/docs`. It is a path on the authority.

### Deephaven URI structure

Deephaven URIs use a similar syntax:

`dh+plain://<authority>[:<port>]/<scope>/<table_name>`

The components are:

- **`dh+plain`** is the scheme.
  - This is **always** `dh+plain` for all Deephaven table URIs.
  - The scheme identifies the protocol for accessing Deephaven tables.
  - This has no relation to application types (script, static, dynamic, qst) which are configuration options in [Application Mode](./application-mode.md).
- **`<authority>`** is the authority, which will be either:
  - A Docker container name (for local container-to-container communication).
  - A hostname/IP address (for network communication).
- **`<port>`** is optional and only needed when:
  - The Deephaven instance is running on a non-default port (something other than 10000).
  - You're connecting across a network to a specific port.
- **`<scope>`** identifies the namespace where the table exists (typically `scope` for tables in the default namespace).
- **`<table_name>`** is the exact name of the table you want to access.

### Resolving URIs in your code

To access a table via its URI, use the `resolve()` function from the `deephaven.uri` module:

```python skip-test
from deephaven.uri import resolve

# Basic usage
table = resolve("dh+plain://hostname/scope/table_name")

# With explicit port
table = resolve("dh+plain://hostname:9876/scope/table_name")
```

The `resolve()` function connects to the specified Deephaven instance, retrieves the table, and returns it as a local reference that you can use in your code.

## Share tables locally

For this first example, we will spin up two Docker containers that run Deephaven with Python on different ports.

### Docker compose

Spinning up multiple Deephaven instances from Docker is simple. In order to do so, we will create two containers, which we will name `table-producer`, which runs on port `10000`, and `table-consumer`, which runs on port `9999`. Our `docker-compose.yml` file will look like this:

```yml
version: "3"

services:
  table-producer:
    image: ghcr.io/deephaven/server:0.36.0
    ports:
      - "10000:10000"
  table-consumer:
    image: ghcr.io/deephaven/server:0.36.0
    ports:
      - "9999:10000"
```

After a `docker compose pull` and `docker compose up --build -d`, both instances are up and running.

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

## Table scopes and paths

In Deephaven, tables exist within specific scopes (namespaces) that determine how they're accessed:

- `scope` - The default scope for tables created in the main session.
- `app/<app_name>` - For tables created within a specific [Application Mode](./application-mode.md) namespace.
- `session/<session_id>` - For tables associated with a specific user session.

### URI format by scope type

```
# Default scope table (most common)
dh+plain://hostname/scope/table_name

# App scope table
dh+plain://hostname/app/my_application/table_name

# Session scope table
dh+plain://hostname/session/12345/table_name
```

### Scope resolution rules

1. When no explicit scope is provided, Deephaven first looks in the current scope.
2. If the table isn't found there, it then checks the default scope.
3. Tables in app or session scopes must always be accessed using their full path.

:::note
When using URIs across different scopes, you must have appropriate permissions to access the tables in those scopes.
:::

## Share tables across a network

Tables can also be shared across networks, public or private. Just like the previous example of sharing across a machine, this works in the same way. Rather than the container name, you only need the hostname/IP and port of the instance producing the table.

:::note

- When sharing tables across a network, you do **not** need to specify the port if Deephaven is running on the default port `10000`.
- You **must** specify the port in the URI when:
  - The remote Deephaven instance runs on a non-default port (not 10000).
  - You're connecting to a custom port forwarding configuration.

Example format with port: `dh+plain://hostname:9876/scope/table_name`
Example format without port (default 10000): `dh+plain://hostname/scope/table_name`
:::

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
- **Bandwidth**: Large tables or tables with frequent updates require more bandwidth. Network throughput can become a bottleneck for data-intensive operations.
- **Connection reliability**: Unstable network connections can affect the reliability of table access. Implement appropriate error handling for network disruptions.

### Table characteristics

- **Table size**: Larger tables require more resources to transfer and update. The overhead increases with row count and column width.
- **Update frequency**: Tables with high update frequencies consume more network resources. Each update must be synchronized across instances.
- **Column types**: Tables with complex data types like large strings or nested structures may have higher overhead when shared across instances.

### Optimization strategies

- **Filter at source**: Apply filters on the source table before resolving it in the consumer instance to reduce data transfer volume.
- **Project necessary columns**: Only include columns that are actually needed by the consumer to minimize network payload size.
- **Pre-aggregate when possible**: For large tables, use aggregations at the source to reduce data volume while preserving analytical value.
- **Minimize resolution frequency**: For latency-sensitive applications, cache resolved table references rather than repeatedly resolving the same URI.
- **Use table snapshots**: When point-in-time consistency is required, use table snapshots instead of live updating tables.
- **Organize with appropriate scopes**: Structure your tables in logical scopes to improve discovery and resolution efficiency.
- **Monitor performance**: Use Deephaven's performance tables to identify bottlenecks in URI operations.

### Caching behavior

When a table is accessed via URI, it is cached on the consumer instance. This means:

- Subsequent accesses to the same URI will use the cached table reference.
- Updates to the source table will propagate to all consumers automatically.
- If the source table becomes unavailable, consumers will continue to see the last known state until reconnection is possible.

## Related documentation

- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [Capture Python client tables](./capture-tables.md)
- [Application Mode](./application-mode.md)
- [Pydoc](https://deephaven.io/core/pydoc/code/deephaven.uri.html)
