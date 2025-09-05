---
title: Use URIs to share tables
sidebar_label: URI
---

This guide will show you to use Deephaven's [URIs](/core/javadoc/io/deephaven/uri/package-summary.html) to share tables across instances and networks.

A URI, short for [Uniform Resource Identifier](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier), is a sequence of characters that identifies a resource on the web. Think of a URI as a generalization of a URL. A Deephaven URI identifies a table. By linking to a URI, you share your results with others without them needing to replicate your setup or know the details of your queries.

> [!NOTE]
> URIs can be used to share tables across Groovy and Python instances interchangably. For how to use URIs in Python, see [the equivalent guide](/core/docs/how-to-guides/use-uris).

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

Deephaven URIs use a similar syntax:

`dh+plain://<authority>/<path>`

- `dh+plain` is the scheme.
- `<authority>` is the authority, which will be either a Docker container name or hostname/IP.
- `<path>` is the path to a table, which is generally `scope/<table_name>`.

Let's explore this with a couple of examples.

## Share tables locally

For this first example, we will spin up two Docker containers that run Deephaven with Groovy on different ports.

### Docker compose

Spinning up multiple Deephaven instances from Docker is simple. In order to do so, we will create two containers, which we will name `table-producer`, which runs on port `10000`, and `table-consumer`, which runs on port `9999`. Our `docker-compose.yml` file will look like this:

```
version: '3'

services:
  table-producer:
    image: ghcr.io/deephaven/server-slim:0.36.0
    ports:
      - '10000:10000'
  table-consumer:
    image: ghcr.io/deephaven/server-slim:0.36.0
    ports:
      - '9999:10000'
```

After a `docker compose pull` and `docker compose up --build -d`, both instances are up and running.

### Create a table

In the `table-producer` container running on port `10000`, we create a real-time table with [timeTable](../reference/table-operations/create/timeTable.md).

```groovy order=null
myTable = timeTable("PT00:00:01").update("X = 0.1 * i", "Y = sin(X)")
```

### Get the table via a URI

In order to acquire a table from some producer, the consumer needs its URI. The URI consists of the scheme, Docker container, and table name. In the case of this example, that URI is `dh+plain://table-producer/scope/myTable`.

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

resolvedTable = resolve("dh+plain://table-producer/scope/myTable")
```

![The above ticking table](../assets/how-to/resolved-table-uri.gif)

By resolving the URI, we acquire `myTable` from the `table-producer` container using the syntax given above.

## Share tables across a network

Tables can also be shared across networks, public or private. Just like the previous example of sharing across a machine, this works in the same way. Rather than the container name, you only need the hostname/IP and port of the instance producing the table.

> [!NOTE]
> When sharing tables across a network, whether public or private, you do _not_ need the port if Deephaven is being run on the default Deephaven port `10000`. In all other cases, you _must_ provide the port on which the table can be found.

### Create a table

Let's assume we're on a private network, and our colleague is running Deephaven on port `9876` on a machine with IP `192.168.5.1`. From there, they create a table:

```groovy order=null
colleaguesTable = emptyTable(50).update("X = i", "Y = i % 2")
```

### Get the table via a URI

Once again, we need only the IP, port, and table name to resolve its URI.

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

myColleaguesTable = resolve("dh+plain://192.168.5.1:9876/scope/colleaguesTable")
```

If we have the hostname of our colleague's machine, that can be used in place of the IP address.

## Share tables publicly

If the machine on which a table exists is public, then consuming that table is done the same way as if it were a private network. All that's needed is the hostname/IP and table name.

<!-- TODO:

## Paths

Tables can exist in different scopes, such as in app mode and others. When this is the case, the scope changes.

Update this section. I need to learn more about different scopes. -->

## Related documentation

- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
