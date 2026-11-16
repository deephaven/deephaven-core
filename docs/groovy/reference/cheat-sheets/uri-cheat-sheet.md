---
title: URI cheat sheet
---

Deephaven servers can share tables through [URIs](../../how-to-guides/use-uris.md). [`resolve`](../data-import-export/uri.md) turns a URI into a live table that updates as the remote table changes.

> [!IMPORTANT]
> [`resolve`](../data-import-export/uri.md) connects to the remote server with [anonymous authentication](../../how-to-guides/authentication/auth-anon.md). It does not support [pre-shared key (PSK) authentication](../../how-to-guides/authentication/auth-psk.md), which Deephaven servers use by default. The remote server must therefore allow anonymous authentication, or resolution fails. GitHub issues [#5383](https://github.com/deephaven/deephaven-core/issues/5383) and [#3421](https://github.com/deephaven/deephaven-core/issues/3421) track this limitation.

## Syntax

```syntax
dh+plain://<host>[:<port>]/scope/<tableName>
dh://<host>[:<port>]/scope/<tableName>
dh+plain://<host>[:<port>]/app/<appId>/field/<fieldName>
dh://<host>[:<port>]/app/<appId>/field/<fieldName>
```

- `dh+plain` connects without encryption.
- `dh` connects with TLS.
- `<host>` is the remote server's Docker container name, Docker Compose service name, hostname, or IP address.
- `<port>` is optional. When the URI omits it, the connection uses port `10000` for `dh+plain` and port `443` for `dh`.
- `scope/<tableName>` refers to a table in the remote server's [query scope](../../how-to-guides/query-scope.md), where `<tableName>` is the name of the variable that holds it.
- `app/<appId>/field/<fieldName>` refers to a table that an [Application Mode](../../how-to-guides/application-mode.md) application exports as a field. `<appId>` is the application's ID, and `<fieldName>` is the field's name.
- Without a host (`dh:///scope/<tableName>` or `dh:///app/<appId>/field/<fieldName>`), [`resolve`](../data-import-export/uri.md) reads from the resolving server's own query scope or applications.

## Share tables between Docker containers

To resolve a table from a server in another Docker container on the same user-defined network:

- Use the remote server's container name as the host.
- With Docker Compose, use the remote server's service name. Compose creates the shared network.
- If you include a port, use the port the remote server listens on inside its container, not the port mapped to it on the host.

To put the containers on one network without Docker Compose, see [Without Docker Compose](../../how-to-guides/use-uris.md#without-docker-compose).

This example resolves `myTable` from the server in the container named `table-producer`:

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

containerName = "table-producer"

resolvedTable = resolve("dh+plain://${containerName}/scope/myTable")
```

## Share tables across a network

To resolve a table from a server on another machine:

- Use the remote server's IP address or hostname as the host.
- Add the port when the remote server does not listen on the scheme's [default port](#syntax).
- Use `dh` to encrypt the connection.
- Expose the remote server only on a network you trust. It must allow anonymous authentication, so anyone who can reach it can connect.

This example resolves `myTable` by IP address and port, by hostname and port, and over TLS on the default port `443`:

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

ipAddress = "192.168.5.1"
hostname = "my-host"
port = 9876
tlsHostname = "my-tls-host"

tableFromIp = resolve("dh+plain://${ipAddress}:${port}/scope/myTable")
tableFromHostname = resolve("dh+plain://${hostname}:${port}/scope/myTable")
tableOverTls = resolve("dh://${tlsHostname}/scope/myTable")
```

## Resolve an Application Mode field

To resolve a table that an [Application Mode](../../how-to-guides/application-mode.md) application exports as a field, use the `app/<appId>/field/<fieldName>` path in place of `scope/<tableName>`. This example resolves the field `myField` that the application `myApp` exports on the server in the container named `table-producer`:

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

appField = resolve("dh+plain://table-producer/app/myApp/field/myField")
```

## Resolve CSV and Parquet files

[`resolve`](../data-import-export/uri.md) also reads [CSV](../../how-to-guides/data-import-export/csv-import.md) and [Parquet](../../how-to-guides/data-import-export/parquet-import.md) files from the resolving server's file system, and CSV files over HTTP or HTTPS:

- `csv:///data/file.csv` reads a CSV file.
- `csv+https://host/path/file.csv` reads a CSV file over HTTPS (`csv+http` for HTTP).
- `parquet:///data/file.parquet` reads a Parquet file or a directory of Parquet files.

## Related documentation

- [How to use URIs to share tables](../../how-to-guides/use-uris.md)
- [Query scope](../../how-to-guides/query-scope.md)
- [Application Mode](../../how-to-guides/application-mode.md)
- [Anonymous authentication](../../how-to-guides/authentication/auth-anon.md)
- [Pre-shared key authentication](../../how-to-guides/authentication/auth-psk.md)
- [Read CSV or other delimited files](../../how-to-guides/data-import-export/csv-import.md)
- [Read Parquet files](../../how-to-guides/data-import-export/parquet-import.md)
- [`resolve`](../data-import-export/uri.md)
- [Javadoc](/core/javadoc/io/deephaven/uri/ResolveTools.html)
