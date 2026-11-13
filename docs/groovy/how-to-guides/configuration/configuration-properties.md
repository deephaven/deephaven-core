---
title: Configuration properties reference
---

This guide shows how to set Deephaven Community configuration properties for Docker and the production application, then lists common properties by area.

## How to set properties

Set a property in a configuration file, or pass it as a `-D<property>=<value>` JVM argument.

- A `-D` argument overrides the same property in the configuration file.
- If the file marks that property [`final`](./config-file.md#how-settings-combine), the `-D` argument causes a configuration error instead.

Use `-D` arguments for quick testing and a configuration file for production. The subsections below show where the file goes and how to pass `-D` arguments for each way of running the server.

### Configuration file

Create a `deephaven.prop` file that sets your properties, and save it in the config directory, which defaults to the location in [OS-dependent directories](#os-dependent-directories). For example:

```properties
includefiles=dh-defaults.prop

# Override default properties
deephaven.console.type=groovy
http.session.durationMs=3600000
AuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

Keep the `includefiles=dh-defaults.prop` line first. It loads the built-in defaults that the tables on this page list, and the server doesn't start without some of them. [Deephaven configuration files](./config-file.md) describes how the server finds and layers configuration files.

### Docker application

To configure the [Docker application](./docker-application.md), mount your configuration file at `/opt/deephaven/config/deephaven.prop`. `/opt/deephaven/config` is the config directory that [Docker image defaults](#docker-image-defaults) lists. Set JVM options such as [heap size](../heap-size.md) in the `START_OPTS` environment variable, which the server's start script passes to the JVM as arguments:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "10000:10000"
    volumes:
      - ./deephaven.prop:/opt/deephaven/config/deephaven.prop:ro
    environment:
      - START_OPTS=-Xmx4g
```

The `server-slim` image doesn't include Python. Its own `deephaven.prop` sets `deephaven.console.type=groovy`, but your mounted file replaces it. Set `deephaven.console.type=groovy` in your file, as the [configuration file example](#configuration-file) does. Otherwise, the server tries to start a Python console and fails.

For quick testing or a small number of properties, add `-D` arguments to `START_OPTS` as well. Each `-D` argument sets one property:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "10000:10000"
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

Don't change `http.port` in a Docker container, and set it back to `10000` if you turn on [SSL](#ssltls-configuration). The server listens on port `10000` inside the container, and the Docker health check connects to that port, so Docker reports a container that listens on another port as unhealthy. To use a different host port, change only the host side of the `ports` mapping, as the [complete example](#complete-example) does.

### Production application

The [production application](./configure-production-application.md) is the Deephaven server distributed as a tar archive and started with `./bin/start`. Set its properties in a configuration file in the config directory, which defaults to the location in [OS-dependent directories](#os-dependent-directories). Set JVM options, such as [heap size](../heap-size.md), in `START_OPTS`. As with Docker, `-D` arguments in `START_OPTS` set individual properties for quick testing.

The production application starts a Python console by default. Set `deephaven.console.type=groovy` in your configuration file, as the [configuration file example](#configuration-file) does, or with `-D` in `START_OPTS`:

```bash
export START_OPTS="-Xmx4g -Ddeephaven.console.type=groovy -Dhttp.session.durationMs=3600000"
./bin/start
```

### JVM arguments

Pass these common JVM arguments in `START_OPTS`. For more about heap size, see [Configure and adjust heap size](../heap-size.md).

| Argument | Description       | Example  |
| -------- | ----------------- | -------- |
| `-Xmx`   | Maximum heap size | `-Xmx4g` |
| `-Xms`   | Initial heap size | `-Xms2g` |

## Bootstrap configuration

Deephaven reads bootstrap configuration at startup, before it reads any configuration file, so a configuration file can't set it. Set each value with its environment variable or as a `-D` JVM argument (a system property). If both are set, the system property wins.

| Setting          | Environment variable    | System property         | Description                                                      | Default      |
| ---------------- | ----------------------- | ----------------------- | ---------------------------------------------------------------- | ------------ |
| Application name | `DEEPHAVEN_APPLICATION` | `deephaven.application` | Name used in the default data, cache, and config directory paths | `deephaven`  |
| Data directory   | `DEEPHAVEN_DATA_DIR`    | `deephaven.dataDir`     | Directory where users and applications read and write data       | OS-dependent |
| Cache directory  | `DEEPHAVEN_CACHE_DIR`   | `deephaven.cacheDir`    | Directory where the server reads and writes cache data           | OS-dependent |
| Config directory | `DEEPHAVEN_CONFIG_DIR`  | `deephaven.configDir`   | Directory where the server reads configuration files             | OS-dependent |
| Quiet flag       | `DEEPHAVEN_QUIET`       | `deephaven.quiet`       | Suppress bootstrap logging                                       | `false`      |

### OS-dependent directories

When neither its environment variable nor its system property sets the data, cache, or config directory, Deephaven uses the default location for the operating system. In these paths, `<application>` is the application name from the table above, which defaults to `deephaven`.

| OS    | Data directory                                                            | Cache directory                                              | Config directory                                                          |
| ----- | ------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------- |
| Linux | `$HOME/.local/share/<application>/`                                       | `$HOME/.cache/<application>/`                                | `$HOME/.config/<application>/`                                            |
| macOS | `$HOME/Library/Application Support/io.Deephaven-Data-Labs.<application>/` | `$HOME/Library/Caches/io.Deephaven-Data-Labs.<application>/` | `$HOME/Library/Application Support/io.Deephaven-Data-Labs.<application>/` |

On Linux, if `XDG_DATA_HOME`, `XDG_CACHE_HOME`, or `XDG_CONFIG_HOME` is set, Deephaven uses that directory in place of `$HOME/.local/share`, `$HOME/.cache`, or `$HOME/.config`.

### Docker image defaults

Docker images set the following environment variables by default. The first three set the bootstrap directories above:

- `DEEPHAVEN_DATA_DIR=/data`
- `DEEPHAVEN_CACHE_DIR=/cache`
- `DEEPHAVEN_CONFIG_DIR=/opt/deephaven/config`
- `EXTRA_CLASSPATH=/apps/libs/*`

`EXTRA_CLASSPATH` adds the JAR files in `/apps/libs` to the server's classpath.

## Server configuration

These properties control the core server behavior.

| Property                     | Description                                                                                                                       | Default                       |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `http.host`                  | Network interface to bind to (IP address or hostname)                                                                             | All interfaces                |
| `http.port`                  | Port the server listens on. Defaults to `443` when `ssl.identity.type` is set, which turns on [SSL](#ssltls-configuration).       | `10000`                       |
| `http.targetUrl`             | User-accessible target URL (useful behind proxies)                                                                                | Auto-computed from host/port  |
| `http.session.durationMs`    | Session duration in milliseconds                                                                                                  | `300000` (5 minutes)          |
| `storage.path`               | Root directory for the web UI's notebooks and layouts                                                                             | `<deephaven.dataDir>/storage` |
| `deephaven.console.type`     | Script language for the server's console session: `python` or `groovy`. The language must be installed.                           | `python`                      |
| `grpc.maxInboundMessageSize` | Maximum size in bytes of a gRPC message the server accepts from a client                                                          | `104857600` (100 MiB)         |
| `scheduler.poolSize`         | Size of the scheduler thread pool                                                                                                 | `4`                           |
| `proxy.hint`                 | Hint that the server runs behind a proxy. When `true`, `http.websockets` defaults to `none` and `http.http1` defaults to `false`. | Not set                       |
| `shutdown.timeoutMs`         | Milliseconds to wait for graceful shutdown                                                                                        | `10000`                       |

### Session initialization scripts

The server doesn't read a configuration property for Groovy init scripts. To run a Groovy script when the server starts, use a [script application](../application-mode.md#script-application-example).

## Authentication configuration

These properties control how users authenticate to the server.

| Property                                   | Description                                                                                                      | Default                                                    |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `AuthHandlers`                             | Comma-separated list of authentication handler class names                                                       | `io.deephaven.authentication.psk.PskAuthenticationHandler` |
| `authentication.psk`                       | Pre-shared key value for [PSK authentication](../authentication/auth-psk.md)                                     | Randomly generated key                                     |
| `authentication.anonymous.warn`            | Log a warning when [anonymous authentication](../authentication/auth-anon.md) is enabled                         | `true`                                                     |
| `authentication.client.configuration.list` | Comma-separated list of properties the server sends to clients before they log in. See the note below the table. | `AuthHandlers`                                             |

The start script of the [production application](#production-application), which the Docker images also use, already sets `authentication.client.configuration.list` with `-D`, so a value in `deephaven.prop` has no effect there. Set it with `-D` in `START_OPTS` instead.

### Available authentication handlers

The server includes these handlers:

| Handler class                                              | Description                                                                    |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `io.deephaven.authentication.psk.PskAuthenticationHandler` | [Pre-shared key (PSK) authentication](../authentication/auth-psk.md) (default) |
| `io.deephaven.auth.AnonymousAuthenticationHandler`         | [No authentication required](../authentication/auth-anon.md)                   |

Other authentication handlers require adding their provider JAR to the server's classpath (for example, `/apps/libs` in the Docker images). Each guide shows how:

| Handler class                                                   | Description                                                                       |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `io.deephaven.authentication.mtls.MTlsAuthenticationHandler`    | [Mutual TLS (mTLS) authentication](../authentication/auth-mtls.md)                |
| `io.deephaven.authentication.oidc.OidcAuthenticationHandler`    | [Keycloak/OIDC authentication](../authentication/auth-keycloak.md)                |
| `io.deephaven.authentication.sql.BasicSqlAuthenticationHandler` | [SQL-backed username/password authentication](../authentication/auth-uname-pw.md) |

## HTTP server configuration

These properties configure the server's HTTP, HTTP/2, and WebSocket transport.

| Property                     | Description                                                                                                                                                                                          | Default            |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `http.websockets`            | WebSocket support mode: `none`, `grpc-websockets`, `grpc-websockets-multiplex`, `both`. Defaults to `none` when `ssl.identity.type` turns on [SSL](#ssltls-configuration) or `proxy.hint` is `true`. | `both`             |
| `http.http1`                 | Enable HTTP/1.1 support. Defaults to `false` when `proxy.hint` is `true`.                                                                                                                            | `true`             |
| `http.compression`           | Enable HTTP compression                                                                                                                                                                              | `true`             |
| `https.sniHostCheck`         | Reject HTTPS requests whose `Host` header doesn't match the server certificate (SNI host check)                                                                                                      | `true`             |
| `http.requireHttp2`          | Reject gRPC calls that are not made over HTTP/2                                                                                                                                                      | `true`             |
| `http2.stream.idleTimeoutMs` | HTTP/2 stream idle timeout in milliseconds. `0` or a negative value disables it.                                                                                                                     | `0`                |
| `http2.maxConcurrentStreams` | Maximum concurrent HTTP/2 streams. The default comes from Jetty.                                                                                                                                     | `128`              |
| `http.maxHeaderRequestSize`  | Maximum HTTP header size in bytes. The default comes from Jetty.                                                                                                                                     | `8192`             |
| `http.allowedMethods`        | Allowed HTTP methods (comma-separated)                                                                                                                                                               | `GET,POST,OPTIONS` |

## SSL/TLS configuration

Configure SSL with these properties, in a configuration file or as `-D` arguments. Setting `ssl.identity.type` turns on SSL. None of these properties is set by default.

Turning on SSL also changes the default `http.port` from `10000` to `443`. In a Docker container, set `http.port=10000` so the `ports` mapping still reaches the server. The image's health check connects without TLS, so Docker reports an SSL-enabled container as unhealthy even when the server runs normally. See [mTLS authentication](../authentication/auth-mtls.md) for detailed examples.

| Property                      | Description                                                                                                                     |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `ssl.identity.type`           | Identity type. Only `privatekey` is supported.                                                                                  |
| `ssl.identity.certChainPath`  | Path to the certificate chain (required when `ssl.identity.type` is set)                                                        |
| `ssl.identity.privateKeyPath` | Path to the private key (required when `ssl.identity.type` is set)                                                              |
| `ssl.trust.type`              | Trust type. Only `certs` is supported.                                                                                          |
| `ssl.trust.path`              | Path to trust certificates (required when `ssl.trust.type` is set)                                                              |
| `ssl.clientAuthentication`    | Client auth mode: `NONE`, `WANTED`, `NEEDED`                                                                                    |
| `outbound.ssl.*`              | Outbound SSL configuration. Takes the same suffixes as the `ssl.*` properties in this table, such as `outbound.ssl.trust.type`. |

## Security headers

These properties configure HTTP security headers for production deployments. Each header has an `.enabled` and a `.value` property, and the server sends a header only when its `.enabled` property is `true` and its `.value` property is set. The table lists the headers that `dh-defaults.prop` defines. To send another response header, set `http.add.header.<Header-Name>.enabled=true` and `http.add.header.<Header-Name>.value`.

Turn on Strict-Transport-Security only after TLS is deployed and working. Browsers remember the header for its `max-age` (one year by default), so turning it off later doesn't reach clients that already received it.

| Header                       | Enabled property                                       | Value property                                       | Default enabled | Default value                         |
| ---------------------------- | ------------------------------------------------------ | ---------------------------------------------------- | --------------- | ------------------------------------- |
| Strict-Transport-Security    | `http.add.header.Strict-Transport-Security.enabled`    | `http.add.header.Strict-Transport-Security.value`    | `false`         | `max-age=31536000; includeSubDomains` |
| X-Frame-Options              | `http.add.header.X-Frame-Options.enabled`              | `http.add.header.X-Frame-Options.value`              | `false`         | `DENY`                                |
| Content-Security-Policy      | `http.add.header.Content-Security-Policy.enabled`      | `http.add.header.Content-Security-Policy.value`      | `false`         | `frame-ancestors 'none';`             |
| X-XSS-Protection             | `http.add.header.X-XSS-Protection.enabled`             | `http.add.header.X-XSS-Protection.value`             | `true`          | `1; mode=block`                       |
| X-Content-Type-Options       | `http.add.header.X-Content-Type-Options.enabled`       | `http.add.header.X-Content-Type-Options.value`       | `true`          | `nosniff`                             |
| Referrer-Policy              | `http.add.header.Referrer-Policy.enabled`              | `http.add.header.Referrer-Policy.value`              | `true`          | `no-referrer`                         |
| Cross-Origin-Resource-Policy | `http.add.header.Cross-Origin-Resource-Policy.enabled` | `http.add.header.Cross-Origin-Resource-Policy.value` | `true`          | `same-origin`                         |
| Cross-Origin-Embedder-Policy | `http.add.header.Cross-Origin-Embedder-Policy.enabled` | `http.add.header.Cross-Origin-Embedder-Policy.value` | `true`          | `require-corp`                        |
| Cross-Origin-Opener-Policy   | `http.add.header.Cross-Origin-Opener-Policy.enabled`   | `http.add.header.Cross-Origin-Opener-Policy.value`   | `true`          | `same-origin`                         |

## Web UI configuration

These properties configure the Deephaven web UI defaults.

| Property                         | Description                                                                                                                   | Default      |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `web.storage.layout.directory`   | Subdirectory of `storage.path` that the server creates at startup for layouts. The web UI always uses `/layouts`.             | `/layouts`   |
| `web.storage.notebook.directory` | Subdirectory of `storage.path` that the server creates at startup for notebooks. The web UI always uses `/notebooks`.         | `/notebooks` |
| `web.webgl`                      | Enable WebGL support in the web UI                                                                                            | `true`       |
| `web.webgl.editable`             | Allow users to toggle the WebGL setting                                                                                       | `true`       |
| `web.flattenViewports`           | [Flatten](../../reference/table-operations/create/flatten.md) each table before the web UI subscribes to the rows it displays | `false`      |

## Formula validation

These properties configure validation of the query formulas that clients send over gRPC. See [Formula validation configuration](../../conceptual/formula-validation-configuration.md) for details.

| Property                                     | Description                                                                                                                                                                                                                      | Default                        |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `ColumnExpressionValidator`                  | Validator type: `parsed` or `method_name`                                                                                                                                                                                        | `parsed`                       |
| `ColumnExpressionValidator.allowedMethods.*` | Used by the `parsed` validator. Patterns that name the methods that formulas may call. See [Formula validation configuration](../../conceptual/formula-validation-configuration.md) for the pattern syntax.                      | Set in `dh-defaults.prop`      |
| `ColumnExpressionValidator.annotationSets.*` | Used by the `parsed` validator. Annotation sets of methods that formulas may call. The server combines the values of every property with this prefix. `ColumnExpressionValidator.annotationSets.default` sets the default value. | `base,vector,function_library` |

## Engine configuration

These properties configure the query engine: its update cycle, calendar and time zone, and performance monitoring. The [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) recomputes [ticking tables](../../conceptual/table-types.md) in repeating refresh cycles. Its guide explains how the cycle duration affects updates.

| Property                                        | Description                                                                                                                    | Default |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `PeriodicUpdateGraph.targetCycleDurationMillis` | Target cycle duration for the [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md) in milliseconds | `1000`  |

### Calendar and time zone

These properties set the default business calendar, the calendars the server loads at startup, and the time zone aliases the server recognizes. To load your own calendars, see [Work with calendars](../business-calendar.md).

| Property                  | Description                                                                                            | Default                          |
| ------------------------- | ------------------------------------------------------------------------------------------------------ | -------------------------------- |
| `Calendar.default`        | Default [business calendar](../business-calendar.md)                                                   | `UTC`                            |
| `Calendar.importPath`     | Classpath location of the list of built-in calendars. To add calendars, use `Calendar.userImportPath`. | `/default_calendar_imports.txt`  |
| `Calendar.userImportPath` | Classpath location of a list of extra calendars to load at startup                                     | Not set                          |
| `timezone.aliases`        | File path or classpath location of the time zone alias definitions                                     | `/default_time_zone_aliases.csv` |

### Performance monitoring

This property sets how often the server writes update performance reports to the [performance tables](../performance/performance-tables.md).

| Property                                        | Description                                 | Default |
| ----------------------------------------------- | ------------------------------------------- | ------- |
| `UpdatePerformanceTracker.reportIntervalMillis` | Performance report interval in milliseconds | `60000` |

## Complete example

This example shows a Docker Compose configuration with several customizations. The server listens on port `10000` inside the container, and the `8080:10000` mapping makes it reachable on host port `8080`. It also mounts `./data` at `/data`, the [data directory](#bootstrap-configuration), so notebooks, layouts, and other data persist on the host.

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "8080:10000"
    volumes:
      - ./data:/data
      - ./config/deephaven.prop:/opt/deephaven/config/deephaven.prop:ro
    environment:
      - START_OPTS=-Xmx8g
```

The mounted `./config/deephaven.prop` file contains:

```properties
includefiles=dh-defaults.prop

# Session duration (1 hour)
http.session.durationMs=3600000

# Use anonymous authentication
AuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler

# Set the console language to Groovy
deephaven.console.type=groovy

# Run update cycles every 100 ms instead of every second
PeriodicUpdateGraph.targetCycleDurationMillis=100
```

## Related documentation

- [Periodic Update Graph configuration](../../conceptual/periodic-update-graph-configuration.md)
- [Formula validation configuration](../../conceptual/formula-validation-configuration.md)
- [Table types](../../conceptual/table-types.md)
- [Deephaven configuration files](./config-file.md)
- [Configure the Docker application](./docker-application.md)
- [Configure the production application](./configure-production-application.md)
- [Application Mode](../application-mode.md)
- [Configure and adjust heap size](../heap-size.md)
- [Work with calendars](../business-calendar.md)
- [How to use Deephaven's performance tables for system monitoring](../performance/performance-tables.md)
- [Pre-shared key authentication](../authentication/auth-psk.md)
- [Anonymous authentication](../authentication/auth-anon.md)
- [mTLS authentication](../authentication/auth-mtls.md)
- [Keycloak/OIDC authentication](../authentication/auth-keycloak.md)
- [SQL-backed username/password authentication](../authentication/auth-uname-pw.md)
- [`flatten`](../../reference/table-operations/create/flatten.md)
