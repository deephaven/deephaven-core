---
title: Configuration properties reference
sidebar_label: Configuration properties
---

This document covers the most frequently used Deephaven Community configuration properties. Properties can be set in several ways depending on your deployment method.

## How to set properties

### Configuration file

Create a `deephaven.prop` file with properties (see [Deephaven configuration files](./config-file.md)):

```properties
includefiles=dh-defaults.prop

# Override default properties
http.port=8080
AuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

### Docker Compose

Mount a configuration file to `/opt/deephaven/config/deephaven.prop`:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "10000:10000"
    volumes:
      - ./deephaven.prop:/opt/deephaven/config/deephaven.prop:ro
    environment:
      - START_OPTS=-Xmx4g
```

For quick testing or a small number of properties, you can set them directly via the `START_OPTS` environment variable using `-D` flags. For production or complex configurations, prefer the mounted config file approach above:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "8080:8080"
    environment:
      - START_OPTS=-Xmx4g -Dhttp.port=8080 -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

### Production application

Use a configuration file for production deployments. Set JVM memory options via `START_OPTS`:

```bash
export START_OPTS="-Xmx4g"
./bin/start
```

### pip install (Python)

For development and testing, pass properties as JVM arguments when starting the server. For production, use a [configuration file](./config-file.md):

```python skip-test
from deephaven_server import Server

s = Server(port=10000, jvm_args=["-Xmx4g", "-Dauthentication.psk=MySecretKey"]).start()
```

### Command line

For development and testing, use the CLI with `--jvm-args`. For production, use a [configuration file](./config-file.md):

```bash
deephaven server --port 10000 --jvm-args "-Xmx4g -Dauthentication.psk=MySecretKey"
```

## Bootstrap configuration

Deephaven sets bootstrap configuration parameters early in the application startup lifecycle, before reading configuration files. Set them via environment variables or system properties.

| Property         | Environment Variable    | System Property         | Description                                                | Default      |
| ---------------- | ----------------------- | ----------------------- | ---------------------------------------------------------- | ------------ |
| Application name | `DEEPHAVEN_APPLICATION` | `deephaven.application` | Name used for default values in other configuration        | `deephaven`  |
| Data directory   | `DEEPHAVEN_DATA_DIR`    | `deephaven.dataDir`     | Directory where users and applications read and write data | OS-dependent |
| Cache directory  | `DEEPHAVEN_CACHE_DIR`   | `deephaven.cacheDir`    | Directory where the server reads and writes cache data     | OS-dependent |
| Config directory | `DEEPHAVEN_CONFIG_DIR`  | `deephaven.configDir`   | Directory where the server reads configuration files       | OS-dependent |
| Quiet flag       | `DEEPHAVEN_QUIET`       | `deephaven.quiet`       | Whether to suppress bootstrap logging                      | `false`      |

### OS-dependent directories

| OS    | Data directory                                                            | Cache directory                                              | Config directory                                                          |
| ----- | ------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------- |
| Linux | `$HOME/.local/share/<application>/`                                       | `$HOME/.cache/<application>/`                                | `$HOME/.config/<application>/`                                            |
| macOS | `$HOME/Library/Application Support/io.Deephaven-Data-Labs.<application>/` | `$HOME/Library/Caches/io.Deephaven-Data-Labs.<application>/` | `$HOME/Library/Application Support/io.Deephaven-Data-Labs.<application>/` |

### Docker default directories

Docker images set these environment variables by default:

- `DEEPHAVEN_DATA_DIR=/data`
- `DEEPHAVEN_CACHE_DIR=/cache`
- `DEEPHAVEN_CONFIG_DIR=/opt/deephaven/config`
- `EXTRA_CLASSPATH=/apps/libs/*`

## Server configuration

These properties control the core server behavior.

| Property                  | Description                                                                                 | Default                      |
| ------------------------- | ------------------------------------------------------------------------------------------- | ---------------------------- |
| `http.host`               | Network interface to bind to (IP address or hostname). If not set, binds to all interfaces. | All interfaces               |
| `http.port`               | Port the server listens on                                                                  | `10000` (or `443` with SSL)  |
| `http.targetUrl`          | User-accessible target URL (useful behind proxies)                                          | Auto-computed from host/port |
| `http.session.durationMs` | Session duration in milliseconds                                                            | `300000` (5 minutes)         |

## Authentication configuration

These properties control how users authenticate to the server.

| Property                        | Description                                                       | Default                                                    |
| ------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------- |
| `AuthHandlers`                  | Comma-separated list of authentication handler class names        | `io.deephaven.authentication.psk.PskAuthenticationHandler` |
| `authentication.psk`            | Pre-shared key value for PSK authentication                       | Random generated key                                       |
| `authentication.anonymous.warn` | Whether to log a warning when anonymous authentication is enabled | `true`                                                     |

### Available authentication handlers

These handlers are included in the standard server images:

| Handler class                                                | Description                                   |
| ------------------------------------------------------------ | --------------------------------------------- |
| `io.deephaven.authentication.psk.PskAuthenticationHandler`   | Pre-shared key (PSK) authentication (default) |
| `io.deephaven.auth.AnonymousAuthenticationHandler`           | No authentication required                    |
| `io.deephaven.authentication.mtls.MTlsAuthenticationHandler` | Mutual TLS (mTLS) authentication              |

Additional authentication methods require custom builds:

- [Keycloak/OIDC authentication](../authentication/auth-keycloak.md)
- [SQL-backed username/password authentication](../authentication/auth-uname-pw.md)

## HTTP and web server configuration

These properties configure the HTTP server and web interface.

| Property             | Description                                                                            | Default                              |
| -------------------- | -------------------------------------------------------------------------------------- | ------------------------------------ |
| `http.websockets`    | WebSocket support mode: `none`, `grpc-websockets`, `grpc-websockets-multiplex`, `both` | `both` (no SSL) or `none` (with SSL) |
| `http.http1`         | Enable HTTP/1.1 support                                                                | `true`                               |
| `http.compression`   | Enable HTTP compression                                                                | `true`                               |
| `https.sniHostCheck` | Enable SNI host checking for HTTPS                                                     | `true`                               |

## Web UI configuration

These properties configure the Deephaven web interface defaults.

| Property                         | Description                                | Default      |
| -------------------------------- | ------------------------------------------ | ------------ |
| `deephaven.console.type`         | Default console type: `python` or `groovy` | `python`     |
| `web.storage.layout.directory`   | Directory for storing layouts              | `/layouts`   |
| `web.storage.notebook.directory` | Directory for storing notebooks            | `/notebooks` |
| `web.webgl`                      | Enable WebGL support in the web interface  | `true`       |
| `web.webgl.editable`             | Allow users to toggle WebGL setting        | `true`       |
| `web.flattenViewports`           | Flatten web viewports by default           | `false`      |

## Security headers

These properties configure HTTP security headers for production deployments. Each header has an `.enabled` and `.value` property.

| Header                       | Enabled Property                                       | Value Property                                       | Default Enabled | Default Value                         |
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

## Engine configuration

These properties configure the query engine behavior.

| Property                                        | Description                                                            | Default               |
| ----------------------------------------------- | ---------------------------------------------------------------------- | --------------------- |
| `PeriodicUpdateGraph.targetCycleDurationMillis` | Target cycle duration for the update graph in milliseconds             | `1000`                |
| `grpc.maxInboundMessageSize`                    | Maximum inbound gRPC message size in bytes (increase for large tables) | `104857600` (100 MiB) |

### Calendar and time zone

| Property              | Description                         | Default                          |
| --------------------- | ----------------------------------- | -------------------------------- |
| `Calendar.default`    | Default business calendar           | `UTC`                            |
| `Calendar.importPath` | Path to calendar import definitions | `/default_calendar_imports.txt`  |
| `timezone.aliases`    | Path to time zone alias definitions | `/default_time_zone_aliases.csv` |

### Session initialization scripts

| Property                             | Description                                  | Default |
| ------------------------------------ | -------------------------------------------- | ------- |
| `PythonDeephavenSession.initScripts` | Comma-separated paths to Python init scripts | Empty   |
| `GroovyDeephavenSession.initScripts` | Comma-separated paths to Groovy init scripts | Empty   |

## SSL/TLS configuration

Configure SSL via system properties. See [mTLS authentication](../authentication/auth-mtls.md) for detailed examples.

| Property                        | Description                                        |
| ------------------------------- | -------------------------------------------------- |
| `ssl.identity.type`             | Identity store type: `privatekey` or `keystore`    |
| `ssl.identity.certChainPath`    | Path to certificate chain (for privatekey type)    |
| `ssl.identity.privateKeyPath`   | Path to private key (for privatekey type)          |
| `ssl.identity.keystorePath`     | Path to keystore (for keystore type)               |
| `ssl.identity.keystorePassword` | Keystore password                                  |
| `ssl.trust.type`                | Trust store type                                   |
| `ssl.trust.path`                | Path to trust certificates                         |
| `ssl.clientAuthentication`      | Client auth mode: `NONE`, `WANTED`, `NEEDED`       |
| `outbound.ssl.*`                | Outbound SSL configuration (same options as above) |

## JVM arguments

These are common JVM arguments passed via `START_OPTS`:

| Argument | Description       | Example  |
| -------- | ----------------- | -------- |
| `-Xmx`   | Maximum heap size | `-Xmx4g` |
| `-Xms`   | Initial heap size | `-Xms2g` |

Use a [configuration file](#configuration-file) rather than `-D` flags for Deephaven properties.

## Additional configuration

These properties are less commonly needed but available for specific use cases.

### Server tuning

| Property             | Description                                                 | Default |
| -------------------- | ----------------------------------------------------------- | ------- |
| `scheduler.poolSize` | Size of the scheduler thread pool                           | `4`     |
| `proxy.hint`         | Hint that the server runs behind a proxy (affects defaults) | Not set |
| `shutdown.timeoutMs` | Milliseconds to wait for graceful shutdown                  | `10000` |

### HTTP tuning

| Property                     | Description                                | Default                |
| ---------------------------- | ------------------------------------------ | ---------------------- |
| `http.requireHttp2`          | Require HTTP/2 protocol                    | `true`                 |
| `http2.stream.idleTimeoutMs` | HTTP/2 stream idle timeout in milliseconds | `0` (disabled)         |
| `http2.maxConcurrentStreams` | Maximum concurrent HTTP/2 streams          | `128` (Jetty default)  |
| `http.maxHeaderRequestSize`  | Maximum HTTP header size in bytes          | `8192` (Jetty default) |
| `http.allowedMethods`        | Allowed HTTP methods (comma-separated)     | `GET,POST,OPTIONS`     |

### Formula validation

These properties configure query formula validation for security. See [Formula validation configuration](/core/groovy/docs/conceptual/formula-validation-configuration) for details.

| Property                                           | Description                               | Default                        |
| -------------------------------------------------- | ----------------------------------------- | ------------------------------ |
| `ColumnExpressionValidator`                        | Validator type: `parsed` or `method_name` | `parsed`                       |
| `ColumnExpressionValidator.allowedMethods.*`       | Pointcut expressions for allowed methods  | See dh-defaults.prop           |
| `ColumnExpressionValidator.annotationSets.default` | Default annotation sets to apply          | `base,vector,function_library` |

### Performance monitoring

| Property                                        | Description                                                | Default         |
| ----------------------------------------------- | ---------------------------------------------------------- | --------------- |
| `UpdatePerformanceTracker.reportingMode`        | Performance reporting mode: `NONE`, `LISTENER_ONLY`, `ALL` | `LISTENER_ONLY` |
| `UpdatePerformanceTracker.reportIntervalMillis` | Performance report interval                                | `60000`         |

## Complete example

This example shows a Docker Compose configuration with several customizations:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data
      - ./config/deephaven.prop:/opt/deephaven/config/deephaven.prop:ro
    environment:
      - START_OPTS=-Xmx8g
```

With a configuration file (`./config/deephaven.prop`):

```properties
includefiles=dh-defaults.prop

# Server port
http.port=8080

# Session timeout (1 hour)
http.session.durationMs=3600000

# Use anonymous authentication
AuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler

# Set default console to Groovy
deephaven.console.type=groovy

# Configure the update graph for faster updates
PeriodicUpdateGraph.targetCycleDurationMillis=100

# Enable HSTS for production
http.add.header.Strict-Transport-Security.enabled=true
```

## Related documentation

- [Deephaven configuration files](./config-file.md)
- [Configure the Docker application](./docker-application.md)
- [Configure the production application](./configure-production-application.md)
- [Pre-shared key authentication](../authentication/auth-psk.md)
- [Anonymous authentication](../authentication/auth-anon.md)
- [Formula validation configuration](/core/groovy/docs/conceptual/formula-validation-configuration)
- [Periodic Update Graph configuration](../../conceptual/periodic-update-graph-configuration.md)
