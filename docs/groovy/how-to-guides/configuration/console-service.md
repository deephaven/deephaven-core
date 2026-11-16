---
title: Configure the Deephaven console service
---

This guide shows how to disable console code execution and autocomplete with configuration properties.

The console service is a gRPC service that executes code and returns autocompletion results. For background, see [Protocol Buffers and Remote Procedure Calls](../protobuf.md). For the full service definition, see [`console.proto`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/console.proto).

## Configuration

Set these properties as JVM flags, such as `-Ddeephaven.console.disable=true`, or in a [configuration file](./config-file.md).

- `deephaven.console.disable`: Set to `true` to disable console-based code execution. The default is `false`.
- `deephaven.console.autocomplete.disable`: Set to `true` to disable autocomplete for both Groovy and Python. The default is `false`.

Disabling console code execution prevents API clients, including the web UI, from starting REPL sessions and from subscribing to server logs. It also stops the server from returning console types and from publishing [application mode](../application-mode.md) fields to clients. Those sessions run with the same privileges on the host as the server process.

## Related documentation

- [How to configure the Deephaven production application](./configure-production-application.md)
- [How to create a Deephaven configuration file](./config-file.md)
- [Run Deephaven in application mode](../application-mode.md)
- [Protocol Buffers and Remote Procedure Calls](../protobuf.md)
- [Configuration properties reference](./configuration-properties.md)
- [Is it possible disable the Deephaven console?](../../reference/community-questions/possible-to-disable-console.md)
