---
title: URI cheat sheet
sidebar_label: URI
---

Deephaven allows users to share tables and other assets via [URIs](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier).

## Syntax

`dh+plain://<authority>/<path>`

## Share tables locally

> [!NOTE]
> URI sharing locally requires the Docker container name.

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

resolvedTable = resolve("dh+plain://{container_name}/scope/myTable")
```

## Share tables across a network

> [!NOTE]
> Sharing across a network requires the IP/hostname and port on which Deephaven is running.

```groovy skip-test
import static io.deephaven.uri.ResolveTools.resolve

tableFromIp = resolve("dh+plain://{ip_address}:{port}/scope/myTable")
tableFromHostname = resolve("dh+plain://{hostname}:{port}/scope/myTable")
```
