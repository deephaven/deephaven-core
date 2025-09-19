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

```python skip-test
from deephaven.uri import resolve

resolved_table = resolve(f"dh+plain://{container_name}/scope/my_table")
```

## Share tables across a network

> [!NOTE]
> Sharing across a network requires the IP/hostname and port on which Deephaven is running.

```python skip-test
from deephaven.uri import resolve

table_from_ip = resolve(f"dh+plain://{ip_address}:{port}/scope/my_table")
table_from_hostname = resolve(f"dh+plain://{hostname}:{port}/scope/my_table")
```
