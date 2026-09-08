---
title: Configure the Deephaven console service
sidebar_label: Deephaven console service
---

The console service is a gRPC service that enables you to execute code and get autocompletion results (see [console.proto](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/console.proto) for more details).

## Configuration

- `deephaven.console.disable`: Set to `true` to disable console-based code execution.
- `deephaven.console.autocomplete.disable`: Set to `true` to disable autocomplete for both Groovy and Python.

See the [configuration file](./config-file.md) guide for more details on where to set these configuration properties.

## Jedi

Deephaven uses [jedi](https://github.com/davidhalter/jedi) for the console autocompletion service that powers the [web UI](https://github.com/deephaven/web-client-ui).

To disable jedi autocomplete at runtime, run the following script:

```python skip-test
from deephaven_internal.auto_completer import jedi_settings, Mode

jedi_settings.mode = Mode.OFF
```

Valid options for `mode` are one of:

- `STRONG`: (default) Looks in the user's `globals` for answers to autocomplete and analyzes runtime Python objects.
- `OFF`: Turns off autocomplete.
- `SAFE`: Uses static analysis of source files. No code can be executed.

## Related documentation

- [How to configure the Deephaven production application](./configure-production-application.md)
- [How to create a Deephaven configuration file](./config-file.md)
