---
id: common-problems
title: Common problems when debugging Deephaven
---

This guide covers two Deephaven-specific reasons Groovy code doesn't behave as you expect under a debugger: breakpoints in Groovy scripts that are never hit, and table operations that evaluate lazily. Neither is caused by your debugger setup.

The examples use IntelliJ IDEA connected to [Docker-run Deephaven](./docker-setup.md), but these problems apply to any debugger attached to the server.

## Setup for the examples

The examples below use an [Application Mode](../application-mode.md) script loaded at server startup. Inside your [Docker debug project](./docker-setup.md) (the one containing `docker-compose.yml`), create the following structure:

```text
my-docker-debug-project/
├── docker-compose.yml
└── data/
    └── app.d/
        ├── debug-demo.app
        └── debug-demo.groovy
```

`debug-demo.app`:

```text
type=script
scriptType=groovy
enabled=true
id=debug.demo
name=Debug Demo
file_0=./debug-demo.groovy
```

`debug-demo.groovy`:

```groovy test-set=1 order=null
addOne = { long x -> x + 1 }

t = emptyTable(10).update("X = ii")
tLazy = t.updateView("Y = addOne(X)")
tEager = t.select("X", "Y = addOne(X)")
```

The script builds the same column twice: `tLazy` with [`updateView`](../../reference/table-operations/select/update-view.md) and `tEager` with [`select`](../../reference/table-operations/select/select.md).

> [!NOTE]
> [Query strings](../query-string-overview.md) can call a function you define in your script only when it is a [closure](../groovy-closures.md) assigned to a top-level variable without `def`, as `addOne = { long x -> x + 1 }` is above. That assignment puts the closure in the [Query Scope](../query-scope.md), the set of variables query strings can use. Query strings can't call:
>
> - A method defined with `def name(...) { ... }`.
> - A closure stored in a variable declared with `def`.

Add `-Ddeephaven.application.dir=/data/app.d` to `START_OPTS` in your `docker-compose.yml`. The `volumes` entry mounts your project's `data` directory at `/data` in the container, so `data/app.d` is `/data/app.d` there:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - "127.0.0.1:${DEEPHAVEN_PORT:-10000}:10000"
      - "127.0.0.1:5005:5005"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -Ddeephaven.application.dir=/data/app.d
```

> [!NOTE]
> The `127.0.0.1:` prefix on each port publishes it only on your own machine. This matters because, with this configuration, anyone who can reach either port has full access to the server:
>
> - The `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler` setting turns on [anonymous authentication](../authentication/auth-anon.md), so any client that reaches port 10000 can run code without a password.
> - Any client that reaches port 5005 can attach a debugger to the server's JVM.
>
> Inside the container, the debug agent still listens on all interfaces (`address=*:5005`), so that Docker can forward the port to it.

Stop the running container (Ctrl+C in its terminal), then run [`docker compose up`](https://docs.docker.com/reference/cli/docker/compose/up/) again from the directory containing `docker-compose.yml`. Compose recreates the container with the new `START_OPTS`, and the script loads at startup. [`docker compose restart`](https://docs.docker.com/reference/cli/docker/compose/restart/) doesn't pick up changes to `docker-compose.yml`.

## Breakpoints in Groovy scripts are not hit

You've set a breakpoint in a Groovy script, whether loaded with [Application Mode](../application-mode.md) or entered in the Deephaven console. IntelliJ is connected, but execution runs through the breakpoint without pausing.

Deephaven compiles Groovy code dynamically before evaluating it, whether the code comes from an [Application Mode](../application-mode.md) script or the console. IntelliJ can't match the compiled class to your `.groovy` file, so it doesn't pause at your breakpoints. Two changes cause the mismatch:

- **Line numbers**: Deephaven prepends a `package io.deephaven.dynamic;` declaration, which shifts every line in the compiled class down by one.
- **Class name**: The compiled class gets a generated name, not your file's name. The name ends in a counter that increases with every script the session runs, including Deephaven's startup scripts, so your numbers will differ:
  - An [Application Mode](../application-mode.md) script is named after its file path, with every character that isn't a letter or digit replaced by `_`. For example, `/data/app.d/./debug-demo.groovy` becomes `io.deephaven.dynamic._data_app_d___debug_demo_groovy_6`.
  - Console code is named `Script_` plus the counter, for example `io.deephaven.dynamic.Script_7`.

As a workaround, use `println` to trace execution. The output appears in the server log. For a [source build](./source-setup.md), that's the terminal where you ran `./gradlew server-jetty-app:run -Pgroovy`. For Docker, it's the container logs, which you can follow with [`docker compose logs`](https://docs.docker.com/reference/cli/docker/compose/logs/):

```shell
docker compose logs -f
```

Update `debug-demo.groovy` to add trace output:

```groovy test-set=1 order=null
addOne = { long x ->
    println "[DEBUG] addOne called: x=$x"
    def result = x + 1
    println "[DEBUG] addOne returning: $result"
    result
}

t = emptyTable(10).update("X = ii")
tLazy = t.updateView("Y = addOne(X)")
tEager = t.select("X", "Y = addOne(X)")
```

When the server starts and loads the script, the log shows `[DEBUG]` lines from `addOne` for `tEager`. For why `tLazy` produces none, see [Lazy evaluation of some table operations](#lazy-evaluation-of-some-table-operations).

To step through the Deephaven Java code your script calls, set breakpoints in Deephaven's Java source instead. See [Use the IntelliJ debugger](./docker-setup.md#5-use-the-intellij-debugger) for Docker-run Deephaven, or [Use the IntelliJ debugger](./source-setup.md#5-use-the-intellij-debugger) for built-from-source Deephaven.

## Lazy evaluation of some table operations

Some Deephaven table operations, including [`view`](../../reference/table-operations/select/view.md), [`updateView`](../../reference/table-operations/select/update-view.md), and [`lazyUpdate`](../../reference/table-operations/select/lazy-update.md), use _lazy evaluation_. Deephaven doesn't compute a column's values until something reads them, such as a downstream operation or opening the table in the Deephaven web IDE.

Until then, the closure that the column's [formula](../formulas.md) calls never runs. A `println` trace in it prints nothing, and the debugger doesn't stop at a breakpoint in the Deephaven Java code that evaluates the column.

The last two lines of `debug-demo.groovy` build the same column both ways:

```groovy test-set=1 order=null
tLazy = t.updateView("Y = addOne(X)") // addOne is NOT called here
tEager = t.select("X", "Y = addOne(X)") // addOne IS called here, once per row
```

- `tLazy` uses [`updateView`](../../reference/table-operations/select/update-view.md), so Deephaven doesn't call `addOne` for it when the script loads. The trace prints nothing until something reads the `Y` column of `tLazy`.
- `tEager` uses [`select`](../../reference/table-operations/select/select.md), which evaluates all expressions immediately and stores the results. With the `println` trace from [Breakpoints in Groovy scripts are not hit](#breakpoints-in-groovy-scripts-are-not-hit), the logs show 20 lines of output when the script loads, two for each of its 10 rows.

To force evaluation for debugging purposes, use [`select`](../../reference/table-operations/select/select.md) in place of [`updateView`](../../reference/table-operations/select/update-view.md).

## Related documentation

- [Application Mode](../application-mode.md)
- [Enable anonymous authentication](../authentication/auth-anon.md)
- [Formulas](../formulas.md)
- [User-defined functions](../groovy-closures.md)
- [Query Scope](../query-scope.md)
- [Query strings](../query-string-overview.md)
- [Debug Docker-run Deephaven with IntelliJ IDEA](./docker-setup.md)
- [Debug built-from-source Deephaven with IntelliJ IDEA](./source-setup.md)
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md)
- [`lazyUpdate`](../../reference/table-operations/select/lazy-update.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`view`](../../reference/table-operations/select/view.md)
