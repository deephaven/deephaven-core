---
id: common-problems
title: Common problems when debugging Deephaven
sidebar_label: Common problems
---

This guide describes common Deephaven-specific issues you may encounter when debugging Groovy code.

> [!NOTE]
> These issues are specific to how Deephaven works and are not related to your debugger setup. The examples below apply when your code lives in a file that IntelliJ can match to the running JVM — for example, an [application mode](../../how-to-guides/application-mode.md) script loaded at server startup. Code typed interactively into the Deephaven console is compiled dynamically and cannot be matched to a source file.

## Setup for the examples FIXME: fix from here down, docker-setup might need updates too

The examples below use an application mode script loaded at server startup. Inside your Docker debug project (the one containing `docker-compose.yml`), create the following structure:

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

```groovy
def addOne(int x) {
    return x + 1
}

t = emptyTable(10).update("X = ii")
tLazy = t.updateView("Y = addOne(X)")
tEager = t.select("X", "Y = addOne(X)")
```

Then add `-Ddeephaven.application.dir=/data/app.d` to `START_OPTS` in your `docker-compose.yml`:

```yaml
environment:
  - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -Ddeephaven.application.dir=/data/app.d
```

When you restart the container, the script loads automatically. Set your breakpoints in IntelliJ by opening `debug-demo.groovy` from wherever you saved it — IntelliJ will match them to the running JVM because the file is loaded from disk.

## Lazy evaluation of some table operations

Some Deephaven table operations — including [`updateView`](../../reference/table-operations/select/update-view.md) — use lazy evaluation, where results are not computed until a downstream operation requires them. If no such downstream operation exists, the debugger may not reach the expected code because no real work is being done. This is particularly relevant when debugging methods called from lazy table operations.

In this example, a breakpoint inside `addOne` will not be hit:

```groovy
def addOne(int x) {
    return x + 1  // Breakpoint set here — will NOT be hit
}

t = emptyTable(10).update("X = ii")
tLazy = t.updateView("Y = addOne(X)")
```

<!-- TODO: Add screenshot showing breakpoint not triggered with updateView -->

`updateView` defers calling `addOne` until another operation forces the result to materialize. To force evaluation for debugging purposes, use [`select`](../../reference/table-operations/select/select.md):

```groovy
def addOne(int x) {
    return x + 1  // Breakpoint set here — WILL be hit
}

t = emptyTable(10).update("X = ii")
tEager = t.select("X", "Y = addOne(X)")
```

<!-- TODO: Add screenshot showing breakpoint triggered with select -->

`select` evaluates all expressions immediately and stores the results, so `addOne` is called during the `select` operation and any breakpoints inside it will be hit.

## Related documentation

- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`select`](../../reference/table-operations/select/select.md)
- [Table types](../../conceptual/table-types.md)
