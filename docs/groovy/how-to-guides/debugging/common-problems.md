---
id: common-problems
title: Common problems when debugging Deephaven
sidebar_label: Common problems
---

This guide describes common Deephaven-specific issues you may encounter when debugging Groovy code.

> [!NOTE]
> These issues are specific to how Deephaven works and are not related to your debugger setup. The examples below apply when your code lives in a file that IntelliJ can match to the running JVM — for example, an [application mode](../../how-to-guides/application-mode.md) script loaded at server startup. Code typed interactively into the Deephaven console is compiled dynamically and cannot be matched to a source file.

## Setup for the examples

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
addOne = { long x -> x + 1 }

t = emptyTable(10).update("X = ii")
tLazy = t.updateView("Y = addOne(X)")
tEager = t.select("X", "Y = addOne(X)")
```

> [!NOTE]
> Define functions as closures (assigned to a variable without `def`) rather than as regular Groovy methods. Closures are placed into the query scope and can be called from formula strings. Methods defined with `def` are not accessible to the formula compiler.

Then add `-Ddeephaven.application.dir=/data/app.d` to `START_OPTS` in your `docker-compose.yml`:

```yaml
environment:
  - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -Ddeephaven.application.dir=/data/app.d
```

When you restart the container, the script loads automatically.

## Breakpoints in app mode scripts are not hit

**Problem**: You've set a breakpoint in an app mode script, IntelliJ is connected, but execution runs through the breakpoint without pausing.

**Cause**: Deephaven compiles app mode scripts dynamically before evaluating them. It prepends a `package io.deephaven.dynamic;` declaration to the script content, which shifts all line numbers by one in the compiled class. The compiled class also receives a path-derived internal name (for example, `_data_app_d___debug_demo_groovy`) rather than the original filename. IntelliJ cannot reliably map these compiled classes back to your source file, so breakpoints set in your `.groovy` files are never hit.

This applies to both app mode scripts and code entered in the Deephaven console.

**Workaround**: Use `println` to trace execution. Output appears in the container logs:

```shell
docker compose logs -f
```

Update `debug-demo.groovy` to add trace output:

```groovy
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

When the server starts and loads the script, `tEager` evaluates immediately and you will see 20 lines of output in the logs (two per row, 10 rows). `tLazy` produces no output at startup — `addOne` is not called until a downstream operation forces evaluation.

For line-by-line breakpoint debugging of user Groovy code, see [source debugging setup](./source-setup.md).

## Lazy evaluation of some table operations

Some Deephaven table operations — including [`updateView`](../../reference/table-operations/select/update-view.md) — use lazy evaluation, where results are not computed until a downstream operation requires them. [`select`](../../reference/table-operations/select/select.md) evaluates immediately and stores the results.

The `debug-demo.groovy` script creates both kinds of tables:

```groovy
tLazy = t.updateView("Y = addOne(X)")    // addOne is NOT called here
tEager = t.select("X", "Y = addOne(X)") // addOne IS called here, once per row
```

`updateView` defers calling `addOne` until a downstream operation forces the result to materialize. `select` evaluates all expressions immediately and stores the results.

## Related documentation

- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`select`](../../reference/table-operations/select/select.md)
- [Table types](../../conceptual/table-types.md)
