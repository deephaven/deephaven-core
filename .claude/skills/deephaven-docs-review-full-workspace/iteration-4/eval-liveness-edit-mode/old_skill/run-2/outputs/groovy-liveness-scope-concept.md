---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide discusses liveness scopes. It covers what a liveness scope is, how to use one, and why queries can benefit from its use.

## Why use a liveness scope?

Liveness scopes give you a finer degree of control over cleanup of unreferenced nodes in the [query update graph](./table-update-model.md). A node in the update graph can be a table, plot, or any other object.

Deephaven's engine runs in Java, in which the JVM handles garbage collection (GC). You have little control over when garbage collection takes place, as the JVM tries to optimize when it occurs to minimize GC runtime and maximize the amount of memory left available after it runs.

By default, objects that a console command creates are managed by the script session, and are cleaned up only when they are garbage collected or when the session closes. Without a liveness scope, a query relies on the JVM's garbage collector to clean up unreferenced nodes in the update graph. In most cases, this is acceptable. However, some queries benefit from releasing the objects they no longer need at a time of their choosing, which is what a liveness scope provides.

## How liveness scopes work

A liveness scope automatically manages reference counting of the nodes created while it is open. Deephaven's liveness system cleans up the nodes in a refreshing query's update propagation graph proactively, by tracking each node's "liveness" (whether anything still needs it), rather than only through the actions of the Java garbage collector. This does not replace GC, but it does allow a node to be cleaned up as soon as nothing needs it anymore. The engine does this reference counting internally and automatically.

When a table is live in Deephaven, the update propagation graph accumulates parent nodes that produce data (data sources) at the top, and all the child nodes that stream down the graph. The liveness system keeps track of these referents: when a node's reference count drops to zero, the node is cleaned up immediately, without waiting for GC, and it releases its own references to its parents. Releasing a liveness scope drops the scope's references to everything it manages, so any of those objects that nothing else still needs are cleaned up.

The [`LivenessScope`](../reference/engine/LivenessScope.md) and [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) classes give you direct control over the reference counts of query engine artifacts. Opening a scope before running a refreshing query holds together the related artifacts created in its update propagation graph, so you can release them together once the query no longer needs them.

## Demonstrating the problem

Before demonstrating liveness scopes in action, this section shows the problem that liveness scopes solve.

This query creates a simple tree table grouped by `Instrument`. Two tables open: `crypto` and `comboTree`.

```groovy order=null
import static io.deephaven.csv.CsvTools.readCsv

crypto = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
).firstBy("Instrument").update("ID = Instrument", "Parent = (String)null").update("Timestamp = (Instant)null", "Exchange = (String)null", "Price = (Double)null", "Size = (Double)null")

data = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv").update("ID = Long.toString(ii)", "Parent = Instrument")

combo = merge(crypto, data)

comboTree = combo.tree("ID", "Parent")

data = null

combo = null
```

The above example works, but the query creates many objects that are not needed after it runs. A [`LivenessScope`](../reference/engine/LivenessScope.md) can manage these objects and release them when they are no longer needed. This is best practice because it allows Deephaven to conserve memory.

The following example runs the same query, but encloses it in a [`LivenessScope`](../reference/engine/LivenessScope.md). Before opening the scope, the example saves the enclosing manager returned by `LivenessScopeStack.peek`, and the tables that stay open after the query runs are handed to it with `manage`. Passing `true` to `LivenessScopeStack.open` releases the scope when the block exits, which cleans up everything else it manages that those tables don't depend on.

```groovy order=null
import io.deephaven.engine.liveness.*
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.util.SafeCloseable

outerScope = LivenessScopeStack.peek()
scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, true) ) {

    crypto = readCsv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
    ).firstBy("Instrument").update("ID = Instrument", "Parent = (String)null").update("Timestamp = (Instant)null", "Exchange = (String)null", "Price = (Double)null", "Size = (Double)null")

    data = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv").update("ID = Long.toString(ii)", "Parent = Instrument")

    combo = merge(crypto, data)

    comboTree = combo.tree("ID", "Parent")

    // Keep the tables that stay open after the scope is released
    outerScope.manage(crypto)
    outerScope.manage(comboTree)

}

data = null

combo = null
```

## How to use a `LivenessScope`

Creating a `LivenessScope` for your query lets you decide when the objects it creates are released. When the scope is released, it drops its references to the objects it manages, and any of those objects that nothing else still needs are cleaned up.

The following syntax creates a `LivenessScope` that can preface any Deephaven query:

```groovy skip-test
import io.deephaven.engine.liveness.*

// Create a new LivenessScope
scope = new LivenessScope()

// Add scope to the LivenessScopeStack. This makes the scope the current scope for the current thread.
LivenessScopeStack.push(scope)

// Your query here

// Remove the scope from the stack. This makes the previous scope current again, but does not release this scope.
LivenessScopeStack.pop(scope)

// Release the scope's references to the liveness referents it manages
scope.release()
```

> [!NOTE]
> This example is intended to illustrate how the `LivenessScopeStack` manages `LivenessScope` instances. In practice, use try-with-resources blocks to manage scopes.

The example above first imports the `liveness` package and creates a new `LivenessScope`. Next, it pushes the `LivenessScope` onto the `LivenessScopeStack`. This makes it the current scope for the thread, so it manages any query artifacts created while it is on the stack. After the query (or queries) runs, `LivenessScopeStack.pop(scope)` removes the `LivenessScope` from the stack, and calling `release` on the scope releases the query artifacts it was managing. Popping a scope does not release it — without the call to `release`, the scope keeps managing its artifacts.

You can also enclose a scope in a try-with-resources block using the `LivenessScopeStack.open(scope, releaseOnClose)` method. Setting `releaseOnClose` to `true` automatically releases the scope when the block exits; setting it to `false` only pops the scope from the stack.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, true) ) {

    // Your query here

}
```

Calling `LivenessScopeStack.open` with no parameters creates an anonymous scope and automatically releases it when the block exits.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

try ( SafeCloseable ignored = LivenessScopeStack.open() ) {

    // Your query here, managed by the anonymous scope that open() creates

}
```

## Multiple liveness scopes

Liveness scopes can be nested. The `LivenessScopeStack` uses the scopes in the order they are pushed onto the stack — that is, the active `LivenessScope` is the one most recently pushed to the `LivenessScopeStack`. When a scope is popped from the stack, the next scope in the stack becomes the active scope. This can be done manually with `LivenessScopeStack.push(scope)` and `LivenessScopeStack.pop(scope)`, but best practice is to use try-with-resources blocks.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable


try ( SafeCloseable ignored = LivenessScopeStack.open() ) {

    // Your query here, managed by an anonymous scope

    try ( SafeCloseable ignored2 = LivenessScopeStack.open() ) {

        // Your query here, managed by a second anonymous scope that is enclosed by the first
    }
}
```

## `LivenessScopeStack` methods

The `LivenessScopeStack` class provides these static methods for controlling which scope manages new query engine artifacts on the current thread:

- [`LivenessScopeStack.push(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) — Push a scope onto the current thread's scope stack, making it the scope that manages new query artifacts.
- [`LivenessScopeStack.peek`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) — Get the scope at the top of the current thread's scope stack, or the base manager if no scopes have been pushed but not popped on this thread. This determines which scope automatically manages new query artifacts.
- [`LivenessScopeStack.pop(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) — Pop the scope from the top of the current thread's scope stack. The scope must be the current top of the stack. Popping a scope does not release it.
- [`LivenessScopeStack.open(scope, releaseOnClose)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) — Push a scope onto the scope stack, and get a `SafeCloseable` that pops it. The first parameter specifies the scope; the second boolean parameter determines whether the scope is also released when the result is closed. This is useful for enclosing scope usage in a try-with-resources block.
- [`LivenessScopeStack.open`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) (no arguments) — Push an anonymous scope onto the scope stack, and get a `SafeCloseable` that pops it and then releases it. This is useful for enclosing a series of query engine actions whose results must be explicitly retained externally in order to preserve liveness.
- [`LivenessScopeStack.computeEnclosed(computation, shouldEnclose, shouldManageResult)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#computeEnclosed(java.util.function.Supplier,boolean,java.util.function.Predicate)) — Run a computation, inside a new scope that is released before the method returns when `shouldEnclose` is `true`, and manage the result with the enclosing scope when `shouldManageResult` accepts it. `computeArrayEnclosed` does the same for a computation that returns an array of results.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
