---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide discusses liveness scopes. It covers what a liveness scope is, how to use one, and why queries can benefit from its use.

Liveness scopes give users a finer degree of control over cleanup of unreferenced nodes in the [query update graph](./table-update-model.md). A node in the update graph can be a table, a listener, or another query engine object. A liveness scope automatically manages reference counting of the nodes created within it. You can also release a liveness scope's resources programmatically.

## Why use a liveness scope?

Deephaven's engine runs in Java, in which garbage collection is handled by the JVM. Users have little control over when garbage collection takes place, as the JVM tries to optimize when it occurs to minimize GC runtime and maximize the amount of memory left available after it's run.

Liveness scopes give users more control over when nodes in the query update graph are cleaned up. Without a liveness scope, queries rely on the JVM's garbage collector to clean up unreferenced nodes in the update graph. In most cases, this is acceptable. However, there are cases where a query can benefit from the use of a liveness scope.

## How liveness scopes work

Deephaven's liveness scopes allow the nodes in a refreshing query's update propagation graph to be cleaned up proactively by tracking the nodes' "liveness" (whether anything still needs them), rather than only via actions of the Java garbage collector. This does not replace garbage collection (GC), but it lets the engine clean up an object as soon as nothing needs it. Deephaven does this automatically, with internal reference counting. For developers building new functionality using the Deephaven query engine, the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) and [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) classes allow a finer degree of control over the reference counts of various query engine artifacts. Constructing an external scope before running a refreshing query holds together the related artifacts created in its query update propagation graph. Releasing the scope after the query runs releases any referents that are no longer "live".

When a table is live in Deephaven, the update propagation graph accumulates parent nodes that produce data (data sources) at the top, and all the child nodes that stream down the graph. Deephaven's liveness system tracks references to these nodes. When an object's reference count drops to zero, the engine cleans it up immediately, without waiting for GC: a cleaned-up table stops updating and releases its references to its parents, which can then be cleaned up in turn.

## Demonstrating the problem

Before we demonstrate liveness scopes in action, let's demonstrate the problem that liveness scopes solve.

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

The above example works, but the query creates many objects that are not needed after it is run. The [`LivenessScope`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) can manage these objects and release them when they are no longer needed, so the engine stops maintaining objects the query no longer uses.

The following example runs the same query inside a `LivenessScope`, hands the two tables to keep to the enclosing scope, and then releases the scope.

```groovy order=null
import io.deephaven.engine.liveness.*
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, false) ) {

    crypto = readCsv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
    ).firstBy("Instrument").update("ID = Instrument", "Parent = (String)null").update("Timestamp = (Instant)null", "Exchange = (String)null", "Price = (Double)null", "Size = (Double)null")

    data = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv").update("ID = Long.toString(ii)", "Parent = Instrument")

    combo = merge(crypto, data)

    comboTree = combo.tree("ID", "Parent")

}

// Keep the tables to display by having the enclosing scope manage them
LivenessScopeStack.peek().manage(crypto)
LivenessScopeStack.peek().manage(comboTree)

// Release everything else the scope manages
scope.release()

data = null

combo = null
```

## How to use a `LivenessScope`

Deephaven automatically releases the objects the web UI requests from the server once the UI stops using them. The objects your query creates, however, are managed by the console session. To release them sooner, create a `LivenessScope` for your query. When the scope is released, the engine cleans up every object the scope manages that nothing else still references.

The following syntax creates a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) that can preface any Deephaven query:

```groovy skip-test
import io.deephaven.engine.liveness.*

// Create a new LivenessScope
scope = new LivenessScope()

// Add scope to the LivenessScopeStack. This makes the scope the current scope for the current thread.
LivenessScopeStack.push(scope)

// Your query here

// Remove the scope from the stack. This makes the previous scope current again.
LivenessScopeStack.pop(scope)

// Release the scope's references to the liveness referents it manages
scope.release()
```

> [!NOTE]
> This example is intended to illustrate how the LivenessScopeStack manages LivenessScopes. In practice, you should use try-with-resources blocks to manage scopes.

In the example above, we first import the `liveness` package, and then create a new [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html). Next, we push the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) onto the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html). This automatically opens the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html), and it manages any query artifacts created when the scope is open. After the query runs, `LivenessScopeStack.pop(scope)` removes the scope from the stack, and `scope.release()` releases the scope's references to the query artifacts it manages. The engine then cleans up any of those artifacts that nothing else references.

You can also enclose a scope in a try-with-resources block using the `LivenessScopeStack.open(LivenessScope, boolean)` method. Setting the second parameter to `true` automatically releases the scope when the block is exited.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, true) ) {

    // Your query here

}
```

`LivenessScopeStack.open()` can be called with no parameters to create an anonymous scope and automatically release it.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

try ( SafeCloseable ignored = LivenessScopeStack.open() ) {

    // Your query here, managed by the scope created above

}
```

## Multiple LivenessScopes

Liveness scopes can be nested. The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) uses the scopes in the order they are pushed onto the stack — that is, the active [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) is the one that was most recently pushed to the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html). When a scope is popped from the stack, the next scope in the stack becomes the active scope. This can be done manually with `LivenessScopeStack.push(scope)` and `LivenessScopeStack.pop(scope)`, but best practice is to use try-with-resources blocks.

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

## Methods

The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) class provides additional methods for controlling how the reference counts of various query engine artifacts are managed, such as the order of a scope on the stack.

- [`LivenessScopeStack.peek()`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) - Get the scope at the top of the current thread's scope stack, or the base manager if no scopes have been pushed but not popped on this thread. This determines which scope automatically manages new query artifacts.
- [`LivenessScopeStack.pop(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) - Pop the scope from the top of the current thread's scope stack.
- [`LivenessScopeStack.open(scope, true)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) - Push a scope onto the scope stack, and get a SafeCloseable that pops it. The first parameter specifies the scope; the second boolean parameter determines whether the scope should release when the result is closed. This is useful for enclosing scope usage in a try-with-resources block.
- [`LivenessScopeStack.open()`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) - Push an anonymous scope onto the scope stack, and get a SafeCloseable that pops it and then uses LivenessScope.release(). This is useful for enclosing a series of query engine actions whose results must be explicitly retained externally in order to preserve liveness.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
