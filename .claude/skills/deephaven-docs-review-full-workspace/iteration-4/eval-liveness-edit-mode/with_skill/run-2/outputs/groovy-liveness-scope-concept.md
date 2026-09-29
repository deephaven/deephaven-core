---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide discusses liveness scopes. It covers what a liveness scope is, how to use one, and why queries can benefit from its use.

Liveness scopes give users a finer degree of control over cleanup of unreferenced nodes in the [query update graph](./table-update-model.md). A node in the update graph can be a table, plot, or any other object. A liveness scope automatically manages reference counting of the nodes created within it. Resources can be programmatically freed from a liveness scope.

## Why use a liveness scope?

Deephaven's engine runs in Java, in which garbage collection is handled by the JVM. Users have little control over when garbage collection takes place, as the JVM tries to optimize when it occurs to minimize GC runtime and maximize the amount of memory left available after it's run.

Liveness scopes give users much more control over when nodes leave the query update graph. Without a liveness scope, queries rely solely on the JVM to perform garbage collection to clean up unreferenced nodes in the update graph. In most cases, this is acceptable. However, there are cases where a query can benefit from the use of a liveness scope.

## How liveness scopes work

Deephaven tracks the "liveness" of each node in a refreshing query's update graph (whether it is still needed) with reference counting, so it can clean up a node proactively rather than only when the Java garbage collector runs. This does not replace garbage collection (GC). When a node's reference count drops to zero, the engine stops updating it right away, and GC reclaims its memory later. A liveness scope holds a reference to each node created while the scope is open, and drops those references when the scope is released.

For developers building new functionality using the Deephaven query engine, the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) and [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) classes allow a finer degree of control over the reference counts of various query engine artifacts. Constructing an external scope before running a refreshing query holds together the related artifacts created in its update graph. Releasing the scope after the query runs releases any referents that are no longer "live".

In the update graph, the nodes that produce data (data sources) sit at the top, and each derived table is a child of the tables it is computed from. A refreshing child holds a reference to its parents, so a parent stays live as long as any of its children do. When a child's reference count drops to zero, it releases its parents, and any parent with no remaining references is cleaned up in turn, without waiting for GC.

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

The above example works, but the query creates many objects that are not needed after it runs. A [`LivenessScope`](../reference/engine/LivenessScope.md) can manage these objects and release them when they are no longer needed, so the engine can clean them up without waiting for garbage collection.

This example runs the same query, but encloses it in a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html).

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

data = null

combo = null
```

The scope now manages every table the query created, and holds them until the scope is released. Because the example passes `false` to `LivenessScopeStack.open`, the scope isn't released when the block exits. The sections below show how to release it.

## How to use a `LivenessScope`

Deephaven's reference counting instrumentation only cleans up objects created purely for the GUI. This can be augmented by creating a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) for your query. When the scope is released, any objects that are no longer needed or are not refreshing are let go.

The following syntax creates a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) that can preface any Deephaven query:

```groovy skip-test
import io.deephaven.engine.liveness.*

// Create a new LivenessScope
scope = new LivenessScope()

// Add scope to the LivenessScopeStack. This makes the scope the current scope for the current thread.
LivenessScopeStack.push(scope)

// Your query here

// Remove the scope from the stack. This does not release it.
LivenessScopeStack.pop(scope)

// Release the scope's references to the liveness referents it manages.
scope.release()
```

> [!NOTE]
> This example is intended to illustrate how the LivenessScopeStack manages LivenessScopes. In practice, you should use try-with-resources blocks to manage scopes.

In the example above, we first import the `liveness` package, and then create a new [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html). Next, we push the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) onto the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html). This makes it the current scope, and it manages any query artifacts created while it is on top of the stack. After we have run our query (or queries) in the console, we use `LivenessScopeStack.pop(scope)` to remove the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) from the stack. Popping does not release the scope, so we then call `release` on the scope to drop its references to the query artifacts it was managing.

You can also enclose a scope in a try-with-resources block using the `LivenessScopeStack.open(LivenessScope, boolean)` method. Setting the second parameter to `true` automatically releases the scope when the block exits.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, true) ) {

    // Your query here

}
```

`LivenessScopeStack.open` can be called with no parameters to create an anonymous scope and automatically release it.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

try ( SafeCloseable ignored = LivenessScopeStack.open() ) {

    // Your query here, managed by an anonymous scope

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

- [`LivenessScopeStack.push(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) — Push a scope onto the current thread's scope stack, making it the current scope.
- [`LivenessScopeStack.peek`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) — Get the scope at the top of the current thread's scope stack, or the base manager if no scopes have been pushed but not popped on this thread. This determines which scope automatically manages new query artifacts.
- [`LivenessScopeStack.pop(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) — Pop the scope from the top of the current thread's scope stack. The scope must be at the top of the stack. Popping does not release the scope.
- [`LivenessScopeStack.open(scope, true)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) — Push a scope onto the scope stack, and get a `SafeCloseable` that pops it. The first parameter specifies the scope; the second boolean parameter determines whether the scope should release when the result is closed. This is useful for enclosing scope usage in a try-with-resources block.
- [`LivenessScopeStack.open()`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) — Push an anonymous scope onto the scope stack, and get a `SafeCloseable` that pops it and then releases the scope. This is useful for enclosing a series of query engine actions whose results must be explicitly retained externally in order to preserve liveness.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
