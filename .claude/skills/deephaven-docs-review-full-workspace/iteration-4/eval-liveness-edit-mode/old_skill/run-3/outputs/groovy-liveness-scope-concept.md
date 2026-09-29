---
title: Liveness scopes
sidebar_label: Liveness scope
---

This guide explains what a liveness scope is, how the engine uses one to clean up objects it no longer needs, and how to create and use your own.

Liveness scopes give you a finer degree of control over when unneeded nodes in the [update graph](./table-update-model.md) are cleaned up. A node is a table, a tree table, or another engine object that takes part in liveness tracking (a _liveness referent_). A liveness scope automatically manages the reference counts of the referents created while it is open, and releasing the scope releases them.

## Why use a liveness scope?

Deephaven's engine runs in Java, where the Java Virtual Machine (JVM) handles garbage collection (GC). You have little control over when GC takes place: the JVM decides when to run it to minimize GC runtime and maximize the memory left available afterward.

Without an explicit liveness scope, you rely on GC to clean up nodes your query no longer references. Until GC runs, a refreshing node that nothing uses keeps processing updates. In most cases, this is acceptable. A liveness scope lets you decide when those nodes are released instead of waiting for GC, which is useful for queries that create many short-lived or intermediate objects.

## How liveness scopes work

Every liveness referent carries a reference count. A liveness scope, or another referent, increments that count when it _manages_ the referent. When the count drops to zero, the engine destroys the referent immediately, without waiting for GC: a refreshing table removes its listener from its parent, so it stops receiving updates, and drops its own references to the parents it manages. A parent whose count then drops to zero is destroyed in turn. The memory these objects occupied is still reclaimed by the JVM's garbage collector.

The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) holds a per-thread stack of liveness scopes, and each new referent is managed by the scope at the top of the stack. When you run code in the console, the console session's own scope is at the top of the stack, so it manages everything your code creates and doesn't release those objects until the session closes. Pushing your own [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) onto the stack makes it manage the objects created while it is on top. When you release it, everything it manages that no other scope or referent still manages is destroyed.

Destroying a referent has the most effect on refreshing tables, since it detaches them from the update graph. Static tables have no update listeners to remove.

## Demonstrating the problem

Before demonstrating liveness scopes in action, this section demonstrates the problem that liveness scopes solve.

This query creates a simple tree table grouped by `Instrument`. Two tables open in the UI: `crypto` and the tree table `comboTree`.

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

The above example works, but the query creates many intermediate objects that are not needed after it runs, and the console session's scope manages all of them until the session closes. A [`LivenessScope`](../reference/engine/LivenessScope.md) can manage these objects and release them when they are no longer needed.

The following example runs the same query inside a [`LivenessScope`](../reference/engine/LivenessScope.md). After the `try` block pops the scope from the stack, `LivenessScopeStack.peek` returns the enclosing scope, which manages `crypto` and `comboTree` so that they stay live. Releasing the scope then releases every other object it manages.

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

// Keep the results in the enclosing scope
LivenessScopeStack.peek().manage(crypto)
LivenessScopeStack.peek().manage(comboTree)

// Release everything else the scope manages
scope.release()

data = null

combo = null
```

## How to use a `LivenessScope`

The following syntax creates a `LivenessScope` that can preface any Deephaven query:

```groovy skip-test
import io.deephaven.engine.liveness.*

// Create a new LivenessScope
scope = new LivenessScope()

// Push the scope onto the LivenessScopeStack. This makes it the current scope for the current thread.
LivenessScopeStack.push(scope)

// Your query here

// Pop the scope from the stack. The scope still manages its referents.
LivenessScopeStack.pop(scope)

// Release the scope's references to the liveness referents it manages
scope.release()
```

> [!NOTE]
> This example illustrates how the `LivenessScopeStack` manages a `LivenessScope`. In practice, use try-with-resources blocks to manage scopes.

The example above first imports the `io.deephaven.engine.liveness` package and creates a new `LivenessScope`. Next, it pushes the scope onto the `LivenessScopeStack`, which makes it the current scope: it manages any query artifacts created while it is at the top of the stack. After the query (or queries) runs, `LivenessScopeStack.pop(scope)` removes the scope from the stack. Popping doesn't release anything — the scope still manages its artifacts until you call its `release` method.

You can also enclose a scope in a try-with-resources block using the `LivenessScopeStack.open(LivenessScope, boolean)` method. Setting the second parameter to `true` automatically releases the scope when the block exits.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, true) ) {

    // Your query here

}
```

Calling `LivenessScopeStack.open` with no arguments creates an anonymous scope and releases it automatically when the block exits.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

try ( SafeCloseable ignored = LivenessScopeStack.open() ) {

    // Your query here, managed by the anonymous scope

}
```

## Nested liveness scopes

Liveness scopes can be nested. The `LivenessScopeStack` uses scopes in the order they are pushed onto the stack — that is, the active `LivenessScope` is the one most recently pushed. When a scope is popped from the stack, the next scope in the stack becomes the active scope. You can do this manually with `LivenessScopeStack.push(scope)` and `LivenessScopeStack.pop(scope)`, but best practice is to use try-with-resources blocks.

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

The `LivenessScopeStack` class provides these methods for controlling which scope manages new query engine artifacts and when scopes are released:

- [`LivenessScopeStack.push(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) — Push a scope onto the current thread's scope stack.
- [`LivenessScopeStack.pop(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) — Pop the scope from the current thread's scope stack. The scope must be the current top of the stack. Popping a scope doesn't release it.
- [`LivenessScopeStack.peek`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) — Get the scope at the top of the current thread's scope stack, or the base manager if no scopes have been pushed but not popped on this thread. This determines which scope automatically manages new query artifacts.
- [`LivenessScopeStack.open(scope, releaseOnClose)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) — Push a scope onto the scope stack, and get a `SafeCloseable` that pops it. The first parameter specifies the scope; the second determines whether to release the scope when the `SafeCloseable` is closed. This is useful for enclosing scope usage in a try-with-resources block.
- [`LivenessScopeStack.open`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) (no arguments) — Push an anonymous scope onto the scope stack, and get a `SafeCloseable` that pops it and then releases it. This is useful for enclosing a series of query engine actions whose results must be explicitly retained externally in order to preserve liveness.
- [`LivenessScopeStack.computeEnclosed(computation, shouldEnclose, shouldManageResult)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#computeEnclosed(java.util.function.Supplier,boolean,java.util.function.Predicate)) — Perform a computation guarded by a new `LivenessScope` that is released before the method returns. The enclosing scope manages the result. `computeArrayEnclosed` does the same for a computation that returns an array.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [Execution context](./execution-context.md)
- [Incremental update model](./table-update-model.md)
