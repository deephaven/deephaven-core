---
title: How to use liveness scopes
sidebar_label: Liveness Scopes
---

Deephaven's liveness scopes allow the nodes in a refreshing query's update propagation graph to be updated proactively by assessing the nodes' "liveness" (whether or not they are active), rather than only via actions of the Java garbage collector. This does not replace garbage collection (GC), but it does allow cleanup to happen immediately when objects in the GUI are not needed. This is accomplished internally via reference counting, and works automatically for all users. For developers building new functionality using the Deephaven query engine, the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) and [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) classes allow a finer degree of control over the reference counts of various query engine artifacts. Constructing an external scope before running a refreshing query will hold together the related artifacts created in its query update propagation graph. Releasing the scope after the query runs will release any referents that are no longer "live".

When a table is live in Deephaven, the update propagation graph accumulates parent nodes that produce data (data sources) at the top, and all the child nodes that stream down the graph. Deephaven's liveness scope system keeps track of these referents: when child objects cease to be referenced and the parents' liveness count goes down, they will be cleaned up immediately without waiting for GC.

Before we demonstrate liveness scopes in action, let's demonstrate the problem that liveness scopes solve.

This query creates a simple tree table grouped by Sym. Two tables will open: `crypto` and `data`.

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

The above example works, but the query creates many objects that are not needed after it is run. The [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) can manage these objects and release them when they are no longer needed. This is best practice because it allows Deephaven to conserve memory.

In this example, we will run the same query, but enclose it in a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html).

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

## Using a LivenessScope

Deephaven's reference counting instrumentation will only clean up objects created purely for the GUI. This can be augmented by creating a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) for your query. When the scope is released, any objects that are no longer needed or are not refreshing are let go.

The following syntax creates a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) that can preface any Deephaven query:

```groovy skip-test
import io.deephaven.engine.liveness.*

// Create a new LivenessScope
scope = new LivenessScope()

// Add scope to the LivenessScopeStack. This makes the scope the current scope for the current thread.
LivenessScopeStack.push(scope)

// Your query here

// Release the scope from the stack. This will release the scope's references to the Liveness Referents it manages.
LivenessScopeStack.pop(scope)
```

> [!NOTE]
> This example is intended to illustrate how the LivenessScopeStack manages LivenessScopes. In practice, you should use try-with-resources blocks to manage scopes.

In the example above, we first import the `liveness` package, and then create a new [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html). Next, we push the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) onto the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html). This automatically opens the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html), and it will manage any query artifacts created when the scope is open. After we have run our query (or queries) in the console, we use `LivenessScopeStack.pop(scope)` to remove the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) from the stack, making the query artifacts it was managing eligible for garbage collection.

You can also enclose a scope in a try-with-resources block using the `LivenessScopeStack.open(LivenessScope, boolean)` method. Setting the second parameter to `true` will automatically release the scope when the block is exited.

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

Liveness scopes can be nested. The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) will use the scopes in the order they are pushed onto the stack - that is, the active [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) will be the one that was most recently pushed to the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html). When a scope is popped from the stack, the next scope in the stack will become the active scope. This can be done manually with `LivenessScopeStack.push(scope)` and `LivenessScopeStack.pop(scope)`, but best practice is to use try-with-resources blocks.

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
- [`LivenessScopeStack.open()`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) - Push an anonymous scope onto the scope stack, and get a SafeCloseable that pops it and then uses LivenessScope.release(). This is useful enclosing a series of query engine actions whose results must be explicitly retained externally in order to preserve liveness.
