---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide explains what a liveness scope is, how it works, why a query can benefit from one, and how to use one.

A liveness scope controls when the nodes in the [query update graph](./table-update-model.md) that it manages are released. A node can be a table, a plot, or another query engine object.

## How liveness scopes work

Deephaven's engine runs in Java, where the JVM decides when garbage collection (GC) takes place. Users have little control over its timing. Rather than relying only on GC, the engine reference counts the nodes in the update graph. A node stays live — a refreshing table keeps processing updates — as long as at least one manager holds a reference to it. A refreshing table holds references to the tables it depends on, so its parents stay live as long as it does.

When a node's reference count reaches zero, the engine destroys it immediately: the node stops receiving updates and drops its references to its parents, which can bring their counts to zero in turn. A destroyed object may be unusable for later operations. The JVM still reclaims its memory later through GC.

Every new node is managed by the scope at the top of the current thread's [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html). When you run code in the console, that is the script session's own scope, which retains everything your code creates until the session closes. Otherwise, those objects are cleaned up only when the JVM garbage-collects them. A [`LivenessScope`](../reference/engine/LivenessScope.md) you push onto the stack manages the nodes created while it is on top instead. Releasing the scope drops its references to all of them, and any node that nothing else still references — another scope, a dependent table, or a client that has the table open — is destroyed at once.

## Why use a liveness scope?

Most queries don't need a liveness scope. It helps when a query creates refreshing intermediate tables that it needs only for a while: releasing the scope stops those tables from updating as soon as you are done with them, instead of whenever the JVM next garbage-collects them.

## Demonstrating the problem

Before showing liveness scopes in action, this section demonstrates the problem that they solve.

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

The above example works, but the query creates intermediate objects, such as `data` and `combo`, that are not needed after it runs. Setting their variables to `null` does not release them immediately, because the script session's scope still manages them. A `LivenessScope` can manage these objects and release them when they are no longer needed.

The following example runs the same query inside a `LivenessScope`. After the `try` block pops the scope, the next scope on the stack is the script session's, so `LivenessScopeStack.peek().manage` hands it the two tables to keep, `crypto` and `comboTree`. Then calling `release` on the scope releases everything else it manages.

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

// Keep these two tables live after the scope is released
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

// Pop the scope from the stack. This does not release the objects it manages.
LivenessScopeStack.pop(scope)

// Release the scope's references to the objects it manages
scope.release()
```

> [!NOTE]
> This example is intended to illustrate how the `LivenessScopeStack` manages liveness scopes. In practice, you should use try-with-resources blocks to manage scopes.

The example above first imports the `io.deephaven.engine.liveness` package and creates a new `LivenessScope`. Next, it pushes the scope onto the `LivenessScopeStack`, which makes it the current scope: it manages any query engine objects created while it is on top of the stack. After the query runs, `LivenessScopeStack.pop(scope)` removes the scope from the stack. Popping the scope does not release anything; calling `release` on the scope drops its references to the objects it manages, and any object that nothing else references stops updating.

You can also enclose a scope in a try-with-resources block using `LivenessScopeStack.open(scope, releaseOnClose)`. Setting `releaseOnClose` to `true` automatically releases the scope when the block exits.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, true) ) {

    // Your query here

}
```

Calling `LivenessScopeStack.open` with no arguments creates an anonymous scope, pushes it onto the stack, and releases it when the block exits.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

try ( SafeCloseable ignored = LivenessScopeStack.open() ) {

    // Your query here, managed by the anonymous scope

}
```

## Nested liveness scopes

Liveness scopes can be nested. The `LivenessScopeStack` uses scopes in the order they are pushed onto the stack — that is, the active scope is the one most recently pushed onto the stack. When a scope is popped from the stack, the next scope in the stack becomes the active scope. This can be done manually with `LivenessScopeStack.push(scope)` and `LivenessScopeStack.pop(scope)`, but best practice is to use try-with-resources blocks.

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

The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) class provides the following methods for controlling which scope manages new query engine objects:

- [`LivenessScopeStack.push(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) — Push a scope onto the current thread's scope stack.
- [`LivenessScopeStack.pop(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) — Pop the scope from the current thread's scope stack. The scope must be the current top of the stack. Popping a scope does not release it.
- [`LivenessScopeStack.peek`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) — Get the scope at the top of the current thread's scope stack, or the base manager if no scopes have been pushed but not popped on this thread. This determines which scope automatically manages new query engine objects.
- [`LivenessScopeStack.open(scope, releaseOnClose)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) — Push a scope onto the scope stack, and get a `SafeCloseable` that pops it. If `releaseOnClose` is `true`, closing the `SafeCloseable` also releases the scope. This is useful for enclosing scope usage in a try-with-resources block.
- [`LivenessScopeStack.open`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) with no arguments — Push an anonymous scope onto the scope stack, and get a `SafeCloseable` that pops it and then releases it. This is useful for enclosing a series of query engine actions whose results must be explicitly retained externally in order to preserve liveness.
- [`LivenessScopeStack.computeEnclosed`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#computeEnclosed(java.util.function.Supplier,boolean,java.util.function.Predicate)) — Perform a computation inside a new scope that is released before the method returns. If the given predicate accepts the result, the enclosing scope manages it, so the result stays live. `computeArrayEnclosed` does the same for a computation that returns an array.

To release a `LivenessScope` you created yourself, call its [`release`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html#release()) method.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
