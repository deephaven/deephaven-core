---
title: Liveness scopes
sidebar_label: Liveness scope
---

A liveness scope lets you decide when the tables and other objects a query creates stop updating and leave the [query update graph](./table-update-model.md), rather than waiting for the Java garbage collector to find them. Most queries never need one. This guide explains how liveness works, the problem a liveness scope solves, and how to use the [`LivenessScope`](../reference/engine/LivenessScope.md) and [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) classes.

## How liveness works

Deephaven tracks whether each node in the update graph — a table, a plot, or another query object — is still needed by counting references to it. This is called the node's liveness. A table that depends on a refreshing parent holds a reference to that parent, and a liveness scope holds a reference to every node created on the same thread while the scope is open.

When a node's reference count drops to zero, Deephaven destroys it immediately: the node stops listening for updates from its parents and gives up its own references to them. A parent that nothing else needs is destroyed in turn, so cleanup can cascade up the graph.

Destroying a node takes it out of the update graph, but it doesn't free the node's memory. The JVM's garbage collector still does that, and it decides on its own when to run. Liveness changes _when a node stops doing work_, not when its memory is reclaimed.

## Why use a liveness scope?

Code you run in the console executes inside a scope that belongs to the console session, and that scope isn't released until the session ends. Until then, a refreshing table that no variable points to isn't destroyed. It keeps processing updates on every update cycle until the garbage collector reclaims it, which can take an unpredictable amount of time.

For most queries, that's fine. It matters when a query creates refreshing tables it needs only briefly — an aggregation used once to take a static snapshot, or tables created in a loop or inside a closure that runs repeatedly. In this example, the [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) table exists only to produce a [`snapshot`](../reference/table-operations/snapshot/snapshot.md):

```groovy ticking-table order=null
source = timeTable("PT1s").update("Sym = (ii % 2 == 0) ? `A` : `B`", "Price = ii * 1.5")

// The lastBy table is only needed long enough to take a static snapshot
latestPrices = source.lastBy("Sym").snapshot()
```

No variable refers to the `lastBy` table, but it keeps updating every second until the garbage collector reclaims it.

Wrapping the same query in a liveness scope ends the `lastBy` table's work as soon as the block exits:

```groovy ticking-table order=null
import io.deephaven.engine.liveness.LivenessScopeStack
import io.deephaven.util.SafeCloseable

source = timeTable("PT1s").update("Sym = (ii % 2 == 0) ? `A` : `B`", "Price = ii * 1.5")

// The console session's scope, which is current before the new scope opens
def outerScope = LivenessScopeStack.peek()

try (SafeCloseable ignored = LivenessScopeStack.open()) {
    latestPrices = source.lastBy("Sym").snapshot()
    // Keep the snapshot after the new scope is released
    outerScope.manage(latestPrices)
}
```

When the `try` block exits, Deephaven releases the scope, and the `lastBy` table is destroyed. `latestPrices` survives because the console session's scope also manages it.

Releasing a scope destroys every object it manages that nothing else still needs, _even if a Groovy variable still refers to it_. Liveness counts references from the query graph and from scopes, not from variables. Manage anything you want to keep using in an outer scope before the inner scope is released.

## Create and use a liveness scope

Scopes live on the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html), a per-thread stack. The scope at the top of the stack is the current scope, and it manages every new object created on that thread. Use a try-with-resources block to put a scope on the stack, so the scope comes off the stack even if the query throws an exception.

`LivenessScopeStack.open` with no arguments creates an anonymous scope and releases it when the block exits:

```groovy
import io.deephaven.engine.liveness.LivenessScopeStack
import io.deephaven.util.SafeCloseable

try (SafeCloseable ignored = LivenessScopeStack.open()) {

    // Your query here, managed by the anonymous scope

}
```

To keep a reference to the scope, create a [`LivenessScope`](../reference/engine/LivenessScope.md) yourself and pass it to `LivenessScopeStack.open(scope, releaseOnClose)`. When the second argument is `true`, the scope is released when the block exits. When it's `false`, the scope only comes off the stack, and everything it manages stays live until you call `release` on the scope:

```groovy
import io.deephaven.engine.liveness.LivenessScope
import io.deephaven.engine.liveness.LivenessScopeStack
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try (SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {

    // Your query here, managed by scope

}

// Later, when the query's results are no longer needed
scope.release()
```

### Push and pop a scope manually

`LivenessScopeStack.open` is a wrapper around `LivenessScopeStack.push` and `LivenessScopeStack.pop`. You can call them directly, but a try-with-resources block is safer, because an exception between `push` and `pop` leaves the scope on the stack:

```groovy skip-test
import io.deephaven.engine.liveness.LivenessScope
import io.deephaven.engine.liveness.LivenessScopeStack

scope = new LivenessScope()

// Make scope the current scope for this thread
LivenessScopeStack.push(scope)

// Your query here

// Remove scope from the stack. Its objects stay live until the scope is released.
LivenessScopeStack.pop(scope)

// Release the scope's references to the objects it manages
scope.release()
```

`pop` only removes the scope from the stack; it doesn't release anything. The objects the scope manages stay live until you call `release` on the scope.

### Nested scopes

Liveness scopes can be nested. The most recently opened scope is the current one, and when it comes off the stack, the scope below it becomes current again:

```groovy
import io.deephaven.engine.liveness.LivenessScopeStack
import io.deephaven.util.SafeCloseable

try (SafeCloseable ignored = LivenessScopeStack.open()) {

    // Your query here, managed by an anonymous scope

    try (SafeCloseable ignored2 = LivenessScopeStack.open()) {

        // Your query here, managed by a second anonymous scope nested inside the first

    }
}
```

## `LivenessScopeStack` methods

The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) class provides these methods for putting scopes on the current thread's stack and taking them off:

- [`LivenessScopeStack.push(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) — Push a scope onto the current thread's scope stack.
- [`LivenessScopeStack.pop(scope)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) — Pop the scope from the current thread's scope stack. The scope must be at the top of the stack.
- [`LivenessScopeStack.peek`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) — Get the scope at the top of the current thread's scope stack, or the base manager if no scopes have been pushed but not popped on this thread. This determines which scope automatically manages new query artifacts.
- [`LivenessScopeStack.open(scope, releaseOnClose)`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) — Push a scope onto the scope stack, and get a `SafeCloseable` that pops it. The second parameter determines whether the scope is also released when the `SafeCloseable` is closed. This is useful for enclosing scope usage in a try-with-resources block.
- [`LivenessScopeStack.open`](https://deephaven.io/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) with no arguments — Push an anonymous scope onto the scope stack, and get a `SafeCloseable` that pops it and then releases it. This is useful for enclosing a series of query engine actions whose results must be explicitly retained externally in order to preserve liveness.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [Execution context](./execution-context.md)
- [Incremental update model](./table-update-model.md)
