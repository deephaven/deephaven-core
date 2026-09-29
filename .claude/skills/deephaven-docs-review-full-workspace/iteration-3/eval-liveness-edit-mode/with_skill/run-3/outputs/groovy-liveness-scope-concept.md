---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide explains what a liveness scope is, how it works, and when a query benefits from one.

Deephaven uses reference counting to track which tables in the [query update graph](./table-update-model.md) are still in use. A liveness scope groups the tables and other query engine objects you create so you can release them all at once. Released tables stop updating right away, instead of when the JVM garbage-collects them. Most queries don't need a liveness scope. It's for queries that create ticking tables and later discard them.

## Why use a liveness scope?

Deephaven's engine runs in Java, where the JVM decides when garbage collection (GC) happens. A ticking table that your code no longer references doesn't leave the update graph until GC reclaims it, and until then it keeps processing updates every cycle.

For most queries, that delay doesn't matter. It matters when a query repeatedly creates ticking tables and throws them away — for example, a method that builds a new filtered table each time it's called. The discarded tables keep using CPU and memory until the next collection. A liveness scope lets you release them at a point you choose.

## How liveness scopes work

Tables, and other query engine objects such as partitioned tables and tree tables, are _liveness referents_: each has a reference count, and when the count drops to zero, the engine destroys the object. A destroyed table stops listening for updates and gives up its references to its parents. A refreshing table holds a reference to each refreshing parent it depends on, so a parent stays live as long as any child still needs it.

Each thread has a [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html), and each new object is managed by the scope at the top of it. In the console, that's the script session's own scope, which keeps objects until they're garbage-collected or the session closes. When you push your own [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) onto the stack, it manages the objects you create instead. Releasing the scope drops its references, and any object that nothing else references — no child table, no enclosing scope, no client such as the web UI — is destroyed immediately.

## Demonstrating the problem

The following query creates three ticking tables, then removes every variable that refers to them:

```groovy order=null
source = timeTable("PT1s").update("X = ii % 5")
filtered = source.where("X > 1")
latest = filtered.lastBy("X")

// The tables are no longer needed
source = null
filtered = null
latest = null
```

The console can no longer reach the tables, but they stay in the update graph and keep updating every second until the JVM garbage-collects them.

The next query creates the same tables inside a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html). Releasing the scope destroys the tables immediately, so they stop updating at a point you choose:

```groovy order=null
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try (SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
    source = timeTable("PT1s").update("X = ii % 5")
    filtered = source.where("X > 1")
    latest = filtered.lastBy("X")
}

// Later, when the tables are no longer needed:
source = null
filtered = null
latest = null
scope.release()
```

## How to use a `LivenessScope`

A scope manages the objects created while it's at the top of the `LivenessScopeStack`. The following example shows the underlying steps — push the scope, run the query, pop the scope, and release it:

```groovy skip-test
import io.deephaven.engine.liveness.*

// Create a new LivenessScope
scope = new LivenessScope()

// Push the scope onto the LivenessScopeStack. This makes it the current scope for the current thread.
LivenessScopeStack.push(scope)

// Your query here

// Remove the scope from the stack. The scope still manages the objects created above.
LivenessScopeStack.pop(scope)

// Release the scope's references to the objects it manages
scope.release()
```

> [!NOTE]
> This example illustrates how the `LivenessScopeStack` manages scopes. In practice, use a try-with-resources block, as shown below, so the scope is popped even if the query throws an exception.

While the scope is on the stack, it manages every object the query creates. Popping it only removes it from the stack — the scope still holds its references. Calling `release` drops those references, so any object that nothing else references is destroyed.

You can also enclose a scope in a try-with-resources block with `LivenessScopeStack.open(scope, releaseOnClose)`. The block pops the scope when it exits, and if the second argument is `true`, it releases the scope too.

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try (SafeCloseable ignored = LivenessScopeStack.open(scope, true)) {

    // Your query here

}
```

Calling `LivenessScopeStack.open` with no arguments creates an anonymous scope, which the block pops and releases when it exits:

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

try (SafeCloseable ignored = LivenessScopeStack.open()) {

    // Your query here, managed by the anonymous scope

}
```

To keep a result after its scope is released, have the enclosing scope manage it before the block exits. `LivenessScopeStack.peek` returns the scope at the top of the stack, so call it before opening your own scope. In the following example, the ticking tables stop updating when the block exits, and the static snapshot `snap` stays live:

```groovy order=null
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

outerScope = LivenessScopeStack.peek()

try (SafeCloseable ignored = LivenessScopeStack.open()) {
    source = timeTable("PT1s").update("X = ii % 5")
    snap = source.lastBy("X").snapshot()
    outerScope.manage(snap)
}

source = null
```

## Nested liveness scopes

Liveness scopes can be nested. The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) uses scopes in the order they're pushed onto the stack — that is, the active scope is the one most recently pushed. When a scope is popped, the next scope on the stack becomes the active scope. You can push and pop scopes manually with `LivenessScopeStack.push(scope)` and `LivenessScopeStack.pop(scope)`, but best practice is to use try-with-resources blocks. Objects created in the inner block are released when it exits, unless something outside it still references them:

```groovy
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable


try (SafeCloseable ignored = LivenessScopeStack.open()) {

    // Your query here, managed by an anonymous scope

    try (SafeCloseable ignored2 = LivenessScopeStack.open()) {

        // Your query here, managed by a second anonymous scope that is enclosed by the first
    }
}
```

## Methods

The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) class provides these methods for controlling which scope manages new query engine objects:

- [`LivenessScopeStack.push(scope)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) — Push a scope onto the current thread's scope stack, making it the scope that manages new objects.
- [`LivenessScopeStack.pop(scope)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) — Pop the scope from the top of the current thread's scope stack. The scope must be at the top of the stack. Popping a scope doesn't release it.
- [`LivenessScopeStack.peek`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) — Get the scope at the top of the current thread's scope stack, or the thread's base manager if no scopes have been pushed but not popped on this thread. This scope manages new query engine objects.
- [`LivenessScopeStack.open(scope, releaseOnClose)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) — Push a scope onto the scope stack, and get a `SafeCloseable` that pops it. If `releaseOnClose` is `true`, closing the `SafeCloseable` also releases the scope. This is useful for enclosing scope usage in a try-with-resources block.
- [`LivenessScopeStack.open`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) with no arguments — Push an anonymous scope onto the scope stack, and get a `SafeCloseable` that pops it and then releases it. This is useful for enclosing a series of query engine actions whose results you must explicitly retain elsewhere to keep them live.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
