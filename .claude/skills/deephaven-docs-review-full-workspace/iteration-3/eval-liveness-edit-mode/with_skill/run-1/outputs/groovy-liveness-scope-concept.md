---
title: Liveness scopes
sidebar_label: Liveness scope
---

A liveness scope controls how long the tables and other query objects created inside it stay alive. When you release a scope, every object it manages that nothing else still needs is cleaned up right away: a ticking table stops updating and leaves the [update graph](./table-update-model.md) immediately, rather than whenever the Java garbage collector (GC) happens to run.

Most queries don't need a liveness scope. They're useful when a query creates ticking tables it only needs temporarily — for example, a ticking table used only to take a snapshot, or tables built inside a method that runs repeatedly. This guide explains how Deephaven decides that an object is still alive, the problem that creates for temporary ticking tables, and how to use the [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) and [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) classes to solve it.

## How Deephaven tracks liveness

Deephaven's engine runs in Java, where the JVM decides when garbage collection happens. You have little control over its timing — the JVM schedules it to balance collection cost against available memory.

On top of garbage collection, the engine tracks whether each query object is still needed with reference counting. Objects that take part in this tracking — tables and the other objects the engine uses to keep them updating — are called _liveness referents_. Each referent is _managed_ by one or more _liveness managers_, and it stays alive as long as at least one manager holds a reference to it. When its last reference is dropped, the engine cleans it up immediately: a ticking table stops listening to its parents and is removed from the update graph.

A table that depends on a ticking parent keeps that parent alive, so a parent is never cleaned up while a live table still needs it.

Where a new object's references come from depends on who created it:

- Objects that a client such as the web UI requests from the server are managed by that client's session. They're cleaned up as soon as the client releases them — for example, when you close a table in the UI.
- Objects that your script creates are managed by the console's script session. The session only holds them _weakly_: once your code no longer refers to an object, the session doesn't keep it alive, but it isn't cleaned up until the JVM garbage-collects it.

That second case is the problem a liveness scope solves.

## The problem: temporary ticking tables

The following query creates a ticking table, takes a static snapshot of it, and then drops its own reference to the ticking table, since only the snapshot is needed:

```groovy ticking-table order=null
ticking = timeTable("PT1s").update("X = ii")
snap = ticking.snapshot()

ticking = null
```

`snap` is static and doesn't depend on `ticking`. But dropping the reference to `ticking` doesn't stop it: the time table and the `update` built on it stay in the update graph and keep recomputing every second until the JVM garbage-collects them, which might not happen for a long time. A query that does this repeatedly accumulates ticking tables that consume CPU and memory for no benefit.

## Using a liveness scope

Running the same query inside a liveness scope cleans up the ticking tables as soon as the scope closes. Calling `LivenessScopeStack.open` with no arguments creates a new scope and makes it the current scope until the try-with-resources block exits; any object created in the block is managed by that scope. To keep an object after the block, manage it with the scope that was current before the block opened:

```groovy ticking-table order=null
import io.deephaven.engine.liveness.LivenessScopeStack
import io.deephaven.util.SafeCloseable

// The scope that is current before the block opens - here, the script session
enclosing = LivenessScopeStack.peek()

try (SafeCloseable ignored = LivenessScopeStack.open()) {
    ticking = timeTable("PT1s").update("X = ii")
    snap = ticking.snapshot()
    // Keep the snapshot by handing it to the enclosing scope
    enclosing.manage(snap)
}

// The scope was released when the block exited, so the ticking tables have
// already stopped updating and are no longer usable
ticking = null
```

When the block exits, the scope releases everything it manages. `ticking` and its time table have no other managers, so they're cleaned up immediately. `snap` survives because the enclosing scope — the console's script session — also manages it.

Releasing a scope only cleans up objects that nothing else needs. If you keep a ticking table instead of a snapshot, its ticking parents stay alive because the kept table depends on them. Conversely, any object that isn't managed outside the scope can't be used after the scope is released, even if a variable still refers to it.

## Ways to open a liveness scope

The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) holds a stack of scopes for each thread. The scope at the top of the stack is the current scope, and it manages every new object created on that thread. There are three ways to put a scope on the stack.

### An anonymous scope

Calling `LivenessScopeStack.open` with no arguments creates an anonymous scope, pushes it onto the stack, and returns a `SafeCloseable` that pops and releases it. Use it in a try-with-resources block, as in the example above. This is the simplest option when everything you want to keep can be handed to an outer scope before the block exits.

### A scope you create yourself

Create a [`LivenessScope`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html) yourself when you want to decide later when to release its objects. `LivenessScopeStack.open(scope, releaseOnClose)` pushes the scope and returns a `SafeCloseable` that pops it. If `releaseOnClose` is `true`, closing also releases the scope. If it's `false`, the scope keeps managing its objects after the block exits, until you call the scope's `release` method:

```groovy ticking-table order=null
import io.deephaven.engine.liveness.LivenessScope
import io.deephaven.engine.liveness.LivenessScopeStack
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try (SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
    filtered = timeTable("PT1s").update("X = ii").where("X % 2 == 0")
}

// ... use filtered for a while, then stop it and the tables it depends on
scope.release()
```

A scope opened with `releaseOnClose` set to `false` isn't released automatically. Call `release` on every scope you create once you no longer need the objects it manages.

### Pushing and popping a scope manually

`LivenessScopeStack.push(scope)` and `LivenessScopeStack.pop(scope)` add and remove a scope without a try-with-resources block. Popping a scope only removes it from the stack — it doesn't release the scope or the objects it manages. Call the scope's `release` method for that:

```groovy order=null
import io.deephaven.engine.liveness.LivenessScope
import io.deephaven.engine.liveness.LivenessScopeStack

scope = new LivenessScope()

// Make scope the current scope for this thread
LivenessScopeStack.push(scope)

// Your query here

// Remove scope from the stack. Its objects are still live.
LivenessScopeStack.pop(scope)

// Release the objects the scope manages
scope.release()
```

> [!NOTE]
> Prefer try-with-resources blocks. If the query between `push` and `pop` throws an exception, `pop` never runs and the scope stays on the stack.

## Nested scopes

Liveness scopes can be nested. The current scope is always the one most recently pushed onto the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html); when it's popped, the scope beneath it becomes current again. A scope must be at the top of the stack when it's popped. Nested try-with-resources blocks keep this ordering for you:

```groovy
import io.deephaven.engine.liveness.LivenessScopeStack
import io.deephaven.util.SafeCloseable

try (SafeCloseable ignored = LivenessScopeStack.open()) {

    // Objects created here are managed by the outer anonymous scope

    try (SafeCloseable ignored2 = LivenessScopeStack.open()) {

        // Objects created here are managed by the inner anonymous scope
    }
}
```

## `LivenessScopeStack` methods

The [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) class provides these methods:

- [`LivenessScopeStack.push(scope)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) - Push a scope onto the current thread's scope stack, making it the current scope.
- [`LivenessScopeStack.pop(scope)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) - Pop the scope from the top of the current thread's scope stack. The scope must be at the top of the stack. Popping doesn't release the scope.
- [`LivenessScopeStack.peek()`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) - Get the scope at the top of the current thread's scope stack, or the thread's base manager if no scopes have been pushed but not popped on this thread. This determines which scope automatically manages new query objects.
- [`LivenessScopeStack.open(scope, releaseOnClose)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) - Push a scope onto the scope stack, and get a `SafeCloseable` that pops it. If `releaseOnClose` is `true`, closing the `SafeCloseable` also releases the scope.
- [`LivenessScopeStack.open()`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) - Push an anonymous scope onto the scope stack, and get a `SafeCloseable` that pops it and then releases it. Anything created in the scope that you want to keep must be managed by another scope before the `SafeCloseable` closes.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
