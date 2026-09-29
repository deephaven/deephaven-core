---
title: Liveness scopes
sidebar_label: Liveness scope
---

A liveness scope controls how long the tables and other query objects created inside it stay alive. When you release a scope, every object it manages that nothing else still needs is cleaned up right away: a ticking table stops updating and leaves the [update graph](./table-update-model.md) immediately, rather than whenever the Java garbage collector (GC) happens to run.

Most queries don't need a liveness scope. They're useful when a query creates ticking tables it only needs temporarily — for example, a ticking table used only to take a snapshot, or tables built inside a function that runs repeatedly. This guide explains how Deephaven decides that an object is still alive, the problem that creates for temporary ticking tables, and how to use a liveness scope to solve it.

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

```python ticking-table order=null
from deephaven import time_table

ticking = time_table("PT1s").update("X = ii")
snap = ticking.snapshot()

ticking = None
```

`snap` is static and doesn't depend on `ticking`. But dropping the reference to `ticking` doesn't stop it: the time table and the `update` built on it stay in the update graph and keep recomputing every second until the JVM garbage-collects them, which might not happen for a long time. A query that does this repeatedly accumulates ticking tables that consume CPU and memory for no benefit.

## Using a liveness scope

Running the same query inside a liveness scope cleans up the ticking tables as soon as the scope closes. Any object created while a scope is open is managed by that scope. Call `preserve` on the objects you want to keep so they outlive the scope:

```python ticking-table order=null
from deephaven import time_table
from deephaven.liveness_scope import liveness_scope

with liveness_scope() as scope:
    ticking = time_table("PT1s").update("X = ii")
    snap = ticking.snapshot()
    # Keep the snapshot; it's managed by the enclosing scope from here on
    scope.preserve(snap)

# The scope was released when the block exited, so the ticking tables have
# already stopped updating and are no longer usable
ticking = None
```

When the `with` block exits, the scope releases everything it manages. `ticking` and its time table have no other managers, so they're cleaned up immediately. `snap` survives because `preserve` handed it to the next scope out — here, the console's script session.

Releasing a scope only cleans up objects that nothing else needs. If you preserve a ticking table instead of a snapshot, its ticking parents stay alive because the preserved table depends on them. Conversely, any object you _don't_ preserve can't be used after the scope is released, even if a variable still refers to it.

## The `liveness_scope` function and the `LivenessScope` class

Python offers two ways to create a liveness scope. Neither takes any arguments.

- The [`liveness_scope`](../reference/engine/liveness-scope.md) function opens a scope for the duration of a `with` block or a decorated function call, and releases it automatically when the block or call ends. It's the right choice for most queries.
- The [`LivenessScope`](../reference/engine/LivenessScope.md) class gives you a scope that you open with its `open` method and release yourself with its `release` method. Use it when the objects a scope manages need to outlive a single block — for example, when you want to keep a ticking table running for a while and release it later.

Both kinds of scope provide these methods for managing objects directly:

- `preserve(referent)` hands the object to the next scope outside this one, so it survives when this scope is released. Call it while this scope is open.
- `manage(referent)` adds the object to this scope. Objects created while the scope is open are already managed by it, so avoid managing them twice.
- `unmanage(referent)` stops this scope from managing the object, however it was managed to begin with.

Only scopes created from the `LivenessScope` class have these methods:

- `open` makes the scope current for the duration of a `with` block. You can open the same scope more than once.
- `release` releases the scope and every object it manages that nothing else still needs.

### The `liveness_scope` function

Use `liveness_scope()` in a `with` block, as in the example above, or as a function decorator. Either way, the scope is released when the block or function exits. As a decorator, it cleans up every object the function creates when the function returns, which suits functions that build temporary tables to compute a result:

```python ticking-table order=null
import deephaven.numpy as dhnp
from deephaven import time_table
from deephaven.liveness_scope import liveness_scope

source = time_table("PT1s").update(["Sym = ii % 2 == 0 ? `A` : `B`", "X = ii"])


@liveness_scope()
def latest_x():
    # These tables are released when the function returns
    latest = source.last_by("Sym")
    return dhnp.to_numpy(latest.snapshot(), cols=["X"])


values = latest_x()
```

`source` is created outside the function, so it isn't managed by the function's scope and keeps ticking. The `last_by` table and its snapshot are released on every return.

### The `LivenessScope` class

Create a `LivenessScope` directly when you decide later when to release its objects. In the following example, each call to `make_filtered_table` returns a ticking table along with the scope that manages it. Releasing the scope stops that table and the tables it depends on:

```python ticking-table order=null
from deephaven import time_table
from deephaven.liveness_scope import LivenessScope


def make_filtered_table(divisor: int):
    scope = LivenessScope()
    with scope.open():
        filtered = time_table("PT1s").update("X = ii").where(f"X % {divisor} == 0")
    return filtered, scope


t1, s1 = make_filtered_table(2)
# ... use t1 for a while, then stop it
s1.release()
t2, s2 = make_filtered_table(3)
```

A `LivenessScope` isn't released automatically. Call `release` on every scope you create once you no longer need the objects it manages; until then, they behave like any other script object.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
