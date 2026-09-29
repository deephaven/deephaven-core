---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide explains what a liveness scope is, how it works, and when a query benefits from one.

Deephaven uses reference counting to track which tables in the [query update graph](./table-update-model.md) are still in use. A liveness scope groups the tables and other query engine objects you create so you can release them all at once. Released tables stop updating right away, instead of when the JVM garbage-collects them. Most queries don't need a liveness scope. It's for queries that create ticking tables and later discard them.

## Why use a liveness scope?

Deephaven's engine runs in Java, where the JVM decides when garbage collection (GC) happens. A ticking table that your code no longer references doesn't leave the update graph until GC reclaims it, and until then it keeps processing updates every cycle.

For most queries, that delay doesn't matter. It matters when a query repeatedly creates ticking tables and throws them away — for example, a function that builds a new filtered table each time it's called. The discarded tables keep using CPU and memory until the next collection. A liveness scope lets you release them at a point you choose.

## How liveness scopes work

Tables, and other query engine objects such as partitioned tables and tree tables, are _liveness referents_: each has a reference count, and when the count drops to zero, the engine destroys the object. A destroyed table stops listening for updates and gives up its references to its parents. A refreshing table holds a reference to each refreshing parent it depends on, so a parent stays live as long as any child still needs it.

Each new object is managed by the innermost open liveness scope on the current thread. In the console, that's the script session's own scope, which keeps objects until they're garbage-collected or the session closes. When you open your own scope, it manages the objects you create instead. Releasing the scope drops its references, and any object that nothing else references — no enclosing scope, no child table, no client such as the web UI — is destroyed immediately.

## Demonstrating the problem

The following query creates three ticking tables, then removes every variable that refers to them:

```python order=null
from deephaven import time_table

source = time_table("PT1s").update("X = ii % 5")
filtered = source.where("X > 1")
latest = filtered.last_by("X")

# The tables are no longer needed
source = None
filtered = None
latest = None
```

The console can no longer reach the tables, but they stay in the update graph and keep updating every second until the JVM garbage-collects them.

The next query creates the same tables inside a [`LivenessScope`](../reference/engine/LivenessScope.md). Releasing the scope destroys the tables immediately, so they stop updating at a point you choose:

```python order=null
from deephaven.liveness_scope import LivenessScope
from deephaven import time_table

scope = LivenessScope()

with scope.open():
    source = time_table("PT1s").update("X = ii % 5")
    filtered = source.where("X > 1")
    latest = filtered.last_by("X")

# Later, when the tables are no longer needed:
source = None
filtered = None
latest = None
scope.release()
```

## How to create a liveness scope

The `deephaven.liveness_scope` module offers two ways to create a scope, and neither takes any arguments:

- The [`liveness_scope`](../reference/engine/liveness-scope.md) function opens a scope for the duration of a [`with`](https://peps.python.org/pep-0343/) block or a decorated function, and releases it when the block or function exits. Use it only in a `with` statement or as a decorator — calling it on its own doesn't open a scope.
- The [`LivenessScope`](../reference/engine/LivenessScope.md) class creates a scope that you open with `open` and release with `release`. You can open it more than once, and you decide when to release it.

```python order=null
from deephaven.liveness_scope import liveness_scope, LivenessScope

# Opened when the block starts, released when it exits
with liveness_scope() as scope_from_function:
    pass

# Opened and released explicitly
scope_from_class = LivenessScope()
with scope_from_class.open():
    pass
scope_from_class.release()
```

## How to use a liveness scope

While a scope is open, it automatically manages every object created in it. Both kinds of scope also have three methods for managing objects directly:

- `manage(referent)` makes the scope manage the object.
- `preserve(referent)` makes the next scope out manage the object, so the object stays live after this scope is released. In the console, the next scope out is usually the script session's scope. Call `preserve` on the innermost open scope.
- `unmanage(referent)` stops the scope from managing the object.

Only a `LivenessScope` created from the class has `open` and `release`. `open` makes the scope the current scope for the duration of a `with` block. Exiting the block doesn't release the scope. `release` drops the scope's references to everything it manages, which destroys any object that nothing else references.

### The `liveness_scope` function

`liveness_scope` yields a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which is released when the `with` block or decorated function exits. Anything created inside it that you don't preserve is released at that point.

In the first function below, `preserve` keeps the joined table live after the `with` block exits, while `ticking_table` is released. In the second, the decorator releases `ticking_table` after `get_values` copies its data into a NumPy array:

```python skip-test
import numpy as np
import numpy.typing as npt
import deephaven.numpy as dhnp
from deephaven.liveness_scope import liveness_scope


def get_joined_table():
    with liveness_scope() as scope:
        ticking_table = some_ticking_source()
        table = ticking_table.snapshot().join(table=other_ticking_table, on=key_cols)
        scope.preserve(table)
    return table


@liveness_scope()
def get_values() -> npt.NDArray[np.double]:
    ticking_table = some_ticking_source().last_by(["Sym"])
    return dhnp.to_numpy(ticking_table)
```

### The `LivenessScope` class

Create the class directly when a scope must outlive a single block of code — for example, to keep a table live until you replace it. The function below returns a table along with the scope that manages it. Releasing that scope later destroys the table:

```python skip-test
from deephaven.liveness_scope import LivenessScope


def make_table_and_scope(a: int):
    scope = LivenessScope()
    with scope.open():
        ticking_table = some_ticking_source().where(f"A = {a}")
        return ticking_table, scope


t1, s1 = make_table_and_scope(1)
# .. wait for a while
s1.release()  # t1 stops updating
t2, s2 = make_table_and_scope(2)
# .. wait for a while again
s2.release()  # t2 stops updating
```

## Choose the function or the class

Use `liveness_scope` when the objects' lifetime matches a block of code or a single function call. It manages everything created in the block or function and releases those objects on exit, except the ones you keep with `preserve`.

Use `LivenessScope` when you need to decide later when to release the objects, or to open the same scope more than once.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
