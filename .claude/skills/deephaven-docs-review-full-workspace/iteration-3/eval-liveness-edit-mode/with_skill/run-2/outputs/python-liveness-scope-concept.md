---
title: Liveness scopes
sidebar_label: Liveness scope
---

A liveness scope lets you decide when the tables and other objects a query creates stop updating and leave the [query update graph](./table-update-model.md), rather than waiting for the Java garbage collector to find them. Most queries never need one. This guide explains how liveness works, the problem a liveness scope solves, and how to use the [`liveness_scope`](../reference/engine/liveness-scope.md) function and the [`LivenessScope`](../reference/engine/LivenessScope.md) class.

## How liveness works

Deephaven tracks whether each node in the update graph — a table, a plot, or another query object — is still needed by counting references to it. This is called the node's liveness. A table that depends on a refreshing parent holds a reference to that parent, and a liveness scope holds a reference to every node created on the same thread while the scope is open.

When a node's reference count drops to zero, Deephaven destroys it immediately: the node stops listening for updates from its parents and gives up its own references to them. A parent that nothing else needs is destroyed in turn, so cleanup can cascade up the graph.

Destroying a node takes it out of the update graph, but it doesn't free the node's memory. The JVM's garbage collector still does that, and it decides on its own when to run. Liveness changes _when a node stops doing work_, not when its memory is reclaimed.

## Why use a liveness scope?

Code you run in the console executes inside a scope that belongs to the console session, and that scope isn't released until the session ends. Until then, a refreshing table that no variable points to isn't destroyed. It keeps processing updates on every update cycle until the garbage collector reclaims it, which can take an unpredictable amount of time.

For most queries, that's fine. It matters when a query creates refreshing tables it needs only briefly — an aggregation used once to take a static snapshot, or tables created in a loop or inside a function that runs repeatedly. In this example, the [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) table exists only to produce a [`snapshot`](../reference/table-operations/snapshot/snapshot.md):

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT1s").update(
    ["Sym = (ii % 2 == 0) ? `A` : `B`", "Price = ii * 1.5"]
)

# The last_by table is only needed long enough to take a static snapshot
latest_prices = source.last_by("Sym").snapshot()
```

No variable refers to the `last_by` table, but it keeps updating every second until the garbage collector reclaims it.

Wrapping the same query in a liveness scope ends the `last_by` table's work as soon as the block exits:

```python ticking-table order=null
from deephaven import time_table
from deephaven.liveness_scope import liveness_scope

source = time_table("PT1s").update(
    ["Sym = (ii % 2 == 0) ? `A` : `B`", "Price = ii * 1.5"]
)

with liveness_scope() as scope:
    latest_prices = source.last_by("Sym").snapshot()
    # Keep the snapshot after the scope is released
    scope.preserve(latest_prices)
```

When the `with` block exits, Deephaven releases the scope, and the `last_by` table is destroyed. `latest_prices` survives because `preserve` hands it to the next scope out — here, the console session's scope.

Releasing a scope destroys every object it manages that nothing else still needs, _even if a Python variable still refers to it_. Liveness counts references from the query graph and from scopes, not from variables. Preserve anything you want to keep using after the scope is released.

## Create and use a liveness scope

Python offers two ways to create a liveness scope:

- The [`liveness_scope`](../reference/engine/liveness-scope.md) function opens a scope for one `with` block or one function call and releases it automatically when that block or call ends. Use it when the scope's lifetime matches a block of code.
- The [`LivenessScope`](../reference/engine/LivenessScope.md) class creates a scope that you open, possibly more than once, and release yourself. Use it when the objects must stay live for a while after the code that creates them finishes.

### The `liveness_scope` function

`liveness_scope` works in a [`with`](https://peps.python.org/pep-0343/) statement, as shown above, or as a function decorator. It creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which Deephaven opens and releases for you; you can't reopen it. Calling `liveness_scope` outside of a `with` statement or decorator doesn't open a scope.

As a decorator, `liveness_scope` opens a fresh scope for each call to the function and releases it when the function returns. In this example, each call creates a temporary `last_by` table and returns a copy of its data as a NumPy array with [`to_numpy`](../reference/numpy/to-numpy.md). The table stops updating as soon as the function returns:

```python ticking-table order=null
from deephaven import time_table
from deephaven.liveness_scope import liveness_scope
import deephaven.numpy as dhnp

source = time_table("PT1s").update(
    ["Sym = (ii % 2 == 0) ? `A` : `B`", "Price = ii * 1.5"]
)


@liveness_scope()
def get_latest_prices():
    latest = source.last_by("Sym")
    return dhnp.to_numpy(latest, cols=["Price"])


prices = get_latest_prices()
```

### The `LivenessScope` class

A `LivenessScope` stays open only inside a `with scope.open():` block, but you choose when to release it. Until you call `release`, every object it manages stays live. In this example, a function returns a filtered ticking table along with the scope that manages it, so the caller can release the table when it's done with it:

```python ticking-table order=null
from deephaven import time_table
from deephaven.liveness_scope import LivenessScope


def make_filtered_table(sym: str):
    scope = LivenessScope()
    with scope.open():
        source = time_table("PT1s").update(
            ["Sym = (ii % 2 == 0) ? `A` : `B`", "Price = ii * 1.5"]
        )
        filtered = source.where(f"Sym = `{sym}`")
    return filtered, scope


table_a, scope_a = make_filtered_table("A")

# When table_a is no longer needed, release its scope
scope_a.release()
table_a = None
```

Releasing `scope_a` destroys both `filtered` and its `source`, and `source` stops ticking.

### Scope methods

Both kinds of scope provide these methods:

- `preserve(referent)` hands the object to the next scope outside this one, so it stays live after this scope is released. Call it while this scope is open.
- `manage(referent)` makes this scope manage the object. Don't manage an object that the scope already manages automatically.
- `unmanage(referent)` stops this scope from managing the object.

Only scopes created from the `LivenessScope` class have these methods:

- `open` makes the scope the current scope for the duration of a `with` block. You can open the same scope more than once.
- `release` releases the scope and every object it manages that nothing else still needs.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution context](./execution-context.md)
- [Incremental update model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
