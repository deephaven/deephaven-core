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

The engine keeps a stack of liveness scopes, and each new referent is managed by the scope at the top of the stack. When you run code in the console, the console session's own scope is at the top of the stack, so it manages everything your code creates and doesn't release those objects until the session closes. Opening your own liveness scope puts it on top of the stack, so it manages the objects created while it is open. When you release it, everything it manages that no other scope or referent still manages is destroyed.

Destroying a referent has the most effect on refreshing tables, since it detaches them from the update graph. Static tables have no update listeners to remove.

## Demonstrating the problem

Before demonstrating liveness scopes in action, this section demonstrates the problem that liveness scopes solve.

This query creates a simple tree table grouped by `Instrument`. Two tables open in the UI: `crypto` and the tree table `combo_tree`.

```python order=null
from deephaven.csv import read as read_csv
from deephaven import merge

crypto = (
    read_csv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
    )
    .first_by("Instrument")
    .update(["ID = Instrument", "Parent = (String)null"])
    .update(
        [
            "Timestamp = (Instant)null",
            "Exchange = (String)null",
            "Price = (Double)null",
            "Size = (Double)null",
        ]
    )
)

data = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
).update(["ID = String.valueOf(ii)", "Parent = Instrument"])

combo = merge([crypto, data])

combo_tree = combo.tree("ID", "Parent")

data = None
combo = None
```

The above example works, but the query creates many intermediate objects that are not needed after it runs, and the console session's scope manages all of them until the session closes. A [`LivenessScope`](../reference/engine/LivenessScope.md) can manage these objects and release them when they are no longer needed.

The following example runs the same query inside a [`LivenessScope`](../reference/engine/LivenessScope.md). Inside the `with` block, `preserve` hands `crypto` and `combo_tree` to the enclosing scope so that they stay live. Releasing the scope then releases every other object it manages.

```python order=null
from deephaven.liveness_scope import LivenessScope
from deephaven.csv import read as read_csv
from deephaven import merge

scope = LivenessScope()

with scope.open():
    crypto = (
        read_csv(
            "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
        )
        .first_by("Instrument")
        .update(["ID = Instrument", "Parent = (String)null"])
        .update(
            [
                "Timestamp = (Instant)null",
                "Exchange = (String)null",
                "Price = (Double)null",
                "Size = (Double)null",
            ]
        )
    )

    data = read_csv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
    ).update(["ID = String.valueOf(ii)", "Parent = Instrument"])

    combo = merge([crypto, data])

    combo_tree = combo.tree("ID", "Parent")

    # Keep the results in the enclosing scope
    scope.preserve(crypto)
    scope.preserve(combo_tree)

# Release everything else the scope manages
scope.release()

data = None
combo = None
```

## How to create a liveness scope

Creating a liveness scope takes no input parameters. You can create one with the [`liveness_scope`](../reference/engine/liveness-scope.md) function or with the [`LivenessScope`](../reference/engine/LivenessScope.md) class directly. The function is intended to be used _only_ in a [`with`](https://peps.python.org/pep-0343/) block or as a function decorator. The class gives a finer degree of control: you can open a scope more than once, and you release the resources it manages explicitly.

```python order=null
from deephaven.liveness_scope import liveness_scope, LivenessScope

# The function creates and opens a scope for the duration of the with block
with liveness_scope() as scope_from_function:
    pass

# The class creates a scope that you open and release yourself
scope_from_class = LivenessScope()
scope_from_class.release()
```

## How to use a liveness scope

A liveness scope, once created, has several methods:

- `manage(referent)` explicitly manages the object in this scope.
- `preserve(referent)` manages the object in the next scope outside this one, so it stays live after this scope is released. Call it while this scope is open — that is, while it is at the top of the scope stack.
- `unmanage(referent)` causes this scope to no longer manage the given object.
- `open` opens a liveness scope for the duration of a `with` block. This method is _only_ available to liveness scopes created directly from the class.
- `release` releases a liveness scope and all of its managed resources. This method is _only_ available to liveness scopes created directly from the class.

### The function

The `liveness_scope` function creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which can only be opened once. Use it in a `with` block or as a decorator. The scope automatically manages every liveness referent created within it and releases them when the `with` block or decorated function exits, unless you `preserve` them.

```python skip-test
from deephaven.liveness_scope import liveness_scope
from deephaven import numpy as dhnp
import numpy as np
import numpy.typing as npt


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

### The class

Creating the class directly gives greater control. You can open the scope more than once, and you decide when it releases the resources it manages. If your query creates many objects that you eventually want cleaned up, consider using a liveness scope to control when they are released.

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
s2.release()
```

## To use the function or the class?

The function is typically sufficient for simpler use cases. It automatically manages the objects created within it and releases them when the `with` block or decorated function exits, unless you explicitly keep them with `preserve`.

For queries that need more fine-grained control over the lifespan of objects, use the class.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution context](./execution-context.md)
- [Incremental update model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
