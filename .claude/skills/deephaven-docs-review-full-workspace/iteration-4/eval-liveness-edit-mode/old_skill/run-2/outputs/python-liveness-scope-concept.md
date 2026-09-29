---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide discusses liveness scopes. It covers what a liveness scope is, how to use one, and why queries can benefit from its use.

## Why use a liveness scope?

Liveness scopes give you a finer degree of control over cleanup of unreferenced nodes in the [query update graph](./table-update-model.md). A node in the update graph can be a table, plot, or any other object.

Deephaven's engine runs in Java, in which the JVM handles garbage collection (GC). You have little control over when garbage collection takes place, as the JVM tries to optimize when it occurs to minimize GC runtime and maximize the amount of memory left available after it runs.

By default, objects that a console command creates are managed by the script session, and are cleaned up only when they are garbage collected or when the session closes. Without a liveness scope, a query relies on the JVM's garbage collector to clean up unreferenced nodes in the update graph. In most cases, this is acceptable. However, some queries benefit from releasing the objects they no longer need at a time of their choosing, which is what a liveness scope provides.

## How liveness scopes work

A liveness scope automatically manages reference counting of the nodes created while it is open. Deephaven's liveness system cleans up the nodes in a refreshing query's update propagation graph proactively, by tracking each node's "liveness" (whether anything still needs it), rather than only through the actions of the Java garbage collector. This does not replace GC, but it does allow a node to be cleaned up as soon as nothing needs it anymore. The engine does this reference counting internally and automatically.

When a table is live in Deephaven, the update propagation graph accumulates parent nodes that produce data (data sources) at the top, and all the child nodes that stream down the graph. The liveness system keeps track of these referents: when a node's reference count drops to zero, the node is cleaned up immediately, without waiting for GC, and it releases its own references to its parents. Releasing a liveness scope drops the scope's references to everything it manages, so any of those objects that nothing else still needs are cleaned up.

## Demonstrating the problem

Before demonstrating liveness scopes in action, this section shows the problem that liveness scopes solve.

This query creates a simple tree table grouped by `Instrument`. Two tables open: `crypto` and `combo_tree`.

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

The above example works, but the query creates many objects that are not needed after it runs. A [`LivenessScope`](../reference/engine/LivenessScope.md) can manage these objects and release them when they are no longer needed. This is best practice because it allows Deephaven to conserve memory.

The following example runs the same query, but encloses it in a [`LivenessScope`](../reference/engine/LivenessScope.md). The tables that stay open after the query runs are passed to `preserve`, which hands them to the enclosing scope. Releasing the scope then cleans up everything else it manages that the preserved tables don't depend on.

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

    # Keep the tables that stay open after the scope is released
    scope.preserve(crypto)
    scope.preserve(combo_tree)

# Release everything else the scope manages
scope.release()

data = None
combo = None
```

## How to create a liveness scope

A liveness scope takes no input parameters. You can create one with the [`liveness_scope`](../reference/engine/liveness-scope.md) function or with the [`LivenessScope`](../reference/engine/LivenessScope.md) class directly:

- The `liveness_scope` function is intended to be used _only_ in a [`with`](https://peps.python.org/pep-0343/) block or as a function decorator. It releases everything it manages when the block or decorated function exits.
- The `LivenessScope` class gives a finer degree of control. A scope created from the class can be opened more than once, and you decide when to release the resources it manages.

```python skip-test
from deephaven.liveness_scope import liveness_scope, LivenessScope

# The function creates a scope that is open for the duration of the with block
with liveness_scope() as scope_from_function:
    ...

# The class creates a scope that you open and release yourself
scope_from_class = LivenessScope()
```

## How to use a liveness scope

A liveness scope, once created, has several methods:

- `manage(referent)` explicitly manages the object in this scope.
- `preserve(referent)` preserves the object in the next outer scope, so that it stays live after this scope is released. The scope must be the current (innermost open) scope when you call `preserve`.
- `unmanage(referent)` stops this scope from managing the given object.
- `open` opens a liveness scope. It is meant to be used in a `with` statement. This method is _only_ available to liveness scopes created directly from the class.
- `release` closes a liveness scope and releases all of its managed resources. This method is _only_ available to liveness scopes created directly from the class.

### The `liveness_scope` function

Using the function creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which can only be opened once. The function must be used in a `with` block or as a decorator. The scope automatically manages every liveness referent created within it, and releases them when the block or decorated function exits, except for objects passed to `preserve`.

```python skip-test
from deephaven.liveness_scope import liveness_scope
import deephaven.numpy as dhnp
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

### The `LivenessScope` class

Queries with many objects that eventually need to be cleaned up should consider creating the class directly, to control when those objects are released and their memory freed.

```python skip-test
from deephaven.liveness_scope import LivenessScope


def make_table_and_scope(a: int):
    scope = LivenessScope()
    with scope.open():
        ticking_table = some_ticking_source().where(f"A={a}")
        return ticking_table, scope


t1, s1 = make_table_and_scope(1)
# .. wait for a while
s1.release()
t2, s2 = make_table_and_scope(2)
# .. wait for a while again
s2.release()
```

## Choose between the function and the class

Queries with simpler use cases for a liveness scope typically find the function sufficient. It automatically manages objects created within it, and releases them when the `with` block or decorated function exits, unless you pass them to `preserve`.

For queries that need more fine-grained control over the lifespan of objects, use the class.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
