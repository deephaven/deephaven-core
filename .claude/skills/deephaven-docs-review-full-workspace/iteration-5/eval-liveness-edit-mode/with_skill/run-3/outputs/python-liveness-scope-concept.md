---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide discusses liveness scopes. It covers what a liveness scope is, how to use one, and why queries can benefit from its use.

Liveness scopes give users a finer degree of control over cleanup of unreferenced nodes in the [query update graph](./table-update-model.md). A node in the update graph can be a table, a plot, or another query engine object. A liveness scope automatically manages reference counting of the nodes created within it. Resources can be programmatically freed from a liveness scope.

## Why use a liveness scope?

Deephaven's engine runs in Java, in which garbage collection is handled by the JVM. Users have little control over when garbage collection takes place, as the JVM tries to optimize when it occurs to minimize GC runtime and maximize the amount of memory left available after it's run.

Liveness scopes give users much more control over when nodes in the query update graph are cleaned up. Without a liveness scope, queries rely solely on the JVM to perform garbage collection to clean up unreferenced nodes in the DAG. In most cases, this is acceptable. However, there are cases where a query can benefit from the use of a liveness scope.

## How liveness scopes work

Deephaven's liveness scopes allow the nodes in a refreshing query's update propagation graph to be cleaned up proactively by assessing the nodes' "liveness" (whether or not they are active), rather than only via actions of the Java garbage collector. This does not replace garbage collection (GC), but it does allow cleanup to happen immediately when objects in the GUI are not needed. This is accomplished internally via reference counting, and works automatically for all users.

When a table is live in Deephaven, the update propagation graph accumulates parent nodes that produce data (data sources) at the top, and all the child nodes that stream down the graph. Deephaven's liveness scope system keeps track of these referents: when an object's reference count drops to zero, it is cleaned up immediately, without waiting for GC, and it drops its own references to its parents.

## Demonstrating the problem

Before we demonstrate liveness scopes in action, let's demonstrate the problem that liveness scopes solve.

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

The above example works, but the query creates many objects that are not needed after it is run. A [`LivenessScope`](../reference/engine/LivenessScope.md) can manage these objects and release them when they are no longer needed. Releasing the scope cleans them up immediately, rather than waiting for garbage collection.

The following example runs the same query inside a `LivenessScope`. It preserves the two tables to keep, then releases the scope, which cleans up everything else the scope manages.

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

    scope.preserve(crypto)
    scope.preserve(combo_tree)

scope.release()

data = None
combo = None
```

## How to create a liveness scope

Creating a liveness scope is easy, as it takes no input parameters. It can be created from the [`liveness_scope`](../reference/engine/liveness-scope.md) function or the [`LivenessScope`](../reference/engine/LivenessScope.md) class directly. The former is intended to be used _only_ in a [`with`](https://peps.python.org/pep-0343/) block or as a function decorator. The latter gives a finer degree of control by allowing a scope to be opened more than once. It also allows the explicit release of resources that it manages.

```python order=null
from deephaven.liveness_scope import liveness_scope, LivenessScope

# liveness_scope creates and opens a scope when the with block is entered
with liveness_scope() as scope_from_function:
    pass

scope_from_class = LivenessScope()
```

## How to use a liveness scope

Deephaven's reference counting instrumentation only cleans up objects created purely for the GUI. This can be augmented by creating a `LivenessScope` for your query. When the scope is released, it drops its references to everything it manages, and any of those objects that nothing else still references are cleaned up.

A liveness scope, once created, has several methods that can be used:

- `manage(referent)` explicitly manages the object in this scope.
- `preserve(referent)` keeps the object live in the next scope outside this one. Call it while this scope is open.
- `unmanage(referent)` causes this scope to no longer manage the given object.
- `open` opens a liveness scope. This is meant to be used in a `with` statement. This method is _only_ available to liveness scopes created directly from the class.
- `release` closes a liveness scope and releases all of its managed resources. This method is _only_ available to liveness scopes created directly from the class.

### The `liveness_scope` function

Using the function creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which can only be opened once. If you use the function, use it in a `with` block or as a decorator. The scope automatically manages every table and other query engine object created within it.

```python skip-test
from deephaven.liveness_scope import liveness_scope
from deephaven import numpy as dhnp
import numpy as np
import numpy.typing as npt


def make_joined_table():
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

Creating the class directly gives greater control. It allows a scope to be opened more than once. The scope can also release the resources it manages when it is no longer needed. Users writing queries with many objects that eventually need to be garbage collected should consider using a liveness scope to control when the objects are deleted and the memory freed.

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

## Choosing the function or the class

Queries with simpler use cases for a liveness scope typically find the function sufficient for their needs. It automatically manages objects created within it, and releases them when the `with` block or decorated function exits, unless you keep them with `preserve`.

For queries in which more fine-grained control over the lifespan of objects is required, the class is recommended.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
