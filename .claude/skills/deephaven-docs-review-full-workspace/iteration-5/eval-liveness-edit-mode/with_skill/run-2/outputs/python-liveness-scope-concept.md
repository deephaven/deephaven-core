---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide discusses liveness scopes. It covers what a liveness scope is, how to use one, and why queries can benefit from its use.

Liveness scopes give users a finer degree of control over cleanup of unreferenced nodes in the [query update graph](./table-update-model.md). A node in the update graph can be a table, plot, or any other object. A liveness scope automatically manages reference counting of the nodes created within it. Resources can be programmatically freed from a liveness scope.

## Why use a liveness scope?

Deephaven's engine runs in Java, in which garbage collection is handled by the JVM. Users have little control over when garbage collection takes place, as the JVM tries to optimize when it occurs to minimize GC runtime and maximize the amount of memory left available after it's run.

Liveness scopes give users much more control over when nodes in the query update graph are cleaned up. Without a liveness scope, queries rely solely on the JVM to perform garbage collection to clean up unreferenced nodes in the DAG. In most cases, this is acceptable. However, there are cases where a query can benefit from the use of a liveness scope.

## How liveness scopes work

Deephaven's liveness scopes allow the nodes in a refreshing query's update propagation graph to be cleaned up proactively by assessing the nodes' "liveness" (whether or not they are active), rather than only via actions of the Java garbage collector. This does not replace garbage collection (GC), but it does allow cleanup to happen immediately when objects in the GUI are not needed. This is accomplished internally via reference counting, and works automatically for all users.

When a table is live in Deephaven, the update propagation graph accumulates parent nodes that produce data (data sources) at the top, and all the child nodes that stream down the graph. Deephaven's liveness scope system keeps track of these referents: when child objects cease to be referenced and the parents' liveness count goes down, they are cleaned up immediately without waiting for GC.

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

The above example works, but the query creates many objects that are not needed after it is run. The [`LivenessScope`](https://docs.deephaven.io/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope) can manage these objects and release them when they are no longer needed. This is best practice because it allows Deephaven to conserve memory.

In this example, we run the same query, but enclose it in a [`LivenessScope`](https://docs.deephaven.io/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope).

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

    # Keep the tables you want to use after the block; the rest are released below
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

with liveness_scope() as scope_from_function:
    pass  # Objects created here are managed by scope_from_function

scope_from_class = LivenessScope()
```

## How to use a liveness scope

Deephaven's reference counting instrumentation will only clean up objects created purely for the GUI. This can be augmented by creating a [`LivenessScope`](https://docs.deephaven.io/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope) for your query. When the scope is released, it lets go of every object it manages that nothing else still depends on.

A liveness scope, once created, has several methods that can be used:

- `manage(referent)` explicitly manages the object in this scope.
- `preserve(referent)` keeps the object live in the next scope outside this one.
- `unmanage(referent)` causes the current scope to no longer manage the given object.
- `open` opens a liveness scope. This is meant to be used in a `with` statement. This method is _only_ available to liveness scopes created directly from the class.
- `release` closes a liveness scope and all of its managed resources. This method is _only_ available to liveness scopes created directly from the class.

### The function

Using the function creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which can only be opened once. If the function is used, it must be done in a `with` block or as a decorator. The scope automatically manages all objects created within it.

```python skip-test
from deephaven.liveness_scope import liveness_scope
import deephaven.numpy as dhnp
import numpy as np
import numpy.typing as npt


def get_table():
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

Creating the class directly gives greater control. It allows a scope to be opened more than once. The scope can also release the resources it manages when it is no longer needed. Users writing queries with many objects that eventually need to be cleaned up should consider using a liveness scope to control when those objects are released, rather than waiting for garbage collection.

```python skip-test
from deephaven.liveness_scope import LivenessScope


def make_table_and_scope(a: int):
    scope = LivenessScope()
    with scope.open():
        ticking_table = some_ticking_source().where(f"A = {a}")
        return ticking_table, scope


t1, s1 = make_table_and_scope(1)
# .. wait for a while
s1.release()
t2, s2 = make_table_and_scope(2)
# .. wait for a while again
s2.release()
```

## To use the function or the class?

Queries with simpler use cases for a liveness scope typically find the function sufficient for their needs. It automatically manages objects created within it, and releases them when the `with` block or decorated function exits, except for objects passed to `preserve`.

For queries in which more fine-grained control over the lifespan of objects is required, the class is recommended.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
