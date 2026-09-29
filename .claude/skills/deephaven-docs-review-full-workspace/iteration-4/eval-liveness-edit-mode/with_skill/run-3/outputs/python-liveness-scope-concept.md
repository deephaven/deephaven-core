---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide discusses liveness scopes. It covers what a liveness scope is, how to use one, and why queries can benefit from its use.

Liveness scopes give users a finer degree of control over cleanup of unreferenced nodes in the [query update graph](./table-update-model.md). A node in the update graph can be a table, a listener, or another query engine object. A liveness scope automatically manages reference counting of the nodes created within it. You can also release a liveness scope's resources programmatically.

## Why use a liveness scope?

Deephaven's engine runs in Java, in which garbage collection is handled by the JVM. Users have little control over when garbage collection takes place, as the JVM tries to optimize when it occurs to minimize GC runtime and maximize the amount of memory left available after it's run.

Liveness scopes give users more control over when nodes in the query update graph are cleaned up. Without a liveness scope, queries rely on the JVM's garbage collector to clean up unreferenced nodes in the update graph. In most cases, this is acceptable. However, there are cases where a query can benefit from the use of a liveness scope.

## How liveness scopes work

Deephaven's liveness scopes allow the nodes in a refreshing query's update propagation graph to be cleaned up proactively by tracking the nodes' "liveness" (whether anything still needs them), rather than only via actions of the Java garbage collector. This does not replace garbage collection (GC), but it lets the engine clean up an object as soon as nothing needs it. Deephaven does this automatically, with internal reference counting.

When a table is live in Deephaven, the update propagation graph accumulates parent nodes that produce data (data sources) at the top, and all the child nodes that stream down the graph. Deephaven's liveness system tracks references to these nodes. When an object's reference count drops to zero, the engine cleans it up immediately, without waiting for GC: a cleaned-up table stops updating and releases its references to its parents, which can then be cleaned up in turn.

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

The above example works, but the query creates many objects that are not needed after it is run. The [`LivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope) can manage these objects and release them when they are no longer needed, so the engine stops maintaining objects the query no longer uses.

The following example runs the same query inside a `LivenessScope`, preserves the two tables to keep, and then releases the scope.

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

    # Keep the tables to display after the scope is released
    scope.preserve(crypto)
    scope.preserve(combo_tree)

# Release everything else the scope manages
scope.release()

data = None
combo = None
```

## How to create a liveness scope

Creating a liveness scope is easy, as it takes no input parameters. It can be created from the [`liveness_scope`](../reference/engine/liveness-scope.md) function or the [`LivenessScope`](../reference/engine/LivenessScope.md) class directly. The former is intended to be used _only_ in a [`with`](https://peps.python.org/pep-0343/) block or as a function decorator. The latter gives a finer degree of control by allowing a scope to be opened more than once. It also allows the explicit release of resources that it manages.

```python order=null
from deephaven.liveness_scope import liveness_scope, LivenessScope

# liveness_scope opens a scope for the duration of a with block
with liveness_scope() as scope_from_function:
    pass

scope_from_class = LivenessScope()
```

## How to use a liveness scope

Deephaven automatically releases the objects the web UI requests from the server once the UI stops using them. The objects your query creates, however, are managed by the console session. To release them sooner, create a `LivenessScope` for your query. When the scope is released, the engine cleans up every object the scope manages that nothing else still references.

A liveness scope, once created, has several methods that can be used:

- `manage(referent)` explicitly manages the object in this scope.
- `preserve(referent)` keeps the object live after this scope is released by having the next outer scope manage it.
- `unmanage(referent)` causes this scope to no longer manage the given object.
- `open` opens a liveness scope. This is meant to be used in a `with` statement. This method is _only_ available to liveness scopes created directly from the class.
- `release` releases the scope and its references to the objects it manages. This method is _only_ available to liveness scopes created directly from the class.

### The function

Calling `liveness_scope` creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which can only be opened once. Use it in a `with` block or as a decorator. The scope automatically manages every object created within it. When the block exits or the function returns, it releases them all except the objects passed to `preserve`.

```python skip-test
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

Creating a `LivenessScope` directly gives greater control. It allows a scope to be opened more than once. The scope can also release the resources it manages when it is no longer needed. Users writing queries with many objects that eventually need to be garbage collected should consider using a liveness scope to control when the objects are released.

```python skip-test
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

Queries with simpler use cases for a liveness scope typically find the `liveness_scope` function sufficient. It automatically manages objects created within it and releases them when the block exits or the function returns, except for objects passed to `preserve`.

For queries in which more fine-grained control over the lifespan of objects is required, the class is recommended.

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
