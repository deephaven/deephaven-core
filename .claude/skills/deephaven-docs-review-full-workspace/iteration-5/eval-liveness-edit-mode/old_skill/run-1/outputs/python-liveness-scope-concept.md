---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide explains what a liveness scope is, how it works, why a query can benefit from one, and how to use one.

A liveness scope controls when the nodes in the [query update graph](./table-update-model.md) that it manages are released. A node can be a table, a plot, or another query engine object.

## How liveness scopes work

Deephaven's engine runs in Java, where the JVM decides when garbage collection (GC) takes place. Users have little control over its timing. Rather than relying only on GC, the engine reference counts the nodes in the update graph. A node stays live — a refreshing table keeps processing updates — as long as at least one manager holds a reference to it. A refreshing table holds references to the tables it depends on, so its parents stay live as long as it does.

When a node's reference count reaches zero, the engine destroys it immediately: the node stops receiving updates and drops its references to its parents, which can bring their counts to zero in turn. A destroyed object may be unusable for later operations. The JVM still reclaims its memory later through GC.

Every new node is managed by the scope at the top of the current thread's liveness scope stack. When you run code in the console, that is the script session's own scope, which retains everything your code creates until the session closes. Otherwise, those objects are cleaned up only when the JVM garbage-collects them. A liveness scope you open goes on top of the stack, so it manages the nodes created while it is open instead. Releasing the scope drops its references to all of them, and any node that nothing else still references — another scope, a dependent table, or a client that has the table open — is destroyed at once.

## Why use a liveness scope?

Most queries don't need a liveness scope. It helps when a query creates refreshing intermediate tables that it needs only for a while: releasing the scope stops those tables from updating as soon as you are done with them, instead of whenever the JVM next garbage-collects them.

## Demonstrating the problem

Before showing liveness scopes in action, this section demonstrates the problem that they solve.

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

The above example works, but the query creates intermediate objects, such as `data` and `combo`, that are not needed after it runs. Setting their variables to `None` does not release them immediately, because the script session's scope still manages them. A [`LivenessScope`](../reference/engine/LivenessScope.md) can manage these objects and release them when they are no longer needed.

The following example runs the same query inside a `LivenessScope`. It uses `preserve` to hand the two tables it keeps, `crypto` and `combo_tree`, to the next outer scope (the script session's), and then calls `release` to release everything else the scope manages.

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

    # Keep these two tables live after the scope is released
    scope.preserve(crypto)
    scope.preserve(combo_tree)

# Release everything else the scope manages
scope.release()

data = None
combo = None
```

## How to create a liveness scope

Neither way of creating a liveness scope takes any parameters. You can use the [`liveness_scope`](../reference/engine/liveness-scope.md) function or the [`LivenessScope`](../reference/engine/LivenessScope.md) class:

- The `liveness_scope` function is meant to be used _only_ in a [`with`](https://peps.python.org/pep-0343/) block or as a function decorator. It opens a scope when the block or function starts and releases it when the block or function exits. This is sufficient for most simple use cases.
- The `LivenessScope` class gives finer control over the lifespan of objects. You can open the scope more than once, and you decide when to release it.

```python order=null
from deephaven.liveness_scope import liveness_scope, LivenessScope

# liveness_scope opens a scope for the duration of a with block, then releases it
with liveness_scope() as scope_from_function:
    pass

# LivenessScope creates a scope that you open and release yourself
scope_from_class = LivenessScope()
```

## How to use a liveness scope

A liveness scope has the following methods:

- `manage(referent)` explicitly manages the given object in this scope.
- `preserve(referent)` manages the given object in the next outer scope, so that it stays live after this scope is released. Call it while this scope is open.
- `unmanage(referent)` causes this scope to no longer manage the given object.
- `open` opens the scope for the duration of a `with` block. This method is _only_ available to liveness scopes created directly from the class.
- `release` closes the scope and releases all of the resources it manages. This method is _only_ available to liveness scopes created directly from the class.

### Use the `liveness_scope` function

The `liveness_scope` function creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which is open only for the duration of the `with` block or decorated function. It automatically manages every object created while it is open, and releases them all when the block or function exits — except the objects you pass to `preserve`.

```python skip-test
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

### Use the `LivenessScope` class

Creating a `LivenessScope` directly lets you open the scope more than once and release the resources it manages when you no longer need them. Consider it for queries that create many refreshing objects whose lifespan you want to control, rather than leaving their cleanup to the JVM's garbage collector.

In the following example, each call returns a table along with the scope that manages it. Releasing a scope stops its table from updating, unless something else, such as a client that has the table open, still references it.

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

## Related documentation

- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [Execution Context](./execution-context.md)
- [Table Update Model](./table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
