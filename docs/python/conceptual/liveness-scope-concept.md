---
title: How to use liveness scopes
sidebar_label: Liveness scope
---

This guide discusses liveness scopes. It covers what a liveness scope is, how to use one, and why queries can benefit from its use.

Liveness scopes give users a finer degree of control over cleanup of unreferenced nodes in the [query update graph](./table-update-model.md). A node in the update graph can be a table, plot, or any other object. A liveness scope automatically manages reference counting of the nodes created within it. Resources can be programmatically freed from a liveness scope.

## Why use a liveness scope?

Deephaven's engine runs in Java, in which garbage collection is handled by the JVM. Users have little control over when garbage collection takes place, as the JVM tries to optimize when it occurs to minimize GC runtime and maximize the amount of memory left available after it's run.

Liveness scopes give users much more control over the query update graph and over garbage collection. Without liveness scope, queries rely solely on the JVM to perform garbage collection to clean up unreferenced nodes in the DAG. In most cases, this is acceptable. However, there are cases where a query can benefit from the use of a liveness scope.

## How to create a liveness scope

Creating a liveness scope is easy, as it takes no input parameters. It can be created from the [`liveness_scope`](../reference/engine/liveness-scope.md) function or the [`LivenessScope`](../reference/engine/LivenessScope.md) class directly. The former is intended to be used _only_ in a [`with`](https://peps.python.org/pep-0343/) block or as a function decorator. The latter gives a finer degree of control by allowing a scope to be opened more than once. It also allows the explicit release of resources that it manages.

```python order=null
from deephaven.liveness_scope import liveness_scope, LivenessScope

scope_from_method = liveness_scope()
scope_from_class = LivenessScope()
```

## How to use a liveness scope

A liveness scope, once created, has several methods that can be used:

- `manage(referent)` explicitly manages the object in the current scope.
- `preserve(referent)` preserves the object in the scope outside the current scope.
- `unmanage(referent)` causes the current scope to no longer manage the given object.
- `open()` opens a liveness scope. This is meant to be used in a `with` statement. This method is _only_ available to liveness scopes created directly from the class.
- `release()` closes a liveness scope and all of its managed resources. This method is _only_ available to liveness scopes created directly from the class.

### The method

Using the method creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which can only be opened once. If the method is used, it must be done in a `with` block or as a decorator. Any and all objects created within the scope will be automatically managed by it.

```python skip-test
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

Creating the class directly gives greater control. It allows a scope to be opened more than once. The scope can also release the resources it manages when it is no longer needed. Users writing queries with many objects that will eventually need to be garbage collected should consider using a liveness scope to control when the objects are deleted and the memory freed.

```python skip-test
def make_table_and_scope(a: int):
    scope = LivenessScope()
    with scope.open():
        ticking_table = some_ticking_source().where(f"A={a}")
        return some_ticking_table, scope


t1, s1 = make_table_and_scope(1)
# .. wait for a while
s1.release()
t2, s2 = make_table_and_scope(2)
# .. wait for a while again
s2.release()
```

## To use the method or the class?

Queries with simpler use cases for a liveness scope will typically find the method to be sufficient for their needs. It automatically manages objects created within it, and stops managing any objects outside of the scope unless it's explicitly told otherwise via `preserve`.

For queries in which more fine-grained control over the lifespan of objects is required, the class is recommended.

## Related documentation

- [LivenessScope](../reference/engine/LivenessScope.md)
- [liveness_scope](../reference/engine/liveness-scope.md)
- [Execution Context](./execution-context.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
