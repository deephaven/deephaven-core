---
title: LivenessScope
---

`LivenessScope` is a Python class that manages the [reference counting](../../conceptual/liveness-scope-concept.md) of tables and other query resources created within it. These managed objects are called the scope's referents. Use the `LivenessScope` class to open a scope more than once or to choose when to release it. For a scope that is released automatically when a `with` block exits or a decorated function returns, use the [`liveness_scope`](./liveness-scope.md) function.

## Syntax

```python syntax
scope = LivenessScope()
```

## Parameters

None.

## Returns

A new `LivenessScope`.

## Methods

The `LivenessScope` class provides the following methods. For details, see the [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope).

- [`open`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.open): Pushes the scope onto the thread's [stack of open scopes](../../conceptual/liveness-scope-concept.md#how-liveness-scopes-work) for the duration of a `with` block, during which the scope manages the tables and other query resources the block creates. A scope can be opened more than once until it is released.
- [`release`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.release): Releases the scope, dropping its references to everything it manages, unless something else still holds a reference to the scope itself. Objects that nothing else references are released. Raises a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) if `release` has already been called on this `LivenessScope`.
- [`preserve(referent)`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.preserve): Keeps the object live in the enclosing scope, which is the scope below this one on the stack of open scopes. The scope must be open and at the top of the stack.
- [`manage(referent)`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.manage): Adds a reference from this scope to the object.
- [`unmanage(referent)`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.unmanage): Drops one reference from this scope to the object.

`preserve`, `manage`, and `unmanage` raise a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) if `referent` is neither a liveness referent, such as a table, nor a wrapper around one. `preserve` also raises one if the scope is not at the top of the stack.

## Examples

The following example uses [function-generated tables](../table-operations/create/function_generated_table.md) to produce a refreshing table that replaces its 5 rows at most once per second. Because the example creates the function-generated table and a [filtered](../table-operations/filter/where.md) table in two separate openings of the same liveness scope, releasing the scope once drops the scope's references to both tables.

```python ticking-table order=null
from deephaven.liveness_scope import LivenessScope
from deephaven import function_generated_table, empty_table


def random_data():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


scope = LivenessScope()

with scope.open():
    table_from_func = function_generated_table(
        table_generator=random_data, refresh_interval_ms=1000
    )

with scope.open():
    big_x = table_from_func.where("X > 5")

# After a while...
scope.release()
# The scope no longer keeps the tables live.
# Drop the variables so the console does not hold handles to the released tables.
table_from_func = None
big_x = None
```

Calling [`release`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.release) on `scope` drops the scope's references to `table_from_func` and `big_x`. Each table stops updating once nothing else holds a reference to it. Another scope can hold such a reference, as can a refreshing table derived from it or an open view of the table in the UI. An open view keeps the table updating until the user closes it.

## Related documentation

- [Liveness Scopes](../../conceptual/liveness-scope-concept.md)
- [`liveness_scope`](./liveness-scope.md)
- [`function_generated_table`](../table-operations/create/function_generated_table.md)
- [`where`](../table-operations/filter/where.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope)
