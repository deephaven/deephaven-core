---
title: LivenessScope
---

`LivenessScope` is a Python class that manages the reference counting of tables and other query resources that are created within it.

## Syntax

```python syntax
scope = LivenessScope()
```

## Parameters

None.

## Returns

An instance of a `LivenessScope` class.

## Examples

The following example uses [function generated tables](../table-operations/create/function_generated_table.md) to produce a blink table with 5 new rows per second. Encapsulating the function generated table inside of a liveness scope enables the safe deletion of data once the scope has been released. `table_from_func` will continue to tick after the scope is released until _all_ referents have been deleted. That includes the ticking table in the UI.

```python ticking-table order=null
from deephaven.liveness_scope import liveness_scope, LivenessScope
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

# After a while...
del table_from_func
scope.release()
```

## Related documentation

- [How to use Liveness Scopes](../../conceptual/liveness-scope-concept.md)
- [`liveness_scope`](./liveness-scope.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope)
