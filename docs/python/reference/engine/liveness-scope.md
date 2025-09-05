---
title: liveness_scope
---

`liveness_scope` is a Python method for creating and opening a [`LivenessScope`](./LivenessScope.md) for running a block of code. Use this function to wrap a block of code using a `with` statement. For the duration of the `with` block, the liveness scope will be open and any liveness referents created will be automatically managed by it.

## Syntax

```python syntax
scope = liveness_scope()
```

## Parameters

None.

## Returns

A [`LivenessScope`](./LivenessScope.md).

## Examples

The method can be used in a `with` block to encapsulate code that produces ticking data. In the example below, `table` is explicitly preserved outside of the `with` block. `ticking_table` is cleaned up once the `with` block is exited.

```python skip-test
from deephaven.liveness_scope import liveness_scope

with liveness_scope() as scope:
    ticking_table = some_ticking_source()
    table = ticking_table.snapshot().join(table=other_ticking_table, on=...)
    scope.preserve(table)
return table
```

It can also be used as a decorator to achieve a similar result.

```python skip-test
@liveness_scope()
def get_values():
    ticking_table = some_ticking_source().last_by("Sym")
    return dhnp.to_numpy(ticking_table)
```

## Related documentation

- [How to use Liveness Scopes](../../conceptual/liveness-scope-concept.md)
- [`LivenessScope`](./LivenessScope.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.liveness_scope)
