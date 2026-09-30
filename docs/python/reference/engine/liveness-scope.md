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

A `SimpleLivenessScope`, which shares the `preserve`, `manage`, and `unmanage` interface of [`LivenessScope`](./LivenessScope.md).

## Examples

Liveness scopes give you fine-grained control over when ticking tables and their dependencies are removed from the update graph. The primary use case is limiting the lifetime of intermediate objects: when the scope is released (on block exit for a `with` statement), anything the scope manages that is no longer needed stops ticking.

`preserve` transfers ownership of an object to the next outer scope — typically the script session itself — so that it outlives the inner scope. Preserved objects keep their own parents and dependencies live for as long as the preserved object itself is live. Intermediate objects that are _not_ preserved stop being live when the scope is released, but only if they are truly no longer needed at that point.

In this example, a function-generated blink table and an intermediate aggregation are created inside the scope. Only the final `result` is preserved; ownership transfers to the outer scope (typically the script session). `blink_table` and `aggregated` remain live because `result` depends on them — but they are now managed through `result`'s liveness rather than the scope's. When `result` is eventually released, `blink_table` and `aggregated` stop updating too:

```python skip-test
from deephaven.liveness_scope import liveness_scope
from deephaven import function_generated_table, empty_table


def random_data():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


with liveness_scope() as scope:
    blink_table = function_generated_table(
        table_generator=random_data, refresh_interval_ms=1000
    )
    aggregated = blink_table.last_by("X")
    result = aggregated.where("Y > 0")
    scope.preserve(result)  # Transfer ownership to the next outer scope

# scope is released here; result continues ticking, with blink_table and
# aggregated remaining live as its dependencies
```

You can also create a [`LivenessScope`](./LivenessScope.md) explicitly when you need to release it programmatically rather than at block exit.

Using `liveness_scope` as a decorator automatically manages all liveness referents created during the function call — they stop updating when the function returns:

```python skip-test
import deephaven.numpy as dhnp
from deephaven.liveness_scope import liveness_scope


@liveness_scope
def get_current_values(source_table):
    latest = source_table.last_by("Sym")
    return dhnp.to_numpy(latest)  # Table stops updating after return
```

## Related documentation

- [How to use Liveness Scopes](../../conceptual/liveness-scope-concept.md)
- [`LivenessScope`](./LivenessScope.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.liveness_scope)
