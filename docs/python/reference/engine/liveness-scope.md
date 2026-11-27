---
title: liveness_scope
---

`liveness_scope` is a Python function that creates and opens a liveness scope for the duration of a `with` block or a decorated function call. A liveness referent is an object whose lifetime the engine tracks by reference counting, such as a table. A liveness scope holds a reference to each referent it manages. While the scope is the innermost open scope on the current thread, it manages the liveness referents created in the block or function. The main use of a liveness scope is to limit the lifetime of intermediate objects. See [How liveness scopes work](../../conceptual/liveness-scope-concept.md#how-liveness-scopes-work).

`liveness_scope` releases the scope automatically when the `with` block exits or the decorated function returns or raises an exception. Releasing the scope frees every referent it manages that nothing else still needs. A released [ticking](../../conceptual/table-update-model.md) table leaves the [update graph](../../conceptual/dag.md#update-graph-cycles) and stops updating.

To open a scope more than once or to choose when to release it, use the [`LivenessScope`](./LivenessScope.md) class.

## Syntax

```python syntax
with liveness_scope() as scope:
    ...


@liveness_scope()
def func(): ...
```

## Parameters

None.

## Returns

A context manager that can also be used as a function decorator. Entering it in a `with liveness_scope() as scope` statement binds `scope` to a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which shares the [`preserve`, `manage`, and `unmanage`](./LivenessScope.md#methods) interface of [`LivenessScope`](./LivenessScope.md). Used as a decorator, it opens a new scope for each call of the decorated function.

## Examples

The following example uses a `with` statement. It creates a [function-generated table](../table-operations/create/function_generated_table.md), an intermediate [`last_by`](../table-operations/group-and-aggregate/lastBy.md) aggregation, and two [filtered](../table-operations/filter/where.md) tables inside the scope, and preserves only the final `result`. `preserve` keeps `result` live in the enclosing scope, the scope below this one on the [stack of open scopes](../../conceptual/liveness-scope-concept.md#how-liveness-scopes-work), which here is typically the console's liveness scope.

`source` and `aggregated` stay live after the scope is released because `result` depends on them. `big_x` is not preserved and nothing depends on it, so it stops updating when the block exits. For a longer example, see [Release a query's intermediate tables](../../conceptual/liveness-scope-concept.md#example-release-a-querys-intermediate-tables).

```python ticking-table order=null
from deephaven.liveness_scope import liveness_scope
from deephaven import function_generated_table, empty_table


def random_data():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


with liveness_scope() as scope:
    source = function_generated_table(
        table_generator=random_data, refresh_interval_ms=1000
    )
    aggregated = source.last_by("X")
    result = aggregated.where("Y > 0")
    big_x = source.where("X > 5")  # Not preserved
    scope.preserve(result)  # Keep result live in the enclosing scope

# The scope is released here: result keeps ticking, and big_x stops updating.
# Drop the variable so the console does not hold a handle to the released table.
big_x = None
```

The following example uses `liveness_scope` as a decorator. `get_current_values` creates an intermediate `last_by` table, `latest`, from a [time table](../table-operations/create/timeTable.md) and copies its `Value` column into a NumPy array with [`to_numpy`](../numpy/to-numpy.md). The scope releases `latest`, along with the static snapshot that `to_numpy` takes of it, when the function returns. The array is a copy of the data, so it is the only object the function creates that outlives the call.

Because the decorator form does not give the function a handle to the scope, the function cannot call [`preserve`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope.preserve). A ticking table that the decorated function creates and returns therefore stops updating as soon as the call ends, unless something outside the scope, such as another liveness scope, also holds a reference to it. A variable that refers to the returned table does not keep it live. To return a live table, use the `with` form and call `preserve` on the table, or use the [`LivenessScope`](./LivenessScope.md) class.

```python ticking-table order=null
import datetime
from deephaven import time_table
from deephaven import numpy as dhnp
from deephaven.liveness_scope import liveness_scope

# Start 10 seconds in the past so the table already has rows to copy
start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)
ticking_source = time_table("PT1s", start_time=start).update(
    ["Sym = (ii % 2 == 0) ? `A` : `B`", "Value = ii"]
)


@liveness_scope()
def get_current_values(source_table):
    latest = source_table.last_by("Sym")
    return dhnp.to_numpy(latest, cols=["Value"])  # latest stops updating after return


current_values = get_current_values(ticking_source)
```

## Related documentation

- [Liveness Scopes](../../conceptual/liveness-scope-concept.md)
- [Deephaven's live directed acyclic graph (DAG)](../../conceptual/dag.md)
- [Incremental update model](../../conceptual/table-update-model.md)
- [`LivenessScope`](./LivenessScope.md)
- [`function_generated_table`](../table-operations/create/function_generated_table.md)
- [`last_by`](../table-operations/group-and-aggregate/lastBy.md)
- [`where`](../table-operations/filter/where.md)
- [`time_table`](../table-operations/create/timeTable.md)
- [`to_numpy`](../numpy/to-numpy.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.liveness_scope)
