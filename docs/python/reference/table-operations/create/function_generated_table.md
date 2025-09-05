---
title: function_generated_table
---

The `function_generated_table` method is useful for creating tables that are dependent on one or more ticking tables, or for creating tables that need to be refreshed at a regular interval. The method creates a table by running the user-defined `table_generator` function. This function will be run once when the table is created, and then again when either the `source_tables` tick or when `refresh_interval_ms` milliseconds have elapsed.

> [!NOTE]
> The `table_generator` may access data in the `source_tables`, but should not perform further table operations without careful handling. Table operations may be memoized, and it is possible that a table operation will return a table created by a previous invocation of the same operation. Since that result will not have been included in the `source_table’`, it is not automatically treated as a dependency for purposes of determining when it is safe to invoke `table_generator`. This allows race conditions to exist between (1) accessing the operation result and (2) that result’s own update processing.
>
> It is best to include all dependencies directly in `source_table` or only compute on-demand inputs under a `LivenessScope`.

## Syntax

```python syntax
function_generated_table(
  table_generator: Callable[[], Table],
  source_tables: Union[Table, List[Table]] = None,
  refresh_interval_ms: int = None,
  exec_ctx: ExecutionContext = None,
  args: Tuple = None,
  kwargs: Dict = None
) -> Table
```

## Parameters

<ParamTable>
<Param name="table_generator" type="Callable[[], Table]">

The table generator function. This function must return a table.

</Param>
<Param name="source_tables" type="Union[Table, List[Table]]" optional>

The source tables to be used by the generator function.

Either this parameter or `refresh_interval_ms` must be specified, but not both.

</Param>
<Param name="refresh_interval_ms" type="int" optional>

The interval (in milliseconds) at which the `table_generator` function is re-run.

Either this parameter or `source_tables` must be specified, but not both.

</Param>
<Param name="exec_ctx" type="ExecutionContext" optional>

A custom [execution context](../../../conceptual/execution-context.md) to use for this operation. If not specified, the default [execution context](../../../conceptual/execution-context.md) will be used.

</Param>
<Param name="args" type="Tuple" optional>

A Tuple of positional arguments to pass to `table_generator`. Defaults to `()`.

</Param>
<Param name="kwargs" type="Dict" optional>

Dictionary of keyword arguments to pass to `table_generator`. Defaults to `{}`.

</Param>
</ParamTable>

## Returns

A new function-generated table.

## Example

In the following example, we create an [execution context](../../../conceptual/execution-context.md) and a `table_generator` function, which we then use to generate a table that re-runs the `table_generator` function every 2000ms.

```python order=result
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table
import random, threading, time
from deephaven import function_generated_table


def make_table():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


ctx = get_exec_ctx()  # Get the systemic execution context

result = function_generated_table(
    table_generator=make_table, refresh_interval_ms=2000, exec_ctx=ctx
)
```

If the `table_generator` function depends on one or more ticking tables, the tables must be specified. Adding `tt` as a `source_tables` parameter causes the generated table to be recomputed on each tick of the time table.

```python order=result
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table, time_table
import random, threading, time
from deephaven import function_generated_table


def make_table():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


ctx = get_exec_ctx()  # Get the systemic execution context

tt = time_table("PT1S")

result = function_generated_table(
    table_generator=make_table, source_tables=tt, exec_ctx=ctx
)
```

`function_generated_table`'s `args` and `kwargs` parameters allow you to pass positional and keyword arguments to the `table_generator` function.

```python order=result
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table
import random, threading, time
from deephaven import function_generated_table


def make_table(nrows, x_lower, x_upper, y_lower, y_upper):
    return empty_table(nrows).update(
        [
            f"X = randomInt({x_lower}, {x_upper})",
            f"Y = randomDouble({y_lower}, {y_upper})",
        ]
    )


ctx = get_exec_ctx()  # Get the systemic execution context

result = function_generated_table(
    table_generator=make_table,
    refresh_interval_ms=2000,
    exec_ctx=ctx,
    args=(10, 0, 100),
    kwargs={"y_lower": 0.0, "y_upper": 1.0},
)
```

## Related documentation

- [Execution Context](../../../conceptual/execution-context.md)
- [How to create function-generated tables](../../../how-to-guides/function-generated-tables.md)
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.function_generated_table)
