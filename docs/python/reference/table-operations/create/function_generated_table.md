---
title: function_generated_table
---

The `function_generated_table` function is useful for creating tables that are dependent on one or more [ticking tables](../../../conceptual/table-types.md), or for creating tables that need to be refreshed at a regular interval. Because the `table_generator` is arbitrary Python, it can bring data from outside Deephaven, such as files or websites, into a live table, or build the result with libraries such as pandas or NumPy.

The function creates a table by running the user-defined `table_generator` function. This function runs once when the table is created, and then again whenever one of the `source_tables` ticks or `refresh_interval_ms` milliseconds elapse. Specify at most one of `source_tables` and `refresh_interval_ms`. When neither is specified, nothing triggers a refresh: the `table_generator` runs exactly once, and the result is a static table.

Every refresh in which the `table_generator` produces a table replaces the result in full. The [table update](../../../conceptual/table-update-model.md) removes all previous rows and adds all newly generated rows. It has no modified rows and no shifts, even when the generated data is unchanged. If both the previous result and the new table are empty, no update is sent.

A refresh in which the `table_generator` returns `None` produces no update and keeps the previous result. If the result is a [blink table](../../../conceptual/table-types.md#specialization-3-blink), it is cleared instead. The `copy_data` parameter controls how the result stores each replacement, and `blink_table` makes the result keep only the current cycle's rows.

When the input is already a Deephaven table, regular table operations are generally preferable. Many of them update incrementally, while a function-generated table replaces its whole result on every refresh. A function-generated table may require less development effort for small tables.

## Syntax

```python syntax
function_generated_table(
  table_generator: Callable[..., Table | None],
  source_tables: Table | Sequence[Table] | None = None,
  refresh_interval_ms: int | None = None,
  exec_ctx: ExecutionContext | None = None,
  args: tuple = (),
  kwargs: dict | None = None,
  copy_data: bool = True,
  blink_table: bool = False,
  table_definition: TableDefinitionLike | None = None,
) -> Table
```

## Parameters

<ParamTable>
<Param name="table_generator" type="Callable[..., Table | None]">

The table generator function. This function must return a table or `None`. Returning `None` declines to produce a new table, so the previous cycle's result is retained. For a [blink table](../../../conceptual/table-types.md#specialization-3-blink) (see `blink_table`), the result is cleared instead. If the first invocation returns `None`, `table_definition` must be provided so that the result's columns are known. If the `table_generator` raises an error or returns something other than a table or `None`, `function_generated_table` raises an error on the first invocation; on a later refresh, the result table fails with that error and stops updating.

</Param>
<Param name="source_tables" type="Table | Sequence[Table] | None" optional>

The source tables the `table_generator` depends on. The `table_generator` re-runs once in each [update cycle](../../../conceptual/table-update-model.md) in which any of these tables ticks. Every source table must be a refreshing ([ticking](../../../conceptual/table-types.md)) table; passing a static table raises an error.

This parameter and `refresh_interval_ms` are mutually exclusive. Specifying both raises an error.

> [!NOTE]
> The `table_generator` may access data in the `source_tables`, but further table operations on them need the precautions below. Deephaven may reuse the result of an earlier identical table operation, so a table operation can return a table created by a previous invocation of the same operation. Because that result is not included in the `source_tables`, it is not automatically treated as a dependency for purposes of determining when it is safe to invoke `table_generator`. This allows race conditions to exist between (1) accessing the operation result and (2) that result's own update processing.
>
> It is best to include all dependencies directly in `source_tables`, or to compute any on-demand inputs only under a [`LivenessScope`](../../engine/LivenessScope.md).

</Param>
<Param name="refresh_interval_ms" type="int | None" optional>

The interval (in milliseconds) at which the `table_generator` function is re-run. The interval is checked once per update cycle, so the `table_generator` runs on the first cycle after the interval elapses, and never more than once per cycle. A non-positive interval is treated the same as no trigger at all, producing a static result. An interval greater than 2,147,483,647 ms (about 24.8 days) raises an error.

This parameter and `source_tables` are mutually exclusive. Specifying both raises an error.

</Param>
<Param name="exec_ctx" type="ExecutionContext | None" optional>

A custom [execution context](../../../conceptual/execution-context.md) in which to run the `table_generator`. If not specified, the `table_generator` runs in the execution context that is current when `function_generated_table` is called. Pass `exec_ctx` only to run the `table_generator` in a different context.

</Param>
<Param name="args" type="tuple" optional>

A tuple of positional arguments to pass to `table_generator`. Defaults to `()`.

</Param>
<Param name="kwargs" type="dict | None" optional>

Dictionary of keyword arguments to pass to `table_generator`. Defaults to `None`, meaning no keyword arguments are passed.

</Param>
<Param name="copy_data" type="bool" optional>

When `True`, the generated data is copied into storage that the result owns, and the result's row keys are contiguous, from `0` through the table size minus one.

When `False`, the result reads the generated table's column data directly instead of copying it, and it keeps the generated table's row keys. Each update adds the generated table's current rows and removes the previous cycle's rows. In this mode, every column of a refreshing generated table must be immutable, meaning that the value stored at a row key never changes once it is written. If the first generated table breaks this rule, `function_generated_table` raises an error. If a table generated on a later refresh breaks it, the result table fails with that error and stops updating. A static table produced fresh on each refresh, such as the output of [`snapshot`](../snapshot/snapshot.md), always meets this requirement.

Defaults to `True`.

</Param>
<Param name="blink_table" type="bool" optional>

When `True`, `function_generated_table` presents the result as a blink table, which keeps only the rows generated during the current cycle. Each refresh is still a full replacement, and the setting works independently of `copy_data`. Rows generated in one update cycle are removed on the next cycle, whether or not the `table_generator` runs again. A refresh in which the `table_generator` returns `None` clears the result instead of retaining it.

This parameter requires a refresh trigger: `source_tables` or a positive `refresh_interval_ms`. Setting it without one raises an error. Defaults to `False`.

</Param>
<Param name="table_definition" type="TableDefinitionLike | None" optional>

A [table definition](./TableDefinitionLike.md) that sets the result's columns and their order. When it is provided, every table the `table_generator` produces must be compatible with it. A compatible table has the same column names, each with the same data type, in any order. Defaults to `None`, in which case the result uses the definition of the first table the `table_generator` produces, and every later table must be compatible with that definition.

</Param>
</ParamTable>

## Returns

A new function-generated table.

## Examples

The following example defines a `table_generator` function and re-runs it about every 2000 ms.

```python ticking-table order=null
from deephaven import empty_table, function_generated_table


def make_table():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


result = function_generated_table(table_generator=make_table, refresh_interval_ms=2000)
```

If the `table_generator` function reads from one or more ticking tables, pass those tables as `source_tables` so that the `table_generator` re-runs when they tick. In the following example, `make_table` creates one row for each row of the [time table](./timeTable.md) `tt`, so the `table_generator` recomputes the result each time `tt` ticks and the result grows.

```python ticking-table order=null
from deephaven import empty_table, function_generated_table, time_table

tt = time_table("PT1S")


def make_table():
    return empty_table(tt.size).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


result = function_generated_table(table_generator=make_table, source_tables=tt)
```

The `args` and `kwargs` parameters pass positional and keyword arguments to the `table_generator` function.

```python ticking-table order=null
from deephaven import empty_table, function_generated_table


def make_table(nrows, x_lower, x_upper, y_lower, y_upper):
    return empty_table(nrows).update(
        [
            f"X = randomInt({x_lower}, {x_upper})",
            f"Y = randomDouble({y_lower}, {y_upper})",
        ]
    )


result = function_generated_table(
    table_generator=make_table,
    refresh_interval_ms=2000,
    args=(10, 0, 100),
    kwargs={"y_lower": 0.0, "y_upper": 1.0},
)
```

The following `table_generator` returns `None` while the time table `tt` has no rows. Because the first invocation may return `None`, the example supplies a `table_definition`. The result has a `Count` column even before the `table_generator` produces its first table.

```python ticking-table order=null
from deephaven import function_generated_table, new_table, time_table
from deephaven.column import int_col
import deephaven.dtypes as dht

tt = time_table("PT1S")


def make_table():
    if tt.size == 0:
        return None
    return new_table([int_col("Count", [tt.size])])


result = function_generated_table(
    table_generator=make_table,
    source_tables=tt,
    table_definition={"Count": dht.int32},
)
```

With `blink_table=True`, the result holds only the rows generated in the current update cycle. With a 2000 ms refresh interval, the result is empty between refreshes.

```python ticking-table order=null
from deephaven import empty_table, function_generated_table


def make_table():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


result = function_generated_table(
    table_generator=make_table, refresh_interval_ms=2000, blink_table=True
)
```

With `copy_data=False`, the result reads the generated table's data without copying it. The `table_generator` here returns a new static table on each refresh, so it meets the immutability requirement described for `copy_data`.

```python ticking-table order=null
from deephaven import empty_table, function_generated_table


def make_table():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


result = function_generated_table(
    table_generator=make_table, refresh_interval_ms=2000, copy_data=False
)
```

## Related documentation

- [Execution Context](../../../conceptual/execution-context.md)
- [How to create function-generated tables](../../../how-to-guides/function-generated-tables.md)
- [`empty_table`](./emptyTable.md)
- [`LivenessScope`](../../engine/LivenessScope.md)
- [`new_table`](./newTable.md)
- [`snapshot`](../snapshot/snapshot.md)
- [`TableDefinitionLike`](./TableDefinitionLike.md)
- [Table types](../../../conceptual/table-types.md)
- [Table update model](../../../conceptual/table-update-model.md)
- [`time_table`](./timeTable.md)
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.function_generated_table)
