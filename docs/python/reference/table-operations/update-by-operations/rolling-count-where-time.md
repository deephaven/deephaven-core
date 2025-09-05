---
title: rolling_count_where_time
---

`rolling_count_where_time` creates an [`update_by`](./updateBy.md) table operation that keeps a count of the number of values in a rolling window that pass a set of filters, using time as the windowing unit. Data is windowed by reverse and forward
time intervals relative to the current row.

## Syntax

```
def rolling_count_where_time(
    ts_col: str,
    col: str,
    filters: Union[str, Filter, List[str], List[Filter]],
    rev_time: Union[int, str] = 0,
    fwd_time: Union[int, str] = 0) -> UpdateByOperation:
```

## Parameters

<ParamTable>

<Param name="ts_col" type="str">

The name of the column containing timestamps.

</Param>
<Param name="col" type="str">

The name of the column that will contain the count of values that pass the filters.

</Param>
<Param name="filters" type="Union[str, Filter, Sequence[str], Sequence[Filter]]">

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md).

Any filter is permitted, as long as it is not refreshing and does not use row position/key variables or arrays.

</Param>
<Param name="rev_time" type="Union[int,str]">

The look-behind window size. This can be expressed as an integer in nanoseconds or a string [duration](../../query-language/types/durations.md); e.g., `"PT00:00:00.001"` or `"PTnHnMnS"`, where `H` is hour, `M` is minute, and `S` is second.

</Param>
<Param name="fwd_time" type="Union[int,str]">

The look-forward window size. This can be expressed as an integer in nanoseconds or a string [duration](../../query-language/types/durations.md); e.g., `"PT00:00:00.001"` or `"PTnHnMnS"`, where `H` is hour, `M` is minute, and `S` is second.

</Param>
</ParamTable>

> [!TIP]
> Providing multiple filter strings in the `filters` parameter results in an `AND` operation being applied to the filters. For example,
> `"Number % 3 == 0", "Number % 5 == 0"` returns the count of values where `Number` is evenly divisible by both `3` and `5`. You can also write this as a single conditional filter (`"Number % 3 == 0 && Number % 5 == 0"`) and receive the same result.
>
> You can use the `||` operator to `OR` multiple filters. For example, `` Y == `M` || Y == `N` `` matches when `Y` equals `M` or `N`.

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an `update_by` on the `source` table using three `rolling_count_where_time` operations.
Each operation gives varying `rev_time` and `fwd_time` values to show how they affect the output. The windows for each operation are as follows:

- `op_before`: The window starts five seconds before the current row, and ends one second before the current row.
- `op_after`: The window starts one second after the current row, and ends five seconds after the current row.
- `op_middle`: The window starts three seconds before the current row, and ends three seconds after the current row.

```python order=source,result
from deephaven.updateby import rolling_count_where_time
from deephaven.time import to_j_instant
from deephaven import empty_table

base_time = to_j_instant("2023-01-01T00:00:00 ET")

source = empty_table(10).update(
    [
        "Timestamp = base_time + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = i",
        "Y = randomInt(0, 100)",
    ]
)

filter_to_apply = ["Y >= 20", "Y < 99"]

op_before = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_before",
    filters=filter_to_apply,
    rev_time=int(5e9),
    fwd_time=int(-1e9),
)
op_after = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_after",
    filters=filter_to_apply,
    rev_time="PT1S",
    fwd_time="PT5S",
)
op_middle = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_middle",
    filters=filter_to_apply,
    rev_time="PT3S",
    fwd_time="PT3S",
)

result = source.update_by(ops=[op_before, op_after, op_middle])
```

The following example performs an `OR` filter and computes the results for each value in the `Letter` column separately.

```python order=source,result
from deephaven.updateby import rolling_count_where_time
from deephaven.time import to_j_instant
from deephaven import empty_table

base_time = to_j_instant("2023-01-01T00:00:00 ET")

source = empty_table(10).update(
    [
        "Timestamp = base_time + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = i",
        "Y = randomInt(0, 100)",
    ]
)

filter_to_apply = ["Y < 20 || Y >= 80"]

op_before = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_before",
    filters=filter_to_apply,
    rev_time=int(5e9),
    fwd_time=int(-1e9),
)
op_after = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_after",
    filters=filter_to_apply,
    rev_time="PT1S",
    fwd_time="PT5S",
)
op_middle = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_middle",
    filters=filter_to_apply,
    rev_time="PT3S",
    fwd_time="PT3S",
)

result = source.update_by(ops=[op_before, op_after, op_middle], by=["Letter"])
```

The following example uses a complex filter involving multiple columns, with the results bucketed by the `Letter` column.

```python order=source,result
from deephaven.updateby import rolling_count_where_time
from deephaven.time import to_j_instant
from deephaven import empty_table

base_time = to_j_instant("2023-01-01T00:00:00 ET")

source = empty_table(10).update(
    [
        "Timestamp = base_time + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = i",
        "Y = randomInt(0, 100)",
    ]
)

filter_to_apply = ["(Y < 20 || Y >= 80 || Y % 7 == 0) && X >= 3"]

op_before = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_before",
    filters=filter_to_apply,
    rev_time=int(5e9),
    fwd_time=int(-1e9),
)
op_after = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_after",
    filters=filter_to_apply,
    rev_time="PT1S",
    fwd_time="PT5S",
)
op_middle = rolling_count_where_time(
    ts_col="Timestamp",
    col="count_middle",
    filters=filter_to_apply,
    rev_time="PT3S",
    fwd_time="PT3S",
)

result = source.update_by(ops=[op_before, op_after, op_middle], by=["Letter"])
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingCountWhere(java.lang.String,java.time.Duration,java.time.Duration,java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_count_where_time)
