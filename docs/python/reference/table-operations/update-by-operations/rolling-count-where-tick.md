---
title: rolling_count__where_tick
---

`rolling_count__where_tick` creates an [`update_by`](./updateBy.md) table operation that keeps a count of values in a rolling window that pass a set of filters in an [`update_by`](./updateBy.md) table operation. This call uses table ticks as the
windowing unit. Ticks are row counts. The rolling count can be calculated using forward and/or backward windows.

## Syntax

```
rolling_count_where_tick(
    col: str,
    filters: Union[str, Filter, List[str], List[Filter]],
    rev_ticks: int = 0,
    fwd_ticks: int = 0) -> UpdateByOperation:
```

## Parameters

<ParamTable>
<Param name="col" type="str">

The name of the column that will contain the count of values that pass the filters.

</Param>
<Param name="filters" type="Union[str, Filter, Sequence[str], Sequence[Filter]]">

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md).

Any filter is permitted, as long as it is not refreshing and does not use row position/key variables or arrays.

</Param>
<Param name="rev_ticks" type="int">

The look-behind window size in rows. If positive, it defines the maximum number of rows _before_ the current row that will be used. If negative, it defines the minimum number of rows _after_ the current row that will be used. Includes the current row.

</Param>
<Param name="fwd_ticks" type="int">

The look-forward window size in rows. If positive, it defines the maximum number of rows _after_ the current row that will be used. If negative, it defines the minimum number of rows _before_ the current row that will be used.

</Param>
</ParamTable>

> [!TIP]
> Providing multiple filter strings in the `filters` parameter results in an `AND` operation being applied to the
> filters. For example,
> `"Number % 3 == 0", "Number % 5 == 0"` returns the count of values where `Number` is evenly divisible by
> both `3` and `5`. You can also write this as a single conditional filter (`"Number % 3 == 0 && Number % 5 == 0"`) and
> receive the same result.
>
> You can use the `||` operator to `OR` multiple filters. For example, `` Y == `M` || Y == `N` `` matches when `Y` equals
> `M` or `N`.

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an `update_by` table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using `rolling_count_where_tick`. The same filter is used but we are providing three different windows for the rolling calculation.

```python order=source,result
from deephaven.updateby import rolling_count_where_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 100)"]
)

filter_to_apply = ["Y >= 20", "Y < 99"]

op_before = rolling_count_where_tick(
    col="count_before", filters=filter_to_apply, rev_ticks=3, fwd_ticks=-1
)
op_after = rolling_count_where_tick(
    col="count_after", filters=filter_to_apply, rev_ticks=-1, fwd_ticks=3
)
op_middle = rolling_count_where_tick(
    col="count_middle", filters=filter_to_apply, rev_ticks=1, fwd_ticks=1
)

result = source.update_by(ops=[op_before, op_after, op_middle])
```

The following example performs an `OR` filter and computes the results for each value in the `Letter` column separately.

```python order=source,result
from deephaven.updateby import rolling_count_where_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 100)"]
)

filter_to_apply = ["Y < 20 || Y >= 80"]

op_before = rolling_count_where_tick(
    col="count_before", filters=filter_to_apply, rev_ticks=3, fwd_ticks=-1
)
op_after = rolling_count_where_tick(
    col="count_after", filters=filter_to_apply, rev_ticks=-1, fwd_ticks=3
)
op_middle = rolling_count_where_tick(
    col="count_middle", filters=filter_to_apply, rev_ticks=1, fwd_ticks=1
)

result = source.update_by(ops=[op_before, op_after, op_middle], by="Letter")
```

The following example uses a complex filter involving multiple columns, with the results bucketed by the `Letter` column.

```python order=source,result
from deephaven.updateby import rolling_count_where_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomInt(0, 100)"]
)

filter_to_apply = ["(Y < 20 || Y >= 80 || Y % 7 == 0) && X >= 3"]

op_before = rolling_count_where_tick(
    col="count_before", filters=filter_to_apply, rev_ticks=3, fwd_ticks=-1
)
op_after = rolling_count_where_tick(
    col="count_after", filters=filter_to_apply, rev_ticks=-1, fwd_ticks=3
)
op_middle = rolling_count_where_tick(
    col="count_middle", filters=filter_to_apply, rev_ticks=1, fwd_ticks=1
)

result = source.update_by(ops=[op_before, op_after, op_middle], by="Letter")
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingCountWhere(long,long,java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_count_where_tick)
