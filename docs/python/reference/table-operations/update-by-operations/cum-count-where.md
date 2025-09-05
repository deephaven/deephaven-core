---
title: cum_count_where
---

`cum_count_where` performs a cumulative count of values that pass a set of filters in an [`update_by`](./updateBy.md) table operation.

## Syntax

```
cum_count_where(col: str, filters: Union[str, Filter, List[str], List[Filter]]) -> UpdateByOperation
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
</ParamTable>

> [!TIP]
> Providing multiple filter strings in the `filters` parameter results in an `AND` operation being applied to the filters. For example, `"Number % 3 == 0", "Number % 5 == 0"` returns the count of values where `Number` is evenly divisible by both `3` and `5`. You can also write this as a single conditional filter (`"Number % 3 == 0 && Number % 5 == 0"`) and receive the same result.
>
> You can use the `||` operator to `OR` multiple filters. For example, `` Y == `M` || Y == `N` `` matches when `Y` equals `M` or `N`.

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using the `cum_count_where` operation
and counting rows where `Y` is `>= 20` and `< 99`. No grouping columns are given, so the cumulative count is calculated for all rows in the table.

```python order=result,source
from deephaven.updateby import cum_count_where
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i % 3", "Y = randomInt(0, 100)"]
)

result = source.update_by(
    ops=cum_count_where(col="count", filters=["Y >= 20", "Y < 99"])
)
```

The following example builds off the previous by specifying `Letter` as the grouping column. Thus, the cumulative count includes rows where `Y` is `>= 20` and `< 99` and is calculated for each unique letter.

```python order=result,source
from deephaven.updateby import cum_count_where
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i % 3", "Y = randomInt(0, 100)"]
)

result = source.update_by(
    ops=cum_count_where(col="count", filters="Y >= 20 && Y < 99"), by="Letter"
)
```

In the next example, `cum_count_where` returns the number of rows where `Letter` equals `'A'` and `Y` > `50` over the entire table (without grouping).

```python order=result,source
from deephaven.updateby import cum_count_where
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i % 3", "Y = randomInt(0, 100)"]
)

result = source.update_by(
    ops=cum_count_where(col="count", filters=["Letter == `A`", "Y > 50"])
)
```

This example returns the number of rows where `Y` is between `50` and `100` (inclusive), as grouped by `Letter` and `X`.

```python order=result,source
from deephaven.updateby import cum_count_where
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = i % 3", "Y = randomInt(0, 100)"]
)

result = source.update_by(
    ops=cum_count_where(col="count", filters=["Y >= 50", "Y <= 100"]),
    by=["Letter", "X"],
)
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumCountWhere(java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.cum_count_where)
