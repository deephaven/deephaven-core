---
title: CumCountWhere
---

`CumCountWhere` performs a cumulative count of values that pass a set of filters in an [`updateBy`](./updateBy.md) table operation.

## Syntax

```
CumCountWhere(resultColumn, filters...)
```

## Parameters

<ParamTable>
<Param name="resultColumn" type="str">

The name of the column that will contain the count of values that pass the filters.

</Param>
<Param name="filters" type="Union[str, Filter, Sequence[str], Sequence[Filter]]">

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md).

Any filter is permitted, as long as it is not refreshing and does not use row position/key variables or arrays.

</Param>
</ParamTable>

> [!TIP]
> Providing multiple filter strings in the `filters` parameter results in an `AND` operation being applied to the filters. For example, `"Number % 3 == 0", "Number % 5 == 0"` returns the count of values where `Number` is evenly divisible by both `3` and `5`. You can also write this as a single conditional filter (`"Number % 3 == 0 && Number % 5 == 0"`) and
> receive the same result.
>
> You can use the `||` operator to `OR` multiple filters. For example, `` Y == `M` || Y == `N` `` matches when `Y` equals `M` or `N`.

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`updateBy`](./updateBy.md) table operation.

## Examples

The following example performs an [`updateBy`](./updateBy.md) on the `source` table using the `CumCountWhere` operation
and counting rows where `Y` is `>= 20` and `< 99`. No grouping columns are given, so the cumulative count is calculated
for all rows in the table.

```groovy order=result,source
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i % 3", "Y = randomInt(0, 100)")

result = source.updateBy(CumCountWhere("count", "Y >= 20", "Y < 99"))
```

The following example builds off the previous by specifying `Letter` as the grouping column. Thus, the cumulative count
includes rows where `Y` is `>= 20` and `< 99` and is calculated for each unique letter.

```groovy order=result,source
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i % 3", "Y = randomInt(0, 100)")

result = source.updateBy(CumCountWhere("count", "Y >= 20 && Y < 99"), "Letter")
```

In the next example, `CumCountWhere` returns the number of rows where `Letter` equals `'A'` and `Y` > `50` over the
entire table (without grouping).

```groovy order=result,source
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i % 3", "Y = randomInt(0, 100)")

result = source.updateBy(CumCountWhere("count", "Letter == `A`", "Y > 50"))
```

This example returns the number of rows where `Y` is between `50` and `100` (inclusive), as grouped by `Letter` and `X`.

```groovy order=result,source
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i % 3", "Y = randomInt(0, 100)")

result = source.updateBy(CumCountWhere("count", "Y >= 50", "Y <= 100"), "Letter", "X")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#CumCountWhere(java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.cum_count_where)
