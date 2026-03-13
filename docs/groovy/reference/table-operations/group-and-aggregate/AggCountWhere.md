---
title: AggCountWhere
---

`AggCountWhere` returns an aggregator that computes the number of values that pass a set of filters within an aggregation group.

## Syntax

```
AggCountWhere(resultColumn, filters...)
```

## Parameters

<ParamTable>
<Param name="resultColumn" type="String">

The name of the column that will contain the count of values that pass the filters.

</Param>
<Param name="filters" type="String...">

Formulas for filtering as a list of [Strings](../../query-language/types/strings.md).

Filters can be:

- A match filter. <!--TODO: add links [#474](https://github.com/deephaven/deephaven.io/issues/474) -->
- A conditional filter. This may be a custom boolean function. <!--TODO: add links [#201](https://github.com/deephaven/deephaven.io/issues/201) -->

</Param>
</ParamTable>

> [!TIP]
> Providing multiple filter strings in the `filters` parameter will result in an `AND` operation being applied to the
> filters. For example,
> `"Number % 3 == 0", "Number % 5 == 0"` will return the count of values where `Number` is evenly divisible by
> both `3` and `5`. You can also write this as a single conditional filter, `"Number % 3 == 0 && Number % 5 == 0"` and
> receive the same result.
>
> You can use the `||` operator to `OR` multiple filters. For example, `` Y == `M` || Y == `N` `` matches when `Y` equals
> `M` or `N`.

## Returns

An aggregator that computes the number of values within an aggregation group that pass the provided filters.

## Examples

In this example, `AggCountWhere` returns the number of values of `Number` that are `>= 20` and `< 99` as grouped by `X`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy(AggCountWhere("countNum", "Number >= 20", "Number < 99"), "X")
```

In this example, `AggCountWhere` returns the number of values of `Y` that are `M` or `N`, as grouped by `X`.

```groovy order=source,result
source = newTable(
        stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
        stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
        intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy(AggCountWhere("countY", "Y == `M` || Y == `N`"), "X")
```

In this example, `AggCountWhere` returns the number of rows where `Y` equals `M` and `Number` > `50` over the entire table (no grouping).

```groovy order=source,result
source = newTable(
        stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
        stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
        intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy(AggCountWhere("count", "Y == `M` && Number > 50"))
```

In this example, `AggCountWhere` returns the number of rows where `Number` is between `50` and `100` (inclusive), as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
        stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
        stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
        intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy(AggCountWhere("countNum", "Number >= 50", "Number <= 100"), "X", "Y")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`AggCountWhere`](./AggCountWhere.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggCountDistinct(java.lang.String...))
