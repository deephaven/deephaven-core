---
title: AggGroup
---

`AggGroup` returns an aggregator that computes an [array](../../query-language/types/arrays.md) of all values within an aggregation group, for each input column.

## Syntax

```
AggGroup(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The source column(s) for the calculations.

- `"X"` will output an array of all values in the `X` column for each group.
- `"Y = X"` will output an [array](../../query-language/types/arrays.md) of all values in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output an [array](../../query-language/types/arrays.md) of all values in the `X` column for each group and an [array](../../query-language/types/arrays.md) of all values in the `B` value column renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes an [array](../../query-language/types/arrays.md) of all values, within an aggregation group, for each input column.

## Examples

In this example, `AggGroup` returns an [array](../../query-language/types/arrays.md) of values of `Y` as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggGroup

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggGroup("Y")], "X")
```

In this example, `AggGroup` returns an [array](../../query-language/types/arrays.md) of values of `Y` (renamed to `Z`), as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggGroup

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggGroup("Z = Y")], "X")
```

In this example, `AggGroup` returns an [array](../../query-language/types/arrays.md) of values of `Y` (renamed to `Letters`), and an [array](../../query-language/types/arrays.md) of values of `Number` (renamed to `Numbers`), as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggGroup

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)


result = source.aggBy([AggGroup("Letters = Y", "Numbers = Number")], "X")
```

In this example, `AggGroup` returns an [array](../../query-language/types/arrays.md) of values (the `Number` column renamed to `Numbers`), as grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggGroup

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggGroup("Numbers = Number")], "X", "Y")
```

In this example, `AggGroup` returns an [array](../../query-language/types/arrays.md) of values `Number` (renamed to `Numbers`), and [`AggMax`](./AggMax.md) returns the maximum `Number` integer, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggGroup
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggGroup("Numbers = Number"),AggMax("MaxNumber = Number")], "X")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`AggMax`](./AggMax.md)
- [Arrays](../../query-language/types/arrays.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggGroup(java.lang.String...))
