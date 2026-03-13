---
title: AggDistinct
---

`AggDistinct` returns an aggregator that computes a set of distinct values within an aggregation group, for each input column.

## Syntax

```
AggDistinct(includeNulls, pairs...)
AggDistinct(pairs...)
```

## Parameters

<ParamTable>
<Param name="includeNulls" type="boolean">

If `true`, then null values are included in the result; otherwise null values are ignored. The default value is `false`.

</Param>
<Param name="pairs" type="String...">

The source column(s) for the calculations.

- `"X"` will output the set of distinct values in the `X` column for each group.
- `"Y = X"` will output the set of distinct values in the `X` column for each group and rename it to `"Y"`.
- `"X, A = B"` will output the set of distinct values in the `X` column for each group and the set of distinct values in the `B` value column renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes a set of distinct values, within an aggregation group, for each input column.

## Examples

In this example, `AggDistinct` returns the distinct set of values of `Y` as grouped by `X`. `includeNulls` is `true` and null values are included.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggDistinct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggDistinct(true, "Y")], "X")
```

In this example, `AggDistinct` returns the distinct set of values of `Y` as grouped by `X`. `includeNulls` is `false` and null values are ignored.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggDistinct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggDistinct(false, "Y")], "X")
```

In this example, `AggDistinct` returns the distinct set of values of `Y` (renamed to `Z`), as grouped by `X`. `includeNulls` is true and null values are included.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggDistinct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggDistinct(true, "Z = Y")], "X")
```

In this example, `AggDistinct` returns the distinct set of values of `Y` (renamed to `Z`), as grouped by `X`. `includeNulls` is `false` and null values are ignored. Also, `AggLast` returns the last `Number` integer, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggDistinct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", null),
    stringCol("Y", "M", "N", null, "N", "P", "M", null, "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggDistinct(false, "Z = Y")], "X")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggDistinct(boolean,java.lang.String...))
