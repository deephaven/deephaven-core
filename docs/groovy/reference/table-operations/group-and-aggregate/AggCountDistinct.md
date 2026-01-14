---
title: AggCountDistinct
---

`AggCountDistinct` returns an aggregator that computes the number of distinct values, within an aggregation group, for each input column.

## Syntax

```
AggCountDistinct(countNulls, pairs...)
AggCountDistinct(pairs...)
```

## Parameters

<ParamTable>
<Param name="countNulls" type="boolean">

If `true`, null values are counted as a distinct value; otherwise null values are ignored. The default value is `false`.

</Param>
<Param name="pairs" type="String...">

The source column(s) for the calculations.

- `"X"` will output the number of distinct values in the `X` column for each group.
- `"Y = X"` will output the number of distinct values in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the number of distinct values in the `X` column for each group and the number of distinct values in the `B` column while renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the number of distinct values, within an aggregation group, for each input column.

## Examples

In this example, `AggCountDistinct` returns the number of distinct values of `Y` as grouped by `X`

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggCountDistinct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy([AggCountDistinct("Y")], "X")
```

In this example, `AggCountDistinct` returns the number of distinct values of `Y` (renamed to `Z`), as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggCountDistinct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy([AggCountDistinct("Z = Y")], "X")
```

In this example, `AggCountDistinct` returns the number of distinct values of `Y` and the number of distinct values of `Number`, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggCountDistinct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy([AggCountDistinct("Y", "Number")], "X")
```

In this example, `AggCountDistinct` returns the number of distinct values of `Number`, as grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggCountDistinct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy([AggCountDistinct("Number")], "X", "Y")
```

In this example, `AggCountDistinct` returns the number of distinct values of `Number`, and `AggLast` returns the last `Number` integer, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggCountDistinct
import static io.deephaven.api.agg.Aggregation.AggLast

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "A", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "M", "N", "N", "M", "O", "P", "N", "M", "N", "M", "N", "N", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 55, 130, 230, 50, 76, 137, 214, 55, 76, 55, 130, 230, 50, 76, 137, 214),
)

result = source.aggBy([AggCountDistinct("FirstNumber = Number"),AggLast("LastNumber = Number")], "X")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggCountDistinct(java.lang.String...))
