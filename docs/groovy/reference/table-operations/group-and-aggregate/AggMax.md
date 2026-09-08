---
title: AggMax
---

`AggMax` returns an aggregator that computes the maximum value, within an aggregation group, for each input column.

## Syntax

```
AggMax(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The source column(s) for the calculations.

- `"X"` will output the maximum value in the `X` column for each group.
- `"Y = X"` will output the maximum value in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the maximum value in the `X` column for each group and the maximum value in the `B` column while renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the maximum value, within an aggregation group, for each input column.

## Examples

In this example, `AggMax` returns the maximum `Y` value as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMax("Y")], "X")
```

In this example, `AggMax` returns the maximum `Y` value (renamed to `Z`), as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMax("Z = Y")], "X")
```

In this example, `AggMax` returns the maximum `Y` string and maximum `Number` integer as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMax("Y", "Number")], "X")
```

In this example, `AggMax` returns the maximum `Number` integer, as grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMax("Number")], "X", "Y")
```

In this example, `AggMax` returns the maximum `Number` integer, and `AggLast` returns the last `Number` integer, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMax
import static io.deephaven.api.agg.Aggregation.AggLast

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMax("MaxNumber = Number"),AggLast("LastNumber = Number")], "X")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`maxBy`](./maxBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggMax(java.lang.String...))
