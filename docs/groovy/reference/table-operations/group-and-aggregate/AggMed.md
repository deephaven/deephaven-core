---
title: AggMed
---

`AggMed` returns an aggregator that computes the median value, within an aggregation group, for each input column.

## Syntax

```
AggMed(average, pairs...)
AggMed(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The source column(s) for the calculations.

- `"X"` will output the median value in the `X` column for each group.
- `"Y = X"` will output the median value in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the median value in the `X` column for each group and the median value in the `B` column while renaming it to `A`.

</Param>
<Param name="average" type="boolean">

Whether to average the two middle values for even-sized result sets of numeric types. If no value is given, the default behavior is to average the two center values.
See [AggSpecMedian](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpecMedian.html#averageEvenlyDivided()).

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the median value, within an aggregation group, for each input column.

## Examples

In this example, `AggMed` returns the median `Number` value as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMed

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMed("Number")], "X")
```

In this example, `AggMed` returns the median `Number` value (renamed to `Z`), as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMed

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMed("Z = Number")], "X")
```

In this example, `AggMed` returns the median `Number`, as grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMed

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMed("Number")], "X", "Y")
```

In this example, `AggMed` returns the median `Number`, and `AggMax` returns the maximum `Number`, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggMed
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggMed("MedNumber = Number"),AggMax("MaxNumber = Number")], "X")
```

<!--TODO: https://github.com/deephaven/deephaven.io/issues/2460 add code examples -->

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`medianBy`](./medianBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggMed(java.lang.String...))
