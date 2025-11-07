---
title: AggPct
---

`AggPct` returns an aggregator that computes the designated percentile of values, within an aggregation group, for each input column.

## Syntax

```
AggPct(percentile, average, pairs...)
AggPct(percentile, pairs...)
AggPct(inputColumn, average, percentileOutputs...)
AggPct(inputColumn, percentileOutputs...)
```

## Parameters

<ParamTable>

<Param name="percentile" type="double">

The percentile to use for all component aggregations.

</Param>

<Param name="pairs" type="String...">

The input/output column name pairs.

- `"X"` will output the designated percentile value in the `X` column for each group.
- `"Y = X"` will output the designated percentile value in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the designated percentile value in the `X` column for each group and the designated percentile value in the `B` value column renaming it to `A`.

</Param>
<Param name="average" type="boolean">

Whether to average the two middle values for even-sized result sets of numeric types. See [AggSpecMedian](https://deephaven.io/core/javadoc/io/deephaven/api/agg/spec/AggSpecMedian.html#averageEvenlyDivided()).

</Param>
<Param name="percentileOutputs" type="PercentileOutput...">

The percentile/output column name pairs for the component aggregations. See [PctOut(double, String)](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#PctOut(double,java.lang.String))

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the designated percentile value, within an aggregation group, for each input column.

## Examples

In this example, `AggPct` returns the 68th percentile value `Number` as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggPct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggPct(0.68, "PctNumber = Number")], "X")
```

In this example, `AggPct` returns the 68th percentile value `Number` and the 99th percentile value `Number` as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggPct
import static io.deephaven.api.agg.Aggregation.AggPct

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggPct(0.68, "Pct68Number = Number"),AggPct(0.99, "Pct99Number = Number")], "X")
```

In this example, `AggPct` returns the 97th percentile value `Number`, and `AggMed` returns the median `Number`, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggPct
import static io.deephaven.api.agg.Aggregation.AggMed

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggPct(0.97, "Pct97Number = Number"),AggMed("MedNumber = Number")], "X")
```

<!--TODO: https://github.com/deephaven/deephaven.io/issues/2460 add code examples -->

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,java.lang.String...))
