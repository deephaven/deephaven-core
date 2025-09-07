---
title: AggAvg
---

`AggAvg` returns an aggregator that computes the average (mean) of values, within an aggregation group, for each input column.

## Syntax

```
AggAvg(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The source column(s) for the calculations.

- `"X"` will output the average of values in the `X` column for each group.
- `"Y = X"` will output the average of values in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the average of values in the `X` column for each group and the average of values in the `B` value column renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the average (mean) of values, within an aggregation group, for each input column.

## Examples

In this example, `AggAvg` returns the average value of `Number` as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggAvg("Number")], "X")
```

In this example, `AggAvg` returns the average value of `Number` (renamed to `Avg`), as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggAvg("Avg = Number")], "X")
```

In this example, `AggAvg` returns the average value of `Number`, as grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggAvg("Avg = Number")], "X", "Y")
```

In this example, `AggAvg` returns the average value of `Number`, and `AggStd` returns the standard deviation of `Number`, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggStd

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggAvg("AvgNumber = Number"),AggStd("StdNumber = Number")], "X")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggAvg(java.lang.String...))
