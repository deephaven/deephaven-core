---
title: AggWAvg
---

`AggWAvg` returns an aggregator that computes the weighted average of values, within an aggregation group, for each input column.

## Syntax

```
AggWAvg(weightColumn, pairs...)
```

## Parameters

<ParamTable>
<Param name="weightColumn" type="String">

The weight column for the calculation.

</Param>
<Param name="pairs" type="String...">

The input/output names for the column(s) for the calculations.

- `"X"` will output the weighted average of values in the `X` column for each group.
- `"Y = X"` will output the weighted average of values in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the weighted average of values in the `X` column for each group and the weighted average of values in the `B` column and rename it to `A`.

</Param>

</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the weighted average of values, within an aggregation group, for each input column.

## Examples

In this example, `AggWAvg` returns the weighted average of values of `Number`, as weighed by `Weight` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
)

result = source.aggBy([AggWAvg("Weight", "Number")], "X")
```

In this example, `AggWAvg` returns the weighted average of values of `Number` (renamed to `WAvgNumber`), as weighed by `Weight` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
)

result = source.aggBy([AggWAvg("Weight", "WAvgNumber = Number")], "X")
```

In this example, `AggWAvg` returns the weighted average of values of `Number` (renamed to `WAvgNumber`), as weighed by `Weight` and grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
)

result = source.aggBy([AggWAvg("Weight", "WAvgNumber = Number")], "X", "Y")
```

In this example, `AggWAvg` returns the weighted average of values of `Number1` (renamed to `WAvgNumber1`), and the weighted average of values of `Number2` (renamed to `WAvgNumber2`), both as weighed by `Weight` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
    intCol("Number1", 55, 76, 20, 130, 230, 50, 73, 137, 214),
    intCol("Number2", 25, 14, 51, 23, 12, 15, 17, 72, 21),
)

result = source.aggBy([AggWAvg("Weight", "WAvgNumber1 = Number1", "WAvgNumber2 = Number2")], "X")
```

In this example, `AggWAvg` returns the weighted average of values of `Number` (renamed to `WAvgNumber`), as weighed by `Weight` and grouped by `X`, and `AggAvg` returns the total average of values of `Number`, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWAvg
import static io.deephaven.api.agg.Aggregation.AggAvg

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
)

result = source.aggBy([AggWAvg("Weight", "WAvgNumber = Number"),AggAvg("Avg = Number")], "X")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`avgBy`](./avgBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggWAvg(java.lang.String,java.lang.String...))
