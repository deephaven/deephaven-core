---
title: AggWSum
---

`AggWSum` returns an aggregator that computes the weighted sum of values, within an aggregation group, for each input column.

## Syntax

```
AggWSum(weightColumn, pairs...)
```

## Parameters

<ParamTable>

<Param name="weightColumn" type="String">

The weight column for the calculation.

</Param>
<Param name="pairs" type="String...">

The input/output names of source column(s) for the calculations.

- `"X"` will output the weighted sum of values in the `X` column for each group.
- `"Y = X"` will output the weighted sum of values in the `X` column for each group and rename it to `Y`.
  `"X, A = B"` will output the weighted sum of values in the `X` column for each group and the weighted sum of values in the `B` column and rename it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the weighted sum of values, within an aggregation group, for each input column.

## Examples

In this example, `AggWSum` returns the weighted sum of values of `Number`, as weighed by `Weight` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWSum

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
)

result = source.aggBy([AggWSum("Weight", "Number")], "X")
```

In this example, `AggWSum` returns the weighted sum of values of `Number` (renamed to `WSumNumber`), as weighed by `Weight` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWSum

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
)

result = source.aggBy([AggWSum("Weight", "WSumNumber = Number")], "X")
```

In this example, `AggWSum` returns the weighted sum of values of `Number` (renamed to `WSumNumber`), as weighed by `Weight` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggWSum
import static io.deephaven.api.agg.Aggregation.AggSum

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
    intCol("Weight", 1, 2, 1, 3, 2, 1, 4, 1, 2),
)

result = source.aggBy([AggWSum("Weight", "WSumNumber = Number"),AggSum("Sum = Number")], "X")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`sumBy`](./sumBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggWSum(java.lang.String,java.lang.String...))
