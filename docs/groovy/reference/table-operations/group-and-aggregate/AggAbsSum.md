---
title: AggAbsSum
---

`AggAbsSum` returns an aggregator that computes an absolute sum, within an aggregation group, for each input column.

## Syntax

```
AggAbsSum(columnNames...)
```

## Parameters

<ParamTable>
<Param name="columnNames" type="String...">

The source column(s) for the calculations.

- `"X"` will output the total sum of values in the `X` column for each group.
- `"Y = X"` will output the total sum of values in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the total sum of values in the `X` column for each group and the total sum of values in the `B` value column renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg("X")`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the absolute sum of values, within an aggregation group, for each input column.

## Examples

In this example, `AggAbsSum` returns the sum of absolute values of the `X` (renamed to `AbsSumX`) column grouped by `Letter`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAbsSum

source = emptyTable(20).update("X = i - 10", "Letter = (X % 2 == 0) ? `A` : `B`")

result = source.aggBy([AggAbsSum("AbsSumX = X")], "Letter")
```

In this example, `AggAbsSum` returns the sum of the absolute values of the `Number` column (renamed to `AbsSumNumber`), grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggAbsSum

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", -55, -76, 20, -130, -230, 50, -73, -137, 214),
)

result = source.aggBy([AggAbsSum("AbsSumNumber = Number")], "X", "Y")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggAbsSum(java.lang.String...))
