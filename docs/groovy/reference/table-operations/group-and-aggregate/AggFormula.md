---
title: AggFormula
---

`AggFormula` returns an aggregator that computes a user-defined formula aggregation across specified columns. This allows you to apply custom formulas to groups of rows, leveraging built-in, mathematical, or user-defined functions.

## Syntax

```
AggFormula(formula)
AggFormula(formula, paramToken, columnNames...)
```

## Parameters

<ParamTable>
<Param name="formula" type="String">

The user-defined formula to apply to each group. This formula can contain:

- Built-in functions such as `min`, `max`, etc.
- Mathematical operations such as `*`, `+`, `/`, etc.
- [User-defined closures](../../../how-to-guides/groovy-closures.md)

The formula is typically specified as `OutputColumn = Expression` (when `paramToken` is `null`). The formula is applied to any column or literal value present in the formula. For example, `Out = KeyColumn * max(ValueColumn)`, produces an output column with the name `Out` and uses `KeyColumn` and `ValueColumn` as inputs.

If `paramToken` is not `null`, the formula can only be applied to one column at a time, and it is applied to the specified `paramToken`. In this case, the formula does not supply an output column, but rather it is derived from the `columnNames` parameter. The use of `paramToken` is deprecated.

Key column(s) can be used as input to the formula. When this happens, key values are treated as scalars.

</Param>
<Param name="paramToken" type="String">

The parameter name within the formula. If `paramToken` is `each`, then `formula` must contain `each`. For example, `max(each)`, `min(each)`, etc. Use of this parameter is deprecated. A non-null value is not permitted in [rollups](../create/rollup.md).

</Param>
<Param name="columnNames" type="String...">

The source column(s) for the calculations. The source column names are only used when `paramToken` is not `null`, and are thus similarly deprecated.

- `"X"` applies the formula to each value in the `X` column for each group.
- `"Y = X"` applies the formula to each value in the `X` column for each group and renames it to `Y`.
- `"X, A = B"` applies the formula to each value in the `X` column for each group and the formula to each value in the `B` column while renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum("X"), agg.AggAvg("X")])`, both the sum and the average aggregators produce column `X`, which results in a name conflict error.

## Returns

An aggregator that computes a user-defined formula within an aggregation group, for each input column.

## Examples

The following example uses `AggFormula` to calculate several formula aggregations by the `Letter` column. Since `paramToken` is `null`, the formulas are applied to any column or literal value present in the formula. The specified formulas operate on zero, one, two, and three different columns at a time.

```groovy order=source,result
source = emptyTable(20).update("X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`")

result = source.aggBy([
    AggFormula("OutA = sqrt(5.0)"),
    AggFormula("OutB = min(X)"),
    AggFormula("OutC = min(X) + max(Y)"),
    AggFormula("OutD = sum(X + Y + Z)"),
], "Letter")
```

The following example uses `AggFormula` to calculate the aggregate minimum across several columns by the `Letter` column. Since `paramToken` is not `null`, the formula is applied to the specified `paramToken`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFormula

source = emptyTable(20).update("X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`")

result = source.aggBy([AggFormula("min(each)", "each", "MinX = X", "MinY = Y", "MinZ = Z")], "Letter")
```

In this example, `AggFormula` is used to calculate the aggregated average across several column by the `Letter` and `Color` columns. Since `paramToken` is not `null`, the formula is applied to the specified `paramToken`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFormula

colors = ["Red", "Green", "Blue"]

source = emptyTable(40).update("X = 0.1 * i", "Y1 = Math.pow(X, 2)", "Y2 = Math.sin(X)", "Y3 = Math.cos(X)", "Letter = (i % 2 == 0) ? `A` : `B`", "Color = (String)colors[i % 3]")

myAgg = [AggFormula("avg(k)", "k", "AvgY1 = Y1", "AvgY2 = Y2", "AvgY3 = Y3")]

result = source.aggBy(myAgg, "Letter", "Color")
```

In this example, `AggFormula` is used to calculate the aggregate sum of squares across each of the `X`, `Y` and `Z` columns by the `Letter` column. Since `paramToken` is not `null`, the formula is applied to the specified `paramToken`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFormula

source = emptyTable(20).update("X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`")

myAgg = [AggFormula("sum(each * each)", "each", "SumSqrX = X", "SumSqrY = Y", "SumSqrZ = Z")]

result = source.aggBy(myAgg, "Letter")
```

In this example, `AggFormula` calls a user-defined closure to compute the range of the `X`, `Y`, and `Z` columns by the `Letter` column. Since `paramToken` is not `null`, the formula is applied to the specified `paramToken`.

> [!NOTE]
> The output of an `AggFormula` is of type `java.lang.Object` unless an explicit typecast is made in the `formula` string.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFormula

source = emptyTable(20).update("X = i", "Y = 2 * i", "Z = 3 * i", "Letter = (X % 2 == 0) ? `A` : `B`")

rangeX = {x -> x.max() - x.min()}

myAgg = [AggFormula("(int)rangeX(each)", "each", "RangeX = X", "RangeY = Y", "RangeZ = Z")]

result = source.aggBy(myAgg, "Letter")
```

## Related documentation

- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`AggMin`](./AggMin.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggFormula(java.lang.String,java.lang.String,java.lang.String...))
