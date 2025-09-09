---
title: AggUnique
---

`AggUnique` returns an aggregator that computes:

- the single unique value contained in each specified column,
- a customizable value, if there are no values present,
- or a customizable value, if there is more than one value present.

## Syntax

```
AggUnique(includeNulls, nonUniqueSentinel, pairs...)
AggUnique(includeNulls, pairs...)
AggUnique(pairs...)
```

## Parameters

<ParamTable>
<Param name="includeNulls" type="boolean">

When set to `true`, the aggregation treats `null` values as a countable value. The default value is `false`.

</Param>
<Param name="nonUniqueSentinel" type="UnionObject">

The value to output for non-unique groups. The default value is `null`.

</Param>
<Param name="pairs" type="String...">

The input/output column names to compute uniqueness for.

- `"X"` will output the single unique value in the `X` column for each group.
- `"Y = X"` will output the single unique value in the `X` column for each group and rename it to `Y`.
- `"X", "A = B"` will output the single unique value in the `X` column for each group and the single unique value in the `B` value column renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the single unique value, within an aggregation group, for each input column. If there are no unique values or if there are multiple values, the aggregator returns a specified value.

## Examples

In the following example, `AggUnique` is used to find and return only the unique values of column `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggUnique

source = newTable(
    stringCol("X", "A", "A", "B", "B", "B", "C", "C", "D"),
    stringCol("Y", "Q", "Q", "R", "R", null, "S", "T", null)
)

result = source.aggBy([AggUnique("Y")], "X")
```

In the following example, `AggUnique` is used to find and return only the unique values of column `X`. Nulls are included.

<!--TODO: update example https://github.com/deephaven/deephaven-core/issues/1661 -->

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggUnique

source = newTable(
    stringCol("X", "A", "A", "B", "B", "B", "C", "C", "D"),
    stringCol("Y", "Q", "Q", "R", "R", null, "S", "T", null)
)

result = source.aggBy([AggUnique(true, "Y")], "X")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`AggDistinct`](./AggDistinct.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggUnique(java.lang.String...))
