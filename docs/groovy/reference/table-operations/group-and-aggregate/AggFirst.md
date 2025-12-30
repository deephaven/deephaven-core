---
title: AggFirst
---

`AggFirst` returns an aggregator that computes the first value, within an aggregation group, for each input column.

## Syntax

```
AggFirst(pairs...)
```

## Parameters

<ParamTable>
<Param name="pairs" type="String...">

The source columns for the calculations.

- `"X"` will output the first value in the `X` column for each group.
- `"Y = X"` will output the first value in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the first value in the `X` column for each group and the first value in the `B` column while renaming it to `A`.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that computes the first value, within an aggregation group, for each input column.

## Examples

In this example, `AggFirst` returns the first `Y` value as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggFirst("Y")], "X")
```

In this example, `AggFirst` returns the first `Y` value (renamed to `Z`), as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggFirst("Z = Y")], "X")
```

In this example, `AggFirst` returns the first `Y` string and first `Number` integer, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggFirst("Y", "Number")], "X")
```

In this example, `AggFirst` returns the first `Number` integer, as grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggFirst("Number")], "X", "Y")
```

In this example, `AggFirst` returns the first `Number` integer, and `AggLast` returns the last `Number` integer, as grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFirst
import static io.deephaven.api.agg.Aggregation.AggLast

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "P", "O", "N", "P", "M", "O", "P", "N"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.aggBy([AggFirst("FirstNumber = Number"),AggLast("LastNumber = Number")], "X")
```

## Related Documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggFirst(java.lang.String...))
