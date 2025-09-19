---
title: AggSortedLast
---

`AggSortedLast` returns an aggregator that sorts a table in descending order and then computes the first value, within an aggregation group, for each input column.

> [!NOTE]
> `AggSortedLast` will produce the same results as a [`sort`](../sort/sort.md) operation followed by [`AggLast`](./AggLast.md).

## Syntax

```
AggSortedLast(sortColumn, pairs...)
AggSortedLast(sortColumns, pairs...)
```

## Parameters

<ParamTable>
<Param name="sortColumn" type="String">

The column to sort by.

</Param>
<Param name="pairs" type="String...">

The input/output column names for calculations.

- `"X"` will output the last value in the `X` column for each group.
- `"Y = X"` will output the last value in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the last value in the `X` column for each group and the last value in the `B` column while renaming it to `A`.

</Param>
<Param name="sortColumns" type="Collection<? extends String>">

The sort column names.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that sorts the table in descending order and then computes the first value, within an aggregation group, for each input column.

## Examples

In this example, `AggSortedLast` returns the last `Y` value as sorted by `Z` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedLast

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedLast("Z", "Y")], "X")
```

In this example, `AggSortedLast` returns the last `Y` value (renamed to `Z`), as sorted by `Z` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedLast

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedLast("Z", "Z = Y")], "X")
```

In this example, `AggSortedLast` returns the last `Y` string and last `Z` integer, as sorted by `Z` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedLast

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedLast("Z", "Y", "Z")], "X")
```

In this example, `AggSortedLast` returns the last `Z` integer, as sorted by `Z` and grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedLast

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedLast("Z", "Z")], "X", "Y")
```

In this example, `AggSortedLast` returns the last `Y` string, and [`AggMax`](./AggMax.md) returns the maximum `Z` integer, as sorted by `Z` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedLast
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedLast("Z", "SortedLastY = Y"),AggMax("MaxZ = Z")], "X")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [`aggBy`](./aggBy.md)
- [`lastBy`](./lastBy.md)
- [`sort`](../sort/sort.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggSortedLast(java.lang.String,java.lang.String...))
