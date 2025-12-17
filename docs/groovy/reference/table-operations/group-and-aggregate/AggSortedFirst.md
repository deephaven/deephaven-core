---
title: AggSortedFirst
---

`AggSortedFirst` returns an aggregator that sorts a table in ascending order and then computes the first value, within an aggregation group, for each input column.

> [!NOTE]
> `AggSortedFirst` will produce the same results as a [`sort`](../sort/sort.md) operation followed by [`AggFirst`](./AggLast.md).

## Syntax

```
AggSortedFirst(sortColumn, pairs...)
AggSortedFirst(sortColumns, pairs...)
```

## Parameters

<ParamTable>
<Param name="sortColumn" type="String">

The column to sort by.

</Param>
<Param name="pairs" type="pairs...">

The input/output column name pairs.

- `"X"` will output the first value in the `X` column for each group.
- `"Y = X"` will output the first value in the `X` column for each group and rename it to `Y`.
- `"X, A = B"` will output the first value in the `X` column for each group and the first value in the `B` column, while renaming it to `A`.

</Param>
<Param name="sortColumns" type="Collection<? extends String>">

The sort column names.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.aggBy([agg.AggSum(“X”), agg.AggAvg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

An aggregator that sorts the table in ascending order and then computes the first value, within an aggregation group, for each input column.

## Examples

In this example, `AggSortedFirst` returns the first `Y` value as sorted by `Z` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedFirst("Z", "Y")], "X")
```

In this example, `AggSortedFirst` returns the first `Y` value (renamed to `Z`), as sorted by `Z` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedFirst("Z", "Z = Y")], "X")
```

In this example, `AggSortedFirst` returns the first `Y` string and first `Z` integer, as sorted by `Z` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 1, 2, 3, 1, 2, 4, 1, 2),
)

result = source.aggBy([AggSortedFirst("Z", "Y", "Z")], "X")
```

In this example, `AggSortedFirst` returns the first `Z` integer, as sorted by `Z` and grouped by `X` and `Y`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedFirst

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedFirst("Z", "Z")], "X", "Y")
```

In this example, `AggSortedFirst` returns the first `Y` string, and [`AggMax`](./AggMax.md) returns the maximum `Z` integer, as sorted by `Z` and grouped by `X`.

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggSortedFirst
import static io.deephaven.api.agg.Aggregation.AggMax

source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "N", "O", "P", "N", "P", "N", "", "Q", "O"),
    intCol("Z", 3, 2, 1, 1, 3, 1, 4, 1, 2),
)

result = source.aggBy([AggSortedFirst("Z", "SortedFirstY = Y"),AggMax("MaxZ = Z")], "X")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [`aggBy`](./aggBy.md)
- [`firstBy`](./firstBy.md)
- [`sort`](../sort/sort.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggSortedFirst(java.util.Collection,java.lang.String...))
