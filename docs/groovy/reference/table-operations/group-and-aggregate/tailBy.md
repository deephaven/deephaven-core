---
title: tailBy
---

`tailBy` returns the last `n` rows for each group.

## Syntax

```
table.tailBy(nRows, groupByColumnNames...)
```

## Parameters

<ParamTable>
<Param name="nRows" type="long">

The number of rows to return for each group.

</Param>
<Param name="groupByColumnNames" type="String...">

The column(s) by which to group data.

- `"X"` will output the entire last `n` row(s) of each group in column `X`.
- `"X", "Y"` will output the entire last `n` row(s) of each group designated from the `X` and `Y` columns.

</Param>
<Param name="groupByColumnNames" type="Collection<String>">

The column(s) by which to group data.

- `"X"` will output the entire last `n` row(s) of each group in column `X`.
- `"X", "Y"` will output the entire last `n` row(s) of each group designated from the `X` and `Y` columns.

</Param>
</ParamTable>

## Returns

A new table containing the last `n` rows for each group.

## Examples

In this example, `tailBy` returns the last 2 rows, as grouped by `X`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "A", "B", "A", "B", "B", "B"),
    stringCol("Y", "M", "M", "M", "N", "M", "M", "M", "M", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.tailBy(2, "X")
```

In this example, `tailBy` returns the last 2 rows, as grouped by `X` and `Y`.

```groovy order=source,result
source = newTable(
    stringCol("X", "A", "B", "A", "A", "B", "A", "B", "B", "B"),
    stringCol("Y", "M", "M", "M", "N", "M", "M", "M", "M", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

result = source.tailBy(2, "X", "Y")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#tailBy(long,java.lang.String...))

<!--TODO: https://github.com/deephaven/deephaven-core/issues/778> -->
