---
title: moveColumns
---

The `moveColumns` method creates a new table with specified columns moved to a specific column index value.

## Syntax

```
table.moveColumns(index, columnsToMove...)
```

## Parameters

<ParamTable>
<Param name="index" type="int">

Column index where the specified columns will be moved in the new table. The index is zero-based, so column index number 2 would be the third column.

</Param>
<Param name="columnsToMove" type="String...">

Columns to be moved.

</Param>
</ParamTable>

## Returns

A new table with specified columns moved to a specific column index value.

## Examples

The following example moves column `C` to the second position (1st index) in the new table.

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3),
)

result = source.moveColumns(1, "C")
```

The following example moves columns `B` and `C` to the second position (1st index) and third position in the new table.

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3),
)

result = source.moveColumns(1, "C", "D")
```

<!--TODO: Demonstrate moveToEnd when the param is fixed: -->

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#moveColumns(int,java.lang.String...))
