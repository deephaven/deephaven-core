---
title: moveColumnsDown
---

The `moveDownColumns` method creates a new table with specified columns appearing last in order, to the far right.

## Syntax

```
table.moveColumnsDown(columnsToMove...)
```

## Parameters

<ParamTable>
<Param name="columnsToMove" type="String...">

Columns to move to the right side of the new table. Columns can be renamed with syntax matching `NewColumnName=OldColumnName`.

</Param>
</ParamTable>

## Returns

A new table with specified columns appearing last in order, to the far right.

## Examples

The following example moves column `B` to the last position in the resulting table.

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3),
)

result = source.moveColumnsDown("B")
```

The following example moves columns `B` and `D` to the last positions in the resulting table.

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3),
)

result = source.moveColumnsDown("B", "D")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#moveColumnsDown(java.lang.String...))
