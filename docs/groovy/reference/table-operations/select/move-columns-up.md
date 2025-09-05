---
title: moveColumnsUp
---

The `moveColumnsUp` method creates a new table with specified columns appearing first in order, to the far left.

## Syntax

```
table.moveColumnsUp(columnsToMove...)
```

## Parameters

<ParamTable>
<Param name="columnsToMove" type="String...">

Columns to move to the left side of the new table. Columns can be renamed with syntax matching `NewColumnName=OldColumnName`.

</Param>
</ParamTable>

## Returns

A new table with specified columns appearing first in order, to the far left.

## Examples

The following query moves column `B` to the first position in the resulting table.

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3),
)

result = source.moveColumnsUp("B")
```

The following example moves columns `B` and `D` to the first positions in the resulting table.

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3),
)

result = source.moveColumnsUp("B", "D")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#moveColumnsUp(java.lang.String...))
