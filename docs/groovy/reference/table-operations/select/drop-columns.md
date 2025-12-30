---
title: dropColumns
---

The `dropColumns` method creates a table with the same number of rows as the source table but omits any columns included in the arguments.

## Syntax

```
table.dropColumns(columnNames...)
```

## Parameters

<ParamTable>
<Param name="columnNames" type="String...">

Columns that will be omitted in the new table.

</Param>
<Param name="columnNames" type="Collection<String>">

Columns that will be omitted in the new table.

</Param>
<Param name="columnNames" type="ColumnName...">

Columns that will be omitted in the new table.

</Param>
</ParamTable>

## Returns

A new table that includes the same number of rows as the source table, with the specified column names omitted.

## Examples

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3)
)

result = source.dropColumns("B","D")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#dropColumns(java.lang.String...))
