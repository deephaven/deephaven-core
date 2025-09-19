---
title: hasColumns
---

The `hasColumns` method is used to check whether a table contains a column (or columns) with the provided column name(s).

## Syntax

```groovy syntax
source.hasColumns(columnNames...)
source.hasColumns(columnNames)
```

## Parameters

<ParamTable>
<Param name="columnNames" type="String...">

The column name(s).

</Param>
<Param name="columnNames" type="Collection<String>">

The column name(s).

</Param>
</ParamTable>

## Returns

A boolean value that is `true` if all columns are in the table, or `false` if _any_ of the provided column names are not found in the table.

## Example

The following example checks for the existence of certain column names in a table with three columns.

```groovy order=:log
source = newTable(
    stringCol("Title", "content"),
    stringCol("ColumnName", "column_content"),
    stringCol("AnotherColumn", "string"),
)

println source.hasColumns("Title")
println source.hasColumns("NotAColumnName")
println source.hasColumns("Title", "ColumnName")
println source.hasColumns("Title", "NotAName")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#hasColumns(java.lang.String...))
