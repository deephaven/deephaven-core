---
title: numColumns
---

The `numColumns` method returns the number of columns in a table. This is equivalent to `getDefinition().getColumns().length`.

## Syntax

```groovy syntax
source.numColumns()
```

## Parameters

This method takes no parameters.

## Returns

The number of columns defined in the source table.

## Example

The following example checks for the existence of certain column names in a table with three columns.

```groovy order=:log
source = newTable(
    stringCol("Title", "content"),
    stringCol("ColumnName", "column_content"),
    stringCol("AnotherColumn", "string"),
)

println source.numColumns()
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#numColumns())
