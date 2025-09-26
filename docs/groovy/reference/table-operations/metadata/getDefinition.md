---
title: getDefinition
---

The `getDefinition` method returns the source table's `TableDefinition`.

## Syntax

```groovy syntax
table.getDefinition()
```

## Parameters

This method takes no arguments.

## Returns

The source table's `TableDefinition`.

## Examples

```groovy order=:log
source = newTable(
    stringCol("Title", "content"),
    stringCol("ColumnName", "column_content"),
    stringCol("AnotherColumn", "string"),
)

println source.getDefinition()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getDefinition())
