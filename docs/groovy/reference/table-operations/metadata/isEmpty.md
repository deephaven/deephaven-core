---
title: isEmpty
---

The `isEmpty` method returns a boolean value that is `true` if the table is empty (i.e., `size() == 0`), or `false` if the table is not empty.

## Syntax

```groovy syntax
source.isEmpty()
```

## Parameters

This method takes no arguments.

## Returns

A boolean value that is `true` if the table is empty or `false` if the table is not empty.

## Example

```groovy order=:log
t1 = newTable(
    stringCol("Title", "content"),
    stringCol("ColumnName", "column_content"),
    stringCol("AnotherColumn", "string"),
)

t2 = emptyTable(0)

println t1.isEmpty()
println t2.isEmpty()
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#isEmpty())
