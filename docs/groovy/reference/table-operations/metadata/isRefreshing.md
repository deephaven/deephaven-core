---
title: isRefreshing
---

The `isRefreshing` method returns a boolean value that is `true` if the table is refreshing, or `false` if it is not.

## Syntax

```groovy syntax
source.isRefreshing()
```

## Parameters

This method takes no arguments.

## Returns

A boolean value that is `true` if the table is refreshing or `false` if it is not.

## Example

In this example, we create two tables - a static table and a time table - and check whether they are refreshing.

```groovy order=:log
t1 = newTable(
    stringCol("Title", "content"),
    stringCol("ColumnName", "column_content"),
    stringCol("AnotherColumn", "string"),
)

t2 = timeTable("PT1S")

println t1.isRefreshing()
println t2.isRefreshing()
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#isRefreshing())
