---
title: keyColumnNames
---

The `keyColumnNames` method returns the names of all "key" columns that are part of `table().getDefinition()`. If there are no key columns, the result will be empty. This set is explicitly ordered.

## Syntax

```
keyColumnNames()
```

## Parameters

This function does not take any parameters.

## Returns

The names of all "key" columns that are part of `table().getDefinition()`, as a `Set<String>`.

## Examples

```groovy order=:log
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

partitionedTable = source.partitionBy("X")

println partitionedTable.keyColumnNames()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#keyColumnNames())
