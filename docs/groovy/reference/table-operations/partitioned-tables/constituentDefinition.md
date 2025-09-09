---
title: constituentDefinition
---

The `constituentDefinition` method returns a `TableDefinition` shared by or mutually compatible with all of the partitioned table's constituent tables.

## Syntax

```
constituentDefinition()
```

## Parameters

This function does not take any parameters.

## Returns

A `TableDefinition` shared by or mutually compatible with all of the partitioned table's constituent tables.

## Examples

```groovy order=:log
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

partitionedTable = source.partitionBy("X")

println partitionedTable.constituentDefinition()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#constituentDefinition())
