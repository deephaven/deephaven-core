---
title: constituentColumnName
---

The `constituentColumnName` method returns the name of the "constituent" column in a partitioned table. The constituent column is the column that is used to partition the table.

## Syntax

```
constituentColumnName()
```

## Parameters

This function does not take any parameters.

## Returns

The name of the partitioned table's "constituent" column, as a String.

## Examples

```groovy order=:log
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

partitionedTable = source.partitionBy("X")

println partitionedTable.constituentChangesPermitted()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#constituentColumnName())
