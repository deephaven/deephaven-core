---
title: constituentChangesPermitted
---

The `constituentChangesPermitted` method returns a boolean value indicating whether changes to the constituents of a partitioned table are permitted. This is completely unrelated to whether the constituents themselves are refreshing, or whether the underlying partitioned table is refreshing.

> [!NOTE]
> The underlying partitioned table must be refreshing if it contains any refreshing constituents.

Partitioned tables that specify `constituentChangesPermitted() == false` must be guaranteed never to change their constituents. Formally, it is expected that `table()` will never report additions, removals, or shifts, and that any modifications reported will not change values in the "constituent" column (that is, `table().getColumnSource(constituentColumnName())`).

## Syntax

```
constituentChangesPermitted()
```

## Parameters

This function does not take any parameters.

## Returns

A boolean value indicating whether changes to the constituents of a partitioned table are permitted.

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

[Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#constituentChangesPermitted())
