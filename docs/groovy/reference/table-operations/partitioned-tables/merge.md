---
title: merge
---

The `merge` method creates a new `Table` that contains the rows from all the constituent tables of this `PartitionedTable`, in the same relative order as the underlying partitioned table and its constituents. If constituent tables contain extra columns not in the constituent definition, those columns will be ignored. If constituent tables are missing columns in the constituent definition, the corresponding output rows will be null.

## Syntax

```
merge()
```

## Parameters

This function does not take any parameters.

## Returns

A new `Table` that contains the rows from all the constituent tables of this `PartitionedTable`, in the same relative order as the underlying partitioned table and its constituents.

## Examples

```groovy order=result,source
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

partitionedTable = source.partitionBy("X")

result = partitionedTable.merge()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#merge())
