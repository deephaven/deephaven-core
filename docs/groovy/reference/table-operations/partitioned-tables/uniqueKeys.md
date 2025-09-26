---
title: uniqueKeys
---

The `uniqueKeys` method returns a boolean indicating whether the keys (key column values for a row considered as a tuple) in the underlying partitioned table are unique.

If keys are unique, one can expect that `table().selectDistinct(keyColumnNames.toArray(String[]::new))` is equivalent to `table().view(keyColumnNames.toArray(String[]::new))`.

## Syntax

```
uniqueKeys()
```

## Parameters

This function does not take any parameters.

## Returns

A boolean indicating whether the keys in the underlying partitioned table are unique.

## Examples

```groovy order=:log
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

partitionedTable = source.partitionBy("X")

println partitionedTable.uniqueKeys()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#uniqueKeys())
