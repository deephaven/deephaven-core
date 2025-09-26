---
title: constituents
---

The `constituents` method returns all the constituent tables of a partitioned table.

The results will be managed by the enclosing liveness scope.

## Syntax

```
constituents()
```

## Parameters

This function does not take any parameters.

## Returns

An array of tables, each of which contains a unique key from the key column(s).

## Examples

```groovy order=source,result0,result1,result2
source = emptyTable(10).update('Key = i % 3', 'Value = i')

partitionedTable = source.partitionBy('Key')

result = partitionedTable.constituents()

result0 = result[0]
result1 = result[1]
result2 = result[2]
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#constituents())
