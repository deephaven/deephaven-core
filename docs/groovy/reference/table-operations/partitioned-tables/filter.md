---
title: filter
---

The `filter` method creates a new `PartitionedTable` from the result of applying filters to the underlying partitioned table.

> [!NOTE]
> Filters must not reference the "constituent" column.

## Syntax

```
filter(filters)
```

## Parameters

<ParamTable>
<Param name="filters" type="Collection<? extends Filter>">

The filters to apply. Must not reference the "constituent" column.

</Param>
</ParamTable>

## Returns

A new `PartitionedTable` with the supplied `filters` applied.

## Examples

```groovy order=resultFiltered,result,source
source = emptyTable(5).update('IntCol = i', 'StrCol = `value`')
partitionedTable = source.partitionBy('IntCol')

filt = Filter.from('IntCol > 2')

result = partitionedTable.table()
resultFiltered = partitionedTable.filter(filt).table()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#filter(java.util.Collection))
