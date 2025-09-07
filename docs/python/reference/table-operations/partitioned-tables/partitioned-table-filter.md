---
title: filter
---

The `filter` method creates a new `PartitionedTable` containing only the rows meeting the filter criteria. This method is analagous to a [`where`](../filter/where.md) applied to a regular table.

> [!WARNING]
> Partitioned tables cannot be filtered on the `__CONSTITUENT__` column.

## Syntax

```python syntax
filter(
  filters: Union[str, Filter, Sequence[str], Sequence[Filter]]
) -> PartitionedTable
```

## Parameters

<ParamTable>
<Param name="filters" type="Union[str, Filter, Sequence[str], Sequence[Filter]]">

The filter conditions or Filter objects to apply.

</Param>
</ParamTable>

## Returns

A `PartitionedTable`.

## Example

In the following example, `partitioned_table` is filtered to create a new partitioned table containing only the rows where `IntCol` is greater than 2.

```python order=result_filtered,result,source
from deephaven import empty_table

source = empty_table(5).update(["IntCol = i", "StrCol = `value`"])
partitioned_table = source.partition_by(["IntCol"])

result = partitioned_table.table
result_filtered = partitioned_table.filter("IntCol > 2").table
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.filter)
