---
title: get_constituent
---

The `get_constituent` method returns a single constituent table according to the specified key column value(s). If there are no matching rows, the result is `None`. If there are multiple matching rows, a DHError is thrown.

## Syntax

```python syntax
get_constituent(key_values: Union[Any, Sequence[Any]]) -> Table
```

## Parameters

<ParamTable>
<Param name="key_values" type="Union[Any, Sequence[Any]]">

The values for key column(s) to match.

</Param>
</ParamTable>

## Returns

The specified constituent table, or `None`.

## Examples

The following example creates a `source` table and then partitions it by the `Key` column. The partitioned table's unique keys are `1`, `2`, and `3`. The constituent table with the key `2` is then grabbed via `get_constituent`.

```python order=result,source
from deephaven import empty_table

source = empty_table(10).update(["Key = i % 3", "Value = i"])
partitioned_table = source.partition_by(["Key"])

result = partitioned_table.get_constituent(2)
```

The following example creates a `source` table and then partitions it by the `Key1` and `Key2` columns. The `result` table is then created via `get_constituent`, where `Key1` is `0` and `Key2` is `2`.

```python order=result,source
from deephaven import empty_table

source = empty_table(10).update(["Key1 = i % 2", "Key2 = i % 3", "Value = i + 25"])
partitioned_table = source.partition_by(["Key1", "Key2"])

result = partitioned_table.get_constituent([0, 2])
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.get_constituent)
