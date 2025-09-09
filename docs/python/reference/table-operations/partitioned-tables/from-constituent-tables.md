---
title: from_constituent_tables
---

`from_constituent_tables` is a `PartitionedTable` method that creates a new `PartitionedTable` with a single column named `__CONSTITUENT__`, containing the provided constituent tables. The resultant partitioned table has no key columns, and its `unique_keys` and `constituent_changes_permitted` are False. It is a `PartitionedTable` class method, meaning it can be called on an instance of the class or the class itself.

## Syntax

```python syntax
PartitionedTable.from_constituent_tables(
  tables: List[Table],
  constituent_table_columns: List[Column]
) -> PartitionedTable
```

## Parameters

<ParamTable>
<Param name="tables" type="List[Table]">

The constituent tables.

</Param>
<Param name="constituent_table_columns" type="List[Column]">

A list of column definitions compatible with all the constituent tables. Default is `None`. When `constituent_table_columns` is not provided, it will be set to the column definitions of the first table in the provided constituent tables.

</Param>
</ParamTable>

## Returns

A `PartitionedTable` with a single column named `__CONSTITUENT__`, containing the provided constituent tables. The resulting `PartitionedTable` has no key columns, and both its `unique_keys` and `constituent_changes_permitted` properties are set to `False`.

## Example

The following example uses `from_constituent_tables` to construct a partitioned table with no key columns.

```python order=result,source1,source2
from deephaven.table import PartitionedTable
from deephaven import empty_table

source1 = empty_table(5).update(["X = i", "Y = (i % 2 == 0) ? `A` : `B`"])
source2 = empty_table(5).update(["X = i + 2", "Y = (i % 3 == 1) ? `C` : `B`"])

pt = PartitionedTable.from_constituent_tables(tables=[source1, source2])
result = pt.table
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.from_constituent_tables)
