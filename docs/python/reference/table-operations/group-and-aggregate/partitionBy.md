---
title: partition_by
---

`partition_by` partitions a table into constituent tables (also called subtables) via one or more key columns. The resultant object is called a partitioned table. A partitioned table is a table with a column containing other Deephaven tables, plus additional key columns that are used to index and access particular constituent tables. All constituent tables of a single partitioned table _must_ have the same schema.

## Syntax

```
table.partition_by(by: Union[str, list[str]], drop_keys: bool = False) -> PartitionedTable
```

## Parameters

<ParamTable>
<Param name="by" type="Union[str, list[str]]">

The column(s) by which to group data.

</Param>
<Param name="drop_keys" type="bool" optional>

Whether to drop key columns in the constituent tables. Default is `False`.

</Param>
</ParamTable>

## Returns

A `PartitionedTable` containing a subtable for each group.

## Examples

The following example partitions a table into subtables using a single key column. After creating the partitioned table, [`keys`](../partitioned-tables/keys.md) is used to generate a table showing all of the unique keys in `partitioned_table`. Constituent tables are then grabbed by index with [`constituent_tables`](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.constituent_tables).

```python order=source,partition_keys,const_table1,const_table2,const_table3
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

partitioned_table = source.partition_by(by="X")

partition_keys = partitioned_table.keys()

const_table1 = partitioned_table.constituent_tables[0]
const_table2 = partitioned_table.constituent_tables[1]
const_table3 = partitioned_table.constituent_tables[2]
```

The following example partitions the same `source` table by two key columns. As a result, the keys that define `partitioned_table` are unique combinations of values in the `X` and `Y` columns. After `source` is partitioned into subtables, [`get_constituent`](../partitioned-tables/get-constituent.md) is used to grab a constituent table based on its key.

```python order=source,result_a_m
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

partitioned_table = source.partition_by(by=["X", "Y"])

result_a_m = partitioned_table.get_constituent(key_values=["A", "M"])
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to group and ungroup data](../../../how-to-guides/grouping-data.md)
- [How to create and use partitioned tables](../../../how-to-guides/partitioned-tables.md)
- [`from_constituent_tables`](../partitioned-tables/from-constituent-tables.md)
- [`from_partitioned_table`](../partitioned-tables/from-partitioned-table.md)
- [`get_constituent`](../partitioned-tables/get-constituent.md)
- [`agg_by`](./aggBy.md)
- [`partitioned_agg_by`](./partitionedAggBy.md)
- [`ungroup`](./ungroup.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#partitionBy(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.partition_by)
