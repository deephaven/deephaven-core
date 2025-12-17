---
title: from_partitioned_table
---

The `from_partitioned_table` method creates a new `PartitionedTable` from an underlying partitioned table with specifications such as which key columns to include, whether to allow changes to constituent tables, and more. It is a class method, meaning it can be called on both an instance of a class, or the class itself.

> [!WARNING]
> The `key_cols`, `unique_keys`, `constituent_column`, `constituent_table_columns`, and `constituent_changes_permitted` parameters either must _all_ be `None`, or must _all_ have values.

## Syntax

```python syntax
PartitionedTable.from_partitioned_table(
  table: Table,
  key_cols: Union[str, List[str]] = None,
  unique_keys: bool = None,
  constituent_column: str = None,
  constituent_table_columns: List[Column] = None,
  constituent_changes_permitted: bool = None,
) -> PartitionedTable
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The underlying `PartitionedTable`.

</Param>
<Param name="key_cols" type="Union[str, List[str]]" optional>

The key column(s) of `table`.

If `None`, the names of _all_ columns with a non-Table data type will be used as key columns.

</Param>
<Param name="unique_keys" type="bool" optional>

Whether the keys in `table` are guaranteed to be unique.

If `None`, the value defaults to `False`.

</Param>
<Param name="constituent_column" type="str" optional>

The constituent column name in `table`.

If `None`, the value defaults to the name of the first column with a Table data type (usually `__CONSTITUENT__`).

</Param>
<Param name="constituent_table_columns" type="List[Column]" optional>

The column definitions of the constituent table.

If `None`, the value defaults to the column definitions of the first cell (constituent table) in the constituent column. Consequently, the constituent column cannot be empty.

</Param>
<Param name="constituent_changes_permitted" type="bool" optional>

Whether the constituent tables can be changed.

If `None`, the value defaults to the result of `table.is_refreshing`.

</Param>
</ParamTable>

## Returns

A `PartitionedTable`.

## Example

The following example uses `from_partitioned_table` to construct a partitioned table from an underlying partitioned table (`agg_table`). It calls the method on the `PartitionedTable` class itself, and sets each of the optional input parameters.

```python order=source,agg_table
from deephaven.table import PartitionedTable
from deephaven import empty_table
from deephaven import agg

source = empty_table(50).update(["X=i", "Y=i%13", "Z=X*Y"])

pt = source.partition_by("Y")

agg_partition = agg.partition("aggPartition")
agg_table = source.agg_by(agg_partition, ["Y"])
new_pt = PartitionedTable.from_partitioned_table(
    table=agg_table,
    key_cols="Y",
    unique_keys=True,
    constituent_column="aggPartition",
    constituent_table_columns=source.columns,
    constituent_changes_permitted=True,
)
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.from_partitioned_table)
