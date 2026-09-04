---
title: PartitionedTable's metadata methods
sidebar_label: Metadata methods
---

The `PartitionedTable` class has a handful of methods that return metadata about the `PartitionedTable`.

Examples on this page rely on the following code block:

```python test-set=1
from deephaven import empty_table

source = empty_table(5).update(["IntCol = i", "StrCol = `value`"])
partitioned_table = source.partition_by(["IntCol"])
```

## `constituent_changes_permitted`

The `constituent_changes_permitted` method returns a boolean indicating whether the constituent tables can be changed.

```python syntax
PartitionedTable.constituent_changes_permitted -> bool
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.constituent_changes_permitted)
```

## `constituent_column`

The `constituent_column` method returns the name of the column containing the constituent tables - `__CONSTITUENT__`.

```python syntax
PartitionedTable.constituent_column -> str
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.constituent_column)
```

## `constituent_tables`

The `constituent_tables` property returns a `PartitionedTable`'s current constituent tables as a list of `Table` objects.

```python syntax
PartitionedTable.constituent_tables -> List[Table]
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.constituent_tables)
```

## `constituent_table_columns`

The `constituent_table_columns` method returns the column definitions of the constituent tables.

> [!NOTE]
> All constituent tables in a `PartitionedTable` have the same column definitions.

```python syntax
PartitionedTable.constituent_table_columns -> List[Column]
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.constituent_table_columns)
```

## `constituent_table_definition`

The `constituent_table_definition` property returns a table definition that is either shared by, or mutually compatible with, all constituent tables of a `PartitionedTable`.

> [!NOTE]
> Mutual compatibility means the returned definition's columns match each constituent's columns by name, data type, and component type — it does not guarantee identical storage or derivation details.

```python syntax
PartitionedTable.constituent_table_definition -> TableDefinition
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.constituent_table_definition)
```

## `is_refreshing`

The `is_refreshing` method returns whether the `PartitionedTable` is refreshing.

```python syntax
PartitionedTable.is_refreshing -> bool
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.is_refreshing)
```

## `key_columns`

The `key_columns` method returns the `PartitionedTable`'s key column names.

```python syntax
PartitionedTable.key_columns -> List[str]
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.key_columns)
```

## `table`

The `table` property returns the underlying `Table` of a `PartitionedTable`, whose rows contain the constituent tables and key values.

```python syntax
PartitionedTable.table -> Table
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.table)
```

## `unique_keys`

The `unique_keys` method returns whether the `PartitionedTable`'s keys are guaranteed to be unique.

```python syntax
PartitionedTable.unique_keys -> bool
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.unique_keys)
```

## `update_graph`

The `update_graph` method returns the `PartitionedTable`'s update graph.

```python syntax
PartitionedTable.update_graph -> UpdateGraph
```

Here is an example:

```python test-set=1 order=:log
print(partitioned_table.update_graph)
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable)
