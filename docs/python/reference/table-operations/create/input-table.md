---
title: input_table
---

The `input_table` function creates an input table, a table that users can change manually in the UI or programmatically with the input table's [methods](#methods).

An input table created by `input_table` is either append-only or keyed:

- An append-only input table adds new rows to the end of the table and does not support edits or deletions.
- A keyed input table has one or more key columns. It adds, modifies, and deletes rows by key, and an added row whose key matches an existing row replaces that row.

In the UI, added rows take effect when the user clicks **Commit**, and edits to existing rows of a keyed input table take effect immediately. See [How to use input tables](../../../how-to-guides/input-tables.md) for details, including [how to add data in the UI](../../../how-to-guides/input-tables.md#add-data-to-the-table).

## Syntax

```python syntax
input_table(
    col_defs: TableDefinitionLike | None = None,
    init_table: Table | None = None,
    key_cols: str | Sequence[str] | None = None,
    ) -> InputTable
```

## Parameters

<ParamTable>
<Param name="col_defs" type="TableDefinitionLike | None" optional>

A set of column definitions that define the column names and types in the input table. `col_defs` accepts any [`TableDefinitionLike`](./TableDefinitionLike.md), such as a `dict` that maps column names to [data types](../../python/deephaven-python-types.md). Provide exactly one of `col_defs` and `init_table`. `input_table` raises an error if a call passes neither, or passes a non-empty `col_defs` together with `init_table`.

</Param>
<Param name="init_table" type="Table | None" optional>

The table whose column definitions and rows are copied into the input table. If the input table is keyed and several rows of `init_table` share a key, only the last of those rows is kept. Provide exactly one of `col_defs` and `init_table`. `input_table` raises an error if a call passes neither, or passes a non-empty `col_defs` together with `init_table`.

</Param>
<Param name="key_cols" type="str | Sequence[str] | None" optional>

One or more key column names. Each key column must be one of the input table's columns. The default, `None`, creates an append-only input table. Providing key columns creates a keyed input table.

</Param>
</ParamTable>

## Returns

An [`InputTable`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable), a subclass of [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table).

## Methods

An input table supports the following methods:

- [`add`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.add): add data to an input table. The added data must fit the schema of the input table.
- [`add_async`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.add_async): add data to an input table asynchronously. The added data must fit the schema of the input table. It returns immediately and accepts optional `on_success` and `on_error` callbacks.
- [`delete`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.delete): delete the rows whose keys are in the given table, which must contain all of the input table's key columns and no other columns. Only keyed input tables support this method.
- [`delete_async`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.delete_async): asynchronously delete the rows whose keys are in the given table, which must contain all of the input table's key columns and no other columns. Only keyed input tables support this method. It returns immediately and accepts optional `on_success` and `on_error` callbacks.

## Properties

An input table has the following properties:

- [`key_names`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.key_names): the names of the key columns.
- [`value_names`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.value_names): the names of the value columns. By default, every column that is not a key column is a value column.

## Examples

The following example creates an input table from a pre-existing table by using the `init_table` argument. It does not specify any key columns, which means the `result` table is append-only.

```python test-set=1 order=source,result
from deephaven import empty_table, input_table

source = empty_table(10).update(["X = i"])

result = input_table(init_table=source)
```

The following example creates an input table from a `dict` of column definitions. It does not specify any key columns, which means the `my_input_table` table is append-only. The table is initially empty.

```python test-set=1 order=my_input_table
from deephaven import input_table
from deephaven import dtypes as dht

my_column_defs = {"StringCol": dht.string, "DoubleCol": dht.double}

my_input_table = input_table(col_defs=my_column_defs)
```

The following example creates an input table with `StringCol` as the key column, so the resulting input table is keyed rather than append-only.

```python test-set=1 order=my_input_table
from deephaven import input_table
from deephaven import dtypes as dht

my_column_defs = {"StringCol": dht.string, "DoubleCol": dht.double}

my_input_table = input_table(col_defs=my_column_defs, key_cols="StringCol")
```

The following example adds data to an input table with the [`add`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.add) method.

```python test-set=1 order=my_table,my_input_table
from deephaven import empty_table, input_table
from deephaven import dtypes as dht

my_col_defs = {"X": dht.int32, "Y": dht.double}
my_table = empty_table(5).update(["X = i", "Y = (double)(2 * i)"])
my_input_table = input_table(col_defs=my_col_defs)
my_input_table.add(my_table)
```

## Related documentation

- [`empty_table`](./emptyTable.md)
- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use input tables](../../../how-to-guides/input-tables.md)
- [Table types](../../../conceptual/table-types.md)
- [`TableDefinitionLike`](./TableDefinitionLike.md)
- [Deephaven Python dtypes](../../python/deephaven-python-types.md)
- [AppendOnlyArrayBackedInputTable Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html)
- [KeyedArrayBackedInputTable Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html)
- [InputTableUpdater Javadoc](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html)
- [InputTable Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable)
- [Table Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
