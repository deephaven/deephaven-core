---
title: input_table
---

The `input_table` method creates an input table, which allows users to manually add data via the UI or programmatically via queries.

## Syntax

```python syntax
input_table = input_table(
    init_table: Table,
    col_defs: dict[str, DType] = None,
    key_cols: list[str],
    ) -> InputTable
```

## Parameters

<ParamTable>
<Param name="init_table" type="Table">

The table from which the input table will be created. If used, column definitions must be `None` (the default value).

</Param>
<Param name="col_defs" type="dict[str, DType]">

A set of column definitions that define the column names and types in the input table. If used, an initial table must be `None` (the default value).

</Param>
<Param name="key_cols" type="list[str]">

One or more key column names. The default value is `None`, which results in an append-only input table. If not `None`, it becomes a keyed input table, which allows for the creation and modification of pre-existing cells.

</Param>
</ParamTable>

## Returns

An InputTable, a subclass of a Deephaven table in which data can be manually entered by clicking on cells and typing values.

## Methods

An input table supports the following methods:

- [`add`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.add): add data to an input table. The added data must fit the schema of the input table.
- [`add_async`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.add_async): add data to an input table asynchronously. The added data must fit the schema of the input table.
- [`delete`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.delete): delete data from an input table. This method can only be used on a keyed input table.
- [`delete_async`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.delete_async): delete data from an input table asynchronously. This method can only be used on a keyed input table.

## Examples

The following example creates an input table from a pre-existing table by using the `init_table` argument. It does not specify any key columns, which means the `result` table is append-only.

```python test-set=1 order=source,result
from deephaven import empty_table, input_table

source = empty_table(10).update(["X = i"])

result = input_table(init_table=source)
```

The following example creates an input table from a `dict` of column definitions. It does not specify any key columns, which means the `result` table is append-only.

```python test-set=1 order=null
from deephaven import input_table
from deephaven import dtypes as dht

my_column_defs = {"StringCol": dht.string, "DoubleCol": dht.double}

my_input_table = input_table(col_defs=my_column_defs)
```

The third example creates an input table and specifies a key column, so the resulting input table is keyed rather than append-only.

```python test-set=1 order=null
from deephaven import input_table
from deephaven import dtypes as dht

my_column_defs = {"StringCol": dht.string, "DoubleCol": dht.double}

my_input_table = input_table(col_defs=my_column_defs, key_cols="StringCol")
```

Data can be added to an input table programmatically using the `add` method.

```python test-set=1 order=my_table,my_input_table
from deephaven import empty_table, input_table
from deephaven import dtypes as dht

my_col_defs = {"X": dht.int32, "Y": dht.double}
my_table = empty_table(5).update(["X = i", "Y = (double)(2 * i)"])
my_input_table = input_table(col_defs=my_col_defs)
my_input_table.add(my_table)
```

Data can be added to an input table manually via the UI.

![A user manually adds integers and strings to cells in an input table](../../../assets/how-to/input-tables/input-table-manual.gif)

![The user selects the **Commit** button to save changes to the input table](../../../assets/how-to/input-tables/input-table-commit.gif)

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use input tables](../../../how-to-guides/input-tables.md)
- [Deephaven Python dtypes](../../../reference/python/deephaven-python-types.md)
- [Javadoc](/core/javadoc/io/deephaven/qst/table/InputTable.html)
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable)
