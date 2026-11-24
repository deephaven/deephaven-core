---
title: new_table
---

The `new_table` function creates an in-memory table from a list of columns or a dictionary of column names and column data. Each column must have an equal number of elements.

## Syntax

```python syntax
new_table(cols: Sequence[InputColumn] | Mapping[str, Sequence]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="Sequence[InputColumn] | Mapping[str, Sequence]">

The columns of the new table, given as either a list of [`InputColumn`](./inputColumn.md) objects or a dictionary that maps column names to column data.

Each of the following functions returns an [`InputColumn`](./inputColumn.md):

- [`bool_col`](./boolCol.md)
- [`byte_col`](./byteCol.md)
- [`char_col`](./charCol.md)
- [`datetime_col`](./dateTimeCol.md)
- [`double_col`](./doubleCol.md)
- [`float_col`](./floatCol.md)
- [`int_col`](./intCol.md)
- [`jobj_col`](./jobj_col.md)
- [`long_col`](./longCol.md)
- [`pyobj_col`](./pyobj_col.md)
- [`short_col`](./shortCol.md)
- [`string_col`](./stringCol.md)

To create a column of a type that no helper function creates, such as an array type, construct an [`InputColumn`](./inputColumn.md) directly.

When `cols` is a dictionary, `new_table` first converts it to a pandas `DataFrame`, so pandas type inference determines the column types. The conversion also rewrites each dictionary key: each run of characters other than letters, digits, and underscores becomes a single underscore, and such characters at the start or end of a key are removed. For example, the key `"Price ($)"` becomes the column `Price`. Keys that are still not valid column names after this rewrite, such as `"123"` or `"in"`, raise an error.

</Param>
</ParamTable>

## Returns

An in-memory table.

## Examples

The following example creates a table with one column of three integer values.

```python
from deephaven import new_table
from deephaven.column import int_col

result = new_table([int_col("IntegerColumn", [1, 2, 3])])
```

The following example creates a table with a double and a string column.

```python
from deephaven import new_table
from deephaven.column import string_col, double_col

result = new_table(
    [
        double_col("Doubles", [3.1, 5.45, -1.0]),
        string_col("Strings", ["Creating", "New", "Tables"]),
    ]
)
```

The following example creates a table from a dictionary. In this example, pandas infers the column types: `Ints` is a `long` column and `Strs` is a `String` column.

```python
from deephaven import new_table

result = new_table({"Ints": [1, 2, 3], "Strs": ["a", "b", "c"]})
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`empty_table`](./emptyTable.md)
- [`InputColumn`](./inputColumn.md)
- [`bool_col`](./boolCol.md)
- [`byte_col`](./byteCol.md)
- [`char_col`](./charCol.md)
- [`datetime_col`](./dateTimeCol.md)
- [`double_col`](./doubleCol.md)
- [`float_col`](./floatCol.md)
- [`int_col`](./intCol.md)
- [`jobj_col`](./jobj_col.md)
- [`long_col`](./longCol.md)
- [`pyobj_col`](./pyobj_col.md)
- [`short_col`](./shortCol.md)
- [`string_col`](./stringCol.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/TableFactory.html#newTable(io.deephaven.qst.column.Column...))
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.new_table)
