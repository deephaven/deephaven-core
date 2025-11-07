---
title: new_table
---

The `new_table` method creates an in-memory table from a list of columns. Each column must have an equal number of elements.

## Syntax

```python syntax
new_table(cols: list[InputColumn]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="InputColumn">

Columns are created using the following methods:

- [`bool_col`](./boolCol.md)
- [`byte_col`](./byteCol.md)
- [`char_col`](./charCol.md)
- [`datetime_col`](./dateTimeCol.md)
- [`double_col`](./doubleCol.md)
- [`float_col`](./floatCol.md)
- [`int_col`](./intCol.md)
- [`long_col`](./longCol.md)
- [`pyobj_col`](./pyobj_col.md)
- [`short_col`](./shortCol.md)
- [`string_col`](./stringCol.md)

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

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`empty_table`](./emptyTable.md)
- [`bool_col`](./boolCol.md)
- [`byte_col`](./byteCol.md)
- [`char_col`](./charCol.md)
- [`pyobj_col`](./pyobj_col.md)
- [`datetime_col`](./dateTimeCol.md)
- [`double_col`](./doubleCol.md)
- [`float_col`](./floatCol.md)
- [`long_col`](./longCol.md)
- [`short_col`](./shortCol.md)
- [`string_col`](./stringCol.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#emptyTable(long))
- [Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.new_table)
