---
title: pyobj_col
---

The `pyobj_col` function creates a column of type [`org.jpy.PyObject`](../../../how-to-guides/pyobjects.md#what-is-a-pyobject) that holds arbitrary Python objects, such as dictionaries, lists, or class instances. The column keeps the values as Python objects and does not convert them to Java types. To create a column of Java objects instead, use [`jobj_col`](./jobj_col.md).

> [!NOTE]
> Pass the column that `pyobj_col` returns to [`new_table`](./newTable.md) to create a table.

> [!CAUTION]
> Creating and operating on `PyObject` columns is slower than on columns of Java types, because almost every operation on their values crosses the [Python-Java boundary](../../../conceptual/python-java-boundary.md). `PyObject` columns cannot be sorted, and many [built-in query language functions](../../../how-to-guides/built-in-functions.md) do not accept `PyObject` values. When all values share a type that has a typed column function, use that function instead, such as [`int_col`](./intCol.md) for integers or [`string_col`](./stringCol.md) for strings. See [Handle PyObjects in tables](../../../how-to-guides/pyobjects.md).

## Syntax

```python syntax
pyobj_col(name: str, data: Sequence) -> InputColumn
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the new column.

</Param>
<Param name="data" type="Sequence">

The column values, as a sequence such as a list or tuple. The values can be Python objects of any type.

</Param>
</ParamTable>

## Returns

An [`InputColumn`](./inputColumn.md).

## Example

The following example uses [`new_table`](./newTable.md) to create a table with a single column named `Values` that holds a dictionary, a list, and a string. The [`meta_table`](../metadata/meta_table.md) shows that the column's data type is `org.jpy.PyObject`.

```python order=result,result_meta
from deephaven import new_table
from deephaven.column import pyobj_col

result = new_table([pyobj_col("Values", [{"a": 1}, [1, 2, 3], "text"])])
result_meta = result.meta_table
```

## Related documentation

- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [Handle PyObjects in tables](../../../how-to-guides/pyobjects.md)
- [`InputColumn`](./inputColumn.md)
- [`int_col`](./intCol.md)
- [`jobj_col`](./jobj_col.md)
- [`meta_table`](../metadata/meta_table.md)
- [`new_table`](./newTable.md)
- [Python-Java boundary](../../../conceptual/python-java-boundary.md)
- [`string_col`](./stringCol.md)
- [Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.pyobj_col)
