---
title: to_numpy
---

The `to_numpy` method produces a NumPy array from a table.

> [!CAUTION]
> `to_numpy` copies the _entire_ source table into memory. For large tables, consider limiting table size before using `to_numpy`.

> [!IMPORTANT]
> All of the columns in the table must have the same type. If the table contains columns with different types, `to_numpy` will raise an exception.

## Syntax

```python syntax
to_numpy(table: Table, cols: List[str]) -> numpy.ndarray
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The source table to convert to a `numpy.ndarray`.

</Param>
<Param name="cols" type="List[str]">

The names of columns in the source table to be included in the result. The default is `None`, which includes all columns.

</Param>
</ParamTable>

## Returns

A `numpy.ndarray` from the given Java array and the Table column definition.

## Examples

In the following example, we create a basic Deephaven table and convert it to a `numpy.ndarray`, then print the result.

```python order=source
from deephaven import empty_table
from deephaven.numpy import to_numpy

source = empty_table(10).update(["X = i", "Y = i * 2"])

np_result = to_numpy(source)

print(np_result)
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.numpy.html#deephaven.numpy.to_numpy)
