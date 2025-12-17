---
title: to_table
---

The `to_table` method creates a new table from a NumPy [NDArray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html).

## Syntax

```python syntax
to_table(np_array: numpy.ndarray, cols: List[str]) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The source table to convert to a `numpy.ndarray`.

</Param>
<Param name="cols" type="List[str]">

Defines the names of the columns in the resulting table. This parameter _must_ have exactly as many column names as there are columns in the source NumPy [NDArray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html).

</Param>
</ParamTable>

## Returns

A Deephaven Table from the given NumPy [NDArray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html) and the column names.

## Examples

In the following example, we create a NumPy [NDArray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html) and convert it to a Deephaven table using `to_table`.

```python order=result
import numpy as np
from deephaven.numpy import to_table

source = np.array([[0, 1, 2], [3, 4, 5]])

result = to_table(source, cols=["col1", "col2", "col3"])
```

In many cases, NumPy arrays are too large to manually enumerate all of the column names. In such cases, you may prefer to set column names programmatically, like so:

```python order=result
import numpy as np
from deephaven.numpy import to_table

source = np.arange(10000).reshape(100, 100)

# programmatically name columns
result = to_table(source, cols=[f"X{i}" for i in range(source.shape[1])])
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.numpy.html#deephaven.numpy.to_table)
