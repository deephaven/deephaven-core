---
title: Simple table constructors
---

This cheat sheet shows the basic ways to create a table:

- From column data
- With a set number of rows
- As a [time table](../../how-to-guides/time-table.md), which adds rows on a timer
- As a [ring table](../../how-to-guides/ring-table.md), which keeps the latest rows of another table
- As an [input table](../../how-to-guides/input-tables.md), which accepts new rows

It ends with how to add [formula](../../how-to-guides/formulas.md) columns to a new table.

## `new_table`

[`new_table`](../table-operations/create/newTable.md) creates a table from supplied column data.

Create columns with the following functions:

- [`bool_col`](../table-operations/create/boolCol.md)
- [`byte_col`](../table-operations/create/byteCol.md)
- [`char_col`](../table-operations/create/charCol.md)
- [`datetime_col`](../table-operations/create/dateTimeCol.md)
- [`double_col`](../table-operations/create/doubleCol.md)
- [`float_col`](../table-operations/create/floatCol.md)
- [`int_col`](../table-operations/create/intCol.md)
- [`jobj_col`](../table-operations/create/jobj_col.md)
- [`long_col`](../table-operations/create/longCol.md)
- [`pyobj_col`](../table-operations/create/pyobj_col.md)
- [`short_col`](../table-operations/create/shortCol.md)
- [`string_col`](../table-operations/create/stringCol.md)

```python order=result
from deephaven import new_table
from deephaven.column import string_col, int_col

result = new_table(
    [
        int_col("Integers", [1, 2, 3]),
        string_col("Strings", ["These", "are", "Strings"]),
    ]
)
```

[`new_table`](../table-operations/create/newTable.md) also accepts a dictionary that maps column names to data. In that case, pandas infers the column types, so `Integers` becomes a `long` column rather than the `int` column that [`int_col`](../table-operations/create/intCol.md) creates.

```python order=result
from deephaven import new_table

result = new_table({"Integers": [1, 2, 3], "Strings": ["These", "are", "Strings"]})
```

### Array columns

The column functions listed above cannot create typed [array columns](../../how-to-guides/work-with-arrays.md), where each cell holds an array of a given element type. ([`jobj_col`](../table-operations/create/jobj_col.md) can hold arrays, but only in an [`Object`](../query-language/types/objects.md) column.) To create one, pass [`InputColumn`](../table-operations/create/inputColumn.md) an array column type, such as [`dtypes.int32_array`](../python/deephaven-python-types.md). Then build each cell with [`dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array). It takes the element type, such as `dtypes.int32`, and a sequence such as a NumPy array, and returns a Java array.

```python order=source
from deephaven.column import InputColumn
from deephaven import new_table
from deephaven import dtypes
import numpy as np

int_array_col = InputColumn(
    "IntArrayCol",
    dtypes.int32_array,
    input_data=[
        dtypes.array(dtypes.int32, np.array([1, 2, 3], dtype=np.int32)),
        dtypes.array(dtypes.int32, np.array([4, 5], dtype=np.int32)),
    ],
)

source = new_table([int_array_col])
```

## `empty_table`

[`empty_table`](../table-operations/create/emptyTable.md) creates a table with a given number of rows and no columns. Add columns to it with [`update`](../table-operations/select/update.md), as shown in [Add formula columns](#add-formula-columns).

```python order=result,result1
from deephaven import empty_table

result = empty_table(5)

result1 = result.update(formulas=["X = 5"])
```

## `time_table`

[`time_table`](../table-operations/create/timeTable.md) creates a table that adds a new row at a regular interval.

```python ticking-table order=null
from deephaven import time_table

result = time_table(period="PT2S")
```

<LoopedVideo src='../../assets/tutorials/timetable.mp4' />

## `ring_table`

[`ring_table`](../table-operations/create/ringTable.md) creates a table that keeps only the latest rows added to a parent table, up to a given capacity. Ring tables are most useful with [blink tables](../../conceptual/table-types.md#specialization-3-blink), which keep only the rows from the latest update cycle.

```python ticking-table order=null
from deephaven import time_table, ring_table

source = time_table(period="PT1S")
result = ring_table(parent=source, capacity=3)
```

![The `source` time table and the `result` ring table ticking side by side in the Deephaven console](../../assets/how-to/ring-table-1.gif)

## `input_table`

[`input_table`](../table-operations/create/input-table.md) creates an in-memory table that users can add rows to. It starts from either an existing table or a set of column definitions, such as a dictionary that maps each column name to a type from [`deephaven.dtypes`](../python/deephaven-python-types.md). Without `key_cols`, the result is append-only: new rows go to the end, and rows cannot be edited or deleted. Pass `key_cols` to create a [keyed input table](../../how-to-guides/input-tables.md#create-a-keyed-input-table), which adds, replaces, or deletes rows by key.

```python order=source,result,result2
from deephaven import empty_table, input_table
from deephaven import dtypes

# from an existing table
source = empty_table(10).update(formulas=["X = i"])

result = input_table(init_table=source)

# from column definitions
my_col_defs = {
    "Integers": dtypes.int32,
    "Doubles": dtypes.double,
    "Strings": dtypes.string,
}

result2 = input_table(col_defs=my_col_defs)
```

## Add formula columns

After creating a table, add columns calculated from its existing columns by passing a [formula](../../how-to-guides/formulas.md) to [`update`](../table-operations/select/update.md). A formula can combine columns, [built-in functions](../../how-to-guides/built-in-functions.md) such as [`sqrt`](../query-language/query-library/auto-imported/math.md), [variables](../../how-to-guides/query-scope.md), and [Python functions](../../how-to-guides/python-functions.md). The [type hints](../../how-to-guides/python-functions.md#type-hints) on `f` tell Deephaven that it returns a Python `int`, which Deephaven stores as a Java `long`. Without a return type hint, the function returns a [`PyObject`](../../how-to-guides/pyobjects.md), which the formula cannot add to the other terms.

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

var = 3


def f(a: int, b: int) -> int:
    return a + b


source = new_table([int_col("A", [1, 2, 3, 4, 5]), int_col("B", [10, 20, 30, 40, 50])])

result = source.update(formulas=["X = A + 3 * sqrt(B) + var + f(A, B)"])
```

## Related documentation

- [Create static tables](../../how-to-guides/new-and-empty-table.md)
- [Select and create columns](../../how-to-guides/use-select-view-update.md)
- [Formulas](../../how-to-guides/formulas.md)
- [Work with arrays](../../how-to-guides/work-with-arrays.md)
- [Built-in functions](../../how-to-guides/built-in-functions.md)
- [Query scope](../../how-to-guides/query-scope.md)
- [Python functions](../../how-to-guides/python-functions.md)
- [Handle PyObjects in tables](../../how-to-guides/pyobjects.md)
- [Convert between Deephaven tables and pandas DataFrames](../../how-to-guides/use-pandas.md)
- [Create a time table](../../how-to-guides/time-table.md)
- [Create a ring table](../../how-to-guides/ring-table.md)
- [Create and use input tables](../../how-to-guides/input-tables.md)
- [`deephaven.dtypes`](../python/deephaven-python-types.md)
- [`empty_table`](../table-operations/create/emptyTable.md)
- [`input_table`](../table-operations/create/input-table.md)
- [`InputColumn`](../table-operations/create/inputColumn.md)
- [`int_col`](../table-operations/create/intCol.md)
- [`new_table`](../table-operations/create/newTable.md)
- [`ring_table`](../table-operations/create/ringTable.md)
- [`sqrt`](../query-language/query-library/auto-imported/math.md)
- [`string_col`](../table-operations/create/stringCol.md)
- [`time_table`](../table-operations/create/timeTable.md)
- [`update`](../table-operations/select/update.md)
- [`dtypes.array` Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array)
