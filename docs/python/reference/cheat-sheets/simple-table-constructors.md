---
title: Simple table constructors
---

## `new_table`

- [`new_table`](../table-operations/create/newTable.md)

Columns are created using the following methods:

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

```python syntax
from deephaven import new_table
from deephaven.column import string_col, int_col

result = new_table(
    [
        int_col("Integers", [1, 2, 3]),
        string_col("Strings", ["These", "are", "Strings"]),
    ]
)
```

### Formula columns

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

var = 3


def f(a, b):
    return a + b


source = new_table([int_col("A", [1, 2, 3, 4, 5]), int_col("B", [10, 20, 30, 40, 50])])

result = source.update(formulas=["X = A + 3 * sqrt(B) + var + (int)f(A, B)"])
```

### Array columns

```python order=source
from deephaven.column import InputColumn
from deephaven import new_table
from deephaven import dtypes
import numpy as np

int_array = dtypes.array(dtypes.int32, np.array([1, 2, 3], dtype=np.int32))
int_array_col = InputColumn("IntArrayCol", dtypes.int32_array, input_data=[int_array])

source = new_table([int_array_col])
```

### String columns

```python test-set=1 order=total,scores,average
from deephaven import new_table
from deephaven.column import string_col, int_col

scores = new_table(
    [
        string_col("Name", ["James", "Lauren", "Zoey"]),
        int_col("Math", [95, 72, 100]),
        int_col("Science", [100, 78, 98]),
        int_col("Art", [90, 92, 96]),
    ]
)

total = scores.update(formulas=["Total = Math + Science + Art"])
average = scores.update(formulas=["Average = (Math + Science + Art) / 3 "])
```

## `empty_table`

- [`empty_table`](../table-operations/create/emptyTable.md)

```python order=result,result1
from deephaven import empty_table

result = empty_table(5)

# Empty tables are often followed with a formula
result1 = result.update(formulas=["X = 5"])
```

## `time_table`

- [`time_table`](../table-operations/create/timeTable.md)

```python order=null
from deephaven import time_table

result = time_table(period="PT2S")
```

<LoopedVideo src='../../assets/tutorials/timetable.mp4' />

## `ring_table`

- [`ring_table`](../table-operations/create/ringTable.md)

```python order=null
from deephaven import time_table, ring_table

source = time_table("PT00:00:01")
result = ring_table(parent=source, capacity=3)
```

![The `source` time table and the `result` ring table ticking side-by-side in the Deephaven console](../../assets/how-to/ring-table-1.gif)

## `input_table`

- [`input_table`](../table-operations/create/input-table.md)

```python order=source,result,result2
from deephaven import empty_table, input_table
from deephaven import dtypes as dht

# from an existing table
source = empty_table(10).update(["X = i"])

result = input_table(init_table=source)

# from scratch
my_col_defs = {"Integers": dht.int32, "Doubles": dht.double, "Strings": dht.string}

result2 = input_table(col_defs=my_col_defs)
```

## Related documentation

- [Create static tables](../../how-to-guides/new-and-empty-table.md)
- [Select and create columns](../../how-to-guides/use-select-view-update.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`input_table`](../table-operations/create/input-table.md)
- [`new_table`](../../reference/table-operations/create/newTable.md)
- [`ring_table`](../table-operations/create/ringTable.md)
- [`input_table`](../table-operations/create/input-table.md)
