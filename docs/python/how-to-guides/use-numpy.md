---
title: Use NumPy in Deephaven queries
sidebar_label: NumPy
---

This guide will cover the intersection of [NumPy](https://numpy.org/) and Deephaven.

NumPy is an open-source Python module that includes a library of powerful numerical capabilities. These capabilities include support for multi-dimensional data structures, mathematical functions, and an API that enables calls to functions written in [C](https://numpy.org/doc/stable/reference/c-api/index.html) for faster performance. It is one of the most popular and widely used Python modules.

NumPy is one of Deephaven's Python dependencies. Deephaven's Python API comes stock with NumPy.

## Intersection with Deephaven

Deephaven intersects with NumPy in a few different ways. Each of the following subsections will cover one of those intersections.

### `deephaven.numpy`

[`deephaven.numpy`](/core/pydoc/code/deephaven.numpy.html) is a submodule of the Deephaven Python package that provides three functions:

- [`to_np_busdaycalendar`](../reference/numpy/to-np-busdaycalendar.md): Converts a Deephaven `BusinessCalendar` to a NumPy [`busdaycalendar`](https://numpy.org/doc/stable/reference/generated/numpy.busdaycalendar.html).
- [`to_numpy`](../reference/numpy/to-numpy.md): Converts a Deephaven table to a NumPy array.
- [`to_table`](../reference/numpy/to-table.md): Converts a NumPy array to a Deephaven table.

The following code block converts a Deephaven table to a NumPy array and back:

```python order=source,result
from deephaven import numpy as dhnp
from deephaven import empty_table

source = empty_table(10).update(
    ["X = randomDouble(0, 10)", "Y = randomDouble(100, 200)"]
)

np_source = dhnp.to_numpy(source)

print(np_source)

result = dhnp.to_table(np_source, cols=["X", "Y"])
```

> [!IMPORTANT]
>
> - NumPy arrays can only have one data type. If the table has multiple data types, the conversion will fail.
> - `to_numpy` copies an entire table into memory, so copy only the data you need.

The following code block imports one of Deephaven's example business calendars and converts it to a NumPy `busdaycalendar`. For more on calendars in Deephaven, see [business calendars](./business-calendar.md).

```python order=:log
from deephaven import calendar as dhcal
from deephaven import numpy as dhnp

usnyse_example = dhcal.calendar("USNYSE_EXAMPLE")
print(type(usnyse_example))

np_usnyse_example = dhnp.to_np_busdaycalendar(usnyse_example)
print(type(np_usnyse_example))
```

### NumPy in query strings

NumPy can be used in query strings like any other Python module. Deephaven recommends wrapping NumPy function calls in [Python functions](./python-functions.md) when called in query strings.

> [!IMPORTANT]
>
> - Python functions called in query strings should use [type hints](https://docs.python.org/3/library/typing.html) to ensure the resultant column(s) are of the correct data type.
> - Python functions are usually slower than equivalent [built-in methods](../reference/query-language/query-library/auto-imported/index.md).

The following code block calculates the cube root of an array column with NumPy:

```python test-set=1 order=source,result
from deephaven import empty_table
from typing import Sequence
import numpy as np


def np_cuberoot(arr) -> Sequence[float]:
    return np.cbrt(arr)


source = empty_table(4).update("X = pow(i + 1, 3)").group_by()

result = source.update("Y = np_cuberoot(X)").ungroup()
```

The following code block calculates the great circle distance between different coordinates on the earth with NumPy:

```python order=source,result
from deephaven.column import double_col
from deephaven import new_table
import numpy as np


def gcd_earth(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2

    return 2 * 6378.137 * np.arcsin(np.sqrt(a))


source = new_table(
    [
        double_col("Lat1", [15.5, 45.2, 75.3]),
        double_col("Lon1", [156.4, -121.1, 90.0]),
        double_col("Lat2", [30.9, 62.8, 79.3]),
        double_col("Lon2", [-60.1, 26.6, -18.7]),
    ]
)

result = source.update("DistanceKM = gcd_earth(Lat1, Lon1, Lat2, Lon2)")
```

### deephaven.learn

[`deephaven.learn`](./use-deephaven-learn.md) is a submodule of the Deephaven Python package that provides functions to facilitate data transfer between Deephaven tables and Python objects, namely NumPy arrays. It is typically used for AI/ML applications, but can be used for any data science workflow. It follows a gather-compute-scatter paradigm, in which data is gathered from tables into NumPy arrays, computations are performed, and the results are scattered back into a table.

The code below uses [`deephaven.learn`](./use-deephaven-learn.md) to calculate the sine of a set of values.

Three functions are defined:

- `compute_sin`, which applies computations to gathered data.
- `table_to_numpy`, which gathers rows and columns from a table into a NumPy array.
- `numpy_to_table`, which scatters the results of the computations back into a table.

At the end, the [learn function](./use-deephaven-learn.md#putting-it-all-together) calls these functions on the `source` table.

```python order=source,result
from deephaven import empty_table
from deephaven.learn import gather
from deephaven import learn
import numpy as np

source = empty_table(101).update(formulas=["X = (i / 101) * 2 * Math.PI"])


# Calculate the sine of a value or sequence of values
def compute_sin(x):
    return np.sin(x)


# Convert table data to a 2d NumPy array
def table_to_numpy(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.double)


# Return the model's answer so that it can be scattered back into a table
def numpy_to_table(data, idx):
    return data[idx]


result = learn.learn(
    table=source,
    model_func=compute_sin,
    inputs=[learn.Input("X", table_to_numpy)],
    outputs=[learn.Output("SinX", numpy_to_table, "double")],
    batch_size=101,
)
```

For a deeper dive on this subject, see the [`deephaven.learn` guide](./use-deephaven-learn.md).

## Related documentation

- [deephaven.learn](./use-deephaven-learn.md)
- [Pandas in Python queries](./use-pandas.md)
- [Install and use Python packages](./install-and-use-python-packages.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`to_pandas`](../reference/pandas/to-pandas.md)
- [special variables](../reference/query-language/variables/special-variables.md)
- [`update`](../reference/table-operations/select/update.md)
