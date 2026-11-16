---
title: Use NumPy in Deephaven queries
---

[NumPy](https://numpy.org/) is an open-source Python module that includes a library of powerful numerical capabilities. These capabilities include multi-dimensional arrays, mathematical functions implemented in compiled C for speed, and a [C API](https://numpy.org/doc/stable/reference/c-api/index.html) for writing extensions. It is one of the most popular and widely used Python modules.

NumPy is a dependency of the [Deephaven Python package](./deephaven-python-package.md), so installing Deephaven also installs NumPy.

This guide shows three ways to use NumPy with Deephaven tables:

- Convert tables and [business calendars](./business-calendar.md) to NumPy objects, and NumPy arrays back to tables, with [`deephaven.numpy`](/core/pydoc/code/deephaven.numpy.html). The array is a one-time copy that does not update when the table [ticks](../conceptual/table-update-model.md) (receives new data).
- Call NumPy from [query strings](./query-string-overview.md), one row or one grouped array at a time. The result column updates as the table ticks.
- Pass table data to a NumPy-based function in batches and write the results back to a table with [`deephaven.learn`](./use-deephaven-learn.md). When the source table is [add-only](../conceptual/table-types.md#specialization-2-add-only) or [blink](../conceptual/table-types.md#specialization-3-blink), the output table updates as it ticks.

## `deephaven.numpy`

[`deephaven.numpy`](/core/pydoc/code/deephaven.numpy.html) is a submodule of the Deephaven Python package that provides three functions:

- [`to_numpy`](../reference/numpy/to-numpy.md): Converts a Deephaven table to a NumPy array.
- [`to_table`](../reference/numpy/to-table.md): Converts a NumPy array to a Deephaven table.
- [`to_np_busdaycalendar`](../reference/numpy/to-np-busdaycalendar.md): Converts a Deephaven [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) to a NumPy [`busdaycalendar`](https://numpy.org/doc/stable/reference/generated/numpy.busdaycalendar.html).

### Convert between tables and arrays

The following code block converts a Deephaven table to a NumPy array and back:

```python order=:log,source,result
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
> - [`to_numpy`](../reference/numpy/to-numpy.md) produces a single 2D array, so all columns it converts must have the same data type. Otherwise, it raises an error. Use the `cols` argument to choose same-typed columns.
> - [`to_numpy`](../reference/numpy/to-numpy.md) copies the selected data into memory, every column by default. To copy less, pass `cols` and [filter](./use-filters.md) the table first.
> - [`to_numpy`](../reference/numpy/to-numpy.md) does not convert null values. See [Handle nulls](#handle-nulls) below.
> - If the table is [refreshing](../conceptual/table-update-model.md) (its data changes over time), [`to_numpy`](../reference/numpy/to-numpy.md) converts a snapshot of its current data. The array does not update when the table ticks.

#### Handle nulls

When [`to_numpy`](../reference/numpy/to-numpy.md) converts a table, it does not replace nulls with `numpy.nan`:

- Nulls in primitive columns appear as Deephaven's [null constants](../reference/query-language/types/nulls.md).
- Nulls in Boolean columns appear as `True`.
- Nulls in `Instant` columns appear as `NaT`.
- Nulls in `String` columns appear as the string `'None'`, and nulls in other object columns appear as `None`.

Use [`dh_null_to_nan`](../reference/numpy/dh-null-to-nan.md) from [`deephaven.jcompat`](/core/pydoc/code/deephaven.jcompat.html) to convert null constants to `numpy.nan`. For integer, Boolean, or character arrays, pass `type_promotion=True`, which returns a new `float64` array. Without it, [`dh_null_to_nan`](../reference/numpy/dh-null-to-nan.md) raises an error for those arrays.

The following code block converts an `int` column that contains nulls and replaces the nulls with `numpy.nan`:

```python order=:log,source
from deephaven import empty_table
from deephaven import numpy as dhnp
from deephaven.jcompat import dh_null_to_nan

source = empty_table(4).update("X = i % 2 == 0 ? NULL_INT : i")

np_source = dhnp.to_numpy(source)
print(np_source)

print(dh_null_to_nan(np_source, type_promotion=True))
```

### Convert a business calendar

The following code block gets one of Deephaven's example [business calendars](./business-calendar.md) by name with [`calendar`](../reference/time/calendar/calendar.md) and converts it to a NumPy [`busdaycalendar`](https://numpy.org/doc/stable/reference/generated/numpy.busdaycalendar.html) with [`to_np_busdaycalendar`](../reference/numpy/to-np-busdaycalendar.md). It then prints the converted calendar's weekmask and its first five holidays:

```python order=:log
from deephaven import calendar as dhcal
from deephaven import numpy as dhnp

usnyse_example = dhcal.calendar("USNYSE_EXAMPLE")
print(type(usnyse_example))

np_usnyse_example = dhnp.to_np_busdaycalendar(usnyse_example)
print(type(np_usnyse_example))
print(np_usnyse_example.weekmask)
print(np_usnyse_example.holidays[:5])
```

The weekmask marks Monday through Friday as business days, and the holidays come from the Deephaven calendar.

## NumPy in query strings

You can use NumPy in [query strings](./query-string-overview.md) like any other Python module. Two rules apply:

- Wrap NumPy calls in a [Python function](./python-functions.md) with [type hints](https://docs.python.org/3/library/typing.html), so the resulting column has the correct data type.
- Prefer a [built-in query language function](./built-in-functions.md) when one exists. [Python functions](./python-functions.md) are usually [slower](../conceptual/python-java-boundary.md) because each call crosses the Python-Java boundary.

The following code block converts sensor voltages to temperatures by passing a scalar column to a NumPy-based function, one row at a time. The function uses [`np.interp`](https://numpy.org/doc/stable/reference/generated/numpy.interp.html) to look up each voltage on a fixed calibration curve. The query language has no [built-in](./built-in-functions.md) equivalent, so this is a good job for NumPy:

```python order=source,result
from deephaven.column import double_col
from deephaven import new_table
import numpy as np

# Calibration curve: each sensor voltage and the temperature it corresponds to
CURVE_VOLTS = [0.0, 1.0, 2.0, 3.0]
CURVE_TEMPS_C = [-20.0, 5.0, 25.0, 60.0]


def volts_to_temp_c(volts: float) -> float:
    return float(np.interp(volts, CURVE_VOLTS, CURVE_TEMPS_C))


source = new_table([double_col("Volts", [0.5, 1.2, 2.5, 3.4])])

result = source.update("TempC = volts_to_temp_c(Volts)")
```

Voltages between two points on the curve get a linearly interpolated temperature. A voltage past the end of the curve, such as `3.4`, gets the temperature at that end (`60.0`).

NumPy functions also work on whole arrays, which is useful when each result depends on more than one row. The following code block:

1. Uses the row index [`i`](../reference/query-language/variables/special-variables.md) to build four values of `X`.
2. Uses [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) to collect them into one row, so `X` becomes a [vector column](./work-with-arrays.md#vector-columns).
3. Passes that array to [`np.gradient`](https://numpy.org/doc/stable/reference/generated/numpy.gradient.html) in a [query string](./query-string-overview.md). `np.gradient` has no [built-in](./built-in-functions.md) equivalent.
4. Uses [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md) to expand the result back to one value per row.

```python order=source,result
from deephaven import empty_table
import numpy as np
import numpy.typing as npt


def np_gradient(arr: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    return np.gradient(arr)


source = empty_table(4).update("X = pow(i + 1, 3)").group_by()

result = source.update("Y = np_gradient(X)").ungroup()
```

Each value of `Y` depends on its neighbors in `X`. The first and last rows use one-sided differences (8 − 1 = 7), and the middle rows use central differences ((27 − 1) / 2 = 13).

## `deephaven.learn`

[`deephaven.learn`](./use-deephaven-learn.md) is a submodule of the Deephaven Python package that moves data between Deephaven tables and Python objects, such as the NumPy arrays its [`gather`](/core/pydoc/code/deephaven.learn.gather.html) module produces. AI/ML applications use it most often, but it works for other batch computations too. It follows the [gather-compute-scatter paradigm](./use-deephaven-learn.md#the-gather-compute-scatter-paradigm):

- **Gather**: A gather function copies data from table columns into a Python object, such as a NumPy array.
- **Compute**: Your function computes on that object.
- **Scatter**: A scatter function writes the results back into a table.

[`to_numpy`](../reference/numpy/to-numpy.md) copies a one-time snapshot of a table, even a [refreshing](../conceptual/table-update-model.md) one. [`deephaven.learn`](./use-deephaven-learn.md) instead adds its results as columns of a new table, which updates as the source table ticks. That works only when the source table is one of these types:

- [Static](../conceptual/table-types.md#static-tables)
- [Add-only](../conceptual/table-types.md#specialization-2-add-only), which includes [append-only](../conceptual/table-types.md#specialization-1-append-only)
- [Blink](../conceptual/table-types.md#specialization-3-blink)

Because [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) identifies rows by their [row keys](../conceptual/table-update-model.md#describing-table-updates), it raises an error on other refreshing tables. For those, see [AI/ML workflows](./ml-ai-no-learn.md).

The code below uses [`deephaven.learn`](./use-deephaven-learn.md) to calculate the sine of a set of values.

The code defines three functions:

- `table_to_numpy` gathers rows and columns from a table into a NumPy array.
- `compute_sin` takes the sine of the gathered data.
- `numpy_to_table` scatters the results back into a table.

At the end, the code calls the [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function with these arguments:

- `table`: `source`, the input table.
- `model_func`: `compute_sin`, the compute function.
- `inputs`: a [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input) that pairs the input column `X` with the gather function `table_to_numpy`.
- `outputs`: a [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output) that pairs the output column `SinX` with the scatter function `numpy_to_table`.
- `batch_size`: `101`, so that [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) processes all 101 rows in one call.

```python order=source,result
from deephaven import empty_table
from deephaven.learn import gather
from deephaven import learn
import numpy as np

source = empty_table(101).update(formulas=["X = (i / 101) * 2 * Math.PI"])


# Convert table data to a 2D NumPy array
def table_to_numpy(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.double)


# Calculate the sine of a value or sequence of values
def compute_sin(x):
    return np.sin(x)


# Return the result computed by compute_sin so that it can be scattered back into a table
def numpy_to_table(data, idx):
    return data[idx, 0].item()


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

- [Incremental update model](../conceptual/table-update-model.md)
- [Table types](../conceptual/table-types.md)
- [Python-Java boundary](../conceptual/python-java-boundary.md)
- [`deephaven.learn`](./use-deephaven-learn.md)
- [AI/ML workflows](./ml-ai-no-learn.md)
- [Business calendars](./business-calendar.md)
- [Filters](./use-filters.md)
- [Query strings](./query-string-overview.md)
- [Arrays](./work-with-arrays.md)
- [Convert between Deephaven tables and pandas DataFrames](./use-pandas.md)
- [Install and use Python packages](./install-and-use-python-packages.md)
- [Python functions in query strings](./python-functions.md)
- [Built-in query language functions](./built-in-functions.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [`calendar`](../reference/time/calendar/calendar.md)
- [`dh_null_to_nan`](../reference/numpy/dh-null-to-nan.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [`to_np_busdaycalendar`](../reference/numpy/to-np-busdaycalendar.md)
- [`to_numpy`](../reference/numpy/to-numpy.md)
- [`to_table`](../reference/numpy/to-table.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
- [`update`](../reference/table-operations/select/update.md)
- [`deephaven.numpy` Pydoc](/core/pydoc/code/deephaven.numpy.html)
- [`deephaven.learn` Pydoc](/core/pydoc/code/deephaven.learn.html)
- [`deephaven.learn.gather` Pydoc](/core/pydoc/code/deephaven.learn.gather.html)
- [`deephaven.jcompat` Pydoc](/core/pydoc/code/deephaven.jcompat.html)
