---
title: dh_null_to_nan
---

The `dh_null_to_nan` method converts Deephaven primitive null values in the given numpy array to `numpy.nan`. No conversion is performed on non-primitive values.

> [!NOTE]
> The input numpy array is modified in place if it is a float or double type. If that is not the desired behavior, pass a copy of the new array instead. For input arrays of other types, a new array is always returned and the input numpy array is unmodified.

## Syntax

```python syntax
dh_null_to_nan(
  np_array: numpy.ndarray,
  type_promotion: bool = False,
) -> numpy.ndarray
```

## Parameters

<ParamTable>
<Param name="np_array" type="numpy.ndarray">

The numpy array to convert.

</Param>
<Param name="type_promotion" type="bool" optional>

When True, integer, boolean, or character arrays are converted to new np.float64 arrays and Deephaven null values in them are converted to `np.nan`. Numpy arrays of float or double types are not affected by this flag, and Deephaven nulls will always be converted to `np.nan` in place.

When False, integer, boolean, or character arrays will cause an exception to be raised. Default is False.

</Param>
</ParamTable>

## Returns

A `numpy.ndarray`.

## Examples

In the example below, we create a simple Deephaven table with null values. We then convert the table to a numpy array and use `dh_null_to_nan` to convert the Deephaven null values to `numpy.nan`.

```python order=:log
from deephaven import new_table
from deephaven.column import int_col
from deephaven.constants import NULL_INT

source = new_table(
    [int_col("Number1", [1, NULL_INT, 3]), int_col("Number2", [1, 2, NULL_INT])]
)

from deephaven.numpy import to_numpy
from deephaven.jcompat import dh_null_to_nan

np = to_numpy(source)

np_nan = dh_null_to_nan(np, True)

print(np_nan)
```

## Related documentation
