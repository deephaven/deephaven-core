---
title: NaNs
---

NaN, or not-a-number, values indicate non-numeric results that come from computations on a dataset.

## Example

Deephaven supports constants for NaN values.

```python
from deephaven import new_table
from deephaven.column import float_col, double_col

NAN_VALUE = float("nan")

result = new_table(
    [float_col("Floats", [NAN_VALUE]), double_col("Doubles", [NAN_VALUE])]
)
```

NaNs typically come from computations on datasets. The following example shows how NaNs might end up in a table.

```python test-set=1
from deephaven import new_table
from deephaven.column import float_col, double_col
from deephaven.constants import NULL_DOUBLE, NULL_FLOAT

source = new_table(
    [float_col("NaNFloat", [-1.0]), double_col("NaNDouble", [-1.0])]
).update(
    formulas=[
        "NaNFloat = java.lang.Math.sqrt(NaNFloat)",
        "NaNDouble = java.lang.Math.sqrt(NaNDouble)",
    ]
)
```

NaN values can be detected using the `isNaN` filter.

```python test-set=1
result = source.update(
    formulas=[
        "NaNFloatNaN = isNaN(NaNFloat)",
        "NaNDoubleNaN = isNaN(NaNDouble)",
    ]
)
```

## Related Documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [How to handle null, infinity, and not-a-number values](../../../how-to-guides/null-inf-nan.md)
- [Nulls](./nulls.md)
