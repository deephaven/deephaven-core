---
title: DeltaControl
---

`DeltaControl` is a Python enum that defines ways to handle null values during a [`delta`](./delta.md) [`UpdateByOperation`](./updateBy.md#parameters). `delta` calculates the difference between the current row and the previous row.

## Syntax

```python syntax
DeltaControl.NULL_DOMINATES
DeltaControl.VALUE_DOMINATES
DeltaControl.ZERO_DOMINATES
```

## Values

- `DeltaControl.NULL_DOMINATES`: A valid value following a null value returns null. This is the default for [`delta`](./delta.md).
- `DeltaControl.VALUE_DOMINATES`: A valid value following a null value returns the valid value.
- `DeltaControl.ZERO_DOMINATES`: A valid value following a null value returns zero.

## Examples

The following example creates a table with three int columns. Then it uses [`update_by`](./updateBy.md) and invokes the [`delta`](./delta.md) [UpdateByOperation](./updateBy.md#parameters) to demonstrate how the three enum values for `DeltaControl` affect the output.

```python order=result,source
from deephaven import new_table
from deephaven.updateby import delta, DeltaControl
from deephaven.column import int_col
from deephaven.constants import NULL_INT

source = new_table(
    [
        int_col("X", [1, NULL_INT, 3, NULL_INT, 5, NULL_INT, NULL_INT, 8, 9]),
        int_col("Y", [0, 5, 3, NULL_INT, NULL_INT, 3, 0, NULL_INT, 9]),
        int_col("Z", [5, 6, 3, 4, 2, NULL_INT, 5, 0, 3]),
    ]
)

result = source.update_by(
    [
        delta(["X_null = X", "Y_null = Y", "Z_null = Z"], DeltaControl.NULL_DOMINATES),
        delta(
            ["X_value = X", "Y_value = Y", "Z_value = Z"], DeltaControl.VALUE_DOMINATES
        ),
        delta(["X_zero = X", "Y_zero = Y", "Z_zero = Z"], DeltaControl.ZERO_DOMINATES),
    ]
)
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- ['new_table`](../create/newTable.md)
- [`delta`](./delta.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](/core/javadoc/io/deephaven/api/updateby/OperationControl.html)
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.BadDataBehavior)
