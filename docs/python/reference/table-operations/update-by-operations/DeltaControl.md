---
title: DeltaControl
---

`DeltaControl` is a Python class that defines ways to handle null values during a [`delta`](./delta.md) [`UpdateByOperation`](./updateBy.md#parameters). `delta` calculates the difference between the current row and the previous row.

## Syntax

```python syntax
DeltaControl(value: Enum = None) -> DeltaControl
```

## Parameters

<ParamTable>
<Param name="value" type="Enum">

Defines how special cases should behave. Enum options are:

- `DeltaControl.NULL_DOMINATES`: A valid value following a null value returns null.
- `DeltaControl.VALUE_DOMINATES`: A valid value following a null value returns the valid value.
- `DeltaControl.ZERO_DOMINATES`: A valid value following a null value returns zero.

</Param>
</ParamTable>

## Returns

An instance of a `DeltaControl` class that can be used in an [`update_by`](./updateBy.md) [`delta`](./delta.md) operation.

## Examples

The following example creates a table with three rows of int columns. Next, it creates three `DeltaControl` objects by calling its constructor. Finally, it uses [`update_by`](./updateBy.md) and invokes the [`delta`](./delta.md) [UpdateByOperation](./updateBy.md#parameters) to demonstrate how the three Enum values for `DeltaControl` affect the output.

```python order=result,source
from deephaven import new_table
from deephaven.updateby import delta, DeltaControl
from deephaven.column import int_col
from deephaven.constants import NULL_INT

null_dominates = DeltaControl(DeltaControl.NULL_DOMINATES)
value_dominates = DeltaControl(DeltaControl.VALUE_DOMINATES)
zero_dominates = DeltaControl(DeltaControl.ZERO_DOMINATES)

source = new_table(
    [
        int_col("X", [1, NULL_INT, 3, NULL_INT, 5, NULL_INT, NULL_INT, 8, 9]),
        int_col("Y", [0, 5, 3, NULL_INT, NULL_INT, 3, 0, NULL_INT, 9]),
        int_col("Z", [5, 6, 3, 4, 2, NULL_INT, 5, 0, 3]),
    ]
)

result = source.update_by(
    [
        delta(["X_null = X", "Y_null = Y", "Z_null = Z"], null_dominates),
        delta(["X_value = X", "Y_value = Y", "Z_value = Z"], value_dominates),
        delta(["X_zero = X", "Y_zero = Y", "Z_zero = Z"], zero_dominates),
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
