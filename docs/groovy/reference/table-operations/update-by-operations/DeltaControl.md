---
title: DeltaControl
---

`DeltaControl` is a class that defines ways to handle null values during Delta [`updateBy`](./updateBy.md) operations, where [`Delta`](./delta.md) operations return the difference between the current row and the previous row.

## Syntax

```
DeltaControl(value)
```

## Parameters

<ParamTable>
<Param name="value" type="Enum">

Defines how special cases should behave. When `None`, the default value is `VALUE_DOMINATES`.
Enum options are:

- `DeltaControl.NULL_DOMINATES`: A valid value following a null value returns null.
- `DeltaControl.VALUE_DOMINATES`: A valid value following a null value returns the valid value.
- `DeltaControl.ZERO_DOMINATES`: A valid value following a null value returns zero.

</Param>
</ParamTable>

## Returns

An instance of a `DeltaControl` class that can be used in an [`updateBy`](./updateBy.md) [`Delta`](./delta.md) operation.

## Examples

The following example creates a [`newTable`](../create/newTable.md) with three rows of `int` columns. Next, it creates three `DeltaControl` objects. Finally, it uses [`updateBy`](./updateBy.md) and invokes the [`Delta`](./delta.md) [UpdateByOperation](./updateBy.md#parameters) to demonstrate how the three Enum values for `DeltaControl` affect the output.

```groovy order=source,result
null_dominates = DeltaControl.NULL_DOMINATES
value_dominates = DeltaControl.VALUE_DOMINATES
zero_dominates = DeltaControl.ZERO_DOMINATES

source = newTable(
    intCol("X", 1, NULL_INT, 3, NULL_INT, 5, NULL_INT, NULL_INT, 8, 9),
    intCol("Y", 0, 5, 3, NULL_INT, NULL_INT, 3, 0, NULL_INT, 9),
    intCol("Z", 5, 6, 3, 4, 2, NULL_INT, 5, 0, 3)
)

result = source.updateBy([
    Delta(null_dominates, "X_null = X", "Y_null = Y", "Z_null = Z"),
    Delta(value_dominates, "X_value = X", "Y_value = Y", "Z_value = Z"),
    Delta(zero_dominates, "X_zero = X", "Y_zero = Y", "Z_zero = Z"),
])
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/rolling-aggregations.md)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`newTable`](../create/newTable.md)
- [`Delta`](./delta.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](/core/javadoc/io/deephaven/api/updateby/OperationControl.html)
