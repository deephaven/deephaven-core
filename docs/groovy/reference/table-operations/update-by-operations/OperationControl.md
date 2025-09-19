---
title: OperationControl
---

`OperationControl` is a Java class that defines the control parameters of some [`UpdateByOperations`](./updateBy.md#parameters) used in an [updateBy](./updateBy.md) table operation. The [`UpdateByOperations`](./updateBy.md#parameters) that can use `OperationControl` to handle erroneous data are:

- [Ema](./ema.md)
- [EmMax](./em-max.md)
- [EmMin](./em-min.md)
- [EmStd](./em-std.md)
- [Ems](./ems.md)

## Syntax

```
opControl = OperationControl.builder().onNullValue(BadDataBehavior.SKIP).onNanValue(BadDataBehavior.SKIP).bigValueContext(MathContext.DECIMAL128).build()
```

## Construction

`OperationControl` is constructed using a builder pattern. The builder can set parameters on how to handle [nulls](../../query-language/types/nulls.md), [NaNs](../../query-language/types/NaNs.md), and large values, respectively, with the following methods:

- `onNullValue`(https://deephaven.io/core/javadoc/io/deephaven/api/updateby/OperationControl.html#onNullValue())
- `onNanValue`(https://deephaven.io/core/javadoc/io/deephaven/api/updateby/OperationControl.html#onNanValue())
- `bigValueContext`(https://deephaven.io/core/javadoc/io/deephaven/api/updateby/OperationControl.html#bigValueContext())

The first two take an enum value from [`BadDataBehavior`](/core/javadoc/io/deephaven/api/updateby/BadDataBehavior.html), whereas the third takes a value from [`MathContext`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/MathContext.html).

`BadDataBehavior` has four enum values:

- `POISON`: Allow bad data to poison the result.
- `RESET`: Reset the state of the bucket to `null` when any bad data is encountered.
- `SKIP`: Skip and do not process invalid data without changing state.
- `THROW`: Throw an exception and abort processing when bad data is encountered.

The default value is `SKIP`.

`MathContext` has four values:

- `DECIMAL128`: Precision matching IEEE 754R Decimal128 format. Has 34 digits and rounding mode of `HALF_EVEN`.
- `DECIMAL32`: Precision matching the IEEE 754R Decimal32 format. Has 7 digits and rounding mode of `HALF_EVEN`.
- `DECIMAL64`: Precision matching the IEEE 754R Decimal64 format. Has 16 digits and a rounding mode of `HALF_EVEN`.
- `UNLIMITED`: Unlimited precision arithmetic.

## Returns

An instance of an `OperationControl` class that can be used in an [updateBy](./updateBy.md) operation.

## Examples

The following example does not set `control`. It uses the default settings of `BadDataBehavior.SKIP` and `MathContext.DECIMAL128`. Null values in the `source` table are skipped.

```groovy order=source,result
source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 5 == 0) ? NULL_INT : randomInt(0, 100)")

result = source.updateBy(Ema(5, "EmaX = X"), "Letter")
```

The following example sets `control` to use `BadDataBehavior.RESET` when null values occur, so that the EMA is reset when null values are encountered.

```groovy order=source,result
ctrl = OperationControl.builder().onNullValue(BadDataBehavior.RESET).build()

source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 5 == 0) ? NULL_INT : randomInt(0, 100)")

result = source.updateBy(Ema(ctrl, 5, "EmaX = X"), "Letter")
```

The following example sets `op_control` to use `BadDataBehavior.RESET` when NaN values occur, so that the EMA is reset when NaN values are encountered.

```groovy order=source,result
ctrl = OperationControl.builder().onNanValue(BadDataBehavior.RESET).build()

source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 5 == 0) ? (0/0) : randomInt(0, 100)")

result = source.updateBy(Ema(ctrl, 5, "EmaX = X"), "Letter")
```

The following example sets `op_control` to use `BadDataBehavior.POISON` when NaN values occur. This results in the EMA being poisoned with NaN values.

```groovy order=source,result
ctrl = OperationControl.builder().onNanValue(BadDataBehavior.POISON).build()

source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 5 == 0) ? (0/0) : randomInt(0, 100)")

result = source.updateBy(Ema(ctrl, 5, "EmaX = X"), "Letter")
```

The following example sets `op_control` to `BadDataBehavior.THROW` when null values occur. The query throws an error when it encounters a null value in the first row.

```groovy should-fail
ctrl = OperationControl.builder().onNullValue(BadDataBehavior.THROW).build()

source = emptyTable(25).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 5 == 0) ? NULL_INT : randomInt(0, 100)")

result = source.updateBy(Ema(ctrl, 5, "EmaX = X"), "Letter")
```

## Related documentation

- [How to use `updateBy`](../../../how-to-guides/use-update-by.md)
- [How to create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to handle handle null, infinity, and not-a-number values](../../../how-to-guides/null-inf-nan.md)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`updateBy`](./updateBy.md)
- [Javadoc](/core/javadoc/io/deephaven/api/updateby/OperationControl.html)
