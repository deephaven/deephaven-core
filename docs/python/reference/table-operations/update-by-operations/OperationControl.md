---
title: OperationControl
---

`OperationControl` is a Python class that defines the control parameters of some [`UpdateByOperations`](./updateBy.md#parameters) used in an [`update_by`](./updateBy.md) table operation. The [`UpdateByOperations`](./updateBy.md#parameters) can use `OperationControl` to handle erroneous data are:

- [`ema_tick`](./ema-tick.md)
- [`ema_time`](./ema-time.md)
- [`emmax_tick`](./emmax-tick.md)
- [`emmax_time`](./emmax-time.md)
- [`emmin_tick`](./emmin-tick.md)
- [`emmin_time`](./emmin-time.md)
- [`ems_tick`](./ems-tick.md)
- [`ems_time`](./ems-time.md)

## Syntax

```python syntax
OperationControl(
    on_null: BadDataBehavior = BadDataBehavior.SKIP,
    on_nan : BadDataBehavior = BadDataBehavior.SKIP,
    big_value_context: BadDataBehavior = MathContext.DECIMAL128
) -> OperationControl
```

## Parameters

<ParamTable>
<Param name="on_null" type="BadDataBehavior">

Defines how an [`UpdateByOperation`](./updateBy.md#parameters) handles null values it encounters. `SKIP` is the default. The following values are available:

- `POISON`: Allow bad data to poison the result. This is only valid for use with NaN.
- `RESET`: Reset the state for the bucket to `None` when invalid data is encountered.
- `SKIP`: Skip and do not process the invalid data without changing state.
- `THROW`: Throw an exception and abort processing when bad data is encountered.

</Param>
<Param name="on_nan" type="BadDataBehavior">

Defines how an [UpdateByOperation](./updateBy.md#parameters) handles NaN values it encounters. `SKIP` is the default. The following values are available:

- `POISON`: Allow bad data to poison the result. This is only valid for use with NaN.
- `RESET`: Reset the state for the bucket to `None` when invalid data is encountered.
- `SKIP`: Skip and do not process the invalid data without changing state.
- `THROW`: Throw an exception and abort processing when bad data is encountered.

</Param>
<Param name="big_value_context" type="MathContext">

Defines how an [UpdateByOperation](./updateBy.md#parameters) handles exceptionally large values it encounters. The default value is `DECIMAL128`. The following values are available:

- `DECIMAL128`: IEEE 754R `Decimal128` format. 34 digits and rounding is half-even.
- `DECIMAL32`: IEEE 754R `Decimal32` format. 7 digits and rounding is half-even.
- `DECIMAL64`: IEEE 754R `Decimal64` format. 16 digits and rounding is half-even.
- `UNLIMITED`: Unlimited precision arithmetic. Rounding is half-up.

</Param>
</ParamTable>

## Returns

An instance of an `OperationControl` class that can be used in an [`update_by`](./updateBy.md) operation.

## Examples

The following example does not set `op_control`, and thus uses the default settings of `BadDataBehavior.SKIP` and `MathContext.DECIMAL128`. Null values in the `source` table are skipped.

```python order=result,source
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(25).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = (i % 5 == 0) ? NULL_INT : randomInt(0, 100)",
    ]
)
result = source.update_by(ops=[ema_tick(decay_ticks=5, cols=["EmaX = X"])], by="Letter")
```

The following example sets `op_control` to use `BadDataBehavior.RESET` when null values occur, so that the EMA is reset when null values are encountered.

```python order=result,source
from deephaven.updateby import ema_tick, OperationControl, BadDataBehavior
from deephaven import empty_table

source = empty_table(25).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = (i % 5 == 0) ? NULL_INT : randomInt(0, 100)",
    ]
)

ema_w_opcontrol = [
    ema_tick(
        decay_ticks=5,
        cols=["EmaX = X"],
        op_control=OperationControl(on_null=BadDataBehavior.RESET),
    )
]

result = source.update_by(ops=ema_w_opcontrol, by="Letter")
```

The following example sets `op_control` to use `BadDataBehavior.RESET` when NaN values occur, so that the EMA is reset when NaN values are encountered.

```python order=result,source
from deephaven.updateby import ema_tick, OperationControl, BadDataBehavior
from deephaven import empty_table
import numpy as np


def create_w_nan(idx) -> np.double:
    if idx % 7 == 0:
        return np.nan
    else:
        return np.random.uniform(0, 10)


source = empty_table(30).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = create_w_nan(i)"]
)

ema_w_opcontrol = [
    ema_tick(
        decay_ticks=5,
        cols=["EmaX = X"],
        op_control=OperationControl(on_nan=BadDataBehavior.RESET),
    )
]

result = source.update_by(ops=ema_w_opcontrol, by="Letter")
```

The following example sets `op_control` to use `BadDataBehavior.POISON` when NaN values occur. This results in the EMA being poisoned with NaN values.

```python order=result,source
from deephaven.updateby import ema_tick, OperationControl, BadDataBehavior
from deephaven import empty_table
import numpy as np


def create_w_nan(idx) -> np.double:
    if idx % 7 == 0:
        return np.nan
    else:
        return np.random.uniform(0, 10)


source = empty_table(30).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = create_w_nan(i)"]
)

ema_w_opcontrol = [
    ema_tick(
        decay_ticks=5,
        cols=["EmaX = X"],
        op_control=OperationControl(on_nan=BadDataBehavior.POISON),
    )
]

result = source.update_by(ops=ema_w_opcontrol, by="Letter")
```

The following example sets `op_control` to `BadDataBehavior.THROW` when null values occur. The query throws an error when it encounters a null value in the first row.

```python should-fail
from deephaven.updateby import ema_tick, OperationControl, BadDataBehavior
from deephaven import empty_table

source = empty_table(25).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = (i % 5 == 0) ? NULL_INT : randomInt(0, 100)",
    ]
)

ema_w_opcontrol = [
    ema_tick(
        decay_ticks=5,
        cols=["EmaX = X"],
        op_control=OperationControl(on_null=BadDataBehavior.THROW),
    )
]

result = source.update_by(ops=ema_w_opcontrol, by="Letter")
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](/core/javadoc/io/deephaven/api/updateby/OperationControl.html)
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.BadDataBehavior)
