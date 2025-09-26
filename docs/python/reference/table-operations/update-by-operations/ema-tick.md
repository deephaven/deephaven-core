---
title: ema_tick
---

`ema_tick` creates a tick-based (row-based) EMA (exponential moving average) for an [`update_by`](./updateBy.md) table operation. The formula for the tick-based EMA of a column $X$ is:

$a = e^{\frac{-1}{\tau}}$

$\bar{x}_0 = x_0$

$\bar{x}_i = a*\bar{x}_{i-1} + (1-a)*x_i$

Where:

- $\tau$ is `decay_ticks`, an [input parameter](#parameters) to the method.
- $\bar{x}_i$ is the exponential moving average of $X$ at step $i$.
- $x_i$ is the current value.
- $i$ denotes the time step, ranging from $i=1$ to $i = n-1$, where $n$ is the number of elements in $X$.

## Syntax

```python syntax
ema_tick(
    decay_ticks: int,
    cols: list[str],
    op_control: OperationControl = None,
) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="decay_ticks" type="int">

The decay rate in ticks (rows).

</Param>
<Param name="cols" type="list[str]">

The columns to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). If `None`, EMA is calculated for all columns.

</Param>
<Param name="op_control" optional type="OperationControl">

Defines how special cases should behave. The default value is `None`, which uses default [`OperationControl`](./OperationControl.md) settings.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

### One column, no groups

The following example calculates the tick-based (row-based) EMA of the `X` column, renaming the resultant column to `EmaX`. The decay rate, `decay_ticks`, is set to 2. No grouping columns are specified, so the EMA is calculated over all rows.

```python order=result,source
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

result = source.update_by(ops=ema_tick(decay_ticks=2, cols=["EmaX = X"]))
```

### One EMA column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the EMA is calculated on a per-letter basis.

```python order=result,source
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

result = source.update_by(ops=ema_tick(decay_ticks=2, cols=["EmaX = X"]), by=["Letter"])
```

### Multiple EMA columns, multiple grouping columns

The following example builds on the previous by calculating the EMA of multiple columns in the same [`UpdateByOperation`](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```python order=result,source
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = i",
        "Y = randomInt(5, 10)",
    ]
)

result = source.update_by(
    ops=ema_tick(decay_ticks=2, cols=["EmaX = X", "EmaY = Y"]), by=["Letter", "Truth"]
)
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the EMA of multiple columns, each with its own [`UpdateByOperation`](./updateBy.md#parameters). This allows each EMA to have its own decay rate. The different decay rates are reflected in the renamed resultant column names.

```python order=result,source
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = i",
        "Y = randomInt(5, 10)",
    ]
)

ema_x = ema_tick(decay_ticks=2, cols=["EmaX2rows = X"])
ema_y = ema_tick(decay_ticks=4, cols=["EmaY5rows = Y"])

result = source.update_by(ops=[ema_x, ema_y], by=["Letter", "Truth"])
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [How to calculate an EMA](../../../how-to-guides/rolling-aggregations.md)
- [ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [`OperationControl`](./OperationControl.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Ema(double,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.ema_tick)
