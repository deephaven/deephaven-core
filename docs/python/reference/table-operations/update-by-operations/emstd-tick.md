---
title: emstd_tick
---

`emstd_tick` creates a tick-based (row-based) EMSTD (exponential moving standard deviation) for an [`update_by`](./updateBy.md) table operation. The formula for the tick-based EMSTD of a column $X$ is:

$a = e^{\frac{-1}{\tau}}$

$s^2_0 = 0$

$s^2_i = a*(s^2_{i-1} + (1-a)*(x_i - \bar{x}_{i-1})^2)$

$s_i = \sqrt{s^2_i}$

Where:

- $\tau$ is `decay_ticks`, an [input parameter](#parameters) to the method.
- $\bar{x}_i$ is the [exponential moving average](./ema-tick.md) of $X$ at step $i$
- $s_i$ is the exponential moving standard deviation of $X$ at step $i$.
- $x_i$ is the current value.
- $i$ denotes the time step, ranging from $i=1$ to $i = n-1$, where $n$ is the number of elements in $X$.

> [!NOTE]
> In the above formula, $s^2_0 = 0$ yields the correct results for subsequent calculations. However, sample variance for fewer than two data points is undefined, so the first element of an EMSTD calculation will always be `NaN`.

## Syntax

```python syntax
emstd_tick(
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

The columns to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). If `None`, EMSTD is calculated for all columns.

</Param>
<Param name="op_control" optional type="OperationControl">

Defines how special cases should behave. The default value is `None`, which uses default [`OperationControl`](./OperationControl.md) settings.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

### One column, no groups

The following example calculates the tick-based (row-based) EMSTD of the `X` column, renaming the resultant column to `EmStdX`. The decay rate, `decay_ticks`, is set to 2. No grouping columns are specified, so the EMSTD is calculated over all rows.

```python order=result,source
from deephaven.updateby import emstd_tick
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

result = source.update_by(ops=emstd_tick(decay_ticks=2, cols=["EmStdX = X"]))
```

### One EMSTD column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the EMSTD is calculated on a per-letter basis.

```python order=result,source
from deephaven.updateby import emstd_tick
from deephaven import empty_table

source = empty_table(10).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = i"])

result = source.update_by(
    ops=emstd_tick(decay_ticks=2, cols=["EmStdX = X"]), by=["Letter"]
)
```

### Multiple EMSTD columns, multiple grouping columns

The following example builds on the previous by calculating the EMSTD of multiple columns in the same [`UpdateByOperation`](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```python order=result,source
from deephaven.updateby import emstd_tick
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
    ops=emstd_tick(decay_ticks=2, cols=["EmStdX = X", "EmStdY = Y"]),
    by=["Letter", "Truth"],
)
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the EMSTD of multiple columns, each with its own [`UpdateByOperation`](./updateBy.md#parameters). This allows each EMSTD to have its own decay rate. The different decay rates are reflected in the renamed resultant column names.

```python order=result,source
from deephaven.updateby import emstd_tick
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = i",
        "Y = randomInt(5, 10)",
    ]
)

emstd_x = emstd_tick(decay_ticks=2, cols=["EmStdX2rows = X"])
emstd_y = emstd_tick(decay_ticks=4, cols=["EmStdY5rows = Y"])

result = source.update_by(ops=[emstd_x, emstd_y], by=["Letter", "Truth"])
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Tick-based exponential moving average in UpdateBy](./ema-tick.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [`OperationControl`](./OperationControl.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Ema(double,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.emstd_tick)
