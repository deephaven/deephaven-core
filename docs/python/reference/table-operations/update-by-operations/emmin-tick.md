---
title: emmin_tick
---

`emmin_tick` creates a tick-based (row-based) EMMIN (exponential moving minimum) for an [`update_by`](./updateBy.md) table operation. The formula for the tick-based EMMIN of a column $X$ is:

$a = e^{\frac{-1}{\tau}}$

$\min_0(X) = x_0$

$\min_i(X) = \min(a*\min_{i-1}(X), \; x_i)$

Where:

- $\tau$ is `decay_ticks`, an [input parameter](#parameters) to the method.
- $\min_i(X)$ is the exponential moving minimum of $X$ at step $i$.
- $x_i$ is the current value.
- $i$ denotes the time step, ranging from $i=1$ to $i = n-1$, where $n$ is the number of elements in $X$.

## Syntax

```
emmin_tick(
    decay_ticks: int,
    cols: list[str],
    op_control: OperationControl = None,
) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="decay_ticks" type="int">

The decay rate in ticks.

</Param>
<Param name="cols" type="list[str]">

The column(s) to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). When left empty, the rolling count is calculated for all applicable columns.

</Param>
<Param name="op_control" type="OperationControl">

Defines how special cases should behave. When `None`, the default [`OperationControl`](./OperationControl.md) settings will be used.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

### One column, no groups

The following example calculates the tick-based (row-based) EMMIN of the `X` column, renaming the resulting column to `EmMinX`. The decay rate, `decay_ticks` is set to 2. No grouping columns are specified, so the EMMIN is calculated over all rows.

```python order=result,source
from deephaven.updateby import emmin_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0,25)"]
)

result = source.update_by(ops=emmin_tick(decay_ticks=2, cols=["EmMinX = X"]))
```

### One EMMIN column, one grouping column

The following example builds on the previous by specifying `Letter` as the key column. Thus, the EMMIN is calculated on a per-letter basis.

```python order=result,source
from deephaven.updateby import emmin_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0,25)"]
)

result = source.update_by(
    ops=emmin_tick(decay_ticks=2, cols=["EmMaxX = X"]), by=["Letter"]
)
```

### Multiple EMMIN columns, multiple grouping columns

The following example builds on the previous by calculating the EMMIN of multiple columns in the same [`UpdateByOperation`](./updateBy.md#parameters). Also, the groups are defined by unique combinations of letter and boolean in the `Letter` and `Truth` columns, respectively.

```python order=result,source
from deephaven.updateby import emmin_tick
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0,25)",
        "Y = randomInt(0,25)",
    ]
)

result = source.update_by(
    ops=emmin_tick(decay_ticks=2, cols=["EmMinX = X", "EmMinY = Y"]),
    by=["Letter", "Truth"],
)
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the EMMIN of multiple columns, each with its own [`UpdateByOperation`](./updateBy.md#parameters). This allows each EMMIN to have its own decay rate. The different decay rates are reflected in the renamed resultant column names.

```python order=result,source
from deephaven.updateby import emmin_tick
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0,25)",
        "Y = randomInt(0,25)",
    ]
)

emmin_x = emmin_tick(decay_ticks=2, cols=["EmMinX2rows = X"])
emmin_y = emmin_tick(decay_ticks=4, cols=["EmMinY5rows = Y"])

result = source.update_by(ops=[emmin_x, emmin_y], by=["Letter", "Truth"])
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [OperationControl](./OperationControl.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#EmMin(java.lang.String,java.time.Duration,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.emmin_tick)
