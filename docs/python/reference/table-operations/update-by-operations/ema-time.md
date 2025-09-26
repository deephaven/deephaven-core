---
title: ema_time
---

`ema_time` creates a time-based EMA (exponential moving average) for an [`update_by`](./updateBy.md) table operation. The formula for the time-based EMA of a column $X$ is:

$a_i = e^{\frac{-dt_i}{\tau}}$

$\bar{x}_0 = x_0$

$\bar{x}_i = a_i*\bar{x}_{i-1} + (1-a_i)*x_i$

Where:

- $dt_i$ is the difference between time $t_i$ and $t_{i-1}$ in nanoseconds.
- $\tau$ is `decay_time` in nanoseconds, an [input parameter](#parameters) to the method.
- $\bar{x}_i$ is the exponential moving average of $X$ at time step $i$.
- $x_i$ is the current value.
- $i$ denotes the time step, ranging from $i=1$ to $i = n-1$, where $n$ is the number of elements in $X$.

## Syntax

```python syntax
ema_time(
    ts_col: str,
    decay_time: Union[str, int],
    cols: list[str],
    op_control: OperationControl = None,
) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="ts_col" type="str">

The name of the column containing timestamps.

</Param>
<Param name="decay_time" type="Union[str,int]">

The decay rate. This can be expressed as an integer in nanoseconds or a string [duration](../../query-language/types/durations.md), e.g., `"PT00:00:00.001"` or `"PTnHnMnS"`, where `H` is hour, `M` is minute, and `S` is second.

</Param>
<Param name="cols" type="list[str]">

The column(s) to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). If `None`, EMA is calculated for all applicable columns.

</Param>
<Param name="op_control" optional type="OperationControl">

Defines how special cases should behave. The default value is `None`, which uses default [`OperationControl`](./OperationControl.md) settings.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

### One column, no group

The following example calculates the time-based EMA of the `X` column, renaming the resultant column to `EmaX`. The decay rate, `decay_time`, is set to 5 seconds. No grouping columns are specified, so the EMA is calculated for all rows.

```python order=result,source
from deephaven.updateby import ema_time
from deephaven import empty_table

source = empty_table(60).update(
    [
        "Timestamp = '2023-05-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
    ]
)

result = source.update_by(
    ops=[ema_time(ts_col="Timestamp", decay_time="PT00:00:05", cols=["EmaX = X"])]
)
```

### One EMA column, one grouping column

The following example builds on the previous by specifying a single grouping column, `Letter`. Thus, the time-based EMA is calculated separately for each unique letter in `Letter`.

```python order=result,source
from deephaven.updateby import ema_time
from deephaven import empty_table

source = empty_table(60).update(
    [
        "Timestamp = '2023-05-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
    ]
)

result = source.update_by(
    ops=[ema_time(ts_col="Timestamp", decay_time="PT00:00:05", cols=["EmaX = X"])],
    by=["Letter"],
)
```

### Multiple EMA columns, multiple grouping columns

The following example builds on the previous by specifying multiple columns in a single EMA and renaming both appropriately. Additionally, groups are created from both the `Letter` and `Truth` columns, so groups are defined by unique combinations of letter and boolean, respectively.

```python order=result,source
from deephaven.updateby import ema_time
from deephaven import empty_table

source = empty_table(60).update(
    [
        "Timestamp = '2023-05-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0, 25)",
        "Y = i",
    ]
)

result = source.update_by(
    ops=[
        ema_time(
            ts_col="Timestamp", decay_time="PT00:00:05", cols=["EmaX = X", "EmaY = Y"]
        )
    ],
    by=["Letter", "Truth"],
)
```

### Multiple `UpdateByOperations`, multiple grouping columns

The following example builds on the previous by calculating the EMA of multiple columns, each with its own [`UpdateByOperation`](./updateBy.md#parameters). This allows each EMA to have its own decay rate. The different decay rates are reflected in the renamed resultant column names.

```python order=result,source
from deephaven.updateby import ema_time
from deephaven import empty_table

source = empty_table(60).update(
    [
        "Timestamp = '2023-05-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0, 25)",
        "Y = i",
    ]
)

ema_x = ema_time(ts_col="Timestamp", decay_time="PT5S", cols=["EmaX5sec = X"])
ema_y = ema_time(ts_col="Timestamp", decay_time="PT3S", cols=["EmaY3sec = Y"])

result = source.update_by(ops=[ema_x, ema_y], by=["Letter", "Truth"])
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [How to calculate an EMA](../../../how-to-guides/rolling-aggregations.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Ema(java.lang.String,java.time.Duration,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.ema_time)
