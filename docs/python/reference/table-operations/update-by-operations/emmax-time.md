---
title: emmax_time
---

`emmax_time` creates a time-based EMMAX (exponential moving maximum) for an [`update_by`](./updateBy.md) table operation. The formula for the time-based EMMAX of a column $X$ is:

$a_i = e^{\frac{-dt_i}{\tau}}$

$\max_0(X) = x_0$

$\max_i(X) = \max(a*\max_{i-1}(X), \; x_i)$

Where:

- $dt_i$ is the difference between time $t_i$ and $t_{i-1}$ in nanoseconds.
- $\tau$ is `decay_time` in nanoseconds, an [input parameter](#parameters) to the method.
- $\max_i(X)$ is the exponential moving maximum of $X$ at time step $i$.
- $x_i$ is the current value.
- $i$ denotes the time step, ranging from $i=1$ to $i = n-1$, where $n$ is the number of elements in $X$.

## Syntax

```
emmax_time(
    ts_col: str,
    decay_time: Union[int, str],
    cols: list[str],
    op_control: OperationControl = None,
) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="ts_col" type="str">

The name of the column in the source table containing timestamps.

</Param>
<Param name="decay_time" type="Union[int, str]">

The decay rate. This can be expressed as an integer in nanoseconds or a string [duration](../../query-language/types/durations.md), e.g., `"PT00:00:00.001"` or `"PTnHnMnS"`, where `H` is hour, `M` is minute, and `S` is second.

</Param>
<Param name="cols" type="Union[str, list[str]]">

The string names of columns to be operated on. These can include expressions to rename the output, e.g., `"new_col = col"`. When this parameter is left empty, [`update_by`](./updateBy.md) will perform the operation on all applicable columns.

</Param>
<Param name="op_control" type="OperationControl">

An [`OperationControl`](./OperationControl.md) to define how special cases should behave. When `None`, default [`OperationControl`](./OperationControl.md) settings are used. See [`OperationControl`](./OperationControl.md) for more information.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using two `emmax_time` operations. Each uses a different `decay_time` value to demonstrate how it affects the output.

```python order=result,result2,source
from deephaven.updateby import emmax_time
from deephaven.time import to_j_instant
from deephaven import empty_table

base_time = to_j_instant("2023-05-01T00:00:00 ET")

source = empty_table(20).update(
    ["Timestamp = '2023-05-01T00:00:00 ET' + i * SECOND", "X = randomInt(0,25)"]
)

result = source.update_by(
    ops=[emmax_time(ts_col="Timestamp", decay_time="PT00:03:00", cols="EmMaxX = X")]
)

result2 = source.update_by(
    ops=[emmax_time(ts_col="Timestamp", decay_time="PT00:01:00", cols="EmMaxX = X")]
)
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [OperationControl](./OperationControl.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#EmMax(java.lang.String,long,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.emmax_time)
