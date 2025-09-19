---
title: rolling_avg_time
---

`rolling_avg_time` creates a simple moving average in an [`update_by`](./updateBy.md) table operation using time as the windowing unit. Time can be specified as integer numbers of nanoseconds or strings. The moving average can be calculated using forward and/or backward windows.

$SMA = \frac{\sum_{1}^{n}x_{i}}{n}$

Where:

- $n$ is the number window size in ticks
- $x_{i}$ is the current value

For a time-based SMA, `n` is the number of observations in the window, determined by [`fwd_time`](#parameters) and [`rev_time`](#parameters).

## Syntax

```
rolling_avg_time(
    ts_col: str, cols: list[str],
    rev_time: Union[int, str],
    fwd_time: Union[int, str],
) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="ts_col" type="str">

The name of the column containing timestamps.

</Param>
<Param name="cols" type="list[str]">

The column(s) to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). If `None`, the rolling average is calculated for all applicable columns.

</Param>
<Param name="rev_time" type="Union[int,str]">

The look-behind window size. This can be expressed as an integer in nanoseconds or a string [duration](../../query-language/types/durations.md), e.g., `"PT00:00:00.001"` or `"PTnHnMnS"`, where `H` is hour, `M` is minute, and `S` is second.

</Param>
<Param name="fwd_time" type="Union[int,str]">

The look-forward window size. This can be expressed as an integer in nanoseconds or a string [duration](../../query-language/types/durations.md), e.g., `"PT00:00:00.001"` or `"PTnHnMnS"`, where `H` is hour, `M` is minute, and `S` is second.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an [`update_by`](./updateBy.md) table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using three `rolling_avg_time` operations. Each uses different `rev_time` and `fwd_time` values to show how they affect the output.

```python order=source,result
from deephaven.updateby import rolling_avg_time
from deephaven.time import to_j_instant
from deephaven import empty_table

base_time = to_j_instant("2023-01-01T00:00:00 ET")

source = empty_table(10).update(
    [
        "Timestamp = base_time + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
    ]
)

op_before = rolling_avg_time(
    ts_col="Timestamp",
    cols=["WindowBefore = X"],
    rev_time="PT00:00:03",
    fwd_time=int(-1e9),
)
op_after = rolling_avg_time(
    ts_col="Timestamp",
    cols=["WindowAfter = X"],
    rev_time="-PT00:00:01",
    fwd_time=int(3e9),
)
op_middle = rolling_avg_time(
    ts_col="Timestamp",
    cols=["WindowMiddle = X"],
    rev_time="PT00:00:01",
    fwd_time="PT00:00:01",
)

result = source.update_by(ops=[op_before, op_after, op_middle], by="Letter")
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingAvg(long,long,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_avg_time)
