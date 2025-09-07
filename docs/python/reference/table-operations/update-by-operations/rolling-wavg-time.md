---
title: rolling_wavg_time
---

`rolling_wavg_time` creates a time-based windowed weighted average operator to be used in an [`update_by`](./updateBy.md) table operation. Data is windowed by reverse and forward time intervals relative to the current row, and the rolling weighted average of values within the window is calculated.

## Syntax

```python syntax
rolling_wavg_time(
    ts_col: str,
    wcol: str,
    cols: Union[str, list[str]],
    rev_time: Union[int, str],
    fwd_time: Union[int, str],
) -> UpdateByOperation
```

## Parameters

<ParamTable>
<Param name="ts_col" type="str">

The name of the column containing timestamps.

</Param>

<Param name="cols" type="Union[str, list[str]]">

The column(s) to be operated on. These can include expressions to rename the output (e.g., `NewCol = Col`). If `None`, the rolling sample standard deviation is calculated for all applicable columns.

</Param>
<Param name="wcol" type="str">

The column containing the weight values.

</Param>
<Param name="rev_time" type="Union[int,str]">

The look-behind window size. This can be expressed as an integer in nanoseconds or a string [duration](../../query-language/types/durations.md), e.g., `"PT00:00:00.001"` or `"PTnHnMnS"`, where `H` is hour, `M` is minute, and `S` is second.

</Param>
<Param name="fwd_time" type="Union[int,str]">

The look-forward window size. This can be expressed as an integer in nanoseconds or a string [duration](../../query-language/types/durations.md), e.g., `"PT00:00:00.001"` or `"PTnHnMnS"`, where `H` is hour, `M` is minute, and `S` is second.

</Param>
</ParamTable>

## Returns

An [`UpdateByOperation`](./updateBy.md#parameters) to be used in an `update_by` table operation.

## Examples

The following example performs an [`update_by`](./updateBy.md) on the `source` table using three `rolling_wavg_time` operations. Each operation gives varying `rev_time` and `fwd_time` values to show how they affect the output. The windows for each operation are as follows:

- `op_before`: The window starts five seconds before the current row, and ends one second before the current row.
- `op_after`: The window starts one second after the current row, and ends five seconds after of the current row.
- `op_middle`: The window starts three seconds before the current row, and ends three seconds after the current row.

```python order=source,result
from deephaven.updateby import rolling_wavg_time
from deephaven.time import dh_now
from deephaven import empty_table

base_time = dh_now()

source = empty_table(10).update(
    [
        "Timestamp = base_time + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = i",
        "X = ii",
        "Weight = i * 2",
    ]
)

op_before = rolling_wavg_time(
    ts_col="Timestamp",
    wcol="Weight",
    cols=["Wavg1 = X"],
    rev_time=int(5e9),
    fwd_time=int(-1e9),
)
op_after = rolling_wavg_time(
    ts_col="Timestamp",
    wcol="Weight",
    cols=["Wavg2 = X"],
    rev_time="PT-1S",
    fwd_time="PT5S",
)
op_middle = rolling_wavg_time(
    ts_col="Timestamp",
    wcol="Weight",
    cols=["Wavg3 = X"],
    rev_time="PT3S",
    fwd_time="PT3S",
)

result = source.update_by(ops=[op_before, op_after, op_middle], by=["Letter"])
```

## Related documentation

- [How to use `update_by`](../../../how-to-guides/rolling-aggregations.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Ternary conditional operator](../../../how-to-guides/ternary-if-how-to.md)
- [`update`](../select/update.md)
- [`update_by`](./updateBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#RollingWAvg(java.lang.String,java.time.Duration,java.time.Duration,java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.rolling_wavg_time)
