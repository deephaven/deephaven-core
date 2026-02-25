---
title: Rolling aggregations
---

This guide provides a comprehensive overview of how to use the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation for rolling calculations and aggregations. This operation creates a new table containing columns with aggregated calculations (called `UpdateByOperations`) from a source table. The calculations can be cumulative, windowed by rows (ticks), or windowed by time. They can also be performed on a per-group basis, where groups are defined by one or more key columns.

For a complete list of all available `UpdateByOperations` that can be performed with [`update_by`](../reference/table-operations/update-by-operations/updateBy.md), see the [reference page](../reference/table-operations/update-by-operations/updateBy.md#parameters).

The use of [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) requires one or more of these calculations, as well as zero or more key columns to define groups. The resultant table contains all columns from the source table, as well as new columns if the output of the `UpdateByOperation` renames them. If no key columns are given, then the calculations are applied to all rows in the specified columns. If one or more key columns are given, the calculations are applied to each unique group in the key column(s).

## What are rolling aggregations?

Rolling aggregations are unique from [dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations for two reasons.

### Preservation of data history

When performing aggregations with [dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations, the output table contains a single row for each group. In contrast, rolling aggregation tables have the same number of rows as the source table. Thus, they preserve data history, whereas the others do not.

To illustrate this, consider a cumulative sum on a single grouping column with two key values. Notice how `result_dedicated` and `result_combined` have two rows - one for each group. In contrast, `result_rolling` has 20 rows - one for each row in the source table. You can see how the cumulative sum increases over time in the latter case.

```python order=result_rolling,result_dedicated,result_combined,source
from deephaven import empty_table
from deephaven.updateby import cum_sum
from deephaven import agg

source = empty_table(20).update(["Key = (ii % 2 = 0) ? `A` : `B`", "Value = ii"])

result_rolling = source.update_by(cum_sum("SumValue=Value"), "Key")
result_dedicated = source.sum_by("Key")
result_combined = source.agg_by(agg.sum_("SumValue=Value"), "Key")
```

### Windowing

[Dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations are all cumulative. They calculate aggregated values over an entire table. In contrast, rolling aggregations can be either [cumulative](#cumulative-aggregations) or [windowed](#windowed-aggregations).

A windowed aggregation is one that only calculates aggregated values over a subset of the table. This subset is defined by a window, which can be specified in terms of rows (ticks) or time. For example, a windowed sum may calculate the aggregated sum over the previous 10 rows in the table. When a new row ticks in, the aggregated value is updated to reflect the new row and the oldest row that is no longer in the window.

To illustrate this, consider the following example, which calculates a cumulative sum and a rolling sum with [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). The rolling sum is applied over the previous 3 rows, so the `SumValue` column differs from its cumulative counterpart:

```python order=result,source
from deephaven import empty_table
from deephaven.updateby import cum_sum, rolling_sum_tick
from deephaven import agg

source = empty_table(6).update("Value = ii")

result = source.update_by(
    [
        cum_sum("CumSumValue=Value"),
        rolling_sum_tick("RollingSumValue=Value", rev_ticks=3),
    ]
)
```

## Basic usage examples

The following examples demonstrate the fundamental patterns for using [`update_by`](../reference/table-operations/update-by-operations/updateBy.md).

### A single `UpdateByOperation` with no grouping columns

The following example calculates the tick-based rolling sum of the `X` column in the `source` table. No key columns are provided, so a single group exists that contains all rows of the table.

```python order=source,result
from deephaven.updateby import rolling_sum_tick
from deephaven import empty_table

source = empty_table(20).update(["X = i"])

result = source.update_by(
    ops=rolling_sum_tick(cols=["RollingSumX = X"], rev_ticks=3, fwd_ticks=0)
)
```

### Multiple `UpdateByOperations` with no grouping columns

The following example builds on the [previous](#a-single-updatebyoperation-with-no-grouping-columns) by performing two `UpdateByOperations` in a single [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). The cumulative minimum and maximum are calculated, and the range is derived from them.

```python order=source,result
from deephaven.updateby import cum_min, cum_max
from deephaven import empty_table

source = empty_table(20).update(["X = randomInt(0, 25)"])
result = source.update_by(
    ops=[cum_min(cols=["MinX = X"]), cum_max(cols=["MaxX = X"])]
).update(["RangeX = MaxX - MinX"])
```

### Multiple `UpdateByOperations` with a single grouping column

The following example builds on the [previous](#multiple-updatebyoperations-with-no-grouping-columns) by specifying a grouping column. The grouping column is `Letter`, which contains alternating letters `A` and `B`. As a result, the cumulative minimum, maximum, and range are calculated on a per-letter basis. The `result` table is split by letter via [`where`](../reference/table-operations/filter/where.md) to show this.

```python order=source,result,result_a,result_b
from deephaven.updateby import cum_min, cum_max
from deephaven import empty_table

source = empty_table(20).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)"]
)
result = source.update_by(
    ops=[cum_min(cols=["MinX = X"]), cum_max(cols=["MaxX = X"])], by=["Letter"]
).update(["RangeX = MaxX - MinX"])
result_a = result.where(["Letter == `A`"])
result_b = result.where(["Letter == `B`"])
```

### A single `UpdateByOperation` applied to multiple columns with multiple grouping columns

The following example builds on the [previous](#multiple-updatebyoperations-with-a-single-grouping-column) by applying a single `UpdateByOperation` to multiple columns as well as specifying multiple grouping columns. The grouping columns, `Letter` and `Truth`, contain alternating letters and random true/false values. Thus, groups are defined by unique combinations of letter and boolean. The `result` table is split by letter and truth value to show the unique groups.

```python order=source,result,result_a_true,result_a_false,result_b_true,result_b_false
from deephaven.updateby import rolling_sum_tick, cum_max, cum_min
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "Truth = randomBool()",
        "X = randomInt(0, 25)",
        "Y = randomInt(50, 75)",
    ]
)

rolling_sum_ops = rolling_sum_tick(
    cols=["RollingSumX = X", "RollingSumY = Y"], rev_ticks=5, fwd_ticks=0
)
min_ops = cum_min(cols=["MinX = X", "MinY = Y"])
max_ops = cum_max(cols=["MaxX = X", "MaxY = Y"])

result = source.update_by(
    ops=[rolling_sum_ops, min_ops, max_ops], by=["Letter", "Truth"]
).update(["RangeX = MaxX - MinX", "RangeY = MaxY - MinY"])
result_a_true = result.where(["Letter == `A`", "Truth == true"])
result_a_false = result.where(["Letter == `A`", "Truth == false"])
result_b_true = result.where(["Letter == `B`", "Truth == true"])
result_b_false = result.where(["Letter == `B`", "Truth == false"])
```

### Applying an `UpdateByOperation` to all columns

The following example uses [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md) to fill null values with the most recent previous non-null value. No columns are given to `forward_fill`, so the forward-fill is applied to _all_ columns in the `source` table except for the specified key column(s). This means the original columns (e.g., `X` and `Y`) are replaced in the `result` table by new columns of the same name containing the forward-filled values.

```python order=source,result
from deephaven.updateby import forward_fill
from deephaven.constants import NULL_INT
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = (i % 3 == 0) ? NULL_INT : i",
        "Y = (i % 5 == 2) ? i : NULL_INT",
    ]
)

result = source.update_by(ops=forward_fill(cols=[]), by=["Letter"])
```

## Cumulative aggregations

Cumulative aggregations are the simplest operations in the [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) Python module. They are statistics computed over all previous data points in a series. The following cumulative statistics are supported:

| Cumulative statistic | [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) function |
| -------------------- | ----------------------------------------------------------------------------------------- |
| Minimum              | [`cum_min`](../reference/table-operations/update-by-operations/cum-min.md)                |
| Maximum              | [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md)                |
| Sum                  | [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md)                |
| Product              | [`cum_prod`](../reference/table-operations/update-by-operations/cum-prod.md)              |

To illustrate a cumulative statistic, consider the cumulative sum. For each row, this operation calculates the sum of all previous values in a column, including the current row's value. The following illustration shows this:

![Cumulative sum illustration](../assets/how-to/updateby-cum-sum.png)

The code for the illustration above looks like this:

```python order=source,result
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(8).update("X = ii")
result = source.update_by(cum_sum("SumX=X"))
```

> [!NOTE]
> The syntax `SumX=X` indicates that the resultant column from the operation is named `SumX`.

## Windowed aggregations

Windowed aggregations are similar to cumulative aggregations but operate on a finite-sized window of data. This means they only consider a limited number of rows for their calculations. The window size can be defined either by a number of rows (ticks) or by a duration of time.

### Tick-based vs time-based windows

**Tick-based windows** aggregate values over a specified number of rows with fixed window sizes defined by `rev_ticks` and `fwd_ticks` parameters:

- `rev_ticks`: The number of previous rows to include in the window. A value of `N` includes the current row and the `N-1` preceding rows.
- `fwd_ticks`: The number of subsequent rows to include in the window.
- A negative value for `rev_ticks` or `fwd_ticks` excludes rows in that direction. For example, `fwd_ticks=-1` ensures the window does not include any future rows.
- Example: `rev_ticks=3, fwd_ticks=0` creates a 3-row lookback window (the current row and the two previous rows).

**Time-based windows** aggregate values over time durations with variable window sizes defined by `rev_time` and `fwd_time` parameters:

- `rev_time`: Duration to look back in time (as ISO duration string like `"PT5s"` or nanoseconds).
- `fwd_time`: Duration to look forward in time.
- Negative durations exclude time in that direction.
- Require a timestamp column specified by `ts_col` parameter.
- Example: `rev_time="PT1m", fwd_time="PT0s"` creates a 1-minute lookback window.

### Tick-based windowed calculations

The following example:

- Creates a static source table with two columns.
- Calculates the rolling sum of `X` grouped by `Letter`.
  - Three rolling sums are calculated using windows that are before, containing, and after the current row.
- Splits the `result` table by letter via [`where`](../reference/table-operations/filter/where.md) to show how the windowed calculations are performed on a per-group basis.

```python order=source,result,result_a,result_b
from deephaven.updateby import rolling_sum_tick
from deephaven import empty_table

source = empty_table(20).update(["X = i", "Letter = (i % 2 == 0) ? `A` : `B`"])

op_contains = rolling_sum_tick(cols=["ContainsX = X"], rev_ticks=1, fwd_ticks=1)
op_before = rolling_sum_tick(cols=["PriorX = X"], rev_ticks=3, fwd_ticks=-1)
op_after = rolling_sum_tick(cols=["PosteriorX = X"], rev_ticks=-1, fwd_ticks=3)

result = source.update_by(ops=[op_contains, op_before, op_after], by=["Letter"])
result_a = result.where(["Letter == `A`"])
result_b = result.where(["Letter == `B`"])
```

### Time-based windowed calculations

Time-based operations require a timestamp column and use `rev_time` and `fwd_time` parameters (as nanoseconds or ISO duration strings like `PT10s`).

The following example:

- Creates a static source table with three columns.
- Calculates the rolling sum of `X` grouped by `Letter`.
  - Three rolling sums are calculated using windows that are before, containing, and after the current row's timestamp.
- Splits the `result` table by letter via [`where`](../reference/table-operations/filter/where.md) to show how the windowed calculations are performed on a per-group basis.

```python order=source,result,result_a,result_b
from deephaven.updateby import rolling_sum_time
from deephaven.time import to_j_instant
from deephaven import empty_table

base_time = to_j_instant("2023-01-01T00:00:00 ET")

source = empty_table(20).update(
    ["Timestamp = base_time + i * SECOND", "X = i", "Letter = (i % 2 == 0) ? `A` : `B`"]
)

op_before = rolling_sum_time(
    ts_col="Timestamp", cols=["PriorX = X"], rev_time="PT00:00:03", fwd_time=int(-1e9)
)
op_contains = rolling_sum_time(
    ts_col="Timestamp",
    cols=["ContainsX = X"],
    rev_time="PT00:00:01",
    fwd_time="PT00:00:01",
)
op_after = rolling_sum_time(
    ts_col="Timestamp",
    cols=["PosteriorX = X"],
    rev_time="-PT00:00:01",
    fwd_time=int(3e9),
)

result = source.update_by(ops=[op_before, op_contains, op_after], by=["Letter"])
result_a = result.where(["Letter == `A`"])
result_b = result.where(["Letter == `B`"])
```

> [!NOTE]
> In tick-based operations, windows are calculated per-group (each group maintains its own window of rows). In time-based operations, windows are defined by timestamps across the entire table regardless of grouping.

### Simple moving (rolling) aggregations

Simple moving (or rolling) aggregations are statistics computed over a finite, moving window of data. These operations weigh each data point in the window equally, regardless of its distance from the current row. The following simple moving statistics are supported:

| Simple moving statistic | Tick-based                                                                                       | Time-based                                                                                       |
| ----------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| Count                   | [`rolling_count_tick`](../reference/table-operations/update-by-operations/rolling-count-tick.md) | [`rolling_count_time`](../reference/table-operations/update-by-operations/rolling-count-time.md) |
| Minimum                 | [`rolling_min_tick`](../reference/table-operations/update-by-operations/rolling-min-tick.md)     | [`rolling_min_time`](../reference/table-operations/update-by-operations/rolling-min-time.md)     |
| Maximum                 | [`rolling_max_tick`](../reference/table-operations/update-by-operations/rolling-max-tick.md)     | [`rolling_max_time`](../reference/table-operations/update-by-operations/rolling-max-time.md)     |
| Sum                     | [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md)     | [`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md)     |
| Product                 | [`rolling_prod_tick`](../reference/table-operations/update-by-operations/rolling-prod-tick.md)   | [`rolling_prod_time`](../reference/table-operations/update-by-operations/rolling-prod-time.md)   |
| Average                 | [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md)     | [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md)     |
| Weighted Average        | [`rolling_wavg_tick`](../reference/table-operations/update-by-operations/rolling-wavg-tick.md)   | [`rolling_wavg_time`](../reference/table-operations/update-by-operations/rolling-wavg-time.md)   |
| Standard Deviation      | [`rolling_std_tick`](../reference/table-operations/update-by-operations/rolling-std-tick.md)     | [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md)     |

To illustrate a simple moving aggregation, consider the rolling average. For each row, this operation calculates the average of all values within a specified window. The following illustration shows this:

![Rolling average illustration](../assets/how-to/updateby-rolling-avg.png)

The code for the illustration above looks like this:

```python order=source,result
from deephaven.updateby import rolling_avg_tick
from deephaven import empty_table

source = empty_table(8).update("X = ii")
result = source.update_by(ops=rolling_avg_tick(cols="AvgX=X", rev_ticks=4))
```

The following example demonstrates a series of different tick-based windows that look backwards, forwards, and both. Each example calculates the rolling sum of the `Value` column:

```python order=source,result
from deephaven.updateby import rolling_sum_tick
from deephaven import empty_table

source = empty_table(50).update("Value = ii")

updateby_ops = [
    rolling_sum_tick(cols="SumValueBackwards=Value", rev_ticks=10, fwd_ticks=0),
    rolling_sum_tick(cols="SumValueForwards=Value", rev_ticks=1, fwd_ticks=9),
    rolling_sum_tick(cols="SumValueBoth=Value", rev_ticks=6, fwd_ticks=5),
]

result = source.update_by(ops=updateby_ops)
```

The above example can be modified to use time-based windows instead of tick-based windows:

```python order=source,result
from deephaven.updateby import rolling_sum_time
from deephaven import empty_table

source = empty_table(50).update(
    ["Timestamp = '2025-05-01T09:30:00Z' + ii * SECOND", "Value = ii"]
)

updateby_ops = [
    rolling_sum_time(
        ts_col="Timestamp",
        cols="SumValueBackwards=Value",
        rev_time="PT10s",
        fwd_time="PT00:00:00",
    ),
    rolling_sum_time(
        ts_col="Timestamp",
        cols="SumValueForwards=Value",
        rev_time="PT00:00:00",
        fwd_time="PT10s",
    ),
    rolling_sum_time(
        ts_col="Timestamp", cols="SumValueBoth=Value", rev_time="PT5s", fwd_time="PT5s"
    ),
]

result = source.update_by(ops=updateby_ops)
```

### Exponential moving aggregations

Exponential moving statistics are another form of moving aggregations. Unlike simple moving aggregations that use a fixed window, these statistics use _all_ preceding data points. However, they assign more weight to recent data and exponentially down-weight older data. This means distant observations have little effect on the result, while closer observations have a greater influence. The rate at which the weight decreases is controlled by a decay parameter. The following exponential moving statistics are supported:

| Exponential moving statistic | Tick-based                                                                       | Time-based                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Minimum (EMMin)              | [`emmin_tick`](../reference/table-operations/update-by-operations/emmin-tick.md) | [`emmin_time`](../reference/table-operations/update-by-operations/emmin-time.md) |
| Maximum (EMMax)              | [`emmax_tick`](../reference/table-operations/update-by-operations/emmax-tick.md) | [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md) |
| Sum (EMS)                    | [`ems_tick`](../reference/table-operations/update-by-operations/ems-tick.md)     | [`ems_time`](../reference/table-operations/update-by-operations/ems-time.md)     |
| Average (EMA)                | [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md)     | [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md)     |
| Standard Deviation (EMStd)   | [`emstd_tick`](../reference/table-operations/update-by-operations/emstd-tick.md) | [`emstd_time`](../reference/table-operations/update-by-operations/emstd-time.md) |

To visualize an exponential moving statistic, consider the exponential moving average (EMA). The following illustration shows this:

![img](../assets/how-to/updateby-ema.png)

The code for the illustration above looks like this:

```python order=source,result
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(8).update("X = ii")
result = source.update_by(ops=ema_tick(decay_ticks=2, cols="EmaX=X"))
```

> [!NOTE]
> The syntax `EmaX=X` indicates that the resultant column from the operation is named `EmaX`.

The following example demonstrates the effect that different decay rates have on the exponential moving average:

```python order=source,result
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(50).update("Value = ii")
decay_tick_rates = [2, 4, 6]

updateby_ops = [
    ema_tick(decay_ticks=decay_rate, cols=f"EmaX_{decay_rate}=Value")
    for decay_rate in decay_tick_rates
]

result = source.update_by(ops=updateby_ops)
```

#### Understanding decay parameters

**Tick-based decay** (`decay_ticks`):

- Smaller values = faster decay, more weight on recent data.
- `decay_ticks=1`: Each previous row has ~37% the weight of the current row.
- `decay_ticks=10`: Each previous row has ~90% the weight of the current row.
- Higher values create smoother, slower-responding averages.

**Time-based decay** (`decay_time`):

- Controls how quickly older data loses influence over time.
- `decay_time="PT1s"`: Data from 1 second ago has ~37% weight.
- `decay_time="PT10s"`: Data from 10 seconds ago has ~37% weight.
- Longer decay times create more stable, less responsive averages.

The same example can be modified to use time-based windows instead of tick-based windows:

```python order=source,result
from deephaven.updateby import ema_time
from deephaven import empty_table

source = empty_table(50).update(
    ["Timestamp = '2025-05-01T09:30:00Z' + ii * SECOND", "Value = ii"]
)

updateby_ops = [
    ema_time(ts_col="Timestamp", decay_time="PT2s", cols="EmaX_2=Value"),
    ema_time(ts_col="Timestamp", decay_time="PT4s", cols="EmaX_4=Value"),
    ema_time(ts_col="Timestamp", decay_time="PT6s", cols="EmaX_6=Value"),
]

result = source.update_by(ops=updateby_ops)
```

In this time-based example:

- `decay_time="PT2s"`: Creates a fast-responding average where data loses ~63% influence after 2 seconds.
- `decay_time="PT4s"`: Medium responsiveness, data loses ~63% influence after 4 seconds.
- `decay_time="PT6s"`: Slower response, data loses ~63% influence after 6 seconds.

#### Additional rolling operations

Deephaven offers two additional rolling operations that are not simple statistics, but rather for grouping or custom formulas:

| Simple moving operation | Tick-based                                                                                           | Time-based                                                                                           |
| ----------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Grouping                | [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md)     | [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md)     |
| Custom formula          | [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md) | [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md) |

The grouping operations collect values within the rolling window into an array. The custom formula operations allow you to define a custom formula that is applied to the values in the window.

##### Rolling group operations

The following example demonstrates the grouping operation. It creates a rolling group of the last 4 rows for each group defined by the `ID` column:

```python order=source,result
from deephaven.updateby import rolling_group_tick
from deephaven import empty_table

source = empty_table(50).update(["ID = (ii % 2 == 0) ? `A` : `B`", "Value = ii"])
result = source.update_by(
    ops=rolling_group_tick(cols="Group=Value", rev_ticks=4), by="ID"
)
```

The same example can be modified to use time-based windows instead of tick-based windows:

```python order=source,result
from deephaven.updateby import rolling_group_time
from deephaven import empty_table

source = empty_table(50).update(
    [
        "Timestamp = '2025-05-01T09:30:00Z' + ii * SECOND",
        "ID = (ii % 2 == 0) ? `A` : `B`",
        "Value = ii",
    ]
)
result = source.update_by(
    ops=rolling_group_time(ts_col="Timestamp", cols="Group=Value", rev_time="PT8s"),
    by="ID",
)
```

> [!NOTE]
> It is always more performant to use a rolling aggregation than to perform a rolling group and then apply calculations.

##### Rolling formula operations

The following example demonstrates the custom formula operation. It computes the rolling geometric mean of the `X` column, grouped by the `ID` column:

```python order=source,result
from deephaven.updateby import rolling_formula_tick
from deephaven import empty_table

source = empty_table(100).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)"]
)

result = source.update_by(
    rolling_formula_tick(
        formula="pow(product(x), 1/count(x))",
        formula_param="x",
        cols="GeomMeanX=X",
        rev_ticks=3,
    ),
    by="Letter",
)
```

The same can be done with a time-based window:

```python order=source,result
from deephaven.updateby import rolling_formula_time
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
    ]
)

result = source.update_by(
    rolling_formula_time(
        ts_col="Timestamp",
        formula="pow(product(x), 1/count(x))",
        formula_param="x",
        cols="GeomMeanX=X",
        rev_time="PT5s",
    ),
    by="Letter",
)
```

## Handling erroneous data

It's common for tables to contain null, NaN, or other erroneous values. Different [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations handle these values in various ways:

### Operations with explicit error handling controls

Certain operations can be configured to handle erroneous data through control parameters:

- [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md) and [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md): Use `op_control` parameter with [OperationControl](../reference/table-operations/update-by-operations/OperationControl.md).
- [`delta`](../reference/table-operations/update-by-operations/delta.md): Use `delta_control` parameter with [DeltaControl](../reference/table-operations/update-by-operations/DeltaControl.md).

### Default null handling behavior

Most [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations have consistent default behavior for null values:

- **Cumulative operations** (`cum_sum`, `cum_max`, etc.): Skip null values and continue with the last valid result.
- **Rolling operations** (`rolling_avg_tick`, `rolling_sum_time`, etc.): Exclude null values from window calculations.
- **Exponential operations** (`ema_tick`, `emmax_time`, etc.): Skip null values unless configured otherwise via `op_control`.

### Specialized null handling operations

- **[`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md)**: Specifically designed to replace null values with the most recent non-null value.
- **[`delta`](../reference/table-operations/update-by-operations/delta.md)**: Configurable null handling through `DeltaControl` (see [Sequential difference](#sequential-difference) section).

For detailed configuration options, see the [OperationControl reference guide](../reference/table-operations/update-by-operations/OperationControl.md).

## Additional operations

[`update_by`](../reference/table-operations/update-by-operations/updateBy.md) also supports several other operations that are not strictly rolling aggregations. This section provides a brief overview of these operations with examples.

### Sequential difference

The [`delta`](../reference/table-operations/update-by-operations/delta.md) operation calculates the difference between a row's value and the previous row's value in a given column. This is useful for calculating the rate of change. The following example demonstrates this:

```python order=source,result
from deephaven.updateby import delta
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "X = randomInt(0, 25)",
    ]
)

result = source.update_by(delta(cols="DiffX=X"))
```

Sequential differences can be calculated on a per-group basis like all other [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations. The following example demonstrates this:

```python order=source,result
from deephaven.updateby import delta
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = randomBool() ? `A` : `B`",
        "X = randomInt(0, 25)",
    ]
)

result = source.update_by(delta(cols="DiffX=X"), by="Letter")
```

#### Detrend time-series data

Sequential differencing is a common first measure for detrending time-series data. The [`delta`](../reference/table-operations/update-by-operations/delta.md) operation makes this easy:

```python order=no_detrend,detrend,source,result
from deephaven.updateby import delta
from deephaven import empty_table
from deephaven.plot import Figure

source = empty_table(1000).update(
    [
        "Timestamp='2023-01-13T12:00 ET' + i*MINUTE",
        "Ticker = i%2==0 ? `ABC` : `XYZ`",
        "Price = i%2==0 ? 100*sin(i/40)+100*random() : 100*cos(i/40)+100*random()+i/2",
    ]
)

result = source.update_by(delta("DiffPrice=Price"), by="Ticker")

no_detrend = (
    Figure()
    .plot_xy(
        series_name="ABC", t=result.where("Ticker == `ABC`"), x="Timestamp", y="Price"
    )
    .plot_xy(
        series_name="XYZ", t=result.where("Ticker == `XYZ`"), x="Timestamp", y="Price"
    )
    .show()
)

detrend = (
    Figure()
    .plot_xy(
        series_name="ABC",
        t=result.where("Ticker == `ABC`"),
        x="Timestamp",
        y="DiffPrice",
    )
    .plot_xy(
        series_name="XYZ",
        t=result.where("Ticker == `XYZ`"),
        x="Timestamp",
        y="DiffPrice",
    )
    .show()
)
```

#### Null handling

The [`delta`](../reference/table-operations/update-by-operations/delta.md) function takes an optional `delta_control` argument to determine how null values are treated. You must supply a [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md) instance to use this argument. The following behaviors are available:

- `DeltaControl.NULL_DOMINATES`: A valid value following a null value returns null.
- `DeltaControl.VALUE_DOMINATES`: A valid value following a null value returns the valid value.
- `DeltaControl.ZERO_DOMINATES`: A valid value following a null value returns zero.

```python order=source,result
from deephaven.updateby import delta, DeltaControl
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = randomBool() ? `A` : `B`",
        "X = randomInt(0, 25)",
    ]
)

result = source.update_by(
    [
        delta(cols="DefaultDeltaX=X"),
        delta(
            cols="NullDomDeltaX=X",
            delta_control=DeltaControl(DeltaControl.NULL_DOMINATES),
        ),
        delta(
            cols="ValueDomDeltaX=X",
            delta_control=DeltaControl(DeltaControl.VALUE_DOMINATES),
        ),
        delta(
            cols="ZeroDomDeltaX=X",
            delta_control=DeltaControl(DeltaControl.ZERO_DOMINATES),
        ),
    ],
    by="Letter",
)
```

### Forward fill

The [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md) operation fills in null values with the most recent non-null value. This is useful for filling in missing data points in a time series. The following example demonstrates this:

```python order=source,result
from deephaven.updateby import forward_fill
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = randomBool() ? `A` : `B`",
        "X = randomBool() ? NULL_INT : randomInt(0, 25)",
    ]
)

result = source.update_by(forward_fill("FillX=X"), by="Letter")
```

## Choose the right aggregation

Choosing the right aggregation method depends on your specific use case and data requirements. Consider these key questions:

### `agg_by` vs `update_by`

[`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) and [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) share some distinct similarities and differences. Understanding how the two relate to each other is important for understanding when to use each of them.

`agg_by` is functionally equivalent to `sum_by`, `avg_by`, `min_by`, etc. When you see `sum_by` in examples below, know that you could replace it with `agg_by(agg.sum_(cols), by=groups)` to achieve the same result. Both approaches perform the same aggregation operations.

#### Similarities

Both operations perform aggregations on a table. For instance, you can calculate a cumulative sum with both. Additionally, both operations also support performing aggregations over groups of rows, such as calculating the average price of a given stock, or the sum of sales for a given product.

#### Differences

There are two main differences between the two operations:

##### `agg_by` output

The output table of a [single aggregation](./dedicated-aggregations.md) or [multiple aggregation](./combined-aggregations.md) only contains one row for each group. To illustrate this, consider the following example, which calculates the aggregated sum of the `Value` column for each `ID` in the source table:

```python order=result,source
from deephaven import empty_table
from random import choice


def rand_id() -> str:
    return choice(["A", "B", "C", "D"])


source = empty_table(100).update(["ID = rand_id()", "Value = randomDouble(0, 1)"])
result = source.sum_by("ID")
```

If the input table is ticking, then the value in each row of the output table changes when the input table ticks.

##### `update_by` output

[`update_by`](../reference/table-operations/update-by-operations/updateBy.md) produces an output table that contains the same number of rows as the input table. Each row in the output table contains the aggregated value up to that point for the corresponding group. This means that [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) keeps data history while performing the aggregation. To illustrate this, the previous example is modified to use [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) instead of [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md):

```python order=result,source
from deephaven.updateby import cum_sum
from deephaven import empty_table
from random import choice


def rand_id() -> str:
    return choice(["A", "B", "C", "D"])


source = empty_table(100).update(["ID = rand_id()", "Value = randomDouble(0, 1)"])
result = source.update_by(ops=cum_sum("Value"), by="ID")
```

Note how the `result` table has the same number of rows as the `source` table. You can look back at previous values to see what the aggregated value was at any point in the table's history.

**Use [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)** (or `sum_by`, `avg_by`, etc.) when you want a single aggregated result per group. The output contains one row for each group with the final aggregated value.

**Use [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)** when you need to see the progression of aggregated values over time. The output preserves all rows from the source table, showing how the aggregation evolves.

### Do you need to preserve data history?

- **Use [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)** (or `sum_by`, `avg_by`, etc.) when you want a single aggregated result per group. The output contains one row for each group with the final aggregated value.
- **Use [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)** when you need to see the progression of aggregated values over time. The output preserves all rows from the source table, showing how the aggregation evolves.

#### Capturing data history with traditional aggregations

While [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) naturally preserves data history, it's also possible to capture some data history with traditional [single aggregations](./dedicated-aggregations.md) and [multiple aggregations](./combined-aggregations.md). However, this requires a key column that places rows into buckets, such as intervals of time.

For example, you can create time-based buckets to see how aggregations change over different time periods:

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(
    [
        "TimeGroup = i / 5",
        "Symbol = i % 2 == 0 ? `A` : `B`",
        "Volume = i + 1",
    ]
)

result = source.sum_by(by=["TimeGroup", "Symbol"])
```

In this example, the traditional aggregation groups data into buckets (using `TimeGroup` to simulate time intervals), showing how aggregated values change across different groups while providing partial history.

For more information on splitting temporal data into buckets of time, see [Downsampling](./downsampling.md).

### What type of aggregation window do you need?

- **Cumulative aggregations** ([`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md), [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md), etc.): Use when you want to aggregate all data from the beginning up to the current row.
- **Windowed aggregations**: Use when you only want to consider a subset of recent data:
  - **Tick-based windows** ([`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md), etc.): Use when your window should be defined by a fixed number of rows.
  - **Time-based windows** ([`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md), etc.): Use when your window should be defined by a time duration and you have a timestamp column.

### How should recent vs. distant data be weighted?

- **Simple moving aggregations** ([`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md), [`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md), etc.): Use when all data points in the window should be weighted equally.
- **Exponential moving aggregations** ([`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md), [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md), etc.): Use when recent data should have more influence than older data, with exponential decay over time.

### Do you need custom calculations?

- **Standard operations**: Use built-in functions like [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md), [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md), [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md), etc. for common statistical calculations.
- **Custom formulas**: Use [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md) or [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md) when you need to apply custom mathematical expressions to windowed data.
- **Grouping operations**: Use [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md) or [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md) when you need to collect data into arrays for further processing.

### Do you need to handle missing or erroneous data?

- **Forward fill**: Use [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md) to replace null values with the most recent non-null value.
- **Sequential differences**: Use [`delta`](../reference/table-operations/update-by-operations/delta.md) to calculate row-to-row changes, useful for detrending time-series data.
- **Error handling**: Some operations like [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md) and [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md) support `op_control` parameters for handling null, NaN, or other erroneous values.

### Performance considerations

- Rolling aggregations are more performant than rolling groups followed by calculations.
- Tick-based operations maintain separate windows per group, while time-based operations use timestamps across the entire table.
- Exponential moving aggregations use all historical data but weight recent observations more heavily.

## Related documentation

- [Create an empty table](./new-and-empty-table.md#empty_table)
- [Create a time table](./time-table.md)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Combined aggregations](./combined-aggregations.md)
- [Downsampling](./downsampling.md)
- [Select, view, and update](./use-select-view-update.md)
- [Handle nulls, infs, and NaNs](./null-inf-nan.md)
- [How to create XY series plots](./plotting/api-plotting.md#xy-series)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
