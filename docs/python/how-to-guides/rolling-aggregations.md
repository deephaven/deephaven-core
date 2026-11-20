---
title: Rolling aggregations
---

This guide shows how to use the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation for rolling calculations and aggregations. The calculations can be cumulative, windowed by rows (ticks) or by time, or exponentially weighted.

[`update_by`](../reference/table-operations/update-by-operations/updateBy.md) takes two arguments:

- One or more [`UpdateByOperations`](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.UpdateByOperation), which define the new columns.
- Zero or more key columns (also called grouping columns). Without them, each operation runs over all rows. With them, each operation runs separately on each group.

The result keeps every source column and adds the new ones. An output column with the same name as a source column replaces it.

For the full list of [`UpdateByOperations`](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.UpdateByOperation) you can pass to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md), see the [`update_by` parameters reference](../reference/table-operations/update-by-operations/updateBy.md#parameters). To pick an operation for your task, see [Choose the right `update_by` operation](#choose-the-right-update_by-operation).

To compare rolling aggregations with the other ways Deephaven summarizes data, and to choose between them, see [Aggregations in Deephaven](../conceptual/aggregations.md).

## What are rolling aggregations?

Rolling aggregations differ from [dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations in two ways: they preserve data history, and they can calculate over a window of rows instead of the whole group. Rolling aggregations come in three families: [cumulative](#cumulative-aggregations), [windowed](#windowed-aggregations), and [exponential moving](#exponential-moving-aggregations). On this page, _rolling aggregation_ means any of these per-row calculations. The operations whose names start with `rolling_` are the windowed ones.

### Preservation of data history

Most [dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations output a single row for each group. The exceptions are [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md) and [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md), which output up to `n` rows for each group. In contrast, rolling aggregation tables have the same number of rows as the source table, so they preserve the data history that other aggregations discard.

To illustrate this, sum `Value` by `Key` three ways: a cumulative sum with [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md), a total with [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md), and a total with [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) and [`sum_`](../reference/table-operations/group-and-aggregate/AggSum.md). `result_dedicated` and `result_combined` have two rows, one for each group. `result_rolling` has 20 rows, one for each source row, and its `SumValue` column grows row by row.

```python order=result_rolling,result_dedicated,result_combined,source
from deephaven import empty_table
from deephaven.updateby import cum_sum
from deephaven import agg

source = empty_table(20).update(["Key = (ii % 2 == 0) ? `A` : `B`", "Value = ii"])

result_rolling = source.update_by(cum_sum("SumValue = Value"), "Key")
result_dedicated = source.sum_by("Key")
result_combined = source.agg_by(agg.sum_("SumValue = Value"), "Key")
```

> [!NOTE]
> The syntax `SumValue = Value` names the output column `SumValue` and computes it from the input column `Value`.

### Windowing

[Dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations calculate each group's aggregated value over all of the group's rows. A [windowed](#windowed-aggregations) rolling aggregation instead calculates over only part of the group, called a window.

You define the window as a number of rows (ticks) or as a span of time. For example, a windowed sum can add up the previous 10 rows. When a new row arrives in a ticking (live, updating) table, Deephaven updates the result to include the new row and drop the row that left the window.

To illustrate this, consider the following example, which calculates a cumulative sum with [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) and a windowed sum with [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md) in one [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) call. The windowed sum uses a 3-row window (the current row and the two rows before it), so the `RollingSumValue` column differs from `CumSumValue`:

```python order=result,source
from deephaven import empty_table
from deephaven.updateby import cum_sum, rolling_sum_tick

source = empty_table(6).update("Value = ii")

result = source.update_by(
    [
        cum_sum("CumSumValue = Value"),
        rolling_sum_tick("RollingSumValue = Value", rev_ticks=3),
    ]
)
```

[`update_by`](../reference/table-operations/update-by-operations/updateBy.md) works on ticking tables too. The following example creates a [`time_table`](../reference/table-operations/create/timeTable.md) that adds a row every second, then computes a 5-row rolling average with [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md). For more about ticking tables, see [Create a time table](./time-table.md).

```python ticking-table order=null
from deephaven import time_table
from deephaven.updateby import rolling_avg_tick

source = time_table("PT1s").update("X = ii")
result = source.update_by(rolling_avg_tick("AvgX = X", rev_ticks=5))
```

Deephaven updates `result` as each row arrives: the new row gets an `AvgX` value computed from that row and up to four rows before it.

## `update_by` vs `updateby`

This document refers to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) and [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) throughout. They are _not_ identical:

- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) always refers to the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) _table operation_, which you call as a method on a table:

```python syntax
result = source.update_by(...)
```

- [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) is the Python module that defines the operations you pass to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md), such as [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md) and [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md).

Keep this distinction in mind throughout the guide.

## Basic usage examples

The following examples demonstrate the fundamental patterns for using [`update_by`](../reference/table-operations/update-by-operations/updateBy.md).

### A single `UpdateByOperation` with no grouping columns

This example isolates the windowed sum from [Windowing](#windowing): [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md) is the only operation, and both window parameters are written out. `rev_ticks=3, fwd_ticks=0` makes the window the current row and the two before it ([Tick-based vs time-based windows](#tick-based-vs-time-based-windows) explains both). With no key columns, all rows form one group.

```python order=source,result
from deephaven.updateby import rolling_sum_tick
from deephaven import empty_table

source = empty_table(20).update(["X = i"])

result = source.update_by(
    ops=rolling_sum_tick(cols=["RollingSumX = X"], rev_ticks=3, fwd_ticks=0)
)
```

### Multiple `UpdateByOperations` with no grouping columns

The following example builds on the [single-operation example](#a-single-updatebyoperation-with-no-grouping-columns) by performing two [`UpdateByOperations`](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.UpdateByOperation) in a single [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). It calculates the cumulative minimum and maximum with [`cum_min`](../reference/table-operations/update-by-operations/cum-min.md) and [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md), then derives the range from them with [`update`](../reference/table-operations/select/update.md).

```python order=source,result
from deephaven.updateby import cum_min, cum_max
from deephaven import empty_table

source = empty_table(20).update(["X = randomInt(0, 25)"])
result = source.update_by(
    ops=[cum_min(cols=["MinX = X"]), cum_max(cols=["MaxX = X"])]
).update(["RangeX = MaxX - MinX"])
```

### Multiple `UpdateByOperations` with a single grouping column

The following example builds on the [ungrouped multiple-operation example](#multiple-updatebyoperations-with-no-grouping-columns) by specifying a grouping column. The grouping column is `Letter`, which contains alternating letters `A` and `B`. As a result, [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) calculates the cumulative minimum, maximum, and range separately for each letter. The example splits `result` by letter with [`where`](../reference/table-operations/filter/where.md) to show this.

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

### Multiple `UpdateByOperations` applied to multiple columns with multiple grouping columns

The following example builds on the [single-grouping-column example](#multiple-updatebyoperations-with-a-single-grouping-column) by applying each [`UpdateByOperation`](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.UpdateByOperation) to multiple columns and specifying multiple grouping columns. The grouping columns, `Letter` and `Truth`, contain alternating letters and random true/false values. Each unique combination of letter and boolean forms a group. The example splits `result` by letter and truth value to show the groups.

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

The following example uses [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md) to fill null values with the most recent previous non-null value. The example passes no columns to [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md), so it fills _all_ columns in `source` except the key columns. The forward-filled columns replace the original columns of the same name, such as `X` and `Y`, in `result`.

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

Cumulative aggregations are the simplest operations in the [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) Python module. They are statistics computed over every row from the start of the series (or group) through the current row. Deephaven supports the following cumulative statistics:

| Cumulative statistic | [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) function  |
| -------------------- | ------------------------------------------------------------------------------------------ |
| Minimum              | [`cum_min`](../reference/table-operations/update-by-operations/cum-min.md)                 |
| Maximum              | [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md)                 |
| Sum                  | [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md)                 |
| Product              | [`cum_prod`](../reference/table-operations/update-by-operations/cum-prod.md)               |
| Count where          | [`cum_count_where`](../reference/table-operations/update-by-operations/cum-count-where.md) |

The count-where operations, such as [`cum_count_where`](../reference/table-operations/update-by-operations/cum-count-where.md), count the rows that match a [filter](./filters.md) you supply, rather than aggregating a column's values.

### Cumulative sum

To illustrate a cumulative statistic, consider the cumulative sum. For each row, this operation sums the column's values from the first row (of the group) through the current row. The following illustration shows this:

![Cumulative sum illustration](../assets/how-to/updateby-cum-sum.png)

The code for the illustration above looks like this:

```python order=source,result
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(8).update("X = ii")
result = source.update_by(cum_sum("SumX = X"))
```

### Cumulative average

The [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) module has no function that computes the cumulative average of a column. However, you can still compute the cumulative average with a single [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) operation that sums both the column and a column of ones, then divide:

```python order=result
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(8).update("X = ii")

result = (
    source.update("Ones = 1")
    .update_by(cum_sum(cols=["SumX = X", "Ones"]))
    .update("CumAvgX = SumX / Ones")
    .drop_columns(cols=["SumX", "Ones"])
)
```

This demonstrates the flexibility of the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation. When no single operation computes the statistic you need, you can often combine an [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) call with [`update`](../reference/table-operations/select/update.md), as shown here.

## Windowed aggregations

Windowed aggregations operate on a finite window of rows rather than on all previous rows, as introduced in [Windowing](#windowing). This section shows how to define a window by rows (ticks) or by time, and lists the operations that use one.

### Tick-based vs time-based windows

**Tick-based windows** aggregate values over a fixed number of rows. The `rev_ticks` and `fwd_ticks` parameters set the window:

- `rev_ticks`: The number of rows the window extends backward, counting the current row. A value of `N` includes the current row and the `N-1` preceding rows.
- `fwd_ticks`: The number of subsequent rows to include in the window. The default is 0.

To build a window that lies entirely before the current row, use a negative `fwd_ticks`. To build one that lies entirely after it, use a `rev_ticks` of 0 or less.

Here are some examples of tick-based windows:

- `rev_ticks=1, fwd_ticks=0`: Contains only the current row.
- `rev_ticks=10, fwd_ticks=0`: Contains the 9 previous rows and the current row.
- `rev_ticks=0, fwd_ticks=10`: Contains the following 10 rows and excludes the current row.
- `rev_ticks=10, fwd_ticks=10`: Contains the previous 9 rows, the current row, and the following 10 rows.
- `rev_ticks=10, fwd_ticks=-5`: Contains 5 rows, from 9 rows before to 5 rows before the current row (inclusive).
- `rev_ticks=11, fwd_ticks=-1`: Contains 10 rows, from 10 rows before to 1 row before the current row (inclusive).
- `rev_ticks=-5, fwd_ticks=10`: Contains 5 rows, from 6 rows after to 10 rows after the current row (inclusive).

Cumulative operations like [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) are special cases of tick-based operations, where the window begins at the first row of the group and continues through to the current row.

**Time-based windows** aggregate values over a span of time, so the number of rows in each window varies. The `rev_time` and `fwd_time` parameters set the window:

- `rev_time`: Duration to look back in time, as a duration string such as `"PT5s"` or `"PT00:00:05"`, or as a number of nanoseconds.
- `fwd_time`: Duration to look forward in time, in the same forms. The default is 0.

Time-based windows require a timestamp column, which you name with the `ts_col` parameter. A negative `fwd_time` ends the window before the current row's timestamp, and a negative `rev_time` starts the window after it.

Here are some examples of time-based windows:

- `rev_time="PT0s", fwd_time="PT0s"`: Contains rows that exactly match the current timestamp.
- `rev_time="PT1m", fwd_time="PT0s"`: Contains rows from 1 minute earlier through the current timestamp (inclusive).
- `rev_time="PT10m", fwd_time="PT0s"`: Contains rows from 10 minutes earlier through the current timestamp (inclusive).
- `rev_time="PT0s", fwd_time="PT10m"`: Contains rows from the current timestamp through 10 minutes following the current timestamp (inclusive).
- `rev_time=60_000_000_000, fwd_time=60_000_000_000`: Contains rows from 1 minute earlier through 1 minute following the current timestamp (inclusive).
- `rev_time="PT10m", fwd_time="-PT5m"`: Contains rows from 10 minutes earlier through 5 minutes before the current timestamp (inclusive). This is a purely backward-looking window.
- `rev_time=-5_000_000_000, fwd_time=10_000_000_000`: Contains rows from 5 seconds following through 10 seconds following the current timestamp (inclusive). This is a purely forward-looking window.

### Tick-based windowed calculations

The following example:

- Creates a static source table with two columns.
- Calculates the rolling sum of `X` grouped by `Letter`.
  - Uses three windows: one before, one containing, and one after the current row.
- Splits the `result` table by letter via [`where`](../reference/table-operations/filter/where.md) to show that each group gets its own windowed calculation.

```python order=source,result,result_a,result_b
from deephaven.updateby import rolling_sum_tick
from deephaven import empty_table

source = empty_table(20).update(["X = i", "Letter = (i % 2 == 0) ? `A` : `B`"])

op_contains = rolling_sum_tick(cols=["ContainsX = X"], rev_ticks=1, fwd_ticks=1)
op_before = rolling_sum_tick(cols=["PriorX = X"], rev_ticks=3, fwd_ticks=-1)
op_after = rolling_sum_tick(cols=["PosteriorX = X"], rev_ticks=0, fwd_ticks=3)

result = source.update_by(ops=[op_contains, op_before, op_after], by=["Letter"])
result_a = result.where(["Letter == `A`"])
result_b = result.where(["Letter == `B`"])
```

### Time-based windowed calculations

The following example uses the time-based window parameters described in [Tick-based vs time-based windows](#tick-based-vs-time-based-windows). It:

- Creates a static source table with three columns.
- Calculates the rolling sum of `X` grouped by `Letter`.
  - Uses three windows: one before, one containing, and one after the current row's timestamp.
- Splits the `result` table by letter via [`where`](../reference/table-operations/filter/where.md) to show that each group gets its own windowed calculation.

```python order=source,result,result_a,result_b
from deephaven.updateby import rolling_sum_time
from deephaven import empty_table

source = empty_table(20).update(
    [
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "X = i",
        "Letter = (i % 2 == 0) ? `A` : `B`",
    ]
)

op_before = rolling_sum_time(
    ts_col="Timestamp",
    cols=["PriorX = X"],
    rev_time="PT00:00:04",
    fwd_time=-1_000_000_000,
)
op_contains = rolling_sum_time(
    ts_col="Timestamp",
    cols=["ContainsX = X"],
    rev_time="PT00:00:02",
    fwd_time="PT00:00:02",
)
op_after = rolling_sum_time(
    ts_col="Timestamp",
    cols=["PosteriorX = X"],
    rev_time="-PT00:00:01",
    fwd_time=4_000_000_000,
)

result = source.update_by(ops=[op_before, op_contains, op_after], by=["Letter"])
result_a = result.where(["Letter == `A`"])
result_b = result.where(["Letter == `B`"])
```

### Simple moving aggregations

Simple moving aggregations are statistics computed over a finite, moving window of data. Every point in the window has equal weight, except in the weighted average, where a weight column you supply sets each point's weight. Deephaven supports the following simple moving statistics:

| Simple moving statistic | Tick-based                                                                                                   | Time-based                                                                                                   |
| ----------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| Count                   | [`rolling_count_tick`](../reference/table-operations/update-by-operations/rolling-count-tick.md)             | [`rolling_count_time`](../reference/table-operations/update-by-operations/rolling-count-time.md)             |
| Minimum                 | [`rolling_min_tick`](../reference/table-operations/update-by-operations/rolling-min-tick.md)                 | [`rolling_min_time`](../reference/table-operations/update-by-operations/rolling-min-time.md)                 |
| Maximum                 | [`rolling_max_tick`](../reference/table-operations/update-by-operations/rolling-max-tick.md)                 | [`rolling_max_time`](../reference/table-operations/update-by-operations/rolling-max-time.md)                 |
| Sum                     | [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md)                 | [`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md)                 |
| Product                 | [`rolling_prod_tick`](../reference/table-operations/update-by-operations/rolling-prod-tick.md)               | [`rolling_prod_time`](../reference/table-operations/update-by-operations/rolling-prod-time.md)               |
| Average                 | [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md)                 | [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md)                 |
| Weighted average        | [`rolling_wavg_tick`](../reference/table-operations/update-by-operations/rolling-wavg-tick.md)               | [`rolling_wavg_time`](../reference/table-operations/update-by-operations/rolling-wavg-time.md)               |
| Standard deviation      | [`rolling_std_tick`](../reference/table-operations/update-by-operations/rolling-std-tick.md)                 | [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md)                 |
| Count where             | [`rolling_count_where_tick`](../reference/table-operations/update-by-operations/rolling-count-where-tick.md) | [`rolling_count_where_time`](../reference/table-operations/update-by-operations/rolling-count-where-time.md) |

To illustrate a simple moving aggregation, consider the rolling average. For each row, this operation calculates the average of all values within a specified window. The following illustration shows this:

![Rolling average illustration](../assets/how-to/updateby-rolling-avg.png)

The code for the illustration above looks like this:

```python order=source,result
from deephaven.updateby import rolling_avg_tick
from deephaven import empty_table

source = empty_table(8).update("X = ii")
result = source.update_by(ops=rolling_avg_tick(cols="AvgX = X", rev_ticks=4))
```

To compare window sizes, pass several [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md) operations at once. This example computes 2-, 3-, and 5-row backward-looking averages _by group_, using `Letter` as the key column:

```python order=source,result
from deephaven.updateby import rolling_avg_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)"]
)

sma_2 = rolling_avg_tick(cols="AvgX2 = X", rev_ticks=2)
sma_3 = rolling_avg_tick(cols="AvgX3 = X", rev_ticks=3)
sma_5 = rolling_avg_tick(cols="AvgX5 = X", rev_ticks=5)

result = source.update_by(ops=[sma_2, sma_3, sma_5], by="Letter")
```

For a time-based window, pass a timestamp column and a `rev_time` to [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md). This example computes 2-, 3-, and 5-second moving averages:

```python order=source,result
from deephaven.updateby import rolling_avg_time
from deephaven import empty_table

source = empty_table(10).update(
    ["Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND", "X = randomInt(0, 25)"]
)

sma_2_sec = rolling_avg_time(ts_col="Timestamp", cols="AvgX2Sec = X", rev_time="PT2s")
sma_3_sec = rolling_avg_time(ts_col="Timestamp", cols="AvgX3Sec = X", rev_time="PT3s")
sma_5_sec = rolling_avg_time(ts_col="Timestamp", cols="AvgX5Sec = X", rev_time="PT5s")

result = source.update_by(ops=[sma_2_sec, sma_3_sec, sma_5_sec])
```

### Additional rolling operations

Deephaven offers two more kinds of rolling operations that don't compute a fixed statistic. One kind groups values, and the other applies a custom formula:

| Rolling operation | Tick-based                                                                                           | Time-based                                                                                           |
| ----------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Grouping          | [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md)     | [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md)     |
| Custom formula    | [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md) | [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md) |

The grouping operations collect the values in each window into a vector. To work with those vectors, see [how to work with arrays](./work-with-arrays.md). The custom formula operations apply a formula you write to the values in each window.

#### Rolling groups

The following example uses [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md) to collect the values of the current row and the 3 rows before it into a vector, separately for each `ID` value:

```python order=source,result
from deephaven.updateby import rolling_group_tick
from deephaven import empty_table

source = empty_table(50).update(["ID = (ii % 2 == 0) ? `A` : `B`", "Value = ii"])
result = source.update_by(
    ops=rolling_group_tick(cols="Group = Value", rev_ticks=4), by="ID"
)
```

The same example can use [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md) to define the window by time instead of by rows. Rows with the same `ID` are 2 seconds apart, so a 6-second window holds the same four values as the tick-based example:

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
    ops=rolling_group_time(ts_col="Timestamp", cols="Group = Value", rev_time="PT6s"),
    by="ID",
)
```

> [!NOTE]
> A built-in simple moving aggregation, such as [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md), updates its result as rows enter and leave the window. A calculation on a rolling group reads every value in each row's window, so the built-in operation is usually faster.

#### Rolling formulas

A rolling formula can use any of Deephaven's [built-in functions](../reference/query-language/query-library/auto-imported/index.md), [arithmetic operators](./operators.md), or [user-defined Python functions](./python-functions.md).

The following example uses [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md) to compute the rolling geometric mean of the `X` column, grouped by the `Letter` column. The formula names the output column (`GeomMeanX`) and refers to the input column (`X`) directly. Within the formula, `X` is the vector of the window's values:

```python order=source,result
from deephaven.updateby import rolling_formula_tick
from deephaven import empty_table

source = empty_table(100).update(
    ["Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)"]
)

result = source.update_by(
    rolling_formula_tick(
        formula="GeomMeanX = pow(product(X), 1 / count(X))",
        rev_ticks=3,
    ),
    by="Letter",
)
```

[`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md) does the same with a time-based window:

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
        formula="GeomMeanX = pow(product(X), 1 / count(X))",
        rev_time="PT5s",
    ),
    by="Letter",
)
```

## Exponential moving aggregations

Exponential moving aggregations use _all_ preceding rows, unlike [windowed aggregations](#windowed-aggregations). Recent rows count most: each older row's weight shrinks exponentially, so distant rows barely affect the result. A decay parameter sets how fast the weight shrinks. Deephaven supports the following exponential moving statistics:

| Exponential moving statistic | Tick-based                                                                       | Time-based                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Minimum (EMMin)              | [`emmin_tick`](../reference/table-operations/update-by-operations/emmin-tick.md) | [`emmin_time`](../reference/table-operations/update-by-operations/emmin-time.md) |
| Maximum (EMMax)              | [`emmax_tick`](../reference/table-operations/update-by-operations/emmax-tick.md) | [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md) |
| Sum (EMS)                    | [`ems_tick`](../reference/table-operations/update-by-operations/ems-tick.md)     | [`ems_time`](../reference/table-operations/update-by-operations/ems-time.md)     |
| Average (EMA)                | [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md)     | [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md)     |
| Standard deviation (EMStd)   | [`emstd_tick`](../reference/table-operations/update-by-operations/emstd-tick.md) | [`emstd_time`](../reference/table-operations/update-by-operations/emstd-time.md) |

The decay parameter is `decay_ticks` for tick-based operations and `decay_time` for time-based operations:

**Tick-based decay** (`decay_ticks`): Smaller values decay faster and put more weight on recent data. Larger values create smoother, slower-responding averages. For example:

- `decay_ticks=1`: Each row has ~37% of the weight of the row after it.
- `decay_ticks=10`: Each row has ~90% of the weight of the row after it.

**Time-based decay** (`decay_time`): Longer decay times create more stable, less responsive averages. For example:

- `decay_time="PT1s"`: A row's weight falls to ~37% of its starting weight after 1 second.
- `decay_time="PT10s"`: A row's weight falls to ~37% of its starting weight after 10 seconds.

To visualize an exponential moving statistic, consider the exponential moving average (EMA). The following illustration shows this:

![Exponential moving average illustration](../assets/how-to/updateby-ema.png)

The code for the illustration above looks like this:

```python order=source,result
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(8).update("X = ii")
result = source.update_by(ops=ema_tick(decay_ticks=2, cols="EmaX = X"))
```

The following example uses [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md) to show the effect that different decay rates have on the exponential moving average:

```python order=source,result
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(50).update("Value = ii")
decay_tick_rates = [2, 4, 6]

updateby_ops = [
    ema_tick(decay_ticks=decay_rate, cols=f"EmaValue{decay_rate} = Value")
    for decay_rate in decay_tick_rates
]

result = source.update_by(ops=updateby_ops)
```

To decay by time instead of by ticks, pass a timestamp column and a `decay_time` to [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md). This version of the decay-rate example uses 2-, 4-, and 6-second decay times:

```python order=source,result
from deephaven.updateby import ema_time
from deephaven import empty_table

source = empty_table(50).update(
    ["Timestamp = '2025-05-01T09:30:00Z' + ii * SECOND", "Value = ii"]
)

updateby_ops = [
    ema_time(ts_col="Timestamp", decay_time="PT2s", cols="EmaValue2Sec = Value"),
    ema_time(ts_col="Timestamp", decay_time="PT4s", cols="EmaValue4Sec = Value"),
    ema_time(ts_col="Timestamp", decay_time="PT6s", cols="EmaValue6Sec = Value"),
]

result = source.update_by(ops=updateby_ops)
```

A row's weight falls to ~37% after one `decay_time`, so `EmaValue2Sec` responds fastest and `EmaValue6Sec` responds slowest.

Each exponential operation also accepts an optional `op_control` parameter, which takes an [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md) that sets how it handles [erroneous data](#handling-erroneous-data).

## Sequential difference and forward fill

[`update_by`](../reference/table-operations/update-by-operations/updateBy.md) also supports two operations that are not rolling aggregations: [`delta`](../reference/table-operations/update-by-operations/delta.md) and [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md).

### Sequential difference

The [`delta`](../reference/table-operations/update-by-operations/delta.md) operation calculates the difference between a row's value and the previous row's value in a given column. This shows how much a value changes from one row to the next. The following example demonstrates this:

```python order=source,result
from deephaven.updateby import delta
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "X = randomInt(0, 25)",
    ]
)

result = source.update_by(delta(cols="DiffX = X"))
```

You can calculate sequential differences per group, like all other [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations:

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

result = source.update_by(delta(cols="DiffX = X"), by="Letter")
```

#### Detrend time-series data

Sequential differencing is a common first step in detrending time-series data, which means removing a long-term upward or downward trend so that short-term changes are easier to see. The [`delta`](../reference/table-operations/update-by-operations/delta.md) operation computes that difference for every row in one step. The following example plots `Price` and its sequential difference, `DiffPrice`, for each ticker with [`dx.line`](/core/plotly/docs/line/):

```python order=no_detrend,detrend,source,result
from deephaven.updateby import delta
from deephaven import empty_table
import deephaven.plot.express as dx

source = empty_table(1000).update(
    [
        "Timestamp = '2023-01-13T12:00 ET' + i * MINUTE",
        "Ticker = i % 2 == 0 ? `ABC` : `XYZ`",
        "Price = i % 2 == 0 ? 100 * sin(i / 40) + 100 * random() : 100 * cos(i / 40) + 100 * random() + i / 2",
    ]
)

result = source.update_by(delta("DiffPrice = Price"), by="Ticker")

no_detrend = dx.line(result, x="Timestamp", y="Price", by="Ticker")
detrend = dx.line(result, x="Timestamp", y="DiffPrice", by="Ticker")
```

#### Null handling with `DeltaControl`

To control how [`delta`](../reference/table-operations/update-by-operations/delta.md) treats null values, pass a [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md) value as its optional `delta_control` argument. The following behaviors are available:

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
        "X = (i % 4 == 1) ? NULL_INT : randomInt(0, 25)",
    ]
)

result = source.update_by(
    [
        delta(cols="DefaultDeltaX = X"),
        delta(
            cols="NullDomDeltaX = X",
            delta_control=DeltaControl.NULL_DOMINATES,
        ),
        delta(
            cols="ValueDomDeltaX = X",
            delta_control=DeltaControl.VALUE_DOMINATES,
        ),
        delta(
            cols="ZeroDomDeltaX = X",
            delta_control=DeltaControl.ZERO_DOMINATES,
        ),
    ],
    by="Letter",
)
```

Because `X` contains nulls, the delta columns differ on the rows where a valid value follows a null. `DefaultDeltaX` matches `NullDomDeltaX`, because [`delta`](../reference/table-operations/update-by-operations/delta.md) uses `NULL_DOMINATES` by default.

### Forward fill

[`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md) fills in null values with the most recent non-null value, which is useful for filling in missing data points in a time series. The example in [Applying an `UpdateByOperation` to all columns](#applying-an-updatebyoperation-to-all-columns) fills every column. This example fills a single column per group:

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

result = source.update_by(forward_fill("FillX = X"), by="Letter")
```

## Handling erroneous data

Tables often contain null, NaN, or other erroneous values. Each family of [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations handles them in a default way, listed below, and some operations take a control parameter that changes it.

To replace nulls with the most recent non-null value, use [`forward_fill`](#forward-fill). For how Deephaven represents null, NaN, and infinite values, see [Handle nulls, infs, and NaNs](./null-inf-nan.md).

### Default null handling behavior

By default, [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations handle null values as follows:

- **Cumulative operations** ([`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md), [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md), etc.): Skip null values and continue with the last valid result.
- **Simple moving aggregations** ([`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md), [`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md), etc.): Exclude null values from window calculations.
- **Count-where operations** ([`cum_count_where`](../reference/table-operations/update-by-operations/cum-count-where.md), [`rolling_count_where_tick`](../reference/table-operations/update-by-operations/rolling-count-where-tick.md), [`rolling_count_where_time`](../reference/table-operations/update-by-operations/rolling-count-where-time.md)): Evaluate their filter on every row, so they count a row with a null value when the filter matches it.
- **Rolling groups and formulas** ([`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md), [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md), etc.): Keep null values in the window's vector, so your formula decides how to treat them.
- **Exponential operations** ([`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md), [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md), etc.): Skip null and NaN values unless an [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md) passed as `op_control` changes this.
- **Sequential difference** ([`delta`](../reference/table-operations/update-by-operations/delta.md)): Returns null for a valid value that follows a null (`DeltaControl.NULL_DOMINATES`) unless a [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md) passed as `delta_control` changes this.

For the options each control offers, see the [`OperationControl` reference guide](../reference/table-operations/update-by-operations/OperationControl.md) and [Null handling with `DeltaControl`](#null-handling-with-deltacontrol).

## Bollinger Bands

Bollinger Bands are a technical-analysis tool, common in finance, that uses a moving average and a moving standard deviation to draw bands around a price series.

To compute Bollinger Bands:

1. Compute the moving average.
2. Compute the moving standard deviation.
3. Compute the upper and lower envelopes: the moving average plus and minus `w` moving standard deviations, where `w` sets the width of the bands.

### Tick-based Bollinger Bands using simple moving statistics

This example passes [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md) and [`rolling_std_tick`](../reference/table-operations/update-by-operations/rolling-std-tick.md) to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the upper and lower envelopes with [`update`](../reference/table-operations/select/update.md). `rev_ticks` sets the size of the rolling window in ticks. The `plot_bollinger` function plots one ticker's price and bands with [`dx.line`](/core/plotly/docs/line/).

```python test-set=bollinger order=f_abc,f_xyz,source,result
from deephaven.updateby import rolling_avg_tick, rolling_std_tick
from deephaven import empty_table
import deephaven.plot.express as dx

# Generate some random example data

source = empty_table(1000).update(
    [
        "Timestamp = '2023-01-13T12:00 ET' + i * MINUTE",
        "Ticker = i % 2 == 0 ? `ABC` : `XYZ`",
        "Price = i % 2 == 0 ? 100 * sin(i / 40) + 100 * random() : 100 * cos(i / 40) + 100 * random() + i / 2",
    ]
)

# Compute the Bollinger Bands

rev_ticks = 20

# Coverage parameter - determines the width of the bands
w = 2

result = source.update_by(
    [
        rolling_avg_tick(cols="AvgPrice = Price", rev_ticks=rev_ticks),
        rolling_std_tick(cols="StdPrice = Price", rev_ticks=rev_ticks),
    ],
    by="Ticker",
).update(["Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice"])

# Plot the Bollinger Bands


def plot_bollinger(t, ticker):
    return dx.line(
        t.where(f"Ticker == `{ticker}`"),
        x="Timestamp",
        y=["Price", "AvgPrice", "Upper", "Lower"],
    )


f_abc = plot_bollinger(result, "ABC")
f_xyz = plot_bollinger(result, "XYZ")
```

### Time-based Bollinger Bands using simple moving statistics

Using the `source` table, `w`, and `plot_bollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md) and [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md) to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the upper and lower envelopes with [`update`](../reference/table-operations/select/update.md). `rev_time` sets the length of both rolling windows.

```python test-set=bollinger order=f_abc,f_xyz,result
from deephaven.updateby import rolling_avg_time, rolling_std_time

rev_time = "PT00:20:00"

result = source.update_by(
    [
        rolling_avg_time(
            ts_col="Timestamp", cols="AvgPrice = Price", rev_time=rev_time
        ),
        rolling_std_time(
            ts_col="Timestamp", cols="StdPrice = Price", rev_time=rev_time
        ),
    ],
    by="Ticker",
).update(["Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice"])

f_abc = plot_bollinger(result, "ABC")
f_xyz = plot_bollinger(result, "XYZ")
```

### Tick-based Bollinger Bands using exponential moving statistics

Using the `source` table, `w`, and `plot_bollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md) and [`emstd_tick`](../reference/table-operations/update-by-operations/emstd-tick.md) to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the upper and lower envelopes with [`update`](../reference/table-operations/select/update.md). `decay_ticks` sets the decay rate in ticks, which controls how much weight earlier rows get.

```python test-set=bollinger order=f_abc,f_xyz,result
from deephaven.updateby import ema_tick, emstd_tick

decay_ticks = 20

result = source.update_by(
    [
        ema_tick(decay_ticks=decay_ticks, cols="AvgPrice = Price"),
        emstd_tick(decay_ticks=decay_ticks, cols="StdPrice = Price"),
    ],
    by="Ticker",
).update(["Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice"])

f_abc = plot_bollinger(result, "ABC")
f_xyz = plot_bollinger(result, "XYZ")
```

### Time-based Bollinger Bands using exponential moving statistics

Using the `source` table, `w`, and `plot_bollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md) and [`emstd_time`](../reference/table-operations/update-by-operations/emstd-time.md) to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the upper and lower envelopes with [`update`](../reference/table-operations/select/update.md). `decay_time` sets the decay rate as a duration, which controls how much weight earlier rows get.

```python test-set=bollinger order=f_abc,f_xyz,result
from deephaven.updateby import ema_time, emstd_time

decay_time = "PT00:20:00"

result = source.update_by(
    [
        ema_time(ts_col="Timestamp", decay_time=decay_time, cols="AvgPrice = Price"),
        emstd_time(ts_col="Timestamp", decay_time=decay_time, cols="StdPrice = Price"),
    ],
    by="Ticker",
).update(["Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice"])

f_abc = plot_bollinger(result, "ABC")
f_xyz = plot_bollinger(result, "XYZ")
```

## Choose the right `update_by` operation

The following questions help you pick an [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operation. To decide whether [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) fits your task at all, rather than a [dedicated](./dedicated-aggregations.md) or [combined](./combined-aggregations.md) aggregation, see [Choose an operation](../conceptual/aggregations.md#choose-an-operation).

### What type of aggregation window do you need?

- **Cumulative aggregations** ([`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md), [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md), etc.): Use when you want to aggregate every row from the first row of the group through the current row.
- **Windowed aggregations**: Use when you want to consider only a window of rows before, around, or after the current row:
  - **Tick-based windows** ([`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md), etc.): Use when a fixed number of rows sets the window.
  - **Time-based windows** ([`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md), etc.): Use when a time duration sets the window and you have a timestamp column.

### How should you weight recent vs distant data?

- **Simple moving aggregations** ([`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md), [`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md), etc.): Use when a data point's weight shouldn't depend on how far it is from the current row.
- **Exponential moving aggregations** ([`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md), [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md), etc.): Use when recent data should have more influence than older data, with weights that decay exponentially by rows or by time.

### Do you need custom calculations?

- **Built-in operations**: Use built-in [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) operations such as [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md), [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md), and [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md) for common statistical calculations.
- **Custom formulas**: Use [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md) or [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md) when you need to apply custom mathematical expressions to windowed data.
- **Grouping operations**: Use [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md) or [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md) when you need to collect data into vectors for further processing.

### Do you need to handle missing or erroneous data?

See [Handling erroneous data](#handling-erroneous-data).

## Related documentation

- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Create an empty table](./new-and-empty-table.md#empty_table)
- [Create a time table](./time-table.md)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Combined aggregations](./combined-aggregations.md)
- [Downsampling](./downsampling.md)
- [Filters](./filters.md)
- [Select, view, and update](./use-select-view-update.md)
- [Handle nulls, infs, and NaNs](./null-inf-nan.md)
- [How to work with arrays](./work-with-arrays.md)
- [Deephaven Express](/core/plotly/docs/)
- [Formulas](./formulas.md)
- [Operators](./operators.md)
- [Python functions in query strings](./python-functions.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`cum_count_where`](../reference/table-operations/update-by-operations/cum-count-where.md)
- [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md)
- [`cum_min`](../reference/table-operations/update-by-operations/cum-min.md)
- [`cum_prod`](../reference/table-operations/update-by-operations/cum-prod.md)
- [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md)
- [`delta`](../reference/table-operations/update-by-operations/delta.md)
- [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`dx.line`](/core/plotly/docs/line/)
- [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md)
- [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md)
- [`emmax_tick`](../reference/table-operations/update-by-operations/emmax-tick.md)
- [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md)
- [`emmin_tick`](../reference/table-operations/update-by-operations/emmin-tick.md)
- [`emmin_time`](../reference/table-operations/update-by-operations/emmin-time.md)
- [`ems_tick`](../reference/table-operations/update-by-operations/ems-tick.md)
- [`ems_time`](../reference/table-operations/update-by-operations/ems-time.md)
- [`emstd_tick`](../reference/table-operations/update-by-operations/emstd-tick.md)
- [`emstd_time`](../reference/table-operations/update-by-operations/emstd-time.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md)
- [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md)
- [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md)
- [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md)
- [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md)
- [`rolling_count_tick`](../reference/table-operations/update-by-operations/rolling-count-tick.md)
- [`rolling_count_time`](../reference/table-operations/update-by-operations/rolling-count-time.md)
- [`rolling_count_where_tick`](../reference/table-operations/update-by-operations/rolling-count-where-tick.md)
- [`rolling_count_where_time`](../reference/table-operations/update-by-operations/rolling-count-where-time.md)
- [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md)
- [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md)
- [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md)
- [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md)
- [`rolling_max_tick`](../reference/table-operations/update-by-operations/rolling-max-tick.md)
- [`rolling_max_time`](../reference/table-operations/update-by-operations/rolling-max-time.md)
- [`rolling_min_tick`](../reference/table-operations/update-by-operations/rolling-min-tick.md)
- [`rolling_min_time`](../reference/table-operations/update-by-operations/rolling-min-time.md)
- [`rolling_prod_tick`](../reference/table-operations/update-by-operations/rolling-prod-tick.md)
- [`rolling_prod_time`](../reference/table-operations/update-by-operations/rolling-prod-time.md)
- [`rolling_std_tick`](../reference/table-operations/update-by-operations/rolling-std-tick.md)
- [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md)
- [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md)
- [`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md)
- [`rolling_wavg_tick`](../reference/table-operations/update-by-operations/rolling-wavg-tick.md)
- [`rolling_wavg_time`](../reference/table-operations/update-by-operations/rolling-wavg-time.md)
- [`sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`updateby` Pydoc](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby)
