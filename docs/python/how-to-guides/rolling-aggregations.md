---
title: Rolling aggregations
---

This guide shows how to use the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation for rolling calculations and aggregations. It returns a new table that adds columns calculated by one or more [`UpdateByOperation`](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.UpdateByOperation) objects. The calculations can be cumulative, windowed by rows (ticks) or by time, or exponentially weighted.

[`update_by`](../reference/table-operations/update-by-operations/updateBy.md) takes one or more [`UpdateByOperations`](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.UpdateByOperation) and zero or more key columns. The result contains every source column plus any new columns the operations create. An operation whose output column has the same name as an existing column replaces that column. Without key columns, each operation runs over all rows. With key columns (also called grouping columns), each operation runs separately on each unique group.

For the full list of [`UpdateByOperations`](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.UpdateByOperation) you can pass to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md), see the [`update_by` parameters reference](../reference/table-operations/update-by-operations/updateBy.md#parameters).

## `update_by` vs `updateby`

This document refers to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) and [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) throughout. They are _not_ identical:

- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) always refers to the `update_by` _table operation_. You always call it as a method on a table:

```python syntax
result = source.update_by(...)
```

- [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) is the Python module that defines the operations you pass to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md), such as [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md) and [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md).

Keep this distinction in mind throughout the guide.

## What are rolling aggregations?

Rolling aggregations differ from [dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations in two ways: they preserve data history, and they can calculate over a window of rows instead of the whole group.

### Preservation of data history

Most [dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations output a single row for each group. The exceptions are [`head_by`](../reference/table-operations/group-and-aggregate/headBy.md) and [`tail_by`](../reference/table-operations/group-and-aggregate/tailBy.md), which output up to `n` rows for each group. In contrast, rolling aggregation tables have the same number of rows as the source table, so they preserve the data history that other aggregations discard.

To illustrate this, consider a cumulative sum on a single grouping column with two key values. Notice how `result_dedicated` and `result_combined` have two rows — one for each group. In contrast, `result_rolling` has 20 rows — one for each row in the source table. You can see how the cumulative sum increases over time in the latter case.

```python order=result_rolling,result_dedicated,result_combined,source
from deephaven import empty_table
from deephaven.updateby import cum_sum
from deephaven import agg

source = empty_table(20).update(["Key = (ii % 2 == 0) ? `A` : `B`", "Value = ii"])

result_rolling = source.update_by(cum_sum("SumValue=Value"), "Key")
result_dedicated = source.sum_by("Key")
result_combined = source.agg_by(agg.sum_("SumValue=Value"), "Key")
```

> [!NOTE]
> The syntax `SumValue=Value` names the output column `SumValue` and computes it from the input column `Value`.

### Windowing

[Dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations calculate each group's aggregated value over all of the group's rows. In contrast, rolling aggregations can be [cumulative](#cumulative-aggregations), [windowed](#windowed-aggregations), or [exponentially weighted](#exponential-moving-aggregations).

A windowed aggregation calculates over a subset of the table called a window. You define the window as a number of rows (ticks) or as a span of time. For example, a windowed sum can add up the previous 10 rows. When a new row arrives in a ticking (live, updating) table, Deephaven updates the result to include the new row and drop the row that left the window.

To illustrate this, consider the following example, which calculates a cumulative sum and a rolling sum with [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). The rolling sum uses a 3-row window (the current row and the two rows before it), so the `RollingSumValue` column differs from `CumSumValue`:

```python order=result,source
from deephaven import empty_table
from deephaven.updateby import cum_sum, rolling_sum_tick

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

The following example uses [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md) to calculate the rolling sum of the `X` column in the `source` table. Setting `rev_ticks=3` and `fwd_ticks=0` makes the window the current row and the two previous rows. [Tick-based vs time-based windows](#tick-based-vs-time-based-windows) explains both parameters. The example passes no key columns, so all rows of the table form a single group.

```python order=source,result
from deephaven.updateby import rolling_sum_tick
from deephaven import empty_table

source = empty_table(20).update(["X = i"])

result = source.update_by(
    ops=rolling_sum_tick(cols=["RollingSumX = X"], rev_ticks=3, fwd_ticks=0)
)
```

### Multiple `UpdateByOperations` with no grouping columns

The following example builds on the [single-operation example](#a-single-updatebyoperation-with-no-grouping-columns) by performing two [`UpdateByOperations`](/core/pydoc/code/deephaven.updateby.html#deephaven.updateby.UpdateByOperation) in a single [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). It calculates the cumulative minimum and maximum, then derives the range from them with [`update`](../reference/table-operations/select/update.md).

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

The following example uses [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md) to fill null values with the most recent previous non-null value. The example passes no columns to `forward_fill`, so it fills _all_ columns in `source` except the key columns. The forward-filled columns replace the original columns of the same name, such as `X` and `Y`, in `result`.

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

Cumulative aggregations are the simplest operations in the [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) Python module. They are statistics computed over all previous data points in a series. Deephaven supports the following cumulative statistics:

| Cumulative statistic | [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) function  |
| -------------------- | ------------------------------------------------------------------------------------------ |
| Minimum              | [`cum_min`](../reference/table-operations/update-by-operations/cum-min.md)                 |
| Maximum              | [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md)                 |
| Sum                  | [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md)                 |
| Product              | [`cum_prod`](../reference/table-operations/update-by-operations/cum-prod.md)               |
| Count where          | [`cum_count_where`](../reference/table-operations/update-by-operations/cum-count-where.md) |

The count-where operations, such as [`cum_count_where`](../reference/table-operations/update-by-operations/cum-count-where.md), count the rows that match a filter you supply, rather than aggregating a column's values.

### Cumulative sum

To illustrate a cumulative statistic, consider the cumulative sum. For each row, this operation calculates the sum of all previous values in a column, including the current row's value. The following illustration shows this:

![Cumulative sum illustration](../assets/how-to/updateby-cum-sum.png)

The code for the illustration above looks like this:

```python order=source,result
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(8).update("X = ii")
result = source.update_by(cum_sum("SumX=X"))
```

### Cumulative average

The [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) module does not directly support a function to compute the cumulative average of a column. However, you can still compute the cumulative average with a single [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) operation that sums both the column and a column of ones, then divide:

```python order=result
from deephaven.updateby import cum_sum
from deephaven import empty_table

source = empty_table(8).update("X = ii")

result = (
    source.update("Ones = 1")
    .update_by(cum_sum(cols=["SumX=X", "Ones"]))
    .update("CumAvgX = SumX / Ones")
    .drop_columns(cols=["SumX", "Ones"])
)
```

This demonstrates the flexibility of the [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) table operation. When no single operation computes the statistic you need, you can often combine an `update_by` call with [`update`](../reference/table-operations/select/update.md), as shown here.

## Windowed aggregations

Windowed aggregations operate on a finite window of rows rather than on all previous rows, as introduced in [Windowing](#windowing). This section shows how to define a window by rows (ticks) or by time, and lists the operations that use one.

### Tick-based vs time-based windows

**Tick-based windows** aggregate values over a specified number of rows with fixed window sizes defined by the `rev_ticks` and `fwd_ticks` parameters:

- `rev_ticks`: The number of rows the window extends backward, counting the current row. A value of `N` includes the current row and the `N-1` preceding rows.
- `fwd_ticks`: The number of subsequent rows to include in the window. The default is 0.
- A negative `fwd_ticks` ends the window before the current row, and a `rev_ticks` of 0 or less starts the window after the current row. Use them to build windows that lie entirely before or entirely after the current row.

Here are some examples of tick-based windows:

- `rev_ticks=1, fwd_ticks=0`: Contains only the current row.
- `rev_ticks=10, fwd_ticks=0`: Contains the 9 previous rows and the current row.
- `rev_ticks=0, fwd_ticks=10`: Contains the following 10 rows and excludes the current row.
- `rev_ticks=10, fwd_ticks=10`: Contains the previous 9 rows, the current row, and the following 10 rows.
- `rev_ticks=10, fwd_ticks=-5`: Contains 5 rows, from 9 rows before to 5 rows before the current row (inclusive).
- `rev_ticks=11, fwd_ticks=-1`: Contains 10 rows, from 10 rows before to 1 row before the current row (inclusive).
- `rev_ticks=-5, fwd_ticks=10`: Contains 5 rows, from 6 rows after to 10 rows after the current row (inclusive).

Cumulative operators like [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) are special cases of tick-based operators, where the window begins at the first row of the group and continues through to the current row.

**Time-based windows** aggregate values over time durations with variable window sizes defined by the `rev_time` and `fwd_time` parameters:

- `rev_time`: Duration to look back in time, as a duration string such as `"PT5s"` or `"PT00:00:05"`, or as a number of nanoseconds.
- `fwd_time`: Duration to look forward in time.
- A negative `fwd_time` ends the window before the current row's timestamp, and a negative `rev_time` starts the window after it.
- Time-based windows require a timestamp column, which you name with the `ts_col` parameter.

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
op_after = rolling_sum_tick(cols=["PosteriorX = X"], rev_ticks=-1, fwd_ticks=3)

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
from deephaven.time import to_j_instant
from deephaven import empty_table

base_time = to_j_instant("2023-01-01T00:00:00 ET")

source = empty_table(20).update(
    ["Timestamp = base_time + i * SECOND", "X = i", "Letter = (i % 2 == 0) ? `A` : `B`"]
)

op_before = rolling_sum_time(
    ts_col="Timestamp", cols=["PriorX = X"], rev_time="PT00:00:04", fwd_time=int(-1e9)
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
    fwd_time=int(4e9),
)

result = source.update_by(ops=[op_before, op_contains, op_after], by=["Letter"])
result_a = result.where(["Letter == `A`"])
result_b = result.where(["Letter == `B`"])
```

### Simple moving aggregations

Simple moving aggregations are statistics computed over a finite, moving window of data. These operations don't weight data points by their distance from the current row. Every point in the window counts equally, except in the weighted average, where a weight column you supply sets each point's weight. Deephaven supports the following simple moving statistics:

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
result = source.update_by(ops=rolling_avg_tick(cols="AvgX=X", rev_ticks=4))
```

### Additional rolling operations

Deephaven offers two more kinds of rolling operations that don't compute a fixed statistic. One kind groups values, and the other applies a custom formula:

| Rolling operation | Tick-based                                                                                           | Time-based                                                                                           |
| ----------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Grouping          | [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md)     | [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md)     |
| Custom formula    | [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md) | [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md) |

The grouping operations collect the values in each window into a vector. The custom formula operations apply a formula you write to the values in each window. To work with the grouped vectors, see [how to work with arrays](./work-with-arrays.md).

#### Rolling groups

The following example uses [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md) to collect the values of the current row and the 3 rows before it into a vector, separately for each `ID` value:

```python order=source,result
from deephaven.updateby import rolling_group_tick
from deephaven import empty_table

source = empty_table(50).update(["ID = (ii % 2 == 0) ? `A` : `B`", "Value = ii"])
result = source.update_by(
    ops=rolling_group_tick(cols="Group=Value", rev_ticks=4), by="ID"
)
```

The same example can use [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md) to define the window by time instead of by rows:

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
> A built-in simple moving aggregation, such as [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md), is generally more performant than a rolling group followed by a calculation.

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

Exponential moving aggregations are moving statistics that, unlike [windowed aggregations](#windowed-aggregations), use _all_ preceding data points. However, they assign more weight to recent data and exponentially down-weight older data. This means distant observations have little effect on the result, while closer observations have a greater influence. A decay parameter controls how fast the weight decreases. Deephaven supports the following exponential moving statistics:

| Exponential moving statistic | Tick-based                                                                       | Time-based                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Minimum (EMMin)              | [`emmin_tick`](../reference/table-operations/update-by-operations/emmin-tick.md) | [`emmin_time`](../reference/table-operations/update-by-operations/emmin-time.md) |
| Maximum (EMMax)              | [`emmax_tick`](../reference/table-operations/update-by-operations/emmax-tick.md) | [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md) |
| Sum (EMS)                    | [`ems_tick`](../reference/table-operations/update-by-operations/ems-tick.md)     | [`ems_time`](../reference/table-operations/update-by-operations/ems-time.md)     |
| Average (EMA)                | [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md)     | [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md)     |
| Standard deviation (EMStd)   | [`emstd_tick`](../reference/table-operations/update-by-operations/emstd-tick.md) | [`emstd_time`](../reference/table-operations/update-by-operations/emstd-time.md) |

The decay parameter is `decay_ticks` for tick-based operations and `decay_time` for time-based operations:

**Tick-based decay** (`decay_ticks`):

- Smaller values decay faster and put more weight on recent data.
- `decay_ticks=1`: Each row has ~37% of the weight of the row after it.
- `decay_ticks=10`: Each row has ~90% of the weight of the row after it.
- Higher values create smoother, slower-responding averages.

**Time-based decay** (`decay_time`):

- Controls how quickly older data loses influence over time.
- `decay_time="PT1s"`: Data from 1 second ago has ~37% of the current row's weight.
- `decay_time="PT10s"`: Data from 10 seconds ago has ~37% of the current row's weight.
- Longer decay times create more stable, less responsive averages.

To visualize an exponential moving statistic, consider the exponential moving average (EMA). The following illustration shows this:

![Exponential moving average illustration](../assets/how-to/updateby-ema.png)

The code for the illustration above looks like this:

```python order=source,result
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(8).update("X = ii")
result = source.update_by(ops=ema_tick(decay_ticks=2, cols="EmaX=X"))
```

The following example uses [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md) to show the effect that different decay rates have on the exponential moving average:

```python order=source,result
from deephaven.updateby import ema_tick
from deephaven import empty_table

source = empty_table(50).update("Value = ii")
decay_tick_rates = [2, 4, 6]

updateby_ops = [
    ema_tick(decay_ticks=decay_rate, cols=f"EmaValue_{decay_rate}=Value")
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
    ema_time(ts_col="Timestamp", decay_time="PT2s", cols="EmaValue_2=Value"),
    ema_time(ts_col="Timestamp", decay_time="PT4s", cols="EmaValue_4=Value"),
    ema_time(ts_col="Timestamp", decay_time="PT6s", cols="EmaValue_6=Value"),
]

result = source.update_by(ops=updateby_ops)
```

A row's weight falls to ~37% after one `decay_time`, so `EmaValue_2` responds fastest and `EmaValue_6` responds slowest.

## Bollinger Bands

Bollinger Bands are a technical-analysis tool, common in finance, that uses a moving average and a moving standard deviation to draw bands around a price series.

To compute Bollinger Bands:

1. Compute the moving average.
2. Compute the moving standard deviation.
3. Compute the upper and lower envelopes: the moving average plus and minus `w` moving standard deviations, where `w` sets the width of the bands.

> [!NOTE]
> The examples in this section plot with the built-in plotting API, which is no longer under active development. For most plotting tasks, use [Deephaven Express](/core/plotly/docs/) instead.

### Tick-based Bollinger Bands using simple moving statistics

This example passes [`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md) and [`rolling_std_tick`](../reference/table-operations/update-by-operations/rolling-std-tick.md) to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the envelope with [`update`](../reference/table-operations/select/update.md). `rev_ticks` sets the size of the rolling window in ticks.

```python test-set=bollinger order=f_abc,f_xyz,source,result
from deephaven.updateby import rolling_avg_tick, rolling_std_tick
from deephaven import empty_table
from deephaven.plot import Figure

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
        rolling_avg_tick(cols="AvgPrice=Price", rev_ticks=rev_ticks),
        rolling_std_tick(cols="StdPrice=Price", rev_ticks=rev_ticks),
    ],
    by="Ticker",
).update(["Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice"])

# Plot the Bollinger Bands


def plot_bollinger(t, ticker):
    d = t.where(f"Ticker == `{ticker}`")
    return (
        Figure()
        .plot_xy(series_name="Price", t=d, x="Timestamp", y="Price")
        .plot_xy(series_name="AvgPrice", t=d, x="Timestamp", y="AvgPrice")
        .plot_xy(series_name="Upper", t=d, x="Timestamp", y="Upper")
        .plot_xy(series_name="Lower", t=d, x="Timestamp", y="Lower")
        .show()
    )


f_abc = plot_bollinger(result, "ABC")
f_xyz = plot_bollinger(result, "XYZ")
```

### Time-based Bollinger Bands using simple moving statistics

Using the `source` table, `w`, and `plot_bollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md) and [`rolling_std_time`](../reference/table-operations/update-by-operations/rolling-std-time.md) to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the envelope with [`update`](../reference/table-operations/select/update.md). `rev_time` sets the length of both rolling windows.

```python test-set=bollinger order=f_abc,f_xyz,result
from deephaven.updateby import rolling_avg_time, rolling_std_time

rev_time = "PT00:20:00"

result = source.update_by(
    [
        rolling_avg_time(ts_col="Timestamp", cols="AvgPrice=Price", rev_time=rev_time),
        rolling_std_time(ts_col="Timestamp", cols="StdPrice=Price", rev_time=rev_time),
    ],
    by="Ticker",
).update(["Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice"])

f_abc = plot_bollinger(result, "ABC")
f_xyz = plot_bollinger(result, "XYZ")
```

### Tick-based Bollinger Bands using exponential moving statistics

Using the `source` table, `w`, and `plot_bollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md) and [`emstd_tick`](../reference/table-operations/update-by-operations/emstd-tick.md) to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the envelope with [`update`](../reference/table-operations/select/update.md). `decay_ticks` sets the decay rate in ticks, which controls how much weight earlier rows get.

```python test-set=bollinger order=f_abc,f_xyz,result
from deephaven.updateby import ema_tick, emstd_tick

decay_ticks = 20

result = source.update_by(
    [
        ema_tick(decay_ticks=decay_ticks, cols="AvgPrice=Price"),
        emstd_tick(decay_ticks=decay_ticks, cols="StdPrice=Price"),
    ],
    by="Ticker",
).update(["Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice"])

f_abc = plot_bollinger(result, "ABC")
f_xyz = plot_bollinger(result, "XYZ")
```

### Time-based Bollinger Bands using exponential moving statistics

Using the `source` table, `w`, and `plot_bollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`ema_time`](../reference/table-operations/update-by-operations/ema-time.md) and [`emstd_time`](../reference/table-operations/update-by-operations/emstd-time.md) to [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the envelope with [`update`](../reference/table-operations/select/update.md). `decay_time` sets the decay rate as a duration, which controls how much weight earlier rows get.

```python test-set=bollinger order=f_abc,f_xyz,result
from deephaven.updateby import ema_time, emstd_time

decay_time = "PT00:20:00"

result = source.update_by(
    [
        ema_time(ts_col="Timestamp", decay_time=decay_time, cols="AvgPrice=Price"),
        emstd_time(ts_col="Timestamp", decay_time=decay_time, cols="StdPrice=Price"),
    ],
    by="Ticker",
).update(["Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice"])

f_abc = plot_bollinger(result, "ABC")
f_xyz = plot_bollinger(result, "XYZ")
```

## Sequential difference and forward fill

[`update_by`](../reference/table-operations/update-by-operations/updateBy.md) also supports two operations that are not rolling aggregations: [`delta`](../reference/table-operations/update-by-operations/delta.md) and [`forward_fill`](../reference/table-operations/update-by-operations/forward-fill.md).

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

result = source.update_by(delta(cols="DiffX=X"), by="Letter")
```

#### Detrend time-series data

> [!NOTE]
> This example plots with the built-in plotting API, which is no longer under active development. For most plotting tasks, use [Deephaven Express](/core/plotly/docs/) instead.

Sequential differencing is a common first step in detrending time-series data, which means removing a long-term upward or downward trend so that short-term changes are easier to see. The [`delta`](../reference/table-operations/update-by-operations/delta.md) operation makes this easy. The following example plots `Price` and its sequential difference, `DiffPrice`, for each ticker:

```python order=no_detrend,detrend,source,result
from deephaven.updateby import delta
from deephaven import empty_table
from deephaven.plot import Figure

source = empty_table(1000).update(
    [
        "Timestamp = '2023-01-13T12:00 ET' + i * MINUTE",
        "Ticker = i % 2 == 0 ? `ABC` : `XYZ`",
        "Price = i % 2 == 0 ? 100 * sin(i / 40) + 100 * random() : 100 * cos(i / 40) + 100 * random() + i / 2",
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
        delta(cols="DefaultDeltaX=X"),
        delta(
            cols="NullDomDeltaX=X",
            delta_control=DeltaControl.NULL_DOMINATES,
        ),
        delta(
            cols="ValueDomDeltaX=X",
            delta_control=DeltaControl.VALUE_DOMINATES,
        ),
        delta(
            cols="ZeroDomDeltaX=X",
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

result = source.update_by(forward_fill("FillX=X"), by="Letter")
```

## Handling erroneous data

It's common for tables to contain null, NaN, or other erroneous values. Each family of [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations has a default way of handling them, and some operations accept a control parameter that changes it. To replace nulls with the most recent non-null value, use [`forward_fill`](#forward-fill).

### Default null handling behavior

By default, [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations handle null values as follows:

- **Cumulative operations** ([`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md), [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md), etc.): Skip null values and continue with the last valid result.
- **Rolling statistics** ([`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md), [`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md), etc.): Exclude null values from window calculations.
- **Count-where operations** ([`cum_count_where`](../reference/table-operations/update-by-operations/cum-count-where.md), [`rolling_count_where_tick`](../reference/table-operations/update-by-operations/rolling-count-where-tick.md), [`rolling_count_where_time`](../reference/table-operations/update-by-operations/rolling-count-where-time.md)): Evaluate their filter on every row, so a row with a null value is counted when the filter matches it.
- **Rolling groups and formulas** ([`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md), [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md), etc.): Keep null values in the window's vector, so your formula decides how to treat them.
- **Exponential operations** ([`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md), [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md), etc.): Skip null values unless configured otherwise via `op_control`.

### Operations with explicit error handling controls

You can configure some operations to handle erroneous data through control parameters:

- Exponential moving operations ([`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md), [`ems_tick`](../reference/table-operations/update-by-operations/ems-tick.md), [`emmin_tick`](../reference/table-operations/update-by-operations/emmin-tick.md), [`emmax_tick`](../reference/table-operations/update-by-operations/emmax-tick.md), [`emstd_tick`](../reference/table-operations/update-by-operations/emstd-tick.md), and their `_time` versions): Pass an [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md) as the `op_control` parameter.
- [`delta`](../reference/table-operations/update-by-operations/delta.md): Pass a [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md) as the `delta_control` parameter. See [Null handling with `DeltaControl`](#null-handling-with-deltacontrol).

For detailed configuration options, see the [`OperationControl` reference guide](../reference/table-operations/update-by-operations/OperationControl.md).

## Choose the right aggregation

The following questions help you choose between dedicated aggregations, combined aggregations, and [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations.

### Do you need to preserve data history?

As shown in [Preservation of data history](#preservation-of-data-history), dedicated and combined aggregations and [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) differ in how many rows they return:

- **Use [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)** (or [`sum_by`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md), etc.) when you want a single aggregated result per group.
- **Use [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)** when you need to see how the aggregated value progresses row by row.
- **Use a dedicated or combined aggregation with a time-bucket key column** when you want one row per group per interval, which gives a partial history. See [Downsampling](./downsampling.md).

For example, the following groups rows into buckets of five with a `TimeGroup` key column, which stands in for a time interval. `(int)` keeps the bucket number an integer, because dividing two integers in a query string returns a `double`:

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(
    [
        "TimeGroup = (int) (i / 5)",
        "Symbol = i % 2 == 0 ? `A` : `B`",
        "Volume = i + 1",
    ]
)

result = source.sum_by(by=["TimeGroup", "Symbol"])
```

### What type of aggregation window do you need?

- **Cumulative aggregations** ([`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md), [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md), etc.): Use when you want to aggregate all data from the beginning up to the current row.
- **Windowed aggregations**: Use when you want to consider only a window of rows before, around, or after the current row:
  - **Tick-based windows** ([`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md), etc.): Use when your window should be defined by a fixed number of rows.
  - **Time-based windows** ([`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md), etc.): Use when your window should be defined by a time duration and you have a timestamp column.

### How should recent vs distant data be weighted?

- **Simple moving aggregations** ([`rolling_avg_tick`](../reference/table-operations/update-by-operations/rolling-avg-tick.md), [`rolling_sum_time`](../reference/table-operations/update-by-operations/rolling-sum-time.md), etc.): Use when a data point's weight shouldn't depend on how far it is from the current row.
- **Exponential moving aggregations** ([`ema_tick`](../reference/table-operations/update-by-operations/ema-tick.md), [`emmax_time`](../reference/table-operations/update-by-operations/emmax-time.md), etc.): Use when recent data should have more influence than older data, with weights that decay exponentially by rows or by time.

### Do you need custom calculations?

- **Standard operations**: Use built-in functions such as [`rolling_sum_tick`](../reference/table-operations/update-by-operations/rolling-sum-tick.md), [`rolling_avg_time`](../reference/table-operations/update-by-operations/rolling-avg-time.md), and [`cum_max`](../reference/table-operations/update-by-operations/cum-max.md) for common statistical calculations.
- **Custom formulas**: Use [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md) or [`rolling_formula_time`](../reference/table-operations/update-by-operations/rolling-formula-time.md) when you need to apply custom mathematical expressions to windowed data.
- **Grouping operations**: Use [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md) or [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md) when you need to collect data into vectors for further processing.

### Do you need to handle missing or erroneous data?

See [Handling erroneous data](#handling-erroneous-data).

## Related documentation

- [Create an empty table](./new-and-empty-table.md#empty_table)
- [Create a time table](./time-table.md)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Combined aggregations](./combined-aggregations.md)
- [Downsampling](./downsampling.md)
- [Select, view, and update](./use-select-view-update.md)
- [Handle nulls, infs, and NaNs](./null-inf-nan.md)
- [How to work with arrays](./work-with-arrays.md)
- [How to create XY series plots](./plotting/api-plotting.md#xy-series)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
