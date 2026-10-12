---
title: Rolling aggregations
---

This guide shows how to use the [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation for rolling calculations and aggregations. It returns a new table that adds columns calculated by one or more [`UpdateByOperation`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html) objects. The calculations can be cumulative, windowed by rows (ticks) or by time, or exponentially weighted.

[`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) takes one or more [`UpdateByOperations`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html) and zero or more key columns. The result contains every source column plus any new columns the operations create. An operation whose output column has the same name as an existing column replaces that column. Without key columns, each operation runs over all rows. With key columns (also called grouping columns), each operation runs separately on each unique group.

For the full list of [`UpdateByOperations`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html) you can pass to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md), see the [`updateBy` parameters reference](../reference/table-operations/update-by-operations/updateBy.md#parameters).

## `updateBy` vs `updateby`

This document refers to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) and [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) throughout. They are _not_ identical:

- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) always refers to the `updateBy` _table operation_. You always call it as a method on a table:

```groovy syntax
result = source.updateBy(...)
```

- [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) is the Java package that defines the operations you pass to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md). Its [`UpdateByOperation`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html) interface provides these operations, for example [`EmMax`](../reference/table-operations/update-by-operations/em-max.md) and [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md).

Keep this distinction in mind throughout the guide.

## What are rolling aggregations?

Rolling aggregations differ from [dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations in two ways: they preserve data history, and they can calculate over a window of rows instead of the whole group.

### Preservation of data history

[Dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations output a single row for each group. In contrast, rolling aggregation tables have the same number of rows as the source table. Thus, they preserve data history, whereas the others do not.

To illustrate this, consider a cumulative sum on a single grouping column with two key values. Notice how `resultDedicated` and `resultCombined` have two rows — one for each group. In contrast, `resultRolling` has 20 rows — one for each row in the source table. You can see how the cumulative sum increases over time in the latter case.

```groovy order=resultRolling,resultDedicated,resultCombined,source
source = emptyTable(20).update("Key = (ii % 2 == 0) ? `A` : `B`", "Value = ii")

resultRolling = source.updateBy(CumSum("SumValue=Value"), "Key")
resultDedicated = source.sumBy("Key")
resultCombined = source.aggBy(AggSum("SumValue=Value"), "Key")
```

> [!NOTE]
> The syntax `SumValue=Value` names the output column `SumValue` and computes it from the input column `Value`.

### Windowing

[Dedicated](./dedicated-aggregations.md) and [combined](./combined-aggregations.md) aggregations calculate each group's aggregated value over all of the group's rows. In contrast, rolling aggregations can be [cumulative](#cumulative-aggregations), [windowed](#windowed-aggregations), or [exponentially weighted](#exponential-moving-aggregations).

A windowed aggregation calculates over a subset of the table called a window. You define the window as a number of rows (ticks) or as a span of time. For example, a windowed sum can add up the previous 10 rows. When a new row arrives in a ticking (live, updating) table, Deephaven updates the result to include the new row and drop the row that left the window.

To illustrate this, consider the following example, which calculates a cumulative sum and a rolling sum with [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md). The rolling sum uses a 3-row window (the current row and the two rows before it), so the `RollingSumValue` column differs from `CumSumValue`:

```groovy order=result,source
source = emptyTable(6).update("Value = ii")

result = source.updateBy([
    CumSum("CumSumValue=Value"),
    RollingSum(3, "RollingSumValue=Value")
])
```

## Basic usage examples

The following examples demonstrate the fundamental patterns for using [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md).

### A single `UpdateByOperation` with no grouping columns

The following example calculates the tick-based rolling sum of the `X` column in the `source` table. [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md) takes `revTicks` and an optional `fwdTicks` as its first arguments. Omitting `fwdTicks` is the same as passing 0. `RollingSum(3, 0, ...)` sets `revTicks = 3` and `fwdTicks = 0`, so the window is the current row and the two previous rows. [Tick-based vs time-based windows](#tick-based-vs-time-based-windows) explains both parameters. The example passes no key columns, so all rows of the table form a single group.

```groovy order=source,result
source = emptyTable(20).update("X = i")

result = source.updateBy(RollingSum(3, 0, "RollingSumX = X"))
```

### Multiple `UpdateByOperations` with no grouping columns

The following example builds on the [single-operation example](#a-single-updatebyoperation-with-no-grouping-columns) by performing two [`UpdateByOperations`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html) in a single [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md). It calculates the cumulative minimum and maximum, then derives the range from them with [`update`](../reference/table-operations/select/update.md).

```groovy order=source,result
source = emptyTable(20).update("X = randomInt(0, 25)")

result = source.updateBy([CumMin("MinX = X"), CumMax("MaxX = X")]).update("RangeX = MaxX - MinX")
```

### Multiple `UpdateByOperations` with a single grouping column

The following example builds on the [ungrouped multiple-operation example](#multiple-updatebyoperations-with-no-grouping-columns) by specifying a grouping column. The grouping column is `Letter`, which contains alternating letters `A` and `B`. As a result, [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) calculates the cumulative minimum, maximum, and range separately for each letter. The example splits `result` by letter with [`where`](../reference/table-operations/filter/where.md) to show this.

```groovy order=source,result,resultA,resultB
source = emptyTable(20).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)")

result = source.updateBy([CumMin("MinX = X"), CumMax("MaxX = X")], "Letter").update("RangeX = MaxX - MinX")

resultA = result.where("Letter == `A`")
resultB = result.where("Letter == `B`")
```

### Multiple `UpdateByOperations` applied to multiple columns with multiple grouping columns

The following example builds on the [single-grouping-column example](#multiple-updatebyoperations-with-a-single-grouping-column) by applying each [`UpdateByOperation`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html) to multiple columns and specifying multiple grouping columns. The grouping columns, `Letter` and `Truth`, contain alternating letters and random true/false values. Each unique combination of letter and boolean forms a group. The example splits `result` by letter and truth value to show the groups.

```groovy order=source,result,resultATrue,resultAFalse,resultBTrue,resultBFalse
source = emptyTable(20).update("Letter = (i % 2 == 0) ? `A` : `B`", "Truth = randomBool()", "X = randomInt(0, 25)", "Y = randomInt(50, 75)")

rollingSumOps = RollingSum(5, 0, "RollingSumX = X", "RollingSumY = Y")
minOps = CumMin("MinX = X", "MinY = Y")
maxOps = CumMax("MaxX = X", "MaxY = Y")

result = source.updateBy([rollingSumOps, minOps, maxOps], "Letter", "Truth").update("RangeX = MaxX - MinX", "RangeY = MaxY - MinY")
resultATrue = result.where("Letter == `A`", "Truth == true")
resultAFalse = result.where("Letter == `A`", "Truth == false")
resultBTrue = result.where("Letter == `B`", "Truth == true")
resultBFalse = result.where("Letter == `B`", "Truth == false")
```

### Applying an `UpdateByOperation` to all columns

The following example uses [`Fill`](../reference/table-operations/update-by-operations/fill.md) to fill null values with the most recent previous non-null value. The example passes no columns to `Fill`, so it fills _all_ columns in `source` except the key columns. The forward-filled columns replace the original columns of the same name, such as `X` and `Y`, in `result`.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 3 == 0) ? NULL_INT : i", "Y = (i % 5 == 2) ? i : NULL_INT")

result = source.updateBy(Fill(), "Letter")
```

## Cumulative aggregations

Cumulative aggregations are the simplest operations in the [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) package. They are statistics computed over all previous data points in a series. Deephaven supports the following cumulative statistics:

| Cumulative statistic | [`UpdateByOperation`](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html) method |
| -------------------- | -------------------------------------------------------------------------------------------- |
| Minimum              | [`CumMin`](../reference/table-operations/update-by-operations/cum-min.md)                    |
| Maximum              | [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md)                    |
| Sum                  | [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md)                    |
| Product              | [`CumProd`](../reference/table-operations/update-by-operations/cum-prod.md)                  |
| Count where          | [`CumCountWhere`](../reference/table-operations/update-by-operations/cum-count-where.md)     |

The count-where operations, such as [`CumCountWhere`](../reference/table-operations/update-by-operations/cum-count-where.md), count the rows that match a filter you supply, rather than aggregating a column's values.

### Cumulative sum

To illustrate a cumulative statistic, consider the cumulative sum. For each row, this operation calculates the sum of all previous values in a column, including the current row's value. The following illustration shows this:

![Cumulative sum illustration](../assets/how-to/updateby-cum-sum.png)

The code for the illustration above looks like this:

```groovy order=source,result
source = emptyTable(8).update("X = ii")

result = source.updateBy(CumSum("SumX=X"))
```

### Cumulative average

The [`updateby`](/core/javadoc/io/deephaven/api/updateby/package-summary.html) package has no method that computes the cumulative average of a column. However, you can still compute the cumulative average with a single [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) operation that sums both the column and a column of ones, then divide:

```groovy order=result
source = emptyTable(8).update("X = ii")

result = (
    source.update("Ones = 1")
    .updateBy(CumSum("SumX=X", "Ones"))
    .update("CumAvgX = SumX / Ones")
    .dropColumns("SumX", "Ones")
)
```

This demonstrates the flexibility of the [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation. When no single operation computes the statistic you need, you can often combine an `updateBy` call with [`update`](../reference/table-operations/select/update.md), as shown here.

## Windowed aggregations

Windowed aggregations operate on a finite window of rows rather than on all previous rows, as introduced in [Windowing](#windowing). This section shows how to define a window by rows (ticks) or by time, and lists the operations that use one.

### Tick-based vs time-based windows

**Tick-based windows** aggregate values over a fixed number of rows. Tick-based operations take `revTicks` and an optional `fwdTicks` (default 0) as their first arguments, for example `RollingSum(revTicks, fwdTicks, "Out=In")` or `RollingSum(revTicks, "Out=In")`. The parameters set the window as follows:

- `revTicks`: The number of rows the window extends backward, counting the current row. A value of `N` includes the current row and the `N-1` preceding rows.
- `fwdTicks`: The number of subsequent rows to include in the window.
- A negative `fwdTicks` ends the window before the current row, and a `revTicks` of 0 or less starts the window after the current row. Use them to build windows that lie entirely before or entirely after the current row.

Here are some examples of tick-based windows:

- `revTicks = 1, fwdTicks = 0`: Contains only the current row.
- `revTicks = 10, fwdTicks = 0`: Contains 9 previous rows and the current row.
- `revTicks = 0, fwdTicks = 10`: Contains the following 10 rows and excludes the current row.
- `revTicks = 10, fwdTicks = 10`: Contains the previous 9 rows, the current row, and the following 10 rows.
- `revTicks = 10, fwdTicks = -5`: Contains 5 rows, from 9 rows before to 5 rows before the current row (inclusive).
- `revTicks = 11, fwdTicks = -1`: Contains 10 rows, from 10 rows before to 1 row before the current row (inclusive).
- `revTicks = -5, fwdTicks = 10`: Contains 5 rows, from 6 rows after to 10 rows after the current row (inclusive).

Cumulative operators like [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) are special cases of tick-based operators, where the window begins at the first row of the group and continues through to the current row.

**Time-based windows** aggregate values over time durations with variable window sizes defined by the `revTime` and `fwdTime` parameters:

- `revTime`: Duration to look back in time, either a `Duration` such as [`parseDuration("PT5s")`](../reference/time/datetime/parseDuration.md) or a number of nanoseconds.
- `fwdTime`: Duration to look forward in time.
- A negative `fwdTime` ends the window before the current row's timestamp, and a negative `revTime` starts the window after it.
- Time-based windows require a timestamp column, which is the first argument.

Here are some examples of time-based windows:

- `revTime = parseDuration("PT0s"), fwdTime = parseDuration("PT0s")`: Contains rows that exactly match the current timestamp.
- `revTime = parseDuration("PT10m"), fwdTime = parseDuration("PT0s")`: Contains rows from 10 minutes earlier through the current timestamp (inclusive).
- `revTime = parseDuration("PT0s"), fwdTime = parseDuration("PT10m")`: Contains rows from the current timestamp through 10 minutes following the current timestamp (inclusive).
- `revTime = 60_000_000_000L, fwdTime = 60_000_000_000L`: Contains rows from 1 minute earlier through 1 minute following the current timestamp (inclusive).
- `revTime = parseDuration("PT10m"), fwdTime = parseDuration("-PT5m")`: Contains rows from 10 minutes earlier through 5 minutes before the current timestamp (inclusive). This is a purely backward-looking window.
- `revTime = -5_000_000_000L, fwdTime = 10_000_000_000L`: Contains rows from 5 seconds following through 10 seconds following the current timestamp (inclusive). This is a purely forward-looking window.

### Tick-based windowed calculations

The following example:

- Creates a static source table with two columns.
- Calculates the rolling sum of `X` grouped by `Letter`.
  - Uses three windows: one before, one containing, and one after the current row.
- Splits the `result` table by letter via [`where`](../reference/table-operations/filter/where.md) to show that each group gets its own windowed calculation.

```groovy order=source,result,resultA,resultB
source = emptyTable(20).update("X = i", "Letter = (i % 2 == 0) ? `A` : `B`")

opContains = RollingSum(1, 1, "ContainsX = X")
opBefore = RollingSum(3, -1, "PriorX = X")
opAfter = RollingSum(-1, 3, "PosteriorX = X")

result = source.updateBy([opContains, opBefore, opAfter], "Letter")
resultA = result.where("Letter == `A`")
resultB = result.where("Letter == `B`")
```

### Time-based windowed calculations

The following example uses the time-based window parameters described in [Tick-based vs time-based windows](#tick-based-vs-time-based-windows). It:

- Creates a static source table with three columns.
- Calculates the rolling sum of `X` grouped by `Letter`.
  - Uses three windows: one before, one containing, and one after the current row's timestamp.
- Splits the `result` table by letter via [`where`](../reference/table-operations/filter/where.md) to show that each group gets its own windowed calculation.

```groovy order=source,result,resultA,resultB
source = emptyTable(20).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "X = i",
    "Letter = (i % 2 == 0) ? `A` : `B`"
)

opBefore = RollingSum("Timestamp", parseDuration("PT4s"), parseDuration("-PT1s"), "PriorX = X")
opContains = RollingSum("Timestamp", parseDuration("PT2s"), parseDuration("PT2s"), "ContainsX = X")
opAfter = RollingSum("Timestamp", parseDuration("-PT1s"), parseDuration("PT4s"), "PosteriorX = X")

result = source.updateBy([opBefore, opContains, opAfter], "Letter")
resultA = result.where("Letter == `A`")
resultB = result.where("Letter == `B`")
```

### Simple moving aggregations

Simple moving aggregations are statistics computed over a finite, moving window of data. These operations don't weight data points by their distance from the current row. Every point in the window counts equally, except in the weighted average, where a weight column you supply sets each point's weight. Deephaven supports the following simple moving statistics:

| Simple moving statistic | Tick- and time-based                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------ |
| Count                   | [`RollingCount`](../reference/table-operations/update-by-operations/rolling-count.md)            |
| Minimum                 | [`RollingMin`](../reference/table-operations/update-by-operations/rolling-min.md)                |
| Maximum                 | [`RollingMax`](../reference/table-operations/update-by-operations/rolling-max.md)                |
| Sum                     | [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md)                |
| Product                 | [`RollingProduct`](../reference/table-operations/update-by-operations/rolling-product.md)        |
| Average                 | [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md)                |
| Weighted average        | [`RollingWAvg`](../reference/table-operations/update-by-operations/rolling-wavg.md)              |
| Standard deviation      | [`RollingStd`](../reference/table-operations/update-by-operations/rolling-std.md)                |
| Count where             | [`RollingCountWhere`](../reference/table-operations/update-by-operations/rolling-count-where.md) |

To illustrate a simple moving aggregation, consider the rolling average. For each row, this operation calculates the average of all values within a specified window. The following illustration shows this:

![Rolling average illustration](../assets/how-to/updateby-rolling-avg.png)

The code for the illustration above looks like this:

```groovy order=source,result
source = emptyTable(8).update("X = ii")

result = source.updateBy(RollingAvg(4, "AvgX=X"))
```

To compare window sizes, pass several [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) operations at once. This example computes 2-, 3-, and 5-row backward-looking averages _by group_, using `Letter` as the key column:

```groovy order=result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)")

sma2 = RollingAvg(2, "AvgX2=X")
sma3 = RollingAvg(3, "AvgX3=X")
sma5 = RollingAvg(5, "AvgX5=X")

result = source.updateBy([sma2, sma3, sma5], "Letter")
```

For a time-based window, pass a timestamp column and a `Duration` to [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md). This example computes 2-, 3-, and 5-second moving averages:

```groovy order=result
source = emptyTable(10).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "X = randomInt(0, 25)",
)

sma2sec = RollingAvg("Timestamp", parseDuration("PT00:00:02"), "AvgX2Sec=X")
sma3sec = RollingAvg("Timestamp", parseDuration("PT3s"), "AvgX3Sec=X")
sma5sec = RollingAvg("Timestamp", parseDuration("PT5s"), "AvgX5Sec=X")

result = source.updateBy([sma2sec, sma3sec, sma5sec])
```

### Additional rolling operations

Deephaven offers two more rolling operations that don't compute a fixed statistic. One groups values, and the other applies a custom formula:

| Rolling operation | Tick- and time-based                                                                      |
| ----------------- | ----------------------------------------------------------------------------------------- |
| Grouping          | [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)     |
| Custom formula    | [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) |

[`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) collects the values in each window into an array. [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) applies a formula you write to the values in each window.

#### Rolling groups

The following examples create rolling groups with tick-based windows, then with time-based windows. To work with the resulting arrays, see [how to work with arrays](./work-with-arrays.md).

Use [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) to create tick-based rolling groups, where each row's array holds the values from a window of rows set by `revTicks` and `fwdTicks`. Here's an example that collects the three previous rows and the current row:

```groovy order=result
source = emptyTable(100).update("Letter = randomBool() ? `A` : `B`", "X = randomInt(0, 100)")

result = source.updateBy(RollingGroup(4, "GroupX=X"), "Letter")
```

To create groups that include data after the current row, use the `fwdTicks` parameter. This example creates a group that consists of the two previous rows, the current row, and the next four rows:

```groovy order=result
source = emptyTable(100).update("Letter = randomBool() ? `A` : `B`", "X = randomInt(0, 100)")

result = source.updateBy(RollingGroup(3, 4, "GroupX=X"), "Letter")
```

Pass a timestamp column and a `Duration` to [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) to create time-based rolling groups:

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "Letter = randomBool() ? `A` : `B`",
    "X = randomInt(0, 25)",
)

result = source.updateBy(
    RollingGroup("Timestamp", parseDuration("PT3s"), "GroupX=X"),
    "Letter",
)
```

A time-based rolling group contains only the rows whose timestamps fall inside the window, so the array's size varies from row to row. When no other row with the same `Letter` falls inside the window, the array holds only the current row's value. A backward-looking tick-based array reaches a fixed size once enough rows exist.

Use `fwdTime` to include rows after the current row. The following example groups every row with the same `Letter` whose timestamp is within five seconds of the current row:

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "Letter = randomBool() ? `A` : `B`",
    "X = randomInt(0, 25)",
)

result = source.updateBy(RollingGroup("Timestamp", parseDuration("PT5s"), parseDuration("PT5s"), "GroupX=X"), "Letter")
```

> [!NOTE]
> A built-in simple moving aggregation, such as [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md), is generally more performant than a rolling group followed by a calculation.

#### Rolling formulas

A rolling formula can use any of Deephaven's [built-in functions](../reference/query-language/query-library/auto-imported/index.md), [arithmetic operators](./operators.md), or [user-defined Groovy functions](./groovy-closures.md).

Use [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) to create custom tick-based rolling formulas. Here's an example that computes the rolling geometric mean of a column `X` by group. The formula names the output column (`GeomMeanX`) and refers to the input column (`X`) directly. Within the formula, `X` is the array of the window's values:

```groovy order=result
source = emptyTable(100).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)")

result = source.updateBy(
    RollingFormula(3, "GeomMeanX = pow(product(X), 1 / count(X))"), "Letter"
)
```

[`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) also creates custom time-based rolling formulas. You must supply a timestamp column, and you can make the time window backward-looking, forward-looking, or both. Here's an example that computes the 5-second rolling geometric mean of a column `X` by group:

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "Letter = (i % 2 == 0) ? `A` : `B`",
    "X = randomInt(0, 25)",
)

result = source.updateBy(
    RollingFormula("Timestamp", parseDuration("PT5s"), "GeomMeanX = pow(product(X), 1 / count(X))"),
    "Letter"
)
```

## Exponential moving aggregations

Exponential moving aggregations are moving statistics that, unlike [windowed aggregations](#windowed-aggregations), use _all_ preceding data points. However, they assign more weight to recent data and exponentially down-weight older data. This means distant observations have little effect on the result, while closer observations have a greater influence. A decay parameter controls how fast the weight decreases. Deephaven supports the following exponential moving statistics:

| Exponential moving statistic | Tick- and time-based                                                    |
| ---------------------------- | ----------------------------------------------------------------------- |
| Minimum (EMMin)              | [`EmMin`](../reference/table-operations/update-by-operations/em-min.md) |
| Maximum (EMMax)              | [`EmMax`](../reference/table-operations/update-by-operations/em-max.md) |
| Sum (EMS)                    | [`Ems`](../reference/table-operations/update-by-operations/ems.md)      |
| Average (EMA)                | [`Ema`](../reference/table-operations/update-by-operations/ema.md)      |
| Standard deviation (EMStd)   | [`EmStd`](../reference/table-operations/update-by-operations/em-std.md) |

Each operation can take an optional [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md) as its first argument to configure how it handles [erroneous data](#handling-erroneous-data). The decay parameter comes next. Tick-based operations take `tickDecay`. Time-based operations take a timestamp column followed by `durationDecay` (a `Duration`) or `timeDecay` (nanoseconds).

**Tick-based decay** (`tickDecay`):

- Smaller values decay faster and put more weight on recent data.
- `tickDecay = 1`: Each row has ~37% of the weight of the row after it.
- `tickDecay = 10`: Each row has ~90% of the weight of the row after it.
- Higher values create smoother, slower-responding averages.

**Time-based decay** (`durationDecay`):

- Controls how quickly older data loses influence over time.
- `durationDecay = parseDuration("PT1s")`: Data from 1 second ago has ~37% of the current row's weight.
- `durationDecay = parseDuration("PT10s")`: Data from 10 seconds ago has ~37% of the current row's weight.
- Longer decay times create more stable, less responsive averages.

To visualize an exponential moving statistic, consider the exponential moving average (EMA). The following illustration shows this:

![Exponential moving average illustration](../assets/how-to/updateby-ema.png)

The code for the illustration above looks like this:

```groovy order=result
source = emptyTable(8).update("X = ii")

result = source.updateBy(Ema(2, "EmaX=X"))
```

The following example uses [`Ema`](../reference/table-operations/update-by-operations/ema.md) to show the effect that different decay rates have on the exponential moving average:

```groovy order=source,result
source = emptyTable(50).update("Value = ii")

ema2 = Ema(2, "EmaValue2=Value")
ema4 = Ema(4, "EmaValue4=Value")
ema6 = Ema(6, "EmaValue6=Value")

result = source.updateBy([ema2, ema4, ema6])
```

To decay by time instead of by ticks, pass a timestamp column and a `Duration` to [`Ema`](../reference/table-operations/update-by-operations/ema.md). This version of the decay-rate example uses 2-, 4-, and 6-second decay times:

```groovy order=result
source = emptyTable(50).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "Letter = (i % 2 == 0) ? `A` : `B`",
    "X = randomInt(0, 25)",
)

ema2sec = Ema("Timestamp", parseDuration("PT2s"), "EmaX2Sec=X")
ema4sec = Ema("Timestamp", parseDuration("PT4s"), "EmaX4Sec=X")
ema6sec = Ema("Timestamp", parseDuration("PT6s"), "EmaX6Sec=X")

result = source.updateBy([ema2sec, ema4sec, ema6sec], "Letter")
```

A row's weight falls to ~37% after one `durationDecay`, so `EmaX2Sec` responds fastest and `EmaX6Sec` responds slowest.

## Bollinger Bands

Bollinger Bands are a technical-analysis tool, common in finance, that uses a moving average and a moving standard deviation to draw bands around a price series.

To compute Bollinger Bands:

1. Compute the moving average.
2. Compute the moving standard deviation.
3. Compute the upper and lower envelopes: the moving average plus and minus `w` moving standard deviations, where `w` sets the width of the bands.

### Tick-based Bollinger Bands using simple moving statistics

This example passes [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) and [`RollingStd`](../reference/table-operations/update-by-operations/rolling-std.md) to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the envelope with [`update`](../reference/table-operations/select/update.md). `revTicks` sets the size of the rolling window in ticks.

```groovy test-set=bollinger order=fAbc,fXyz,source,result
// Generate some random example data

source = emptyTable(1000).update(
    "Timestamp = '2023-01-13T12:00 ET' + i * MINUTE",
    "Ticker = i % 2 == 0 ? `ABC` : `XYZ`",
    "Price = i % 2 == 0 ? 100 * sin(i / 40) + 100 * random() : 100 * cos(i / 40) + 100 * random() + i / 2",
)

// Compute the Bollinger Bands

revTicks = 20

// Coverage parameter - determines the width of the bands
w = 2

result = source.updateBy(
    [
        RollingAvg(revTicks, "AvgPrice=Price"),
        RollingStd(revTicks, "StdPrice=Price"),
    ],
    "Ticker",
).update("Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice")

// Plot the Bollinger Bands

plotBollinger = { t, ticker ->
    def d = t.where("Ticker == `${ticker}`")
    return plot("Price", d, "Timestamp", "Price")
        .plot("AvgPrice", d, "Timestamp", "AvgPrice")
        .plot("Upper", d, "Timestamp", "Upper")
        .plot("Lower", d, "Timestamp", "Lower")
        .show()
}

fAbc = plotBollinger(result, "ABC")
fXyz = plotBollinger(result, "XYZ")
```

### Time-based Bollinger Bands using simple moving statistics

Using the `source` table, `w`, and `plotBollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) and [`RollingStd`](../reference/table-operations/update-by-operations/rolling-std.md) to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the envelope with [`update`](../reference/table-operations/select/update.md). `revTime` sets the length of both rolling windows.

```groovy test-set=bollinger order=fAbc,fXyz,result
revTime = parseDuration("PT00:20:00")

result = source.updateBy(
    [
        RollingAvg("Timestamp", revTime, "AvgPrice=Price"),
        RollingStd("Timestamp", revTime, "StdPrice=Price"),
    ],
    "Ticker",
).update("Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice")

fAbc = plotBollinger(result, "ABC")
fXyz = plotBollinger(result, "XYZ")
```

### Tick-based Bollinger Bands using exponential moving statistics

Using the `source` table, `w`, and `plotBollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`Ema`](../reference/table-operations/update-by-operations/ema.md) and [`EmStd`](../reference/table-operations/update-by-operations/em-std.md) to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the envelope with [`update`](../reference/table-operations/select/update.md). `tickDecay` sets the decay rate in ticks, which controls how much weight earlier rows get.

```groovy test-set=bollinger order=fAbc,fXyz,result
tickDecay = 20

result = source.updateBy(
    [
        Ema(tickDecay, "AvgPrice=Price"),
        EmStd(tickDecay, "StdPrice=Price"),
    ],
    "Ticker",
).update("Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice")

fAbc = plotBollinger(result, "ABC")
fXyz = plotBollinger(result, "XYZ")
```

### Time-based Bollinger Bands using exponential moving statistics

Using the `source` table, `w`, and `plotBollinger` from [the tick-based example](#tick-based-bollinger-bands-using-simple-moving-statistics), this example passes [`Ema`](../reference/table-operations/update-by-operations/ema.md) and [`EmStd`](../reference/table-operations/update-by-operations/em-std.md) to [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) to compute the moving average and standard deviation, then computes the envelope with [`update`](../reference/table-operations/select/update.md). `durationDecay` sets the decay rate as a duration, which controls how much weight earlier rows get.

```groovy test-set=bollinger order=fAbc,fXyz,result
durationDecay = parseDuration("PT00:20:00")

result = source.updateBy(
    [
        Ema("Timestamp", durationDecay, "AvgPrice=Price"),
        EmStd("Timestamp", durationDecay, "StdPrice=Price"),
    ],
    "Ticker",
).update("Upper = AvgPrice + w * StdPrice", "Lower = AvgPrice - w * StdPrice")

fAbc = plotBollinger(result, "ABC")
fXyz = plotBollinger(result, "XYZ")
```

## Sequential difference and forward fill

[`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) also supports two operations that are not rolling aggregations: [`Delta`](../reference/table-operations/update-by-operations/delta.md) and [`Fill`](../reference/table-operations/update-by-operations/fill.md).

### Sequential difference

The [`Delta`](../reference/table-operations/update-by-operations/delta.md) operation calculates the difference between a row's value and the previous row's value in a given column. This is useful for calculating the rate of change. The following example demonstrates this:

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "X = randomInt(0, 25)",
)

result = source.updateBy(Delta("DiffX=X"))
```

You can calculate sequential differences per group, like all other [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) operations:

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "Letter = randomBool() ? `A` : `B`",
    "X = randomInt(0, 25)",
)

result = source.updateBy(Delta("DiffX=X"), "Letter")
```

#### Detrend time-series data

Sequential differencing is a common first step in detrending time-series data, which means removing a long-term upward or downward trend so that short-term changes are easier to see. The [`Delta`](../reference/table-operations/update-by-operations/delta.md) operation makes this easy. The following example plots `Price` and its sequential difference, `DiffPrice`, for each ticker:

```groovy order=noDetrend,detrend,source,result
source = emptyTable(1000).update(
    "Timestamp = '2023-01-13T12:00 ET' + i * MINUTE",
    "Ticker = i % 2 == 0 ? `ABC` : `XYZ`",
    "Price = i % 2 == 0 ? 100 * sin(i / 40) + 100 * random() : 100 * cos(i / 40) + 100 * random() + i / 2",
)

result = source.updateBy(Delta("DiffPrice=Price"), "Ticker")

noDetrend = plot("ABC", result.where("Ticker == `ABC`"), "Timestamp", "Price")
    .plot("XYZ", result.where("Ticker == `XYZ`"), "Timestamp", "Price")
    .show()

detrend = plot("ABC", result.where("Ticker == `ABC`"), "Timestamp", "DiffPrice")
    .plot("XYZ", result.where("Ticker == `XYZ`"), "Timestamp", "DiffPrice")
    .show()
```

#### Null handling with `DeltaControl`

To control how [`Delta`](../reference/table-operations/update-by-operations/delta.md) treats null values, pass a [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md) as its optional first argument, `control`. The following behaviors are available:

- `DeltaControl.NULL_DOMINATES`: A valid value following a null value returns null.
- `DeltaControl.VALUE_DOMINATES`: A valid value following a null value returns the valid value.
- `DeltaControl.ZERO_DOMINATES`: A valid value following a null value returns zero.

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "Letter = randomBool() ? `A` : `B`",
    "X = (i % 4 == 1) ? NULL_INT : randomInt(0, 25)",
)

result = source.updateBy(
    [
        Delta("DefaultDeltaX=X"),
        Delta(DeltaControl.NULL_DOMINATES, "NullDomDeltaX=X"),
        Delta(DeltaControl.VALUE_DOMINATES, "ValueDomDeltaX=X"),
        Delta(DeltaControl.ZERO_DOMINATES, "ZeroDomDeltaX=X"),
    ],
    "Letter",
)
```

Because `X` contains nulls, the delta columns differ on the rows where a valid value follows a null. `DefaultDeltaX` matches `NullDomDeltaX`, because [`Delta`](../reference/table-operations/update-by-operations/delta.md) uses `NULL_DOMINATES` by default.

### Forward fill

[`Fill`](../reference/table-operations/update-by-operations/fill.md) fills in null values with the most recent non-null value, which is useful for filling in missing data points in a time series. The example in [Applying an `UpdateByOperation` to all columns](#applying-an-updatebyoperation-to-all-columns) fills every column. This example fills a single column per group:

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "Letter = randomBool() ? `A` : `B`",
    "X = randomBool() ? NULL_INT : randomInt(0, 25)",
)

result = source.updateBy(Fill("FillX=X"), "Letter")
```

## Handling erroneous data

It's common for tables to contain null, NaN, or other erroneous values. Each family of [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) operations has a default way of handling them, and some operations accept a control argument that changes it. To replace nulls with the most recent non-null value, use [`Fill`](#forward-fill).

### Default null handling behavior

By default, [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) operations handle null values as follows:

- **Cumulative operations** ([`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md), [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md), etc.): Skip null values and continue with the last valid result.
- **Rolling statistics** ([`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md), [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md), etc.): Exclude null values from window calculations.
- **Count-where operations** ([`CumCountWhere`](../reference/table-operations/update-by-operations/cum-count-where.md), [`RollingCountWhere`](../reference/table-operations/update-by-operations/rolling-count-where.md)): Evaluate their filter on every row, so a row with a null value is counted when the filter matches it.
- **Rolling groups and formulas** ([`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md), [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)): Keep null values in the window's array, so your formula decides how to treat them.
- **Exponential operations** ([`Ema`](../reference/table-operations/update-by-operations/ema.md), [`EmMax`](../reference/table-operations/update-by-operations/em-max.md), etc.): Skip null values unless configured otherwise via [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md).

### Operations with explicit error handling controls

You can configure some operations to handle erroneous data through control parameters:

- Exponential moving operations ([`Ema`](../reference/table-operations/update-by-operations/ema.md), [`Ems`](../reference/table-operations/update-by-operations/ems.md), [`EmMin`](../reference/table-operations/update-by-operations/em-min.md), [`EmMax`](../reference/table-operations/update-by-operations/em-max.md), and [`EmStd`](../reference/table-operations/update-by-operations/em-std.md)): Pass an [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md) as the first argument, `control`.
- [`Delta`](../reference/table-operations/update-by-operations/delta.md): Pass a [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md) as the first argument, `control`. See [Null handling with `DeltaControl`](#null-handling-with-deltacontrol).

For detailed configuration options, see the [`OperationControl` reference guide](../reference/table-operations/update-by-operations/OperationControl.md).

## Choose the right aggregation

The following questions help you choose between dedicated aggregations, combined aggregations, and [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) operations.

### Do you need to preserve data history?

As shown in [Preservation of data history](#preservation-of-data-history), dedicated and combined aggregations and [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) differ in how many rows they return:

- **Use [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)** (or [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md), [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md), etc.) when you want a single aggregated result per group.
- **Use [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)** when you need to see how the aggregated value progresses row by row.
- **Use a dedicated or combined aggregation with a time-bucket key column** when you want one row per group per interval, which gives a partial history. See [Downsampling](./downsampling.md).

For example, the following groups rows into buckets of five with a `TimeGroup` key column, which stands in for a time interval. `(int)` keeps the bucket number an integer, because dividing two integers in a query string returns a `double`:

```groovy order=source,result
source = emptyTable(10).update(
    "TimeGroup = (int) (i / 5)",
    "Symbol = i % 2 == 0 ? `A` : `B`",
    "Volume = i + 1"
)

result = source.sumBy("TimeGroup", "Symbol")
```

### What type of aggregation window do you need?

- **Cumulative aggregations** ([`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md), [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md), etc.): Use when you want to aggregate all data from the beginning up to the current row.
- **Windowed aggregations**: Use when you want to consider only a window of rows before, around, or after the current row:
  - **Tick-based windows** ([`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md), etc.): Use when your window should be defined by a fixed number of rows.
  - **Time-based windows** ([`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md) with a timestamp column, etc.): Use when your window should be defined by a time duration.

### How should recent vs distant data be weighted?

- **Simple moving aggregations** ([`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md), [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md), etc.): Use when a data point's weight shouldn't depend on how far it is from the current row.
- **Exponential moving aggregations** ([`Ema`](../reference/table-operations/update-by-operations/ema.md), [`EmMax`](../reference/table-operations/update-by-operations/em-max.md), etc.): Use when recent data should have more influence than older data, with weights that decay exponentially by rows or by time.

### Do you need custom calculations?

- **Standard operations**: Use built-in methods such as [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md), [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md), and [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md) for common statistical calculations.
- **Custom formulas**: Use [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) when you need to apply custom mathematical expressions to windowed data.
- **Grouping operations**: Use [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) when you need to collect data into arrays for further processing.

### Do you need to handle missing or erroneous data?

See [Handling erroneous data](#handling-erroneous-data).

## Related documentation

- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Create a time table](./time-table.md)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Combined aggregations](./combined-aggregations.md)
- [Downsampling](./downsampling.md)
- [Select, view, and update](./use-select-view-update.md)
- [Handle nulls, infs, and NaNs](./null-inf-nan.md)
- [How to create XY series plots](./plotting/api-plotting.md#xy-series)
- [How to work with arrays](./work-with-arrays.md)
- [Formulas](./formulas.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)
- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`Delta`](../reference/table-operations/update-by-operations/delta.md)
- [`Fill`](../reference/table-operations/update-by-operations/fill.md)
