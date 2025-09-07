---
title: Use updateBy
sidebar_label: updateBy
---

This guide will show you how to use the [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) table operation in your queries. [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) creates a new table with resultant columns containing aggregated calculations (referred to as `UpdateByOperations`) of columns in a source table. The calculations can be cumulative, windowed by rows (ticks), or windowed by time. The calculations are optionally done on a per-group basis, where groups are defined by one or more key columns.

## Available `UpdateByOperations`

The calculations (`UpdateByOperations`) that can be performed with [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) are:

- [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md)
- [`CumMin`](../reference/table-operations/update-by-operations/cum-min.md)
- [`CumProd`](../reference/table-operations/update-by-operations/cum-prod.md)
- [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md)
- [`Delta`](../reference/table-operations/update-by-operations/delta.md)
- [`EmMax`](../reference/table-operations/update-by-operations/em-max.md)
- [`EmMin`](../reference/table-operations/update-by-operations/em-min.md)
- [`EmStd`](../reference/table-operations/update-by-operations/em-std.md)
- [`Ema`](../reference/table-operations/update-by-operations/ema.md)
- [`Ems`](../reference/table-operations/update-by-operations/ems.md)
- [`Fill`](../reference/table-operations/update-by-operations/fill.md)
- [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md)
- [`RollingCount`](../reference/table-operations/update-by-operations/rolling-count.md)
- [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)
- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`RollingMax`](../reference/table-operations/update-by-operations/rolling-max.md)
- [`RollingMin`](../reference/table-operations/update-by-operations/rolling-min.md)
- [`RollingProduct`](../reference/table-operations/update-by-operations/rolling-product.md)
- [`RollingStd`](../reference/table-operations/update-by-operations/rolling-std.md)
- [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md)
- [`RollingWAvg`](../reference/table-operations/update-by-operations/rolling-wavg.md)

The use of [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) requires one or more of the calculations in the list above, as well as zero or more key columns to define groups. The resultant table contains all columns from the source table, as well as new columns if the output of the `UpdateByOperation` renames them. If no key columns are given, then the calculations are applied to all rows in the specified columns. If one or more key columns are given, the calculations are applied to each unique group in the key column(s).

## Examples

Each of the following subsections illustrates how to use [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md).

### A single `UpdateByOperation` with no grouping columns

The following example calculates the tick-based rolling sum of the `X` column in the `source` table. No key columns are provided, so a single group exists that contains all rows of the table.

```groovy order=source,result
source = emptyTable(20).update("X = i")

result = source.updateBy(RollingSum(3, 0, "RollingSumX = X"))
```

### Multiple `UpdateByOperations` with no grouping columns

The following example builds on the [previous](#a-single-updatebyoperation-with-no-grouping-columns) by performing two `UpdateByOperations` in a single [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md). The cumulative minimum and maximum are calculated, and the range is derived from them.

```groovy order=source,result
source = emptyTable(20).update("X = randomInt(0, 25)")

result = source.updateBy([CumMin("MinX = X"), CumMax("MaxX = X")]).update("RangeX = MaxX - MinX")
```

### Multiple `UpdateByOperations` with a single grouping column

The following example builds on the [previous](#multiple-updatebyoperations-with-no-grouping-columns) by specifying a grouping column. The grouping column is `Letter`, which contains alternating letters `A` and `B`. As a result, the cumulative minimum, maximum, and range are calculated on a per-letter basis. The `result` table is split by letter via [`where`](../reference/table-operations/filter/where.md) to show this.

```groovy order=source,result,resultA,resultB
source = emptyTable(20).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 25)")

result = source.updateBy([CumMin("MinX = X"), CumMax("MaxX = X")], "Letter").update("RangeX = MaxX - MinX")

resultA = result.where("Letter == `A`")
resultB = result.where("Letter == `B`")
```

### A single `UpdateByOperation` applied to multiple columns with multiple grouping columns

The following example builds on the [previous](#multiple-updatebyoperations-with-a-single-grouping-column) by applying a single `UpdateByOperation` to multiple columns as well as specifying multiple grouping columns. The grouping columns, `Letter` and `Truth`, contain alternating letters and random true/false values. Thus, groups are defined by unique combinations of letter and boolean. The `result` table is split by letter and truth value to show the unique groups.

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

The following example uses [`Fill`](../reference/table-operations/update-by-operations/fill.md) to fill null values with the most recent previous non-null value. No columns are given to `fill`, so the forward-fill is applied to _all_ columns in the `source` table except for the specified key column(s). This also means that the `X` column is replaced in the `result` table by the forward-filled X values.

```groovy order=source,result
source = emptyTable(10).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = (i % 3 == 0) ? NULL_INT : i", "Y = (i % 5 == 2) ? i : NULL_INT")

result = source.updateBy(Fill(), "Letter")
```

### Tick-based windowed calculations

There are multiple `UpdateByOperations` that are windowed by ticks. When an operation is windowed, the window is defined when creating the operation.

For all tick-based windowed calculations, the window size and location relative to the current row are defined by two input parameters: `fwdTicks` and `revTicks`. The former defines how far _forward_ the window goes, whereas the latter defines how far _backwards_ it goes. `fwdTicks` is _inclusive_ of the current row: `fwdTicks = 0` means the window ends at the current row. `revTicks` is _not inclusive_ of the current row: `revTicks = 1` means the window starts at the current row. Both of these values can be either positive or negative. The bulleted list below gives several examples of these two parameters and the rolling window they create.

- `revTicks = 1, fwdTicks = 0` - Contains only the current row.
- `revTicks = 10, fwdTicks = 0` - Contains 9 previous rows and the current row.
- `revTicks = 0, fwdTicks = 10` - Contains the following 10 rows; excludes the current row.
- `revTicks = 10, fwdTicks = 10` - Contains the previous 9 rows, the current row and the 10 rows following.
- `revTicks = 10, fwdTicks = -5` - Contains 5 rows, beginning at 9 rows before, ending at 5 rows before the current row (inclusive).
- `revTicks = 11, fwdTicks = -1` - Contains 10 rows, beginning at 10 rows before, ending at 1 row before the current row (inclusive).
- `revTicks = -5, fwdTicks = 10` - Contains 5 rows, beginning 5 rows following, ending at 10 rows following the current row (inclusive).

The following example:

- Creates a static source table with two columns.
- Calculates the rolling sum of `X` grouped by `Letter`.
  - Three rolling sums are calculated using a window before, containing, and after to the current row.
- Splits the `result` table by letter via [where](../reference/table-operations/filter/where.md) to show how the windowed calculations are performed on a per-group basis.

```groovy order=source,result,resultA,resultB
source = emptyTable(20).update("X = i", "Letter = (i % 2 == 0) ? `A` : `B`")

opContains = RollingSum(2, 1, "ContainsX = X")
opBefore = RollingSum(3, -1, "PriorX = X")
opAfter = RollingSum(-1, 3, "PosteriorX = X")

result = source.updateBy([opContains, opBefore, opAfter], by="Letter")

resultA = result.where("Letter == `A`")
resultB = result.where("Letter == `B`")
```

### Time-based windowed calculations

There are multiple `UpdateByOperations` that are windowed by time. When an operation is windowed, the window is defined when creating the operation. These operations _require_ the source table to contain a column of [DateTimes](../reference/query-language/types/date-time.md).

For all time-based windowed calculations, the window size and location relative to the current row are defined by two input parameters: `revTime` and `fwdTime`. The former defines how far _forward_ the window goes, whereas the latter defines how far _backwards_ it goes. These parameters parameter can be given as an `long` number of nanoseconds or a [Duration](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html). The bulleted list below explains how window sizes vary based on the two parameters:

- `revTime = 0, fwdTime = 0` - Contains rows that exactly match the current timestamp.
- `revTime = 10 * MINUTE, fwdTime = 0` - Contains rows from 10m earlier through the current timestamp (inclusive).
- `revTime = 0, fwdTime = 10 * MINUTE` - Contains rows from the current timestamp through 10m following the current row timestamp (inclusive).
- `revTime = MINUTE, fwdTime = MINUTE` - Contains rows from 1m earlier through 1m following the current timestamp (inclusive).
- `revTime = 10 * MINUTE, fwdTime = -5 * MINUTE` - Contains rows from 10m earlier through 5m before the current timestamp (inclusive). This is a purely backwards-looking window.
- `revTime = -5 * SECOND, fwdTime = 10 * SECOND` - Contains rows from 5s following through 10s following the current timestamp (inclusive). This is a purely forwards-looking window.

The following example:

- Creates a static source table with three columns.
- Calculates the rolling sum of `X` grouped by `Letter`.
  - Three rolling sums are calculated using a window before, containing, and after the current timestamp.
- Splits the `result` table by letter via [`where`](../reference/table-operations/filter/where.md) to show how the windowed calculations are performed on a per-group basis.

```groovy skip-test
baseTime = convertDateTime("2023-01-01T00:00:00 NY")

source = emptyTable(20).update("Timestamp = baseTime + i * SECOND", "X = i", "Letter = (i % 2 == 0) ? `A` : `B`")

opBefore = RollingSum("Timestamp", 3 * SECOND, -1 * SECOND, "PriorX = X")
opContains = RollingSum("Timestamp", SECOND, SECOND, "ContainsX = X")
opAfter = RollingSum("Timestamp", -1 * SECOND, 3 * SECOND, "PosteriorX = X")

result = source.updateBy([opBefore, opContains, opAfter], "Letter")
resultA = result.where("Letter == `A`")
resultB = result.where("Letter == `B`")
```

## Handling erroneous data

It's common for tables to contain null, NaN, or other erroneous values. Certain [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) operations can be told how to handle these through the use of the [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md) class.

To see how erroneous data can be handled differently, see the [`OperationControl` reference guide](../reference/table-operations/update-by-operations/OperationControl.md).

## Related documentation

- [How to create an empty table](../how-to-guides/new-and-empty-table.md#newtable)
- [`CumMax`](../reference/table-operations/update-by-operations/cum-max.md)
- [`CumMin`](../reference/table-operations/update-by-operations/cum-min.md)
- [`CumProd`](../reference/table-operations/update-by-operations/cum-prod.md)
- [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md)
- [`Ema`](../reference/table-operations/update-by-operations/ema.md)
- [`Fill`](../reference/table-operations/update-by-operations/fill.md)
- [`OperationControl`](../reference/table-operations/update-by-operations/OperationControl.md)
- [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md)
- [`RollingCount`](../reference/table-operations/update-by-operations/rolling-count.md)
- [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)
- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`RollingMax`](../reference/table-operations/update-by-operations/rolling-max.md)
- [`RollingMin`](../reference/table-operations/update-by-operations/rolling-min.md)
- [`RollingProduct`](../reference/table-operations/update-by-operations/rolling-product.md)
- [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md)
