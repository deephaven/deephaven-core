---
title: Other rolling utilities with update_by
sidebar_label: Other rolling utilities
---

This guide covers general rolling operations in the [`updateBy`](/core/javadoc/io/deephaven/engine/table/impl/updateby/UpdateBy.html) Groovy module. To learn about cumulative, rolling, and moving statistics, see our [related guide](./rolling-calculations.md).

## Rolling formulas

The [`updateBy`](/core/javadoc/io/deephaven/engine/table/impl/updateby/UpdateBy.html) module enables users to create custom rolling aggregations with the [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) function. For more information on tick vs. time operations, see [this section](../how-to-guides/rolling-calculations.md#time-based-vs-tick-based-operations) of the rolling statistics guide.

The user-defined formula can utilize any of Deephaven's [built-in functions](../reference/query-language/query-library/auto-imported-functions.md), [arithmetic operators](../how-to-guides/formulas-how-to.md#arithmetic-operators), or even [user-defined Groovy functions](../how-to-guides/user-defined-functions.md).

### `RollingFormula`

Use [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) to create custom tick-based rolling formulas. Here's an example that computes the rolling geometric mean of a column `X` by group:

```groovy order=result
source = emptyTable(100).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = randomInt(0, 100)")

result = source.updateBy(
    RollingFormula(3, "pow(product(x), 1/count(x))", "x", "GeomMeanX=X"), "Letter"
)
```

### `RollingFormula`

[`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md) can also be used to create custom time-based rolling formulas. You must supply a timestamp column, and can specify the time window as backward-looking, forward-looking, or both. Here's an example that computes the 5-second rolling geometric mean of a column `X` by group:

```groovy order=result
source = emptyTable(100).update(
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = (i % 2 == 0) ? `A` : `B`",
        "X = randomInt(0, 25)",
)

result = source.updateBy(
    RollingFormula("Timestamp", 5_000_000, "pow(product(x), 1/count(x))", "x", "GeomMeanX=X"),
    "Letter"
)
```

## Rolling groups

In addition to custom rolling formulas, [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) provides the ability to create rolling groups with [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md). The grouped data are represented as arrays. See the guide on [how to work with arrays](../how-to-guides/work-with-arrays.md) for more details.

### Tick-based `RollingGroup`

Use [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) to create tick-based rolling groups, where each group will have a specified number of entries determined by `revTicks` and `fwdTicks`. Here's an example that creates rolling groups with the three previous rows and the current row:

```groovy order=result
source = emptyTable(100).update("Letter = randomBool() ? `A` : `B`", "X = randomInt(0, 100)")

result = source.updateBy(RollingGroup(4, "GroupX=X"), "Letter")
```

To create groups that include data after the current row, use the `fwdTicks` parameter. This example creates a group that consists of the two previous rows, the current row, and the next four rows:

```groovy order=result
source = emptyTable(100).update("Letter = randomBool() ? `A` : `B`", "X = randomInt(0, 100)")

result = source.updateBy(RollingGroup(3, 4, "GroupX=X"), "Letter")
```

### Time-based `RollingGroup`

Similarly, use [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md) to create time-based rolling groups:

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

These groups are timestamp-based, so they are not guaranteed to contain elements from any previous row. This is in contrast to tick-based [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md), which always yields groups of a fixed size after that size has been reached.

The `fwdTime` parameter is used to create groups that include rows occuring after the current row. Here's an example that creates rolling groups out of every row within five seconds of the current row:

```groovy order=result
source = emptyTable(100).update(
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = randomBool() ? `A` : `B`",
        "X = randomInt(0, 25)",
)

result = source.updateBy(RollingGroup("Timestamp", parseDuration("PT5s"), parseDuration("PT5s"), "GroupX=X"), "Letter")
```

## Sequential differences with `Delta`

Deephaven's [`Delta`](../reference/table-operations/update-by-operations/delta.md) function can be used to compute sequential differences in a column of a table. Here's a simple example:

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "X = randomInt(0, 25)",
)

result = source.updateBy(Delta("DiffX=X"))
```

Like all other [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) functions, the `by` argument is used to specify grouping columns, so that sequential differences can be computed on a per-group basis:

```groovy order=result
source = emptyTable(100).update(
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = randomBool() ? `A` : `B`",
        "X = randomInt(0, 25)",
)

result = source.updateBy(Delta("DiffX=X"), "Letter")
```

### Detrend time-series data

Sequential differencing is often used as a first measure for detrending time-series data. The [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) module provides the [`Delta`](../reference/table-operations/update-by-operations/delta.md) function to make this easy:

```groovy order=noDetrend,detrend,source,result
source = emptyTable(1000).update(
    "Timestamp='2023-01-13T12:00 ET' + i*MINUTE",
    "Ticker = i%2==0 ? `ABC` : `XYZ`",
    "Price = i%2==0 ? 100*sin(i/40)+100*random() : 100*cos(i/40)+100*random()+i/2",
)

result = source.updateBy(Delta("DiffPrice=Price"), "Ticker")

noDetrend = plot("ABC", result.where("Ticker = `ABC`"), "Timestamp", "Price")
    .plot("XYZ", result.where("Ticker = `XYZ`"), "Timestamp", "Price")
    .show()

detrend = plot("ABC", result.where("Ticker = `ABC`"), "Timestamp", "DiffPrice")
    .plot("XYZ", result.where("Ticker = `XYZ`"), "Timestamp", "DiffPrice")
    .show()
```

### Handle nulls with `DeltaControl`

The [`Delta`](../reference/table-operations/update-by-operations/delta.md) function takes an optional argument `DeltaControl` that is used to determine how null values are treated. To use this argument, you must supply a [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md) instance. The following behaviors are available via [`DeltaControl`](../reference/table-operations/update-by-operations/DeltaControl.md):

- `DeltaControl.NULL_DOMINATES`: A valid value following a null value returns null.
- `DeltaControl.VALUE_DOMINATES`: A valid value following a null value returns the valid value.
- `DeltaControl.ZERO_DOMINATES`: A valid value following a null value returns zero.

To see how each of these behave in context, consider the following example:

```groovy order=result
source = emptyTable(100).update(
    "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
    "Letter = randomBool() ? `A` : `B`",
    "X = randomInt(0, 25)",
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

By default, [`Delta`](../reference/table-operations/update-by-operations/delta.md) uses `NULL_DOMINATES`, so differencing a number from a null will always return a null.

## Handle nulls with `Fill`

The [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) module provides the [`Fill`](../reference/table-operations/update-by-operations/fill.md) function to help deal with null data values. It fills in null values with the most recent non-null values, and like all [`updateby`](/core/pydoc/code/deephaven.updateby.html#module-deephaven.updateby) operations, can do so on a per-group basis.

Here's an example of using [`fill`](../reference/table-operations/update-by-operations/fill.md) to fill up null values by group:

```groovy order=result
source = emptyTable(100).update(
        "Timestamp = '2023-01-01T00:00:00 ET' + i * SECOND",
        "Letter = randomBool() ? `A` : `B`",
        "X = randomBool() ? NULL_INT : randomInt(0, 25)",
)

result = source.updateBy(Fill("FillX=X"), "Letter")
```

## Related documentation

- [Create a time table](./time-table.md)
- [Create a new or empty table](./new-and-empty-table.md)
- [How to create plots with the legacy API](./plotting/api-plotting.md)
- [How to use select, view, and update](./use-select-view-update.md)
- [How to use update_by](./use-update-by.md)
- [Handle nulls, infs, and NaNs](./handle-null-inf-nan.md)
- [Formulas](../how-to-guides/formulas-how-to.md)
- [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)
- [`RollingGroup`](../reference/table-operations/update-by-operations/rolling-group.md)
- [`delta`](../reference/table-operations/update-by-operations/delta.md)
- [`Fill`](../reference/table-operations/update-by-operations/fill.md)
