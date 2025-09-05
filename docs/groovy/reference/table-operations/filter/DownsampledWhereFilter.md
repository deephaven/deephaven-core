---
title: DownsampledWhereFilter
---

The `DownsampledWhereFilter` allows users to downsample time series data by calculating the bin intervals for values, and then using [`upperBin`](../../time/datetime/upperBin.md) and [`firstBy`](../group-and-aggregate/firstBy.md) to select the last row for each bin.

> [!CAUTION]
> The column containing the data to be binned must be sorted for the method to work.

## Syntax

```groovy syntax
DownsampledWhereFilter(column, binSize)
DownsampledWhereFilter(column, binSize, order)
```

## Parameters

<ParamTable>
<Param name="column" type="String">

[Instant](../../../reference/query-language/types/date-time.md) column to use for filtering.

</Param>
<Param name="binSize" type="long">

Size in nanoseconds fo the time bins. [Constants](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MINUTE) like `DateTimeUtils.MINUTE` are often used.

</Param>
<Param name="order" type="DownsampledWhereFilter.SampleOrder">

By default, this method downsamples bins by `upperBin` and `lastBy`, represented by the `UPPERLAST` constant. Use `LOWERFIRST` to change this order to `lowerBin` and `firstBy`.

DownsampledWhereFilter.SampleOrder sets the desired behavior:

- `DownsampledWhereFilter.SampleOrder.UPPERLAST` - constant for `upperBin`/`lastBy`.
- `DownsampledWhereFilter.SampleOrder.LOWERFIRST` - constant for `lowerBin`/`firstBy`.

</Param>
</ParamTable>

## Returns

A `DownsampledWhereFilter` that can be used in a `.where` clause to downsample time series rows.

## Examples

```groovy order=source,result
import io.deephaven.engine.table.impl.select.DownsampledWhereFilter

startTime = minus(now(), parseDuration("PT1H"))

source = timeTable(startTime, "PT1S").head(100).snapshot()

F = new DownsampledWhereFilter("Timestamp", SECOND * 30, DownsampledWhereFilter.SampleOrder.LOWERFIRST)

result = source.where(F)
```

## Related documentation

- [`firstBy`](../group-and-aggregate/firstBy.md)
- [`lowerBin`](../../time/datetime/lowerBin.md)
- [`upperBin`](../../time/datetime/upperBin.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/DownsampledWhereFilter.html)
