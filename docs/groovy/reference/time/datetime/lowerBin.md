---
title: lowerBin
---

`lowerBin` returns a [date-time](../../query-language/types/date-time.md) value, which is at the starting (lower) end of a time range defined by the interval. For example, calculating the lower bin of a time given a 15-minute interval value would return the [date-time](../../query-language/types/date-time.md) value for the start of the fifteen-minute window (00-15, 15-30, 30-45, 45-60) that contains the input date-time.

## Syntax

```
lowerBin(instant, intervalNanos)
lowerBin(instant, interval)
lowerBin(dateTime, intervalNanos)
lowerBin(dateTime, interval)
lowerBin(instant, intervalNanos, offset)
lowerBin(instant, interval, offset)
lowerBin(dateTime, intervalNanos, offset)
lowerBin(dateTime, interval, offset)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) for which to evaluate the start of the containing window.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) for which to evaluate the start of the containing window.

</Param>
<Param name="intervalNanos" type="long">

The time interval represented as nanoseconds. If this parameter is used, the offset parameter must also be in nanoseconds.

</Param>
<Param name="interval" type="Duration">

The time interval represented as a [Duration](../../query-language/types/durations.md). If this parameter is used, the offset parameter must also be a [Duration](../../query-language/types/durations.md).

</Param>
<Param name="offset" type="long">

The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by one minute.

</Param>
<Param name="offset" type="Duration">

The window start offset as a [Duration](../../query-language/types/durations.md). For example, a value of "PT1M" would offset all windows by one minute.

</Param>
</ParamTable>

## Returns

A [date-time](../../query-language/types/date-time.md) representing the start of the window.

## Example

The following example converts a [date-time](../../query-language/types/date-time.md) to the lower end of a 15-minute interval. Output is shown for no offset, and an offset of 2 minutes.

```groovy order=null
import io.deephaven.time.DateTimeUtils
datetime = parseInstant("2020-01-01T00:35:00 ET")
nanos_bin = MINUTE * 15
nanos_offset = MINUTE * 2
duration_bin = parseDuration("PT15M")
duration_offset = parseDuration("PT2M")

// no offset
result1 = DateTimeUtils.lowerBin(datetime, nanos_bin)
println result1

// offset of two minutes
result2 = DateTimeUtils.lowerBin(datetime, nanos_bin, nanos_offset)
println result2

// repeat result2, but with duration types
result2_durations = DateTimeUtils.lowerBin(datetime, duration_bin, duration_offset)
println result2_durations
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.ZonedDateTime,long))
