---
title: upperBin
---

`upperBin` returns a [date-time](../../query-language/types/date-time.md) value, which is at the ending (upper) end of a time range defined by the interval nanoseconds. For example, a `5*MINUTE` intervalNanos value would return the instant value for the end of the five-minute window that contains the input instant.

## Syntax

```
upperBin(instant, intervalNanos)
upperBin(instant, interval)
upperBin(dateTime, intervalNanos)
upperBin(dateTime, interval)
upperBin(instant, intervalNanos, offset)
upperBin(instant, interval, offset)
upperBin(dateTime, intervalNanos, offset)
upperBin(dateTime, interval, offset)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) for which to evaluate the end of the containing window.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) for which to evaluate the end of the containing window.

</Param>
<Param name="intervalNanos" type="long">

The time interval represented as nanoseconds. If this parameter is used, the offset parameter must also be in nanoseconds.

</Param>
<Param name="interval" type="Duration">

The time interval represented as a [Duration](../../query-language/types/durations.md). If this parameter is used, the offset parameter must also be a [Duration](../../query-language/types/durations.md).

</Param>
<Param name="offset" type="long">

The window end offset in nanoseconds. For example, a value of MINUTE would offset all windows by one minute.

</Param>
<Param name="offset" type="Duration">

The window end offset as a [Duration](../../query-language/types/durations.md). For example, a value of "PT1M" would offset all windows by one minute.

</Param>
</ParamTable>

## Returns

A [date-time](../../query-language/types/date-time.md) representing the end of the window.

## Example

The following example converts a [date-time](../../query-language/types/date-time.md) to the upper end of a 15-minute interval. Output is shown for no offset and an offset of 2 minutes.

```groovy order=null
import io.deephaven.time.DateTimeUtils
datetime = parseInstant("2020-01-01T00:35:00 ET")
nanos_bin = MINUTE * 15
nanos_offset = MINUTE * 2
duration_bin = parseDuration("PT15M")
duration_offset = parseDuration("PT2M")

// no offset
result1 = DateTimeUtils.upperBin(datetime, nanos_bin)
println result1

// offset of two minutes
result2 = DateTimeUtils.upperBin(datetime, nanos_bin, nanos_offset)
println result2

// repeat result2, but with duration types
result2_durations = DateTimeUtils.upperBin(datetime, duration_bin, duration_offset)
println result2_durations
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.ZonedDateTime,long,long))
