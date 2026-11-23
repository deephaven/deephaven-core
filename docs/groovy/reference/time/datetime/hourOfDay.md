---
title: hourOfDay
---

`hourOfDay` returns an int representing the hour of the input [date-time](../../query-language/types/date-time.md). Hours are from 0-23, with times between midnight and 1AM on a given day returning the hour `0`.

> [!NOTE]
> On days when daylight savings time events occur, results may be different from what is expected based upon the local time. For example, on daylight savings time change days, 9:30AM may be earlier or later in the day based upon if the daylight savings time adjustment is forwards or backwards.

## Syntax

```
hourOfDay(instant, timeZone, asLocalTime)
hourOfDay(dateTime, asLocalTime)
hourOfDay(localDateTime)
hourOfDay(localTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The time for which to find the hour of the day.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone to use when interpreting the [date-time](../../query-language/types/date-time.md).

</Param>
<Param name="dateTime" type="ZonedDateTime">

The zoned date-time for which to find the hour of the day.

</Param>
<Param name="asLocalTime" type="boolean">

Set this parameter to `false` if you need Deephaven to account for daylight savings time.

- If `false`, returns the number of hours from the start of the day. However, on days when daylight savings time events occur, results may be different from what is expected based on the local time. For example, on DST change days, 9:30 AM may be earlier or later in the day based on whether the daylight savings time adjustment is forward or backward.
- If `true`, returns the number of hours from the start of the day according to the local time. In this case, `9:30` will always return the same value.

On non-DST days, the result is the same regardless of the value of `asLocalTime`.

</Param>
<Param name="localDateTime" type="LocalDateTime">

The local date-time for which to find the hour of the day.

</Param>
<Param name="localTime" type="LocalTime">

The local time for which to find the hour of the day.

</Param>
</ParamTable>

## Returns

Returns a 0-based int value of the hour of the day.

## Examples

```groovy order=:log
datetime = parseInstant("2024-02-29T01:23:45 ET")
datetime_zoned = toZonedDateTime(datetime, timeZone("MT"))

hour = hourOfDay(datetime, timeZone("ET"), false)
hour_zoned = hourOfDay(datetime_zoned, false)

println hour
println hour_zoned
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#hourOfDay(java.time.Instant,java.time.ZoneId,boolean))
