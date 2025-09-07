---
title: minuteOfDay
---

`minuteOfDay` returns the minutes since the top of the day (midnight) in the specified time zone.

## Syntax

```
minuteOfDay(instant, timeZone, localTime)
minuteOfDay(dateTime, localTime)
minuteOfDay(localTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) from which to return the number of minutes.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) from which to return the number of minutes.

</Param>
<Param name="localTime" type="boolean">

Set this parameter to `false` if you need Deephaven to account for daylight savings time.

- If `false`, returns the number of minutes from the start of the day. However, on days when daylight savings time events occur, results may be different from what is expected based on the local time. For example, on DST change days, 9:30 AM may be earlier or later in the day based on whether the daylight savings time adjustment is forward or backward. On non-DST days, the result is the same as if `localTime` is `false`.
- If `true`, returns the number of minutes from the start of the day according to the local time. In this case, `9:30` will always return the same value.

</Param>
</ParamTable>

## Returns

Returns an int value of minutes since midnight for a specified [date-time](../../query-language/types/date-time.md).

## Examples

```groovy order=:log
datetime = parseInstant("2022-01-01T12:34:56 ET")

minute = minuteOfDay(datetime, timeZone("ET"), false)

println minute
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#minuteOfDay(java.time.Instant,java.time.ZoneId,boolean))
