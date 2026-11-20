---
title: millisOfDay
---

`millisOfDay` returns the number of milliseconds since midnight for a specified [date-time](../../query-language/types/date-time.md).

## Syntax

```
millisOfDay(instant, timeZone, asLocalTime)
millisOfDay(dateTime, asLocalTime)
millisOfDay(localDateTime)
millisOfDay(localTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) from which to return the elapsed time.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) from which to return the elapsed time.

</Param>
<Param name="asLocalTime" type="boolean">

Set this parameter to `false` if you need Deephaven to account for daylight savings time.

- If `false`, returns the number of milliseconds from the start of the day. However, on days when daylight savings time events occur, results may be different from what is expected based on the local time. For example, on DST change days, 9:30 AM may be earlier or later in the day based on whether the daylight savings time adjustment is forward or backward.
- If `true`, returns the number of milliseconds from the start of the day according to the local time. In this case, `9:30` will always return the same value.

On non-DST days, the result is the same regardless of the value of `asLocalTime`.

</Param>
<Param name="localDateTime" type="LocalDateTime">

The local date-time from which to return the elapsed time.

</Param>
<Param name="localTime" type="LocalTime">

The local time from which to return the elapsed time.

</Param>
</ParamTable>

## Returns

Returns the number of milliseconds since midnight for the [date-time](../../query-language/types/date-time.md), or `NULL_INT` if `instant`, `timeZone`, `dateTime`, `localDateTime`, or `localTime` is `null`.

## Examples

```groovy order=:log
datetime = parseInstant("2023-09-09T12:34:56.123456 ET")

millis = millisOfDay(datetime, timeZone("ET"), false)

println millis
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#millisOfDay(java.time.ZonedDateTime,boolean))
