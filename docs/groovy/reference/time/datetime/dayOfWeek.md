---
title: dayOfWeek
---

`dayOfWeek` returns the day of the week, as a `java.time.DayOfWeek` value, for an input date-time in the specified time zone. To get the day of the week as an integer, with 1 being Monday and 7 being Sunday, use `dayOfWeekValue` instead.

## Syntax

```
dayOfWeek(instant, timeZone)
dayOfWeek(dateTime)
dayOfWeek(localDateTime)
dayOfWeek(localDate)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The instant for which to find the day of the week.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone to use when interpreting the [date-time](../../query-language/types/date-time.md).

</Param>
<Param name="dateTime" type="ZonedDateTime">

The zoned date-time for which to find the day of the week.

</Param>
<Param name="localDateTime" type="LocalDateTime">

The local date-time for which to find the day of the week.

</Param>
<Param name="localDate" type="LocalDate">

The local date for which to find the day of the week.

</Param>
</ParamTable>

## Returns

Returns the day of the week as a `java.time.DayOfWeek` value, such as `THURSDAY`.

## Examples

```groovy order=:log
datetime = parseInstant("2024-02-29T01:23:45 ET")
datetime_zoned = toZonedDateTime(datetime, timeZone("MT"))

day = dayOfWeek(datetime, timeZone("ET"))
day_zoned = dayOfWeek(datetime_zoned)

println day
println day_zoned
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`toZonedDateTime`](./toZonedDateTime.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfWeek(java.time.Instant,java.time.ZoneId))
